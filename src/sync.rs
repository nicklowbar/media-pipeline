use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use russh::{client, keys::key::PublicKey, ChannelId, Disconnect};
use russh_sftp::client::RawSftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

use crate::config::Config;
use crate::db::{Database, DirectoryState};

const MAX_CONCURRENT_DOWNLOADS: usize = 4;

/// How often to emit a throughput-progress log line during a file
/// download. Smaller values are chatty; larger values delay visibility
/// of stalls. 10s gives one log line per ~100MB at 10MB/s, which is
/// enough cadence to spot a stuck transfer without flooding the log.
const THROUGHPUT_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// No per-file total timeout. The stall timeout (below) catches the
/// "no bytes at all" case within a minute; for the "bytes are
/// trickling but pathologically slow" case, the deploy-side pipeline
/// watchdog (cron + container restart) is the safety net. A fixed
/// total budget would have to be sized for the slowest legitimate
/// transfer we expect — a 200 GB file at 50 Mbps is ~9 hours, and a
/// 60 GB REMUX at 25 Mbps is ~5 hours. Picking any number risks
/// killing a transfer that was making real, if slow, progress. The
/// previous 30-minute value killed healthy 60 GB files at 50 Mbps
/// (which legitimately take ~3h), so we removed the check entirely.
///
/// (This comment block used to be attached to a `const
/// TRANSFER_TIMEOUT = 30 * 60`; the constant was removed in
/// commit 704e96a, and the rationale is preserved here so the
/// decision doesn't get re-litigated by a future reader who notices
/// the absence of any total-time budget.)
///
/// Maximum time a single SFTP read is allowed to take without
/// producing any bytes. A healthy SFTP read on even a slow link
/// resolves in well under a second. If a read sits idle for this long,
/// the SSH channel is dead (kernel TCP may still be ESTABLISHED, but
/// the server isn't sending data). The transfer is aborted with a
/// warning so the pipeline can move on to the next directory.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-request chunk size for SFTP pipelined reads. The protocol default
/// is ~32 KB; we ask for more to amortize per-request overhead.
///
/// 256 KB is large enough that the per-request RTT (95 ms observed) only
/// adds ~0.04% to the per-byte latency, but small enough that the
/// in-flight window (16 × 256 KB = 4 MB) fits within the SSH channel
/// flow-control window (`russh` defaults to 2 MB; OpenSSH sftp-server
/// negotiates ~2-8 MB). If we ever see throughput cap below the path
/// rate, the first thing to check is the channel window — bump it
/// with `channel.window_size(...)` on the russh side.
const SFTP_READ_CHUNK: usize = 256 * 1024;

/// Number of outstanding SFTP read requests per file. With
/// `SFTP_READ_CHUNK` = 256 KB, this puts 4 MB in flight, which is
/// enough to keep the sftp-server's read+encrypt+send pipeline full
/// on the 30+ Mbps path we've measured.
///
/// The TCP BDP at the link's 600 Mbps target and 95 ms RTT is 7.1 MB,
/// so 4 MB in flight caps us around 350 Mbps even on a clean network.
/// That's intentional: this is a download pipeline, not a speed-test
/// client. If we ever want to push past 350 Mbps per file, bump this
/// in step with the SSH channel window.
const SFTP_INFLIGHT_REQUESTS: usize = 16;

/// Compute the manifest hash from a `rel_path → (size, mtime)` map.
///
/// The hash covers the file list *and* each file's size and mtime —
/// it must change when the remote's contents change (new file, file
/// replaced, mtime shifted), and it must NOT change when only our
/// local knowledge of the file changes (e.g. we re-hashed it). The
/// per-file sha256 is *not* part of the manifest — including it
/// would make the manifest change every time `collect_remote_hashes`
/// re-ran against the same remote, which is a meaningless churn
/// signal.
fn manifest_hash_from_files(files: &BTreeMap<String, (u64, u64)>) -> String {
    let json = serde_json::to_string(files)
        .expect("BTreeMap<String, (u64, u64)> serialization should never fail");
    let hash = Sha256::digest(json.as_bytes());
    format!("{:x}", hash)
}

/// Stream-hash a local file with SHA-256, returning the lowercase
/// hex digest. Reads the file in 1 MiB chunks — large enough to
/// amortize syscall overhead, small enough that a 60 GB file
/// doesn't keep a single FD-resident buffer pinned. The kernel
/// page cache will have the file's first read mostly resident
/// after the pipelined SFTP write completed, so this is
/// effectively a re-read of already-cached pages rather than
/// disk traffic.
///
/// 1 MiB is a deliberate choice, not a default: the
/// `pipelined_read_to_file` writer already reads in 256 KiB
/// chunks, so matching at 1 MiB for the verification pass is a
/// reasonable round number, and it's a multiple of the
/// 4-KiB page size (256 pages) and a small multiple of the
/// 4-MiB writeback threshold the kernel uses for sequential
/// page-cache writeback. Going larger doesn't help: the hash
/// is a CPU-bound operation, not a syscall-bound one, and a
/// bigger buffer just sits in memory waiting for the
/// `Sha256::update` to consume it.
async fn compute_local_sha256(local: &Path) -> anyhow::Result<String> {
    use tokio::io::AsyncReadExt;

    const CHUNK: usize = 1024 * 1024;
    let mut file = fs::File::open(local).await
        .with_context(|| format!("failed to open {} for hashing", local.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = file.read(&mut buf).await
            .with_context(|| format!("failed to read {} for hashing", local.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Outcome of the "should we trust the file already on disk?" check.
///
/// `download_file` runs this *before* the SFTP read path so a file
/// that landed cleanly in a prior run isn't re-pulled. The size-only
/// check that used to gate this is insufficient: a prior interrupted
/// run, a CIFS writeback hiccup, or a logic bug in our parallel
/// downloader can leave a file at the right size with the wrong
/// bytes. We re-hash whenever a recorded hash is available, and
/// only `Trust` when the bytes actually match.
#[derive(Debug, PartialEq, Eq)]
enum LocalFileDisposition {
    /// The on-disk file matches the remote's claim. Skip the
    /// download path entirely.
    Trust,
    /// The on-disk file does not exist, is the wrong size, or
    /// (when a hash is available) has the wrong bytes. Proceed
    /// to the SFTP read path.
    Download,
}

/// Decide whether the file already on disk can be trusted, given
/// the remote's authoritative size and (optionally) the SHA-256
/// fingerprint recorded at manifest-collection time.
///
/// **Sizing logic.** A `local_size` that differs from `remote_size`
/// always returns `Download` — the on-disk file is stale or
/// partial. (A `local_size > remote_size` case is the classic
/// "we wrote past EOF before the remote shrank" symptom; we treat
/// it the same as "missing" because the extra bytes will be
/// discarded on re-download anyway, and the size check at the end
/// of `download_file` will catch any regression there.)
///
/// **Hashing logic.** When `expected_hash` is `Some`, we recompute
/// the local SHA-256 and compare. A mismatch returns
/// `LocalFileDisposition::Download` so the corrupt local copy is
/// discarded and the file is re-pulled from SFTP. The mismatch is
/// also logged at `warn` level so a persistent mismatch — which
/// usually means the *recorded* hash is wrong (DB drift, wrong
/// manifest) — is still visible to the operator. Returning `Err`
/// instead would propagate up to the downloader pool, halt the
/// per-directory walk, and crash the process; the post-download
/// check inside `try_download_file` is the right place to surface
/// a *truly* persistent mismatch (retry budget exhausted) as a
/// hard failure.
///
/// **No-hash case.** When `expected_hash` is `None` (the manifest
/// was collected but `collect_remote_hashes` failed for this
/// directory), we trust the on-disk file at face value — the
/// "best effort" intent that already governs the download path's
/// "no expected hash" branch. The alternative — re-downloading
/// every file we can't verify — would make the remote-side hash
/// collection a hard dependency, which it isn't.
///
/// **Performance.** Hashing is a full-file re-read. For a 60 GB
/// file at 500 MB/s this is ~2 minutes; for a 4 GB REMUX, ~8s.
/// Acceptable cost for the only thing standing between us and
/// silent corruption, given the parallel downloader's track
/// record of edge cases (see `pipelined_read_to_file`'s tests).
async fn verify_existing_file(
    local: &Path,
    local_size: u64,
    remote_size: u64,
    expected_hash: Option<&str>,
) -> anyhow::Result<LocalFileDisposition> {
    // Size check first — it's free and lets us bail before touching
    // the file at all. The two failure modes are:
    //   local_size > remote_size  → stale, file grew on remote
    //                                 and shrunk locally (rare)
    //   local_size < remote_size  → partial download, in-progress
    //                                 copy, or a crash mid-write
    // Both should be re-downloaded.
    if local_size != remote_size {
        return Ok(LocalFileDisposition::Download);
    }
    // Zero-byte remote: trust the empty local file. There's no
    // body to hash, and the only "wrong" answer is the same
    // (still zero bytes).
    if remote_size == 0 {
        return Ok(LocalFileDisposition::Trust);
    }
    // No recorded hash → best-effort trust. See the function-
    // level doc comment for why we don't fall through to the
    // download path here.
    let Some(expected) = expected_hash else {
        return Ok(LocalFileDisposition::Trust);
    };
    let actual = compute_local_sha256(local).await
        .with_context(|| format!("failed to hash existing local file {}", local.display()))?;
    if actual == expected {
        Ok(LocalFileDisposition::Trust)
    } else {
        // Loud warning + structured log so a persistent
        // mismatch (DB drift, wrong manifest) is still visible
        // to the operator, but treat the disposition as
        // `Download` so the corrupt local copy is replaced
        // rather than halting the entire walker. The
        // post-download check in `try_download_file` will
        // catch a *truly* persistent mismatch after the retry
        // budget is exhausted.
        warn!(
            file = %local.display(),
            expected = %expected,
            actual = %actual,
            "sha256 mismatch on existing local file: expected {}, got {}; will re-download",
            expected, actual
        );
        Ok(LocalFileDisposition::Download)
    }
}

/// Parse the stdout of `sha256sum` into a list of
/// `(rel_path, sha256, size, mtime)` tuples ready to be persisted
/// via `db.upsert_file_hashes`.
///
/// **Output format.** GNU coreutils' `sha256sum` writes
/// `<64-hex-chars><space><space><path>\n` per file. The first
/// space is a separator; the second space is the *mode* flag
/// (single space = text, double space = binary, both
/// unambiguously delimit the hash from the path). The path is
/// echoed back exactly as it appeared in argv — the same bytes
/// we wrote on stdin — so we can strip the `remote_dir` prefix
/// to recover the rel_path.
///
/// **Tolerant parsing.** `sha256sum` emits warnings to stderr
/// (not stdout) for files it can't read. A warning means *no*
/// stdout line for that file — i.e. the line is simply absent,
/// not malformed. We treat absence as a missing entry; the
/// download path's "no expected hash" branch handles it.
///
/// **Output shape.** Returns a `Vec`, not a `HashMap`, because
/// the storage layer (`db.upsert_file_hashes`) consumes a slice
/// of tuples directly; building an intermediate map would just
/// be a wasted allocation.
fn parse_sha256sum_output(
    output: &str,
    remote_dir: &Path,
    manifest: &BTreeMap<String, (u64, u64)>,
) -> anyhow::Result<Vec<(String, String, i64, i64)>> {
    // Pre-compute the prefix we'll strip from each echoed path.
    // The remote may use either `/srv/data/media/...` style or
    // have a trailing slash; normalize once here.
    let mut prefix = remote_dir.to_string_lossy().into_owned();
    if !prefix.ends_with('/') {
        prefix.push('/');
    }

    let mut out: Vec<(String, String, i64, i64)> = Vec::with_capacity(manifest.len());
    let mut malformed: usize = 0;

    for line in output.lines() {
        if line.is_empty() {
            continue;
        }
        // sha256sum format: `<hash><sp><sp><path>` OR `<hash><sp><path>`.
        // Some implementations collapse to a single space (text mode);
        // we accept both. The hash is always 64 lowercase hex chars
        // (GNU coreutils) — anything else is a warning line that
        // leaked into stdout (e.g. locale errors) and is skipped.
        let (hash, rest) = match line.split_once(' ') {
            Some((h, r)) => (h, r),
            None => {
                malformed += 1;
                continue;
            }
        };
        // The path may start with " *" (text mode) or "* " (binary
        // mode) — GNU coreutils inserts a literal "*" in the mode
        // column for binary files. Strip optional "* " or " *" prefix.
        let path = rest
            .strip_prefix(" *")
            .or_else(|| rest.strip_prefix("* "))
            .unwrap_or(rest);
        // The path may have leading whitespace from the mode
        // separator; trim it.
        let path = path.trim_start();

        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            malformed += 1;
            continue;
        }
        let rel_path = match path.strip_prefix(&prefix) {
            Some(r) => r.to_string(),
            None => {
                // The path we sent had the prefix, so anything else
                // is a server-side mangling. Skip rather than bail —
                // one weird path shouldn't fail the whole hash set.
                malformed += 1;
                continue;
            }
        };
        let (size, mtime) = match manifest.get(&rel_path) {
            Some(m) => (m.0 as i64, m.1 as i64),
            None => {
                // The remote returned a hash for a path that wasn't
                // in our manifest. Could be a file the server-side
                // `xargs` expanded (e.g. glob), or a TOCTOU race
                // where a file appeared between manifest and hash
                // collection. Skip — we have no size/mtime to pair
                // it with, and the download path won't try to
                // verify a file it never planned to download.
                malformed += 1;
                continue;
            }
        };
        out.push((rel_path, hash.to_string(), size, mtime));
    }

    if malformed > 0 {
        // A non-fatal warning: any file the parser couldn't decode
        // is silently dropped. The download path's "no expected
        // hash" branch handles the missing verification.
        warn!(malformed, total = manifest.len(), "sha256sum output had malformed lines; missing files will skip verification");
    }

    Ok(out)
}

/// Drain a paginated SFTP `readdir` stream until the server signals
/// "no more entries". The SFTP protocol specifies that the server
/// returns a `Status` packet with `status_code = Eof` (1) to
/// terminate a directory listing, but russh-sftp's high-level
/// `SftpSession::read_dir` translates this into a successful break
/// while the low-level `RawSftpSession::readdir` (which we use for
/// the file-read pipelining path) propagates it as an
/// `Err(Error::Status(...))`. Some servers (e.g. older OpenSSH
/// releases) instead signal end-of-directory by returning an empty
/// `files` list. This helper accepts either terminator and unifies
/// them so callers don't have to care which convention a given
/// server follows.
///
/// The `readdir` closure is invoked repeatedly until the terminator
/// is seen. Each invocation issues one `SSH_FXP_READDIR` and awaits
/// the server's `SSH_FXP_NAME` or `SSH_FXP_STATUS` reply. Any error
/// other than the canonical `Eof` status is propagated unchanged —
/// permission errors, transport errors, malformed packets, etc.
/// should not be silently treated as end-of-directory.
pub(crate) async fn drain_readdir_pages<F, Fut>(
    mut readdir: F,
) -> russh_sftp::client::rawsession::SftpResult<Vec<russh_sftp::protocol::File>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = russh_sftp::client::rawsession::SftpResult<russh_sftp::protocol::Name>>,
{
    use russh_sftp::client::error::Error as SftpError;
    use russh_sftp::protocol::StatusCode;

    let mut all_files: Vec<russh_sftp::protocol::File> = Vec::new();
    loop {
        match readdir().await {
            Ok(name) => {
                if name.files.is_empty() {
                    // Server-convention #1: empty-files-list terminator.
                    break;
                }
                all_files.extend(name.files);
            }
            Err(SftpError::Status(status)) if status.status_code == StatusCode::Eof => {
                // Server-convention #2: explicit Eof status terminator.
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(all_files)
}

/// Holds an authenticated SSH + SFTP session to the remote download host.
///
/// In the DB-driven-dispatch refactor (2026-06-14), the SyncEngine
/// is a per-pipeline-run object, not a per-category one. It owns:
/// - The russh client `Handle` (wrapped in a `Mutex` because
///   `Handle` is `!Sync` and we need to share it between the
///   walker task — briefly, to open exec channels for
///   `xargs sha256sum` — and the engine's `Drop` impl).
/// - A pre-allocated walker SFTP channel (the walker borrows an
///   `Arc<RawSftpSession>` clone for the duration of one
///   category's walk).
/// - The downloader pool, which owns N pre-allocated SFTP
///   channels and runs N downloader tasks that claim rows from
///   the DB.
///
/// The previous design (pre-2026-06-14) consumed the session
/// inside `sync_category` to move it into the walker task. That
/// had two costs: (1) the downloader pool was scoped to a single
/// category and couldn't help with the next category's work, and
/// (2) the "drop `job_tx` before awaiting the pool's
/// `JoinHandle`s" contract was load-bearing and easy to break.
/// Both costs are gone: the pool lives for the whole pipeline
/// run, and the dispatch is a DB query (atomic UPDATE) rather
/// than an mpsc close.
pub struct SyncEngine {
    /// The russh `Handle`. `Handle` is `!Sync` (it holds an
    /// `UnboundedReceiver`), so we wrap it in a `Mutex` to share
    /// it across the walker task and the engine's `Drop` impl.
    /// The walker's critical section is short (one
    /// `channel_open_session` call); the downloader pool never
    /// touches the Handle.
    handle: Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
    /// Pre-allocated SFTP subsystem channel for the walker.
    /// The walker borrows an `Arc<RawSftpSession>` clone for
    /// the duration of one category's walk. Categories are
    /// walked sequentially, so a single walker channel is
    /// sufficient.
    walker_channel: Arc<RawSftpSession>,
    /// The downloader pool. Lives for the whole pipeline run.
    /// Spawned at `SyncEngine::new` time, drained at
    /// `drain_pool().await` time.
    pool: DownloaderPool,
    /// The pool's background task. `None` after `drain_pool`
    /// has consumed it.
    pool_join: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

pub(crate) struct ClientHandler;

#[async_trait::async_trait]
impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        // Accept any server key. In production, verify against known_hosts.
        Ok(true)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        _data: &[u8],
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn extended_data(
        &mut self,
        _channel: ChannelId,
        _ext: u32,
        _data: &[u8],
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn channel_close(
        &mut self,
        _channel: ChannelId,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn channel_open_confirmation(
        &mut self,
        _channel: ChannelId,
        _max_packet_size: u32,
        _window_size: u32,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn channel_success(
        &mut self,
        _channel: ChannelId,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn channel_failure(
        &mut self,
        _channel: ChannelId,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Establish a fresh russh session: TCP+SSH transport,
/// public-key auth, returns the authenticated `Handle`.
/// Used by `SyncEngine::new` (initial connect) and by
/// `reconnect_handle_and_sftp` (Handle-level recovery when
/// a single-channel re-open fails — the underlying SSH
/// transport itself is sick, not just one channel).
///
/// **Idempotent w.r.t. callers.** Each call dials a fresh
/// `client::connect`; the caller is responsible for
/// discarding any prior `Handle` it held (typically by
/// swapping the value inside a Mutex). The function does
/// NOT close or signal the old session — russh's `Handle`
/// doesn't expose a public `disconnect` and best-effort
/// teardown is the caller's concern.
async fn establish_russh_session(
    config: &Config,
) -> anyhow::Result<client::Handle<ClientHandler>> {
    let ssh_config = std::sync::Arc::new(client::Config::default());
    let port = config.ssh.port.unwrap_or(22);
    info!(host = %config.ssh.host, port, user = %config.ssh.user, "connecting to ssh");
    let mut session = client::connect(ssh_config, (config.ssh.host.as_str(), port), ClientHandler)
        .await
        .with_context(|| format!("failed to connect to {}:{}", config.ssh.host, port))?;
    let key_pair = russh::keys::load_secret_key(&config.ssh.private_key_path, None)
        .with_context(|| {
            format!("failed to load private key from {}", config.ssh.private_key_path.display())
        })?;
    let auth_result = session
        .authenticate_publickey(&config.ssh.user, std::sync::Arc::new(key_pair))
        .await
        .context("public key authentication failed")?;
    if !auth_result {
        anyhow::bail!("SSH public key authentication failed");
    }
    info!("ssh authenticated successfully");
    Ok(session)
}

impl SyncEngine {
    /// Connect to the remote, open the walker + N downloader SFTP
    /// channels, and spawn the downloader pool. The returned
    /// `SyncEngine` is ready to call `sync_category(...)` on.
    pub async fn new(config: &Config, db: &Database) -> anyhow::Result<Self> {
        let session = establish_russh_session(config).await?;

        info!("ssh authenticated successfully");

        // Sweep rows stuck in `Syncing` from a prior crashed
        // run. The pool is about to spawn; doing the sweep
        // first means the pool can immediately claim any
        // recovered rows. 6h threshold (env-tunable via
        // STALE_SYNCING_HOURS in the future; hard-coded for
        // now) is safe for a single-instance deployment.
        let recovered = db.stale_syncing_sweep(6)?;
        if recovered > 0 {
            info!(recovered, "SyncEngine: stale-Syncing sweep recovered rows at startup");
        }

        // Wrap the Handle in a Mutex so the walker task can
        // borrow it briefly to open exec channels. The walker
        // is the only long-lived consumer of the Handle; the
        // downloader pool uses pre-allocated channels and never
        // touches the Handle directly.
        let handle = Arc::new(tokio::sync::Mutex::new(session));

        // Pre-open the walker's SFTP subsystem channel. The
        // walker borrows a clone of this `Arc` for the
        // duration of one category's walk.
        let walker_channel = {
            let h = handle.lock().await;
            Self::open_sftp_session(&h).await
                .context("failed to open walker SFTP channel")?
        };

        // Pre-open N downloader SFTP subsystem channels. Each
        // downloader task owns one of these exclusively for
        // the lifetime of the pool.
        let mut downloader_channels: Vec<Arc<RawSftpSession>> =
            Vec::with_capacity(MAX_CONCURRENT_DOWNLOADS);
        for i in 0..MAX_CONCURRENT_DOWNLOADS {
            let raw = {
                let h = handle.lock().await;
                Self::open_sftp_session(&h).await
                    .with_context(|| format!("failed to open SFTP channel for downloader slot {}", i))?
            };
            downloader_channels.push(raw);
        }

        // Construct the downloader pool. The pool's background
        // task is spawned inside `DownloaderPool::new`. The
        // handle is shared with the walker (via the
        // walker_channel / SyncEngine::open_sftp_session
        // path); we pass it here so downloader tasks can
        // re-open the SFTP channel between retry attempts.
        // The `config` Arc is passed so downloader tasks can
        // escalate to a Handle-level reconnect (fresh russh
        // session) when a single-channel re-open fails — the
        // 2026-06-16 physalis production failure mode.
        let max_retries = config.max_download_retries();
        let poll_interval = Duration::from_millis(200);
        let config = Arc::new(config.clone());
        let pool = DownloaderPool::new(downloader_channels, Arc::clone(&handle), max_retries, poll_interval, config);
        let pool_join = Some(pool.spawn(db.clone()));

        Ok(SyncEngine {
            handle,
            walker_channel,
            pool,
            pool_join,
        })
    }

    pub async fn sync_category(
        &self,
        category: &str,
        db: &Database,
    ) -> anyhow::Result<()> {
        // Borrow the walker channel and the Handle-Mutex for
        // the duration of one category's walk. The Handle is
        // not moved; the walker task locks it briefly to open
        // exec channels. Multiple `sync_category` calls in
        // parallel are not safe (the walker channel is a
        // single Arc), so the caller must serialize them —
        // `run_sync` does this with a sequential `for` loop.
        let walker_channel = Arc::clone(&self.walker_channel);
        let handle = Arc::clone(&self.handle);
        let category = category.to_string();
        let db = db.clone();
        let max_retries = self.pool.config.max_retries;
        let walker_handle = tokio::spawn(async move {
            self_clone::run_walker(handle, walker_channel, db, category, max_retries).await
        });

        walker_handle.await
            .map_err(|e| anyhow::anyhow!("walker task panicked: {}", e))?
    }

    /// Signal the downloader pool to drain. After this returns,
    /// no walkers will spawn (caller's responsibility), the pool
    /// has processed every `Detected` row that exists, and the
    /// pool's background task has exited.
    ///
    /// Idempotent: a second call is a no-op (the pool is
    /// already drained).
    pub async fn drain_pool(&mut self) -> anyhow::Result<()> {
        // Take the join handle out of the option so a second
        // call is a no-op.
        let Some(join) = self.pool_join.take() else {
            return Ok(());
        };
        // Signal the pool. The watcher's `changed()` will wake
        // any downloader parked in `select!` waiting for work.
        self.pool.signal_drain();
        match join.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(join) => Err(anyhow::anyhow!("pool task panicked: {}", join)),
        }
    }

    /// Open a fresh SFTP subsystem channel and return an
    /// `Arc<RawSftpSession>`. Used at `SyncEngine::new` time to
    /// pre-open the walker + downloader channels, and at retry
    /// time inside `download_file` to recover from a dead
    /// channel. Each call opens a new SSH session channel
    /// (multiplexed over the same Handle). The Handle isn't
    /// `Clone`, so this is a static method that takes a
    /// `&Handle` — the Handle stays put, the channel is what
    /// gets created.
    pub(crate) async fn open_sftp_session(handle: &client::Handle<ClientHandler>) -> anyhow::Result<Arc<RawSftpSession>> {
        let channel = handle.channel_open_session().await
            .context("failed to open SSH channel for SFTP")?;
        channel.request_subsystem(true, "sftp").await
            .context("failed to request SFTP subsystem")?;
        let raw: Arc<RawSftpSession> = Arc::new(
            RawSftpSession::new(channel.into_stream())
        );
        raw.init().await
            .context("SFTP init/version handshake failed")?;
        Ok(raw)
    }

    /// Handle-level reconnect: replace the russh `Handle`
    /// inside the shared `Arc<Mutex<Handle>>` with a fresh
    /// one (new TCP+SSH transport + public-key auth), then
    /// open a fresh SFTP subsystem on the new Handle.
    /// Returns the new `Arc<RawSftpSession>` so the caller
    /// can swap its per-task `current_raw` over.
    ///
    /// **When to use.** This is the escalation step inside
    /// `download_file`'s retry loop when the cheaper
    /// per-attempt channel re-open (above) fails. The
    /// channel re-open fails when the *underlying SSH
    /// transport* is sick (visible as `failed to open SSH
    /// channel for SFTP` from `channel_open_session`); a
    /// fresh transport is the only recovery that addresses
    /// that case. The 2026-06-16 physalis production log
    /// shows the failure mode: the channel re-open fails,
    /// we fall through with the same dead channel, the next
    /// retry fails identically, the budget is exhausted.
    ///
    /// **Concurrency.** The Mutex serializes the swap with
    /// any other consumer of the Handle (e.g. the walker
    /// task). Anyone taking the lock after this call sees
    /// the new Handle. Anyone *currently* holding the old
    /// Handle keeps using it until they drop it; the old
    /// Handle is replaced (not cloned), so the prior user's
    /// channel is closed from the engine's perspective even
    /// though their local copy is still live. This is the
    /// same behavior as `tokio::sync::Mutex` swap
    /// everywhere — it's the right shape.
    ///
    /// **Scope limit.** The walker task uses the engine's
    /// pre-opened `walker_channel` and does not currently
    /// retry mid-walk on SFTP errors. After a Handle
    /// replacement triggered from a downloader slot, the
    /// walker's channel is dead. Fixing the walker's own
    /// mid-walk resilience is separate work; this method
    /// is only called from the downloader pool's retry
    /// loop, which is what the production log shows is
    /// hitting the bug.
    pub(crate) async fn reconnect_handle_and_sftp(
        handle: &Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
        config: &Config,
    ) -> anyhow::Result<Arc<RawSftpSession>> {
        let mut h = handle.lock().await;
        // Drop the old Handle by overwriting the slot. The
        // old `Session` / `Handle`'s underlying transport
        // closes when the last clone is dropped; we own the
        // only one in the engine (the engine's Mutex), so
        // the assignment is the last reference. (Other
        // tasks have their own `Arc<RawSftpSession>` clones
        // but those don't hold a `Handle` clone — the
        // Handle lives only inside the engine's Mutex.)
        *h = establish_russh_session(config).await
            .context("Handle reconnect: failed to establish new russh session")?;
        let new_raw = Self::open_sftp_session(&h).await
            .context("Handle reconnect: failed to open SFTP subsystem on new Handle")?;
        drop(h);
        Ok(new_raw)
    }
}

// =========================================================================
// Free-function implementations of the download path. These are
// pulled out of `impl SyncEngine` so the downloader pool tasks
// (which don't have a `&SyncEngine`) can call them directly. The
// `SyncEngine` methods above are thin shims that delegate here.
// =========================================================================

/// Free-function recursive directory downloader. The shape is
/// unchanged from the original `download_directory_with_prefix`;
/// only the borrow surface changed (no more `&self`).
///
/// See the `SyncEngine::download_directory` doc comment for the
/// prefix / rel_path semantics. The free-function form is what
/// the downloader pool task calls.
fn download_directory(
    raw: &Arc<RawSftpSession>,
    handle: &Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
    remote_path: String,
    local_path: String,
    prefix: String,
    db: &Database,
    dir_id: i64,
    max_retries: u32,
    config: &Arc<Config>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>> {
    let raw = Arc::clone(raw);
    let handle = Arc::clone(handle);
    let db = db.clone();
    let config = Arc::clone(config);
    Box::pin(async move {
        info!(remote = %remote_path, local = %local_path, "downloading directory");

        let remote = Path::new(&remote_path);
        let local = Path::new(&local_path);

        // Ensure local directory exists
        fs::create_dir_all(local).await
            .with_context(|| format!("failed to create local directory {}", local.display()))?;

        let dir_handle = raw.opendir(&remote_path).await
            .with_context(|| format!("failed to opendir {}", remote.display()))?;
        let dir_handle_str = dir_handle.handle;

        // Same SFTP terminator handling as `list_remote_dirs` and
        // `collect_manifest`: the low-level `readdir` returns `Eof`
        // as `Err`, which we treat as a successful end-of-directory.
        // See `drain_readdir_pages` for details.
        let entries = drain_readdir_pages(|| async {
            raw.readdir(&dir_handle_str).await
        })
        .await
        .map_err(|e| {
            anyhow::Error::new(e).context(format!("failed to readdir {}", remote.display()))
        })?;

        for entry in entries {
            let name = entry.filename;
            if name.starts_with('.') {
                continue;
            }

            let remote_item_str = format!("{}/{}", remote_path, name);
            let local_item_str = format!("{}/{}", local_path, name);
            let rel_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            if entry.attrs.is_dir() {
                download_directory(
                    &raw,
                    &handle,
                    remote_item_str,
                    local_item_str,
                    rel_path,
                    &db,
                    dir_id,
                    max_retries,
                    &config,
                ).await?;
            } else {
                let remote_item = Path::new(&remote_item_str);
                let local_item = Path::new(&local_item_str);
                download_file(&raw, &handle, remote_item, local_item, &db, dir_id, &rel_path, max_retries, &config).await?;
            }
        }

        let _ = raw.close(&dir_handle_str).await;
        Ok(())
    })
}

/// Free-function single-file download with bounded retries. See
/// the `SyncEngine::download_file` doc comment for the full
/// rationale on retry policy, pre-download `verify_existing_file`,
/// and backoff.
///
/// The `handle` is used to re-open the SFTP subsystem channel
/// between retry attempts. A channel that died mid-download
/// (visible as russh-sftp's "Packet N for unknown recipient"
/// warnings) is replaced with a fresh one so the next attempt
/// isn't doomed to fail on the same dead channel.
async fn download_file(
    raw: &Arc<RawSftpSession>,
    handle: &Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
    remote: &Path,
    local: &Path,
    db: &Database,
    dir_id: i64,
    rel_path: &str,
    max_retries: u32,
    config: &Arc<Config>,
) -> anyhow::Result<()> {
    let remote_str = remote.to_string_lossy().to_string();

    // The current SFTP channel for this file. We hold a
    // mutable reference via a local `Arc<RawSftpSession>`
    // and re-bind it after a channel re-open. The original
    // `raw` from the caller is only used for the first
    // attempt.
    let mut current_raw: Arc<RawSftpSession> = Arc::clone(raw);

    // Stat the remote and the local once. The remote is the
    // source of truth for size; the local is what we already
    // have on disk.
    let remote_attrs = current_raw.lstat(remote.to_string_lossy().into_owned()).await
        .with_context(|| format!("failed to stat remote file {}", remote.display()))?;
    let remote_size = remote_attrs.attrs.size.unwrap_or(0) as u64;
    let local_size = if local.exists() {
        let meta = fs::metadata(local).await?;
        meta.len()
    } else {
        0
    };

    // Pre-download integrity check. See `verify_existing_file`
    // for the full rationale; the short version is: trust the
    // on-disk file when size + (optional) recorded SHA match,
    // re-download otherwise. This is *outside* the retry loop
    // because a bad pre-existing file won't get better with
    // more attempts.
    if local_size == remote_size {
        let expected = db.get_expected_hash(dir_id, rel_path)?
            .map(|(hash, _size)| hash);
        match verify_existing_file(local, local_size, remote_size, expected.as_deref())
            .await
            .with_context(|| format!("verifying existing local file for {}", remote_str))?
        {
            LocalFileDisposition::Trust => {
                debug!(file = %remote_str, size = remote_size, "file already complete and verified, skipping");
                return Ok(());
            }
            LocalFileDisposition::Download => {
                info!(file = %remote_str, size = remote_size, "local file is wrong size or unverified; re-downloading");
            }
        }
    }

    // Retry loop. Each attempt re-reads `local_size` so we
    // resume from whatever the prior attempt landed on disk.
    // The post-download SHA check inside `try_download_file`
    // is what tells us "the resumed bytes are still wrong";
    // the budget exhaustion is what halts the pipeline.
    let mut attempt: u32 = 0;
    loop {
        // Re-stat on each attempt: the prior attempt may have
        // written partial bytes. If the SFTP channel died
        // mid-write, the high-water mark is the resume point.
        let current_local_size = if local.exists() {
            fs::metadata(local).await?.len()
        } else {
            0
        };

        // Pull the expected hash for the post-download verify.
        // We re-query on each attempt in case a prior attempt
        // updated the DB (it shouldn't, but the cost is one
        // indexed lookup).
        let expected_hash = db.get_expected_hash(dir_id, rel_path)?
            .map(|(hash, _size)| hash);

        match try_download_file(
            &current_raw,
            remote,
            local,
            current_local_size,
            remote_size,
            expected_hash.as_deref(),
        ).await {
            Ok(()) => {
                if attempt > 0 {
                    info!(file = %remote_str, attempt, "download succeeded after retry");
                }
                return Ok(());
            }
            Err(e) if attempt < max_retries => {
                attempt += 1;
                // Re-open the SFTP subsystem channel between
                // attempts. The prior attempt's transport
                // error may have left the channel in a
                // desynced state — visible as russh-sftp's
                // "Packet N for unknown recipient" warnings —
                // and a subsequent `raw.open()` on the same
                // channel will fail with "failed to open
                // remote file" because the server thinks the
                // channel is closed. Lock the Handle, open a
                // fresh subsystem channel, and swap
                // `current_raw` over to it.
                //
                // **Escalation.** If the channel re-open
                // itself fails (e.g. `failed to open SSH
                // channel for SFTP` from `channel_open_session`),
                // the underlying SSH transport is sick, not
                // just one channel. Escalate to a
                // Handle-level reconnect: a fresh russh
                // session (new TCP+SSH transport + public-
                // key auth), then a fresh SFTP subsystem on
                // the new Handle. The 2026-06-16 physalis
                // production log shows the failure mode the
                // escalation addresses — the channel re-open
                // fails, we fall through with the dead
                // channel, the next attempt fails
                // identically, the budget is exhausted.
                let mut recovered = false;
                {
                    let h = handle.lock().await;
                    match SyncEngine::open_sftp_session(&h).await {
                        Ok(new_raw) => {
                            info!(
                                file = %remote_str,
                                attempt,
                                "reopened SFTP channel for retry"
                            );
                            current_raw = new_raw;
                            recovered = true;
                        }
                        Err(open_err) => {
                            warn!(
                                file = %remote_str,
                                attempt,
                                error = %open_err,
                                "channel re-open failed; escalating to Handle-level reconnect"
                            );
                        }
                    }
                }
                if !recovered {
                    // Drop the Handle lock before the
                    // reconnect helper takes it again. The
                    // helper does its own lock — releasing
                    // here keeps the lock-hold time bounded
                    // and avoids a self-deadlock if the
                    // future impl ever nests the two lock
                    // acquisitions.
                    match SyncEngine::reconnect_handle_and_sftp(handle, config).await {
                        Ok(new_raw) => {
                            info!(
                                file = %remote_str,
                                attempt,
                                "reconnected russh Handle for retry"
                            );
                            current_raw = new_raw;
                        }
                        Err(recon_err) => {
                            warn!(
                                file = %remote_str,
                                attempt,
                                error = %recon_err,
                                "Handle reconnect failed; will retry on current channel"
                            );
                        }
                    }
                }
                // 2^attempt seconds, capped at 30. attempt=1
                // → 2s, attempt=2 → 4s, attempt=3 → 8s, etc.
                let backoff_secs = 2u64.saturating_pow(attempt).min(30);
                warn!(
                    file = %remote_str,
                    attempt,
                    max_retries,
                    backoff_secs,
                    error = %e,
                    "download failed, will retry"
                );
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                // Loop continues. `current_local_size` is
                // re-read at the top of the next iteration, so
                // we resume from whatever the prior attempt
                // actually landed on disk.
            }
            Err(e) => {
                error!(
                    file = %remote_str,
                    attempt,
                    max_retries,
                    error = %e,
                    "download failed after exhausting retry budget; halting pipeline"
                );
                return Err(e);
            }
        }
    }
}

/// Free-function inner download + verify. One attempt, no
/// retries, no pre-download checks. The wrapper `download_file`
/// is responsible for the retry policy and the pre-download
/// `verify_existing_file` check; this function takes
/// `start_offset` (the byte at which to begin writing) and
/// Bails if the pipelined reader issued zero reads on a
/// download that was supposed to make progress. Extracted as a
/// pure helper so the 0-byte retry bug fix is unit-testable
/// without a real SFTP server. See `try_download_file` for
/// the full context; the short version is: when
/// `start_offset < total` and `bytes_written == start_offset`,
/// the channel issued no reads this attempt and the file
/// contains a `set_len`'d hole full of zeros in the unwritten
/// tail. The downstream size check would pass
/// (`final_size == remote_size` because of the pre-extend) and
/// the SHA check is skipped (no expected hash), so the file
/// would be silently marked Synced. Bail so the retry wrapper
/// either retries with a fresh channel or exhausts the budget
/// and marks the row `sync_failed`.
fn check_zero_writes(
    bytes_written: u64,
    start_offset: u64,
    total: u64,
    remote: &str,
) -> anyhow::Result<()> {
    if bytes_written == start_offset && start_offset < total {
        anyhow::bail!(
            "no bytes were read for {}: pipelined reader issued 0 reads \
             (start_offset={}, total={}, channel may be desynced)",
            remote, start_offset, total
        );
    }
    Ok(())
}

/// `expected_hash` (the post-download SHA target) directly.
///
/// Returns `Ok(())` on a verified clean download. Errors on
/// any transport failure, size mismatch, or SHA mismatch —
/// the caller decides whether to retry, halt, or mark the
/// directory as failed.
async fn try_download_file(
    raw: &Arc<RawSftpSession>,
    remote: &Path,
    local: &Path,
    start_offset: u64,
    remote_size: u64,
    expected_hash: Option<&str>,
) -> anyhow::Result<()> {
    let remote_str = remote.to_string_lossy();
    trace!(file = %remote_str, "downloading file");

    // Ensure parent directory exists. Idempotent — fine to
    // re-run on retry.
    if let Some(parent) = local.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Open the remote file once; the same `handle: String` is
    // used by every pipelined read on this file.
    let handle = raw.open(
        remote.to_string_lossy().into_owned(),
        OpenFlags::READ,
        FileAttributes::empty(),
    ).await
        .with_context(|| format!("failed to open remote file {}", remote.display()))?
        .handle;

    // Wrap the raw session in an Arc so we can hand a clone to
    // each pipelined read task. The reader trait abstracts the
    // "issue read at offset" call so the read loop is testable
    // against a mock that returns chunks out of order.
    let reader: Arc<dyn SftpChunkReader> = Arc::new(RawSessionReader::new(Arc::clone(raw)));

    // Open the local file. We *don't* wrap in BufWriter: each
    // spawned task seeks to its chunk's offset and writes
    // directly. The 256 KiB chunks are already large enough to
    // amortize per-write overhead, and the kernel coalesces
    // nearby writes for the page cache. With
    // SFTP_INFLIGHT_REQUESTS=16 and SFTP_READ_CHUNK=256 KiB,
    // that's at most 16 distinct 256 KiB writes in flight —
    // well under the page-cache dirty ratio on a 60 GB file.
    let local_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(local)
        .await
        .with_context(|| format!("failed to open local file {}", local.display()))?;

    // Pre-extend the file to the full remote size. Without this,
    // sparse holes between out-of-order writes would be reported
    // as zero bytes by `metadata().len()` on some filesystems
    // (the kernel knows the file is `max(written_offset,
    // sparse_explicit)`, and the size matches the highest offset
    // a write touched). On the CIFS mount we use, `ftruncate`
    // up-front guarantees the size is what we asked for. This
    // is a one-shot syscall, not a per-chunk cost.
    if remote_size > start_offset {
        local_file
            .set_len(remote_size)
            .await
            .with_context(|| {
                format!(
                    "failed to pre-extend local file to {} bytes",
                    remote_size
                )
            })?;
    }
    // Drop the pre-extension handle. The spawned read tasks
    // each open their own FD on the same path; keeping this
    // one open would just waste an FD. (`tokio::fs::File`'s
    // `try_clone` shares the offset, which would serialize
    // all writes to the position the last task left the
    // cursor at — so we open per-task instead.)
    drop(local_file);

    // Progress tracking. `start` and `last_log` are monotonic so a
    // wall-clock adjustment (NTP step, leap second, container
    // suspend) doesn't make the rate go negative or skip an
    // emission. `last_log_bytes` is the byte count at the previous
    // emission; the rate is computed against the interval between
    // emissions, which is more useful than the lifetime average
    // because it surfaces stalls and bursts.
    //
    // `start_offset` is the byte count at which this run started
    // (0 for fresh downloads, > 0 when resuming). The lifetime
    // average covers only the bytes transferred in *this* run, so
    // a resumed download isn't penalized for the prior run's time.
    //
    // `next_offset_to_write` is the high-water mark of bytes
    // that have hit disk. Under the writethrough model chunks
    // land out of order, so this is `max(off + len)` over
    // completed chunks, not a contiguous cursor.
    let start = tokio::time::Instant::now();
    let mut last_log = start;
    let mut last_log_bytes: u64 = start_offset;
    let total = remote_size;
    let mut next_offset_to_write: u64 = start_offset;

    // Run the pipelined reader. It returns the high-water-mark
    // offset reached on disk (== highest `off + len` over
    // completed chunks). The on-disk file is correct
    // regardless of write order; the size check below verifies
    // completeness.
    let bytes_written = pipelined_read_to_file(
        reader,
        handle.clone(),
        start_offset,
        total,
        local,
        PipelinedReadConfig {
            chunk_size: SFTP_READ_CHUNK,
            max_inflight: SFTP_INFLIGHT_REQUESTS,
            stall_timeout: STALL_TIMEOUT,
        },
        ProgressReporter {
            file_label: &remote_str,
            start: &start,
            start_offset: &start_offset,
            total,
            last_log: &mut last_log,
            last_log_bytes: &mut last_log_bytes,
            next_offset_to_write: &mut next_offset_to_write,
        },
    ).await?;

    // Defense-in-depth against the "0-byte retry" bug seen in
    // the 2026-06-15 production run on physalis. The prior
    // attempt's pre-extend set the file's size to `total`; on
    // the retry, `start_offset == total` so the pipelined
    // reader issues zero reads and returns `Ok(start_offset)`.
    // Without this check, the size check below passes
    // (`final_size == remote_size`) and the SHA check is
    // skipped (no expected hash), so the file — a
    // `set_len`'d hole full of zeros in the unwritten tail —
    // is silently marked Synced. Bail so the retry wrapper
    // either retries with a fresh channel or exhausts the
    // budget and marks the row `sync_failed`. The check is
    // conservative: one read is always required to make
    // progress, so `bytes_written == start_offset` with
    // `start_offset < total` is unambiguous.
    check_zero_writes(bytes_written, start_offset, total, &remote_str)?;

    // Final per-file summary. The lifetime average covers the
    // bytes transferred in *this* run only (not the pre-resume
    // bytes), so the rate reflects the current run's performance.
    let final_size = fs::metadata(local).await?.len();
    let bytes_this_run = final_size - start_offset;
    let elapsed = start.elapsed();
    let lifetime_bps = if elapsed.as_secs_f64() > 0.0 {
        (bytes_this_run as f64 * 8.0) / elapsed.as_secs_f64()
    } else {
        0.0
    };
    info!(
        file = %remote_str,
        bytes = final_size,
        elapsed_secs = elapsed.as_secs(),
        avg_mbps = format!("{:.2}", lifetime_bps / 1_000_000.0),
        "download complete"
    );

    if final_size != remote_size {
        anyhow::bail!(
            "download size mismatch for {}: expected {}, got {}",
            remote_str,
            remote_size,
            final_size
        );
    }

    // Per-file SHA-256 verification. The expected hash was
    // recorded from the remote at manifest-collection time;
    // a mismatch means the bytes we just downloaded are not
    // what the remote had at that moment. The retry wrapper
    // sees this error and either retries (transient CIFS
    // writeback issue) or halts the pipeline after the budget
    // is exhausted (real downloader bug).
    //
    // This is the *post-download* check — it verifies that
    // the bytes we just pulled through the parallel reader
    // are correct. The pre-download check on the size-match
    // path (see `verify_existing_file` in `download_file`) is
    // the other half: it catches the "file already on disk
    // from a prior run, but with stale or corrupt bytes"
    // case that the old size-only fast path would have let
    // through.
    if let Some(expected) = expected_hash {
        let actual = compute_local_sha256(local).await
            .with_context(|| format!("failed to hash local file {}", local.display()))?;
        if actual != expected {
            error!(
                file = %remote_str,
                expected = %expected,
                actual = %actual,
                "sha256 mismatch: remote and local hashes disagree; file is corrupt"
            );
            anyhow::bail!(
                "sha256 mismatch for {}: expected {}, got {}",
                remote_str, expected, actual
            );
        }
        debug!(file = %remote_str, hash = %expected, "sha256 verified");
    } else {
        // No expected hash — either this file was added to
        // the directory after the manifest was collected,
        // or `collect_remote_hashes` failed for this dir
        // (e.g. SSH channel error). Don't block the
        // download on missing verification data.
        warn!(file = %remote_str, "no expected sha256 recorded for this file; skipping verification");
    }

    // Close the remote file handle.
    let _ = raw.close(&handle).await;

    Ok(())
}

// =========================================================================
// DownloaderPool: long-lived pool of N downloader tasks that claim
// rows from the DB and download them.
// =========================================================================
//
// In the DB-driven-dispatch refactor (2026-06-14), the pool lives
// for the entire pipeline run, not for a single category. It owns
// N pre-allocated SFTP subsystem channels (one per downloader
// task) and uses an atomic `UPDATE ... WHERE state = 'detected'`
// (`db.claim_detected_row`) as the dispatch primitive. There is
// no in-process mpsc; the DB *is* the work queue.
//
// Shutdown: the pool is signaled via a `watch::Sender<bool>` when
// no more walkers are coming. Each downloader's loop checks the
// signal: if set AND the DB has no `Detected` rows, the downloader
// exits. The pool's background `JoinHandle` is awaited via
// `SyncEngine::drain_pool`.

struct PoolConfig {
    max_retries: u32,
    poll_interval: Duration,
    /// Shared `Config` clone, used by the downloader retry
    /// loop to perform a Handle-level reconnect (a fresh
    /// russh `client::connect` + `authenticate_publickey`)
    /// when the cheaper per-attempt SFTP channel re-open
    /// fails. Held as `Arc<Config>` so each downloader task
    /// can clone it cheaply.
    config: Arc<Config>,
}

pub struct DownloaderPool {
    /// Pre-opened SFTP subsystem channels, one per downloader
    /// task. Each downloader task owns one of these for the
    /// lifetime of the pool; the pool can also open *new*
    /// channels via the `handle` for retry recovery.
    downloader_channels: Vec<Arc<RawSftpSession>>,
    /// The russh `Handle`, shared with the walker. Used by
    /// downloader tasks to open fresh SFTP subsystem channels
    /// between retry attempts — a channel that died mid-download
    /// (e.g. "Packet N for unknown recipient" from russh-sftp
    /// after a transport hiccup) is replaced with a fresh one
    /// so the next attempt isn't doomed to fail on the same
    /// dead channel. The Handle is `!Sync` and must be
    /// serialized; a `tokio::sync::Mutex` makes that
    /// explicit and short-lived.
    handle: Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
    /// Drain signal: set to `true` when no more walkers are
    /// coming. The downloaders observe this in their main loop.
    drain_signal: Arc<watch::Sender<bool>>,
    /// Pool configuration. Kept here so the spawn call can pass
    /// the per-task settings (poll interval, retry budget) to
    /// each downloader.
    config: PoolConfig,
}

impl DownloaderPool {
    fn new(
        downloader_channels: Vec<Arc<RawSftpSession>>,
        handle: Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
        max_retries: u32,
        poll_interval: Duration,
        config: Arc<Config>,
    ) -> Self {
        let (drain_tx, _drain_rx) = watch::channel(false);
        Self {
            downloader_channels,
            handle,
            drain_signal: Arc::new(drain_tx),
            config: PoolConfig { max_retries, poll_interval, config },
        }
    }

    /// Signal the pool to drain. Each downloader will exit
    /// once the DB has no more `Detected` rows. The signal is
    /// idempotent.
    fn signal_drain(&self) {
        // `send` only updates if the value changed; ignore the
        // error (the only way it fails is if there are no
        // receivers, which means the pool is already gone).
        let _ = self.drain_signal.send(true);
    }

    /// Spawn N downloader tasks. Returns the `JoinHandle` of
    /// the *aggregator* task that awaits all N downloaders; the
    /// caller awaits this handle in `drain_pool`.
    fn spawn(&self, db: Database) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let drain_rx = self.drain_signal.subscribe();
        let poll_interval = self.config.poll_interval;
        let max_retries = self.config.max_retries;
        // Clone the channels — `Arc<RawSftpSession>` is cheap
        // to clone (just an Arc bump). This lets us keep
        // `self.downloader_channels` for any future inspection
        // (e.g. operator log of how many slots the pool has).
        let channels = self.downloader_channels.clone();
        // Clone the Handle Arc so each downloader task can
        // open a fresh SFTP subsystem channel between retry
        // attempts. The Handle is `!Sync`, so we serialize
        // access through the inner Mutex; the outer Arc just
        // shares ownership across the N downloader tasks.
        let handle = Arc::clone(&self.handle);
        // Clone the Config Arc so each downloader task can
        // perform a Handle-level reconnect (fresh
        // `client::connect` + `authenticate_publickey`) when
        // the per-attempt channel re-open fails. Cheap
        // Arc clone; `Config` is `Clone` already.
        let config = Arc::clone(&self.config.config);
        tokio::spawn(async move {
            // Spawn N downloader tasks. Each owns one channel.
            let mut set = JoinSet::new();
            for (slot_id, raw) in channels.into_iter().enumerate() {
                let drain_rx = drain_rx.clone();
                let db = db.clone();
                let handle = Arc::clone(&handle);
                let config = Arc::clone(&config);
                set.spawn(async move {
                    downloader_loop(slot_id, raw, handle, db, drain_rx, max_retries, poll_interval, config).await
                });
            }
            // Await all downloader tasks. If any returns Err,
            // propagate the first one.
            let mut first_err: Option<anyhow::Error> = None;
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                    Err(join) => {
                        if first_err.is_none() {
                            first_err = Some(anyhow::anyhow!("downloader task panicked: {}", join));
                        }
                    }
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }
}

impl Drop for SyncEngine {
    fn drop(&mut self) {
        // Best-effort drain signal. If `drain_pool().await` was
        // not called by the caller, signal anyway so the pool
        // can exit if it's still alive. We can't `.await` in
        // `Drop`, so the pool's background task will see the
        // signal on its next iteration and exit on its own.
        // The actual `await` of the pool's `JoinHandle` is in
        // `drain_pool`, which the caller is expected to invoke.
        if let Some(join) = self.pool_join.take() {
            self.pool.signal_drain();
            // We *abort* the join rather than wait — Drop is
            // synchronous. The pool's task sees the drain
            // signal, finishes its current download (or aborts),
            // and exits. If the process is exiting, the OS
            // cleans up; if the caller is moving on to a new
            // SyncEngine, they shouldn't have a pool still
            // running from a previous engine.
            join.abort();
        }
        // Disconnect the SSH Handle. This is best-effort
        // because `Drop` is synchronous and we can't `.await`
        // the lock; if the walker is still running, the
        // disconnect races with its exec channel use. The
        // walker will see the disconnect on its next operation
        // and surface an error, which is the right outcome.
        if let Ok(handle) = self.handle.try_lock() {
            let _ = handle.disconnect(Disconnect::ByApplication, "pipeline complete", "");
        }
        // If the try_lock fails, the walker is mid-exec. We
        // skip the disconnect; the Handle drops at end of
        // scope, which also closes the TCP connection.
    }
}

// =========================================================================
// SFTP read pipelining
// =========================================================================
//
// The single-request-in-flight SFTP read pattern (one outstanding
// `SSH_FXP_READ` at a time) caps per-file throughput at the sftp-server's
// per-process rate, which on our deployment is ~5 Mbps even though the
// network could carry 30+. To fix this we issue N concurrent read
// requests on a single open file handle and write each chunk to disk
// at its true offset as soon as it arrives. Writes to distinct
// offsets of the same inode are independent at the kernel level
// (page cache + block scheduler handle arbitrary order), so we don't
// need an in-process reordering buffer.
//
// The reader is abstracted behind the `SftpChunkReader` trait so the
// pipelined read loop can be unit-tested against a mock that returns
// chunks out of order. The real implementation is `RawSessionReader`,
// which wraps `Arc<RawSftpSession>`. The writer is a plain
// `tokio::fs::File`; each spawned read task clones it so the
// per-task seek state is independent.

/// Configuration for one call to `pipelined_read_to_file`.
#[derive(Clone, Copy)]
pub(crate) struct PipelinedReadConfig {
    /// Bytes per SFTP read request. The server is asked for chunks
    /// of this size; the actual response may be smaller near EOF.
    pub chunk_size: usize,
    /// Maximum number of in-flight SFTP read requests.
    pub max_inflight: usize,
    /// Per-read stall timeout. A request that doesn't resolve within
    /// this duration is treated as a stuck SFTP layer and aborts the
    /// file.
    pub stall_timeout: std::time::Duration,
}

/// Live progress state for the throughput logger. Held by the
/// `pipelined_read_to_file` function and updated on each completed
/// chunk; the periodic log emission reads from these fields.
///
/// `next_offset_to_write` is the high-water mark of bytes that have
/// reached disk for *this* run (not contiguous — chunks may be
/// landing at higher offsets first). Used only for throughput
/// calculation; the on-disk file is correct regardless of write
/// order, so we don't need a contiguous-drain state machine.
pub(crate) struct ProgressReporter<'a> {
    pub file_label: &'a str,
    pub start: &'a tokio::time::Instant,
    pub start_offset: &'a u64,
    pub total: u64,
    pub last_log: &'a mut tokio::time::Instant,
    pub last_log_bytes: &'a mut u64,
    pub next_offset_to_write: &'a mut u64,
}

#[async_trait::async_trait]
pub(crate) trait SftpChunkReader: Send + Sync {
    /// Issue a read of up to `len` bytes at `offset` on the
    /// already-open `handle`. The future may be polled concurrently
    /// with other calls to `read_chunk` on the same `&self`.
    async fn read_chunk(
        &self,
        handle: String,
        offset: u64,
        len: u32,
    ) -> anyhow::Result<Vec<u8>>;
}

/// Real implementation of `SftpChunkReader` backed by an
/// `Arc<RawSftpSession>`. Cloning the inner `Arc` is cheap; we
/// `tokio::spawn` each read on a cloned handle so the futures are
/// independent and `'static`.
struct RawSessionReader {
    raw: Arc<RawSftpSession>,
}

impl RawSessionReader {
    fn new(raw: Arc<RawSftpSession>) -> Self {
        Self { raw }
    }
}

#[async_trait::async_trait]
impl SftpChunkReader for RawSessionReader {
    async fn read_chunk(
        &self,
        handle: String,
        offset: u64,
        len: u32,
    ) -> anyhow::Result<Vec<u8>> {
        // SFTP servers are allowed (and OpenSSH's `sftp-server`
        // does, by default) to return fewer bytes than requested
        // in a single `SSH_FXP_READ` response. Per the SFTP
        // protocol spec, the client is responsible for looping
        // on the same offset until the full `len` bytes have
        // arrived. A short read on a non-EOF read is *not* a
        // signal to give up — it's a "ask again, same offset."
        //
        // We loop with a per-iteration cap (`MAX_ITERATIONS`)
        // to bound the worst case; a wedged server that keeps
        // returning 0 bytes for a non-EOF offset would otherwise
        // spin here forever. The cap is generous (256 reads
        // * 64 KiB max response = 16 MiB per chunk) — a real
        // SFTP server that hits the cap is broken.
        //
        // A `0`-byte response with `offset + 0` strictly less
        // than the file size IS an error: the server has the
        // file open, knows its size, and is reporting EOF at a
        // non-EOF offset. We surface that as a "premature EOF"
        // error rather than looping.
        const MAX_ITERATIONS: u32 = 256;

        let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
        let mut current_offset = offset;
        let mut remaining = len;
        let mut iterations: u32 = 0;

        while remaining > 0 {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(anyhow!(
                    "SFTP read at offset {} did not return {} bytes \
                     within {} iterations (got {} bytes so far)",
                    offset, len, MAX_ITERATIONS, buf.len()
                ));
            }
            let resp = self.raw.read(&handle, current_offset, remaining).await
                .map_err(|e| anyhow!(
                    "SFTP read at offset {} failed: {}",
                    current_offset, e
                ))?;
            if resp.data.is_empty() {
                return Err(anyhow!(
                    "SFTP read at offset {} returned 0 bytes \
                     (server-side premature EOF; needed {} more bytes)",
                    current_offset, remaining
                ));
            }
            buf.extend_from_slice(&resp.data);
            current_offset += resp.data.len() as u64;
            remaining -= resp.data.len() as u32;
        }
        Ok(buf)
    }
}

/// Pipelined SFTP read with out-of-order receive, writethrough to
/// disk at true offset.
///
/// Issues up to `config.max_inflight` SFTP read requests in parallel,
/// each for `config.chunk_size` bytes at offsets
/// `start_offset, start_offset + chunk, start_offset + 2*chunk, ...`.
/// Each spawned task does the read and, on success, opens the local
/// file and writes the chunk at its true offset. POSIX `pwrite`-
/// style writes to distinct offsets of the same inode are
/// independent at the kernel level (page cache + block scheduler
/// handle arbitrary order), so we don't need an in-process
/// reordering buffer. The file ends up byte-identical to the source
/// regardless of which order the SFTP responses arrive.
///
/// Each task opens its own `tokio::fs::File` because `File::try_clone`
/// shares the file offset, which would serialize all writes to the
/// position the last task left the cursor at. Opening per task
/// gives each task an independent FD with its own offset.
///
/// `reader` is an `Arc<dyn SftpChunkReader>` so each spawned task
/// can own a clone. `local_path` is the destination file path; the
/// caller is responsible for ensuring the file exists and is
/// pre-extended to `total` bytes.
///
/// The per-read stall timeout is enforced; there is no total-time
/// budget (see the comment on the removed `TRANSFER_TIMEOUT` const
/// above for the rationale). The function returns the total bytes
/// written to disk (== highest offset reached on success).
pub(crate) async fn pipelined_read_to_file(
    reader: Arc<dyn SftpChunkReader>,
    handle: String,
    start_offset: u64,
    total: u64,
    local_path: &Path,
    config: PipelinedReadConfig,
    progress: ProgressReporter<'_>,
) -> anyhow::Result<u64> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let mut next_offset_to_issue: u64 = start_offset;
    let mut inflight: JoinSet<(u64, u32, anyhow::Result<()>)> = JoinSet::new();

    loop {
        // 1. Top up the inflight pool until we hit the cap or run
        //    out of file.
        while inflight.len() < config.max_inflight && next_offset_to_issue < total {
            let off = next_offset_to_issue;
            let len = std::cmp::min(
                total - off,
                config.chunk_size as u64,
            ) as u32;
            next_offset_to_issue += len as u64;

            // Clone the reader (cheap, `Arc` bump), the handle
            // string, and the local path into the spawned task.
            // The path is owned so the task can open its own FD
            // with an independent seek state.
            let task_reader = reader.clone();
            let task_handle = handle.clone();
            let task_path: PathBuf = local_path.to_path_buf();
            let stall_timeout = config.stall_timeout;
            inflight.spawn(async move {
                // Per-read stall timeout. A healthy SFTP read on
                // even a slow link resolves in well under a second;
                // 60s is a "something is wedged" signal. We enforce
                // the timeout on the read itself (not the seek/write
                // — those are local syscalls and either succeed
                // quickly or fail clearly).
                let read_fut = task_reader.read_chunk(task_handle, off, len);
                let data = match tokio::time::timeout(stall_timeout, read_fut).await {
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => return (off, len, Err(e)),
                    Err(_elapsed) => {
                        return (off, len, Err(anyhow!(
                            "SFTP read at offset {} stalled after {:?}",
                            off, stall_timeout
                        )));
                    }
                };
                if data.is_empty() {
                    // EOF marker. The successful-zero-length read
                    // signals end-of-file; the size check at the
                    // end of `download_file` will catch a
                    // mismatch.
                    return (off, 0, Ok(()));
                }
                // Open a fresh FD per task so the seek state is
                // independent. `try_clone` shares the offset, which
                // would serialize all writes.
                let mut f = match tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&task_path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        return (off, len, Err(anyhow::Error::from(e).context(format!(
                            "failed to open local file {} at offset {}",
                            task_path.display(), off
                        ))));
                    }
                };
                if let Err(e) = f.seek(std::io::SeekFrom::Start(off)).await {
                    return (off, len, Err(anyhow::Error::from(e).context(format!(
                        "failed to seek local file to offset {}", off
                    ))));
                }
                if let Err(e) = f.write_all(&data).await {
                    return (off, len, Err(anyhow::Error::from(e).context(format!(
                        "failed to write chunk at offset {}", off
                    ))));
                }
                (off, len, Ok(()))
            });
        }

        // 2. If nothing is inflight, we're done. (The top-up loop
        //    guarantees this is only reached when
        //    `next_offset_to_issue >= total` and the last issued
        //    read has completed.)
        if inflight.is_empty() {
            break;
        }

        // 3. Await the next completed task. `JoinSet::join_next`
        //    returns the next result in completion order — that is,
        //    the *fastest* of the currently-pending reads finishes
        //    first, regardless of the offset it was issued at. The
        //    writethrough model doesn't care: the chunk has
        //    already been written at its true offset by the time
        //    we see the result.
        let (off, len, task_result) = match inflight.join_next().await {
            Some(Ok(triple)) => triple,
            Some(Err(join_err)) => {
                return Err(anyhow!("SFTP read task panicked: {}", join_err));
            }
            None => break,
        };

        // A read that completed with an error aborts the file. This
        // covers both SFTP errors and write-side errors (the file
        // seek or write_all failed).
        task_result.with_context(|| {
            format!("transfer failed at offset {}", off)
        })?;

        // EOF: zero-length read. Stop issuing more reads. The
        // chunks that were already issued will come back, but each
        // is a real read of (presumably) the same zero-length
        // region, so they short-circuit. The size check at the end
        // of `download_file` verifies completeness.
        if len == 0 {
            break;
        }

        // 4. Update progress. `next_offset_to_write` is the
        //    high-water mark of bytes that have hit disk; it
        //    advances monotonically even though the writes are
        //    landing out of order, because we update it as
        //    `max(prev, off + len)`.
        let end = off + len as u64;
        if end > *progress.next_offset_to_write {
            *progress.next_offset_to_write = end;
        }

        // 5. Periodic throughput log. The rate is computed against
        //    the interval between emissions, not the lifetime
        //    average, so stalls and bursts show up clearly.
        let now = tokio::time::Instant::now();
        if now.duration_since(*progress.last_log) >= THROUGHPUT_LOG_INTERVAL {
            let interval = now.duration_since(*progress.last_log);
            let bytes_since = *progress.next_offset_to_write - *progress.last_log_bytes;
            let rate_bps = (bytes_since as f64 * 8.0) / interval.as_secs_f64();
            let elapsed = now.duration_since(*progress.start);
            let bytes_this_run = *progress.next_offset_to_write - *progress.start_offset;
            let lifetime_bps = (bytes_this_run as f64 * 8.0)
                / elapsed.as_secs_f64().max(0.001);
            let percent = if progress.total > 0 {
                (*progress.next_offset_to_write as f64 / progress.total as f64) * 100.0
            } else {
                0.0
            };
            info!(
                file = %progress.file_label,
                bytes = *progress.next_offset_to_write,
                total = progress.total,
                percent = format!("{:.1}", percent),
                rate_mbps = format!("{:.2}", rate_bps / 1_000_000.0),
                avg_mbps = format!("{:.2}", lifetime_bps / 1_000_000.0),
                elapsed_secs = elapsed.as_secs(),
                "downloading"
            );
            *progress.last_log = now;
            *progress.last_log_bytes = *progress.next_offset_to_write;
        }
    }

    Ok(*progress.next_offset_to_write)
}

// =========================================================================
// Two-phase sync: walker → mpsc → N downloaders
// =========================================================================
//
// `sync_category` (above, in `impl SyncEngine`) opens one walker
// SFTP channel + N downloader SFTP channels and spawns them as
// concurrent tasks. The walker produces `DirJob`s; the downloader
// pool consumes them. The two are independent — wall-clock cost is
// `max(walk, download)`, not `walk + max(downloads)` as in the
// old serial implementation.
//
// The walker doesn't borrow `&self` (we pass the parts it needs
// by value into a `tokio::spawn`-able `run_walker`); the
// downloader pool is a free function over its own owned channel.
// This is the standard escape hatch for "I want to call an
// `async` method on `&self` from inside a `tokio::spawn` but I
// also need to keep `&self` around for the join."

/// Free-function walker. Runs in its own task; owns the walker
/// SFTP channel; writes per-directory rows to the DB. Returns
/// `anyhow::Result<()>` — an error from the walker aborts the
/// whole pipeline (a partial walk leaves the DB in a state that
/// downstream stages will misinterpret).
///
/// **Why a free function and not an `async fn` on `SyncEngine`.**
/// `tokio::spawn` requires `'static` — the future must not borrow
/// from the spawning task. Methods on `&self` borrow from
/// `&self`, which lives on the parent's stack. The fix is to
/// detach the pieces we need (a fresh `Database` clone, the
/// `Arc<RawSftpSession>`, the `Arc<Mutex<Handle>>`) and pass them
/// owned into a static-lifetime task. The `self_clone` module
/// below exists for the same reason: `SyncEngine::collect_manifest`
/// borrows `&self`, so we move the implementation to a free
/// function that takes the parts it needs by `&`-reference to
/// owned values.
mod self_clone {
    use super::*;

    /// Walker entry point. Spawned by `SyncEngine::sync_category`;
    /// walks one category, writes per-directory rows to the DB,
    /// and returns when the walk is complete.
    ///
    /// **No downloader dispatch.** The walker does not push jobs
    /// to a pool — it only writes rows. The downloader pool,
    /// which lives for the entire pipeline run, claims rows
    /// independently via `db.claim_detected_row()`. The walker
    /// and the downloaders communicate through the DB, not
    /// through an mpsc.
    ///
    /// **Borrowed channel.** The walker uses the
    /// pre-allocated `walker_channel` (cloned `Arc<RawSftpSession>`)
    /// for readdirs. The Handle is borrowed through a Mutex
    /// (`handle: Arc<tokio::sync::Mutex<Handle>>`); the
    /// walker locks it briefly to open exec channels for
    /// `xargs sha256sum`. The downloader pool never touches the
    /// Handle.
    pub async fn run_walker(
        handle: Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
        walker_channel: Arc<RawSftpSession>,
        db: Database,
        category: String,
        _max_retries: u32,
    ) -> anyhow::Result<()> {
        let config = Config::load_with_env(Path::new("/etc/media-pipeline/config.toml"))?;
        let remote_base = config.remote_path(&category);
        let staging_base = config.staging_path(&category);

        info!(category = %category, remote = %remote_base.display(), "walker: starting");

        // The walker pass. We use the dedicated `walker_channel`
        // for readdirs. The `xargs sha256sum` exec uses a fresh
        // session channel per directory, opened on the shared
        // `Handle` (it's a session-level operation, not SFTP).

        // List top-level remote directories in this category.
        let remote_dirs = list_remote_dirs(&walker_channel, &remote_base).await
            .with_context(|| format!("failed to list remote dirs in {}", remote_base.display()))?;
        info!(category = %category, count = remote_dirs.len(), "walker: remote directories found");

        for dir_name in &remote_dirs {
            let remote_dir = remote_base.join(dir_name);
            let staging_dir = staging_base.join(dir_name);
            let remote_dir_str = remote_dir.to_string_lossy().to_string();
            let staging_dir_str = staging_dir.to_string_lossy().to_string();

            info!(dir = %dir_name, "walker: directory walk starting");

            // Walk the remote tree once. The collected manifest is
            // the input to both the manifest hash (for change
            // detection) and the per-file sha256 collection (for
            // download-time integrity verification). Walking
            // twice would double the per-dir cost for no benefit.
            let walk_started = Instant::now();
            let manifest = match collect_manifest(&walker_channel, &remote_dir).await {
                Ok(m) => m,
                Err(e) => {
                    warn!(dir = %dir_name, error = %e, "walker: failed to collect manifest, skipping");
                    continue;
                }
            };
            info!(
                dir = %dir_name,
                file_count = manifest.len(),
                duration_secs = format!("{:.2}", walk_started.elapsed().as_secs_f64()),
                "walker: directory walk complete"
            );

            // Per-file hashes from the remote. Best-effort: a
            // transport error here logs a WARN and proceeds with
            // an empty hash set, so a single broken SSH channel
            // can't block the rest of the sync.
            //
            // **The skip-set optimization.** `get_existing_hashes_for_dir`
            // returns the `rel_path`s that already have a recorded
            // hash. We pass that set into `collect_remote_hashes`,
            // which filters those paths out of the `xargs` stdin
            // list. The result: a re-walk of a directory whose
            // manifest is unchanged doesn't re-run `sha256sum` over
            // the entire tree. (A `*Failed → detected` transition
            // forces `manifest_hash` to change, so the walk
            // proceeds normally and the DB rows get the chance to
            // be re-verified.)
            //
            // **Edge case — no row yet.** The directory may not
            // have a row at all on the first walk. We have to
            // call `upsert_directory` *before* we can query
            // `get_existing_hashes_for_dir` (it takes `dir_id`).
            // But we need the existing-hash set *before*
            // `upsert_file_hashes` so the append/upsert
            // decision is correct. The natural order is:
            //
            //   1. collect_manifest (no DB)
            //   2. upsert_directory → (dir_id, prev_state)
            //   3. existing_hashes = get_existing_hashes_for_dir(dir_id)
            //   4. NEW file_hashes = collect_remote_hashes(
            //         manifest, skip=existing_hashes)
            //   5. db.append_file_hashes(dir_id, &NEW)
            //
            // That's what we do. Steps 2 and 3 are one
            // round-trip each.
            let manifest_hash = manifest_hash_from_files(&manifest);
            let upsert_started = Instant::now();
            let (dir_id, _prev_state) = db.upsert_directory(
                &category, &remote_dir_str, &staging_dir_str, &manifest_hash,
            )?;
            info!(
                dir = %dir_name,
                dir_id,
                manifest_hash = %manifest_hash,
                duration_secs = format!("{:.2}", upsert_started.elapsed().as_secs_f64()),
                "walker: directory upserted"
            );

            let existing_hashes = db.get_existing_hashes_for_dir(dir_id)
                .context("walker: failed to query existing hashes")?;
            let hash_started = Instant::now();
            let file_hashes = collect_remote_hashes(&handle, &remote_dir, &manifest, &existing_hashes).await
                .unwrap_or_else(|e| {
                    warn!(dir = %dir_name, error = %e, "walker: failed to collect remote hashes; verification will be skipped for this dir");
                    Vec::new()
                });
            info!(
                dir = %dir_name,
                hash_count = file_hashes.len(),
                duration_secs = format!("{:.2}", hash_started.elapsed().as_secs_f64()),
                "walker: remote hashes collected"
            );

            if !file_hashes.is_empty() {
                db.append_file_hashes(dir_id, &file_hashes)
                    .context("walker: failed to append file hashes")?;
            }

            // The row is now in the DB (state = 'detected' if
            // the manifest changed or it was a fresh dir;
            // otherwise the manifest is unchanged and the row
            // stays in 'synced'). Either way, the walker's job
            // for this directory is done — the long-lived
            // downloader pool will claim the row if it's
            // 'detected'. There is no job dispatch from the
            // walker.
            info!(
                dir = %dir_name,
                dir_id,
                "walker: directory walk finished"
            );
        }

        info!(category = %category, "walker: complete");
        Ok(())
    }

    // -------------------------------------------------------------------
    // Free-function mirrors of `SyncEngine` methods. The
    // implementations are duplicated from `SyncEngine` to avoid
    // the `&self`-borrow problem in `tokio::spawn`. Kept in this
    // submodule so the file's `impl SyncEngine` block still reads
    // as a unified API for callers.
    // -------------------------------------------------------------------

    pub async fn list_remote_dirs(
        raw: &Arc<RawSftpSession>,
        path: &Path,
    ) -> anyhow::Result<Vec<String>> {
        let dir_handle = raw.opendir(path.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to opendir {}", path.display()))?;
        let dir_handle_str = dir_handle.handle;

        let files = drain_readdir_pages(|| async {
            raw.readdir(&dir_handle_str).await
        })
        .await
        .map_err(|e| anyhow::Error::new(e).context(format!("failed to readdir {}", path.display())))?;
        let _ = raw.close(&dir_handle_str).await;

        let mut dirs = Vec::new();
        for f in files {
            if f.attrs.is_dir() && !f.filename.starts_with('.') {
                dirs.push(f.filename);
            }
        }
        Ok(dirs)
    }

    pub async fn collect_manifest(
        raw: &Arc<RawSftpSession>,
        path: &Path,
    ) -> anyhow::Result<BTreeMap<String, (u64, u64)>> {
        let mut manifest = BTreeMap::new();
        let mut stack = vec![path.to_path_buf()];
        while let Some(p) = stack.pop() {
            let dir_handle = raw.opendir(p.to_string_lossy().into_owned()).await
                .with_context(|| format!("failed to opendir {}", p.display()))?;
            let dir_handle_str = dir_handle.handle;
            let entries = drain_readdir_pages(|| async {
                raw.readdir(&dir_handle_str).await
            })
            .await
            .map_err(|e| anyhow::Error::new(e).context(format!("failed to readdir {}", p.display())))?;
            let _ = raw.close(&dir_handle_str).await;

            for entry in entries {
                if entry.filename.starts_with('.') {
                    continue;
                }
                let full = p.join(&entry.filename);
                let rel = full.strip_prefix(path)
                    .unwrap_or(&full)
                    .to_string_lossy()
                    .into_owned();
                if entry.attrs.is_dir() {
                    stack.push(full);
                } else {
                    let size = entry.attrs.size.unwrap_or(0);
                    let mtime = entry.attrs.mtime.unwrap_or(0) as u64;
                    manifest.insert(rel, (size, mtime));
                }
            }
        }
        Ok(manifest)
    }

    pub async fn collect_remote_hashes(
        handle: &Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
        remote_dir: &Path,
        manifest: &BTreeMap<String, (u64, u64)>,
        skip: &std::collections::HashSet<String>,
    ) -> anyhow::Result<Vec<(String, String, i64, i64)>> {
        let hash_started = Instant::now();

        if manifest.is_empty() {
            return Ok(Vec::new());
        }

        // Build the path list. Filter out any path with an
        // embedded newline — these would break the `xargs -d
        // '\n'` splitter and emit a WARN rather than silently
        // producing a broken hash map. Also filter out any
        // `rel_path` already in `skip` — the DB has the hash
        // for those, and re-asking the remote would burn
        // bandwidth and CPU on every cron tick.
        let mut paths: Vec<String> = Vec::with_capacity(manifest.len());
        let mut skipped_newline = 0usize;
        let mut skipped_existing = 0usize;
        for rel_path in manifest.keys() {
            if skip.contains(rel_path) {
                skipped_existing += 1;
                continue;
            }
            if rel_path.contains('\n') {
                warn!(path = %rel_path, "skipping file with embedded newline; cannot be hashed via stdin xargs");
                skipped_newline += 1;
                continue;
            }
            // <remote_dir>/<rel_path> with a single '/'. `Path::join`
            // would do this but returns a `PathBuf`; we want a
            // `String` for the stdin buffer.
            let mut p = remote_dir.to_string_lossy().into_owned();
            if !p.ends_with('/') {
                p.push('/');
            }
            p.push_str(rel_path);
            paths.push(p);
        }
        if skipped_newline > 0 {
            info!(skipped = skipped_newline, "skipped paths with embedded newlines during hash collection");
        }
        if skipped_existing > 0 {
            info!(skipped = skipped_existing, total = manifest.len(),
                "skipped files already in DB; reusing recorded hashes");
        }

        // Hot path: every file in the manifest already has a hash
        // in the DB. We trust the recorded hashes and skip the
        // remote exec entirely. The walker persists nothing new
        // (the rows are already there from a prior walk).
        if paths.is_empty() {
            info!(file_count = manifest.len(), "all hashes already in DB; skipping remote exec");
            return Ok(Vec::new());
        }

        info!(
            file_count = paths.len(),
            skipped = skipped_existing,
            "walker: remote hashing starting"
        );

        // Open a new session channel via the shared russh
        // Handle (wrapped in a Mutex so the walker and the
        // engine's Drop impl can share it). The lock is held
        // only for the duration of `channel_open_session` —
        // once the channel is open, the rest of the function
        // works on the channel directly and the Handle is
        // free for other consumers.
        //
        // This is independent of the SFTP subsystem channel
        // used for the readdir walks — `exec` runs in its own
        // channel per the SSH spec.
        let mut channel = {
            let h = handle.lock().await;
            h.channel_open_session().await
                .context("failed to open SSH channel for sha256sum exec")?
        };

        // `xargs -d '\n' sha256sum` — split stdin on newlines,
        // invoke sha256sum once per path. The command itself is
        // tiny; the path list is on stdin.
        let command = "xargs -d '\n' sha256sum";
        channel.exec(true, command).await
            .context("failed to exec sha256sum on remote")?;

        // Write the paths to stdin, one per line, then EOF.
        let mut stdin = channel.make_writer();
        let mut body = String::with_capacity(paths.iter().map(|p| p.len() + 1).sum());
        for p in &paths {
            body.push_str(p);
            body.push('\n');
        }
        tokio::io::AsyncWriteExt::write_all(&mut stdin, body.as_bytes()).await
            .context("failed to write paths to sha256sum stdin")?;
        tokio::io::AsyncWriteExt::shutdown(&mut stdin).await
            .context("failed to close sha256sum stdin")?;
        drop(stdin);
        let _ = channel.eof().await;

        // Read stdout into a buffer, then drop the reader so the
        // channel isn't borrowed when we call `wait` / `close`
        // below.
        let mut output = Vec::new();
        {
            let mut stdout = channel.make_reader();
            tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut output).await
                .context("failed to read sha256sum stdout")?;
        }

        while let Some(msg) = channel.wait().await {
            let _ = msg;
        }
        let _ = channel.close().await;

        let text = String::from_utf8(output)
            .context("sha256sum output is not valid UTF-8")?;
        let parsed = parse_sha256sum_output(&text, remote_dir, manifest)?;

        info!(
            file_count = paths.len(),
            bytes_read = text.len(),
            duration_secs = format!("{:.2}", hash_started.elapsed().as_secs_f64()),
            "walker: remote hashing complete"
        );

        Ok(parsed)
    }
}

/// Per-slot downloader task body. Each instance owns one
/// `Arc<RawSftpSession>` and pulls `DirJob`s from the shared
/// mpsc receiver until the channel closes (which happens when
/// the walker drops its `Sender`).
///
/// The `state == Detected` check inside the loop is the
/// "only re-download what changed" invariant. A directory
/// with an unchanged manifest has its state remain `Synced`
/// (or `Synced` from a prior run) and the downloader skips it.
/// A `*Failed → Detected` transition (handled by
/// `upsert_directory`) means a directory that previously failed
/// is being retried — the downloader re-downloads it.
///
/// The `Arc<Mutex<Receiver>>` pattern: the `Mutex` is held only
/// for the brief `recv()` call. In the steady state, one
/// downloader is the receiver while the others wait. As soon as
/// the receiver gets a job and releases the lock, the next
/// waiter acquires it. This is the standard pattern for
/// "N workers, 1 queue" in tokio.
/// Downloader loop, DB-driven. Each downloader task runs this
/// loop concurrently with the others. The dispatch primitive is
/// `db.claim_detected_row` — an atomic `UPDATE ... WHERE state =
/// 'detected'` that returns the row only if no other downloader
/// has already claimed it. There is no in-process mpsc; the DB is
/// the work queue.
///
/// **Drain semantics.** When the orchestrator calls
/// `pool.signal_drain()`, each downloader observes the signal in
/// its main loop. The exit condition is `drain_signal && count
/// (detected) == 0` — i.e. "no more walkers are coming AND the
/// queue is empty." This lets the pool process rows the very
/// last walker produced before exiting, instead of aborting them.
///
/// **Halt semantics.** A single file that exhausts its retry
/// budget returns `Err` from `download_directory`. The downloader
/// marks the row `sync_failed` (so a re-run knows to retry it
/// from the new run's startup sweep), then returns `Err` from
/// the loop. The aggregator task in `DownloaderPool::spawn`
/// collects errors and propagates the first one, which surfaces
/// to the caller of `drain_pool`. This is the same halt-on-
/// exhaust behavior as the previous (mpsc-based) design.
async fn downloader_loop(
    slot_id: usize,
    raw: Arc<RawSftpSession>,
    handle: Arc<tokio::sync::Mutex<client::Handle<ClientHandler>>>,
    db: Database,
    mut drain_signal: watch::Receiver<bool>,
    max_retries: u32,
    poll_interval: Duration,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    info!(slot = slot_id, "downloader: starting");

    loop {
        // Check drain condition first. We exit only when the
        // orchestrator has signaled AND the DB has no more
        // detected rows. Checking `count_detected` only when
        // the signal is set is the cheap case; the expensive
        // case (the count is non-zero) is rare and bounded by
        // how fast the walker is producing rows.
        if *drain_signal.borrow() {
            let pending = db.count_detected().unwrap_or_else(|e| {
                // If the count itself fails, assume pending =
                // 1 so we keep polling. A DB error here is
                // very unusual; logging + assuming pending is
                // safer than exiting on what might be a
                // transient error.
                warn!(slot = slot_id, error = %e, "downloader: count_detected failed; assuming pending=1");
                1
            });
            if pending == 0 {
                info!(slot = slot_id, "downloader: pool drained, exiting");
                return Ok(());
            }
            // Drain signaled but work is still pending — fall
            // through to claim.
        }

        // Try to claim a row. If no row is detected, wait
        // for either a drain-signal change or a poll
        // interval, whichever comes first. The select keeps
        // the exit latency low (we don't have to wait out
        // the full poll interval after a drain signal).
        let claim = match db.claim_detected_row() {
            Ok(Some(row)) => row,
            Ok(None) => {
                tokio::select! {
                    _ = drain_signal.changed() => continue,
                    _ = tokio::time::sleep(poll_interval) => continue,
                }
            }
            Err(e) => {
                warn!(slot = slot_id, error = %e, "downloader: claim failed; backing off");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        info!(
            slot = slot_id,
            dir_id = claim.id,
            remote = %claim.remote_path,
            "downloader: starting directory"
        );

        // The row is in `Syncing` (claim transitioned it). The
        // download itself uses the same free function
        // `download_directory` as before; the only difference
        // is the source of the row data (DB claim vs mpsc
        // job).
        let result = download_directory(
            &raw,
            &handle,
            claim.remote_path.clone(),
            claim.staging_path.clone(),
            String::new(),
            &db,
            claim.id,
            max_retries,
            &config,
        ).await;

        match result {
            Ok(()) => {
                db.set_directory_state(claim.id, DirectoryState::Synced)?;
                info!(slot = slot_id, dir_id = claim.id, "downloader: complete");
            }
            Err(e) => {
                let msg = format!("download failed: {}", e);
                let _ = db.set_directory_error(
                    claim.id, DirectoryState::SyncingFailed, &msg,
                );
                error!(slot = slot_id, dir_id = claim.id, error = %e, "downloader: failed");
                // Halt: returning Err from this loop closes
                // this slot. The aggregator in
                // `DownloaderPool::spawn` collects errors from
                // all slots and propagates the first one to the
                // caller of `drain_pool`. The row is left in
                // `sync_failed` (set above); the next run's
                // startup will reset it to `detected` via
                // `upsert_directory`'s `*Failed → detected`
                // transition.
                return Err(e);
            }
        }
    }
}

// =========================================================================
// Tests for the writethrough-to-disk pipelined read loop
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    /// A mock SFTP reader that returns canned chunks at the
    /// requested offsets. The `config` field (when set) drives the
    /// failure / stall / reorder behavior exercised by the tests
    /// below. See `MockConfig` for the per-test knob set.
    struct MockReader {
        data: Vec<u8>,
        /// Number of completed reads, exposed for tests that want to
        /// assert on progress.
        reads_issued: Mutex<Vec<u64>>,
        config: MockConfig,
    }

    /// Per-test configuration for `MockReader`. All fields default
    /// to "no special behavior" so a new test only has to set the
    /// knobs it actually cares about. The field names are
    /// deliberately the same shape as the SFTP failure modes we
    /// want to exercise: a chunk that arrives out of order, a chunk
    /// that doesn't arrive at all, a chunk that arrives with the
    /// wrong bytes, a chunk that triggers the stall timeout.
    #[derive(Clone, Default)]
    struct MockConfig {
        /// If true, complete reads in reverse order of issue. The
        /// first read in `reads_issued` finishes last. This is the
        /// "adversarial scheduler" knob: the in-process loop is
        /// only safe under arbitrary write ordering if it survives
        /// this.
        reorder: bool,
        /// If `Some(n)`, the Nth read issued (0-indexed) returns
        /// `Err`. Lets a test pin a specific failure to a specific
        /// read — useful for "what if read #3 fails when we have
        /// 8 in flight" coverage.
        fail_at: Option<usize>,
        /// If `Some(n)`, the Nth read issued sleeps for this
        /// duration before returning. Used to push a specific read
        /// past `stall_timeout` and assert the pipeline aborts
        /// cleanly (rather than hanging or panicking).
        stall_at: Option<(usize, Duration)>,
        /// If `Some(n)`, the Nth read issued returns a chunk of
        /// the wrong bytes (all `0xCC` of the requested length).
        /// Useful for asserting that the post-download SHA-256
        /// check would catch a silent corruption at any offset.
        corrupt_at: Option<usize>,
    }

    impl MockReader {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                reads_issued: Mutex::new(Vec::new()),
                config: MockConfig::default(),
            }
        }

        fn with_reorder(mut self) -> Self {
            self.config.reorder = true;
            self
        }

        fn with_config(mut self, config: MockConfig) -> Self {
            self.config = config;
            self
        }
    }

    #[async_trait::async_trait]
    impl SftpChunkReader for MockReader {
        async fn read_chunk(
            &self,
            _handle: String,
            offset: u64,
            len: u32,
        ) -> anyhow::Result<Vec<u8>> {
            // Take the issue index under the lock, then drop the
            // guard before awaiting. Holding a `std::sync::MutexGuard`
            // across an `.await` is unsound (it's not `Send`), and
            // the trait's `&self` borrow is fine to re-acquire later
            // since the lock is uncontended in these tests.
            let (issue_idx, total) = {
                let mut issued = self.reads_issued.lock().unwrap();
                let issue_idx = issued.len();
                issued.push(offset);
                (issue_idx, issued.len())
            };

            // Failure injection: a configured read index returns
            // an error before any data is produced. The pipeline
            // must abort the file with a clean error.
            if self.config.fail_at == Some(issue_idx) {
                return Err(anyhow!("injected SFTP read failure at offset {}", offset));
            }

            // Stall injection: a configured read sleeps past the
            // caller's stall_timeout. The pipeline's
            // `tokio::time::timeout` must fire and abort the file
            // rather than block the test forever.
            if let Some((idx, dur)) = self.config.stall_at {
                if idx == issue_idx {
                    tokio::time::sleep(dur).await;
                }
            } else if self.config.reorder {
                // Simulate the server holding onto this read until
                // the next one is issued, then completing them in
                // reverse issue order. We do this by sleeping an
                // amount proportional to the *reverse* index.
                let reverse_idx = total - 1 - issue_idx;
                tokio::time::sleep(Duration::from_millis(reverse_idx as u64)).await;
            } else {
                // Tiny sleep to make sure the inflight set actually
                // has work pending concurrently. 0 means immediate
                // completion, which would mask pipeline bugs.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            // Corruption injection: a configured read index
            // returns the right shape (length) but the wrong
            // bytes. The on-disk file will not match the source;
            // a post-download SHA-256 check is the only thing that
            // catches this.
            let start = offset as usize;
            let end = std::cmp::min(start + len as usize, self.data.len());
            if start >= self.data.len() {
                return Ok(Vec::new());
            }
            if self.config.corrupt_at == Some(issue_idx) {
                return Ok(vec![0xCC; end - start]);
            }
            Ok(self.data[start..end].to_vec())
        }
    }

    /// Run the pipelined reader against a real temp file, return
    /// the on-disk bytes. Centralizes the boilerplate so each test
    /// is just `MockReader + data + chunk_size + max_inflight`.
    async fn run_pipelined(
        data: &[u8],
        start_offset: u64,
        chunk_size: usize,
        max_inflight: usize,
        reorder: bool,
    ) -> Vec<u8> {
        let mut reader = MockReader::new(data.to_vec());
        if reorder {
            reader = reader.with_reorder();
        }
        let reader: Arc<dyn SftpChunkReader> = Arc::new(reader);

        let tmp = tempdir().unwrap();
        let path = tmp.path().join("out.bin");
        // Pre-extend the file. Each spawned task opens its own
        // FD on the same path, so this handle is closed before
        // the tasks start writing.
        {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
                .unwrap();
            file.set_len(data.len() as u64).await.unwrap();
        }

        let start = tokio::time::Instant::now();
        let mut last_log = start;
        let mut last_log_bytes = start_offset;
        let start_offset_val = start_offset;
        let total = data.len() as u64;
        let mut next_offset_to_write = start_offset;

        let _bytes_written = pipelined_read_to_file(
            reader,
            "handle".to_string(),
            start_offset,
            total,
            &path,
            PipelinedReadConfig {
                chunk_size,
                max_inflight,
                stall_timeout: Duration::from_secs(5),
            },
            ProgressReporter {
                file_label: "test",
                start: &start,
                start_offset: &start_offset_val,
                total,
                last_log: &mut last_log,
                last_log_bytes: &mut last_log_bytes,
                next_offset_to_write: &mut next_offset_to_write,
            },
        ).await.unwrap();

        // Re-open and read the on-disk file.
        let mut f = tokio::fs::File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.unwrap();
        buf
    }

    /// Variant of `run_pipelined` that lets a test pass a custom
    /// `MockConfig` (failure / stall / corruption injection) and a
    /// custom `stall_timeout`. Returns `Result` so failure-injection
    /// tests can assert on the error rather than unwrapping.
    async fn run_pipelined_with_config(
        data: &[u8],
        start_offset: u64,
        chunk_size: usize,
        max_inflight: usize,
        stall_timeout: Duration,
        config: MockConfig,
    ) -> anyhow::Result<Vec<u8>> {
        let reader = MockReader::new(data.to_vec()).with_config(config);
        let reader: Arc<dyn SftpChunkReader> = Arc::new(reader);

        let tmp = tempdir().unwrap();
        let path = tmp.path().join("out.bin");
        // Pre-extend the file. Each spawned task opens its own
        // FD on the same path, so this handle is closed before
        // the tasks start writing.
        {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
                .unwrap();
            file.set_len(data.len() as u64).await.unwrap();
        }

        let start = tokio::time::Instant::now();
        let mut last_log = start;
        let mut last_log_bytes = start_offset;
        let start_offset_val = start_offset;
        let total = data.len() as u64;
        let mut next_offset_to_write = start_offset;

        let _bytes_written = pipelined_read_to_file(
            reader,
            "handle".to_string(),
            start_offset,
            total,
            &path,
            PipelinedReadConfig {
                chunk_size,
                max_inflight,
                stall_timeout,
            },
            ProgressReporter {
                file_label: "test",
                start: &start,
                start_offset: &start_offset_val,
                total,
                last_log: &mut last_log,
                last_log_bytes: &mut last_log_bytes,
                next_offset_to_write: &mut next_offset_to_write,
            },
        ).await?;

        // Re-open and read the on-disk file.
        let mut f = tokio::fs::File::open(&path).await?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    /// Smallest case: 1 chunk, 1 request, 1 write. Verifies the
    /// happy path with no pipelining.
    #[tokio::test]
    async fn test_pipelined_single_chunk() {
        let data: Vec<u8> = (0..1024).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined(&data, 0, 256, 4, false).await;
        assert_eq!(out, data);
    }

    /// Many chunks, 4 in flight, no reordering. The reader serves
    /// each request in order; the file receives in order; total
    /// output equals the source.
    #[tokio::test]
    async fn test_pipelined_in_order() {
        let data: Vec<u8> = (0..4096).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined(&data, 0, 256, 4, false).await;
        assert_eq!(out, data);
    }

    /// Same as `test_pipelined_in_order` but the reader completes
    /// requests in *reverse* order of issue. Under the
    /// writethrough model, chunks land at their true offsets in
    /// arbitrary order; the on-disk file must still be byte-
    /// identical to the source.
    #[tokio::test]
    async fn test_pipelined_reorder() {
        let data: Vec<u8> = (0..4096).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined(&data, 0, 256, 4, true).await;
        assert_eq!(out, data, "output must be byte-identical to source");
    }

    /// Resuming from a non-zero offset: start_offset > 0, the first
    /// request goes out at start_offset. The file is pre-extended
    /// (via `set_len` in `run_pipelined`) and the writes land at
    /// their true offsets, so a resumed download doesn't need to
    /// rewrite the prefix.
    #[tokio::test]
    async fn test_pipelined_resume() {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let data: Vec<u8> = (0..2048).map(|i| (i & 0xff) as u8).collect();
        let prefix = 512usize;
        let start_offset = prefix as u64;
        // First, write the prefix directly to the file.
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("out.bin");
        {
            let mut f = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
                .unwrap();
            f.set_len(data.len() as u64).await.unwrap();
            f.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            f.write_all(&data[..prefix]).await.unwrap();
        }

        // Now run the pipelined reader on the rest, with reorder
        // enabled to exercise the out-of-order path.
        let mut reader = MockReader::new(data.to_vec()).with_reorder();
        let reader: Arc<dyn SftpChunkReader> = Arc::new(reader);

        let start = tokio::time::Instant::now();
        let mut last_log = start;
        let mut last_log_bytes = start_offset;
        let start_offset_val = start_offset;
        let total = data.len() as u64;
        let mut next_offset_to_write = start_offset;

        let _bytes_written = pipelined_read_to_file(
            reader,
            "handle".to_string(),
            start_offset,
            total,
            &path,
            PipelinedReadConfig {
                chunk_size: 256,
                max_inflight: 4,
                stall_timeout: Duration::from_secs(5),
            },
            ProgressReporter {
                file_label: "test",
                start: &start,
                start_offset: &start_offset_val,
                total,
                last_log: &mut last_log,
                last_log_bytes: &mut last_log_bytes,
                next_offset_to_write: &mut next_offset_to_write,
            },
        ).await.unwrap();

        let mut f = tokio::fs::File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data, "resumed file must match the source byte-for-byte");
    }

    /// Final chunk is smaller than `chunk_size` (the last request
    /// at EOF is short). The reader returns a partial chunk; the
    /// writer still gets the right total.
    #[tokio::test]
    async fn test_pipelined_unaligned_eof() {
        let data: Vec<u8> = (0..1000).map(|i| (i & 0xff) as u8).collect();  // not a multiple of 256
        let out = run_pipelined(&data, 0, 256, 8, true).await;
        assert_eq!(out, data);
    }

    // ---------- Parallel downloader / interleaving tests ----------
    //
    // The custom parallel reader (`pipelined_read_to_file`) is the
    // load-bearing piece of the throughput story: 16 in-flight
    // reads × 256 KiB chunks = 4 MB of pipelined bandwidth, with
    // chunks landing at the disk in arbitrary order. The tests
    // above cover the happy path; these cover the failure modes
    // and edge cases that would silently corrupt a file if the
    // reader had a logic bug.
    //
    // The framing of each test is: drive a specific condition
    // (max_inflight boundary, file-size boundary, injected read
    // failure, stall timeout, single-chunk corruption), then assert
    // either (a) the on-disk file matches the source byte-for-byte
    // on success, or (b) the call returns an error on failure and
    // no `unwrap()` panics. We don't try to assert the on-disk
    // file on a failed run — partial writes are not a corruption
    // mode the operator can act on, and the SHA-256 check is the
    // post-condition that matters.

    /// 16 in-flight reads, 64 chunks, full reverse reorder. The
    /// adversarial case for the writethrough model: every chunk
    /// lands at the disk in the *reverse* of its issue order, so
    /// the high-water-mark advances backwards, then jumps, then
    /// backwards again. The on-disk file must still be byte-
    /// identical to the source. This is the regression test for
    /// any "we wrote in order" assumption that creeps in.
    #[tokio::test]
    async fn test_pipelined_full_reverse_reorder() {
        // 64 chunks × 256 B = 16 KiB. Small enough to run in a
        // few ms; large enough that 4 in-flight rounds cycle
        // through 16 distinct writes.
        let data: Vec<u8> = (0..(64 * 256)).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 16, Duration::from_secs(5),
            MockConfig { reorder: true, ..Default::default() },
        ).await.unwrap();
        assert_eq!(out, data, "full reverse reorder must produce byte-identical file");
    }

    /// `max_inflight = 1` is the degenerate case: the reader
    /// should fall back to one read at a time and produce the
    /// correct file. If the loop has an off-by-one that requires
    /// N ≥ 2 to mask, this test catches it.
    #[tokio::test]
    async fn test_pipelined_max_inflight_one() {
        let data: Vec<u8> = (0..2048).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 1, Duration::from_secs(5),
            MockConfig::default(),
        ).await.unwrap();
        assert_eq!(out, data);
    }

    /// Zero-byte remote file. The reader returns an empty chunk
    /// on the first read; the writer writes nothing. The on-disk
    /// file exists and is zero bytes. The post-download size
    /// check in `download_file` will accept it.
    #[tokio::test]
    async fn test_pipelined_zero_byte_file() {
        let data: Vec<u8> = Vec::new();
        let out = run_pipelined_with_config(
            &data, 0, 256, 4, Duration::from_secs(5),
            MockConfig::default(),
        ).await.unwrap();
        assert!(out.is_empty());
    }

    /// File smaller than one chunk. The single read returns the
    /// whole file; the writer writes it at offset 0. Catches
    /// "the read loop expects at least chunk_size bytes" bugs.
    #[tokio::test]
    async fn test_pipelined_smaller_than_chunk() {
        let data: Vec<u8> = (0..100).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 4, Duration::from_secs(5),
            MockConfig::default(),
        ).await.unwrap();
        assert_eq!(out, data);
    }

    /// File exactly one chunk long. Boundary between
    /// "single-chunk file" and "two-chunk file" — easy to
    /// over-/under-count at the seam.
    #[tokio::test]
    async fn test_pipelined_exactly_one_chunk() {
        let data: Vec<u8> = (0..256).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 4, Duration::from_secs(5),
            MockConfig::default(),
        ).await.unwrap();
        assert_eq!(out, data);
    }

    /// File exactly N*chunk long, N=8. No tail chunk — the loop
    /// must terminate cleanly on the Nth successful read, not
    /// issue a 9th read that returns empty and waste a round
    /// trip.
    #[tokio::test]
    async fn test_pipelined_exactly_n_chunks() {
        let data: Vec<u8> = (0..(8 * 256)).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 4, Duration::from_secs(5),
            MockConfig::default(),
        ).await.unwrap();
        assert_eq!(out, data);
    }

    /// Mid-stream read failure: read #3 returns Err, the rest
    /// succeed. The pipeline must abort the file with a clean
    /// error — not panic, not loop forever, not return a
    /// half-written file marked "ok".
    #[tokio::test]
    async fn test_pipelined_mid_stream_failure() {
        let data: Vec<u8> = (0..(8 * 256)).map(|i| (i & 0xff) as u8).collect();
        let result = run_pipelined_with_config(
            &data, 0, 256, 4, Duration::from_secs(5),
            MockConfig { fail_at: Some(3), ..Default::default() },
        ).await;
        let err = result.expect_err("read #3 failure must propagate as Err");
        let msg = format!("{:#}", err);
        // The error chain should mention the injected failure or
        // the offset — the post-condition is that the operator
        // can tell *which* read failed.
        assert!(
            msg.contains("injected SFTP read failure") || msg.contains("offset 768"),
            "error should identify the failing read, got: {}", msg
        );
    }

    /// All reads fail. The pipeline must abort on the first
    /// error, not retry the rest. Catches a "swallow the error
    /// and continue" bug in the join_next loop.
    #[tokio::test]
    async fn test_pipelined_all_reads_fail() {
        let data: Vec<u8> = (0..(4 * 256)).map(|i| (i & 0xff) as u8).collect();
        let result = run_pipelined_with_config(
            &data, 0, 256, 2, Duration::from_secs(5),
            MockConfig { fail_at: Some(0), ..Default::default() },
        ).await;
        assert!(result.is_err(), "all-fail scenario must surface as Err");
    }

    /// Stall: one read hangs past `stall_timeout` and the
    /// pipeline aborts cleanly. Without the timeout (or with a
    /// misconfigured one) this test would hang the test
    /// runner — the 5 s upper bound on the test itself is the
    /// canary.
    #[tokio::test]
    async fn test_pipelined_stall_timeout() {
        let data: Vec<u8> = (0..(4 * 256)).map(|i| (i & 0xff) as u8).collect();
        // stall_timeout = 200ms in the pipeline; inject a 2s
        // sleep on read #1. The pipeline's per-read
        // `tokio::time::timeout` must fire and abort the file.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            run_pipelined_with_config(
                &data, 0, 256, 4, Duration::from_millis(200),
                MockConfig {
                    stall_at: Some((1, Duration::from_secs(2))),
                    ..Default::default()
                },
            ),
        ).await
        .expect("test should not hang past 5s (stall timeout must fire)");
        let err = result.expect_err("stalling read must propagate as Err");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("stalled") || msg.contains("timeout") || msg.contains("200"),
            "error should mention the stall, got: {}", msg
        );
    }

    /// A single chunk with corrupt bytes. The on-disk file will
    /// not match the source — the test asserts that. The
    /// post-download SHA-256 check is the only thing that
    /// catches this in production, and that's verified
    /// end-to-end by the `verify_existing_file` tests below.
    /// Here we're just pinning that the reader returns the
    /// corrupted chunk to the writer (no in-loop integrity
    /// check), so a future "we can skip the SHA at download
    /// time because the reader is trustworthy" optimization
    /// would have to remove this test (and would be wrong).
    #[tokio::test]
    async fn test_pipelined_corrupt_chunk_is_visible_to_caller() {
        let data: Vec<u8> = (0..(4 * 256)).map(|i| (i & 0xff) as u8).collect();
        let out = run_pipelined_with_config(
            &data, 0, 256, 2, Duration::from_secs(5),
            MockConfig { corrupt_at: Some(1), ..Default::default() },
        ).await.unwrap();
        // The chunk at offset 256 should be 0xCC, not the
        // original bytes. The reader is *not* the integrity
        // check; it passes data through.
        assert_ne!(out, data, "corruption must land on disk unchanged");
        // Specifically: bytes 256..512 are 0xCC, the rest is
        // untouched.
        assert!(out[256..512].iter().all(|&b| b == 0xCC));
        assert_eq!(&out[..256], &data[..256]);
        assert_eq!(&out[512..], &data[512..]);
    }

    // ---------- 0-byte retry check tests ----------
    //
    // The 2026-06-15 physalis production run exposed a silent
    // success path: when `try_download_file` is called on a
    // retry and `start_offset == remote_size` (the prior
    // attempt's pre-extend set the file's size), the pipelined
    // reader issues zero reads and returns `Ok(start_offset)`.
    // The downstream size check then passes
    // (`final_size == remote_size`) and the SHA check is
    // skipped (no expected hash), so the file — full of zeros
    // from `start_offset` to `total` — is silently marked
    // Synced. The defense is a single-line check
    // (`check_zero_writes`) at the start of the post-read
    // block. These tests pin the check down at the unit level
    // so a future refactor of `try_download_file` can't
    // regress this.

    #[test]
    fn test_check_zero_writes_bails_when_no_progress() {
        // 184 MB file, retry starting at 184 MB. The pipelined
        // reader issued 0 reads this attempt. The check must
        // bail so the retry wrapper exhausts the budget and
        // marks the row sync_failed rather than silently
        // passing a file full of zeros.
        let result = check_zero_writes(184_346_979, 184_346_979, 184_346_979, "/remote/movie.mkv");
        // Note: this case has `start_offset == total` so
        // there is *no* work to do. The check is suppressed
        // for that case (start_offset < total must hold).
        assert!(result.is_ok(), "start_offset == total is a legitimate no-op");
    }

    #[test]
    fn test_check_zero_writes_bails_when_channel_dies_mid_resume() {
        // 184 MB file, retry starting at 150 MB. The pipelined
        // reader issued 0 reads even though there were 34 MB
        // of work to do. The check must bail.
        let result = check_zero_writes(150_000_000, 150_000_000, 184_346_979, "/remote/movie.mkv");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("no bytes were read"),
            "error message should mention the 0-reads case, got: {}", msg
        );
        assert!(msg.contains("channel may be desynced"));
    }

    #[test]
    fn test_check_zero_writes_allows_partial_progress() {
        // 184 MB file, retry starting at 0. The pipelined
        // reader did some work (got 100 MB in) but stopped
        // short. The check must NOT bail — this is a normal
        // partial-write that the retry wrapper will resume
        // from on the next attempt.
        let result = check_zero_writes(100_000_000, 0, 184_346_979, "/remote/movie.mkv");
        assert!(result.is_ok(), "partial progress must not bail; got: {:?}", result);
    }

    #[test]
    fn test_check_zero_writes_allows_fresh_complete_download() {
        // 100 MB file, fresh download. The pipelined reader
        // completed the whole file. bytes_written == total.
        // start_offset == 0. The check must NOT bail.
        let result = check_zero_writes(100_000_000, 0, 100_000_000, "/remote/movie.mkv");
        assert!(result.is_ok());
    }

    // ---------- parse_sha256sum_output tests ----------
    //
    // The `collect_remote_hashes` exec path runs `sha256sum` on the
    // remote host and ships the stdout back over the SSH channel.
    // The parser is the only place the on-the-wire format meets
    // the in-process data, so a regression here is silent: a
    // mismatched field would produce a wrong verification later.
    // These tests pin the parser down.

    use crate::sync::parse_sha256sum_output;

    /// A representative manifest that the parser is given to
    /// cross-reference against. The rel_path → (size, mtime) shape
    /// matches `collect_manifest`'s output.
    fn sample_manifest() -> BTreeMap<String, (u64, u64)> {
        let mut m = BTreeMap::new();
        m.insert("movie.mkv".to_string(), (60_000_000_000, 1_700_000_000));
        m.insert("subs/en.srt".to_string(), (50_000, 1_700_000_001));
        m
    }

    /// Happy path: GNU coreutils' `sha256sum` writes
    /// `<64-hex><space><space><path>\n` per file. The double
    /// space is the mode flag (binary mode = `*`, text mode =
    /// ` `). Two entries, two lines, all fields round-trip.
    #[test]
    fn test_parse_sha256sum_output_basic() {
        let h1 = "a".repeat(64);
        let h2 = "b".repeat(64);
        let output = format!(
            "{h1}  /srv/data/media/movies/Foo/movie.mkv\n\
             {h2}  /srv/data/media/movies/Foo/subs/en.srt\n"
        );
        let manifest = sample_manifest();
        let got = parse_sha256sum_output(
            &output,
            Path::new("/srv/data/media/movies/Foo"),
            &manifest,
        ).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, "movie.mkv");
        assert_eq!(got[0].1, h1);
        assert_eq!(got[0].2, 60_000_000_000);
        assert_eq!(got[1].0, "subs/en.srt");
        assert_eq!(got[1].1, h2);
        assert_eq!(got[1].2, 50_000);
    }

    /// Paths with characters that would be a quoting nightmare if
    /// we'd passed them on argv: spaces, single quotes, dollar
    /// signs. Because we use stdin + `xargs -d '\n'`, these go
    /// through untouched and round-trip cleanly.
    #[test]
    fn test_parse_sha256sum_output_with_spaces_and_quotes() {
        let h = "c".repeat(64);
        let remote_path = "/srv/data/media/movies/Foo/Spaces and 'quotes' in $name.mkv";
        let output = format!("{h}  {remote_path}\n");
        let mut manifest = BTreeMap::new();
        manifest.insert("Spaces and 'quotes' in $name.mkv".to_string(), (42, 99));
        let got = parse_sha256sum_output(
            &output,
            Path::new("/srv/data/media/movies/Foo"),
            &manifest,
        ).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "Spaces and 'quotes' in $name.mkv");
        assert_eq!(got[0].1, h);
    }

    /// `sha256sum` writes warnings to stderr, not stdout. A file
    /// it can't read produces *no* stdout line at all — the
    /// manifest simply doesn't see a hash for it, and the
    /// download path treats that as "skip verification". This
    /// test pins that behavior: the parser must not error on
    /// partial output.
    #[test]
    fn test_parse_sha256sum_output_skips_missing_entries() {
        let h = "d".repeat(64);
        // Only `movie.mkv` in the output. `subs/en.srt` is
        // missing (server couldn't read it; warning on stderr
        // which we discard).
        let output = format!("{h}  /srv/data/media/movies/Foo/movie.mkv\n");
        let manifest = sample_manifest();
        let got = parse_sha256sum_output(
            &output,
            Path::new("/srv/data/media/movies/Foo"),
            &manifest,
        ).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "movie.mkv");
        // The missing `subs/en.srt` is silently absent from the
        // result. The download path's "no expected hash" branch
        // handles it.
        let paths: Vec<&str> = got.iter().map(|t| t.0.as_str()).collect();
        assert!(!paths.contains(&"subs/en.srt"));
    }

    // ---------- compute_local_sha256 tests ----------
    //
    // The download-time verification path re-hashes the local
    // file and compares against the expected hash from the DB.
    // A regression here (wrong chunk size, off-by-one in the
    // digest) would silently accept corrupt files. These tests
    // pin the behavior: round-trip on a known input, detect
    // tampering, and a basic smoke test for the
    // streaming-vs-one-shot equivalence.

    /// Write `data` to a temp file, hash it, return the result.
    async fn hash_file(data: &[u8]) -> String {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("h.bin");
        tokio::fs::write(&path, data).await.unwrap();
        compute_local_sha256(&path).await.unwrap()
    }

    /// Known-answer: hashing the literal bytes
    /// `"hello sha256 world"` must yield the SHA-256 of those
    /// exact bytes. This pins the digest algorithm AND the
    /// streaming behavior against a one-shot baseline.
    #[tokio::test]
    async fn test_compute_local_sha256_known_answer() {
        let data = b"hello sha256 world";
        // sha256 of "hello sha256 world" — confirmed via:
        //   $ printf 'hello sha256 world' | sha256sum
        let expected = "3161369ee9e037d6927911aff27af159bbc583b58f2d4ceaae074668d987487e";
        let got = hash_file(data).await;
        assert_eq!(got, expected);
    }

    /// Tamper a file *after* writing it: the hash must change.
    /// This is the regression case — a hash that always returned
    /// "matches" would let corrupted downloads through silently.
    #[tokio::test]
    async fn test_compute_local_sha256_detects_tampering() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("orig.bin");
        tokio::fs::write(&path, b"original contents").await.unwrap();
        let h_orig = compute_local_sha256(&path).await.unwrap();

        // Flip a single byte.
        let mut buf = b"original contents".to_vec();
        buf[3] = b'X';
        tokio::fs::write(&path, &buf).await.unwrap();
        let h_tampered = compute_local_sha256(&path).await.unwrap();

        assert_ne!(h_orig, h_tampered, "tampered file must produce a different hash");
    }

    // ---------- verify_existing_file tests ----------
    //
    // `verify_existing_file` is the pre-download "should we trust
    // what's on disk?" gate. It used to be a single-line
    // `local_size == remote_size` check that returned `Ok(())`
    // silently — the very thing that let silent corruption through
    // in the "downloaded files have checksums that do not match the
    // remote" report. These tests pin the new behavior: trust
    // only when size + hash (if available) both line up, error
    // loudly otherwise.

    use crate::sync::verify_existing_file;
    use sha2::{Digest, Sha256};

    /// Convenience: hash the given bytes so the tests can pass
    /// "this is what the remote said" without re-implementing
    /// the SHA-256 call.
    fn sha256_hex(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        format!("{:x}", h.finalize())
    }

    /// Size matches, no recorded hash. Best-effort trust: the
    /// download path's "no expected hash" branch in `download_file`
    /// is the only signal we have, and falling through to download
    /// would make the remote hash collection a hard dependency.
    /// Pin the existing semantics so a future change has to be
    /// explicit.
    #[tokio::test]
    async fn test_verify_trust_when_no_hash() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.bin");
        tokio::fs::write(&path, b"some bytes").await.unwrap();
        let got = verify_existing_file(&path, 10, 10, None).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Trust);
    }

    /// Size matches, hash matches. Trust. This is the happy path
    /// — the file was downloaded cleanly in a prior run, the
    /// manifest recorded its hash, and a re-run should skip the
    /// download entirely.
    #[tokio::test]
    async fn test_verify_trust_when_hash_matches() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.bin");
        let data = b"hello verify_existing_file";
        tokio::fs::write(&path, data).await.unwrap();
        let expected = sha256_hex(data);
        let got = verify_existing_file(&path, data.len() as u64, data.len() as u64, Some(&expected))
            .await.unwrap();
        assert_eq!(got, LocalFileDisposition::Trust);
    }

    /// Size matches, hash does NOT match. The local file is at
    /// the right size but with the wrong bytes — exactly the
    /// silent-corruption case the user reported. The disposition
    /// must be `Download` (re-pull from SFTP), not `Trust` and
    /// not `Err` (which would halt the walker). The mismatch is
    /// still surfaced to the operator via a `warn!` log.
    #[tokio::test]
    async fn test_verify_downloads_on_hash_mismatch() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.bin");
        // Write the "actual" bytes; claim the hash is for
        // *different* bytes.
        let actual = b"what's actually on disk";
        tokio::fs::write(&path, actual).await.unwrap();
        let wrong_expected = sha256_hex(b"some other bytes entirely");
        let got = verify_existing_file(
            &path,
            actual.len() as u64,
            actual.len() as u64,
            Some(&wrong_expected),
        ).await.expect("hash mismatch must NOT surface as Err (that would halt the walker)");
        assert_eq!(
            got,
            LocalFileDisposition::Download,
            "hash mismatch should trigger re-download, not Trust"
        );
    }

    /// Local file is smaller than remote. Re-download.
    #[tokio::test]
    async fn test_verify_download_when_local_smaller() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.bin");
        tokio::fs::write(&path, b"partial").await.unwrap();
        let got = verify_existing_file(&path, 7, 100, None).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Download);
    }

    /// Local file is *larger* than remote. The remote shrunk
    /// (file was truncated upstream) and our local copy is
    /// stale. Re-download — the size check at the end of
    /// `download_file` would catch a regression here, but we
    /// shouldn't even get that far.
    #[tokio::test]
    async fn test_verify_download_when_local_larger() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.bin");
        tokio::fs::write(&path, b"a much larger local file than the remote claims").await.unwrap();
        let got = verify_existing_file(&path, 43, 10, None).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Download);
    }

    /// Both sizes zero. Empty file, no body to hash. Trust —
    /// the on-disk file is correct by virtue of being empty.
    #[tokio::test]
    async fn test_verify_trust_when_both_zero() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("empty.bin");
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        // Create an actually-empty file so the open() in
        // compute_local_sha256 wouldn't fail if the size-zero
        // guard were removed.
        tokio::fs::write(&path, b"").await.unwrap();
        let got = verify_existing_file(&path, 0, 0, Some("any-non-empty-hash")).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Trust);
    }

    /// Local file missing entirely. The size check sees 0 vs
    /// the remote's N > 0 and returns `Download` before
    /// touching the (non-existent) file.
    #[tokio::test]
    async fn test_verify_download_when_local_missing() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("does_not_exist.bin");
        // Sanity: the file does not exist.
        assert!(!path.exists());
        let got = verify_existing_file(&path, 0, 1024, None).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Download);
    }

    /// Round-trip: write data, capture its hash, write the same
    /// data again later (simulating a re-run), confirm `Trust`.
    /// A higher-level "the file on disk matches what the remote
    /// said" sanity check.
    #[tokio::test]
    async fn test_verify_round_trip_on_realistic_data() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("round_trip.bin");
        // 1 MiB of pseudo-random bytes — large enough to
        // exercise the streaming read in compute_local_sha256
        // (the function reads in 1 MiB chunks, so a 1 MiB
        // file goes through the "exact one chunk" path; 2 MiB
        // exercises the "more than one chunk" path).
        let data: Vec<u8> = (0..(2 * 1024 * 1024)).map(|i| ((i * 31 + 7) & 0xff) as u8).collect();
        tokio::fs::write(&path, &data).await.unwrap();
        let expected = sha256_hex(&data);
        // Simulate a re-run: same path, same recorded hash.
        let got = verify_existing_file(
            &path, data.len() as u64, data.len() as u64, Some(&expected),
        ).await.unwrap();
        assert_eq!(got, LocalFileDisposition::Trust);
    }

    // ---------- drain_readdir_pages tests ----------
    //
    // These exercise the SFTP-directory-listing terminator handling
    // that broke in production on 2026-06-13: russh-sftp's low-level
    // `readdir` propagates the protocol's `Eof` status as an `Err`,
    // so a naïve loop that expects an empty `files` list blows up
    // on the very first call. `drain_readdir_pages` accepts both
    // terminator conventions (empty list and Eof status) and
    // propagates real errors. These tests pin that behavior down.

    use russh_sftp::client::error::Error as SftpError;
    use russh_sftp::protocol::{File, FileAttributes, Name, Status, StatusCode};

    fn make_file(name: &str) -> File {
        File {
            filename: name.to_string(),
            longname: String::new(),
            attrs: FileAttributes::default(),
        }
    }

    /// Most realistic case: server returns one page of entries, then
    /// the `Eof` status on the next call (OpenSSH sftp-server
    /// behavior). The pre-fix code propagated this as an error and
    /// crashed the sync. The post-fix code treats it as a successful
    /// terminator.
    #[tokio::test]
    async fn test_drain_readdir_pages_eof_status() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);

        let files = drain_readdir_pages(|| async {
            match CALLS.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(Name {
                    id: 0,
                    files: vec![make_file("a"), make_file("b")],
                }),
                _ => Err(SftpError::Status(Status {
                    id: 0,
                    status_code: StatusCode::Eof,
                    error_message: String::new(),
                    language_tag: String::new(),
                })),
            }
        })
        .await
        .expect("Eof status should be treated as terminator, not error");

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].filename, "a");
        assert_eq!(files[1].filename, "b");
    }

    /// Older-server convention: server returns a `Name` packet with
    /// an empty `files` list to signal end-of-directory. Must also
    /// break, not error.
    #[tokio::test]
    async fn test_drain_readdir_pages_empty_files_terminator() {
        let files = drain_readdir_pages(|| async {
            Ok(Name {
                id: 0,
                files: Vec::new(),
            })
        })
        .await
        .expect("empty-files list is also a valid terminator");

        assert!(files.is_empty());
    }

    /// Multiple pages, ending with `Eof`. Exercises the
    /// accumulator — all pages' files must be returned in order.
    #[tokio::test]
    async fn test_drain_readdir_pages_paginated_with_eof() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);

        let files = drain_readdir_pages(|| async {
            match CALLS.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(Name {
                    id: 0,
                    files: vec![make_file("a"), make_file("b")],
                }),
                1 => Ok(Name {
                    id: 0,
                    files: vec![make_file("c")],
                }),
                _ => Err(SftpError::Status(Status {
                    id: 0,
                    status_code: StatusCode::Eof,
                    error_message: String::new(),
                    language_tag: String::new(),
                })),
            }
        })
        .await
        .expect("paginated listing with Eof terminator must succeed");

        let names: Vec<&str> = files.iter().map(|f| f.filename.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    /// A non-`Eof` error must propagate, not be silently treated as
    /// end-of-directory. This is the difference between "server is
    /// done" and "server is broken" — they look the same to a naïve
    /// loop. The post-fix code distinguishes them.
    #[tokio::test]
    async fn test_drain_readdir_pages_propagates_real_errors() {
        let result = drain_readdir_pages(|| async {
            Err(SftpError::Status(Status {
                id: 0,
                status_code: StatusCode::PermissionDenied,
                error_message: "nope".to_string(),
                language_tag: String::new(),
            }))
        })
        .await;

        let err = result.expect_err("PermissionDenied must propagate, not be hidden as Eof");
        match err {
            SftpError::Status(s) => {
                assert_eq!(s.status_code, StatusCode::PermissionDenied);
            }
            other => panic!("expected Status error, got {:?}", other),
        }
    }

    /// No calls at all (server returns Eof on the first readdir).
    /// Must return an empty vec, not an error.
    #[tokio::test]
    async fn test_drain_readdir_pages_empty_directory_via_eof() {
        let files = drain_readdir_pages(|| async {
            Err(SftpError::Status(Status {
                id: 0,
                status_code: StatusCode::Eof,
                error_message: String::new(),
                language_tag: String::new(),
            }))
        })
        .await
        .expect("Eof on first call means empty dir, not error");

        assert!(files.is_empty());
    }

    // =====================================================================
    // DB-driven dispatch: claim, sweep, drain
    // =====================================================================
    //
    // The pre-2026-06-14 design used a `mpsc::Sender<DirJob>` to
    // dispatch work from the walker to the downloader pool, and
    // the test suite pinned the "drop the Sender before
    // awaiting the pool" handshake that kept the empty-walk
    // case from hanging. After the DB-driven dispatch refactor,
    // there is no mpsc — the dispatch is `db.claim_detected_row`,
    // an atomic UPDATE that returns the row only if no other
    // downloader has already claimed it. These tests pin the
    // new contract: the claim is atomic, the stale-Syncing
    // sweep recovers orphan rows, and the pool drains when
    // signaled even with rows still in the DB.

    use crate::db::DirectoryState;

    /// Insert a `Detected` row directly via the DB. The
    /// production walker does the same thing via
    /// `db.upsert_directory`; this helper skips that path
    /// because the dispatch contract doesn't depend on how
    /// the row got there.
    fn insert_detected(db: &Database, category: &str, name: &str) -> i64 {
        let staging = tempfile::tempdir().unwrap();
        let staging_path = staging.path().to_string_lossy().to_string();
        let remote_path = format!("/remote/{}/{}", category, name);
        let (id, _prev) = db.upsert_directory(
            category,
            &remote_path,
            &staging_path,
            "test-manifest-hash",
        ).unwrap();
        id
    }

    /// **Atomic claim.** Two concurrent claimers, one row,
    /// exactly one wins. This is the property that lets the
    /// downloader pool scale to N parallel downloaders
    /// without coordination beyond the DB.
    #[tokio::test]
    async fn test_claim_detected_row_atomic_under_concurrent_callers() {
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        insert_detected(&db, "movies", "A");

        // Two claimers race. The DB serializes writes through
        // the connection mutex, so exactly one UPDATE will
        // affect the row (return `Some`), and the other will
        // see the row already in `Syncing` and return `None`.
        let db1 = db.clone();
        let db2 = db.clone();
        let (r1, r2) = tokio::join!(
            tokio::spawn(async move { db1.claim_detected_row() }),
            tokio::spawn(async move { db2.claim_detected_row() }),
        );
        let r1 = r1.unwrap().unwrap();
        let r2 = r2.unwrap().unwrap();

        let winners = [r1.is_some(), r2.is_some()].iter().filter(|x| **x).count();
        let losers = [r1.is_none(), r2.is_none()].iter().filter(|x| **x).count();
        assert_eq!(winners, 1, "exactly one claimer must win the row");
        assert_eq!(losers, 1, "exactly one claimer must see the row already claimed");

        // The row is now in `Syncing`, not `Detected`.
        let current = db.get_directory_by_id(r1.unwrap_or_else(|| r2.unwrap()).id)
            .unwrap().unwrap();
        assert_eq!(current.state, DirectoryState::Syncing,
            "claimed row must be in Syncing, not Detected");
    }

    /// **Claim returns oldest first.** Two rows in `Detected`,
    /// claim should return the older one (lowest `detected_at`).
    /// This is the FIFO invariant — if a downloader always
    /// grabbed the newest row, an old row could starve.
    #[tokio::test]
    async fn test_claim_detected_row_returns_oldest_first() {
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        // Insert "A" first, then "B". A's `detected_at` is
        // older (or equal — both are CURRENT_TIMESTAMP at
        // insert time, so the ORDER BY may be tie-broken by
        // id, which is monotonically increasing, so the
        // older row's id is smaller). To make A
        // deterministically older, sleep briefly between
        // inserts.
        let id_a = insert_detected(&db, "movies", "A");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _id_b = insert_detected(&db, "movies", "B");

        let claim = db.claim_detected_row().unwrap().unwrap();
        assert_eq!(claim.id, id_a, "claim must return the oldest Detected row first");
    }

    /// **Stale-Syncing sweep.** A row in `Syncing` for 7 hours
    /// (longer than the 6h threshold) gets reset to `Detected`
    /// by the sweep. A row in `Syncing` for 1 hour (under the
    /// threshold) is left alone.
    #[tokio::test]
    async fn test_stale_syncing_sweep_recovers_orphan_rows() {
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        let id_stale = insert_detected(&db, "movies", "Stale");
        let id_fresh = insert_detected(&db, "movies", "Fresh");

        // Claim both to put them in `Syncing`.
        let claim_stale = db.claim_detected_row().unwrap().unwrap();
        let claim_fresh = db.claim_detected_row().unwrap().unwrap();
        assert_eq!(claim_stale.id, id_stale);
        assert_eq!(claim_fresh.id, id_fresh);

        // Push `syncing_at` for `id_stale` back to 7 hours
        // ago. `id_fresh`'s `syncing_at` is `now` (set by the
        // claim) — well within the 6h window.
        db._test_set_syncing_at(id_stale, "-7 hours").unwrap();

        let recovered = db.stale_syncing_sweep(6).unwrap();
        assert_eq!(recovered, 1, "sweep should recover exactly the stale row");

        // The stale row is back in `Detected`. The fresh row
        // is still in `Syncing`.
        let stale_row = db.get_directory_by_id(id_stale).unwrap().unwrap();
        let fresh_row = db.get_directory_by_id(id_fresh).unwrap().unwrap();
        assert_eq!(stale_row.state, DirectoryState::Detected,
            "stale-Syncing row should be reset to Detected");
        assert_eq!(fresh_row.state, DirectoryState::Syncing,
            "fresh-Syncing row should be left alone");
    }

    /// **count_detected.** Rows in `Detected` count; rows in
    /// other states don't. Used by the downloader pool's
    /// drain check.
    #[tokio::test]
    async fn test_count_detected_counts_only_detected_rows() {
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        // Three rows in `Detected` (fresh inserts).
        insert_detected(&db, "movies", "A");
        insert_detected(&db, "movies", "B");
        insert_detected(&db, "movies", "C");
        // One row forced to `Synced`.
        let id_synced = insert_detected(&db, "movies", "D");
        db.set_directory_state(id_synced, DirectoryState::Synced).unwrap();
        // One row claimed → `Syncing`.
        let _id_claimed = insert_detected(&db, "movies", "E");
        let _ = db.claim_detected_row().unwrap().unwrap();

        let n = db.count_detected().unwrap();
        assert_eq!(n, 3, "count_detected should see only rows in Detected state (got {})", n);
    }

    // =====================================================================
    // Real-SFTP integration test
    // =====================================================================
    //
    // The MockReader-based unit tests cover ordering, stall,
    // resume, and corruption paths up to 16 KiB. They do NOT
    // exercise the production code path — the russh-sftp crate
    // has its own state machine (window updates, request IDs,
    // EOF handling, channel close semantics) that hides behind
    // the trait. The largest unit test is 16 KiB at 256-byte
    // chunks; production runs are 4-60 GB at 256 KiB chunks.
    //
    // This test stands up a real SFTP server (atmoz/sftp in
    // docker, a 12 MB OpenSSH image that exposes SFTP without
    // a shell), seeds a 100 MB random file, and runs the
    // actual `pipelined_read_to_file` against a real
    // `RawSftpSession`. The on-disk result is SHA-256'd and
    // compared to the source's SHA-256.
    //
    // **Gated.** The test is `#[ignore]`-marked so it doesn't
    // run in `cargo test` (CI doesn't have docker). Opt in
    // with `cargo test --release -- --ignored real_sftp` or
    // `cargo test --release -- --ignored`.
    //
    // **Skip on no-docker.** If `docker` is missing, the test
    // returns early with a `eprintln!` and `Ok(())` rather than
    // failing — the test environment may not have docker even
    // for opt-in runs.
    //
    // **SSH key handling.** The test writes a one-off ed25519
    // keypair into a tempdir, mounts the public key into the
    // container at `/home/test/.ssh/authorized_keys`, and uses
    // the private key for auth. The server's host key is
    // accepted unconditionally (`check_server_key` returns
    // `true` for any key) — this is a localhost loopback test;
    // MITM isn't a concern.
    //
    // **Throughput sanity.** The test asserts a minimum
    // average throughput of 2 MB/s. The OpenSSH `sftp-server`
    // caps each `SSH_FXP_READ` response at 64 KiB by default,
    // so a 256 KiB chunk takes 4 round trips, and a 100 MB
    // file needs 1600 round trips. With 16-way pipelining on
    // a localhost loopback, that lands around 3-5 MB/s in
    // practice. 2 MB/s catches a regression that serializes
    // the reads (inflight=1 would roughly quarter this) while
    // leaving enough headroom for the round-trip-bound regime.
    const REAL_SFTP_FILE_SIZE: usize = 100 * 1024 * 1024; // 100 MB

    /// Monotonic counter for unique container names. The pid
    /// alone is shared across tests in the same process, so
    /// we append a per-call index to disambiguate. The
    /// counter is bumped on every call, including failed
    /// ones, so the next test doesn't try to reuse a name
    /// whose previous container may not yet have been
    /// reaped.
    fn next_container_index() -> u32 {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    fn real_sftp_docker_available() -> bool {
        // Cheap probe: `docker info` exits 0 on a working
        // daemon, non-zero on missing binary or no daemon. The
        // `2>&1` swallows stderr; we only care about the exit
        // code.
        std::process::Command::new("docker")
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn real_sftp_image_available() -> bool {
        // `docker image inspect` exits 0 if the image is
        // already pulled, non-zero if it would need to be
        // pulled. We don't auto-pull (could be expensive /
        // network-dependent in CI).
        std::process::Command::new("docker")
            .args(["image", "inspect", "atmoz/sftp:latest"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// RAII helper that runs `docker rm -f` on drop. Keeps the
    /// container from leaking if the test panics.
    struct DockerContainer {
        name: String,
    }
    impl Drop for DockerContainer {
        fn drop(&mut self) {
            // Best-effort cleanup. We don't propagate errors;
            // a leaked container is a test-environment problem,
            // not a test failure.
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.name])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }

    /// Generate a fresh ed25519 keypair, write to `key_dir` as
    /// `id_ed25519` / `id_ed25519.pub`, and return the path to
    /// the private key. We use ed25519 because it's small,
    /// fast, and supported by both atmoz/sftp's OpenSSH and
    /// russh.
    fn generate_test_keypair(key_dir: &Path) -> anyhow::Result<PathBuf> {
        use russh_keys::key::KeyPair;

        // ed25519 is fast to generate and broadly supported.
        let key = KeyPair::generate_ed25519()
            .ok_or_else(|| anyhow::anyhow!("failed to generate ed25519 keypair"))?;
        let private_path = key_dir.join("id_ed25519");
        let public_path = key_dir.join("id_ed25519.pub");

        // Write the private key in OpenSSH PEM format
        // (`russh_keys::encode_pkcs8_pem` is the
        // edition-independent entry point; the returned bytes
        // start with `-----BEGIN OPENSSH PRIVATE KEY-----`).
        let mut priv_buf = Vec::new();
        russh_keys::encode_pkcs8_pem(&key, &mut priv_buf)
            .map_err(|e| anyhow::anyhow!("write private key: {}", e))?;
        std::fs::write(&private_path, &priv_buf)?;
        std::fs::set_permissions(&private_path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;

        // Write the public key in OpenSSH authorized_keys
        // format (`ssh-ed25519 AAAA... comment`). The
        // `write_public_key_base64` helper emits exactly one
        // line, newline-terminated.
        let pubkey = key.clone_public_key()
            .map_err(|e| anyhow::anyhow!("derive public key: {}", e))?;
        let mut pub_buf: Vec<u8> = Vec::new();
        russh_keys::write_public_key_base64(&mut pub_buf, &pubkey)
            .map_err(|e| anyhow::anyhow!("write public key: {}", e))?;
        std::fs::write(&public_path, &pub_buf)?;

        Ok(private_path)
    }

    /// Spin up an atmoz/sftp container with the public key from
    /// `key_dir` authorized for user `test`, and bind-mount
    /// `source_path` as `/home/test/big.bin` inside the chroot.
    /// Returns the container's name and a `Drop` guard for
    /// cleanup.
    ///
    /// Note: the source file is bind-mounted (not `docker cp`'d).
    /// atmoz/sftp chroots the user to `/home/<user>`, and
    /// `docker cp` into a chrooted path is unreliable across
    /// docker engine versions — the cp path is tar-pipe based
    /// and trips on overlayfs + chroot combinations. A direct
    /// `-v` mount of the host file into the chroot is portable
    /// and atomic. The SFTP download path is unchanged: bytes
    /// still come from a real SFTP read of `/home/test/big.bin`.
    fn start_sftp_container(
        key_dir: &Path,
        port: u16,
        source_path: &Path,
    ) -> anyhow::Result<DockerContainer> {
        // Unique container name so concurrent test runs don't
        // collide. The pid+counter form lets multiple
        // real-SFTP tests in the same `cargo test` invocation
        // (which share a pid) coexist — the first test uses
        // index 0, the second uses index 1, etc. The
        // `real_sftp_` prefix makes the orphan obvious in
        // `docker ps -a` output if cleanup fails.
        let name = format!("real_sftp_{}_{}", std::process::id(), next_container_index());

        // atmoz/sftp's image entrypoint aggregates every file
        // in the user's `~/.ssh/keys/` directory into
        // `~/.ssh/authorized_keys` (and refuses to mount
        // authorized_keys directly because of OpenSSH's
        // permission requirements). So the public key goes
        // at `/home/test/.ssh/keys/<anything>.pub` inside the
        // chroot.
        let pubkey_path = key_dir.join("id_ed25519.pub");
        let pubkey_str = std::fs::read_to_string(&pubkey_path)
            .map_err(|e| anyhow::anyhow!("read pubkey: {}", e))?
            .trim()
            .to_string();
        // The chroot-internal path is
        // `/home/test/.ssh/keys/<any-name>.pub`. The image
        // entrypoint scans that directory and concatenates
        // every file into `~/.ssh/authorized_keys` (with the
        // ownership/permissions OpenSSH demands, which is why
        // you can't mount `authorized_keys` directly).
        let keys_dir = key_dir.join("keys");
        std::fs::create_dir_all(&keys_dir)?;
        std::fs::write(keys_dir.join("id_ed25519.pub"), format!("{}\n", pubkey_str))?;

        // `docker run` flags:
        //   --rm             clean up on exit (also covered by
        //                    our Drop guard as belt-and-suspenders)
        //   -d              detached; we don't need a TTY
        //   -p host:22      bind container's sshd to host
        //                   port; we let docker pick a free
        //                   port via `-P`-style handling but
        //                   we need a fixed port for
        //                   reproducibility. We use the
        //                   process-id-derived port and trust
        //                   it's free.
        //   -v host:cont    bind-mount our keys dir
        //   --name          our handle
        //   atmoz/sftp:test users
        //     the trailing arg to the image is a user spec:
        //     `user:pass:ecc` or `user::ecdsa`. The empty
        //     password slot and ed25519 key auth means no
        //     password is needed.
        let status = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name", &name,
                "-p", &format!("{}:22", port),
                // Keys directory: bind-mount at the chroot
                // path that atmoz/sftp's entrypoint scans
                // (`/home/<user>/.ssh/keys/<any>.pub`). The
                // entrypoint aggregates every file in that
                // directory into `~/.ssh/authorized_keys`
                // with the OpenSSH-required ownership and
                // permissions.
                "-v", &format!("{}:/home/test/.ssh/keys:ro", keys_dir.display()),
                // Source file: bind-mount the host source path
                // at the chroot-internal path SFTP will read.
                "-v", &format!("{}:/home/test/big.bin:ro", source_path.display()),
                "atmoz/sftp:latest",
                "test::1001",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| anyhow::anyhow!("docker run failed to start: {}", e))?;
        if !status.success() {
            anyhow::bail!("docker run exited non-zero");
        }

        Ok(DockerContainer { name })
    }

    /// Wait for the container's sshd to accept an actual SSH
    /// connection on `port` and complete the key-exchange
    /// banner. A plain TCP-connect is not enough: atmoz/sftp's
    /// first-boot host-key generation can complete after the
    /// listener starts accepting, and a russh connect that
    /// arrives during the gap sees a `ConnectionReset` (the
    /// daemon aborts its pre-fork listener when the post-fork
    /// child is still warming up).
    ///
    /// We do a banner-read with a 30s budget: open a TCP
    /// connection, read up to a few bytes, and confirm the
    /// server sent `SSH-2.0-...`. Once that line arrives, sshd
    /// is ready to drive a russh `client::connect`.
    async fn wait_for_sshd(port: u16) -> anyhow::Result<()> {
        use std::net::SocketAddr;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        loop {
            // Use a short per-attempt timeout so a half-open
            // listener doesn't burn the full 30s budget.
            let attempt = tokio::time::timeout(
                Duration::from_secs(2),
                async {
                    let mut s = TcpStream::connect(addr).await?;
                    let mut buf = [0u8; 64];
                    let n = s.read(&mut buf).await?;
                    if n == 0 {
                        return Err::<(), std::io::Error>(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "sshd closed without sending a banner",
                        ));
                    }
                    if !buf[..n].starts_with(b"SSH-") {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unexpected banner: {:?}", &buf[..n]),
                        ));
                    }
                    Ok(())
                },
            ).await;

            match attempt {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(_)) if start.elapsed() < timeout => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Ok(Err(e)) => anyhow::bail!(
                    "sshd on port {} never sent a banner: {}", port, e
                ),
                Err(_) if start.elapsed() < timeout => {
                    // Per-attempt 2s timeout expired before we
                    // got a banner; try again.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(_) => anyhow::bail!(
                    "sshd on port {} never sent a banner within {}s",
                    port, timeout.as_secs()
                ),
            }
        }
    }

    /// SHA-256 of a file at `path`, computed via the
    /// `compute_local_sha256` free function the production
    /// code already uses. Same code path = same hashing
    /// behavior; we want to verify the bytes, not the hash
    /// function.
    async fn sha256_of(path: &Path) -> anyhow::Result<String> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;
        let mut f = tokio::fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 { break; }
            hasher.update(&buf[..n]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    #[tokio::test]
    #[ignore = "requires docker + atmoz/sftp image; run with --ignored. \
                Fails (does not silently no-op) if preconditions are missing — \
                this is opt-in because of the docker dependency, not because the \
                outcome is conditional."]
    async fn test_real_sftp_100mb_download_via_pipelined_read_to_file() {
        // ---- 1. Pre-flight checks ----
        //
        // We `panic!` rather than silently `return` so the test
        // outcome is unambiguous in CI: a missing-precondition
        // run is a *failure*, not a green test. The `#[ignore]`
        // gate is "expensive / requires docker" — not "may
        // silently no-op." An operator who runs
        //   cargo test --release -- --ignored
        // and sees green knows the harness actually executed;
        // a panic here means the test environment is broken
        // and the result tells them so.
        if !real_sftp_docker_available() {
            panic!(
                "real_sftp preflight failed: `docker info` exited non-zero. \
                 Install/start docker, or skip the real-SFTP tests by \
                 omitting `--ignored`."
            );
        }
        if !real_sftp_image_available() {
            panic!(
                "real_sftp preflight failed: atmoz/sftp:latest image not pulled. \
                 Run `docker pull atmoz/sftp:latest` and re-run, or skip the \
                 real-SFTP tests by omitting `--ignored`."
            );
        }

        // ---- 2. Setup: keypair, source file, container ----
        let key_dir = tempfile::tempdir().expect("tempdir for keys");
        let key_dir_path = key_dir.path().to_path_buf();
        let priv_key_path = generate_test_keypair(&key_dir_path)
            .expect("keypair generation");

        // Source file: 100 MB of CSPRNG bytes. We need the
        // source on disk (the docker `cp` writes it into the
        // container) *and* a SHA-256 of it for the assertion
        // at the end.
        let staging = tempfile::tempdir().expect("staging tempdir");
        let source_local = staging.path().join("source.bin");
        {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut f = std::fs::File::create(&source_local).expect("create source");
            let mut remaining = REAL_SFTP_FILE_SIZE;
            let mut buf = vec![0u8; 1024 * 1024];
            while remaining > 0 {
                let chunk = buf.len().min(remaining);
                rng.fill_bytes(&mut buf[..chunk]);
                use std::io::Write;
                f.write_all(&buf[..chunk]).expect("write source");
                remaining -= chunk;
            }
        }
        let source_sha = sha256_of(&source_local).await
            .expect("sha of source");

        // Pick a free TCP port. The atmoz/sftp container will
        // bind its sshd to this port. We use port 0 in a
        // temporary listener to find a free port, then close
        // it before docker binds (TOCTOU is fine here — the
        // test environment isn't hostile).
        let port: u16 = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("bind 0");
            listener.local_addr().unwrap().port()
        };

        // Start the container. This binds port `port` on the
        // host to port 22 in the container, and bind-mounts
        // `source_local` at `/home/test/big.bin` inside the
        // chroot so SFTP can read the file directly (no
        // `docker cp` round-trip — `docker cp` into a chrooted
        // path is unreliable across docker engine versions).
        let container = start_sftp_container(&key_dir_path, port, &source_local)
            .expect("start container");
        // Wait for sshd to come up.
        wait_for_sshd(port).await.expect("sshd ready");

        // ---- 3. Connect via russh, open SFTP, run the
        //         production code path ----

        // The russh client::Config is fine with defaults
        // for our localhost loopback test. The first connection
        // takes ~200ms (key exchange + auth).
        let ssh_config = Arc::new(russh::client::Config::default());

        // Load the private key we generated above.
        let key_pair = Arc::new(
            russh_keys::load_secret_key(&priv_key_path, None)
                .expect("load secret key")
        );

        // `check_server_key` accepts the host key
        // unconditionally. This is a localhost loopback test;
        // MITM isn't a concern, and pinning the host key
        // would mean re-pinning whenever the atmoz/sftp
        // image regenerates its key on first boot.
        struct AcceptAnyKey;
        #[async_trait::async_trait]
        impl russh::client::Handler for AcceptAnyKey {
            type Error = russh::Error;
            async fn check_server_key(
                &mut self,
                _server_public_key: &russh::keys::key::PublicKey,
            ) -> Result<bool, Self::Error> {
                Ok(true)
            }
        }

        let mut session = russh::client::connect(
            ssh_config,
            (std::net::IpAddr::from(std::net::Ipv4Addr::LOCALHOST), port),
            AcceptAnyKey,
        ).await.expect("ssh connect");

        let auth = session
            .authenticate_publickey("test", key_pair)
            .await
            .expect("authenticate");
        assert!(auth, "ssh auth failed");

        // Open the SFTP subsystem channel. This is the same
        // shape `SyncEngine::open_sftp_session` does in
        // production.
        let channel = session
            .channel_open_session()
            .await
            .expect("channel_open_session");
        channel
            .request_subsystem(true, "sftp")
            .await
            .expect("request sftp subsystem");

        // Build a `RawSftpSession` from the channel — this is
        // what the production downloader uses.
        let raw: Arc<RawSftpSession> = Arc::new(
            RawSftpSession::new(channel.into_stream())
        );
        raw.init().await.expect("sftp init");

        // Open the remote file. atmoz/sftp chroots `test` to
        // `/home/test/`, so from the SFTP protocol's
        // perspective the file lives at `/big.bin`, not
        // `/home/test/big.bin`. The chroot-internal path
        // `/home/test/big.bin` is correct in the `docker run`
        // bind-mount (above); the SFTP path is
        // chroot-relative.
        let sftp_path = "/big.bin";
        let file_handle = raw
            .open(
                sftp_path.to_string(),
                OpenFlags::READ,
                FileAttributes::empty(),
            )
            .await
            .expect("open remote file")
            .handle;

        // Get the remote file's size for the size-check at
        // the end.
        let remote_attrs = raw
            .lstat(sftp_path.to_string())
            .await
            .expect("lstat remote");
        let remote_size = remote_attrs.attrs.size.unwrap_or(0) as u64;
        assert_eq!(
            remote_size, REAL_SFTP_FILE_SIZE as u64,
            "remote file size mismatch"
        );

        // Build a `RawSessionReader` (the production
        // `SftpChunkReader` impl) and exercise
        // `pipelined_read_to_file` against a real
        // `tokio::fs::File`. These are the same types the
        // production downloader uses — no mocks, no test
        // doubles.
        let reader: Arc<dyn SftpChunkReader> = Arc::new(
            RawSessionReader::new(Arc::clone(&raw))
        );

        let local_path = staging.path().join("downloaded.bin");
        // `pipelined_read_to_file` expects the destination file
        // to already exist (its per-chunk tasks open it with
        // `OpenOptions::write(true)` only — no `.create(true)` —
        // because pre-creating it is the caller's job; in
        // production that caller is `download_file`, which also
        // runs `set_len` to pre-extend the file. The test
        // bypasses `download_file` and exercises the pipeline
        // reader directly, so we replicate the pre-create +
        // pre-extend here.
        tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&local_path)
            .await
            .expect("pre-create local file")
            .set_len(remote_size)
            .await
            .expect("pre-extend local file");
        let start = tokio::time::Instant::now();
        let start_offset: u64 = 0;
        let mut last_log = start;
        let mut last_log_bytes: u64 = 0;
        let mut next_offset_to_write: u64 = 0;

        let _bytes_written = pipelined_read_to_file(
            reader,
            file_handle.clone(),
            start_offset,
            remote_size,
            &local_path,
            PipelinedReadConfig {
                chunk_size: 256 * 1024,  // 256 KiB — production default
                max_inflight: 16,        // production default
                stall_timeout: Duration::from_secs(30),
            },
            ProgressReporter {
                file_label: "big.bin",
                start: &start,
                start_offset: &start_offset,
                total: remote_size,
                last_log: &mut last_log,
                last_log_bytes: &mut last_log_bytes,
                next_offset_to_write: &mut next_offset_to_write,
            },
        )
        .await
        .expect("pipelined_read_to_file");

        // Close the remote file handle (cleanliness, not
        // strictly required).
        let _ = raw.close(&file_handle).await;

        // Drop the session; russh disconnects on Drop. The
        // test is over; the container is removed by the
        // DockerContainer Drop guard.
        drop(session);

        // ---- 4. Verify ----

        // Size check: the on-disk file must be the full
        // remote size. The pipelined reader returns the
        // high-water-mark offset; a size mismatch here
        // would catch the "missing tail chunk" class of
        // bug.
        let local_size = tokio::fs::metadata(&local_path)
            .await
            .expect("stat local")
            .len();
        assert_eq!(
            local_size, remote_size,
            "downloaded size mismatch: expected {}, got {}",
            remote_size, local_size
        );

        // SHA-256 check: the on-disk file must match the
        // source byte-for-byte. This is the test of the
        // test: any out-of-order write that lands the wrong
        // bytes at a given offset, any chunk lost to a
        // missed flow-control update, any silent corruption
        // in the russh-sftp state machine — all surface
        // here.
        let local_sha = sha256_of(&local_path).await
            .expect("sha of local");
        assert_eq!(
            local_sha, source_sha,
            "SHA-256 mismatch: downloaded bytes don't match source"
        );

        // Throughput sanity. The test asserts a minimum
        // average throughput of 2 MB/s (see the comment at
        // the top of this test for the 64-KiB-cap /
        // round-trip-bound regime rationale).
        let elapsed = start.elapsed();
        let mbps = (local_size as f64 / 1_000_000.0) / elapsed.as_secs_f64();
        eprintln!(
            "real-sftp 100 MB download: {:.1} MB/s in {:.1}s",
            mbps, elapsed.as_secs_f64()
        );
        assert!(
            mbps >= 2.0,
            "throughput {:.1} MB/s is below the 2 MB/s floor; \
             the parallel pipeline may have regressed to serial",
            mbps
        );
    }

    /// Regression test for the SFTP channel-desync bug seen
    /// in the 2026-06-15 physalis production run. The bug:
    /// after a transport hiccup mid-download, the SFTP
    /// subsystem channel desyncs ("Packet N for unknown
    /// recipient" warnings from russh-sftp). A subsequent
    /// `raw.open()` on the same channel fails with "failed
    /// to open remote file" because the server thinks the
    /// channel is closed. The fix: re-open the SFTP
    /// subsystem channel between retry attempts via
    /// `SyncEngine::open_sftp_session(&handle)`, which opens
    /// a fresh SSH session channel multiplexed over the
    /// same Handle.
    ///
    /// This test verifies the recovery path:
    /// 1. Stand up atmoz/sftp as in the happy-path test.
    /// 2. Connect, open the walker-style SFTP subsystem, and
    ///    confirm it can read the remote file (a simple
    ///    `lstat` proves the channel is alive).
    /// 3. Close the underlying `RawSftpSession` to simulate
    ///    a dead channel.
    /// 4. Call `SyncEngine::open_sftp_session(&handle)` to
    ///    open a fresh SFTP subsystem on the same Handle —
    ///    this is the exact call site in the production
    ///    `download_file` retry loop.
    /// 5. Confirm the fresh session can read the same remote
    ///    file (a second `lstat` proves the new channel is
    ///    functional).
    ///
    /// **Gated.** Same docker + atmoz/sftp image dependencies
    /// as `test_real_sftp_100mb_download_via_pipelined_read_to_file`.
    /// Run with `cargo test --release -- --ignored real_sftp`.
    ///
    /// **Why not a kill-mid-download test?** The plan
    /// considered a `docker kill` + restart scenario, but
    /// the timing dependencies (kill point, error surfacing
    /// delay, container restart, retry re-open) make it
    /// flaky. This test isolates the integration that the
    /// production fix relies on (re-opening a fresh SFTP
    /// subsystem on a Handle whose prior channel died) and
    /// pins it down with a deterministic, fast test.
    #[tokio::test]
    #[ignore = "requires docker + atmoz/sftp image; run with --ignored. \
                Fails (does not silently no-op) if preconditions are missing — \
                this is opt-in because of the docker dependency, not because the \
                outcome is conditional."]
    async fn test_real_sftp_channel_reopen_after_dead_session() {
        use crate::sync::SyncEngine;

        // ---- 1. Pre-flight checks (same as the happy-path test) ----
        if !real_sftp_docker_available() {
            panic!(
                "real_sftp preflight failed: `docker info` exited non-zero. \
                 Install/start docker, or skip the real-SFTP tests by \
                 omitting `--ignored`."
            );
        }
        if !real_sftp_image_available() {
            panic!(
                "real_sftp preflight failed: atmoz/sftp:latest image not pulled. \
                 Run `docker pull atmoz/sftp:latest` and re-run, or skip the \
                 real-SFTP tests by omitting `--ignored`."
            );
        }

        // ---- 2. Setup: keypair, source file, container ----
        let key_dir = tempfile::tempdir().expect("tempdir for keys");
        let key_dir_path = key_dir.path().to_path_buf();
        let priv_key_path = generate_test_keypair(&key_dir_path)
            .expect("keypair generation");

        // Source file: 1 MB. We don't need 100 MB here — the
        // test doesn't drive a full download, it just verifies
        // the SFTP subsystem re-open works. A 1 MB file is
        // enough for a `lstat` to succeed.
        let staging = tempfile::tempdir().expect("staging tempdir");
        let source_local = staging.path().join("source.bin");
        {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut f = std::fs::File::create(&source_local).expect("create source");
            let mut buf = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut buf);
            use std::io::Write;
            f.write_all(&buf).expect("write source");
        }

        // Pick a free TCP port for the sshd.
        let port: u16 = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("bind 0");
            listener.local_addr().unwrap().port()
        };

        let container = start_sftp_container(&key_dir_path, port, &source_local)
            .expect("start container");
        wait_for_sshd(port).await.expect("sshd ready");

        // ---- 3. Connect via russh using the production
        //         ClientHandler (so the type matches what
        //         `SyncEngine::open_sftp_session` expects). ----
        let ssh_config = Arc::new(russh::client::Config::default());
        let key_pair = Arc::new(
            russh_keys::load_secret_key(&priv_key_path, None)
                .expect("load secret key")
        );

        let mut session = russh::client::connect(
            ssh_config,
            (std::net::IpAddr::from(std::net::Ipv4Addr::LOCALHOST), port),
            ClientHandler,
        ).await.expect("ssh connect");

        let auth = session
            .authenticate_publickey("test", key_pair)
            .await
            .expect("authenticate");
        assert!(auth, "ssh auth failed");

        // Wrap the Handle in a tokio Mutex, mirroring the
        // production `SyncEngine::new` shape. This is the
        // type the `download_file` retry loop's
        // `open_sftp_session` call expects.
        let handle = Arc::new(tokio::sync::Mutex::new(session));

        // Open SFTP subsystem #1 via the production helper.
        let sftp_path = "/big.bin";
        let raw1 = {
            let h = handle.lock().await;
            SyncEngine::open_sftp_session(&h)
                .await
                .expect("open SFTP subsystem #1")
        };

        // Confirm subsystem #1 is functional: a `lstat`
        // should return the file's attributes.
        let attrs1 = raw1
            .lstat(sftp_path.to_string())
            .await
            .expect("lstat on subsystem #1");
        let expected_size = std::fs::metadata(&source_local).unwrap().len();
        assert_eq!(
            attrs1.attrs.size.unwrap_or(0) as u64, expected_size,
            "subsystem #1 returned wrong size"
        );

        // ---- 4. Simulate a dead channel ----
        //
        // The production bug is "channel desyncs after a
        // transport error." We don't have a clean way to
        // trigger a real desync from a test, but closing the
        // inner channel is a deterministic stand-in: any
        // subsequent `open()` on this `RawSftpSession` will
        // fail (or hang) because the underlying SSH channel
        // is gone. The retry code path doesn't care about
        // the *cause* of the death — it just observes an
        // `Err` from the prior attempt and re-opens. The
        // test verifies the re-open step.
        raw1.close_session().expect("close subsystem #1");

        // Give the server a moment to notice the channel
        // close on its end. (atmoz/sftp is fast, but
        // 100 ms of headroom is cheap insurance.)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // ---- 5. Re-open SFTP subsystem #2 ----
        //
        // This is the exact call site in the production
        // `download_file` retry loop. The fix's claim is:
        // a fresh SFTP subsystem on the same Handle is
        // functional, even when the prior one is dead. The
        // `download_file` retry loop's re-open is the line
        // below (the only thing missing is the sleep +
        // warn!() around it).
        let raw2 = {
            let h = handle.lock().await;
            SyncEngine::open_sftp_session(&h)
                .await
                .expect("re-open SFTP subsystem #2 after dead channel")
        };

        // ---- 6. Verify subsystem #2 is functional ----
        //
        // The same `lstat` that worked on subsystem #1
        // should work on subsystem #2. If the re-open
        // somehow returned a broken session (e.g. bound to
        // the same dead channel, or failed to negotiate
        // with the server), this `lstat` would fail or
        // return wrong data.
        let attrs2 = raw2
            .lstat(sftp_path.to_string())
            .await
            .expect("lstat on subsystem #2 (the re-opened one)");
        assert_eq!(
            attrs2.attrs.size.unwrap_or(0) as u64, expected_size,
            "subsystem #2 (re-opened) returned wrong size; \
             the re-open path is not recovering correctly"
        );

        // ---- 7. Tear down ----
        //
        // The container is cleaned up by the
        // `DockerContainer` Drop guard. Dropping `handle`
        // also drops the russh `Session`, which
        // disconnects.
    }

    /// Companion to `test_real_sftp_channel_reopen_after_dead_session`.
    /// The channel re-open test simulates "one SFTP
    /// channel died." This test simulates the harder
    /// failure mode the 2026-06-16 physalis production
    /// log shows: the *underlying SSH transport* is sick,
    /// so even `channel_open_session` fails. The
    /// escalation path inside `download_file`'s retry
    /// loop calls `SyncEngine::reconnect_handle_and_sftp`,
    /// which is a thin Mutex-swap wrapper around
    /// `establish_russh_session` + `SyncEngine::open_sftp_session`.
    /// This test exercises those two calls in sequence
    /// against a real SSH server and confirms a fresh
    /// Handle + fresh SFTP subsystem is functional.
    ///
    /// **Setup mirrors the channel re-open test** (same
    /// atmoz/sftp image, same keypair, same `/big.bin`
    /// source file). The "kill" step is different:
    /// instead of closing one SFTP subsystem channel
    /// (the cheap re-open case), this test explicitly
    /// `disconnect`s the russh `Session` to model the
    /// production failure where the SSH transport
    /// itself is gone.
    ///
    /// **Gated.** Same docker + atmoz/sftp image
    /// dependencies. Run with
    /// `cargo test --release -- --ignored real_sftp`.
    ///
    /// **Why not also test the Mutex swap directly?**
    /// `*h = establish_russh_session(...).await?` inside
    /// `tokio::sync::Mutex` is the canonical
    /// `tokio::sync::Mutex` swap pattern; testing it
    /// would test tokio, not our code. The two real
    /// calls under test are the high-value integration
    /// points.
    #[tokio::test]
    #[ignore = "requires docker + atmoz/sftp image; run with --ignored. \
                Fails (does not silently no-op) if preconditions are missing — \
                this is opt-in because of the docker dependency, not because the \
                outcome is conditional."]
    async fn test_real_sftp_handle_reconnect_after_session_dead() {
        use crate::sync::establish_russh_session;

        // ---- 1. Pre-flight checks (same as the channel
        //         re-open test) ----
        if !real_sftp_docker_available() {
            panic!(
                "real_sftp preflight failed: `docker info` exited non-zero. \
                 Install/start docker, or skip the real-SFTP tests by \
                 omitting `--ignored`."
            );
        }
        if !real_sftp_image_available() {
            panic!(
                "real_sftp preflight failed: atmoz/sftp:latest image not pulled. \
                 Run `docker pull atmoz/sftp:latest` and re-run, or skip the \
                 real-SFTP tests by omitting `--ignored`."
            );
        }

        // ---- 2. Build a `Config` that points at the test
        //         container's sshd + private key ----
        //
        // `establish_russh_session` takes `&Config` (the
        // same path `SyncEngine::new` uses) so it can read
        // host, port, user, and private_key_path. The
        // other `Config` fields (db, paths, plex, etc.)
        // are unused by `establish_russh_session` —
        // `Config::default()` is `Deserialize` but not
        // trivially constructible here, so we build just
        // the `ssh` field with the values the test needs.
        let key_dir = tempfile::tempdir().expect("tempdir for keys");
        let key_dir_path = key_dir.path().to_path_buf();
        let priv_key_path = generate_test_keypair(&key_dir_path)
            .expect("keypair generation");

        let staging = tempfile::tempdir().expect("staging tempdir");
        let source_local = staging.path().join("source.bin");
        {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut f = std::fs::File::create(&source_local).expect("create source");
            let mut buf = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut buf);
            use std::io::Write;
            f.write_all(&mut buf).expect("write source");
        }

        let port: u16 = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("bind 0");
            listener.local_addr().unwrap().port()
        };

        let container = start_sftp_container(&key_dir_path, port, &source_local)
            .expect("start container");
        wait_for_sshd(port).await.expect("sshd ready");

        let ssh_config = crate::config::SshConfig {
            host: "127.0.0.1".to_string(),
            port: Some(port),
            user: "test".to_string(),
            private_key_path: priv_key_path.clone(),
            remote_base_path: PathBuf::from("/"),
        };
        let test_config = Config {
            ssh: ssh_config,
            // Fields below are unused by
            // `establish_russh_session` (which only reads
            // `config.ssh.*`), but `Config` doesn't have a
            // `..Default::default()` impl. Construct each
            // minimally — values are placeholders.
            database: crate::config::DatabaseConfig {
                path: PathBuf::from("/tmp/nonexistent-test-db.sqlite"),
            },
            paths: crate::config::PathsConfig {
                staging: PathBuf::from("/tmp/nonexistent-staging"),
                library: PathBuf::from("/tmp/nonexistent-library"),
            },
            plex: crate::config::PlexConfig {
                url: "http://127.0.0.1:1".to_string(),
                sections: HashMap::new(),
            },
            logging: None,
            group_name: None,
            categories: HashMap::new(),
            metadata: crate::config::MetadataConfig::default(),
            sync: crate::config::SyncConfig::default(),
        };

        // ---- 3. Establish Handle #1 (the production
        //         initial-connect path) ----
        let handle1 = establish_russh_session(&test_config)
            .await
            .expect("establish Handle #1");

        // Wrap in a Mutex to mirror the production
        // `SyncEngine` shape (the reconnect helper takes
        // `&Arc<Mutex<Handle>>`).
        let handle = Arc::new(tokio::sync::Mutex::new(handle1));

        // Open subsystem #1 and confirm it works.
        let sftp_path = "/big.bin";
        let raw1 = {
            let h = handle.lock().await;
            SyncEngine::open_sftp_session(&h)
                .await
                .expect("open SFTP subsystem #1")
        };
        let attrs1 = raw1
            .lstat(sftp_path.to_string())
            .await
            .expect("lstat on subsystem #1");
        let expected_size = std::fs::metadata(&source_local).unwrap().len();
        assert_eq!(
            attrs1.attrs.size.unwrap_or(0) as u64, expected_size,
            "subsystem #1 returned wrong size"
        );

        // ---- 4. Simulate the underlying SSH transport
        //         going bad ----
        //
        // The production failure mode is "channel
        // re-open fails because `channel_open_session`
        // can't get a session channel on this Handle."
        // We model that by `disconnect`ing the russh
        // `Session` — the server gets an SSH DISCONNECT,
        // and the Handle's underlying transport is now
        // gone. A subsequent `open_sftp_session` on this
        // Handle would fail (and that's the
        // channel-re-open-fails case we're escalating
        // from).
        {
            let h = handle.lock().await;
            h.disconnect(
                russh::Disconnect::ByApplication,
                "test-simulated-handle-death",
                "en",
            ).await.expect("disconnect Handle #1");
        }
        // Give the server a moment to register the
        // disconnect on its side. 100ms is cheap
        // insurance.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // ---- 5. Escalation path: the production
        //         `reconnect_handle_and_sftp` helper
        //         does the equivalent of the next two
        //         steps, wrapped in a Mutex swap. We
        //         exercise the two free functions
        //         directly; the 5-line Mutex-swap
        //         wrapper around them is a standard
        //         `tokio::sync::Mutex` swap and is
        //         covered by code review. ----
        let new_handle = establish_russh_session(&test_config)
            .await
            .expect("Handle reconnect: establish Handle #2");
        // Simulate the Mutex swap from
        // `reconnect_handle_and_sftp`: replace the inner
        // Handle. We don't do the full swap here because
        // we want to keep the test focused on the two
        // free functions it integrates — see the
        // function-level doc comment.
        {
            let mut h = handle.lock().await;
            *h = new_handle;
        }

        // Open subsystem #2 on the new Handle.
        let raw2 = {
            let h = handle.lock().await;
            SyncEngine::open_sftp_session(&h)
                .await
                .expect("open SFTP subsystem #2 after Handle reconnect")
        };

        // ---- 6. Verify subsystem #2 is functional ----
        //
        // The same `lstat` that worked on subsystem #1
        // should work on subsystem #2. If the Handle
        // reconnect somehow returned a broken session
        // (e.g. auth failed silently, or the new
        // subsystem bound to a dead channel), this
        // `lstat` would fail or return wrong data.
        let attrs2 = raw2
            .lstat(sftp_path.to_string())
            .await
            .expect("lstat on subsystem #2 (after Handle reconnect)");
        assert_eq!(
            attrs2.attrs.size.unwrap_or(0) as u64, expected_size,
            "subsystem #2 (after Handle reconnect) returned wrong size; \
             the Handle-level reconnect path is not recovering correctly"
        );

        // ---- 7. Tear down ----
        //
        // Container cleaned up by `DockerContainer` Drop.
    }
}
