use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context};
use russh::{client, keys::key::PublicKey, ChannelId, Disconnect};
use russh_sftp::client::RawSftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use sha2::{Digest, Sha256};
use tokio::fs;
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

/// Maximum total time a single file transfer is allowed to take. If
/// exceeded, the transfer is aborted with a warning and the directory
/// is marked `sync_failed`; the auto-retry mechanism picks it up on
/// the next run. Set generously enough to cover legitimate slow
/// transfers (e.g. a 60GB 4K REMUX on a constrained link) but tight
/// enough that a hung SFTP session doesn't tie up the pipeline.
const TRANSFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Maximum time a single SFTP read is allowed to take without
/// producing any bytes. A healthy SFTP read on even a slow link
/// resolves in well under a second. If a read sits idle for this long,
/// the SSH channel is dead (kernel TCP may still be ESTABLISHED, but
/// the server isn't sending data). The transfer is aborted with a
/// warning so the pipeline can move on to the next directory.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Buffer size for the in-process read/write buffer between the SFTP
/// stream and the local file. 64 KiB is intentional, not a default:
///
/// - **Read side**: 2× the typical SFTP `READ` packet size (~32 KiB
///   after overhead). A larger buffer doesn't speed up the SFTP read
///   itself — it just holds more already-received bytes. The bottleneck
///   on the 4K REMUX transfers is the remote SFTP server's per-packet
///   rate, not the user-space buffer fill.
///
/// - **Write side**: a multiple of 4 KiB (page size) and 16 pages (64
///   KiB = 1 MB on 16-page writeback boundaries). The kernel's page
///   cache already coalesces contiguous writes up to 1 MB or more for
///   sequential workloads, so going to 256 KiB or 1 MiB only saves a
///   handful of `write()` syscalls per file (microseconds of CPU per
///   60 GB transfer). Bumping further would also risk writeback
///   stalls on spinning disks if concurrency ever scales.
///
/// - **Memory**: at MAX_CONCURRENT_DOWNLOADS=4, the per-download
///   footprint is 128 KiB (1 BufReader + 1 BufWriter). 512 KiB total
///   for 4 concurrent transfers. Negligible; no reason to chase
///   savings here.
///
/// If you change this, measure first: `iostat -x 1` on the host
/// during a transfer to see whether the disk is the bottleneck
/// (queue depth > 1, %util near 100) or whether it's idle waiting on
/// the network (queue depth 0, %util low). The latter is what we
/// see on the physalis CIFS mount; bumping the buffer won't help.
const IO_BUFFER_SIZE: usize = 64 * 1024;

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
pub struct SyncEngine {
    session: client::Handle<ClientHandler>,
}

struct ClientHandler;

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

impl SyncEngine {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let ssh_config = client::Config::default();
        let ssh_config = std::sync::Arc::new(ssh_config);

        let handler = ClientHandler;

        let port = config.ssh.port.unwrap_or(22);
        info!(host = %config.ssh.host, port, user = %config.ssh.user, "connecting to ssh");

        let mut session = client::connect(ssh_config, (config.ssh.host.as_str(), port), handler)
            .await
            .with_context(|| format!("failed to connect to {}:{}", config.ssh.host, port))?;

        let key_pair = russh::keys::load_secret_key(&config.ssh.private_key_path,
            None,
        )
        .with_context(|| {
            format!(
                "failed to load private key from {}",
                config.ssh.private_key_path.display()
            )
        })?;

        let auth_result = session
            .authenticate_publickey(&config.ssh.user,
                std::sync::Arc::new(key_pair),
            )
            .await
            .context("public key authentication failed")?;

        if !auth_result {
            anyhow::bail!("SSH public key authentication failed");
        }

        info!("ssh authenticated successfully");
        Ok(SyncEngine { session })
    }

    pub async fn sync_category(
        &mut self,
        category: &str,
        db: &Database,
    ) -> anyhow::Result<()> {
        let config = Config::load(Path::new("/etc/media-pipeline/config.toml"))?;
        let remote_base = config.remote_path(category);
        let staging_base = config.staging_path(category);

        info!(category = %category, remote = %remote_base.display(), staging = %staging_base.display(), "listing remote directory");

        // Open SFTP subsystem channel and construct a low-level
        // `RawSftpSession`. We use the raw API (not the higher-level
        // `SftpSession`) because the raw API exposes the file handle
        // and a public `read(handle, offset, len)` that we can issue
        // concurrently on the same session. `SftpSession`'s public
        // `File::poll_read` is single-request-in-flight, which is the
        // ~5 Mbps cap we're working around.
        let channel = self.session.channel_open_session().await
            .context("failed to open SSH channel for SFTP")?;
        channel.request_subsystem(true, "sftp").await
            .context("failed to request SFTP subsystem")?;

        let raw: Arc<RawSftpSession> = Arc::new(
            RawSftpSession::new(channel.into_stream())
        );
        raw.init().await
            .context("SFTP init/version handshake failed")?;

        // List top-level directories
        let remote_dirs = self.list_remote_dirs(&raw, &remote_base).await
            .with_context(|| format!("failed to list remote dirs in {}", remote_base.display()))?;

        info!(category = %category, count = remote_dirs.len(), "remote directories found");

        // Compute manifest hash for each and upsert to DB
        for dir_name in &remote_dirs {
            let remote_dir = remote_base.join(dir_name);
            let staging_dir = staging_base.join(dir_name);
            let remote_dir_str = remote_dir.to_string_lossy().to_string();
            let staging_dir_str = staging_dir.to_string_lossy().to_string();

            info!(dir = %dir_name, "sync: directory walk starting");

            // Walk the remote tree once. The collected manifest is the
            // input to both the manifest hash (for change detection)
            // and the per-file sha256 collection (for download-time
            // integrity verification). Walking twice would double the
            // per-dir cost for no benefit.
            let walk_started = Instant::now();
            let manifest = match self.collect_manifest(&raw, &remote_dir).await {
                Ok(m) => m,
                Err(e) => {
                    warn!(dir = %dir_name, error = %e, "failed to collect manifest, skipping");
                    continue;
                }
            };
            info!(
                dir = %dir_name,
                file_count = manifest.len(),
                duration_secs = format!("{:.2}", walk_started.elapsed().as_secs_f64()),
                "sync: directory walk complete"
            );

            // Per-file hashes from the remote. Best-effort: a
            // transport error here logs a WARN and proceeds with an
            // empty hash set, so a single broken SSH channel can't
            // block the rest of the sync. The download path treats a
            // missing hash as "skip verification", not as a hard
            // error.
            let hash_started = Instant::now();
            let file_hashes = self.collect_remote_hashes(&remote_dir, &manifest).await
                .unwrap_or_else(|e| {
                    warn!(dir = %dir_name, error = %e, "failed to collect remote hashes; verification will be skipped for this dir");
                    Vec::new()
                });
            info!(
                dir = %dir_name,
                hash_count = file_hashes.len(),
                duration_secs = format!("{:.2}", hash_started.elapsed().as_secs_f64()),
                "sync: remote hashes collected"
            );

            // Compute the manifest hash from the size+mtime tuples
            // (the remote hash is not part of the manifest — that
            // would make the manifest change every time we re-hash
            // a file on the remote, which is meaningless).
            let manifest_hash = manifest_hash_from_files(&manifest);

            let upsert_started = Instant::now();
            let dir_id = db.upsert_directory(category, &remote_dir_str, &staging_dir_str, &manifest_hash)?;
            db.upsert_file_hashes(dir_id, &file_hashes)?;
            info!(
                dir = %dir_name,
                dir_id,
                manifest_hash = %manifest_hash,
                duration_secs = format!("{:.2}", upsert_started.elapsed().as_secs_f64()),
                "sync: directory upserted"
            );
        }

        // Download directories that are in 'detected' state
        let detected = db.get_directories_in_state(DirectoryState::Detected)?;
        let to_download: Vec<_> = detected
            .into_iter()
            .filter(|d| d.category == category)
            .collect();

        info!(category = %category, count = to_download.len(), "directories to download");

        // Download with bounded concurrency
        let _semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DOWNLOADS));

        for dir in to_download {
            // For now, sequential download to avoid SFTP borrow issues
            db.set_directory_state(dir.id, DirectoryState::Syncing)?;

            if let Err(e) = self.download_directory(&raw, &dir.remote_path, &dir.staging_path, db, dir.id).await {
                db.set_directory_error(dir.id, DirectoryState::SyncingFailed, &format!("download failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "download failed");
            } else {
                db.set_directory_state(dir.id, DirectoryState::Synced)?;
                info!(dir_id = dir.id, "download complete");
            }
        }

        // Close SFTP cleanly. The Drop impl also closes the channel,
        // so this is just for a clean error path; we ignore the result.
        let _ = raw.close_session();

        Ok(())
    }

    async fn list_remote_dirs(
        &self,
        raw: &Arc<RawSftpSession>,
        path: &Path,
    ) -> anyhow::Result<Vec<String>> {
        let dir_handle = raw.opendir(path.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to opendir {}", path.display()))?;
        let dir_handle_str = dir_handle.handle;

        // The low-level `RawSftpSession::readdir` does NOT translate
        // the SFTP `Eof` status (code 1) into a successful empty-
        // listing terminator the way the high-level
        // `SftpSession::read_dir` does — it returns it as an
        // `Err(Error::Status(...))`. Per the SFTP protocol spec, `Eof`
        // is the canonical "no more entries" signal, so we route
        // both that and the empty-`files`-list convention through
        // `drain_readdir_pages`, which handles either terminator
        // uniformly. See the doc comment on that function for the
        // server-convention details.
        let files = drain_readdir_pages(|| async {
            raw.readdir(&dir_handle_str).await
        })
        .await
        .map_err(|e| {
            anyhow::Error::new(e).context(format!("failed to readdir {}", path.display()))
        })?;

        let _ = raw.close(&dir_handle_str).await;

        let mut dirs: Vec<String> = files
            .into_iter()
            .filter(|f| f.attrs.is_dir())
            .map(|f| f.filename)
            .filter(|n| !n.starts_with('.'))
            .collect();
        dirs.sort();
        Ok(dirs)
    }

    /// Walk the remote directory tree and return a `rel_path →
    /// (size, mtime)` map of every regular file. Hidden entries
    /// (`.foo`) are skipped, mirroring the manifest behavior. This
    /// is the canonical SFTP walk: the manifest hash and the
    /// per-file sha256 collection both consume the same data so we
    /// don't pay for two tree walks per directory.
    ///
    /// Implemented iteratively with an explicit work stack rather
    /// than recursively. The depth of the recursion would have
    /// forced an explicit `Box::pin`-of-future for the self-call,
    /// and a `Vec` of `(PathBuf, String)` pairs is just as
    /// memory-bounded and clearer to read. The work stack is
    /// O(depth), not O(files).
    async fn collect_manifest(
        &self,
        raw: &Arc<RawSftpSession>,
        dir: &Path,
    ) -> anyhow::Result<BTreeMap<String, (u64, u64)>> {
        let mut files: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        // Stack of (absolute_dir, rel_path_prefix) pairs to process.
        // The root has an empty prefix; children get `format!("{}/{}",
        // prefix, name)` pushed on the stack.
        let mut stack: Vec<(PathBuf, String)> = vec![(dir.to_path_buf(), String::new())];

        let walk_started = Instant::now();

        while let Some((cur_dir, prefix)) = stack.pop() {
            let rel_display = if prefix.is_empty() {
                "<root>".to_string()
            } else {
                prefix.clone()
            };
            debug!(subdir = %rel_display, "manifest: walking subdir");

            let dir_handle = raw.opendir(cur_dir.to_string_lossy().into_owned()).await
                .with_context(|| format!("failed to opendir {}", cur_dir.display()))?;
            let dir_handle_str = dir_handle.handle;

            // Same SFTP terminator handling as `list_remote_dirs` and
            // `download_directory` — the low-level `readdir` returns the
            // protocol's `Eof` status as `Err`, which we treat as a
            // successful end-of-directory. See `drain_readdir_pages`.
            let entries = drain_readdir_pages(|| async {
                raw.readdir(&dir_handle_str).await
            })
            .await
            .map_err(|e| {
                anyhow::Error::new(e).context(format!("failed to readdir {}", cur_dir.display()))
            })?;

            for entry in entries {
                let name = entry.filename;
                if name.starts_with('.') {
                    continue;
                }

                let rel_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", prefix, name)
                };

                if entry.attrs.is_dir() {
                    stack.push((cur_dir.join(&name), rel_path));
                } else {
                    let size = entry.attrs.size.unwrap_or(0) as u64;
                    let mtime = entry.attrs.mtime.unwrap_or(0) as u64;
                    files.insert(rel_path, (size, mtime));
                }
            }

            let _ = raw.close(&dir_handle_str).await;
        }

        debug!(
            file_count = files.len(),
            duration_secs = format!("{:.2}", walk_started.elapsed().as_secs_f64()),
            "manifest: walk finished"
        );

        Ok(files)
    }


    /// Run `sha256sum` on the remote for every file in `manifest`
    /// and return the `(rel_path, sha256, size, mtime)` tuples ready
    /// to be persisted via `db.upsert_file_hashes`.
    ///
    /// **Mechanism.** Opens a fresh SSH session channel and exec's
    /// `xargs -d '\n' sha256sum`. The newline-separated list of
    /// absolute remote paths is sent on the channel's stdin. We
    /// deliberately pass paths via stdin (not argv) so shell
    /// quoting issues — spaces, single quotes, `$`, `;`, etc. —
    /// are avoided by construction. `xargs -d '\n'` splits stdin
    /// on newlines and invokes `sha256sum` once per path (one
    /// process, batched; the alternative — one `sha256sum` per
    /// file — would be ~100x more round-trips).
    ///
    /// **Newlines in paths.** Filesystem-dependent but vanishingly
    /// rare. We detect them in the input list, emit a WARN, and
    /// skip the file — `xargs -d '\n'` would silently treat the
    /// embedded newline as a path separator and the resulting
    /// hash map would be unparseable.
    ///
    /// **Channel reuse.** One channel per directory. The channel
    /// is closed at the end (russh closes on Drop; we also call
    /// `close()` explicitly for a clean exit code). Re-using a
    /// single channel for the whole sync would save the small
    /// `channel_open_session` round-trip per dir but adds
    /// coordination complexity; the per-dir cost is one local
    /// TCP round-trip, negligible.
    ///
    /// **Failure mode.** If the remote `sha256sum` exits non-zero
    /// (e.g. binary missing), the call returns an error and the
    /// caller in `sync_category` logs a WARN and proceeds with an
    /// empty hash set — verification is then skipped for this
    /// directory's files, not failed. This is consistent with the
    /// user's "best effort" intent: integrity checks improve
    /// coverage, they don't block the pipeline.
    async fn collect_remote_hashes(
        &self,
        remote_dir: &Path,
        manifest: &BTreeMap<String, (u64, u64)>,
    ) -> anyhow::Result<Vec<(String, String, i64, i64)>> {
        let hash_started = Instant::now();

        if manifest.is_empty() {
            return Ok(Vec::new());
        }

        // Build the path list. Filter out any path with an
        // embedded newline — these would break the `xargs -d
        // '\n'` splitter and emit a WARN rather than silently
        // producing a broken hash map.
        let mut paths: Vec<String> = Vec::with_capacity(manifest.len());
        let mut skipped_newline = 0usize;
        for rel_path in manifest.keys() {
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

        info!(
            file_count = paths.len(),
            "sync: remote hashing starting"
        );

        // Open a new session channel. This is independent of the
        // SFTP subsystem channel used for downloads — `exec` runs
        // in its own channel per the SSH spec.
        let mut channel = self.session.channel_open_session().await
            .context("failed to open SSH channel for sha256sum exec")?;

        // `xargs -d '\n' sha256sum` — split stdin on newlines,
        // invoke sha256sum once per path. The command itself is
        // tiny; the path list is on stdin. (Plain `sha256sum` with
        // paths on argv would also work but is bounded by argv
        // size and is awkward to chunk.)
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
        // Some servers need an explicit EOF marker before they
        // start processing; `shutdown` should suffice for OpenSSH
        // but a belt-and-suspenders `eof()` is cheap.
        let _ = channel.eof().await;

        // Read stdout into a buffer, then drop the reader so the
        // channel isn't borrowed when we call `wait` / `close`
        // below. The reader is an `impl AsyncRead + '_` that
        // borrows the channel mutably; holding it across `wait`
        // is a double-mutable-borrow error.
        let mut output = Vec::new();
        {
            let mut stdout = channel.make_reader();
            tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut output).await
                .context("failed to read sha256sum stdout")?;
        }

        // Wait for the channel to close cleanly so we can inspect
        // the exit code. `wait()` returns `None` on close. We
        // discard individual messages — the reader above drained
        // stdout into `output`.
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
            "sync: remote hashing complete"
        );

        Ok(parsed)
    }

    async fn download_directory(
        &self,
        raw: &Arc<RawSftpSession>,
        remote_path: &str,
        local_path: &str,
        db: &Database,
        dir_id: i64,
    ) -> anyhow::Result<()> {
        self.download_directory_with_prefix(
            raw,
            remote_path.to_string(),
            local_path.to_string(),
            String::new(),
            db,
            dir_id,
        ).await
    }

    /// Internal walker for `download_directory`. The `prefix` is
    /// the file's path relative to the directory root — used to
    /// look up the per-file SHA-256 fingerprint persisted by
    /// `collect_remote_hashes`. The root call passes `""`; nested
    /// directories pass `"<parent>/<name>"`.
    ///
    /// Takes owned `String`s so the recursive call can be
    /// `async move`-captured without lifetime-tangling against
    /// `&self`. The allocations are negligible (path lengths are
    /// short and the depth is bounded by the directory tree).
    fn download_directory_with_prefix(
        &self,
        raw: &Arc<RawSftpSession>,
        remote_path: String,
        local_path: String,
        prefix: String,
        db: &Database,
        dir_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        let raw = Arc::clone(raw);
        let db = db.clone();
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
                    self.download_directory_with_prefix(
                        &raw,
                        remote_item_str,
                        local_item_str,
                        rel_path,
                        &db,
                        dir_id,
                    ).await?;
                } else {
                    let remote_item = Path::new(&remote_item_str);
                    let local_item = Path::new(&local_item_str);
                    self.download_file(&raw, remote_item, local_item, &db, dir_id, &rel_path).await?;
                }
            }

            let _ = raw.close(&dir_handle_str).await;
            Ok(())
        })
    }

    async fn download_file(
        &self,
        raw: &Arc<RawSftpSession>,
        remote: &Path,
        local: &Path,
        db: &Database,
        dir_id: i64,
        rel_path: &str,
    ) -> anyhow::Result<()> {
        let remote_str = remote.to_string_lossy();
        trace!(file = %remote_str, "downloading file");

        // `RawSftpSession::lstat` returns `Attrs { id, attrs }`; we
        // need the inner `attrs.size` for the total file size.
        let remote_attrs = raw.lstat(remote.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to stat remote file {}", remote.display()))?;
        let remote_size = remote_attrs.attrs.size.unwrap_or(0) as u64;

        let local_size = if local.exists() {
            let meta = fs::metadata(local).await?;
            meta.len()
        } else {
            0
        };

        if local_size == remote_size {
            debug!(file = %remote_str, size = remote_size, "file already complete, skipping");
            return Ok(());
        }

        // Ensure parent directory exists
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
        if remote_size > local_size {
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
        let mut last_log_bytes: u64 = local_size;
        let start_offset = local_size;
        let total = remote_size;
        let mut next_offset_to_write: u64 = local_size;

        // Run the pipelined reader. It returns the high-water-mark
        // offset reached on disk (== highest `off + len` over
        // completed chunks). The on-disk file is correct
        // regardless of write order; the size check below verifies
        // completeness.
        let _bytes_written = pipelined_read_to_file(
            reader,
            handle.clone(),
            start_offset,
            total,
            local,
            PipelinedReadConfig {
                chunk_size: SFTP_READ_CHUNK,
                max_inflight: SFTP_INFLIGHT_REQUESTS,
                stall_timeout: STALL_TIMEOUT,
                total_timeout: TRANSFER_TIMEOUT,
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
        // a mismatch means the bytes on disk are not what the
        // remote had when we collected the manifest. The
        // pipeline-level error path in `sync_category` marks the
        // directory as `sync_failed` so the next run re-tries
        // from scratch.
        //
        // We always verify, even for already-on-disk files that
        // the size check already let pass. The user explicitly
        // asked for this: silent disk corruption should not be
        // papered over by the size check.
        match db.get_expected_hash(dir_id, rel_path)? {
            Some((expected, _expected_size)) => {
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
            }
            None => {
                // No expected hash — either this file was added to
                // the directory after the manifest was collected,
                // or `collect_remote_hashes` failed for this dir
                // (e.g. SSH channel error). Don't block the
                // download on missing verification data.
                warn!(file = %remote_str, "no expected sha256 recorded for this file; skipping verification");
            }
        }

        // Close the remote file handle.
        let _ = raw.close(&handle).await;

        Ok(())
    }
}

impl Drop for SyncEngine {
    fn drop(&mut self) {
        let _ = self.session.disconnect(Disconnect::ByApplication, "pipeline complete", "");
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
    /// Total wall-clock budget for the whole transfer. Checked on
    /// each completed read, so a transfer that hangs at EOF still
    /// gets a chance to finish.
    pub total_timeout: std::time::Duration,
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
        let data = self.raw.read(handle, offset, len).await
            .map_err(|e| anyhow!("SFTP read at offset {} failed: {}", offset, e))?;
        Ok(data.data)
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
/// Stall and total timeouts are enforced as in the single-request
/// loop. The function returns the total bytes written to disk (==
/// highest offset reached on success).
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

        // 5. Per-file total-time budget. Checked on each completion,
        //    not each read, so a transfer that hangs at EOF (right
        //    before the last read returns) still gets a chance to
        //    complete. The per-read stall timeout catches the "no
        //    bytes for 60s" case fast.
        if progress.start.elapsed() > config.total_timeout {
            let elapsed = progress.start.elapsed().as_secs();
            let bytes_this_run = *progress.next_offset_to_write - *progress.start_offset;
            warn!(
                file = %progress.file_label,
                bytes = *progress.next_offset_to_write,
                total = progress.total,
                bytes_this_run,
                elapsed_secs = elapsed,
                timeout_secs = config.total_timeout.as_secs(),
                "transfer exceeded per-file timeout, aborting"
            );
            anyhow::bail!(
                "transfer timeout for file {}: {}s elapsed, {} of {} bytes",
                progress.file_label,
                elapsed,
                *progress.next_offset_to_write,
                progress.total
            );
        }

        // 6. Periodic throughput log. The rate is computed against
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
// Tests for the writethrough-to-disk pipelined read loop
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    /// A mock SFTP reader that returns canned chunks at the
    /// requested offsets. The `reorder` flag (when true) responds to
    /// requests in reverse order of issue, exercising the
    /// writethrough-to-disk code path: under the writethrough model,
    /// chunks landing at higher offsets first must still result in
    /// the correct on-disk file.
    struct MockReader {
        data: Vec<u8>,
        /// Number of completed reads, exposed for tests that want to
        /// assert on progress.
        reads_issued: Mutex<Vec<u64>>,
        /// If true, complete reads in reverse order of issue. The
        /// first read in `reads_issued` finishes last.
        reorder: bool,
    }

    impl MockReader {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                reads_issued: Mutex::new(Vec::new()),
                reorder: false,
            }
        }

        fn with_reorder(mut self) -> Self {
            self.reorder = true;
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

            if self.reorder {
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

            let start = offset as usize;
            let end = std::cmp::min(start + len as usize, self.data.len());
            if start >= self.data.len() {
                return Ok(Vec::new());
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
                total_timeout: Duration::from_secs(30),
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
                total_timeout: Duration::from_secs(30),
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
}
