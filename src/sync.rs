use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use russh::{client, keys::key::PublicKey, ChannelId, Disconnect};
use russh_sftp::client::SftpSession;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, error, info, trace, warn};

use crate::config::Config;
use crate::db::{Database, DirectoryState};

const MAX_CONCURRENT_DOWNLOADS: usize = 4;

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

        // Open SFTP session
        let channel = self.session.channel_open_session().await
            .context("failed to open SSH channel for SFTP")?;
        channel.request_subsystem(true, "sftp").await
            .context("failed to request SFTP subsystem")?;

        let sftp = SftpSession::new(channel.into_stream()).await
            .context("failed to initialize SFTP session")?;

        // List top-level directories
        let remote_dirs = self.list_remote_dirs(&sftp, &remote_base).await
            .with_context(|| format!("failed to list remote dirs in {}", remote_base.display()))?;

        info!(category = %category, count = remote_dirs.len(), "remote directories found");

        // Compute manifest hash for each and upsert to DB
        for dir_name in &remote_dirs {
            let remote_dir = remote_base.join(dir_name);
            let staging_dir = staging_base.join(dir_name);
            let remote_dir_str = remote_dir.to_string_lossy().to_string();
            let staging_dir_str = staging_dir.to_string_lossy().to_string();

            let manifest_hash = match self.compute_manifest_hash(&sftp, &remote_dir).await {
                Ok(hash) => hash,
                Err(e) => {
                    warn!(dir = %dir_name, error = %e, "failed to compute manifest hash, skipping");
                    continue;
                }
            };

            db.upsert_directory(category, &remote_dir_str, &staging_dir_str, &manifest_hash)?;
            trace!(dir = %dir_name, hash = %manifest_hash, "upserted directory");
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
            let sftp = &sftp; // borrow checker: we need to clone sftp or restructure
            // For now, sequential download to avoid SFTP borrow issues
            db.set_directory_state(dir.id, DirectoryState::Syncing)?;

            if let Err(e) = self.download_directory(&sftp, &dir.remote_path, &dir.staging_path, db, dir.id).await {
                db.set_directory_error(dir.id, &format!("download failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "download failed");
            } else {
                db.set_directory_state(dir.id, DirectoryState::Synced)?;
                info!(dir_id = dir.id, "download complete");
            }
        }

        // Close SFTP cleanly
        let _ = sftp.close().await;

        Ok(())
    }

    async fn list_remote_dirs(
        &self,
        sftp: &SftpSession,
        path: &Path,
    ) -> anyhow::Result<Vec<String>> {
        let entries = sftp.read_dir(path.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to read directory {}", path.display()))?;

        let mut dirs = Vec::new();
        for entry in entries {
            if entry.file_type().is_dir() {
                let name = entry.file_name();
                // Skip hidden directories
                if !name.starts_with('.') {
                    dirs.push(name);
                }
            }
        }

        dirs.sort();
        Ok(dirs)
    }

    async fn compute_manifest_hash(
        &self,
        sftp: &SftpSession,
        dir: &Path,
    ) -> anyhow::Result<String> {
        let mut files: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        self.collect_manifest(sftp, dir, "", &mut files).await?;

        let json = serde_json::to_string(&files)
            .context("failed to serialize manifest to JSON")?;

        let hash = Sha256::digest(json.as_bytes());
        Ok(format!("{:x}", hash))
    }

    async fn collect_manifest(
        &self,
        sftp: &SftpSession,
        dir: &Path,
        prefix: &str,
        files: &mut BTreeMap<String, (u64, u64)>,
    ) -> anyhow::Result<()> {
        let entries = sftp.read_dir(dir.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to read directory {}", dir.display()))?;

        for entry in entries {
            let name = entry.file_name();
            if name.starts_with('.') {
                continue;
            }

            let rel_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            if entry.file_type().is_dir() {
                let subdir = dir.join(&name);
                Box::pin(self.collect_manifest(sftp, &subdir, &rel_path, files)).await?;
            } else {
                let meta = entry.metadata();
                let size = meta.size.unwrap_or(0) as u64;
                let mtime = meta.mtime.unwrap_or(0) as u64;
                files.insert(rel_path, (size, mtime));
            }
        }

        Ok(())
    }

    async fn download_directory(
        &self,
        sftp: &SftpSession,
        remote_path: &str,
        local_path: &str,
        db: &Database,
        dir_id: i64,
    ) -> anyhow::Result<()> {
        info!(remote = %remote_path, local = %local_path, "downloading directory");

        let remote = Path::new(remote_path);
        let local = Path::new(local_path);

        // Ensure local directory exists
        fs::create_dir_all(local).await
            .with_context(|| format!("failed to create local directory {}", local.display()))?;

        let entries = sftp.read_dir(remote.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to read remote directory {}", remote.display()))?;

        for entry in entries {
            let name = entry.file_name();
            if name.starts_with('.') {
                continue;
            }

            let remote_item = remote.join(&name);
            let local_item = local.join(&name);

            if entry.file_type().is_dir() {
                Box::pin(self.download_directory(sftp, &remote_item.to_string_lossy(), &local_item.to_string_lossy(), db, dir_id)).await?;
            } else {
                self.download_file(sftp, &remote_item, &local_item).await?;
            }
        }

        Ok(())
    }

    async fn download_file(
        &self,
        sftp: &SftpSession,
        remote: &Path,
        local: &Path,
    ) -> anyhow::Result<()> {
        let remote_str = remote.to_string_lossy();
        trace!(file = %remote_str, "downloading file");

        let remote_meta = sftp.metadata(remote.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to stat remote file {}", remote.display()))?;
        let remote_size = remote_meta.size.unwrap_or(0) as u64;

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

        // Open remote file
        let mut remote_file = sftp.open(remote.to_string_lossy().into_owned()).await
            .with_context(|| format!("failed to open remote file {}", remote.display()))?;

        // Seek if resuming
        let mut offset = local_size;
        if offset > 0 {
            remote_file.seek(std::io::SeekFrom::Start(offset)).await
                .with_context(|| format!("failed to seek remote file {}", remote.display()))?;
            info!(file = %remote_str, offset, "resuming download");
        }

        // Open local file for append
        let mut local_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(local)
            .await
            .with_context(|| format!("failed to open local file {}", local.display()))?;

        // Stream data
        let mut buffer = vec![0u8; 65536];
        loop {
            let n = remote_file.read(&mut buffer).await
                .with_context(|| format!("failed to read from remote file {}", remote.display()))?;
            if n == 0 {
                break;
            }
            local_file.write_all(&buffer[..n]).await
                .with_context(|| format!("failed to write to local file {}", local.display()))?;
            offset += n as u64;
        }

        local_file.flush().await?;
        drop(local_file);

        // Verify size
        let final_size = fs::metadata(local).await?.len();
        if final_size != remote_size {
            anyhow::bail!(
                "download size mismatch for {}: expected {}, got {}",
                remote.display(),
                remote_size,
                final_size
            );
        }

        debug!(file = %remote_str, size = final_size, "download complete");
        Ok(())
    }
}

impl Drop for SyncEngine {
    fn drop(&mut self) {
        let _ = self.session.disconnect(Disconnect::ByApplication, "pipeline complete", "");
    }
}
