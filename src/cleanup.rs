use std::path::PathBuf;
use crate::{Result, Wal};

#[derive(Debug)]
pub struct CleanupResult {
    pub deleted: Vec<DeletedSegment>,
    pub live_count: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Debug)]
pub struct DeletedSegment {
    pub path: PathBuf,
    pub expiration_ms: i64,
    pub file_size: u64,
}

impl Wal {
    pub fn cleanup(&self) -> Result<CleanupResult> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut deleted = Vec::new();
        let mut live_count = 0u64;
        let mut bytes_reclaimed = 0u64;

        let entries = match std::fs::read_dir(self.dir()) {
            Ok(e) => e,
            Err(_) => return Ok(CleanupResult { deleted, live_count, bytes_reclaimed }),
        };

        for entry in entries.flatten() {
            let filename = match entry.file_name().into_string() {
                Ok(f) => f,
                Err(_) => continue,
            };

            let expiration_ms = match self.parse_segment_filename(&filename) {
                Some(exp) => exp,
                None => continue,
            };

            if expiration_ms <= now_ms {
                let file_size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                let path = entry.path();
                if std::fs::remove_file(&path).is_ok() {
                    bytes_reclaimed += file_size;
                    deleted.push(DeletedSegment { path, expiration_ms, file_size });
                } else {
                    live_count += 1;
                }
            } else {
                live_count += 1;
            }
        }

        Ok(CleanupResult { deleted, live_count, bytes_reclaimed })
    }
}

#[cfg(test)]
mod tests {
    use crate::{Wal, WalOptions};
    use tempfile::TempDir;
    use std::time::Duration;

    #[test]
    fn test_cleanup_empty_dir() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "clean", WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }).unwrap();
        let result = wal.cleanup().unwrap();
        assert_eq!(result.deleted.len(), 0);
        assert_eq!(result.live_count, 0);
    }

    #[test]
    fn test_cleanup_deletes_expired() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "exp", WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }).unwrap();

        let expired_path = dir.path().join("exp_1000.seg");
        let mut header = b"NANO-LOG".to_vec();
        header.extend_from_slice(&1000i64.to_le_bytes());
        std::fs::write(&expired_path, &header).unwrap();

        let result = wal.cleanup().unwrap();
        assert_eq!(result.deleted.len(), 1);
        assert_eq!(result.deleted[0].expiration_ms, 1000);
        assert!(!expired_path.exists());
    }

    #[test]
    fn test_cleanup_keeps_live_segments() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "live", WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }).unwrap();

        let live_path = dir.path().join("live_99999999999999.seg");
        std::fs::write(&live_path, b"NANO-LOG\x00\x00\x00\x00\x00\x00\x00\x00").unwrap();

        let result = wal.cleanup().unwrap();
        assert_eq!(result.deleted.len(), 0);
        assert_eq!(result.live_count, 1);
        assert!(live_path.exists());
    }

    #[test]
    fn test_cleanup_ignores_other_prefixes() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "mine", WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }).unwrap();

        let other_path = dir.path().join("other_1000.seg");
        std::fs::write(&other_path, b"NANO-LOG\x00\x00\x00\x00\x00\x00\x00\x00").unwrap();

        let result = wal.cleanup().unwrap();
        assert_eq!(result.deleted.len(), 0);
        assert!(other_path.exists());
    }
}
