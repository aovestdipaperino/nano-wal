use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::segment::Segment;
use crate::{WalError, Result};

pub struct WalOptions {
    pub retention: Duration,
    pub segment_duration: Duration,
}

pub struct Wal {
    dir: PathBuf,
    prefix: String,
    #[allow(dead_code)]
    options: WalOptions,
    active_segment: ArcSwap<Option<Arc<Segment>>>,
    shutdown: AtomicBool,
    segment_duration_ms: i64,
    retention_ms: i64,
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wal")
            .field("dir", &self.dir)
            .field("prefix", &self.prefix)
            .field("shutdown", &self.shutdown.load(Ordering::Relaxed))
            .finish()
    }
}

/// Calculate the expiration timestamp for a segment given an ingestion time.
///
/// The window start is the largest multiple of `segment_duration_ms` that is
/// <= `ingestion_time`.  Expiration = window_start + segment_duration_ms + retention_ms.
pub fn calculate_expiration(ingestion_time: i64, segment_duration_ms: i64, retention_ms: i64) -> i64 {
    let window_start = ingestion_time - (ingestion_time % segment_duration_ms);
    window_start + segment_duration_ms + retention_ms
}

impl Wal {
    /// Create a new WAL instance.
    ///
    /// Validates the options, creates the directory if needed, and recovers
    /// any existing non-expired segment that matches the prefix.
    pub fn new(dir: &Path, prefix: &str, options: WalOptions) -> Result<Self> {
        // Validate options
        if options.segment_duration.is_zero() {
            return Err(WalError::InvalidConfig(
                "segment_duration must be greater than zero".into(),
            ));
        }
        if options.retention < options.segment_duration {
            return Err(WalError::InvalidConfig(
                "retention must be >= segment_duration".into(),
            ));
        }

        // Ensure directory exists
        fs::create_dir_all(dir)?;

        let segment_duration_ms = options.segment_duration.as_millis() as i64;
        let retention_ms = options.retention.as_millis() as i64;

        let wal = Wal {
            dir: dir.to_path_buf(),
            prefix: prefix.to_string(),
            options,
            active_segment: ArcSwap::from_pointee(None),
            shutdown: AtomicBool::new(false),
            segment_duration_ms,
            retention_ms,
        };

        // Attempt recovery of existing segments
        if let Some(seg) = wal.recover()? {
            wal.active_segment.store(Arc::new(Some(seg)));
        }

        Ok(wal)
    }

    /// Return (or create) the active segment for the given ingestion time.
    ///
    /// Uses a CAS loop: load current segment, check if its expiration matches,
    /// and if not, create a new segment and atomically swap it in. On CAS failure
    /// (another thread won the race), delete the orphan file and retry.
    pub fn ensure_segment(&self, ingestion_time: i64) -> Result<Arc<Segment>> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(WalError::Shutdown);
        }

        let needed_expiration = calculate_expiration(
            ingestion_time,
            self.segment_duration_ms,
            self.retention_ms,
        );

        loop {
            let current = self.active_segment.load();

            // If there's an active segment with the right expiration, reuse it.
            if let Some(ref seg) = **current {
                if seg.expiration_ms() == needed_expiration {
                    return Ok(Arc::clone(seg));
                }
            }

            // Need a new segment — create one.
            let path = self.segment_path(needed_expiration);
            let new_seg = Arc::new(Segment::create(&path, needed_expiration)?);
            let new_val = Arc::new(Some(Arc::clone(&new_seg)));

            // CAS: try to swap in our new segment.
            let prev = self.active_segment.compare_and_swap(&current, new_val);

            if Arc::ptr_eq(&prev, &current) {
                // We won the race.
                return Ok(new_seg);
            }

            // We lost the race — another thread installed a segment.
            // Delete our orphan file and retry.
            let _ = fs::remove_file(&path);
        }
    }

    /// Sync the active segment to disk.
    pub fn sync(&self) -> Result<()> {
        let current = self.active_segment.load();
        if let Some(ref seg) = **current {
            let file = seg.file.lock().map_err(|e| {
                WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            file.sync_data()?;
        }
        Ok(())
    }

    /// Shut down the WAL: set the shutdown flag, sync, and clear the active segment.
    pub fn shutdown(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.sync()?;
        self.active_segment.store(Arc::new(None));
        Ok(())
    }

    /// Returns true if the WAL has been shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Accessor for the directory path (used by cleanup).
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Accessor for the prefix (used by cleanup).
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Build the segment file path for a given expiration.
    fn segment_path(&self, expiration_ms: i64) -> PathBuf {
        self.dir.join(format!("{}_{}.seg", self.prefix, expiration_ms))
    }

    /// Parse an expiration timestamp from a segment filename matching this WAL's prefix.
    fn parse_segment_filename(&self, filename: &str) -> Option<i64> {
        let stem = filename.strip_suffix(".seg")?;
        let after_prefix = stem.strip_prefix(&self.prefix)?.strip_prefix('_')?;
        after_prefix.parse::<i64>().ok()
    }

    /// Scan the directory for existing segments matching our prefix.
    /// Returns the latest non-expired segment, if any.
    fn recover(&self) -> Result<Option<Arc<Segment>>> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut best: Option<(i64, PathBuf)> = None;

        for entry in entries {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = match filename.to_str() {
                Some(s) => s,
                None => continue,
            };

            if let Some(expiration) = self.parse_segment_filename(filename_str) {
                // Skip expired segments
                if expiration <= now_ms {
                    continue;
                }

                match &best {
                    Some((best_exp, _)) if expiration <= *best_exp => {}
                    _ => {
                        best = Some((expiration, entry.path()));
                    }
                }
            }
        }

        match best {
            Some((expiration, path)) => {
                let seg = Segment::open(&path, expiration)?;
                Ok(Some(Arc::new(seg)))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::time::Duration;

    fn test_options() -> WalOptions {
        WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }
    }

    #[test]
    fn test_new_creates_wal() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "test-0", test_options()).unwrap();
        assert!(!wal.is_shutdown());
    }

    #[test]
    fn test_invalid_config_zero_duration() {
        let dir = TempDir::new().unwrap();
        let opts = WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::ZERO,
        };
        let err = Wal::new(dir.path(), "test-0", opts).unwrap_err();
        assert!(matches!(err, WalError::InvalidConfig(_)));
    }

    #[test]
    fn test_invalid_config_retention_less_than_duration() {
        let dir = TempDir::new().unwrap();
        let opts = WalOptions {
            retention: Duration::from_secs(60),
            segment_duration: Duration::from_secs(600),
        };
        let err = Wal::new(dir.path(), "test-0", opts).unwrap_err();
        assert!(matches!(err, WalError::InvalidConfig(_)));
    }

    #[test]
    fn test_calculate_segment_expiration() {
        let segment_duration_ms = 600_000i64;
        let retention_ms = 3_600_000i64;

        let ingestion = 1_200_000i64;
        let exp = calculate_expiration(ingestion, segment_duration_ms, retention_ms);
        assert_eq!(exp, 1_200_000 + 600_000 + 3_600_000);

        let ingestion = 1_500_000i64;
        let exp = calculate_expiration(ingestion, segment_duration_ms, retention_ms);
        assert_eq!(exp, 1_200_000 + 600_000 + 3_600_000);
    }

    #[test]
    fn test_ensure_segment_creates_file() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "myprefix", test_options()).unwrap();
        let now_ms = 1_711_234_567_890i64;
        let seg = wal.ensure_segment(now_ms).unwrap();
        assert!(seg.path().exists());
        assert!(seg.path().to_str().unwrap().contains("myprefix_"));
        assert!(seg.path().extension().unwrap() == "seg");
    }

    #[test]
    fn test_ensure_segment_reuses_same_window() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "reuse", test_options()).unwrap();
        let t1 = 1_711_234_567_890i64;
        let t2 = t1 + 1000;
        let seg1 = wal.ensure_segment(t1).unwrap();
        let seg2 = wal.ensure_segment(t2).unwrap();
        assert_eq!(seg1.path(), seg2.path());
        assert_eq!(seg1.expiration_ms(), seg2.expiration_ms());
    }

    #[test]
    fn test_ensure_segment_rotates_on_new_window() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "rotate", test_options()).unwrap();
        let t1 = 1_711_234_567_890i64;
        let seg1 = wal.ensure_segment(t1).unwrap();
        let t2 = t1 + 600_001;
        let seg2 = wal.ensure_segment(t2).unwrap();
        assert_ne!(seg1.expiration_ms(), seg2.expiration_ms());
        assert_ne!(seg1.path(), seg2.path());
    }

    #[test]
    fn test_shutdown_prevents_writes() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "shut", test_options()).unwrap();
        wal.shutdown().unwrap();
        assert!(wal.is_shutdown());
        let err = wal.ensure_segment(1_000_000).unwrap_err();
        assert!(matches!(err, WalError::Shutdown));
    }
}
