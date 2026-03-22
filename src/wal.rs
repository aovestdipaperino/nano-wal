use std::fmt;
use std::fs;
use std::io::Write;
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

    /// Append a single record to the WAL.
    ///
    /// Writes `[NANORC (6)][header_len LE u16 (2)][header bytes][content_len LE u64 (8)][content bytes]`
    /// using vectored I/O (writev) for a single syscall.  Returns an [`EntryRef`] with the file
    /// offset and total byte size of the written record.
    ///
    /// If `durable` is true, an `fdatasync` is issued after the write.
    pub fn append(
        &self,
        header: Option<&[u8]>,
        content: &[u8],
        ingestion_time: i64,
        durable: bool,
    ) -> Result<crate::EntryRef> {
        if let Some(h) = header {
            if h.len() > crate::MAX_HEADER_SIZE {
                return Err(WalError::HeaderTooLarge { size: h.len(), max: crate::MAX_HEADER_SIZE });
            }
        }

        let segment = self.ensure_segment(ingestion_time)?;
        let mut file = segment.file.lock().unwrap_or_else(|p| p.into_inner());

        // Use atomic file_size as offset — avoids seek issues with append mode
        let file_offset = segment.file_size();

        let header_bytes = header.unwrap_or(&[]);
        let header_len = (header_bytes.len() as u16).to_le_bytes();
        let content_len = (content.len() as u64).to_le_bytes();

        let iovecs = [
            std::io::IoSlice::new(&crate::NANO_REC_SIGNATURE),
            std::io::IoSlice::new(&header_len),
            std::io::IoSlice::new(header_bytes),
            std::io::IoSlice::new(&content_len),
            std::io::IoSlice::new(content),
        ];

        let byte_size = crate::RECORD_FRAMING_SIZE + header_bytes.len() + content.len();
        let written = file.write_vectored(&iovecs)?;
        if written != byte_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("short writev: expected {}, wrote {}", byte_size, written),
            ).into());
        }

        if durable {
            file.sync_data()?;
        }

        segment.add_file_size(byte_size as u64);

        Ok(crate::EntryRef { file_offset, byte_size })
    }

    /// Append a batch of records to the WAL in a single vectored write.
    ///
    /// All entries are validated, then written as one `writev` syscall to the active
    /// segment. Returns a `Vec<EntryRef>` with one entry per input record.
    ///
    /// If `durable` is true, an `fdatasync` is issued after the batch write.
    pub fn append_batch(
        &self,
        entries: &[crate::WriteEntry],
        ingestion_time: i64,
        durable: bool,
    ) -> Result<Vec<crate::EntryRef>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        for entry in entries {
            if let Some(h) = entry.header {
                if h.len() > crate::MAX_HEADER_SIZE {
                    return Err(WalError::HeaderTooLarge { size: h.len(), max: crate::MAX_HEADER_SIZE });
                }
            }
        }

        let segment = self.ensure_segment(ingestion_time)?;
        let mut file = segment.file.lock().unwrap_or_else(|p| p.into_inner());
        let file_offset_start = segment.file_size();

        // Pre-encode frames
        let mut header_lens: Vec<[u8; 2]> = Vec::with_capacity(entries.len());
        let mut content_lens: Vec<[u8; 8]> = Vec::with_capacity(entries.len());
        let mut byte_sizes: Vec<usize> = Vec::with_capacity(entries.len());

        for entry in entries {
            let hdr = entry.header.unwrap_or(&[]);
            header_lens.push((hdr.len() as u16).to_le_bytes());
            content_lens.push((entry.content.len() as u64).to_le_bytes());
            byte_sizes.push(crate::RECORD_FRAMING_SIZE + hdr.len() + entry.content.len());
        }

        // Build iovecs: always include header slice even when empty
        let mut iovecs: Vec<std::io::IoSlice> = Vec::with_capacity(entries.len() * 5);
        for (i, entry) in entries.iter().enumerate() {
            let hdr = entry.header.unwrap_or(&[]);
            iovecs.push(std::io::IoSlice::new(&crate::NANO_REC_SIGNATURE));
            iovecs.push(std::io::IoSlice::new(&header_lens[i]));
            iovecs.push(std::io::IoSlice::new(hdr));
            iovecs.push(std::io::IoSlice::new(&content_lens[i]));
            iovecs.push(std::io::IoSlice::new(entry.content));
        }

        let total_bytes: usize = byte_sizes.iter().sum();
        let written = file.write_vectored(&iovecs)?;
        if written != total_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("short batch writev: expected {}, wrote {}", total_bytes, written),
            ).into());
        }

        if durable { file.sync_data()?; }

        segment.add_file_size(total_bytes as u64);

        let mut refs = Vec::with_capacity(entries.len());
        let mut offset = file_offset_start;
        for size in byte_sizes {
            refs.push(crate::EntryRef { file_offset: offset, byte_size: size });
            offset += size as u64;
        }

        Ok(refs)
    }

    /// Read a single record at the given offset within a segment.
    pub fn read_at(
        &self,
        segment: &Arc<crate::segment::Segment>,
        file_offset: u64,
        byte_size: usize,
    ) -> Result<crate::read::Record> {
        crate::read::read_single(segment.read_fd(), file_offset, byte_size)
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
    pub(crate) fn parse_segment_filename(&self, filename: &str) -> Option<i64> {
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

    use crate::FILE_HEADER_SIZE;

    #[test]
    fn test_append_single() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "app", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entry = wal.append(None, b"hello", now, false).unwrap();
        assert_eq!(entry.file_offset, FILE_HEADER_SIZE as u64);
        // NANORC(6) + header_len(2) + content_len(8) + "hello"(5) = 21
        assert_eq!(entry.byte_size, 6 + 2 + 8 + 5);
    }

    #[test]
    fn test_append_with_header() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "hdr", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entry = wal.append(Some(b"meta"), b"data", now, false).unwrap();
        // NANORC(6) + header_len(2) + "meta"(4) + content_len(8) + "data"(4) = 24
        assert_eq!(entry.byte_size, 6 + 2 + 4 + 8 + 4);
    }

    #[test]
    fn test_append_header_too_large() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "big", test_options()).unwrap();
        let big_header = vec![0u8; 70000];
        let err = wal.append(Some(&big_header), b"data", 1_000_000, false).unwrap_err();
        assert!(matches!(err, WalError::HeaderTooLarge { .. }));
    }

    #[test]
    fn test_append_batch_multiple() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "batch", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entries = vec![
            crate::WriteEntry { header: None, content: b"one" },
            crate::WriteEntry { header: None, content: b"two" },
            crate::WriteEntry { header: Some(b"m"), content: b"three" },
        ];
        let refs = wal.append_batch(&entries, now, false).unwrap();
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].file_offset, FILE_HEADER_SIZE as u64);
        assert_eq!(refs[1].file_offset, refs[0].file_offset + refs[0].byte_size as u64);
    }

    #[test]
    fn test_append_after_shutdown() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "sd", test_options()).unwrap();
        wal.shutdown().unwrap();
        let err = wal.append(None, b"nope", 1_000_000, false).unwrap_err();
        assert!(matches!(err, WalError::Shutdown));
    }
}
