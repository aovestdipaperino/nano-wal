//! A simple Write-Ahead Log (WAL) implementation with per-key segment sets.
//!
//! This crate provides a lightweight, performant WAL implementation designed for
//! append-only operations with automatic segment rotation and expiration.
//!
//! # Features
//!
//! - Per-key segment isolation for better performance
//! - Automatic segment rotation based on retention policies
//! - Optional record headers for metadata storage
//! - Configurable durability guarantees
//! - Batch operations for improved throughput
//!
//! # Examples
//!
//! ```no_run
//! use nano_wal::{Wal, WalOptions};
//! use bytes::Bytes;
//! use std::time::Duration;
//!
//! # fn main() -> Result<(), nano_wal::WalError> {
//! // Create a new WAL with custom retention
//! let options = WalOptions::default()
//!     .retention(Duration::from_secs(3600));
//! let mut wal = Wal::new("./my_wal", options)?;
//!
//! // Append an entry
//! let entry_ref = wal.append_entry(
//!     "user_123",
//!     None,
//!     Bytes::from("event data"),
//!     true
//! )?;
//!
//! // Read the entry back
//! let data = wal.read_entry_at(entry_ref)?;
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fmt::{self, Debug, Display};
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// UTF-8 'NANO-LOG' signature for segment file headers.
///
/// This signature is written at the beginning of each segment file
/// to identify it as a valid nano-wal segment. The value is chosen
/// to be human-readable in hex editors while being unlikely to occur
/// naturally in data files.
const NANO_LOG_SIGNATURE: [u8; 8] = [b'N', b'A', b'N', b'O', b'-', b'L', b'O', b'G'];

/// UTF-8 'NANORC' signature for individual records.
///
/// This signature precedes each record within a segment file,
/// allowing for record boundary detection and corruption recovery.
/// The 6-byte size is chosen to balance overhead with reliability.
const NANO_REC_SIGNATURE: [u8; 6] = [b'N', b'A', b'N', b'O', b'R', b'C'];

/// Maximum size for record headers in bytes (64KB).
///
/// Headers larger than this will be rejected to prevent memory exhaustion
/// and ensure reasonable performance. This size is sufficient for most
/// metadata use cases while preventing abuse.
const MAX_HEADER_SIZE: usize = 65535;

/// Custom error type for WAL operations.
///
/// Provides detailed error information for debugging and error handling.
#[derive(Debug)]
pub enum WalError {
    /// I/O operation failed
    Io(io::Error),
    /// Invalid configuration provided
    InvalidConfig(String),
    /// Entry not found at the specified location
    EntryNotFound(String),
    /// Data corruption detected
    CorruptedData(String),
    /// Header size exceeds maximum allowed
    HeaderTooLarge { size: usize, max: usize },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "I/O error: {}", e),
            WalError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            WalError::EntryNotFound(msg) => write!(f, "Entry not found: {}", msg),
            WalError::CorruptedData(msg) => write!(f, "Data corruption: {}", msg),
            WalError::HeaderTooLarge { size, max } => {
                write!(f, "Header size {} exceeds maximum {}", size, max)
            }
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

/// Custom Result type for WAL operations.
pub type Result<T> = std::result::Result<T, WalError>;

/// Reference to a specific entry location in the WAL.
///
/// An `EntryRef` uniquely identifies an entry's location within the WAL,
/// allowing for efficient random access reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRef {
    /// Hash of the key for segment set identification
    pub key_hash: u64,
    /// Sequence number of the segment file
    pub sequence_number: u64,
    /// Byte offset within the segment file (after header)
    pub offset: u64,
}

/// Configuration options for WAL behavior.
///
/// # Examples
///
/// ```
/// use nano_wal::WalOptions;
/// use std::time::Duration;
///
/// let options = WalOptions::default()
///     .retention(Duration::from_secs(3600))
///     .segments_per_retention_period(5);
/// ```
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// Duration for which entries are retained before expiration
    pub entry_retention: Duration,
    /// Number of segments per retention period for rotation
    pub segments_per_retention_period: u32,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            entry_retention: Duration::from_secs(60 * 60 * 24 * 7), // 1 week
            segments_per_retention_period: 10,
        }
    }
}

impl WalOptions {
    /// Creates options with custom retention duration.
    ///
    /// # Examples
    ///
    /// ```
    /// use nano_wal::WalOptions;
    /// use std::time::Duration;
    ///
    /// let options = WalOptions::with_retention(Duration::from_secs(3600));
    /// ```
    pub fn with_retention(retention: Duration) -> Self {
        Self {
            entry_retention: retention,
            ..Default::default()
        }
    }

    /// Creates options with custom segment count.
    ///
    /// # Examples
    ///
    /// ```
    /// use nano_wal::WalOptions;
    ///
    /// let options = WalOptions::with_segments_per_retention_period(20);
    /// ```
    pub fn with_segments_per_retention_period(segments: u32) -> Self {
        Self {
            segments_per_retention_period: segments,
            ..Default::default()
        }
    }

    /// Sets retention period (chainable).
    pub fn retention(mut self, retention: Duration) -> Self {
        self.entry_retention = retention;
        self
    }

    /// Sets segments per retention period (chainable).
    pub fn segments_per_retention_period(mut self, segments: u32) -> Self {
        self.segments_per_retention_period = segments;
        self
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `WalError::InvalidConfig` if:
    /// - `entry_retention` is zero
    /// - `segments_per_retention_period` is zero
    pub fn validate(&self) -> Result<()> {
        if self.entry_retention.as_secs() == 0 {
            return Err(WalError::InvalidConfig(
                "entry_retention must be greater than 0".to_string(),
            ));
        }
        if self.segments_per_retention_period == 0 {
            return Err(WalError::InvalidConfig(
                "segments_per_retention_period must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Information about an active segment for a specific key.
#[derive(Debug)]
struct ActiveSegment {
    /// Current active file handle
    file: File,
    /// Sequence number of this segment
    sequence_number: u64,
    /// Unix timestamp when this segment expires
    expiration_timestamp: u64,
}

/// Write-Ahead Log with per-key segment sets.
///
/// The `Wal` struct provides the main interface for WAL operations,
/// managing segment files and ensuring durability guarantees.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    options: WalOptions,
    /// Map from key hash to active segment info
    active_segments: HashMap<u64, ActiveSegment>,
    /// Map from key hash to next sequence number
    next_sequence: HashMap<u64, u64>,
}

impl Wal {
    /// Creates a new WAL instance.
    ///
    /// # Arguments
    ///
    /// * `filepath` - Directory path for WAL files
    /// * `options` - Configuration options
    ///
    /// # Errors
    ///
    /// Returns `WalError::InvalidConfig` if options are invalid.
    /// Returns `WalError::Io` if directory creation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use nano_wal::{Wal, WalOptions};
    ///
    /// let wal = Wal::new("./my_wal", WalOptions::default())?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn new(filepath: &str, options: WalOptions) -> Result<Self> {
        options.validate()?;

        let dir = Path::new(filepath);
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }

        let mut wal = Wal {
            dir: dir.to_path_buf(),
            options,
            active_segments: HashMap::new(),
            next_sequence: HashMap::new(),
        };

        wal.scan_existing_files()?;
        Ok(wal)
    }

    /// Scans existing files to determine next sequence numbers.
    fn scan_existing_files(&mut self) -> Result<()> {
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        if let Some((key_hash, sequence)) = self.parse_filename(filename) {
                            let current_max = *self.next_sequence.get(&key_hash).unwrap_or(&0);
                            self.next_sequence
                                .insert(key_hash, current_max.max(sequence + 1));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Parses segment filename to extract key hash and sequence.
    fn parse_filename(&self, filename: &str) -> Option<(u64, u64)> {
        if let Some(name_part) = filename.strip_suffix(".log") {
            let parts: Vec<&str> = name_part.split('-').collect();
            if parts.len() >= 3 {
                let len = parts.len();
                if let (Ok(sequence), Ok(key_hash)) =
                    (parts[len - 1].parse::<u64>(), parts[len - 2].parse::<u64>())
                {
                    return Some((key_hash, sequence));
                }
            }
        }
        None
    }

    /// Generates a filename for a segment.
    fn generate_filename<K: Display>(&self, key: &K, key_hash: u64, sequence: u64) -> String {
        let key_str = format!("{}", key);
        let sanitized_key = key_str
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .take(20)
            .collect::<String>();

        format!("{}-{}-{:04}.log", sanitized_key, key_hash, sequence)
    }

    /// Gets or creates an active segment for the given key.
    fn get_or_create_active_segment<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: &K,
    ) -> Result<u64> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        let now = Utc::now().timestamp() as u64;

        // Check if rotation is needed
        if let Some(active) = self.active_segments.get(&key_hash) {
            if now >= active.expiration_timestamp {
                self.active_segments.remove(&key_hash);
            }
        }

        // Create new segment if needed
        if !self.active_segments.contains_key(&key_hash) {
            let sequence = *self.next_sequence.get(&key_hash).unwrap_or(&1);
            self.next_sequence.insert(key_hash, sequence + 1);

            let segment_duration = self.options.entry_retention.as_secs()
                / self.options.segments_per_retention_period as u64;
            let expiration_timestamp = now + segment_duration;

            let filename = self.generate_filename(key, key_hash, sequence);
            let file_path = self.dir.join(&filename);

            let mut file = OpenOptions::new()
                .create(true)
                
                .append(true)
                .open(&file_path)?;

            self.write_file_header(&mut file, key, expiration_timestamp)?;

            let active_segment = ActiveSegment {
                file,
                sequence_number: sequence,
                expiration_timestamp,
            };

            self.active_segments.insert(key_hash, active_segment);
        }

        Ok(key_hash)
    }

    /// Writes file header for new segment.
    fn write_file_header<K: AsRef<[u8]>>(
        &self,
        file: &mut File,
        key: &K,
        expiration_timestamp: u64,
    ) -> Result<()> {
        file.write_all(&NANO_LOG_SIGNATURE)?;
        file.write_all(&0u64.to_le_bytes())?; // Sequence placeholder
        file.write_all(&expiration_timestamp.to_le_bytes())?;

        let key_bytes = key.as_ref();
        let key_len = key_bytes.len() as u64;
        file.write_all(&key_len.to_le_bytes())?;
        file.write_all(key_bytes)?;

        Ok(())
    }

    /// Appends an entry to the WAL.
    ///
    /// # Arguments
    ///
    /// * `key` - Entry key for segment selection
    /// * `header` - Optional metadata header (max 64KB)
    /// * `content` - Entry content
    /// * `durable` - If true, syncs to disk before returning
    ///
    /// # Errors
    ///
    /// Returns `WalError::HeaderTooLarge` if header exceeds 64KB.
    /// Returns `WalError::Io` for I/O failures.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # use bytes::Bytes;
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// let entry_ref = wal.append_entry(
    ///     "user_123",
    ///     Some(Bytes::from("metadata")),
    ///     Bytes::from("data"),
    ///     true
    /// )?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn append_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        header: Option<Bytes>,
        content: Bytes,
        durable: bool,
    ) -> Result<EntryRef> {
        // Validate header size
        if let Some(ref h) = header {
            if h.len() > MAX_HEADER_SIZE {
                return Err(WalError::HeaderTooLarge {
                    size: h.len(),
                    max: MAX_HEADER_SIZE,
                });
            }
        }

        let key_hash = self.get_or_create_active_segment(&key)?;
        let active_segment = self.active_segments.get_mut(&key_hash).unwrap();

        let current_position = active_segment.file.stream_position()?;
        let file_header_size = 8 + 8 + 8 + 8 + key.as_ref().len() as u64;
        let entry_offset = current_position - file_header_size;

        // Write record
        active_segment.file.write_all(&NANO_REC_SIGNATURE)?;

        let header_len = header.as_ref().map(|h| h.len()).unwrap_or(0);
        active_segment
            .file
            .write_all(&(header_len as u16).to_le_bytes())?;
        if let Some(header_bytes) = &header {
            active_segment.file.write_all(header_bytes.as_ref())?;
        }

        let content_len = content.len() as u64;
        active_segment.file.write_all(&content_len.to_le_bytes())?;
        active_segment.file.write_all(content.as_ref())?;

        if durable {
            active_segment.file.sync_data()?;
        } else {
            active_segment.file.flush()?;
        }

        Ok(EntryRef {
            key_hash,
            sequence_number: active_segment.sequence_number,
            offset: entry_offset,
        })
    }

    /// Appends multiple entries in a batch.
    ///
    /// Batch operations provide better throughput by reducing I/O overhead.
    ///
    /// # Arguments
    ///
    /// * `entries` - Iterator of (key, header, content) tuples
    /// * `durable` - If true, syncs after all entries are written
    ///
    /// # Errors
    ///
    /// Returns first error encountered; partial writes may occur.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # use bytes::Bytes;
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// let entries = vec![
    ///     ("key1", None, Bytes::from("data1")),
    ///     ("key2", Some(Bytes::from("meta")), Bytes::from("data2")),
    /// ];
    /// let refs = wal.append_batch(entries, true)?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn append_batch<K, I>(&mut self, entries: I, durable: bool) -> Result<Vec<EntryRef>>
    where
        K: Hash + AsRef<[u8]> + Display,
        I: IntoIterator<Item = (K, Option<Bytes>, Bytes)>,
    {
        let mut refs = Vec::new();

        for (key, header, content) in entries {
            refs.push(self.append_entry(key, header, content, false)?);
        }

        if durable {
            self.sync()?;
        }

        Ok(refs)
    }

    /// Logs an entry with durability guarantee.
    ///
    /// Convenience method equivalent to `append_entry(key, header, content, true)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # use bytes::Bytes;
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// wal.log_entry("key", None, Bytes::from("data"))?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn log_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        header: Option<Bytes>,
        content: Bytes,
    ) -> Result<EntryRef> {
        self.append_entry(key, header, content, true)
    }

    /// Enumerates all keys in the WAL.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` for filesystem errors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let wal = Wal::new("./wal", WalOptions::default())?;
    /// for key in wal.enumerate_keys()? {
    ///     println!("Found key: {}", key);
    /// }
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn enumerate_keys(&self) -> Result<impl Iterator<Item = String>> {
        let mut keys = std::collections::HashSet::new();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        let segment_path = entry.path();
                        if let Ok(key) = self.read_key_from_file(&segment_path) {
                            keys.insert(key);
                        }
                    }
                }
            }
        }

        Ok(keys.into_iter())
    }

    /// Reads key from segment file header.
    fn read_key_from_file(&self, file_path: &Path) -> Result<String> {
        let mut file = File::open(file_path)?;

        let mut signature_buf = [0u8; 8];
        file.read_exact(&mut signature_buf)?;
        if signature_buf != NANO_LOG_SIGNATURE {
            return Err(WalError::CorruptedData(
                "Invalid NANO-LOG signature".to_string(),
            ));
        }

        file.seek(SeekFrom::Current(16))?; // Skip sequence and expiration

        let mut key_len_bytes = [0u8; 8];
        file.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);

        let mut key_bytes = vec![0u8; key_len as usize];
        file.read_exact(&mut key_bytes)?;

        Ok(String::from_utf8_lossy(&key_bytes).to_string())
    }

    /// Enumerates records for a specific key.
    ///
    /// # Arguments
    ///
    /// * `key` - Key to enumerate records for
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` for filesystem errors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let wal = Wal::new("./wal", WalOptions::default())?;
    /// for record in wal.enumerate_records("my_key")? {
    ///     println!("Record size: {}", record.len());
    /// }
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn enumerate_records<K: Hash + AsRef<[u8]> + Display>(
        &self,
        key: K,
    ) -> Result<impl Iterator<Item = Bytes>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        let mut records = Vec::new();

        let key_str = format!("{}", key);
        let sanitized_key = key_str
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .take(20)
            .collect::<String>();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            let mut segment_files = Vec::new();

            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.starts_with(&format!("{}-{}-", sanitized_key, key_hash))
                        && filename.ends_with(".log")
                    {
                        if let Some((_, sequence)) = self.parse_filename(filename) {
                            segment_files.push((sequence, entry.path()));
                        }
                    }
                }
            }

            segment_files.sort_by_key(|(seq, _)| *seq);

            for (_, file_path) in segment_files {
                if let Ok(file_records) = self.read_records_from_segment(&file_path) {
                    records.extend(file_records);
                }
            }
        }

        Ok(records.into_iter())
    }

    /// Reads all records from a segment file.
    fn read_records_from_segment(&self, file_path: &Path) -> Result<Vec<Bytes>> {
        let mut file = File::open(file_path)?;
        let mut records = Vec::new();

        self.skip_file_header(&mut file)?;

        loop {
            let mut signature_buf = [0u8; 6];
            match file.read_exact(&mut signature_buf) {
                Ok(_) => {
                    if signature_buf != NANO_REC_SIGNATURE {
                        break;
                    }
                }
                Err(_) => break,
            }

            let mut header_len_bytes = [0u8; 2];
            if file.read_exact(&mut header_len_bytes).is_err() {
                break;
            }
            let header_len = u16::from_le_bytes(header_len_bytes);

            if file.seek(SeekFrom::Current(header_len as i64)).is_err() {
                break;
            }

            let mut content_len_bytes = [0u8; 8];
            if file.read_exact(&mut content_len_bytes).is_err() {
                break;
            }
            let content_len = u64::from_le_bytes(content_len_bytes);

            let mut content = vec![0u8; content_len as usize];
            if file.read_exact(&mut content).is_err() {
                break;
            }

            records.push(Bytes::from(content));
        }

        Ok(records)
    }

    /// Skips file header to position at first record.
    fn skip_file_header(&self, file: &mut File) -> Result<()> {
        file.seek(SeekFrom::Current(24))?; // Skip signature, sequence, expiration

        let mut key_len_bytes = [0u8; 8];
        file.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);
        file.seek(SeekFrom::Current(key_len as i64))?;

        Ok(())
    }

    /// Reads entry at specified location.
    ///
    /// # Arguments
    ///
    /// * `entry_ref` - Reference to the entry location
    ///
    /// # Errors
    ///
    /// Returns `WalError::EntryNotFound` if segment doesn't exist.
    /// Returns `WalError::CorruptedData` if signature is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # use bytes::Bytes;
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// # let entry_ref = wal.append_entry("key", None, Bytes::from("data"), true)?;
    /// let data = wal.read_entry_at(entry_ref)?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn read_entry_at(&self, entry_ref: EntryRef) -> Result<Bytes> {
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if let Some((key_hash, sequence)) = self.parse_filename(filename) {
                        if key_hash == entry_ref.key_hash && sequence == entry_ref.sequence_number {
                            let file_path = entry.path();
                            return self.read_entry_from_file(&file_path, entry_ref.offset);
                        }
                    }
                }
            }
        }

        Err(WalError::EntryNotFound(format!(
            "Segment for key_hash {} sequence {} not found",
            entry_ref.key_hash, entry_ref.sequence_number
        )))
    }

    /// Reads specific entry from segment file.
    fn read_entry_from_file(&self, file_path: &Path, offset: u64) -> Result<Bytes> {
        let mut file = File::open(file_path)?;

        self.skip_file_header(&mut file)?;
        file.seek(SeekFrom::Current(offset as i64))?;

        let mut signature_buf = [0u8; 6];
        file.read_exact(&mut signature_buf)?;
        if signature_buf != NANO_REC_SIGNATURE {
            return Err(WalError::CorruptedData(
                "NANORC signature not found".to_string(),
            ));
        }

        let mut header_len_bytes = [0u8; 2];
        file.read_exact(&mut header_len_bytes)?;
        let header_len = u16::from_le_bytes(header_len_bytes);

        file.seek(SeekFrom::Current(header_len as i64))?;

        let mut content_len_bytes = [0u8; 8];
        file.read_exact(&mut content_len_bytes)?;
        let content_len = u64::from_le_bytes(content_len_bytes);

        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content)?;

        Ok(Bytes::from(content))
    }

    /// Removes expired segments from disk.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` for filesystem errors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// wal.compact()?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn compact(&mut self) -> Result<()> {
        let now = Utc::now().timestamp() as u64;

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        let file_path = entry.path();

                        if let Ok(mut file) = File::open(&file_path) {
                            let mut signature = [0u8; 8];
                            if file.read_exact(&mut signature).is_ok()
                                && signature == NANO_LOG_SIGNATURE
                            {
                                let mut sequence_bytes = [0u8; 8];
                                let mut expiration_bytes = [0u8; 8];

                                if file.read_exact(&mut sequence_bytes).is_ok()
                                    && file.read_exact(&mut expiration_bytes).is_ok()
                                {
                                    let expiration_timestamp = u64::from_le_bytes(expiration_bytes);

                                    if now > expiration_timestamp {
                                        let _ = fs::remove_file(&file_path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Syncs all active segments to disk.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if sync fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// wal.sync()?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn sync(&mut self) -> Result<()> {
        for active_segment in self.active_segments.values_mut() {
            active_segment.file.sync_data()?;
        }
        Ok(())
    }

    /// Returns count of active segments.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let wal = Wal::new("./wal", WalOptions::default())?;
    /// println!("Active segments: {}", wal.active_segment_count());
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn active_segment_count(&self) -> usize {
        self.active_segments.len()
    }

    /// Shuts down WAL and removes all storage.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if removal fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use nano_wal::{Wal, WalOptions};
    /// # let mut wal = Wal::new("./wal", WalOptions::default())?;
    /// wal.shutdown()?;
    /// # Ok::<(), nano_wal::WalError>(())
    /// ```
    pub fn shutdown(&mut self) -> Result<()> {
        self.active_segments.clear();
        fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}
