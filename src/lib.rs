//! A simple Write-Ahead Log (WAL) implementation with per-key segment sets.
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The UTF-8 'NANO-LOG' signature for segment file headers
const NANO_LOG_SIGNATURE: [u8; 8] = [b'N', b'A', b'N', b'O', b'-', b'L', b'O', b'G'];

/// The UTF-8 'NANO-REC' signature for individual records
const NANO_REC_SIGNATURE: [u8; 8] = [b'N', b'A', b'N', b'O', b'-', b'R', b'E', b'C'];

/// A reference to a specific entry location in the WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRef {
    /// Hash of the key for which segment set this entry belongs to
    pub key_hash: u64,
    /// The sequence number of the segment file
    pub sequence_number: u64,
    /// The byte offset within the segment file (after the header)
    pub offset: u64,
}

/// Options for configuring a `Wal`.
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// The duration for which entries are retained.
    pub entry_retention: Duration,
    /// The number of segments per retention period for calculating expiration.
    pub segments_per_retention_period: u32,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            entry_retention: Duration::from_secs(60 * 60 * 24 * 7), // 1 week
            segments_per_retention_period: 10, // 10 segments per retention period
        }
    }
}

impl WalOptions {
    /// Creates a new WalOptions with custom retention and default segment size.
    pub fn with_retention(retention: Duration) -> Self {
        Self {
            entry_retention: retention,
            ..Default::default()
        }
    }

    /// Creates a new WalOptions with custom segments per retention period.
    pub fn with_segments_per_retention_period(segments: u32) -> Self {
        Self {
            segments_per_retention_period: segments,
            ..Default::default()
        }
    }

    /// Sets the retention period for this configuration (chainable).
    pub fn retention(mut self, retention: Duration) -> Self {
        self.entry_retention = retention;
        self
    }

    /// Sets the segments per retention period for this configuration (chainable).
    pub fn segments_per_retention_period(mut self, segments: u32) -> Self {
        self.segments_per_retention_period = segments;
        self
    }

    /// Validates the options and returns an error if invalid.
    pub fn validate(&self) -> io::Result<()> {
        if self.entry_retention.as_secs() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entry_retention must be greater than 0",
            ));
        }
        if self.segments_per_retention_period == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segments_per_retention_period must be greater than 0",
            ));
        }
        Ok(())
    }
}

/// Information about an active segment for a specific key
#[derive(Debug)]
struct ActiveSegment {
    /// The current active file handle
    file: File,
    /// The sequence number of this segment
    sequence_number: u64,
    /// Timestamp when this segment expires (Unix timestamp)
    expiration_timestamp: u64,
}

/// A Write-Ahead Log (WAL) with per-key segment sets.
pub struct Wal {
    dir: PathBuf,
    options: WalOptions,
    /// Map from key hash to active segment info
    active_segments: HashMap<u64, ActiveSegment>,
    /// Map from key hash to next sequence number for that key
    next_sequence: HashMap<u64, u64>,
}

/// An enumerable collection of items.
pub struct Enumerable<T> {
    items: Vec<T>,
}

impl<T> Iterator for Enumerable<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.items.is_empty() {
            None
        } else {
            Some(self.items.remove(0))
        }
    }
}

impl Wal {
    /// Creates a new `Wal` instance.
    ///
    /// # Arguments
    ///
    /// * `filepath` - The path to the directory where the WAL files will be stored.
    /// * `options` - The options for configuring the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or opened.
    pub fn new(filepath: &str, options: WalOptions) -> io::Result<Self> {
        // Validate options first
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

        // Scan existing files to determine next sequence numbers
        wal.scan_existing_files()?;

        Ok(wal)
    }

    /// Scans existing files to determine the next sequence numbers for each key
    fn scan_existing_files(&mut self) -> io::Result<()> {
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        if let Some((key_hash, sequence)) = self.parse_filename(filename) {
                            let current_max = self.next_sequence.get(&key_hash).unwrap_or(&0);
                            self.next_sequence
                                .insert(key_hash, (*current_max).max(sequence + 1));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Parses a segment filename to extract key hash and sequence number
    /// Expected format: "key-hash-sequence.log"
    fn parse_filename(&self, filename: &str) -> Option<(u64, u64)> {
        if let Some(name_part) = filename.strip_suffix(".log") {
            let parts: Vec<&str> = name_part.split('-').collect();
            if parts.len() >= 3 {
                // Find the last two parts that could be hash and sequence
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

    /// Generates a filename for a segment
    fn generate_filename<K: Display>(&self, key: &K, key_hash: u64, sequence: u64) -> String {
        let key_str = format!("{}", key);
        let sanitized_key = key_str
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .take(20)
            .collect::<String>();

        format!("{}-{}-{:04}.log", sanitized_key, key_hash, sequence)
    }

    /// Gets or creates an active segment for the given key
    fn get_or_create_active_segment<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: &K,
    ) -> io::Result<u64> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        let now = Utc::now().timestamp() as u64;

        // Check if we need to rotate the current segment due to expiration
        if let Some(active) = self.active_segments.get(&key_hash) {
            if now >= active.expiration_timestamp {
                // Need to rotate - remove from active segments
                self.active_segments.remove(&key_hash);
            }
        }

        // Create new segment if none exists or after rotation
        if !self.active_segments.contains_key(&key_hash) {
            let sequence = self.next_sequence.get(&key_hash).unwrap_or(&1).clone();
            self.next_sequence.insert(key_hash, sequence + 1);

            // Calculate expiration timestamp based on segments per retention period
            let segment_duration = self.options.entry_retention.as_secs()
                / self.options.segments_per_retention_period as u64;
            let expiration_timestamp = now + segment_duration;

            let filename = self.generate_filename(key, key_hash, sequence);
            let file_path = self.dir.join(&filename);

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&file_path)?;

            // Write file header for new file
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

    /// Writes the file header for a new segment
    fn write_file_header<K: AsRef<[u8]>>(
        &self,
        file: &mut File,
        key: &K,
        expiration_timestamp: u64,
    ) -> io::Result<()> {
        // Write NANO-LOG signature
        file.write_all(&NANO_LOG_SIGNATURE)?;

        // Write sequence number (will be updated by caller)
        let sequence = 0u64; // Placeholder, caller should update
        file.write_all(&sequence.to_le_bytes())?;

        // Write expiration timestamp
        file.write_all(&expiration_timestamp.to_le_bytes())?;

        // Write key length and key
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
    /// * `key` - The key of the entry.
    /// * `content` - The content of the entry.
    /// * `durable` - Whether to ensure the entry is persisted to disk before returning.
    ///
    /// # Returns
    ///
    /// Returns an `EntryRef` pointing to the location of the written entry.
    pub fn append_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        content: Bytes,
        durable: bool,
    ) -> io::Result<EntryRef> {
        let key_hash = self.get_or_create_active_segment(&key)?;

        let active_segment = self.active_segments.get_mut(&key_hash).unwrap();

        // Calculate current file position for offset (after header)
        let current_position = active_segment.file.seek(SeekFrom::Current(0))?;
        let file_header_size = 8 + 8 + 8 + 8 + key.as_ref().len() as u64; // NANO-LOG + sequence + expiration + key_len + key
        let entry_offset = current_position - file_header_size;

        // Write entry: NANO-REC signature + content_length + content
        active_segment.file.write_all(&NANO_REC_SIGNATURE)?;

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

    /// Enumerates the keys in the WAL.
    pub fn enumerate_keys(&self) -> Result<Enumerable<String>, std::io::Error> {
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

        Ok(Enumerable {
            items: keys.into_iter().collect(),
        })
    }

    /// Reads the key from a segment file header
    fn read_key_from_file(&self, file_path: &Path) -> io::Result<String> {
        let mut file = File::options().read(true).open(file_path)?;

        // Read and verify NANO-LOG signature
        let mut signature_buf = [0u8; 8];
        file.read_exact(&mut signature_buf)?;
        if signature_buf != NANO_LOG_SIGNATURE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid NANO-LOG signature in file header",
            ));
        }

        // Skip sequence number
        file.seek(SeekFrom::Current(8))?;

        // Skip expiration timestamp
        file.seek(SeekFrom::Current(8))?;

        // Read key
        let mut key_len_bytes = [0u8; 8];
        file.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);

        let mut key_bytes = vec![0u8; key_len as usize];
        file.read_exact(&mut key_bytes)?;

        Ok(String::from_utf8_lossy(&key_bytes).to_string())
    }

    /// Enumerates the records for a given key.
    pub fn enumerate_records<K: Hash + AsRef<[u8]> + Display>(
        &self,
        key: K,
    ) -> Result<Enumerable<Bytes>, std::io::Error> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        let mut records = Vec::new();

        // Find all segment files for this key
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

            // Sort by sequence number to read in order
            segment_files.sort_by_key(|(seq, _)| *seq);

            // Read records from each segment file
            for (_, file_path) in segment_files {
                if let Ok(file_records) = self.read_records_from_segment(&file_path) {
                    records.extend(file_records);
                }
            }
        }

        Ok(Enumerable { items: records })
    }

    /// Reads all records from a specific segment file
    fn read_records_from_segment(&self, file_path: &Path) -> io::Result<Vec<Bytes>> {
        let mut file = File::options().read(true).open(file_path)?;
        let mut records = Vec::new();

        // Skip file header
        self.skip_file_header(&mut file)?;

        // Read records until end of file
        loop {
            // Try to read NANO-REC signature
            let mut signature_buf = [0u8; 8];
            match file.read_exact(&mut signature_buf) {
                Ok(_) => {
                    if signature_buf != NANO_REC_SIGNATURE {
                        break; // Invalid signature, stop reading
                    }
                }
                Err(_) => break, // End of file
            }

            // Read content length
            let mut content_len_bytes = [0u8; 8];
            if file.read_exact(&mut content_len_bytes).is_err() {
                break;
            }
            let content_len = u64::from_le_bytes(content_len_bytes);

            // Read content
            let mut content_bytes = vec![0u8; content_len as usize];
            if file.read_exact(&mut content_bytes).is_err() {
                break;
            }

            records.push(Bytes::from(content_bytes));
        }

        Ok(records)
    }

    /// Skips the file header to position at the first record
    fn skip_file_header(&self, file: &mut File) -> io::Result<()> {
        // Skip NANO-LOG signature
        file.seek(SeekFrom::Current(8))?;

        // Skip sequence number
        file.seek(SeekFrom::Current(8))?;

        // Skip expiration timestamp
        file.seek(SeekFrom::Current(8))?;

        // Read and skip key
        let mut key_len_bytes = [0u8; 8];
        file.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);
        file.seek(SeekFrom::Current(key_len as i64))?;

        Ok(())
    }

    /// Reads an entry at the specified reference location.
    pub fn read_entry_at(&self, entry_ref: EntryRef) -> io::Result<Bytes> {
        // Find the segment file for this entry
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

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "Segment file for key_hash {} sequence {} not found",
                entry_ref.key_hash, entry_ref.sequence_number
            ),
        ))
    }

    /// Reads a specific entry from a segment file at the given offset
    fn read_entry_from_file(&self, file_path: &Path, offset: u64) -> io::Result<Bytes> {
        let mut file = File::options().read(true).open(file_path)?;

        // Skip file header first
        self.skip_file_header(&mut file)?;

        // Then seek to the entry offset (relative to start of records)
        file.seek(SeekFrom::Current(offset as i64))?;

        // Verify NANO-REC signature
        let mut signature_buf = [0u8; 8];
        file.read_exact(&mut signature_buf)?;
        if signature_buf != NANO_REC_SIGNATURE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "NANO-REC signature not found at specified location",
            ));
        }

        // Read content length
        let mut content_len_bytes = [0u8; 8];
        file.read_exact(&mut content_len_bytes)?;
        let content_len = u64::from_le_bytes(content_len_bytes);

        // Read content
        let mut content_bytes = vec![0u8; content_len as usize];
        file.read_exact(&mut content_bytes)?;

        Ok(Bytes::from(content_bytes))
    }

    /// Shuts down the WAL and removes all persisted storage.
    pub fn shutdown(&mut self) -> Result<(), std::io::Error> {
        // Close all active segments
        self.active_segments.clear();
        fs::remove_dir_all(&self.dir)
    }

    /// Compacts the WAL by removing expired segments.
    pub fn compact(&mut self) -> Result<(), std::io::Error> {
        let now = Utc::now().timestamp() as u64;

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        let file_path = entry.path();

                        // Read the expiration timestamp from the file header
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

                                    // Remove file if it has expired
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

    /// Logs an entry to the WAL with durability enabled.
    ///
    /// # Returns
    ///
    /// Returns an `EntryRef` pointing to the location of the written entry.
    pub fn log_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        content: Bytes,
    ) -> io::Result<EntryRef> {
        self.append_entry(key, content, true)
    }

    /// Returns the number of active segments.
    pub fn active_segment_count(&self) -> usize {
        self.active_segments.len()
    }

    /// Syncs all active segments to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        for active_segment in self.active_segments.values_mut() {
            active_segment.file.sync_data()?;
        }
        Ok(())
    }
}
