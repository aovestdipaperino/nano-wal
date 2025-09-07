//! A simple Write-Ahead Log (WAL) implementation.
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Options for configuring a `Wal`.
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// The duration for which entries are retained.
    pub entry_retention: Duration,
    /// The number of segments to use per retention period.
    pub segments: u8,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            entry_retention: Duration::from_secs(60 * 60 * 24 * 7), // 1 week
            segments: 10,                                           // 10 segments
        }
    }
}

impl WalOptions {
    /// Creates a new WalOptions with custom retention and default segments.
    pub fn with_retention(retention: Duration) -> Self {
        Self {
            entry_retention: retention,
            ..Default::default()
        }
    }

    /// Creates a new WalOptions with custom number of segments and default retention.
    pub fn with_segments(segments: u8) -> Self {
        Self {
            segments,
            ..Default::default()
        }
    }

    /// Validates the options and returns an error if invalid.
    pub fn validate(&self) -> io::Result<()> {
        if self.segments == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segments must be greater than 0",
            ));
        }
        if self.entry_retention.as_secs() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entry_retention must be greater than 0",
            ));
        }
        if self.entry_retention.as_secs() < self.segments as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entry_retention must be at least as many seconds as segments",
            ));
        }
        Ok(())
    }
}

/// A Write-Ahead Log (WAL).
pub struct Wal {
    dir: PathBuf,
    entry_retention: Duration,
    active_segment: File,
    active_segment_id: u64,
    segments: u8,
    segment_duration: Duration,
    index: HashMap<u64, Vec<(u64, u64)>>,
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

        // Find the highest existing segment ID
        let mut active_segment_id = 0;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        if let Some(id_str) = filename.strip_suffix(".log") {
                            // Extract segment ID from filename format "id_key.log"
                            if let Some(underscore_pos) = id_str.find('_') {
                                let id_part = &id_str[..underscore_pos];
                                if let Ok(id) = id_part.parse::<u64>() {
                                    active_segment_id = active_segment_id.max(id);
                                }
                            } else if let Ok(id) = id_str.parse::<u64>() {
                                active_segment_id = active_segment_id.max(id);
                            }
                        }
                    }
                }
            }
        }

        let segment_path = dir.join(format!("{}.log", active_segment_id));
        let active_segment = OpenOptions::new()
            .append(true)
            .create(true)
            .open(segment_path)?;

        // Ensure minimum segment duration of 1 second
        let segment_duration = Duration::from_secs(std::cmp::max(
            1,
            options.entry_retention.as_secs() / options.segments as u64,
        ));

        let mut wal = Wal {
            dir: dir.to_path_buf(),
            entry_retention: options.entry_retention,
            active_segment,
            active_segment_id,
            segments: options.segments,
            segment_duration,
            index: HashMap::new(),
        };

        // Rebuild index from existing segments
        wal.rebuild_index()?;

        Ok(wal)
    }

    /// Appends an entry to the WAL.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry.
    /// * `content` - The content of the entry.
    /// * `durable` - Whether to ensure the entry is persisted to disk before returning.
    pub fn append_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        content: Bytes,
        durable: bool,
    ) -> io::Result<()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        // Check if we need to rotate to a new segment based on time
        let now = Utc::now().timestamp() as u64;
        let segment_start_time = now - (now % self.segment_duration.as_secs());
        let expected_segment_id = segment_start_time / self.segment_duration.as_secs();

        if expected_segment_id > self.active_segment_id {
            self.active_segment_id = expected_segment_id;
            // Include a sanitized version of the key in the filename for better debugging
            let key_str = format!("{}", key);
            let sanitized_key = key_str
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
                .take(20)
                .collect::<String>();
            let segment_path = if sanitized_key.is_empty() {
                self.dir.join(format!("{}.log", self.active_segment_id))
            } else {
                self.dir
                    .join(format!("{}_{}.log", self.active_segment_id, sanitized_key))
            };
            self.active_segment = OpenOptions::new()
                .append(true)
                .create(true)
                .open(segment_path)?;
        }

        let current_offset = self.active_segment.seek(SeekFrom::End(0))?;

        // Write entry: timestamp + key_len + key + content_len + content
        let timestamp = Utc::now().timestamp() as u64;
        self.active_segment.write_all(&timestamp.to_le_bytes())?;

        let key_bytes = key.as_ref();
        let key_len = key_bytes.len() as u64;
        self.active_segment.write_all(&key_len.to_le_bytes())?;
        self.active_segment.write_all(key_bytes)?;

        let content_len = content.len() as u64;
        self.active_segment.write_all(&content_len.to_le_bytes())?;
        self.active_segment.write_all(content.as_ref())?;

        if durable {
            self.active_segment.sync_data()?;
        } else {
            // For non-durable writes, still flush to OS buffers
            self.active_segment.flush()?;
        }

        // Update index
        self.index
            .entry(key_hash)
            .or_default()
            .push((self.active_segment_id, current_offset));

        Ok(())
    }

    /// Enumerates the keys in the WAL.
    pub fn enumerate_keys(&self) -> Result<Enumerable<String>, std::io::Error> {
        let mut keys = std::collections::HashSet::new();

        for i in 0..=self.active_segment_id {
            // Try both formats: with and without key suffix
            let _segment_paths = vec![
                self.dir.join(format!("{}.log", i)),
                // Also check for files with key suffixes
            ];

            // Additionally, scan directory for any files matching the segment ID pattern
            if let Ok(entries) = fs::read_dir(&self.dir) {
                for entry in entries.flatten() {
                    if let Some(filename) = entry.file_name().to_str() {
                        if filename.starts_with(&format!("{}_", i)) && filename.ends_with(".log") {
                            let segment_path = entry.path();
                            if segment_path.exists() {
                                let mut segment_reader = File::open(&segment_path)?;
                                while let Ok(key) = self.read_key(&mut segment_reader) {
                                    keys.insert(String::from_utf8_lossy(&key).to_string());
                                }
                            }
                        }
                    }
                }
            }

            // Also check the simple format
            let segment_path = self.dir.join(format!("{}.log", i));
            if segment_path.exists() {
                let mut segment_reader = File::open(segment_path)?;
                while let Ok(key) = self.read_key(&mut segment_reader) {
                    keys.insert(String::from_utf8_lossy(&key).to_string());
                }
            }
        }

        Ok(Enumerable {
            items: keys.into_iter().collect(),
        })
    }

    fn read_key(&self, segment_reader: &mut File) -> Result<Vec<u8>, io::Error> {
        // Skip timestamp
        segment_reader.seek(SeekFrom::Current(8))?;

        // Read key length
        let mut key_len_bytes = [0u8; 8];
        segment_reader.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);

        // Read key
        let mut key_bytes = vec![0u8; key_len as usize];
        segment_reader.read_exact(&mut key_bytes)?;

        // Skip content length and content
        let mut content_len_bytes = [0u8; 8];
        segment_reader.read_exact(&mut content_len_bytes)?;
        let content_len = u64::from_le_bytes(content_len_bytes);
        segment_reader.seek(SeekFrom::Current(content_len as i64))?;

        Ok(key_bytes)
    }

    /// Enumerates the records for a given key.
    pub fn enumerate_records<K: Hash + AsRef<[u8]> + Display>(
        &self,
        key: K,
    ) -> Result<Enumerable<Bytes>, std::io::Error> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.as_ref().hash(&mut hasher);
        let key_hash = hasher.finish();

        if let Some(offsets) = self.index.get(&key_hash) {
            let mut records = Vec::new();

            for &(segment_id, offset) in offsets {
                // Try to find the segment file (could have different naming patterns)
                let mut record_found = false;

                // First try simple format
                let simple_path = self.dir.join(format!("{}.log", segment_id));
                if simple_path.exists() {
                    if let Ok(mut segment_reader) = File::open(&simple_path) {
                        if let Ok(record) = self.read_record_at_offset(&mut segment_reader, offset)
                        {
                            records.push(record);
                            record_found = true;
                        }
                    }
                }

                // If not found, look for files with key suffixes
                if !record_found {
                    if let Ok(entries) = fs::read_dir(&self.dir) {
                        for entry in entries.flatten() {
                            if let Some(filename) = entry.file_name().to_str() {
                                if filename.starts_with(&format!("{}_", segment_id))
                                    && filename.ends_with(".log")
                                {
                                    if let Ok(mut segment_reader) = File::open(entry.path()) {
                                        if let Ok(record) =
                                            self.read_record_at_offset(&mut segment_reader, offset)
                                        {
                                            records.push(record);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(Enumerable { items: records })
        } else {
            Ok(Enumerable { items: Vec::new() })
        }
    }

    fn read_record_at_offset(&self, segment_reader: &mut File, offset: u64) -> io::Result<Bytes> {
        segment_reader.seek(SeekFrom::Start(offset))?;

        // Skip timestamp
        segment_reader.seek(SeekFrom::Current(8))?;

        // Skip key
        let mut key_len_bytes = [0u8; 8];
        segment_reader.read_exact(&mut key_len_bytes)?;
        let key_len = u64::from_le_bytes(key_len_bytes);
        segment_reader.seek(SeekFrom::Current(key_len as i64))?;

        // Read content
        let mut content_len_bytes = [0u8; 8];
        segment_reader.read_exact(&mut content_len_bytes)?;
        let content_len = u64::from_le_bytes(content_len_bytes);
        let mut record_bytes = vec![0u8; content_len as usize];
        segment_reader.read_exact(&mut record_bytes)?;

        Ok(Bytes::from(record_bytes))
    }

    /// Shuts down the WAL and removes all persisted storage.
    pub fn shutdown(&mut self) -> Result<(), std::io::Error> {
        self.active_segment.sync_data()?;
        fs::remove_dir_all(&self.dir)
    }

    /// Compacts the WAL by removing expired segments.
    pub fn compact(&mut self) -> Result<(), std::io::Error> {
        let now = Utc::now().timestamp() as u64;
        let retention_secs = self.entry_retention.as_secs();

        // Scan all .log files in the directory
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        let segment_path = entry.path();

                        let mut segment_reader = File::open(&segment_path)?;
                        let mut timestamp_bytes = [0u8; 8];
                        if segment_reader.read_exact(&mut timestamp_bytes).is_ok() {
                            let timestamp = u64::from_le_bytes(timestamp_bytes);
                            if now - timestamp > retention_secs {
                                drop(segment_reader); // Close file before deletion
                                fs::remove_file(&segment_path)?;
                            }
                        }
                    }
                }
            }
        }

        self.rebuild_index()
    }

    fn rebuild_index(&mut self) -> io::Result<()> {
        self.index.clear();

        // Scan all .log files in the directory and track the highest segment ID
        let mut max_segment_id = 0u64;

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.ends_with(".log") {
                        // Extract segment ID from filename
                        let segment_id = if let Some(id_str) = filename.strip_suffix(".log") {
                            if let Some(underscore_pos) = id_str.find('_') {
                                let id_part = &id_str[..underscore_pos];
                                id_part.parse::<u64>().unwrap_or(0)
                            } else {
                                id_str.parse::<u64>().unwrap_or(0)
                            }
                        } else {
                            continue;
                        };

                        max_segment_id = max_segment_id.max(segment_id);

                        let segment_path = entry.path();
                        if let Ok(mut segment_reader) = File::open(&segment_path) {
                            let mut offset = 0u64;

                            loop {
                                let current_offset = offset;

                                // Read timestamp
                                let mut timestamp_bytes = [0u8; 8];
                                if segment_reader.read_exact(&mut timestamp_bytes).is_err() {
                                    break;
                                }
                                offset += 8;

                                // Read key
                                let mut key_len_bytes = [0u8; 8];
                                if segment_reader.read_exact(&mut key_len_bytes).is_err() {
                                    break;
                                }
                                let key_len = u64::from_le_bytes(key_len_bytes);
                                offset += 8;

                                if key_len == 0 || key_len > 1024 * 1024 {
                                    // Invalid key length, stop processing this file
                                    break;
                                }

                                let mut key_bytes = vec![0u8; key_len as usize];
                                if segment_reader.read_exact(&mut key_bytes).is_err() {
                                    break;
                                }
                                offset += key_len;

                                // Calculate key hash
                                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                key_bytes.hash(&mut hasher);
                                let key_hash = hasher.finish();

                                // Add to index
                                self.index
                                    .entry(key_hash)
                                    .or_default()
                                    .push((segment_id, current_offset));

                                // Skip content
                                let mut content_len_bytes = [0u8; 8];
                                if segment_reader.read_exact(&mut content_len_bytes).is_err() {
                                    break;
                                }
                                let content_len = u64::from_le_bytes(content_len_bytes);
                                offset += 8;

                                if content_len > 1024 * 1024 * 100 {
                                    // Invalid content length, stop processing this file
                                    break;
                                }

                                if segment_reader
                                    .seek(SeekFrom::Current(content_len as i64))
                                    .is_err()
                                {
                                    break;
                                }
                                offset += content_len;
                            }
                        }
                    }
                }
            }
        }

        // Update active segment ID to the highest found
        if max_segment_id > self.active_segment_id {
            self.active_segment_id = max_segment_id;
            // Reopen the active segment file
            let segment_path = self.dir.join(format!("{}.log", self.active_segment_id));
            if let Ok(segment) = OpenOptions::new()
                .append(true)
                .create(true)
                .open(segment_path)
            {
                self.active_segment = segment;
            }
        }

        Ok(())
    }

    /// Logs an entry to the WAL with durability enabled.
    pub fn log_entry<K: Hash + AsRef<[u8]> + Display>(
        &mut self,
        key: K,
        content: Bytes,
    ) -> io::Result<()> {
        self.append_entry(key, content, true)
    }

    /// Returns the number of entries in the index.
    pub fn entry_count(&self) -> usize {
        self.index.values().map(|v| v.len()).sum()
    }

    /// Returns the current active segment ID.
    pub fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    /// Syncs the active segment to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.active_segment.sync_data()
    }

    /// Returns the number of segments configured for this WAL.
    pub fn segments(&self) -> u8 {
        self.segments
    }
}
