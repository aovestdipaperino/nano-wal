use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Result, WalError};
use crate::{FILE_HEADER_SIZE, NANO_LOG_SIGNATURE};

/// A single WAL segment file with concurrent read/write support.
///
/// Writes go through the mutex-protected `File` handle.
/// Reads use a dup'd file descriptor (`read_fd`) that is independent
/// of the write cursor, enabling lock-free `pread`/`preadv` calls.
pub struct Segment {
    pub(crate) file: Mutex<File>,
    read_fd: Arc<OwnedFd>,
    path: PathBuf,
    expiration_ms: i64,
    file_size: AtomicU64,
}

impl Segment {
    /// Create a new segment file, writing the 16-byte header.
    ///
    /// Header layout: `[NANO-LOG (8 bytes)][expiration_ms LE (8 bytes)]`
    pub(crate) fn create(path: &Path, expiration_ms: i64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // Write header
        let mut header = [0u8; FILE_HEADER_SIZE];
        header[..8].copy_from_slice(&NANO_LOG_SIGNATURE);
        header[8..16].copy_from_slice(&expiration_ms.to_le_bytes());
        file.write_all(&header)?;
        file.flush()?;

        let read_fd = Arc::new(dup_read_fd(&file)?);

        Ok(Segment {
            file: Mutex::new(file),
            read_fd,
            path: path.to_path_buf(),
            expiration_ms,
            file_size: AtomicU64::new(FILE_HEADER_SIZE as u64),
        })
    }

    /// Reopen an existing segment file, validating its header.
    pub(crate) fn open(path: &Path, expected_expiration_ms: i64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)?;

        // Read and validate header
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header).map_err(|e| {
            WalError::CorruptedData(format!("failed to read segment header: {}", e))
        })?;

        // Validate magic
        if &header[..8] != &NANO_LOG_SIGNATURE {
            return Err(WalError::CorruptedData(format!(
                "invalid segment magic: expected NANO-LOG, got {:?}",
                &header[..8]
            )));
        }

        // Validate expiration
        let stored_expiration = i64::from_le_bytes(header[8..16].try_into().unwrap());
        if stored_expiration != expected_expiration_ms {
            return Err(WalError::CorruptedData(format!(
                "expiration mismatch: expected {}, got {}",
                expected_expiration_ms, stored_expiration
            )));
        }

        // Seek to end to get file size
        let file_size = file.seek(SeekFrom::End(0))?;

        let read_fd = Arc::new(dup_read_fd(&file)?);

        Ok(Segment {
            file: Mutex::new(file),
            read_fd,
            path: path.to_path_buf(),
            expiration_ms: expected_expiration_ms,
            file_size: AtomicU64::new(file_size),
        })
    }

    /// Returns the dup'd read file descriptor for lock-free pread/preadv.
    pub fn read_fd(&self) -> &Arc<OwnedFd> {
        &self.read_fd
    }

    /// Returns the current total file size (header + data).
    pub fn file_size(&self) -> u64 {
        self.file_size.load(Ordering::Acquire)
    }

    /// Atomically adds `bytes` to the tracked file size; returns the previous value.
    pub(crate) fn add_file_size(&self, bytes: u64) -> u64 {
        self.file_size.fetch_add(bytes, Ordering::AcqRel)
    }

    /// Returns the immutable expiration timestamp (ms since epoch).
    pub fn expiration_ms(&self) -> i64 {
        self.expiration_ms
    }

    /// Returns the path to this segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("path", &self.path)
            .field("expiration_ms", &self.expiration_ms)
            .field("file_size", &self.file_size.load(Ordering::Relaxed))
            .finish()
    }
}

/// Duplicate a file descriptor for independent read access.
///
/// The duplicated fd is configured with `POSIX_FADV_SEQUENTIAL` on Linux
/// to hint the kernel for sequential read-ahead.
pub(crate) fn dup_read_fd(file: &File) -> Result<OwnedFd> {
    let raw = file.as_raw_fd();
    let duped = unsafe { libc::dup(raw) };
    if duped < 0 {
        return Err(WalError::Io(std::io::Error::last_os_error()));
    }

    // On Linux, hint the kernel for sequential reads
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::posix_fadvise(duped, 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
    }

    Ok(unsafe { OwnedFd::from_raw_fd(duped) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_create_segment_writes_header() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_12345.seg");
        let segment = Segment::create(&path, 12345).unwrap();

        assert_eq!(segment.expiration_ms(), 12345);
        assert_eq!(segment.file_size(), FILE_HEADER_SIZE as u64);
        assert!(segment.path() == path);

        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[0..8], b"NANO-LOG");
        assert_eq!(i64::from_le_bytes(data[8..16].try_into().unwrap()), 12345);
    }

    #[test]
    fn test_read_fd_is_independent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_99999.seg");
        let segment = Segment::create(&path, 99999).unwrap();

        {
            let mut file = segment.file.lock().unwrap();
            file.write_all(b"extra data").unwrap();
            file.flush().unwrap();
        }

        let fd = segment.read_fd();
        let mut buf = [0u8; 8];
        let ret = unsafe {
            libc::pread(
                std::os::fd::AsRawFd::as_raw_fd(fd.as_ref()),
                buf.as_mut_ptr() as *mut libc::c_void,
                8,
                0,
            )
        };
        assert_eq!(ret, 8);
        assert_eq!(&buf, b"NANO-LOG");
    }

    #[test]
    fn test_reopen_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_55555.seg");

        {
            let segment = Segment::create(&path, 55555).unwrap();
            let mut file = segment.file.lock().unwrap();
            file.write_all(b"payload").unwrap();
        }

        let segment = Segment::open(&path, 55555).unwrap();
        assert_eq!(segment.expiration_ms(), 55555);
        assert_eq!(segment.file_size(), (FILE_HEADER_SIZE + 7) as u64);
    }
}
