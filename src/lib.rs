//! A concurrent Write-Ahead Log (WAL) with CAS-based segment rotation
//! and coalesced preadv reads.
//!
//! # Features
//!
//! - Lock-free segment rotation via ArcSwap + CAS
//! - Concurrent reads via dup'd file descriptors (preadv)
//! - Coalesced batch reads for minimal syscalls
//! - Clock-aligned segment expiration windows
//! - Vectored writes (writev) for batch appends
//!
//! # Examples
//!
//! ```ignore
//! use nano_wal::{Wal, WalOptions};
//! use std::time::Duration;
//!
//! let options = WalOptions {
//!     retention: Duration::from_secs(3600),
//!     segment_duration: Duration::from_secs(600),
//! };
//! let wal = Wal::new("/tmp/my_wal", "stream-0", options).unwrap();
//!
//! let now_ms = 1711234567890i64;
//! let entry = wal.append(None, b"hello world", now_ms, false).unwrap();
//! ```

mod error;
mod segment;
mod wal;
mod read;
mod cleanup;

pub use error::{WalError, Result};
pub use segment::Segment;
pub use wal::{Wal, WalOptions};
pub use read::{ReadDescriptor, Record, read_batch};
pub use cleanup::{CleanupResult, DeletedSegment};

/// Re-exported for callers who need to hold segment read fds.
pub use bytes::Bytes;

/// UTF-8 'NANO-LOG' signature for segment file headers.
pub(crate) const NANO_LOG_SIGNATURE: [u8; 8] = *b"NANO-LOG";

/// UTF-8 'NANORC' signature for individual records.
pub(crate) const NANO_REC_SIGNATURE: [u8; 6] = *b"NANORC";

/// Segment file header size: NANO-LOG (8) + expiration_ms (8)
pub(crate) const FILE_HEADER_SIZE: usize = 16;

/// Record framing overhead: NANORC (6) + header_len (2) + content_len (8)
pub(crate) const RECORD_FRAMING_SIZE: usize = 16;

/// Maximum size for record headers in bytes (64KB).
pub(crate) const MAX_HEADER_SIZE: usize = 65535;

/// EntryRef returned from append operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRef {
    /// Byte offset within the segment file
    pub file_offset: u64,
    /// Total on-disk size (NANORC framing + header + content)
    pub byte_size: usize,
}

/// A single entry for batch append operations.
#[derive(Debug)]
pub struct WriteEntry<'a> {
    pub header: Option<&'a [u8]>,
    pub content: &'a [u8],
}
