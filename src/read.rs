use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::error::{Result, WalError};
use crate::{NANO_REC_SIGNATURE, RECORD_FRAMING_SIZE};

/// Maximum entries per single preadv call (safety margin below UIO_MAXIOV).
const MAX_IOVECS_PER_PREADV: usize = 512;

/// Descriptor for a single record to be read from a segment.
pub struct ReadDescriptor {
    pub read_fd: Arc<OwnedFd>,
    pub file_offset: u64,
    pub byte_size: usize,
}

/// A parsed record read from the WAL.
pub struct Record {
    pub header: Option<Bytes>,
    pub content: Bytes,
}

/// Read a single record from the given fd at the specified offset.
pub(crate) fn read_single(fd: &Arc<OwnedFd>, file_offset: u64, byte_size: usize) -> Result<Record> {
    if byte_size < RECORD_FRAMING_SIZE {
        return Err(WalError::CorruptedData(format!(
            "record byte_size {} is less than framing size {}",
            byte_size, RECORD_FRAMING_SIZE
        )));
    }

    let mut buf = vec![0u8; byte_size];
    let iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: byte_size,
    };

    let n = unsafe {
        libc::preadv(
            fd.as_raw_fd(),
            &iov as *const libc::iovec,
            1,
            file_offset as i64,
        )
    };

    if n < 0 {
        return Err(WalError::Io(std::io::Error::last_os_error()));
    }
    if (n as usize) != byte_size {
        return Err(WalError::CorruptedData(format!(
            "short preadv: expected {} bytes, got {}",
            byte_size, n
        )));
    }

    parse_record(&buf)
}

/// Parse a record from a raw buffer.
///
/// Layout: `[NANORC (6)][header_len LE u16 (2)][header (N)][content_len LE u64 (8)][content (N)]`
fn parse_record(buf: &[u8]) -> Result<Record> {
    if buf.len() < RECORD_FRAMING_SIZE {
        return Err(WalError::CorruptedData(format!(
            "buffer too small: {} < {}",
            buf.len(),
            RECORD_FRAMING_SIZE
        )));
    }

    // Validate signature
    if &buf[0..6] != &NANO_REC_SIGNATURE {
        return Err(WalError::CorruptedData(format!(
            "invalid record signature: expected NANORC, got {:?}",
            &buf[0..6]
        )));
    }

    // Read header_len
    let header_len = u16::from_le_bytes(buf[6..8].try_into().unwrap()) as usize;

    // Offset to content_len field
    let content_len_offset = 8 + header_len;
    if buf.len() < content_len_offset + 8 {
        return Err(WalError::CorruptedData(format!(
            "buffer too small for content_len: need {}, have {}",
            content_len_offset + 8,
            buf.len()
        )));
    }

    let content_len = u64::from_le_bytes(
        buf[content_len_offset..content_len_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;

    let content_offset = content_len_offset + 8;
    let expected_total = content_offset + content_len;
    if buf.len() < expected_total {
        return Err(WalError::CorruptedData(format!(
            "buffer too small for content: need {}, have {}",
            expected_total,
            buf.len()
        )));
    }

    let header = if header_len > 0 {
        Some(Bytes::copy_from_slice(&buf[8..8 + header_len]))
    } else {
        None
    };

    let content = Bytes::copy_from_slice(&buf[content_offset..content_offset + content_len]);

    Ok(Record { header, content })
}

/// Read a batch of records, coalescing contiguous reads on the same fd into
/// single preadv syscalls.
///
/// Records are returned in the same order as the input descriptors.
pub fn read_batch(reads: &[ReadDescriptor]) -> Result<Vec<Record>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }

    if reads.len() == 1 {
        let r = &reads[0];
        let record = read_single(&r.read_fd, r.file_offset, r.byte_size)?;
        return Ok(vec![record]);
    }

    // Results vec indexed by original position
    let mut results: Vec<Option<Record>> = (0..reads.len()).map(|_| None).collect();

    // Group indices by fd (using raw fd value as key)
    let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
    for (i, r) in reads.iter().enumerate() {
        groups
            .entry(r.read_fd.as_raw_fd())
            .or_default()
            .push(i);
    }

    for (_fd_raw, mut indices) in groups {
        // Sort by file_offset within this fd group
        indices.sort_by_key(|&i| reads[i].file_offset);

        // Detect contiguous runs
        let mut run_start = 0;
        while run_start < indices.len() {
            let mut run_end = run_start;
            while run_end + 1 < indices.len() {
                let curr_idx = indices[run_end];
                let next_idx = indices[run_end + 1];
                let curr = &reads[curr_idx];
                let next = &reads[next_idx];
                if next.file_offset == curr.file_offset + curr.byte_size as u64 {
                    run_end += 1;
                } else {
                    break;
                }
            }

            let run = &indices[run_start..=run_end];
            if run.len() == 1 {
                // Single entry run
                let idx = run[0];
                let r = &reads[idx];
                results[idx] = Some(read_single(&r.read_fd, r.file_offset, r.byte_size)?);
            } else {
                // Coalesced multi-entry run -- chunk to respect MAX_IOVECS_PER_PREADV
                for chunk in run.chunks(MAX_IOVECS_PER_PREADV) {
                    let base_offset = reads[chunk[0]].file_offset;
                    let fd = &reads[chunk[0]].read_fd;

                    // Allocate buffers for each entry in the chunk
                    let mut bufs: Vec<BytesMut> = chunk
                        .iter()
                        .map(|&idx| BytesMut::zeroed(reads[idx].byte_size))
                        .collect();

                    // Build iovecs
                    let iovecs: Vec<libc::iovec> = bufs
                        .iter_mut()
                        .map(|b| libc::iovec {
                            iov_base: b.as_mut_ptr() as *mut libc::c_void,
                            iov_len: b.len(),
                        })
                        .collect();

                    let total_bytes: usize = chunk.iter().map(|&idx| reads[idx].byte_size).sum();

                    let n = unsafe {
                        libc::preadv(
                            fd.as_raw_fd(),
                            iovecs.as_ptr(),
                            iovecs.len() as i32,
                            base_offset as i64,
                        )
                    };

                    if n < 0 {
                        return Err(WalError::Io(std::io::Error::last_os_error()));
                    }
                    if (n as usize) != total_bytes {
                        return Err(WalError::CorruptedData(format!(
                            "short coalesced preadv: expected {} bytes, got {}",
                            total_bytes, n
                        )));
                    }

                    // Parse each buffer
                    for (j, &idx) in chunk.iter().enumerate() {
                        results[idx] = Some(parse_record(&bufs[j])?);
                    }
                }
            }

            run_start = run_end + 1;
        }
    }

    // Unwrap all results in order
    let records: Vec<Record> = results
        .into_iter()
        .enumerate()
        .map(|(i, opt)| {
            opt.unwrap_or_else(|| panic!("BUG: missing record at index {}", i))
        })
        .collect();

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Wal, WalOptions, WriteEntry};
    use tempfile::TempDir;
    use std::time::Duration;

    fn test_options() -> WalOptions {
        WalOptions {
            retention: Duration::from_secs(3600),
            segment_duration: Duration::from_secs(600),
        }
    }

    #[test]
    fn test_read_single_no_header() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "r1", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entry = wal.append(None, b"hello world", now, false).unwrap();
        let seg = wal.ensure_segment(now).unwrap();
        let record = wal.read_at(&seg, entry.file_offset, entry.byte_size).unwrap();
        assert!(record.header.is_none());
        assert_eq!(record.content.as_ref(), b"hello world");
    }

    #[test]
    fn test_read_single_with_header() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "r2", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entry = wal.append(Some(b"meta"), b"payload", now, false).unwrap();
        let seg = wal.ensure_segment(now).unwrap();
        let record = wal.read_at(&seg, entry.file_offset, entry.byte_size).unwrap();
        assert_eq!(record.header.as_ref().unwrap().as_ref(), b"meta");
        assert_eq!(record.content.as_ref(), b"payload");
    }

    #[test]
    fn test_read_batch_multiple() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::new(dir.path(), "rb", test_options()).unwrap();
        let now = 1_711_234_567_890i64;
        let entries = vec![
            WriteEntry { header: None, content: b"first" },
            WriteEntry { header: Some(b"h"), content: b"second" },
            WriteEntry { header: None, content: b"third" },
        ];
        let refs = wal.append_batch(&entries, now, false).unwrap();
        let seg = wal.ensure_segment(now).unwrap();

        let descriptors: Vec<ReadDescriptor> = refs.iter().map(|r| ReadDescriptor {
            read_fd: seg.read_fd().clone(),
            file_offset: r.file_offset,
            byte_size: r.byte_size,
        }).collect();

        let records = read_batch(&descriptors).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].content.as_ref(), b"first");
        assert!(records[0].header.is_none());
        assert_eq!(records[1].content.as_ref(), b"second");
        assert_eq!(records[1].header.as_ref().unwrap().as_ref(), b"h");
        assert_eq!(records[2].content.as_ref(), b"third");
    }

    #[test]
    fn test_read_batch_empty() {
        let records = read_batch(&[]).unwrap();
        assert!(records.is_empty());
    }
}
