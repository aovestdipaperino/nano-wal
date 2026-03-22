# nano-wal

![nano-wal](nano-wal.png)

A concurrent Write-Ahead Log (WAL) in Rust with lock-free segment rotation, vectored I/O, and coalesced batch reads. Designed for high-throughput, multi-threaded workloads where readers and writers must not block each other.

## What's New in v1.0.0

v1.0.0 is a ground-up rewrite. The API has changed — this is not backward compatible with 0.5.x.

| Change | Benefit |
|---|---|
| **`&self` API with interior mutability** | `Wal` is `Send + Sync` — share via `Arc<Wal>` across threads with no external locking |
| **CAS-based segment rotation** | Lock-free rotation via `ArcSwap`. Multiple threads can trigger rotation without blocking each other |
| **Dup'd read file descriptors** | Readers use an independent fd via `libc::dup()` — zero contention with writers, no shared file position |
| **Coalesced `preadv` batch reads** | `read_batch()` groups reads by fd, detects contiguous runs, and coalesces up to 512 entries into a single syscall |
| **Vectored writes (`writev`)** | `append_batch()` writes multiple records in a single `write_vectored` call — one syscall for N records |
| **Clock-aligned segment expiration** | Segments rotate on predictable time boundaries, not ingestion time. Simplifies reasoning about retention |
| **NANORC record framing** | Each record has a 6-byte signature for boundary detection and corruption recovery |
| **Structured cleanup results** | `cleanup()` returns `CleanupResult` with deleted files, byte counts, and live segment count — wire up your own metrics |
| **Startup recovery** | `Wal::new()` scans for existing segments and reopens the latest non-expired one. Handles process restarts |
| **Flat directory layout** | Segments use a caller-provided prefix (`{prefix}_{expiration}.seg`). Multiple WAL instances can share a directory |
| **Dropped `chrono` dependency** | Timestamps are caller-provided `i64` milliseconds. No runtime clock dependency |

### Removed from 0.5.x

- Per-key hash-based segment routing (replaced by one WAL instance per stream)
- `enumerate_keys()`, `enumerate_records()` (no key concept in v1)
- `shutdown()` that deletes all files (v1 shutdown is non-destructive)
- `log_entry()` convenience wrapper
- `chrono` dependency

## Installation

```toml
[dependencies]
nano-wal = "1.0.0"
```

## Quick Start

```rust
use nano_wal::{Wal, WalOptions, ReadDescriptor, read_batch};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), nano_wal::WalError> {
    let options = WalOptions {
        retention: Duration::from_secs(3600),       // 1 hour
        segment_duration: Duration::from_secs(600), // 10-minute windows
    };
    let wal = Arc::new(Wal::new("./my_wal", "stream-0", options)?);

    let now_ms = 1_711_234_567_890i64;

    // Append a record
    let entry = wal.append(None, b"hello world", now_ms, false)?;

    // Append with metadata header
    let entry2 = wal.append(Some(b"meta"), b"payload", now_ms, false)?;

    // Read it back
    let seg = wal.ensure_segment(now_ms)?;
    let record = wal.read_at(&seg, entry.file_offset, entry.byte_size)?;
    assert_eq!(record.content.as_ref(), b"hello world");

    // Batch read — coalesced into minimal syscalls
    let descs = vec![
        ReadDescriptor {
            read_fd: seg.read_fd().clone(),
            file_offset: entry.file_offset,
            byte_size: entry.byte_size,
        },
        ReadDescriptor {
            read_fd: seg.read_fd().clone(),
            file_offset: entry2.file_offset,
            byte_size: entry2.byte_size,
        },
    ];
    let records = read_batch(&descs)?;
    assert_eq!(records.len(), 2);

    // Cleanup expired segments
    let result = wal.cleanup()?;
    println!("Deleted {} segments, reclaimed {} bytes", result.deleted.len(), result.bytes_reclaimed);

    // Graceful shutdown
    wal.shutdown()?;

    Ok(())
}
```

## Concurrent Writers

`Wal` is `Send + Sync`. Share it across threads with `Arc<Wal>`:

```rust
use nano_wal::{Wal, WalOptions};
use std::sync::Arc;
use std::time::Duration;

let wal = Arc::new(Wal::new("./wal", "partition-0", WalOptions {
    retention: Duration::from_secs(3600),
    segment_duration: Duration::from_secs(600),
}).unwrap());

let mut handles = Vec::new();
for i in 0..4 {
    let wal = wal.clone();
    handles.push(std::thread::spawn(move || {
        let now = 1_711_234_567_890i64;
        for _ in 0..1000 {
            wal.append(None, format!("thread-{}", i).as_bytes(), now, false).unwrap();
        }
    }));
}
for h in handles { h.join().unwrap(); }
```

Writers acquire a `Mutex<File>` for the active segment. Readers use an independent dup'd fd via `preadv` — no contention with writers.

## Batch Operations

Write multiple records in a single `writev` syscall:

```rust
use nano_wal::{Wal, WalOptions, WriteEntry};
use std::time::Duration;

let wal = Wal::new("./wal", "batch", WalOptions {
    retention: Duration::from_secs(3600),
    segment_duration: Duration::from_secs(600),
}).unwrap();

let entries = vec![
    WriteEntry { header: None, content: b"first" },
    WriteEntry { header: Some(b"meta"), content: b"second" },
    WriteEntry { header: None, content: b"third" },
];
let refs = wal.append_batch(&entries, 1_711_234_567_890, false).unwrap();
assert_eq!(refs.len(), 3);
```

## Configuration

```rust
use nano_wal::WalOptions;
use std::time::Duration;

let options = WalOptions {
    // How long entries are retained before segments expire
    retention: Duration::from_secs(86400), // 1 day

    // Segment rotation interval (clock-aligned)
    // Must be <= retention
    segment_duration: Duration::from_secs(3600), // 1 hour windows
};
```

Segments rotate on clock-aligned boundaries. Expiration is calculated as:
```
window_start = (ingestion_time / segment_duration) * segment_duration
expiration = window_start + segment_duration + retention
```

## API Reference

### Wal

| Method | Description |
|---|---|
| `new(dir, prefix, options)` | Create WAL, recover existing segments |
| `ensure_segment(ingestion_time)` | Get or create active segment (CAS rotation) |
| `append(header, content, ingestion_time, durable)` | Append single record |
| `append_batch(entries, ingestion_time, durable)` | Append multiple records in one `writev` |
| `read_at(segment, offset, size)` | Read single record via `preadv` |
| `cleanup()` | Delete expired segments, return `CleanupResult` |
| `sync()` | `fdatasync` the active segment |
| `shutdown()` | Sync and close. Further writes return `WalError::Shutdown` |

### Segment

| Method | Description |
|---|---|
| `read_fd()` | Dup'd fd for lock-free `preadv` reads |
| `file_size()` | Total bytes written |
| `expiration_ms()` | Immutable expiration timestamp |
| `path()` | Filesystem path |

### Free Function

| Function | Description |
|---|---|
| `read_batch(descriptors)` | Coalesced multi-entry read. Groups by fd, detects contiguous runs, minimal syscalls |

### Types

- `EntryRef` — `{ file_offset: u64, byte_size: usize }` returned from append
- `WriteEntry` — `{ header: Option<&[u8]>, content: &[u8] }` for batch appends
- `ReadDescriptor` — `{ read_fd: Arc<OwnedFd>, file_offset: u64, byte_size: usize }` for batch reads
- `Record` — `{ header: Option<Bytes>, content: Bytes }` returned from reads
- `CleanupResult` — `{ deleted: Vec<DeletedSegment>, live_count: u64, bytes_reclaimed: u64 }`

## File Format

### Segment file header (16 bytes)

```
[NANO-LOG: 8 bytes][expiration_ms: 8 bytes LE]
```

### Record format

```
[NANORC: 6 bytes][header_len: 2 bytes LE][header: N bytes][content_len: 8 bytes LE][content: N bytes]
```

### Filename convention

```
{prefix}_{expiration_ms}.seg
```

## Error Handling

```rust
use nano_wal::WalError;

match result {
    Err(WalError::Io(e)) => eprintln!("I/O error: {}", e),
    Err(WalError::InvalidConfig(msg)) => eprintln!("Bad config: {}", msg),
    Err(WalError::CorruptedData(msg)) => eprintln!("Corruption: {}", msg),
    Err(WalError::HeaderTooLarge { size, max }) => eprintln!("Header {} > {}", size, max),
    Err(WalError::Shutdown) => eprintln!("WAL is shut down"),
    Ok(_) => {}
}
```

## Testing

```bash
cargo test          # Unit + integration tests
cargo clippy        # Lint checks
```

## License

MIT License. See [LICENSE](LICENSE).
