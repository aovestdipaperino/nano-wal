use nano_wal::{Wal, WalOptions, ReadDescriptor, read_batch};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn opts() -> WalOptions {
    WalOptions {
        retention: Duration::from_secs(3600),
        segment_duration: Duration::from_secs(600),
    }
}

#[test]
fn test_full_write_read_cycle() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::new(dir.path(), "cycle", opts()).unwrap();
    let now = 1_711_234_567_890i64;

    let e1 = wal.append(None, b"record one", now, false).unwrap();
    let e2 = wal.append(Some(b"hdr"), b"record two", now, false).unwrap();
    let seg = wal.ensure_segment(now).unwrap();

    let r1 = wal.read_at(&seg, e1.file_offset, e1.byte_size).unwrap();
    assert_eq!(r1.content.as_ref(), b"record one");
    assert!(r1.header.is_none());

    let r2 = wal.read_at(&seg, e2.file_offset, e2.byte_size).unwrap();
    assert_eq!(r2.content.as_ref(), b"record two");
    assert_eq!(r2.header.unwrap().as_ref(), b"hdr");

    let descs = vec![
        ReadDescriptor { read_fd: seg.read_fd().clone(), file_offset: e1.file_offset, byte_size: e1.byte_size },
        ReadDescriptor { read_fd: seg.read_fd().clone(), file_offset: e2.file_offset, byte_size: e2.byte_size },
    ];
    let records = read_batch(&descs).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].content.as_ref(), b"record one");
    assert_eq!(records[1].content.as_ref(), b"record two");
}

#[test]
fn test_concurrent_writers() {
    let dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(dir.path(), "conc", opts()).unwrap());
    let now = 1_711_234_567_890i64;
    let mut handles = Vec::new();

    for i in 0..4 {
        let wal = wal.clone();
        handles.push(std::thread::spawn(move || {
            let data = format!("thread-{}", i);
            for _ in 0..100 {
                wal.append(None, data.as_bytes(), now, false).unwrap();
            }
        }));
    }

    for h in handles { h.join().unwrap(); }

    let seg = wal.ensure_segment(now).unwrap();
    let num_threads = 4;
    let writes_per_thread = 100;
    let expected_records = num_threads * writes_per_thread;
    let record_size = 6 + 2 + 8 + "thread-0".len(); // NANORC + header_len + content_len + content
    let expected_size = 16 + (expected_records * record_size);
    assert_eq!(seg.file_size(), expected_size as u64);
}

#[test]
fn test_segment_rotation_and_recovery() {
    let dir = TempDir::new().unwrap();
    let opts_val = WalOptions {
        retention: Duration::from_secs(3600),
        segment_duration: Duration::from_secs(600),
    };

    {
        let wal = Wal::new(dir.path(), "recov", opts_val.clone()).unwrap();
        let t1 = 1_711_234_567_890i64;
        wal.append(None, b"first window", t1, true).unwrap();
        let t2 = t1 + 601_000;
        wal.append(None, b"second window", t2, true).unwrap();
    }

    let wal2 = Wal::new(dir.path(), "recov", opts()).unwrap();
    let t3 = 1_711_234_567_890i64 + 601_000 + 1000;
    wal2.append(None, b"after recovery", t3, false).unwrap();
}

#[test]
fn test_shutdown_and_cleanup() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::new(dir.path(), "shut", opts()).unwrap();
    let now = 1_711_234_567_890i64;
    wal.append(None, b"data", now, false).unwrap();
    wal.shutdown().unwrap();
    assert!(wal.append(None, b"more", now, false).is_err());
}

#[test]
fn test_sync_does_not_error() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::new(dir.path(), "syn", opts()).unwrap();
    let now = 1_711_234_567_890i64;
    wal.append(None, b"data", now, false).unwrap();
    wal.sync().unwrap();
    let seg = wal.ensure_segment(now).unwrap();
    let entry = wal.append(None, b"more", now, false).unwrap();
    let record = wal.read_at(&seg, entry.file_offset, entry.byte_size).unwrap();
    assert_eq!(record.content.as_ref(), b"more");
}

#[test]
fn test_durable_append() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::new(dir.path(), "dur", opts()).unwrap();
    let now = 1_711_234_567_890i64;
    let entry = wal.append(None, b"durable data", now, true).unwrap();
    let seg = wal.ensure_segment(now).unwrap();
    let record = wal.read_at(&seg, entry.file_offset, entry.byte_size).unwrap();
    assert_eq!(record.content.as_ref(), b"durable data");
}

#[test]
fn test_read_batch_cross_segment() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::new(dir.path(), "cross", opts()).unwrap();

    let t1 = 1_711_234_567_890i64;
    let e1 = wal.append(None, b"seg one", t1, false).unwrap();
    let seg1 = wal.ensure_segment(t1).unwrap();

    let t2 = t1 + 601_000;
    let e2 = wal.append(None, b"seg two", t2, false).unwrap();
    let seg2 = wal.ensure_segment(t2).unwrap();

    let descs = vec![
        ReadDescriptor { read_fd: seg1.read_fd().clone(), file_offset: e1.file_offset, byte_size: e1.byte_size },
        ReadDescriptor { read_fd: seg2.read_fd().clone(), file_offset: e2.file_offset, byte_size: e2.byte_size },
    ];
    let records = read_batch(&descs).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].content.as_ref(), b"seg one");
    assert_eq!(records[1].content.as_ref(), b"seg two");
}
