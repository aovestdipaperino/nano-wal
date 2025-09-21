use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use nano_wal::{Wal, WalOptions};
use std::time::Duration;
use tempfile::TempDir;

fn bench_append_entry(c: &mut Criterion) {
    c.bench_function("append_entry_non_durable", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                (wal, temp_dir)
            },
            |(mut wal, _temp_dir)| {
                let content = Bytes::from("test data for benchmarking");
                wal.append_entry(
                    black_box("bench_key"),
                    black_box(None),
                    black_box(content),
                    black_box(false),
                )
                .unwrap()
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("append_entry_durable", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                (wal, temp_dir)
            },
            |(mut wal, _temp_dir)| {
                let content = Bytes::from("test data for benchmarking");
                wal.append_entry(
                    black_box("bench_key"),
                    black_box(None),
                    black_box(content),
                    black_box(true),
                )
                .unwrap()
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_batch_operations(c: &mut Criterion) {
    c.bench_function("append_batch_10_entries", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                let entries: Vec<_> = (0..10)
                    .map(|i| {
                        (
                            format!("key_{}", i),
                            None,
                            Bytes::from(format!("data_{}", i)),
                        )
                    })
                    .collect();
                (wal, entries, temp_dir)
            },
            |(mut wal, entries, _temp_dir)| wal.append_batch(entries, black_box(false)).unwrap(),
            BatchSize::SmallInput,
        );
    });

    c.bench_function("append_batch_100_entries", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                let entries: Vec<_> = (0..100)
                    .map(|i| {
                        (
                            format!("key_{}", i % 10), // Use 10 different keys
                            None,
                            Bytes::from(format!("data_{}", i)),
                        )
                    })
                    .collect();
                (wal, entries, temp_dir)
            },
            |(mut wal, entries, _temp_dir)| wal.append_batch(entries, black_box(false)).unwrap(),
            BatchSize::SmallInput,
        );
    });
}

fn bench_read_operations(c: &mut Criterion) {
    c.bench_function("read_entry_at", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();

        // Prepare test data
        let mut refs = Vec::new();
        for i in 0..100 {
            let content = Bytes::from(format!("test data {}", i));
            refs.push(
                wal.append_entry(&format!("key_{}", i % 10), None, content, false)
                    .unwrap(),
            );
        }

        let mut idx = 0;
        b.iter(|| {
            let entry_ref = &refs[idx % refs.len()];
            idx += 1;
            wal.read_entry_at(black_box(*entry_ref)).unwrap()
        });
    });

    c.bench_function("enumerate_records", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();

        // Prepare test data
        for i in 0..100 {
            let content = Bytes::from(format!("test data {}", i));
            wal.append_entry("test_key", None, content, false).unwrap();
        }

        b.iter(|| {
            let records: Vec<_> = wal
                .enumerate_records(black_box("test_key"))
                .unwrap()
                .collect();
            black_box(records)
        });
    });
}

fn bench_with_headers(c: &mut Criterion) {
    c.bench_function("append_with_small_header", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                (wal, temp_dir)
            },
            |(mut wal, _temp_dir)| {
                let header = Bytes::from("metadata");
                let content = Bytes::from("test data for benchmarking");
                wal.append_entry(
                    black_box("bench_key"),
                    black_box(Some(header)),
                    black_box(content),
                    black_box(false),
                )
                .unwrap()
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("append_with_large_header", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal =
                    Wal::new(temp_dir.path().to_str().unwrap(), WalOptions::default()).unwrap();
                let header = Bytes::from(vec![b'x'; 1024]); // 1KB header
                (wal, header, temp_dir)
            },
            |(mut wal, header, _temp_dir)| {
                let content = Bytes::from("test data for benchmarking");
                wal.append_entry(
                    black_box("bench_key"),
                    black_box(Some(header)),
                    black_box(content),
                    black_box(false),
                )
                .unwrap()
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_segment_rotation(c: &mut Criterion) {
    c.bench_function("segment_rotation", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                // Short retention to force rotation
                let options = WalOptions::default()
                    .retention(Duration::from_millis(1))
                    .segments_per_retention_period(1);
                let wal = Wal::new(temp_dir.path().to_str().unwrap(), options).unwrap();
                (wal, temp_dir)
            },
            |(mut wal, _temp_dir)| {
                // This should trigger rotation on each iteration
                std::thread::sleep(Duration::from_millis(2));
                let content = Bytes::from("test data");
                wal.append_entry(
                    black_box("rotating_key"),
                    black_box(None),
                    black_box(content),
                    black_box(false),
                )
                .unwrap()
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_compact(c: &mut Criterion) {
    c.bench_function("compact", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let options = WalOptions::default()
                    .retention(Duration::from_millis(1))
                    .segments_per_retention_period(2);
                let mut wal = Wal::new(temp_dir.path().to_str().unwrap(), options).unwrap();

                // Create some expired segments
                for i in 0..10 {
                    let content = Bytes::from(format!("test data {}", i));
                    wal.append_entry(&format!("key_{}", i), None, content, false)
                        .unwrap();
                    std::thread::sleep(Duration::from_millis(1));
                }

                (wal, temp_dir)
            },
            |(mut wal, _temp_dir)| wal.compact().unwrap(),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_append_entry,
    bench_batch_operations,
    bench_read_operations,
    bench_with_headers,
    bench_segment_rotation,
    bench_compact
);
criterion_main!(benches);
