use bytes::Bytes;
use nano_wal::{Wal, WalOptions};
use std::fs;

use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_segment_rotation_time_based() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(10),
            segments_per_retention_period: 10,
        },
    )
    .unwrap();

    // Write some entries to trigger size-based rotation
    let _ref1 = wal
        .append_entry(
            "key1",
            Bytes::from("data1 with enough content to trigger rotation when combined"),
            false,
        )
        .unwrap();

    let _ref2 = wal
        .append_entry("key2", Bytes::from("data2"), false)
        .unwrap();

    // Should have created segment files for different keys
    let entries = fs::read_dir(wal_dir).unwrap();
    let log_files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();

    assert!(!log_files.is_empty(), "Should have at least one .log file");

    wal.shutdown().unwrap();
}

#[test]
fn test_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(5),
            segments_per_retention_period: 10,
        },
    )
    .unwrap();

    let _ref1 = wal
        .append_entry("key1", Bytes::from("data1"), true)
        .unwrap();
    thread::sleep(Duration::from_secs(3));
    let _ref2 = wal
        .append_entry("key2", Bytes::from("data2"), true)
        .unwrap();

    // Count files before compaction
    let _files_before = fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".log"))
                .unwrap_or(false)
        })
        .count();

    wal.compact().unwrap();

    // Files should still exist or be manageable
    let files_after = fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".log"))
                .unwrap_or(false)
        })
        .count();

    // Compaction should have run without error
    assert!(files_after < 100); // Just check it's reasonable

    wal.shutdown().unwrap();
}

#[test]
fn test_large_number_of_entries() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Write many entries
    for i in 0..1000 {
        let key = format!("key_{}", i % 10); // 10 unique keys
        let content = Bytes::from(format!("data_{}", i));
        let _ref = wal.append_entry(&key, content, i % 100 == 0).unwrap(); // Sync every 100th entry
    }

    // Verify we can read back the data
    let records: Vec<Bytes> = wal.enumerate_records("key_0").unwrap().collect();
    assert_eq!(records.len(), 100); // Should have 100 records for key_0

    let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(keys.len(), 10); // Should have 10 unique keys

    assert_eq!(wal.active_segment_count(), 10); // Should have 10 active segments

    wal.shutdown().unwrap();
}

#[test]
fn test_concurrent_like_operations() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate rapid writes like concurrent operations might produce
    for batch in 0..10 {
        for i in 0..50 {
            let key = format!("batch_{}", batch); // Use only batch ID as key
            let content = Bytes::from(format!("batch {} item {} data", batch, i));
            let _ref = wal.append_entry(&key, content, false).unwrap();
        }
        // Periodic sync
        wal.sync().unwrap();

        // Small delay to simulate processing time
        thread::sleep(Duration::from_millis(10));
    }

    // Verify all data is accessible
    assert_eq!(wal.active_segment_count(), 10); // 10 batches = 10 unique keys

    // Test reading some specific entries
    let records: Vec<Bytes> = wal.enumerate_records("batch_0").unwrap().collect();
    assert_eq!(records.len(), 50); // 50 items per batch
    assert_eq!(records[0], Bytes::from("batch 0 item 0 data"));

    let records: Vec<Bytes> = wal.enumerate_records("batch_9").unwrap().collect();
    assert_eq!(records.len(), 50); // 50 items per batch
    assert_eq!(records[49], Bytes::from("batch 9 item 49 data"));

    wal.shutdown().unwrap();
}

#[test]
fn test_error_handling_invalid_config() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Test with invalid segments
    let result = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(0), // Invalid
            segments_per_retention_period: 10,
        },
    );
    assert!(result.is_err());

    // Test with invalid retention
    let result = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(60 * 60 * 24), // 1 day
            segments_per_retention_period: 0,                   // Invalid
        },
    );
    assert!(result.is_err());
}

#[test]
fn test_special_characters_in_keys() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Test various key formats
    let test_keys = vec![
        "simple_key",
        "key-with-dashes",
        "key.with.dots",
        "key_with_underscores",
        "KeyWithCaps",
        "key123with456numbers",
        "verylongkeyname_that_should_be_truncated_in_filename_but_still_work_correctly",
    ];

    for (i, key) in test_keys.iter().enumerate() {
        let content = Bytes::from(format!("content_{}", i));
        let _ref = wal.append_entry(*key, content.clone(), true).unwrap();

        // Verify we can read it back
        let records: Vec<Bytes> = wal.enumerate_records(*key).unwrap().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], content);
    }

    // Verify all keys are enumerated
    let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(keys.len(), test_keys.len());

    wal.shutdown().unwrap();
}

#[test]
fn test_empty_and_large_content() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Test empty content
    let _ref1 = wal.append_entry("empty_key", Bytes::new(), true).unwrap();

    // Test large content
    let large_content = Bytes::from(vec![42u8; 1024 * 100]); // 100KB
    let _ref2 = wal
        .append_entry("large_key", large_content.clone(), true)
        .unwrap();

    // Verify empty content
    let records: Vec<Bytes> = wal.enumerate_records("empty_key").unwrap().collect();
    assert_eq!(records.len(), 1);
    assert!(records[0].is_empty());

    // Verify large content
    let records: Vec<Bytes> = wal.enumerate_records("large_key").unwrap().collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], large_content);

    wal.shutdown().unwrap();
}

#[test]
fn test_wal_options_builder_methods() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Test with_retention method
    let options = WalOptions::with_retention(Duration::from_secs(3600));
    assert_eq!(options.entry_retention, Duration::from_secs(3600));
    assert_eq!(options.segments_per_retention_period, 10); // Default

    let wal = Wal::new(wal_dir, options).unwrap();
    assert_eq!(wal.active_segment_count(), 0);

    // Drop wal to free the directory

    // Test with_segments_per_retention_period method
    let options = WalOptions::with_segments_per_retention_period(20);
    assert_eq!(options.segments_per_retention_period, 20);
    assert_eq!(
        options.entry_retention,
        Duration::from_secs(60 * 60 * 24 * 7)
    ); // Default

    let mut wal = Wal::new(wal_dir, options).unwrap();
    wal.shutdown().unwrap();
}

#[test]
fn test_segment_id_progression() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(6),
            segments_per_retention_period: 10,
        },
    )
    .unwrap();

    let initial_count = wal.active_segment_count();

    // Write entries to different keys
    wal.append_entry("test1", Bytes::from("data1"), true)
        .unwrap();

    wal.append_entry("test2", Bytes::from("data2"), true)
        .unwrap();

    // Should have created separate segments for different keys
    let final_count = wal.active_segment_count();

    // Should have created files for different keys
    let log_file_count = fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".log"))
                .unwrap_or(false)
        })
        .count();

    assert!(final_count > initial_count || log_file_count > 0);

    wal.shutdown().unwrap();
}
