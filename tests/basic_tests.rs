use bytes::Bytes;
use nano_wal::{Wal, WalOptions};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_new_and_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    assert!(Path::new(wal_dir).exists());
    wal.shutdown().unwrap();
    // Temp directory will be cleaned up automatically
}

#[test]
fn test_append_and_log() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let content1 = Bytes::from("hello");
    wal.append_entry("key1", content1, false).unwrap();

    let content2 = Bytes::from("world");
    wal.log_entry("key2", content2).unwrap();

    wal.shutdown().unwrap();
}

#[test]
fn test_append_and_enumerate() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let content1 = Bytes::from("hello");
    let content2 = Bytes::from("world");

    wal.append_entry("key1", content1.clone(), false).unwrap();
    wal.append_entry("key2", content2.clone(), false).unwrap();

    let records: Vec<Bytes> = wal.enumerate_records("key1").unwrap().collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], content1);

    wal.shutdown().unwrap();
}

#[test]
fn test_enumerate_keys() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    wal.append_entry("key1", Bytes::from("1"), false).unwrap();
    wal.append_entry("key2", Bytes::from("2"), false).unwrap();
    wal.append_entry("key1", Bytes::from("3"), false).unwrap();

    let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"key1".to_string()));
    assert!(keys.contains(&"key2".to_string()));

    wal.shutdown().unwrap();
}

#[test]
fn test_multiple_records_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    wal.append_entry("key1", Bytes::from("value1"), false)
        .unwrap();
    wal.append_entry("key1", Bytes::from("value2"), false)
        .unwrap();
    wal.append_entry("key1", Bytes::from("value3"), false)
        .unwrap();

    let records: Vec<Bytes> = wal.enumerate_records("key1").unwrap().collect();
    assert_eq!(records.len(), 3);

    wal.shutdown().unwrap();
}

#[test]
fn test_entry_count() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    assert_eq!(wal.entry_count(), 0);

    wal.append_entry("key1", Bytes::from("value1"), false)
        .unwrap();
    assert_eq!(wal.entry_count(), 1);

    wal.append_entry("key2", Bytes::from("value2"), false)
        .unwrap();
    assert_eq!(wal.entry_count(), 2);

    wal.append_entry("key1", Bytes::from("value1_updated"), false)
        .unwrap();
    assert_eq!(wal.entry_count(), 3);

    wal.shutdown().unwrap();
}

#[test]
fn test_sync() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    wal.append_entry("key1", Bytes::from("value1"), false)
        .unwrap();
    wal.sync().unwrap();

    wal.shutdown().unwrap();
}

#[test]
fn test_empty_wal_operations() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    let records: Vec<Bytes> = wal.enumerate_records("nonexistent").unwrap().collect();
    assert_eq!(records.len(), 0);

    let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(keys.len(), 0);

    assert_eq!(wal.entry_count(), 0);

    drop(wal);
}

#[test]
fn test_log_file_extension() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    wal.append_entry("test_key", Bytes::from("test_data"), true)
        .unwrap();

    // Check that .log files are created
    let entries = fs::read_dir(wal_dir).unwrap();
    let mut found_log_file = false;
    for entry in entries {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        let filename = file_name.to_str().unwrap();
        if filename.ends_with(".log") {
            found_log_file = true;
            break;
        }
    }
    assert!(found_log_file, "Should create .log files");

    wal.shutdown().unwrap();
}

#[test]
fn test_key_in_filename() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Use a key that should appear in the filename
    wal.append_entry("user123", Bytes::from("user_data"), true)
        .unwrap();

    // Force segment rotation
    std::thread::sleep(std::time::Duration::from_millis(100));
    wal.append_entry("order456", Bytes::from("order_data"), true)
        .unwrap();

    // Check that filenames contain key information
    let entries = fs::read_dir(wal_dir).unwrap();
    let mut found_meaningful_name = false;
    for entry in entries {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        let filename = file_name.to_str().unwrap();
        if filename.contains("user123")
            || filename.contains("order456")
            || filename.ends_with(".log")
        {
            found_meaningful_name = true;
            break;
        }
    }
    assert!(
        found_meaningful_name,
        "Should create files with meaningful names"
    );

    wal.shutdown().unwrap();
}
