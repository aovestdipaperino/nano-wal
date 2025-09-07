use bytes::Bytes;
use nano_wal::{EntryRef, Wal, WalOptions};

use tempfile::TempDir;

#[test]
fn test_append_returns_entry_ref() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    let entry_ref = wal
        .append_entry("test_key", Bytes::from("test_value"), true)
        .unwrap();

    // Verify EntryRef structure
    assert!(entry_ref.key_hash > 0);
    assert_eq!(entry_ref.sequence_number, 1);
    // offset is u64, so it's always >= 0

    wal.shutdown().unwrap();
}

#[test]
fn test_log_entry_returns_entry_ref() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    let entry_ref = wal
        .log_entry("test_key", Bytes::from("test_value"))
        .unwrap();

    // Verify EntryRef structure
    assert!(entry_ref.key_hash > 0);
    assert_eq!(entry_ref.sequence_number, 1);
    // offset is u64, so it's always >= 0

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_valid_reference() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    let test_data = Bytes::from("hello world");
    let entry_ref = wal
        .append_entry("test_key", test_data.clone(), true)
        .unwrap();

    // Read the entry using the reference
    let retrieved_data = wal.read_entry_at(entry_ref).unwrap();
    assert_eq!(retrieved_data, test_data);

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_multiple_entries() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Write multiple entries
    let data1 = Bytes::from("first entry");
    let data2 = Bytes::from("second entry");
    let data3 = Bytes::from("third entry");

    let ref1 = wal.append_entry("key1", data1.clone(), true).unwrap();
    let ref2 = wal.append_entry("key2", data2.clone(), true).unwrap();
    let ref3 = wal.append_entry("key3", data3.clone(), true).unwrap();

    // Read entries in random order
    assert_eq!(wal.read_entry_at(ref3).unwrap(), data3);
    assert_eq!(wal.read_entry_at(ref1).unwrap(), data1);
    assert_eq!(wal.read_entry_at(ref2).unwrap(), data2);

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let entry_ref;
    let test_data = Bytes::from("persistent data");

    // First: Create WAL and write data
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        entry_ref = wal
            .append_entry("persistent_key", test_data.clone(), true)
            .unwrap();
    }

    // Second: Create new WAL instance and read using reference
    {
        let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        let retrieved_data = wal.read_entry_at(entry_ref).unwrap();
        assert_eq!(retrieved_data, test_data);
    }

    // Cleanup
    std::fs::remove_dir_all(wal_dir).unwrap();
}

#[test]
fn test_read_entry_at_invalid_signature() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Write some data first
    let _entry_ref = wal
        .append_entry("test_key", Bytes::from("test_data"), true)
        .unwrap();

    // Create an invalid reference pointing to a non-existent key/sequence
    let invalid_ref = EntryRef {
        key_hash: 99999,        // Non-existent key hash
        sequence_number: 99999, // Non-existent sequence
        offset: 0,
    };

    // Should return error due to missing segment file
    let result = wal.read_entry_at(invalid_ref);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_nonexistent_segment() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Create a reference to a non-existent segment
    let invalid_ref = EntryRef {
        key_hash: 999999,
        sequence_number: 999999,
        offset: 0,
    };

    // Should return error due to missing segment file
    let result = wal.read_entry_at(invalid_ref);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);

    drop(wal);
}

#[test]
fn test_read_entry_at_with_different_key_types() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Test with different key types
    let string_data = Bytes::from("string key data");
    let number_data = Bytes::from("number key data");

    let string_ref = wal
        .append_entry("string_key", string_data.clone(), true)
        .unwrap();
    let number_ref = wal
        .append_entry("12345", number_data.clone(), true)
        .unwrap();

    // Verify we can read both entries
    assert_eq!(wal.read_entry_at(string_ref).unwrap(), string_data);
    assert_eq!(wal.read_entry_at(number_ref).unwrap(), number_data);

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_large_content() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Create large content (100KB)
    let large_data = Bytes::from(vec![42u8; 1024 * 100]);
    let entry_ref = wal
        .append_entry("large_key", large_data.clone(), true)
        .unwrap();

    // Verify we can read the large entry
    let retrieved_data = wal.read_entry_at(entry_ref).unwrap();
    assert_eq!(retrieved_data, large_data);

    wal.shutdown().unwrap();
}

#[test]
fn test_read_entry_at_empty_content() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Test with empty content
    let empty_data = Bytes::new();
    let entry_ref = wal
        .append_entry("empty_key", empty_data.clone(), true)
        .unwrap();

    // Verify we can read the empty entry
    let retrieved_data = wal.read_entry_at(entry_ref).unwrap();
    assert_eq!(retrieved_data, empty_data);

    wal.shutdown().unwrap();
}

#[test]
fn test_entry_ref_across_segment_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: std::time::Duration::from_secs(10),
            max_segment_size: 50, // Small segments to force rotation
        },
    )
    .unwrap();

    let data1 = Bytes::from("data in first segment that is long enough to trigger rotation");
    let ref1 = wal.append_entry("key1", data1.clone(), true).unwrap();

    // Add more data to trigger segment rotation
    let _extra_data = wal
        .append_entry("key1", Bytes::from("extra data to trigger rotation"), true)
        .unwrap();

    let data2 = Bytes::from("data in second segment");
    let ref2 = wal.append_entry("key2", data2.clone(), true).unwrap();

    // Both references should work even across segments
    assert_eq!(wal.read_entry_at(ref1).unwrap(), data1);
    assert_eq!(wal.read_entry_at(ref2).unwrap(), data2);

    // Key hashes should be different
    assert_ne!(ref1.key_hash, ref2.key_hash);

    wal.shutdown().unwrap();
}

#[test]
fn test_entry_ref_serialization_compatibility() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    let test_data = Bytes::from("serialization test");
    let entry_ref = wal
        .append_entry("serial_key", test_data.clone(), true)
        .unwrap();

    // EntryRef should be copyable and comparable
    let copied_ref = entry_ref;
    assert_eq!(entry_ref, copied_ref);
    assert_eq!(entry_ref.key_hash, copied_ref.key_hash);
    assert_eq!(entry_ref.sequence_number, copied_ref.sequence_number);
    assert_eq!(entry_ref.offset, copied_ref.offset);

    // Should be able to read using copied reference
    assert_eq!(wal.read_entry_at(copied_ref).unwrap(), test_data);

    wal.shutdown().unwrap();
}

#[test]
fn test_random_access_performance_characteristics() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Write multiple entries and collect their references
    let mut refs = Vec::new();
    for i in 0..100 {
        let data = Bytes::from(format!("entry_{}", i));
        let entry_ref = wal
            .append_entry(&format!("key_{}", i), data, false)
            .unwrap();
        refs.push(entry_ref);
    }

    wal.sync().unwrap();

    // Random access should work regardless of order
    for &entry_ref in refs.iter().rev() {
        let data = wal.read_entry_at(entry_ref).unwrap();
        // Verify the data format
        assert!(String::from_utf8_lossy(&data).starts_with("entry_"));
    }

    wal.shutdown().unwrap();
}
