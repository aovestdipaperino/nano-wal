use bytes::Bytes;
use nano_wal::{Wal, WalOptions};

use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // First: Create WAL and add some data
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        let _ref1 = wal
            .append_entry("persistent_key1", None, Bytes::from("value1"), true)
            .unwrap();
        let _ref2 = wal
            .append_entry("persistent_key2", None, Bytes::from("value2"), true)
            .unwrap();
        let _ref3 = wal
            .append_entry("persistent_key1", None, Bytes::from("value1_updated"), true)
            .unwrap();
        // WAL goes out of scope here, simulating process termination
    }

    // Second: Create new WAL instance from same directory
    {
        let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

        // Verify data persisted
        let records1: Vec<Bytes> = wal.enumerate_records("persistent_key1").unwrap().collect();
        assert_eq!(records1.len(), 2);
        assert_eq!(records1[0], Bytes::from("value1"));
        assert_eq!(records1[1], Bytes::from("value1_updated"));

        let records2: Vec<Bytes> = wal.enumerate_records("persistent_key2").unwrap().collect();
        assert_eq!(records2.len(), 1);
        assert_eq!(records2[0], Bytes::from("value2"));

        let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"persistent_key1".to_string()));
        assert!(keys.contains(&"persistent_key2".to_string()));
    }
}

#[test]
fn test_simulated_process_kill_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Simulate a process that writes data and then gets "killed" (scope ends abruptly)
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

        // Write critical data with durability
        let _ref1 = wal
            .append_entry("critical_key1", None, Bytes::from("important_data_1"), true)
            .unwrap();
        let _ref2 = wal
            .append_entry("critical_key2", None, Bytes::from("important_data_2"), true)
            .unwrap();
        let _ref3 = wal
            .append_entry(
                "critical_key1",
                None,
                Bytes::from("updated_important_data"),
                true,
            )
            .unwrap();

        // Write some non-durable data
        let _ref4 = wal
            .append_entry("temp_key", None, Bytes::from("temp_data"), false)
            .unwrap();

        // Force sync to ensure data is written
        wal.sync().unwrap();

        // Simulate process termination by dropping WAL without proper shutdown
        // This tests that data persists even when shutdown() is not called
    } // WAL is dropped here, simulating abrupt process termination

    // Wait a moment to simulate time between process death and restart
    thread::sleep(Duration::from_millis(100));

    // Simulate new process starting - create new WAL instance from same directory
    {
        let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

        // Verify all durable data persisted
        let records1: Vec<Bytes> = wal.enumerate_records("critical_key1").unwrap().collect();
        assert_eq!(records1.len(), 2, "Should have 2 records for critical_key1");
        assert_eq!(records1[0], Bytes::from("important_data_1"));
        assert_eq!(records1[1], Bytes::from("updated_important_data"));

        let records2: Vec<Bytes> = wal.enumerate_records("critical_key2").unwrap().collect();
        assert_eq!(records2.len(), 1, "Should have 1 record for critical_key2");
        assert_eq!(records2[0], Bytes::from("important_data_2"));

        // Verify temp data also persisted (even though not explicitly synced)
        let temp_records: Vec<Bytes> = wal.enumerate_records("temp_key").unwrap().collect();
        assert_eq!(temp_records.len(), 1, "Should have 1 record for temp_key");
        assert_eq!(temp_records[0], Bytes::from("temp_data"));

        // Verify key enumeration works
        let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
        assert_eq!(keys.len(), 3, "Should have 3 unique keys");
        assert!(keys.contains(&"critical_key1".to_string()));
        assert!(keys.contains(&"critical_key2".to_string()));
        assert!(keys.contains(&"temp_key".to_string()));

        // Verify keys are available (active segments are only created on write)
        let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
        assert_eq!(keys.len(), 3, "Should have 3 unique keys");
    }
}

#[test]
fn test_multiple_restart_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // First process
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        let _ref1 = wal
            .append_entry("session1", None, Bytes::from("data1"), true)
            .unwrap();
    }

    // Second process
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        let _ref2 = wal
            .append_entry("session2", None, Bytes::from("data2"), true)
            .unwrap();

        // Verify first session data is still there
        let session1_records: Vec<Bytes> = wal.enumerate_records("session1").unwrap().collect();
        assert_eq!(session1_records.len(), 1);
    }

    // Third process
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
        let _ref3 = wal
            .append_entry("session1", None, Bytes::from("data1_updated"), true)
            .unwrap();
        let _ref4 = wal
            .append_entry("session3", None, Bytes::from("data3"), true)
            .unwrap();

        // Verify all data from all sessions
        let session1_records: Vec<Bytes> = wal.enumerate_records("session1").unwrap().collect();
        assert_eq!(session1_records.len(), 2);

        let session2_records: Vec<Bytes> = wal.enumerate_records("session2").unwrap().collect();
        assert_eq!(session2_records.len(), 1);

        let session3_records: Vec<Bytes> = wal.enumerate_records("session3").unwrap().collect();
        assert_eq!(session3_records.len(), 1);

        // Verify all keys are available
        let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
        assert_eq!(keys.len(), 3, "Should have 3 unique keys");
    }
}

#[test]
fn test_crash_recovery_with_partial_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Simulate a crash scenario where some data might be partially written
    {
        let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

        // Write some complete entries
        let _ref1 = wal
            .append_entry("complete1", None, Bytes::from("complete_data_1"), true)
            .unwrap();
        let _ref2 = wal
            .append_entry("complete2", None, Bytes::from("complete_data_2"), true)
            .unwrap();

        // Write some entries without explicit sync (might be lost)
        let _ref3 = wal
            .append_entry("maybe_lost", None, Bytes::from("might_be_lost"), false)
            .unwrap();

        // Write one more durable entry
        let _ref4 = wal
            .append_entry("complete3", None, Bytes::from("complete_data_3"), true)
            .unwrap();

        // Simulate crash by dropping without shutdown
    }

    // Recovery: Create new WAL instance
    {
        let wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

        // Verify at least the durable entries survived
        let complete1_records: Vec<Bytes> = wal.enumerate_records("complete1").unwrap().collect();
        assert_eq!(complete1_records.len(), 1);
        assert_eq!(complete1_records[0], Bytes::from("complete_data_1"));

        let complete2_records: Vec<Bytes> = wal.enumerate_records("complete2").unwrap().collect();
        assert_eq!(complete2_records.len(), 1);
        assert_eq!(complete2_records[0], Bytes::from("complete_data_2"));

        let complete3_records: Vec<Bytes> = wal.enumerate_records("complete3").unwrap().collect();
        assert_eq!(complete3_records.len(), 1);
        assert_eq!(complete3_records[0], Bytes::from("complete_data_3"));

        // The non-durable entry might or might not be there depending on OS buffering
        let _maybe_lost_records: Vec<Bytes> =
            wal.enumerate_records("maybe_lost").unwrap().collect();
        // We don't assert on this since it's implementation-dependent

        // But we should have at least the durable entries available
        let keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
        assert!(keys.len() >= 3, "Should have at least 3 keys available");
    }
}
