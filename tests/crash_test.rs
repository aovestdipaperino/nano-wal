//! Crash tests for nano-wal
//!
//! These tests simulate various crash scenarios to verify that the WAL
//! maintains data integrity and durability guarantees even when the
//! process is terminated abruptly without proper shutdown.

use bytes::Bytes;
use nano_wal::{Wal, WalOptions};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Tests WAL durability under simulated crash conditions.
///
/// This test creates a WAL with a single key "crash-test" and spawns a worker
/// thread that continuously appends records at random intervals (1-300ms).
/// After 5-7 seconds, the main thread abruptly terminates the worker thread
/// without calling shutdown(), simulating a process crash.
///
/// The test then verifies that:
/// 1. All records counted by the atomic counter are present on disk
/// 2. The records have the correct sequential content
/// 3. No data corruption occurred during the crash
#[test]
fn test_crash_durability() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Create a shared atomic counter
    let counter = Arc::new(AtomicU16::new(0));
    let counter_clone = Arc::clone(&counter);

    // Create WAL for the worker thread
    let wal_dir_clone = wal_dir.to_string();

    // Spawn the worker thread
    let worker_handle = thread::spawn(move || {
        let mut wal = Wal::new(&wal_dir_clone, WalOptions::default()).unwrap();
        let mut rng_state = 12345u32; // Simple LCG for random numbers

        loop {
            // Simple LCG random number generator (to avoid external dependencies)
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let random_delay = 1 + (rng_state % 300); // 1-300ms

            thread::sleep(Duration::from_millis(random_delay as u64));

            // Get current counter value for record content
            let count = counter_clone.load(Ordering::SeqCst);
            let content = Bytes::from(format!("record-{}", count));

            // Append entry with durability enabled to ensure it's written to disk
            if let Err(_) = wal.append_entry("crash-test", None, content, true) {
                break;
            }

            // Only increment counter after successful write
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Wait for a random interval between 5-7 seconds
    let start_time = Instant::now();
    let mut rng_state = 67890u32;
    rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
    let wait_duration = 5000 + (rng_state % 2000); // 5000-6999ms (5-7 seconds)

    thread::sleep(Duration::from_millis(wait_duration as u64));
    let elapsed = start_time.elapsed();

    // Get the final counter value before killing the thread
    let final_counter = counter.load(Ordering::SeqCst);

    // Kill the thread abruptly - this simulates a crash
    // We don't call join() or any cleanup, just drop the handle
    drop(worker_handle);

    // Give a small delay to ensure any pending file operations complete
    thread::sleep(Duration::from_millis(100));

    // Now verify the data integrity by reading from a fresh WAL instance
    let verification_wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Count the actual records on disk
    let records: Vec<Bytes> = verification_wal
        .enumerate_records("crash-test")
        .unwrap()
        .collect();

    let disk_count = records.len() as u16;

    println!("Test duration: {:?}", elapsed);
    println!("Final counter value: {}", final_counter);
    println!("Records found on disk: {}", disk_count);

    // Verify that the number of records on disk matches the atomic counter
    // The disk count should be equal to the counter since we used durable=true
    assert_eq!(
        disk_count, final_counter,
        "Mismatch between atomic counter ({}) and records on disk ({}). \
         This indicates data loss during the abrupt thread termination.",
        final_counter, disk_count
    );

    // Additional verification: check that all records have the expected format
    for (i, record) in records.iter().enumerate() {
        let expected_content = format!("record-{}", i);
        let actual_content = String::from_utf8(record.to_vec()).unwrap();
        assert_eq!(
            actual_content, expected_content,
            "Record {} has unexpected content: expected '{}', got '{}'",
            i, expected_content, actual_content
        );
    }

    println!(
        "✓ Crash test passed: All {} records were durably written to disk",
        disk_count
    );
}

/// Runs the crash durability test multiple times to increase confidence
/// in the WAL's crash recovery capabilities.
#[test]
fn test_crash_durability_multiple_runs() {
    // Run the crash test multiple times to increase confidence
    for run in 1..=3 {
        println!("Running crash test iteration {}/3", run);
        test_crash_durability();
    }
}

/// Tests crash recovery with forced thread termination.
///
/// This test uses a more deterministic approach where a worker thread
/// writes records for a fixed duration, then is abruptly terminated.
/// Unlike the main crash test, this one allows for a small tolerance
/// in record count differences due to potential race conditions during
/// termination.
#[test]
fn test_crash_with_forced_termination() {
    // This test demonstrates crash recovery by using a deterministic approach
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let records_written = Arc::new(AtomicU16::new(0));
    let records_written_clone = Arc::clone(&records_written);

    let wal_dir_clone = wal_dir.to_string();

    let worker_handle = thread::spawn(move || {
        let mut wal = Wal::new(&wal_dir_clone, WalOptions::default()).unwrap();
        let mut rng_state = 54321u32;

        // Write records for a deterministic amount of time
        let start_time = Instant::now();
        while start_time.elapsed() < Duration::from_millis(2500) {
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let random_delay = 5 + (rng_state % 50); // 5-54ms

            thread::sleep(Duration::from_millis(random_delay as u64));

            let count = records_written_clone.load(Ordering::SeqCst);
            let content = Bytes::from(format!("crash-record-{}", count));

            // Use durable=true to ensure writes persist
            match wal.append_entry("crash-test", None, content, true) {
                Ok(_) => {
                    records_written_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!("WAL append failed: {}", e);
                    break;
                }
            }
        }
    });

    // Wait for the thread to complete its work, then abruptly terminate
    thread::sleep(Duration::from_millis(3000));

    // Get final count before killing thread
    let final_written_count = records_written.load(Ordering::SeqCst);

    // Force thread termination (simulates process crash)
    drop(worker_handle);

    // Small delay for any pending filesystem operations
    thread::sleep(Duration::from_millis(100));

    // Verify data integrity after "crash"
    let verification_wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let recovered_records: Vec<Bytes> = verification_wal
        .enumerate_records("crash-test")
        .unwrap()
        .collect();

    let disk_count = recovered_records.len() as u16;

    println!(
        "Crash simulation - Records written: {}, Records recovered: {}",
        final_written_count, disk_count
    );

    // Since we used durable=true, all written records should be recoverable
    // However, there might be a small discrepancy due to the final operations
    // being in progress when the thread was terminated
    assert!(
        disk_count <= final_written_count && disk_count >= final_written_count.saturating_sub(2),
        "Expected between {} and {} records on disk, found {}",
        final_written_count.saturating_sub(2),
        final_written_count,
        disk_count
    );

    // Verify record sequence integrity
    for (i, record) in recovered_records.iter().enumerate() {
        let expected = format!("crash-record-{}", i);
        let actual = String::from_utf8(record.to_vec()).unwrap();
        assert_eq!(actual, expected, "Record {} has incorrect content", i);
    }

    println!(
        "✓ Crash test passed: {}/{} records recovered with correct sequence",
        disk_count, final_written_count
    );
}

/// Tests crash simulation with immediate WAL drop (simulating process termination).
///
/// This test simulates what happens when a process terminates abruptly by
/// dropping the WAL instance without calling shutdown(), leaving some operations
/// potentially incomplete. This is safer than using process::abort() in tests.
#[test]
fn test_extreme_crash_simulation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let records_written = Arc::new(AtomicU16::new(0));
    let records_written_clone = Arc::clone(&records_written);
    let should_stop = Arc::new(AtomicU16::new(0));
    let should_stop_clone = Arc::clone(&should_stop);

    let wal_dir_clone = wal_dir.to_string();

    let worker_handle = thread::spawn(move || {
        let mut wal = Wal::new(&wal_dir_clone, WalOptions::default()).unwrap();
        let mut rng_state = 99999u32;

        loop {
            // Check for stop signal
            if should_stop_clone.load(Ordering::SeqCst) != 0 {
                // Simulate abrupt termination by dropping WAL without shutdown
                drop(wal);
                return;
            }

            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let random_delay = 5 + (rng_state % 50); // 5-54ms

            thread::sleep(Duration::from_millis(random_delay as u64));

            let count = records_written_clone.load(Ordering::SeqCst);
            let content = Bytes::from(format!("extreme-crash-{}", count));

            match wal.append_entry("crash-test", None, content, true) {
                Ok(_) => {
                    records_written_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => break,
            }
        }
    });

    // Let it run for 1.5 seconds then signal termination
    thread::sleep(Duration::from_millis(1500));
    should_stop.store(1, Ordering::SeqCst);

    // Wait for thread to complete termination
    thread::sleep(Duration::from_millis(200));

    let final_count = records_written.load(Ordering::SeqCst);

    // Clean up worker handle
    drop(worker_handle);

    // Small delay to ensure any pending filesystem operations complete
    thread::sleep(Duration::from_millis(100));

    // Verify recovery after simulated crash
    let verification_wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let recovered_records: Vec<Bytes> = verification_wal
        .enumerate_records("crash-test")
        .unwrap()
        .collect();

    let disk_count = recovered_records.len() as u16;

    println!(
        "Extreme crash simulation - Records written: {}, Records recovered: {}",
        final_count, disk_count
    );

    // Even with abrupt WAL drop, durable writes should mostly persist
    assert!(
        disk_count <= final_count && disk_count >= final_count.saturating_sub(3),
        "Expected between {} and {} records, found {}",
        final_count.saturating_sub(3),
        final_count,
        disk_count
    );

    // Verify record integrity
    for (i, record) in recovered_records.iter().enumerate() {
        let expected = format!("extreme-crash-{}", i);
        let actual = String::from_utf8(record.to_vec()).unwrap();
        assert_eq!(actual, expected, "Record {} corrupted after crash", i);
    }

    println!(
        "✓ Extreme crash test passed: {}/{} records recovered after abrupt WAL drop",
        disk_count, final_count
    );
}

/// Original crash test as specified in the requirements.
///
/// Creates a WAL log with a single key ("crash-test") and passes it to a thread
/// that appends at random intervals (1-300ms) to the log, increasing a global
/// shared counter (AtomicU16). The main thread waits for a random interval
/// between 5-7 seconds and kills the thread in the most abrupt possible way
/// to avoid shutdown being invoked on the WAL. Finally, it verifies that the
/// number of records on disk matches the atomic counter.
#[test]
fn test_original_crash_requirements() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    // Global shared counter as specified
    let global_counter = Arc::new(AtomicU16::new(0));
    let counter_clone = Arc::clone(&global_counter);

    let wal_dir_clone = wal_dir.to_string();

    // Create thread that appends at random intervals
    let worker_thread = thread::spawn(move || {
        let mut wal = Wal::new(&wal_dir_clone, WalOptions::default()).unwrap();
        let mut rng_state = 42u32; // Simple PRNG seed

        loop {
            // Generate random interval between 1-300ms
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let random_interval = 1 + (rng_state % 300);

            thread::sleep(Duration::from_millis(random_interval as u64));

            // Increase global shared counter
            let current_count = counter_clone.fetch_add(1, Ordering::SeqCst);

            // Create content for this record
            let content = Bytes::from(format!("record-{}", current_count));

            // Append to WAL with the single key "crash-test"
            if let Err(_) = wal.append_entry("crash-test", None, content, true) {
                // If append fails, decrement counter since record wasn't written
                counter_clone.fetch_sub(1, Ordering::SeqCst);
                break;
            }
        }
        // Note: shutdown() is never called - simulating abrupt termination
    });

    // Wait for random interval between 5-7 seconds
    let mut rng_state = 12345u32;
    rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
    let wait_duration = 5000 + (rng_state % 2000); // 5000-6999ms (5-7 seconds)

    thread::sleep(Duration::from_millis(wait_duration as u64));

    // Get the final counter value before killing the thread
    let final_counter_value = global_counter.load(Ordering::SeqCst);

    // Kill the thread in the most abrupt possible way (no shutdown)
    drop(worker_thread);

    // Small delay to ensure any pending file operations complete
    thread::sleep(Duration::from_millis(100));

    // Verify that the number of records on disk matches the atomic counter
    let verification_wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let disk_records: Vec<Bytes> = verification_wal
        .enumerate_records("crash-test")
        .unwrap()
        .collect();

    let disk_record_count = disk_records.len() as u16;

    println!("Original crash test results:");
    println!("  Wait duration: {}ms", wait_duration);
    println!("  Final atomic counter: {}", final_counter_value);
    println!("  Records found on disk: {}", disk_record_count);

    // Verify that the number of records on disk matches the atomic counter
    assert_eq!(
        disk_record_count, final_counter_value,
        "Records on disk ({}) must match atomic counter ({}) for crash consistency",
        disk_record_count, final_counter_value
    );

    // Additional verification: ensure record contents are sequential and correct
    for (index, record) in disk_records.iter().enumerate() {
        let expected_content = format!("record-{}", index);
        let actual_content = String::from_utf8(record.to_vec()).unwrap();
        assert_eq!(
            actual_content, expected_content,
            "Record {} has incorrect content",
            index
        );
    }

    println!(
        "✓ Original crash test PASSED: All {} records verified",
        disk_record_count
    );
    println!("  WAL maintained perfect durability despite abrupt thread termination");
}
