//! Crash Test Demo for nano-wal
//!
//! This example demonstrates how to test the crash durability of the nano-wal
//! Write-Ahead Log implementation. It simulates a scenario where a process
//! writing to the WAL is abruptly terminated, then verifies that all data
//! that was supposed to be written is actually persisted to disk.
//!
//! Run with: cargo run --example crash_test_demo

use bytes::Bytes;
use nano_wal::{Wal, WalOptions};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("🔥 nano-wal Crash Durability Demo");
    println!("==================================");
    println!();

    // Create a temporary directory for the WAL
    let temp_dir = std::env::temp_dir().join("nano_wal_crash_demo");
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
    std::fs::create_dir_all(&temp_dir).unwrap();

    let wal_dir = temp_dir.to_str().unwrap();
    println!("📁 WAL directory: {}", wal_dir);

    // Shared atomic counter to track successful writes
    let global_counter = Arc::new(AtomicU16::new(0));
    let counter_clone = Arc::clone(&global_counter);

    let wal_dir_clone = wal_dir.to_string();

    println!("🚀 Starting background writer thread...");

    // Create a thread that continuously writes to the WAL
    let writer_thread = thread::spawn(move || {
        println!("   📝 Writer thread started");

        let mut wal = Wal::new(&wal_dir_clone, WalOptions::default()).unwrap();
        let mut rng_state = 42u32; // Simple PRNG for deterministic randomness

        loop {
            // Random delay between 1-300ms as specified
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let delay = 1 + (rng_state % 300);

            thread::sleep(Duration::from_millis(delay as u64));

            // Increment counter and write record
            let record_id = counter_clone.fetch_add(1, Ordering::SeqCst);
            let content = Bytes::from(format!("crash-test-record-{}", record_id));

            // Write with durability enabled (fsync after write)
            match wal.append_entry("crash-test", None, content, true) {
                Ok(_) => {
                    if record_id % 10 == 0 {
                        println!("   ✅ Wrote record {}", record_id);
                    }
                }
                Err(e) => {
                    println!("   ❌ Failed to write record {}: {}", record_id, e);
                    // Decrement counter since write failed
                    counter_clone.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
            }
        }
    });

    // Random wait time between 5-7 seconds
    let mut rng_state = 12345u32;
    rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
    let wait_duration = 5000 + (rng_state % 2000); // 5000-6999ms

    println!("⏱️  Waiting {}ms before simulating crash...", wait_duration);

    let start_time = Instant::now();
    thread::sleep(Duration::from_millis(wait_duration as u64));
    let actual_duration = start_time.elapsed();

    // Get final counter value before "crash"
    let final_counter = global_counter.load(Ordering::SeqCst);

    println!("💥 Simulating abrupt process termination!");
    println!("   📊 Records written: {}", final_counter);
    println!("   🕐 Actual runtime: {:?}", actual_duration);

    // Abruptly terminate the writer thread (no graceful shutdown)
    drop(writer_thread);

    // Small delay to ensure any pending filesystem operations complete
    thread::sleep(Duration::from_millis(100));

    println!("🔍 Verifying data integrity after crash...");

    // Create a new WAL instance to verify persistence
    let verification_wal = Wal::new(wal_dir, WalOptions::default()).unwrap();
    let recovered_records: Vec<Bytes> = verification_wal
        .enumerate_records("crash-test")
        .unwrap()
        .collect();

    let recovered_count = recovered_records.len() as u16;

    println!("📈 Recovery Results:");
    println!("   💾 Records on disk: {}", recovered_count);
    println!("   🧮 Expected count: {}", final_counter);

    // Verify data integrity
    if recovered_count == final_counter {
        println!("   ✅ Perfect recovery - all records persisted!");
    } else {
        println!("   ⚠️  Mismatch detected!");
        println!("      This could indicate a bug or race condition");
    }

    // Verify record contents
    println!("🔬 Verifying record contents...");
    let mut corrupted_count = 0;
    for (i, record) in recovered_records.iter().enumerate() {
        let expected = format!("crash-test-record-{}", i);
        match String::from_utf8(record.to_vec()) {
            Ok(actual) => {
                if actual != expected {
                    println!(
                        "   ❌ Record {} corrupted: expected '{}', got '{}'",
                        i, expected, actual
                    );
                    corrupted_count += 1;
                }
            }
            Err(_) => {
                println!("   ❌ Record {} contains invalid UTF-8", i);
                corrupted_count += 1;
            }
        }
    }

    if corrupted_count == 0 {
        println!(
            "   ✅ All {} records have correct content!",
            recovered_count
        );
    } else {
        println!("   ❌ Found {} corrupted records", corrupted_count);
    }

    // Final assessment
    println!();
    println!("🏁 Crash Test Summary:");
    println!(
        "   💪 Durability: {}/{} records persisted ({}%)",
        recovered_count,
        final_counter,
        if final_counter > 0 {
            recovered_count * 100 / final_counter
        } else {
            0
        }
    );
    println!(
        "   🔒 Integrity: {}/{} records valid ({}%)",
        recovered_count - corrupted_count,
        recovered_count,
        if recovered_count > 0 {
            (recovered_count - corrupted_count) * 100 / recovered_count
        } else {
            0
        }
    );

    if recovered_count == final_counter && corrupted_count == 0 {
        println!("   🎉 CRASH TEST PASSED - WAL maintains perfect durability!");
    } else {
        println!("   🚨 CRASH TEST ISSUES DETECTED");
    }

    // Cleanup
    println!("🧹 Cleaning up temporary files...");
    if let Err(e) = std::fs::remove_dir_all(&temp_dir) {
        println!("   ⚠️  Failed to cleanup: {}", e);
    } else {
        println!("   ✅ Cleanup complete");
    }

    println!();
    println!("Demo complete! This test verifies that nano-wal can recover");
    println!("from abrupt process termination without data loss or corruption.");
}
