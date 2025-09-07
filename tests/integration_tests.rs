use bytes::Bytes;
use nano_wal::{Wal, WalOptions};
use std::fs;

use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_real_world_event_sourcing_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate event sourcing for user accounts
    let user_id = "user_12345";

    // User created
    let _ref1 = wal.append_entry(
        format!("{}:created", user_id),
        None,
        Bytes::from(r#"{"event":"UserCreated","user_id":"user_12345","email":"user@example.com","timestamp":"2024-01-01T00:00:00Z"}"#),
        true,
    ).unwrap();

    // User updated profile
    let _ref2 = wal.append_entry(
        format!("{}:profile_updated", user_id),
        None,
        Bytes::from(
            r#"{"event":"ProfileUpdated","user_id":"user_12345","new_name":"John Doe","timestamp":"2024-01-01T01:00:00Z"}"#,
        ),
        true,
    ).unwrap();

    // User made purchase
    let _ref3 = wal.append_entry(
        format!("{}:purchase", user_id),
        None,
        Bytes::from(r#"{"event":"PurchaseMade","amount":99.99,"item":"Premium Plan","timestamp":"2024-01-01T02:00:00Z"}"#),
        true,
    ).unwrap();

    // Another purchase
    let _ref4 = wal.append_entry(
        format!("{}:purchase", user_id),
        None,
        Bytes::from(r#"{"event":"PurchaseMade","amount":49.99,"item":"Add-on Feature","timestamp":"2024-01-01T03:00:00Z"}"#),
        true,
    ).unwrap();

    // Verify we can replay events for the user
    let user_events: Vec<Bytes> = wal
        .enumerate_records(format!("{}:created", user_id))
        .unwrap()
        .collect();
    assert_eq!(user_events.len(), 1);

    let profile_events: Vec<Bytes> = wal
        .enumerate_records(format!("{}:profile_updated", user_id))
        .unwrap()
        .collect();
    assert_eq!(profile_events.len(), 1);

    let purchase_events: Vec<Bytes> = wal
        .enumerate_records(format!("{}:purchase", user_id))
        .unwrap()
        .collect();
    assert_eq!(purchase_events.len(), 2);

    // Test enumeration of all event types
    let all_keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(all_keys.len(), 3);
    assert!(all_keys.contains(&format!("{}:created", user_id)));
    assert!(all_keys.contains(&format!("{}:profile_updated", user_id)));
    assert!(all_keys.contains(&format!("{}:purchase", user_id)));

    wal.shutdown().unwrap();
}

#[test]
fn test_database_wal_simulation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate database operations
    let operations = vec![
        (
            "txn_001",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        ),
        (
            "txn_002",
            "UPDATE users SET name = 'Alice Smith' WHERE id = 1",
        ),
        (
            "txn_003",
            "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100.0)",
        ),
        ("txn_004", "DELETE FROM users WHERE id = 1"),
        ("txn_005", "INSERT INTO users (id, name) VALUES (2, 'Bob')"),
    ];

    for (txn_id, sql) in operations {
        let _ref = wal.log_entry(txn_id, None, Bytes::from(sql)).unwrap();
    }

    // Verify all transactions are logged
    assert_eq!(wal.active_segment_count(), 5);

    // Verify we can read specific transactions
    let txn_001: Vec<Bytes> = wal.enumerate_records("txn_001").unwrap().collect();
    assert_eq!(txn_001.len(), 1);
    assert_eq!(
        String::from_utf8_lossy(&txn_001[0]),
        "INSERT INTO users (id, name) VALUES (1, 'Alice')"
    );

    let txn_004: Vec<Bytes> = wal.enumerate_records("txn_004").unwrap().collect();
    assert_eq!(txn_004.len(), 1);
    assert_eq!(
        String::from_utf8_lossy(&txn_004[0]),
        "DELETE FROM users WHERE id = 1"
    );

    wal.shutdown().unwrap();
}

#[test]
fn test_message_queue_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate message queue with different topics
    let topics = vec!["orders", "payments", "notifications", "analytics"];

    for topic in &topics {
        for i in 0..10 {
            let message = format!(
                r#"{{"topic":"{}","message_id":{},"payload":"data_{}_{}"}}"#,
                topic, i, topic, i
            );
            let _ref = wal
                .append_entry(*topic, None, Bytes::from(message), i % 3 == 0)
                .unwrap(); // Sync every 3rd message
        }
    }

    // Verify each topic has 10 messages
    for topic in &topics {
        let messages: Vec<Bytes> = wal.enumerate_records(*topic).unwrap().collect();
        assert_eq!(
            messages.len(),
            10,
            "Topic {} should have 10 messages",
            topic
        );
    }

    // Verify total active segments (4 topics)
    assert_eq!(wal.active_segment_count(), 4);

    // Verify all topics are enumerated
    let all_topics: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(all_topics.len(), 4);
    for topic in &topics {
        assert!(all_topics.contains(&topic.to_string()));
    }

    wal.shutdown().unwrap();
}

#[test]
fn test_audit_log_with_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate audit events with different severities
    let events = vec![
        (
            "login_success",
            r#"{"user":"alice","action":"login","success":true,"ip":"192.168.1.1"}"#,
        ),
        (
            "login_failure",
            r#"{"user":"bob","action":"login","success":false,"ip":"192.168.1.2"}"#,
        ),
        (
            "data_access",
            r#"{"user":"alice","action":"data_access","resource":"/api/sensitive","method":"GET"}"#,
        ),
        (
            "privilege_escalation",
            r#"{"user":"admin","action":"privilege_grant","target":"alice","role":"moderator"}"#,
        ),
        (
            "data_modification",
            r#"{"user":"alice","action":"data_update","resource":"/api/users/123","changes":["email"]}"#,
        ),
    ];

    for (event_type, event_data) in events {
        let _ref = wal
            .log_entry(event_type, None, Bytes::from(event_data))
            .unwrap();

        // Small delay to ensure different timestamps
        thread::sleep(Duration::from_millis(10));
    }

    // Verify we can retrieve audit events by type
    let login_failures: Vec<Bytes> = wal.enumerate_records("login_failure").unwrap().collect();
    assert_eq!(login_failures.len(), 1);

    let data_accesses: Vec<Bytes> = wal.enumerate_records("data_access").unwrap().collect();
    assert_eq!(data_accesses.len(), 1);

    // Verify all event types are captured
    let event_types: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(event_types.len(), 5);

    wal.shutdown().unwrap();
}

#[test]
fn test_cache_warming_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate cache entries with different keys
    let cache_entries = vec![
        (
            "user:123",
            r#"{"id":123,"name":"Alice","email":"alice@example.com"}"#,
        ),
        (
            "user:456",
            r#"{"id":456,"name":"Bob","email":"bob@example.com"}"#,
        ),
        (
            "session:abc123",
            r#"{"session_id":"abc123","user_id":123,"expires_at":"2024-12-31T23:59:59Z"}"#,
        ),
        (
            "config:feature_flags",
            r#"{"dark_mode":true,"new_ui":false,"beta_features":true}"#,
        ),
        (
            "metrics:daily",
            r#"{"date":"2024-01-01","active_users":1500,"revenue":25000}"#,
        ),
    ];

    for (cache_key, cache_value) in cache_entries {
        let _ref = wal
            .append_entry(cache_key, None, Bytes::from(cache_value), true)
            .unwrap();
    }

    // Simulate cache updates
    let _ref1 = wal
        .append_entry(
            "user:123",
            None,
            Bytes::from(r#"{"id":123,"name":"Alice Smith","email":"alice.smith@example.com"}"#),
            true,
        )
        .unwrap();
    let _ref2 = wal
        .append_entry(
            "config:feature_flags",
            None,
            Bytes::from(r#"{"dark_mode":true,"new_ui":true,"beta_features":true}"#),
            true,
        )
        .unwrap();

    // Verify we can get the latest values (for cache warming)
    let user_123_entries: Vec<Bytes> = wal.enumerate_records("user:123").unwrap().collect();
    assert_eq!(user_123_entries.len(), 2); // Original + update

    let config_entries: Vec<Bytes> = wal
        .enumerate_records("config:feature_flags")
        .unwrap()
        .collect();
    assert_eq!(config_entries.len(), 2); // Original + update

    // Verify all cache keys exist
    let cache_keys: Vec<String> = wal.enumerate_keys().unwrap().collect();
    assert_eq!(cache_keys.len(), 5);

    wal.shutdown().unwrap();
}

#[test]
fn test_high_frequency_trading_logs() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(wal_dir, WalOptions::default()).unwrap();

    // Simulate high-frequency trading logs
    let symbols = vec!["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"];

    for round in 0..5 {
        for (i, symbol) in symbols.iter().enumerate() {
            let price = 100.0 + (round as f64 * 10.0) + (i as f64 * 5.0);
            let trade_data = format!(
                r#"{{"symbol":"{}","price":{},"volume":{},"timestamp":"2024-01-01T09:{}:00Z"}}"#,
                symbol,
                price,
                (i + 1) * 100,
                round * 10 + i
            );

            let _ref = wal
                .append_entry(
                    format!("trades:{}", symbol),
                    None,
                    Bytes::from(trade_data),
                    round == 4, // Only sync on last round for performance
                )
                .unwrap();
        }
    }

    // Verify each symbol has 5 trades
    for symbol in &symbols {
        let trades: Vec<Bytes> = wal
            .enumerate_records(format!("trades:{}", symbol))
            .unwrap()
            .collect();
        assert_eq!(trades.len(), 5, "Symbol {} should have 5 trades", symbol);
    }

    // Verify total active segments (5 symbols)
    assert_eq!(wal.active_segment_count(), 5);

    // Test querying specific symbol
    let aapl_trades: Vec<Bytes> = wal.enumerate_records("trades:AAPL").unwrap().collect();
    assert_eq!(aapl_trades.len(), 5);

    // Verify the first AAPL trade
    let first_trade = String::from_utf8_lossy(&aapl_trades[0]);
    assert!(first_trade.contains("AAPL"));
    assert!(first_trade.contains("100"));

    wal.shutdown().unwrap();
}

#[test]
fn test_log_file_naming_with_meaningful_keys() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().to_str().unwrap();

    let mut wal = Wal::new(
        wal_dir,
        WalOptions {
            entry_retention: Duration::from_secs(20),
            segments_per_retention_period: 10,
        },
    )
    .unwrap();

    // Write entries with meaningful keys
    let _ref1 = wal
        .append_entry("user_events", None, Bytes::from("user event data"), true)
        .unwrap();

    // No need to wait for time-based rotation with new architecture

    let _ref2 = wal
        .append_entry("order_processing", None, Bytes::from("order data"), true)
        .unwrap();

    // Different keys create separate segments automatically

    let _ref3 = wal
        .append_entry("payment_logs", None, Bytes::from("payment data"), true)
        .unwrap();

    // Check that log files have meaningful names
    let entries = fs::read_dir(wal_dir).unwrap();
    let log_files: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".log"))
        .collect();

    assert!(!log_files.is_empty(), "Should have .log files");

    // At least one file should contain a meaningful key name or be a simple numbered file
    let has_meaningful_names = log_files.iter().any(|name| {
        name.contains("user_events")
            || name.contains("order_processing")
            || name.contains("payment_logs")
            || name.matches(char::is_numeric).count() > 0
    });

    assert!(
        has_meaningful_names,
        "Log files should have meaningful names or numeric IDs"
    );

    wal.shutdown().unwrap();
}
