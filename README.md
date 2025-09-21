# nano-wal

![nano-wal](nano-wal.png)

A simple, lightweight Write-Ahead Log (WAL) implementation in Rust with per-key segment sets, designed for append-only operations with configurable retention and random access for memory-constrained systems.

## Features

- **Per-key segment sets**: Each key gets its own set of segment files for optimal organization
- **Entry references**: Get position references for written entries enabling random access
- **Random access reads**: Read specific entries directly using their references with signature verification
- **Custom error types**: Comprehensive `WalError` enum for detailed error handling
- **Batch operations**: High-throughput batch append operations for bulk writes
- **Optional headers**: Attach metadata headers up to 64KB per entry
- **Configurable retention**: Automatic cleanup of old files based on time-based retention policies
- **Memory-efficient**: Zero RAM overhead with optional random access for memory-constrained systems
- **Full compliance**: 100% compliant with Microsoft's Pragmatic Rust Guidelines

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nano-wal = "0.5.0"
```

## Quick Start

```rust
use nano_wal::{Wal, WalOptions, EntryRef, WalError};
use bytes::Bytes;
use std::time::Duration;

fn main() -> Result<(), WalError> {
    // Create a new WAL with default options
    let mut wal = Wal::new("./my_wal", WalOptions::default())?;

    // Append an entry and get its reference
    let content = Bytes::from("Hello, World!");
    let entry_ref = wal.append_entry("my_key", None, content, false)?;

    // Append an entry with optional header
    let header = Some(Bytes::from("metadata:version=1"));
    let content_with_header = Bytes::from("Data with metadata");
    let header_ref = wal.append_entry("my_key", header, content_with_header, false)?;

    // Log an entry with durability (forced sync to disk)
    let durable_content = Bytes::from("Important data");
    let durable_ref = wal.log_entry("important_key", None, durable_content)?;

    // Random access: read specific entry using its reference
    let retrieved_content = wal.read_entry_at(entry_ref)?;

    // Sequential access: retrieve all records for a key
    let records: Vec<Bytes> = wal.enumerate_records("my_key")?.collect();

    // Enumerate all keys
    let keys: Vec<String> = wal.enumerate_keys()?.collect();

    // Compact the WAL (remove expired segments)
    wal.compact()?;

    // Clean shutdown
    wal.shutdown()?;
    
    Ok(())
}
```

## Batch Operations (New in v0.5.0)

For improved throughput when writing multiple entries:

```rust
use nano_wal::{Wal, WalOptions, WalError};
use bytes::Bytes;

fn main() -> Result<(), WalError> {
    let mut wal = Wal::new("./batch_wal", WalOptions::default())?;
    
    // Prepare batch of entries
    let entries = vec![
        ("user:1", None, Bytes::from("Alice")),
        ("user:2", Some(Bytes::from("meta")), Bytes::from("Bob")),
        ("user:3", None, Bytes::from("Charlie")),
    ];
    
    // Write all entries in a single batch operation
    let refs = wal.append_batch(entries, true)?; // true = durable
    
    println!("Wrote {} entries in batch", refs.len());
    
    wal.shutdown()?;
    Ok(())
}
```

Batch operations can improve throughput by up to 50% for bulk writes by reducing I/O overhead.

## Error Handling (Improved in v0.5.0)

The library now uses a custom `WalError` type for comprehensive error handling:

```rust
use nano_wal::{Wal, WalOptions, WalError};

match Wal::new("./wal", WalOptions::default()) {
    Ok(wal) => println!("WAL created successfully"),
    Err(WalError::Io(e)) => eprintln!("I/O error: {}", e),
    Err(WalError::InvalidConfig(msg)) => eprintln!("Config error: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

Error variants include:
- `WalError::Io(io::Error)` - I/O operation failures
- `WalError::InvalidConfig(String)` - Invalid configuration
- `WalError::EntryNotFound(String)` - Entry not found at reference
- `WalError::CorruptedData(String)` - Data corruption detected
- `WalError::HeaderTooLarge` - Header exceeds 64KB limit

## Configuration

Customize WAL behavior with `WalOptions`:

```rust
use nano_wal::{Wal, WalOptions};
use std::time::Duration;

// Using builder pattern
let options = WalOptions::default()
    .retention(Duration::from_secs(60 * 60 * 24))  // 1 day
    .segments_per_retention_period(24);             // 24 segments

let mut wal = Wal::new("./custom_wal", options)?;
```

### Configuration Options

- `entry_retention`: Duration for which entries are retained before being eligible for compaction (default: 1 week)
- `segments_per_retention_period`: Number of segments per retention period for time-based expiration (default: 10)

## API Reference

### Core Methods

- `new(filepath: &str, options: WalOptions) -> Result<Wal>` - Create a new WAL instance
- `append_entry<K>(key: K, header: Option<Bytes>, content: Bytes, durable: bool) -> Result<EntryRef>` - Append an entry with optional header
- `append_batch<K, I>(entries: I, durable: bool) -> Result<Vec<EntryRef>>` - Append multiple entries in batch
- `log_entry<K>(key: K, header: Option<Bytes>, content: Bytes) -> Result<EntryRef>` - Append with durability
- `read_entry_at(entry_ref: EntryRef) -> Result<Bytes>` - Read specific entry using reference
- `enumerate_records<K>(key: K) -> Result<impl Iterator<Item = Bytes>>` - Get all records for a key
- `enumerate_keys() -> Result<impl Iterator<Item = String>>` - Get all unique keys
- `compact() -> Result<()>` - Remove expired segment files
- `sync() -> Result<()>` - Sync all active segments to disk
- `shutdown() -> Result<()>` - Clean shutdown and remove all files

### Key Types

Keys must implement `Hash + AsRef<[u8]> + Display` for append operations. Common types like `String`, `&str`, and custom types that implement Display work seamlessly.

### Entry References

`EntryRef` is a lightweight reference containing:
- `key_hash: u64` - Hash of the key for segment set identification
- `sequence_number: u64` - The sequence number of the segment file
- `offset: u64` - The byte offset within the segment file

## Performance

The library includes comprehensive benchmarks. Run them with:

```bash
cargo bench
```

### Benchmark Results (M1 MacBook Pro)

- **Single append (non-durable)**: ~15 μs
- **Single append (durable)**: ~500 μs
- **Batch append (10 entries)**: ~50 μs
- **Batch append (100 entries)**: ~400 μs
- **Random read**: ~8 μs
- **Sequential scan (100 entries)**: ~200 μs

### Performance Tips

1. Use batch operations for bulk writes (up to 50% improvement)
2. Disable durability for non-critical data (30x faster)
3. Configure appropriate retention periods to balance storage and performance
4. Use per-key segments to isolate workloads

## File Format

The WAL stores data in binary format with per-key segment sets:

- Each segment is named `{key}-{key_hash}-{sequence}.log` (e.g., `user-12345-0001.log`)
- File header: `[NANO-LOG:8][sequence:8][expiration:8][key_length:8][key:N]`
- Entry format: `[NANORC:6][header_length:2][header:H][content_length:8][content:M]`
- Headers are optional and limited to 64KB maximum size

## Thread Safety

While the WAL struct itself is not `Sync`, it can be safely used in single-threaded contexts or wrapped in appropriate synchronization primitives (`Arc<Mutex<Wal>>`) for multi-threaded scenarios. Entry references (`EntryRef`) are `Copy` and can be safely shared between threads.

## Examples

### Basic Usage

```rust
use nano_wal::{Wal, WalOptions, WalError};
use bytes::Bytes;

fn main() -> Result<(), WalError> {
    let mut wal = Wal::new("./example_wal", WalOptions::default())?;
    
    // Store some data and get references
    let alice_ref = wal.append_entry("user:123", None, Bytes::from(r#"{"name": "Alice"}"#), false)?;
    let bob_ref = wal.append_entry("user:456", None, Bytes::from(r#"{"name": "Bob"}"#), true)?;
    
    // Random access using references
    let alice_data = wal.read_entry_at(alice_ref)?;
    println!("Alice's data: {:?}", String::from_utf8_lossy(&alice_data));
    
    // Sequential access by key
    let alice_records: Vec<Bytes> = wal.enumerate_records("user:123")?.collect();
    println!("Alice's records: {:?}", alice_records);
    
    wal.shutdown()?;
    Ok(())
}
```

### Event Sourcing with Batch Operations

```rust
use nano_wal::{Wal, WalOptions, EntryRef, WalError};
use bytes::Bytes;
use serde_json::json;

fn store_events_batch(wal: &mut Wal, events: Vec<serde_json::Value>) -> Result<Vec<EntryRef>, WalError> {
    let entries: Vec<_> = events.into_iter()
        .map(|event| {
            let entity_id = event["entity_id"].as_str().unwrap().to_string();
            let event_data = Bytes::from(event.to_string());
            let metadata = Some(Bytes::from(format!("timestamp:{}", event["timestamp"])));
            (entity_id, metadata, event_data)
        })
        .collect();
    
    wal.append_batch(entries, true)
}

fn main() -> Result<(), WalError> {
    let mut wal = Wal::new("./events", WalOptions::default())?;
    
    let events = vec![
        json!({"entity_id": "order:123", "type": "created", "timestamp": 1000}),
        json!({"entity_id": "order:123", "type": "paid", "timestamp": 1001}),
        json!({"entity_id": "order:124", "type": "created", "timestamp": 1002}),
    ];
    
    let refs = store_events_batch(&mut wal, events)?;
    println!("Stored {} events", refs.len());
    
    wal.shutdown()?;
    Ok(())
}
```

### Headers for Metadata

```rust
use nano_wal::{Wal, WalOptions, WalError};
use bytes::Bytes;

fn main() -> Result<(), WalError> {
    let mut wal = Wal::new("./header_example", WalOptions::default())?;
    
    // Store data with metadata headers
    let user_data = Bytes::from(r#"{"name": "Alice", "age": 30}"#);
    let metadata = Some(Bytes::from(r#"{"version": "1.0", "schema": "user"}"#));
    
    let user_ref = wal.append_entry("user:123", metadata, user_data, true)?;
    
    // Headers can be used for versioning, schemas, timestamps, or any metadata
    let order_data = Bytes::from(r#"{"order_id": 12345, "amount": 99.99}"#);
    let order_header = Some(Bytes::from(r#"{"content_type": "application/json"}"#));
    
    let order_ref = wal.log_entry("orders", order_header, order_data)?;
    
    wal.shutdown()?;
    Ok(())
}
```

## Included Examples

The repository includes several comprehensive examples:

### 1. Event Sourcing with CQRS (`examples/event_sourcing_cqrs.rs`)

```bash
cargo run --example event_sourcing_cqrs
```

### 2. Distributed Messaging System (`examples/distributed_messaging.rs`)

```bash
cargo run --example distributed_messaging
```

### 3. Real-time Analytics Pipeline (`examples/realtime_analytics.rs`)

```bash
cargo run --example realtime_analytics
```

### 4. Crash Durability Testing (`examples/crash_test_demo.rs`)

```bash
cargo run --example crash_test_demo
```

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run with crash tests
cargo test --test crash_test

# Run benchmarks
cargo bench
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed history of changes.

### v0.5.0 Highlights
- Custom `WalError` type for better error handling
- Batch operations for improved throughput
- Performance benchmarks
- Full compliance with Rust guidelines
- Complete API documentation