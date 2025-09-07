# nano-wal

![nano-wal](nano-wal.png)

A simple, lightweight Write-Ahead Log (WAL) implementation in Rust with per-key segment sets, designed for append-only operations with configurable retention and random access for memory-constrained systems.

## Features

- **Per-key segment sets**: Each key gets its own set of segment files for optimal organization
- **Entry references**: Get position references for written entries enabling random access
- **Random access reads**: Read specific entries directly using their references with signature verification
- **Size-based rotation**: Automatic segment file rotation based on configurable size limits
- **Meaningful filenames**: Segment files include key names and sequence numbers (e.g., `topic-partition-0001.log`)
- **Dual signatures**: NANO-LOG file headers and NANO-REC entry signatures for data integrity
- **Configurable retention**: Automatic cleanup of old files based on time-based retention policies
- **Memory-efficient**: Zero RAM overhead with optional random access for memory-constrained systems

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nano-wal = "0.4.0"
```

## Quick Start

```rust
use nano_wal::{Wal, WalOptions, EntryRef};
use bytes::Bytes;
use std::time::Duration;

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
```

## Configuration

Customize WAL behavior with `WalOptions`:

```rust
use nano_wal::{Wal, WalOptions};
use std::time::Duration;

let options = WalOptions {
    entry_retention: Duration::from_secs(60 * 60 * 24), // 1 day
    segments_per_retention_period: 24, // 24 segments = 1 hour per segment
};

let mut wal = Wal::new("./custom_wal", options)?;
```

### Configuration Options

- `entry_retention`: Duration for which entries are retained before being eligible for compaction (default: 1 week)
- `segments_per_retention_period`: Number of segments per retention period for time-based expiration (default: 10)

## API Reference

### Core Methods

- `new(filepath: &str, options: WalOptions)` - Create a new WAL instance
- `append_entry<K>(key: K, header: Option<Bytes>, content: Bytes, durable: bool) -> EntryRef` - Append an entry with optional header to the WAL, returns reference
- `log_entry<K>(key: K, header: Option<Bytes>, content: Bytes) -> EntryRef` - Append an entry with optional header and durability enabled, returns reference
- `read_entry_at(entry_ref: EntryRef) -> Bytes` - Read specific entry using its reference (random access)
- `enumerate_records<K>(key: K)` - Get all records for a specific key (sequential access)
- `enumerate_keys() -> Vec<String>` - Get all unique keys in the WAL
- `compact()` - Remove expired segment files based on retention policy
- `shutdown()` - Clean shutdown and remove all WAL files

### Key Types

Keys must implement `Hash + AsRef<[u8]> + Display` for append operations. Common types like `String`, `&str`, and custom types that implement Display work seamlessly.

### Entry References

`EntryRef` is a lightweight reference containing:
- `key_hash: u64` - Hash of the key for which segment set this entry belongs to
- `sequence_number: u64` - The sequence number of the segment file
- `offset: u64` - The byte offset within the segment file (after the header)

Entry references enable efficient random access while maintaining zero RAM overhead for the main WAL operations.

## File Format

The WAL stores data in binary format with per-key segment sets:

- Each segment is named `{key}-{key_hash}-{sequence}.log` (e.g., `hits-12345-0001.log`)
- File header: `[NANO-LOG:8][sequence:8][expiration:8][key_length:8][key:N]`
- Entry format: `[NANORC:6][header_length:2][header:H][content_length:8][content:M]`
- Segments rotate based on time expiration (when current segment expires)
- Headers are optional and limited to 64KB maximum size

## Use Cases

- **Topic/Partition Systems**: Each key represents a topic-partition pair with isolated segment files
- **Event Sourcing**: Store events per entity with dedicated segment sets for optimal performance
- **Database WAL**: Write-ahead logging with per-table or per-operation-type isolation
- **Message Queues**: Persistent message storage with topic-based segment organization
- **Audit Logs**: Tamper-evident logging with dual signature verification (file + entry level)
- **Memory-Constrained Systems**: Support RAM-based structures with disk-backed random access per key

## Performance Characteristics

- **Write throughput**: Optimized for sequential writes per key with minimal overhead
- **Read performance**: Direct file access per key, no cross-key index lookups required
- **Storage efficiency**: Time-based segment rotation and automatic file-based compaction
- **Memory usage**: Zero RAM overhead for entry storage, minimal active segment tracking
- **Random access**: Direct entry retrieval with dual signature verification for data integrity
- **Header support**: Optional metadata headers up to 64KB per entry for enhanced functionality

## Thread Safety

While the WAL struct itself is not `Sync`, it can be safely used in single-threaded contexts or wrapped in appropriate synchronization primitives (`Arc<Mutex<Wal>>`) for multi-threaded scenarios. Entry references (`EntryRef`) are `Copy` and can be safely shared between threads. The per-key segment design makes it ideal for partitioned workloads.

## Examples

### Basic Usage

```rust
use nano_wal::{Wal, WalOptions};
use bytes::Bytes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

### Event Sourcing with Random Access

```rust
use nano_wal::{Wal, WalOptions, EntryRef};
use bytes::Bytes;
use serde_json::json;
use std::collections::HashMap;

fn store_event(wal: &mut Wal, entity_id: &str, event: serde_json::Value) -> Result<EntryRef, Box<dyn std::error::Error>> {
    let event_data = Bytes::from(event.to_string());
    let entry_ref = wal.log_entry(entity_id, None, event_data)?;
    Ok(entry_ref)
}

fn store_event_with_metadata(wal: &mut Wal, entity_id: &str, event: serde_json::Value, metadata: &str) -> Result<EntryRef, Box<dyn std::error::Error>> {
    let event_data = Bytes::from(event.to_string());
    let header = Some(Bytes::from(metadata));
    let entry_ref = wal.log_entry(entity_id, header, event_data)?;
    Ok(entry_ref)
}

fn replay_events(wal: &Wal, entity_id: &str) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let records: Vec<Bytes> = wal.enumerate_records(entity_id)?.collect();
    let events: Result<Vec<_>, _> = records.iter()
        .map(|r| serde_json::from_slice(r))
        .collect();
    Ok(events?)
}

fn get_specific_event(wal: &Wal, event_ref: EntryRef) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let event_data = wal.read_entry_at(event_ref)?;
    let event: serde_json::Value = serde_json::from_slice(&event_data)?;
    Ok(event)
}

// Memory-efficient approach: store only references in RAM
fn build_event_index(wal: &mut Wal, events: Vec<serde_json::Value>) -> Result<HashMap<String, EntryRef>, Box<dyn std::error::Error>> {
    let mut index = HashMap::new();
    
    for event in events {
        let event_id = event["id"].as_str().unwrap();
        let entity_id = event["entity_id"].as_str().unwrap();
        let event_ref = store_event(wal, entity_id, event)?;
        index.insert(event_id.to_string(), event_ref);
    }
    
    Ok(index)
}
```

### Headers for Metadata

```rust
use nano_wal::{Wal, WalOptions};
use bytes::Bytes;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut wal = Wal::new("./header_example", WalOptions::default())?;
    
    // Store data with metadata headers
    let user_data = Bytes::from(r#"{"name": "Alice", "age": 30}"#);
    let metadata = Some(Bytes::from(r#"{"version": "1.0", "schema": "user", "timestamp": "2024-01-01T12:00:00Z"}"#));
    
    let user_ref = wal.append_entry("user:123", metadata, user_data, true)?;
    
    // Store data without headers
    let simple_data = Bytes::from("Simple log message");
    let simple_ref = wal.append_entry("logs", None, simple_data, true)?;
    
    // Headers are stored internally but content retrieval remains the same
    let retrieved_user = wal.read_entry_at(user_ref)?;
    println!("User data: {}", String::from_utf8_lossy(&retrieved_user));
    
    // Headers can be used for versioning, schemas, timestamps, or any metadata
    let order_data = Bytes::from(r#"{"order_id": 12345, "amount": 99.99}"#);
    let order_header = Some(Bytes::from(r#"{"content_type": "application/json", "encoding": "utf-8", "checksum": "abc123"}"#));
    
    let order_ref = wal.log_entry("orders", order_header, order_data)?;
    
    wal.shutdown()?;
    Ok(())
}
```

## Examples

The repository includes several comprehensive examples demonstrating real-world usage patterns:

### 1. Event Sourcing with CQRS (`examples/event_sourcing_cqrs.rs`)

A complete implementation of event sourcing with Command Query Responsibility Segregation pattern:

```bash
cargo run --example event_sourcing_cqrs
```

**Features demonstrated:**
- Domain event storage with metadata headers
- Aggregate reconstruction from events
- Event metadata tracking (correlation IDs, causation chains)
- Separate command and query models
- User and order aggregate examples

### 2. Distributed Messaging System (`examples/distributed_messaging.rs`)

A message broker implementation with routing and acknowledgments:

```bash
cargo run --example distributed_messaging
```

**Features demonstrated:**
- Topic partitioning with configurable partition counts
- Message routing using routing keys and hash-based partitioning
- Priority message handling with expiration
- Consumer acknowledgments with retry logic
- Dead letter queue handling
- Message replay capabilities
- Performance monitoring and statistics

### 3. Real-time Analytics Pipeline (`examples/realtime_analytics.rs`)

A high-throughput analytics system for real-time metrics:

```bash
cargo run --example realtime_analytics
```

**Features demonstrated:**
- High-frequency event ingestion (page views, purchases, errors)
- Real-time metrics calculation and aggregation
- Event deduplication using headers
- Multi-stream processing with different retention policies
- Performance metrics collection
- Time-window aggregations
- Metrics snapshots for historical analysis

### Running Examples

All examples require additional dependencies that are included in `dev-dependencies`:

```toml
[dev-dependencies]
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4"] }
```

Each example creates temporary directories and cleans up after execution, making them safe to run multiple times.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.