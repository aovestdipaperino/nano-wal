# nano-wal

![nano-wal](nano-wal.png)

A simple, lightweight Write-Ahead Log (WAL) implementation in Rust designed for append-only operations with configurable retention, segment management, and optional random access for memory-constrained systems.

## Features

- **Append-only operations**: Efficient write operations with optional durability guarantees
- **Entry references**: Get position references for written entries enabling random access
- **Random access reads**: Read specific entries directly using their references with signature verification
- **Segment rotation**: Automatic segment file rotation based on configurable time intervals
- **Key-based indexing**: Fast lookups and enumeration of records by key
- **Configurable retention**: Automatic cleanup of old entries based on time-based retention policies
- **Compaction**: Remove expired segments to reclaim disk space
- **Memory-efficient**: Zero RAM overhead with optional random access for memory-constrained systems

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nano-wal = "0.1.1"
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
let entry_ref = wal.append_entry("my_key", content, false)?;

// Log an entry with durability (forced sync to disk)
let durable_content = Bytes::from("Important data");
let durable_ref = wal.log_entry("important_key", durable_content)?;

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
    segments: 24, // 24 segments (1 hour per segment)
};

let mut wal = Wal::new("./custom_wal", options)?;
```

### Configuration Options

- `entry_retention`: Duration for which entries are retained before being eligible for compaction (default: 1 week)
- `segments`: Number of segments to use per retention period (default: 10 segments, creating time-based rotation)

## API Reference

### Core Methods

- `new(filepath: &str, options: WalOptions)` - Create a new WAL instance
- `append_entry<K>(key: K, content: Bytes, durable: bool) -> EntryRef` - Append an entry to the WAL, returns reference
- `log_entry<K>(key: K, content: Bytes) -> EntryRef` - Append an entry with durability enabled, returns reference
- `read_entry_at(entry_ref: EntryRef) -> Bytes` - Read specific entry using its reference (random access)
- `enumerate_records<K>(key: K)` - Get all records for a specific key (sequential access)
- `enumerate_keys() -> Vec<String>` - Get all unique keys in the WAL
- `compact()` - Remove expired segments based on retention policy
- `shutdown()` - Clean shutdown and remove all WAL files

### Key Types

Keys must implement `Hash + AsRef<[u8]> + Display` for append operations. Common types like `String`, `&str`, and custom types that implement Display work seamlessly.

### Entry References

`EntryRef` is a lightweight reference containing:
- `segment_id: u64` - The segment file identifier
- `offset: u64` - The byte offset within the segment

Entry references enable efficient random access while maintaining zero RAM overhead for the main WAL operations.

## File Format

The WAL stores data in binary format across multiple segment files:

- Each segment is named `{segment_id}.log` or `{segment_id}_{key}.log` for better debugging
- Each entry is prefixed with the UTF-8 'NANO-WAL' signature for integrity verification
- Entries contain: signature (8 bytes) + timestamp (8 bytes) + key_length (8 bytes) + key + content_length (8 bytes) + content
- Segments rotate based on time intervals (retention_period / segments)

## Use Cases

- **Event Sourcing**: Store events in append-only fashion with key-based retrieval and random access
- **Database WAL**: Write-ahead logging for database systems with entry-level recovery
- **Message Queues**: Persistent message storage with direct access to specific messages
- **Audit Logs**: Tamper-evident logging with time-based retention and signature verification
- **Memory-Constrained Systems**: Support RAM-based append-only structures with disk-backed random access
- **Cache Persistence**: Persistent storage for cache warming with selective entry retrieval

## Performance Characteristics

- **Write throughput**: Optimized for sequential writes with signature prefixing
- **Read performance**: O(1) key lookups via in-memory index for sequential access, direct file I/O for random access
- **Storage efficiency**: Time-based segment rotation and automatic compaction
- **Memory usage**: Zero RAM overhead for entry storage, lightweight in-memory index for key mapping
- **Random access**: Direct entry retrieval with signature verification for data integrity

## Thread Safety

While the WAL struct itself is not `Sync`, it can be safely used in single-threaded contexts or wrapped in appropriate synchronization primitives (`Arc<Mutex<Wal>>`) for multi-threaded scenarios. Entry references (`EntryRef`) are `Copy` and can be safely shared between threads.

## Examples

### Basic Usage

```rust
use nano_wal::{Wal, WalOptions};
use bytes::Bytes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut wal = Wal::new("./example_wal", WalOptions::default())?;
    
    // Store some data and get references
    let alice_ref = wal.append_entry("user:123", Bytes::from(r#"{"name": "Alice"}"#), false)?;
    let bob_ref = wal.append_entry("user:456", Bytes::from(r#"{"name": "Bob"}"#), true)?;
    
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
    let entry_ref = wal.log_entry(entity_id, event_data)?;
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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.