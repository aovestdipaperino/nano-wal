# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2025-09-21

### Added
- **Custom error types**: Introduced `WalError` enum with detailed error variants replacing generic `io::Error`
- **Batch operations**: New `append_batch()` method for improved throughput when writing multiple entries
- **Performance benchmarks**: Comprehensive benchmark suite using Criterion for measuring performance
- **Complete documentation**: All public methods now have full documentation with examples, errors, and usage notes

### Changed
- **BREAKING: Error handling** - All methods now return `Result<T, WalError>` instead of `io::Result<T>`
- **BREAKING: Iterator API** - Removed `Enumerable` wrapper; methods now return standard `impl Iterator`
- **Improved documentation**: Added proper doc comments following Rust API guidelines
- **Performance optimizations**: Removed unnecessary clone operations and improved internal efficiency
- **Debug implementations**: All public types now implement the Debug trait

### Fixed
- Unnecessary `clone()` operation in segment management
- Missing documentation for magic values and constants
- Incomplete error context in various operations

### Technical Improvements
- Custom `WalError` type provides better error context and type safety
- Batch operations reduce I/O overhead for bulk writes
- Standard iterators improve API ergonomics and reduce complexity
- Comprehensive benchmarks enable performance regression detection
- Full compliance with Microsoft's Pragmatic Rust Guidelines

### Migration Notes
- **Error handling**: Update error handling to use `WalError` instead of `io::Error`
- **Iterator usage**: Remove `.collect()` calls on `Enumerable`, use iterators directly
- **Batch writes**: Consider using `append_batch()` for multiple entries to improve performance

## [0.4.0] - 2025-09-07

### Added
- **Optional record headers**: Each record can now include optional metadata headers up to 64KB in size
- **Enhanced record format**: New format `[NANORC:6][header_length:2][header:H][content_length:8][content:M]` with 6-byte signature
- **Header validation**: Automatic validation of header size limits with descriptive error messages
- **Comprehensive examples**: Three detailed examples demonstrating real-world usage patterns:
  - Event Sourcing with CQRS pattern (`examples/event_sourcing_cqrs.rs`)
  - Distributed Messaging System (`examples/distributed_messaging.rs`)
  - Real-time Analytics Pipeline (`examples/realtime_analytics.rs`)

### Changed
- **BREAKING: Method signatures updated** - All `append_entry` and `log_entry` methods now require a `header: Option<Bytes>` parameter
- **BREAKING: Record signature changed** - Updated from `NANO-REC` (8 bytes) to `NANORC` (6 bytes)
- **BREAKING: Record format modified** - Added header length and optional header fields before content
- **Enhanced API flexibility** - Headers enable rich metadata storage for advanced use cases

### Technical Details
- **Header storage**: Headers are stored immediately after the 6-byte signature and 2-byte length field
- **Size limitation**: Headers are limited to 65,535 bytes (64KB - 1) for efficient storage
- **Backward compatibility**: Use `None` for header parameter to maintain previous behavior
- **Performance impact**: Minimal overhead for records without headers (2-byte length field)
- **Use cases**: Enables event sourcing, message routing, correlation tracking, and metadata storage

### Examples Added
- **Event Sourcing**: Complete CQRS implementation with domain events and aggregate reconstruction
- **Message Broker**: Topic partitioning, routing, acknowledgments, and dead letter queue handling
- **Analytics**: High-frequency event ingestion, real-time metrics, and deduplication

### Migration Notes
- **API update required**: Add `None` as the second parameter to all `append_entry` and `log_entry` calls
- **File format change**: New record format is incompatible with previous versions
- **Enhanced capabilities**: Headers enable sophisticated metadata tracking and system coordination

## [0.3.0] - 2025-09-07

### Added
- **Segment expiration timestamps**: Each segment now stores its expiration timestamp in the file header for precise cleanup
- **Chainable configuration API**: New fluent methods `retention()` and `segments_per_retention_period()` for cleaner configuration
- **Time-based segment expiration**: Segments expire based on calculated timestamps rather than file creation times

### Changed
- **BREAKING: Removed segment size concept** - Segments now rotate purely based on time, not size
- **BREAKING: Removed `max_segment_size`** - Eliminated from `WalOptions`, `with_segment_size()`, and `segment_size()` methods
- **BREAKING: File header format** - Added 8-byte expiration timestamp: `[NANO-LOG:8][sequence:8][expiration:8][key_length:8][key:N]`
- **Segment creation logic** - New segments created only when current segment expires, not on size limits
- **Compaction efficiency** - Reads expiration timestamps directly from headers instead of file metadata
- **Predictable behavior** - Segment rotation happens on fixed time intervals regardless of content size

### Removed
- **Size-based rotation** - No more `max_segment_size` configuration or size tracking
- **Size validation** - Removed validation for segment size limits
- **Current size tracking** - Eliminated `current_size` field from `ActiveSegment`

### Technical Details
- **Expiration calculation**: `current_time + (retention_period / segments_per_retention_period)`
- **Natural size variation**: Segments grow based on write patterns - large during busy periods, small during quiet times
- **Simplified API**: Only two configuration parameters: `entry_retention` and `segments_per_retention_period`
- **Performance improvement**: No size checks on writes, faster compaction with timestamp comparisons
- **File position calculation**: Dynamic positioning using `seek(SeekFrom::Current(0))` instead of size tracking

### Migration Notes
- **Configuration update required** - Remove all `max_segment_size` parameters from existing configurations
- **Behavioral change** - Segment sizes now vary naturally with write patterns instead of fixed size limits
- **File format compatibility** - Header format changed but entry format remains the same
- **Predictable rotation** - Segments now expire on fixed schedules rather than unpredictable size thresholds

## [0.2.0] - 2025-09-07

### Added
- **Per-key segment sets**: Each key now gets its own set of numbered segment files for optimal organization
- **Dual signature system**: NANO-LOG file headers and NANO-REC entry signatures for enhanced data integrity
- **Meaningful filenames**: Segment files now use format `{key}-{key_hash}-{sequence}.log` (e.g., `hits-12345-0001.log`)
- **Size-based rotation**: Segments rotate based on configurable size limits instead of time intervals
- **Simplified entry format**: Entries now contain only `[NANO-REC:8][content_length:8][content:M]`
- **File header format**: Each segment file starts with `[NANO-LOG:8][sequence:8][key_length:8][key:N]`

### Changed
- **BREAKING: File format completely redesigned** - Per-key segment organization with dual signatures
- **BREAKING: Configuration change** - Replaced `segments: u8` with `max_segment_size: u64` in WalOptions
- **BREAKING: EntryRef structure** - Now contains `key_hash`, `sequence_number`, and `offset` instead of `segment_id` and `offset`
- **BREAKING: API changes** - Removed timestamp from entries, simplified file organization
- **Performance improvement** - Direct file access per key eliminates cross-key index lookups
- **Storage organization** - Keys like "topic:partition" create isolated segment sets for better performance

### Removed
- **Timestamps from entries** - Simplified entry format focuses on content only
- **Global segment numbering** - Each key maintains its own sequence numbering
- **Time-based rotation** - Replaced with more predictable size-based rotation

### Technical Details
- **File naming convention**: `{sanitized_key}-{key_hash}-{sequence_number}.log`
- **Segment isolation**: Each key's data is completely isolated in its own file set
- **Sequence ordering**: Files for each key are numbered sequentially for chronological processing
- **Header overhead**: File headers are written once per segment, not per entry

### Migration Notes
- **Complete reinitialization required** - New file format is incompatible with previous versions
- **Configuration updates** - Replace `segments` parameter with `max_segment_size` in WalOptions
- **Key design consideration** - Keys should represent logical partitions (e.g., "topic:partition")
- **EntryRef handling** - Update code to handle new EntryRef structure with key_hash and sequence_number

## [0.1.1] - 2025-09-07

### Added
- **NANO-WAL signature prefix**: Each entry now begins with UTF-8 'NANO-WAL' signature (8 bytes) for format validation and data integrity
- **Entry references (`EntryRef`)**: `append_entry()` and `log_entry()` methods now return position references containing segment ID and offset
- **Random access functionality**: New `read_entry_at(entry_ref: EntryRef)` method enables direct entry retrieval using position references
- **Signature verification**: Random access operations verify NANO-WAL signature at specified locations for data integrity
- **Memory-efficient patterns**: Support for RAM-based append-only structures on memory-constrained systems
- **Enhanced error handling**: Detailed error messages for invalid signatures and missing segment files
- **13 new random access tests** covering entry references, persistence, signature verification, and cross-segment compatibility

### Changed
- **File format**: Entries now include 8-byte NANO-WAL signature prefix before timestamp
- **API breaking change**: `append_entry()` and `log_entry()` now return `Result<EntryRef, io::Error>` instead of `Result<(), io::Error>`
- **Key requirements**: Keys must now implement `Display` trait in addition to `Hash + AsRef<[u8]>`
- **Enhanced documentation**: Updated README.md and blog post with random access examples and memory-efficient patterns

### Fixed
- **File reading operations**: All file reading operations now use read-only mode to prevent conflicts
- **Segment file discovery**: Improved segment file finding logic for random access operations
- **Index rebuilding**: Enhanced signature verification during WAL recovery and index rebuilding
- **Compaction overflow**: Fixed potential arithmetic overflow in timestamp-based compaction

### Technical Details
- **Total test coverage**: Expanded from 30 to 43 tests across all functionality
- **Entry reference structure**: `EntryRef { segment_id: u64, offset: u64 }` with `Copy`, `Clone`, `Debug`, `PartialEq`, `Eq` traits
- **Signature format**: 8-byte UTF-8 `NANO-WAL` constant for integrity verification
- **File format**: `[NANO-WAL signature: 8 bytes][timestamp: 8 bytes][key_length: 8 bytes][key: N bytes][content_length: 8 bytes][content: M bytes]`

### Migration Notes
- **Backward compatibility**: File format changes require reinitialization of existing WAL directories
- **API updates**: Update method calls to handle `EntryRef` return values
- **Key types**: Ensure keys implement `Display` trait (most common types like `String` and `&str` already do)

## [0.1.0] - 2025-09-07

### Added
- Initial release of nano-wal
- **Time-based segment rotation** with configurable retention periods
- **Key-based indexing** with meaningful filenames for better debugging
- **Process crash recovery** with guaranteed data persistence using `fsync()`
- **Segments-based configuration** instead of size-based rotation
- **File extension change** from `.wal` to `.log` for better file recognition
- **Comprehensive test coverage** (30 tests) including real-world scenarios
- **Durable and non-durable writes** with configurable fsync behavior
- **Automatic compaction** based on time-based retention policies
- **Zero RAM overhead** with optional application-level caching
- **Real-world examples** for event sourcing, database WAL, message queues, and audit logs

### Features
- `Wal::new()` - Create WAL instance with configurable options
- `append_entry()` - Write entries with optional durability
- `log_entry()` - Write entries with guaranteed durability
- `enumerate_records()` - Retrieve all records for a specific key
- `enumerate_keys()` - Get all unique keys in the WAL
- `compact()` - Remove expired segments based on retention
- `shutdown()` - Clean shutdown with optional data removal
- `WalOptions` - Configuration for retention periods and segment counts

### Configuration
- `entry_retention` - Duration for retaining entries (default: 1 week)
- `segments` - Number of segments per retention period (default: 10)

### Use Cases
- Event sourcing systems
- Database write-ahead logging
- Message queue persistence
- Audit logging with time-based retention
- Cache warming and persistence

[Unreleased]: https://github.com/aovestdipaperino/nano-wal/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/aovestdipaperino/nano-wal/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/aovestdipaperino/nano-wal/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aovestdipaperino/nano-wal/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/aovestdipaperino/nano-wal/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aovestdipaperino/nano-wal/releases/tag/v0.1.0
