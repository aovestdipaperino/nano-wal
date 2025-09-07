# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2024-01-01

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

## [0.1.1] - 2024-01-01

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

## [0.1.0] - 2024-01-01

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

[Unreleased]: https://github.com/yourusername/nano-wal/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/yourusername/nano-wal/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/yourusername/nano-wal/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/yourusername/nano-wal/releases/tag/v0.1.0