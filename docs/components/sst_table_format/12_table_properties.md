# Table Properties

**Files:** `include/rocksdb/table_properties.h`, `table/table_properties.cc`, `table/meta_blocks.h`, `table/meta_blocks.cc`, `db/table_properties_collector.h`, `db/table_properties_collector.cc`

## Overview

Table properties are metadata stored in every SST file, providing information about the file's contents, configuration, and origin. Properties are stored in a dedicated meta block within the SST file and are accessible without reading the data blocks.

RocksDB supports two categories of table properties:
1. **Built-in properties**: Automatically collected by the table builder (entry counts, sizes, compression info, timestamps, etc.).
2. **User-collected properties**: Custom properties collected via user-defined `TablePropertiesCollector` callbacks during table building.

## TableProperties Struct

The `TableProperties` struct (see `include/rocksdb/table_properties.h`) contains all built-in properties. Key fields:

### Size and Count Properties

| Property | Description |
|----------|-------------|
| `data_size` | Total size of all data blocks (compressed, on disk) |
| `uncompressed_data_size` | Total uncompressed size of all data blocks |
| `index_size` | Size of the index block |
| `filter_size` | Size of the filter block |
| `raw_key_size` | Total uncompressed, undelineated key size |
| `raw_value_size` | Total uncompressed, undelineated value size |
| `num_data_blocks` | Number of data blocks in the file |
| `num_entries` | Total number of entries |
| `num_deletions` | Number of deletion entries |
| `num_merge_operands` | Number of merge operand entries |
| `num_range_deletions` | Number of range deletion entries |
| `num_filter_entries` | Number of unique entries added to filters |

### Index Properties

| Property | Description |
|----------|-------------|
| `index_partitions` | Number of index partitions (for `kTwoLevelIndexSearch`) |
| `top_level_index_size` | Size of the top-level index (for `kTwoLevelIndexSearch`) |
| `index_key_is_user_key` | Whether the index stores user keys (vs internal keys with sequence numbers) |
| `index_value_is_delta_encoded` | Whether index values use delta encoding |

### Format and Configuration

| Property | Description |
|----------|-------------|
| `format_version` | SST format version |
| `fixed_key_len` | Fixed key length (0 = variable length) |
| `compression_name` | Compression algorithm identifier |
| `compression_options` | Compression options used |
| `data_block_restart_interval` | Restart interval for data blocks |
| `index_block_restart_interval` | Restart interval for index blocks |
| `separate_key_value_in_data_block` | Whether separated KV storage is used in data blocks |

### Identity and Timestamp Properties

| Property | Description |
|----------|-------------|
| `db_id` | Database identity (generated at DB creation) |
| `db_session_id` | Session identity (changes every DB open) |
| `db_host_id` | Host location identifier |
| `orig_file_number` | File number at creation time |
| `column_family_id` | Column family ID |
| `column_family_name` | Column family name |
| `creation_time` | Oldest ancestor time (see details below) |
| `oldest_key_time` | Timestamp of the earliest key (0 = unknown) |
| `newest_key_time` | Timestamp of the newest key (0 = unknown) |
| `file_creation_time` | Actual SST file creation time (0 = unknown) |
| `key_largest_seqno` | Largest sequence number in the file (UINT64_MAX = unknown) |
| `key_smallest_seqno` | Smallest sequence number in the file (UINT64_MAX = unknown) |
| `user_defined_timestamps_persisted` | Whether user-defined timestamps are persisted (default true) |

### Compression Estimation

| Property | Description |
|----------|-------------|
| `slow_compression_estimated_data_size` | Estimated data size with slow compression algorithm |
| `fast_compression_estimated_data_size` | Estimated data size with fast compression algorithm |

These estimates are populated when `ColumnFamilyOptions::sample_for_compression` is configured. They help users evaluate compression trade-offs without rebuilding the file.

### Other Properties

| Property | Description |
|----------|-------------|
| `comparator_name` | Name of the comparator |
| `merge_operator_name` | Name of the merge operator ("nullptr" if none) |
| `prefix_extractor_name` | Name of the prefix extractor ("nullptr" if none) |
| `filter_policy_name` | Name of the filter policy (empty if none) |
| `property_collectors_names` | Comma-separated names of property collector factories |
| `seqno_to_time_mapping` | Delta-encoded sequence number to time mapping |
| `tail_start_offset` | Offset where the "tail" (non-data blocks) begins |
| `external_sst_file_global_seqno_offset` | Offset of the global seqno property value for ingested files (0 if not present) |

### Creation Time Semantics

The `creation_time` field has special semantics:
- For flush output: the oldest key time in the file, or flush time if unavailable.
- For compaction output: the oldest among all input files' oldest key times, since the output could be a chain of compactions from older SST files. Falls back to compaction output creation time if unavailable.

## Property Serialization

### Property Names

Each built-in property has a string name defined in `TablePropertiesNames` (see `include/rocksdb/table_properties.h`). Names follow the pattern `rocksdb.<property>`, for example:
- `rocksdb.data.size`
- `rocksdb.num.entries`
- `rocksdb.compression`
- `rocksdb.creating.db.identity`

### Storage Format

Properties are stored in a meta block named `rocksdb.properties` within the SST file. The `PropertyBlockBuilder` (see `table/meta_blocks.h`) uses a `BlockBuilder` to serialize properties as key-value pairs in sorted order. Numeric properties are encoded as varint64. String properties are stored as raw bytes.

### Write Workflow

Step 1: During table building, the builder accumulates property values (entry counts, sizes, etc.).

Step 2: At `Finish()` time:
  - `PropertyBlockBuilder::AddTableProperty()` serializes all built-in properties.
  - User-collected properties are added via `PropertyBlockBuilder::Add()`.
  - `NotifyCollectTableCollectorsOnFinish()` calls `Finish()` on each `TablePropertiesCollector` to collect final user properties.

Step 3: The property block is written to the SST file and its `BlockHandle` is recorded in the metaindex block under the key `rocksdb.properties`.

### Read Workflow

`ReadTableProperties()` (see `table/meta_blocks.h`) reads properties from an SST file:

Step 1: Read the footer to find the metaindex block.

Step 2: Read the metaindex block and look up the `rocksdb.properties` entry.

Step 3: Read the properties block and parse each key-value pair into the `TableProperties` struct.

## User-Collected Properties

### TablePropertiesCollector

`TablePropertiesCollector` (see `include/rocksdb/table_properties.h`) provides callbacks for users to collect custom properties during table building.

| Method | Description |
|--------|-------------|
| `AddUserKey(key, value, type, seq, file_size)` | Called for each key-value pair added to the table. The `file_size` parameter reflects the current file size (data blocks written so far, excluding the current block being built). |
| `BlockAdd(uncomp_bytes, comp_fast, comp_slow)` | Called after each data block is completed. Provides uncompressed and estimated compressed sizes. |
| `Finish(properties)` | Called once when the table is complete. The collector writes its final properties to the provided map. |
| `GetReadableProperties()` | Returns human-readable property values for logging. |
| `NeedCompact()` | Returns true if the file should be further compacted. Used by compaction filters to trigger additional compaction. |

Important: `TablePropertiesCollector` methods do not need to be thread-safe -- RocksDB creates exactly one collector per table and calls it sequentially. However, if `Finish()` returns a non-OK status, the collected properties are not written to the file.

### TablePropertiesCollectorFactory

`TablePropertiesCollectorFactory` (see `include/rocksdb/table_properties.h`) creates collector instances for each table file. The factory must be thread-safe since it is shared across concurrent table builds.

The factory's `CreateTablePropertiesCollector()` method receives a `Context` with:
- `column_family_id`: The column family of the table being built.
- `level_at_creation`: The LSM level at file creation time (-1 if unknown).
- `num_levels`: Total number of levels (-1 if unknown).
- `last_level_inclusive_max_seqno_threshold`: Sequence number threshold for tiered storage eligibility.

Returning `nullptr` from the factory declines collection for that file, reducing callback overhead.

### Registration

User collector factories are registered via `ColumnFamilyOptions::table_properties_collector_factories` (see `include/rocksdb/advanced_options.h`). Multiple factories can be registered, and each produces an independent collector for every table file.

## Aggregation and Comparison

`TableProperties` supports aggregation across multiple files:
- `Add()`: Sums a subset of numeric fields from another `TableProperties` instance (not all fields are aggregated).
- `GetAggregatablePropertiesAsMap()`: Returns numeric properties as a `map<string, uint64_t>` for external aggregation.

For serialization and comparison:
- `Serialize()` / `Parse()`: Convert to/from string representation.
- `AreEqual()`: Compares two property instances field by field.

## Approximate Memory Usage

`ApproximateMemoryUsage()` provides an intentionally approximate estimate of the memory footprint of a `TableProperties` object. It includes the struct itself and most string/map members, but may omit some fields (e.g., `seqno_to_time_mapping`). This is used for memory accounting in the table cache.

## Internal Property Collectors

RocksDB includes internal property collectors (see `db/table_properties_collector.h`) that are active during table building:
- **InternalTblPropColl**: The base class for internal collectors. Deletion counts and merge operand counts are tracked directly in `TableProperties` fields during table building, not via a separate collector. The old user-property accessors (`GetDeletedKeys()`, `GetMergeOperands()`) are deprecated.
- **BlockBasedTablePropertiesCollector**: Records block-based table specific properties: index type, whole key filtering, prefix filtering, and decoupled partitioned filters.
- **TimestampTablePropertiesCollector**: Active when timestamp-aware configurations are used.
- **SstFileWriterPropertiesCollector**: Active when building external SST files via `SstFileWriter`.

## Interactions With Other Components

- **Table Builders**: All table formats (block-based, plain, cuckoo) write a properties meta block using `PropertyBlockBuilder`. Each format may add format-specific user-collected properties.
- **Table Cache**: `TableCache::GetTableProperties()` retrieves properties without opening the full table reader.
- **Compaction**: Compaction reads properties from input files (e.g., `creation_time`, `oldest_key_time`) to propagate to output files. The `NeedCompact()` signal from collectors can trigger additional compaction.
- **SstFileWriter**: External SST files include version and global sequence number properties (see `ExternalSstFilePropertyNames` in `table/sst_file_writer_collectors.h`).
- **sst_dump tool**: Reads and displays all table properties including user-collected properties.
