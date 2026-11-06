# Options System Overview

**Files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `options/db_options.h`, `options/db_options.cc`, `options/cf_options.h`, `options/cf_options.cc`, `options/options.cc`, `options/options_helper.h`, `options/options_helper.cc`

## Options Hierarchy

RocksDB's configuration is organized in a layered hierarchy. Each layer controls a different scope of behavior:

| Struct | Scope | Header |
|--------|-------|--------|
| `DBOptions` | Database-wide (environment, threading, WAL, I/O) | `include/rocksdb/options.h` |
| `ColumnFamilyOptions` | Per-column-family (memtable, compaction, compression) | `include/rocksdb/options.h` |
| `Options` | Combined convenience struct (single-CF shortcut) | `include/rocksdb/options.h` |
| `ReadOptions` | Per-read-operation (snapshot, checksums, bounds) | `include/rocksdb/options.h` |
| `WriteOptions` | Per-write-operation (sync, WAL, priority) | `include/rocksdb/options.h` |
| `BlockBasedTableOptions` | Table format (block cache, block size, filters, checksums) | `include/rocksdb/table.h` |

The `Options` struct inherits from both `DBOptions` and `ColumnFamilyOptions` (which in turn inherits from `AdvancedColumnFamilyOptions`). This allows simple single-column-family use cases to configure everything in one struct via `DB::Open(options, path, &db)`.

For multi-column-family databases, `DBOptions` and per-CF `ColumnFamilyOptions` must be provided separately via `DB::Open(db_options, path, cf_descriptors, &cf_handles, &db)`.

## Mutable vs Immutable Options

RocksDB distinguishes between options that can be changed at runtime (mutable) and those fixed at database/column-family creation (immutable). Internally, options are split into four structs:

| Internal Struct | Scope | Mutability |
|-----------------|-------|------------|
| `ImmutableDBOptions` | Database-wide | Fixed at open |
| `MutableDBOptions` | Database-wide | Changeable via `SetDBOptions()` |
| `ImmutableCFOptions` | Per-column-family | Fixed at CF creation |
| `MutableCFOptions` | Per-column-family | Changeable via `SetOptions()` |

See `ImmutableDBOptions` and `MutableDBOptions` in `options/db_options.h`, and `ImmutableCFOptions` and `MutableCFOptions` in `options/cf_options.h`.

### How Mutability is Determined

Each option field is registered in `OptionTypeInfo` maps (see `options/cf_options.cc` and `options/db_options.cc`). The registration includes an `OptionTypeFlags` bitmask where `kMutable` indicates the option can be changed at runtime.

### Changing Options at Runtime

Step 1 -- Call `DB::SetOptions()` (for CF options) or `DB::SetDBOptions()` (for DB options) with a string map of option names to new values.

Step 2 -- RocksDB validates that all provided option names exist and are mutable. If any option is immutable, the entire call fails.

Step 3 -- `ValidateOptions()` checks cross-option consistency on the new values.

Step 4 -- The new options take effect and a new OPTIONS file is persisted.

Important: `SetOptions()` does **not** run `SanitizeOptions()`. Only `ValidateOptions()` is called. This means some auto-adjustment that happens during `DB::Open()` does not apply to runtime changes.

Important: `SetOptions()` applied to multiple column families processes them sequentially without rollback. If a later CF fails validation, earlier CFs already have updated options. Additionally, if OPTIONS file persistence fails, the call returns an error even though in-memory state has already changed.

## Combined ImmutableOptions

The `ImmutableOptions` struct (see `options/cf_options.h`) inherits from both `ImmutableDBOptions` and `ImmutableCFOptions`, providing a unified view of all immutable settings for use in internal code paths that need both DB-level and CF-level immutable state.

## Options Lifecycle

### DB::Open() Flow

Step 1 -- User provides `DBOptions` + `ColumnFamilyOptions` (or combined `Options`).

Step 2 -- `SanitizeOptions()` adjusts options for consistency (e.g., clamps `write_buffer_size` to at least 64KB, ensures `max_write_buffer_number >= 2`). See `db/column_family.cc` for CF sanitization and `db/db_impl/db_impl_open.cc` for DB sanitization.

Step 3 -- `ValidateOptions()` checks cross-option consistency and returns errors for invalid combinations (e.g., FIFO compaction with TTL requires `max_open_files = -1`). See `options/options_helper.cc`.

Step 4 -- Immutable and mutable options are split into their respective internal structs.

Step 5 -- An OPTIONS file is written to persist the configuration.

### OPTIONS File Persistence

RocksDB automatically persists options to `OPTIONS-XXXXXX` files in the database directory. These are human-readable INI-format files generated during:

- `DB::Open()` -- initial configuration snapshot
- `SetOptions()` / `SetDBOptions()` -- after runtime changes
- `CreateColumnFamily()` / `DropColumnFamily()` -- after CF topology changes

The format has sections for `[Version]`, `[DBOptions]`, `[CFOptions "name"]`, and `[TableOptions/BlockBasedTable "name"]`. See `options/options_parser.cc` for the parser implementation and `PersistRocksDBOptions()` for the writer.

### Loading Persisted Options

Applications can load persisted options using `LoadLatestOptions()` (see `include/rocksdb/convenience.h`). This reads the most recent OPTIONS file and populates `DBOptions` and a vector of `ColumnFamilyDescriptor`.

Important: Pointer-based options (such as `env`, `comparator`, `merge_operator`, `compaction_filter`, `block_cache`, `prefix_extractor`, `memtable_factory`) are set to defaults by `LoadLatestOptions()` and must be manually restored by the application before calling `DB::Open()`.

## Options Serialization Framework

The `OptionTypeInfo` system (see `include/rocksdb/utilities/options_type.h`) provides type-safe, metadata-driven parsing and serialization for all registered options. Each option field has a registration entry specifying:

- `OptionType` -- the data type (bool, int, uint64_t, string, enum, struct, etc.)
- Field offset within the containing struct
- `OptionVerificationType` -- how to compare values
- `OptionTypeFlags` -- mutability and deprecation status
- Optional custom parse/serialize/equals functions

This framework powers `GetStringFromDBOptions()`, `GetDBOptionsFromString()`, `GetColumnFamilyOptionsFromString()`, and their map-based equivalents (all in `include/rocksdb/convenience.h`).

## ConfigOptions

The `ConfigOptions` struct (see `include/rocksdb/convenience.h`) controls parsing behavior:

| Field | Purpose | Default |
|-------|---------|---------|
| `ignore_unknown_options` | Skip unrecognized option names (forward compatibility) | `false` |
| `ignore_unsupported_options` | Skip options not supported on current platform | `true` |
| `input_strings_escaped` | Whether input strings use RocksDB escaping | `true` |
| `mutable_options_only` | Only accept mutable options | `false` |
| `delimiter` | Separator between key=value pairs | `";"` |

## Convenience Optimization Functions

RocksDB provides helper methods on `ColumnFamilyOptions`, `DBOptions`, and `Options` to apply common tuning profiles:

| Method | Target Workload |
|--------|----------------|
| `OptimizeForSmallDb()` | Databases under 1GB, minimizes memory |
| `OptimizeForPointLookup()` | Point lookup only, no iteration |
| `OptimizeLevelStyleCompaction()` | Level compaction with high throughput |
| `OptimizeUniversalStyleCompaction()` | Universal compaction, lower write amp |
| `IncreaseParallelism()` | Multi-core systems, more background threads |
| `PrepareForBulkLoad()` | Bulk loading, disables compaction |
| `DisableExtraChecks()` | Removes non-essential checks for speed |

These methods return `this` to enable call chaining (e.g., `options.OptimizeLevelStyleCompaction()->IncreaseParallelism()`). See the implementations in `options/options.cc`.
