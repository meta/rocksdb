# OPTIONS File

**Files:** `options/options_parser.h`, `options/options_parser.cc`, `utilities/options/options_util.cc`, `include/rocksdb/utilities/options_util.h`

## Overview

RocksDB automatically persists the full database configuration to disk in OPTIONS files. These files capture both DB-wide and per-column-family options in a human-readable INI-like format. They are generated on every configuration-changing event and can be loaded to reconstruct options for reopening a database.

## File Naming and Location

OPTIONS files are stored in the database directory with the naming pattern `OPTIONS-{file_number}`, where `file_number` is a monotonically increasing sequence number from the MANIFEST file number space. Multiple OPTIONS files may exist simultaneously; the one with the highest number is the latest.

`GetLatestOptionsFileName()` (in `include/rocksdb/utilities/options_util.h`) finds the most recent OPTIONS file by scanning the database directory.

## When OPTIONS Files Are Generated

A new OPTIONS file is created automatically during:

- `DB::Open()` -- captures the initial configuration
- `DB::SetOptions()` -- records per-column-family option changes
- `DB::SetDBOptions()` -- records database-wide option changes
- `DB::CreateColumnFamily()` -- reflects the new column family
- `DB::DropColumnFamily()` -- reflects the removed column family

The generation is performed by `PersistRocksDBOptions()` (declared in `options/options_parser.h`, defined in `options/options_parser.cc`), called internally by `DBImpl::WriteOptionsFile()`.

## File Format

The OPTIONS file uses a section-based INI format with four section types:

### Version Section

Always appears first. Contains the RocksDB version that generated the file and the options file format version.

```ini
[Version]
  rocksdb_version=9.10.0
  options_file_version=1.1
```

The `rocksdb_version` field is used during parsing to determine whether unknown options should be treated as corruption (same or earlier version) or ignored (newer version).

The `options_file_version` is currently `1.1` (defined by `ROCKSDB_OPTION_FILE_MAJOR` and `ROCKSDB_OPTION_FILE_MINOR` in `options/options_parser.h`). Version must be at least `1.0`.

### DBOptions Section

Exactly one `[DBOptions]` section contains all database-wide options, serialized via `GetStringFromDBOptions()`.

```ini
[DBOptions]
  max_background_jobs=2
  bytes_per_sync=1048576
  create_if_missing=false
```

### CFOptions Sections

One `[CFOptions "name"]` section per column family. The `"default"` column family must appear first.

```ini
[CFOptions "default"]
  write_buffer_size=67108864
  compression=kSnappyCompression
  num_levels=7
```

Column family names are escaped using `EscapeOptionString()` to handle special characters.

### TableOptions Sections

One `[TableOptions/FactoryName "cf_name"]` section per column family that has a table factory. The section title includes the table factory name (e.g., `BlockBasedTable`).

```ini
[TableOptions/BlockBasedTable "default"]
  block_size=4096
  filter_policy=nullptr
  cache_index_and_filter_blocks=false
```

Note: `block_cache` configuration (type and capacity) can be serialized in OPTIONS files, but cache contents and state cannot be reconstructed from the persisted representation. When loading options via `LoadOptionsFromFile()` / `LoadLatestOptions()`, a `shared_ptr<Cache>` can be passed to restore the block cache; otherwise it defaults to an 8MB LRU cache.

### Format Rules

- Comments start with `#` (unescaped)
- Statements are `name=value` on a single line (multi-line values not supported)
- Leading/trailing whitespace is trimmed
- Each section must appear in order: Version, DBOptions, then CFOptions/TableOptions pairs
- The default column family must be the first CFOptions section
- No duplicate section names are allowed

## Writing OPTIONS Files

The write path in `PersistRocksDBOptions()` follows these steps:

1. **Open file**: Create a new writable file at the specified path
2. **Write header**: Emit comment block and `[Version]` section with RocksDB version and options file version
3. **Write DBOptions**: Emit `[DBOptions]` section via `GetStringFromDBOptions()` using newline delimiter
4. **Write per-CF sections**: For each column family, emit `[CFOptions "name"]` via `GetStringFromColumnFamilyOptions()`, followed by `[TableOptions/FactoryName "name"]` if a table factory exists
5. **Sync and close**: `fsync` the file to ensure durability
6. **Verify**: Parse the file back and compare with the input options via `VerifyRocksDBOptionsFromFile()` to detect serialization bugs

The verification step (Step 6) is a round-trip check: the file is re-parsed and compared field-by-field against the original options. This catches serialization/deserialization bugs early.

## Reading OPTIONS Files

### RocksDBOptionsParser

`RocksDBOptionsParser` (defined in `options/options_parser.h`) is the core parser. Its `Parse()` method reads the file line by line:

1. **Read lines**: Use `LineFileReader` with configurable readahead size (default 512KB, controlled by `ConfigOptions::file_readahead_size`)
2. **Classify lines**: Each line is either a section header (`[...]`), a statement (`name=value`), a comment (`#...`), or blank
3. **Accumulate**: Collect `name=value` pairs into an option map until the next section header
4. **Process section**: On section transition, call `EndSection()` which delegates to `GetDBOptionsFromMap()`, `GetColumnFamilyOptionsFromMap()`, or `TableFactory::CreateFromString()` + `ConfigureFromMap()` as appropriate
5. **Validate**: After parsing, `ValidityCheck()` ensures both a `[DBOptions]` section and a `[CFOptions "default"]` section were found

The parser stores results in member fields: `db_opt_` for DB options, `cf_opts_` for column family options (with corresponding names in `cf_names_`), and the raw string maps in `db_opt_map_` and `cf_opt_maps_` for later verification use.

### Integrity and Corruption Detection

Unlike MANIFEST and SST files which use checksums for integrity verification, OPTIONS files rely on syntax checking for corruption detection. The INI parser validates section structure, statement format, and option value parsing. Most corruption is caught because it produces syntactically invalid lines or unrecognizable option values. However, silent corruption that produces valid but incorrect values (e.g., a bit flip changing a digit in a numeric option) would not be detected by the parser alone. The post-write verification step in `PersistRocksDBOptions()` provides an additional layer of protection for newly written files.

### Retry on Corruption

If parsing fails with `Corruption` or `InvalidArgument` and the filesystem supports verified reads (`FSSupportedOps::kVerifyAndReconstructRead`), the parser retries once with read verification enabled. This handles cases where storage-level bit flips corrupt the OPTIONS file.

### Public API: LoadLatestOptions / LoadOptionsFromFile

The high-level API for loading options is in `include/rocksdb/utilities/options_util.h`:

- `LoadLatestOptions()` -- finds the latest OPTIONS file and loads it
- `LoadOptionsFromFile()` -- loads a specific OPTIONS file

Both functions return `DBOptions` and a vector of `ColumnFamilyDescriptor`. The workflow for using loaded options:

Step 1: Call `LoadLatestOptions()` to get base options

Step 2: Manually restore pointer-based options that cannot be serialized:
  - `env` / `file_system`
  - `comparator`
  - `merge_operator`
  - `compaction_filter` / `compaction_filter_factory`
  - `prefix_extractor`
  - `table_factory` (including `block_cache`)
  - `memtable_factory`
  - `statistics`
  - `rate_limiter`
  - `listeners`

Step 3: Call `DB::Open()` with the restored options

Important: If a `shared_ptr<Cache>` is passed to `LoadOptionsFromFile()` / `LoadLatestOptions()`, the loaded `BlockBasedTableOptions` will use that cache as `block_cache`. Otherwise, `block_cache` defaults to an 8MB LRU cache (the `BlockBasedTableOptions` constructor default).

## Verification

### VerifyRocksDBOptionsFromFile

`RocksDBOptionsParser::VerifyRocksDBOptionsFromFile()` compares in-memory options against a persisted OPTIONS file. It:

1. Parses the file into a separate set of options
2. Calls `VerifyDBOptions()` to compare DB options field by field
3. Verifies column family names match in count and order
4. Calls `VerifyCFOptions()` for each column family
5. Calls `VerifyTableFactory()` for each column family's table factory

### CheckOptionsCompatibility

`CheckOptionsCompatibility()` (in `include/rocksdb/utilities/options_util.h`) checks whether a set of options is compatible with a previously persisted configuration. This is used to detect breaking changes before opening a database. The following options cause compatibility failures:

- `comparator`
- `prefix_extractor`
- `table_factory` (type mismatch)
- `merge_operator`
- `persist_user_defined_timestamps`

### Sanity Levels

The `ConfigOptions::SanityLevel` controls comparison strictness:

| Level | Value | Behavior |
|-------|-------|----------|
| `kSanityLevelNone` | `0x01` | No comparison performed |
| `kSanityLevelLooselyCompatible` | `0x02` | Only checks format-critical options (comparator, table format) |
| `kSanityLevelExactMatch` | `0xFF` | Every option must match exactly |

`CheckOptionsCompatibility()` uses `kSanityLevelLooselyCompatible` by default, while `PersistRocksDBOptions()` verification uses `kSanityLevelExactMatch`.

## Forward and Backward Compatibility

OPTIONS files support version-aware compatibility:

- **Forward compatibility** (reading files from newer RocksDB): Set `ignore_unknown_options=true` in `ConfigOptions`. Unknown options are skipped rather than causing errors. However, this only applies when the file's `rocksdb_version` is newer than the current binary; files from the same or older version treat unknown options as corruption.

- **Backward compatibility** (reading files from older RocksDB): Options added in newer versions will have their default values when not present in the file. Deprecated options are handled via `OptionVerificationType::kDeprecated` in the `OptionTypeInfo` map -- they are accepted during parsing but not written during serialization.

- **Table factory compatibility**: If a table factory type in the file is not recognized (e.g., a custom factory not registered in the `ObjectRegistry`), the section is silently skipped and `table_factory` is reset to `nullptr` rather than failing the parse.
