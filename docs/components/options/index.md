# RocksDB Options

## Overview

RocksDB's configuration system organizes hundreds of tunable parameters into a layered hierarchy: `DBOptions` for database-wide settings, `ColumnFamilyOptions` for per-CF settings, and `BlockBasedTableOptions` for table format settings. Options are split internally into mutable and immutable categories, with mutable options changeable at runtime via `SetOptions()` and `SetDBOptions()`. The system includes automatic sanitization, validation, string-based parsing and serialization, OPTIONS file persistence, and a Configurable/Customizable framework for polymorphic component configuration.

**Key source files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/table.h`, `include/rocksdb/convenience.h`, `options/db_options.h`, `options/cf_options.h`, `options/options_helper.cc`, `options/options_parser.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Options System Overview | [01_overview.md](01_overview.md) | Options hierarchy, mutable vs immutable split, lifecycle during `DB::Open()`, `SetOptions()` semantics, serialization framework, and convenience tuning methods. |
| 2. DBOptions Fields | [02_db_options.md](02_db_options.md) | Database-wide options for environment, file management, threading, WAL, write path, I/O tuning, observability, data integrity, and recovery. |
| 3. ColumnFamilyOptions Fields | [03_cf_options.md](03_cf_options.md) | Per-column-family options for memtable, compaction, compression, table factory, prefix extraction, blob storage, data integrity, and tiered storage. |
| 4. Table Options | [04_table_options.md](04_table_options.md) | `BlockBasedTableOptions` for block cache, metadata pinning, block configuration, index types, filter configuration, checksums, format versions, and readahead; plus `PlainTableOptions` and `CuckooTableOptions`. |
| 5. Tuning Guide | [05_tuning_guide.md](05_tuning_guide.md) | Step-by-step tuning for parallelism, memtable sizing, level sizing, write stalls, compression, block cache, and file sizes; common configuration profiles for write-heavy, read-heavy, space-optimized, bulk loading, and small databases. |
| 6. Option Defaults and Sanitization | [06_option_defaults_and_sanitization.md](06_option_defaults_and_sanitization.md) | Default values for all option structs, sanitization rules applied during `DB::Open()`, validation checks that reject invalid combinations, and implicit option interactions. |
| 7. Option String Parsing | [07_option_string_parsing.md](07_option_string_parsing.md) | String and map-based parsing APIs, value type formatting rules, `OptionTypeInfo` metadata framework, enum string maps, nested struct and table factory parsing, and serialization. |
| 8. OPTIONS File | [08_options_file.md](08_options_file.md) | INI-format OPTIONS file structure, generation triggers, `RocksDBOptionsParser`, `LoadLatestOptions()` API, round-trip verification, `CheckOptionsCompatibility()`, and forward/backward compatibility. |
| 9. Customizable Framework | [09_customizable_framework.md](09_customizable_framework.md) | `Configurable` and `Customizable` base classes, registration and configuration workflow, `ObjectRegistry` factory mechanism, serialization/comparison, and integration with the options system. |
| 10. Dynamic Option Changes | [10_option_change_migration.md](10_option_change_migration.md) | `SetOptions()` and `SetDBOptions()` execution flow, caveats (no rollback, no sanitization), immediate vs gradual effect timing, immutable option change workflow, and OPTIONS file lifecycle. |

## Key Invariants

- The `comparator` must have the same name and produce the same key ordering across all opens of a database

## Key Characteristics

- **Layered hierarchy**: `DBOptions` (database-wide), `ColumnFamilyOptions` (per-CF), `BlockBasedTableOptions` (table format), `ReadOptions`/`WriteOptions` (per-operation)
- **Mutable/immutable split**: Each option is registered as mutable or immutable; mutable options changeable at runtime via `SetOptions()` / `SetDBOptions()`
- **Automatic sanitization**: `DB::Open()` silently adjusts options for consistency (e.g., clamps `write_buffer_size` to [64KB, 64GB], enforces L0 trigger ordering)
- **Validation**: Cross-option consistency checks reject invalid combinations (e.g., mmap reads + direct reads)
- **String-based configuration**: Full round-trip parsing and serialization via `OptionTypeInfo` framework with support for nested structs, enums, and Customizable objects
- **OPTIONS file persistence**: Configuration automatically persisted to INI-format files on every configuration-changing event
- **Configurable/Customizable framework**: Polymorphic components (caches, filters, table factories) participate in unified configuration via `ObjectRegistry`
- **Convenience methods**: `OptimizeForSmallDb()`, `OptimizeForPointLookup()`, `PrepareForBulkLoad()`, `IncreaseParallelism()` for common tuning profiles
- `SetOptions()` runs only `ValidateOptions()`, not `SanitizeOptions()` -- sanitization rules do not apply to runtime changes
- `max_write_buffer_number` is always sanitized to at least 2 (one active, one flushing)
- L0 trigger ordering is enforced by sanitization: `level0_stop_writes_trigger >= level0_slowdown_writes_trigger >= level0_file_num_compaction_trigger`
