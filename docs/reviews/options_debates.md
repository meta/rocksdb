# Options Documentation Debates

## Debate: OptimizeForSmallDb two-level index search

- **CC position**: "The code does NOT set two-level index search. [...] Remove the two-level index claim."
- **Doc position**: "Two-level index search (`kTwoLevelIndexSearch`) to reduce LRU cache imbalance"
- **Code evidence**: `options/options.cc:627-628` clearly sets `table_options.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;` inside `ColumnFamilyOptions::OptimizeForSmallDb()`. The code comment says "Two level iterator to avoid LRU cache imbalance" which matches the doc exactly.
- **Resolution**: CC review was wrong. The doc is correct. No change needed.
- **Risk level**: low -- straightforward code read

## Debate: Most CC review items already fixed in current docs

- **CC position**: Raised 12 correctness issues and 6 completeness gaps
- **Doc position**: Current docs already have correct values for most items
- **Code evidence**: The following CC claims of errors do not match the current docs:
  - `level_compaction_dynamic_level_bytes` default: doc already says `true` (03_cf_options.md)
  - `PrepareForBulkLoad` values: doc already lists all 12 options with correct values (05_tuning_guide.md)
  - `OptimizeForPointLookup`: doc already describes the correct behavior without HashSkipListRepFactory (05_tuning_guide.md)
  - `strict_bytes_per_sync` mutability: doc already says "Yes" (02_db_options.md)
  - `max_total_wal_size` single-CF claim: matches source comment in `include/rocksdb/options.h:820-821`
  - `max_compaction_bytes` FIFO exception: confirmed in `db/column_family.cc:404-410`
  - FIFO `use_kv_ratio_compaction` and `max_data_files_size` precedence: already documented (03_cf_options.md)
  - `max_read_amp`: already mentioned in universal compaction section (03_cf_options.md)
  - `configurable_helper.h`: file exists at `options/configurable_helper.h`
- **Resolution**: CC review appears to have been run against an earlier version of the docs. Most issues were already addressed.
- **Risk level**: low
