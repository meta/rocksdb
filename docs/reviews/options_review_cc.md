# Review: options -- Claude Code

## Summary
Overall quality rating: **needs work**

The options documentation is ambitious in scope, covering 10 chapters across the full configuration system. The structural organization is good and most default values are correct. However, there are several factually incorrect claims about option defaults and convenience method behavior that could mislead users. The most critical error is documenting `level_compaction_dynamic_level_bytes` as defaulting to `false` when it actually defaults to `true` -- this is a key tuning option that significantly affects compaction behavior. Several convenience methods (`OptimizeForPointLookup`, `PrepareForBulkLoad`, `OptimizeForSmallDb`) are described inaccurately. There are also notable completeness gaps: many DBOptions fields are entirely undocumented.

## Correctness Issues

### [WRONG] level_compaction_dynamic_level_bytes default is false
- **File:** 03_cf_options.md, Compaction Options table; 06_option_defaults_and_sanitization.md is silent on it; index.md does not mention it
- **Claim:** `level_compaction_dynamic_level_bytes` has default `false`
- **Reality:** Default is `true` since RocksDB 8.x. The code reads: `bool level_compaction_dynamic_level_bytes = true;`
- **Source:** `include/rocksdb/advanced_options.h:665`
- **Fix:** Change default to `true`. This is a significant semantic difference -- with `true`, RocksDB auto-adjusts level sizes based on actual data, which is the recommended production setting. The tuning guide in 05_tuning_guide.md should also reflect this as the default rather than presenting it as something to enable.

### [WRONG] PrepareForBulkLoad max_write_buffer_number
- **File:** 05_tuning_guide.md, Bulk Loading profile table
- **Claim:** `max_write_buffer_number = 2` with rationale "Minimal memtable memory"
- **Reality:** Code sets `max_write_buffer_number = 6` with comment "Need to allow more write buffers to allow more parallelism of flushes."
- **Source:** `options/options.cc:512`
- **Fix:** Change to `max_write_buffer_number = 6` with rationale "More flush parallelism". Also add the other options set by `PrepareForBulkLoad()` that are not mentioned: `num_levels = 2`, `max_compaction_bytes = (1 << 60)`, `soft_pending_compaction_bytes_limit = 0`, `hard_pending_compaction_bytes_limit = 0`, `max_background_flushes = 4`, `max_background_compactions = 2`, `target_file_size_base = 256MB`, `min_write_buffer_number_to_merge = 1`.

### [WRONG] OptimizeForPointLookup uses HashSkipListRepFactory
- **File:** 05_tuning_guide.md, Point Lookup Only section
- **Claim:** "Enables `HashSkipListRepFactory` memtable for faster point lookups"
- **Reality:** Does NOT set `HashSkipListRepFactory`. It sets `data_block_index_type = kDataBlockBinaryAndHash`, `memtable_prefix_bloom_size_ratio = 0.02`, and `memtable_whole_key_filtering = true`.
- **Source:** `options/options.cc:634-647`
- **Fix:** Replace the bullet about HashSkipListRepFactory with: "Enables binary+hash data block index (`kDataBlockBinaryAndHash`)", "Sets `memtable_prefix_bloom_size_ratio = 0.02`", "Enables `memtable_whole_key_filtering`"

### [WRONG] OptimizeForPointLookup sets cache_index_and_filter_blocks
- **File:** 05_tuning_guide.md, Point Lookup Only section
- **Claim:** "Sets `cache_index_and_filter_blocks = true`"
- **Reality:** The code does NOT set `cache_index_and_filter_blocks`. It only configures the block-based table with bloom filter and hash index.
- **Source:** `options/options.cc:634-647`
- **Fix:** Remove the claim about `cache_index_and_filter_blocks`.

### [WRONG] OptimizeForSmallDb enables two-level index search
- **File:** 05_tuning_guide.md, Small Database section
- **Claim:** "Two-level index search to reduce per-file memory overhead"
- **Reality:** The code does NOT set two-level index search. It sets: `write_buffer_size = 2MB`, `target_file_size_base = 2MB`, `max_bytes_for_level_base = 10MB`, `soft_pending_compaction_bytes_limit = 256MB`, `hard_pending_compaction_bytes_limit = 1GB`, with a 16MB LRU block cache shared with WriteBufferManager. For `DBOptions::OptimizeForSmallDb`: `max_file_opening_threads = 1`, `max_open_files = 5000`.
- **Source:** `options/options.cc:529-611`
- **Fix:** Remove the two-level index claim. Document actual settings. Also note the block cache is 16MB (not just "small").

### [WRONG] strict_bytes_per_sync is immutable
- **File:** 02_db_options.md, I/O Tuning table
- **Claim:** `strict_bytes_per_sync` Mutable: No
- **Reality:** It is registered in `MutableDBOptions` with `OptionTypeFlags::kMutable`.
- **Source:** `options/db_options.cc:115-118`
- **Fix:** Change Mutable column to Yes.

### [MISLEADING] flush_verify_memtable_count labeled DEPRECATED
- **File:** 02_db_options.md, Data Integrity table
- **Claim:** "DEPRECATED: verify entry count during flush"
- **Reality:** Only `compaction_verify_record_count` has the DEPRECATED annotation in the code. `flush_verify_memtable_count` says "The option is here to turn the feature off in case this new validation feature has a bug. The option may be removed in the future once the feature is stable." -- this is NOT the same as deprecated.
- **Source:** `include/rocksdb/options.h:636-641`
- **Fix:** Remove "DEPRECATED" label from `flush_verify_memtable_count`. It is an active option.

### [WRONG] max_total_wal_size auto-computation formula
- **File:** 02_db_options.md, WAL section
- **Claim:** "The auto-computed max_total_wal_size when set to 0 is `[sum of all write_buffer_size * max_write_buffer_number across all CFs] * 4`. This only takes effect when there are more than one column family."
- **Reality:** The 4x multiplier and the formula are correct per the comment in `include/rocksdb/options.h:809-814`. However, the claim "This only takes effect when there are more than one column family" needs verification -- the comment says "as otherwise the wal size is dictated by the write_buffer_size" but does not explicitly say the auto-computation doesn't apply for single-CF.
- **Source:** `include/rocksdb/options.h:809-821`
- **Fix:** Soften the claim or verify the actual logic in `db_impl.cc`.

### [MISLEADING] CompactionOptionsFIFO max_data_files_size undocumented behavior
- **File:** 03_cf_options.md, FIFO Compaction Options section
- **Claim:** Lists `max_data_files_size` as a FIFO option
- **Reality:** The doc lists it but doesn't explain its relationship to `max_table_files_size`. When `max_data_files_size > 0`, it takes precedence over `max_table_files_size` for ALL FIFO compaction decisions. Also missing `use_kv_ratio_compaction` option which was added recently.
- **Source:** `include/rocksdb/advanced_options.h:131-174`
- **Fix:** Add note about precedence behavior and document `use_kv_ratio_compaction`.

### [WRONG] max_compaction_bytes sanitization with FIFO exception
- **File:** 06_option_defaults_and_sanitization.md, CF Options Sanitization, Compaction options table
- **Claim:** `max_compaction_bytes` set to `target_file_size_base * 25` if 0 (unless FIFO with `use_kv_ratio_compaction`)
- **Reality:** The parenthetical exception for FIFO `use_kv_ratio_compaction` needs verification. The code in `db/column_family.cc` should be checked to confirm this exception exists.
- **Source:** `db/column_family.cc` and `include/rocksdb/advanced_options.h:688-696`
- **Fix:** Verify and if needed, correct the exception condition description.

### [STALE] OPTIONS file block_cache serialization note
- **File:** 08_options_file.md, TableOptions section
- **Claim:** "`block_cache` is not serialized in OPTIONS files because cache objects cannot be reconstructed from a string representation."
- **Reality:** This may be stale. With the Customizable framework and `ObjectRegistry`, some cache types CAN be serialized (e.g., `LRUCache`, `HyperClockCache`). The `LoadLatestOptions()` API accepts a `shared_ptr<Cache>` parameter specifically for restoring cache. The block_cache IS serialized as "nullptr" or the cache type name; it's the cache *state* that can't be reconstructed.
- **Source:** `include/rocksdb/utilities/options_util.h`, `options/options_parser.cc`
- **Fix:** Clarify that cache configuration can be serialized (type + capacity), but not cache contents. Note the `Cache` parameter in `LoadLatestOptions()`.

### [MISLEADING] Compression type validation error code
- **File:** 06_option_defaults_and_sanitization.md, CF-Level Validation table
- **Claim:** "Compression type not supported by build" returns `InvalidArgument`
- **Reality:** Returns `InvalidArgument` only for built-in compression types not linked with the binary (without `CompressionManager`). When using `CompressionManager`, or for non-built-in types, it returns `NotSupported`.
- **Source:** `db/column_family.cc:113-137`
- **Fix:** Clarify the error code depends on whether the compression type is a built-in type and whether `CompressionManager` is in use.

## Completeness Gaps

### Undocumented DBOptions fields (22 missing)
- **Why it matters:** Users reading this documentation would think they have a complete reference but are missing 22 options.
- **Where to look:** `include/rocksdb/options.h`
- **Missing options:**
  - `sst_file_manager` -- controls file deletion rate limiting and space tracking
  - `db_log_dir` -- directory for info LOG files (not WALs)
  - `max_log_file_size` -- max info log file size before rolling (default: 0)
  - `log_file_time_to_roll` -- time-based info log rolling in seconds (default: 0)
  - `keep_log_file_num` -- max number of info log files to keep (default: 1000)
  - `log_readahead_size` -- prefetch size for manifest/WAL reads during Open
  - `wal_filter` -- callback for filtering WAL records during recovery
  - `max_bgerror_resume_count` -- auto-resume attempts after background errors (default: INT_MAX)
  - `bgerror_resume_retry_interval` -- retry interval for auto-resume (default: 1000000 us)
  - `allow_data_in_errors` -- include data in error messages for debugging
  - `db_host_id` -- hostname string written as SST property
  - `file_checksum_gen_factory` -- factory for file-level checksums
  - `checksum_handoff_file_types` -- file types for checksum handoff to filesystem
  - `compaction_service` -- EXPERIMENTAL: remote compaction service
  - `lowest_used_cache_tier` -- controls which cache tiers are used
  - `enforce_single_del_contracts` -- DEPRECATED: enforce SingleDelete/Delete contract
  - `metadata_write_temperature` -- temperature for non-SST/blob/WAL file creation
  - `wal_write_temperature` -- temperature for WAL file creation
  - `calculate_sst_write_lifetime_hint_set` -- EXPERIMENTAL: compaction styles using SST lifetime hints
  - `follower_refresh_catchup_period_ms` -- EXPERIMENTAL: follower mode refresh interval
  - `follower_catchup_retry_count` -- EXPERIMENTAL: follower catchup retry count
  - `follower_catchup_retry_wait_ms` -- EXPERIMENTAL: wait between follower catchup retries
- **Suggested scope:** Add logging options to Observability section, error-recovery options to Recovery section, temperature options to a new Temperature section in 02_db_options.md.

### Undocumented AdvancedColumnFamilyOptions fields (5 missing)
- **Why it matters:** Some of these are well-known configuration knobs (compaction sub-options) or newer features.
- **Where to look:** `include/rocksdb/advanced_options.h`
- **Missing options:**
  - `inplace_callback` -- callback for in-place memtable updates
  - `compaction_options_universal` -- the struct field itself is not listed (though referenced in text)
  - `compaction_options_fifo` -- the struct field itself is not listed (though referenced in text)
  - `memtable_veirfy_per_key_checksum_on_seek` -- memtable checksum verification during seek (note: typo "veirfy" in field name in source)
  - `memtable_batch_lookup_optimization` -- EXPERIMENTAL: batch lookup for memtable MultiGet
- **Suggested scope:** Add to 03_cf_options.md in appropriate sections.

### Undocumented FIFO use_kv_ratio_compaction
- **Why it matters:** New FIFO compaction mode for BlobDB workloads.
- **Where to look:** `include/rocksdb/advanced_options.h:150-174`
- **Suggested scope:** Add to 03_cf_options.md FIFO Compaction Options section.

### Universal compaction max_read_amp undocumented
- **Why it matters:** Controls sorted run limit in universal compaction. Referenced in validation checks but not in the compaction options.
- **Where to look:** `include/rocksdb/universal_compaction.h:73-96`
- **Suggested scope:** Add to 03_cf_options.md Universal Compaction Options section.

### ReadOptions and WriteOptions not covered
- **Why it matters:** The index mentions them in the hierarchy but no chapter covers them. These are per-operation options users set frequently.
- **Where to look:** `include/rocksdb/options.h` (ReadOptions, WriteOptions structs)
- **Suggested scope:** Brief mention is acceptable -- full chapter may be out of scope for this component doc.

### CompactionOptionsUniversal fields not enumerated
- **Why it matters:** 03_cf_options.md says "Key fields include size_ratio, min_merge_width, max_merge_width..." but doesn't provide a complete table.
- **Where to look:** `include/rocksdb/universal_compaction.h`
- **Suggested scope:** Add a table similar to FIFO compaction options.

## Depth Issues

### PrepareForBulkLoad description is incomplete
- **Current:** Only lists 5 options with incorrect values.
- **Missing:** The method sets 11+ options. Key omissions: `num_levels = 2` (critical for bulk load strategy), `max_compaction_bytes = (1<<60)`, `soft/hard_pending_compaction_bytes_limit = 0`, `max_background_flushes = 4`, `target_file_size_base = 256MB`.
- **Source:** `options/options.cc:490-527`

### SetOptions execution flow lacks detail on LogAndApply
- **Current:** 10_option_change_migration.md Step 3 mentions `LogAndApply()` with a pre-callback
- **Missing:** The doc doesn't explain that this atomically applies the options change and logs it to the MANIFEST, making the change durable. Also doesn't mention the `InstallSuperVersionForConfigChange` step explicitly enough.
- **Source:** `db/db_impl/db_impl.cc`

### max_manifest_file_size description is confusing
- **Current:** 02_db_options.md says "Minimum for auto-tuned max manifest size"
- **Missing:** This is not intuitive. Should explain that with `max_manifest_space_amp_pct`, the actual threshold is `max(max_manifest_file_size, estimated_compacted_size * (1 + max_manifest_space_amp_pct/100))`. The description in the same doc section is good, but the table Description column is misleading.
- **Source:** `include/rocksdb/options.h`

## Structure and Style Violations

### index.md line count is acceptable
- **File:** index.md
- **Details:** 41 lines. Within the 40-80 range.

### Hierarchy diagram uses ASCII pipe characters (not box-drawing)
- **File:** 09_customizable_framework.md, Class Hierarchy section
- **Details:** Uses `|`, `+--` which are plain ASCII, not box-drawing characters. This is acceptable.

### Missing **Files:** path verification
- **File:** 09_customizable_framework.md
- **Details:** Lists `options/configurable_helper.h` -- verify this file exists (it may be named differently).

### index.md key invariant about SetOptions and sanitization
- **File:** index.md, lines 38-40
- **Details:** Three of four "Key Invariants" are not true correctness invariants (no data corruption or crash if violated):
  - "`SetOptions()` runs only `ValidateOptions()`, not `SanitizeOptions()`" -- behavioral characteristic
  - "`max_write_buffer_number` is always at least 2" -- sanitization rule, not invariant
  - "L0 trigger ordering is enforced" -- sanitization rule, not invariant
  - Only the comparator invariant (line 37) is a genuine correctness invariant
- **Fix:** Move the three non-invariant items to "Key Characteristics" section.

## Undocumented Complexity

### DB options sanitization creates default WriteBufferManager and SstFileManager
- **What it is:** During `DB::Open()`, if `write_buffer_manager` is nullptr, a default one is created with `db_write_buffer_size`. If `sst_file_manager` is nullptr, a default one is created. These are non-obvious side effects.
- **Why it matters:** Users who check `write_buffer_manager == nullptr` after open may be surprised to find it non-null. The auto-created SstFileManager has its own thread for file deletion.
- **Key source:** `db/db_impl/db_impl_open.cc` SanitizeOptions function
- **Suggested placement:** Add to 06_option_defaults_and_sanitization.md DB Options Sanitization table.

### IncreaseParallelism details undocumented
- **What it is:** `IncreaseParallelism()` sets `max_background_jobs` but also adjusts `Env` thread pool sizes via `SetBackgroundThreads()`. The doc mentions this but doesn't show what it actually does.
- **Why it matters:** Users calling this need to know it modifies the Env's thread pools, not just the option value.
- **Key source:** `options/options.cc` (search for `IncreaseParallelism`)
- **Suggested placement:** Add detail in 01_overview.md convenience functions section.

### ConfigOptions::invoke_prepare_options behavior
- **What it is:** When true (default), `PrepareOptions()` is called after parsing, which can trigger validation and initialization. When false, parsing is raw and no validation occurs.
- **Why it matters:** `SetOptions()` uses `invoke_prepare_options = true`, meaning `PrepareOptions()` runs on all nested Customizable objects. This can have side effects.
- **Key source:** `include/rocksdb/convenience.h`, `options/configurable.cc`
- **Suggested placement:** Add to 07_option_string_parsing.md ConfigOptions section.

### Mutable BlockBasedTableOptions -- which are actually mutable?
- **What it is:** 04_table_options.md says "Except as specifically noted (e.g., block_cache, no_block_cache), all options are mutable via SetOptions()". But this blanket statement may be inaccurate. Some table options may be immutable.
- **Why it matters:** Users relying on this blanket statement may try to change immutable table options at runtime and get errors.
- **Key source:** `table/block_based/block_based_table_factory.cc` type info map
- **Suggested placement:** Clarify which specific table options are immutable in 04_table_options.md.

## Positive Notes

- The 10-chapter structure is well-organized and covers the options system comprehensively from overview through dynamic changes.
- The sanitization tables in 06_option_defaults_and_sanitization.md are particularly valuable -- these silent adjustments are extremely hard to discover from reading source comments alone.
- The validation checks enumeration is thorough and will save users time debugging `NotSupported` / `InvalidArgument` errors.
- The Customizable framework documentation in 09_customizable_framework.md is clear and covers the complex multi-pass configuration logic well.
- The OPTIONS file chapter correctly identifies the round-trip verification mechanism and the retry-on-corruption behavior.
- Option interaction documentation (L0 trigger ordering, rate limiter + bytes_per_sync, etc.) captures non-obvious coupling that would be easy to miss.
