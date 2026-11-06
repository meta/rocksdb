# Review: secondary_cache -- Claude Code

## Summary
Overall quality rating: **good**

The secondary cache documentation is well-structured and covers the major concepts thoroughly. The multi-layered admission protocol description (chapter 3) is especially well done, with clear step-by-step traces that match the actual code flow. The tiered cache architecture (chapter 5) and proportional reservation mechanism (chapter 6) are accurately described with appropriate detail.

The main concerns are: (1) a few misleading or imprecise claims about the adapter-to-secondary-cache parameter passing, (2) one incorrect Files reference, (3) missing documentation of the `lowest_used_cache_tier` option and its interaction with secondary cache, and (4) the index.md is slightly below the 40-line minimum.

## Correctness Issues

### [MISLEADING] advise_erase parameter is actually found_dummy_entry
- **File:** 02_adapter.md, "Lookup Flow" Step 3
- **Claim:** "look up in the secondary cache with `secondary_cache_->Lookup()`" (no mention of what is passed for advise_erase)
- **Reality:** The adapter passes `found_dummy_entry` as the `advise_erase` parameter to `secondary_cache_->Lookup()`. This is a critical detail because it drives whether the secondary cache erases the entry or keeps it. The connection between the adapter's dummy-entry tracking and the secondary cache's erase-on-lookup behavior is the key to understanding the whole two-hit protocol, but neither chapter 2 nor chapter 3 makes this explicit.
- **Source:** `cache/secondary_cache_adapter.cc` `CacheWithSecondaryAdapter::Lookup()`, lines 306-307
- **Fix:** In chapter 2 Step 3, explicitly state: "look up in the secondary cache via `secondary_cache_->Lookup()`, passing `found_dummy_entry` as the `advise_erase` parameter. This tells the secondary cache whether to erase its copy (second access) or keep it (first access)."

### [WRONG] Chapter 1 Files reference includes empty file
- **File:** 01_interface.md, Files line
- **Claim:** "**Files:** `include/rocksdb/secondary_cache.h`, `include/rocksdb/advanced_cache.h`, `cache/secondary_cache.cc`"
- **Reality:** `cache/secondary_cache.cc` contains only a namespace declaration and an include -- it is effectively empty. The `kSliceCacheItemHelper` (the main relevant definition) is in `cache/cache.cc`, not `cache/secondary_cache.cc`.
- **Source:** `cache/secondary_cache.cc` (10 lines, empty namespace), `cache/cache.cc` lines 94-97
- **Fix:** Replace `cache/secondary_cache.cc` with `cache/cache.cc` in the Files line.

### [MISLEADING] Standalone handle dummy charge description
- **File:** 02_adapter.md, "Standalone Handle Path" Step 2
- **Claim:** "Insert a **dummy entry** (charge = 0) into the primary cache to record the access."
- **Reality:** The dummy entry is inserted with `kDummyObj` as the value and `&kNoopCacheItemHelper` as the helper (which is NOT secondary-cache-compatible). The charge of 0 is correct, but the doc omits the fact that the dummy uses `kNoopCacheItemHelper`, which means it will not trigger the eviction handler's secondary cache insertion path. This is an important implementation detail that prevents dummy entries from being demoted.
- **Source:** `cache/secondary_cache_adapter.cc` `Promote()`, line 220
- **Fix:** Mention that the dummy uses `kNoopCacheItemHelper` (not secondary-cache-compatible), so it is silently discarded on eviction rather than being sent to the secondary cache.

### [MISLEADING] kAdmPolicyAllowCacheHits eviction behavior description is incomplete
- **File:** 03_admission_policies.md, TieredAdmissionPolicy table
- **Claim:** "`kAdmPolicyAllowCacheHits` | If the evicted entry had been hit (`was_hit`), uses `force_insert=true`; otherwise `force_insert=false`"
- **Reality:** When `force_insert=false` and the secondary cache is `CompressedSecondaryCache`, the entry still goes through the two-hit dummy protocol. The doc's description is technically correct but doesn't make it clear that non-hit entries are NOT simply discarded -- they enter the secondary's own admission pipeline. A reader might think `force_insert=false` means "don't insert."
- **Source:** `cache/secondary_cache_adapter.cc` `EvictionHandler()`, line 151: `secondary_cache_->Insert(key, obj, helper, force)`
- **Fix:** Add a note: "When `force_insert=false`, the entry is still offered to the secondary cache, which applies its own admission policy (e.g., the two-hit dummy protocol in `CompressedSecondaryCache`)."

### [MISLEADING] InsertSaved no-op conditions description
- **File:** 04_compressed_secondary_cache.md, "From SST Block (InsertSaved)" Step 1
- **Claim:** "`InsertSaved` currently requires `type != kNoCompression` and `source != kVolatileCompressedTier` and `!enable_custom_split_merge`. If any condition fails, the call is silently a no-op."
- **Reality:** The conditions are checked sequentially with early returns, not as a single predicate. More importantly, the `source != kVolatileCompressedTier` check includes a `assert(source != CacheTier::kVolatileCompressedTier)` that will crash in debug builds, making it not truly "silent." The other two are genuinely silent.
- **Source:** `cache/compressed_secondary_cache.cc` `InsertSaved()`, lines 304-316
- **Fix:** Note that the `kVolatileCompressedTier` source check includes a debug assertion, so in debug builds it will abort rather than silently returning.

## Completeness Gaps

### lowest_used_cache_tier option not documented
- **Why it matters:** `DBOptions::lowest_used_cache_tier` (default `kNonVolatileBlockTier`) controls whether secondary cache is used at all for block cache lookups. Setting it to `kVolatileTier` disables secondary cache lookups. This is the primary on/off switch for secondary cache from the DB options level, and recent PR #13030 fixed its handling for compressed blocks.
- **Where to look:** `include/rocksdb/options.h` line 1678, `table/block_based/block_based_table_reader.cc`
- **Suggested scope:** Mention in chapter 7 (Configuration and Tuning) under a "DB-Level Options" subsection.

### FaultInjectionSecondaryCache does not forward Deflate/Inflate
- **Why it matters:** `FaultInjectionSecondaryCache` extends `SecondaryCache` directly (not `SecondaryCacheWrapper`), so `Deflate()` and `Inflate()` return `Status::NotSupported()`. This means fault injection cannot be used with `distribute_cache_res=true` (proportional reservation) without breaking. Any developer wrapping secondary caches for testing should know this limitation.
- **Where to look:** `utilities/fault_injection_secondary_cache.h` -- no Deflate/Inflate overrides
- **Suggested scope:** Brief mention in chapter 8 under a "Limitations" subsection.

### No documentation of ROCKSDB_MALLOC_USABLE_SIZE charge accounting
- **Why it matters:** When `ROCKSDB_MALLOC_USABLE_SIZE` is defined and `enable_custom_split_merge` is false, the charge for entries in the internal LRU is based on `malloc_usable_size()` rather than the logical data size. This can cause the cache to use significantly less or more capacity than expected, depending on the allocator's overhead. PR #13032 improved this accounting.
- **Where to look:** `cache/compressed_secondary_cache.cc` `InsertInternal()`, lines 274-278
- **Suggested scope:** Brief mention in chapter 4 under "Capacity Management" or as a note in chapter 7.

### V2 compression framework details underdocumented
- **Why it matters:** The doc correctly mentions that CompressedSecondaryCache uses `Compressor` and `Decompressor` objects from `GetBuiltinV2CompressionManager()`, but doesn't explain that this was a recent refactoring (PR #13797, #13540, #13805). Developers modifying the compression path need to know they should use the V2 API (not the older `Compress`/`Uncompress` functions).
- **Where to look:** `include/rocksdb/advanced_compression.h`, `cache/compressed_secondary_cache.cc` constructor
- **Suggested scope:** Brief note in chapter 4.

## Depth Issues

### Adapter's async lookup does not fully explain inner vs. outer secondary caches
- **Current:** Chapter 2 describes `StartAsyncLookup` and `WaitAll` with "inner" vs. "my" secondary cache terminology.
- **Missing:** It's not explained when "inner" secondary caches exist. This happens when `CacheWithSecondaryAdapter` wraps a primary cache that itself has secondary cache support (e.g., nested adapters). The current doc doesn't provide a concrete example of when this stacking occurs.
- **Source:** `cache/secondary_cache_adapter.cc` `WaitAll()`, lines 390-465

### Proportional reservation example table has imprecise numbers
- **Current:** Chapter 6 shows an example with 100MB total, 70/30 split, 10MB placeholder. After deflate/adjust: "63MB primary usable, 27MB sec cache reserve."
- **Missing:** The example doesn't account for the chunking behavior. `reserved_usage_` is rounded down to the nearest 1MB boundary: `10MB & ~(1MB-1) = 10MB` (10 is already aligned). So `new_sec_reserved = 10 * 0.3 = 3MB`, `sec_charge = 3 - 0 = 3MB`. The secondary deflates by 3MB (27MB effective), and `pri_cache_res_` decreases by 3MB (primary gains 3MB usable). The table is correct but could show the rounding step explicitly since non-aligned values would behave differently.
- **Source:** `cache/secondary_cache_adapter.cc` `Insert()`, lines 256-276

## Structure and Style Violations

### index.md is below 40-line minimum
- **File:** `docs/components/secondary_cache/index.md`
- **Details:** The file is 38 lines (with trailing newline), below the 40-line target. Add 2-3 lines of content to the Key Characteristics or Key Invariants sections.

### Missing chapter number in naming
- **File:** All chapter files
- **Details:** There is no chapter 0 or logical gap, but chapters skip from 06 to 08 (no 07... wait, 07 exists). Actually the numbering is contiguous 01-08. This is fine. No issue.

## Undocumented Complexity

### CompressedSecondaryCache dummy charge of 0 in MaybeInsertDummy
- **What it is:** When `MaybeInsertDummy` inserts a dummy entry, it uses `charge=0`. This means dummy entries consume no cache capacity, allowing potentially unbounded dummy accumulation. The LRU eviction won't reclaim them because they have no charge. They can only be replaced by real entries for the same key or by explicit erase.
- **Why it matters:** In workloads with many unique keys that are accessed only once, the cache could accumulate many zero-charge dummies that occupy hash table slots but contribute no useful data. The dummies are eventually evicted by LRU ordering when capacity pressure pushes out entries regardless of charge.
- **Key source:** `cache/compressed_secondary_cache.cc` `MaybeInsertDummy()`, line 166
- **Suggested placement:** Add a note to chapter 3 or chapter 4 explaining this trade-off.

### TieredSecondaryCache has unused comp_sec_cache_ member
- **What it is:** The `TieredSecondaryCache` class declares a `std::shared_ptr<SecondaryCache> comp_sec_cache_` private member, but the constructor never initializes it (the compressed cache is passed to the `SecondaryCacheWrapper` base class as `target_`). The `comp_sec_cache_` member is dead code.
- **Why it matters:** This is confusing for developers reading the code. The compressed secondary cache is accessed via `target()` (inherited from `SecondaryCacheWrapper`), not via `comp_sec_cache_`.
- **Key source:** `cache/tiered_secondary_cache.h`, line 154
- **Suggested placement:** Not a doc issue, but a code cleanup opportunity.

### UpdateTieredCache applies changes non-atomically
- **What it is:** `UpdateTieredCache()` applies capacity, ratio, and policy changes as three independent operations (lines 739-747 of `secondary_cache_adapter.cc`). If the ratio update fails, the capacity change is not rolled back. The function returns only the last non-OK status, potentially hiding errors.
- **Why it matters:** Callers should know that partial updates are possible. The function could succeed on capacity change but fail on ratio change, leaving the cache in an inconsistent state.
- **Key source:** `cache/secondary_cache_adapter.cc` `UpdateTieredCache()`, lines 728-749
- **Suggested placement:** Add a caveat in chapter 7 under "Dynamic Updates."

### Decompression failure handling in CompressedSecondaryCache::Lookup
- **What it is:** If decompression fails during Lookup, the entry is erased from the internal LRU cache (`erase_if_last_ref=true`) and nullptr is returned. The caller (adapter) treats this as a cache miss and falls back to reading from storage. The original compressed entry is lost.
- **Why it matters:** This is defensive behavior but could lead to silent data loss in the cache. In debug builds, assertions will fire on decompression failure.
- **Key source:** `cache/compressed_secondary_cache.cc` `Lookup()`, lines 114-117
- **Suggested placement:** Add a note about error handling in chapter 4.

## Positive Notes

- The multi-layered admission protocol trace in chapter 3 ("Interaction Between Layers") is excellent. It correctly maps the 6-step sequence of first access through fourth access, showing how the adapter's dummy entries and the secondary cache's dummy entries interact. This was verified step-by-step against the code and is accurate.
- The proportional reservation mechanism (chapter 6) is one of the most complex parts of the secondary cache subsystem, and the doc does a good job explaining both the memory layout and the ordering of operations during resize (shrink before expand, expand before reserve). The example table helps cement understanding.
- The chapter structure is logical, progressing from interface to implementation to configuration. Each chapter has clear Files references and step-by-step descriptions.
- The "Common Pitfalls" section in chapter 7 covers the most important gotchas, especially the split/merge + InsertSaved incompatibility and the inability to re-enable after setting ratio to 0.0.
- The description of the V2 compression framework usage in chapter 4 is up-to-date with the recent refactoring.
