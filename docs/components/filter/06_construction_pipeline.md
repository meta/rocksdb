# Filter Construction Pipeline

**Files:** `table/block_based/filter_policy.cc`, `table/block_based/filter_policy_internal.h`

## Overview

Filter construction accumulates key hashes during SST file creation and generates filter bits in a single `Finish()` call. The pipeline is shared between FastLocalBloom and Ribbon via the `XXPH3FilterBitsBuilder` base class.

## Hash Accumulation

### XXPH3FilterBitsBuilder

All modern filter builders (FastLocalBloom and Ribbon) extend `XXPH3FilterBitsBuilder`, which uses XXH3 (via `GetSliceHash64()`) to hash keys to 64-bit values.

**Hash storage:** Hashes are stored in a `std::deque<uint64_t>` (not `std::vector`) to avoid unnecessary copying during growth and minimize peak memory usage.

**Thread-safe entry counting:** `entries_count` is a `RelaxedAtomic<size_t>` separate from `entries.size()`, allowing `EstimateEntriesAdded()` to be called safely from background threads during parallel compression.

### Deduplication

**AddKey(key):** Hashes the key and appends to the deque only if different from the last entry. Since keys arrive in sorted order, adjacent duplicates are the only duplicates that need elimination.

**AddKeyAndAlt(key, alt):** Handles the common case of adding both a whole key and its prefix. Performs five-way deduplication across successive calls (matching the code comment's k/a notation where k=key, a=alt):
- key_hash vs prev_key_hash (k3<>k2: adjacent key dedup)
- alt_hash vs prev_alt_hash (a3<>a2: adjacent prefix dedup)
- key_hash vs prev_alt_hash (k3<>a2: key matches previous prefix)
- alt_hash vs prev_key_hash (a3<>k2: prefix matches previous key)
- alt_hash vs key_hash (a2<>k2: key equals its own prefix)

The alt is added first so that entries.back() always contains the most recent key hash for the primary dedup path. Note: AddKey() after AddKeyAndAlt() only deduplicates against the previous key hash, not the previous alt hash.

## optimize_filters_for_memory

When `BlockBasedTableOptions::optimize_filters_for_memory` is `true` (default), the filter builder adjusts filter sizes to minimize memory fragmentation using `malloc_usable_size`.

### How It Works

`AllocateMaybeRounding()` in `XXPH3FilterBitsBuilder`:

Step 1 -- Compute the target filter size normally.
Step 2 -- Check `aggregate_rounding_balance_` (a per-policy atomic counter tracking cumulative FP rate deviation).
Step 3 -- If the balance is negative (previous filters were rounded down, contributing less FP rate than target), try smaller filter sizes (3/4, 13/16, 7/8, 15/16 of target) to find one that restores balance.
Step 4 -- Allocate with the chosen size, then call `malloc_usable_size()` to discover the actual allocation size.
Step 5 -- If the usable size exceeds the requested size (up to 4/3 ratio), expand the filter to use the extra space via `RoundDownUsableSpace()`.
Step 6 -- Update `aggregate_rounding_balance_` with the FP rate difference.

This approach saves ~10% memory on average by aligning filter sizes with allocator bucket boundaries, at the cost of ~1-2% more disk usage.

**Requirement:** `format_version >= 5` (only FastLocalBloom and Ribbon support it). Only effective when `ROCKSDB_MALLOC_USABLE_SIZE` is defined (Linux with glibc).

## Cache Reservation During Construction

Filter construction memory can be charged to the block cache to prevent OOM during bulk loading:

**Hash entry charging:** Every `kUint64tHashEntryCacheResBucketSize` hashes, a cache reservation is made via `CacheReservationManager`. The bucket size matches the dummy entry size for `CacheEntryRole::kFilterConstruction`.

**Banding memory charging (Ribbon only):** Before allocating the banding structure, its estimated memory usage is charged. If this fails with `IsMemoryLimit()`, Ribbon falls back to Bloom.

**Final filter charging:** After `AllocateMaybeRounding()`, the final filter buffer is charged to the cache.

Cache charging is enabled when `BlockBasedTableOptions::block_cache` is set and `cache_usage_options.options_overrides[kFilterConstruction].charged == kEnabled`.

## Corruption Detection

When `BlockBasedTableOptions::detect_filter_construct_corruption` is `true` (default `false`):

**During construction:** An XOR checksum of all added hashes is maintained in hash_entries_info_.xor_checksum. After populating the filter bits, MaybeVerifyHashEntriesChecksum() recomputes the checksum and compares. On mismatch, returns Status::Corruption.

**Post-construction verification:** MaybePostVerify() creates a BuiltinFilterBitsReader from the completed filter and re-queries every stored hash. If any hash fails to match (false negative), returns Status::Corruption. For full filters, post-verification is invoked from the table builder's WriteMaybeCompressedBlock(), not from Finish(). For partitioned filters, post-verification is performed inside CutAFilterBlock() at partition-cut time.

**Build failure on corruption:** When corruption is detected, the table build fails. WriteFilterBlock() checks the status from Finish() and stops on Status::Corruption. Post-verification failures in WriteMaybeCompressedBlock() also fail the table build. The result is that flush/compaction returns an error rather than silently installing a degraded filter.

**Cost:** ~30% increase in construction time due to the verification pass. The hash entries are kept in memory until post-verification completes, doubling peak memory for the hash list.

## Legacy Filter Construction

`LegacyBloomBitsBuilder` uses 32-bit hashes (`BloomHash()` instead of `GetSliceHash64()`) and stores them in a `std::vector<uint32_t>`. It does not support `optimize_filters_for_memory` or cache reservation.

The legacy builder also warns about excessive keys: if `num_entries >= 3 million` and the estimated FP rate exceeds 1.5x what the same bits/key would give at 65536 keys, it logs a warning suggesting upgrade to `format_version >= 5`.
