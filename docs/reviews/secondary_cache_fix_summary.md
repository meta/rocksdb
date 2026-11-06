# Fix Summary: secondary_cache

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 5 |
| Completeness | 4 |
| Structure/Style | 1 |
| **Total** | **10** |

## Disagreements Found

0 -- Only the CC review existed (no Codex review file found).

## Changes Made

### 01_interface.md
- Fixed Files line: replaced `cache/secondary_cache.cc` (effectively empty file) with `cache/cache.cc` (where `kSliceCacheItemHelper` is defined)

### 02_adapter.md
- Lookup Flow Step 3: explicitly documented that `found_dummy_entry` is passed as the `advise_erase` parameter to `secondary_cache_->Lookup()`, explaining how this drives erase-on-lookup behavior
- Standalone Handle Path Step 2: documented that dummy uses `kNoopCacheItemHelper` (not secondary-cache-compatible), so it is silently discarded on eviction rather than demoted

### 03_admission_policies.md
- `kAdmPolicyAllowCacheHits` table entry: clarified that `force_insert=false` still offers the entry to secondary cache (which applies its own admission policy), not a simple discard

### 04_compressed_secondary_cache.md
- InsertSaved Step 1: corrected "silently a no-op" -- the `kVolatileCompressedTier` check includes a debug assertion that will abort in debug builds
- Capacity Management: added note about `ROCKSDB_MALLOC_USABLE_SIZE` charge accounting using `malloc_usable_size()` instead of logical data size
- Added note about decompression failure handling: entry is erased, `nullptr` returned, original compressed entry is lost; assertions fire in debug builds

### 07_configuration.md
- Added "DB-Level Options" section documenting `DBOptions::lowest_used_cache_tier` as the primary on/off switch for secondary cache
- Added caveat about `UpdateTieredCache()` non-atomic application of capacity/ratio/policy changes

### 08_testing.md
- Added "Limitations" subsection documenting that `FaultInjectionSecondaryCache` does not override `Deflate()`/`Inflate()`, breaking compatibility with proportional reservation

### index.md
- Added "DB-level control" bullet to Key Characteristics
- Added dummy entry charge = 0 invariant to Key Invariants
- Expanded from 38 to 40 lines (meeting 40-line minimum)

## Review Issues Not Addressed (Intentional)

The following items from the CC review were noted but not addressed as doc fixes:

- **TieredSecondaryCache unused `comp_sec_cache_` member**: code cleanup opportunity, not a documentation issue
- **V2 compression framework migration history**: the doc already mentions V2 framework usage; adding PR history is out of scope for component docs
- **Async lookup inner vs. outer secondary caches**: the review acknowledged this as a depth issue, not a correctness issue; the current description is accurate
- **Proportional reservation chunking example**: the review acknowledged the example is correct; explicitly showing the rounding step would add detail without fixing an error
