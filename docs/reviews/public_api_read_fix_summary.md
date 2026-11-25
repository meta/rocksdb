# Fix Summary: public_api_read

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 5 |
| Completeness | 3 |
| Depth | 4 |
| Style | 0 |

## Correctness Fixes

1. **Range deletion short-circuit (ch01)** -- Replaced misleading claim that Get() encounters `kTypeRangeDeletion` records during point key lookup. In reality, range tombstones are stored separately in `FragmentedRangeTombstoneIterator` and checked as a pre-pass; the `kTypeRangeDeletion` type is synthetically assigned when a covering tombstone's sequence number exceeds the point key's.

2. **Iterator corruption with Valid()=true (ch03)** -- Removed incorrect claim that `Valid()=true` can coexist with `status()=Corruption`. At the public DBIter level, corruption always sets `valid_=false` immediately. The code has `assert(!valid_)` in the non-OK `status()` path. Rewrote with correct pattern.

3. **Readahead reset at level boundary (ch07)** -- Changed "(at each level boundary)" to "when the iterator moves to a new SST file (within or across levels)". The implementation resets on non-sequential access via `IsBlockSequential()`, which fires at any file boundary, not just level transitions.

4. **Iterator Next() CPU cost statistics (ch03)** -- Removed unverifiable specific percentages ("~90%", "~81%") and replaced with qualified language about typical leveled compaction workloads.

5. **kPersistedTier description (ch08)** -- Changed from "SST files only; skip memtable when WAL disabled" to accurate description: memtable data is included when WAL is enabled (since WAL makes it persisted). Added note that iterators are not supported.

## Completeness Fixes

1. **MultiScanArgs API surface (ch04)** -- Expanded from a thin description to document the full class: `Comparator*` constructor, `insert()` method overloads, configuration fields (`io_coalesce_threshold`, `max_prefetch_size`, `use_async_io`), `CopyConfigFrom()`, and `ScanOptions::property_bag`.

2. **Missing auxiliary read APIs (ch06)** -- Added `GetPropertiesOfTablesByLevel()`, `GetIntProperty()`/`GetAggregatedIntProperty()`, and `VerifyChecksum()`/`VerifyFileChecksums()`.

3. **kPersistedTier iterator limitation (ch08)** -- Added "Iterators not supported" to the ReadTier table.

## Depth Fixes

1. **MultiCFSnapshot retry mechanism (ch02)** -- Added retry limit (3 attempts), mutex acquisition on last retry, and the actual check condition (`sv->mem->GetEarliestSequenceNumber() > snapshot`).

2. **auto_refresh_iterator_with_snapshot UDT limitation (ch03)** -- Added NOTE 2 about user-defined timestamps with `persist_user_defined_timestamps=false`.

3. **Deadline usage pattern (ch08)** -- Added recommended usage `env->NowMicros() + timeout` to the deadline field description.

4. **GetMergeOperands thresholds (ch06)** -- Simplified to describe behavior ("when cumulative operand size is large enough") rather than exposing internal thresholds (32KB, 256B) that could change.

## Disagreements Found

0 -- Only the CC review existed (Codex review file not found). No inter-reviewer disagreements to resolve.

## CC Review Claims Rejected

1. **GetApproximateMemTableStats return type** -- CC claimed the API returns a struct (`TableProperties::ApproximateMemTableStats`). Verified against `include/rocksdb/db.h`: the API uses separate out-parameters (`uint64_t* count`, `uint64_t* size`), not a struct. The existing doc description was already correct.

## Changes Made

| File | Changes |
|------|---------|
| `01_point_lookups.md` | Fixed range deletion mechanism description |
| `02_batched_point_lookups.md` | Added MultiCFSnapshot retry details |
| `03_iterators_and_range_scans.md` | Fixed error handling section, removed unverifiable percentages, added auto_refresh UDT limitation |
| `04_cross_cf_multi_range.md` | Expanded MultiScanArgs API documentation |
| `06_auxiliary_read_apis.md` | Added missing APIs, simplified GetMergeOperands thresholds |
| `07_async_io_and_prefetching.md` | Fixed readahead reset description |
| `08_readoptions_reference.md` | Fixed kPersistedTier description, added deadline usage hint |

## Self-Review Checklist

- [x] No line number references
- [x] No inline code quotes
- [x] No box-drawing characters
- [x] INVARIANT used correctly
- [x] Every claim validated against current code
- [x] index.md is 42 lines (within 40-80 range)
- [x] Each chapter has **Files:** line
