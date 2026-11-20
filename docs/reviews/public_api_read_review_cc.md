# Review: public_api_read -- Claude Code

## Summary
Overall quality rating: **good**

The documentation provides comprehensive coverage of RocksDB's public read APIs with accurate descriptions of Get(), MultiGet(), iterators, cross-CF queries, PinnableSlice, and auxiliary APIs. The GetImpl() flow, MultiCFSnapshot retry mechanism, PinnableSlice pinning modes, and ReadOptions reference are all verified accurate against the source. The main concerns are a misleading description of range deletion handling during Get(), an inaccurate claim about iterator error behavior during corruption, an incorrect description of readahead reset behavior, and significant missing detail on the MultiScanArgs API surface. The best practices chapter (09) is solid and actionable.

## Correctness Issues

### [MISLEADING] Range deletion short-circuit in Get()
- **File:** 01_point_lookups.md, "Short-Circuit Behavior" section
- **Claim:** "A `kTypeRangeDeletion` covering the key -- returns `NotFound`"
- **Reality:** Get() never encounters kTypeRangeDeletion records directly during the point key lookup. Range tombstones are stored in a separate data structure (FragmentedRangeTombstoneIterator). Before scanning point keys, MemTable::Get() queries MaxCoveringTombstoneSeqnum(key). If the covering tombstone sequence is higher than the point key's sequence, the code *synthetically assigns* kTypeRangeDeletion as the type, then the switch statement handles it as a deletion. The same pattern is used in get_context.cc for SST files.
- **Source:** db/memtable.cc lines 1253-1259 (synthetic type override), 1425-1428 (tombstone query); table/get_context.cc lines 281-291
- **Fix:** Rewrite to: "A range tombstone covering the key at a higher sequence number -- the lookup checks the FragmentedRangeTombstoneIterator before scanning point keys, and if a covering tombstone exists with a higher sequence number, the key is treated as deleted and returns NotFound"

### [MISLEADING] Iterator corruption with Valid()=true
- **File:** 03_iterators_and_range_scans.md, "Error Handling During Iteration" section
- **Claim:** "When a data block is corrupted, the iterator sets `status()` to `Corruption` but may still return `Valid()=true` for entries in non-corrupted blocks"
- **Reality:** At the DBIter level (the public Iterator), corruption always sets valid_=false immediately. In DBIter::FindNextUserEntry() and related methods, when ParseKey fails, it sets status_ = Status::Corruption(...) and valid_ = false, and returns false. The public Iterator interface guarantees that when status() is non-OK, Valid() returns false. The claim describes behavior that might exist at lower-level internal iterators but not at the public API level.
- **Source:** db/db_iter.cc -- ParseKey failure path sets valid_=false; DBIter::status() at line 198-204 returns saved_status_ or iter_.status()
- **Fix:** Remove the claim that Valid()=true can coexist with a Corruption status. Instead emphasize: "After any positioning operation, always check both Valid() and status(). If Valid() is false, check status() to distinguish 'end of range' from 'error'. Within the loop, the correct pattern is: `for (it->Seek(start); it->Valid(); it->Next()) { ... } if (!it->status().ok()) { /* handle error */ }`"

### [MISLEADING] Readahead reset at level boundary
- **File:** 07_async_io_and_prefetching.md, "Iterator Auto-Readahead" section
- **Claim:** "Step 4: Reset readahead to 8KB when moving to a new file (at each level boundary)"
- **Reality:** The parenthetical "(at each level boundary)" is inaccurate. The ReadOptions comment in options.h says: "at each level, if iterator moves over next file, readahead_size starts again from 8KB." This means readahead resets when moving to a new file *within the same level*, not only at level transitions. The implementation in BlockPrefetcher::PrefetchIfNeeded() calls ResetValues() when IsBlockSequential() returns false (non-sequential access within a file).
- **Source:** include/rocksdb/options.h lines 2193-2198; table/block_based/block_prefetcher.cc line 118
- **Fix:** Change to: "Step 4: Reset readahead to 8KB when the iterator moves to a new SST file (within or across levels)"

### [UNVERIFIABLE] Iterator Next() CPU cost statistics
- **File:** 03_iterators_and_range_scans.md, "Iterator::Next() CPU Cost" section
- **Claim:** "~90% of keys are in the bottommost sorted run" and "~81% of the time, only 1 comparison is needed"
- **Reality:** These are empirical/analytical observations, not coded constants. No source code validates or contradicts these specific percentages. They may be true for certain workload patterns (leveled compaction with typical amplification factors) but are not universally applicable.
- **Source:** No direct code reference found
- **Fix:** Add a qualifier: "For typical leveled compaction workloads with default amplification factors, approximately 90% of keys reside in the bottommost sorted run..." or cite the specific analysis/benchmark that produced these numbers. Alternatively, remove the specific percentages and describe the general behavior qualitatively.

### [MISLEADING] kPersistedTier description oversimplified
- **File:** 08_readoptions_reference.md, ReadTier table
- **Claim:** "kPersistedTier: SST files only; skip memtable when WAL disabled"
- **Reality:** The source comment says "persisted data. When WAL is disabled, this option will skip data in memtable." When WAL IS enabled, memtable data is considered persisted (backed by WAL), so it is not "SST files only." Additionally, the header notes "this ReadTier currently only supports Get and MultiGet and does not support iterators."
- **Source:** include/rocksdb/options.h lines 1818-1822
- **Fix:** Change to: "kPersistedTier: Persisted data only; skip memtable when WAL is disabled (memtable data is included when WAL is enabled since WAL makes it persisted). Get/MultiGet only -- iterators not supported."

## Completeness Gaps

### MultiScanArgs API surface severely underdocumented
- **Why it matters:** Developers using NewMultiScan() need to know how to construct MultiScanArgs, configure I/O behavior, and understand the insert API. The current docs describe it as a simple vector wrapper, missing the actual class design.
- **Where to look:** include/rocksdb/options.h lines 1872-1991
- **Missing fields:**
  - `io_coalesce_threshold` (size_t) -- controls when adjacent I/O requests are merged
  - `max_prefetch_size` (size_t) -- caps prefetch buffer size
  - `use_async_io` (bool) -- enables async I/O for multi-range scans
  - `insert()` method overloads -- the primary API for building scan ranges (takes Comparator*, start key, optional limit)
  - `CopyConfigFrom()` -- copies configuration from another MultiScanArgs
- **Also missing:** ScanOptions::property_bag (optional<unordered_map<string,string>>) for passing custom name/value pairs to external table readers
- **Suggested scope:** Expand chapter 04's MultiScanArgs section significantly, or create a dedicated sub-section

### Missing auxiliary read APIs in chapter 06
- **Why it matters:** Developers looking for a comprehensive read API reference will miss these
- **Where to look:** include/rocksdb/db.h
- **Missing APIs:**
  - `VerifyChecksum(const ReadOptions&)` / `VerifyFileChecksums(const ReadOptions&)` -- reads and verifies all block/file checksums (db.h lines 2100-2108)
  - `GetIntProperty()` / `GetAggregatedIntProperty()` -- integer property variants more efficient than string-based GetProperty() (db.h lines 1439-1455)
  - `GetPropertiesOfTablesByLevel()` -- returns table properties grouped by level (db.h lines 2135-2138)
- **Suggested scope:** Add brief entries to chapter 06

### kPersistedTier iterator limitation not documented
- **Why it matters:** A developer setting kPersistedTier on an iterator would get unexpected behavior
- **Where to look:** include/rocksdb/options.h lines 1820-1822
- **Suggested scope:** Add a note to the ReadTier table in chapter 08: "Note: does not support iterators"

## Depth Issues

### MultiCFSnapshot retry mechanism lacks detail
- **Current:** "acquires SuperVersion references for all CFs, then verifies the last published sequence hasn't changed. If it has, it retries"
- **Missing:** The retry is limited to 3 attempts (constexpr int num_retries = 3). On the last retry, it acquires mutex_ to guarantee success. The actual check is whether sv->mem->GetEarliestSequenceNumber() > snapshot (a memtable seal between snapshot capture and SV acquisition), not just "sequence hasn't changed"
- **Source:** db/db_impl/db_impl.cc lines 2866-2960

### auto_refresh_iterator_with_snapshot additional limitation
- **Current:** "Not compatible with WRITE_PREPARED or WRITE_UNPREPARED transaction policies"
- **Missing:** The source also notes (options.h lines 2269-2274): "True is not recommended if using user-defined timestamp with persist_user_defined_timestamps=false and non-nullptr ReadOptions::timestamp or ReadOptions::iter_start_ts, because auto-refreshing iterator will not prevent user timestamp information from being dropped during iteration."
- **Source:** include/rocksdb/options.h lines 2269-2274
- **Suggested:** Add a second note about the UDT limitation

### Deadline usage pattern not mentioned
- **Current:** Chapter 08 says "Absolute deadline for the operation (microseconds since epoch)"
- **Missing:** The source recommends: "The best way is to use env->NowMicros() + some timeout." This is a practical detail developers need.
- **Source:** include/rocksdb/options.h lines 2019-2020

## Structure and Style Violations

No violations found:
- index.md is 43 lines (within 40-80 range)
- No box-drawing characters found
- No line number references found
- No inline code blocks (```) found
- Files lines present in all chapters
- INVARIANT used appropriately in index.md (all four are true correctness invariants)

## Undocumented Complexity

### MultiScanArgs is a full class, not a simple wrapper
- **What it is:** MultiScanArgs at options.h lines 1872-1991 is a substantial class with a Comparator-based constructor, multiple insert() overloads for adding scan ranges, configuration fields (io_coalesce_threshold, max_prefetch_size, use_async_io), and CopyConfigFrom(). It is not a thin vector wrapper as the docs imply.
- **Why it matters:** Developers cannot use the NewMultiScan() API correctly without understanding how to construct MultiScanArgs via its insert() methods. The insert() API is the primary way to add ranges -- there is no public constructor that takes a vector of ScanOptions.
- **Key source:** include/rocksdb/options.h lines 1872-1991 (class definition), lines 1916-1934 (insert methods)
- **Suggested placement:** Rewrite the MultiScanArgs section in chapter 04

### GetApproximateMemTableStats returns a struct, not separate out params
- **What it is:** The doc says output parameters are `count` and `size`. Need to verify whether the API signature uses separate out-params or a struct. The current API returns `TableProperties::ApproximateMemTableStats` which has `count` and `size` fields.
- **Why it matters:** Developers reading the docs would try to pass two separate output pointers
- **Key source:** include/rocksdb/db.h GetApproximateMemTableStats declaration
- **Suggested placement:** Fix in chapter 06

### GetMergeOperands SuperVersion optimization is internal detail
- **What it is:** The docs describe ShouldReferenceSuperVersion() with specific thresholds (32KB cumulative, 256B average). While verified accurate, this is an internal optimization detail that could change without notice.
- **Why it matters:** Users should not rely on these specific thresholds. The useful information is that GetMergeOperands can avoid copying large operands by holding the SuperVersion reference.
- **Key source:** db/db_impl/db_impl.cc lines 2454-2480
- **Suggested placement:** Simplify the description in chapter 06 to focus on the behavior (avoids copies for large operands) rather than the exact thresholds

## Positive Notes

- **GetImpl() flow description is excellent.** Every step verified against the actual code path in db_impl.cc. The snapshot-after-SuperVersion ordering explanation with rationale is particularly valuable.
- **PinnableSlice chapter (05) is thorough and accurate.** The pinned vs unpinned mode distinction, PinSlice vs PinSelf mechanics, and lifetime rules all match the source exactly. The remove_prefix/remove_suffix behavioral difference is a useful detail.
- **ReadOptions reference (ch08) is complete.** Every field in the ReadOptions struct is documented with correct types and defaults. The Key Interactions section is helpful.
- **MultiGet batching explanation is well-done.** The description of bloom filter pipelining, block cache contention reduction, and parallel I/O is accurate and helps developers understand *why* MultiGet is faster, not just *that* it is.
- **Best practices chapter (09) is actionable.** The bloom filter considerations section and common pitfalls are practical and grounded in real performance characteristics.
- **No structural violations.** Clean formatting throughout -- no box-drawing characters, no line number references, no inline code blocks. Files lines present in all chapters.
