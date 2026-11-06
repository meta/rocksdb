# Fix Summary: user_defined_index

## Reviews Available
- CC review only (no Codex review exists for this component)

## Issues Fixed

**Correctness (4):**
1. kUnknown vs kOutOfBound distinction: Clarified that `Valid()` treats both as invalid, but `UpperBoundCheckResult()` returns the raw enum (01_public_api.md)
2. Range deletion exclusion mechanism: Clarified that `BlockBasedTableBuilder::Add()` routes range deletions before they reach UDI, and `MapToUDIValueType` assert-fails on `kTypeRangeDeletion` (01_public_api.md)
3. Cutoff level breakeven: Corrected from "~25 children" to "~28 children (256/9)" based on actual `ComputeCutoffLevel()` math (05_trie_index.md)
4. TrieBlockHandle clarification: Added note that `TrieBlockHandle` is `uint64_t` in memory, serialized as `uint32_t` arrays (04_sequence_numbers.md). Note: CC review claimed `TrieBlockHandle` uses `uint32_t` -- actually uses `uint64_t` in the struct, only the serialized arrays are `uint32_t`.

**Completeness (7):**
1. mmap_read incompatibility: Added new subsection to Chapter 8 documenting this critical limitation (08_limitations_performance.md)
2. Minimum chain length threshold: Documented `kMinChainLength = 8` in path compression section (05_trie_index.md)
3. chain_lens_ uint16_t limit: Documented 65535-byte chain length limit (05_trie_index.md)
4. Comparator pointer identity check: Documented that `TrieIndexFactory` uses pointer identity, not name comparison (05_trie_index.md, 08_limitations_performance.md)
5. UserDefinedIndexOption default comparator: Documented that default is `BytewiseComparator()`, not nullptr (01_public_api.md)
6. db_crashtest.py details: Added crash test section with sampling rate (12.5%), non-lambda evaluation, and option sanitization (09_stress_test_db_bench.md)
7. ApproximateAuxMemoryUsage limitations: Documented that it only counts child position tables, not aligned_copy_ or chain metadata (05_trie_index.md)

**Depth (2):**
1. Leaf index formulas: Added prefix-key sub-case formulas for dense and sparse regions (06_trie_seek_iteration.md)
2. Bounds checking equality: Clarified that `Compare(reference_key, limit) >= 0` means equality returns kOutOfBound (06_trie_seek_iteration.md)

**Undocumented complexity (2):**
1. udi_finished_ guard: Documented that UDI Finish() is called only once via flag guard, even with partitioned indexes (02_table_integration.md)
2. Alignment memory duplication: Documented that unaligned data causes full trie duplication in aligned_copy_ (05_trie_index.md)

**Style (0):** No style violations found.

## Disagreements Found
0 -- Only one review (CC) was available; no Codex review exists for this component.

## CC Review Claims Not Applied
- CC review claimed `TrieBlockHandle` has `uint32_t offset` and `uint32_t size`. Code verification shows `TrieBlockHandle` uses `uint64_t` for both fields. The `uint32_t` limitation applies only to the serialized (packed) arrays, not the in-memory struct. Applied a clarification instead.

## Changes Made
| File | Changes |
|------|---------|
| 01_public_api.md | Fixed kUnknown/kOutOfBound distinction, range deletion mechanism, added default comparator info |
| 02_table_integration.md | Added udi_finished_ guard documentation |
| 04_sequence_numbers.md | Clarified TrieBlockHandle uint64_t vs serialized uint32_t |
| 05_trie_index.md | Fixed cutoff breakeven, added chain length thresholds, comparator pointer identity, alignment duplication, ApproximateAuxMemoryUsage limitations |
| 06_trie_seek_iteration.md | Added prefix-key leaf index formulas, clarified bounds equality semantics |
| 08_limitations_performance.md | Added mmap_read incompatibility section, comparator pointer identity |
| 09_stress_test_db_bench.md | Added crash test details (sampling rate, non-lambda, sanitization) |
