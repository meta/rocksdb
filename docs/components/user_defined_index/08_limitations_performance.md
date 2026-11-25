# Limitations and Performance

**Files:** `include/rocksdb/user_defined_index.h`, `table/block_based/user_defined_index_wrapper.h`, `table/block_based/block_based_table_builder.cc`

## Limitations

### Parallel Compression Incompatibility

When `user_defined_index_factory` is set, parallel compression (`CompressionOptions::parallel_threads > 1` or `bottommost_compression_opts.parallel_threads > 1`) is rejected with `Status::InvalidArgument`.

This is enforced in two places:
- `BlockBasedTableFactory::ValidateOptions()` returns an error
- The builder's `Rep` constructor sets a failure status

The root cause: `UserDefinedIndexBuilderWrapper` does not implement the `PrepareIndexEntry()`/`FinishIndexEntry()` split that parallel compression requires. The stubs assert-fail if called.

As a defense-in-depth measure, the builder's `compression_parallel_threads` member is sanitized to 1 when UDI is present (see `Rep` constructor in `table/block_based/block_based_table_builder.cc`).

### Forward-Only Iteration

Reverse iteration operations return `Status::NotSupported` through `UserDefinedIndexIteratorWrapper`:

- `SeekToLast()`: Not supported
- `SeekForPrev(target)`: Not supported
- `Prev()`: Not supported

Workaround: Do not set `ReadOptions::table_index_factory` for queries that need reverse iteration. The internal index handles reverse operations.

### Monolithic UDI Block

The UDI block is always a single meta block — it cannot be partitioned into sub-blocks with a top-level index. For SST files with millions of keys, the entire UDI block is loaded into memory (or block cache).

Note: This applies only to the UDI block itself. UDI works on top of partitioned internal indexes.

### Comparator Restrictions

The UDI framework supports arbitrary comparators via `UserDefinedIndexOption`. However, the bundled `TrieIndexFactory` requires `BytewiseComparator` and returns `Status::NotSupported` for any other comparator. The comparator check uses **pointer identity** (not name comparison), so only the singleton `BytewiseComparator()` is accepted. Custom UDI implementations can use any comparator.

### mmap_read Incompatibility

The trie index is incompatible with `mmap_read` mode. The trie uses zero-copy pointers into block data, which assumes the data remains at a fixed address backed by block cache entries. With `mmap_read`, block data may reside at unaligned mmap offsets, and the trie's `aligned_copy_` mechanism was not designed for this mode. The crash test explicitly disables `mmap_read` when `use_trie_index` is set.

### Memory Overhead

UDI blocks consume memory beyond the internal index. For workloads with many open SST files, this adds to the memory footprint. Mitigation:

- UDI blocks can be cached in the block cache (subject to eviction)
- `UserDefinedIndexReader::ApproximateMemoryUsage()` reports consumption
- The trie index is generally smaller than the internal binary search index for prefix-heavy key sets

## Performance Characteristics

### Space Efficiency

The trie index exploits common key prefixes to reduce index size. For key sets with substantial prefix sharing (e.g., structured keys with common prefixes), the trie can achieve significant space savings compared to the binary search index.

Actual savings depend on key distribution. Keys with no common prefixes see minimal benefit or potentially higher overhead from trie metadata.

### Seek Performance

| Index Type | Complexity | Characteristics |
|------------|-----------|-----------------|
| Binary search (internal) | O(log N) comparisons | Predictable, comparison-based |
| Trie (UDI) | O(M) byte operations | M = key length, better cache locality for prefix-similar keys |

The trie's per-level operations (bitmap lookup, rank computation) are O(1) but involve popcount and memory access patterns different from binary search.

### Iteration Performance

Sequential iteration through the trie reconstructs keys by appending one byte per level, with backtracking via the path stack. The `autovector<LevelPos, 24>` avoids heap allocation for typical key depths.

For random seeks, binary search may have better worst-case latency due to fewer pointer chases and simpler access patterns.

### Memory Access Patterns

The trie's bitvectors are laid out in BFS order, so traversal of upper levels has good spatial locality. Lower levels (sparse region) use compact label arrays that are also sequential. The precomputed child position tables (`s_child_start_pos_`, `s_child_end_pos_`) eliminate Select operations, keeping traversal to O(1) Rank + array lookup per level.

### Benchmarking

Use `db_bench` with `--use_trie_index` to compare trie index performance against the default binary search index. Build a release binary for meaningful results:

```
make clean && DEBUG_LEVEL=0 make -j128 db_bench
```

Key benchmarks to compare:
- `readrandom`: Point lookup latency
- `seekrandom`: Seek latency
- `readseq`: Sequential read throughput
- `fillseq` / `fillrandom`: Write throughput (UDI adds build-time overhead)

Compare SST file sizes to measure index space savings.
