# Stress Test and db_bench

**Files:** `tools/db_bench_tool.cc`, `db_stress_tool/db_stress_test_base.cc`, `db_stress_tool/db_stress_gflags.cc`, `db_stress_tool/db_stress_filters.h`, `db_stress_tool/db_stress_filters.cc`

## db_bench Integration

The `--use_trie_index` flag enables trie-based UDI in db_bench:

**Build time:** Creates a `TrieIndexFactory` and sets it as `BlockBasedTableOptions::user_defined_index_factory`.

**Read time:** Sets `ReadOptions::table_index_factory` to the same factory for all read operations (readrandom, seekrandom, readseq, etc.).

### Usage

```bash
# Build release binary
make clean && DEBUG_LEVEL=0 make -j128 db_bench

# Populate with trie index
./db_bench --benchmarks=fillseq --use_trie_index --num=1000000

# Benchmark reads
./db_bench --benchmarks=readrandom,seekrandom --use_trie_index --use_existing_db
```

### Comparing Index Performance

To measure the impact of the trie index, run the same benchmark with and without `--use_trie_index`:

```bash
# Baseline (binary search index)
./db_bench --benchmarks=fillseq,readrandom --num=1000000

# With trie index
./db_bench --benchmarks=fillseq,readrandom --use_trie_index --num=1000000
```

Compare:
- Operation throughput (ops/sec)
- Latency percentiles (p50, p99)
- SST file sizes (index block size)
- Memory usage

## Stress Test Integration

The db_stress tool supports UDI via the `--use_trie_index` flag:

**Setup:** During `StressTest::Open()`, if `--use_trie_index` is set, a `TrieIndexFactory` is created and stored as `udi_factory_`. It is passed to `InitializeOptionsFromFlags()` which sets `BlockBasedTableOptions::user_defined_index_factory`.

**Read operations:** `ReadOptions::table_index_factory` is set to `udi_factory_.get()` for all read paths.

**Reverse iteration guard:** The stress test detects when UDI is active (either via `ReadOptions` or CF-level configuration) and avoids reverse iteration operations (`SeekToLast`, `SeekForPrev`, `Prev`) that would return `NotSupported`.

### SstQueryFilter Integration

The stress test also exercises `SstQueryFilterConfigs` (a separate filtering mechanism from UDI). The `SstQueryFilter` system filters entire SST files based on key segment ranges before opening them, while UDI replaces the block-level index within an opened SST file. The two systems are independent and can be used together.

### Running Stress Tests

```bash
# Build debug binary for stress testing
make -j128 db_stress

# Run with trie index
./db_stress --use_trie_index --ops_per_thread=100000
```

For crash test integration via `db_crashtest.py`, the `--use_trie_index` flag is propagated through the test harness.

### Crash Test Details

`use_trie_index` is enabled with ~12.5% probability (`random.choice([0,0,0,0,0,0,0,1])`). Unlike most random parameters in `db_crashtest.py` (which use lambdas to re-randomize per invocation), `use_trie_index` is evaluated once at import time. This is intentional: all invocations within a crash test series must use the same value so that all SST files in a DB are opened with matching table options.

When `use_trie_index` is enabled, the test harness automatically sanitizes incompatible options:
- `mmap_read` is forced to 0 (trie UDI uses zero-copy pointers into block data, incompatible with mmap)
- `compression_parallel_threads` is forced to 1 (parallel compression is incompatible with UDI)
