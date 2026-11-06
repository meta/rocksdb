# Performance

**Files:** `db/db_impl/db_impl_write.cc`, `db/write_thread.h`, `db/write_controller.h`, `db/memtable.h`, `db/log_writer.h`

## Hot Path Optimizations

### WriteThread

- **Lock-free enqueue**: `LinkOne()` uses a CAS loop on `newest_writer_`, avoiding mutex acquisition for every write
- **Adaptive wait**: Three-phase spin-yield-block strategy minimizes latency for short waits while falling back to condvar for longer waits
- **Group commit**: Leader batches multiple writers' WAL writes into a single fsync, amortizing sync cost across the group
- **O(sqrt(n)) parallel launch**: Large parallel memtable groups use stride leaders to reduce serial wake-up overhead

### MemTable

- **Lock-free skiplist**: Default `MemTableRep` uses a lock-free skiplist that allows concurrent reads during writes
- **Concurrent inserts**: When `allow_concurrent_memtable_write` is enabled, multiple writers insert their batches simultaneously using CAS-based skiplist insertion
- **Thread-local batching**: Concurrent mode uses `MemTablePostProcessInfo` to batch atomic counter updates in thread-local storage, reducing contention
- **Bloom filter**: Prefix and/or whole-key bloom filter reduces memtable lookups on misses

### WAL

- **Pre-computed type CRC**: `type_crc_[kMaxRecordType+1]` avoids recomputing the CRC of the type byte on every write
- **CRC combine**: Uses `crc32c::Crc32cCombine()` to efficiently merge pre-computed type CRC with payload CRC
- **Optional compression**: WAL supports streaming compression (ZSTD only; other algorithms are not supported for WAL streaming and are reset to `kNoCompression` by option sanitization) to reduce I/O bandwidth

## Write Amplification

Write amplification is the ratio of bytes written to storage vs bytes written by the application.

**Sources:**

| Component | Amplification | Notes |
|-----------|---------------|-------|
| WAL | 1.0x | Each byte written once (plus CRC/header overhead) |
| Flush (memtable to L0) | ~1.0x | Direct serialization to SST |
| L0 to L1 compaction | ~1 + L1_size/L0_size | Reads all L0 files + overlapping L1 files; per-byte amplification is roughly constant (~2x) in steady state |
| L1+ leveled compaction | O(fanout) per level | Default fanout is 10 (see `max_bytes_for_level_multiplier`) |

**Total write amplification:** Typically 10-30x for leveled compaction, 2-10x for universal compaction.

## Throughput Tuning

### Write Buffer Configuration

| Option | Effect | Tradeoff |
|--------|--------|----------|
| `write_buffer_size` | Larger memtable = fewer L0 flushes | More memory usage; longer recovery time |
| `max_write_buffer_number` | More immutable memtables before stall | More memory; delayed flush may increase L0 pressure |
| `min_write_buffer_number_to_merge` | Merge multiple memtables in one flush | Reduces write amplification; increases flush latency |

### Write Mode Selection

| Option | Effect | Tradeoff |
|--------|--------|----------|
| `enable_pipelined_write` | Overlap WAL and memtable phases | Higher throughput; slightly more complex failure handling |
| `unordered_write` | Independent memtable inserts | Maximum throughput; relaxed inter-writer ordering |
| `allow_concurrent_memtable_write` | Parallel memtable insertion within a group | Higher throughput for multi-writer workloads |
| `max_write_batch_group_size_bytes` | Controls maximum group size | Larger groups amortize sync cost; may increase tail latency |

### WAL Configuration

| Option | Effect | Tradeoff |
|--------|--------|----------|
| `sync` (WriteOptions) | fsync after each write group | Durability guarantee; significant latency cost |
| `disableWAL` (WriteOptions) | Skip WAL entirely | Maximum write speed; no crash recovery for this write |
| `manual_wal_flush` | Application controls when WAL flushes | Better batching; risk of data loss on crash |
| `recycle_log_file_num` | Reuse old WAL files | Avoids filesystem allocation overhead |
| `wal_compression` | Compress WAL records | Reduces I/O bandwidth; adds CPU cost |

### Flow Control Tuning

| Option | Effect | Tradeoff |
|--------|--------|----------|
| `level0_slowdown_writes_trigger` | L0 file count before delay | Higher value = more L0 accumulation before slowdown |
| `level0_stop_writes_trigger` | L0 file count before full stop | Higher value = more L0 files tolerated |
| `soft_pending_compaction_bytes_limit` | Delay threshold for pending bytes | Higher value = more tolerance for compaction debt |
| `hard_pending_compaction_bytes_limit` | Stop threshold for pending bytes | Higher value = more risk of space amplification |
| `delayed_write_rate` | Initial write rate during delay | Lower value = more aggressive throttling |

## Benchmarking with db_bench

Key db_bench flags for write workloads:

| Benchmark | Description |
|-----------|-------------|
| `fillseq` | Sequential key insertion |
| `fillrandom` | Random key insertion |
| `overwrite` | Overwrite existing random keys |
| `fill100K` | Large-value writes (100 KB) |

Example for benchmarking write throughput:

```
# Build release binary
make clean && DEBUG_LEVEL=0 make -j128 db_bench

# Sequential write throughput
./db_bench --benchmarks=fillseq --num=10000000 --value_size=100 \
  --compression_type=none --disable_wal=false --sync=false

# Random write throughput with concurrent writers
./db_bench --benchmarks=fillrandom --num=10000000 --threads=8 \
  --allow_concurrent_memtable_write=true --enable_pipelined_write=true
```

## Cross-Component Interactions

| From | To | Interaction |
|------|-----|-------------|
| WriteImpl | WriteThread | Leader election, group batching |
| WriteImpl | WalWriter | Serialize WriteBatch via `AddRecord()` |
| WriteImpl | MemTable | Insert via `WriteBatchInternal::InsertInto()` |
| WriteImpl | WriteController | Check `IsStopped()` / `NeedsDelay()` in `PreprocessWrite()` |
| WriteImpl | WriteBufferManager | Check `ShouldFlush()` / `ShouldStall()` |
| MemTable | FlushJob | Flush immutable memtables to L0 SST |
| FlushJob | VersionSet | `LogAndApply(VersionEdit)` commits flush to MANIFEST |
| FlushJob | WriteBufferManager | `FreeMem()` releases memory, may end stall |
| FlushJob | CompactionPicker | New L0 file may trigger compaction |
| CompactionJob | WriteController | `RecalculateWriteStallConditions()` updates tokens |
