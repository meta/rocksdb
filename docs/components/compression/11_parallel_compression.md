# Parallel Compression

**Files:** `table/block_based/block_based_table_builder.cc`, `include/rocksdb/compression_type.h`, `util/bit_fields.h`, `util/semaphore.h`, `table/block_based/index_builder.h`

## Overview

Parallel compression accelerates SST file creation by distributing block compression across multiple threads during flush and compaction. It is implemented entirely within `BlockBasedTableBuilder` using a lock-free ring buffer and a single 64-bit atomic state word for coordination. The design parallelizes three phases of data block production: **emit** (serialize the uncompressed block), **compress**, and **write** (append compressed block to the SST file).

Parallel compression is beneficial only for heavyweight codecs (ZSTD, Zlib) where compression is the bottleneck. For lightweight codecs (Snappy, LZ4), compression throughput typically exceeds I/O throughput, making parallelism unnecessary.

## Configuration

### Enabling Parallel Compression

Set `CompressionOptions::parallel_threads` to a value greater than 1:

```cpp
options.compression_opts.parallel_threads = 4;
// Also configurable for bottommost level:
options.bottommost_compression_opts.parallel_threads = 4;
```

Default is 1 (disabled). The value controls the maximum number of worker threads spawned per table builder.

### When Parallel Compression Is Disabled

Even with `parallel_threads > 1`, parallel compression is automatically disabled (sanitized to 1) when:

- **Old-style partitioned filters**: `partition_filters == true && decouple_partitioned_filters == false`
- **User-defined index factory**: `user_defined_index_factory` is set

Additionally, `MaybeStartParallelCompression()` skips spawning workers if no `data_block_compressor` is configured (compression is disabled), since the ring buffer overhead is not worthwhile without CPU-bound compression work.

## Architecture

### Thread Model

The system uses two kinds of threads, encoded as `ThreadKind` in `ParallelCompressionRep`:

| Thread | Count | Role |
|--------|-------|------|
| **Emitter** (kEmitter) | 1 (the flush/compaction thread) | Serializes uncompressed blocks into ring buffer slots. Can also perform compression as quasi-work-stealing. Never writes to the SST file. |
| **Workers** (kWorker) | Up to `parallel_threads` (capped at ring buffer size - 1) | Compress blocks and write them to the SST file. Only one worker writes at a time, enforced by ordering. |

### Ring Buffer

The central data structure is a power-of-two ring buffer of `BlockRep` entries. Each entry holds both the uncompressed and compressed forms of a data block, plus a prepared index entry.

Ring buffer sizing follows a step function based on `parallel_threads`:

| parallel_threads | Ring buffer slots | Worker threads |
|-----------------|-------------------|----------------|
| 2 | 4 | 2 |
| 3-4 | 8 | min(parallel_threads, 7) |
| 5-8 | 16 | min(parallel_threads, 15) |
| 9+ | 32 | min(parallel_threads, 31) |

The ring buffer is always at least twice the worker count to provide buffering headroom between emission and consumption.

### Atomic State Word

All thread coordination is performed via CAS operations on a single 64-bit atomic word (`BitFieldsAtomic<State>`), avoiding locks on the critical path. The state word packs these fields:

| Field | Width | Purpose |
|-------|-------|---------|
| NeedsWriter | 32 bits | Per-slot bitmask: which slots have finished compression and need writing |
| IdleWorkerCount | 5 bits | Number of worker threads currently sleeping |
| IdleEmitFlag | 1 bit | Whether the emit thread is sleeping |
| NoMoreToEmitFlag | 1 bit | Set when the last block has been emitted |
| AbortFlag | 1 bit | Set on error, causes all threads to exit |
| NextToWrite | 8 bits | Ordinal of the next block to write (monotonic counter) |
| NextToCompress | 8 bits | Ordinal of the next block to compress |
| NextToEmit | 8 bits | Ordinal of the next slot to emit into |

**Ordering invariant**: `NextToWrite <= NextToCompress <= NextToEmit`. The ring buffer is full when `NextToEmit - NextToWrite > ring_buffer_mask`.

### Synchronization Primitives

| Primitive | Type | Purpose |
|-----------|------|---------|
| `idle_worker_sem` | `CountingSemaphore` | Workers sleep here when idle; emitter releases to wake a worker |
| `idle_emit_sem` | `BinarySemaphore` | Emitter sleeps here when ring buffer is full; a writer releases after freeing a slot |
| `atomic_state` | `BitFieldsAtomic<State>` | All scheduling state in one 64-bit atomic word (acquire-release ordering) |

The semaphore implementations in `util/semaphore.h` default to mutex + condvar. An opt-in `ROCKSDB_USE_STD_SEMAPHORES` mode uses `std::counting_semaphore`.

## Workflow

### Startup

`MaybeStartParallelCompression()` is called when the table builder begins producing data blocks:

1. Creates `ParallelCompressionRep` with the configured thread count
2. Allocates ring buffer entries, each with a `PreparedIndexEntry` from the index builder
3. Creates per-worker compression/decompression working areas
4. Spawns worker threads, each running `BGWorker()`
5. In debug builds, spawns a watchdog thread for deadlock detection

### Block Emission

When a data block is full, the emit thread calls `EmitBlockForParallel()`:

1. Swaps the uncompressed block data into `ring_buffer[emit_slot].uncompressed`
2. Calls `IndexBuilder::PrepareIndexEntry()` to capture separator key info for later index construction
3. Updates the estimated in-flight data size
4. Enters the `StateTransition` loop:
   - If assigned `kEmitting`: returns to the caller (ring buffer has room for the next block)
   - If assigned `kCompressing`: performs compression work on that slot (quasi-work-stealing), then loops back
   - If this is the last block (`first_key_in_next_block == nullptr`): sets `NoMoreToEmitFlag`

### Worker Loop

Each worker thread runs `BGWorker()` in a loop:

1. Start idle, sleeping on `idle_worker_sem`
2. Call `StateTransition` to get the next assignment
3. Execute based on assigned state:
   - **kCompressing**: Compress the block in `ring_buffer[slot]`, then re-enter state transition
   - **kCompressingAndWriting**: Compress then immediately write (combined state, avoids an extra transition)
   - **kWriting**: Write the compressed block to the SST file, call `IndexBuilder::FinishIndexEntry()`, update data sizes
   - **kEnd**: Exit the loop
4. On I/O error: set the error status and call `SetAbort()` to terminate all threads

### State Transition (Core Scheduling)

`StateTransition()` is the heart of the lock-free scheduler. It is templated on `ThreadKind` and implements a CAS loop that:

1. **Marks completion** of the current work item (advances the relevant counter, sets `NeedsWriter` bits, wakes idle threads)
2. **Selects next work** based on thread-specific priority:
   - **Emitter priority**: (1) Emit if ring buffer has room, (2) compress if a slot is available, (3) go idle
   - **Worker priority**: (1) Write the next block if its `NeedsWriter` bit is set, (2) compress if available, (3) go idle or end
3. **Attempts CAS** to atomically transition from old state to new state; retries on failure
4. **Handles idle**: sleeps on the appropriate semaphore, then re-enters the CAS loop on wake-up

### Write Ordering

Blocks must be written to the SST file in the same order they were emitted, regardless of which worker finishes compression first. This is enforced by the `NextToWrite` counter: a worker can only claim the write role for a slot when:

- The slot's `NeedsWriter` bit is set (compression is complete)
- The slot ordinal matches `NextToWrite`

The `kCompressingAndWriting` optimization allows a worker to skip the intermediate `NeedsWriter` state when the block being compressed is also the next block to write, saving a state transition round-trip.

### Worker Wake-Up Heuristics

The emitter thread uses an auto-tuning mechanism to decide when to wake idle workers:

- A counter (`emit_counter_toward_wake_up`) tracks emissions since the last wake-up
- The threshold (`emit_counter_for_wake_up`) starts low (aggressive wake-up) and gradually increases to stabilize the active thread count
- Workers are only woken when the compression backlog (`NextToEmit - NextToCompress`) exceeds a threshold proportional to active threads

This avoids thundering-herd wake-ups and lets the system stabilize at a natural parallelism level.

### Index Entry Handling

Index entries are split across threads to avoid locking the index builder:

1. **Emit thread** calls `IndexBuilder::PrepareIndexEntry()` to capture the separator key information
2. **Write thread** calls `IndexBuilder::FinishIndexEntry()` to commit the entry with the final block handle

The `PreparedIndexEntry` object is stored in each ring buffer slot, transferring ownership from emitter to writer.

### Shutdown

`StopParallelCompression()` is called from `Finish()` (normal completion) or `Abandon()` (abort):

1. Sets `NoMoreToEmitFlag` or `AbortFlag` in the atomic state
2. Calls `WakeAllIdle()` to wake all sleeping threads
3. Joins all worker threads
4. In debug builds, shuts down the watchdog thread
5. Resets the `ParallelCompressionRep` pointer

### File Size Estimation

`EstimatedFileSize()` returns `FileSize() + estimated_inflight_size` to account for blocks that have been emitted but not yet written. The in-flight size is maintained as:

- Incremented by `uncompressed_size + trailer_size` when a block is emitted
- Decremented by `uncompressed_size - compressed_size` after compression (if the block got smaller)
- Decremented by `compressed_size + trailer_size` after writing

This is important for compaction output file size splitting decisions.

## Design Properties

| Property | Mechanism |
|----------|-----------|
| Lock-free coordination | Single 64-bit atomic CAS word packs all scheduling state |
| Write ordering | `NextToWrite` counter ensures sequential file writes |
| Quasi work-stealing | Emit thread performs compression when the ring buffer is full |
| Auto-tuned parallelism | Gradual worker wake-up with adaptive threshold |
| No emitter writes | Emit thread never writes to the SST file, preventing I/O blocking from stalling block emission |
| Per-thread working areas | Each worker has its own compressor/decompressor working area to avoid contention |
| Deadlock detection | DEBUG-only watchdog thread terminates after ~70s of apparent deadlock |

## When to Use

**Recommended for:**
- ZSTD or Zlib compression where compression CPU is the bottleneck
- High-throughput compaction workloads on machines with available CPU cores
- Bottommost-level compression with ZSTD at high compression levels

**Not recommended for:**
- Snappy or LZ4 where compression is already fast enough
- I/O-bound workloads where disk throughput is the bottleneck
- Systems with limited CPU headroom (parallel compression adds CPU load)

## Interaction with Other Features

- **Dictionary compression**: Compatible. Each worker uses the same shared dictionary, but has its own compression context.
- **Compression ratio threshold**: Applied per-block after compression, same as the serial path.
- **Partitioned filters**: Requires `decouple_partitioned_filters=true` (or no partitioned filters) for parallel compression to be active.
- **Compaction output file splitting**: Uses `EstimatedFileSize()` which accounts for in-flight blocks, so file size targets remain approximately correct despite buffering.
