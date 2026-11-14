# Arena and Memory Allocation

**Files:** `memory/arena.h`, `memory/arena.cc`, `memory/concurrent_arena.h`, `memory/concurrent_arena.cc`, `memory/allocator.h`

## Why Arena Allocation

MemTable uses arena allocation instead of per-entry `malloc` for four reasons:

1. **Avoid malloc overhead**: Single block allocation amortizes the cost across many entries
2. **Improve cache locality**: Related keys stored in contiguous memory
3. **Simplify memory management**: Entire arena freed at once when MemTable is destroyed -- no per-entry deallocation
4. **Enable lock-free allocation**: Per-core shards in `ConcurrentArena` eliminate contention

## Arena

`Arena` in `memory/arena.h` is a bump-pointer allocator that carves memory from fixed-size blocks.

### Inline Block

Arena starts with a stack-allocated inline block of `kInlineSize = 2048` bytes. This avoids heap allocation for small or empty memtables, which is important because RocksDB may maintain thousands of memtables across column families.

### Block Allocation

When the inline block is exhausted, Arena allocates heap blocks of `kBlockSize` bytes:

- `kBlockSize` is computed from the `arena_block_size` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). When `arena_block_size` is 0 (the default), `SanitizeCfOptions()` computes it as `min(1 MB, write_buffer_size / 8)`, then aligns it up to 4 KB
- Clamped to `[kMinBlockSize=4096, kMaxBlockSize=2GB]` and aligned to `alignof(std::max_align_t)`
- Computed by `Arena::OptimizeBlockSize()`

### Allocation Strategy

Each block supports two allocation directions:

- **Unaligned allocations** (`Allocate`): carved from the end of the block, moving `unaligned_alloc_ptr_` downward
- **Aligned allocations** (`AllocateAligned`): carved from the beginning, moving `aligned_alloc_ptr_` upward

This dual-direction approach minimizes wasted padding from alignment.

For allocations larger than `kBlockSize / 4`, Arena allocates a dedicated "irregular" block of exactly the requested size, avoiding waste in the current active block.

### Huge Page Support

Arena optionally allocates blocks from huge page TLB when `memtable_huge_page_size` is set (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). This can improve TLB hit rates for large memtables. Falls back to regular allocation if huge page allocation fails.

### Memory Accounting

| Method | What it measures |
|--------|-----------------|
| `ApproximateMemoryUsage()` | `blocks_memory_ + block_count * sizeof(ptr) - alloc_bytes_remaining_` (bytes actually used) |
| `MemoryAllocatedBytes()` | Total bytes in all allocated blocks (including unused portions) |
| `AllocatedAndUnused()` | Bytes remaining in the current active block |

## ConcurrentArena

`ConcurrentArena` in `memory/concurrent_arena.h` wraps `Arena` to provide thread-safe allocation with low contention.

### Design

ConcurrentArena uses two levels of allocation:

1. **Per-core shards**: Small allocations go through thread-local shards to avoid contention
2. **Main arena**: Large allocations and shard refills go through the underlying `Arena` protected by a `SpinMutex`

### Shard Structure

Each shard (see `Shard` struct in `memory/concurrent_arena.h`) contains:

- A `SpinMutex` for shard-level locking
- `free_begin_` pointer to the start of available space
- `allocated_and_unused_` tracking remaining bytes
- 40 bytes of padding to prevent false sharing between adjacent shards

Shards are indexed via `CoreLocalArray`, which maps each CPU core to a shard. When a thread cannot lock its preferred shard, it calls `Repick()` to try another.

### Allocation Path

The allocation decision in `AllocateImpl()`:

1. If the allocation exceeds `shard_block_size_ / 4`, go directly to the main arena (too large for shards)
2. If this is the first allocation and the arena mutex is available without contention, go directly to the main arena (avoiding shard overhead when there is no concurrency)
3. Otherwise, pick a per-core shard, lock it, and allocate from the shard's local buffer

### Shard Refill

When a shard runs out of space, it acquires the arena mutex and allocates a new chunk from the main arena. The refill size is approximately `shard_block_size_` bytes, adjusted to match arena block boundaries to avoid fragmentation. The shard block size is computed as `min(128 KB, block_size / 8)`, independent of hardware concurrency. The 128 KB cap prevents large arena block sizes from causing each core to quickly allocate excessive memory, which could trigger premature flushes.

A special optimization: if the main arena is still using its inline block, shard allocations go directly to the arena to avoid allocating a full arena block for the first few small allocations.

### Memory Accounting

`ConcurrentArena` tracks memory across all shards:

- `ApproximateMemoryUsage()`: arena usage minus per-shard unused space
- `MemoryAllocatedBytes()`: total allocated, maintained atomically via `Fixup()` after each arena operation
- `AllocatedAndUnused()`: arena unused plus all shard unused (summed across all cores)

## WriteBufferManager Integration

MemTable memory is tracked globally via `WriteBufferManager` (see `include/rocksdb/write_buffer_manager.h`). The `AllocTracker` is connected to the `ConcurrentArena` during construction and reports allocation changes to the write buffer manager.

The write buffer manager maintains:

- `memory_used_`: total memory across all memtables (all column families, all DB instances)
- `memory_active_`: memory used by mutable memtables only
- `mutable_limit_`: threshold at `buffer_size * 7/8` for triggering flushes

When enabled, it can trigger flushes (`ShouldFlush()`) and optionally stall writes (`ShouldStall()`) when total memory usage exceeds the configured `buffer_size`.
