# Direct I/O

**Files:** `include/rocksdb/env.h`, `include/rocksdb/options.h`, `env/io_posix.h`, `env/io_posix.cc`, `file/writable_file_writer.h`

## Overview

Direct I/O (O_DIRECT on Linux, `FILE_FLAG_NO_BUFFERING` on Windows) bypasses the operating system page cache, giving RocksDB full control over caching via its block cache. This eliminates double caching (page cache + block cache) and reduces memory pressure, but requires strict alignment of all I/O operations.

## Configuration

| Option | Location | Effect |
|--------|----------|--------|
| `use_direct_reads` | `DBOptions` | O_DIRECT for SST/blob file reads |
| `use_direct_io_for_flush_and_compaction` | `DBOptions` | O_DIRECT for flush and compaction writes |

Note: Direct reads and direct writes are configured independently. A common configuration is to enable direct reads (avoiding page cache pollution from compaction reads) while keeping buffered writes (which are simpler and often sufficient).

Important: Direct I/O is mutually exclusive with mmap reads. `PosixRandomAccessFile` asserts `!use_direct_reads || !use_mmap_reads`.

## Alignment Requirements

Direct I/O imposes three alignment requirements:

1. **File offsets** must be aligned to the logical block size (typically 512 bytes or 4KB).
2. **Buffer addresses** must be aligned to the logical block size.
3. **I/O sizes** must be multiples of the logical block size.

### Logical Block Size Detection

On Linux, PosixHelper::GetLogicalBlockSizeOfFd() (see env/io_posix.h) determines the block size by looking up the device via /sys/dev/block/<major>:<minor> (using fstat to obtain the device numbers), resolving the symlink to the parent device if the file resides on a partition (e.g., sda3 to sda, nvme0n1p1 to nvme0n1), then reading queue/logical_block_size from the resolved device directory. The LogicalBlockSizeCache class caches this per-directory to avoid repeated sysfs lookups. Files return their cached logical block size via GetRequiredBufferAlignment().

### AlignedBuffer

RocksDB uses `AlignedBuffer` (see `util/aligned_buffer.h`) for all direct I/O buffers. This class ensures buffer memory is allocated with the required alignment using platform-specific aligned allocation.

## Direct I/O Write Path

`WritableFileWriter::WriteDirect()` handles aligned writes:

Step 1: Pads the buffer to alignment boundary.
Step 2: Writes the entire buffer to the file at `next_write_offset_` via `PositionedAppend()`.
Step 3: Retains the partial tail (data past the last page boundary) via `buf_.RefitTail()`.
Step 4: Advances `next_write_offset_` by only the whole-page portion.

The partial tail is re-written with the next batch to satisfy alignment requirements. This means the same data may be written to disk twice -- once with padding, and once with the subsequent batch.

Important: Direct writes require writable_file_max_buffer_size > 0. WritableFileWriter::Create() enforces this with a runtime check that returns IOStatus::InvalidArgument. A debug assertion in the constructor provides an additional safeguard.

## Direct I/O Read Path

For direct I/O reads, `RandomAccessFileReader::Read()` allocates an aligned buffer internally, reads the aligned range that covers the requested range, and either transfers buffer ownership to the caller (via `aligned_buf`) or copies the relevant portion to `scratch`.

`RandomAccessFileReader::MultiRead()` aligns each request via `Align()`, merges overlapping aligned requests via `TryMerge()`, performs the aligned reads, and copies results back to the original request buffers.

## Platform Support

| Platform | Direct Read | Direct Write | Notes |
|----------|-------------|--------------|-------|
| Linux | O_DIRECT | O_DIRECT | Full support with io_uring |
| macOS | F_NOCACHE | F_NOCACHE | Uses fcntl(F_NOCACHE) instead of O_DIRECT |
| Windows | FILE_FLAG_NO_BUFFERING | FILE_FLAG_NO_BUFFERING | Full support |

## Tradeoffs

**Benefits:**
- Eliminates double caching (page cache + block cache)
- Reduces OS memory pressure from large datasets
- More predictable memory usage (only block cache)
- Avoids page cache thrashing from compaction reads

**Costs:**
- Alignment overhead (partial blocks may be read/written multiple times)
- Higher CPU cost for alignment management
- Readahead must be handled by RocksDB (no OS-level readahead)
- mmap reads are incompatible
- `FilePrefetchBuffer` or explicit readahead is recommended for sequential access patterns

## Configuration Recommendations

- Enable `use_direct_reads` for workloads with large datasets where page cache thrashing is a concern
- Enable `use_direct_io_for_flush_and_compaction` to avoid polluting page cache with compaction I/O
- Set a reasonable `compaction_readahead_size` (e.g., 2MB) when using direct reads to compensate for the lack of OS readahead
- Do not use direct I/O on tmpfs or in-memory filesystems, as they do not support O_DIRECT
