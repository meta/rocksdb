# Read Path

**Files:** `db/blob/blob_source.h`, `db/blob/blob_source.cc`, `db/blob/blob_file_reader.h`, `db/blob/blob_file_reader.cc`, `db/blob/blob_file_cache.h`, `db/blob/blob_file_cache.cc`, `db/blob/blob_contents.h`, `db/blob/blob_fetcher.h`

## BlobSource: Caching Gateway

`BlobSource` is the central coordinator for all blob reads. It manages a two-level cache hierarchy:

| Cache Layer | Class | Contents | Eviction |
|-------------|-------|----------|----------|
| Blob cache | `SharedCacheInterface` wrapping user-provided `Cache` | Uncompressed blob values (`BlobContents`) | Depends on `Cache` implementation; inserted with `BOTTOM` priority |
| File cache | `BlobFileCache` | Open `BlobFileReader` handles | Depends on table cache implementation; explicit eviction on file obsolescence |

## BlobSource::GetBlob() Workflow

The primary read method follows a cache-first pattern:

Step 1: **Build cache key**. The cache key is constructed from `(db_id, db_session_id, file_number, offset)` using `OffsetableCacheKey`. The `file_size` parameter is accepted but not used in the key computation.

Step 2: **Check blob cache**. If a blob cache is configured, look up the uncompressed blob. On hit, the cached blob is pinned to the output `PinnableSlice` via zero-copy transfer and statistics (`BLOB_DB_CACHE_HIT`, `BLOB_DB_CACHE_BYTES_READ`) are updated.

Step 3: **Check read tier**. If `read_options.read_tier == kBlockCacheTier`, return `Status::Incomplete` since disk I/O is not allowed.

Step 4: **Get file reader**. Obtain a `BlobFileReader` from `BlobFileCache`. The file cache uses striped mutexes (`kNumberOfMutexStripes = 128`) to prevent multiple threads from racing to open the same file.

Step 5: **Read blob from file**. `BlobFileReader::GetBlob()` reads the blob record, verifies checksums if enabled, and decompresses if needed.

Step 6: **Populate blob cache**. If a cache is configured and `fill_cache` is true, insert the uncompressed blob into the cache with `Cache::Priority::BOTTOM`. Important: unlike flush-time cache prepopulation (which is best-effort), a cache insertion failure here causes `GetBlob()` to return an error to the caller.

Step 7: **Transfer to PinnableSlice**. If the blob was cached, transfer the cache handle to the `PinnableSlice`. Otherwise, transfer ownership of the `BlobContents` directly.

## BlobFileReader::GetBlob() Workflow

The file-level read method handles I/O and verification:

Step 1: **Validate offset**. `IsValidBlobOffset()` checks that the offset is within the file bounds, accounting for header, record header, key size, and footer.

Step 2: **Verify compression type**. The compression type from the BlobIndex must match the file-level compression type stored in the header.

Step 3: **Calculate read range**. If `verify_checksums` is enabled, the full record (header + key + value) is read. The adjustment is `BlobLogRecord::kHeaderSize + key_size = 32 + key_size` bytes before the value offset. If checksums are disabled, only the value portion is read.

Step 4: **Read data**. Try the prefetch buffer first (used during compaction with `blob_compaction_readahead_size`). If not prefetched, read from the file directly, supporting both buffered and direct I/O. With direct I/O, aligned buffers (`AlignedBuf`) are used for reads.

Step 5: **Verify checksums**. `VerifyBlob()` decodes the record header, checks `header_crc`, verifies key size/value size/key content match, then calls `CheckBlobCRC()` to verify the blob CRC over key + value.

Step 6: **Decompress**. `UncompressBlobIfNeeded()` extracts the uncompressed size from the compressed data, allocates an output buffer (using the cache allocator if caching is enabled), and decompresses.

## MultiGet Optimization

BlobDB supports batched reads for multiple blobs:

### BlobSource::MultiGetBlob()

Handles blobs across multiple files:

Step 1: Sort blob requests within each file by offset (ascending order requirement).

Step 2: Call `MultiGetBlobFromOneFile()` for each file.

### BlobSource::MultiGetBlobFromOneFile()

Handles multiple blobs from a single file:

Step 1: Check blob cache for each request, building a bitmask of cache hits.

Step 2: If all blobs are cached, return immediately.

Step 3: For remaining blobs, obtain a `BlobFileReader` and call `BlobFileReader::MultiGetBlob()`.

Step 4: `BlobFileReader::MultiGetBlob()` uses `RandomAccessFileReader::MultiRead()` to issue all reads in a single system call (when supported by the OS/filesystem).

Step 5: Verify checksums and decompress each blob individually.

Step 6: Insert successfully read blobs into the cache if `fill_cache` is true.

## Zero-Copy Read Optimization

BlobDB avoids copying large blob values through `PinnableSlice`:

**Cached blobs**: `PinCachedBlob()` calls `PinnableSlice::PinSlice()` with the blob data and transfers the cache handle ownership. The cache entry remains pinned (not evictable) until the `PinnableSlice` is destroyed or reset. This avoids copying potentially multi-megabyte blobs.

**Uncached blobs**: `PinOwnedBlob()` pins the `BlobContents` allocation directly. A custom deleter (`delete static_cast<BlobContents*>(arg1)`) is registered to free the memory when the `PinnableSlice` is destroyed.

## BlobFileCache Details

`BlobFileCache` caches open `BlobFileReader` instances using the table cache (the same cache that stores open SST file readers):

- Cache entries use `CacheEntryRole::kMisc`.
- Striped mutexes prevent races when multiple threads try to open the same blob file simultaneously.
- `BlobFileCache::Evict()` is called when a blob file becomes obsolete to remove its reader from the cache and close the file handle.
- Each `BlobFileReader` stores the file size, compression type, and a `Decompressor` instance.

## Charged Cache Support

If the `BlockBasedTableOptions::cache_usage_options` has `CacheEntryRole::kBlobCache` charged enabled, `BlobSource` wraps the blob cache in a `ChargedCache`. This charges blob cache usage against the block cache capacity, enabling unified memory management when using the same cache for both blocks and blobs.

## Cache Tier Support

`BlobSource` respects the `lowest_used_cache_tier` setting from `ImmutableOptions`. This controls whether blob cache lookups and insertions also interact with secondary cache (e.g., a tiered caching configuration). The tier is passed to `blob_cache_.LookupFull()` and `blob_cache_.InsertFull()`.

## BlobContents

`BlobContents` in `db/blob/blob_contents.h` represents a single uncompressed blob value:

- Owns a `CacheAllocationPtr` (memory allocated via the cache's `MemoryAllocator` if available)
- Stores a `Slice` pointing into the allocation
- Reports `ApproximateMemoryUsage()` for cache charging
- Uses `CacheEntryRole::kBlobValue` for cache accounting

`BlobContentsCreator` provides a `Create()` method used by the cache framework to reconstruct `BlobContents` from serialized data (e.g., when promoting from secondary cache).
