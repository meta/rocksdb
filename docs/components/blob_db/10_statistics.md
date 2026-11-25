# Statistics and Monitoring

**Files:** `include/rocksdb/statistics.h`, `monitoring/statistics_impl.h`, `db/blob/blob_source.cc`, `db/blob/blob_file_reader.cc`, `db/blob/blob_log_writer.cc`, `db/blob/blob_file_builder.cc`, `db/compaction/compaction_job.cc`, `include/rocksdb/listener.h`, `db/internal_stats.cc`

## Tickers (Counters)

The following tickers are actively recorded by integrated BlobDB:

### Cache Statistics

| Ticker | Description |
|--------|-------------|
| `BLOB_DB_CACHE_MISS` | Number of blob cache misses |
| `BLOB_DB_CACHE_HIT` | Number of blob cache hits |
| `BLOB_DB_CACHE_ADD` | Number of blobs successfully added to cache |
| `BLOB_DB_CACHE_ADD_FAILURES` | Number of failed blob cache insertions |
| `BLOB_DB_CACHE_BYTES_READ` | Total bytes read from blob cache |
| `BLOB_DB_CACHE_BYTES_WRITE` | Total bytes written to blob cache |

### I/O Statistics

| Ticker | Description |
|--------|-------------|
| `BLOB_DB_BLOB_FILE_BYTES_WRITTEN` | Total bytes written to blob files |
| `BLOB_DB_BLOB_FILE_BYTES_READ` | Total bytes read from blob files |
| `BLOB_DB_BLOB_FILE_SYNCED` | Number of blob file sync operations |

### GC Statistics (Integrated BlobDB)

| Ticker | Description |
|--------|-------------|
| `BLOB_DB_GC_NUM_KEYS_RELOCATED` | Number of keys relocated by GC during compaction |
| `BLOB_DB_GC_BYTES_RELOCATED` | Bytes relocated by GC during compaction |

### Legacy/Stacked BlobDB Only

The following tickers are declared in the enum but only recorded by the legacy stacked BlobDB (`utilities/blob_db/`). They are NOT recorded by integrated BlobDB and will always be zero:

| Ticker | Description |
|--------|-------------|
| `BLOB_DB_NUM_PUT` | Number of Put operations |
| `BLOB_DB_NUM_WRITE` | Number of Write operations |
| `BLOB_DB_NUM_GET` | Number of Get operations |
| `BLOB_DB_NUM_MULTIGET` | Number of MultiGet operations |
| `BLOB_DB_NUM_SEEK` | Number of Seek operations |
| `BLOB_DB_NUM_NEXT` | Number of Next operations |
| `BLOB_DB_NUM_PREV` | Number of Prev operations |
| `BLOB_DB_NUM_KEYS_WRITTEN` | Number of keys written |
| `BLOB_DB_NUM_KEYS_READ` | Number of keys read |
| `BLOB_DB_BYTES_WRITTEN` | Total bytes written (keys + values) |
| `BLOB_DB_BYTES_READ` | Total bytes read (keys + values) |
| `BLOB_DB_WRITE_BLOB` | Number of blobs written to blob files |
| `BLOB_DB_WRITE_BLOB_TTL` | Number of TTL blobs written |
| `BLOB_DB_GC_NUM_FILES` | Number of blob files processed by GC |
| `BLOB_DB_GC_NUM_NEW_FILES` | Number of new blob files created by GC |
| `BLOB_DB_GC_FAILURES` | Number of GC failures |
| `BLOB_DB_BLOB_INDEX_EXPIRED_COUNT` | Expired blob index entries (TTL) |
| `BLOB_DB_BLOB_INDEX_EXPIRED_SIZE` | Bytes of expired blob indices |
| `BLOB_DB_BLOB_INDEX_EVICTED_COUNT` | Evicted blob index entries (FIFO) |
| `BLOB_DB_BLOB_INDEX_EVICTED_SIZE` | Bytes of evicted blob indices |
| `BLOB_DB_FIFO_NUM_FILES_EVICTED` | Blob files evicted by FIFO |
| `BLOB_DB_FIFO_NUM_KEYS_EVICTED` | Keys evicted by FIFO |
| `BLOB_DB_FIFO_BYTES_EVICTED` | Bytes evicted by FIFO |
| `BLOB_DB_WRITE_INLINED_DEPRECATED` | Deprecated |
| `BLOB_DB_WRITE_INLINED_TTL_DEPRECATED` | Deprecated |

## Histograms (Latency Distributions)

The following histograms are actively recorded by integrated BlobDB:

| Histogram | Description |
|-----------|-------------|
| `BLOB_DB_BLOB_FILE_WRITE_MICROS` | Blob file write latency |
| `BLOB_DB_BLOB_FILE_READ_MICROS` | Blob file read latency |
| `BLOB_DB_BLOB_FILE_SYNC_MICROS` | Blob file sync latency |
| `BLOB_DB_COMPRESSION_MICROS` | Blob compression latency |
| `BLOB_DB_DECOMPRESSION_MICROS` | Blob decompression latency |

The following histograms are declared but only recorded by the legacy stacked BlobDB:

| Histogram | Description |
|-----------|-------------|
| `BLOB_DB_KEY_SIZE` | Distribution of key sizes |
| `BLOB_DB_VALUE_SIZE` | Distribution of value sizes |
| `BLOB_DB_WRITE_MICROS` | Write operation latency |
| `BLOB_DB_GET_MICROS` | Get operation latency |
| `BLOB_DB_MULTIGET_MICROS` | MultiGet operation latency |
| `BLOB_DB_SEEK_MICROS` | Seek operation latency |
| `BLOB_DB_NEXT_MICROS` | Next operation latency |
| `BLOB_DB_PREV_MICROS` | Prev operation latency |

## Perf Context Counters

Per-request performance counters available via `get_perf_context()`:

| Counter | Description |
|---------|-------------|
| `blob_read_count` | Number of blob reads in the current request |
| `blob_read_byte` | Total bytes of blob reads |
| `blob_read_time` | Time spent reading blobs (nanoseconds) |
| `blob_checksum_time` | Time spent verifying blob checksums |
| `blob_decompress_time` | Time spent decompressing blobs |
| `blob_cache_hit_count` | Blob cache hits in the current request |

## Compaction Statistics

`CompactionIterationStats` tracks blob-related activity during compaction:

| Field | Description |
|-------|-------------|
| `num_blobs_read` | Blobs read for GC relocation |
| `total_blob_bytes_read` | Total bytes of blobs read for GC |
| `num_blobs_relocated` | Blobs successfully relocated to new files |
| `total_blob_bytes_relocated` | Total bytes of source blobs relocated (original compressed size from old BlobIndex, not new file bytes written) |

These are reported in compaction logs and aggregated into the `InternalStats` for monitoring.

## Monitoring Blob Storage

`VersionStorageInfo::GetBlobStats()` provides an aggregate view:

| Metric | Description |
|--------|-------------|
| `total_file_size` | Total on-disk size of all blob files |
| `total_garbage_size` | Total garbage bytes across all blob files |
| `space_amp` | Ratio of `total_file_size / (total_file_size - total_garbage_size)` |

Note: The blob file count is not part of `GetBlobStats()`. It is available from `GetBlobFiles().size()` or the `rocksdb.num-blob-files` DB property.

These metrics are useful for monitoring garbage accumulation and tuning GC parameters. High space amplification suggests the GC cutoff should be increased or the force threshold should be lowered.

## DB Properties

The following DB properties (queryable via `DB::GetProperty()`) provide blob-related information:

| Property | Description |
|----------|-------------|
| `rocksdb.num-blob-files` | Number of blob files in the current Version |
| `rocksdb.blob-stats` | Total number, size, garbage bytes, and space amplification of blob files |
| `rocksdb.total-blob-file-size` | Total size of all blob files aggregated across all Versions |
| `rocksdb.live-blob-file-size` | Total size of all blob files in the current Version |
| `rocksdb.live-blob-file-garbage-size` | Total garbage bytes in all blob files in the current Version |
| `rocksdb.estimate-live-data-size` | Includes live blob data (total bytes minus garbage bytes) |
| `rocksdb.blob-cache-capacity` | Blob cache capacity (includes block cache data if using shared cache) |
| `rocksdb.blob-cache-usage` | Current blob cache memory usage |
| `rocksdb.blob-cache-pinned-usage` | Pinned (non-evictable) blob cache memory |

Note: When using a shared block/blob cache, the `blob-cache-capacity`, `blob-cache-usage`, and `blob-cache-pinned-usage` properties report values that include both block and blob entries.

## Metadata APIs

`ColumnFamilyMetaData` (see `include/rocksdb/metadata.h`) includes blob file information:

- A vector of `BlobMetaData` objects, one per live blob file, containing: file number, file name/path, file size, total blob count/bytes, garbage blob count/bytes, and file checksum.
- Total count and size of all live blob files for the column family.

## Event Listener Notifications

BlobDB provides lifecycle notifications via `EventListener` (see `include/rocksdb/listener.h`):

- `OnBlobFileCreationStarted`: Called when a new blob file is about to be created.
- `OnBlobFileCreated`: Called when a blob file creation attempt completes, whether the file was successfully created or not. Applications must inspect `info.status` to determine success or failure.
- `OnBlobFileDeleted`: Called when a blob file is deleted.
- `FlushJobInfo` and `CompactionJobInfo` include vectors of `BlobFileAdditionInfo` for newly created blob files.
- `CompactionJobInfo` also includes `BlobFileGarbageInfo` structures describing garbage produced by the compaction.
