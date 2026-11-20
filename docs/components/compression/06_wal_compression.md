# WAL Compression

**Files:** `include/rocksdb/options.h`, `db/log_writer.cc`, `db/log_reader.cc`, `db/log_format.h`

## Write-Ahead Log Compression

RocksDB supports compressing WAL records to reduce write amplification and log storage.

**Configuration**:

```cpp
options.wal_compression = kZSTD;  // Default: kNoCompression
```

**Supported algorithms**: Any supported `CompressionType` that works with streaming compression.

## WAL Compression Format

WAL compression uses standard WAL physical records. The first record in a compressed WAL file is a `kSetCompressionType` record (type=9) that declares the compression type used for subsequent records (see `kSetCompressionType` in `db/log_format.h`):

```
kSetCompressionType record:
[header: checksum(4) + length(2) + type(1)][compression_type: Fixed32]
```

Subsequent logical records are fed through a `StreamingCompress` instance, and the compressed output is fragmented into normal WAL physical records (kFirst/kMiddle/kLast).

## Compression Workflow

The write path is in `db/log_writer.cc`, the read path in `db/log_reader.cc`:

1. Write `kSetCompressionType` record at file start (`AddCompressionTypeRecord`)
2. Initialize `StreamingCompress` with the declared compression type
3. For each `WriteBatch`: feed data through streaming compressor
4. Fragment compressed output into standard WAL physical records
5. On recovery: detect `kSetCompressionType`, initialize `StreamingUncompress`, decompress records

**Invariant**: WAL compression uses **streaming compression** per WAL file, not per-record compression. The compression type is declared once at file start. `db/wal_manager.cc` handles only bookkeeping (listing, archiving, purging WAL files), not compression logic.

## Performance Considerations

- **Write latency**: +10-30% due to compression CPU cost (mitigated by batching)
- **WAL size**: -50% to -70% for typical workloads
- **Recovery time**: +10-20% due to decompression (linear in WAL size)

**Recommendation**: Enable for write-heavy workloads where WAL write amplification is significant (e.g., NVMe SSDs with high IOPS).
