---
title: Blob Direct Write With Partitioned Blob Files
layout: post
author: xbw
category: blog
---

## TL;DR

Blob Direct Write moves large-value separation earlier in RocksDB's write path.
When `enable_blob_files` and `enable_blob_direct_write` are enabled, values at
or above `min_blob_size` can be written directly to blob files during a write,
while the WAL and memtable store a compact `BlobIndex` reference instead of the
full value.

The companion partitioning support makes this more than a write-path
optimization. A column family can have multiple direct-write blob partitions,
and applications can provide a `BlobFilePartitionStrategy` to choose where each
large value goes. That turns blob files into a policy-controlled grouping unit.
For example, an application can route values with similar TTLs into the same
set of blob files while using Universal Compaction for the key and metadata
part of the LSM.

The reduced-scope v1 implementation landed in
[pull request #14535](https://github.com/meta/rocksdb/pull/14535), and
custom partition selection was added in
[pull request #14565](https://github.com/meta/rocksdb/pull/14565).

## Background

Integrated BlobDB already separates large values from the LSM tree. The LSM
stores keys plus blob references, and blob files store the large value bytes.
This reduces compaction write amplification because compaction can rewrite
keys and references without repeatedly copying large values.

Before Blob Direct Write, however, large values still entered RocksDB through
the normal write path first. They were serialized into a write batch, written
to the WAL, inserted into the memtable, and later extracted into blob files
during flush or compaction. That design is simple and broadly compatible, but
it means large values still consume WAL bandwidth and memtable memory before
they become out-of-line blobs.

Blob Direct Write changes that placement point. The write path can externalize
a large value immediately, then publish a `BlobIndex` through the normal WAL
and memtable machinery.

## Write Path

The core write-path logic lives in `BlobWriteBatchTransformer` and
`BlobFilePartitionManager`.

For a regular `Put` inside a `WriteBatch`, the transformer does the following:

1. Checks the column family's current blob direct-write settings.
2. Leaves small values inline when `value.size() < min_blob_size`.
3. Writes qualifying large values to a blob file through
   `BlobFilePartitionManager::WriteBlob()`.
4. Encodes the returned blob file number, offset, size, and compression type
   into a `BlobIndex`.
5. Rewrites the batch entry as `PutBlobIndex`.

The rest of RocksDB still sees an ordinary ordered write: the transformed batch
goes through WAL logging, sequence assignment, and memtable insertion. The
difference is that the payload traveling through those structures is the blob
reference rather than the original large value.

The implementation also handles `PutEntity()` for wide-column entities. Large
columns can be moved to blob files while small columns, such as metadata, stay
inline in the entity. The serialized entity then uses the V2 wide-column format
to store `BlobIndex` references for the blob-backed columns.

If a transformed write fails after appending blob bytes but before the
transformed batch is committed, RocksDB does not try to rewrite the physical
blob file. Instead, it records those appended records as initial garbage for
the file. This keeps the append-only blob-file contract simple while preserving
correct garbage accounting.

## Partitioned Blob Files

Blob Direct Write maintains a per-column-family `BlobFilePartitionManager`.
The manager owns one active blob writer slot per configured partition:

```cpp
Options options;
options.enable_blob_files = true;
options.enable_blob_direct_write = true;
options.allow_concurrent_memtable_write = false;
options.min_blob_size = 1024;
options.blob_direct_write_partitions = 8;
```

By default, RocksDB uses a round-robin strategy. Applications that need
policy-driven grouping can install a custom `BlobFilePartitionStrategy`:

```cpp
class TtlBucketPartitionStrategy : public rocksdb::BlobFilePartitionStrategy {
 public:
  using rocksdb::BlobFilePartitionStrategy::SelectPartition;

  const char* Name() const override { return "TtlBucketPartitionStrategy"; }

  uint32_t SelectPartition(uint32_t num_partitions, uint32_t column_family_id,
                           const rocksdb::Slice& key,
                           const rocksdb::Slice& value) override {
    return ExtractTtlBucket(key, value) % num_partitions;
  }
};

options.blob_direct_write_partition_strategy =
    std::make_shared<TtlBucketPartitionStrategy>();
```

The strategy sees the logical write inputs. If blob compression is enabled, the
strategy still receives the original uncompressed value. Its return value is
normalized modulo `blob_direct_write_partitions`, so implementations can return
a hash or bucket id directly.

For `PutEntity()`, the strategy has a wide-column overload:

```cpp
uint32_t SelectPartition(uint32_t num_partitions, uint32_t column_family_id,
                         const rocksdb::Slice& key,
                         const rocksdb::WideColumns& columns) override;
```

RocksDB calls this once per entity and reuses the selected partition for all
blob-backed columns in that entity. This is useful when inline metadata, such
as a TTL bucket or schema version, should determine placement for the large
payload columns.

The strategy runs on the write hot path. It must be thread-safe, must not
throw exceptions into RocksDB, and should avoid I/O, callbacks into RocksDB, or
other blocking work. It is also an application callback rather than a
serialized OPTIONS object, so applications that rely on custom placement must
provide the strategy again every time they open the DB.

## Lifecycle and Manifest Registration

Direct-write blob files have to line up with memtable lifetime. RocksDB uses a
generation-based lifecycle:

1. While a memtable is mutable, direct writes append to the current generation
   of active partition files.
2. When RocksDB switches memtables, `RotateCurrentGeneration()` moves those
   active files into an immutable generation associated with the memtable that
   now contains their `BlobIndex` references.
3. When that memtable is flushed, `PrepareFlushAdditions()` seals the matching
   blob files, builds `BlobFileAddition` records, and attaches them to the
   flush's `VersionEdit`.
4. The MANIFEST commit makes those blob files part of the column family's
   versioned state.

This is the key correctness boundary. Before flush commits, active
direct-write blob files can be read by the primary DB, but they are not yet
normal MANIFEST-visible BlobDB files. After flush commits, the usual versioned
BlobDB read and file-lifetime machinery owns them.

Old memtables and old `SuperVersion`s can temporarily outlive the flush that
registered their blob files. RocksDB protects those sealed file numbers until
the corresponding memtables are released, preventing obsolete-file cleanup from
racing with delayed reads through older in-memory state.

## Reading Direct-Write Blobs

A read can encounter a `BlobIndex` before its blob file is visible in the
current `Version`. This happens for values that have been written to an active
direct-write blob file but whose memtable has not flushed yet.

`BlobFilePartitionManager::ResolveBlobDirectWriteIndex()` handles that by
trying the normal versioned BlobDB path first. If the blob file is already
known to the `Version`, RocksDB returns that result directly, including any
real I/O or corruption error. If the file is not yet versioned, RocksDB falls
back to opening the blob file through the blob file cache and reading it as an
in-flight direct-write file.

This fallback also handles an important cache edge case. A reader may have
cached a blob file reader while the file was still open and smaller. After more
records are appended or after the file is sealed, that cached reader can have a
stale view of file size or footer state. The direct-write path evicts and
reopens the reader when needed so later reads see the finalized file.

## What Partitioning Enables

The most important capability is policy-aware physical grouping.

Consider a large-value workload where each key has a TTL, but different keys
expire on different horizons. A single DB-wide FIFO lifetime is too coarse:
short-lived values can remain on disk until the longest-lived values in the
same file age out. Standard compaction can delete keys individually, but if
large values are scattered across blob files, the deleted values become
garbage spread across many files.

With partitioned direct-write blobs, an application can route values into
coarse TTL buckets:

```text
TTL bucket 0 -> blob partition 0
TTL bucket 1 -> blob partition 1
TTL bucket 2 -> blob partition 2
...
```

The LSM can still use Universal Compaction for keys and metadata. A compaction
filter can remove expired keys based on metadata, and BlobDB can account for
the corresponding blob bytes as garbage. Because values with similar expiry
times were colocated, expiration tends to concentrate garbage in a smaller set
of blob files. Once a blob file contains no live values, it can be deleted as a
whole file.

That gives RocksDB a middle ground:

* The LSM keeps flexible key-level semantics.
* Blob files provide bulk physical reclamation.
* The application controls the grouping policy without creating separate DBs
  or column families for every group.

TTL bucketing is only one example. The same mechanism can group values by key
range, tenant, object class, expected lifetime, value size, or any other
application-level placement policy that can be computed cheaply during the
write.

## Interaction With Wide Columns

Partitioned Blob Direct Write becomes especially useful with wide columns.
Small metadata columns can stay inline in the LSM while large payload columns
move to blob files. That means read paths and compaction filters can inspect
metadata without always resolving the large blob.

For the TTL example, an entity might keep a compact TTL bucket column inline
and put the large payload column in a blob file. The partition strategy can use
the TTL column to choose a blob partition, while a compaction filter can later
drop expired keys by inspecting the inline metadata. Blob I/O is only needed
when the value itself is actually required.

This keeps RocksDB byte-oriented. RocksDB manages keys, sequence numbers,
`BlobIndex` references, blob file lifetime, and byte storage. The application
or a higher layer remains responsible for schema, TTL interpretation, and value
decoding.

## Limitations

Blob Direct Write is still a reduced-scope v1 feature. The durable boundary,
write-thread model, and API compatibility are deliberately conservative.

The most important limitation is crash recovery. Direct-write blob files are
registered in the MANIFEST only when the memtable containing their `BlobIndex`
references is flushed. Until that flush commits, the WAL can contain
references to active blob files that are not part of versioned metadata. v1
does not support recovering that active direct-write state through WAL replay,
so applications should not treat WAL logging alone as the crash-recovery
boundary for direct-written blobs. RocksDB forces a final flush for live
direct-write column families during clean close, but a process crash, failed
close-time flush, or shutdown path that cannot complete the flush can leave
active direct-write blob files unregistered.

There are also configuration and API limits:

* `enable_blob_direct_write` requires `enable_blob_files`.
* `enable_blob_direct_write`, `blob_direct_write_partitions`, and
  `blob_direct_write_partition_strategy` are not dynamically changeable
  through `SetOptions()`.
* The v1 write path requires the ordered single-memtable-writer mode. It is not
  compatible with `unordered_write`, `enable_pipelined_write`,
  `two_write_queues`, or `allow_concurrent_memtable_write`.
* `DB::IngestWriteBatchWithIndex()` is not supported while any live column
  family has `enable_blob_direct_write` enabled.
* MemPurge and user-defined timestamps are not supported with Blob Direct
  Write.
* Pre-serialized wide-column entities that already contain blob references are
  rejected when Blob Direct Write is enabled. RocksDB needs to create and track
  the direct-write blob references inside the current write path.
* Checkpoint, backup, and live-file enumeration must flush pending
  direct-write state first. Calls that intentionally skip the flush, or that
  run while the WAL is locked and therefore cannot flush, can return
  `NotSupported`.

One subtle implementation point is that partitions are currently a file
placement and lifecycle abstraction. The manager has multiple active partition
files, but v1 still protects partition state with a single manager mutex.
Future work can add finer-grained per-partition concurrency without changing
the placement or generation contract.

Custom file systems also need to support the visibility model. The primary DB
may read an active direct-write blob file while it is still open for writing,
so appended bytes must be visible to a separate reader before the writer is
closed. File systems that do not provide that behavior should avoid enabling
Blob Direct Write until they can satisfy the contract.

## When to Consider It

Blob Direct Write is worth considering when large values dominate the write
path and there is a useful physical grouping policy for those values. The most
natural cases are workloads where the key and small metadata should stay cheap
to compact, scan, or filter, while the large payload should be stored
out-of-line and reclaimed in policy-controlled groups.

For workloads with mixed TTLs, the combination of Universal Compaction,
metadata-aware filtering, and partitioned blob placement can provide
per-value-expiration behavior while preserving whole-file reclamation as the
physical cleanup unit. More generally, partitioned blob files give RocksDB a
new placement hook: applications can keep RocksDB's LSM semantics for keys
while shaping blob-file layout around how data will age, be read, or be
deleted.
