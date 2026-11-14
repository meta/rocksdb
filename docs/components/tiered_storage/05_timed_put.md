# TimedPut API

**Files:** `include/rocksdb/write_batch_base.h`, `include/rocksdb/write_batch.h`, `db/write_batch.cc`, `db/builder.cc`, `db/dbformat.h`, `db/seqno_to_time_mapping.h`, `db/compaction/compaction_iterator.cc`

## Overview

The `TimedPut` API allows applications to specify an explicit unix write time when inserting data. This is designed for use cases where data is being bulk-loaded or replicated with known historical write times, and the caller wants the data to be placed in the correct temperature tier immediately rather than waiting for it to age naturally.

## API

`WriteBatch::TimedPut()` (see `include/rocksdb/write_batch_base.h`) accepts a column family handle, key, value, and a unix timestamp in seconds:

    Status TimedPut(ColumnFamilyHandle* column_family, const Slice& key,
                    const Slice& value, uint64_t write_unix_time);

The intended usage is to write data with a past timestamp so that it is fast-tracked to the correct tier during compaction. For example, bulk-loading 30-day-old data with `preclude_last_level_data_seconds = 7 days` would set `write_unix_time` to 30 days ago, causing the data to go directly to the last level.

## Limitations

The feature is EXPERIMENTAL with known limitations:

- **Snapshot immutability**: Reading from a snapshot created before a `TimedPut(k, v, t)` may or may not see `k->v`. This is because the preferred sequence number swap (described below) can change the apparent write order.
- **User-defined timestamps**: Not compatible with user-defined timestamp feature.
- **Wide columns**: Not compatible with wide column entities.
- **WriteBatchWithIndex**: Not supported. `WriteBatchWithIndex::TimedPut()` returns `Status::NotSupported`. This means `TimedPut` cannot be used with transaction APIs that rely on `WriteBatchWithIndex`.

## Internal Representation: Two-Stage Lifecycle

TimedPut entries go through a two-stage lifecycle:

**Stage 1 -- Write batch encoding**: `WriteBatchInternal::TimedPut()` (see `db/write_batch.cc`) encodes the entry with record type `kTypeValuePreferredSeqno` (value `0x18`, see `db/dbformat.h`). At this stage, the value is packed with the caller-provided `write_unix_time` using `PackValueAndWriteTime()`:

    [original_value | write_unix_time (fixed64)]

**Stage 2 -- Flush translation**: During flush (see `db/builder.cc`), the `write_unix_time` is translated to a `preferred_seqno` using the seqno-to-time mapping. If a useful preferred seqno can be derived, the value is repacked using `PackValueAndSeqno()`:

    [original_value | preferred_seqno (varint)]

If no useful preferred seqno can be derived (e.g., the mapping lacks sufficient history), the entry may be downgraded to a plain `kTypeValue`. The functions `PackValueAndSeqno()` and `ParsePackedValueWithSeqno()` (see `db/seqno_to_time_mapping.h`) handle the preferred-seqno form.

## Preferred Sequence Number Swap

During compaction, the `CompactionIterator` (see `db/compaction/compaction_iterator.cc`) handles `kTypeValuePreferredSeqno` entries by potentially swapping the entry's actual sequence number with its preferred sequence number. This is the key mechanism that makes TimedPut work with per-key placement: after the swap, the entry's sequence number reflects its logical write time, so the standard `proximal_after_seqno_` threshold correctly routes it.

The swap occurs when all of the following conditions are met:

1. The entry's actual seqno is in the earliest snapshot range (`DefinitelyInSnapshot`)
2. Either compaction is to the bottommost level, or `KeyNotExistsBeyondOutputLevel` confirms no older versions exist below
3. No range tombstone would cover the entry after the swap

### Swap Safety

The swap is only safe when entries with the same user key and smaller sequence numbers are all within the same snapshot range. If there were entries in earlier snapshots with sequence numbers between the preferred and actual seqno, the swap would break internal key ordering invariants.

### Range Tombstone Interaction

Before swapping, the compaction iterator checks whether a range tombstone that does not cover the `kTypeValuePreferredSeqno` entry at its current seqno would cover it at the swapped seqno. If so, the swap is skipped and the entry is output as-is with its original type, to avoid incorrectly deleting the entry.

### Post-Swap State

After a successful swap:
- `ikey_.sequence` is set to `min(preferred_seqno, actual_seqno)`
- `ikey_.type` is changed from `kTypeValuePreferredSeqno` to `kTypeValue`
- The packed value is unpacked to the original value (preferred_seqno is stripped)

## Statistics

The compaction iterator tracks `num_input_timed_put_records` (total TimedPut entries seen) and `num_timed_put_swap_preferred_seqno` (successful swaps) in `CompactionIteratorStats`.
