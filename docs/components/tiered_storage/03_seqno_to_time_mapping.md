# Sequence-Number-to-Time Mapping

**Files:** `db/seqno_to_time_mapping.h`, `db/seqno_to_time_mapping.cc`, `db/compaction/compaction_job.cc`, `db/db_impl/db_impl.cc`

## Purpose

RocksDB needs to determine the approximate write time of each key during compaction to decide whether it should stay in the hot tier (proximal level) or migrate to the cold tier (last level). Since storing a timestamp per key would be too expensive, RocksDB uses a sampled mapping from sequence numbers to unix timestamps.

## SeqnoToTimeMapping Class

`SeqnoToTimeMapping` (see `db/seqno_to_time_mapping.h`) maintains a sorted list of `(sequence_number, unix_time)` pairs. The mapping provides approximate answers to two questions:

- **Given a seqno, what time was it written?** -- `GetProximalTimeBeforeSeqno()`
- **Given a time, what is the latest seqno written at or before that time?** -- `GetProximalSeqnoBeforeTime()`

Both queries use binary search and return conservative lower bounds. The mapping is "proximal" in that it returns the closest known value that does not exceed the query parameter.

## Sampling Cadence

The mapping is populated by periodic sampling in the DB background. The sampling cadence is determined by `MinAndMaxPreserveSeconds::GetRecodingCadence()` (see `db/seqno_to_time_mapping.h`):

    cadence = ceil(min_preserve_seconds / kMaxSeqnoTimePairsPerCF)

The ceiling division formula `(min_preserve_seconds + kMaxSeqnoTimePairsPerCF - 1) / kMaxSeqnoTimePairsPerCF` guarantees at least 1-second cadence when enabled. `min_preserve_seconds` is the minimum of all CFs' `max(preserve_internal_time_seconds, preclude_last_level_data_seconds)` and `kMaxSeqnoTimePairsPerCF = 100`. For example, with `preclude_last_level_data_seconds = 86400` (1 day), sampling occurs approximately every 864 seconds (~14 minutes).

The capacity is capped at `kMaxSeqnoToTimeEntries = 1000` entries across CFs. Each SST file stores at most `kMaxSeqnoTimePairsPerSST = 100` entries.

## Storage Format

Seqno-to-time mappings are stored as an SST table property under the key `"rocksdb.seqno.time.map"`. The format uses delta encoding from a zero base:

All pairs are delta-encoded relative to a running base that starts at `(seqno=0, time=0)`. The first pair is effectively stored as its raw values (since deltas from zero equal the values themselves), and subsequent pairs are stored as deltas from the previous pair. Each delta is varint-encoded.

This achieves compact representation -- typically < 0.3 KB per SST file.

## Enforced vs. Unenforced State

The mapping operates in two modes:

- **Enforced**: The list is sorted, capacity limits are met, and time span limits are met. Required for queries and serialization.
- **Unenforced**: Entries can be added in any order. Used during bulk loading from multiple SST properties. The `Enforce()` method transitions back to enforced state by sorting, merging, and compacting entries.

## Tiering Cutoff Computation

The key method for tiered storage is `GetCurrentTieringCutoffSeqnos()` (see `db/seqno_to_time_mapping.cc`). Given the current time and the two time-based options, it computes two cutoff sequence numbers:

1. **preserve_time_min_seqno**: The minimum seqno whose time information should be preserved. Based on `max(preserve_internal_time_seconds, preclude_last_level_data_seconds)`.
2. **preclude_last_level_min_seqno**: The minimum seqno that should be kept out of the last level. Based on `preclude_last_level_data_seconds` only.

The computation uses `GetProximalSeqnoBeforeTime(current_time - duration) + 1` to find the earliest seqno that might have been written within the time window.

Important: `preserve_time_min_seqno <= preclude_last_level_min_seqno` always holds. The preserve window is at least as large as the preclude window.

## Compaction Integration

During compaction preparation (see `CompactionJob::PrepareTimes()` in `db/compaction/compaction_job.cc`):

Step 1 -- Collect all seqno-to-time mappings from input SST files into a combined mapping.

Step 2 -- Enforce the mapping and compute cutoff sequence numbers using `GetCurrentTieringCutoffSeqnos()`.

Step 3 -- The `preclude_last_level_min_seqno` is adjusted with snapshot information. If the preclude feature is active and the earliest snapshot is older than the cutoff, `preclude_last_level_min_seqno` is lowered to the snapshot's seqno (keeping snapshot data in the hot tier heuristically).

Step 4 -- The final threshold `proximal_after_seqno_` is set to `max(preclude_last_level_min_seqno, keep_in_last_level_through_seqno)`, using the already-adjusted `preclude_last_level_min_seqno` from Step 3. Keys with seqno > this threshold go to the proximal level; keys with seqno <= go to the last level.

Step 5 -- After computing cutoffs, the mapping capacity is limited to `kMaxSeqnoToTimeEntries` for propagation to output SST files.

## Cross-CF Coordination

When multiple column families enable time preservation, the sampling cadence uses the shortest time window across all CFs (for finer granularity), while the capacity uses the longest time window (to retain enough history). This is coordinated through the `MinAndMaxPreserveSeconds` helper struct (see `db/seqno_to_time_mapping.h`).

## Sequence Number Zeroing

When data reaches the last level through compaction and is no longer needed by any snapshot, its sequence number is typically zeroed out. This is an important space optimization but means that data in the last level loses its time information. The seqno-to-time mapping is stored in all output SST files regardless of level. For last-level files where sequence numbers have been zeroed, the mapping entries may reference seqno 0 and provide limited value, but the mapping is still persisted. Over successive bottommost compactions, the mapping progressively loses usefulness as more sequence numbers are zeroed.

This has practical implications: once data is in the last level with seqno 0, it cannot be reliably aged or migrated back to the hot tier based on time. This is why the two-phase migration strategy (see chapter 10) is important when first enabling tiered storage.
