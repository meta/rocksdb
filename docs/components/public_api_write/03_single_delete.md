# SingleDelete Semantics

**Files:** `include/rocksdb/db.h`, `include/rocksdb/write_batch.h`, `db/write_batch.cc`, `db/compaction/compaction_iterator.cc`, `db/dbformat.h`

## Overview

SingleDelete is an optimized deletion operation that removes a key known to have been written exactly once since the last SingleDelete for that key. Unlike the regular Delete (which works with any number of prior Puts), SingleDelete has strict preconditions that enable more aggressive space reclamation during compaction.

## Correctness Requirements

The contract for SingleDelete is defined in `DB::SingleDelete()` in `include/rocksdb/db.h`:

1. There must be exactly one Put for the key since the previous SingleDelete (or since the key was first created)
2. The key must not have been written using Merge
3. SingleDelete and regular Delete must not be mixed for the same key

Violating any of these requirements results in undefined behavior, which may include data inconsistency, stale reads, or unexpected values surfacing after compaction.

Key Invariant: SingleDelete only behaves correctly if there has been exactly one Put for this key since the previous SingleDelete for this key. This is a true correctness invariant -- violating it can cause data corruption visible to readers.

## How SingleDelete Differs from Delete

| Aspect | Delete | SingleDelete |
|--------|--------|--------------|
| Internal type tag | `kTypeDeletion` | `kTypeSingleDeletion` |
| Precondition | None | Exactly one Put per SingleDelete |
| Compaction behavior | Tombstone persists until it can be dropped (may be dropped before bottommost level when `KeyNotExistsBeyondOutputLevel()` allows) | Cancels with matching Put and both are dropped |
| Space reclamation | Deferred; tombstone may persist across multiple compactions | Immediate when Put-SingleDelete pair compacted together |
| Merge compatibility | Yes | No |
| Mix with Delete | Yes (idempotent) | No (undefined behavior) |

The key advantage of SingleDelete is that during compaction, when the compaction iterator encounters a SingleDelete followed by its matching Put, both records can be dropped immediately (subject to snapshot constraints). Regular Delete tombstones can also be dropped before reaching the bottommost level when `KeyNotExistsBeyondOutputLevel()` and snapshot conditions allow it, but SingleDelete enables pairwise cancellation more reliably and earlier in the compaction process.

## Compaction Behavior

The compaction iterator processes SingleDelete records with a "peek ahead" strategy, implemented in `CompactionIterator::NextFromInput()` in `db/compaction/compaction_iterator.cc`. The workflow is:

1. **Encounter SingleDelete** -- When the iterator sees a `kTypeSingleDeletion` record
2. **Peek at next key** -- Advance the input iterator and check if the next record has the same user key
3. **Match with Put** -- If the next record is a Put (`kTypeValue`) with the same key, both records can potentially be dropped
4. **Check snapshot constraints** -- Dropping is only safe if no active snapshot needs to see the intermediate state. Specifically, both conditions must hold:
   - The corresponding Put is found (Rule 1)
   - The SingleDelete is already visible in the earliest snapshot, or there are no earlier write-conflict snapshots (Rule 2)
5. **Drop or keep** -- If both rules are satisfied, drop both records. Otherwise, output the SingleDelete (and possibly the Put with its value cleared as an optimization)

### Anomalous Cases

The compaction iterator handles several edge cases that arise from contract violations:

- **SingleDelete followed by SingleDelete** -- Two consecutive SingleDeletes for the same key. The compaction iterator skips the first and lets the next iteration handle the second. Mismatch counters are incremented (`num_single_del_mismatch`).
- **SingleDelete followed by Merge** -- This is explicitly unsupported. The compaction iterator detects this anomaly and reports it via `iter_stats_`.
- **SingleDelete followed by Delete** -- Also anomalous. When `enforce_single_del_contracts_` is enabled (the default for non-transaction workloads), this causes compaction to fail with `Status::Corruption`. When disabled (e.g., in some transaction configurations), a warning is logged, the SingleDelete is dropped, and the Delete is left for subsequent processing. The `TransactionDBOptions::rollback_deletion_type_callback` can be configured to avoid this scenario in transaction rollback paths.
- **SingleDelete with no following key** -- If the key doesn't appear below the current level (`KeyNotExistsBeyondOutputLevel()` returns true and we are not in `ingest_behind` mode), the SingleDelete can be dropped on its own.

### Optimization 3

When a SingleDelete-Put pair is found but Rule 2 is not satisfied (snapshot prevents dropping), the compaction iterator outputs both records but clears the Put's value. This saves space while preserving the logical chain. When Rule 2 is later satisfied in a subsequent compaction, both records can be fully dropped.

## WriteBatch Encoding

SingleDelete uses `kTypeSingleDeletion` (default column family) or `kTypeColumnFamilySingleDeletion` (non-default column family) as the type tag. The record format is identical to Delete -- only the key is encoded, with no value field:

- Default CF: `kTypeSingleDeletion` key(varstring)
- Non-default CF: `kTypeColumnFamilySingleDeletion` cf_id(varint32) key(varstring)

The encoding logic in `WriteBatchInternal::SingleDelete()` in `db/write_batch.cc` follows the same pattern as Delete: increment count, write tag, write length-prefixed key, set `HAS_SINGLE_DELETE` content flag, and optionally compute protection info.

## User-Defined Timestamp Interaction

SingleDelete supports user-defined timestamps with the same calling conventions as other write operations. When timestamps are enabled, the timestamp affects whether a SingleDelete-Put pair can be dropped during compaction. Both records must have timestamps below `full_history_ts_low` (if set) to be eligible for garbage collection.

## When to Use SingleDelete

SingleDelete is appropriate for workloads where:

- Each key is written exactly once before being deleted (e.g., temporary keys, queue-like patterns)
- Space efficiency is critical and tombstone accumulation is a concern
- The application can guarantee the single-Put contract

SingleDelete should be avoided when:

- Keys may be overwritten (Put called multiple times before delete)
- Merge operations are used on the same keys
- The application cannot strictly track whether a key has been written once
- Multiple writers may Put the same key concurrently

## Performance Benefit

The primary performance benefit of SingleDelete over Delete is reduced space amplification. With regular Delete, tombstones accumulate and are only fully cleaned up when they reach the bottommost level during compaction. SingleDelete enables pair-wise cancellation at any level, reducing the number of records that flow through the LSM tree.

This benefit is most pronounced in workloads with high key churn (frequent put-then-delete patterns) where tombstone accumulation would otherwise cause significant space overhead. Note that regular Delete tombstones also have optimized early-drop cases (via `KeyNotExistsBeyondOutputLevel()`), but SingleDelete's pairwise cancellation is more deterministic.

## Known Performance Pitfall: Memtable Tombstone Accumulation

When SingleDelete records accumulate in the memtable while their matching Put records reside in deeper SST levels, range scans must traverse many tombstones without resolving them. This can cause significant CPU overhead during iteration, because the tombstones cannot be cancelled until they meet their matching Puts during compaction.

This scenario is common in queue-like workloads where new keys are SingleDeleted shortly after being Put, but the Puts have already been flushed to SST files. Deletion-triggered compaction does not help here because the SingleDelete records are still in the memtable, not in SST files.

Mitigations include:
- Flushing more aggressively to move SingleDelete records into SST files where compaction can cancel them with their matching Puts
- Using `CompactOnDeletionCollectorFactory` (see `NewCompactOnDeletionCollectorFactory()` in `include/rocksdb/utilities/table_properties_collectors.h`) to prioritize compaction of SST files with high tombstone density
- Tuning `max_write_buffer_number` and `write_buffer_size` to control how many tombstones accumulate before flush
