# Sharded Explicit-Snapshot Design

This document is the authoritative design for explicit RocksDB snapshot
concurrency. It covers `DB::GetSnapshot()`, `DB::ReleaseSnapshot()`,
timestamped snapshots, snapshot-list readers, external file ingestion, and
the compaction work enabled when the oldest snapshot advances.

## Scope

The design separates two responsibilities:

1. Maintain the live snapshot set used for read visibility.
2. Propagate oldest-snapshot progress to compaction state.

Snapshot creation and release are common read-side operations. Oldest-snapshot
maintenance is less frequent and interacts with Version state protected by the
DB mutex. On a read-write primary, the common operations therefore use
snapshot-shard locks, while maintenance is performed by existing
DB-mutex-protected paths. Secondary and follower snapshot creation retains its
DB-mutex synchronization with view installation.

The design does not change snapshot visibility, file formats, or
compaction-picking policy. It also does not make the snapshot list lock-free,
introduce reader/writer locks, or tune the shard count dynamically.

## Goals

- Keep primary snapshot creation and all snapshot release out of the DB-mutex
  domain in the common case.
- Allow independent readers to create and release snapshots concurrently.
- Preserve sequence ordering and timestamped-snapshot rules.
- Never report an oldest snapshot newer than a live snapshot.
- Eventually schedule cleanup made eligible by a snapshot release.
- Preserve external-ingestion and snapshot-support serialization.
- Keep CPU, waits, and I/O under the DB mutex tightly bounded.

## Data Model

`ShardedSnapshotList` owns 16 cache-line-aligned shards. Each shard contains:

- a mutex;
- a sequence-ordered `SnapshotList`;
- a mutex-protected snapshot-creation gate; and
- a mutex-protected count of releases from that shard.

Each `SnapshotImpl` records the index of its creation shard. Creation normally
masks the current physical CPU ID by the fixed shard count. On platforms
without a physical CPU ID, each thread caches a value selected by a relaxed
round-robin counter and applies the same mask. A coroutine may resume on a
different worker or CPU without affecting release: the snapshot, rather than
the current thread, identifies the owning shard.

`DBImpl` retains DB-mutex-protected maintenance values for:

- the largest exact oldest-snapshot sequence processed;
- the sum of the per-shard release counts at the last refresh;
- the minimum bottommost-file and standalone-range-deletion thresholds across
  current column families.

Timestamped snapshots also remain in `TimestampedSnapshotList`, which is
protected by the DB mutex. A timestamped snapshot is linked into both the
sharded list and the timestamp index.

## Synchronization by Operation

| Operation | Synchronization |
|-----------|-----------------|
| Primary regular creation | One shard mutex |
| Secondary/follower creation | DB mutex, then one shard mutex |
| Regular release | The recorded shard mutex |
| Timestamped creation | DB mutex, then one shard mutex |
| `GetAll()` | One shard mutex at a time |
| Oldest time or sequence scan | All shard mutexes in index order |
| Snapshot count | All shard mutexes in index order |
| Oldest-sequence maintenance | DB mutex, then all shard mutexes in index order |
| Snapshot-support disable | All shard mutexes in index order |
| External-ingestion gate | DB mutex, then each shard mutex in index order |

Both creation and release mutate a list, so a reader/writer lock does not help
the common operations. A lock-free linked list would also require safe memory
reclamation for concurrently released snapshots and is outside this design.

## Snapshot Creation

On a read-write primary, regular creation allocates the snapshot and reads the
wall-clock time before entering a shard. `TryNew()` then performs the following
work under one shard mutex:

1. Check whether external ingestion has blocked DB-mutex-free creation.
2. Check whether the configured memtable supports snapshots.
3. Read the last published sequence number.
4. Link the snapshot at the tail of the shard's ordered list.

Reading the sequence inside the shard mutex keeps each shard ordered. A later
creator in the same shard cannot append an older sequence number.

If the ingestion gate is active, a DB-mutex-free creator unlocks its shard,
acquires the DB mutex, and retries. This is the only regular-creation fallback
into the DB-mutex domain.

Secondary and follower creation also reads the clock and allocates before
locking, but then deliberately acquires the DB mutex around sequence/view
capture and shard insertion. This preserves synchronization with catch-up view
installation without putting user clock or allocator work under the DB mutex.

### Snapshot-Support Changes

The supported-to-unsupported transition locks all shards before publishing the
new state. A creator therefore either completes while support is still enabled
or observes the disabled state while holding its shard. The state can be read
with relaxed ordering because the shard lock supplies the synchronization.

### Timestamped Snapshots

Timestamped creation holds the DB mutex for the timestamp index and one shard
mutex for the sharded list. Validation against the latest timestamped snapshot
happens before insertion.

When a caller supplies an explicit sequence, the snapshot is linked before the
sequence is published, and both operations occur under the same shard mutex.
A regular creator in that shard cannot append the old published sequence after
the explicit-sequence snapshot. This keeps insertion at the tail and avoids a
linear search while the DB mutex is held.

## Snapshot Release

`ReleaseSnapshot()` uses the shard recorded in `SnapshotImpl`:

1. Lock that shard.
2. Unlink the snapshot.
3. Increment that shard's release count.
4. Unlock the shard and destroy the snapshot.

It does not acquire the DB mutex, scan other shards, inspect column families,
or schedule compaction. The release count is updated under the shard lock
already required for unlinking, so release adds no atomic read-modify-write or
shared cache line.

## Snapshot-List Readers

### `GetAll()`

`GetAll()` locks one shard at a time and copies that shard's ordered sequence
numbers. After all copies are complete, a min-heap merge across the 16 shards:

- returns sequence numbers in ascending order;
- removes duplicate sequence numbers;
- excludes sequence numbers greater than `max_seq`; and
- reports the oldest write-conflict-boundary snapshot.

The merge is `O(N log 16)` for `N` copied snapshots and uses at most 16 heap
entries. The method does not hold all shard mutexes while copying an unbounded
number of snapshots.

The per-shard copies are not one global point-in-time observation. That is
intentional. Flush and compaction need the returned snapshot set plus a safe
published-sequence boundary, rather than an atomic snapshot of every list.

### Exact and Statistical Readers

Oldest-snapshot sequence properties lock all shards and compare their heads.
The oldest-time property returns the minimum wall-clock time among the shard
heads. It is approximate when creation races the query or the wall clock moves
backward; it is intended for statistics rather than snapshot visibility.

Snapshot count locks all shards and sums their list counts, so the public
`rocksdb.num-snapshots` property observes one exact point in time.
Statistics-only property reads do not trigger maintenance.

BlobDB uses the exact all-shard oldest observation when deciding whether an
obsolete blob file remains visible.

WritePrepared holds the DB mutex while loading snapshots, preserving its
serialization with timestamped-snapshot changes. Regular snapshots can still
be created concurrently. Before scanning, eviction publishes
`future_max_evicted_seq_`; a concurrent snapshot at or below that boundary
observes it, advances the published sequence, and retries until its sequence
is above the boundary. That protocol keeps the filtered snapshot vector
complete.

## Exact Oldest-Snapshot Scan

Maintenance locks all shards in index order, reads the minimum sequence among
the shard heads (or the last published sequence when all shards are empty),
and sums the per-shard release counts.

The empty-list callback runs while every shard remains locked. A new snapshot
therefore cannot insert a sequence older than the returned value. The scan is
bounded by 16 shards and does not copy the unbounded snapshot lists. It is
separate from `GetAll()` so that the latter can continue copying one shard at a
time.

## Oldest-Snapshot Maintenance

`UpdateOldestSnapshotAndScheduleWork()` runs with the DB mutex held:

1. Stop sequence advancement if no snapshot has been released since the last
   refresh, even if normal writes advanced the empty-list fallback sequence.
2. Stop if the oldest sequence did not advance.
3. Compare the sequence with the DB-wide bottommost-file and standalone-range-
   deletion thresholds.
4. Reconcile all current column families only when a threshold is crossed.
5. Recompute the DB-wide thresholds after reconciliation.
6. Schedule background compaction if a file becomes eligible.

This distinction is required when the snapshot list is empty. Its exact oldest
value is the latest published sequence, which advances on every write. Without
the release counts, a flush could misinterpret ordinary write progress as a
snapshot release and schedule bottommost cleanup that the old release-driven
behavior would not have scheduled.

This restores the global threshold optimization: an exact shard scan is
bounded, and ordinary maintenance does not scan every column family until the
oldest sequence can change compaction eligibility.

SuperVersion installation also reconciles the installed column family against
the last processed sequence and refreshes the DB-wide thresholds by scanning
the current column-family thresholds. This is needed even when there is no new
release: the newly installed Version can contain older per-Version snapshot
state. The scan is DB-mutex-protected work proportional to the number of column
families, not to the number of live snapshots.

For each column family, reconciliation calls `UpdateOldestSnapshot()` only
when its per-Version oldest value advances. Any nonempty bottommost marked-file
list enqueues the column family; queue deduplication avoids duplicate work and
lets a failed or blocked pick be retried. A standalone range-tombstone file is
enqueued on every later per-Version advance at or above its threshold.
Universal compaction can consume the pending entry while deferring that file
for snapshot visibility, so later advances must make it eligible again.

Scheduling only places work on RocksDB's background queues. Compaction CPU,
compression, and file I/O do not run in the maintenance call.

### Maintenance Triggers

| Trigger | Purpose |
|---------|---------|
| SuperVersion installation | Reconcile newly installed Version state |
| Flush/compaction snapshot context | Run an exact oldest/release-count scan after loading snapshots |
| WritePrepared snapshot load | Run an exact oldest/release-count scan after loading snapshots |
| Trigger-compaction periodic task | Provide eventual progress on an otherwise idle DB |
| `WaitForCompact()` | Schedule eligible work before reporting idle |

Recovery can load snapshots without scheduling maintenance because the DB is
not yet serving normal concurrent work.

A release does not itself schedule a background job. Cleanup becomes eligible
at the next trigger in the table. With default statistics scheduling this is
normally within 600 seconds on an otherwise idle DB. If both statistics tasks
are disabled and no column family has a shorter time-based trigger, the
trigger-compaction fallback can delay maintenance by up to 12 hours. This is
an intentional eventual-progress tradeoff that keeps release out of the DB
mutex and background scheduler.

## External File Ingestion

Snapshot-consistent external ingestion needs to prevent a new snapshot between
its snapshot-set check and installation of the new Version. While holding the
DB mutex, ingestion increments the creation gate in every shard. A
DB-mutex-free creator that observes the gate retries under the DB mutex and
therefore waits for the serialized portion of ingestion.

Atomic multi-column-family ingestion can already hold an arbitrary number of
per-column-family ingestion locks. Its snapshot-existence check therefore
locks one gated shard at a time rather than adding all 16 shard locks to that
lock set. SuperVersion installation reconciles each new Version against the
last exact oldest sequence, then releases the ingestion locks before performing
one new exact all-shard scan. This keeps TSAN's held-lock set bounded without
weakening visibility: the gated snapshot set can only shrink during the
one-shard-at-a-time check, and stale oldest state is conservative for
compaction until the deferred exact scan completes.

`LogAndApply()` can temporarily release the DB mutex while writing the MANIFEST.
A creator that acquires the DB mutex during that interval may create a snapshot
even though the gate remains active. This preserves the established ingestion
serialization boundary. Ingestion removes the gate after installing or
aborting its Version changes.

## Lock Ordering

Shard mutexes are leaf locks relative to the DB mutex:

1. Code may hold the DB mutex and then acquire shard mutexes.
2. Code must not acquire the DB mutex while holding a shard mutex.
3. Multi-shard operations lock shards in increasing index order.

A creator that must fall back to the DB mutex first releases its shard. Snapshot
release destroys the object after releasing its shard and never enters the
DB-mutex domain.

## DB-Mutex Performance Review

The common regular create/release pair performs no work under the DB mutex.
Creation does allocation, wall-clock access, one shard lock, and constant-time
list insertion. Release takes its shard lock and performs constant-time list
removal. The ingestion-gated retry is exceptional and preserves required
serialization.

Maintenance adds 16 shard lock/unlock pairs and head comparisons to a DB-mutex
critical section. The global thresholds normally avoid iterating the DB's
column-family set. A SuperVersion installation still reconciles the Version
being installed.
When a global threshold is crossed, per-Version work can scan bottommost-file
metadata, call the configured clock, and, for user-defined timestamps, call
`Comparator::CompareTimestamp()` once per relevant bottommost file. Those
calls execute under the DB mutex. The path performs no file I/O.

`GetAll()` callers generally continue to hold the DB mutex while the merge
runs. Its 16 staging vectors can allocate independently and hold `N` sequence
numbers in aggregate; the result vector holds a second copy, so peak staging is
`O(N)`. The staging-vector descriptors use inline storage. Compared with a flat
list pass, the merge adds at most one heap pop and one heap push per copied
entry, each over no more than 16 cursors. The merge was not moved outside the DB
mutex because doing so would require pinning and revalidating the surrounding
flush, compaction, and transaction state. The extra complexity is not justified
without evidence that snapshot-list merging is a material critical-section
cost.

## Correctness Invariants

- Every live snapshot is linked in exactly one shard.
- A snapshot is released from the shard recorded at creation.
- Each shard remains ordered by sequence number.
- An exact maintenance scan is never newer than a live snapshot.
- Processed oldest sequences never move backward.
- Every release increments one shard's release count and is eventually
  observed by an exact maintenance scan.
- Ordinary write progress with an empty snapshot list does not masquerade as
  a snapshot release.
- `GetAll()` preserves ordering, deduplication, maximum-sequence filtering, and
  write-conflict-boundary semantics.
- Snapshot-support disabling is linearizable with creation.
- External ingestion preserves snapshot-consistency serialization.
- Statistics observe snapshot state but do not schedule background work.
- Stale maintenance state can retain extra data but cannot expose wrong data.

## Limitations

- The shard count is fixed at 16. Increasing it did not improve the measured
  supported configurations enough to justify public tuning and would add more
  DB-mutex work to all-shard scans.
- Shards are selected by CPU ID, not by load; several CPUs can map to one
  shard.
- Platforms without a physical CPU ID use per-thread fallback assignment.
- `GetAll()` merging remains inside callers' DB-mutex critical sections.
- Periodic maintenance provides eventual, not deadline-based, cleanup.

## Source Map

- `db/snapshot_impl.{h,cc}`: shard storage, creation, release, readers, and
  exact maintenance scans.
- `db/db_impl/db_impl.cc`: public snapshot operations, ingestion gating, and
  oldest-snapshot reconciliation.
- `db/db_impl/db_impl_compaction_flush.cc`: SuperVersion, flush/compaction,
  and `WaitForCompact()` triggers.
- `utilities/transactions/write_prepared_txn_db.cc`: WritePrepared snapshot
  loading and timestamped-snapshot serialization.
- `utilities/blob_db/blob_db_impl.cc`: exact oldest-snapshot visibility check.
- `db/db_etc2_test.cc` and `db/db_compaction_test.cc`: concurrency,
  publication, maintenance, and compaction regressions.
