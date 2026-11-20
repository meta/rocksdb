# SuperVersion

**Files:** `db/column_family.h`, `db/column_family.cc`

## Overview

SuperVersion is the key concurrency mechanism for readers. It bundles all three data sources -- active memtable, immutable memtables, and SST files (Version) -- into a single ref-counted object. Readers acquire a SuperVersion to get a consistent view of the database without holding the DB mutex.

## Structure

SuperVersion (defined in `db/column_family.h`) contains:

| Field | Description |
|-------|-------------|
| cfd | Pointer to owning ColumnFamilyData |
| mem | Active memtable (ReadOnlyMemTable*) |
| imm | Immutable memtable list (MemTableListVersion*) |
| current | SST file set (Version*) |
| mutable_cf_options | Snapshot of mutable CF options |
| version_number | Monotonically increasing SuperVersion number |
| write_stall_condition | Current write stall state |
| full_history_ts_low | UDT history cutoff (immutable once installed) |
| seqno_to_time_mapping | Shared mapping from sequence numbers to timestamps |

## Thread-Local Caching

To avoid mutex acquisition on every read, ColumnFamilyData caches the current SuperVersion in thread-local storage via local_sv_ (a ThreadLocalPtr):

**Acquisition** (GetThreadLocalSuperVersion()):

Step 1: Atomically swap the TLS slot with kSVInUse sentinel.

Step 2: If the previous value was a valid SuperVersion*, use it directly (fast path -- no mutex).

Step 3: If the previous value was kSVObsolete (background thread installed a new SuperVersion), acquire the mutex and get a fresh reference via GetReferencedSuperVersion() (slow path).

INVARIANT: Reentrant access (calling GetThreadLocalSuperVersion() while already holding a TLS SuperVersion) is prohibited. The code asserts that the swapped-out value is never kSVInUse; violating this triggers an assertion failure.

**Return** (ReturnThreadLocalSuperVersion()):

Step 1: Use CompareAndSwap to restore the SuperVersion pointer only if the slot still contains kSVInUse.

Step 2: If CAS succeeds, the SuperVersion is cached for reuse (no ref count change).

Step 3: If CAS fails (slot was changed to kSVObsolete by background thread), ReturnThreadLocalSuperVersion() returns false. The caller is responsible for unreffing the obsolete SuperVersion.

**Referenced acquisition** (GetReferencedSuperVersion()):

This method takes an extra ref on the SuperVersion before attempting to return it via ReturnThreadLocalSuperVersion(). If the return fails (CAS failed because slot was set to kSVObsolete), the extra ref is dropped via Unref(). This ensures a reader always gets a properly ref'd SuperVersion regardless of concurrent installations.

## Sentinel Values

| Sentinel | Value | Meaning |
|----------|-------|---------|
| kSVInUse | &SuperVersion::dummy | TLS slot is occupied by a reader |
| kSVObsolete | nullptr | Background thread has installed a new SuperVersion |

Using the address of a static member (dummy) for kSVInUse guarantees it cannot conflict with any valid SuperVersion pointer on any platform.

## Installation

ColumnFamilyData::InstallSuperVersion() is called whenever the memtable, immutable memtable list, or Version changes:

Step 1: Create a new SuperVersion and call Init() which refs mem, imm, current, and cfd.

Step 2: Replace super_version_ with the new one. Increment super_version_number_ atomically.

Step 3: Call ResetThreadLocalSuperVersions() to scrape all TLS slots to kSVObsolete, so readers will pick up the new SuperVersion on their next access. Any SuperVersions collected from TLS slots are unreffed (but the assert guarantees these are never the last ref, since the old super_version_ still holds one).

Step 4: Unref the old SuperVersion. If Unref() returns true (last reference released), call Cleanup() with the mutex held. The old SuperVersion is then pushed to sv_context->superversions_to_free for deferred deletion outside the mutex. If other readers still hold references, the old SuperVersion stays alive until they release.

Step 5: RecalculateWriteStallConditions() is called to determine the new write stall state. If the stall condition changes, a notification is pushed to sv_context.

INVARIANT: ResetThreadLocalSuperVersions() must run before the old SuperVersion is unreffed. This guarantees TLS never holds the last reference to a SuperVersion, preventing use-after-free.

## Cleanup and Deletion

SuperVersion::Cleanup() (requires mutex):
- Unrefs mem, imm, current, and cfd (via UnrefAndTryDelete())
- Collects memtables to delete in to_delete vector

After Cleanup(), the memtables in to_delete can be deleted outside the mutex.

The ref counting works as follows:
- Ref() increments refs atomically
- Unref() decrements refs atomically; returns true if count reached zero
- When Unref() returns true, the caller must call Cleanup() with the mutex held, then delete the SuperVersion

## SeqnoToTimeMapping

Each SuperVersion holds a shared_ptr<const SeqnoToTimeMapping> that maps sequence numbers to wall-clock times. This is shared between SuperVersions when the mapping doesn't change. ShareSeqnoToTimeMapping() allows the new SuperVersion or a FlushJob to share ownership.

## Performance Characteristics

The TLS caching makes the common read path (when no SuperVersion change has occurred) very fast -- just an atomic swap and comparison, with no mutex acquisition. The slow path (after a SuperVersion installation) is amortized across many reads since each thread only takes it once per installation.
