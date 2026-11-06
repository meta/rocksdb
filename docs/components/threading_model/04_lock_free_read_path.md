# Lock-Free Read Path

**Files:** `db/column_family.h`, `db/column_family.cc`, `util/thread_local.h`, `db/db_impl/db_impl.h`

## Overview

RocksDB achieves lock-free reads through ref-counted `SuperVersion` snapshots cached in thread-local storage. This eliminates mutex acquisition and atomic ref-count contention on the read hot path, allowing read throughput to scale linearly with CPU cores.

## SuperVersion Structure

`SuperVersion` (see `db/column_family.h`) bundles all the state a reader needs into a single ref-counted object:

- mem -- current mutable memtable (as ReadOnlyMemTable*)
- imm -- immutable memtable list version
- current -- current SST file version
- cfd -- pointer back to owning ColumnFamilyData (used in cleanup)
- mutable_cf_options -- snapshot of column family options
- version_number -- monotonically increasing version identifier
- write_stall_condition -- current write stall state
- full_history_ts_low -- effective UDT timestamp low bound
- seqno_to_time_mapping -- shared snapshot of sequence-to-time mapping

The `refs` field is a `std::atomic<uint32_t>` reference counter. `Ref()` increments it; `Unref()` decrements it and returns true if cleanup is needed (refs reaches zero).

## Thread-Local Cache Protocol

The thread-local cache eliminates atomic ref-count operations on the read path under steady state.

### Cache States

Two sentinel values (static members of `SuperVersion`) mark cache state:

| Sentinel | Value | Meaning |
|----------|-------|---------|
| `kSVInUse` | `&SuperVersion::dummy` | Thread is actively using a SuperVersion |
| `kSVObsolete` | `nullptr` | Cached version is outdated, refresh needed |

### Read Acquisition Flow

Step 1 -- Thread calls GetThreadLocalSuperVersion(). It atomically swaps the thread-local pointer with kSVInUse (atomic exchange).

Step 2 -- If the retrieved pointer is kSVObsolete, the cached version has been invalidated by a sweep. Acquire mutex_ and get the current SuperVersion via super_version_->Ref().

Step 3 -- If the retrieved pointer is a valid SuperVersion (not kSVObsolete), use it directly. No version number comparison is performed; staleness is detected solely by the sweep setting kSVObsolete.

Step 4 -- On return, the thread attempts to CAS kSVInUse back to the SuperVersion pointer. If the CAS fails (the slot now contains kSVObsolete from a sweep), the thread is responsible for Unref() on the old SuperVersion.

Note: Some callers use GetReferencedSuperVersion() instead, which takes an extra Ref() on the SuperVersion for longer-lived operations like iterator refresh. GetThreadLocalSuperVersion() is the fast path for point lookups that return the SuperVersion quickly.

### Performance Benefit

Under steady state (no flush/compaction completing), every read avoids the DB mutex and avoids SuperVersion ref-count contention, using only atomic thread-local operations (exchange on acquisition, CAS on return). This was a critical optimization that eliminated a scalability bottleneck at 8+ cores.

## SuperVersion Lifecycle

### Installation

When flush or compaction completes, a new SuperVersion is created and installed under mutex_:

Step 1 -- Create new SuperVersion, call Init() with new mem, imm, current
Step 2 -- Set cfd->super_version_ to the new SuperVersion
Step 3 -- Sweep thread-local storage via ResetThreadLocalSuperVersions() -- this must happen before Unref of the old SuperVersion to ensure thread-local storage never holds the last reference
Step 4 -- Unref() the old SuperVersion -- if no readers hold it, queue cleanup; otherwise it stays alive until the last reader releases
Step 5 -- Increment super_version_number_ and assign it to the new SuperVersion's version_number

### Sweep Protocol

After installing a new SuperVersion, a background sweep (CAS) is performed across all threads' local storage. For each thread:

- CAS the thread-local pointer to `kSVObsolete`
- If the old value was a valid SuperVersion pointer (not `kSVInUse` or `kSVObsolete`), call `Unref()` on it

This ensures that threads re-acquire the latest SuperVersion on their next read. A reader that finds `kSVInUse` was replaced with `kSVObsolete` during its read will handle cleanup when it tries to return the old SuperVersion.

### Memory Safety

Important: The old SuperVersion remains valid as long as any reader holds a reference. Even after a new SuperVersion is installed, readers using the old one continue to see a consistent point-in-time view of memtables and SST files. Cleanup happens asynchronously when the last reference is released.

## Design Rationale

The original design used `mutex_` for every `Get()` to increment per-memtable and per-SST reference counters. This was replaced in stages:

1. **SuperVersion consolidation**: Bundled all ref-counts into a single SuperVersion, reducing per-read mutex-protected operations from O(num_memtables + num_sst) to O(1).

2. **Atomic ref-counts**: Moved `Unref()` to use `std::atomic`, removing the need for mutex on the decrement path.

3. **Thread-local caching**: Eliminated the remaining mutex acquisition for `Ref()` by caching the SuperVersion in thread-local storage.

The sentinel-value approach (using address-of-dummy and nullptr) provides portable, lock-free thread-local state management without platform-specific primitives.
