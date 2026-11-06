# Thread Safety Reference

**Files:** include/rocksdb/db.h, include/rocksdb/iterator.h, include/rocksdb/write_batch.h, include/rocksdb/snapshot.h, db/db_impl/db_impl.h, db/column_family.h, db/write_thread.h, util/thread_local.h, util/threadpool_imp.cc

## Overview

This chapter documents the thread safety guarantees of RocksDB's public API and internal synchronization mechanisms.

## Thread-Safe Operations (No External Synchronization Required)

| Operation | Mechanism |
|-----------|-----------|
| `Get()` / `MultiGet()` | Ref-counted SuperVersion (lock-free under steady state) |
| Iterator creation | Snapshots SuperVersion at creation time |
| `Put()` / `Delete()` / `Merge()` / `Write()` | WriteThread coordinates all writers |
| `GetSnapshot()` / `ReleaseSnapshot()` | Protected by internal synchronization |
| `GetProperty()` / `GetIntProperty()` | Reads atomic counters or acquires `mutex_` internally |
| `GetReferencedSuperVersion()` | Thread-safe acquisition with ref-counting |
| `CompactRange()` | Acquires `mutex_` internally; multiple concurrent manual compactions are coordinated |
| `SetOptions()` / `SetDBOptions()` | Acquires `options_mutex_` and `mutex_` internally |
| `CreateColumnFamily()` | Acquires `options_mutex_` internally |
| `DropColumnFamily()` | Acquires `mutex_` internally |
| `DisableFileDeletions()` / `EnableFileDeletions()` | Uses ref-counted `disable_delete_obsolete_files_` counter |

## NOT Thread-Safe

| Operation | Reason |
|-----------|--------|
| `Close()` | Must not be called concurrently with other operations |
| `DestroyDB()` | Must not be called while DB is open |

## Internal Synchronization Mechanisms

### Atomic Variables

| Variable | Location | Purpose |
|----------|----------|---------|
| `shutting_down_` | `DBImpl` | Shutdown flag, checked by background threads |
| `SuperVersion::refs` | `db/column_family.h` | Reference counting for lock-free reads |
| `WriteGroup::running` | `db/write_thread.h` | Barrier counter for parallel memtable writes |
| `queue_len_` | `util/threadpool_imp.cc` | Thread pool queue depth |
| `newest_writer_` | `db/write_thread.h` | Lock-free writer queue head |
| `newest_memtable_writer_` | `db/write_thread.h` | Pipelined write memtable queue head |
| `pending_memtable_writes_` | `DBImpl` | Counter for unordered write in-flight memtable writes |

### Memory Ordering

| Pattern | Ordering | Examples |
|---------|----------|---------|
| Shutdown flag | `release` on store, `acquire` on load in scheduler; `relaxed` in some other checks | shutting_down_ in CancelAllBackgroundWork / MaybeScheduleFlushOrCompaction vs. DelayWrite |
| Statistics counters | `relaxed` | Thread pool queue length, internal stats |
| Reference counting | `relaxed` fetch_add for Ref(), default-ordered fetch_sub for Unref() | SuperVersion refs |
| Thread-local SuperVersion | Atomic exchange and CAS via ThreadLocalPtr | GetThreadLocalSuperVersion / ReturnThreadLocalSuperVersion |

### Lock-Free Data Structures

| Structure | Mechanism | Used By |
|-----------|-----------|---------|
| Writer linked list | CAS on `newest_writer_` atomic pointer | WriteThread group formation |
| SuperVersion thread-local cache | CAS with sentinel values (`kSVInUse`, `kSVObsolete`) | Lock-free read path |
| SkipList memtable | Lock-free concurrent insert | Concurrent memtable writes |

## Per-Thread State

RocksDB uses ThreadLocalPtr (see util/thread_local.h) for:
- SuperVersion caching (one cached SuperVersion per thread per column family)
- Thread status tracking (for GetThreadList() API when enable_thread_tracking is enabled)

Thread-local data is cleaned up automatically when a thread exits, via destructor callbacks registered with ThreadLocalPtr.

ZSTD decompression context caching uses CoreLocalArray (see util/core_local.h), which is per-core, not per-thread. This is distinct from ThreadLocalPtr and provides cache-friendly access patterns for compression/decompression work.

## Common Concurrency Patterns

### Group Commit

Multiple concurrent writers are serialized into groups where one leader performs the WAL write for the entire group. This amortizes the fsync cost across N writers while maintaining correct ordering. See chapter 2.

### Token-Based Flow Control

WriteController uses RAII tokens (StopWriteToken, DelayWriteToken, CompactionPressureToken) to manage write stalls. Acquiring a token increments a counter; destroying it decrements. This allows multiple column families to independently contribute to the global stall state. See chapter 6.

### Ref-Counted Lazy Cleanup

SuperVersion and Version objects use reference counting to defer cleanup until all readers are done. New versions are installed immediately under `mutex_`, but old versions remain valid until their ref count drops to zero. This provides readers with a consistent point-in-time view without holding any locks during the read.
