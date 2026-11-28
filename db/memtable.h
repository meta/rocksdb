//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/merge_helper.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "memory/allocator.h"
#include "memory/concurrent_arena.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "table/multiget_context.h"
#include "util/atomic.h"
#include "util/cast_util.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class BlobFetcher;
class BlobFilePartitionManager;
struct FlushJobInfo;
class MemTableIterator;
class MergeContext;
class SystemClock;

struct ImmutableMemTableOptions {
  explicit ImmutableMemTableOptions(const ImmutableOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);
  size_t arena_block_size;
  uint32_t memtable_prefix_bloom_bits;
  size_t memtable_huge_page_size;
  bool memtable_whole_key_filtering;
  bool inplace_update_support;
  size_t inplace_update_num_locks;
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);
  size_t max_successive_merges;
  bool strict_max_successive_merges;
  Statistics* statistics;
  MergeOperator* merge_operator;
  Logger* info_log;
  uint32_t protection_bytes_per_key;
  bool allow_data_in_errors;
  bool paranoid_memory_checks;
  bool memtable_verify_per_key_checksum_on_seek;
  bool memtable_batch_lookup_optimization;
};

// Batched counters to updated when inserting keys in one write batch.
// In post process of the write batch, these can be updated together.
// Only used in concurrent memtable insert case.
struct MemTablePostProcessInfo {
  // The live entries of one batch in a memtable with head marks (see
  // MemTableHeadMark): their number, the keys of the first kMaxLiveKeys of
  // them, their smallest and largest user key, and their largest sequence
  // number. The keys point into the memtable entries. BatchPostProcess()
  // checks each key of a small batch against the marks, so a small batch that
  // touches two queues does not lower the marks of the queues between them. A
  // larger batch checks only [min_live_key, max_live_key], so it can also
  // shorten a claim that lies in a gap between its keys. 8 is an estimate that
  // covers most small batches and keeps this allocation small.
  struct HeadMarkLiveKeys {
    static constexpr size_t kMaxLiveKeys = 8;
    size_t num_live_entries = 0;
    Slice live_keys[kMaxLiveKeys];
    Slice min_live_key;
    Slice max_live_key;
    SequenceNumber max_live_seq = 0;
  };
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
  uint64_t num_range_deletes = 0;
  // MemTable::Add() allocates it at the batch's first live entry, and only
  // when head marks are enabled for the memtable, so the concurrent write path
  // without head marks keeps this struct small.
  std::unique_ptr<HeadMarkLiveKeys> head_mark_live_keys;
};

// Head marks
// ==========
//
// Background. A queue stores its items under increasing keys. A consumer reads
// the oldest items with Seek(queue start) and then deletes them. The deletions
// stay in the memtable as point tombstones until flush, so every later scan
// from the queue start steps over a growing dead prefix. Read-path range
// tombstone conversion (see min_tombstones_for_range_conversion) inserts a
// range tombstone over such a run. The merging iterator then seeks the older
// levels over the run, but it still steps over every covered key of the
// memtable that holds the range tombstone.
//
// A head mark is a small record in a memtable M with two key ranges. The
// claimed range [start, end) is the range that writers check. A new claim is
// the dead run that a scan found: `start` is the seek key of the scan, and
// `end` is the end of the run (exclusive, usually the first live key). Writers
// compare their keys against the claimed range and shorten it. The usable range
// [start, usable_end) is the part that is proven deleted at sequence number
// `seq`, in M and in every older level. The publisher raises usable_end after
// it has checked the newer entries of M (see "Publish" below). Readers at `seq`
// or higher may skip only the usable range: a later scan whose seek key falls
// in it seeks M and all older levels to usable_end in one step (see
// MergingIterator::SeekImpl()). A claim alone proves nothing: a later scan can
// raise usable_end and seq and keep an older, longer claim. For example, in the
// test ExtensionInsideClaimChecksOnlyItsRun, the claim ends at "q0020", the
// mark is usable only up to "q0015", and "q0015" is live at the new seq. Terms
// are defined below.
//
// For example, a consumer scan Seek("q") at sequence number 100 steps over the
// tombstones "q0000" to "q0499" and returns "q0500". It publishes the mark
// ["q", "q0500") at 100. The next scan Seek("q") at 120 seeks M and the older
// levels to "q0500" at once.
//
// Terms:
// - Claimed range [start, end): the keys that the mark reserves for checking.
//   Writers compare their keys against it and shorten it. Only the usable
//   part is proven deleted.
// - Usable range [start, usable_end): the part that readers may skip. A new
//   claim is not usable at first. usable_end rises after the validation walk
//   (step 4 below) has checked the claim.
// - Live entry: a point entry that is not a deletion (Put, Merge, PutEntity,
//   and all other types except Deletion, SingleDeletion,
//   DeletionWithTimestamp and RangeDeletion).
//
// Data model. All objects live in the memtable arena. A memtable has one table
// per direction, forward and reverse (see "Reverse marks" below). The forward
// table uses the user comparator's order, and the reverse table the opposite
// order. The protocol text below uses the order of the mark's table: a range
// [start, end) goes from start toward end in that order, `end` and
// `usable_end` are excluded, and `start` is included unless `start_exclusive`
// is set. The examples under "Reverse marks" use user-key order instead, and
// write each end of a range with "(" or "]" to show if it is included.
//
//   MemTable::head_marks_[dir] --> HeadMarkTable (immutable)
//                                    min_start: the start of the first claim
//                                    cells[0..n), sorted by start, n <= 16
//                                       |
//                                       v
//                                  HeadMarkCell (one per claim, never moves)
//                                    start
//                                    mark --> MemTableHeadMark (immutable)
//                                               start, end, usable_end, seq,
//                                               ingest_version, lowered,
//                                               start_exclusive
//
// A change to one mark stores a new MemTableHeadMark into the same cell. A new
// table is published only when a cell is added or removed.
// The table points to the cells, so publishing a new table never loses a
// change that a writer made to a cell. The claims of one table never overlap.
// Every change takes MemTable::head_mark_mu_, except the clear, which drops
// both tables (see "File ingestion" below).
//
// Write path (MemTable::Add() and BatchPostProcess()). A live entry whose key
// falls in a claimed range lowers that mark before the writer's sequence number
// is published: the writer stores a new mark with the same start and seq, end =
// key, and usable_end = min(usable_end, key). Concurrent memtable writers
// lower marks in BatchPostProcess(). A batch with at most
// MemTablePostProcessInfo::HeadMarkLiveKeys::kMaxLiveKeys (8) live entries
// lowers the mark of each live key, as a single write does. A larger batch
// lowers every claim that overlaps [smallest, largest live key] of the batch to
// end at max(start, smallest live key), so it can also shorten a claim that
// lies between two of its keys and holds none of them. Deletions never change a
// mark, because a deleted key stays deleted. Each insert increments
// num_entries_ (acq_rel). An insert of a live entry raises max_inserted_seq_
// before the increment and loads head_marks_ after it. Every claim lies in the
// hull [min_start, end of the last claim), because claims are sorted and never
// overlap; the hull can also hold gaps between claims. A key outside the hull
// costs one or two key compares; a key inside the hull takes one binary
// search, and only a key inside a claim takes the mutex.
//
// Publish (MemTable::PublishHeadMark(), called by DBIter when the first entries
// after its Seek() form a run of point tombstones):
// 1. The scan at sequence number S found the dead run [start, end). A new
//    mark needs a run of at least min_tombstones_for_range_conversion
//    tombstones. A scan sent by a lowered mark may extend it with a shorter
//    run (see MemTableHeadMark::lowered).
// 2. Under head_mark_mu_, the scan stores its claim. If M became immutable, the
//    publish is refused, because the memtable list counts M's memory at that
//    point. If a mark E of M sent the scan, that is, E moved the scan's seek in
//    M, the scan extends E. Only a mark of the active memtable can send a scan:
//    DBIter passes only the active memtable as mark_mem to SeekWithHeadMarks(),
//    so only a mark of that memtable is reported as the sender. A mark of an
//    immutable memtable that moved the seek of that memtable and of the older
//    levels sends nothing, so the scan's claim starts at its seek key. The new
//    end is the end of the scan's run, capped at the start of the next claim.
//    If the new end is at or before E.usable_end, there is nothing to extend.
//    Otherwise the claim keeps E's start, with E's start_exclusive, and E's
//    usable_end: it goes from E.start to the new end (excluded), with
//    usable_end = E.usable_end and seq = S, and only step 5 raises its
//    usable_end. If E already claims up to the new end, the claim is E itself
//    and nothing is stored, so a scan that repeats a seek allocates nothing
//    until its walk gets further. If E is no longer in its cell (a writer
//    lowered it, another scan extended it, or an ingestion cleared it), the
//    publish is refused, because E moved the scan's seek. If no mark sent the
//    scan, the claim is a new mark with usable_end = start, if it overlaps no
//    other claim and the table has room.
// 3. The scan adds 0 to num_entries_ with acq_rel ordering. The count does
//    not change, but the add puts the scan into the same modification order
//    of num_entries_ as the writers' increments (the handshake below).
// 4. If max_inserted_seq_ > S, the validation walk visits M's entries in
//    [usable_end, end). If the claim is E itself, the walk ends at the end of
//    the scan's run instead, because the scan saw only that part dead, and
//    E's own walk may have stopped before a live entry after it. The walk
//    stops at the first live entry with a sequence number above S, or after
//    kMaxHeadMarkWalk entries.
// 5. If the walk got past usable_end, M is still mutable, and the cell still
//    holds the claim of step 2 (checked under head_mark_mu_), the scan raises
//    usable_end to the end of the checked part. The scan allocates the raised
//    mark before it takes head_mark_mu_, so a writer that lowers a mark waits
//    only for the check and the store.
//
// Handshake. Take a live entry with a sequence number above S in the claimed
// range. Its writer increments num_entries_ after the insert, and the scan adds
// 0 to it after it stores its claim (step 3), both with acq_rel ordering. The
// order of the two updates decides who handles the entry. If the writer's
// update comes first, the writer has already inserted the entry and raised
// max_inserted_seq_, so the walk can see both, and it stops at or before the
// entry (at the entry, at an earlier newer live entry, or at the walk cap). If
// the scan's update comes first, the claim is already stored, so the writer
// sees it after its increment and lowers the mark before its sequence number is
// published. Every reader that can see the entry then also sees the lowered
// mark. A large concurrent batch may lower more than needed (see "Write path").
// The older levels hold no entry with a sequence number above S, because
// DBIter::MaybePublishHeadMark() publishes only if S >= the earliest sequence
// number of the active memtable. Only file ingestion (below) breaks this.
//
//   scan at S (publish)                 writer of a live entry above S
//   ----------------------------------  ----------------------------------
//   2. store the claim                  insert the entry, raise
//                                       max_inserted_seq_
//   3. num_entries_ += 0 (acq_rel)      num_entries_ += 1 (acq_rel)
//   4. walk [usable_end, end)           load head_marks_, lower the claim
//                                       if it holds the key
//   5. raise usable_end if the cell     publish the sequence number
//      still holds the claim
//
// The two updates of num_entries_ are in one modification order. If the
// writer's update comes first, the walk of step 4 sees the entry. Otherwise
// the writer sees the claim and lowers it.
//
// Extension is safe: [start, E.usable_end) was deleted at E.seq, and every
// writer after E would have lowered E, so the range is still deleted at S.
//
// File ingestion can add keys to the older levels at any sequence number.
// Ingestion holds the column family's ingest_sst_lock as a reader. Before it
// installs the new Version, it clears the marks of the active memtable and of
// the immutable memtables in the column family's current list. An older
// iterator can still hold a memtable that has left that list and use its marks,
// but such an iterator also keeps its old Version, which does not have the new
// files. After the install, the ingestion records the Version's number and the
// ingested sequence number (see HeadMarkIngestState). PublishHeadMark() tries
// to take ingest_sst_lock as a writer (TryWriteLock) and refuses the publish if
// it cannot. It also refuses a scan whose Version predates the ingestion or
// whose sequence number is below the ingested one. The publish holds the lock
// only while it checks this state and stores its claim (step 2), and releases
// it before the validation walk. An ingestion that clears the marks after that
// removes the claim from the table, so step 5 finds no cell holding the claim
// and does not raise it. The clear runs under the DB mutex, and it stores a
// null table for each direction without taking head_mark_mu_. While the
// ingestion holds ingest_sst_lock, no scan can store a claim, so no other
// thread publishes a table. A step 5 raise and a writer's lowering can still
// run: an ingestion stops the writers, but DeleteFilesInRanges() (below) does
// not. Each of them stores only into a cell of the table that it loaded, and no
// reader can reach that table after the clear. So the clear never waits for
// head_mark_mu_, whose holder can be a lowering that allocates. A column family
// whose active memtables never had marks enabled skips the clear (see
// ColumnFamilyData::may_have_head_marks_).
//
// DeleteFilesInRanges() can remove an SST file whose range tombstone hides an
// older key in a lower level. That key then becomes visible again at every
// sequence number, also below S, but no memtable write lowered a mark over it.
// So DeleteFilesInRanges() takes the same steps as an ingestion, in the same
// order: it holds ingest_sst_lock as a reader, clears the marks before it
// installs the new Version, and records the Version's number. It does not
// change the recorded sequence number. In the rest of this comment and in
// HeadMarkIngestState, an ingestion includes such a call.
//
// An iterator can read at or above an ingested file's sequence number with a
// Version that does not have the file. A read references its Version before it
// reads the sequence number; DBImpl then gives it the current SuperVersion if
// it misses an ingestion (see DBImpl::SuperVersionMissesIngestion()), but an
// in-place Iterator::Refresh() keeps its Version and moves only its sequence
// number. PublishHeadMark() refuses such a scan, because its Version number is
// below the recorded one. A mark that a newer scan publishes must not move such
// a reader's seeks either: the reader may still see a key that the ingested
// file deletes. So every mark keeps the recorded Version number of the latest
// ingestion at its publish (ingest_version), and a reader uses the mark only if
// its own Version number is at least that. A Version number also grows with
// every flush and compaction, so the reader's Version need not be the
// publisher's Version.
//
// Readers (MergingIterator::SeekImpl()) use a mark only in a seek from DBIter,
// only if mark.seq <= the read sequence number, only if mark.ingest_version
// <= the reader's Version number, and only its usable range.
//
// Reverse marks. A reverse scan that starts with SeekForPrev(t) and steps
// backward over tombstones down to the live key K publishes a reverse mark:
// every key in (K, t] is deleted. If a prefix_same_as_start scan instead stops
// at a key outside its prefix, K is the lowest tombstone of the run, because a
// prefix filter may have hidden keys below it (see
// DBIter::MaybePublishHeadMark()). SeekToLast() with iterate_upper_bound U, and
// SeekForPrev() at or above U, are seeks that exclude U, so they publish (K,
// U). A reverse mark is stored as a forward mark in descending key order: start
// = t or U, end = K, and start_exclusive tells whether start is claimed. Each
// direction has its own table (MemTable::head_marks_[HeadMarkDirection]), and
// every rule above holds with the order reversed: the reverse table swaps the
// arguments of the user comparator, so its claims are sorted by start in
// descending user order. In user order, a reverse claim is (end, start] and its
// usable range is (usable_end, start]; both leave out start if start_exclusive
// is set. The write path calls a shortening "lowering" because it works in
// table order. In user order, a live write inside a reverse claim raises the
// claim's lower bound `end` (and usable_end if needed) to the written key: for
// example, a put at "q0015" shrinks the claim ("q0009", "q0019"] to ("q0015",
// "q0019"]. The validation walk goes down in user order from usable_end. A
// reverse seek (MergingIterator::SeekForPrevImpl()) at a key in the usable
// range positions this level and older levels at the last entry at or before
// usable_end. It can land on usable_end itself, which is outside the usable
// range and can be live.
//
// Limits. A memtable holds at most kMaxHeadMarks (16) marks per direction.
// Replaced marks and old tables stay in the arena until the memtable is freed,
// so the marks of a memtable have a byte budget (see
// MemTable::head_mark_bytes_). Marks are made only by scans that start with
// Seek(), SeekForPrev(), or SeekToLast() with iterate_upper_bound. Marks are
// enabled for a memtable only if min_tombstones_for_range_conversion > 0 when
// it is created, because only conversion scans publish them. Marks are off for
// user-defined timestamps, inplace_update_support, unordered_write,
// two_write_queues, FIFO compaction and memtables that are not skip lists (see
// MemTable::head_marks_enabled_). Scans prepared with scan options (MultiScan)
// neither use nor publish marks. A compaction filter that returns
// kRemoveAndSkipUntil drops every entry that it skips, also a point tombstone,
// so an older value of that key in a lower level can become visible again.
// Compaction does not clear the marks, so after such a skip a scan that uses a
// mark can miss a key that Get() returns. A conversion never covers the keys
// that a mark skipped (see DBIter::MaybeInsertRangeTombstone()), so the miss
// stays in reads and never becomes a stored deletion. kRemove, kPurge and
// kChangeValue never drop a tombstone, so they do not have this effect.
struct MemTableHeadMark {
  Slice start;
  Slice end;
  Slice usable_end;
  SequenceNumber seq;
  // HeadMarkIngestState::version_number when the mark was published (0 if the
  // column family had no ingestion). A lowered or raised mark keeps it: any
  // later ingestion clears the mark instead.
  uint64_t ingest_version;
  // True if a writer lowered `end`. A writer sets it when it shortens the
  // claim, and a scan that replaces the mark (an extension or a raise) stores a
  // new mark with lowered false. A scan sent by a lowered mark may extend it
  // with fewer tombstones than min_tombstones_for_range_conversion, because the
  // keys after the lowered end are often covered by a converted range
  // tombstone, and DBIter does not count covered keys. A writer that shortens
  // the claim to its start leaves the usable range empty, so the mark moves no
  // seek.
  bool lowered;
  // True if `start` itself is not claimed. A new reverse mark has it when its
  // seek was clamped to iterate_upper_bound, which the seek excludes: a
  // SeekToLast() with iterate_upper_bound, or a SeekForPrev() at or above
  // iterate_upper_bound (see "Reverse marks"). An extension keeps the
  // sender's start and start_exclusive.
  bool start_exclusive = false;
};

// The key order of a head mark table (see "Reverse marks" at
// MemTableHeadMark).
enum class HeadMarkDirection : uint8_t { kForward = 0, kReverse = 1 };

// The ingestion state of a column family that PublishHeadMark() checks. Owned
// by ColumnFamilyData. Written under the DB mutex while the ingestion holds
// ingest_sst_lock (see MemTableHeadMark).
struct HeadMarkIngestState {
  // The number of the Version that installed the latest ingestion or
  // DeleteFilesInRanges() call. This differs from
  // ColumnFamilyData::GetLastIngestVersionNumber(), which only an ingestion
  // sets and which reads use to get a SuperVersion that has the ingestion.
  Atomic<uint64_t> version_number{0};
  // The largest sequence number assigned to an ingested file.
  Atomic<SequenceNumber> seqno{0};
};

// The result of PublishHeadMark(), which tests read through a sync point.
// It tells how the publish handled the scan's run, not how much became
// usable: tests check usable_end, or seek again, for that.
enum class HeadMarkPublishResult {
  kDisabled,
  // A new claim was added to the table.
  kPublished,
  // An extension of the sender mark was accepted: its claim was replaced
  // with a longer one, or the run ends inside the claim and only its usable
  // end can grow.
  kExtended,
  // ingest_sst_lock is busy (a file ingestion, or another scan's publish or
  // conversion, holds it), or an ingestion happened after the scan's Version
  // or sequence number.
  kRefusedIngest,
  // The memtable became immutable after the scan started.
  kRefusedImmutable,
  // The sender mark is no longer in its cell.
  kRefusedChanged,
  // The run, capped at the start of the next claim, does not reach past the
  // sender's usable end.
  kNothingToExtend,
  // The run has fewer than min_tombstones_for_range_conversion tombstones, and
  // no lowered sender mark allows a short run.
  kRefusedShort,
  kRefusedOverlap,
  kRefusedFull,
  // The new mark or the extension does not fit in the byte budget of the
  // memtable's marks (see MemTable::ReserveHeadMarkBytes()).
  kRefusedBudget,
};

using MultiGetRange = MultiGetContext::Range;

// For each CF, rocksdb maintains an active memtable that accept writes,
// and zero or more sealed memtables that we call immutable memtables.
// This interface contains all methods required for immutable memtables.
// MemTable class inherit from `ReadOnlyMemTable` and implements additional
// methods required for active memtables.
// Immutable memtable list (MemTableList) maintains a list of ReadOnlyMemTable
// objects. This interface enables feature like direct ingestion of an
// immutable memtable with custom implementation, bypassing memtable writes.
//
// Note: Many of the methods in this class have comments indicating that
// external synchronization is required as these methods are not thread-safe.
// It is up to higher layers of code to decide how to prevent concurrent
// invocation of these methods. This is usually done by acquiring either
// the db mutex or the single writer thread.
//
// Some of these methods are documented to only require external
// synchronization if this memtable is immutable. Calling MarkImmutable() is
// not sufficient to guarantee immutability.  It is up to higher layers of
// code to determine if this MemTable can still be modified by other threads.
// Eg: The Superversion stores a pointer to the current MemTable (that can
// be modified) and a separate list of the MemTables that can no longer be
// written to (aka the 'immutable memtables').
//
// MemTables are reference counted. The initial reference count
// is zero and the caller must call Ref() at least once.
class ReadOnlyMemTable {
 public:
  // Do not delete this MemTable unless Unref() indicates it not in use.
  virtual ~ReadOnlyMemTable() = default;

  virtual const char* Name() const = 0;

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual size_t ApproximateMemoryUsage() = 0;

  // used by MemTableListVersion::MemoryAllocatedBytesExcludingLast
  virtual size_t MemoryAllocatedBytes() const = 0;

  // Returns memory charged to this memtable's WriteBufferManager. Memtable
  // implementations that do not allocate through a WBM return zero.
  virtual size_t WBMTrackedMemoryUsage() const = 0;

  // Returns a vector of unique random memtable entries of size 'sample_size'.
  //
  // Note: the entries are stored in the unordered_set as length-prefixed keys,
  //       hence their representation in the set as "const char*".
  // Note2: the size of the output set 'entries' is not enforced to be strictly
  //        equal to 'target_sample_size'. Its final size might be slightly
  //        greater or slightly less than 'target_sample_size'
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  // REQUIRES: SkipList memtable representation. This function is not
  // implemented for any other type of memtable representation (vectorrep,
  // hashskiplist,...).
  virtual void UniqueRandomSample(const uint64_t& target_sample_size,
                                  std::unordered_set<const char*>* entries) = 0;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  // seqno_to_time_mapping: it's used to support return write unix time for the
  // data, currently only needed for iterators serving user reads.
  virtual InternalIterator* NewIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, bool for_flush) = 0;

  // Returns an iterator that wraps a MemTableIterator and logically strips the
  // user-defined timestamp of each key. This API is only used by flush when
  // user-defined timestamps in MemTable only feature is enabled.
  virtual InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, size_t ts_sz) = 0;

  // Returns an iterator that yields the range tombstones of the memtable.
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.
  // @param immutable_memtable Whether this memtable is an immutable memtable.
  // This information is not stored in memtable itself, so it needs to be
  // specified by the caller. This flag is used internally to decide whether a
  // cached fragmented range tombstone list can be returned. This cached version
  // is constructed when a memtable becomes immutable. Setting the flag to false
  // will always yield correct result, but may incur performance penalty as it
  // always creates a new fragmented range tombstone list.
  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable) = 0;

  // Returns an iterator that yields the range tombstones of the memtable and
  // logically strips the user-defined timestamp of each key (including start
  // key, and end key). This API is only used by flush when user-defined
  // timestamps in MemTable only feature is enabled.
  virtual FragmentedRangeTombstoneIterator*
  NewTimestampStrippingRangeTombstoneIterator(const ReadOptions& read_options,
                                              SequenceNumber read_seq,
                                              size_t ts_sz) = 0;

  // Used to get value associated with `key`, or Merge operands associated
  // with key, or get the latest sequence number of `key` (e.g. transaction
  // conflict checking).
  //
  // Keys are considered if they are no smaller than the parameter `key` in
  // the order defined by comparator and share the save user key with `key`.
  //
  // If do_merge = true the default behavior which is Get value for key is
  // executed. Expected behavior is described right below.
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store NotFound() in *status and
  // return true.
  // If memtable contains Merge operation as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operand to *operands.
  //   store MergeInProgress in s, and return false.
  // If an unexpected error or corruption occurs, store Corruption() or other
  // error in *status and return true.
  // Else, return false.
  // If any operation was found, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  // On success, *s may be set to OK, NotFound, or MergeInProgress.  Any other
  // status returned indicates a corruption or other unexpected error.
  //
  // If do_merge = false then any Merge Operands encountered for key are simply
  // stored in merge_context.operands_list and never actually merged to get a
  // final value. The raw Merge Operands are eventually returned to the user.
  // @param value If not null and memtable contains a value for key, `value`
  // will be set to the result value.
  // @param column If not null and memtable contains a value/WideColumn for key,
  // `column` will be set to the result value/WideColumn.
  // Note: only one of `value` and `column` can be non-nullptr.
  // To only query for key existence or the latest sequence number of a key,
  // `value` and `column` can be both nullptr. In this case, returned status can
  // be OK, NotFound or MergeInProgress if a key is found.
  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  virtual bool Get(const LookupKey& key, std::string* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback = nullptr,
                   bool* is_blob_index = nullptr, bool do_merge = true,
                   const BlobFetcher* blob_fetcher = nullptr) = 0;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true, const BlobFetcher* blob_fetcher = nullptr) {
    SequenceNumber seq;
    return Get(key, value, columns, timestamp, s, merge_context,
               max_covering_tombstone_seq, &seq, read_opts, immutable_memtable,
               callback, is_blob_index, do_merge, blob_fetcher);
  }

  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  virtual void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable,
                        const BlobFetcher* blob_fetcher = nullptr) = 0;

  // Get total number of entries in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumEntries() const = 0;

  // Get total number of point deletes in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumDeletion() const = 0;

  // Get total number of range deletions in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumRangeDeletion() const = 0;

  virtual uint64_t GetDataSize() const = 0;

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual SequenceNumber GetFirstSequenceNumber() = 0;

  // Returns if there is no entry inserted to the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual bool IsEmpty() const = 0;

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  virtual SequenceNumber GetEarliestSequenceNumber() = 0;

  virtual uint64_t GetMinLogContainingPrepSection() = 0;

  // Notify the underlying storage that no more items will be added.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  // After MarkImmutable() is called, you should not attempt to
  // write anything to this MemTable().  (Ie. do not call Add() or Update()).
  virtual void MarkImmutable() = 0;

  // Notify the underlying storage that all data it contained has been
  // persisted.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  virtual void MarkFlushed() = 0;

  struct MemTableStats {
    uint64_t size;
    uint64_t count;
  };
  virtual MemTableStats ApproximateStats(const Slice& start_ikey,
                                         const Slice& end_ikey) = 0;

  virtual const InternalKeyComparator& GetInternalKeyComparator() const = 0;

  virtual uint64_t ApproximateOldestKeyTime() const = 0;

  // Inserts a range tombstone [start_key, end_key) that is logically redundant
  // -- it is derived from existing point tombstones observed during iteration
  // and does not delete any data that isn't already deleted. This is a
  // best-effort optimization. It allows future reads to skip iterating over
  // continuous single deletion tombstones.
  //
  // Adding a range tombstone may fail if
  // - memtable switches to immutable state
  // - a range tombstone with the same key+seq already exists (duplicate insert)
  // - the per-memtable ingest seqno barrier already exceeds `seq` (an
  //   ingestion has committed an L0 file at a seq that this converted
  //   tombstone would shadow) or an ingestion is in progress.
  // - `version_number`, the number of the Version the converting iterator
  //   reads, is below the per-memtable ingest version barrier (the iterator
  //   cannot see a file ingested while this memtable was active).
  // Returns true if the range tombstone was inserted, false if skipped.
  virtual bool AddLogicallyRedundantRangeTombstone(
      SequenceNumber /*seq*/, const Slice& /*start_key*/,
      const Slice& /*end_key*/, uint64_t /*version_number*/,
      port::RWMutex& /*ingest_sst_lock*/) {
    return false;
  }

  // Head marks (see MemTableHeadMark). The defaults are for memtables without
  // marks.
  virtual bool HeadMarksEnabled() const { return false; }

  // Returns the mark of direction `dir` that a seek at `user_key` with read
  // sequence number `read_seq` may use, or nullptr. The mark's seq must be at
  // most `read_seq`, its ingest_version must be at most `read_version` (the
  // number of the reader's Version), and `user_key` must lie in [start,
  // usable_end) in the order of `dir`. `key_included` tells if the seek
  // includes `user_key` itself. One case needs it: if `user_key` is an
  // exclusive start, the mark does not claim it, so only a seek that excludes
  // it may use the mark (and only if usable_end is past start).
  // MergingIterator::SeekForPrevOverHeadMark() derives `key_included` from the
  // sequence number of its search key.
  virtual const MemTableHeadMark* FindUsableHeadMark(
      HeadMarkDirection /*dir*/, const Slice& /*user_key*/,
      bool /*key_included*/, SequenceNumber /*read_seq*/,
      uint64_t /*read_version*/) const {
    return nullptr;
  }

  // Publishes the dead run from `start` to `end` (exclusive) in direction
  // `dir` that a scan at sequence number `seq` found right after its seek to
  // `start`. `start_exclusive` tells that `start` itself is not claimed. The
  // scan reads Version `version_number`. `sender` is the mark of this memtable
  // that moved the scan's seek, or nullptr. `run_is_long` tells if the run has
  // at least min_tombstones_for_range_conversion tombstones.
  virtual HeadMarkPublishResult PublishHeadMark(
      HeadMarkDirection /*dir*/, const Slice& /*start*/,
      bool /*start_exclusive*/, const Slice& /*end*/, SequenceNumber /*seq*/,
      const MemTableHeadMark* /*sender*/, bool /*run_is_long*/,
      uint64_t /*version_number*/, const HeadMarkIngestState& /*ingest*/,
      port::RWMutex& /*ingest_sst_lock*/) {
    return HeadMarkPublishResult::kDisabled;
  }

  // Removes all marks. Called by file ingestion and DeleteFilesInRanges()
  // under the DB mutex while they hold ingest_sst_lock as a reader, which
  // keeps every publish out. It does not take head_mark_mu_ (see "File
  // ingestion" at MemTableHeadMark).
  virtual void ClearHeadMarks() {}

  // Returns whether a fragmented range tombstone list is already constructed
  // for this memtable. It should be constructed right before a memtable is
  // added to an immutable memtable list. Note that if a memtable does not have
  // any range tombstone, then no range tombstone list will ever be constructed
  // and true is returned in that case.
  virtual bool IsFragmentedRangeTombstonesConstructed() const = 0;

  // Get the newest user-defined timestamp contained in this MemTable. Check
  // `newest_udt_` for what newer means. This method should only be invoked for
  // an MemTable that has enabled user-defined timestamp feature and set
  // `persist_user_defined_timestamps` to false. The tracked newest UDT will be
  // used by flush job in the background to help check the MemTable's
  // eligibility for Flush.
  virtual Slice GetNewestUDT() const = 0;

  // Increase reference count.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  ReadOnlyMemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      ReleaseProtectedSealedBlobFiles();
      return this;
    }
    return nullptr;
  }

  // Registers sealed direct-write blob files that this memtable can still
  // resolve through lazy blob indexes. The protection lasts until the
  // memtable's final Unref(). The manager handle is shared here because
  // immutable memtables can outlive the ColumnFamilyData that created them.
  // REQUIRES: external synchronization to prevent simultaneous operations on
  // the same MemTable.
  void ProtectSealedBlobFiles(
      const std::shared_ptr<BlobFilePartitionManager>& blob_partition_manager,
      const std::vector<uint64_t>& file_numbers);

  // Returns the edits area that is needed for flushing the memtable
  VersionEdit* GetEdits() { return &edit_; }

  // Returns the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  uint64_t GetNextLogNumber() const { return mem_next_walfile_number_; }

  // Sets the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void SetNextLogNumber(uint64_t num) { mem_next_walfile_number_ = num; }

  // REQUIRES: db_mutex held.
  void SetID(uint64_t id) { id_ = id; }

  uint64_t GetID() const { return id_; }

  void SetFlushCompleted(bool completed) { flush_completed_ = completed; }

  uint64_t GetFileNumber() const { return file_number_; }

  void SetFileNumber(uint64_t file_num) { file_number_ = file_num; }

  void SetFlushInProgress(bool in_progress) {
    flush_in_progress_ = in_progress;
  }

  void SetFlushJobInfo(std::unique_ptr<FlushJobInfo>&& info) {
    flush_job_info_ = std::move(info);
  }

  std::unique_ptr<FlushJobInfo> ReleaseFlushJobInfo() {
    return std::move(flush_job_info_);
  }

  static void HandleTypeValue(
      const Slice& lookup_user_key, const Slice& value, bool value_pinned,
      bool do_merge, bool merge_in_progress, MergeContext* merge_context,
      const MergeOperator* merge_operator, SystemClock* clock,
      Statistics* statistics, Logger* info_log, Status* s,
      std::string* out_value, PinnableWideColumns* out_columns,
      bool* is_blob_index) {
    *s = Status::OK();

    if (!do_merge) {
      // Preserve the value with the goal of returning it as part of
      // raw merge operands to the user
      // TODO(yanqin) update MergeContext so that timestamps information
      // can also be retained.
      merge_context->PushOperand(value, value_pinned);
    } else if (merge_in_progress) {
      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its
      // value.
      if (out_value || out_columns) {
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kPlainBaseValue,
            value, merge_context->GetOperands(), info_log, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      }
    } else if (out_value) {
      out_value->assign(value.data(), value.size());
    } else if (out_columns) {
      out_columns->SetPlainValue(value);
    }

    if (is_blob_index) {
      *is_blob_index = false;
    }
  }

  static void HandleTypeDeletion(
      const Slice& lookup_user_key, bool merge_in_progress,
      MergeContext* merge_context, const MergeOperator* merge_operator,
      SystemClock* clock, Statistics* statistics, Logger* logger, Status* s,
      std::string* out_value, PinnableWideColumns* out_columns) {
    if (merge_in_progress) {
      if (out_value || out_columns) {
        // `op_failure_scope` (an output parameter) is not provided (set to
        // nullptr) since a failure must be propagated regardless of its
        // value.
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kNoBaseValue,
            merge_context->GetOperands(), logger, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      } else {
        // We have found a final value (a base deletion) and have newer
        // merge operands that we do not intend to merge. Nothing remains
        // to be done so assign status to OK.
        *s = Status::OK();
      }
    } else {
      *s = Status::NotFound();
    }
  }

  // Returns if a final value is found.
  static bool HandleTypeMerge(const Slice& lookup_user_key, const Slice& value,
                              bool value_pinned, bool do_merge,
                              MergeContext* merge_context,
                              const MergeOperator* merge_operator,
                              SystemClock* clock, Statistics* statistics,
                              Logger* logger, Status* s, std::string* out_value,
                              PinnableWideColumns* out_columns) {
    if (!merge_operator) {
      *s = Status::InvalidArgument(
          "merge_operator is not properly initialized.");
      // Normally we continue the loop (return true) when we see a merge
      // operand.  But in case of an error, we should stop the loop
      // immediately and pretend we have found the value to stop further
      // seek.  Otherwise, the later call will override this error status.
      return true;
    }
    merge_context->PushOperand(value, value_pinned /* operand_pinned */);
    PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

    if (do_merge && merge_operator->ShouldMerge(
                        merge_context->GetOperandsDirectionBackward())) {
      if (out_value || out_columns) {
        // `op_failure_scope` (an output parameter) is not provided (set to
        // nullptr) since a failure must be propagated regardless of its
        // value.
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kNoBaseValue,
            merge_context->GetOperands(), logger, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      }
      return true;
    }
    if (merge_context->get_merge_operands_options != nullptr &&
        merge_context->get_merge_operands_options->continue_cb != nullptr &&
        !merge_context->get_merge_operands_options->continue_cb(value)) {
      // We were told not to continue. `status` may be MergeInProress(),
      // overwrite to signal the end of successful get. This status
      // will be checked at the end of GetImpl().
      *s = Status::OK();
      return true;
    }

    // no final value found yet
    return false;
  }

  void MarkForFlush() { marked_for_flush_.StoreRelaxed(true); }

  bool IsMarkedForFlush() const { return marked_for_flush_.LoadRelaxed(); }

 protected:
  friend class MemTableList;

  int refs_{0};

  // These are used to manage memtable flushes to storage
  bool flush_in_progress_{false};  // started the flush
  bool flush_completed_{false};    // finished the flush
  uint64_t file_number_{0};

  // The updates to be applied to the transaction log when this
  // memtable is flushed to storage.
  VersionEdit edit_;

  // The log files earlier than this number can be deleted.
  uint64_t mem_next_walfile_number_{0};

  // Memtable id to track flush.
  uint64_t id_ = 0;

  // Sequence number of the atomic flush that is responsible for this memtable.
  // The sequence number of atomic flush is a seq, such that no writes with
  // sequence numbers greater than or equal to seq are flushed, while all
  // writes with sequence number smaller than seq are flushed.
  SequenceNumber atomic_flush_seqno_{kMaxSequenceNumber};

  // Flush job info of the current memtable.
  std::unique_ptr<FlushJobInfo> flush_job_info_;

  RelaxedAtomic<bool> marked_for_flush_{false};

 private:
  void ReleaseProtectedSealedBlobFiles();

  std::shared_ptr<BlobFilePartitionManager> protected_blob_file_manager_;
  std::vector<uint64_t> protected_blob_file_numbers_;
};

class MemTable final : public ReadOnlyMemTable {
 public:
  struct KeyComparator final : public MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* prefix_len_key1,
                   const char* prefix_len_key2) const override;
    int operator()(const char* prefix_len_key,
                   const DecodedType& key) const override;
  };

  // earliest_seq should be the current SequenceNumber in the db such that any
  // key inserted into this memtable will have an equal or larger seq number.
  // (When a db is first created, the earliest sequence number will be 0).
  // If the earliest sequence number is not known, kMaxSequenceNumber may be
  // used, but this may prevent some transactions from succeeding until the
  // first key is inserted into the memtable.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const ImmutableOptions& ioptions,
                    const MutableCFOptions& mutable_cf_options,
                    WriteBufferManager* write_buffer_manager,
                    SequenceNumber earliest_seq, uint32_t column_family_id,
                    FlushInitiator* flush_initiator);
  // No copying allowed
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  MemTable(MemTable&&) = delete;
  MemTable& operator=(MemTable&&) = delete;

  ~MemTable() override;

  const char* Name() const override { return "MemTable"; }

  size_t ApproximateMemoryUsage() override;

  // As a cheap version of `ApproximateMemoryUsage()`, this function doesn't
  // require external synchronization. The value may be less accurate though
  size_t ApproximateMemoryUsageFast() const {
    return approximate_memory_usage_.LoadRelaxed();
  }

  size_t MemoryAllocatedBytes() const override {
    return table_->ApproximateMemoryUsage() +
           range_del_table_->ApproximateMemoryUsage() +
           arena_.MemoryAllocatedBytes();
  }

  size_t WBMTrackedMemoryUsage() const override {
    return mem_tracker_.allocated_bytes();
  }

  bool IsWBMTrackingActive() const {
    return mem_tracker_.IsFlushInitiatorActive();
  }

  void RefreshWBMTracking() {
    ReadLock rl(&immutable_mutex_);
    mem_tracker_.RefreshFlushInitiator();
  }

  void StopWBMTracking() {
    WriteLock wl(&immutable_mutex_);
    mem_tracker_.DeactivateFlushInitiator();
  }

  void UniqueRandomSample(const uint64_t& target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    // TODO(bjlemaire): at the moment, only supported by skiplistrep.
    // Extend it to all other memtable representations.
    table_->UniqueRandomSample(NumEntries(), target_sample_size, entries);
  }

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldScheduleFlush() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_REQUESTED;
  }

  // Returns true if a flush should be scheduled and the caller should
  // be the one to schedule it
  bool MarkFlushScheduled() {
    auto before = FLUSH_REQUESTED;
    return flush_state_.compare_exchange_strong(before, FLUSH_SCHEDULED,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
  }

  // Returns true if a flush has already been scheduled for this memtable
  bool HasFlushScheduled() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_SCHEDULED;
  }

  InternalIterator* NewIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, bool for_flush) override;

  InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, size_t ts_sz) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable) override;

  FragmentedRangeTombstoneIterator* NewTimestampStrippingRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      size_t ts_sz) override;

  Status VerifyEncodedEntry(Slice encoded,
                            const ProtectionInfoKVOS64& kv_prot_info);

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically, value will be empty if type==kTypeDeletion.
  //
  // REQUIRES: if allow_concurrent = false, external synchronization to prevent
  // simultaneous operations on the same MemTable.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  Status Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value, const ProtectionInfoKVOS64* kv_prot_info,
             bool allow_concurrent = false,
             MemTablePostProcessInfo* post_process_info = nullptr,
             void** hint = nullptr);

  using ReadOnlyMemTable::Get;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true,
           const BlobFetcher* blob_fetcher = nullptr) override;

  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                ReadCallback* callback, bool immutable_memtable,
                const BlobFetcher* blob_fetcher = nullptr) override;

  // If `key` exists in current memtable with type value_type and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // adds the new value to the memtable out-of-place.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status Update(SequenceNumber seq, ValueType value_type, const Slice& key,
                const Slice& value, const ProtectionInfoKVOS64* kv_prot_info);

  // If `key` exists in current memtable with type `kTypeValue` and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // if `key` exists in current memtable with type `kTypeValue`, adds the new
  // value to the memtable out-of-place.
  //
  // Returns `Status::NotFound` if `key` does not exist in current memtable or
  // the latest version of `key` does not have `kTypeValue`.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status UpdateCallback(SequenceNumber seq, const Slice& key,
                        const Slice& delta,
                        const ProtectionInfoKVOS64* kv_prot_info);

  // Returns the number of successive merge entries starting from the newest
  // entry for the key. The count ends when the oldest entry in the memtable
  // with which the newest entry would be merged is reached, or the count
  // reaches `limit`.
  size_t CountSuccessiveMergeEntries(const LookupKey& key, size_t limit);

  // Update counters and flush status after inserting a whole write batch
  // Used in concurrent memtable inserts.
  void BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
    table_->BatchPostProcess();
    const uint64_t old_num_entries =
        UNLIKELY(head_marks_enabled_)
            ? HeadMarkBatchPostProcess(update_counters)
            : num_entries_.FetchAddRelaxed(update_counters.num_entries);
    if (old_num_entries == 0 && update_counters.num_entries != 0) {
      mem_tracker_.ActivateFlushInitiator();
    }
    data_size_.FetchAddRelaxed(update_counters.data_size);
    if (update_counters.num_deletes != 0) {
      num_deletes_.FetchAddRelaxed(update_counters.num_deletes);
    }
    if (update_counters.num_range_deletes > 0) {
      num_range_deletes_.FetchAddRelaxed(update_counters.num_range_deletes);
      // noop for skip-list memtable
      // Besides correctness test in stress test, memtable flush record count
      // check will catch this if it were not noop.
      // range_del_table_->BatchPostProcess();
    }
    UpdateFlushState();
  }

  uint64_t NumEntries() const override { return num_entries_.LoadRelaxed(); }

  uint64_t NumDeletion() const override { return num_deletes_.LoadRelaxed(); }

  uint64_t NumRangeDeletion() const override {
    return num_range_deletes_.LoadRelaxed();
  }

  uint64_t GetDataSize() const override { return data_size_.LoadRelaxed(); }

  size_t write_buffer_size() const { return write_buffer_size_.LoadRelaxed(); }

  // Dynamically change the memtable's capacity. If set below the current usage,
  // the next key added will trigger a flush. Can only increase size when
  // memtable prefix bloom is disabled, since we can't easily allocate more
  // space. Non-atomic update ok because this is only called with DB mutex held.
  void UpdateWriteBufferSize(size_t new_write_buffer_size) {
    if (bloom_filter_ == nullptr ||
        new_write_buffer_size < write_buffer_size_.LoadRelaxed()) {
      write_buffer_size_.StoreRelaxed(new_write_buffer_size);
    }
  }

  bool IsEmpty() const override { return first_seqno_ == 0; }

  SequenceNumber GetFirstSequenceNumber() override {
    return first_seqno_.load(std::memory_order_relaxed);
  }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  void SetFirstSequenceNumber(SequenceNumber first_seqno) {
    return first_seqno_.store(first_seqno, std::memory_order_relaxed);
  }

  SequenceNumber GetEarliestSequenceNumber() override {
    // With file ingestion and empty memtable, this seqno needs to be fixed.
    return earliest_seqno_.load(std::memory_order_relaxed);
  }

  // Sets the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  // Used only for MemPurge operation
  void SetEarliestSequenceNumber(SequenceNumber earliest_seqno) {
    return earliest_seqno_.store(earliest_seqno, std::memory_order_relaxed);
  }

  // DB's latest sequence ID when the memtable is created. This number
  // may be updated to a more recent one before any key is inserted.
  SequenceNumber GetCreationSeq() const { return creation_seq_; }

  void SetCreationSeq(SequenceNumber sn) { creation_seq_ = sn; }

  // If this memtable contains data from a committed two phase transaction we
  // must take note of the log which contains that data so we can know when
  // to release that log.
  void RefLogContainingPrepSection(uint64_t log);
  uint64_t GetMinLogContainingPrepSection() override;

  void MarkImmutable() override {
    WriteLock wl(&immutable_mutex_);
    is_immutable_.StoreRelaxed(true);
    table_->MarkReadOnly();
    mem_tracker_.DoneAllocating();
  }

  void MarkFlushed() override { table_->MarkFlushed(); }

  // return true if the current MemTableRep supports merge operator.
  bool IsMergeOperatorSupported() const {
    return table_->IsMergeOperatorSupported();
  }

  // return true if the current MemTableRep supports snapshots.
  // inplace update prevents snapshots,
  bool IsSnapshotSupported() const {
    return table_->IsSnapshotSupported() && !moptions_.inplace_update_support;
  }

  MemTableStats ApproximateStats(const Slice& start_ikey,
                                 const Slice& end_ikey) override;

  // Get the lock associated for the key
  port::RWMutex* GetLock(const Slice& key);

  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return comparator_.comparator;
  }

  const ImmutableMemTableOptions* GetImmutableMemTableOptions() const {
    return &moptions_;
  }

  uint64_t ApproximateOldestKeyTime() const override {
    return oldest_key_time_.load(std::memory_order_relaxed);
  }

  // Returns a heuristic flush decision
  bool ShouldFlushNow();

  // Returns the reason for a flush scheduled because this memtable decided it
  // should stop accepting more writes.
  FlushReason GetFlushReason() const;

  // Updates `fragmented_range_tombstone_list_` that will be used to serve reads
  // when this memtable becomes an immutable memtable (in some
  // MemtableListVersion::memlist_). Should be called when this memtable is
  // about to become immutable. May be called multiple times since
  // SwitchMemtable() may fail.
  void ConstructFragmentedRangeTombstones();

  bool AddLogicallyRedundantRangeTombstone(
      SequenceNumber seq, const Slice& start_key, const Slice& end_key,
      uint64_t version_number, port::RWMutex& ingest_sst_lock) override;

  bool HeadMarksEnabled() const override { return head_marks_enabled_; }

  const MemTableHeadMark* FindUsableHeadMark(
      HeadMarkDirection dir, const Slice& user_key, bool key_included,
      SequenceNumber read_seq, uint64_t read_version) const override;

  HeadMarkPublishResult PublishHeadMark(
      HeadMarkDirection dir, const Slice& start, bool start_exclusive,
      const Slice& end, SequenceNumber seq, const MemTableHeadMark* sender,
      bool run_is_long, uint64_t version_number,
      const HeadMarkIngestState& ingest,
      port::RWMutex& ingest_sst_lock) override;

  void ClearHeadMarks() override;

  // Monotonically raises ingest_seqno_barrier_ to `y` (no-op if `y` is not
  // greater than the current value). The conversion's barrier check
  // (`seq < ingest_seqno_barrier_.LoadRelaxed()`) refuses converted
  // range tombstones that would shadow a just-installed L0 file.
  //
  // REQUIRES: DB mutex held by the caller. The DB mutex serializes all
  // callers, so the load-then-store pattern is race-free without needing
  // a CAS loop. Only IngestExternalFiles calls this.
  void BumpIngestSeqnoBarrier(SequenceNumber y);

  // Monotonically raises ingest_version_barrier_ to `version_number`, the
  // number of the Version that installed an ingestion.
  //
  // REQUIRES: DB mutex held by the caller. Only IngestExternalFiles calls
  // this.
  void BumpIngestVersionBarrier(uint64_t version_number);

  bool IsFragmentedRangeTombstonesConstructed() const override {
    return fragmented_range_tombstone_list_.get() != nullptr ||
           is_range_del_table_empty_.LoadRelaxed();
  }

  //  Gets the newest user defined timestamps in the memtable. This should only
  //  be called when user defined timestamp is enabled.
  Slice GetNewestUDT() const override;

  // Returns Corruption status if verification fails.
  static Status VerifyEntryChecksum(const char* entry,
                                    uint32_t protection_bytes_per_key,
                                    bool allow_data_in_errors = false);

  // Validate the checksum of the key/value pair.
  Status ValidateKey(const char* key, bool allow_data_in_errors);

 private:
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };

  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class MemTableList;
  friend class DBMemTableTest;
  friend class DBHeadMarkTest;

  // Test only: the current marks of direction `dir` in start order. Tests
  // reach it through the fixtures above.
  std::vector<MemTableHeadMark> TEST_HeadMarks(HeadMarkDirection dir) const;
  size_t TEST_HeadMarkBytes() const { return head_mark_bytes_.LoadRelaxed(); }
  size_t TEST_HeadMarkByteBudget() const {
    return kHeadMarkBaseBudget + data_size_.LoadRelaxed();
  }

  KeyComparator comparator_;
  const ImmutableMemTableOptions moptions_;
  const size_t kArenaBlockSize;
  AllocTracker mem_tracker_;
  ConcurrentArena arena_;
  std::unique_ptr<MemTableRep> table_;
  std::unique_ptr<MemTableRep> range_del_table_;
  // This is OK to be relaxed access because consistency between table_ and
  // range_del_table_ is provided by explicit multi-versioning with sequence
  // numbers. It's ok for stale memory to say the range_del_table_ is empty when
  // it's actually not because if it was relevant to our read (based on sequence
  // number), the relaxed memory read would get a sufficiently updated value
  // because of the ordering provided by LastPublishedSequence().
  RelaxedAtomic<bool> is_range_del_table_empty_;

  // Set to true by MarkImmutable(). Used as a "fast-path" to avoid acquiring
  // immutable_mutex_.
  RelaxedAtomic<bool> is_immutable_{false};
  port::RWMutex immutable_mutex_;

  // Total data size of all data inserted
  RelaxedAtomic<uint64_t> data_size_;
  // Every change is a read-modify-write. When head_marks_enabled_ is true,
  // every insert increments it with acq_rel, because head marks use it as the
  // handshake (see MemTableHeadMark). Otherwise the increment is relaxed.
  Atomic<uint64_t> num_entries_;
  RelaxedAtomic<uint64_t> num_deletes_;
  RelaxedAtomic<uint64_t> num_range_deletes_;

  // Dynamically changeable memtable option
  RelaxedAtomic<size_t> write_buffer_size_;

  // The sequence number of the kv that was inserted first
  std::atomic<SequenceNumber> first_seqno_;

  // The db sequence number at the time of creation or kMaxSequenceNumber
  // if not set.
  std::atomic<SequenceNumber> earliest_seqno_;

  // Seqno of the latest ingested external SST. See also
  // ColumnFamilyData::ingest_sst_lock_.
  RelaxedAtomic<SequenceNumber> ingest_seqno_barrier_{0};

  // Number of the latest Version that installed an ingestion. See also
  // ColumnFamilyData::ingest_sst_lock_.
  RelaxedAtomic<uint64_t> ingest_version_barrier_{0};

  SequenceNumber creation_seq_;

  // the earliest log containing a prepared section
  // which has been inserted into this memtable.
  std::atomic<uint64_t> min_prep_log_referenced_;

  // rw locks for inplace updates
  std::vector<port::RWMutex> locks_;

  const SliceTransform* const prefix_extractor_;
  std::unique_ptr<DynamicBloom> bloom_filter_;

  std::atomic<FlushStateEnum> flush_state_;

  SystemClock* clock_;

  // Extract sequential insert prefixes.
  const SliceTransform* insert_with_hint_prefix_extractor_;

  // Insert hints for each prefix.
  UnorderedMapH<Slice, void*, SliceHasher32> insert_hints_;

  // Timestamp of oldest key
  std::atomic<uint64_t> oldest_key_time_;

  // keep track of memory usage in table_, arena_, and range_del_table_.
  // Gets refreshed inside `ApproximateMemoryUsage()` or `ShouldFlushNow`
  RelaxedAtomic<uint64_t> approximate_memory_usage_;

  // max range deletions in a memtable,  before automatic flushing, 0 for
  // unlimited.
  uint32_t memtable_max_range_deletions_ = 0;

  // Size in bytes for the user-defined timestamps.
  size_t ts_sz_;

  // Pointer to the newest user-defined timestamp data in this MemTable. The
  // pointed-to memory lives in the arena and remains valid for the lifetime of
  // the memtable. Stored as an atomic pointer so that concurrent range
  // tombstone inserts from the read path can safely update it via CAS.
  Atomic<const char*> newest_udt_data_{nullptr};

  // Updates flush_state_ using ShouldFlushNow()
  void UpdateFlushState();

  void UpdateOldestKeyTime();

  void GetFromTable(const LookupKey& key,
                    SequenceNumber max_covering_tombstone_seq, bool do_merge,
                    ReadCallback* callback, bool* is_blob_index,
                    std::string* value, PinnableWideColumns* columns,
                    std::string* timestamp, Status* s,
                    MergeContext* merge_context, SequenceNumber* seq,
                    bool* found_final_value, bool* merge_in_progress,
                    const BlobFetcher* blob_fetcher,
                    bool* newer_version_present = nullptr);

  // Always returns non-null and assumes certain pre-checks (e.g.,
  // is_range_del_table_empty_) are done. This is only valid during the lifetime
  // of the underlying memtable.
  // read_seq and read_options.timestamp will be used as the upper bound
  // for range tombstones.
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIteratorInternal(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable);

  // The fragmented range tombstones of this memtable.
  // This is constructed when this memtable becomes immutable
  // if !is_range_del_table_empty_.
  std::unique_ptr<FragmentedRangeTombstoneList>
      fragmented_range_tombstone_list_;

  // The fragmented range tombstone of this memtable with all keys' user-defined
  // timestamps logically stripped. This is constructed and used by flush when
  // user-defined timestamps in memtable only feature is enabled.
  std::unique_ptr<FragmentedRangeTombstoneList>
      timestamp_stripping_fragmented_range_tombstone_list_;

  // makes sure there is a single range tombstone writer to invalidate cache
  std::mutex range_del_mutex_;
  CoreLocalArray<std::shared_ptr<FragmentedRangeTombstoneListCache>>
      cached_range_tombstone_;

  // Head marks (see MemTableHeadMark).
  struct HeadMarkCell {
    explicit HeadMarkCell(const Slice& s) : start(s) {}
    const Slice start;
    Atomic<const MemTableHeadMark*> mark{nullptr};
    // Set under head_mark_mu_ when the publisher of a new claim finishes and
    // the claim still has an empty usable range, for example because a writer
    // lowered it during the walk. Such a claim never becomes usable: it moves
    // no seek, so no scan extends it, and writers only lower it.
    // AddHeadMarkLocked() treats the cell as empty, so the claim does not
    // block new claims over its range.
    RelaxedAtomic<bool> abandoned{false};
  };
  // 16 is an estimate of how many queues of one memtable are scanned at the
  // same time. More cells make the binary search on the write path longer
  // (about 4 compares at 16) and every HeadMarkTable larger, because the table
  // has a fixed size.
  static constexpr size_t kMaxHeadMarks = 16;
  struct HeadMarkTable {
    Slice min_start;
    size_t num_cells = 0;
    HeadMarkCell* cells[kMaxHeadMarks] = {};
  };
  // The validation walk stops after this many entries and keeps the part it
  // checked. The cap bounds the time one scan spends on publishing to about
  // the cost of stepping over 16384 entries (an estimate chosen so that one
  // walk covers a large dead prefix). The rest is checked only by a later
  // extension, which needs a new run of min_tombstones_for_range_conversion
  // tombstones after the mark.
  static constexpr size_t kMaxHeadMarkWalk = 16384;

  static bool IsHeadMarkDeleteType(ValueType type) {
    return type == kTypeDeletion || type == kTypeSingleDeletion ||
           type == kTypeDeletionWithTimestamp || type == kTypeRangeDeletion;
  }

  // BatchPostProcess() for a memtable with head marks: raises
  // max_inserted_seq_, does the num_entries_ handshake and lowers the marks
  // that the batch's live keys may fall in. Returns the old entry count.
  uint64_t HeadMarkBatchPostProcess(const MemTablePostProcessInfo& info);

  // Compares user keys in the order of a direction: the user comparator for
  // forward marks, and the reverse of it for reverse marks.
  struct HeadMarkOrder {
    const Comparator* ucmp;
    bool reverse;
    int operator()(const Slice& a, const Slice& b) const {
      return reverse ? ucmp->Compare(b, a) : ucmp->Compare(a, b);
    }
  };
  HeadMarkOrder OrderOf(HeadMarkDirection dir) const {
    return {comparator_.comparator.user_comparator(),
            dir == HeadMarkDirection::kReverse};
  }

  // Lowers every mark of both directions whose claim overlaps [lo, hi] (user
  // comparator order). A batch with more than
  // MemTablePostProcessInfo::HeadMarkLiveKeys::kMaxLiveKeys live keys passes
  // the range of all of them, so a mark can be lowered even if no key of the
  // batch is in it. The keys must stay valid for the memtable's lifetime (they
  // point into entries).
  void LowerHeadMarks(const Slice& lo, const Slice& hi);

  // Tells if a claim of direction `dir` may overlap [lo, hi] (in the order of
  // `dir`), without the mutex.
  bool MayLowerHeadMarks(HeadMarkDirection dir, const Slice& lo,
                         const Slice& hi) const;

  // Lowers the marks of both directions whose claim contains `key`, the user
  // key of one live entry. A single writer calls this for each live insert, and
  // BatchPostProcess() calls it for each live key of a concurrent batch with at
  // most MemTablePostProcessInfo::HeadMarkLiveKeys::kMaxLiveKeys live keys. The
  // key must stay valid for the memtable's lifetime (it points into an entry).
  void LowerHeadMarkForKey(const Slice& key) {
    if (MayLowerHeadMarkForKey(HeadMarkDirection::kForward, key) ||
        MayLowerHeadMarkForKey(HeadMarkDirection::kReverse, key)) {
      MutexLock l(&head_mark_mu_);
      LowerHeadMarksLocked(HeadMarkDirection::kForward, key, key);
      LowerHeadMarksLocked(HeadMarkDirection::kReverse, key, key);
    }
  }

  // Tells if a claim of direction `dir` may contain `key`, without the mutex.
  // A key outside the hull (see MemTableHeadMark) returns here after one or
  // two key compares. The end of the last claim is compared first, because a
  // queue tail key is past it in the order of `dir`.
  bool MayLowerHeadMarkForKey(HeadMarkDirection dir, const Slice& key) const {
    const HeadMarkTable* table = HeadMarks(dir).Load();
    if (table == nullptr) {
      return false;
    }
    const HeadMarkOrder order = OrderOf(dir);
    const Slice& hull_end =
        table->cells[table->num_cells - 1]->mark.Load()->end;
    if (order(key, hull_end) >= 0 || order(key, table->min_start) < 0) {
      return false;
    }
    return HeadMarkClaimMayHoldKey(dir, *table, key);
  }

  // MayLowerHeadMarkForKey() for a key inside the hull of `table`: finds the
  // claim that may contain `key`. A key in a gap between two claims returns
  // false.
  bool HeadMarkClaimMayHoldKey(HeadMarkDirection dir,
                               const HeadMarkTable& table,
                               const Slice& key) const;
  void LowerHeadMarksLocked(HeadMarkDirection dir, const Slice& lo,
                            const Slice& hi);

  // Returns the index of the cell of `table` whose claim may contain `key`:
  // the last cell whose start is at or before `key` in the order of `dir`.
  // Returns table.num_cells if `key` is outside the hull (see
  // MemTableHeadMark).
  size_t FindHeadMarkCell(HeadMarkDirection dir, const HeadMarkTable& table,
                          const Slice& key) const;

  // FindHeadMarkCell() without the hull check.
  size_t SearchHeadMarkCells(HeadMarkDirection dir, const HeadMarkTable& table,
                             const Slice& key) const;

  // Returns the index of the cell of `table` that holds `mark`, or
  // table.num_cells.
  static size_t FindHeadMarkCellHolding(const HeadMarkTable& table,
                                        const MemTableHeadMark* mark);
  const MemTableHeadMark* NewHeadMark(const Slice& start, bool start_exclusive,
                                      const Slice& end, const Slice& usable_end,
                                      SequenceNumber seq,
                                      uint64_t ingest_version, bool lowered);
  Slice CopyToArena(const Slice& s);

  // Allocates `n` bytes (n > 0) for a head mark object from arena_. The
  // caller counts them in head_mark_bytes_ first.
  char* AllocateHeadMarkBytes(size_t n, bool aligned);

  // The byte budget of the marks of this memtable is kHeadMarkBaseBudget plus
  // data_size_ (the bytes of the memtable's entries). A new mark, an extension
  // and a raise are optional, so each calls this with the exact number of
  // bytes that it will allocate, before it allocates or stores anything. This
  // adds `n` to head_mark_bytes_ and returns true if the sum fits in the
  // budget. Otherwise it takes `n` back and returns false, and the caller
  // refuses. Two reservations that race near the limit can both be refused
  // when one alone would fit; that only drops an optional mark. A writer that
  // lowers a mark counts its bytes without a reservation and is never
  // refused, so a mark never stays wrong. A lowering comes with a write that
  // grows data_size_.
  bool ReserveHeadMarkBytes(size_t n);

  // Publishes a table of direction `dir` with `cells` (sorted by start, n >
  // 0).
  void PublishHeadMarkTableLocked(HeadMarkDirection dir,
                                  HeadMarkCell* const* cells, size_t n);

  // PublishHeadMark() step 2 for an extension and for a new mark. Each
  // returns the claim to validate and its cell, or a refusal. An extension
  // whose run ends inside the sender's claim returns the sender itself and
  // stores nothing. An extension also returns in *walk_end the end of the
  // part that the walk checks: the end of the scan's run, capped at the next
  // claim. That end can point into `end`, which the caller owns.
  HeadMarkPublishResult ExtendHeadMarkLocked(
      HeadMarkDirection dir, const Slice& end, SequenceNumber seq,
      const MemTableHeadMark* sender, HeadMarkCell** cell,
      const MemTableHeadMark** claim, Slice* walk_end);
  HeadMarkPublishResult AddHeadMarkLocked(
      HeadMarkDirection dir, const Slice& start, bool start_exclusive,
      const Slice& end, SequenceNumber seq, uint64_t ingest_version,
      HeadMarkCell** cell, const MemTableHeadMark** claim);

  // PublishHeadMark() step 4: walks the claim's unchecked part, from
  // usable_end to `to` (the claim's end, or the end of the scan's run) in the
  // order of `dir`, and returns the end of the checked part. If usable_end is
  // still an exclusive start, the walk seeks past every version of that key,
  // so the versions of a key the claim does not hold cost nothing; every entry
  // the walk steps over counts toward kMaxHeadMarkWalk.
  Slice ValidateHeadMarkRange(HeadMarkDirection dir,
                              const MemTableHeadMark& claim, const Slice& to,
                              SequenceNumber seq) const;

  // PublishHeadMark() step 5: replaces `claim` in `cell` with a mark whose
  // usable end is `usable_end`, if the memtable is still mutable and the
  // current table still holds `claim`.
  void RaiseHeadMarkUsableEnd(HeadMarkDirection dir, HeadMarkCell* cell,
                              const MemTableHeadMark* claim,
                              const Slice& usable_end, SequenceNumber seq);

  // Set at construction, see the Limits paragraph of MemTableHeadMark.
  bool head_marks_enabled_ = false;
  // One table per HeadMarkDirection. Use HeadMarks() to index it.
  Atomic<const HeadMarkTable*> head_marks_[2];
  Atomic<const HeadMarkTable*>& HeadMarks(HeadMarkDirection dir) {
    return head_marks_[static_cast<size_t>(dir)];
  }
  const Atomic<const HeadMarkTable*>& HeadMarks(HeadMarkDirection dir) const {
    return head_marks_[static_cast<size_t>(dir)];
  }
  port::Mutex head_mark_mu_;
  // The bytes that head mark objects (tables, cells, marks and key copies)
  // asked from arena_, counted before each allocation (see
  // ReserveHeadMarkBytes()). An ingestion drops the marks, but the arena keeps
  // their bytes until the memtable is freed. Without writes, nothing makes the
  // memtable flush, so repeated ingestions and scans would grow it without a
  // limit. The budget keeps the marks at most at the bytes of the memtable's
  // entries plus kHeadMarkBaseBudget. A memtable without writes has no
  // lowering, so its marks stay within the budget. With writes, a lowering
  // can go over it, and the marks' bytes count toward the flush trigger like
  // the entries' bytes.
  RelaxedAtomic<size_t> head_mark_bytes_{0};
  // 64 KB is an estimate that lets a small memtable publish a few hundred
  // marks with short keys (one publish that is raised allocates a table, a
  // cell, two marks and three key copies) before its entries add more budget.
  static constexpr size_t kHeadMarkBaseBudget = 64 << 10;
  // The largest sequence number of a live entry. A plain load and store in
  // the single writer path; a CAS loop in BatchPostProcess().
  Atomic<SequenceNumber> max_inserted_seq_{0};

  void UpdateEntryChecksum(const ProtectionInfoKVOS64* kv_prot_info,
                           const Slice& key, const Slice& value, ValueType type,
                           SequenceNumber s, char* checksum_ptr);

  void MaybeUpdateNewestUDT(const Slice& user_key);

  const std::function<Status(const char*, bool)> key_validation_callback_;
};

const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace ROCKSDB_NAMESPACE
