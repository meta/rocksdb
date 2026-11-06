//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable_list.h"

#include <algorithm>
#include <cinttypes>
#include <iterator>
#include <limits>
#include <queue>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_set.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "table/merging_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class InternalKeyComparator;
class VersionSet;

bool MultiScanOverlapsUserKeyRange(const MultiScanArgs* scan_opts,
                                   const Comparator* user_comparator,
                                   const Slice& smallest_user_key,
                                   const Slice& largest_user_key) {
  if (scan_opts == nullptr || !scan_opts->HasBoundedScanRanges()) {
    return true;
  }

  for (const ScanOptions& scan_range : scan_opts->GetScanRanges()) {
    if (user_comparator->CompareWithoutTimestamp(
            scan_range.range.limit.value(), /*a_has_ts=*/false,
            smallest_user_key, /*b_has_ts=*/true) <= 0) {
      continue;
    }
    if (user_comparator->CompareWithoutTimestamp(
            scan_range.range.start.value(), /*a_has_ts=*/false,
            largest_user_key, /*b_has_ts=*/true) > 0) {
      continue;
    }
    return true;
  }
  return false;
}

bool MultiScanIteratorOverlapsUserKeyRange(InternalIterator* iter,
                                           const MultiScanArgs* scan_opts,
                                           const Comparator* user_comparator) {
  iter->SeekToFirst();
  if (!iter->status().ok()) {
    return true;
  }
  if (!iter->Valid()) {
    return true;
  }
  const std::string smallest_user_key = ExtractUserKey(iter->key()).ToString();
  iter->SeekToLast();
  if (!iter->status().ok()) {
    return true;
  }
  if (!iter->Valid()) {
    return true;
  }
  const std::string largest_user_key = ExtractUserKey(iter->key()).ToString();
  return MultiScanOverlapsUserKeyRange(scan_opts, user_comparator,
                                       smallest_user_key, largest_user_key);
}

bool MultiScanIntersectsMemTable(
    ReadOnlyMemTable* memtable, const ReadOptions& read_options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    const SliceTransform* prefix_extractor, const MultiScanArgs* scan_opts,
    const Comparator* user_comparator) {
  if (scan_opts == nullptr || !scan_opts->HasBoundedScanRanges()) {
    return true;
  }
  if (memtable->NumRangeDeletion() > 0) {
    return true;
  }

  Arena arena;
  ReadOptions intersect_read_options(read_options);
  intersect_read_options.total_order_seek = true;
  intersect_read_options.iterate_lower_bound = nullptr;
  intersect_read_options.iterate_upper_bound = nullptr;
  InternalIterator* iter =
      memtable->NewIterator(intersect_read_options, seqno_to_time_mapping,
                            &arena, prefix_extractor, /*for_flush=*/false);
  ScopedArenaPtr<InternalIterator> iter_guard(iter);
  return MultiScanIteratorOverlapsUserKeyRange(iter, scan_opts,
                                               user_comparator);
}

void MemTableListVersion::AddMemTable(ReadOnlyMemTable* m) {
  if (!memlist_.empty()) {
    // ID can be equal for MemPurge
    assert(m->GetID() >= memlist_.front()->GetID());
  }
  memlist_.push_front(m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

void MemTableListVersion::AddMemTableBefore(ReadOnlyMemTable* m,
                                            ReadOnlyMemTable* before) {
  std::list<ReadOnlyMemTable*>::iterator before_it =
      std::find(memlist_.begin(), memlist_.end(), before);
  assert(before_it != memlist_.end());
  assert(m->GetID() >= before->GetID());
  if (before_it != memlist_.begin()) {
    assert((*std::prev(before_it))->GetID() >= m->GetID());
  }
  memlist_.insert(before_it, m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

void MemTableListVersion::UnrefMemTable(
    autovector<ReadOnlyMemTable*>* to_delete, ReadOnlyMemTable* m) {
  if (m->Unref()) {
    to_delete->push_back(m);
    assert(*parent_memtable_list_memory_usage_ >= m->ApproximateMemoryUsage());
    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage, const MemTableListVersion& old)
    : max_write_buffer_size_to_maintain_(
          old.max_write_buffer_size_to_maintain_),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {
  memlist_ = old.memlist_;
  for (auto& m : memlist_) {
    m->Ref();
  }

  memlist_history_ = old.memlist_history_;
  for (auto& m : memlist_history_) {
    m->Ref();
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage,
    int64_t max_write_buffer_size_to_maintain)
    : max_write_buffer_size_to_maintain_(max_write_buffer_size_to_maintain),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {}

void MemTableListVersion::Ref() { ++refs_; }

// called by superversion::clean()
void MemTableListVersion::Unref(autovector<ReadOnlyMemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // if to_delete is equal to nullptr it means we're confident
    // that refs_ will not be zero
    assert(to_delete != nullptr);
    for (const auto& m : memlist_) {
      UnrefMemTable(to_delete, m);
    }
    for (const auto& m : memlist_history_) {
      UnrefMemTable(to_delete, m);
    }
    delete this;
  }
}

int MemTableList::NumNotFlushed() const {
  int size = current_->NumNotFlushed();
  assert(num_flush_not_started_ <= size);
  return size;
}

int MemTableList::NumFlushed() const { return current_->NumFlushed(); }

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(const LookupKey& key, std::string* value,
                              PinnableWideColumns* columns,
                              std::string* timestamp, Status* s,
                              MergeContext* merge_context,
                              SequenceNumber* max_covering_tombstone_seq,
                              SequenceNumber* seq, const ReadOptions& read_opts,
                              ReadCallback* callback, bool* is_blob_index,
                              const BlobFetcher* blob_fetcher) {
  return GetFromList(&memlist_, key, value, columns, timestamp, s,
                     merge_context, max_covering_tombstone_seq, seq, read_opts,
                     callback, is_blob_index, blob_fetcher);
}

void MemTableListVersion::MultiGet(const ReadOptions& read_options,
                                   MultiGetRange* range, ReadCallback* callback,
                                   const BlobFetcher* blob_fetcher) {
  for (auto memtable : memlist_) {
    memtable->MultiGet(read_options, range, callback,
                       true /* immutable_memtable */, blob_fetcher);
    if (range->empty()) {
      return;
    }
  }
}

bool MemTableListVersion::GetMergeOperands(
    const LookupKey& key, Status* s, MergeContext* merge_context,
    SequenceNumber* max_covering_tombstone_seq, const ReadOptions& read_opts,
    const BlobFetcher* blob_fetcher) {
  for (ReadOnlyMemTable* memtable : memlist_) {
    bool done = memtable->Get(
        key, /*value=*/nullptr, /*columns=*/nullptr, /*timestamp=*/nullptr, s,
        merge_context, max_covering_tombstone_seq, read_opts,
        true /* immutable_memtable */, nullptr, nullptr, false, blob_fetcher);
    if (done) {
      return true;
    }
  }
  return false;
}

bool MemTableListVersion::GetFromHistory(
    const LookupKey& key, std::string* value, PinnableWideColumns* columns,
    std::string* timestamp, Status* s, MergeContext* merge_context,
    SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
    const ReadOptions& read_opts, bool* is_blob_index,
    const BlobFetcher* blob_fetcher) {
  return GetFromList(&memlist_history_, key, value, columns, timestamp, s,
                     merge_context, max_covering_tombstone_seq, seq, read_opts,
                     nullptr /*read_callback*/, is_blob_index, blob_fetcher);
}

bool MemTableListVersion::GetFromList(
    std::list<ReadOnlyMemTable*>* list, const LookupKey& key,
    std::string* value, PinnableWideColumns* columns, std::string* timestamp,
    Status* s, MergeContext* merge_context,
    SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
    const ReadOptions& read_opts, ReadCallback* callback, bool* is_blob_index,
    const BlobFetcher* blob_fetcher) {
  *seq = kMaxSequenceNumber;

  for (auto& memtable : *list) {
    assert(memtable->IsFragmentedRangeTombstonesConstructed());
    SequenceNumber current_seq = kMaxSequenceNumber;

    bool done =
        memtable->Get(key, value, columns, timestamp, s, merge_context,
                      max_covering_tombstone_seq, &current_seq, read_opts,
                      true /* immutable_memtable */, callback, is_blob_index,
                      true /* do_merge */, blob_fetcher);
    if (*seq == kMaxSequenceNumber) {
      // Store the most recent sequence number of any operation on this key.
      // Since we only care about the most recent change, we only need to
      // return the first operation found when searching memtables in
      // reverse-chronological order.
      // current_seq would be equal to kMaxSequenceNumber if the value was to be
      // skipped. This allows seq to be assigned again when the next value is
      // read.
      *seq = current_seq;
    }

    if (done) {
      assert(*seq != kMaxSequenceNumber ||
             (!s->ok() && !s->IsMergeInProgress()));
      return true;
    }
    if (!s->ok() && !s->IsMergeInProgress() && !s->IsNotFound()) {
      return false;
    }
  }
  return false;
}

Status MemTableListVersion::AddRangeTombstoneIterators(
    const ReadOptions& read_opts, Arena* /*arena*/,
    RangeDelAggregator* range_del_agg) {
  assert(range_del_agg != nullptr);
  // Except for snapshot read, using kMaxSequenceNumber is OK because these
  // are immutable memtables.
  SequenceNumber read_seq = read_opts.snapshot != nullptr
                                ? read_opts.snapshot->GetSequenceNumber()
                                : kMaxSequenceNumber;
  for (auto& m : memlist_) {
    assert(m->IsFragmentedRangeTombstonesConstructed());
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        m->NewRangeTombstoneIterator(read_opts, read_seq,
                                     true /* immutable_memtable */));
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }
  return Status::OK();
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    const SliceTransform* prefix_extractor,
    std::vector<InternalIterator*>* iterator_list, Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator(options, seqno_to_time_mapping,
                                            arena, prefix_extractor,
                                            /*for_flush=*/false));
  }
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    const SliceTransform* prefix_extractor,
    MergeIteratorBuilder* merge_iter_builder, bool add_range_tombstone_iter,
    SequenceNumber read_seq, const MultiScanArgs* scan_opts,
    const Comparator* user_comparator) {
  for (auto& m : memlist_) {
    const bool should_probe_scan_intersection =
        scan_opts != nullptr && scan_opts->HasBoundedScanRanges() &&
        m->NumRangeDeletion() == 0;
    if (should_probe_scan_intersection &&
        !MultiScanIntersectsMemTable(m, options, seqno_to_time_mapping,
                                     prefix_extractor, scan_opts,
                                     user_comparator)) {
      continue;
    }
    auto mem_iter =
        m->NewIterator(options, seqno_to_time_mapping,
                       merge_iter_builder->GetArena(), prefix_extractor,
                       /*for_flush=*/false);
    ScopedArenaPtr<InternalIterator> mem_iter_guard(mem_iter);
    if (!add_range_tombstone_iter || options.ignore_range_deletions) {
      merge_iter_builder->AddIterator(mem_iter_guard.release());
    } else {
      const SequenceNumber range_del_read_seq =
          read_seq != kMaxSequenceNumber || options.snapshot == nullptr
              ? read_seq
              : options.snapshot->GetSequenceNumber();
      std::unique_ptr<TruncatedRangeDelIterator> mem_tombstone_iter;
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          m->NewRangeTombstoneIterator(options, range_del_read_seq,
                                       true /* immutale_memtable */));
      if (range_del_iter != nullptr && !range_del_iter->empty()) {
        mem_tombstone_iter = std::make_unique<TruncatedRangeDelIterator>(
            std::move(range_del_iter), &m->GetInternalKeyComparator(),
            nullptr /* smallest */, nullptr /* largest */);
      }
      merge_iter_builder->AddPointAndTombstoneIterator(
          mem_iter_guard.release(), std::move(mem_tombstone_iter));
    }
  }
}

uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->NumEntries();
  }
  return total_num;
}

ReadOnlyMemTable::MemTableStats MemTableListVersion::ApproximateStats(
    const Slice& start_ikey, const Slice& end_ikey) const {
  ReadOnlyMemTable::MemTableStats total_stats = {0, 0};
  for (auto& m : memlist_) {
    auto mStats = m->ApproximateStats(start_ikey, end_ikey);
    total_stats.size += mStats.size;
    total_stats.count += mStats.count;
  }
  return total_stats;
}

uint64_t MemTableListVersion::GetTotalNumDeletes() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->NumDeletion();
  }
  return total_num;
}

SequenceNumber MemTableListVersion::GetEarliestSequenceNumber(
    bool include_history) const {
  if (include_history && !memlist_history_.empty()) {
    return memlist_history_.back()->GetEarliestSequenceNumber();
  } else if (!memlist_.empty()) {
    return memlist_.back()->GetEarliestSequenceNumber();
  } else {
    return kMaxSequenceNumber;
  }
}

SequenceNumber MemTableListVersion::GetFirstSequenceNumber() const {
  SequenceNumber min_first_seqno = kMaxSequenceNumber;
  for (const auto& m : memlist_) {
    min_first_seqno = std::min(m->GetFirstSequenceNumber(), min_first_seqno);
  }
  return min_first_seqno;
}

// caller is responsible for referencing m
void MemTableListVersion::Add(ReadOnlyMemTable* m,
                              autovector<ReadOnlyMemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTable(m);
  // m->MemoryAllocatedBytes() is added in MemoryAllocatedBytesExcludingLast
  TrimHistory(to_delete, 0);
}

// caller is responsible for referencing m
void MemTableListVersion::AddBefore(ReadOnlyMemTable* m,
                                    ReadOnlyMemTable* before,
                                    autovector<ReadOnlyMemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTableBefore(m, before);
  // m->MemoryAllocatedBytes() is added in MemoryAllocatedBytesExcludingLast
  TrimHistory(to_delete, 0);
}

// Removes m from list of memtables not flushed.  Caller should NOT Unref m.
void MemTableListVersion::Remove(ReadOnlyMemTable* m,
                                 autovector<ReadOnlyMemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);

  m->MarkFlushed();
  if (max_write_buffer_size_to_maintain_ > 0) {
    memlist_history_.push_front(m);
    // Unable to get size of mutable memtable at this point, pass 0 to
    // TrimHistory as a best effort.
    TrimHistory(to_delete, 0);
  } else {
    UnrefMemTable(to_delete, m);
  }
}

// return the total memory usage assuming the oldest flushed memtable is dropped
size_t MemTableListVersion::MemoryAllocatedBytesExcludingLast() const {
  size_t total_memtable_size = 0;
  for (auto& memtable : memlist_) {
    total_memtable_size += memtable->MemoryAllocatedBytes();
  }
  for (auto& memtable : memlist_history_) {
    total_memtable_size += memtable->MemoryAllocatedBytes();
  }
  if (!memlist_history_.empty()) {
    total_memtable_size -= memlist_history_.back()->MemoryAllocatedBytes();
  }
  return total_memtable_size;
}

bool MemTableListVersion::MemtableLimitExceeded(size_t usage) {
  if (max_write_buffer_size_to_maintain_ > 0) {
    // calculate the total memory usage after dropping the oldest flushed
    // memtable, compare with max_write_buffer_size_to_maintain_ to decide
    // whether to trim history
    return MemoryAllocatedBytesExcludingLast() + usage >=
           static_cast<size_t>(max_write_buffer_size_to_maintain_);
  } else {
    return false;
  }
}

bool MemTableListVersion::HistoryShouldBeTrimmed(size_t usage) {
  return MemtableLimitExceeded(usage) && !memlist_history_.empty();
}

// Make sure we don't use up too much space in history
bool MemTableListVersion::TrimHistory(autovector<ReadOnlyMemTable*>* to_delete,
                                      size_t usage) {
  bool ret = false;
  while (HistoryShouldBeTrimmed(usage)) {
    ReadOnlyMemTable* x = memlist_history_.back();
    memlist_history_.pop_back();

    UnrefMemTable(to_delete, x);
    ret = true;
  }
  return ret;
}

Slice MemTableListVersion::GetNewestUDT() const {
  for (auto it = memlist_.begin(); it != memlist_.end(); ++it) {
    ReadOnlyMemTable* m = *it;
    Slice timestamp = m->GetNewestUDT();
    assert(!timestamp.empty() || m->IsEmpty());
    if (!timestamp.empty()) {
      return timestamp;
    }
  }
  return Slice();
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  if ((flush_requested_ && num_flush_not_started_ > 0) ||
      (num_flush_not_started_ >= min_write_buffer_number_to_merge_)) {
    assert(imm_flush_needed.load(std::memory_order_relaxed));
    return true;
  }
  return false;
}

bool MemTableList::IsFlushPendingOrRunning() const {
  if (current_->memlist_.size() - num_flush_not_started_ > 0) {
    // Flush is already running on at least one memtable
    return true;
  }
  return IsFlushPending();
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(uint64_t max_memtable_id,
                                        autovector<ReadOnlyMemTable*>* ret,
                                        uint64_t* max_next_log_number) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  const auto& memlist = current_->memlist_;

  // Note: every time MemTableList::Add(mem) is called, it adds the new mem
  // at the FRONT of the memlist (memlist.push_front(mem)). Therefore, by
  // iterating through the memlist starting at the end, the vector<MemTable*>
  // ret is filled with memtables already sorted in increasing MemTable ID.
  auto it = memlist.rbegin();
  for (; it != memlist.rend(); ++it) {
    ReadOnlyMemTable* m = *it;
    if (m->GetID() > max_memtable_id) {
      break;
    }
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.store(false, std::memory_order_release);
      }
      m->flush_in_progress_ = true;  // flushing will start very soon
      if (max_next_log_number) {
        *max_next_log_number =
            std::max(m->GetNextLogNumber(), *max_next_log_number);
      }
      ret->push_back(m);
    } else if (!ret->empty()) {
      // This `break` is necessary to prevent picking non-consecutive memtables
      // in case `memlist` has one or more entries with
      // `flush_in_progress_ == true` sandwiched between entries with
      // `flush_in_progress_ == false`. This could happen after parallel flushes
      // are picked and the one flushing older memtables is rolled back.
      break;
    }
  }
  if (!ret->empty() && it != memlist.rend()) {
    // checks that the first memtable not picked to flush is not ingested wbwi.
    // Ingested memtable should be flushed together with the memtable before it
    // since they map to the same WAL and have the same NextLogNumber().
    assert(strcmp((*it)->Name(), "WBWIMemTable") != 0);
  }
  // The flush request is complete only after every unstarted immutable
  // memtable has been picked. A request can be older than a newer immutable
  // memtable; clearing flush_requested_ after a partial or empty pick would
  // make the queued newer request look unnecessary and leave that memtable
  // unflushed.
  if (num_flush_not_started_ == 0) {
    flush_requested_ = false;  // start-flush request is complete
  }
}

void MemTableList::RollbackMemtableFlush(
    const autovector<ReadOnlyMemTable*>& mems,
    bool rollback_succeeding_memtables) {
  TEST_SYNC_POINT("RollbackMemtableFlush");
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
#ifndef NDEBUG
  for (ReadOnlyMemTable* m : mems) {
    assert(m->flush_in_progress_);
    assert(m->file_number_ == 0);
  }
#endif

  if (rollback_succeeding_memtables && !mems.empty()) {
    std::list<ReadOnlyMemTable*>& memlist = current_->memlist_;
    auto it = memlist.rbegin();
    for (; *it != mems[0] && it != memlist.rend(); ++it) {
    }
    // mems should be in memlist
    assert(*it == mems[0]);
    if (*it == mems[0]) {
      ++it;
    }
    while (it != memlist.rend()) {
      ReadOnlyMemTable* m = *it;
      // Only rollback complete, not in-progress,
      // in_progress can be flushes that are still writing SSTs
      if (m->flush_completed_) {
        m->flush_in_progress_ = false;
        m->flush_completed_ = false;
        m->edit_.Clear();
        m->file_number_ = 0;
        num_flush_not_started_++;
        ++it;
      } else {
        break;
      }
    }
  }

  for (ReadOnlyMemTable* m : mems) {
    if (m->flush_in_progress_) {
      assert(m->file_number_ == 0);
      m->file_number_ = 0;
      m->flush_in_progress_ = false;
      m->flush_completed_ = false;
      m->edit_.Clear();
      num_flush_not_started_++;
    }
  }
  if (!mems.empty()) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
}

// Try record a successful flush in the manifest file. It might just return
// Status::OK letting a concurrent flush to do actual the recording..
Status MemTableList::TryInstallMemtableFlushResults(
    ColumnFamilyData* cfd, const autovector<ReadOnlyMemTable*>& mems,
    LogsWithPrepTracker* prep_tracker, VersionSet* vset, InstrumentedMutex* mu,
    uint64_t file_number, autovector<ReadOnlyMemTable*>* to_delete,
    FSDirectory* db_directory, LogBuffer* log_buffer,
    std::list<std::unique_ptr<FlushJobInfo>>* committed_flush_jobs_info,
    bool write_edits) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  mu->AssertHeld();

  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);

  // Flush was successful
  // Record the status on the memtable object. Either this call or a call by a
  // concurrent flush thread will read the status and write it to manifest.
  for (size_t i = 0; i < mems.size(); ++i) {
    // All the edits are associated with the first memtable of this batch.
    assert(i == 0 || mems[i]->GetEdits()->NumEntries() == 0);

    mems[i]->flush_completed_ = true;
    mems[i]->file_number_ = file_number;
  }

  // if some other thread is already committing, then return
  Status s;
  if (commit_in_progress_) {
    TEST_SYNC_POINT("MemTableList::TryInstallMemtableFlushResults:InProgress");
    return s;
  }

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // Retry until all completed flushes are committed. New flushes can finish
  // while the current thread is writing manifest where mutex is released.
  while (s.ok()) {
    auto& memlist = current_->memlist_;
    // The back is the oldest; if flush_completed_ is not set to it, it means
    // that we were assigned a more recent memtable. The memtables' flushes must
    // be recorded in manifest in order. A concurrent flush thread, who is
    // assigned to flush the oldest memtable, will later wake up and does all
    // the pending writes to manifest, in order.
    if (memlist.empty() || !memlist.back()->flush_completed_) {
      break;
    }
    // scan all memtables from the earliest, and commit those
    // (in that order) that have finished flushing. Memtables
    // are always committed in the order that they were created.
    uint64_t batch_file_number = 0;
    autovector<VersionEdit*> edit_list;
    autovector<ReadOnlyMemTable*> memtables_to_flush;
    // enumerate from the last (earliest) element to see how many batch finished
    for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
      ReadOnlyMemTable* m = *it;
      if (!m->flush_completed_) {
        break;
      }
      if (it == memlist.rbegin() || batch_file_number != m->file_number_) {
        // Oldest memtable in a new batch.
        batch_file_number = m->file_number_;
        if (m->edit_.GetBlobFileAdditions().empty()) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit flush result of table #%" PRIu64
                           " started",
                           cfd->GetName().c_str(), m->file_number_);
        } else {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit flush result of table #%" PRIu64
                           " (+%zu blob files) started",
                           cfd->GetName().c_str(), m->file_number_,
                           m->edit_.GetBlobFileAdditions().size());
        }

        edit_list.push_back(&m->edit_);
        std::unique_ptr<FlushJobInfo> info = m->ReleaseFlushJobInfo();
        if (info != nullptr) {
          committed_flush_jobs_info->push_back(std::move(info));
        }
      }
      memtables_to_flush.push_back(m);
    }

    size_t num_mem_to_flush = memtables_to_flush.size();
    // TODO(myabandeh): Not sure how batch_count could be 0 here.
    if (num_mem_to_flush > 0) {
      VersionEdit edit;
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
      if (memtables_to_flush.size() == memlist.size()) {
        // TODO(yuzhangyu): remove this testing code once the
        // `GetEditForDroppingCurrentVersion` API is used by the atomic data
        // replacement. This function can get the same edits for wal related
        // fields, and some duplicated fields as contained already in edit_list
        // for column family's recovery.
        edit = GetEditForDroppingCurrentVersion(cfd, vset, prep_tracker);
      } else {
        edit = GetDBRecoveryEditForObsoletingMemTables(
            vset, *cfd, edit_list, memtables_to_flush, prep_tracker);
      }
#else
      edit = GetDBRecoveryEditForObsoletingMemTables(
          vset, *cfd, edit_list, memtables_to_flush, prep_tracker);
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
      TEST_SYNC_POINT_CALLBACK(
          "MemTableList::TryInstallMemtableFlushResults:"
          "AfterComputeMinWalToKeep",
          nullptr);
      edit_list.push_back(&edit);

      const auto manifest_write_cb = [this, cfd, num_mem_to_flush, log_buffer,
                                      to_delete, mu](const Status& status) {
        RemoveMemTablesOrRestoreFlags(status, cfd, num_mem_to_flush, log_buffer,
                                      to_delete, mu);
      };
      if (write_edits) {
        // this can release and reacquire the mutex.
        s = vset->LogAndApply(cfd, read_options, write_options, edit_list, mu,
                              db_directory, /*new_descriptor_log=*/false,
                              /*column_family_options=*/nullptr,
                              manifest_write_cb);
      } else {
        // If write_edit is false (e.g: successful mempurge),
        // then remove old memtables, wake up manifest write queue threads,
        // and don't commit anything to the manifest file.
        RemoveMemTablesOrRestoreFlags(s, cfd, num_mem_to_flush, log_buffer,
                                      to_delete, mu);
        // Note: cfd->SetLogNumber is only called when a VersionEdit
        // is written to MANIFEST. When mempurge is succesful, we skip
        // this step, therefore cfd->GetLogNumber is always is
        // earliest log with data unflushed.
        // Notify new head of manifest write queue.
        // wake up all the waiting writers
        // TODO(bjlemaire): explain full reason WakeUpWaitingManifestWriters
        // needed or investigate more.
        vset->WakeUpWaitingManifestWriters();
      }
    }
  }
  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
void MemTableList::Add(ReadOnlyMemTable* m,
                       autovector<ReadOnlyMemTable*>* to_delete) {
  AddImpl(m, nullptr, to_delete);
}

void MemTableList::AddBefore(ReadOnlyMemTable* m, ReadOnlyMemTable* before,
                             autovector<ReadOnlyMemTable*>* to_delete) {
  assert(before != nullptr);
  AddImpl(m, before, to_delete);
}

void MemTableList::AddImpl(ReadOnlyMemTable* m, ReadOnlyMemTable* before,
                           autovector<ReadOnlyMemTable*>* to_delete) {
  assert(static_cast<int>(current_->memlist_.size()) >= num_flush_not_started_);
  InstallNewVersion();
  // The caller transfers its reference on m to the immutable list, so this
  // method does not acquire another reference.
  if (before == nullptr) {
    current_->Add(m, to_delete);
  } else {
    current_->AddBefore(m, before, to_delete);
  }
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
}

bool MemTableList::TrimHistory(autovector<ReadOnlyMemTable*>* to_delete,
                               size_t usage) {
  // Check if history trim is needed first, so that we can avoid installing a
  // new MemTableListVersion without installing a SuperVersion (installed based
  // on return value of this function).
  if (!current_->HistoryShouldBeTrimmed(usage)) {
    ResetTrimHistoryNeeded();
    return false;
  }
  InstallNewVersion();
  bool ret = current_->TrimHistory(to_delete, usage);
  assert(ret);
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
  return ret;
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::WBMTrackedUnflushedMemTablesMemoryUsage() const {
  size_t total_size = 0;
  for (const ReadOnlyMemTable* memtable : current_->memlist_) {
    total_size += memtable->WBMTrackedMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::WBMTrackedUnstartedMemTablesMemoryUsage() const {
  size_t total_size = 0;
  for (const ReadOnlyMemTable* memtable : current_->memlist_) {
    if (!memtable->flush_in_progress_) {
      total_size += memtable->WBMTrackedMemoryUsage();
    }
  }
  return total_size;
}

size_t MemTableList::ApproximateMemoryUsage() { return current_memory_usage_; }

size_t MemTableList::MemoryAllocatedBytesExcludingLast() const {
  const size_t usage = current_memory_allocted_bytes_excluding_last_.load(
      std::memory_order_relaxed);
  return usage;
}

bool MemTableList::HasHistory() const {
  const bool has_history = current_has_history_.load(std::memory_order_relaxed);
  return has_history;
}

void MemTableList::UpdateCachedValuesFromMemTableListVersion() {
  const size_t total_memtable_size =
      current_->MemoryAllocatedBytesExcludingLast();
  current_memory_allocted_bytes_excluding_last_.store(
      total_memtable_size, std::memory_order_relaxed);

  const bool has_history = current_->HasHistory();
  current_has_history_.store(has_history, std::memory_order_relaxed);
}

uint64_t MemTableList::ApproximateOldestKeyTime() const {
  if (!current_->memlist_.empty()) {
    return current_->memlist_.back()->ApproximateOldestKeyTime();
  }
  return std::numeric_limits<uint64_t>::max();
}

void MemTableList::InstallNewVersion() {
  if (current_->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
    current_ = new MemTableListVersion(&current_memory_usage_, *version);
    current_->SetID(++last_memtable_list_version_id_);
    current_->Ref();
    version->Unref();
  }
}

void MemTableList::RemoveMemTablesOrRestoreFlags(
    const Status& s, ColumnFamilyData* cfd, size_t num_mem_to_flush,
    LogBuffer* log_buffer, autovector<ReadOnlyMemTable*>* to_delete,
    InstrumentedMutex* mu) {
  assert(mu);
  mu->AssertHeld();
  assert(to_delete);
  // we will be changing the version in the next code path,
  // so we better create a new one, since versions are immutable
  InstallNewVersion();

  // All the later memtables that have the same filenum
  // are part of the same batch. They can be committed now.
  uint64_t mem_id = 1;  // how many memtables have been flushed.

  // commit new state only if the column family is NOT dropped.
  // The reason is as follows (refer to
  // ColumnFamilyTest.FlushAndDropRaceCondition).
  // If the column family is dropped, then according to LogAndApply, its
  // corresponding flush operation is NOT written to the MANIFEST. This
  // means the DB is not aware of the L0 files generated from the flush.
  // By committing the new state, we remove the memtable from the memtable
  // list. Creating an iterator on this column family will not be able to
  // read full data since the memtable is removed, and the DB is not aware
  // of the L0 files, causing MergingIterator unable to build child
  // iterators. RocksDB contract requires that the iterator can be created
  // on a dropped column family, and we must be able to
  // read full data as long as column family handle is not deleted, even if
  // the column family is dropped.
  if (s.ok() && !cfd->IsDropped()) {  // commit new state
    while (num_mem_to_flush-- > 0) {
      ReadOnlyMemTable* m = current_->memlist_.back();
      // TODO: The logging can be redundant when we flush multiple memtables
      // into one SST file. We should only check the edit_ of the oldest
      // memtable in the group in that case.
      if (m->edit_.GetBlobFileAdditions().empty()) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Level-0 commit flush result of table #%" PRIu64
                         ": memtable #%" PRIu64 " done",
                         cfd->GetName().c_str(), m->file_number_, mem_id);
      } else {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Level-0 commit flush result of table #%" PRIu64
                         " (+%zu blob files)"
                         ": memtable #%" PRIu64 " done",
                         cfd->GetName().c_str(), m->file_number_,
                         m->edit_.GetBlobFileAdditions().size(), mem_id);
      }

      assert(m->file_number_ > 0);
      current_->Remove(m, to_delete);
      UpdateCachedValuesFromMemTableListVersion();
      ResetTrimHistoryNeeded();
      ++mem_id;
    }
  } else {
    for (auto it = current_->memlist_.rbegin(); num_mem_to_flush-- > 0; ++it) {
      ReadOnlyMemTable* m = *it;
      // commit failed. setup state so that we can flush again.
      if (m->edit_.GetBlobFileAdditions().empty()) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "Level-0 commit table #%" PRIu64 ": memtable #%" PRIu64
                         " failed",
                         m->file_number_, mem_id);
      } else {
        ROCKS_LOG_BUFFER(log_buffer,
                         "Level-0 commit table #%" PRIu64
                         " (+%zu blob files)"
                         ": memtable #%" PRIu64 " failed",
                         m->file_number_,
                         m->edit_.GetBlobFileAdditions().size(), mem_id);
      }

      m->flush_completed_ = false;
      m->flush_in_progress_ = false;
      m->edit_.Clear();
      num_flush_not_started_++;
      m->file_number_ = 0;
      imm_flush_needed.store(true, std::memory_order_release);
      ++mem_id;
    }
  }
}

uint64_t MemTableList::PrecomputeMinLogContainingPrepSection(
    const std::unordered_set<ReadOnlyMemTable*>* memtables_to_flush) const {
  uint64_t min_log = 0;

  for (auto& m : current_->memlist_) {
    if (memtables_to_flush && memtables_to_flush->count(m)) {
      continue;
    }

    auto log = m->GetMinLogContainingPrepSection();

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

// Commit a successful atomic flush in the manifest file.
Status InstallMemtableAtomicFlushResults(
    const autovector<MemTableList*>* imm_lists,
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
    VersionSet* vset, LogsWithPrepTracker* prep_tracker, InstrumentedMutex* mu,
    const autovector<FileMetaData*>& file_metas,
    const autovector<std::list<std::unique_ptr<FlushJobInfo>>*>&
        committed_flush_jobs_info,
    autovector<ReadOnlyMemTable*>* to_delete, FSDirectory* db_directory,
    LogBuffer* log_buffer) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  mu->AssertHeld();

  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);

  size_t num = mems_list.size();
  assert(cfds.size() == num);
  if (imm_lists != nullptr) {
    assert(imm_lists->size() == num);
  }
  if (num == 0) {
    return Status::OK();
  }

  for (size_t k = 0; k != num; ++k) {
#ifndef NDEBUG
    const auto* imm =
        (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
    if (!mems_list[k]->empty()) {
      assert((*mems_list[k])[0]->GetID() == imm->GetEarliestMemTableID());
    }
#endif
    assert(nullptr != file_metas[k]);
    for (size_t i = 0; i != mems_list[k]->size(); ++i) {
      assert(i == 0 || (*mems_list[k])[i]->GetEdits()->NumEntries() == 0);
      (*mems_list[k])[i]->SetFlushCompleted(true);
      (*mems_list[k])[i]->SetFileNumber(file_metas[k]->fd.GetNumber());
    }
    if (committed_flush_jobs_info[k]) {
      assert(!mems_list[k]->empty());
      assert((*mems_list[k])[0]);
      std::unique_ptr<FlushJobInfo> flush_job_info =
          (*mems_list[k])[0]->ReleaseFlushJobInfo();
      committed_flush_jobs_info[k]->push_back(std::move(flush_job_info));
    }
  }

  Status s;

  autovector<autovector<VersionEdit*>> edit_lists;
  uint32_t num_entries = 0;
  for (const auto mems : mems_list) {
    assert(mems != nullptr);
    autovector<VersionEdit*> edits;
    assert(!mems->empty());
    edits.emplace_back((*mems)[0]->GetEdits());
    ++num_entries;
    edit_lists.emplace_back(edits);
  }

  WalNumber min_wal_number_to_keep = 0;
  if (vset->db_options()->allow_2pc) {
    min_wal_number_to_keep = PrecomputeMinLogNumberToKeep2PC(
        vset, cfds, edit_lists, mems_list, prep_tracker);
  } else {
    min_wal_number_to_keep =
        PrecomputeMinLogNumberToKeepNon2PC(vset, cfds, edit_lists);
  }

  VersionEdit wal_deletion;
  wal_deletion.SetMinLogNumberToKeep(min_wal_number_to_keep);
  if (vset->db_options()->track_and_verify_wals_in_manifest &&
      min_wal_number_to_keep > vset->GetWalSet().GetMinWalNumberToKeep()) {
    wal_deletion.DeleteWalsBefore(min_wal_number_to_keep);
  }
  edit_lists.back().push_back(&wal_deletion);
  ++num_entries;

  // Mark the version edits as an atomic group if the number of version edits
  // exceeds 1.
  if (cfds.size() > 1) {
    for (size_t i = 0; i < edit_lists.size(); i++) {
      assert((edit_lists[i].size() == 1) ||
             ((edit_lists[i].size() == 2) && (i == edit_lists.size() - 1)));
      for (auto& e : edit_lists[i]) {
        e->MarkAtomicGroup(--num_entries);
      }
    }
    assert(0 == num_entries);
  }

  // this can release and reacquire the mutex.
  s = vset->LogAndApply(cfds, read_options, write_options, edit_lists, mu,
                        db_directory);

  for (size_t k = 0; k != cfds.size(); ++k) {
    auto* imm = (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
    imm->InstallNewVersion();
  }

  if (s.ok() || s.IsColumnFamilyDropped()) {
    for (size_t i = 0; i != cfds.size(); ++i) {
      if (cfds[i]->IsDropped()) {
        continue;
      }
      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
      for (auto m : *mems_list[i]) {
        assert(m->GetFileNumber() > 0);
        uint64_t mem_id = m->GetID();

        const VersionEdit* const edit = m->GetEdits();
        assert(edit);

        if (edit->GetBlobFileAdditions().empty()) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit table #%" PRIu64
                           ": memtable #%" PRIu64 " done",
                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
                           mem_id);
        } else {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit table #%" PRIu64
                           " (+%zu blob files)"
                           ": memtable #%" PRIu64 " done",
                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
                           edit->GetBlobFileAdditions().size(), mem_id);
        }

        imm->current_->Remove(m, to_delete);
        imm->UpdateCachedValuesFromMemTableListVersion();
        imm->ResetTrimHistoryNeeded();
      }
    }
  } else {
    for (size_t i = 0; i != cfds.size(); ++i) {
      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
      for (auto m : *mems_list[i]) {
        uint64_t mem_id = m->GetID();

        const VersionEdit* const edit = m->GetEdits();
        assert(edit);

        if (edit->GetBlobFileAdditions().empty()) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit table #%" PRIu64
                           ": memtable #%" PRIu64 " failed",
                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
                           mem_id);
        } else {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Level-0 commit table #%" PRIu64
                           " (+%zu blob files)"
                           ": memtable #%" PRIu64 " failed",
                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
                           edit->GetBlobFileAdditions().size(), mem_id);
        }

        m->SetFlushCompleted(false);
        m->SetFlushInProgress(false);
        m->GetEdits()->Clear();
        m->SetFileNumber(0);
        imm->num_flush_not_started_++;
      }
      imm->imm_flush_needed.store(true, std::memory_order_release);
    }
  }

  return s;
}

void MemTableList::RemoveOldMemTables(
    uint64_t log_number, autovector<ReadOnlyMemTable*>* to_delete) {
  assert(to_delete != nullptr);
  InstallNewVersion();
  auto& memlist = current_->memlist_;
  autovector<ReadOnlyMemTable*> old_memtables;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    ReadOnlyMemTable* mem = *it;
    if (mem->GetNextLogNumber() > log_number) {
      break;
    }
    old_memtables.push_back(mem);
  }

  for (auto it = old_memtables.begin(); it != old_memtables.end(); ++it) {
    ReadOnlyMemTable* mem = *it;
    current_->Remove(mem, to_delete);
    --num_flush_not_started_;
    if (0 == num_flush_not_started_) {
      imm_flush_needed.store(false, std::memory_order_release);
    }
  }

  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
}

VersionEdit MemTableList::GetEditForDroppingCurrentVersion(
    const ColumnFamilyData* cfd, VersionSet* vset,
    LogsWithPrepTracker* prep_tracker) const {
  assert(cfd);
  auto& memlist = current_->memlist_;
  if (memlist.empty()) {
    return VersionEdit();
  }

  uint64_t max_next_log_number = 0;
  autovector<VersionEdit*> edit_list;
  autovector<ReadOnlyMemTable*> memtables_to_drop;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    ReadOnlyMemTable* m = *it;
    memtables_to_drop.push_back(m);
    max_next_log_number = std::max(m->GetNextLogNumber(), max_next_log_number);
  }

  // Check the obsoleted MemTables' impact on WALs related to DB's recovery (min
  // log number to keep, a delta of WAL files to delete).
  VersionEdit edit_with_log_number;
  edit_with_log_number.SetPrevLogNumber(0);
  edit_with_log_number.SetLogNumber(max_next_log_number);
  edit_list.push_back(&edit_with_log_number);
  VersionEdit edit = GetDBRecoveryEditForObsoletingMemTables(
      vset, *cfd, edit_list, memtables_to_drop, prep_tracker);

  // Set fields related to the column family's recovery.
  edit.SetColumnFamily(cfd->GetID());
  edit.SetPrevLogNumber(0);
  edit.SetLogNumber(max_next_log_number);
  return edit;
}

}  // namespace ROCKSDB_NAMESPACE
