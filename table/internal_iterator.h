// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <string>

#include "db/dbformat.h"
#include "file/readahead_file_info.h"
#include "rocksdb/advanced_iterator.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

class PinnedIteratorsManager;
class Version;

template <class TValue>
class InternalIteratorBase : public Cleanable {
 public:
  InternalIteratorBase() {}

  // No copying allowed
  InternalIteratorBase(const InternalIteratorBase&) = delete;
  InternalIteratorBase& operator=(const InternalIteratorBase&) = delete;
  InternalIteratorBase(InternalIteratorBase&&) = delete;
  InternalIteratorBase& operator=(InternalIteratorBase&&) = delete;

  virtual ~InternalIteratorBase() {}

  // This iterator will only process range tombstones with sequence
  // number <= `read_seqno`.
  // Noop for most child classes.
  // For range tombstone iterators (TruncatedRangeDelIterator,
  // FragmentedRangeTombstoneIterator), will only return range tombstones with
  // sequence number <= `read_seqno`. For LevelIterator, it may open new table
  // files and create new range tombstone iterators during scanning. It will use
  // `read_seqno` as the sequence number for creating new range tombstone
  // iterators.
  virtual void SetRangeDelReadSeqno(SequenceNumber /* read_seqno */) {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  // Always returns false if !status().ok().
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  // All Seek*() methods clear any error status() that the iterator had prior to
  // the call; after the seek, status() indicates only the error (if any) that
  // happened during the seek, not any past errors.
  // 'target' contains user timestamp if timestamp is enabled.
  virtual void Seek(const Slice& target) = 0;

  // Position at the first key in the source that at or before target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  virtual void SeekForPrev(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the next entry in the source, and return result. Iterator
  // implementation should override this method to help methods inline better,
  // or when UpperBoundCheckResult() is non-trivial.
  // REQUIRES: Valid()
  virtual bool NextAndGetResult(IterateResult* result) {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      // Default may_be_out_of_upper_bound to true to avoid unnecessary virtual
      // call. If an implementation has non-trivial UpperBoundCheckResult(),
      // it should also override NextAndGetResult().
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = false;
      assert(UpperBoundCheckResult() != IterBoundCheck::kOutOfBound);
    }
    return is_valid;
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Returns the approximate write time of this entry, which is deduced from
  // sequence number if sequence number to time mapping is available.
  // The default implementation returns maximum uint64_t and that indicates the
  // write time is unknown.
  virtual uint64_t write_unix_time() const {
    return std::numeric_limits<uint64_t>::max();
  }

  // Return user key for the current entry.
  // REQUIRES: Valid()
  virtual Slice user_key() const { return ExtractUserKey(key()); }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  // REQUIRES: PrepareValue() has been called if needed (see PrepareValue()).
  virtual TValue value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const = 0;

  // For some types of iterators, sometimes Seek()/Next()/SeekForPrev()/etc may
  // load key but not value (to avoid the IO cost of reading the value from disk
  // if it won't be not needed). This method loads the value in such situation.
  //
  // Needs to be called before value() at least once after each iterator
  // movement (except if IterateResult::value_prepared = true), for iterators
  // created with allow_unprepared_value = true.
  //
  // Returns false if an error occurred; in this case Valid() is also changed
  // to false, and status() is changed to non-ok.
  // REQUIRES: Valid()
  virtual bool PrepareValue() { return true; }

  // Keys return from this iterator can be smaller than iterate_lower_bound.
  virtual bool MayBeOutOfLowerBound() { return true; }

  // If the iterator has checked the key against iterate_upper_bound, returns
  // the result here. The function can be used by user of the iterator to skip
  // their own checks. If Valid() = true, IterBoundCheck::kUnknown is always
  // a valid value. If Valid() = false, IterBoundCheck::kOutOfBound indicates
  // that the iterator is filtered out by upper bound checks.
  virtual IterBoundCheck UpperBoundCheckResult() {
    return IterBoundCheck::kUnknown;
  }

  // Pass the PinnedIteratorsManager to the Iterator, most Iterators don't
  // communicate with PinnedIteratorsManager so default implementation is no-op
  // but for Iterators that need to communicate with PinnedIteratorsManager
  // they will implement this function and use the passed pointer to communicate
  // with PinnedIteratorsManager.
  virtual void SetPinnedItersMgr(PinnedIteratorsManager* /*pinned_iters_mgr*/) {
  }

  // If true, this means that the Slice returned by key() is valid as long as
  // PinnedIteratorsManager::ReleasePinnedData is not called and the
  // Iterator is not deleted.
  //
  // IsKeyPinned() is guaranteed to always return true if
  //  - Iterator is created with ReadOptions::pin_data = true
  //  - DB tables were created with BlockBasedTableOptions::use_delta_encoding
  //    set to false.
  virtual bool IsKeyPinned() const { return false; }

  // If true, this means that the Slice returned by value() is valid as long as
  // PinnedIteratorsManager::ReleasePinnedData is not called and the
  // Iterator is not deleted.
  // REQUIRES: Same as for value().
  virtual bool IsValuePinned() const { return false; }

  // If true, no key() or value() slice this iterator has returned depends on
  // memory it owns or keeps alive (e.g. through cleanups); the slices stay
  // valid as long as the iterator's data source (e.g. the table reader) does,
  // and that source is guaranteed to outlive the Version the iterator was
  // created from. Such an iterator may be deleted while pinning is enabled
  // instead of being handed to the PinnedIteratorsManager.
  virtual bool IsSafeToDropWhilePinned() const { return false; }

  virtual Status GetProperty(std::string /*prop_name*/, std::string* /*prop*/) {
    return Status::NotSupported("");
  }

  // When iterator moves from one file to another file at same level, new file's
  // readahead state (details of last block read) is updated with previous
  // file's readahead state. This way internal readahead_size of Prefetch Buffer
  // doesn't start from scratch and can fall back to 8KB with no prefetch if
  // reads are not sequential.
  //
  // Default implementation is no-op and its implemented by iterators.
  virtual void GetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) {}

  // Default implementation is no-op and its implemented by iterators.
  virtual void SetReadaheadState(ReadaheadFileInfo* /*readahead_file_info*/) {}

  // When used under merging iterator, LevelIterator treats file boundaries
  // as sentinel keys to prevent it from moving to next SST file before range
  // tombstones in the current SST file are no longer needed. This method makes
  // it cheap to check if the current key is a sentinel key. This should only be
  // used by MergingIterator and LevelIterator for now.
  virtual bool IsDeleteRangeSentinelKey() const { return false; }

  // Prepares the iterator for the scan ranges in scan_opts (see
  // Iterator::Prepare()). scan_opts may be nullptr, meaning there are no
  // bounded scan ranges; implementations should then behave as a regular
  // iterator.
  virtual void Prepare(const MultiScanArgs* /*scan_opts*/) {}

  // Returns the current Version associated with the iterator, if any.
  // Overridden by ForwardIterator to provide the renewed Version to DBIter.
  virtual const Version* current_version() const { return nullptr; }

 protected:
  void SeekForPrevImpl(const Slice& target, const CompareInterface* cmp) {
    Seek(target);
    if (!Valid()) {
      SeekToLast();
    }
    while (Valid() && cmp->Compare(target, key()) < 0) {
      Prev();
    }
  }
};

using InternalIterator = InternalIteratorBase<Slice>;

// Return an empty iterator (yields nothing).
template <class TValue = Slice>
InternalIteratorBase<TValue>* NewEmptyInternalIterator();

// Return an empty iterator with the specified status.
template <class TValue = Slice>
InternalIteratorBase<TValue>* NewErrorInternalIterator(const Status& status);

// Return an empty iterator with the specified status, allocated arena.
template <class TValue = Slice>
InternalIteratorBase<TValue>* NewErrorInternalIterator(const Status& status,
                                                       Arena* arena);

}  // namespace ROCKSDB_NAMESPACE
