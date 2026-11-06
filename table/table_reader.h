//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>

#include "db/range_tombstone_fragmenter.h"
#if USE_COROUTINES
#include "folly/coro/Coroutine.h"
#include "folly/coro/Task.h"
#endif
#include "rocksdb/slice_transform.h"
#include "rocksdb/table_reader_caller.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/multiget_context.h"
#include "util/coro_utils.h"

namespace ROCKSDB_NAMESPACE {

class Iterator;
struct ParsedInternalKey;
class Slice;
class Arena;
struct ReadOptions;
struct TableProperties;
class GetContext;
class MultiGetContext;

// A Table (also referred to as SST) is a sorted map from strings to strings.
// Tables are immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization. Table readers are used
// for reading various types of table formats supported by rocksdb including
// BlockBasedTable, PlainTable and CuckooTable format.
class TableReader {
 public:
  virtual ~TableReader() = default;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // read_options: Must outlive the returned iterator.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        When destroying the iterator, the caller will not call "delete"
  //        but Iterator::~Iterator() directly. The destructor needs to destroy
  //        all the states but those allocated in arena.
  // skip_filters: disables checking the bloom filters even if they exist. This
  //               option is effective only for block-based table format.
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction
  virtual InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool skip_filters, TableReaderCaller caller,
      size_t compaction_readahead_size = 0,
      bool allow_unprepared_value = false) = 0;

  // Same as NewIterator(), for an iterator whose first operation will be
  // Seek(seek_target). Returns nullptr instead when the table can tell, from
  // the prefix filter alone, that the seek would find nothing in this table.
  // That filtered seek is recorded in statistics as if it ran on an iterator.
  // A returned iterator may skip the prefix check on its first Seek() because
  // it already ran here, so that Seek() must be the first operation and use
  // the same target and read_options.
  virtual InternalIterator* NewIteratorForSeek(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool skip_filters, TableReaderCaller caller,
      size_t compaction_readahead_size, bool allow_unprepared_value,
      const Slice& /*seek_target*/) {
    return NewIterator(read_options, prefix_extractor, arena, skip_filters,
                       caller, compaction_readahead_size,
                       allow_unprepared_value);
  }

  // read_options.snapshot needs to outlive this call.
  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& /*read_options*/) {
    return nullptr;
  }

  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      SequenceNumber /* read_seqno */, const Slice* /* timestamp */) {
    return nullptr;
  }

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  // TODO(peterd): Since this function is only used for approximate size
  // from beginning of file, reduce code duplication by removing this
  // function and letting ApproximateSize take optional start and end, so
  // that absolute start and end can be specified and optimized without
  // key / index work.
  virtual uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                                       const Slice& key,
                                       TableReaderCaller caller) = 0;

  // Given start and end keys, return the approximate data size in the file
  // between the keys. The returned value is in terms of file bytes, and so
  // includes effects like compression of the underlying data and applicable
  // portions of metadata including filters and indexes. Nullptr for start or
  // end (or both) indicates absolute start or end of the table.
  virtual uint64_t ApproximateSize(const ReadOptions& read_options,
                                   const Slice& start, const Slice& end,
                                   TableReaderCaller caller) = 0;

  struct Anchor {
    Anchor(const Slice& _user_key, size_t _range_size)
        : user_key(_user_key.ToStringView()), range_size(_range_size) {}
    std::string user_key;
    size_t range_size;
  };

  // Now try to return approximately 128 anchor keys.
  // The last one tends to be the largest key.
  virtual Status ApproximateKeyAnchors(const ReadOptions& /*read_options*/,
                                       std::vector<Anchor>& /*anchors*/) {
    return Status::NotSupported("ApproximateKeyAnchors() not supported.");
  }

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  virtual void SetupForCompaction() = 0;

  virtual std::shared_ptr<const TableProperties> GetTableProperties() const = 0;

  // Prepare work that can be done before the real Get()
  virtual void Prepare(const Slice& /*target*/) {}

  // Report an approximation of how much memory has been used.
  virtual size_t ApproximateMemoryUsage() const = 0;

  // Calls get_context->SaveValue() repeatedly, starting with
  // the entry found after a call to Seek(key), until it returns false.
  // May not make such a call if filter policy says that key is not present.
  //
  // get_context->MarkKeyMayExist needs to be called when it is configured to be
  // memory only and the key is not found in the block cache.
  //
  // readOptions is the options for the read
  // key is the key to search for
  // skip_filters: disables checking the bloom filters even if they exist. This
  //               option is effective only for block-based table format.
  virtual Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters = false) = 0;

#if USE_COROUTINES
  virtual folly::coro::Task<Status> GetCoroutine(
      const ReadOptions& readOptions, const Slice& key, GetContext* get_context,
      const SliceTransform* prefix_extractor, bool skip_filters = false) {
    co_return Get(readOptions, key, get_context, prefix_extractor,
                  skip_filters);
  }
#endif  // USE_COROUTINES

  // Use bloom filters in the table file, if present, to filter out keys. The
  // mget_range will be updated to skip keys that get a negative result from
  // the filter lookup.
  virtual Status MultiGetFilter(const ReadOptions& /*readOptions*/,
                                const SliceTransform* /*prefix_extractor*/,
                                MultiGetContext::Range* /*mget_range*/) {
    return Status::NotSupported();
  }

  virtual void MultiGet(const ReadOptions& readOptions,
                        const MultiGetContext::Range* mget_range,
                        const SliceTransform* prefix_extractor,
                        bool skip_filters = false) {
    for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
      *iter->s = Get(readOptions, iter->ikey, iter->get_context,
                     prefix_extractor, skip_filters);
    }
  }

#if USE_COROUTINES
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& readOptions, const MultiGetContext::Range* mget_range,
      const SliceTransform* prefix_extractor, bool skip_filters = false) {
    MultiGet(readOptions, mget_range, prefix_extractor, skip_filters);
    co_return;
  }
#endif  // USE_COROUTINES

  // Prefetch data corresponding to a give range of keys
  // Typically this functionality is required for table implementations that
  // persists the data on a non volatile storage medium like disk/SSD
  virtual Status Prefetch(const ReadOptions& /* read_options */,
                          const Slice* begin = nullptr,
                          const Slice* end = nullptr) {
    (void)begin;
    (void)end;
    // Default implementation is NOOP.
    // The child class should implement functionality when applicable
    return Status::OK();
  }

  // convert db file to a human readable form
  virtual Status DumpTable(WritableFile* /*out_file*/,
                           bool /*show_sequence_number_type*/ = false) {
    return Status::NotSupported("DumpTable() not supported");
  }

  // check whether there is corruption in this db file
  virtual Status VerifyChecksum(const ReadOptions& /*read_options*/,
                                TableReaderCaller /*caller*/,
                                bool /*meta_blocks_only*/ = false) {
    return Status::NotSupported("VerifyChecksum() not supported");
  }

  // Tell the reader that the file should now be obsolete, e.g. as a hint
  // to delete relevant cache entries on destruction. (It might not be safe
  // to "unpin" cache entries until destruction time.) NOTE: must be thread
  // safe because multiple table cache references might all mark this file as
  // obsolete when they are released (the last of which destroys this reader).
  virtual void MarkObsolete(uint32_t /*uncache_aggressiveness*/) {
    // no-op as default
  }
};

}  // namespace ROCKSDB_NAMESPACE
