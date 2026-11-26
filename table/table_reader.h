//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <functional>
#include <memory>
#include <vector>

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

struct BlobReference;
struct BlobReferenceGroup;

// Receives the references of requested groups in a form the caller chooses, for
// example a compact copy (see BlobReferenceGroupRead::sink).
class BlobReferenceSink {
 public:
  virtual ~BlobReferenceSink() = default;
  virtual size_t size() const = 0;

  // How many more references Append() accepts.
  virtual size_t Room() const = 0;
  virtual Status Append(const BlobReference& reference) = 0;

  // Drops the references after the first `size`.
  virtual void Truncate(size_t size) = 0;

  // A key for the storage that Append() writes to. ReadBlobReferences()
  // checks all requests with the same identity against one shared Room().
  // Override it only when two sink objects write to the same storage.
  virtual const void* identity() const { return this; }
};

// One request of a batch read of exact blob references from one SST: a group
// key and the vector (or sink) its references are appended to.
struct BlobReferenceGroupRead {
  // Only the group key (encoding class, generation number, origin) is used.
  // With `whole_generation`, only the generation number is used.
  const BlobReferenceGroup* group = nullptr;
  // Reads every group of the SST with this generation number (any class, any
  // origin) into one output. The found totals are the sum over those groups.
  // The status is NotFound if there is no such group. Each group is read like
  // its own request. On an error, only the failed group's references are
  // dropped. The output keeps the references of the other groups of the
  // generation number, earlier and later, and the status is the error of the
  // first failed group. Requests of a batch are either all whole-generation
  // (sorted by generation number, unique) or all single-group requests (each
  // names one class, generation number and origin).
  bool whole_generation = false;
  // Must have room for the group's references. Used when `sink` is null.
  std::vector<BlobReference>* output = nullptr;
  // Must have room for the group's references. Before the read, each group is
  // checked against Room() (or the free capacity of `output`), minus the
  // references of earlier requests with the same sink. A group with more
  // references gets Aborted, and nothing is appended for it. An Append() error
  // fails the group like a decode error.
  BlobReferenceSink* sink = nullptr;
  // The group's totals from this SST's reference index. Each read sets them
  // to 0 before it reads the index, then sets them when the SST has the
  // group. After a failed call they are not meaningful, like the status.
  uint64_t found_reference_count = 0;
  uint64_t found_stored_value_bytes = 0;
  uint64_t found_record_bytes = 0;
  // Set by the read: OK, or the error of this group only. See
  // TableReader::ReadBlobReferences() for the error codes. The caller must
  // check it, except after the whole call fails (see ReadBlobReferences()).
  // A caller that builds a request but does not read it (for example, it
  // stops before the call) must mark this status as checked, because a
  // default Status starts unchecked.
  Status status;
};

// Largest number of on-disk bytes of blob reference data blocks that one
// MultiRead batch holds in memory (256 KB). It is the size of the read buffer
// that ReadBlobReferences() allocates. 256 KB is an estimate (not measured): it
// holds many data blocks per read but keeps the buffer small. With MultiRead, a
// single data block whose on-disk size (with its trailer) is larger than this
// fails the read with Corruption. With mmap or direct I/O, blocks are read one
// at a time and this limit does not apply.
inline constexpr size_t kBlobReferenceReadBatchBytes = 256 * 1024;

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

  // The blob reference census uses this to learn which blob files an SST
  // references when it has no summary in memory. It reads the exact blob
  // reference index block of this SST and sets `*groups` to the groups that are
  // not kDirect, in key order. The census does not track kDirect references.
  // Only the index block is read: it holds the totals of every group, so no
  // data block is needed.
  // - `expected_descriptor` is the descriptor that the MANIFEST holds for this
  //   SST. A different descriptor in the table properties is Corruption.
  // - When set, `before_allocation` gets the size in bytes of the group buffer
  //   before the buffer is allocated, so the caller can charge the memory
  //   first. If it returns non-OK, this function returns that status.
  //
  // The default returns NotSupported, because enable_blob_indirection requires
  // a block-based table.
  virtual Status ReadBlobReferenceGroups(
      const ReadOptions& read_options, const Slice& expected_descriptor,
      const std::function<Status(size_t buffer_bytes)>& before_allocation,
      std::vector<BlobReferenceGroup>* groups) const {
    (void)read_options;
    (void)expected_descriptor;
    (void)before_allocation;
    (void)groups;
    return Status::NotSupported(
        "Exact blob reference metadata is not supported");
  }

  // Reads the references of several groups of this SST from its exact reference
  // metadata. Each request is a BlobReferenceGroupRead. The batch form lets one
  // call read several groups that share data blocks, so that each block is
  // read and decompressed only once. Indirect blob GC reads, from each SST,
  // all generation numbers of its inputs that the SST holds, in one batch of
  // whole-generation requests.
  //
  // Input: single-group requests must be sorted by group key (encoding class,
  // generation number, origin). Whole-generation requests must be sorted by
  // generation number. A batch has no repeats and does not mix the two kinds;
  // otherwise InvalidArgument. Each output must already have capacity for its
  // group, so that its owner can charge the memory before this call.
  //
  // Reads: it decodes the reference index once and reads each needed data block
  // once. Adjacent blocks are read together, in MultiRead batches of at most
  // kBlobReferenceReadBatchBytes (one block at a time with mmap or direct I/O).
  //
  // Index memory report: when set, `after_index_read` gets the memory in bytes
  // of the decoded reference index, right after it is decoded and before any
  // data block is read. The caller can charge it there. The size grows with the
  // number of groups and blocks of the SST, so a fixed estimate can be too
  // small. If the callback returns non-OK, this function returns that status.
  //
  // Results: each found group reports its index totals in the found_* fields.
  // A request gets an error status in the cases below, and the other requests
  // continue:
  // - NotFound: the SST does not have the group.
  // - Aborted: the group does not fit its output.
  // - Corruption: a block of the group is corrupt, or the references read for
  //   the group do not match its index totals.
  // Errors of the whole call: if the requests are not sorted, or the
  // descriptor or the reference index cannot be read or decoded, the call
  // returns the error before it reads any group. The request statuses are
  // then not meaningful, and every output is unchanged. If a data block read
  // fails (for example with an I/O error), the call returns that error and
  // also sets it on every request that has not finished. A request that has
  // already finished keeps its status and its output. Callers must check the
  // return value before they read a request status. When the call fails, it
  // marks every request status as checked, so a caller that stops at the call
  // error does not need to read them. After a successful call, the caller
  // must check every request status.
  //
  // A failed request drops only the references it appended. A request that
  // fails before it appends anything leaves the output unchanged. So when
  // several requests or calls share one output, the references that earlier
  // requests or the caller appended stay. For a whole-generation request, this
  // holds for each group, not for the whole request (see
  // BlobReferenceGroupRead::whole_generation).
  //
  // The default returns NotSupported, because SSTs with exact reference
  // metadata must be block-based tables.
  virtual Status ReadBlobReferences(
      const ReadOptions& read_options, const Slice& expected_descriptor,
      const std::function<Status(size_t index_bytes)>& after_index_read,
      std::vector<BlobReferenceGroupRead>* requests) const {
    (void)read_options;
    (void)expected_descriptor;
    (void)after_index_read;
    // The call fails, so mark the request statuses as checked (see above).
    if (requests != nullptr) {
      for (BlobReferenceGroupRead& request : *requests) {
        request.status.PermitUncheckedError();
      }
    }
    return Status::NotSupported(
        "Exact blob reference metadata is not supported");
  }

  // Reads and validates every reference data block without scanning the table's
  // key/value entries, including blocks of legacy direct references.
  virtual Status VerifyBlobReferenceMetadata(
      const ReadOptions& read_options, const Slice& expected_descriptor) const {
    (void)read_options;
    (void)expected_descriptor;
    return Status::NotSupported(
        "Exact blob reference metadata is not supported");
  }

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
