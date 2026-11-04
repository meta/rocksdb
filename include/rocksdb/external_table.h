//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator_base.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/types_util.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
// The interfaces defined in this file are subject to change at any time without
// warning.

enum class ExternalTableMode {
  // Store only Put entries with sequence number zero. RocksDB may assign one
  // global sequence number to every entry in an ingested file.
  kOnlyZeroSeqnoAndPuts,
  // Preserve entry types and sequence numbers.
  kFull,
};

class ExternalTableIteratorBase;
template <ExternalTableMode Mode>
class ExternalTableReaderBase;
template <ExternalTableMode Mode>
class CoroExternalTableReaderBase;

class ExternalTableBuilderBase;

template <ExternalTableMode Mode>
class ExternalTableFactoryBase;

// Receives entries found by a full-mode reader. This object is owned by RocksDB
// and is valid only for the duration of that call.
class ExternalTableGetContext {
 public:
  ExternalTableGetContext(const ExternalTableGetContext&) = delete;
  ExternalTableGetContext& operator=(const ExternalTableGetContext&) = delete;
  ExternalTableGetContext(ExternalTableGetContext&&) = delete;
  ExternalTableGetContext& operator=(ExternalTableGetContext&&) = delete;

  // Saves one stored entry encoded as a RocksDB internal key and sets
  // continue_reading to indicate whether the reader should provide the next
  // older entry for the same user key. ParseEntry() can be used to decode the
  // key. On success, value is consumed and reset; its pinning cleanup may
  // transfer to RocksDB.
  virtual Status Save(const Slice& internal_key, PinnableSlice* value,
                      bool* continue_reading) = 0;

 protected:
  ExternalTableGetContext() = default;
  ~ExternalTableGetContext() = default;
};

// One full-mode point lookup in an ExternalTableMultiGetContext. lookup_key is
// an internal lookup boundary. Its backing storage and the non-null
// get_context are owned by RocksDB and must not be retained after MultiGet().
struct ExternalTableMultiGetRequest {
  Slice lookup_key;
  ExternalTableGetContext* get_context = nullptr;
};

// Provides the full-mode requests in one RocksDB MultiGet batch.
class ExternalTableMultiGetContext {
 public:
  ExternalTableMultiGetContext(const ExternalTableMultiGetContext&) = delete;
  ExternalTableMultiGetContext& operator=(const ExternalTableMultiGetContext&) =
      delete;
  ExternalTableMultiGetContext(ExternalTableMultiGetContext&&) = delete;
  ExternalTableMultiGetContext& operator=(ExternalTableMultiGetContext&&) =
      delete;

  virtual size_t Size() const = 0;
  virtual ExternalTableMultiGetRequest GetRequest(size_t index) = 0;

 protected:
  ExternalTableMultiGetContext() = default;
  ~ExternalTableMultiGetContext() = default;
};

// This file defines an interface for plugging in an external table
// into RocksDB. The external table reader will be used instead of the
// BlockBasedTable to load and query sst files.
//
// The ExternalTable* aliases use kOnlyZeroSeqnoAndPuts mode:
// - Readers and builders receive user keys for point entries.
// - Every point entry is a Put with sequence number zero. Implementations may
//   also store sequence-zero range deletions in a separate range-deletion
//   block.
// - When used in a DB, this mode supports ingestion-only workloads, including
//   overlapping files in multiple levels through file-wide global sequence
//   numbers and range deletions. It does not support live writes or compaction
//   output.
//
// The FullExternalTable* aliases use kFull mode:
// - Readers and builders receive complete RocksDB internal keys for point
//   entries and must preserve their order and encoding.
// - Repeated user keys and nonzero sequence numbers are supported.
// - Put, Delete, SingleDelete, Merge, BlobIndex, and WideColumnEntity entries
//   are supported. Implementations may opt in to RangeDeletion support through
//   the range-deletion block APIs. User-defined timestamps are not supported.
// - External tables can coexist with live writes and participate in multi-level
//   LSMs.
//
// ParseEntry() can be used to decode stored internal keys.
//
// The mode is part of an implementation's file-format contract. An
// implementation must only open a file in the mode used to create it, unless
// it can identify and handle both encodings itself.
//
// The external table can support one or both of the following layouts -
// 1. Total order seek - All the keys in the files are in sorted order, and a
//    user can seek to the first, last, or any key in between and iterate
//    forwards or backwards till the end of the range. To support this mode,
//    the implementation needs to use the comparator passed in
//    ExternalTableOptions to enforce the key ordering. Full-mode
//    implementations use internal_key_comparator for internal keys. The
//    prefix_extractor in ExternalTableOptions and the ExternalTableReader
//    interfaces can be ignored.
// 2. Prefix seek - In this mode, the prefix_extractor is used to extract the
//    prefix from a key. All the keys sharing the same prefix are ordered in
//    ascending order according to the comparator. However, no specific
//    ordering is required across prefixes. Users can scan keys by seeking
//    to a specific key inside a prefix, and iterate forwards or backwards
//    within the prefix. The prefix_same_as_start flag in ReadOptions will
//    be true.
// 3. Both - If supporting both of the above, a user can seek inside a prefix
//    and iterate beyond the prefix. The prefix_same_as_start in ReadOptions
//    will be false. Additionally, the total_order_seek flag can be set to
//    true to seek to the first non-empty prefix (as determined by the key
//    order) if the seek prefix is empty.
//
// Many of the options in ReadOptions and WriteOptions may not be relevant to
// the external table implementation.
// TODO: Specify which options are relevant

// In basic mode, IteratorBase targets and keys are user keys. In full mode,
// seek targets are internal lookup boundaries and returned keys are stored
// internal keys.
class ExternalTableIteratorBase : public IteratorBase {
 public:
  ~ExternalTableIteratorBase() override = default;

  // This can optionally be called to prepare the iterator for a series
  // of scans. The scan_opts parameter specifies the order of scans to
  // follow, as well as the limits for those scans. After calling this,
  // the caller will Seek() the iterator to successive start keys in scan_opts.
  //
  // If Prepare() is called again with a different scan_opts pointer, it
  // means the iterator will be reused for a new multi scan. If scan_opts
  // is null, then the previous Prepare() can be discarded.
  //
  // The caller guarantees the lifetime of scan_opts until its either cleared
  // or replaced by another Prepare().
  // TODO: Update the contract to trim the scan_opts range to only include
  // scans that potentially intersect the file key range.
  //
  // If the sequence of Seeks is interrupted by seeking to some other target
  // key, then the iterator is free to discard anything done during Prepare.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Similar to Next(), except it also fills the result and returns whether
  // the iterator is on a valid key or not. result.key uses the same key
  // representation as key().
  virtual bool NextAndGetResult(IterateResult* result) = 0;

  // Prepares the value if its lazily materialized. The implementation can
  // request that this be called by setting value_prepared to false in
  // IterateResult. Next() should always implicitly materialize the
  // value.
  bool PrepareValue() override = 0;

  // Return the current key's value
  virtual Slice value() const = 0;

  // Return the current position bounds check result - kInbound if the
  // position is a valid key, kOutOfBound if the key is out of bound (i.e
  // scan has terminated), or kUnknown if end of file.
  virtual IterBoundCheck UpperBoundCheckResult() = 0;
};

using ExternalTableIterator = ExternalTableIteratorBase;
using FullExternalTableIterator = ExternalTableIteratorBase;

template <ExternalTableMode Mode>
class ExternalTableReaderLookupBase;

template <>
class ExternalTableReaderLookupBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts> {
 public:
  using GetArgument = PinnableSlice*;
  using MultiGetArgument = std::vector<PinnableSlice>*;

  virtual Status Get(const ReadOptions& read_options, const Slice& key,
                     const SliceTransform* prefix_extractor,
                     PinnableSlice* result) = 0;

  // Point lookup the given vector of user keys and return one value and status
  // per key.
  virtual void MultiGet(const ReadOptions& read_options,
                        const std::vector<Slice>& keys,
                        const SliceTransform* prefix_extractor,
                        std::vector<PinnableSlice>* results,
                        std::vector<Status>* statuses) {
    results->resize(keys.size());
    statuses->resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      (*statuses)[i] =
          Get(read_options, keys[i], prefix_extractor, &(*results)[i]);
    }
  }

 protected:
  ~ExternalTableReaderLookupBase() = default;
};

template <>
class ExternalTableReaderLookupBase<ExternalTableMode::kFull> {
 public:
  using GetArgument = ExternalTableGetContext*;
  using MultiGetArgument = ExternalTableMultiGetContext*;

  // Point lookup an internal boundary. The implementation calls Save() with
  // stored entries for the same user key, in internal-key order, until
  // continue_reading is false.
  virtual Status Get(const ReadOptions& read_options, const Slice& key,
                     const SliceTransform* prefix_extractor,
                     ExternalTableGetContext* context) = 0;

  // Point lookup every request and resize statuses to exactly Size() entries.
  // NotFound indicates that this table did not contain the corresponding key.
  virtual void MultiGet(const ReadOptions& read_options,
                        const SliceTransform* prefix_extractor,
                        ExternalTableMultiGetContext* context,
                        std::vector<Status>* statuses) {
    const size_t num_requests = context->Size();
    statuses->resize(num_requests);
    for (size_t i = 0; i < num_requests; ++i) {
      ExternalTableMultiGetRequest request = context->GetRequest(i);
      (*statuses)[i] = Get(read_options, request.lookup_key, prefix_extractor,
                           request.get_context);
    }
  }

 protected:
  ~ExternalTableReaderLookupBase() = default;
};

template <ExternalTableMode Mode>
class ExternalTableReaderBase : public ExternalTableReaderLookupBase<Mode> {
 public:
  virtual ~ExternalTableReaderBase() = default;

  // Return an Iterator that can be used to scan the table file.
  // The read_options can optionally contain the upper bound
  // key (exclusive) of the scan in iterate_upper_bound.
  virtual ExternalTableIteratorBase* NewIterator(
      const ReadOptions& read_options,
      const SliceTransform* prefix_extractor) = 0;

  // Allocate and return the contents of the properties block. The properties
  // block should be written to the table file as is (no compression or
  // mutation of any kind). Implementations may return NotSupported and provide
  // the complete properties, including user-collected properties, through
  // GetTableProperties() instead.
  virtual Status GetPropertiesBlock(std::unique_ptr<char[]>* /*property_block*/,
                                    uint64_t* /*size*/) {
    return Status::NotSupported();
  }

  // Return TableProperties for the file. This may return nullptr when
  // GetPropertiesBlock() returns OK for the file. When GetPropertiesBlock()
  // returns NotSupported, this must return a non-null value containing at
  // least the following properties -
  //  comparator_name
  //  num_entries
  //  raw_key_size
  //  raw_value_size
  // Implementations storing range deletions must also return -
  //  num_deletions
  //  num_range_deletions
  // Full-mode implementations must also return -
  //  key_smallest_seqno
  //  key_largest_seqno
  //  num_merge_operands
  virtual std::shared_ptr<const TableProperties> GetTableProperties() const = 0;

  virtual Status VerifyChecksum(const ReadOptions& /*ro*/) {
    return Status::OK();
  }

  // Returns the non-owning native coroutine interface, or nullptr when this
  // reader does not support it. The returned pointer must not outlive this
  // reader.
  virtual CoroExternalTableReaderBase<Mode>* GetCoroExternalTableReader() {
    return nullptr;
  }

  // Allocate and return the raw RocksDB range-deletion block stored by
  // PutRangeDeletionBlock(). The returned block must be byte-for-byte
  // unchanged. Implementations may return NotSupported when the file has no
  // range-deletion block or the format does not support one. Formats whose
  // factory returns true from IsDeleteRangeSupported() must override this
  // method.
  virtual Status GetRangeDeletionBlock(
      std::unique_ptr<char[]>* /*range_deletion_block*/, uint64_t* /*size*/) {
    return Status::NotSupported();
  }
};

using ExternalTableReader =
    ExternalTableReaderBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
using FullExternalTableReader =
    ExternalTableReaderBase<ExternalTableMode::kFull>;

enum class ExternalTableBuilderAddResult : uint8_t {
  // The row was consumed by this builder. The caller may advance input.
  kAdded,
  // The row was not consumed. A caller that supports retryable table cuts
  // should finish the current table, create another table through the same
  // ExternalTableFactory, and retry the same row.
  kRequiresNewTable,
  // A real error occurred. The caller should abandon the current output and
  // get the error from status().
  kError,
};

enum class ExternalTableBuilderNewTableReason : uint8_t {
  kNone = 0,
  kRowClassificationChanged,
  kSchemaIdChanged,
  kSchemaVersionIncompatible,
  kUnsupportedEntryType,
  kBuilderPolicy,
};

struct ExternalTableBuilderAddContext {
  ExternalTableBuilderNewTableReason new_table_reason =
      ExternalTableBuilderNewTableReason::kNone;
};

// A table builder interface used by SstFileWriter and, in full mode, by DB
// flush and compaction to write external table files. The sequence of
// operations to write an external table is as follows -
// 1. Add() or TryAdd() is called for point entries in key order. In
//    kOnlyZeroSeqnoAndPuts mode it receives a user key with the sequence
//    number and value type stripped. In kFull mode it receives the complete
//    RocksDB internal key. Range deletions are encoded separately and passed
//    to PutRangeDeletionBlock().
// 2. After every accepted Add()/TryAdd() operation, status() is called to check
//    the current status.
// 3. After the last key is added, Finish() is called to do whatever is
//    necessary to ensure the data is persisted in the table file.
// 4. If there is a failure midway for some reason, Abandon() is called
//    instead of Finish().
// 5. At the end, FileSize(), GetTableProperties(), and status() are called to
//    get the final size of the file, the table properties, and the final
//    status. GetFileChecksum() and GetFileChecksumFuncName() may also be
//    called to get checksum information about the whole file, but their
//    implementation is optional.
class ExternalTableBuilderBase {
 public:
  virtual ~ExternalTableBuilderBase() = default;

  // Write a point key-value pair. In basic mode, key is a user key. In full
  // mode, key is a stored RocksDB internal key and ParseEntry() can be used to
  // decode it. Calls are made in the corresponding key order and that order
  // must be preserved. Range deletions are not passed to this method. Errors
  // are reported through status().
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Add key,value to the table being constructed and report whether the row was
  // accepted or whether the caller should cut the current table and retry this
  // same row with a new table builder. A kRequiresNewTable result is not an
  // error: the row must not have been consumed and status() must remain OK.
  // A kError result must be reported through status().
  //
  // Caller support for kRequiresNewTable is optional. Callers that write
  // through Add() treat such requests as builder errors because they have no
  // retry handshake.
  //
  // Existing external table builders do not request table cuts; they inherit
  // this default adapter around Add().
  //
  // REQUIRES: calls are made in key order.
  // REQUIRES: Finish(), Abandon() have not been called.
  // REQUIRES: context != nullptr.
  virtual ExternalTableBuilderAddResult TryAdd(
      const Slice& key, const Slice& value,
      ExternalTableBuilderAddContext* context) {
    assert(context != nullptr);
    context->new_table_reason = ExternalTableBuilderNewTableReason::kNone;
    Add(key, value);
    return ExternalTableBuilderAddResult::kAdded;
  }

  // Return the current Status. This could return non-ok, for example, if
  // Add() fails for some reason.
  virtual Status status() const = 0;

  // Finalize the table contents and release builder-owned resources. The file
  // remains owned by RocksDB and must not be synced or closed.
  virtual Status Finish() = 0;

  // Discard partial table state and release builder-owned resources. RocksDB
  // owns and cleans up the file. Either this or Finish() will be called, but
  // not both.
  virtual void Abandon() = 0;

  // Return the size generated so far. After Finish(), return the final size.
  virtual uint64_t FileSize() const = 0;

  // Write the raw properties block as is in the table file. Implementations
  // may return NotSupported and provide the complete properties through
  // GetTableProperties() instead, including user-collected properties needed
  // when the file is reopened.
  virtual Status PutPropertiesBlock(const Slice& /*property_block*/) {
    return Status::NotSupported();
  }

  // If PutPropertiesBlock() succeeds, this method does not need to populate
  // any properties. Otherwise, it must return comparator_name and any
  // format-specific properties such as data_size. RocksDB overlays the logical
  // entry counts and sizes, sequence-number bounds, deletion counts, and merge
  // operand count that it tracks while building the table. The implementation
  // is still responsible for persisting enough metadata for its reader's
  // GetTableProperties() to return the complete properties after reopening.
  virtual TableProperties GetTableProperties() const = 0;

  virtual std::string GetFileChecksum() const { return kUnknownFileChecksum; }

  virtual const char* GetFileChecksumFuncName() const {
    return kUnknownFileChecksumFuncName;
  }

  // Persist the raw RocksDB range-deletion block byte-for-byte. The input is
  // valid only for the duration of this call. This is called before Finish()
  // only when the table contains range deletions. Formats whose factory returns
  // true from IsDeleteRangeSupported() must override this method.
  virtual Status PutRangeDeletionBlock(const Slice& /*range_deletion_block*/) {
    return Status::NotSupported();
  }
};

using ExternalTableBuilder = ExternalTableBuilderBase;
using FullExternalTableBuilder = ExternalTableBuilderBase;

struct ExternalTableOptions {
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const Comparator* comparator;
  // Compares the internal keys used by full-mode readers and iterators.
  const CompareInterface* internal_key_comparator;
  std::string column_family_name;
  const std::shared_ptr<FileSystem>& fs;
  const FileOptions& file_options;
  // Opaque implementation context valid only during NewTableReader().
  const void* native_args;

  ExternalTableOptions(
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator, const std::shared_ptr<FileSystem>& _fs,
      const FileOptions& _file_options,
      const std::string& _column_family_name = "")
      : ExternalTableOptions(_prefix_extractor, _comparator,
                             /*_internal_key_comparator=*/nullptr, _fs,
                             _file_options, _column_family_name,
                             /*_native_args=*/nullptr) {}

  ExternalTableOptions(
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator,
      const CompareInterface* _internal_key_comparator,
      const std::shared_ptr<FileSystem>& _fs, const FileOptions& _file_options,
      const std::string& _column_family_name, const void* _native_args)
      : prefix_extractor(_prefix_extractor),
        comparator(_comparator),
        internal_key_comparator(_internal_key_comparator),
        column_family_name(_column_family_name),
        fs(_fs),
        file_options(_file_options),
        native_args(_native_args) {}
};

struct ExternalTableBuilderOptions {
  const ReadOptions& read_options;
  const WriteOptions& write_options;
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const Comparator* comparator;
  const std::string& column_family_name;
  const std::string db_id;
  const std::string db_session_id;
  const TableFileCreationReason reason;
  const std::shared_ptr<FileSystem>& fs;
  // Opaque implementation context valid only during NewTableBuilder().
  const void* native_args;

  ExternalTableBuilderOptions(
      const ReadOptions& _read_options, const WriteOptions& _write_options,
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator, const std::string& _column_family_name,
      const TableFileCreationReason _reason,
      const std::shared_ptr<FileSystem>& _fs)
      : ExternalTableBuilderOptions(
            _read_options, _write_options, _prefix_extractor, _comparator,
            _column_family_name, _reason, _fs, /*_native_args=*/nullptr) {}

  ExternalTableBuilderOptions(
      const ReadOptions& _read_options, const WriteOptions& _write_options,
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator, const std::string& _column_family_name,
      const TableFileCreationReason _reason,
      const std::shared_ptr<FileSystem>& _fs, const void* _native_args)
      : read_options(_read_options),
        write_options(_write_options),
        prefix_extractor(_prefix_extractor),
        comparator(_comparator),
        column_family_name(_column_family_name),
        reason(_reason),
        fs(_fs),
        native_args(_native_args) {}
};

template <ExternalTableMode Mode>
class ExternalTableFactoryBase : public Customizable {
 public:
  ~ExternalTableFactoryBase() override = default;

  const char* Name() const override { return "ExternalTableFactory"; }

  // Ownership of file is transferred to the implementation, which must retain
  // it for as long as the returned reader needs it. file_size is the size of
  // that opened file.
  virtual Status NewTableReader(
      const ReadOptions& read_options, const std::string& file_path,
      const ExternalTableOptions& table_options,
      std::unique_ptr<FSRandomAccessFile>&& file, uint64_t file_size,
      std::unique_ptr<ExternalTableReaderBase<Mode>>* table_reader) const = 0;

  // file is non-owning and remains valid until Finish() or Abandon() returns.
  // Do not retain it afterward, including during builder destruction. Do not
  // sync or close it; RocksDB will do that.
  virtual ExternalTableBuilderBase* NewTableBuilder(
      const ExternalTableBuilderOptions& builder_options,
      const std::string& file_path, FSWritableFile* file) const = 0;

  // Configures this factory using an implementation-defined string. RocksDB
  // calls this method while preparing the table factory, before it is used to
  // create table readers or builders. The configuration is immutable after
  // the DB is opened and is persisted in the RocksDB OPTIONS file.
  //
  // Implementations must copy any needed data from config and should make this
  // method idempotent because RocksDB may prepare an object more than once.
  // The configuration must not contain secrets because OPTIONS files are
  // stored as plaintext.
  virtual Status Configure(const std::string& config) {
    if (config.empty()) {
      return Status::OK();
    }
    return Status::NotSupported(
        "External table factory does not support configuration");
  }

  // Returns whether this factory supports range deletions. Returning true
  // requires the corresponding builder and reader to implement
  // PutRangeDeletionBlock() and GetRangeDeletionBlock(), respectively.
  virtual bool IsDeleteRangeSupported() const { return false; }

  // Returns whether NewTableBuilder() can write tables. A factory that only
  // reads files written elsewhere returns false; see
  // TableFactory::IsTableBuilderSupported() for what RocksDB then requires.
  virtual bool IsTableBuilderSupported() const { return true; }
};

using ExternalTableFactory =
    ExternalTableFactoryBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
using FullExternalTableFactory =
    ExternalTableFactoryBase<ExternalTableMode::kFull>;

// Allocate a TableFactory that wraps around an ExternalTableFactory. Use this
// to allocate and set in ColumnFamilyOptions::table_factory.
std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory);

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<FullExternalTableFactory> inner_factory);

// Returns the wrapped external-table factory when table_factory was created by
// NewExternalTableFactory(). Returns nullptr otherwise.
std::shared_ptr<ExternalTableFactory> GetWrappedExternalTableFactory(
    const TableFactory* table_factory);

}  // namespace ROCKSDB_NAMESPACE
