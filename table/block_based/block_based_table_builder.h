//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <array>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "port/likely.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/atomic.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;
struct BlobRelocationFileMetadata;

extern const uint64_t kBlockBasedTableMagicNumber;

class BlockBasedTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  // `blob_reference_cache_res_mgr` charges the block cache for the memory of
  // the exact blob reference metadata collector. The table factory shares one
  // manager between all its builders. Null means the memory is not charged.
  BlockBasedTableBuilder(
      const BlockBasedTableOptions& table_options,
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file,
      std::shared_ptr<CacheReservationManager> blob_reference_cache_res_mgr);

  // No copying allowed
  BlockBasedTableBuilder(const BlockBasedTableBuilder&) = delete;
  BlockBasedTableBuilder& operator=(const BlockBasedTableBuilder&) = delete;
  BlockBasedTableBuilder(BlockBasedTableBuilder&&) = delete;
  BlockBasedTableBuilder& operator=(BlockBasedTableBuilder&&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~BlockBasedTableBuilder() override;

  // Resets this thread's AutoSkip inter-file estimate carryover (see
  // CompressionOptions::auto_skip). Tools that build many independent files on
  // a single thread (e.g. sst_dump --command=recompress) call this between
  // files so each is measured without inheriting the previous file's estimate.
  static void ResetThreadLocalAutoSkipCarryover();

  // Add key,value to the table being constructed.
  // REQUIRES: Unless key has type kTypeRangeDeletion, key is after any
  //           previously added non-kTypeRangeDeletion key according to
  //           comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Keeps block-based builders behind the retry-capable TableBuilder contract
  // even though the current format accepts every row. Future format-specific
  // boundary policies can then ask callers to cut the file without changing
  // compaction's Add() assumptions.
  TableBuilderAddResult TryAdd(const Slice& key, const Slice& value,
                               TableBuilderAddContext* context) override;

  // Relocation file building. Indirect blob GC writes the live values it copies
  // into a relocation file, which is a block-based table (see
  // db/blob/blob_relocation_file.h). Create the builder with
  // NewBlobRelocationTableBuilder, which also starts the file.

  // Starts a relocation file and writes its 8-byte prefix
  // (kBlobRelocationFilePrefix). Also turns off compression sampling. Requires
  // a fresh builder (nothing added yet) created with embedded blob support
  // (TableBuilderOptions::embedded_blob_options set, as
  // NewBlobRelocationTableBuilder does), a persisted file identity
  // (orig_file_number and db_session_id), and the bytewise comparator without
  // timestamps; otherwise InvalidArgument.
  Status StartBlobRelocationFile();

  // Registers an old file number that may be routed to this file. The file
  // stores this served-file list in its directory (see
  // db/blob/blob_relocation_file.h). `file_number` must be smaller than this
  // file's number; otherwise InvalidArgument. Duplicate calls are allowed and
  // removed in Finish().
  Status AddBlobRelocationServedFile(uint64_t file_number);

  // Adds one stored blob payload under its stable identity (origin, original
  // offset). The payload is already compressed with `compression` and is
  // written as is. `origin_file_number` must be smaller than this file's
  // number, and calls must be in strictly increasing (origin, original offset)
  // order. A payload above kMaxSimpleGen2BlobPayloadSize or an unsupported
  // compression type is also rejected. All these cases return InvalidArgument.
  Status AddBlobRelocation(uint64_t origin_file_number,
                           uint64_t original_value_offset,
                           const Slice& stored_payload,
                           CompressionType compression);

  // Returns the encoded directory and tail size after Finish() succeeds.
  // Otherwise returns the builder's error status, or InvalidArgument if the
  // file is not finished yet.
  Status GetBlobRelocationFileMetadata(
      BlobRelocationFileMetadata* metadata) const;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  bool IsEmpty() const override;

  uint64_t PreCompressionSize() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  // Estimated size of the file generated so far (based on data blocks, this
  // estimate does not include meta blocks). This is used when FileSize() cannot
  // estimate final SST size, e.g. parallel compression is enabled.
  uint64_t EstimatedFileSize() const override;

  // Estimated tail size of the SST file generated so far. The "tail" refers to
  // all blocks written after data blocks (index + filter).
  uint64_t EstimatedTailSize() const override;

  // Get the size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t GetTailSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

  std::shared_ptr<const BlobReferenceSstSummary> GetBlobReferenceSstSummary()
      const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(const SeqnoToTimeMapping& relevant_mapping,
                                   uint64_t oldest_ancestor_time) override;

  uint64_t GetWorkerCPUMicros() const override;

#ifndef NDEBUG
  // Test-only: inject an IOError into the builder's status.
  void TEST_InjectIOError();
#endif  // !NDEBUG

 private:
  bool ok() const;

  // Add() with the caller's decode of a kTypeBlobIndex `value`, or nullptr
  // (see TableBuilderAddContext::decoded_blob_index).
  void AddImpl(const Slice& key, const Slice& value,
               const BlobIndex* decoded_blob_index);

  // Feeds the blob references of one entry to the reference collector.
  // Returns false, with the builder status set, on an error. AddImpl() calls
  // it only when the SST collects references. ROCKSDB_NOINLINE keeps it out of
  // AddImpl(), so the base path stays small.
  ROCKSDB_NOINLINE bool AddBlobReferences(const Slice& entry_ikey,
                                          const Slice& entry_value,
                                          ValueType value_type,
                                          const BlobIndex* value_blob_index);

  // Transition state from buffered to unbuffered if the conditions are met. See
  // `Rep::State` API comment for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  void MaybeEnterUnbuffered(const Slice* first_key_in_next_block);

  // Try to keep some parallel-specific code separate to improve hot code
  // locality for non-parallel case
  void EmitBlock(std::string& uncompressed,
                 const Slice& last_key_in_current_block,
                 const Slice* first_key_in_next_block);
  void EmitBlockForParallel(std::string& uncompressed,
                            const Slice& last_key_in_current_block,
                            const Slice* first_key_in_next_block);

  // Compress and write block content to the file, from a single-threaded
  // context
  // @skip_delta_encoding : This is set to non null for data blocks, so that
  //     caller would know whether the index entry of this data block should
  //     skip delta encoding or not
  void WriteBlock(const Slice& block_contents, BlockHandle* handle,
                  BlockType block_type, bool* skip_delta_encoding = nullptr);
  // Directly write data to the file.
  // The base signatures. They pass warm_cache = true (base behavior: the
  // block may be added to the block cache when the options ask for it).
  void WriteMaybeCompressedBlock(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data = nullptr,
      bool* skip_delta_encoding = nullptr,
      const uint32_t* precomputed_block_contents_crc32c = nullptr);
  IOStatus WriteMaybeCompressedBlockImpl(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data = nullptr,
      bool* skip_delta_encoding = nullptr,
      const uint32_t* precomputed_block_contents_crc32c = nullptr);

  // `warm_cache` false keeps the block out of the block cache even when the
  // options ask to warm it (prepopulate_block_cache). Used for blocks that no
  // reader looks up in the block cache.
  void WriteMaybeCompressedBlock(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data,
      bool* skip_delta_encoding,
      const uint32_t* precomputed_block_contents_crc32c, bool warm_cache);
  IOStatus WriteMaybeCompressedBlockImpl(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data,
      bool* skip_delta_encoding,
      const uint32_t* precomputed_block_contents_crc32c, bool warm_cache);

  void SetupCacheKeyPrefix(const TableBuilderOptions& tbo);

  template <typename TBlocklike>
  Status InsertBlockInCache(const Slice& block_contents,
                            const BlockHandle* handle, BlockType block_type);

  Status InsertBlockInCacheHelper(const Slice& block_contents,
                                  const BlockHandle* handle,
                                  BlockType block_type);

  Status InsertBlockInCompressedCache(const Slice& block_contents,
                                      const CompressionType type,
                                      const BlockHandle* handle);

  void WriteFilterBlock(MetaIndexBuilder* meta_index_builder);
  void WriteIndexBlock(MetaIndexBuilder* meta_index_builder,
                       BlockHandle* index_block_handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  void WriteRangeDelBlock(MetaIndexBuilder* meta_index_builder);
  void WriteBlobReferenceMetadata(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  // Embedded-blob SST support. These are only exercised when the builder is in
  // embedded mode (rep_->embedded_blob_options is set), in which delta encoding
  // of index values is disabled so blob records can be written inline as values
  // are added (possibly interleaved with data blocks).

  // For an embedded-mode value entry, possibly extracts large value payload(s)
  // into inline same-file blob records and rewrites *key / *value to reference
  // them (a kTypeBlobIndex entry for whole values, or a rebuilt wide-column
  // entity with per-column BlobIndex refs). On no-op, *key / *value are left
  // unchanged. Returns false and records a failed status on error.
  bool MaybeExtractEmbeddedBlobs(SequenceNumber seq, ValueType value_type,
                                 Slice* key, Slice* value);

  // Rewrites eligible wide-column values as inline same-file blob records,
  // building the rebuilt entity into rep_->embedded_blob_state->entity_buf.
  // Sets *rewritten when at least one column was extracted (otherwise the
  // original value should be used as-is).
  Status BuildEmbeddedWideColumnEntity(const Slice& value, bool* rewritten);

  // Appends one same-file blob record at the current table offset and advances
  // the offset. `payload` is already in stored form, compressed with
  // `compression`, and is written as is. Also updates the embedded-blob
  // counters and encodes a same-file BlobIndex into
  // rep_->embedded_blob_state->blob_index_buf. When non-null, `record_offset`
  // receives the file offset where the record starts.
  Status WriteEmbeddedBlobRecord(const Slice& payload,
                                 CompressionType compression,
                                 uint64_t* record_offset);

  // Relocation files: a location entry covers a group of values of one origin
  // (see db/blob/blob_relocation_file.h). Before a record of
  // `origin_file_number` is written, this writes the open group's location
  // entry if the record starts a new group. The location entry is added only
  // after the group's records, so a data block is never written between the
  // records of one group.
  void PrepareBlobRelocationGroup(uint64_t origin_file_number);

  // Adds a written record to the open group, and writes the group's location
  // entry when the group is full.
  void AddToBlobRelocationGroup(uint64_t origin_file_number,
                                uint64_t original_value_offset,
                                uint64_t stored_value_size,
                                uint64_t record_offset);
  // Writes the location entry of the open group, if any.
  void FinishBlobRelocationGroup();

  struct Rep;
  class BlockBasedTablePropertiesCollectorFactory;
  class BlockBasedTablePropertiesCollector;
  // Adapter that bridges built-in index writer callbacks to
  // BlockBasedTableBuilder's private write helpers.
  class IndexBlockWriterImpl;
  std::unique_ptr<Rep> rep_;
  struct WorkingAreaPair;
  struct ParallelCompressionRep;

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush(const Slice* first_key_in_next_block);

  // Some compression libraries fail when the uncompressed size is bigger than
  // int. If uncompressed size is bigger than kCompressionSizeLimit, don't
  // compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();

  // Code for a "parallel compression" worker thread, which can really do SST
  // writes and block compressions alternately.
  void BGWorker(WorkingAreaPair& working_area);

  // Given uncompressed block content, try to compress it and return result and
  // compression type. Only ever called to attempt compression; the auto-skip
  // skip/bypass decision is made by the caller (emit thread).
  Status CompressAndVerifyBlock(const Slice& uncompressed_block_data,
                                bool is_data_block,
                                WorkingAreaPair& working_area,
                                GrowableBuffer* compressed_output,
                                CompressionType* result_compression_type,
                                uint32_t* result_checksum);

  // If configured, start worker threads for parallel compression
  void MaybeStartParallelCompression();

  // Stop worker threads for parallel compression
  void StopParallelCompression(bool abort);
};

// The identity and the size target of one relocation file, for
// NewBlobRelocationTableBuilder(). Callers set the fields by name, so values of
// the same type (db_id and db_session_id, file_number and target_file_size)
// cannot be swapped by mistake. The strings are copied.
// NewBlobRelocationTableBuilder() runs once per relocation file, so the copies
// cost little.
struct BlobRelocationTableFileInfo {
  uint32_t column_family_id = 0;
  std::string column_family_name;
  TableFileCreationReason reason = TableFileCreationReason::kMisc;
  std::string db_id;
  std::string db_session_id;
  std::string db_name;
  uint64_t file_number = 0;
  // The file size that the caller aims for; 0 means no target.
  uint64_t target_file_size = 0;
};

// Creates the builder of relocation file `file_info.file_number` of column
// family `file_info.column_family_id`, writing to `file`, and starts the file
// (StartBlobRelocationFile). Settings of the builder:
// - Table options: MakeBlobRelocationTableOptions(`table_options`).
// - Other settings: from `mutable_cf_options`, with blob indirection and the
//   prefix extractor turned off.
// - Data blocks: written with kNoCompression. They hold only location entries
//   (see db/blob/blob_relocation_file.h). The builder writes each value
//   itself, outside the data blocks, already in stored form, as an embedded
//   blob record.
//
// `comparator` must use the bytewise comparator and outlive the builder. If
// starting the file fails, the builder is abandoned and `*builder` is reset, so
// the caller only needs to return the status.
Status NewBlobRelocationTableBuilder(
    const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    const BlockBasedTableOptions& table_options,
    const ReadOptions& read_options, const WriteOptions& write_options,
    const InternalKeyComparator& comparator,
    const BlobRelocationTableFileInfo& file_info, WritableFileWriter* file,
    std::unique_ptr<BlockBasedTableBuilder>* builder);

}  // namespace ROCKSDB_NAMESPACE
