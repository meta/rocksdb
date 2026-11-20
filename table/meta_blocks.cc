//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "table/meta_blocks.h"

#include <array>
#include <cstdio>
#include <map>
#include <string>

#include "block_fetcher.h"
#include "db/dbformat.h"
#include "db/table_properties_collector.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/persistent_cache_helper.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_properties_internal.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

const std::string kPropertiesBlockName = "rocksdb.properties";
// NB: only used with format_version >= 6
const std::string kIndexBlockName = "rocksdb.index";
const std::string kCompressionDictBlockName = "rocksdb.compression_dict";
const std::string kRangeDelBlockName = "rocksdb.range_del";

Status GetGlobalSequenceNumber(const TableProperties& table_properties,
                               SequenceNumber largest_seqno,
                               SequenceNumber* global_seqno) {
  // A wrapping reader will apply the effective global seqno. Keep this reader
  // on the stored per-entry sequence numbers to avoid applying it twice.
  if (largest_seqno == kDisableGlobalSequenceNumber) {
    *global_seqno = kDisableGlobalSequenceNumber;
    return Status::OK();
  }

  const auto& props = table_properties.user_collected_properties;
  const auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
  const auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

  *global_seqno = kDisableGlobalSequenceNumber;
  if (version_pos == props.end()) {
    if (seqno_pos != props.end()) {
      std::array<char, 200> msg_buf{};
      snprintf(
          msg_buf.data(), msg_buf.max_size(),
          "A non-external sst file have global seqno property with value %s",
          seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  const uint32_t version = DecodeFixed32(version_pos->second.c_str());
  if (version != 2) {
    std::array<char, 200> msg_buf{};
    if (version != 1) {
      snprintf(msg_buf.data(), msg_buf.max_size(),
               "An external sst file has corrupted version %u.", version);
      return Status::Corruption(msg_buf.data());
    }
    if (seqno_pos != props.end()) {
      snprintf(msg_buf.data(), msg_buf.max_size(),
               "An external sst file with version %u has global seqno "
               "property with value %s",
               version, seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  // Since we have a plan to deprecate global_seqno, we do not return failure
  // if seqno_pos == props.end(). We rely on version_pos to detect whether the
  // SST is external.
  SequenceNumber resolved_global_seqno = 0;
  if (seqno_pos != props.end()) {
    resolved_global_seqno = DecodeFixed64(seqno_pos->second.c_str());
  }
  // SstTableReader opens a table reader with kMaxSequenceNumber as
  // largest_seqno to denote it is unknown.
  if (largest_seqno < kMaxSequenceNumber) {
    if (resolved_global_seqno == 0) {
      resolved_global_seqno = largest_seqno;
    }
    if (resolved_global_seqno != largest_seqno) {
      std::array<char, 200> msg_buf{};
      snprintf(
          msg_buf.data(), msg_buf.max_size(),
          "An external sst file with version %u have global seqno property "
          "with value %s, while largest seqno in the file is %llu",
          version, seqno_pos->second.c_str(),
          static_cast<unsigned long long>(largest_seqno));
      return Status::Corruption(msg_buf.data());
    }
  }
  *global_seqno = resolved_global_seqno;

  if (resolved_global_seqno > kMaxSequenceNumber) {
    std::array<char, 200> msg_buf{};
    snprintf(msg_buf.data(), msg_buf.max_size(),
             "An external sst file with version %u have global seqno property "
             "with value %llu, which is greater than kMaxSequenceNumber",
             version, static_cast<unsigned long long>(resolved_global_seqno));
    return Status::Corruption(msg_buf.data());
  }

  return Status::OK();
}

MetaIndexBuilder::MetaIndexBuilder()
    : meta_index_block_(new BlockBuilder(BlockBuilder::ForMetaBlock{},
                                         1 /* restart interval */)) {}

void MetaIndexBuilder::Add(const std::string& key, const BlockHandle& handle) {
  std::string handle_encoding;
  handle.EncodeTo(&handle_encoding);
  meta_block_handles_.insert({key, handle_encoding});
}

Slice MetaIndexBuilder::Finish() {
  for (const auto& metablock : meta_block_handles_) {
    // NOTE: meta index keys and block handles are guaranteed < 4GB
    meta_index_block_->Add(metablock.first, metablock.second);
  }
  return meta_index_block_->Finish();
}

// Property block will be read sequentially and cached in a heap located
// object, so there's no need for restart points. Thus we set the restart
// interval to infinity to save space.
PropertyBlockBuilder::PropertyBlockBuilder()
    : properties_block_(new BlockBuilder(
          BlockBuilder::ForMetaBlock{},
          std::numeric_limits<int32_t>::max() /* restart interval */)) {}

void PropertyBlockBuilder::Add(const std::string& name,
                               const std::string& val) {
  assert(props_.find(name) == props_.end());
  props_.insert({name, val});
}

void PropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
  std::string dst;
  PutVarint64(&dst, val);

  Add(name, dst);
}

void PropertyBlockBuilder::Add(
    const UserCollectedProperties& user_collected_properties) {
  for (const auto& prop : user_collected_properties) {
    Add(prop.first, prop.second);
  }
}

void PropertyBlockBuilder::AddTableProperty(const TableProperties& props) {
  TEST_SYNC_POINT_CALLBACK("PropertyBlockBuilder::AddTableProperty:Start",
                           const_cast<TableProperties*>(&props));

  Add(TablePropertiesNames::kOriginalFileNumber, props.orig_file_number);
  Add(TablePropertiesNames::kRawKeySize, props.raw_key_size);
  Add(TablePropertiesNames::kRawValueSize, props.raw_value_size);
  bool skip_referenced_blob_bytes = false;
  TEST_SYNC_POINT_CALLBACK(
      "PropertyBlockBuilder::AddTableProperty:SkipReferencedBlobBytes",
      &skip_referenced_blob_bytes);
  if (props.referenced_blob_bytes.has_value() && !skip_referenced_blob_bytes) {
    Add(TablePropertiesNames::kReferencedBlobBytes,
        *props.referenced_blob_bytes);
  }
  Add(TablePropertiesNames::kDataSize, props.data_size);
  Add(TablePropertiesNames::kUncompressedDataSize,
      props.uncompressed_data_size);
  Add(TablePropertiesNames::kIndexSize, props.index_size);
  if (props.index_partitions != 0) {
    Add(TablePropertiesNames::kIndexPartitions, props.index_partitions);
    Add(TablePropertiesNames::kTopLevelIndexSize, props.top_level_index_size);
  }
  Add(TablePropertiesNames::kIndexKeyIsUserKey, props.index_key_is_user_key);
  Add(TablePropertiesNames::kIndexValueIsDeltaEncoded,
      props.index_value_is_delta_encoded);
  if (props.udi_is_primary_index != 0) {
    Add(TablePropertiesNames::kUDIIsPrimaryIndex, props.udi_is_primary_index);
  }
  if (props.standard_index_is_stub != 0) {
    Add(TablePropertiesNames::kStandardIndexIsStub,
        props.standard_index_is_stub);
  }
  Add(TablePropertiesNames::kNumEntries, props.num_entries);
  Add(TablePropertiesNames::kNumFilterEntries, props.num_filter_entries);
  Add(TablePropertiesNames::kDeletedKeys, props.num_deletions);
  Add(TablePropertiesNames::kMergeOperands, props.num_merge_operands);
  Add(TablePropertiesNames::kNumRangeDeletions, props.num_range_deletions);
  Add(TablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
  if (props.num_data_blocks_compression_rejected > 0) {
    Add(TablePropertiesNames::kNumDataBlocksCompressionRejected,
        props.num_data_blocks_compression_rejected);
  }
  if (props.num_data_blocks_compression_bypassed > 0) {
    Add(TablePropertiesNames::kNumDataBlocksCompressionBypassed,
        props.num_data_blocks_compression_bypassed);
  }
  Add(TablePropertiesNames::kNumUniformBlocks, props.num_uniform_blocks);
  Add(TablePropertiesNames::kFilterSize, props.filter_size);
  Add(TablePropertiesNames::kFormatVersion, props.format_version);
  Add(TablePropertiesNames::kFixedKeyLen, props.fixed_key_len);
  Add(TablePropertiesNames::kColumnFamilyId, props.column_family_id);
  Add(TablePropertiesNames::kOldestAncestorTime, props.oldest_ancestor_time);
  Add(TablePropertiesNames::kOldestKeyTime, props.oldest_key_time);
  Add(TablePropertiesNames::kNewestKeyTime, props.newest_key_time);
  if (props.file_creation_time > 0) {
    Add(TablePropertiesNames::kFileCreationTime, props.file_creation_time);
  }
  if (props.slow_compression_estimated_data_size > 0) {
    Add(TablePropertiesNames::kSlowCompressionEstimatedDataSize,
        props.slow_compression_estimated_data_size);
  }
  if (props.fast_compression_estimated_data_size > 0) {
    Add(TablePropertiesNames::kFastCompressionEstimatedDataSize,
        props.fast_compression_estimated_data_size);
  }
  Add(TablePropertiesNames::kTailStartOffset, props.tail_start_offset);
  if (props.user_defined_timestamps_persisted == 0) {
    Add(TablePropertiesNames::kUserDefinedTimestampsPersisted,
        props.user_defined_timestamps_persisted);
  }
  if (!props.db_id.empty()) {
    Add(TablePropertiesNames::kDbId, props.db_id);
  }
  if (!props.db_session_id.empty()) {
    Add(TablePropertiesNames::kDbSessionId, props.db_session_id);
  }
  if (!props.db_host_id.empty()) {
    Add(TablePropertiesNames::kDbHostId, props.db_host_id);
  }

  if (!props.filter_policy_name.empty()) {
    Add(TablePropertiesNames::kFilterPolicy, props.filter_policy_name);
  }
  if (!props.comparator_name.empty()) {
    Add(TablePropertiesNames::kComparator, props.comparator_name);
  }

  if (!props.merge_operator_name.empty()) {
    Add(TablePropertiesNames::kMergeOperator, props.merge_operator_name);
  }
  if (!props.prefix_extractor_name.empty()) {
    Add(TablePropertiesNames::kPrefixExtractorName,
        props.prefix_extractor_name);
  }
  if (!props.property_collectors_names.empty()) {
    Add(TablePropertiesNames::kPropertyCollectors,
        props.property_collectors_names);
  }
  if (!props.column_family_name.empty()) {
    Add(TablePropertiesNames::kColumnFamilyName, props.column_family_name);
  }

  if (!props.compression_name.empty()) {
    Add(TablePropertiesNames::kCompression, props.compression_name);
  }
  if (!props.compression_options.empty()) {
    Add(TablePropertiesNames::kCompressionOptions, props.compression_options);
  }
  if (!props.seqno_to_time_mapping.empty()) {
    Add(TablePropertiesNames::kSequenceNumberTimeMapping,
        props.seqno_to_time_mapping);
  }
  if (props.key_largest_seqno != UINT64_MAX) {
    Add(TablePropertiesNames::kKeyLargestSeqno, props.key_largest_seqno);
  }
  if (props.key_smallest_seqno != UINT64_MAX) {
    Add(TablePropertiesNames::kKeySmallestSeqno, props.key_smallest_seqno);
  }
  if (props.data_block_restart_interval > 0) {
    Add(TablePropertiesNames::kDataBlockRestartInterval,
        props.data_block_restart_interval);
  }
  if (props.index_block_restart_interval > 0) {
    Add(TablePropertiesNames::kIndexBlockRestartInterval,
        props.index_block_restart_interval);
  }
  if (props.separate_key_value_in_data_block > 0) {
    Add(TablePropertiesNames::kSeparateKeyValueInDataBlock,
        props.separate_key_value_in_data_block);
  }
  if (props.lsm_info_at_creation != 0) {
    Add(TablePropertiesNames::kLsmInfoAtCreation, props.lsm_info_at_creation);
  }
}

Slice PropertyBlockBuilder::Finish() {
  for (const auto& prop : props_) {
    assert(last_prop_added_to_block_.empty() ||
           comparator_->Compare(prop.first, last_prop_added_to_block_) > 0);
    // Use first 4GB of key and value strings to avoid overflow, e.g. from
    // user property collector (see BlockBuilder::Add API comments)
    Slice key(prop.first.data(), static_cast<uint32_t>(prop.first.size()));
    Slice value(prop.second.data(), static_cast<uint32_t>(prop.second.size()));
    properties_block_->Add(key, value);
#ifndef NDEBUG
    last_prop_added_to_block_ = prop.first;
#endif /* !NDEBUG */
  }

  return properties_block_->Finish();
}

void LogPropertiesCollectionError(Logger* info_log, const std::string& method,
                                  const std::string& name) {
  assert(method == "Add" || method == "Finish");

  std::string msg =
      "Encountered error when calling TablePropertiesCollector::" + method +
      "() with collector name: " + name;
  ROCKS_LOG_ERROR(info_log, "%s", msg.c_str());
}

bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key, const Slice& value, uint64_t file_size,
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    Logger* info_log) {
  bool all_succeeded = true;
  for (auto& collector : collectors) {
    Status s = collector->InternalAdd(key, value, file_size);
    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(info_log, "Add" /* method */,
                                   collector->Name());
    }
  }
  return all_succeeded;
}

void NotifyCollectTableCollectorsOnBlockAdd(
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    const uint64_t block_uncomp_bytes,
    const uint64_t block_compressed_bytes_fast,
    const uint64_t block_compressed_bytes_slow) {
  for (auto& collector : collectors) {
    collector->BlockAdd(block_uncomp_bytes, block_compressed_bytes_fast,
                        block_compressed_bytes_slow);
  }
}

bool NotifyCollectTableCollectorsOnFinish(
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    Logger* info_log, PropertyBlockBuilder* builder,
    UserCollectedProperties& user_collected_properties,
    UserCollectedProperties& readable_properties) {
  bool all_succeeded = true;
  for (auto& collector : collectors) {
    UserCollectedProperties user_properties;
    Status s = collector->Finish(&user_properties);
    if (s.ok()) {
      for (const auto& prop : collector->GetReadableProperties()) {
        readable_properties.insert(prop);
      }
#ifndef NDEBUG
      // Check different user properties collectors are not adding properties of
      // the same name.
      for (const auto& pair : user_properties) {
        assert(user_collected_properties.find(pair.first) ==
               user_collected_properties.end());
      }
#endif /* !NDEBUG */
      user_collected_properties.merge(user_properties);
    } else {
      LogPropertiesCollectionError(info_log, "Finish" /* method */,
                                   collector->Name());
      if (all_succeeded) {
        all_succeeded = false;
      }
    }
  }
  builder->Add(user_collected_properties);
  return all_succeeded;
}

Status ParsePropertiesBlock(
    const ImmutableOptions& ioptions, Block& properties_block,
    std::unique_ptr<TableProperties>& new_table_properties,
    uint32_t* global_seqno_value_offset) {
  if (global_seqno_value_offset != nullptr) {
    *global_seqno_value_offset = 0;
  }
  std::unique_ptr<MetaBlockIter> iter(properties_block.NewMetaIterator());

  //  All pre-defined properties of type uint64_t
  std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
      {TablePropertiesNames::kOriginalFileNumber,
       &new_table_properties->orig_file_number},
      {TablePropertiesNames::kDataSize, &new_table_properties->data_size},
      {TablePropertiesNames::kUncompressedDataSize,
       &new_table_properties->uncompressed_data_size},
      {TablePropertiesNames::kLsmInfoAtCreation,
       &new_table_properties->lsm_info_at_creation},
      {TablePropertiesNames::kIndexSize, &new_table_properties->index_size},
      {TablePropertiesNames::kIndexPartitions,
       &new_table_properties->index_partitions},
      {TablePropertiesNames::kTopLevelIndexSize,
       &new_table_properties->top_level_index_size},
      {TablePropertiesNames::kIndexKeyIsUserKey,
       &new_table_properties->index_key_is_user_key},
      {TablePropertiesNames::kIndexValueIsDeltaEncoded,
       &new_table_properties->index_value_is_delta_encoded},
      {TablePropertiesNames::kUDIIsPrimaryIndex,
       &new_table_properties->udi_is_primary_index},
      {TablePropertiesNames::kStandardIndexIsStub,
       &new_table_properties->standard_index_is_stub},
      {TablePropertiesNames::kFilterSize, &new_table_properties->filter_size},
      {TablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size},
      {TablePropertiesNames::kRawValueSize,
       &new_table_properties->raw_value_size},
      {TablePropertiesNames::kNumDataBlocks,
       &new_table_properties->num_data_blocks},
      {TablePropertiesNames::kNumDataBlocksCompressionRejected,
       &new_table_properties->num_data_blocks_compression_rejected},
      {TablePropertiesNames::kNumDataBlocksCompressionBypassed,
       &new_table_properties->num_data_blocks_compression_bypassed},
      {TablePropertiesNames::kNumUniformBlocks,
       &new_table_properties->num_uniform_blocks},
      {TablePropertiesNames::kNumEntries, &new_table_properties->num_entries},
      {TablePropertiesNames::kNumFilterEntries,
       &new_table_properties->num_filter_entries},
      {TablePropertiesNames::kDeletedKeys,
       &new_table_properties->num_deletions},
      {TablePropertiesNames::kMergeOperands,
       &new_table_properties->num_merge_operands},
      {TablePropertiesNames::kNumRangeDeletions,
       &new_table_properties->num_range_deletions},
      {TablePropertiesNames::kFormatVersion,
       &new_table_properties->format_version},
      {TablePropertiesNames::kFixedKeyLen,
       &new_table_properties->fixed_key_len},
      {TablePropertiesNames::kColumnFamilyId,
       &new_table_properties->column_family_id},
      {TablePropertiesNames::kOldestAncestorTime,
       &new_table_properties->oldest_ancestor_time},
      {TablePropertiesNames::kOldestKeyTime,
       &new_table_properties->oldest_key_time},
      {TablePropertiesNames::kNewestKeyTime,
       &new_table_properties->newest_key_time},
      {TablePropertiesNames::kFileCreationTime,
       &new_table_properties->file_creation_time},
      {TablePropertiesNames::kSlowCompressionEstimatedDataSize,
       &new_table_properties->slow_compression_estimated_data_size},
      {TablePropertiesNames::kFastCompressionEstimatedDataSize,
       &new_table_properties->fast_compression_estimated_data_size},
      {TablePropertiesNames::kTailStartOffset,
       &new_table_properties->tail_start_offset},
      {TablePropertiesNames::kUserDefinedTimestampsPersisted,
       &new_table_properties->user_defined_timestamps_persisted},
      {TablePropertiesNames::kKeyLargestSeqno,
       &new_table_properties->key_largest_seqno},
      {TablePropertiesNames::kKeySmallestSeqno,
       &new_table_properties->key_smallest_seqno},
      {TablePropertiesNames::kDataBlockRestartInterval,
       &new_table_properties->data_block_restart_interval},
      {TablePropertiesNames::kIndexBlockRestartInterval,
       &new_table_properties->index_block_restart_interval},
      {TablePropertiesNames::kSeparateKeyValueInDataBlock,
       &new_table_properties->separate_key_value_in_data_block},
  };

  Status s;
  std::string last_key;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = iter->status();
    if (!s.ok()) {
      break;
    }

    auto key = iter->key().ToString();
    // properties block should be strictly sorted with no duplicate key.
    if (!last_key.empty() &&
        BytewiseComparator()->Compare(key, last_key) <= 0) {
      s = Status::Corruption("properties unsorted");
      break;
    }
    last_key = key;

    auto raw_val = iter->value();
    auto pos = predefined_uint64_properties.find(key);

    if (key == ExternalSstFilePropertyNames::kGlobalSeqno &&
        global_seqno_value_offset != nullptr) {
      *global_seqno_value_offset = iter->ValueOffset();
    }

    if (key == TablePropertiesNames::kReferencedBlobBytes) {
      uint64_t val;
      if (GetVarint64(&raw_val, &val)) {
        new_table_properties->referenced_blob_bytes = val;
      } else {
        ROCKS_LOG_ERROR(ioptions.logger,
                        "Detect malformed value in properties "
                        "meta-block:\tkey: %s\tval: %s",
                        key.c_str(), raw_val.ToString().c_str());
      }
    } else if (pos != predefined_uint64_properties.end()) {
      if (key == TablePropertiesNames::kDeletedKeys ||
          key == TablePropertiesNames::kMergeOperands) {
        // Insert in user-collected properties for API backwards compatibility
        new_table_properties->user_collected_properties.insert(
            {key, raw_val.ToString()});
      }
      // handle predefined rocksdb properties
      uint64_t val;
      if (!GetVarint64(&raw_val, &val)) {
        // skip malformed value
        auto error_msg =
            "Detect malformed value in properties meta-block:"
            "\tkey: " +
            key + "\tval: " + raw_val.ToString();
        ROCKS_LOG_ERROR(ioptions.logger, "%s", error_msg.c_str());
        continue;
      }
      *(pos->second) = val;
    } else if (key == TablePropertiesNames::kDbId) {
      new_table_properties->db_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kDbSessionId) {
      new_table_properties->db_session_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kDbHostId) {
      new_table_properties->db_host_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kFilterPolicy) {
      new_table_properties->filter_policy_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kColumnFamilyName) {
      new_table_properties->column_family_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kComparator) {
      new_table_properties->comparator_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kMergeOperator) {
      new_table_properties->merge_operator_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kPrefixExtractorName) {
      new_table_properties->prefix_extractor_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kPropertyCollectors) {
      new_table_properties->property_collectors_names = raw_val.ToString();
    } else if (key == TablePropertiesNames::kCompression) {
      new_table_properties->compression_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kCompressionOptions) {
      new_table_properties->compression_options = raw_val.ToString();
    } else if (key == TablePropertiesNames::kSequenceNumberTimeMapping) {
      new_table_properties->seqno_to_time_mapping = raw_val.ToString();
    } else {
      // handle user-collected properties
      new_table_properties->user_collected_properties.insert(
          {key, raw_val.ToString()});
    }
  }

  return s;
}

// FIXME: should be a parameter for reading table properties to use persistent
// cache?
Status ReadTablePropertiesHelper(
    const ReadOptions& ro, const BlockHandle& handle,
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ImmutableOptions& ioptions,
    std::unique_ptr<TableProperties>* table_properties,
    MemoryAllocator* memory_allocator) {
  assert(table_properties);

  Status s;
  bool retry = false;
  while (true) {
    BlockContents block_contents;
    size_t len = handle.size() + footer.GetBlockTrailerSize();
    if (!retry) {
      ReadOptions modified_ro = ro;
      modified_ro.verify_checksums = false;
      BlockFetcher block_fetcher(
          file, prefetch_buffer, footer, modified_ro, handle, &block_contents,
          ioptions, false /* decompress */, false /*maybe_compressed*/,
          BlockType::kProperties, nullptr /*decompressor*/,
          PersistentCacheOptions::kEmpty, memory_allocator);
      s = block_fetcher.ReadBlockContents();
      if (!s.ok()) {
        return s;
      }
      assert(block_fetcher.GetBlockSizeWithTrailer() == len);
      TEST_SYNC_POINT_CALLBACK("ReadTablePropertiesHelper:0",
                               &block_contents.data);
    } else {
      assert(s.IsCorruption());
      // If retrying, use a stronger file system read to check and correct
      // data corruption
      IOOptions opts;
      IODebugContext dbg;
      if (PrepareIOFromReadOptions(ro, ioptions.clock, opts, &dbg) !=
          IOStatus::OK()) {
        return s;
      }
      opts.verify_and_reconstruct_read = true;
      std::unique_ptr<char[]> data(new char[len]);
      Slice result;
      IOStatus io_s =
          file->Read(handle.offset(), len, opts, &result, data.get(), &dbg);
      RecordTick(ioptions.stats, FILE_READ_CORRUPTION_RETRY_COUNT);
      if (!io_s.ok()) {
        ROCKS_LOG_INFO(ioptions.info_log,
                       "Reading properties block failed - %s",
                       io_s.ToString().c_str());
        // Return the original corruption error as that's more serious
        return s;
      }
      if (result.size() < len) {
        return Status::Corruption("Reading properties block failed - " +
                                  std::to_string(result.size()) +
                                  " bytes read");
      }
      RecordTick(ioptions.stats, FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
      block_contents = BlockContents(std::move(data), handle.size());
    }

    uint64_t block_size = block_contents.data.size();
    Block properties_block(std::move(block_contents));
    std::unique_ptr<TableProperties> new_table_properties{new TableProperties};
    uint32_t global_seqno_value_offset = 0;
    s = ParsePropertiesBlock(ioptions, properties_block, new_table_properties,
                             &global_seqno_value_offset);

    if (s.ok() && ro.verify_checksums && footer.GetBlockTrailerSize() > 0) {
      s = VerifyBlockChecksum(footer, properties_block.data(), block_size,
                              file->file_name(), handle.offset(),
                              BlockType::kProperties);
      if (s.IsCorruption() && global_seqno_value_offset != 0 &&
          block_size >= sizeof(uint64_t) &&
          global_seqno_value_offset <= block_size - sizeof(uint64_t)) {
        // Older ingestion could rewrite this value without updating the block
        // checksum. Verify against the original zero placeholder.
        std::string tmp_buf(properties_block.data(), len);
        EncodeFixed64(&tmp_buf[global_seqno_value_offset], 0);
        s = VerifyBlockChecksum(footer, tmp_buf.data(), block_size,
                                file->file_name(), handle.offset(),
                                BlockType::kProperties);
      }
    }

    // If we detected a corruption and the file system supports verification
    // and reconstruction, retry the read
    if (s.IsCorruption() && !retry &&
        CheckFSFeatureSupport(ioptions.fs.get(),
                              FSSupportedOps::kVerifyAndReconstructRead)) {
      retry = true;
    } else {
      if (s.ok()) {
        *table_properties = std::move(new_table_properties);
      }
      break;
    }
  }

  return s;
}

Status ReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           const ReadOptions& read_options,
                           std::unique_ptr<TableProperties>* properties,
                           MemoryAllocator* memory_allocator,
                           FilePrefetchBuffer* prefetch_buffer) {
  BlockHandle block_handle;
  Footer footer;
  Status s =
      FindMetaBlockInFile(file, file_size, table_magic_number, ioptions,
                          read_options, kPropertiesBlockName, &block_handle,
                          memory_allocator, prefetch_buffer, &footer);
  if (!s.ok()) {
    return s;
  }

  if (!block_handle.IsNull()) {
    s = ReadTablePropertiesHelper(read_options, block_handle, file,
                                  prefetch_buffer, footer, ioptions, properties,
                                  memory_allocator);
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status FindOptionalMetaBlock(InternalIterator* meta_index_iter,
                             const std::string& meta_block_name,
                             BlockHandle* block_handle) {
  assert(block_handle != nullptr);
  meta_index_iter->Seek(meta_block_name);
  if (meta_index_iter->status().ok()) {
    if (meta_index_iter->Valid() && meta_index_iter->key() == meta_block_name) {
      Slice v = meta_index_iter->value();
      return block_handle->DecodeFrom(&v);
    }
  }
  // else
  *block_handle = BlockHandle::NullBlockHandle();
  return meta_index_iter->status();
}

Status FindMetaBlock(InternalIterator* meta_index_iter,
                     const std::string& meta_block_name,
                     BlockHandle* block_handle) {
  Status s =
      FindOptionalMetaBlock(meta_index_iter, meta_block_name, block_handle);
  if (s.ok() && block_handle->IsNull()) {
    return Status::Corruption("Cannot find the meta block", meta_block_name);
  } else {
    return s;
  }
}

Status ReadMetaIndexBlockInFile(RandomAccessFileReader* file,
                                uint64_t file_size, uint64_t table_magic_number,
                                const ImmutableOptions& ioptions,
                                const ReadOptions& read_options,
                                BlockContents* metaindex_contents,
                                MemoryAllocator* memory_allocator,
                                FilePrefetchBuffer* prefetch_buffer,
                                Footer* footer_out) {
  Footer footer;
  IOOptions opts;
  IODebugContext dbg;
  Status s;
  s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (!s.ok()) {
    return s;
  }
  s = ReadFooterFromFile(opts, file, *ioptions.fs, prefetch_buffer, file_size,
                         &footer, table_magic_number, ioptions.stats);
  if (!s.ok()) {
    return s;
  }
  if (footer_out) {
    *footer_out = footer;
  }

  auto metaindex_handle = footer.metaindex_handle();
  return BlockFetcher(file, prefetch_buffer, footer, read_options,
                      metaindex_handle, metaindex_contents, ioptions,
                      false /* do decompression */, false /*maybe_compressed*/,
                      BlockType::kMetaIndex, nullptr /*decompressor*/,
                      PersistentCacheOptions::kEmpty, memory_allocator)
      .ReadBlockContents();
}

Status FindMetaBlockInFile(
    RandomAccessFileReader* file, uint64_t file_size,
    uint64_t table_magic_number, const ImmutableOptions& ioptions,
    const ReadOptions& read_options, const std::string& meta_block_name,
    BlockHandle* block_handle, MemoryAllocator* memory_allocator,
    FilePrefetchBuffer* prefetch_buffer, Footer* footer_out) {
  BlockContents metaindex_contents;
  auto s = ReadMetaIndexBlockInFile(
      file, file_size, table_magic_number, ioptions, read_options,
      &metaindex_contents, memory_allocator, prefetch_buffer, footer_out);
  if (!s.ok()) {
    return s;
  }
  // meta blocks are never compressed. Need to add uncompress logic if we are to
  // compress it.
  Block metaindex_block(std::move(metaindex_contents));

  std::unique_ptr<InternalIterator> meta_iter;
  meta_iter.reset(metaindex_block.NewMetaIterator());

  return FindMetaBlock(meta_iter.get(), meta_block_name, block_handle);
}

Status ReadMetaBlock(RandomAccessFileReader* file,
                     FilePrefetchBuffer* prefetch_buffer, uint64_t file_size,
                     uint64_t table_magic_number,
                     const ImmutableOptions& ioptions,
                     const ReadOptions& read_options,
                     const std::string& meta_block_name, BlockType block_type,
                     BlockContents* contents,
                     MemoryAllocator* memory_allocator) {
  // TableProperties requires special handling because of checksum issues.
  // Call ReadTableProperties instead for that case.
  assert(block_type != BlockType::kProperties);

  BlockHandle block_handle;
  Footer footer;
  Status status =
      FindMetaBlockInFile(file, file_size, table_magic_number, ioptions,
                          read_options, meta_block_name, &block_handle,
                          memory_allocator, prefetch_buffer, &footer);
  if (!status.ok()) {
    return status;
  }

  return BlockFetcher(file, prefetch_buffer, footer, read_options, block_handle,
                      contents, ioptions, false /* decompress */,
                      false /*maybe_compressed*/, block_type,
                      nullptr /*decompressor*/, PersistentCacheOptions::kEmpty,
                      memory_allocator)
      .ReadBlockContents();
}

}  // namespace ROCKSDB_NAMESPACE
