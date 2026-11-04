//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/plain/plain_table_builder.h"

#include <cassert>
#include <limits>
#include <map>
#include <string>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_bloom.h"
#include "table/plain/plain_table_factory.h"
#include "table/plain/plain_table_index.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// a utility that helps writing block content to the file
//   @offset will advance if @block_contents was successfully written.
//   @block_handle the block handle this particular block.
IOStatus WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                    uint64_t* offset, BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(block_contents.size());
  IOStatus io_s = file->Append(block_contents, IOOptions());

  if (io_s.ok()) {
    *offset += block_contents.size();
  }
  return io_s;
}

}  // namespace

// kPlainTableMagicNumber was picked by running
//    echo rocksdb.table.plain | sha1sum
// and taking the leading 64 bits.
const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

PlainTableBuilder::PlainTableBuilder(
    const ImmutableOptions& ioptions, const MutableCFOptions& moptions,
    const InternalTblPropCollFactories* internal_tbl_prop_coll_factories,
    uint32_t column_family_id, int level_at_creation, WritableFileWriter* file,
    uint32_t user_key_len, EncodingType encoding_type, size_t index_sparseness,
    uint32_t bloom_bits_per_key, const std::string& column_family_name,
    uint32_t num_probes, size_t huge_page_tlb_size, double hash_table_ratio,
    bool store_index_in_file, const std::string& db_id,
    const std::string& db_session_id, uint64_t file_number)
    : ioptions_(ioptions),
      moptions_(moptions),
      bloom_block_(num_probes),
      file_(file),
      bloom_bits_per_key_(bloom_bits_per_key),
      huge_page_tlb_size_(huge_page_tlb_size),
      encoder_(encoding_type, user_key_len, moptions.prefix_extractor.get(),
               index_sparseness),
      store_index_in_file_(store_index_in_file),
      prefix_extractor_(moptions.prefix_extractor.get()) {
  // Build index block and save it in the file if hash_table_ratio > 0
  if (store_index_in_file_) {
    assert(hash_table_ratio > 0 || IsTotalOrderMode());
    index_builder_.reset(new PlainTableIndexBuilder(
        &arena_, ioptions, moptions.prefix_extractor.get(), index_sparseness,
        hash_table_ratio, huge_page_tlb_size_));
    properties_
        .user_collected_properties[PlainTablePropertyNames::kBloomVersion] =
        "1";  // For future use
  }

  properties_.fixed_key_len = user_key_len;

  // for plain table, we put all the data in a big chuck.
  properties_.num_data_blocks = 1;
  // Fill it later if store_index_in_file_ == true
  properties_.index_size = 0;
  properties_.filter_size = 0;
  // To support roll-back to previous version, now still use version 0 for
  // plain encoding.
  properties_.format_version = (encoding_type == kPlain) ? 0 : 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;
  properties_.db_id = db_id;
  properties_.db_session_id = db_session_id;
  properties_.db_host_id = ioptions.db_host_id;
  if (!ReifyDbHostIdProperty(ioptions_.env, &properties_.db_host_id).ok()) {
    ROCKS_LOG_INFO(ioptions_.logger, "db_host_id property will not be set");
  }
  properties_.orig_file_number = file_number;
  properties_.prefix_extractor_name =
      moptions_.prefix_extractor != nullptr
          ? moptions_.prefix_extractor->AsString()
          : "nullptr";
  properties_.comparator_name = ioptions_.user_comparator != nullptr
                                    ? ioptions_.user_comparator->Name()
                                    : "nullptr";
  std::string val;
  PutFixed32(&val, static_cast<uint32_t>(encoder_.GetEncodingType()));
  properties_
      .user_collected_properties[PlainTablePropertyNames::kEncodingType] = val;

  assert(internal_tbl_prop_coll_factories);
  for (auto& factory : *internal_tbl_prop_coll_factories) {
    assert(factory);

    std::unique_ptr<InternalTblPropColl> collector{
        factory->CreateInternalTblPropColl(column_family_id, level_at_creation,
                                           ioptions.num_levels)};
    if (collector) {
      table_properties_collectors_.emplace_back(std::move(collector));
    }
  }
}

PlainTableBuilder::~PlainTableBuilder() {
  // They are supposed to have been passed to users through Finish()
  // if the file succeeds.
  status_.PermitUncheckedError();
  io_status_.PermitUncheckedError();
}

void PlainTableBuilder::Add(const Slice& key, const Slice& value) {
  // temp buffer for metadata bytes between key and value.
  char meta_bytes_buf[6];
  size_t meta_bytes_buf_size = 0;
  const IOOptions opts;

  ParsedInternalKey internal_key;
  if (!ParseInternalKey(key, &internal_key, false /* log_err_key */)
           .ok()) {  // TODO
    assert(false);
    return;
  }
  if (internal_key.type == kTypeRangeDeletion) {
    status_ = Status::NotSupported("Range deletion unsupported");
    return;
  }

#ifndef NDEBUG
  bool skip = false;
  TEST_SYNC_POINT_CALLBACK("PlainTableBuilder::Add::skip", (void*)&skip);
  if (skip) {
    return;
  }
#endif  // !NDEBUG

  // Store key hash
  if (store_index_in_file_) {
    if (moptions_.prefix_extractor == nullptr) {
      keys_or_prefixes_hashes_.push_back(GetSliceHash(internal_key.user_key));
    } else {
      Slice prefix =
          moptions_.prefix_extractor->Transform(internal_key.user_key);
      keys_or_prefixes_hashes_.push_back(GetSliceHash(prefix));
    }
  }

  // Write value
  assert(offset_ <= std::numeric_limits<uint32_t>::max());
  auto prev_offset = static_cast<uint32_t>(offset_);
  // Write out the key
  io_status_ = encoder_.AppendKey(key, file_, &offset_, meta_bytes_buf,
                                  &meta_bytes_buf_size);
  if (SaveIndexInFile()) {
    index_builder_->AddKeyPrefix(GetPrefix(internal_key), prev_offset);
  }

  // Write value length
  uint32_t value_size = static_cast<uint32_t>(value.size());
  if (io_status_.ok()) {
    char* end_ptr =
        EncodeVarint32(meta_bytes_buf + meta_bytes_buf_size, value_size);
    assert(end_ptr <= meta_bytes_buf + sizeof(meta_bytes_buf));
    meta_bytes_buf_size = end_ptr - meta_bytes_buf;
    io_status_ =
        file_->Append(Slice(meta_bytes_buf, meta_bytes_buf_size), opts);
  }

  // Write value
  if (io_status_.ok()) {
    io_status_ = file_->Append(value, opts);
    offset_ += value_size + meta_bytes_buf_size;
  }

  if (io_status_.ok()) {
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    if (internal_key.type == kTypeDeletion ||
        internal_key.type == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (internal_key.type == kTypeMerge) {
      properties_.num_merge_operands++;
    }
  }

  // notify property collectors
  NotifyCollectTableCollectorsOnAdd(
      key, value, offset_, table_properties_collectors_, ioptions_.logger);
  status_ = io_status_;
}

Status PlainTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;

  properties_.data_size = offset_;

  //  Write the following blocks
  //  1. [meta block: bloom] - optional
  //  2. [meta block: index] - optional
  //  3. [meta block: properties]
  //  4. [metaindex block]
  //  5. [footer]

  MetaIndexBuilder meta_index_builer;

  if (store_index_in_file_ && (properties_.num_entries > 0)) {
    assert(properties_.num_entries <= std::numeric_limits<uint32_t>::max());
    BlockHandle bloom_block_handle;
    if (bloom_bits_per_key_ > 0) {
      bloom_block_.SetTotalBits(
          &arena_,
          static_cast<uint32_t>(properties_.num_entries) * bloom_bits_per_key_,
          ioptions_.bloom_locality, huge_page_tlb_size_, ioptions_.logger);

      PutVarint32(&properties_.user_collected_properties
                       [PlainTablePropertyNames::kNumBloomBlocks],
                  bloom_block_.GetNumBlocks());

      bloom_block_.AddKeysHashes(keys_or_prefixes_hashes_);

      Slice bloom_finish_result = bloom_block_.Finish();

      properties_.filter_size = bloom_finish_result.size();
      io_status_ =
          WriteBlock(bloom_finish_result, file_, &offset_, &bloom_block_handle);

      if (!io_status_.ok()) {
        status_ = io_status_;
        return status_;
      }
      meta_index_builer.Add(BloomBlockBuilder::kBloomBlock, bloom_block_handle);
    }
    BlockHandle index_block_handle;
    Slice index_finish_result = index_builder_->Finish();

    properties_.index_size = index_finish_result.size();
    io_status_ =
        WriteBlock(index_finish_result, file_, &offset_, &index_block_handle);

    if (!io_status_.ok()) {
      status_ = io_status_;
      return status_;
    }

    meta_index_builer.Add(PlainTableIndexBuilder::kPlainTableIndexBlock,
                          index_block_handle);
  }

  // Calculate bloom block size and index block size
  PropertyBlockBuilder property_block_builder;
  // -- Add basic properties
  property_block_builder.AddTableProperty(properties_);
  // -- Add eixsting user collected properties
  property_block_builder.Add(properties_.user_collected_properties);
  // -- Add more user collected properties
  UserCollectedProperties more_user_collected_properties;
  NotifyCollectTableCollectorsOnFinish(
      table_properties_collectors_, ioptions_.logger, &property_block_builder,
      more_user_collected_properties, properties_.readable_properties);
  properties_.user_collected_properties.insert(
      more_user_collected_properties.begin(),
      more_user_collected_properties.end());

  // -- Write property block
  BlockHandle property_block_handle;
  io_status_ = WriteBlock(property_block_builder.Finish(), file_, &offset_,
                          &property_block_handle);
  if (!io_status_.ok()) {
    status_ = io_status_;
    return status_;
  }
  meta_index_builer.Add(kPropertiesBlockName, property_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  io_status_ = WriteBlock(meta_index_builer.Finish(), file_, &offset_,
                          &metaindex_block_handle);
  if (!io_status_.ok()) {
    status_ = io_status_;
    return status_;
  }

  // Write Footer
  // no need to write out new footer if we're using default checksum
  FooterBuilder footer;
  Status s = footer.Build(kPlainTableMagicNumber, /* format_version */ 0,
                          offset_, kNoChecksum, metaindex_block_handle);
  if (!s.ok()) {
    status_ = s;
    return status_;
  }
  io_status_ = file_->Append(footer.GetSlice(), IOOptions());
  if (io_status_.ok()) {
    offset_ += footer.GetSlice().size();
  }
  status_ = io_status_;
  return status_;
}

void PlainTableBuilder::Abandon() { closed_ = true; }

uint64_t PlainTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t PlainTableBuilder::FileSize() const { return offset_; }

std::string PlainTableBuilder::GetFileChecksum() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* PlainTableBuilder::GetFileChecksumFuncName() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}
void PlainTableBuilder::SetSeqnoTimeTableProperties(
    const SeqnoToTimeMapping& relevant_mapping, uint64_t uint_64) {
  // TODO: storing seqno to time mapping is not yet support for plain table.
  TableBuilder::SetSeqnoTimeTableProperties(relevant_mapping, uint_64);
}

}  // namespace ROCKSDB_NAMESPACE
