//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cache_dump_load_impl.h"

#include <limits>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "file/writable_file_writer.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

// Set the dump filter with a list of DBs. Block cache may be shared by multipe
// DBs and we may only want to dump out the blocks belonging to certain DB(s).
// Therefore, a filter is need to decide if the key of the block satisfy the
// requirement.
Status CacheDumperImpl::SetDumpFilter(const std::vector<DB*>& db_list) {
  Status s = Status::OK();
  dump_all_keys_ = false;
  for (size_t i = 0; i < db_list.size(); i++) {
    assert(i < db_list.size());
    TablePropertiesCollection ptc;
    assert(db_list[i] != nullptr);
    s = db_list[i]->GetPropertiesOfAllTables(&ptc);
    if (!s.ok()) {
      return s;
    }
    for (auto id = ptc.begin(); id != ptc.end(); id++) {
      OffsetableCacheKey base;
      // We only want to save cache entries that are portable to another
      // DB::Open, so only save entries with stable keys.
      bool is_stable;
      BlockBasedTable::SetupBaseCacheKey(id->second.get(),
                                         /*cur_db_session_id*/ "",
                                         /*cur_file_num*/ 0, &base, &is_stable);
      if (is_stable) {
        Slice prefix_slice = base.CommonPrefixSlice();
        assert(prefix_slice.size() == OffsetableCacheKey::kCommonPrefixSize);
        prefix_filter_.insert(prefix_slice.ToString());
      }
    }
  }
  return s;
}

// This is the main function to dump out the cache block entries to the writer.
// The writer may create a file or write to other systems. Currently, we will
// iterate the whole block cache, get the blocks, and write them to the writer
IOStatus CacheDumperImpl::DumpCacheEntriesToWriter() {
  // Prepare stage, check the parameters.
  if (cache_ == nullptr) {
    return IOStatus::InvalidArgument("Cache is null");
  }
  if (writer_ == nullptr) {
    return IOStatus::InvalidArgument("CacheDumpWriter is null");
  }
  // Set the system clock
  if (options_.clock == nullptr) {
    return IOStatus::InvalidArgument("System clock is null");
  }
  clock_ = options_.clock;

  deadline_ = options_.deadline;

  // Set the sequence number
  sequence_num_ = 0;

  // Dump stage, first, we write the hader
  IOStatus io_s = WriteHeader();
  if (!io_s.ok()) {
    return io_s;
  }

  // Then, we iterate the block cache and dump out the blocks that are not
  // filtered out.
  std::string buf;
  cache_->ApplyToAllEntries(DumpOneBlockCallBack(buf), {});

  // Finally, write the footer
  io_s = WriteFooter();
  if (!io_s.ok()) {
    return io_s;
  }
  io_s = writer_->Close();
  return io_s;
}

// Check if we need to filter out the block based on its key
bool CacheDumperImpl::ShouldFilterOut(const Slice& key) {
  if (key.size() < OffsetableCacheKey::kCommonPrefixSize) {
    return /*filter out*/ true;
  }
  Slice key_prefix(key.data(), OffsetableCacheKey::kCommonPrefixSize);
  std::string prefix = key_prefix.ToString();
  // Filter out if not found
  return prefix_filter_.find(prefix) == prefix_filter_.end();
}

// This is the callback function which will be applied to
// Cache::ApplyToAllEntries. In this callback function, we will get the block
// type, decide if the block needs to be dumped based on the filter, and write
// the block through the provided writer. `buf` is passed in for efficiennt
// reuse.
std::function<void(const Slice&, Cache::ObjectPtr, size_t,
                   const Cache::CacheItemHelper*)>
CacheDumperImpl::DumpOneBlockCallBack(std::string& buf) {
  return [&](const Slice& key, Cache::ObjectPtr value, size_t /*charge*/,
             const Cache::CacheItemHelper* helper) {
    if (helper == nullptr || helper->size_cb == nullptr ||
        helper->saveto_cb == nullptr) {
      // Not compatible with dumping. Skip this entry.
      return;
    }

    if (options_.max_size_bytes > 0 &&
        dumped_size_bytes_ > options_.max_size_bytes) {
      return;
    }

    uint64_t timestamp = clock_->NowMicros();
    if (deadline_.count()) {
      std::chrono::microseconds now = std::chrono::microseconds(timestamp);
      if (now >= deadline_) {
        return;
      }
    }

    CacheEntryRole role = helper->role;
    CacheDumpUnitType type = CacheDumpUnitType::kBlockTypeMax;

    switch (role) {
      case CacheEntryRole::kDataBlock:
        type = CacheDumpUnitType::kData;
        break;
      case CacheEntryRole::kFilterBlock:
        type = CacheDumpUnitType::kFilter;
        break;
      case CacheEntryRole::kFilterMetaBlock:
        type = CacheDumpUnitType::kFilterMetaBlock;
        break;
      case CacheEntryRole::kIndexBlock:
        type = CacheDumpUnitType::kIndex;
        break;
      case CacheEntryRole::kDeprecatedFilterBlock:
      case CacheEntryRole::kOtherBlock:
      case CacheEntryRole::kWriteBuffer:
      case CacheEntryRole::kCompressionDictionaryBuildingBuffer:
      case CacheEntryRole::kFilterConstruction:
      case CacheEntryRole::kBlockBasedTableReader:
      case CacheEntryRole::kFileMetadata:
      case CacheEntryRole::kBlobValue:
      case CacheEntryRole::kBlobCache:
      case CacheEntryRole::kMisc:
      default:
        // Filter out other entries
        // FIXME? Do we need the CacheDumpUnitTypes? UncompressionDict?
        return;
    }

    // based on the key prefix, check if the block should be filter out.
    if (!dump_all_keys_ && ShouldFilterOut(key)) {
      return;
    }

    assert(type != CacheDumpUnitType::kBlockTypeMax);

    // Use cache item helper to get persistable data
    // FIXME: reduce copying
    size_t len = helper->size_cb(value);
    buf.assign(len, '\0');
    Status s = helper->saveto_cb(value, /*start*/ 0, len, buf.data());

    if (s.ok()) {
      // Write it out
      WriteBlock(type, key, buf, timestamp).PermitUncheckedError();
      dumped_size_bytes_ += len;
    }
  };
}

// Write the block to the writer. It takes the timestamp of the
// block being copied from block cache, block type, key, block pointer,
// block size and block checksum as the input. When writing the dumper raw
// block, we first create the dump unit and encoude it to a string. Then,
// we calculate the checksum of the whole dump unit string and store it in
// the dump unit metadata.
// First, we write the metadata first, which is a fixed size string. Then, we
// Append the dump unit string to the writer.
IOStatus CacheDumperImpl::WriteBlock(CacheDumpUnitType type, const Slice& key,
                                     const Slice& value, uint64_t timestamp) {
  uint32_t value_checksum = crc32c::Value(value.data(), value.size());

  // First, serialize the block information in a string
  DumpUnit dump_unit;
  dump_unit.timestamp = timestamp;
  dump_unit.key = key;
  dump_unit.type = type;
  dump_unit.value_len = value.size();
  dump_unit.value = const_cast<char*>(value.data());
  dump_unit.value_checksum = value_checksum;
  std::string encoded_data;
  CacheDumperHelper::EncodeDumpUnit(dump_unit, &encoded_data);

  // Second, create the metadata, which contains a sequence number, the dump
  // unit string checksum and the string size. The sequence number monotonically
  // increases from 0.
  DumpUnitMeta unit_meta;
  unit_meta.sequence_num = sequence_num_;
  sequence_num_++;
  unit_meta.dump_unit_checksum =
      crc32c::Value(encoded_data.data(), encoded_data.size());
  unit_meta.dump_unit_size = encoded_data.size();
  std::string encoded_meta;
  CacheDumperHelper::EncodeDumpUnitMeta(unit_meta, &encoded_meta);

  // We write the metadata first.
  assert(writer_ != nullptr);
  IOStatus io_s = writer_->WriteMetadata(encoded_meta);
  if (!io_s.ok()) {
    return io_s;
  }
  // followed by the dump unit.
  return writer_->WritePacket(encoded_data);
}

// Before we write any block, we write the header first to store the cache dump
// format version, rocksdb version, and brief intro.
IOStatus CacheDumperImpl::WriteHeader() {
  std::string header_key = "header";
  std::ostringstream s;
  s << kTraceMagic << "\t"
    << "Cache dump format version: " << kCacheDumpMajorVersion << "."
    << kCacheDumpMinorVersion << "\t" << "RocksDB Version: " << kMajorVersion
    << "." << kMinorVersion << "\t"
    << "Format: dump_unit_metadata <sequence_number, dump_unit_checksum, "
       "dump_unit_size>, dump_unit <timestamp, key, block_type, "
       "block_size, block_data, block_checksum> cache_value\n";
  std::string header_value(s.str());
  CacheDumpUnitType type = CacheDumpUnitType::kHeader;
  uint64_t timestamp = clock_->NowMicros();
  return WriteBlock(type, header_key, header_value, timestamp);
}

// Write the footer after all the blocks are stored to indicate the ending.
IOStatus CacheDumperImpl::WriteFooter() {
  std::string footer_key = "footer";
  std::string footer_value("cache dump completed");
  CacheDumpUnitType type = CacheDumpUnitType::kFooter;
  uint64_t timestamp = clock_->NowMicros();
  return WriteBlock(type, footer_key, footer_value, timestamp);
}

// This is the main function to restore the cache entries to secondary cache.
// First, we check if all the arguments are valid. Then, we read the block
// sequentially from the reader and insert them to the secondary cache.
IOStatus CacheDumpedLoaderImpl::RestoreCacheEntriesToSecondaryCache() {
  // TODO: remove this line when options are used in the loader
  (void)options_;
  // Step 1: we check if all the arguments are valid
  if (secondary_cache_ == nullptr) {
    return IOStatus::InvalidArgument("Secondary Cache is null");
  }
  if (reader_ == nullptr) {
    return IOStatus::InvalidArgument("CacheDumpReader is null");
  }

  // Step 2: read the header
  // TODO: we need to check the cache dump format version and RocksDB version
  // after the header is read out.
  IOStatus io_s;
  DumpUnit dump_unit;
  std::string data;
  io_s = ReadHeader(&data, &dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }

  // Step 3: read out the rest of the blocks from the reader. The loop will stop
  // either I/O status is not ok or we reach to the the end.
  while (io_s.ok()) {
    dump_unit.reset();
    data.clear();
    // read the content and store in the dump_unit
    io_s = ReadCacheBlock(&data, &dump_unit);
    if (!io_s.ok()) {
      break;
    }
    if (dump_unit.type == CacheDumpUnitType::kFooter) {
      break;
    }
    // Create the uncompressed_block based on the information in the dump_unit
    // (There is no block trailer here compatible with block-based SST file.)
    Slice content =
        Slice(static_cast<char*>(dump_unit.value), dump_unit.value_len);
    Status s = secondary_cache_->InsertSaved(dump_unit.key, content);
    if (!s.ok()) {
      io_s = status_to_io_status(std::move(s));
    }
  }
  if (dump_unit.type == CacheDumpUnitType::kFooter) {
    return IOStatus::OK();
  } else {
    return io_s;
  }
}

// Read and copy the dump unit metadata to std::string data, decode and create
// the unit metadata based on the string
IOStatus CacheDumpedLoaderImpl::ReadDumpUnitMeta(std::string* data,
                                                 DumpUnitMeta* unit_meta) {
  assert(reader_ != nullptr);
  assert(data != nullptr);
  assert(unit_meta != nullptr);
  IOStatus io_s = reader_->ReadMetadata(data);
  if (!io_s.ok()) {
    return io_s;
  }
  return status_to_io_status(
      CacheDumperHelper::DecodeDumpUnitMeta(*data, unit_meta));
}

// Read and copy the dump unit to std::string data, decode and create the unit
// based on the string
IOStatus CacheDumpedLoaderImpl::ReadDumpUnit(size_t len, std::string* data,
                                             DumpUnit* unit) {
  assert(reader_ != nullptr);
  assert(data != nullptr);
  assert(unit != nullptr);
  IOStatus io_s = reader_->ReadPacket(data);
  if (!io_s.ok()) {
    return io_s;
  }
  if (data->size() != len) {
    return IOStatus::Corruption(
        "The data being read out does not match the size stored in metadata!");
  }
  Slice block;
  return status_to_io_status(CacheDumperHelper::DecodeDumpUnit(*data, unit));
}

// Read the header
IOStatus CacheDumpedLoaderImpl::ReadHeader(std::string* data,
                                           DumpUnit* dump_unit) {
  DumpUnitMeta header_meta;
  header_meta.reset();
  std::string meta_string;
  IOStatus io_s = ReadDumpUnitMeta(&meta_string, &header_meta);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = ReadDumpUnit(header_meta.dump_unit_size, data, dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }
  uint32_t unit_checksum = crc32c::Value(data->data(), data->size());
  if (unit_checksum != header_meta.dump_unit_checksum) {
    return IOStatus::Corruption("Read header unit corrupted!");
  }
  return io_s;
}

// Read the blocks after header is read out
IOStatus CacheDumpedLoaderImpl::ReadCacheBlock(std::string* data,
                                               DumpUnit* dump_unit) {
  // According to the write process, we read the dump_unit_metadata first
  DumpUnitMeta unit_meta;
  unit_meta.reset();
  std::string unit_string;
  IOStatus io_s = ReadDumpUnitMeta(&unit_string, &unit_meta);
  if (!io_s.ok()) {
    return io_s;
  }

  // Based on the information in the dump_unit_metadata, we read the dump_unit
  // and verify if its content is correct.
  io_s = ReadDumpUnit(unit_meta.dump_unit_size, data, dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }
  uint32_t unit_checksum = crc32c::Value(data->data(), data->size());
  if (unit_checksum != unit_meta.dump_unit_checksum) {
    return IOStatus::Corruption(
        "Checksum does not match! Read dumped unit corrupted!");
  }
  return io_s;
}

}  // namespace ROCKSDB_NAMESPACE
