//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/sst_file_writer.h"

#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "file/writable_file_writer.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/embedded_blob_sst.h"
#include "table/format.h"
#include "table/prepared_file_info.h"
#include "table/sst_file_writer_collectors.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

namespace {

OptionTypeInfo InternalKeyOptionTypeInfo(int offset) {
  OptionTypeInfo info(offset, OptionType::kUnknown);
  info.SetParseFunc([](const ConfigOptions&, const std::string& name,
                       const std::string& value, void* addr) {
    std::string decoded;
    if (!Slice(value).DecodeHex(&decoded)) {
      return Status::InvalidArgument("Invalid encoded ", name);
    }
    auto* key = static_cast<InternalKey*>(addr);
    key->DecodeFrom(decoded);
    if (!key->Valid()) {
      return Status::InvalidArgument("Invalid ", name);
    }
    return Status::OK();
  });
  info.SetSerializeFunc([](const ConfigOptions&, const std::string& name,
                           const void* addr, std::string* value) {
    const auto* key = static_cast<const InternalKey*>(addr);
    if (!key->Valid()) {
      return Status::InvalidArgument("Invalid ", name);
    }
    *value = key->Encode().ToString(true);
    return Status::OK();
  });
  return info;
}

OptionTypeInfo TablePropertiesOptionTypeInfo(int offset) {
  OptionTypeInfo info(offset, OptionType::kUnknown);
  info.SetParseFunc([](const ConfigOptions& config_options,
                       const std::string& name, const std::string& value,
                       void* addr) {
    std::string decoded;
    if (!Slice(value).DecodeHex(&decoded)) {
      return Status::InvalidArgument("Invalid encoded ", name);
    }
    return TableProperties::Parse(config_options, decoded,
                                  static_cast<TableProperties*>(addr));
  });
  info.SetSerializeFunc([](const ConfigOptions& config_options,
                           const std::string&, const void* addr,
                           std::string* value) {
    std::string serialized;
    Status s = static_cast<const TableProperties*>(addr)->Serialize(
        config_options, &serialized);
    if (s.ok()) {
      *value = Slice(serialized).ToString(true);
    }
    return s;
  });
  return info;
}

const std::unordered_map<std::string, OptionTypeInfo>&
PreparedFileInfoTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"file_size",
       {offsetof(PreparedFileInfo, file_size), OptionType::kUInt64T}},
      {"smallest_internal_key",
       InternalKeyOptionTypeInfo(offsetof(PreparedFileInfo, smallest))},
      {"largest_internal_key",
       InternalKeyOptionTypeInfo(offsetof(PreparedFileInfo, largest))},
      {"table_properties", TablePropertiesOptionTypeInfo(
                               offsetof(PreparedFileInfo, table_properties))},
  };
  return type_info;
}

}  // namespace

Status SerializePreparedFileInfo(const PreparedFileInfo& prepared_file_info,
                                 std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("Null PreparedFileInfo output");
  }

  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  std::string serialized;
  Status s =
      OptionTypeInfo::SerializeType(config_options, PreparedFileInfoTypeInfo(),
                                    &prepared_file_info, &serialized);
  if (!s.ok()) {
    return s;
  }

  *output = std::move(serialized);
  PutFixed32(output,
             crc32c::Mask(crc32c::Value(output->data(), output->size())));
  return Status::OK();
}

Status DeserializePreparedFileInfo(
    const Slice& input,
    std::shared_ptr<const PreparedFileInfo>* prepared_file_info) {
  if (prepared_file_info == nullptr) {
    return Status::InvalidArgument("Null PreparedFileInfo output");
  }
  prepared_file_info->reset();

  if (input.size() < sizeof(uint32_t)) {
    return Status::Corruption("PreparedFileInfo is too short");
  }
  Slice encoded(input.data(), input.size() - sizeof(uint32_t));
  const uint32_t stored_checksum =
      DecodeFixed32(input.data() + input.size() - sizeof(uint32_t));
  const uint32_t actual_checksum =
      crc32c::Mask(crc32c::Value(encoded.data(), encoded.size()));
  if (stored_checksum != actual_checksum) {
    return Status::Corruption("PreparedFileInfo checksum mismatch");
  }

  std::unordered_map<std::string, std::string> fields;
  Status s = StringToMap(encoded.ToString(), &fields);
  if (!s.ok()) {
    return s;
  }
  for (const auto& field : PreparedFileInfoTypeInfo()) {
    if (fields.find(field.first) == fields.end()) {
      return Status::Corruption("PreparedFileInfo is missing ", field.first);
    }
  }

  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  auto result = std::make_shared<PreparedFileInfo>();
  s = OptionTypeInfo::ParseType(config_options, fields,
                                PreparedFileInfoTypeInfo(), result.get());
  if (!s.ok()) {
    return s;
  }

  if (!result->smallest.Valid() || !result->largest.Valid()) {
    return Status::Corruption("PreparedFileInfo has invalid key bounds");
  }

  *prepared_file_info = std::move(result);
  return Status::OK();
}

const std::string ExternalSstFilePropertyNames::kVersion =
    "rocksdb.external_sst_file.version";
const std::string ExternalSstFilePropertyNames::kGlobalSeqno =
    "rocksdb.external_sst_file.global_seqno";

const size_t kFadviseTrigger = 1024 * 1024;  // 1MB

struct SstFileWriter::Rep {
  Rep(const EnvOptions& _env_options, const Options& options,
      Env::IOPriority _io_priority, const Comparator* _user_comparator,
      ColumnFamilyHandle* _cfh, bool _invalidate_page_cache,
      std::string _db_session_id)
      : env_options(_env_options),
        ioptions(options),
        mutable_cf_options(options),
        io_priority(_io_priority),
        internal_comparator(_user_comparator),
        cfh(_cfh),
        invalidate_page_cache(_invalidate_page_cache),
        db_session_id(_db_session_id),
        ts_sz(_user_comparator->timestamp_size()),
        strip_timestamp(ts_sz > 0 &&
                        !ioptions.persist_user_defined_timestamps) {
    // TODO (hx235): pass in `WriteOptions` instead of `rate_limiter_priority`
    // during construction
    write_options.rate_limiter_priority = io_priority;
  }

  std::unique_ptr<WritableFileWriter> file_writer;
  std::unique_ptr<TableBuilder> builder;
  EnvOptions env_options;
  ImmutableOptions ioptions;
  MutableCFOptions mutable_cf_options;
  Env::IOPriority io_priority;
  WriteOptions write_options;
  InternalKeyComparator internal_comparator;
  ExternalSstFileInfo file_info;
  InternalKey smallest_internal_key;
  InternalKey ikey;
  InternalKey smallest_range_del_internal_key;
  InternalKey largest_range_del_internal_key;
  std::string column_family_name;
  ColumnFamilyHandle* cfh;
  // If true, We will give the OS a hint that this file pages is not needed
  // every time we write 1MB to the file.
  bool invalidate_page_cache;
  // The size of the file during the last time we called Fadvise to remove
  // cached pages from page cache.
  uint64_t last_fadvise_size = 0;
  std::string db_session_id;
  uint64_t next_file_number = 1;
  size_t ts_sz;
  bool strip_timestamp;
  std::unique_ptr<EmbeddedBlobSstBuilderOptions> embedded_blob_options;

  Status AddImpl(const Slice& user_key, const Slice& value,
                 ValueType value_type) {
    if (!builder) {
      return Status::InvalidArgument("File is not opened");
    }
    if (!builder->status().ok()) {
      return builder->status();
    }

    // user_key + kNumInternalBytes must fit in uint32_t (BlockBuilder
    // assumption). Also check value size.
    if (user_key.size() >
        size_t{std::numeric_limits<uint32_t>::max()} - kNumInternalBytes) {
      return Status::InvalidArgument("key is too large");
    }
    if (value.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("value is too large");
    }

    assert(user_key.size() >= ts_sz);
    if (strip_timestamp) {
      // In this mode, we expect users to always provide a min timestamp.
      if (internal_comparator.user_comparator()->CompareTimestamp(
              Slice(user_key.data() + user_key.size() - ts_sz, ts_sz),
              MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted.");
      }
    }
    if (file_info.num_entries > 0) {
      if (internal_comparator.user_comparator()->Compare(
              user_key, ikey.user_key()) <= 0) {
        // Make sure that keys are added in order
        return Status::InvalidArgument(
            "Keys must be added in strict ascending order.");
      }
    }

    assert(value_type == kTypeValue || value_type == kTypeMerge ||
           value_type == kTypeDeletion ||
           value_type == kTypeDeletionWithTimestamp ||
           value_type == kTypeWideColumnEntity);

    constexpr SequenceNumber sequence_number = 0;

    ikey.Set(user_key, sequence_number, value_type);

    builder->Add(ikey.Encode(), value);

    // update file info
    if (file_info.num_entries == 0) {
      smallest_internal_key = ikey;
    }
    file_info.num_entries++;
    file_info.file_size = builder->FileSize();

    InvalidatePageCache(false /* closing */).PermitUncheckedError();
    return builder->status();
  }

  Status Add(const Slice& user_key, const Slice& value, ValueType value_type) {
    if (internal_comparator.user_comparator()->timestamp_size() != 0) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    return AddImpl(user_key, value, value_type);
  }

  Status Add(const Slice& user_key, const Slice& timestamp, const Slice& value,
             ValueType value_type) {
    const size_t timestamp_size = timestamp.size();

    if (internal_comparator.user_comparator()->timestamp_size() !=
        timestamp_size) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    const size_t user_key_size = user_key.size();

    if (user_key.data() + user_key_size == timestamp.data()) {
      Slice user_key_with_ts(user_key.data(), user_key_size + timestamp_size);
      return AddImpl(user_key_with_ts, value, value_type);
    }

    std::string user_key_with_ts;
    user_key_with_ts.reserve(user_key_size + timestamp_size);
    user_key_with_ts.append(user_key.data(), user_key_size);
    user_key_with_ts.append(timestamp.data(), timestamp_size);

    return AddImpl(user_key_with_ts, value, value_type);
  }

  Status AddEntity(const Slice& user_key, const WideColumns& columns) {
    WideColumns sorted_columns(columns);
    WideColumnsHelper::SortColumns(sorted_columns);

    std::string entity;
    const Status s = WideColumnSerialization::Serialize(sorted_columns, entity);
    if (!s.ok()) {
      return s;
    }
    if (entity.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("wide column entity is too large");
    }
    return Add(user_key, entity, kTypeWideColumnEntity);
  }

  Status DeleteRangeImpl(const Slice& begin_key, const Slice& end_key) {
    if (!builder) {
      return Status::InvalidArgument("File is not opened");
    }
    // begin_key + kNumInternalBytes must fit in uint32_t (BlockBuilder
    // assumption). end_key is stored as the value in the range deletion
    // block, so it only needs to fit in uint32_t.
    if (begin_key.size() >
        size_t{std::numeric_limits<uint32_t>::max()} - kNumInternalBytes) {
      return Status::InvalidArgument("key is too large");
    }
    if (end_key.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("end key is too large");
    }
    int cmp = internal_comparator.user_comparator()->CompareWithoutTimestamp(
        begin_key, end_key);
    if (cmp > 0) {
      // It's an empty range where endpoints appear mistaken. Don't bother
      // applying it to the DB, and return an error to the user.
      return Status::InvalidArgument("end key comes before start key");
    } else if (cmp == 0) {
      // It's an empty range. Don't bother applying it to the DB.
      return Status::OK();
    }

    assert(begin_key.size() >= ts_sz);
    assert(end_key.size() >= ts_sz);
    Slice begin_key_ts =
        Slice(begin_key.data() + begin_key.size() - ts_sz, ts_sz);
    Slice end_key_ts = Slice(end_key.data() + end_key.size() - ts_sz, ts_sz);
    assert(begin_key_ts.compare(end_key_ts) == 0);
    if (strip_timestamp) {
      // In this mode, we expect users to always provide a min timestamp.
      if (internal_comparator.user_comparator()->CompareTimestamp(
              begin_key_ts, MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted for start key.");
      }
      if (internal_comparator.user_comparator()->CompareTimestamp(
              end_key_ts, MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted for end key.");
      }
    }

    RangeTombstone tombstone(begin_key, end_key, 0 /* Sequence Number */);
    InternalKey range_del_start_bound = tombstone.SerializeKey();
    InternalKey range_del_end_bound = tombstone.SerializeEndKey();
    builder->Add(range_del_start_bound.Encode(), end_key);

    if (file_info.num_range_del_entries == 0 ||
        internal_comparator.Compare(range_del_start_bound,
                                    smallest_range_del_internal_key) < 0) {
      smallest_range_del_internal_key = std::move(range_del_start_bound);
    }
    if (file_info.num_range_del_entries == 0 ||
        internal_comparator.Compare(range_del_end_bound,
                                    largest_range_del_internal_key) > 0) {
      largest_range_del_internal_key = std::move(range_del_end_bound);
    }

    // update file info
    file_info.num_range_del_entries++;
    file_info.file_size = builder->FileSize();

    InvalidatePageCache(false /* closing */).PermitUncheckedError();
    return Status::OK();
  }

  Status DeleteRange(const Slice& begin_key, const Slice& end_key) {
    if (internal_comparator.user_comparator()->timestamp_size() != 0) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }
    return DeleteRangeImpl(begin_key, end_key);
  }

  // Returns `user_key` with `timestamp` appended. When the two are already
  // adjacent in memory the result aliases them, otherwise `buf` supplies the
  // backing storage and must outlive the returned slice. A single timestamp
  // slice can be adjacent to at most one key, so each key has to be tested on
  // its own; extending a key that is not ts-adjacent reads out of bounds.
  static Slice AppendTimestamp(const Slice& user_key, const Slice& timestamp,
                               std::string* buf) {
    const size_t user_key_size = user_key.size();
    const size_t timestamp_size = timestamp.size();

    if (user_key.data() + user_key_size == timestamp.data()) {
      return Slice(user_key.data(), user_key_size + timestamp_size);
    }

    buf->reserve(user_key_size + timestamp_size);
    buf->append(user_key.data(), user_key_size);
    buf->append(timestamp.data(), timestamp_size);
    return Slice(*buf);
  }

  // begin_key and end_key should be users keys without timestamp.
  Status DeleteRange(const Slice& begin_key, const Slice& end_key,
                     const Slice& timestamp) {
    if (internal_comparator.user_comparator()->timestamp_size() !=
        timestamp.size()) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    std::string begin_key_buf;
    std::string end_key_buf;
    return DeleteRangeImpl(
        AppendTimestamp(begin_key, timestamp, &begin_key_buf),
        AppendTimestamp(end_key, timestamp, &end_key_buf));
  }

  Status InvalidatePageCache(bool closing) {
    Status s = Status::OK();
    if (invalidate_page_cache == false) {
      // Fadvise disabled
      return s;
    }
    uint64_t bytes_since_last_fadvise = builder->FileSize() - last_fadvise_size;
    if (bytes_since_last_fadvise > kFadviseTrigger || closing) {
      TEST_SYNC_POINT_CALLBACK("SstFileWriter::Rep::InvalidatePageCache",
                               &(bytes_since_last_fadvise));
      // Tell the OS that we don't need this file in page cache
      s = file_writer->InvalidateCache(0, 0);
      if (s.IsNotSupported()) {
        // NotSupported is fine as it could be a file type that doesn't use page
        // cache.
        s = Status::OK();
      }
      last_fadvise_size = builder->FileSize();
    }
    return s;
  }
};

SstFileWriter::SstFileWriter(const EnvOptions& env_options,
                             const Options& options,
                             const Comparator* user_comparator,
                             ColumnFamilyHandle* column_family,
                             bool invalidate_page_cache,
                             Env::IOPriority io_priority)
    : rep_(new Rep(env_options, options, io_priority, user_comparator,
                   column_family, invalidate_page_cache,
                   DBImpl::GenerateDbSessionId(options.env))) {
  // SstFileWriter is used to create sst files that can be added to database
  // later. Therefore, no real db_id and db_session_id are associated with it.
  // Here we mimic the way db_session_id behaves by getting a db_session_id
  // for each SstFileWriter, and (later below) assign unique file numbers
  // in the table properties. The db_id is set to be "SST Writer" for clarity.

  rep_->file_info.file_size = 0;
}

SstFileWriter::~SstFileWriter() {
  if (rep_->builder) {
    // User did not call Finish() or Finish() failed, we need to
    // abandon the builder.
    rep_->builder->Abandon();
  }
}

Status SstFileWriter::Open(const std::string& file_path, Temperature temp) {
  Rep* r = rep_.get();
  if (r->builder) {
    return Status::InvalidArgument("File is already opened");
  }
  Status s;
  std::unique_ptr<FSWritableFile> sst_file;
  FileOptions cur_file_opts(r->env_options);
  cur_file_opts.temperature = temp;
  cur_file_opts.open_contract = FileOpenContract::kNoReopenForWrite |
                                FileOpenContract::kNoReadersWhileOpenForWrite;
  s = r->ioptions.env->GetFileSystem()->NewWritableFile(
      file_path, cur_file_opts, &sst_file, nullptr);
  if (!s.ok()) {
    return s;
  }

  sst_file->SetIOPriority(r->io_priority);

  CompressionType compression_type;
  CompressionOptions compression_opts;
  if (r->mutable_cf_options.bottommost_compression !=
      kDisableCompressionOption) {
    compression_type = r->mutable_cf_options.bottommost_compression;
    if (r->mutable_cf_options.bottommost_compression_opts.enabled) {
      compression_opts = r->mutable_cf_options.bottommost_compression_opts;
    } else {
      compression_opts = r->mutable_cf_options.compression_opts;
    }
  } else if (!r->mutable_cf_options.compression_per_level.empty()) {
    // Use the compression of the last level if we have per level compression
    compression_type = *(r->mutable_cf_options.compression_per_level.rbegin());
    compression_opts = r->mutable_cf_options.compression_opts;
  } else {
    compression_type = r->mutable_cf_options.compression;
    compression_opts = r->mutable_cf_options.compression_opts;
  }

  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  // SstFileWriter properties collector to add SstFileWriter properties.
  internal_tbl_prop_coll_factories.emplace_back(
      new SstFileWriterPropertiesCollectorFactory(2 /* version */));

  // User collector factories
  auto user_collector_factories =
      r->ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < user_collector_factories.size(); i++) {
    internal_tbl_prop_coll_factories.emplace_back(
        new UserKeyTablePropertiesCollectorFactory(
            user_collector_factories[i]));
  }
  int unknown_level = -1;
  uint32_t cf_id;

  if (r->cfh != nullptr) {
    // user explicitly specified that this file will be ingested into cfh,
    // we can persist this information in the file.
    cf_id = r->cfh->GetID();
    r->column_family_name = r->cfh->GetName();
  } else {
    r->column_family_name = "";
    cf_id = TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  }

  // TODO: it would be better to set oldest_key_time to be used for getting the
  //  approximate time of ingested keys.
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  TableBuilderOptions table_builder_options(
      r->ioptions, r->mutable_cf_options, read_options, r->write_options,
      r->internal_comparator, &internal_tbl_prop_coll_factories,
      compression_type, compression_opts, cf_id, r->column_family_name,
      unknown_level, kUnknownNewestKeyTime, false /* is_bottommost */,
      TableFileCreationReason::kSstFileWriter, 0 /* oldest_key_time */,
      "SST Writer" /* db_id */, r->db_session_id, 0 /* target_file_size */,
      r->next_file_number, kMaxSequenceNumber, Slice() /* db_name */,
      false /* is_remote_compaction */, r->embedded_blob_options.get());
  // External SST files used to each get a unique session id. Now for
  // slightly better uniqueness probability in constructing cache keys, we
  // assign fake file numbers to each file (into table properties) and keep
  // the same session id for the life of the SstFileWriter.
  r->next_file_number++;
  FileTypeSet tmp_set = r->ioptions.checksum_handoff_file_types;
  r->file_writer.reset(new WritableFileWriter(
      std::move(sst_file), file_path, r->env_options, r->ioptions.clock,
      nullptr /* io_tracer */, r->ioptions.stats, Histograms::SST_WRITE_MICROS,
      r->ioptions.listeners, r->ioptions.file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kTableFile),
      tmp_set.Contains(FileType::kTableFile)));

  // TODO(tec) : If table_factory is using compressed block cache, we will
  // be adding the external sst file blocks into it, which is wasteful.
  r->builder.reset(r->mutable_cf_options.table_factory->NewTableBuilder(
      table_builder_options, r->file_writer.get()));

  r->file_info = ExternalSstFileInfo();
  r->file_info.file_path = file_path;
  r->file_info.version = 2;

  return s;
}

Status SstFileWriter::OpenWithEmbeddedBlobs(
    const std::string& file_path,
    const SstFileWriterEmbeddedBlobOptions& embedded_blob_options,
    Temperature temp) {
  Rep* r = rep_.get();
  if (r->builder) {
    return Status::InvalidArgument("File is already opened");
  }

  const BlockBasedTableOptions* const table_options =
      r->mutable_cf_options.table_factory == nullptr
          ? nullptr
          : r->mutable_cf_options.table_factory
                ->GetOptions<BlockBasedTableOptions>();
  if (table_options == nullptr) {
    return Status::InvalidArgument(
        "Embedded blob SSTs require block-based table format");
  }
  if (!FormatVersionUsesCompressionManagerName(table_options->format_version)) {
    return Status::InvalidArgument(
        "Embedded blob SSTs require block-based table format_version >= 7");
  }

  r->embedded_blob_options.reset(
      new EmbeddedBlobSstBuilderOptions(embedded_blob_options));

  Status s = Open(file_path, temp);
  r->embedded_blob_options.reset();
  return s;
}

Status SstFileWriter::Put(const Slice& user_key, const Slice& value) {
  return rep_->Add(user_key, value, ValueType::kTypeValue);
}

Status SstFileWriter::Put(const Slice& user_key, const Slice& timestamp,
                          const Slice& value) {
  return rep_->Add(user_key, timestamp, value, ValueType::kTypeValue);
}

Status SstFileWriter::PutEntity(const Slice& user_key,
                                const WideColumns& columns) {
  return rep_->AddEntity(user_key, columns);
}

Status SstFileWriter::Merge(const Slice& user_key, const Slice& value) {
  return rep_->Add(user_key, value, ValueType::kTypeMerge);
}

Status SstFileWriter::Delete(const Slice& user_key) {
  return rep_->Add(user_key, Slice(), ValueType::kTypeDeletion);
}

Status SstFileWriter::Delete(const Slice& user_key, const Slice& timestamp) {
  return rep_->Add(user_key, timestamp, Slice(),
                   ValueType::kTypeDeletionWithTimestamp);
}

Status SstFileWriter::DeleteRange(const Slice& begin_key,
                                  const Slice& end_key) {
  return rep_->DeleteRange(begin_key, end_key);
}

Status SstFileWriter::DeleteRange(const Slice& begin_key, const Slice& end_key,
                                  const Slice& timestamp) {
  return rep_->DeleteRange(begin_key, end_key, timestamp);
}

Status SstFileWriter::Finish(ExternalSstFileInfo* file_info) {
  Rep* r = rep_.get();
  if (!r->builder) {
    return Status::InvalidArgument("File is not opened");
  }
  if (r->file_info.num_entries == 0 &&
      r->file_info.num_range_del_entries == 0) {
    r->builder->status().PermitUncheckedError();
    return Status::InvalidArgument("Cannot create sst file with no entries");
  }

  Status s = r->builder->Finish();
  r->file_info.file_size = r->builder->FileSize();

  IOOptions opts;
  if (s.ok()) {
    s = WritableFileWriter::PrepareIOOptions(r->write_options, opts);
  }
  if (s.ok()) {
    s = r->ioptions.use_fsync ? r->file_writer->Fsync(opts)
                              : r->file_writer->Sync(opts);
    r->InvalidatePageCache(true /* closing */).PermitUncheckedError();
    if (s.ok()) {
      s = r->file_writer->Close(opts);
    }
  }
  if (s.ok()) {
    r->file_info.file_checksum = r->file_writer->GetFileChecksum();
    r->file_info.file_checksum_func_name =
        r->file_writer->GetFileChecksumFuncName();
  }
  if (!s.ok()) {
    Status status = r->ioptions.env->DeleteFile(r->file_info.file_path);
    // Silence ASSERT_STATUS_CHECKED warning, since DeleteFile may fail under
    // some error injection, and we can just ignore the failure
    status.PermitUncheckedError();
    r->builder.reset();
    return s;
  }

  ExternalSstFileInfo finished_file_info = r->file_info;
  PreparedFileInfo finished_prepared_file_info;

  finished_prepared_file_info.file_size = finished_file_info.file_size;
  InternalKey smallest;
  InternalKey largest;
  auto update_file_boundaries = [&](InternalKey smallest_candidate,
                                    InternalKey largest_candidate) {
    if (smallest.unset() ||
        r->internal_comparator.Compare(smallest_candidate, smallest) < 0) {
      smallest = std::move(smallest_candidate);
    }
    if (largest.unset() ||
        r->internal_comparator.Compare(largest_candidate, largest) > 0) {
      largest = std::move(largest_candidate);
    }
  };

  if (finished_file_info.num_entries > 0) {
    finished_file_info.smallest_key =
        r->smallest_internal_key.user_key().ToString();
    finished_file_info.largest_key = r->ikey.user_key().ToString();
    update_file_boundaries(r->smallest_internal_key, r->ikey);
  }

  if (finished_file_info.num_range_del_entries > 0) {
    InternalKey smallest_range_del = r->smallest_range_del_internal_key;
    InternalKey largest_range_del = r->largest_range_del_internal_key;

    finished_file_info.smallest_range_del_key =
        smallest_range_del.user_key().ToString();
    finished_file_info.largest_range_del_key =
        largest_range_del.user_key().ToString();

    if (!r->strip_timestamp && r->ts_sz > 0) {
      std::string max_ts(r->ts_sz, '\xff');
      RangeTombstone tombstone(smallest_range_del.user_key(),
                               largest_range_del.user_key(),
                               0 /* Sequence Number */, max_ts);
      smallest_range_del = tombstone.SerializeKey();
      largest_range_del = tombstone.SerializeEndKey();
    }

    update_file_boundaries(std::move(smallest_range_del),
                           std::move(largest_range_del));
  }

  if (r->strip_timestamp) {
    auto strip_timestamp_from_user_key = [](std::string* user_key,
                                            size_t ts_sz) {
      assert(user_key != nullptr);
      assert(user_key->size() >= ts_sz);
      user_key->erase(user_key->size() - ts_sz);
    };
    if (finished_file_info.num_entries > 0) {
      strip_timestamp_from_user_key(&finished_file_info.smallest_key, r->ts_sz);
      strip_timestamp_from_user_key(&finished_file_info.largest_key, r->ts_sz);
    }
    if (finished_file_info.num_range_del_entries > 0) {
      strip_timestamp_from_user_key(&finished_file_info.smallest_range_del_key,
                                    r->ts_sz);
      strip_timestamp_from_user_key(&finished_file_info.largest_range_del_key,
                                    r->ts_sz);
    }
    assert(finished_prepared_file_info.smallest.unset());
    assert(finished_prepared_file_info.largest.unset());
    StripTimestampFromInternalKey(finished_prepared_file_info.smallest.rep(),
                                  smallest.Encode(), r->ts_sz);
    StripTimestampFromInternalKey(finished_prepared_file_info.largest.rep(),
                                  largest.Encode(), r->ts_sz);
  } else {
    finished_prepared_file_info.smallest = std::move(smallest);
    finished_prepared_file_info.largest = std::move(largest);
  }

  finished_prepared_file_info.table_properties =
      r->builder->GetTableProperties();
  if (file_info != nullptr) {
    finished_file_info.prepared_file_info = std::make_shared<PreparedFileInfo>(
        std::move(finished_prepared_file_info));
    *file_info = std::move(finished_file_info);
  }

  r->builder.reset();
  return s;
}

uint64_t SstFileWriter::FileSize() { return rep_->file_info.file_size; }

bool SstFileWriter::CreatedBySstFileWriter(const TableProperties& tp) {
  const auto& uprops = tp.user_collected_properties;
  return uprops.find(ExternalSstFilePropertyNames::kVersion) != uprops.end();
}

}  // namespace ROCKSDB_NAMESPACE
