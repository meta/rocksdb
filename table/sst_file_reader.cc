//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/sst_file_reader.h"

#include <algorithm>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/types_util.h"
#include "table/get_context.h"
#include "table/sst_file_reader_impl.h"
#include "table/table_builder.h"
#include "table/table_iterator.h"
#include "table/table_reader.h"

// Generate the regular and coroutine versions of the point-read methods.
// clang-format off
#define WITHOUT_COROUTINES
#include "table/sst_file_reader_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "table/sst_file_reader_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

SstFileReader::SstFileReader(const Options& options) : rep_(new Rep(options)) {}

SstFileReader::~SstFileReader() = default;

Status SstFileReader::Open(const std::string& file_path) {
  auto r = rep_.get();
  Status s;
  uint64_t file_size = 0;
  std::unique_ptr<FSRandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  FileOptions fopts(r->soptions);
  fopts.file_checksum_func_name = kNoFileChecksumFuncName;
  const auto& fs = r->options.env->GetFileSystem();
#if USE_COROUTINES
  if (r->options.read_io_executor_threads <= 0) {
    return Status::InvalidArgument(
        "read_io_executor_threads must be greater than zero");
  }
  fs->SetReadIOExecutorThreads(r->options.read_io_executor_threads);
  r->read_executor = fs->GetReadExecutor();
#endif  // USE_COROUTINES

  s = fs->GetFileSize(file_path, fopts.io_options, &file_size, nullptr);
  if (s.ok()) {
    s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
  }
  if (s.ok()) {
    file_reader.reset(new RandomAccessFileReader(
        std::move(file), file_path, r->ioptions.clock,
        /*io_tracer=*/nullptr, r->ioptions.stats, Histograms::SST_READ_MICROS,
        /*file_read_hist=*/nullptr, r->ioptions.rate_limiter.get(),
        r->ioptions.listeners, fopts.temperature, /*is_last_level=*/false,
        fs.get()));
  }
  if (s.ok()) {
    TableReaderOptions t_opt(
        r->ioptions, r->moptions.prefix_extractor,
        r->moptions.compression_manager.get(), r->soptions,
        r->ioptions.internal_comparator,
        r->moptions.block_protection_bytes_per_key,
        /*skip_filters*/ false, /*immortal*/ false,
        /*force_direct_prefetch*/ false, /*level*/ -1,
        /*block_cache_tracer*/ nullptr,
        /*max_file_size_for_l0_meta_pin*/ 0, /*cur_db_session_id*/ "",
        /*cur_file_num*/ 0,
        /* unique_id */ {}, /* largest_seqno */ 0,
        /* tail_size */ 0, r->ioptions.persist_user_defined_timestamps);
    // Allow open file with global sequence number for backward compatibility.
    t_opt.largest_seqno = kMaxSequenceNumber;
    s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                                 file_size, &r->table_reader);
  }
  return s;
}

std::vector<Status> SstFileReader::MultiGet(
    const ReadOptions& roptions, const std::vector<Slice>& keys,
    std::vector<PinnableSlice>* values) {
  return rep_->MultiGet(roptions, keys, values);
}

std::vector<Status> SstFileReader::MultiGet(const ReadOptions& roptions,
                                            const std::vector<Slice>& keys,
                                            std::vector<std::string>* values) {
  std::vector<PinnableSlice> pin_values;
  std::vector<Status> statuses = MultiGet(roptions, keys, &pin_values);
  values->resize(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    if (statuses[i].ok()) {
      (*values)[i].assign(pin_values[i].data(), pin_values[i].size());
    }
  }
  return statuses;
}

Status SstFileReader::Get(const ReadOptions& roptions, const Slice& key,
                          PinnableSlice* value) {
  return rep_->Get(roptions, key, value);
}

Status SstFileReader::Get(const ReadOptions& roptions, const Slice& key,
                          std::string* value) {
  PinnableSlice pin_value;
  Status s = Get(roptions, key, &pin_value);
  if (s.ok()) {
    value->assign(pin_value.data(), pin_value.size());
  }
  return s;
}

Iterator* SstFileReader::NewIterator(const ReadOptions& roptions) {
  assert(roptions.io_activity == Env::IOActivity::kUnknown);
  auto r = rep_.get();
  auto sequence = roptions.snapshot != nullptr
                      ? roptions.snapshot->GetSequenceNumber()
                      : kMaxSequenceNumber;
  ArenaWrappedDBIter* res = new ArenaWrappedDBIter();
  res->Init(r->options.env, roptions, r->ioptions, r->moptions,
            nullptr /* version */, sequence, 0 /* version_number */,
            nullptr /* read_callback */, nullptr /* cfh */,
            true /* expose_blob_index */, false /* allow_refresh */,
            /*active_mem=*/nullptr);
  auto internal_iter = r->table_reader->NewIterator(
      res->GetReadOptions(), r->moptions.prefix_extractor.get(),
      res->GetArena(), false /* skip_filters */,
      TableReaderCaller::kSSTFileReader);
  res->SetIterUnderDBIter(internal_iter);
  return res;
}

std::unique_ptr<Iterator> SstFileReader::NewTableIterator(const Slice* from_key, const Slice* to_key) {
  auto r = rep_.get();
  InternalIterator* internal_iter = r->table_reader->NewIterator(
          r->roptions_for_table_iter,
          r->moptions.prefix_extractor.get(),/*arena*/ nullptr, false /* skip_filters */,
          TableReaderCaller::kSSTFileReader);
  assert(internal_iter);
  if (internal_iter == nullptr) {
    // Do not attempt to create a TableIterator if we cannot get a valid
    // InternalIterator.
    return nullptr;
  }
  return std::make_unique<TableIterator>(internal_iter, from_key, to_key, &r->ioptions.internal_comparator);
}

std::unique_ptr<Iterator> SstFileReader::NewTableIterator() {
  return NewTableIterator(nullptr, nullptr);
}

Status SstFileReader::ParseTableIteratorKey(const Slice& raw_table_key,
                                            ParsedEntryInfo* parsed_key) const {
  return ParseEntry(raw_table_key, rep_->options.comparator, parsed_key);
}

std::shared_ptr<const TableProperties> SstFileReader::GetTableProperties()
    const {
  return rep_->table_reader->GetTableProperties();
}

Status SstFileReader::VerifyChecksum(const ReadOptions& read_options) {
  assert(read_options.io_activity == Env::IOActivity::kUnknown);
  return rep_->table_reader->VerifyChecksum(read_options,
                                            TableReaderCaller::kSSTFileReader);
}

Status SstFileReader::VerifyNumEntries(const ReadOptions& read_options) {
  Rep* r = rep_.get();
  std::unique_ptr<InternalIterator> internal_iter{r->table_reader->NewIterator(
      read_options, r->moptions.prefix_extractor.get(), nullptr,
      false /* skip_filters */, TableReaderCaller::kSSTFileReader)};
  internal_iter->SeekToFirst();
  Status s = internal_iter->status();
  if (!s.ok()) {
    return s;
  }
  uint64_t num_read = 0;
  for (; internal_iter->Valid(); internal_iter->Next()) {
    ++num_read;
  }
  s = internal_iter->status();
  if (!s.ok()) {
    return s;
  }
  std::shared_ptr<const TableProperties> tp = GetTableProperties();
  if (!tp) {
    s = Status::Corruption("table properties not available");
  } else {
    // TODO: verify num_range_deletions
    uint64_t expected = tp->num_entries - tp->num_range_deletions;
    if (num_read != expected) {
      std::ostringstream oss;
      oss << "Table property expects " << expected
          << " entries when excluding range deletions,"
          << " but scanning the table returned " << std::to_string(num_read)
          << " entries";
      s = Status::Corruption(oss.str());
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
