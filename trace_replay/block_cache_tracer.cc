//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/block_cache_tracer.h"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "rocksdb/trace_record.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
bool ShouldTrace(const Slice& block_key,
                 const BlockCacheTraceOptions& trace_options) {
  if (trace_options.sampling_frequency == 0 ||
      trace_options.sampling_frequency == 1) {
    return true;
  }
  // We use spatial downsampling so that we have a complete access history for a
  // block.
  return 0 == GetSliceRangedNPHash(block_key, trace_options.sampling_frequency);
}
}  // namespace

const uint64_t kMicrosInSecond = 1000 * 1000;
const uint64_t kSecondInMinute = 60;
const uint64_t kSecondInHour = 3600;
const std::string BlockCacheTraceHelper::kUnknownColumnFamilyName =
    "UnknownColumnFamily";
const uint64_t BlockCacheTraceRecord::kReservedGetId = 0;
const uint64_t BlockCacheTraceHelper::kReservedGetId = 0;

bool BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(
    const TraceType& block_type, const TableReaderCaller& caller) {
  return (block_type == TraceType::kBlockTraceDataBlock) &&
         IsGetOrMultiGet(caller);
}

bool BlockCacheTraceHelper::IsGetOrMultiGet(const TableReaderCaller& caller) {
  return caller == TableReaderCaller::kUserGet ||
         caller == TableReaderCaller::kUserMultiGet;
}

bool BlockCacheTraceHelper::IsUserAccess(const TableReaderCaller& caller) {
  return caller == TableReaderCaller::kUserGet ||
         caller == TableReaderCaller::kUserMultiGet ||
         caller == TableReaderCaller::kUserIterator ||
         caller == TableReaderCaller::kUserApproximateSize ||
         caller == TableReaderCaller::kUserVerifyChecksum;
}

std::string BlockCacheTraceHelper::ComputeRowKey(
    const BlockCacheTraceRecord& access) {
  if (!IsGetOrMultiGet(access.caller)) {
    return "";
  }
  Slice key = ExtractUserKey(access.referenced_key);
  return std::to_string(access.sst_fd_number) + "_" + key.ToString();
}

uint64_t BlockCacheTraceHelper::GetTableId(
    const BlockCacheTraceRecord& access) {
  if (!IsGetOrMultiGet(access.caller) || access.referenced_key.size() < 4) {
    return 0;
  }
  return static_cast<uint64_t>(DecodeFixed32(access.referenced_key.data())) + 1;
}

uint64_t BlockCacheTraceHelper::GetSequenceNumber(
    const BlockCacheTraceRecord& access) {
  if (!IsGetOrMultiGet(access.caller)) {
    return 0;
  }
  if (access.caller == TableReaderCaller::kUserMultiGet &&
      access.referenced_key.size() < 4) {
    return 0;
  }
  return access.get_from_user_specified_snapshot
             ? 1 + GetInternalKeySeqno(access.referenced_key)
             : 0;
}

uint64_t BlockCacheTraceHelper::GetBlockOffsetInFile(
    const BlockCacheTraceRecord& access) {
  Slice input(access.block_key);
  uint64_t offset = 0;
  while (true) {
    uint64_t tmp = 0;
    if (GetVarint64(&input, &tmp)) {
      offset = tmp;
    } else {
      break;
    }
  }
  return offset;
}

BlockCacheTraceWriterImpl::BlockCacheTraceWriterImpl(
    SystemClock* clock, const BlockCacheTraceWriterOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer)
    : clock_(clock),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)) {}

Status BlockCacheTraceWriterImpl::WriteBlockAccess(
    const BlockCacheTraceRecord& record, const Slice& block_key,
    const Slice& cf_name, const Slice& referenced_key) {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = record.access_timestamp;
  trace.type = record.block_type;
  PutLengthPrefixedSlice(&trace.payload, block_key);
  PutFixed64(&trace.payload, record.block_size);
  PutFixed64(&trace.payload, record.cf_id);
  PutLengthPrefixedSlice(&trace.payload, cf_name);
  PutFixed32(&trace.payload, record.level);
  PutFixed64(&trace.payload, record.sst_fd_number);
  trace.payload.push_back(record.caller);
  trace.payload.push_back(record.is_cache_hit);
  trace.payload.push_back(record.no_insert);
  if (BlockCacheTraceHelper::IsGetOrMultiGet(record.caller)) {
    PutFixed64(&trace.payload, record.get_id);
    trace.payload.push_back(record.get_from_user_specified_snapshot);
    PutLengthPrefixedSlice(&trace.payload, referenced_key);
  }
  if (BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(record.block_type,
                                                        record.caller)) {
    PutFixed64(&trace.payload, record.referenced_data_size);
    PutFixed64(&trace.payload, record.num_keys_in_block);
    trace.payload.push_back(record.referenced_key_exist_in_block);
  }
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

Status BlockCacheTraceWriterImpl::WriteHeader() {
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = TraceType::kTraceBegin;
  PutLengthPrefixedSlice(&trace.payload, kTraceMagic);
  PutFixed32(&trace.payload, kMajorVersion);
  PutFixed32(&trace.payload, kMinorVersion);
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

BlockCacheTraceReader::BlockCacheTraceReader(
    std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {}

Status BlockCacheTraceReader::ReadHeader(BlockCacheTraceHeader* header) {
  assert(header != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  header->start_time = trace.ts;
  Slice enc_slice = Slice(trace.payload);
  Slice magnic_number;
  if (!GetLengthPrefixedSlice(&enc_slice, &magnic_number)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read the magic number.");
  }
  if (magnic_number.ToString() != kTraceMagic) {
    return Status::Corruption(
        "Corrupted header in the trace file: Magic number does not match.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_major_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb major "
        "version number.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_minor_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb minor "
        "version number.");
  }
  // We should have retrieved all information in the header.
  if (!enc_slice.empty()) {
    return Status::Corruption(
        "Corrupted header in the trace file: The length of header is too "
        "long.");
  }
  return Status::OK();
}

Status BlockCacheTraceReader::ReadAccess(BlockCacheTraceRecord* record) {
  assert(record);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  record->access_timestamp = trace.ts;
  record->block_type = trace.type;
  Slice enc_slice = Slice(trace.payload);

  const unsigned int kCharSize = 1;

  Slice block_key;
  if (!GetLengthPrefixedSlice(&enc_slice, &block_key)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read block key.");
  }
  record->block_key = block_key.ToString();
  if (!GetFixed64(&enc_slice, &record->block_size)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read block size.");
  }
  if (!GetFixed64(&enc_slice, &record->cf_id)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read column family ID.");
  }
  Slice cf_name;
  if (!GetLengthPrefixedSlice(&enc_slice, &cf_name)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read column family name.");
  }
  record->cf_name = cf_name.ToString();
  if (!GetFixed32(&enc_slice, &record->level)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read level.");
  }
  if (!GetFixed64(&enc_slice, &record->sst_fd_number)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read SST file number.");
  }
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read caller.");
  }
  record->caller = static_cast<TableReaderCaller>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read is_cache_hit.");
  }
  record->is_cache_hit = static_cast<char>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read no_insert.");
  }
  record->no_insert = static_cast<char>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (BlockCacheTraceHelper::IsGetOrMultiGet(record->caller)) {
    if (!GetFixed64(&enc_slice, &record->get_id)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the get id.");
    }
    if (enc_slice.empty()) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read "
          "get_from_user_specified_snapshot.");
    }
    record->get_from_user_specified_snapshot = static_cast<char>(enc_slice[0]);
    enc_slice.remove_prefix(kCharSize);
    Slice referenced_key;
    if (!GetLengthPrefixedSlice(&enc_slice, &referenced_key)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the referenced key.");
    }
    record->referenced_key = referenced_key.ToString();
  }
  if (BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(record->block_type,
                                                        record->caller)) {
    if (!GetFixed64(&enc_slice, &record->referenced_data_size)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the referenced data size.");
    }
    if (!GetFixed64(&enc_slice, &record->num_keys_in_block)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the number of keys in the "
          "block.");
    }
    if (enc_slice.empty()) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read "
          "referenced_key_exist_in_block.");
    }
    record->referenced_key_exist_in_block = static_cast<char>(enc_slice[0]);
  }
  return Status::OK();
}

BlockCacheHumanReadableTraceWriter::~BlockCacheHumanReadableTraceWriter() {
  if (human_readable_trace_file_writer_) {
    human_readable_trace_file_writer_->Flush().PermitUncheckedError();
    human_readable_trace_file_writer_->Close().PermitUncheckedError();
  }
}

Status BlockCacheHumanReadableTraceWriter::NewWritableFile(
    const std::string& human_readable_trace_file_path,
    ROCKSDB_NAMESPACE::Env* env) {
  if (human_readable_trace_file_path.empty()) {
    return Status::InvalidArgument(
        "The provided human_readable_trace_file_path is null.");
  }
  return env->NewWritableFile(human_readable_trace_file_path,
                              &human_readable_trace_file_writer_, EnvOptions());
}

Status BlockCacheHumanReadableTraceWriter::WriteHumanReadableTraceRecord(
    const BlockCacheTraceRecord& access, uint64_t block_id,
    uint64_t get_key_id) {
  if (!human_readable_trace_file_writer_) {
    return Status::OK();
  }
  int ret = snprintf(
      trace_record_buffer_, sizeof(trace_record_buffer_),
      "%" PRIu64 ",%" PRIu64 ",%u,%" PRIu64 ",%" PRIu64 ",%s,%" PRIu32
      ",%" PRIu64 ",%u,%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%u,%u,%" PRIu64
      ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
      access.access_timestamp, block_id, access.block_type, access.block_size,
      access.cf_id, access.cf_name.c_str(), access.level, access.sst_fd_number,
      access.caller, access.no_insert, access.get_id, get_key_id,
      access.referenced_data_size, access.is_cache_hit,
      access.referenced_key_exist_in_block, access.num_keys_in_block,
      BlockCacheTraceHelper::GetTableId(access),
      BlockCacheTraceHelper::GetSequenceNumber(access),
      static_cast<uint64_t>(access.block_key.size()),
      static_cast<uint64_t>(access.referenced_key.size()),
      BlockCacheTraceHelper::GetBlockOffsetInFile(access));
  if (ret < 0) {
    return Status::IOError("failed to format the output");
  }
  std::string printout(trace_record_buffer_);
  return human_readable_trace_file_writer_->Append(printout);
}

BlockCacheHumanReadableTraceReader::BlockCacheHumanReadableTraceReader(
    const std::string& trace_file_path)
    : BlockCacheTraceReader(/*reader=*/nullptr) {
  human_readable_trace_reader_.open(trace_file_path, std::ifstream::in);
}

BlockCacheHumanReadableTraceReader::~BlockCacheHumanReadableTraceReader() {
  human_readable_trace_reader_.close();
}

Status BlockCacheHumanReadableTraceReader::ReadHeader(
    BlockCacheTraceHeader* /*header*/) {
  return Status::OK();
}

Status BlockCacheHumanReadableTraceReader::ReadAccess(
    BlockCacheTraceRecord* record) {
  std::string line;
  if (!std::getline(human_readable_trace_reader_, line)) {
    return Status::Incomplete("No more records to read.");
  }
  std::stringstream ss(line);
  std::vector<std::string> record_strs;
  while (ss.good()) {
    std::string substr;
    getline(ss, substr, ',');
    record_strs.push_back(substr);
  }
  if (record_strs.size() != 21) {
    return Status::Incomplete("Records format is wrong.");
  }

  record->access_timestamp = ParseUint64(record_strs[0]);
  uint64_t block_key = ParseUint64(record_strs[1]);
  record->block_type = static_cast<TraceType>(ParseUint64(record_strs[2]));
  record->block_size = ParseUint64(record_strs[3]);
  record->cf_id = ParseUint64(record_strs[4]);
  record->cf_name = record_strs[5];
  record->level = static_cast<uint32_t>(ParseUint64(record_strs[6]));
  record->sst_fd_number = ParseUint64(record_strs[7]);
  record->caller = static_cast<TableReaderCaller>(ParseUint64(record_strs[8]));
  record->no_insert = static_cast<char>(ParseUint64(record_strs[9]));
  record->get_id = ParseUint64(record_strs[10]);
  uint64_t get_key_id = ParseUint64(record_strs[11]);

  record->referenced_data_size = ParseUint64(record_strs[12]);
  record->is_cache_hit = static_cast<char>(ParseUint64(record_strs[13]));
  record->referenced_key_exist_in_block =
      static_cast<char>(ParseUint64(record_strs[14]));
  record->num_keys_in_block = ParseUint64(record_strs[15]);
  uint64_t table_id = ParseUint64(record_strs[16]);
  if (table_id > 0) {
    // Decrement since valid table id in the trace file equals traced table id
    // + 1.
    table_id -= 1;
  }
  uint64_t get_sequence_number = ParseUint64(record_strs[17]);
  if (get_sequence_number > 0) {
    record->get_from_user_specified_snapshot = true;
    // Decrement since valid seq number in the trace file equals traced seq
    // number + 1.
    get_sequence_number -= 1;
  }
  uint64_t block_key_size = ParseUint64(record_strs[18]);
  uint64_t get_key_size = ParseUint64(record_strs[19]);
  uint64_t block_offset = ParseUint64(record_strs[20]);

  std::string tmp_block_key;
  PutVarint64(&tmp_block_key, block_key);
  PutVarint64(&tmp_block_key, block_offset);
  // Append 1 until the size is the same as traced block key size.
  while (record->block_key.size() < block_key_size - tmp_block_key.size()) {
    record->block_key += "1";
  }
  record->block_key += tmp_block_key;

  if (get_key_id != 0) {
    std::string tmp_get_key;
    PutFixed64(&tmp_get_key, get_key_id);
    PutFixed64(&tmp_get_key, get_sequence_number << 8);
    PutFixed32(&record->referenced_key, static_cast<uint32_t>(table_id));
    // Append 1 until the size is the same as traced key size.
    while (record->referenced_key.size() < get_key_size - tmp_get_key.size()) {
      record->referenced_key += "1";
    }
    record->referenced_key += tmp_get_key;
  }
  return Status::OK();
}

BlockCacheTracer::BlockCacheTracer() { writer_.store(nullptr); }

BlockCacheTracer::~BlockCacheTracer() { EndTrace(); }

Status BlockCacheTracer::StartTrace(
    const BlockCacheTraceOptions& trace_options,
    std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (writer_.load()) {
    return Status::Busy();
  }
  get_id_counter_.store(1);
  trace_options_ = trace_options;
  writer_.store(trace_writer.release());
  return writer_.load()->WriteHeader();
}

void BlockCacheTracer::EndTrace() {
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (!writer_.load()) {
    return;
  }
  delete writer_.load();
  writer_.store(nullptr);
}

Status BlockCacheTracer::WriteBlockAccess(const BlockCacheTraceRecord& record,
                                          const Slice& block_key,
                                          const Slice& cf_name,
                                          const Slice& referenced_key) {
  if (!writer_.load() || !ShouldTrace(block_key, trace_options_)) {
    return Status::OK();
  }
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (!writer_.load()) {
    return Status::OK();
  }
  return writer_.load()->WriteBlockAccess(record, block_key, cf_name,
                                          referenced_key);
}

uint64_t BlockCacheTracer::NextGetId() {
  if (!writer_.load(std::memory_order_relaxed)) {
    return BlockCacheTraceHelper::kReservedGetId;
  }
  uint64_t prev_value = get_id_counter_.fetch_add(1);
  if (prev_value == BlockCacheTraceHelper::kReservedGetId) {
    // fetch and add again.
    return get_id_counter_.fetch_add(1);
  }
  return prev_value;
}

std::unique_ptr<BlockCacheTraceWriter> NewBlockCacheTraceWriter(
    SystemClock* clock, const BlockCacheTraceWriterOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer) {
  return std::unique_ptr<BlockCacheTraceWriter>(new BlockCacheTraceWriterImpl(
      clock, trace_options, std::move(trace_writer)));
}

}  // namespace ROCKSDB_NAMESPACE
