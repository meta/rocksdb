//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/blob_db/blob_compaction_filter.h"

#include <cinttypes>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE::blob_db {

BlobIndexCompactionFilterBase::~BlobIndexCompactionFilterBase() {
  if (blob_file_) {
    CloseAndRegisterNewBlobFile();
  }
  RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_COUNT, expired_count_);
  RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, expired_size_);
  RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_COUNT, evicted_count_);
  RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_SIZE, evicted_size_);
}

CompactionFilter::Decision BlobIndexCompactionFilterBase::FilterV2(
    int level, const Slice& key, ValueType value_type, const Slice& value,
    std::string* new_value, std::string* skip_until) const {
  const CompactionFilter* ucf = user_comp_filter();
  if (value_type != kBlobIndex) {
    if (ucf == nullptr) {
      return Decision::kKeep;
    }
    // Apply user compaction filter for non-blob data.
    CompactionFilter::Decision decision =
        ucf->FilterV2(level, key, value_type, value, new_value, skip_until);
    if (decision == Decision::kChangeValue) {
      return HandleValueChange(key, new_value);
    }
    return decision;
  }
  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(value);
  if (!s.ok()) {
    // Unable to decode blob index. Keeping the value.
    return Decision::kKeep;
  }
  if (blob_index.HasTTL() && blob_index.expiration() <= current_time_) {
    // Expired
    expired_count_++;
    expired_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  if (blob_index.file_number() < context_.next_file_number &&
      context_.current_blob_files.count(blob_index.file_number()) == 0) {
    evicted_count_++;
    evicted_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  // Apply user compaction filter for all non-TTL blob data.
  if (ucf != nullptr && !blob_index.HasTTL()) {
    // Hack: Internal key is passed to BlobIndexCompactionFilter for it to
    // get sequence number.
    ParsedInternalKey ikey;
    if (!ParseInternalKey(
             key, &ikey,
             context_.blob_db_impl->db_options_.allow_data_in_errors)
             .ok()) {
      assert(false);
      return Decision::kKeep;
    }
    // Read value from blob file.
    PinnableSlice blob;
    if (!ReadBlobFromOldFile(ikey.user_key, blob_index, &blob)) {
      return Decision::kIOError;
    }
    CompactionFilter::Decision decision = ucf->FilterV2(
        level, ikey.user_key, kValue, blob, new_value, skip_until);
    if (decision == Decision::kChangeValue) {
      return HandleValueChange(ikey.user_key, new_value);
    }
    return decision;
  }
  return Decision::kKeep;
}

CompactionFilter::Decision BlobIndexCompactionFilterBase::HandleValueChange(
    const Slice& key, std::string* new_value) const {
  if (!OpenNewBlobFileIfNeeded()) {
    return Decision::kIOError;
  }
  Slice new_blob_value(*new_value);
  uint64_t new_blob_file_number = 0;
  uint64_t new_blob_offset = 0;
  if (!WriteBlobToNewFile(key, new_blob_value, &new_blob_file_number,
                          &new_blob_offset)) {
    return Decision::kIOError;
  }
  if (!CloseAndRegisterNewBlobFileIfNeeded()) {
    return Decision::kIOError;
  }
  BlobIndex::EncodeBlob(new_value, new_blob_file_number, new_blob_offset,
                        new_blob_value.size(), kNoCompression);
  return Decision::kChangeBlobIndex;
}

BlobIndexCompactionFilterGC::~BlobIndexCompactionFilterGC() {
  assert(context().blob_db_impl);

  ROCKS_LOG_INFO(context().blob_db_impl->db_options_.info_log,
                 "GC pass finished %s: encountered %" PRIu64 " blobs (%" PRIu64
                 " bytes), relocated %" PRIu64 " blobs (%" PRIu64
                 " bytes), created %" PRIu64 " new blob file(s)",
                 !gc_stats_.HasError() ? "successfully" : "with failure",
                 gc_stats_.AllBlobs(), gc_stats_.AllBytes(),
                 gc_stats_.RelocatedBlobs(), gc_stats_.RelocatedBytes(),
                 gc_stats_.NewFiles());

  RecordTick(statistics(), BLOB_DB_GC_NUM_KEYS_RELOCATED,
             gc_stats_.RelocatedBlobs());
  RecordTick(statistics(), BLOB_DB_GC_BYTES_RELOCATED,
             gc_stats_.RelocatedBytes());
  RecordTick(statistics(), BLOB_DB_GC_NUM_NEW_FILES, gc_stats_.NewFiles());
  RecordTick(statistics(), BLOB_DB_GC_FAILURES, gc_stats_.HasError());
}

bool BlobIndexCompactionFilterBase::IsBlobFileOpened() const {
  if (blob_file_) {
    assert(writer_);
    return true;
  }
  return false;
}

bool BlobIndexCompactionFilterBase::OpenNewBlobFileIfNeeded() const {
  if (IsBlobFileOpened()) {
    return true;
  }

  BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
  assert(blob_db_impl);

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const Status s = blob_db_impl->CreateBlobFileAndWriter(
      WriteOptions(),
      /* has_ttl */ false, ExpirationRange(), "compaction/GC", &blob_file_,
      &writer_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        blob_db_impl->db_options_.info_log,
        "Error opening new blob file during compaction/GC, status: %s",
        s.ToString().c_str());
    blob_file_.reset();
    writer_.reset();
    return false;
  }

  assert(blob_file_);
  assert(writer_);

  return true;
}

bool BlobIndexCompactionFilterBase::ReadBlobFromOldFile(
    const Slice& key, const BlobIndex& blob_index, PinnableSlice* blob) const {
  BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
  assert(blob_db_impl);

  Status s = blob_db_impl->GetRawBlobFromFile(key, blob_index.file_number(),
                                              blob_index.offset(),
                                              blob_index.size(), blob);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        blob_db_impl->db_options_.info_log,
        "Error reading blob during compaction/GC, key: %s (%s), status: %s",
        key.ToString(/* output_hex */ true).c_str(),
        blob_index.DebugString(/* output_hex */ true).c_str(),
        s.ToString().c_str());

    return false;
  }

  return true;
}

bool BlobIndexCompactionFilterBase::WriteBlobToNewFile(
    const Slice& key, const Slice& blob, uint64_t* new_blob_file_number,
    uint64_t* new_blob_offset) const {
  TEST_SYNC_POINT("BlobIndexCompactionFilterBase::WriteBlobToNewFile");
  assert(new_blob_file_number);
  assert(new_blob_offset);

  assert(blob_file_);
  *new_blob_file_number = blob_file_->BlobFileNumber();

  assert(writer_);
  uint64_t new_key_offset = 0;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const Status s = writer_->AddRecord(WriteOptions(), key, blob, kNoExpiration,
                                      &new_key_offset, new_blob_offset);

  if (!s.ok()) {
    const BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
    assert(blob_db_impl);

    ROCKS_LOG_ERROR(blob_db_impl->db_options_.info_log,
                    "Error writing blob to new file %s during compaction/GC, "
                    "key: %s, status: %s",
                    blob_file_->PathName().c_str(),
                    key.ToString(/* output_hex */ true).c_str(),
                    s.ToString().c_str());
    return false;
  }

  const uint64_t new_size =
      BlobLogRecord::kHeaderSize + key.size() + blob.size();
  blob_file_->BlobRecordAdded(new_size);

  BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
  assert(blob_db_impl);

  blob_db_impl->total_blob_size_ += new_size;

  return true;
}

bool BlobIndexCompactionFilterBase::CloseAndRegisterNewBlobFileIfNeeded()
    const {
  const BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
  assert(blob_db_impl);

  assert(blob_file_);
  if (blob_file_->GetFileSize() < blob_db_impl->bdb_options_.blob_file_size) {
    return true;
  }

  return CloseAndRegisterNewBlobFile();
}

bool BlobIndexCompactionFilterBase::CloseAndRegisterNewBlobFile() const {
  BlobDBImpl* const blob_db_impl = context_.blob_db_impl;
  assert(blob_db_impl);
  assert(blob_file_);

  Status s;

  {
    WriteLock wl(&blob_db_impl->mutex_);

    // TODO: plumb Env::IOActivity, Env::IOPriority
    s = blob_db_impl->CloseBlobFile(WriteOptions(), blob_file_);

    // Note: we delay registering the new blob file until it's closed.
    blob_db_impl->RegisterBlobFile(blob_file_);
  }

  assert(blob_file_->Immutable());

  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        blob_db_impl->db_options_.info_log,
        "Error closing new blob file %s during compaction/GC, status: %s",
        blob_file_->PathName().c_str(), s.ToString().c_str());
  }

  blob_file_.reset();
  return s.ok();
}

CompactionFilter::BlobDecision BlobIndexCompactionFilterGC::PrepareBlobOutput(
    const Slice& key, const Slice& existing_value,
    std::string* new_value) const {
  assert(new_value);

  const BlobDBImpl* const blob_db_impl = context().blob_db_impl;
  (void)blob_db_impl;

  assert(blob_db_impl);
  assert(blob_db_impl->bdb_options_.enable_garbage_collection);

  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(existing_value);
  if (!s.ok()) {
    gc_stats_.SetError();
    return BlobDecision::kCorruption;
  }

  gc_stats_.AddBlob(blob_index.size());

  if (blob_index.HasTTL()) {
    return BlobDecision::kKeep;
  }

  if (blob_index.file_number() >= context_gc_.cutoff_file_number) {
    return BlobDecision::kKeep;
  }

  // Note: each compaction generates its own blob files, which, depending on the
  // workload, might result in many small blob files. The total number of files
  // is bounded though (determined by the number of compactions and the blob
  // file size option).
  if (!OpenNewBlobFileIfNeeded()) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  PinnableSlice blob;
  if (!ReadBlobFromOldFile(key, blob_index, &blob)) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  uint64_t new_blob_file_number = 0;
  uint64_t new_blob_offset = 0;
  if (!WriteBlobToNewFile(key, blob, &new_blob_file_number, &new_blob_offset)) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  if (!CloseAndRegisterNewBlobFileIfNeeded()) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  BlobIndex::EncodeBlob(new_value, new_blob_file_number, new_blob_offset,
                        blob.size(), kNoCompression);

  gc_stats_.AddRelocatedBlob(blob_index.size());

  return BlobDecision::kChangeValue;
}

bool BlobIndexCompactionFilterGC::OpenNewBlobFileIfNeeded() const {
  if (IsBlobFileOpened()) {
    return true;
  }
  bool result = BlobIndexCompactionFilterBase::OpenNewBlobFileIfNeeded();
  if (result) {
    gc_stats_.AddNewFile();
  }
  return result;
}

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactoryBase::CreateUserCompactionFilterFromFactory(
    const CompactionFilter::Context& context) const {
  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory;
  if (user_comp_filter_factory_) {
    user_comp_filter_from_factory =
        user_comp_filter_factory_->CreateCompactionFilter(context);
  }
  return user_comp_filter_from_factory;
}

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& _context) {
  assert(clock());

  int64_t current_time = 0;
  Status s = clock()->GetCurrentTime(&current_time);
  if (!s.ok() || current_time < 0) {
    return nullptr;
  }

  assert(blob_db_impl());

  BlobCompactionContext context;
  blob_db_impl()->GetCompactionContext(&context);

  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory =
      CreateUserCompactionFilterFromFactory(_context);

  return std::unique_ptr<CompactionFilter>(new BlobIndexCompactionFilter(
      std::move(context), user_comp_filter(),
      std::move(user_comp_filter_from_factory), current_time, statistics()));
}

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactoryGC::CreateCompactionFilter(
    const CompactionFilter::Context& _context) {
  assert(clock());

  int64_t current_time = 0;
  Status s = clock()->GetCurrentTime(&current_time);
  if (!s.ok() || current_time < 0) {
    return nullptr;
  }

  assert(blob_db_impl());

  BlobCompactionContext context;
  BlobCompactionContextGC context_gc;
  blob_db_impl()->GetCompactionContext(&context, &context_gc);

  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory =
      CreateUserCompactionFilterFromFactory(_context);

  return std::unique_ptr<CompactionFilter>(new BlobIndexCompactionFilterGC(
      std::move(context), std::move(context_gc), user_comp_filter(),
      std::move(user_comp_filter_from_factory), current_time, statistics()));
}

}  // namespace ROCKSDB_NAMESPACE::blob_db
