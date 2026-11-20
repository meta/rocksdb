//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/compacted_db_impl.h"

#include <algorithm>

#include "db/blob/blob_fetcher.h"
#include "db/db_impl/db_impl.h"
#include "db/db_impl/db_impl_metadata.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "table/get_context.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

CompactedDBImpl::CompactedDBImpl(const DBOptions& options,
                                 const std::string& dbname)
    : DBImpl(options, dbname, /*seq_per_batch*/ false, +/*batch_per_txn*/ true,
             /*read_only*/ true),
      cfd_(nullptr),
      version_(nullptr),
      user_comparator_(nullptr) {}

CompactedDBImpl::~CompactedDBImpl() = default;

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t right = files_.num_files - 1;
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return user_comparator_->Compare(ExtractUserKey(f.largest_key), k) < 0;
  };
  return static_cast<size_t>(
      std::lower_bound(files_.files, files_.files + right, key, cmp) -
      files_.files);
}

Status CompactedDBImpl::GetWithMetadata(const ReadOptions& options,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key, PinnableSlice* value,
                                        OutputMetadata* output_metadata) {
  std::string* timestamp = GetOutputTimestamp(output_metadata);
  bool* newer_version_present = GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    *newer_version_present = false;
  }
  if (value == nullptr) {
    return Status::InvalidArgument(
        "Cannot call GetWithMetadata with a null value");
  }
  return Get(options, column_family, key, value, timestamp);
}

Status CompactedDBImpl::GetEntityWithMetadata(const ReadOptions& options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableWideColumns* columns,
                                              OutputMetadata* output_metadata) {
  Status s = InitGetEntityOutputMetadata(output_metadata);
  if (!s.ok()) {
    return s;
  }
  return GetEntity(options, column_family, key, columns);
}

void CompactedDBImpl::MultiGetWithMetadata(
    const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableSlice* values, Status* statuses,
    MultiGetOutputMetadata* output_metadata, const bool sorted_input) {
  std::vector<std::string>* timestamps = GetOutputTimestamps(output_metadata);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    newer_version_present->assign(num_keys, false);
  }
  autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
      stack_column_families;
  std::vector<ColumnFamilyHandle*> heap_column_families;
  ColumnFamilyHandle** mutable_column_families = MakeMutableCfHandles(
      column_families, num_keys, &stack_column_families, &heap_column_families);
  MultiGet(options, num_keys, mutable_column_families, keys, values,
           timestamps != nullptr ? timestamps->data() : nullptr, statuses,
           sorted_input);
}

void CompactedDBImpl::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableWideColumns* results, Status* statuses,
    MultiGetOutputMetadata* output_metadata, bool sorted_input) {
  const Status s = InitMultiGetEntityOutputMetadata(num_keys, output_metadata);
  if (!s.ok()) {
    std::fill_n(statuses, num_keys, s);
    return;
  }
  MultiGetEntityWithoutNewerVersionTracking(this, options, num_keys,
                                            column_families, keys, results,
                                            statuses, sorted_input);
}

void CompactedDBImpl::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys, const Slice* keys,
    PinnableAttributeGroups* results, MultiGetOutputMetadata* output_metadata) {
  const size_t total_count = CountAttributeGroups(num_keys, results);
  const Status s =
      InitMultiGetEntityOutputMetadata(total_count, output_metadata);
  if (!results) {
    s.PermitUncheckedError();
    return;
  }
  if (!s.ok()) {
    SetAttributeGroupsStatus(num_keys, results, s);
    return;
  }
  MultiGetEntity(options, num_keys, keys, results);
}

Status CompactedDBImpl::Init(const Options& options) {
  SuperVersionContext sv_context(/* create_superversion */ true);
  mutex_.Lock();
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            ColumnFamilyOptions(options));
  Status s = Recover({cf}, true /* read only */, false, true);
  if (s.ok()) {
    cfd_ = static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily())
               ->cfd();
    cfd_->InstallSuperVersion(&sv_context, &mutex_);
  }
  mutex_.Unlock();
  sv_context.Clean();
  if (!s.ok()) {
    return s;
  }
  NewThreadStatusCfInfo(cfd_);
  version_ = cfd_->GetSuperVersion()->current;
  user_comparator_ = cfd_->user_comparator();
  auto* vstorage = version_->storage_info();
  if (vstorage->num_non_empty_levels() == 0) {
    return Status::NotSupported("no file exists");
  }
  const LevelFilesBrief& l0 = vstorage->LevelFilesBrief(0);
  // L0 should not have files
  if (l0.num_files > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (l0.num_files == 1) {
    if (vstorage->num_non_empty_levels() > 1) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    files_ = l0;
    files_level_ = 0;
    return Status::OK();
  }

  for (int i = 1; i < vstorage->num_non_empty_levels() - 1; ++i) {
    if (vstorage->LevelFilesBrief(i).num_files > 0) {
      return Status::NotSupported("Other levels also contain files");
    }
  }

  int level = vstorage->num_non_empty_levels() - 1;
  if (vstorage->LevelFilesBrief(level).num_files > 0) {
    files_ = vstorage->LevelFilesBrief(level);
    files_level_ = level;
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options, const std::string& dbname,
                             std::unique_ptr<DB>* dbptr) {
  *dbptr = nullptr;

  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->Init(options);
  if (s.ok()) {
    {
      InstrumentedMutexLock l(&db->mutex_);
      db->opened_successfully_ = true;
      if (db->immutable_db_options_.open_files_async) {
        db->ScheduleAsyncFileOpening();
      }
    }
    s = db->StartPeriodicTaskScheduler();
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(db->immutable_db_options_.info_log,
                   "Opened the db as fully compacted mode");
    LogFlush(db->immutable_db_options_.info_log);
    *dbptr = std::move(db);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of the functions defined in
// compacted_db_impl_sync_and_async.h by including it twice. The macros expand
// differently based on whether WITH_COROUTINES or WITHOUT_COROUTINES is
// defined.
// clang-format off
#define WITHOUT_COROUTINES
#include "db/db_impl/compacted_db_impl_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/db_impl/compacted_db_impl_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
