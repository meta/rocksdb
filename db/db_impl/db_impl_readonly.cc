//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_readonly.h"

#include <algorithm>
#include <optional>

#include "db/arena_wrapped_db_iter.h"
#include "db/blob/blob_fetcher.h"
#include "db/db_impl/compacted_db_impl.h"
#include "db/db_impl/db_impl.h"
#include "db/db_impl/db_impl_metadata.h"
#include "db/manifest_ops.h"
#include "db/merge_context.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname, /*seq_per_batch*/ false,
             /*batch_per_txn*/ true, /*read_only*/ true) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in read only mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() = default;

void DBImplReadOnly::MultiGetWithMetadata(
    const ReadOptions& options, const size_t num_keys,
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
  DBImpl::MultiGet(options, num_keys, mutable_column_families, keys, values,
                   timestamps != nullptr ? timestamps->data() : nullptr,
                   statuses, sorted_input);
}

void DBImplReadOnly::MultiGetEntityWithMetadata(
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

void DBImplReadOnly::MultiGetEntityWithMetadata(
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

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& _read_options,
                                      ColumnFamilyHandle* column_family) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return NewErrorIterator(Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`"));
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }
  assert(column_family);
  if (read_options.timestamp) {
    const Status s =
        FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    const Status s = FailIfReadCollapsedHistory(cfd, super_version,
                                                *(read_options.timestamp));
    if (!s.ok()) {
      cfd->GetSuperVersion()->Unref();
      return NewErrorIterator(s);
    }
  }
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? static_cast<const SnapshotImpl*>(read_options.snapshot)->number_
          : latest_snapshot;
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  return NewArenaWrappedDbIterator(
      env_, read_options, cfh, super_version, read_seq, read_callback, this,
      /*expose_blob_index=*/false, /*allow_refresh=*/false,
      /*allow_mark_memtable_for_flush=*/false);
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  return NewIterators(
      std::vector<ReadOptions>(column_families.size(), read_options),
      column_families, iterators);
}

Status DBImplReadOnly::NewIterators(
    const std::vector<ReadOptions>& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.size() != column_families.size()) {
    return Status::InvalidArgument(
        "read_options and column_families must have the same size");
  }
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }

  if (column_families.empty()) {
    iterators->clear();
    return Status::OK();
  }

  const Snapshot* const snapshot = read_options.front().snapshot;
  const bool tailing = read_options.front().tailing;
  std::vector<ReadOptions> normalized_read_options;
  normalized_read_options.reserve(read_options.size());
  for (size_t i = 0; i < read_options.size(); ++i) {
    const ReadOptions& options = read_options[i];
    if (options.io_activity != Env::IOActivity::kUnknown &&
        options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
    if (options.snapshot != snapshot) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same snapshot");
    }
    if (options.tailing != tailing) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same tailing setting");
    }

    auto* cf = column_families[i];
    assert(cf);
    Status s;
    if (options.timestamp) {
      s = FailIfTsMismatchCf(cf, *(options.timestamp));
    } else {
      s = FailIfCfHasTs(cf);
    }
    if (!s.ok()) {
      return s;
    }

    normalized_read_options.emplace_back(options);
    if (normalized_read_options.back().io_activity ==
        Env::IOActivity::kUnknown) {
      normalized_read_options.back().io_activity = Env::IOActivity::kDBIterator;
    }
  }

  iterators->clear();
  iterators->reserve(column_families.size());

  SequenceNumber read_seq =
      snapshot != nullptr ? static_cast<const SnapshotImpl*>(snapshot)->number_
                          : versions_->LastSequence();
  autovector<std::tuple<ColumnFamilyHandleImpl*, SuperVersion*>> cfh_to_sv;
  for (auto* cf : column_families) {
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(cf);
    cfh_to_sv.emplace_back(cfh, cfh->cfd()->GetSuperVersion()->Ref());
  }

  const auto unref_super_versions = [&]() {
    for (const auto& entry : cfh_to_sv) {
      std::get<1>(entry)->Unref();
    }
  };
  for (size_t i = 0; i < cfh_to_sv.size(); ++i) {
    const ReadOptions& options = normalized_read_options[i];
    if (options.timestamp && !options.timestamp->empty()) {
      auto* cfd = std::get<0>(cfh_to_sv[i])->cfd();
      auto* sv = std::get<1>(cfh_to_sv[i]);
      const Status s =
          FailIfReadCollapsedHistory(cfd, sv, *(options.timestamp));
      if (!s.ok()) {
        unref_super_versions();
        return s;
      }
    }
  }

  for (size_t i = 0; i < cfh_to_sv.size(); ++i) {
    auto* cfh = std::get<0>(cfh_to_sv[i]);
    auto* sv = std::get<1>(cfh_to_sv[i]);
    iterators->push_back(NewArenaWrappedDbIterator(
        env_, normalized_read_options[i], cfh, sv, read_seq,
        nullptr /* read_callback */, this, /* expose_blob_index */ false,
        /* allow_refresh */ false, /* allow_mark_memtable_for_flush */ false));
  }
  return Status::OK();
}

namespace {
// Return OK if dbname exists in the file system or create it if
// create_if_missing
Status OpenForReadOnlyCheckExistence(const DBOptions& db_options,
                                     const std::string& dbname) {
  Status s;
  if (!db_options.create_if_missing) {
    // Attempt to read "CURRENT" file
    const std::shared_ptr<FileSystem>& fs = db_options.env->GetFileSystem();
    std::string manifest_path;
    uint64_t manifest_file_number;
    s = GetCurrentManifestPath(dbname, fs.get(), /*is_retry=*/false,
                               &manifest_path, &manifest_file_number);
  } else {
    // Historic behavior that doesn't necessarily make sense
    s = db_options.env->CreateDirIfMissing(dbname);
  }
  return s;
}
}  // namespace

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           std::unique_ptr<DB>* dbptr,
                           bool /*error_if_wal_file_exists*/) {
  Status s = OpenForReadOnlyCheckExistence(options, dbname);
  if (!s.ok()) {
    return s;
  }

  *dbptr = nullptr;

  // Try to first open DB as fully compacted DB
  s = CompactedDBImpl::Open(options, dbname, dbptr);
  if (s.ok()) {
    return s;
  }

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  s = DBImplReadOnly::OpenForReadOnlyWithoutCheck(
      db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a
    // reference to default column family
    delete handles[0];
  }
  return s;
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
    bool error_if_wal_file_exists) {
  // If dbname does not exist in the file system, should not do anything
  Status s = OpenForReadOnlyCheckExistence(db_options, dbname);
  if (!s.ok()) {
    return s;
  }

  return DBImplReadOnly::OpenForReadOnlyWithoutCheck(
      db_options, dbname, column_families, handles, dbptr,
      error_if_wal_file_exists);
}

Status DBImplReadOnly::OpenForReadOnlyWithoutCheck(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
    bool error_if_wal_file_exists) {
  *dbptr = nullptr;
  handles->clear();

  SuperVersionContext sv_context(/* create_superversion */ true);
  DBImplReadOnly* impl = new DBImplReadOnly(db_options, dbname);
  impl->mutex_.Lock();
  Status s = impl->Recover(column_families, true /* read only */,
                           error_if_wal_file_exists);
  if (s.ok()) {
    // set column family handles
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd == nullptr) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      sv_context.NewSuperVersion();
      cfd->InstallSuperVersion(&sv_context, &impl->mutex_);
    }

    impl->opened_successfully_ = true;

    if (db_options.open_files_async) {
      impl->ScheduleAsyncFileOpening();
    }
  }
  impl->mutex_.Unlock();
  sv_context.Clean();
  if (s.ok()) {
    dbptr->reset(impl);
    for (auto* h : *handles) {
      impl->NewThreadStatusCfInfo(
          static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of the functions defined in
// db_impl_readonly_sync_and_async.h by including it twice. The macros expand
// differently based on whether WITH_COROUTINES or WITHOUT_COROUTINES is
// defined.
// clang-format off
#define WITHOUT_COROUTINES
#include "db/db_impl/db_impl_readonly_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/db_impl/db_impl_readonly_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
