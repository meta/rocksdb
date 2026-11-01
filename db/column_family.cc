//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction_picker.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/db_impl/db_impl.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/range_del_aggregator.h"
#include "db/table_properties_collector.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "memtable/flush_initiator.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(
    ColumnFamilyData* column_family_data, DBImpl* db, InstrumentedMutex* mutex)
    : cfd_(column_family_data), db_(db), mutex_(mutex) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    for (auto& listener : cfd_->ioptions().listeners) {
      listener->OnColumnFamilyHandleDeletionStarted(this);
    }
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    // Need to hold some shared pointers owned by the initial_cf_options
    // before final cleaning up finishes.
    ColumnFamilyOptions initial_cf_options_copy = cfd_->initial_cf_options();
    JobContext job_context(0);
    mutex_->Lock();
    bool dropped = cfd_->IsDropped();
    if (cfd_->UnrefAndTryDelete()) {
      if (dropped) {
        db_->FindObsoleteFiles(&job_context, false, true);
      }
    }
    mutex_->Unlock();
    if (job_context.HaveSomethingToDelete()) {
      bool defer_purge =
          db_->immutable_db_options().avoid_unnecessary_blocking_io;
      db_->PurgeObsoleteFiles(job_context, defer_purge);
    }
    job_context.Clean();
  }
}

uint32_t ColumnFamilyHandleImpl::GetID() const { return cfd()->GetID(); }

const std::string& ColumnFamilyHandleImpl::GetName() const {
  return cfd()->GetName();
}

Status ColumnFamilyHandleImpl::GetDescriptor(ColumnFamilyDescriptor* desc) {
  // accessing mutable cf-options requires db mutex.
  InstrumentedMutexLock l(mutex_);
  *desc = ColumnFamilyDescriptor(cfd()->GetName(), cfd()->GetLatestCFOptions());
  return Status::OK();
}

const Comparator* ColumnFamilyHandleImpl::GetComparator() const {
  return cfd()->user_comparator();
}

void GetInternalTblPropCollFactory(
    const ImmutableCFOptions& ioptions,
    InternalTblPropCollFactories* internal_tbl_prop_coll_factories) {
  assert(internal_tbl_prop_coll_factories);

  auto& collector_factories = ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < ioptions.table_properties_collector_factories.size();
       ++i) {
    assert(collector_factories[i]);
    internal_tbl_prop_coll_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(collector_factories[i]));
  }
}

Status CheckCompressionSupportedWithManager(
    CompressionType type, UnownedPtr<CompressionManager> mgr) {
  if (mgr) {
    if (!mgr->SupportsCompressionType(type)) {
      return Status::NotSupported("Compression type " +
                                  CompressionTypeToString(type) +
                                  " is not recognized/supported by this "
                                  "version of CompressionManager " +
                                  mgr->GetId());
    }
  } else {
    if (!CompressionTypeSupported(type)) {
      if (type <= kLastBuiltinCompression) {
        return Status::InvalidArgument("Compression type " +
                                       CompressionTypeToString(type) +
                                       " is not linked with the binary.");
      } else {
        return Status::NotSupported(
            "Compression type " + CompressionTypeToString(type) +
            " is not recognized/supported by built-in CompressionManager.");
      }
    }
  }
  return Status::OK();
}

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      Status s = CheckCompressionSupportedWithManager(
          cf_options.compression_per_level[level],
          cf_options.compression_manager.get());
      if (!s.ok()) {
        return s;
      }
    }
  } else {
    Status s = CheckCompressionSupportedWithManager(
        cf_options.compression, cf_options.compression_manager.get());
    if (!s.ok()) {
      return s;
    }
  }
  if (cf_options.compression_opts.zstd_max_train_bytes > 0) {
    if (cf_options.compression_opts.use_zstd_dict_trainer) {
      if (!ZSTD_TrainDictionarySupported()) {
        return Status::InvalidArgument(
            "zstd dictionary trainer cannot be used because ZSTD 1.1.3+ "
            "is not linked with the binary.");
      }
    } else if (!ZSTD_FinalizeDictionarySupported()) {
      return Status::InvalidArgument(
          "zstd finalizeDictionary cannot be used because ZSTD 1.4.5+ "
          "is not linked with the binary.");
    }
    if (cf_options.compression_opts.max_dict_bytes == 0) {
      return Status::InvalidArgument(
          "The dictionary size limit (`CompressionOptions::max_dict_bytes`) "
          "should be nonzero if we're using zstd's dictionary generator.");
    }
  }

  if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    std::ostringstream oss;
    oss << "The specified blob compression type "
        << CompressionTypeToString(cf_options.blob_compression_type)
        << " is not available.";

    return Status::InvalidArgument(oss.str());
  }

  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    return Status::InvalidArgument(
        "In-place memtable updates (inplace_update_support) is not compatible "
        "with concurrent writes (allow_concurrent_memtable_write)");
  }
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't allow concurrent writes "
        "(allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles. ");
    } else if (cf_options.cf_paths.empty() && db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles. ");
    }
  }
  return Status::OK();
}

namespace {
const uint64_t kDefaultTtl = 0xfffffffffffffffe;
const uint64_t kDefaultPeriodicCompSecs = 0xfffffffffffffffe;
}  // anonymous namespace

ColumnFamilyOptions SanitizeCfOptions(const ImmutableDBOptions& db_options,
                                      bool read_only,
                                      const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  ClipToRange(&result.write_buffer_size, (static_cast<size_t>(64)) << 10,
              clamp_max);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size =
        std::min(size_t{1024 * 1024}, result.write_buffer_size / 8);

    // Align up to 4k
    const size_t align = 4 * 1024;
    result.arena_block_size =
        ((result.arena_block_size + align - 1) / align) * align;
  }
  result.min_write_buffer_number_to_merge =
      std::min(result.min_write_buffer_number_to_merge,
               result.max_write_buffer_number - 1);
  if (result.min_write_buffer_number_to_merge < 1) {
    result.min_write_buffer_number_to_merge = 1;
  }

  if (db_options.atomic_flush && result.min_write_buffer_number_to_merge > 1) {
    ROCKS_LOG_WARN(
        db_options.logger,
        "Currently, if atomic_flush is true, then triggering flush for any "
        "column family internally (non-manual flush) will trigger flushing "
        "all column families even if the number of memtables is smaller "
        "min_write_buffer_number_to_merge. Therefore, configuring "
        "min_write_buffer_number_to_merge > 1 is not compatible and should "
        "be satinized to 1. Not doing so will lead to data loss and "
        "inconsistent state across multiple column families when WAL is "
        "disabled, which is a common setting for atomic flush");

    result.min_write_buffer_number_to_merge = 1;
  }
  if (result.disallow_memtable_writes) {
    // A simple memtable that enforces MarkReadOnly (unlike skip list)
    result.memtable_factory = std::make_shared<VectorRepFactory>();
  }

  if (result.num_levels < 1) {
    result.num_levels = 1;
  }
  if (result.compaction_style == kCompactionStyleLevel &&
      result.num_levels < 2) {
    result.num_levels = 2;
  }

  if (result.compaction_style == kCompactionStyleUniversal &&
      (db_options.allow_ingest_behind || result.cf_allow_ingest_behind) &&
      result.num_levels < 3) {
    result.num_levels = 3;
  }

  if (result.max_write_buffer_number < 2) {
    result.max_write_buffer_number = 2;
  }
  if (result.max_write_buffer_size_to_maintain < 0) {
    result.max_write_buffer_size_to_maintain =
        result.max_write_buffer_number *
        static_cast<int64_t>(result.write_buffer_size);
  }
  // bloom filter size shouldn't exceed 1/4 of memtable size.
  if (result.memtable_prefix_bloom_size_ratio > 0.25) {
    result.memtable_prefix_bloom_size_ratio = 0.25;
  } else if (result.memtable_prefix_bloom_size_ratio < 0) {
    result.memtable_prefix_bloom_size_ratio = 0;
  }

  if (!result.prefix_extractor) {
    assert(result.memtable_factory);
    Slice name = result.memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (result.compaction_style == kCompactionStyleFIFO) {
    // since we delete level0 files in FIFO compaction when there are too many
    // of them, these options don't really mean anything
    result.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    result.level0_stop_writes_trigger = std::numeric_limits<int>::max();
  }

  if (result.max_bytes_for_level_multiplier <= 0) {
    result.max_bytes_for_level_multiplier = 1;
  }

  if (result.level0_file_num_compaction_trigger == 0) {
    ROCKS_LOG_WARN(db_options.logger,
                   "level0_file_num_compaction_trigger cannot be 0");
    result.level0_file_num_compaction_trigger = 1;
  }

  if (result.level0_stop_writes_trigger <
          result.level0_slowdown_writes_trigger ||
      result.level0_slowdown_writes_trigger <
          result.level0_file_num_compaction_trigger) {
    ROCKS_LOG_WARN(db_options.logger,
                   "This condition must be satisfied: "
                   "level0_stop_writes_trigger(%d) >= "
                   "level0_slowdown_writes_trigger(%d) >= "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
    if (result.level0_slowdown_writes_trigger <
        result.level0_file_num_compaction_trigger) {
      result.level0_slowdown_writes_trigger =
          result.level0_file_num_compaction_trigger;
    }
    if (result.level0_stop_writes_trigger <
        result.level0_slowdown_writes_trigger) {
      result.level0_stop_writes_trigger = result.level0_slowdown_writes_trigger;
    }
    ROCKS_LOG_WARN(db_options.logger,
                   "Adjust the value to "
                   "level0_stop_writes_trigger(%d) "
                   "level0_slowdown_writes_trigger(%d) "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
  }

  if (result.soft_pending_compaction_bytes_limit == 0) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  } else if (result.hard_pending_compaction_bytes_limit > 0 &&
             result.soft_pending_compaction_bytes_limit >
                 result.hard_pending_compaction_bytes_limit) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  }

  // When the DB is stopped, it's possible that there are some .trash files that
  // were not deleted yet, when we open the DB we will find these .trash files
  // and schedule them to be deleted (or delete immediately if SstFileManager
  // was not used)
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options.sst_file_manager.get());
  for (size_t i = 0; i < result.cf_paths.size(); i++) {
    DeleteScheduler::CleanupDirectory(db_options.env, sfm,
                                      result.cf_paths[i].path)
        .PermitUncheckedError();
  }

  if (result.cf_paths.empty()) {
    result.cf_paths = db_options.db_paths;
  }

  if (result.level_compaction_dynamic_level_bytes) {
    if (result.compaction_style != kCompactionStyleLevel) {
      ROCKS_LOG_INFO(db_options.info_log.get(),
                     "level_compaction_dynamic_level_bytes only makes sense "
                     "for level-based compaction");
      result.level_compaction_dynamic_level_bytes = false;
    } else if (result.cf_paths.size() > 1U) {
      // we don't yet know how to make both of this feature and multiple
      // DB path work.
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "multiple cf_paths/db_paths and "
                     "level_compaction_dynamic_level_bytes "
                     "can't be used together");
      result.level_compaction_dynamic_level_bytes = false;
    }
  }

  if (result.max_compaction_bytes == 0) {
    // For FIFO with use_kv_ratio_compaction, leave max_compaction_bytes as 0
    // to signal "auto-calculate target from capacity and SST/blob ratio."
    // When explicitly set by the user, it overrides the auto-calculated target.
    if (result.compaction_style != kCompactionStyleFIFO ||
        !result.compaction_options_fifo.use_kv_ratio_compaction) {
      result.max_compaction_bytes = result.target_file_size_base * 25;
    }
  }

  bool is_block_based_table = (result.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));

  const uint64_t kAdjustedTtl = 30 * 24 * 60 * 60;
  if (result.ttl == kDefaultTtl) {
    if (is_block_based_table) {
      // FIFO also requires max_open_files=-1, which is checked in
      // ValidateOptions().
      result.ttl = kAdjustedTtl;
    } else {
      result.ttl = 0;
    }
  }

  const uint64_t kAdjustedPeriodicCompSecs = 30 * 24 * 60 * 60;
  if (result.compaction_style == kCompactionStyleLevel) {
    if ((result.compaction_filter != nullptr ||
         result.compaction_filter_factory != nullptr) &&
        result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else if (result.compaction_style == kCompactionStyleUniversal) {
    if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else if (result.compaction_style == kCompactionStyleFIFO) {
    if (result.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "periodic_compaction_seconds does not support FIFO compaction. You"
          "may want to set option TTL instead.");
    }
    if (result.last_level_temperature != Temperature::kUnknown) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "last_level_temperature is ignored with FIFO compaction. Consider "
          "CompactionOptionsFIFO::file_temperature_age_thresholds.");
      result.last_level_temperature = Temperature::kUnknown;
    }
  }

  // For universal compaction, `ttl` and `periodic_compaction_seconds` mean the
  // same thing, take the stricter value.
  if (result.compaction_style == kCompactionStyleUniversal) {
    if (result.periodic_compaction_seconds == 0) {
      result.periodic_compaction_seconds = result.ttl;
    } else if (result.ttl != 0) {
      result.periodic_compaction_seconds =
          std::min(result.ttl, result.periodic_compaction_seconds);
    }
  }

  if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
    result.periodic_compaction_seconds = 0;
  }

  if (read_only && (result.preserve_internal_time_seconds > 0 ||
                    result.preclude_last_level_data_seconds > 0)) {
    // With no writes coming in, we don't need periodic SeqnoToTime entries.
    // Existing SST files may or may not have that info associated with them.
    // Remote compaction runs in a secondary (read-only) instance but writes
    // files, so it restores both from the primary. See
    // DBImplSecondary::CompactWithoutInstallation().
    ROCKS_LOG_WARN(
        db_options.info_log.get(),
        "preserve_internal_time_seconds and preclude_last_level_data_seconds "
        "are ignored in read-only DB");
    result.preserve_internal_time_seconds = 0;
    result.preclude_last_level_data_seconds = 0;
  }

  if (read_only) {
    if (result.memtable_op_scan_flush_trigger) {
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "option memtable_op_scan_flush_trigger is sanitized to "
                     "0(disabled) for read only DB.");
      result.memtable_op_scan_flush_trigger = 0;
    }
    if (result.memtable_avg_op_scan_flush_trigger) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "option memtable_avg_op_scan_flush_trigger is sanitized to "
          "0(disabled) for read only DB.");
      result.memtable_avg_op_scan_flush_trigger = 0;
    }
  }
  if (result.enable_blob_direct_write && !result.enable_blob_files) {
    ROCKS_LOG_WARN(db_options.info_log.get(),
                   "enable_blob_direct_write requires enable_blob_files=true. "
                   "Disabling blob direct write.");
    result.enable_blob_direct_write = false;
  }
  if (result.blob_direct_write_partitions == 0) {
    result.blob_direct_write_partitions = 1;
  }
  return result;
}

int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;

SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  // Since this SuperVersion object is being deleted,
  // decrement reference to the immutable MemtableList
  // this SV object was pointing to.
  imm->Unref(&to_delete);
  ReadOnlyMemTable* m = mem->Unref();
  if (m != nullptr) {
    auto* memory_usage = current->cfd()->imm()->current_memory_usage();
    assert(*memory_usage >= m->ApproximateMemoryUsage());
    *memory_usage -= m->ApproximateMemoryUsage();
    to_delete.push_back(m);
  }
  current->Unref();
  cfd->UnrefAndTryDelete();
}

void SuperVersion::Init(
    ColumnFamilyData* new_cfd, MemTable* new_mem, MemTableListVersion* new_imm,
    Version* new_current,
    std::shared_ptr<const SeqnoToTimeMapping> new_seqno_to_time_mapping) {
  cfd = new_cfd;
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  full_history_ts_low = cfd->GetFullHistoryTsLow();
  seqno_to_time_mapping = std::move(new_seqno_to_time_mapping);
  cfd->Ref();
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);

  // There should be at least one mapping entry iff time tracking is enabled.
#ifndef NDEBUG
  MinAndMaxPreserveSeconds preserve_info{mutable_cf_options};
  if (preserve_info.IsEnabled()) {
    assert(seqno_to_time_mapping);
    assert(!seqno_to_time_mapping->Empty());
  } else {
    assert(seqno_to_time_mapping == nullptr);
  }
#endif  // NDEBUG
}

namespace {
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exits or a ThreadLocalPtr gets
  // destroyed. When the former happens, the thread shouldn't see kSVInUse.
  // When the latter happens, only super_version_ holds a reference
  // to ColumnFamilyData, so no further queries are possible.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  bool was_last_ref __attribute__((__unused__));
  was_last_ref = sv->Unref();
  // Thread-local SuperVersions can't outlive ColumnFamilyData::super_version_.
  // This is important because we can't do SuperVersion cleanup here.
  // That would require locking DB mutex, which would deadlock because
  // SuperVersionUnrefHandle is called with locked ThreadLocalPtr mutex.
  assert(!was_last_ref);
}
}  // anonymous namespace

std::vector<std::string> ColumnFamilyData::GetDbPaths() const {
  std::vector<std::string> paths;
  paths.reserve(ioptions_.cf_paths.size());
  for (const DbPath& db_path : ioptions_.cf_paths) {
    paths.emplace_back(db_path.path);
  }
  return paths;
}

const uint32_t ColumnFamilyData::kDummyColumnFamilyDataId =
    std::numeric_limits<uint32_t>::max();

ColumnFamilyData::ColumnFamilyData(
    uint32_t id, const std::string& name, Version* _dummy_versions,
    Cache* _table_cache, WriteBufferManager* write_buffer_manager,
    const ColumnFamilyOptions& cf_options, const ImmutableDBOptions& db_options,
    const FileOptions* file_options, ColumnFamilySet* column_family_set,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, bool read_only,
    bool options_are_sanitized, bool fast_sst_open)
    : id_(id),
      name_(name),
      dummy_versions_(_dummy_versions),
      current_(nullptr),
      refs_(0),
      initialized_(false),
      dropped_(false),
      flush_skip_reschedule_(false),
      internal_comparator_(cf_options.comparator),
      initial_cf_options_(
          options_are_sanitized
              ? cf_options
              : SanitizeCfOptions(db_options, read_only, cf_options)),
      ioptions_(db_options, initial_cf_options_),
      mutable_cf_options_(initial_cf_options_),
      is_delete_range_supported_(
          cf_options.table_factory->IsDeleteRangeSupported()),
      write_buffer_manager_(write_buffer_manager),
      mem_(nullptr),
      imm_(ioptions_.min_write_buffer_number_to_merge,
           ioptions_.max_write_buffer_size_to_maintain),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle)),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      column_family_set_(column_family_set),
      queued_for_flush_(false),
      queued_for_compaction_(false),
      prev_compaction_needed_bytes_(0),
      allow_2pc_(db_options.allow_2pc),
      last_memtable_id_(0),
      db_paths_registered_(false),
      mempurge_used_(false),
      next_epoch_number_(1) {
  if (id_ != kDummyColumnFamilyDataId) {
    // TODO(cc): RegisterDbPaths can be expensive, considering moving it
    // outside of this constructor which might be called with db mutex held.
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->RegisterDbPaths(GetDbPaths());
    if (s.ok()) {
      db_paths_registered_ = true;
    } else {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to register data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
  Ref();

  // Convert user defined table properties collector factories to internal ones.
  GetInternalTblPropCollFactory(ioptions_, &internal_tbl_prop_coll_factories_);

  // if _dummy_versions is nullptr, then this is a dummy column family.
  if (_dummy_versions != nullptr) {
    internal_stats_.reset(
        new InternalStats(ioptions_.num_levels, ioptions_.clock, this));
    table_cache_.reset(new TableCache(ioptions_, file_options, _table_cache,
                                      block_cache_tracer, io_tracer,
                                      db_session_id, fast_sst_open, name_));
    blob_file_cache_.reset(
        new BlobFileCache(_table_cache, &ioptions(), soptions(), id_,
                          internal_stats_->GetBlobFileReadHist(), io_tracer));
    blob_source_.reset(new BlobSource(ioptions_, mutable_cf_options_, db_id,
                                      db_session_id, blob_file_cache_.get()));
    // Let the table cache route same-file ("embedded") blob reads through the
    // blob value cache + stats. Both objects share this CFD's lifetime.
    table_cache_->SetBlobSource(blob_source_.get());

    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      compaction_picker_.reset(
          new UniversalCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleFIFO) {
      compaction_picker_.reset(
          new FIFOCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleNone) {
      compaction_picker_.reset(
          new NullCompactionPicker(ioptions_, &internal_comparator_));
      ROCKS_LOG_WARN(ioptions_.logger,
                     "Column family %s does not use any background compaction. "
                     "Compactions can only be done via CompactFiles\n",
                     GetName().c_str());
    } else {
      ROCKS_LOG_ERROR(ioptions_.logger,
                      "Unable to recognize the specified compaction style %d. "
                      "Column family %s will use kCompactionStyleLevel.\n",
                      ioptions_.compaction_style, GetName().c_str());
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    }

    if (column_family_set_->NumberOfColumnFamilies() < 10) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "--------------- Options for column family [%s]:\n",
                     name.c_str());
      initial_cf_options_.Dump(ioptions_.logger);
    } else {
      ROCKS_LOG_INFO(ioptions_.logger, "\t(skipping printing options)\n");
    }
  }

  RecalculateWriteStallConditions(mutable_cf_options_);

  if (cf_options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName()) &&
      cf_options.table_factory->GetOptions<BlockBasedTableOptions>()) {
    const BlockBasedTableOptions* bbto =
        cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
    const auto& options_overrides = bbto->cache_usage_options.options_overrides;
    const auto file_metadata_charged =
        options_overrides.at(CacheEntryRole::kFileMetadata).charged;
    if (bbto->block_cache &&
        file_metadata_charged == CacheEntryRoleOptions::Decision::kEnabled) {
      // TODO(hx235): Add a `ConcurrentCacheReservationManager` at DB scope
      // responsible for reservation of `ObsoleteFileInfo` so that we can keep
      // this `file_metadata_cache_res_mgr_` nonconcurrent
      file_metadata_cache_res_mgr_.reset(new ConcurrentCacheReservationManager(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              bbto->block_cache)));
    }
  }
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData() {
  assert(refs_.load(std::memory_order_relaxed) == 0);
  // remove from linked list
  auto prev = prev_;
  auto next = next_;
  prev->next_ = next;
  next->prev_ = prev;

  if (!dropped_ && column_family_set_ != nullptr) {
    // If it's dropped, it's already removed from column family set
    // If column_family_set_ == nullptr, this is dummy CFD and not in
    // ColumnFamilySet
    column_family_set_->RemoveColumnFamily(this);
  }

  if (current_ != nullptr) {
    current_->Unref();
  }

  // It would be wrong if this ColumnFamilyData is in flush_queue_ or
  // compaction_queue_ and we destroyed it
  assert(!queued_for_flush_);
  assert(!queued_for_compaction_);
  assert(super_version_ == nullptr);

  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->Next() == dummy_versions_);
    bool deleted __attribute__((__unused__));
    deleted = dummy_versions_->Unref();
    assert(deleted);
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  autovector<ReadOnlyMemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (auto* m : to_delete) {
    delete m;
  }

  if (db_paths_registered_) {
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->UnregisterDbPaths(GetDbPaths());
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to unregister data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
}

void ColumnFamilyData::SetBlobPartitionManager(
    std::shared_ptr<BlobFilePartitionManager> mgr) {
  blob_partition_manager_ = std::move(mgr);
}

bool ColumnFamilyData::UnrefAndTryDelete() {
  int old_refs = refs_.fetch_sub(1);
  assert(old_refs > 0);

  if (old_refs == 1) {
    assert(super_version_ == nullptr);
    delete this;
    return true;
  }

  if (old_refs == 2 && super_version_ != nullptr) {
    // Only the super_version_ holds me
    SuperVersion* sv = super_version_;
    super_version_ = nullptr;

    // Release SuperVersion references kept in ThreadLocalPtr.
    local_sv_.reset();

    if (sv->Unref()) {
      // Note: sv will delete this ColumnFamilyData during Cleanup()
      assert(sv->cfd == this);
      sv->Cleanup();
      delete sv;
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::SetDropped() {
  // can't drop default CF
  assert(id_ != 0);
  dropped_ = true;
  // A tracker can still contain accounting published while a different WBM
  // policy was active. Cleanup therefore cannot depend on the current policy.
  if (mem_ != nullptr) {
    mem_->StopWBMTracking();
  }
  write_controller_token_.reset();

  // remove from column_family_set
  column_family_set_->RemoveColumnFamily(this);
}

ColumnFamilyOptions ColumnFamilyData::GetLatestCFOptions() const {
  return BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
}

uint64_t ColumnFamilyData::OldestLogToKeep() {
  auto current_log = GetLogNumber();

  if (allow_2pc_) {
    auto imm_prep_log = imm()->PrecomputeMinLogContainingPrepSection();
    auto mem_prep_log = mem()->GetMinLogContainingPrepSection();

    if (imm_prep_log > 0 && imm_prep_log < current_log) {
      current_log = imm_prep_log;
    }

    if (mem_prep_log > 0 && mem_prep_log < current_log) {
      current_log = mem_prep_log;
    }
  }

  return current_log;
}

const double kIncSlowdownRatio = 0.8;
const double kDecSlowdownRatio = 1 / kIncSlowdownRatio;
const double kNearStopSlowdownRatio = 0.6;
const double kDelayRecoverSlowdownRatio = 1.4;

namespace {
// If penalize_stop is true, we further reduce slowdown rate.
std::unique_ptr<WriteControllerToken> SetupDelay(
    WriteController* write_controller, uint64_t compaction_needed_bytes,
    uint64_t prev_compaction_need_bytes, bool penalize_stop,
    bool auto_compactions_disabled) {
  const uint64_t kMinWriteRate = 16 * 1024u;  // Minimum write rate 16KB/s.

  uint64_t max_write_rate = write_controller->max_delayed_write_rate();
  uint64_t write_rate = write_controller->delayed_write_rate();

  if (auto_compactions_disabled) {
    // When auto compaction is disabled, always use the value user gave.
    write_rate = max_write_rate;
  } else if (write_controller->NeedsDelay() && max_write_rate > kMinWriteRate) {
    // If user gives rate less than kMinWriteRate, don't adjust it.
    //
    // If already delayed, need to adjust based on previous compaction debt.
    // When there are two or more column families require delay, we always
    // increase or reduce write rate based on information for one single
    // column family. It is likely to be OK but we can improve if there is a
    // problem.
    // Ignore compaction_needed_bytes = 0 case because compaction_needed_bytes
    // is only available in level-based compaction
    //
    // If the compaction debt stays the same as previously, we also further slow
    // down. It usually means a mem table is full. It's mainly for the case
    // where both of flush and compaction are much slower than the speed we
    // insert to mem tables, so we need to actively slow down before we get
    // feedback signal from compaction and flushes to avoid the full stop
    // because of hitting the max write buffer number.
    //
    // If DB just falled into the stop condition, we need to further reduce
    // the write rate to avoid the stop condition.
    if (penalize_stop) {
      // Penalize the near stop or stop condition by more aggressive slowdown.
      // This is to provide the long term slowdown increase signal.
      // The penalty is more than the reward of recovering to the normal
      // condition.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kNearStopSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > 0 &&
               prev_compaction_need_bytes <= compaction_needed_bytes) {
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kIncSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > compaction_needed_bytes) {
      // We are speeding up by ratio of kSlowdownRatio when we have paid
      // compaction debt. But we'll never speed up to faster than the write rate
      // given by users.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kDecSlowdownRatio);
      if (write_rate > max_write_rate) {
        write_rate = max_write_rate;
      }
    }
  }
  return write_controller->GetDelayToken(write_rate);
}

int GetL0FileCountForCompactionSpeedup(int level0_file_num_compaction_trigger,
                                       int level0_slowdown_writes_trigger) {
  // SanitizeOptions() ensures it.
  assert(level0_file_num_compaction_trigger <= level0_slowdown_writes_trigger);

  if (level0_file_num_compaction_trigger < 0) {
    return std::numeric_limits<int>::max();
  }

  const int64_t twice_level0_trigger =
      static_cast<int64_t>(level0_file_num_compaction_trigger) * 2;

  const int64_t one_fourth_trigger_slowdown =
      static_cast<int64_t>(level0_file_num_compaction_trigger) +
      ((level0_slowdown_writes_trigger - level0_file_num_compaction_trigger) /
       4);

  assert(twice_level0_trigger >= 0);
  assert(one_fourth_trigger_slowdown >= 0);

  // 1/4 of the way between L0 compaction trigger threshold and slowdown
  // condition.
  // Or twice as compaction trigger, if it is smaller.
  int64_t res = std::min(twice_level0_trigger, one_fourth_trigger_slowdown);
  if (res >= std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::max();
  } else {
    // res fits in int
    return static_cast<int>(res);
  }
}

uint64_t GetPendingCompactionBytesForCompactionSpeedup(
    const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) {
  // Compaction debt relatively large compared to the stable (bottommost) data
  // size indicates compaction fell behind.
  const uint64_t kBottommostSizeDivisor = 8;
  // Meaningful progress toward the slowdown trigger is another good indication.
  const uint64_t kSlowdownTriggerDivisor = 4;

  uint64_t bottommost_files_size = 0;
  for (const auto& level_and_file : vstorage->BottommostFiles()) {
    bottommost_files_size += level_and_file.second->fd.GetFileSize();
  }

  // Slowdown trigger might be zero but that means compaction speedup should
  // always happen (undocumented/historical), so no special treatment is needed.
  uint64_t slowdown_threshold =
      mutable_cf_options.soft_pending_compaction_bytes_limit /
      kSlowdownTriggerDivisor;

  // Size of zero, however, should not be used to decide to speedup compaction.
  if (bottommost_files_size == 0) {
    return slowdown_threshold;
  }

  // Prevent a small CF from triggering parallel compactions for other CFs.
  // Require compaction debt to be more than a full L0 to Lbase compaction.
  const uint64_t kMinDebtSize = 2 * mutable_cf_options.max_bytes_for_level_base;
  uint64_t size_threshold =
      std::max(bottommost_files_size / kBottommostSizeDivisor, kMinDebtSize);
  return std::min(size_threshold, slowdown_threshold);
}

uint64_t GetMarkedFileCountForCompactionSpeedup() {
  // When just one file is marked, it is not clear that parallel compaction will
  // help the compaction that the user nicely requested to happen sooner. When
  // multiple files are marked, however, it is pretty clearly helpful, except
  // for the rare case in which a single compaction grabs all the marked files.
  return 2;
}
}  // anonymous namespace

std::pair<WriteStallCondition, WriteStallCause>
ColumnFamilyData::GetWriteStallConditionAndCause(
    int num_unflushed_memtables, int num_l0_files,
    uint64_t num_compaction_needed_bytes,
    const MutableCFOptions& mutable_cf_options,
    const ImmutableCFOptions& immutable_cf_options) {
  if (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number) {
    return {WriteStallCondition::kStopped, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             num_l0_files >= mutable_cf_options.level0_stop_writes_trigger) {
    return {WriteStallCondition::kStopped, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.hard_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kStopped,
            WriteStallCause::kPendingCompactionBytes};
  } else if (mutable_cf_options.max_write_buffer_number > 3 &&
             num_unflushed_memtables >=
                 mutable_cf_options.max_write_buffer_number - 1 &&
             num_unflushed_memtables - 1 >=
                 immutable_cf_options.min_write_buffer_number_to_merge) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.level0_slowdown_writes_trigger >= 0 &&
             num_l0_files >=
                 mutable_cf_options.level0_slowdown_writes_trigger) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.soft_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kDelayed,
            WriteStallCause::kPendingCompactionBytes};
  }
  return {WriteStallCondition::kNormal, WriteStallCause::kNone};
}

WriteStallCondition ColumnFamilyData::RecalculateWriteStallConditions(
    const MutableCFOptions& mutable_cf_options) {
  auto write_stall_condition = WriteStallCondition::kNormal;
  if (current_ != nullptr) {
    auto* vstorage = current_->storage_info();
    auto write_controller = column_family_set_->write_controller_;
    uint64_t compaction_needed_bytes =
        vstorage->estimated_compaction_needed_bytes();

    auto write_stall_condition_and_cause = GetWriteStallConditionAndCause(
        imm()->NumNotFlushed(), vstorage->l0_delay_trigger_count(),
        vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
        ioptions());
    write_stall_condition = write_stall_condition_and_cause.first;
    auto write_stall_cause = write_stall_condition_and_cause.second;

    bool was_stopped = write_controller->IsStopped();
    bool needed_delay = write_controller->NeedsDelay();

    if (write_stall_condition == WriteStallCondition::kStopped &&
        write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d",
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number);
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_STOPS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stopping writes because we have %d level-0 files",
                     name_.c_str(), vstorage->l0_delay_trigger_count());
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because of estimated pending compaction "
          "bytes %" PRIu64,
          name_.c_str(), compaction_needed_bytes);
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d "
          "rate %" PRIu64,
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number,
          write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      // L0 is the last two files from stopping.
      bool near_stop = vstorage->l0_delay_trigger_count() >=
                       mutable_cf_options.level0_stop_writes_trigger - 2;
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_DELAYS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stalling writes because we have %d level-0 files "
                     "rate %" PRIu64,
                     name_.c_str(), vstorage->l0_delay_trigger_count(),
                     write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      // If the distance to hard limit is less than 1/4 of the gap between soft
      // and
      // hard bytes limit, we think it is near stop and speed up the slowdown.
      bool near_stop =
          mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
          (compaction_needed_bytes -
           mutable_cf_options.soft_pending_compaction_bytes_limit) >
              3 *
                  (mutable_cf_options.hard_pending_compaction_bytes_limit -
                   mutable_cf_options.soft_pending_compaction_bytes_limit) /
                  4;

      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because of estimated pending compaction "
          "bytes %" PRIu64 " rate %" PRIu64,
          name_.c_str(), vstorage->estimated_compaction_needed_bytes(),
          write_controller->delayed_write_rate());
    } else {
      assert(write_stall_condition == WriteStallCondition::kNormal);
      if (vstorage->l0_delay_trigger_count() >=
          GetL0FileCountForCompactionSpeedup(
              mutable_cf_options.level0_file_num_compaction_trigger,
              mutable_cf_options.level0_slowdown_writes_trigger)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because we have %d level-0 "
            "files ",
            name_.c_str(), vstorage->l0_delay_trigger_count());
      } else if (mutable_cf_options.soft_pending_compaction_bytes_limit == 0) {
        // If soft pending compaction byte limit is not set, always speed up
        // compaction.
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
      } else if (vstorage->estimated_compaction_needed_bytes() >=
                 GetPendingCompactionBytesForCompactionSpeedup(
                     mutable_cf_options, vstorage)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because of estimated pending "
            "compaction "
            "bytes %" PRIu64,
            name_.c_str(), vstorage->estimated_compaction_needed_bytes());
      } else if (uint64_t(vstorage->FilesMarkedForCompaction().size()) >=
                 GetMarkedFileCountForCompactionSpeedup()) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because we have %" PRIu64
            " files marked for compaction",
            name_.c_str(),
            uint64_t(vstorage->FilesMarkedForCompaction().size()));
      } else {
        write_controller_token_.reset();
      }
      // If the DB recovers from delay conditions, we reward with reducing
      // double the slowdown ratio. This is to balance the long term slowdown
      // increase signal.
      if (needed_delay) {
        uint64_t write_rate = write_controller->delayed_write_rate();
        write_controller->set_delayed_write_rate(static_cast<uint64_t>(
            static_cast<double>(write_rate) * kDelayRecoverSlowdownRatio));
        // Set the low pri limit to be 1/4 the delayed write rate.
        // Note we don't reset this value even after delay condition is relased.
        // Low-pri rate will continue to apply if there is a compaction
        // pressure.
        write_controller->low_pri_rate_limiter()->SetBytesPerSecond(write_rate /
                                                                    4);
      }
    }
    prev_compaction_needed_bytes_ = compaction_needed_bytes;
  }
  return write_stall_condition;
}

const FileOptions* ColumnFamilyData::soptions() const {
  return &(column_family_set_->file_options_);
}

void ColumnFamilyData::SetCurrent(Version* current_version) {
  current_ = current_version;
}

uint64_t ColumnFamilyData::GetNumLiveVersions() const {
  return VersionSet::GetNumLiveVersions(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalSstFilesSize() const {
  return VersionSet::GetTotalSstFilesSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalBlobFileSize() const {
  return VersionSet::GetTotalBlobFileSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetLiveSstFilesSize() const {
  return current_->GetSstFilesSize();
}

MemTable* ColumnFamilyData::ConstructNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  return new MemTable(internal_comparator_, ioptions_, mutable_cf_options,
                      write_buffer_manager_, earliest_seq, id_,
                      column_family_set_ != nullptr
                          ? column_family_set_->flush_initiator()
                          : nullptr);
}

void ColumnFamilyData::CreateNewMemtable(SequenceNumber earliest_seq) {
  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  // NOTE: db mutex must be locked for SetMemtable, so safe for
  // GetLatestMutableCFOptions
  SetMemtable(ConstructNewMemtable(GetLatestMutableCFOptions(), earliest_seq));
  mem_->Ref();
}

bool ColumnFamilyData::NeedsCompaction() const {
  return !mutable_cf_options_.disable_auto_compactions &&
         compaction_picker_->NeedsCompaction(current_->storage_info());
}

Compaction* ColumnFamilyData::PickCompaction(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
    bool require_max_output_level) {
  auto* result = compaction_picker_->PickCompaction(
      GetName(), mutable_options, mutable_db_options, existing_snapshots,
      snapshot_checker, current_->storage_info(), log_buffer,
      GetFullHistoryTsLow(), require_max_output_level);
  if (result != nullptr) {
    result->FinalizeInputInfo(current_);
  }
  return result;
}

bool ColumnFamilyData::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key, int level,
    bool range_limit_exclusive) const {
  return compaction_picker_->RangeOverlapWithCompaction(
      smallest_user_key, largest_user_key, level, range_limit_exclusive);
}

Status ColumnFamilyData::RangesOverlapWithMemtables(
    const autovector<UserKeyRange>& ranges, SuperVersion* super_version,
    bool allow_data_in_errors, bool* overlap, bool range_limit_exclusive) {
  assert(overlap != nullptr);
  *overlap = false;
  // Create an InternalIterator over all unflushed memtables
  Arena arena;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  MergeIteratorBuilder merge_iter_builder(&internal_comparator_, &arena);
  merge_iter_builder.AddIterator(super_version->mem->NewIterator(
      read_opts, /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/false));
  super_version->imm->AddIterators(read_opts, /*seqno_to_time_mapping=*/nullptr,
                                   /*prefix_extractor=*/nullptr,
                                   &merge_iter_builder,
                                   false /* add_range_tombstone_iter */);
  ScopedArenaPtr<InternalIterator> memtable_iter(merge_iter_builder.Finish());

  auto read_seq = super_version->current->version_set()->LastSequence();
  ReadRangeDelAggregator range_del_agg(&internal_comparator_, read_seq);
  auto* active_range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      read_opts, read_seq, false /* immutable_memtable */);
  range_del_agg.AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator>(active_range_del_iter));
  Status status;
  status = super_version->imm->AddRangeTombstoneIterators(
      read_opts, nullptr /* arena */, &range_del_agg);
  // AddRangeTombstoneIterators always return Status::OK.
  assert(status.ok());

  for (size_t i = 0; i < ranges.size() && status.ok() && !*overlap; ++i) {
    auto* vstorage = super_version->current->storage_info();
    auto* ucmp = vstorage->InternalComparator()->user_comparator();
    InternalKey range_start(ranges[i].start, kMaxSequenceNumber,
                            kValueTypeForSeek);
    memtable_iter->Seek(range_start.Encode());
    status = memtable_iter->status();
    ParsedInternalKey seek_result;

    if (status.ok() && memtable_iter->Valid()) {
      status = ParseInternalKey(memtable_iter->key(), &seek_result,
                                allow_data_in_errors);
    }

    if (status.ok()) {
      const int limit_cmp = memtable_iter->Valid()
                                ? ucmp->CompareWithoutTimestamp(
                                      seek_result.user_key, ranges[i].limit)
                                : 1;
      if (memtable_iter->Valid() &&
          (range_limit_exclusive ? limit_cmp < 0 : limit_cmp <= 0)) {
        *overlap = true;
      } else if (range_del_agg.IsRangeOverlapped(
                     ranges[i].start, ranges[i].limit, range_limit_exclusive)) {
        *overlap = true;
      }
    }
  }
  return status;
}

const int ColumnFamilyData::kCompactAllLevels = -1;
const int ColumnFamilyData::kCompactToBaseLevel = -2;

Compaction* ColumnFamilyData::CompactRange(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, int input_level,
    int output_level, const CompactRangeOptions& compact_range_options,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts) {
  auto* result = compaction_picker_->PickCompactionForCompactRange(
      GetName(), mutable_cf_options, mutable_db_options,
      current_->storage_info(), input_level, output_level,
      compact_range_options, begin, end, compaction_end, conflict,
      max_file_num_to_ignore, trim_ts, GetFullHistoryTsLow());
  if (result != nullptr) {
    result->FinalizeInputInfo(current_);
  }
  TEST_SYNC_POINT("ColumnFamilyData::CompactRange:Return");
  return result;
}

SuperVersion* ColumnFamilyData::GetReferencedSuperVersion(DBImpl* db) {
  SuperVersion* sv = GetThreadLocalSuperVersion(db);
  sv->Ref();
  if (!ReturnThreadLocalSuperVersion(sv)) {
    // This Unref() corresponds to the Ref() in GetThreadLocalSuperVersion()
    // when the thread-local pointer was populated. So, the Ref() earlier in
    // this function still prevents the returned SuperVersion* from being
    // deleted out from under the caller.
    sv->Unref();
  }
  return sv;
}

SuperVersion* ColumnFamilyData::GetThreadLocalSuperVersion(DBImpl* db) {
  // The SuperVersion is cached in thread local storage to avoid acquiring
  // mutex when SuperVersion does not change since the last use. When a new
  // SuperVersion is installed, the compaction or flush thread cleans up
  // cached SuperVersion in all existing thread local storage. To avoid
  // acquiring mutex for this operation, we use atomic Swap() on the thread
  // local pointer to guarantee exclusive access. If the thread local pointer
  // is being used while a new SuperVersion is installed, the cached
  // SuperVersion can become stale. In that case, the background thread would
  // have swapped in kSVObsolete. We re-check the value at when returning
  // SuperVersion back to thread local, with an atomic compare and swap.
  // The superversion will need to be released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete) {
    RecordTick(ioptions_.stats, NUMBER_SUPERVERSION_ACQUIRES);
    db->mutex()->Lock();
    sv = super_version_->Ref();
    db->mutex()->Unlock();
  }
  assert(sv != nullptr);
  return sv;
}

bool ColumnFamilyData::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call (after
    // thread local Swap() at the beginning and before CompareAndSwap()).
    // This means the SuperVersion it holds is obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

void ColumnFamilyData::InstallSuperVersion(
    SuperVersionContext* sv_context, InstrumentedMutex* db_mutex,
    std::optional<std::shared_ptr<SeqnoToTimeMapping>>
        new_seqno_to_time_mapping) {
  db_mutex->AssertHeld();

  SuperVersion* new_superversion = sv_context->new_superversion.release();
  new_superversion->mutable_cf_options = GetLatestMutableCFOptions();
  new_superversion->Init(this, mem_, imm_.current(), current_,
                         new_seqno_to_time_mapping.has_value()
                             ? std::move(new_seqno_to_time_mapping.value())
                         : super_version_
                             ? super_version_->ShareSeqnoToTimeMapping()
                             : nullptr);
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  if (old_superversion == nullptr || old_superversion->current != current() ||
      old_superversion->mem != mem_ ||
      old_superversion->imm != imm_.current()) {
    // Should not recalculate slow down condition if nothing has changed, since
    // currently RecalculateWriteStallConditions() treats it as further slowing
    // down is needed.
    super_version_->write_stall_condition =
        RecalculateWriteStallConditions(new_superversion->mutable_cf_options);
  } else {
    super_version_->write_stall_condition =
        old_superversion->write_stall_condition;
  }
  if (old_superversion != nullptr) {
    // Reset SuperVersions cached in thread local storage.
    // This should be done before old_superversion->Unref(). That's to ensure
    // that local_sv_ never holds the last reference to SuperVersion, since
    // it has no means to safely do SuperVersion cleanup.
    ResetThreadLocalSuperVersions();

    if (old_superversion->mutable_cf_options.write_buffer_size !=
        new_superversion->mutable_cf_options.write_buffer_size) {
      mem_->UpdateWriteBufferSize(
          new_superversion->mutable_cf_options.write_buffer_size);
    }
    if (old_superversion->write_stall_condition !=
        new_superversion->write_stall_condition) {
      sv_context->PushWriteStallNotification(
          old_superversion->write_stall_condition,
          new_superversion->write_stall_condition, GetName(), &ioptions());
    }
    if (old_superversion->Unref()) {
      old_superversion->Cleanup();
      sv_context->superversions_to_free.push_back(old_superversion);
    }
  }
  ++super_version_number_;
  super_version_->version_number = super_version_number_;
}

void ColumnFamilyData::ResetThreadLocalSuperVersions() {
  autovector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    bool was_last_ref __attribute__((__unused__));
    was_last_ref = sv->Unref();
    // sv couldn't have been the last reference because
    // ResetThreadLocalSuperVersions() is called before
    // unref'ing super_version_.
    assert(!was_last_ref);
  }
}

Status ColumnFamilyData::ValidateOptions(
    const DBOptions& db_options, const ColumnFamilyOptions& cf_options) {
  Status s;
  s = CheckCompressionSupported(cf_options);
  if (s.ok() && db_options.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(cf_options);
  }
  if (s.ok() && db_options.unordered_write &&
      cf_options.max_successive_merges != 0) {
    s = Status::InvalidArgument(
        "max_successive_merges > 0 is incompatible with unordered_write");
  }
  if (s.ok()) {
    s = CheckCFPathsSupported(db_options, cf_options);
  }
  if (!s.ok()) {
    return s;
  }

  if (cf_options.ttl > 0 && cf_options.ttl != kDefaultTtl) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
    }
  }

  if (cf_options.periodic_compaction_seconds > 0 &&
      cf_options.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
    }
  }

  const auto* ucmp = cf_options.comparator;
  assert(ucmp);
  if (cf_options.enable_blob_direct_write) {
    if (db_options.enable_pipelined_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support pipelined writes.");
    }
    if (db_options.allow_concurrent_memtable_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support concurrent memtable writes.");
    }
    if (db_options.unordered_write) {
      return Status::NotSupported(
          "Blob direct write v1 does not support unordered writes.");
    }
    if (db_options.two_write_queues) {
      return Status::NotSupported(
          "Blob direct write v1 does not support two write queues.");
    }
    if (cf_options.experimental_mempurge_threshold > 0.0) {
      return Status::NotSupported(
          "Blob direct write does not support MemPurge.");
    }
    if (ucmp->timestamp_size() > 0) {
      return Status::NotSupported(
          "Blob direct write does not support user-defined timestamps.");
    }
  }
  if (ucmp->timestamp_size() > 0 &&
      !cf_options.persist_user_defined_timestamps) {
    if (db_options.atomic_flush) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps feature is not supported"
          "in combination with atomic flush.");
    }
    if (db_options.allow_concurrent_memtable_write) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps feature is not supported"
          " in combination with concurrent memtable write.");
    }
    const char* comparator_name = cf_options.comparator->Name();
    size_t name_size = strlen(comparator_name);
    const char* suffix = ".u64ts";
    size_t suffix_size = strlen(suffix);
    if (name_size <= suffix_size ||
        strcmp(comparator_name + name_size - suffix_size, suffix) != 0) {
      return Status::NotSupported(
          "Not persisting user-defined timestamps"
          "feature only support user-defined timestamps formatted as "
          "uint64_t.");
    }
  }

  if (cf_options.enable_blob_garbage_collection) {
    if (cf_options.blob_garbage_collection_age_cutoff < 0.0 ||
        cf_options.blob_garbage_collection_age_cutoff > 1.0) {
      return Status::InvalidArgument(
          "The age cutoff for blob garbage collection should be in the range "
          "[0.0, 1.0].");
    }
    if (cf_options.blob_garbage_collection_force_threshold < 0.0 ||
        cf_options.blob_garbage_collection_force_threshold > 1.0) {
      return Status::InvalidArgument(
          "The garbage ratio threshold for forcing blob garbage collection "
          "should be in the range [0.0, 1.0].");
    }
  }

  if (cf_options.read_triggered_compaction_threshold < 0.0 ||
      std::isnan(cf_options.read_triggered_compaction_threshold) ||
      std::isinf(cf_options.read_triggered_compaction_threshold)) {
    return Status::InvalidArgument(
        "read_triggered_compaction_threshold must be >= 0.0 and finite. "
        "Use 0.0 to disable.");
  }

  if (cf_options.compaction_style == kCompactionStyleFIFO &&
      db_options.max_open_files != -1 && cf_options.ttl > 0) {
    return Status::NotSupported(
        "FIFO compaction only supported with max_open_files = -1.");
  }

  if (db_options.open_files_async) {
    // FIFO TTL picker relies on reading the files table properties inside a
    // DB mutex. This can be slow if files are opened asynchronously, so we
    // disable it.
    //
    // TODO: consider blocking fifo compaction until async file open task
    // completes.
    if (cf_options.compaction_style == kCompactionStyleFIFO) {
      return Status::NotSupported(
          "FIFO compaction is not supported with open_files_async = true.");
    }
    // Open files async is not useful if skip_stats_update_on_db_open=true
    // because DB open will still block on IO.
    //
    // TODO: consider moving stats update inside async file open background
    // task.
    if (!db_options.skip_stats_update_on_db_open) {
      return Status::InvalidArgument(
          "open_files_async requires skip_stats_update_on_db_open = true.");
    }
  }

  std::vector<uint32_t> supported{0, 1, 2, 4, 8};
  if (std::find(supported.begin(), supported.end(),
                cf_options.memtable_protection_bytes_per_key) ==
      supported.end()) {
    return Status::NotSupported(
        "Memtable per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }
  if (std::find(supported.begin(), supported.end(),
                cf_options.block_protection_bytes_per_key) == supported.end()) {
    return Status::NotSupported(
        "Block per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }

  if (!cf_options.compaction_options_fifo.file_temperature_age_thresholds
           .empty()) {
    if (cf_options.compaction_style != kCompactionStyleFIFO) {
      return Status::NotSupported(
          "Option file_temperature_age_thresholds only supports FIFO "
          "compaction.");
    } else if (cf_options.num_levels > 1) {
      return Status::NotSupported(
          "Option file_temperature_age_thresholds is only supported when "
          "num_levels = 1.");
    } else {
      const auto& ages =
          cf_options.compaction_options_fifo.file_temperature_age_thresholds;
      assert(ages.size() >= 1);
      // check that age is sorted
      for (size_t i = 0; i < ages.size() - 1; ++i) {
        if (ages[i].age >= ages[i + 1].age) {
          return Status::NotSupported(
              "Option file_temperature_age_thresholds requires elements to be "
              "sorted in increasing order with respect to `age` field.");
        }
      }
    }
  }

  if (cf_options.compaction_style == kCompactionStyleUniversal) {
    int max_read_amp = cf_options.compaction_options_universal.max_read_amp;
    if (max_read_amp < -1) {
      return Status::NotSupported(
          "CompactionOptionsUniversal::max_read_amp should be at least -1.");
    } else if (0 < max_read_amp &&
               max_read_amp < cf_options.level0_file_num_compaction_trigger) {
      return Status::NotSupported(
          "CompactionOptionsUniversal::max_read_amp limits the number of sorted"
          " runs but is smaller than the compaction trigger "
          "level0_file_num_compaction_trigger.");
    }
  }
  return s;
}

Status ColumnFamilyData::SetOptions(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& options_map) {
  ColumnFamilyOptions cf_opts =
      BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
  ConfigOptions config_opts;
  config_opts.mutable_options_only = true;
#ifndef NDEBUG
  if (TEST_allowSetOptionsImmutableInMutable) {
    config_opts.mutable_options_only = false;
  }
#endif
  Status s = GetColumnFamilyOptionsFromMap(config_opts, cf_opts, options_map,
                                           &cf_opts);
  if (s.ok()) {
    // FIXME: we should call SanitizeOptions() too or consolidate it with
    // ValidateOptions().
    s = ValidateOptions(db_opts, cf_opts);
  }
  if (s.ok()) {
    mutable_cf_options_ = MutableCFOptions(cf_opts);
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  }
  return s;
}

Status ColumnFamilyData::AddDirectories(
    std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs) {
  Status s;
  assert(created_dirs != nullptr);
  assert(data_dirs_.empty());
  for (auto& p : ioptions_.cf_paths) {
    auto existing_dir = created_dirs->find(p.path);

    if (existing_dir == created_dirs->end()) {
      std::unique_ptr<FSDirectory> path_directory;
      s = DBImpl::CreateAndNewDirectory(ioptions_.fs.get(), p.path,
                                        &path_directory);
      if (!s.ok()) {
        return s;
      }
      assert(path_directory != nullptr);
      data_dirs_.emplace_back(path_directory.release());
      (*created_dirs)[p.path] = data_dirs_.back();
    } else {
      data_dirs_.emplace_back(existing_dir->second);
    }
  }
  assert(data_dirs_.size() == ioptions_.cf_paths.size());
  return s;
}

FSDirectory* ColumnFamilyData::GetDataDir(size_t path_id) const {
  if (data_dirs_.empty()) {
    return nullptr;
  }

  assert(path_id < data_dirs_.size());
  return data_dirs_[path_id].get();
}

void ColumnFamilyData::SetFlushSkipReschedule() {
  const Comparator* ucmp = user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0 || ioptions_.persist_user_defined_timestamps) {
    return;
  }
  flush_skip_reschedule_.store(true);
}

bool ColumnFamilyData::GetAndClearFlushSkipReschedule() {
  return flush_skip_reschedule_.exchange(false);
}

bool ColumnFamilyData::ShouldPostponeFlushToRetainUDT(
    uint64_t max_memtable_id) {
  const Comparator* ucmp = user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0 || ioptions_.persist_user_defined_timestamps) {
    return false;
  }
  // If users set the `persist_user_defined_timestamps` flag to false, they
  // should also set the `full_history_ts_low` flag to indicate the range of
  // user-defined timestamps to retain in memory. Otherwise, we do not
  // explicitly postpone flush to retain UDTs.
  const std::string& full_history_ts_low = GetFullHistoryTsLow();
  if (full_history_ts_low.empty()) {
    return false;
  }
  for (const Slice& table_newest_udt :
       imm()->GetTablesNewestUDT(max_memtable_id)) {
    if (table_newest_udt.empty()) {
      continue;
    }
    assert(table_newest_udt.size() == full_history_ts_low.size());
    // Checking the newest UDT contained in MemTable with ascending ID up to
    // `max_memtable_id`. Return immediately on finding the first MemTable that
    // needs postponing.
    if (ucmp->CompareTimestamp(table_newest_udt, full_history_ts_low) >= 0) {
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::RecoverEpochNumbers() {
  assert(current_);
  auto* vstorage = current_->storage_info();
  assert(vstorage);
  vstorage->RecoverEpochNumbers(this);
}

ColumnFamilySet::ColumnFamilySet(
    const std::string& dbname, const ImmutableDBOptions* db_options,
    const FileOptions& file_options, Cache* table_cache,
    WriteBufferManager* _write_buffer_manager,
    WriteController* _write_controller,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, bool fast_sst_open)
    : max_column_family_(0),
      file_options_(file_options),
      dummy_cfd_(new ColumnFamilyData(
          ColumnFamilyData::kDummyColumnFamilyDataId, "", nullptr, nullptr,
          nullptr, ColumnFamilyOptions(), *db_options, &file_options_, nullptr,
          block_cache_tracer, io_tracer, db_id, db_session_id,
          /*read_only=*/true, /*options_are_sanitized=*/false)),
      default_cfd_cache_(nullptr),
      db_name_(dbname),
      db_options_(db_options),
      table_cache_(table_cache),
      write_buffer_manager_(_write_buffer_manager),
      write_controller_(_write_controller),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_id_(db_id),
      db_session_id_(db_session_id),
      fast_sst_open_(fast_sst_open) {
  // initialize linked list
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

ColumnFamilySet::~ColumnFamilySet() {
  while (column_family_data_.size() > 0) {
    // cfd destructor will delete itself from column_family_data_
    auto cfd = column_family_data_.begin()->second;
    bool last_ref __attribute__((__unused__));
    last_ref = cfd->UnrefAndTryDelete();
    assert(last_ref);
  }
  bool dummy_last_ref __attribute__((__unused__));
  dummy_last_ref = dummy_cfd_->UnrefAndTryDelete();
  assert(dummy_last_ref);
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  assert(default_cfd_cache_ != nullptr);
  return default_cfd_cache_;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(
    const std::string& name) const {
  auto cfd_iter = column_families_.find(name);
  if (cfd_iter != column_families_.end()) {
    auto cfd = GetColumnFamily(cfd_iter->second);
    assert(cfd != nullptr);
    return cfd;
  } else {
    return nullptr;
  }
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

uint32_t ColumnFamilySet::GetMaxColumnFamily() { return max_column_family_; }

void ColumnFamilySet::UpdateMaxColumnFamily(uint32_t new_max_column_family) {
  max_column_family_ = std::max(new_max_column_family, max_column_family_);
}

size_t ColumnFamilySet::NumberOfColumnFamilies() const {
  return column_families_.size();
}

void ColumnFamilySet::RefreshFlushableMemAccounting() {
  if (flush_initiator_ == nullptr || write_buffer_manager_ == nullptr ||
      !write_buffer_manager_->ShouldTrackFlushInitiator()) {
    return;
  }

  if (bulk_flushable_mem_accounting_depth_ > 0) {
    bulk_flushable_mem_accounting_dirty_ = true;
    flush_initiator_->InvalidateLargestFlushableCFMem();
    return;
  }

  // Keep candidate readers from consuming a mixture of the previous snapshot
  // and the values published by this rebuild.
  flush_initiator_->InvalidateLargestFlushableCFMem();
  constexpr int kMaxRefreshAttempts = 3;
  for (int attempt = 0; attempt < kMaxRefreshAttempts; ++attempt) {
    const uint64_t update_seq =
        flush_initiator_->GetFlushableMemUpdateSequence();
    bool has_mutable_mem = false;
    bool has_ineligible_cf = false;
    size_t largest = 0;
    size_t waiting_immutable_mem = 0;
    for (auto cfd : *this) {
      if (cfd->IsDropped() || !cfd->initialized()) {
        continue;
      }
      const bool mutable_not_empty = !cfd->mem()->IsEmpty();
      if (mutable_not_empty) {
        cfd->mem()->RefreshWBMTracking();
      }
      const bool has_mutable =
          mutable_not_empty && cfd->mem()->IsWBMTrackingActive();
      has_mutable_mem = has_mutable_mem || has_mutable;
      if (flush_initiator_->UsesTotalMutableMem()) {
        waiting_immutable_mem +=
            cfd->imm()->WBMTrackedUnstartedMemTablesMemoryUsage();
        continue;
      }
      if (cfd->imm()->IsFlushPendingOrRunning()) {
        has_ineligible_cf = true;
        continue;
      }
      const size_t mutable_mem =
          has_mutable ? cfd->mem()->WBMTrackedMemoryUsage() : 0;
      const size_t immutable_mem =
          cfd->imm()->WBMTrackedUnflushedMemTablesMemoryUsage();
      waiting_immutable_mem += immutable_mem;
      largest = std::max(largest, mutable_mem + immutable_mem);
    }
    if (flush_initiator_->UsesTotalMutableMem()) {
      flush_initiator_->SetHasIneligibleCF(false);
      flush_initiator_->SetHasFlushableCF(has_mutable_mem ||
                                          waiting_immutable_mem > 0);
      if (flush_initiator_->TrySetLargestFlushableCFMem(
              0, waiting_immutable_mem, update_seq)) {
        write_buffer_manager_->NotifyFlushInitiatorChanged();
        return;
      }
      continue;
    }
    flush_initiator_->SetHasIneligibleCF(has_ineligible_cf);
    flush_initiator_->SetHasFlushableCF(largest > 0);
    if (flush_initiator_->TrySetLargestFlushableCFMem(
            largest, waiting_immutable_mem, update_seq)) {
      write_buffer_manager_->NotifyFlushInitiatorChanged();
      return;
    }
  }
  flush_initiator_->InvalidateLargestFlushableCFMem();
}

void ColumnFamilySet::BeginBulkFlushableMemAccountingUpdate() {
  ++bulk_flushable_mem_accounting_depth_;
  if (bulk_flushable_mem_accounting_depth_ == 1) {
    bulk_flushable_mem_accounting_dirty_ = true;
    if (flush_initiator_ != nullptr && write_buffer_manager_ != nullptr &&
        write_buffer_manager_->ShouldTrackFlushInitiator()) {
      flush_initiator_->InvalidateLargestFlushableCFMem();
      write_buffer_manager_->NotifyFlushInitiatorChanged();
    }
  }
}

void ColumnFamilySet::EndBulkFlushableMemAccountingUpdate() {
  assert(bulk_flushable_mem_accounting_depth_ > 0);
  --bulk_flushable_mem_accounting_depth_;
  if (bulk_flushable_mem_accounting_depth_ == 0 &&
      bulk_flushable_mem_accounting_dirty_) {
    bulk_flushable_mem_accounting_dirty_ = false;
    TEST_SYNC_POINT(
        "ColumnFamilySet::EndBulkFlushableMemAccountingUpdate:Refresh");
    RefreshFlushableMemAccounting();
  }
}

// under a DB mutex AND write thread
ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options, bool read_only,
    bool options_are_sanitized) {
  assert(column_families_.find(name) == column_families_.end());
  ColumnFamilyData* new_cfd = new ColumnFamilyData(
      id, name, dummy_versions, table_cache_, write_buffer_manager_, options,
      *db_options_, &file_options_, this, block_cache_tracer_, io_tracer_,
      db_id_, db_session_id_, read_only, options_are_sanitized, fast_sst_open_);
  column_families_.insert({name, id});
  column_family_data_.insert({id, new_cfd});
  auto ucmp = new_cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  running_ts_sz_.insert({id, ts_sz});
  if (ts_sz > 0) {
    ts_sz_for_record_.insert({id, ts_sz});
  }
  max_column_family_ = std::max(max_column_family_, id);
  // add to linked list
  new_cfd->next_ = dummy_cfd_;
  auto prev = dummy_cfd_->prev_;
  new_cfd->prev_ = prev;
  prev->next_ = new_cfd;
  dummy_cfd_->prev_ = new_cfd;
  if (id == 0) {
    default_cfd_cache_ = new_cfd;
  }
  return new_cfd;
}

// under a DB mutex AND from a write thread
void ColumnFamilySet::RemoveColumnFamily(ColumnFamilyData* cfd) {
  uint32_t cf_id = cfd->GetID();
  auto cfd_iter = column_family_data_.find(cf_id);
  assert(cfd_iter != column_family_data_.end());
  column_family_data_.erase(cfd_iter);
  column_families_.erase(cfd->GetName());
  running_ts_sz_.erase(cf_id);
  ts_sz_for_record_.erase(cf_id);
  if (bulk_flushable_mem_accounting_depth_ > 0) {
    bulk_flushable_mem_accounting_dirty_ = true;
    if (flush_initiator_ != nullptr) {
      flush_initiator_->InvalidateLargestFlushableCFMem();
    }
    return;
  }
  TEST_SYNC_POINT("ColumnFamilySet::RemoveColumnFamily:Refresh");
  RefreshFlushableMemAccounting();
}

// under a DB mutex OR from a write thread
bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  if (column_family_id == 0) {
    // optimization for common case
    current_ = column_family_set_->GetDefault();
  } else {
    current_ = column_family_set_->GetColumnFamily(column_family_id);
  }
  handle_.SetCFD(current_);
  return current_ != nullptr;
}

uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  assert(current_ != nullptr);
  return current_->GetLogNumber();
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  assert(current_ != nullptr);
  return current_->mem();
}

ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  assert(current_ != nullptr);
  return &handle_;
}

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family) {
  uint32_t column_family_id = 0;
  if (column_family != nullptr) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    column_family_id = cfh->GetID();
  }
  return column_family_id;
}

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family) {
  if (column_family != nullptr) {
    return column_family->GetComparator();
  }
  return nullptr;
}

const ImmutableOptions& GetImmutableOptions(ColumnFamilyHandle* column_family) {
  assert(column_family);

  ColumnFamilyHandleImpl* const handle =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(handle);

  const ColumnFamilyData* const cfd = handle->cfd();
  assert(cfd);

  return cfd->ioptions();
}

}  // namespace ROCKSDB_NAMESPACE
