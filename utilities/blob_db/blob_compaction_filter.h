//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <unordered_set>

#include "db/blob/blob_index.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/compaction_filter.h"
#include "utilities/blob_db/blob_db_gc_stats.h"
#include "utilities/blob_db/blob_db_impl.h"
#include "utilities/compaction_filters/layered_compaction_filter_base.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
namespace blob_db {

struct BlobCompactionContext {
  BlobDBImpl* blob_db_impl = nullptr;
  uint64_t next_file_number = 0;
  std::unordered_set<uint64_t> current_blob_files;
};

struct BlobCompactionContextGC {
  uint64_t cutoff_file_number = 0;
};

// Compaction filter that deletes expired blob indexes from the base DB.
// Comes into two varieties, one for the non-GC case and one for the GC case.
class BlobIndexCompactionFilterBase : public LayeredCompactionFilterBase {
 public:
  BlobIndexCompactionFilterBase(
      BlobCompactionContext&& _context,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : LayeredCompactionFilterBase(_user_comp_filter,
                                    std::move(_user_comp_filter_from_factory)),
        context_(std::move(_context)),
        current_time_(current_time),
        statistics_(stats) {}

  ~BlobIndexCompactionFilterBase() override;

  MergeOrder GetMergeOrder() const override {
    return user_comp_filter() ? user_comp_filter()->GetMergeOrder()
                              : MergeOrder::kFilterBeforeMerge;
  }

  // Filter expired blob indexes regardless of snapshots.
  bool IgnoreSnapshots() const override { return true; }

  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& value, std::string* new_value,
                    std::string* skip_until) const override;

  bool IsStackedBlobDbInternalCompactionFilter() const override { return true; }

 protected:
  bool IsBlobFileOpened() const;
  virtual bool OpenNewBlobFileIfNeeded() const;
  bool ReadBlobFromOldFile(const Slice& key, const BlobIndex& blob_index,
                           PinnableSlice* blob) const;
  bool WriteBlobToNewFile(const Slice& key, const Slice& blob,
                          uint64_t* new_blob_file_number,
                          uint64_t* new_blob_offset) const;
  bool CloseAndRegisterNewBlobFileIfNeeded() const;
  bool CloseAndRegisterNewBlobFile() const;

  Statistics* statistics() const { return statistics_; }
  const BlobCompactionContext& context() const { return context_; }

 private:
  Decision HandleValueChange(const Slice& key, std::string* new_value) const;

 private:
  BlobCompactionContext context_;
  const uint64_t current_time_;
  Statistics* statistics_;

  mutable std::shared_ptr<BlobFile> blob_file_;
  mutable std::shared_ptr<BlobLogWriter> writer_;

  // It is safe to not using std::atomic since the compaction filter, created
  // from a compaction filter factroy, will not be called from multiple threads.
  mutable uint64_t expired_count_ = 0;
  mutable uint64_t expired_size_ = 0;
  mutable uint64_t evicted_count_ = 0;
  mutable uint64_t evicted_size_ = 0;
};

class BlobIndexCompactionFilter : public BlobIndexCompactionFilterBase {
 public:
  BlobIndexCompactionFilter(
      BlobCompactionContext&& _context,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : BlobIndexCompactionFilterBase(std::move(_context), _user_comp_filter,
                                      std::move(_user_comp_filter_from_factory),
                                      current_time, stats) {}

  const char* Name() const override { return "BlobIndexCompactionFilter"; }
};

class BlobIndexCompactionFilterGC : public BlobIndexCompactionFilterBase {
 public:
  BlobIndexCompactionFilterGC(
      BlobCompactionContext&& _context, BlobCompactionContextGC&& context_gc,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : BlobIndexCompactionFilterBase(std::move(_context), _user_comp_filter,
                                      std::move(_user_comp_filter_from_factory),
                                      current_time, stats),
        context_gc_(std::move(context_gc)) {}

  ~BlobIndexCompactionFilterGC() override;

  const char* Name() const override { return "BlobIndexCompactionFilterGC"; }

  BlobDecision PrepareBlobOutput(const Slice& key, const Slice& existing_value,
                                 std::string* new_value) const override;

 private:
  bool OpenNewBlobFileIfNeeded() const override;

 private:
  BlobCompactionContextGC context_gc_;
  mutable BlobDBGarbageCollectionStats gc_stats_;
};

// Compaction filter factory; similarly to the filters above, it comes
// in two flavors, one that creates filters that support GC, and one
// that creates non-GC filters.
class BlobIndexCompactionFilterFactoryBase : public CompactionFilterFactory {
 public:
  BlobIndexCompactionFilterFactoryBase(BlobDBImpl* _blob_db_impl,
                                       SystemClock* _clock,
                                       const ColumnFamilyOptions& _cf_options,
                                       Statistics* _statistics)
      : blob_db_impl_(_blob_db_impl),
        clock_(_clock),
        statistics_(_statistics),
        user_comp_filter_(_cf_options.compaction_filter),
        user_comp_filter_factory_(_cf_options.compaction_filter_factory) {}

 protected:
  std::unique_ptr<CompactionFilter> CreateUserCompactionFilterFromFactory(
      const CompactionFilter::Context& context) const;

  BlobDBImpl* blob_db_impl() const { return blob_db_impl_; }
  SystemClock* clock() const { return clock_; }
  Statistics* statistics() const { return statistics_; }
  const CompactionFilter* user_comp_filter() const { return user_comp_filter_; }

 private:
  BlobDBImpl* blob_db_impl_;
  SystemClock* clock_;
  Statistics* statistics_;
  const CompactionFilter* user_comp_filter_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class BlobIndexCompactionFilterFactory
    : public BlobIndexCompactionFilterFactoryBase {
 public:
  BlobIndexCompactionFilterFactory(BlobDBImpl* _blob_db_impl,
                                   SystemClock* _clock,
                                   const ColumnFamilyOptions& _cf_options,
                                   Statistics* _statistics)
      : BlobIndexCompactionFilterFactoryBase(_blob_db_impl, _clock, _cf_options,
                                             _statistics) {}

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
};

class BlobIndexCompactionFilterFactoryGC
    : public BlobIndexCompactionFilterFactoryBase {
 public:
  BlobIndexCompactionFilterFactoryGC(BlobDBImpl* _blob_db_impl,
                                     SystemClock* _clock,
                                     const ColumnFamilyOptions& _cf_options,
                                     Statistics* _statistics)
      : BlobIndexCompactionFilterFactoryBase(_blob_db_impl, _clock, _cf_options,
                                             _statistics) {}

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactoryGC";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
};

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
