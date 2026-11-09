//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

#include <memory>
#include <sstream>

#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

CompactOnDeletionCollector::CompactOnDeletionCollector(
    size_t sliding_window_size, size_t deletion_trigger, double deletion_ratio,
    uint64_t min_file_size)
    : bucket_size_((sliding_window_size + kNumBuckets - 1) / kNumBuckets),
      current_bucket_(0),
      num_keys_in_current_bucket_(0),
      num_deletions_in_observation_window_(0),
      deletion_trigger_(deletion_trigger),
      deletion_ratio_(deletion_ratio),
      min_file_size_(min_file_size),
      cur_file_size_(0),
      max_deletion_in_window_(0),
      deletion_ratio_enabled_(deletion_ratio > 0 && deletion_ratio <= 1),
      need_compaction_(false) {
  memset(num_deletions_in_buckets_, 0, sizeof(size_t) * kNumBuckets);
}

// AddUserKey() will be called when a new key/value pair is inserted into the
// table.
// @params key    the user key that is inserted into the table.
// @params value  the value that is inserted into the table.
// @params file_size  file size up to now
Status CompactOnDeletionCollector::AddUserKey(const Slice& /*key*/,
                                              const Slice& /*value*/,
                                              EntryType type,
                                              SequenceNumber /*seq*/,
                                              uint64_t file_size) {
  assert(!finished_);
  if (!bucket_size_ && !deletion_ratio_enabled_) {
    // This collector is effectively disabled
    return Status::OK();
  }

  if (need_compaction_) {
    // If the output file already needs to be compacted, skip the check.
    return Status::OK();
  }

  const bool is_delete = (type == kEntryDelete || type == kEntrySingleDelete ||
                          type == kEntryDeleteWithTimestamp);
  if (deletion_ratio_enabled_) {
    total_entries_++;
    if (is_delete) {
      deletion_entries_++;
    }
    cur_file_size_ = file_size;
  }

  if (bucket_size_) {
    if (num_keys_in_current_bucket_ == bucket_size_) {
      // When the current bucket is full, advance the cursor of the
      // ring buffer to the next bucket.
      current_bucket_ = (current_bucket_ + 1) % kNumBuckets;

      // Update the current count of observed deletion keys by excluding
      // the number of deletion keys in the oldest bucket in the
      // observation window.
      assert(num_deletions_in_observation_window_ >=
             num_deletions_in_buckets_[current_bucket_]);
      num_deletions_in_observation_window_ -=
          num_deletions_in_buckets_[current_bucket_];
      num_deletions_in_buckets_[current_bucket_] = 0;
      num_keys_in_current_bucket_ = 0;
    }

    num_keys_in_current_bucket_++;
    if (is_delete) {
      num_deletions_in_observation_window_++;
      num_deletions_in_buckets_[current_bucket_]++;
      if (num_deletions_in_observation_window_ >= max_deletion_in_window_) {
        max_deletion_in_window_ = num_deletions_in_observation_window_;
      }
    }

    // The file may qualify for compaction based on file size constraints,
    // even if max_deletion_in_window_ is not updated.
    if (max_deletion_in_window_ >= deletion_trigger_ &&
        file_size >= min_file_size_) {
      need_compaction_ = true;
    }
  }

  return Status::OK();
}

Status CompactOnDeletionCollector::Finish(
    UserCollectedProperties* /*properties*/) {
  if (!need_compaction_ && deletion_ratio_enabled_ && total_entries_ > 0 &&
      cur_file_size_ >= min_file_size_) {
    double ratio = static_cast<double>(deletion_entries_) / total_entries_;
    need_compaction_ = ratio >= deletion_ratio_;
  }
  finished_ = true;
  return Status::OK();
}
static std::unordered_map<std::string, OptionTypeInfo>
    on_deletion_collector_type_info = {
        {"window_size",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactOnDeletionCollectorFactory*>(addr);
            factory->SetWindowSize(ParseSizeT(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactOnDeletionCollectorFactory*>(addr);
            *value = std::to_string(factory->GetWindowSize());
            return Status::OK();
          },
          nullptr}},
        {"deletion_trigger",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactOnDeletionCollectorFactory*>(addr);
            factory->SetDeletionTrigger(ParseSizeT(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactOnDeletionCollectorFactory*>(addr);
            *value = std::to_string(factory->GetDeletionTrigger());
            return Status::OK();
          },
          nullptr}},
        {"deletion_ratio",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactOnDeletionCollectorFactory*>(addr);
            factory->SetDeletionRatio(ParseDouble(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactOnDeletionCollectorFactory*>(addr);
            *value = std::to_string(factory->GetDeletionRatio());
            return Status::OK();
          },
          nullptr}},
        {"min_file_size",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactOnDeletionCollectorFactory*>(addr);
            factory->SetMinFileSize(ParseUint64(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactOnDeletionCollectorFactory*>(addr);
            *value = std::to_string(factory->GetMinFileSize());
            return Status::OK();
          },
          nullptr}},

};

CompactOnDeletionCollectorFactory::CompactOnDeletionCollectorFactory(
    size_t sliding_window_size, size_t deletion_trigger, double deletion_ratio,
    uint64_t min_file_size)
    : sliding_window_size_(sliding_window_size),
      deletion_trigger_(deletion_trigger),
      deletion_ratio_(deletion_ratio),
      min_file_size_(min_file_size) {
  RegisterOptions("", this, &on_deletion_collector_type_info);
}

TablePropertiesCollector*
CompactOnDeletionCollectorFactory::CreateTablePropertiesCollector(
    TablePropertiesCollectorFactory::Context /*context*/) {
  return new CompactOnDeletionCollector(
      sliding_window_size_.load(), deletion_trigger_.load(),
      deletion_ratio_.load(), min_file_size_.load());
}

std::string CompactOnDeletionCollectorFactory::ToString() const {
  std::ostringstream cfg;
  cfg << Name() << " (Sliding window size = " << sliding_window_size_.load()
      << " Deletion trigger = " << deletion_trigger_.load()
      << " Deletion ratio = " << deletion_ratio_.load() << ')';
  return cfg.str();
}

std::shared_ptr<CompactOnDeletionCollectorFactory>
NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                     size_t deletion_trigger,
                                     double deletion_ratio,
                                     uint64_t min_file_size) {
  return std::make_shared<CompactOnDeletionCollectorFactory>(
      sliding_window_size, deletion_trigger, deletion_ratio, min_file_size);
}

namespace {
static int RegisterTablePropertiesCollectorFactories(
    ObjectLibrary& library, const std::string& /*arg*/) {
  library.AddFactory<TablePropertiesCollectorFactory>(
      CompactOnDeletionCollectorFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<TablePropertiesCollectorFactory>* guard,
         std::string* /* errmsg */) {
        // By default, create a CompactionOnDeletionCollector that is disabled.
        // Users will need to provide configuration parameters or call the
        // corresponding Setter to enable the factory.
        guard->reset(new CompactOnDeletionCollectorFactory(0, 0, 0));
        return guard->get();
      });
  library.AddFactory<TablePropertiesCollectorFactory>(
      CompactForTieringCollectorFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<TablePropertiesCollectorFactory>* guard,
         std::string* /* errmsg */) {
        // By default, create a `CompactForTieringCollectorFactory` that is
        // disabled. Users will need to call corresponding setters to enable
        // the factory.
        guard->reset(new CompactForTieringCollectorFactory(0));
        return guard->get();
      });
  return 1;
}
}  // namespace

Status TablePropertiesCollectorFactory::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<TablePropertiesCollectorFactory>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterTablePropertiesCollectorFactories(*(ObjectLibrary::Default().get()),
                                              "");
  });
  return LoadSharedObject<TablePropertiesCollectorFactory>(options, value,
                                                           result);
}

}  // namespace ROCKSDB_NAMESPACE
