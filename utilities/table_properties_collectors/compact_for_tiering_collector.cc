//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_for_tiering_collector.h"

#include <sstream>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

CompactForTieringCollector::CompactForTieringCollector(
    SequenceNumber last_level_inclusive_max_seqno_threshold,
    double compaction_trigger_ratio, bool collect_data_age_stats)
    : last_level_inclusive_max_seqno_threshold_(
          last_level_inclusive_max_seqno_threshold),
      compaction_trigger_ratio_(compaction_trigger_ratio),
      collect_data_age_stats_(collect_data_age_stats) {
  assert(last_level_inclusive_max_seqno_threshold_ != kMaxSequenceNumber);
  // TODO(yuzhangyu): implement collect the data age stats.
  (void)collect_data_age_stats_;
}

Status CompactForTieringCollector::AddUserKey(const Slice& /*key*/,
                                              const Slice& value,
                                              EntryType type,
                                              SequenceNumber seq,
                                              uint64_t /*file_size*/) {
  SequenceNumber seq_for_check = seq;
  if (type == kEntryTimedPut) {
    seq_for_check = ParsePackedValueForSeqno(value);
  }
  if (seq_for_check < last_level_inclusive_max_seqno_threshold_) {
    last_level_eligible_entries_counter_++;
  }
  total_entries_counter_ += 1;
  return Status::OK();
}

Status CompactForTieringCollector::Finish(UserCollectedProperties* properties) {
  assert(!finish_called_);
  assert(compaction_trigger_ratio_ > 0);
  if (last_level_eligible_entries_counter_ >=
      compaction_trigger_ratio_ * total_entries_counter_) {
    assert(compaction_trigger_ratio_ <= 1);
    need_compaction_ = true;
  }
  if (last_level_eligible_entries_counter_ > 0) {
    *properties = UserCollectedProperties{
        {kNumEligibleLastLevelEntriesPropertyName,
         std::to_string(last_level_eligible_entries_counter_)},
    };
  }
  finish_called_ = true;
  return Status::OK();
}

UserCollectedProperties CompactForTieringCollector::GetReadableProperties()
    const {
  return UserCollectedProperties{
      {kNumEligibleLastLevelEntriesPropertyName,
       std::to_string(last_level_eligible_entries_counter_)},
  };
}

bool CompactForTieringCollector::NeedCompact() const {
  return need_compaction_;
}

void CompactForTieringCollector::Reset() {
  last_level_eligible_entries_counter_ = 0;
  total_entries_counter_ = 0;
  finish_called_ = false;
  need_compaction_ = false;
}

TablePropertiesCollector*
CompactForTieringCollectorFactory::CreateTablePropertiesCollector(
    TablePropertiesCollectorFactory::Context context) {
  double compaction_trigger_ratio = GetCompactionTriggerRatio();
  if (compaction_trigger_ratio <= 0 ||
      context.level_at_creation == context.num_levels - 1 ||
      context.last_level_inclusive_max_seqno_threshold == kMaxSequenceNumber) {
    return nullptr;
  }
  // TODO(yuzhangyu): pass actual value.
  return new CompactForTieringCollector(
      context.last_level_inclusive_max_seqno_threshold,
      compaction_trigger_ratio,
      /*collect_data_age_stats*/ false);
}

static std::unordered_map<std::string, OptionTypeInfo>
    on_compact_for_tiering_type_info = {
        {"compaction_trigger_ratio",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactForTieringCollectorFactory*>(addr);
            factory->SetCompactionTriggerRatio(ParseDouble(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactForTieringCollectorFactory*>(addr);
            *value = std::to_string(factory->GetCompactionTriggerRatio());
            return Status::OK();
          },
          nullptr}},

};

CompactForTieringCollectorFactory::CompactForTieringCollectorFactory(
    double compaction_trigger_ratio)
    : compaction_trigger_ratio_(compaction_trigger_ratio) {
  RegisterOptions("", this, &on_compact_for_tiering_type_info);
}

std::string CompactForTieringCollectorFactory::ToString() const {
  std::ostringstream cfg;
  cfg << Name()
      << ", compaction trigger ratio:" << compaction_trigger_ratio_.load()
      << std::endl;
  return cfg.str();
}

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(double compaction_trigger_ratio) {
  return std::make_shared<CompactForTieringCollectorFactory>(
      compaction_trigger_ratio);
}

Status GetDataCollectionUnixWriteTimeInfoForFile(
    const std::shared_ptr<const TableProperties>& /* table_properties */,
    std::unique_ptr<DataCollectionUnixWriteTimeInfo>* /* file_info */) {
  return Status::NotSupported();
}

Status GetDataCollectionUnixWriteTimeInfoForLevels(
    const std::vector<std::unique_ptr<
        TablePropertiesCollection>>& /* levels_table_properties */,
    std::vector<
        std::unique_ptr<DataCollectionUnixWriteTimeInfo>>* /* levels_info */) {
  return Status::NotSupported();
}

}  // namespace ROCKSDB_NAMESPACE
