//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/table_properties_collectors.h"

namespace ROCKSDB_NAMESPACE {

// A user property collector that marks a SST file as need-compaction when for
// the tiering use case. See documentation for
// `CompactForTieringCollectorFactory`.
class CompactForTieringCollector : public TablePropertiesCollector {
 public:
  static constexpr char kNumEligibleLastLevelEntriesPropertyName[] =
      "rocksdb.eligible.last.level.entries";
  static constexpr char kAverageDataUnixWriteTimePropertyName[] =
      "rocksdb.data.unix.write.time.average";
  static constexpr char kMaxDataUnixWriteTimePropertyName[] =
      "rocksdb.data.unix.write.time.max";
  static constexpr char kMinDataUnixWriteTimePropertyName[] =
      "rocksdb.data.unix.write.time.min";
  static constexpr char kNumInfinitelyOldEntriesPropertyName[] =
      "rocksdb.num.infinitely.old.entries";

  CompactForTieringCollector(
      SequenceNumber last_level_inclusive_max_seqno_threshold,
      double compaction_trigger_ratio, bool collect_data_age_stats);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;

  Status Finish(UserCollectedProperties* properties) override;

  UserCollectedProperties GetReadableProperties() const override;

  const char* Name() const override { return "CompactForTieringCollector"; }

  bool NeedCompact() const override;

 private:
  void Reset();

  SequenceNumber last_level_inclusive_max_seqno_threshold_;
  double compaction_trigger_ratio_;
  size_t last_level_eligible_entries_counter_ = 0;
  size_t total_entries_counter_ = 0;
  bool finish_called_ = false;
  bool need_compaction_ = false;
  bool collect_data_age_stats_ = false;
};
}  // namespace ROCKSDB_NAMESPACE
