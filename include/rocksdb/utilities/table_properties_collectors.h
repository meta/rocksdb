//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <memory>

#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// A factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries or the ratio of tombstone
// entries in the whole file >= the specified deletion ratio.
class CompactOnDeletionCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  // A factory of a table property collector that marks a SST
  // file as need-compaction when it observe at least "D" deletion
  // entries in any "N" consecutive entries, or the ratio of tombstone
  // entries >= deletion_ratio for the entire file.
  //
  // @param sliding_window_size "N"
  // @param deletion_trigger "D"
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  // @param min_file_size, a file needs to be at least this size to be marked
  //     for compaction. See comments above
  //     TablePropertiesCollector::AddUserKey() for limitations/inaccuracies on
  //     the file size.
  CompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                    size_t deletion_trigger,
                                    double deletion_ratio,
                                    uint64_t min_file_size = 0);

  ~CompactOnDeletionCollectorFactory() override = default;

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  // Change the value of sliding_window_size "N"
  // Setting it to 0 disables the delete triggered compaction
  void SetWindowSize(size_t sliding_window_size) {
    sliding_window_size_.store(sliding_window_size);
  }
  size_t GetWindowSize() const { return sliding_window_size_.load(); }

  // Change the value of deletion_trigger "D"
  void SetDeletionTrigger(size_t deletion_trigger) {
    deletion_trigger_.store(deletion_trigger);
  }

  size_t GetDeletionTrigger() const { return deletion_trigger_.load(); }
  // Change deletion ratio.
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  void SetDeletionRatio(double deletion_ratio) {
    deletion_ratio_.store(deletion_ratio);
  }

  double GetDeletionRatio() const { return deletion_ratio_.load(); }

  uint64_t GetMinFileSize() const { return min_file_size_.load(); }
  void SetMinFileSize(uint64_t min_file_size) {
    min_file_size_.store(min_file_size);
  }

  static const char* kClassName() { return "CompactOnDeletionCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  std::atomic<size_t> sliding_window_size_;
  std::atomic<size_t> deletion_trigger_;
  std::atomic<double> deletion_ratio_;
  std::atomic<uint64_t> min_file_size_;
};

// Creates a factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries, or the ratio of tombstone
// entries >= deletion_ratio.
//
// @param sliding_window_size "N". Note that this number will be
//     round up to the smallest multiple of 128 that is no less
//     than the specified size.
// @param deletion_trigger "D".  Note that even when "N" is changed,
//     the specified number for "D" will not be changed.
// @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
//     based on deletion ratio. Disabled by default.
std::shared_ptr<CompactOnDeletionCollectorFactory>
NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                     size_t deletion_trigger,
                                     double deletion_ratio = 0,
                                     uint64_t min_file_size = 0);

// A factory of a table property collector that marks a SST file as
// need-compaction when for the tiering use case, it observes, among all the
// data entries, the ratio of entries that are already eligible to be placed on
// the last level but are not yet on the last level is equal to or higher than
// the configured `compaction_trigger_ratio_`.
// 1) Setting the ratio to be equal to or smaller than 0 disables this collector
// 2) Setting the ratio to be within (0, 1] will write the number of
//     observed eligible entries into a user property and marks a file as
//     need-compaction when aforementioned condition is met.
// 3) Setting the ratio to be higher than 1 can be used to just writes the user
//    table property, and not mark any file as need compaction.
// For a column family that does not enable tiering feature, even if an
// effective configuration is provided, this collector is still disabled.
class CompactForTieringCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  // @param compaction_trigger_ratio: the triggering threshold for the ratio of
  // eligible entries to the total number of entries. See class documentation
  // for what entry is eligible.
  CompactForTieringCollectorFactory(double compaction_trigger_ratio);

  ~CompactForTieringCollectorFactory() override = default;

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  void SetCompactionTriggerRatio(double new_ratio) {
    compaction_trigger_ratio_.store(new_ratio);
  }

  double GetCompactionTriggerRatio() const {
    return compaction_trigger_ratio_.load();
  }

  static const char* kClassName() { return "CompactForTieringCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  std::atomic<double> compaction_trigger_ratio_;
};

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(double compaction_trigger_ratio);

// Information for the unix write time of a collection of data. Combined with
// the current unix time, these stats give an overview of how long the data
// have been written to the DB.
// Check `DataCollectionIsEmpty` and `TrackedDataRatio` before interpreting
// the stats.
struct DataCollectionUnixWriteTimeInfo {
  // The minimum write time, a.k.a. the write time of the oldest key.
  uint64_t min_write_time = 0;
  // The maximum write time, a.k.a. the write time of the newest key.
  uint64_t max_write_time = 0;
  // The average write time.
  uint64_t average_write_time = 0;

  // The number of entries that can be considered infinitely old because their
  // sequence number are zeroed out. We know they are old entries but do not
  // know how old exactly. These entries are separately counted and not
  // aggregated in above stats.
  uint64_t num_entries_infinitely_old = 0;

  // The number of entries used to create above min, max, average stats.
  uint64_t num_entries_write_time_aggregated = 0;

  // The number of entries for which write time is untracked.
  uint64_t num_entries_write_time_untracked = 0;

  DataCollectionUnixWriteTimeInfo() {}

  DataCollectionUnixWriteTimeInfo(uint64_t _min_write_time,
                                  uint64_t _max_write_time,
                                  uint64_t _average_write_time,
                                  uint64_t _num_entries_infinitely_old,
                                  uint64_t _num_entries_write_time_aggregated,
                                  uint64_t _num_entries_write_time_untracked)
      : min_write_time(_min_write_time),
        max_write_time(_max_write_time),
        average_write_time(_average_write_time),
        num_entries_infinitely_old(_num_entries_infinitely_old),
        num_entries_write_time_aggregated(_num_entries_write_time_aggregated),
        num_entries_write_time_untracked(_num_entries_write_time_untracked) {}

  // Returns true if the data collection for which this
  // `DataCollectionUnixWriteTimeInfo` is for is empty.
  bool DataCollectionIsEmpty() const {
    return num_entries_infinitely_old == 0 &&
           num_entries_write_time_aggregated == 0 &&
           num_entries_write_time_untracked == 0;
  }

  // The closer the ratio is to 1, the more accurate the stats reflect the
  // actual write time of this collection of data. If this ratio is 0, there is
  // no write time information available. It could be either the data collection
  // is empty, or none of its data has write time info tracked.
  //
  // For a single file, its data either has write time info tracked or not
  // tracked, this ratio would be either 0 or 1. For a level, this ratio
  // reflects what portion of the data has its write time info tracked in this
  // struct. 0 is returned if the level is empty.
  double TrackedDataRatio() const {
    if (DataCollectionIsEmpty()) {
      return 0;
    }
    uint64_t num_entries_write_time_tracked =
        num_entries_infinitely_old + num_entries_write_time_aggregated;
    return num_entries_write_time_tracked /
           static_cast<double>(num_entries_write_time_tracked +
                               num_entries_write_time_untracked);
  }

  // Whether the file or the level has infinitely old data.
  bool HasInfinitelyOldData() const { return num_entries_infinitely_old > 0; }
};

// Given the table properties of a file, return data's unix write time stats
// if available.
Status GetDataCollectionUnixWriteTimeInfoForFile(
    const std::shared_ptr<const TableProperties>& table_properties,
    std::unique_ptr<DataCollectionUnixWriteTimeInfo>* file_info);

// Given the collection of table properties per level, return data unix write
// time stats if available.
Status GetDataCollectionUnixWriteTimeInfoForLevels(
    const std::vector<std::unique_ptr<TablePropertiesCollection>>&
        levels_table_properties,
    std::vector<std::unique_ptr<DataCollectionUnixWriteTimeInfo>>* levels_info);
}  // namespace ROCKSDB_NAMESPACE
