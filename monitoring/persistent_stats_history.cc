// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/persistent_stats_history.h"

#include <cstring>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
// 10 digit seconds timestamp => [Sep 9, 2001 ~ Nov 20, 2286]
const int kNowSecondsStringLength = 10;
// Every release maintains two versions numbers for persistents stats: Current
// format version and compatible format version. Current format version
// designates what type of encoding will be used when writing to stats CF;
// compatible format version designates the minimum format version that
// can decode the stats CF encoded using the current format version.
const uint64_t kStatsCFCurrentFormatVersion = 1;
const uint64_t kStatsCFCompatibleFormatVersion = 1;

Status DecodePersistentStatsVersionNumber(DBImpl* db, StatsVersionKeyType type,
                                          uint64_t* version_number) {
  if (type >= StatsVersionKeyType::kKeyTypeMax) {
    return Status::InvalidArgument("Invalid stats version key type provided");
  }
  std::string key;
  if (type == StatsVersionKeyType::kFormatVersion) {
    key = kFormatVersionKeyString;
  } else if (type == StatsVersionKeyType::kCompatibleVersion) {
    key = kCompatibleVersionKeyString;
  }

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions options;
  options.verify_checksums = true;
  std::string result;
  Status s = db->Get(options, db->PersistentStatsColumnFamily(), key, &result);
  if (!s.ok() || result.empty()) {
    return Status::NotFound("Persistent stats version key " + key +
                            " not found.");
  }

  // read version_number but do nothing in current version
  *version_number = ParseUint64(result);
  return Status::OK();
}

int EncodePersistentStatsKey(uint64_t now_seconds, const std::string& key,
                             int size, char* buf) {
  char timestamp[kNowSecondsStringLength + 1];
  // make time stamp string equal in length to allow sorting by time
  snprintf(timestamp, sizeof(timestamp), "%010d",
           static_cast<int>(now_seconds));
  timestamp[kNowSecondsStringLength] = '\0';
  return snprintf(buf, size, "%s#%s", timestamp, key.c_str());
}

void OptimizeForPersistentStats(ColumnFamilyOptions* cfo) {
  cfo->write_buffer_size = 2 << 20;
  cfo->target_file_size_base = 2 * 1048576;
  cfo->max_bytes_for_level_base = 10 * 1048576;
  cfo->soft_pending_compaction_bytes_limit = 256 * 1048576;
  cfo->hard_pending_compaction_bytes_limit = 1073741824ul;
  cfo->compression = kNoCompression;
}

PersistentStatsHistoryIterator::~PersistentStatsHistoryIterator() = default;

bool PersistentStatsHistoryIterator::Valid() const { return valid_; }

Status PersistentStatsHistoryIterator::status() const { return status_; }

void PersistentStatsHistoryIterator::Next() {
  // increment start_time by 1 to avoid infinite loop
  AdvanceIteratorByTime(GetStatsTime() + 1, end_time_);
}

uint64_t PersistentStatsHistoryIterator::GetStatsTime() const { return time_; }

const std::map<std::string, uint64_t>&
PersistentStatsHistoryIterator::GetStatsMap() const {
  return stats_map_;
}

std::pair<uint64_t, std::string> parseKey(const Slice& key,
                                          uint64_t start_time) {
  std::pair<uint64_t, std::string> result;
  std::string key_str = key.ToString();
  std::string::size_type pos = key_str.find('#');
  // TODO(Zhongyi): add counters to track parse failures?
  if (pos == std::string::npos) {
    result.first = std::numeric_limits<uint64_t>::max();
    result.second.clear();
  } else {
    uint64_t parsed_time = ParseUint64(key_str.substr(0, pos));
    // skip entries with timestamp smaller than start_time
    if (parsed_time < start_time) {
      result.first = std::numeric_limits<uint64_t>::max();
      result.second = "";
    } else {
      result.first = parsed_time;
      std::string key_resize = key_str.substr(pos + 1);
      result.second = key_resize;
    }
  }
  return result;
}

// advance the iterator to the next time between [start_time, end_time)
// if success, update time_ and stats_map_ with new_time and stats_map
void PersistentStatsHistoryIterator::AdvanceIteratorByTime(uint64_t start_time,
                                                           uint64_t end_time) {
  // try to find next entry in stats_history_ map
  if (db_impl_ != nullptr) {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    ReadOptions ro;
    Iterator* iter =
        db_impl_->NewIterator(ro, db_impl_->PersistentStatsColumnFamily());

    char timestamp[kNowSecondsStringLength + 1];
    snprintf(timestamp, sizeof(timestamp), "%010d",
             static_cast<int>(std::max(time_, start_time)));
    timestamp[kNowSecondsStringLength] = '\0';

    iter->Seek(timestamp);
    // no more entries with timestamp >= start_time is found or version key
    // is found to be incompatible
    if (!iter->Valid()) {
      valid_ = false;
      delete iter;
      return;
    }
    time_ = parseKey(iter->key(), start_time).first;
    valid_ = true;
    // check parsed time and invalid if it exceeds end_time
    if (time_ > end_time) {
      valid_ = false;
      delete iter;
      return;
    }
    // find all entries with timestamp equal to time_
    std::map<std::string, uint64_t> new_stats_map;
    std::pair<uint64_t, std::string> kv;
    for (; iter->Valid(); iter->Next()) {
      kv = parseKey(iter->key(), start_time);
      if (kv.first != time_) {
        break;
      }
      if (kv.second.compare(kFormatVersionKeyString) == 0) {
        continue;
      }
      new_stats_map[kv.second] = ParseUint64(iter->value().ToString());
    }
    stats_map_.swap(new_stats_map);
    delete iter;
  } else {
    valid_ = false;
  }
}

}  // namespace ROCKSDB_NAMESPACE
