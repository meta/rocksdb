//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once
#include <algorithm>

#include "db/version_edit.h"

namespace ROCKSDB_NAMESPACE {
// We boost files that are closer to TTL limit. This boosting could be
// through FileMetaData.compensated_file_size but this compensated size
// is widely used as something similar to file size so dramatically boost
// the value might cause unintended consequences.
//
// This boosting algorithm can go very fancy, but here we use a simple
// formula which can satisify:
// (1) Different levels are triggered slightly differently to avoid
//     too many cascading cases
// (2) Files in the same level get boosting more when TTL gets closer.
//
// Don't do any boosting before TTL has past by half. This is to make
// sure lower write amp for most of the case. And all levels should be
// fully boosted when total TTL compaction threshold triggers.
// Differientiate boosting ranges of each level by 1/2. This will make
// range for each level exponentially increasing. We could do it by
// having them to be equal, or go even fancier. We can adjust it after
// we observe the behavior in production.
// The threshold starting boosting:
// +------------------------------------------------------------------ +
// ^                            ^   ^     ^       ^                 ^
// Age 0                        ... |     |    second last level    thresold
//                                  |     |
//                                  |  third last level
//                                  |
//                            forth last level
//
// We arbitrarily set with 0 when a file is aged boost_age_start and
// grow linearly. The ratio is arbitrarily set so that when the next level
// starts to boost, the previous level's boosting amount is 16.
class FileTtlBooster {
 public:
  FileTtlBooster(uint64_t current_time, uint64_t ttl, int num_non_empty_levels,
                 int level)
      : current_time_(current_time) {
    if (ttl == 0 || level == 0 || level >= num_non_empty_levels - 1) {
      enabled_ = false;
      boost_age_start_ = 0;
      boost_step_ = 1;
    } else {
      enabled_ = true;
      uint64_t all_boost_start_age = ttl / 2;
      // A TTL below 32 seconds rounds the end of the range down to 0, so clamp
      // the range instead of letting it wrap around.
      uint64_t all_boost_end_age = (ttl / 32) * 31;
      uint64_t all_boost_age_range =
          all_boost_end_age > all_boost_start_age
              ? all_boost_end_age - all_boost_start_age
              : 0;
      // TODO(cbi): more reasonable algorithm that gives different values
      //   when num_non_empty_levels - level - 1 > 63.
      uint64_t boost_age_range =
          all_boost_age_range >> std::min(63, num_non_empty_levels - level - 1);
      boost_age_start_ = all_boost_start_age + boost_age_range;
      const uint64_t kBoostRatio = 16;
      // prevent 0 value to avoid divide 0 error.
      boost_step_ = std::max(boost_age_range / kBoostRatio, uint64_t{1});
    }
  }

  uint64_t GetBoostScore(FileMetaData* f) {
    if (!enabled_) {
      return 1;
    }
    uint64_t oldest_ancestor_time = f->TryGetOldestAncestorTime();
    if (oldest_ancestor_time >= current_time_) {
      return 1;
    }
    uint64_t age = current_time_ - oldest_ancestor_time;
    if (age > boost_age_start_) {
      // Use integer just for convenience.
      // We could make all file_to_order double if we want.
      // Technically this can overflow if users override timing and
      // give a very high current time. Ignore the case for simplicity.
      // Boosting is addition to current value, so +1. This will effectively
      // make boosting to kick in after the first boost_step_ is reached.
      return (age - boost_age_start_) / boost_step_ + 1;
    }
    return 1;
  }

 private:
  bool enabled_;
  uint64_t current_time_;
  uint64_t boost_age_start_;
  uint64_t boost_step_;
};
}  // namespace ROCKSDB_NAMESPACE
