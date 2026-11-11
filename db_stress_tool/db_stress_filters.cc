//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db_stress_tool/db_stress_filters.h"

#include <memory>
#include <mutex>

#include "db_stress_tool/db_stress_status.h"

namespace ROCKSDB_NAMESPACE {

#ifdef GFLAGS

using experimental::KeySegmentsExtractor;
using experimental::MakeSharedBytewiseMinMaxSQFC;
using experimental::SelectKeySegment;
using experimental::SstQueryFilterConfigs;
using experimental::SstQueryFilterConfigsManager;

namespace {
class VariableWidthExtractor : public KeySegmentsExtractor {
 public:
  const char* Name() const override { return "VariableWidthExtractor"; }

  void Extract(const Slice& key_or_bound, KeyKind /*kind*/,
               Result* result) const override {
    uint32_t len = static_cast<uint32_t>(key_or_bound.size());
    // This uses as delimiter any zero byte that follows a non-zero byte.
    // And in accordance with best practice, the delimiter is part of the
    // segment leading up to it.
    bool prev_non_zero = false;
    for (uint32_t i = 0; i < len; ++i) {
      if ((prev_non_zero && key_or_bound[i] == 0) || i + 1 == len) {
        result->segment_ends.push_back(i + 1);
      }
      prev_non_zero = key_or_bound[i] != 0;
    }
  }
};
class FixedWidthExtractor : public KeySegmentsExtractor {
 public:
  const char* Name() const override { return "FixedWidthExtractor"; }

  void Extract(const Slice& key_or_bound, KeyKind /*kind*/,
               Result* result) const override {
    uint32_t len = static_cast<uint32_t>(key_or_bound.size());
    // Fixed 8-byte segments, with any leftovers going into another
    // segment.
    uint32_t i = 0;
    while (i + 8 <= len) {
      i += 8;
      result->segment_ends.push_back(i);
    }
    if (i < len) {
      result->segment_ends.push_back(len);
    }
  }
};
}  // namespace

SstQueryFilterConfigsManager& DbStressSqfcManager() {
  std::once_flag flag;
  static std::shared_ptr<SstQueryFilterConfigsManager> mgr;
  std::call_once(flag, []() {
    const auto kVariableWidthExtractor =
        std::make_shared<VariableWidthExtractor>();
    const auto kFixedWidthExtractor = std::make_shared<FixedWidthExtractor>();
    // NOTE: MinMax filter on segment 0 is not normally useful because of
    // metadata on smallest and largest key for each SST file. But it doesn't
    // hurt to test.
    const auto kFilter0 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(0));
    const auto kFilter1 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(1));
    const auto kFilter2 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(2));
    const auto kFilter3 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(3));
    const SstQueryFilterConfigs fooConfigs1{{kFilter0, kFilter2},
                                            kVariableWidthExtractor};
    const SstQueryFilterConfigs fooConfigs2{{kFilter1, kFilter3},
                                            kVariableWidthExtractor};
    const SstQueryFilterConfigs barConfigs2{{kFilter1, kFilter2},
                                            kFixedWidthExtractor};
    const SstQueryFilterConfigsManager::Data data = {
        {1, {{"foo", fooConfigs1}}},
        {2, {{"foo", fooConfigs2}, {"bar", barConfigs2}}},
    };
    Status s = SstQueryFilterConfigsManager::MakeShared(data, &mgr);
    DB_STRESS_ASSERT_OK(s);
    assert(mgr);
  });
  return *mgr;
}

#endif  // GFLAGS

}  // namespace ROCKSDB_NAMESPACE
