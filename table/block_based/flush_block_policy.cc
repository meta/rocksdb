//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/flush_block_policy.h"

#include <cassert>
#include <memory>
#include <mutex>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/customizable_util.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/flush_block_policy_impl.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

// Flush block by size
class FlushBlockBySizePolicy : public RetargetableFlushBlockPolicy {
 public:
  // @params block_size:           Approximate size of user data packed per
  //                               block.
  // @params block_size_deviation: This is used to close a block before it
  //                               reaches the configured
  FlushBlockBySizePolicy(const uint64_t block_size,
                         const uint64_t block_size_deviation, const bool align,
                         const BlockBuilder& data_block_builder)
      : RetargetableFlushBlockPolicy(data_block_builder),
        block_size_(block_size),
        block_size_deviation_limit_(
            ((block_size * (100 - block_size_deviation)) + 99) / 100),
        align_(align) {}

  bool Update(const Slice& key, const Slice& value) override {
    // it makes no sense to flush when the data block is empty
    if (data_block_builder_->empty()) {
      return false;
    }

    auto curr_size = data_block_builder_->CurrentSizeEstimate();

    // Do flush if one of the below two conditions is true:
    // 1) if the current estimated size already exceeds the block size,
    // 2) block_size_deviation is set and the estimated size after appending
    // the kv will exceed the block size and the current size is under the
    // the deviation.
    return curr_size >= block_size_ || BlockAlmostFull(key, value);
  }

 private:
  bool BlockAlmostFull(const Slice& key, const Slice& value) const {
    if (block_size_deviation_limit_ == 0) {
      return false;
    }

    const auto curr_size = data_block_builder_->CurrentSizeEstimate();
    auto estimated_size_after =
        data_block_builder_->EstimateSizeAfterKV(key, value);

    if (align_) {
      estimated_size_after += BlockBasedTable::kBlockTrailerSize;
      return estimated_size_after > block_size_;
    }

    return estimated_size_after > block_size_ &&
           curr_size > block_size_deviation_limit_;
  }

  const uint64_t block_size_;
  const uint64_t block_size_deviation_limit_;
  const bool align_;
};

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const BlockBasedTableOptions& table_options,
    const BlockBuilder& data_block_builder) const {
  return new FlushBlockBySizePolicy(
      table_options.block_size, table_options.block_size_deviation,
      table_options.block_align, data_block_builder);
}

std::unique_ptr<RetargetableFlushBlockPolicy> NewFlushBlockBySizePolicy(
    const uint64_t size, const int deviation,
    const BlockBuilder& data_block_builder) {
  return std::make_unique<FlushBlockBySizePolicy>(size, deviation, false,
                                                  data_block_builder);
}

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const uint64_t size, const int deviation,
    const BlockBuilder& data_block_builder) {
  return NewFlushBlockBySizePolicy(size, deviation, data_block_builder)
      .release();
}

static int RegisterFlushBlockPolicyFactories(ObjectLibrary& library,
                                             const std::string& /*arg*/) {
  library.AddFactory<FlushBlockPolicyFactory>(
      FlushBlockBySizePolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FlushBlockBySizePolicyFactory());
        return guard->get();
      });
  library.AddFactory<FlushBlockPolicyFactory>(
      FlushBlockEveryKeyPolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FlushBlockEveryKeyPolicyFactory());
        return guard->get();
      });
  return 2;
}

FlushBlockBySizePolicyFactory::FlushBlockBySizePolicyFactory()
    : FlushBlockPolicyFactory() {}

Status FlushBlockPolicyFactory::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<FlushBlockPolicyFactory>* factory) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterFlushBlockPolicyFactories(*(ObjectLibrary::Default().get()), "");
  });

  if (value.empty()) {
    *factory = std::make_shared<FlushBlockBySizePolicyFactory>();
    return Status::OK();
  } else {
    return LoadSharedObject<FlushBlockPolicyFactory>(config_options, value,
                                                     factory);
  }
}
}  // namespace ROCKSDB_NAMESPACE
