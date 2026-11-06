//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class OccLockBucketsImplBase : public OccLockBuckets {
 public:
  virtual port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) = 0;
};

template <bool cache_aligned>
class OccLockBucketsImpl : public OccLockBucketsImplBase {
 public:
  explicit OccLockBucketsImpl(size_t bucket_count) : locks_(bucket_count) {}
  port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) override {
    return locks_.Get(key, seed);
  }
  size_t ApproximateMemoryUsage() const override {
    return locks_.ApproximateMemoryUsage();
  }

 private:
  // TODO: investigate optionally using folly::MicroLock to majorly save space
  using M = std::conditional_t<cache_aligned, CacheAlignedWrapper<port::Mutex>,
                               port::Mutex>;
  Striped<M> locks_;
};

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(
      std::unique_ptr<DB>&& db,
      const OptimisticTransactionDBOptions& occ_options)
      : OptimisticTransactionDB(std::move(db)),
        validate_policy_(occ_options.validate_policy) {
    if (validate_policy_ == OccValidationPolicy::kValidateParallel) {
      auto bucketed_locks = occ_options.shared_lock_buckets;
      if (!bucketed_locks) {
        uint32_t bucket_count = std::max(16u, occ_options.occ_lock_buckets);
        bucketed_locks = MakeSharedOccLockBuckets(bucket_count);
      }
      bucketed_locks_ = static_cast_with_check<OccLockBucketsImplBase>(
          std::move(bucketed_locks));
    }
  }

  ~OptimisticTransactionDBImpl() override = default;

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const OptimisticTransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Transactional `DeleteRange()` is not yet supported.
  using StackableDB::DeleteRange;
  Status DeleteRange(const WriteOptions&, ColumnFamilyHandle*, const Slice&,
                     const Slice&) override {
    return Status::NotSupported();
  }

  // Range deletions also must not be snuck into `WriteBatch`es as they are
  // incompatible with `OptimisticTransactionDB`.
  Status Write(const WriteOptions& write_opts, WriteBatch* batch) override {
    if (batch->HasDeleteRange()) {
      return Status::NotSupported();
    }
    return OptimisticTransactionDB::Write(write_opts, batch);
  }

  OccValidationPolicy GetValidatePolicy() const override {
    return validate_policy_;
  }

  port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) {
    return bucketed_locks_->GetLockBucket(key, seed);
  }

 private:
  std::shared_ptr<OccLockBucketsImplBase> bucketed_locks_;

  const OccValidationPolicy validate_policy_;

  void ReinitializeTransaction(Transaction* txn,
                               const WriteOptions& write_options,
                               const OptimisticTransactionOptions& txn_options =
                                   OptimisticTransactionOptions());
};

}  // namespace ROCKSDB_NAMESPACE
