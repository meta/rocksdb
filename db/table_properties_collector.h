//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a collection of statistics collectors.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Base class for internal table properties collector.
class InternalTblPropColl {
 public:
  virtual ~InternalTblPropColl() = default;
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  virtual const char* Name() const = 0;

  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) = 0;

  virtual void BlockAdd(uint64_t block_uncomp_bytes,
                        uint64_t block_compressed_bytes_fast,
                        uint64_t block_compressed_bytes_slow) = 0;

  virtual UserCollectedProperties GetReadableProperties() const = 0;

  virtual bool NeedCompact() const { return false; }
};

// Factory for internal table properties collector.
class InternalTblPropCollFactory {
 public:
  virtual ~InternalTblPropCollFactory() = default;
  // has to be thread-safe
  virtual InternalTblPropColl* CreateInternalTblPropColl(
      uint32_t column_family_id, int level_at_creation, int num_levels,
      SequenceNumber last_level_inclusive_max_seqno_threshold =
          kMaxSequenceNumber) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;
};

using InternalTblPropCollFactories =
    std::vector<std::unique_ptr<InternalTblPropCollFactory>>;

// When rocksdb creates a new table, it will encode all "user keys" into
// "internal keys", which contains meta information of a given entry.
//
// This class extracts user key from the encoded internal key when Add() is
// invoked.
class UserKeyTablePropertiesCollector : public InternalTblPropColl {
 public:
  // transfer of ownership
  explicit UserKeyTablePropertiesCollector(TablePropertiesCollector* collector)
      : collector_(collector) {}

  ~UserKeyTablePropertiesCollector() override = default;

  Status InternalAdd(const Slice& key, const Slice& value,
                     uint64_t file_size) override;

  void BlockAdd(uint64_t block_uncomp_bytes,
                uint64_t block_compressed_bytes_fast,
                uint64_t block_compressed_bytes_slow) override;

  Status Finish(UserCollectedProperties* properties) override;

  const char* Name() const override { return collector_->Name(); }

  UserCollectedProperties GetReadableProperties() const override;

  bool NeedCompact() const override { return collector_->NeedCompact(); }

 protected:
  std::unique_ptr<TablePropertiesCollector> collector_;
};

class UserKeyTablePropertiesCollectorFactory
    : public InternalTblPropCollFactory {
 public:
  explicit UserKeyTablePropertiesCollectorFactory(
      std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory)
      : user_collector_factory_(user_collector_factory) {}
  InternalTblPropColl* CreateInternalTblPropColl(
      uint32_t column_family_id, int level_at_creation, int num_levels,
      SequenceNumber last_level_inclusive_max_seqno_threshold =
          kMaxSequenceNumber) override {
    TablePropertiesCollectorFactory::Context context;
    context.column_family_id = column_family_id;
    context.level_at_creation = level_at_creation;
    context.num_levels = num_levels;
    context.last_level_inclusive_max_seqno_threshold =
        last_level_inclusive_max_seqno_threshold;
    TablePropertiesCollector* collector =
        user_collector_factory_->CreateTablePropertiesCollector(context);
    if (collector) {
      return new UserKeyTablePropertiesCollector(collector);
    } else {
      return nullptr;
    }
  }

  const char* Name() const override { return user_collector_factory_->Name(); }

 private:
  std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory_;
};

// When rocksdb creates a newtable, it will encode all "user keys" into
// "internal keys". This class collects min/max timestamp from the encoded
// internal key when Add() is invoked.
//
// @param cmp  the user comparator to compare the timestamps in internal key.
class TimestampTablePropertiesCollector : public InternalTblPropColl {
 public:
  explicit TimestampTablePropertiesCollector(const Comparator* cmp)
      : cmp_(cmp),
        timestamp_min_(kDisableUserTimestamp),
        timestamp_max_(kDisableUserTimestamp) {}

  Status InternalAdd(const Slice& key, const Slice& /* value */,
                     uint64_t /* file_size */) override {
    auto user_key = ExtractUserKey(key);
    assert(cmp_ && cmp_->timestamp_size() > 0);
    if (user_key.size() < cmp_->timestamp_size()) {
      return Status::Corruption(
          "User key size mismatch when comparing to timestamp size.");
    }
    auto timestamp_in_key =
        ExtractTimestampFromUserKey(user_key, cmp_->timestamp_size());
    if (timestamp_max_ == kDisableUserTimestamp ||
        cmp_->CompareTimestamp(timestamp_in_key, timestamp_max_) > 0) {
      timestamp_max_.assign(timestamp_in_key.data(), timestamp_in_key.size());
    }
    if (timestamp_min_ == kDisableUserTimestamp ||
        cmp_->CompareTimestamp(timestamp_min_, timestamp_in_key) > 0) {
      timestamp_min_.assign(timestamp_in_key.data(), timestamp_in_key.size());
    }
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    return;
  }

  Status Finish(UserCollectedProperties* properties) override {
    // timestamp is empty is table is empty
    assert(timestamp_min_.size() == timestamp_max_.size() &&
           (timestamp_min_.empty() ||
            timestamp_max_.size() == cmp_->timestamp_size()));
    properties->insert({"rocksdb.timestamp_min", timestamp_min_});
    properties->insert({"rocksdb.timestamp_max", timestamp_max_});
    return Status::OK();
  }

  const char* Name() const override {
    return "TimestampTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    return {{"rocksdb.timestamp_min", Slice(timestamp_min_).ToString(true)},
            {"rocksdb.timestamp_max", Slice(timestamp_max_).ToString(true)}};
  }

 protected:
  const Comparator* const cmp_;
  std::string timestamp_min_;
  std::string timestamp_max_;
};

}  // namespace ROCKSDB_NAMESPACE
