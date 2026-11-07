// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"

class MyMerge : public ROCKSDB_NAMESPACE::MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    if (merge_in.existing_value != nullptr) {
      merge_out->new_value.assign(merge_in.existing_value->data(),
                                  merge_in.existing_value->size());
    }
    for (const ROCKSDB_NAMESPACE::Slice& m : merge_in.operand_list) {
      fprintf(stderr, "Merge(%s)\n", m.ToString().c_str());
      // the compaction filter filters out bad values
      assert(m.ToString() != "bad");
      merge_out->new_value.assign(m.data(), m.size());
    }
    return true;
  }

  const char* Name() const override { return "MyMerge"; }
};

class MyFilter : public ROCKSDB_NAMESPACE::CompactionFilter {
 public:
  bool Filter(int level, const ROCKSDB_NAMESPACE::Slice& key,
              const ROCKSDB_NAMESPACE::Slice& existing_value,
              std::string* new_value, bool* value_changed) const override {
    fprintf(stderr, "Filter(%s)\n", key.ToString().c_str());
    ++count_;
    assert(*value_changed == false);
    return false;
  }

  bool FilterMergeOperand(
      int level, const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Slice& existing_value) const override {
    fprintf(stderr, "FilterMerge(%s)\n", key.ToString().c_str());
    ++merge_count_;
    return existing_value == "bad";
  }

  const char* Name() const override { return "MyFilter"; }

  mutable int count_ = 0;
  mutable int merge_count_ = 0;
};

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksmergetest";
std::string kRemoveDirCommand = "rmdir /Q /S ";
#else
std::string kDBPath = "/tmp/rocksmergetest";
std::string kRemoveDirCommand = "rm -rf ";
#endif

int main() {
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
  ROCKSDB_NAMESPACE::Status status;

  MyFilter filter;

  std::string rm_cmd = kRemoveDirCommand + kDBPath;
  int ret = system(rm_cmd.c_str());
  if (ret != 0) {
    fprintf(stderr, "Error deleting %s, code: %d\n", kDBPath.c_str(), ret);
  }
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<MyMerge>();
  options.compaction_filter = &filter;
  status = ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db);
  assert(status.ok());

  ROCKSDB_NAMESPACE::WriteOptions wopts;
  db->Merge(wopts, "0", "bad");  // This is filtered out
  db->Merge(wopts, "1", "data1");
  db->Merge(wopts, "1", "bad");
  db->Merge(wopts, "1", "data2");
  db->Merge(wopts, "1", "bad");
  db->Merge(wopts, "3", "data3");
  db->CompactRange(ROCKSDB_NAMESPACE::CompactRangeOptions(), nullptr, nullptr);
  fprintf(stderr, "filter.count_ = %d\n", filter.count_);
  assert(filter.count_ == 0);
  fprintf(stderr, "filter.merge_count_ = %d\n", filter.merge_count_);
  assert(filter.merge_count_ == 6);
}
