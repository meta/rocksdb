//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/listener.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace blob_db {

class BlobDBListener : public EventListener {
 public:
  explicit BlobDBListener(BlobDBImpl* blob_db_impl)
      : blob_db_impl_(blob_db_impl) {}

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->SyncBlobFiles(WriteOptions(Env::IOActivity::kFlush))
        .PermitUncheckedError();
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize();
  }

  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize();
  }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "BlobDBListener"; }

 protected:
  BlobDBImpl* blob_db_impl_;
};

class BlobDBListenerGC : public BlobDBListener {
 public:
  explicit BlobDBListenerGC(BlobDBImpl* blob_db_impl)
      : BlobDBListener(blob_db_impl) {}

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "BlobDBListenerGC"; }
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    assert(blob_db_impl_);
    TEST_SYNC_POINT_CALLBACK("BlobDBListenerGC::OnFlushCompleted:BeforeProcess",
                             const_cast<FlushJobInfo*>(&info));
    // Keep GC bookkeeping ahead of any DB property queries in the base
    // listener; later compactions can rely on these mappings.
    blob_db_impl_->ProcessFlushJobInfo(info);

    BlobDBListener::OnFlushCompleted(db, info);
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    assert(blob_db_impl_);
    // Keep GC bookkeeping ahead of any DB property queries in the base
    // listener; later compactions can rely on these mappings.
    blob_db_impl_->ProcessCompactionJobInfo(info);

    BlobDBListener::OnCompactionCompleted(db, info);
  }
};

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
