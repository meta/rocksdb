//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/optimistic_transaction_db_impl.h"

#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "utilities/transactions/optimistic_transaction.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<OccLockBuckets> MakeSharedOccLockBuckets(size_t bucket_count,
                                                         bool cache_aligned) {
  if (cache_aligned) {
    return std::make_shared<OccLockBucketsImpl<true>>(bucket_count);
  } else {
    return std::make_shared<OccLockBucketsImpl<false>>(bucket_count);
  }
}

Transaction* OptimisticTransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options, Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new OptimisticTransaction(this, write_options, txn_options);
  }
}

Status OptimisticTransactionDB::Open(const Options& options,
                                     const std::string& dbname,
                                     OptimisticTransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  Status s = Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status OptimisticTransactionDB::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  return OptimisticTransactionDB::Open(db_options,
                                       OptimisticTransactionDBOptions(), dbname,
                                       column_families, handles, dbptr);
}

Status OptimisticTransactionDB::Open(
    const DBOptions& db_options,
    const OptimisticTransactionDBOptions& occ_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  Status s;
  std::unique_ptr<DB> db;

  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;

  // Enable MemTable History if not already enabled
  for (auto& column_family : column_families_copy) {
    ColumnFamilyOptions* options = &column_family.options;
    s = OptimisticTransactionDBImpl::VerifyCFOptions(*options);
    if (!s.ok()) {
      return s;
    }

    if (options->max_write_buffer_size_to_maintain == 0) {
      // Setting to -1 will set the History size to
      // max_write_buffer_number * write_buffer_size.
      options->max_write_buffer_size_to_maintain = -1;
    }
  }

  s = DB::Open(db_options, dbname, column_families_copy, handles, &db);

  if (s.ok()) {
    *dbptr = new OptimisticTransactionDBImpl(std::move(db), occ_options);
  }

  return s;
}

Status OptimisticTransactionDBImpl::VerifyCFOptions(
    const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    // An in-place update keeps the sequence number of the entry it overwrites,
    // so commit-time validation cannot detect a conflicting write.
    return Status::NotSupported(
        "inplace_update_support is not supported with "
        "OptimisticTransactionDB");
  }
  return Status::OK();
}

Status OptimisticTransactionDBImpl::CreateColumnFamily(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle) {
  Status s = VerifyCFOptions(options);
  if (!s.ok()) {
    return s;
  }
  return OptimisticTransactionDB::CreateColumnFamily(
      options, column_family_name, handle);
}

Status OptimisticTransactionDBImpl::CreateColumnFamilies(
    const ColumnFamilyOptions& options,
    const std::vector<std::string>& column_family_names,
    std::vector<ColumnFamilyHandle*>* handles) {
  Status s = VerifyCFOptions(options);
  if (!s.ok()) {
    return s;
  }
  return OptimisticTransactionDB::CreateColumnFamilies(
      options, column_family_names, handles);
}

Status OptimisticTransactionDBImpl::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  for (const auto& cf_desc : column_families) {
    Status s = VerifyCFOptions(cf_desc.options);
    if (!s.ok()) {
      return s;
    }
  }
  return OptimisticTransactionDB::CreateColumnFamilies(column_families,
                                                       handles);
}

Status OptimisticTransactionDBImpl::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const std::vector<const ExportImportFilesMetaData*>& metadatas,
    ColumnFamilyHandle** handle) {
  Status s = VerifyCFOptions(options);
  if (!s.ok()) {
    return s;
  }
  return OptimisticTransactionDB::CreateColumnFamilyWithImport(
      options, column_family_name, import_options, metadatas, handle);
}

void OptimisticTransactionDBImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  assert(dynamic_cast<OptimisticTransaction*>(txn) != nullptr);
  auto txn_impl = static_cast<OptimisticTransaction*>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

}  // namespace ROCKSDB_NAMESPACE
