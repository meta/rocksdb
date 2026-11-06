// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions.

#include <jni.h>

#include "include/org_rocksdb_OptimisticTransactionOptions.h"
#include "rocksdb/comparator.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    newOptimisticTransactionDBOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_OptimisticTransactionDBOptions_newOptimisticTransactionDBOptions(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions* opts =
      new ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions();
  return GET_CPLUSPLUS_POINTER(opts);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    setOccValidationPolicy
 * Signature: (JB)V
 */
void Java_org_rocksdb_OptimisticTransactionDBOptions_setOccValidationPolicy(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jbyte policy) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions*>(
          jhandle);
  opts->validate_policy =
      ROCKSDB_NAMESPACE::OccValidationPolicyJni::toCppOccValidationPolicy(
          policy);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    getOccValidationPolicy
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_OptimisticTransactionDBOptions_getOccValidationPolicy(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions*>(
          jhandle);
  return ROCKSDB_NAMESPACE::OccValidationPolicyJni::toJavaOccValidationPolicy(
      opts->validate_policy);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    setOccValidationPolicy
 * Signature: (JJ)V
 */
void Java_org_rocksdb_OptimisticTransactionDBOptions_setOccLockBuckets(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jlong occ_lock_buckets) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions*>(
          jhandle);
  opts->occ_lock_buckets = static_cast<uint32_t>(occ_lock_buckets);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    getOccValidationPolicy
 * Signature: (J)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDBOptions_getOccLockBuckets(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions*>(
          jhandle);
  return static_cast<long>(opts->occ_lock_buckets);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_OptimisticTransactionDBOptions_disposeInternalJni(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionDBOptions*>(
      jhandle);
}
