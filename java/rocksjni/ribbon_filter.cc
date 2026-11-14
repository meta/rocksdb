//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_RibbonFilter.h"
#include "rocksdb/filter_policy.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_RibbonFilter
 * Method:    createNewRibbonFilter
 * Signature: (DI)J
 */
jlong Java_org_rocksdb_RibbonFilter_createNewRibbonFilter(
    JNIEnv* /*env*/, jclass /*jcls*/, jdouble jbits_per_key,
    jint jbloom_before_level) {
  auto* filter_policy =
      new std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>(
          ROCKSDB_NAMESPACE::NewRibbonFilterPolicy(
              static_cast<double>(jbits_per_key),
              static_cast<int>(jbloom_before_level)));
  return GET_CPLUSPLUS_POINTER(filter_policy);
}
