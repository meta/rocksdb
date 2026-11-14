// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "include/org_rocksdb_BloomFilter.h"
#include "include/org_rocksdb_RibbonFilter.h"
#include "include/org_rocksdb_Filter.h"
#include "rocksdb/filter_policy.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_BloomFilter
 * Method:    createBloomFilter
 * Signature: (DZ)J
 */
jlong Java_org_rocksdb_BloomFilter_createNewBloomFilter(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jdouble bits_per_key) {
  auto* sptr_filter =
      new std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>(
          ROCKSDB_NAMESPACE::NewBloomFilterPolicy(bits_per_key));
  return GET_CPLUSPLUS_POINTER(sptr_filter);
}

/*
 * Class:     org_rocksdb_RibbonFilter
 * Method:    createRibbonFilter
 * Signature: (DZ)J
 */
jlong Java_org_rocksdb_RibbonFilter_createNewRibbonFilter(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jdouble bits_per_key) {
  auto* sptr_filter =
      new std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>(
          ROCKSDB_NAMESPACE::NewRibbonFilterPolicy(bits_per_key));
  return GET_CPLUSPLUS_POINTER(sptr_filter);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Filter_disposeInternalJni(JNIEnv* /*env*/,
                                                jclass /*jcls*/,
                                                jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}
