// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::SimCache.

#include "include/rocksdb/utilities/sim_cache.h"

#include <jni.h>

#include "include/org_rocksdb_SimCache.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_SimCache
 * Method:    newSimCache
 * Signature: (JJI)J
 */
jlong Java_org_rocksdb_SimCache_newSimCache(JNIEnv* /*env*/, jclass /*jcls*/,
                                            jlong jhandle,
                                            jlong jcapacity,
                                            jint jnum_shard_bits) {
  auto* sptr_sim_cache = new std::shared_ptr<ROCKSDB_NAMESPACE::Cache>(
      ROCKSDB_NAMESPACE::NewSimCache(
          *reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle),
          static_cast<size_t>(jcapacity), 
          static_cast<int>(jnum_shard_bits)));
  return GET_CPLUSPLUS_POINTER(sptr_sim_cache);
}

/*
 * Class:     org_rocksdb_SimCache
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SimCache_disposeInternal(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jhandle) {
  auto* sptr_sim_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  delete sptr_sim_cache;  // delete std::shared_ptr
}
