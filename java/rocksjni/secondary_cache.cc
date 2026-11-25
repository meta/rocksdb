//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file implements the JNI bindings for SecondaryCache

#include <jni.h>

#include "include/org_rocksdb_SecondaryCache.h"
#include "rocksdb/cache.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_rocksdb_SecondaryCache_CompressedSecondaryCache
 * Method:    newCompressedSecondaryCacheInstance
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_SecondaryCache_00024CompressedSecondaryCache_newCompressedSecondaryCacheInstance__J(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jcapacity) {
  auto opts = ROCKSDB_NAMESPACE::CompressedSecondaryCacheOptions();
  opts.capacity = static_cast<size_t>(jcapacity);
  auto cache = opts.MakeSharedSecondaryCache();
  auto* cache_ptr =
      new std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache>(cache);
  return GET_CPLUSPLUS_POINTER(cache_ptr);
}

/*
 * Class:     org_rocksdb_SecondaryCache_CompressedSecondaryCache
 * Method:    newCompressedSecondaryCacheInstance
 * Signature: (JIZD)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_SecondaryCache_00024CompressedSecondaryCache_newCompressedSecondaryCacheInstance__JIZD(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jcapacity, jint jnum_shard_bits,
    jboolean jstrict_capacity_limit, jdouble jhigh_pri_pool_ratio) {
  auto opts = ROCKSDB_NAMESPACE::CompressedSecondaryCacheOptions();
  opts.capacity = static_cast<size_t>(jcapacity);
  opts.num_shard_bits = static_cast<int>(jnum_shard_bits);
  opts.strict_capacity_limit = static_cast<bool>(jstrict_capacity_limit);
  opts.high_pri_pool_ratio = static_cast<double>(jhigh_pri_pool_ratio);

  auto cache = opts.MakeSharedSecondaryCache();
  auto* cache_ptr =
      new std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache>(cache);
  return GET_CPLUSPLUS_POINTER(cache_ptr);
}

/*
 * Class:     org_rocksdb_SecondaryCache
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_SecondaryCache_disposeInternalJni(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache>*>(
          jhandle);
  assert(cache != nullptr);
  delete cache;
}

#ifdef __cplusplus
}
#endif
