//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WalIterator methods from Java side.

#include "rocksdb/wal_iterator.h"

#include <jni.h>

#include <cstdio>
#include <cstdlib>

#include "include/org_rocksdb_WalIterator.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WalIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WalIterator_disposeInternalJni(JNIEnv* /*env*/,
                                                     jclass /*jcls*/,
                                                     jlong handle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::WalIterator*>(handle);
}

/*
 * Class:     org_rocksdb_WalIterator
 * Method:    isValid
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WalIterator_isValid(JNIEnv* /*env*/, jclass /*jcls*/,
                                              jlong handle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WalIterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_WalIterator
 * Method:    next
 * Signature: (J)V
 */
void Java_org_rocksdb_WalIterator_next(JNIEnv* /*env*/, jclass /*jcls*/,
                                       jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WalIterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_WalIterator
 * Method:    status
 * Signature: (J)V
 */
void Java_org_rocksdb_WalIterator_status(JNIEnv* env, jclass /*jcls*/,
                                         jlong handle) {
  ROCKSDB_NAMESPACE::Status s =
      reinterpret_cast<ROCKSDB_NAMESPACE::WalIterator*>(handle)->status();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_WalIterator
 * Method:    getBatch
 * Signature: (J)Lorg/rocksdb/WalIterator$BatchResult
 */
jobject Java_org_rocksdb_WalIterator_getBatch(JNIEnv* env, jclass /*jcls*/,
                                              jlong handle) {
  ROCKSDB_NAMESPACE::BatchResult batch_result =
      reinterpret_cast<ROCKSDB_NAMESPACE::WalIterator*>(handle)->GetBatch();
  return ROCKSDB_NAMESPACE::BatchResultJni::construct(env, batch_result);
}
