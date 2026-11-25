// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Comparator.

#include <jni.h>

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <string>

#include "include/org_rocksdb_AbstractComparator.h"
#include "include/org_rocksdb_NativeComparatorWrapper.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_AbstractComparator
 * Method:    createNewComparator
 * Signature: (J)J
 */
jlong Java_org_rocksdb_AbstractComparator_createNewComparator(
    JNIEnv* env, jobject jcomparator, jlong copt_handle) {
  auto* copt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          copt_handle);
  auto* c =
      new ROCKSDB_NAMESPACE::ComparatorJniCallback(env, jcomparator, copt);
  return GET_CPLUSPLUS_POINTER(c);
}

/*
 * Class:     org_rocksdb_AbstractComparator
 * Method:    usingDirectBuffers
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_AbstractComparator_usingDirectBuffers(JNIEnv*, jclass,
                                                                jlong jhandle) {
  auto* c =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(jhandle);
  return static_cast<jboolean>(c->m_options->direct_buffer);
}

/*
 * Class:     org_rocksdb_NativeComparatorWrapper
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_NativeComparatorWrapper_disposeInternal(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jcomparator_handle) {
  auto* comparator =
      reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
  delete comparator;
}
