// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Env methods from Java side.

#include "rocksdb/env.h"

#include <jni.h>

#include <vector>

#include "include/org_rocksdb_Env.h"
#include "include/org_rocksdb_RocksEnv.h"
#include "include/org_rocksdb_RocksMemEnv.h"
#include "include/org_rocksdb_TimedEnv.h"
#include "portal.h"
#include "rocksjni/cplusplus_to_java_convert.h"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM*, void*) {
  // Use the standard 'C' locale to handle option conversions correctly
  assert(setlocale(LC_ALL, "C") != nullptr);

  return JNI_VERSION_1_6;
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getDefaultEnvInternal
 * Signature: ()J
 */
jlong Java_org_rocksdb_Env_getDefaultEnvInternal(JNIEnv*, jclass) {
  return GET_CPLUSPLUS_POINTER(ROCKSDB_NAMESPACE::Env::Default());
}

/*
 * Class:     org_rocksdb_RocksEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksEnv_disposeInternal(JNIEnv*, jclass, jlong jhandle) {
  auto* e = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_rocksdb_Env
 * Method:    setBackgroundThreads
 * Signature: (JIB)V
 */
void Java_org_rocksdb_Env_setBackgroundThreads(JNIEnv*, jclass, jlong jhandle,
                                               jint jnum,
                                               jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  rocks_env->SetBackgroundThreads(
      static_cast<int>(jnum),
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getBackgroundThreads
 * Signature: (JB)I
 */
jint Java_org_rocksdb_Env_getBackgroundThreads(JNIEnv*, jclass, jlong jhandle,
                                               jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  const int num = rocks_env->GetBackgroundThreads(
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(num);
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getThreadPoolQueueLen
 * Signature: (JB)I
 */
jint Java_org_rocksdb_Env_getThreadPoolQueueLen(JNIEnv*, jclass, jlong jhandle,
                                                jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  const int queue_len = rocks_env->GetThreadPoolQueueLen(
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(queue_len);
}

/*
 * Class:     org_rocksdb_Env
 * Method:    incBackgroundThreadsIfNeeded
 * Signature: (JIB)V
 */
void Java_org_rocksdb_Env_incBackgroundThreadsIfNeeded(JNIEnv*, jclass,
                                                       jlong jhandle, jint jnum,
                                                       jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  rocks_env->IncBackgroundThreadsIfNeeded(
      static_cast<int>(jnum),
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    lowerThreadPoolIOPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Env_lowerThreadPoolIOPriority(JNIEnv*, jclass,
                                                    jlong jhandle,
                                                    jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  rocks_env->LowerThreadPoolIOPriority(
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    lowerThreadPoolCPUPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Env_lowerThreadPoolCPUPriority(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  rocks_env->LowerThreadPoolCPUPriority(
      ROCKSDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getThreadList
 * Signature: (J)[Lorg/rocksdb/ThreadStatus;
 */
jobjectArray Java_org_rocksdb_Env_getThreadList(JNIEnv* env, jclass,
                                                jlong jhandle) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  std::vector<ROCKSDB_NAMESPACE::ThreadStatus> thread_status;
  ROCKSDB_NAMESPACE::Status s = rocks_env->GetThreadList(&thread_status);
  if (!s.ok()) {
    // error, throw exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // object[]
  const jsize len = static_cast<jsize>(thread_status.size());
  jobjectArray jthread_status = env->NewObjectArray(
      len, ROCKSDB_NAMESPACE::ThreadStatusJni::getJClass(env), nullptr);
  if (jthread_status == nullptr) {
    // an exception occurred
    return nullptr;
  }
  for (jsize i = 0; i < len; ++i) {
    jobject jts =
        ROCKSDB_NAMESPACE::ThreadStatusJni::construct(env, &(thread_status[i]));
    env->SetObjectArrayElement(jthread_status, i, jts);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jthread_status);
      return nullptr;
    }
  }

  return jthread_status;
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    createMemEnv
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksMemEnv_createMemEnv(JNIEnv*, jclass,
                                                jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jbase_env_handle);
  return GET_CPLUSPLUS_POINTER(ROCKSDB_NAMESPACE::NewMemEnv(base_env));
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksMemEnv_disposeInternalJni(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* e = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_rocksdb_TimedEnv
 * Method:    createTimedEnv
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TimedEnv_createTimedEnv(JNIEnv*, jclass,
                                               jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jbase_env_handle);
  return GET_CPLUSPLUS_POINTER(ROCKSDB_NAMESPACE::NewTimedEnv(base_env));
}

/*
 * Class:     org_rocksdb_TimedEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TimedEnv_disposeInternalJni(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* e = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}
