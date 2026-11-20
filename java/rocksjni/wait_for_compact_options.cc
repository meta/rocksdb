//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::WaitForCompactOptions.

#include <jni.h>

#include "include/org_rocksdb_WaitForCompactOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    newWaitForCompactOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_WaitForCompactOptions_newWaitForCompactOptions(JNIEnv*,
                                                                      jclass) {
  auto* wait_for_compact_opts = new ROCKSDB_NAMESPACE::WaitForCompactOptions();
  return GET_CPLUSPLUS_POINTER(wait_for_compact_opts);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    setAbortOnPause
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WaitForCompactOptions_setAbortOnPause(
    JNIEnv*, jclass, jlong jhandle, jboolean jabort_on_pause) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  wait_for_compact_opts->abort_on_pause = jabort_on_pause;
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    abortOnPause
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WaitForCompactOptions_abortOnPause(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  return static_cast<jboolean>(wait_for_compact_opts->abort_on_pause);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    setFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WaitForCompactOptions_setFlush(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jboolean jflush) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  wait_for_compact_opts->flush = jflush;
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    flush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WaitForCompactOptions_flush(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  return static_cast<jboolean>(wait_for_compact_opts->flush);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    setWaitForPurge
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WaitForCompactOptions_setWaitForPurge(
    JNIEnv*, jclass, jlong jhandle, jboolean jwait_for_purge) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  wait_for_compact_opts->wait_for_purge = jwait_for_purge;
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    waitForPurge
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WaitForCompactOptions_waitForPurge(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  return static_cast<jboolean>(wait_for_compact_opts->wait_for_purge);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    setCloseDb
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WaitForCompactOptions_setCloseDb(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jboolean jclose_db) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  wait_for_compact_opts->close_db = jclose_db;
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    closeDb
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WaitForCompactOptions_closeDb(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  return static_cast<jboolean>(wait_for_compact_opts->close_db);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    setTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WaitForCompactOptions_setTimeout(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jlong jtimeout_micros) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  wait_for_compact_opts->timeout = std::chrono::microseconds(jtimeout_micros);
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    timeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WaitForCompactOptions_timeout(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  return static_cast<jlong>(wait_for_compact_opts->timeout.count());
}

/*
 * Class:     org_rocksdb_WaitForCompactOptions
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
void Java_org_rocksdb_WaitForCompactOptions_disposeInternalJni(JNIEnv*, jclass,
                                                               jlong jhandle) {
  auto* wait_for_compact_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::WaitForCompactOptions*>(jhandle);
  assert(wait_for_compact_opts != nullptr);
  delete wait_for_compact_opts;
}