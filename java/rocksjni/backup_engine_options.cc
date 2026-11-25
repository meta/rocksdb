// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::BackupEnginge and
// ROCKSDB_NAMESPACE::BackupEngineOptions methods from Java side.

#include <jni.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "include/org_rocksdb_BackupEngineOptions.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

///////////////////////////////////////////////////////////////////////////
// BackupDBOptions

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    newBackupEngineOptions
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_BackupEngineOptions_newBackupEngineOptions(
    JNIEnv* env, jclass /*jcls*/, jstring jpath) {
  const char* cpath = env->GetStringUTFChars(jpath, nullptr);
  if (cpath == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  auto* bopt = new ROCKSDB_NAMESPACE::BackupEngineOptions(cpath);
  env->ReleaseStringUTFChars(jpath, cpath);
  return GET_CPLUSPLUS_POINTER(bopt);
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    backupDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_BackupEngineOptions_backupDir(JNIEnv* env,
                                                       jclass /*jcls*/,
                                                       jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return env->NewStringUTF(bopt->backup_dir.c_str());
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setBackupEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setBackupEnv(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jlong jrocks_env_handle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  auto* rocks_env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jrocks_env_handle);
  bopt->backup_env = rocks_env;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setShareTableFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setShareTableFiles(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle,
                                                             jboolean flag) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->share_table_files = flag;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    shareTableFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupEngineOptions_shareTableFiles(JNIEnv* /*env*/,
                                                              jclass /*jcls*/,
                                                              jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->share_table_files;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setInfoLog
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setInfoLog(JNIEnv* /*env*/,
                                                     jclass /*jcls*/,
                                                     jlong jhandle,
                                                     jlong /*jlogger_handle*/) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  auto* sptr_logger =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jhandle);
  bopt->info_log = sptr_logger->get();
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setSync(JNIEnv* /*env*/,
                                                  jclass /*jcls*/,
                                                  jlong jhandle,
                                                  jboolean flag) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->sync = flag;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupEngineOptions_sync(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->sync;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setDestroyOldData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setDestroyOldData(JNIEnv* /*env*/,
                                                            jclass /*jcls*/,
                                                            jlong jhandle,
                                                            jboolean flag) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->destroy_old_data = flag;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    destroyOldData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupEngineOptions_destroyOldData(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->destroy_old_data;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setBackupLogFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setBackupLogFiles(JNIEnv* /*env*/,
                                                            jclass /*jcls*/,
                                                            jlong jhandle,
                                                            jboolean flag) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->backup_log_files = flag;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    backupLogFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupEngineOptions_backupLogFiles(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->backup_log_files;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setBackupRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setBackupRateLimit(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jlong jbackup_rate_limit) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->backup_rate_limit = jbackup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    backupRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupEngineOptions_backupRateLimit(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->backup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setBackupRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setBackupRateLimiter(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jrate_limiter_handle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  auto* sptr_rate_limiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  bopt->backup_rate_limiter = *sptr_rate_limiter;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setRestoreRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setRestoreRateLimit(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jrestore_rate_limit) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->restore_rate_limit = jrestore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    restoreRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupEngineOptions_restoreRateLimit(JNIEnv* /*env*/,
                                                            jclass /*jcls*/,
                                                            jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->restore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setRestoreRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setRestoreRateLimiter(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jrate_limiter_handle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  auto* sptr_rate_limiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  bopt->restore_rate_limiter = *sptr_rate_limiter;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setShareFilesWithChecksum
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setShareFilesWithChecksum(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jboolean flag) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->share_files_with_checksum = flag;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    shareFilesWithChecksum
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupEngineOptions_shareFilesWithChecksum(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return bopt->share_files_with_checksum;
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setMaxBackgroundOperations
 * Signature: (JI)V
 */
void Java_org_rocksdb_BackupEngineOptions_setMaxBackgroundOperations(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jint max_background_operations) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->max_background_operations = static_cast<int>(max_background_operations);
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    maxBackgroundOperations
 * Signature: (J)I
 */
jint Java_org_rocksdb_BackupEngineOptions_maxBackgroundOperations(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return static_cast<jint>(bopt->max_background_operations);
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    setCallbackTriggerIntervalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupEngineOptions_setCallbackTriggerIntervalSize(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jcallback_trigger_interval_size) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  bopt->callback_trigger_interval_size =
      static_cast<uint64_t>(jcallback_trigger_interval_size);
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    callbackTriggerIntervalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupEngineOptions_callbackTriggerIntervalSize(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  return static_cast<jlong>(bopt->callback_trigger_interval_size);
}

/*
 * Class:     org_rocksdb_BackupEngineOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_BackupEngineOptions_disposeInternalJni(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle) {
  auto* bopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(jhandle);
  assert(bopt != nullptr);
  delete bopt;
}
