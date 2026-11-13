//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_util_LoggerFromOptions.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "util/stderr_logger.h"

/*
 * Create an "independent" logger, such as might be supplied to a readonly DB
 * (e.g. in a readonly filesystem)
 *
 * Class:     org_rocksdb_util_LoggerFromOptions
 * Method:    createLoggerFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_util_LoggerFromOptions_createLoggerFromOptions(
    JNIEnv* env, jclass, jstring jdb_name, jlong joptions_handle) {
  auto* db_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(joptions_handle);

  if (jdb_name == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid (null) db name."));
    return 0;
  }
  jboolean has_exception = JNI_FALSE;
  auto db_name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, jdb_name, &has_exception);  // also releases jlog_prefix
  if (has_exception == JNI_TRUE) {
    return 0;
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::Logger> logger;

  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::CreateLoggerFromOptions(db_name, *db_options, &logger);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }

  return GET_CPLUSPLUS_POINTER(
      new std::shared_ptr<ROCKSDB_NAMESPACE::Logger>(std::move(logger)));
}

/*
 * Class:     org_rocksdb_util_LoggerFromOptions
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_util_LoggerFromOptions_setInfoLogLevel(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle,
                                                             jbyte jlog_level) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Logger>*>(jhandle);
  handle->get()->SetInfoLogLevel(
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level));
}

/*
 * Class:     org_rocksdb_util_LoggerFromOptions
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_util_LoggerFromOptions_infoLogLevel(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Logger>*>(jhandle);
  return static_cast<jbyte>(handle->get()->GetInfoLogLevel());
}

/*
 * Class:     org_rocksdb_util_StdErrLogger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_util_StdErrLogger_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Logger>*>(jhandle);
  delete handle;  // delete std::shared_ptr
}
