//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_ImportColumnFamilyOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    newImportColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ImportColumnFamilyOptions_newImportColumnFamilyOptions(
    JNIEnv*, jclass) {
  ROCKSDB_NAMESPACE::ImportColumnFamilyOptions* opts =
      new ROCKSDB_NAMESPACE::ImportColumnFamilyOptions();
  return GET_CPLUSPLUS_POINTER(opts);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    setMoveFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ImportColumnFamilyOptions_setMoveFiles(
    JNIEnv*, jobject, jlong jhandle, jboolean jmove_files) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  options->move_files = static_cast<bool>(jmove_files);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    moveFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ImportColumnFamilyOptions_moveFiles(JNIEnv*, jobject,
                                                              jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(options->move_files);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    setRejectTooOldFile
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ImportColumnFamilyOptions_setRejectTooOldFile(
    JNIEnv*, jobject, jlong jhandle, jboolean jreject_too_old_file) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  options->reject_too_old_file = static_cast<bool>(jreject_too_old_file);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    rejectTooOldFile
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ImportColumnFamilyOptions_rejectTooOldFile(
    JNIEnv*, jobject, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(options->reject_too_old_file);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ImportColumnFamilyOptions_disposeInternal(JNIEnv*,
                                                                jobject,
                                                                jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(
      jhandle);
}
