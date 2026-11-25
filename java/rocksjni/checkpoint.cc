// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Checkpoint methods from Java side.

#include "rocksdb/utilities/checkpoint.h"

#include <jni.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "include/org_rocksdb_Checkpoint.h"
#include "rocksdb/db.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    newCheckpoint
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Checkpoint_newCheckpoint(JNIEnv* /*env*/,
                                                jclass /*jclazz*/,
                                                jlong jdb_handle) {
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  ROCKSDB_NAMESPACE::Checkpoint* checkpoint;
  ROCKSDB_NAMESPACE::Checkpoint::Create(db, &checkpoint);
  return GET_CPLUSPLUS_POINTER(checkpoint);
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    dispose
 * Signature: (J)V
 */
void Java_org_rocksdb_Checkpoint_disposeInternalJni(JNIEnv* /*env*/,
                                                    jclass /*jobj*/,
                                                    jlong jhandle) {
  auto* checkpoint = reinterpret_cast<ROCKSDB_NAMESPACE::Checkpoint*>(jhandle);
  assert(checkpoint != nullptr);
  delete checkpoint;
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    createCheckpoint
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_Checkpoint_createCheckpoint(JNIEnv* env, jclass /*jcls*/,
                                                  jlong jcheckpoint_handle,
                                                  jstring jcheckpoint_path) {
  const char* checkpoint_path = env->GetStringUTFChars(jcheckpoint_path, 0);
  if (checkpoint_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* checkpoint =
      reinterpret_cast<ROCKSDB_NAMESPACE::Checkpoint*>(jcheckpoint_handle);
  ROCKSDB_NAMESPACE::Status s = checkpoint->CreateCheckpoint(checkpoint_path);

  env->ReleaseStringUTFChars(jcheckpoint_path, checkpoint_path);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    exportColumnFamily
 * Signature: (JJLjava/lang/String;)Lorg/rocksdb/ExportImportFilesMetaData;
 */
jlong Java_org_rocksdb_Checkpoint_exportColumnFamily(
    JNIEnv* env, jobject /*jobj*/, jlong jcheckpoint_handle,
    jlong jcolumn_family_handle, jstring jexport_path) {
  const char* export_path = env->GetStringUTFChars(jexport_path, 0);
  if (export_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* checkpoint =
      reinterpret_cast<ROCKSDB_NAMESPACE::Checkpoint*>(jcheckpoint_handle);

  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);

  ROCKSDB_NAMESPACE::ExportImportFilesMetaData* metadata = nullptr;

  ROCKSDB_NAMESPACE::Status s = checkpoint->ExportColumnFamily(
      column_family_handle, export_path, &metadata);

  env->ReleaseStringUTFChars(jexport_path, export_path);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  return GET_CPLUSPLUS_POINTER(metadata);
}
