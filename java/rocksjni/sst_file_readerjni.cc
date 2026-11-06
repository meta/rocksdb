// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::SstFileReader methods
// from Java side.

#include <jni.h>

#include <string>

#include "include/org_rocksdb_SstFileReader.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    newSstFileReader
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileReader_newSstFileReader(JNIEnv* /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong joptions) {
  auto* options = reinterpret_cast<const ROCKSDB_NAMESPACE::Options*>(joptions);
  ROCKSDB_NAMESPACE::SstFileReader* sst_file_reader =
      new ROCKSDB_NAMESPACE::SstFileReader(*options);
  return GET_CPLUSPLUS_POINTER(sst_file_reader);
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_SstFileReader_open(JNIEnv* env, jclass /*jcls*/,
                                         jlong jhandle, jstring jfile_path) {
  const char* file_path = env->GetStringUTFChars(jfile_path, nullptr);
  if (file_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  ROCKSDB_NAMESPACE::Status s =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader*>(jhandle)->Open(
          file_path);
  env->ReleaseStringUTFChars(jfile_path, file_path);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    newIterator
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_SstFileReader_newIterator(JNIEnv* /*env*/,
                                                 jclass /*jcls*/, jlong jhandle,
                                                 jlong jread_options_handle) {
  auto* sst_file_reader =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  return GET_CPLUSPLUS_POINTER(sst_file_reader->NewIterator(*read_options));
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    newTableIterator
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_SstFileReader_newTableIterator0(JNIEnv * /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jhandle,
                                                      jboolean jhas_from,
                                                      jlong from_slice_handle,
                                                      jboolean jhas_to,
                                                      jlong to_slice_handle) {
  auto *sst_file_reader =
          reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader *>(jhandle);
  ROCKSDB_NAMESPACE::Slice* from_slice = nullptr;
  ROCKSDB_NAMESPACE::Slice* to_slice = nullptr;
  bool has_from = static_cast<bool>(jhas_from);
  bool has_to = static_cast<bool>(jhas_to);
  if (has_from) {
      from_slice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(from_slice_handle);
  }
  if (has_to) {
      to_slice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(to_slice_handle);
  }
  return GET_CPLUSPLUS_POINTER(sst_file_reader->NewTableIterator(from_slice, to_slice).release());
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    newTableIterator
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_SstFileReader_newTableIterator1(JNIEnv * /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jhandle) {
  auto *sst_file_reader =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader *>(jhandle);
  return GET_CPLUSPLUS_POINTER(sst_file_reader->NewTableIterator().release());
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileReader_disposeInternalJni(JNIEnv* /*env*/,
                                                       jclass /*jcls*/,
                                                       jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader*>(jhandle);
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    verifyChecksum
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileReader_verifyChecksum(JNIEnv* env, jclass /*jcls*/,
                                                   jlong jhandle) {
  auto* sst_file_reader =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader*>(jhandle);
  auto s = sst_file_reader->VerifyChecksum();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileReader
 * Method:    getTableProperties
 * Signature: (J)J
 */
jobject Java_org_rocksdb_SstFileReader_getTableProperties(JNIEnv* env,
                                                          jclass /*jcls*/,
                                                          jlong jhandle) {
  auto* sst_file_reader =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileReader*>(jhandle);
  std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties> tp =
      sst_file_reader->GetTableProperties();
  jobject jtable_properties =
      ROCKSDB_NAMESPACE::TablePropertiesJni::fromCppTableProperties(
          env, *(tp.get()));
  return jtable_properties;
}
