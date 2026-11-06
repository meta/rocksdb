// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Iterator methods from Java side.

#include <jni.h>
#include <stdlib.h>

#include "include/org_rocksdb_ParsedEntryInfo.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/types_util.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    newParseEntryInstance
 * Signature: ()J
 */
jlong JNICALL Java_org_rocksdb_ParsedEntryInfo_newParseEntryInstance(
    JNIEnv * /*env*/, jclass /*cls*/) {
  ROCKSDB_NAMESPACE::ParsedEntryInfo *parsed_entry_info =
      new ROCKSDB_NAMESPACE::ParsedEntryInfo();
  return GET_CPLUSPLUS_POINTER(parsed_entry_info);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntry
 * Signature: (JJ[BI)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntry(
    JNIEnv *env, jclass /*cls*/, jlong handle, jlong options_handle,
    jbyteArray jtarget, jint len) {
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options *>(options_handle);
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  jbyte* target = new jbyte[len];
  env->GetByteArrayRegion(jtarget, 0, len, target);
  if (env->ExceptionCheck()) {
    return;
  }
  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char *>(target), len);
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                parsed_entry_info, true);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntryDirect
 * Signature: (JJLjava/nio/ByteBuffer;II)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntryDirect(
    JNIEnv *env, jclass /*clz*/, jlong handle, jlong options_handle,
    jobject jbuffer, jint jbuffer_off, jint jbuffer_len) {
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options *>(options_handle);
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  auto parse = [&env, &parsed_entry_info,
                &options](ROCKSDB_NAMESPACE::Slice &target_slice) {
    ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                  parsed_entry_info);
    if (!s.ok()) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
      return;
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(parse, env, jbuffer, jbuffer_off,
                                          jbuffer_len);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntryByteArray
 * Signature: (JJ[BII)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntryByteArray(
    JNIEnv *env, jclass /*clz*/, jlong handle, jlong options_handle,
    jbyteArray jtarget, jint joff, jint jlen) {
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options *>(options_handle);
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  auto parse = [&env, &parsed_entry_info,
                &options](ROCKSDB_NAMESPACE::Slice &target_slice) {
    ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                  parsed_entry_info);
    if (!s.ok()) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
      return true;
    }
    return false;
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_indirect(parse, env, jtarget, joff, jlen);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyDirect
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyDirect(
    JNIEnv *env, jclass /*clz*/, jlong handle, jobject jtarget, jint joffset,
    jint jlen) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  return ROCKSDB_NAMESPACE::JniUtil::copyToDirect(env, key_slice, jtarget,
                                                  joffset, jlen);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyByteArray
 * Signature: (J[BII)I
 */
jint JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyByteArray(
    JNIEnv *env, jclass /*clz*/, jlong handle, jbyteArray jtarget, jint joffset,
    jint jlen) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, key_slice, jtarget, joffset,
                                               jlen);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyJni
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyJni(JNIEnv *env,
                                                               jclass /*clz*/,
                                                               jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  if (jkey == nullptr) {
    // exception thrown: OutOfMemoryError
    ROCKSDB_NAMESPACE::OutOfMemoryErrorJni::ThrowNew(env, "Memory allocation failed in RocksDB JNI function");
    return nullptr;
  }
  ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, key_slice, jkey, 0,
                                        static_cast<jint>(key_slice.size()));
  return jkey;
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    getSequenceNumberJni
 * Signature: (J)J
 */
jlong JNICALL Java_org_rocksdb_ParsedEntryInfo_getSequenceNumberJni(
    JNIEnv * /*env*/, jclass /*clz*/, jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  uint64_t sequence_number = parsed_entry_info->sequence;
  return static_cast<jlong>(sequence_number);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    getValueTypeJni
 * Signature: (J)B
 */
jbyte JNICALL Java_org_rocksdb_ParsedEntryInfo_getEntryTypeJni(JNIEnv * /*env*/,
                                                               jclass /*clz*/,
                                                               jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  ROCKSDB_NAMESPACE::EntryType type = parsed_entry_info->type;
  return ROCKSDB_NAMESPACE::EntryTypeJni::toJavaEntryType(type);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_disposeInternalJni(
    JNIEnv * /*env*/, jclass /*clz*/, jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo *>(handle);
  assert(parsed_entry_info != nullptr);
  if (parsed_entry_info->copied_user_key) {
    delete parsed_entry_info->user_key.data();
  }
  delete parsed_entry_info;
}
