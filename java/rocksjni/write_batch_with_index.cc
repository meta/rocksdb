// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatchWithIndex methods from Java side.

#include "rocksdb/utilities/write_batch_with_index.h"

#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchWithIndex.h"
#include "rocksdb/comparator.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  auto* wbwi = new ROCKSDB_NAMESPACE::WriteBatchWithIndex();
  return GET_CPLUSPLUS_POINTER(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean joverwrite_key) {
  auto* wbwi = new ROCKSDB_NAMESPACE::WriteBatchWithIndex(
      ROCKSDB_NAMESPACE::BytewiseComparator(), 0,
      static_cast<bool>(joverwrite_key));
  return GET_CPLUSPLUS_POINTER(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JBIZ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__JBIZ(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jfallback_index_comparator_handle,
    jbyte jcomparator_type, jint jreserved_bytes, jboolean joverwrite_key) {
  ROCKSDB_NAMESPACE::Comparator* fallback_comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      fallback_comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
              jfallback_index_comparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      fallback_comparator = reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(
          jfallback_index_comparator_handle);
      break;
  }
  auto* wbwi = new ROCKSDB_NAMESPACE::WriteBatchWithIndex(
      fallback_comparator, static_cast<size_t>(jreserved_bytes),
      static_cast<bool>(joverwrite_key));
  return GET_CPLUSPLUS_POINTER(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_rocksdb_WriteBatchWithIndex_count0Jni(JNIEnv* /*env*/,
                                                    jclass /*jcls*/,
                                                    jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putJni__J_3BI_3BI(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto put = [&wbwi](ROCKSDB_NAMESPACE::Slice key,
                     ROCKSDB_NAMESPACE::Slice value) {
    return wbwi->Put(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jkey, jkey_len, jentry_value,
                                        jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putJni__J_3BI_3BIJ(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                 ROCKSDB_NAMESPACE::Slice value) {
    return wbwi->Put(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jkey, jkey_len, jentry_value,
                                        jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putEntityJni
 * Signature: (J[BII[[B[[BJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putEntityJni(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint keyOffset,
    jint keyLen, jobjectArray jnames, jobjectArray jvalues, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);

  auto putEntity = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                       ROCKSDB_NAMESPACE::WideColumns columns) {
    wbwi->PutEntity(cf_handle, key, columns);
  };

  ROCKSDB_NAMESPACE::WideColumnJni::convertWideColumns(
      putEntity, env, jkey, keyOffset, keyLen, jnames, jvalues);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putDirectJni(
    JNIEnv* env, jclass /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jobject jval, jint jval_offset,
    jint jval_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto put = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key,
                               ROCKSDB_NAMESPACE::Slice& value) {
    if (cf_handle == nullptr) {
      wb->Put(key, value);
    } else {
      wb->Put(cf_handle, key, value);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
      put, env, jkey, jkey_offset, jkey_len, jval, jval_offset, jval_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putEntityDirectJni
 * Signature:
 * (JLjava/nio/ByteBuffer;II[Ljava/nio/ByteBuffer;[I[I[Ljava/nio/ByteBuffer;[I[IJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatchWithIndex_putEntityDirectJni(
    JNIEnv* env, jclass, jlong jwbwi_handle, jobject jKey, jint jKeyOffset,
    jint jKeyLen, jobjectArray jNames, jintArray jNamesOffset,
    jintArray jNamesLen, jobjectArray jValues, jintArray jvaluesOffset,
    jintArray jValuesLen, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);

  auto putEntity = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                       ROCKSDB_NAMESPACE::WideColumns columns) {
    wbwi->PutEntity(cf_handle, key, columns);
  };

  ROCKSDB_NAMESPACE::WideColumnJni::convertWideColumnsDirect(
      putEntity, env, jKey, jKeyOffset, jKeyLen, jNames, jNamesOffset,
      jNamesLen, jValues, jvaluesOffset, jValuesLen);
  return;
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_mergeJni__J_3BI_3BI(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto merge = [&wbwi](ROCKSDB_NAMESPACE::Slice key,
                       ROCKSDB_NAMESPACE::Slice value) {
    return wbwi->Merge(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_mergeJni__J_3BI_3BIJ(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                   ROCKSDB_NAMESPACE::Slice value) {
    return wbwi->Merge(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteJni__J_3BI(JNIEnv* env, jclass,
                                                           jlong jwbwi_handle,
                                                           jbyteArray jkey,
                                                           jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto remove = [&wbwi](ROCKSDB_NAMESPACE::Slice key) {
    return wbwi->Delete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteJni__J_3BIJ(JNIEnv* env, jclass,
                                                            jlong jwbwi_handle,
                                                            jbyteArray jkey,
                                                            jint jkey_len,
                                                            jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wbwi->Delete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDeleteJni__J_3BI(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto single_delete = [&wbwi](ROCKSDB_NAMESPACE::Slice key) {
    return wbwi->SingleDelete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDeleteJni__J_3BIJ(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jkey, jint jkey_len,
    jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto single_delete = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wbwi->SingleDelete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteDirect
 * Signature: (JLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteDirectJni(
    JNIEnv* env, jclass /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto remove = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key) {
    if (cf_handle == nullptr) {
      wb->Delete(key);
    } else {
      wb->Delete(cf_handle, key);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRangeJni__J_3BI_3BI(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto deleteRange = [&wbwi](ROCKSDB_NAMESPACE::Slice beginKey,
                             ROCKSDB_NAMESPACE::Slice endKey) {
    return wbwi->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRangeJni__J_3BI_3BIJ(
    JNIEnv* env, jclass, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto deleteRange = [&wbwi, &cf_handle](ROCKSDB_NAMESPACE::Slice beginKey,
                                         ROCKSDB_NAMESPACE::Slice endKey) {
    return wbwi->DeleteRange(cf_handle, beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putLogDataJni(JNIEnv* env, jclass,
                                                        jlong jwbwi_handle,
                                                        jbyteArray jblob,
                                                        jint jblob_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto putLogData = [&wbwi](ROCKSDB_NAMESPACE::Slice blob) {
    return wbwi->PutLogData(blob);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(putLogData, env, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    clear
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_clear0Jni(JNIEnv* /*env*/,
                                                    jclass /*jobj*/,
                                                    jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setSavePoint0Jni(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetSavePoint();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_rollbackToSavePoint0Jni(
    JNIEnv* env, jclass /*jobj*/, jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_popSavePointJni(JNIEnv* env,
                                                          jclass /*jobj*/,
                                                          jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->PopSavePoint();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setMaxBytesJni(JNIEnv* /*env*/,
                                                         jclass /*cls*/,
                                                         jlong jwbwi_handle,
                                                         jlong jmax_bytes) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getWriteBatch
 * Signature: (J)Lorg/rocksdb/WriteBatch;
 */
jobject Java_org_rocksdb_WriteBatchWithIndex_getWriteBatchJni(
    JNIEnv* env, jclass /*jobj*/, jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto* wb = wbwi->GetWriteBatch();

  // TODO(AR) is the `wb` object owned by us?
  return ROCKSDB_NAMESPACE::WriteBatchJni::construct(env, wb);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator0(JNIEnv* /*env*/,
                                                     jclass /*jcls*/,
                                                     jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wbwi_iterator = wbwi->NewIterator();
  return GET_CPLUSPLUS_POINTER(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator1(JNIEnv* /*env*/,
                                                     jclass /*jcls*/,
                                                     jlong jwbwi_handle,
                                                     jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* wbwi_iterator = wbwi->NewIterator(cf_handle);
  return GET_CPLUSPLUS_POINTER(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iteratorWithBase(
    JNIEnv*, jclass, jlong jwbwi_handle, jlong jcf_handle,
    jlong jbase_iterator_handle, jlong jread_opts_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* base_iterator =
      reinterpret_cast<ROCKSDB_NAMESPACE::Iterator*>(jbase_iterator_handle);
  ROCKSDB_NAMESPACE::ReadOptions* read_opts =
      jread_opts_handle == 0
          ? nullptr
          : reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(
                jread_opts_handle);
  auto* iterator =
      wbwi->NewIteratorWithBase(cf_handle, base_iterator, read_opts);
  return GET_CPLUSPLUS_POINTER(iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BI)[B
 */
jbyteArray JNICALL Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BI(
    JNIEnv* env, jclass /*jcls*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdbopt_handle);

  auto getter = [&wbwi, &dbopt](const ROCKSDB_NAMESPACE::Slice& key,
                                std::string* value) {
    return wbwi->GetFromBatch(*dbopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BIJ(
    JNIEnv* env, jclass /*jcls*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdbopt_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &cf_handle, &dbopt](const ROCKSDB_NAMESPACE::Slice& key,
                                            std::string* value) {
    return wbwi->GetFromBatch(cf_handle, *dbopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BI)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BI(
    JNIEnv* env, jclass /*jcls*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jreadopt_handle);

  auto getter = [&wbwi, &db, &readopt](const ROCKSDB_NAMESPACE::Slice& key,
                                       std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BIJ(
    JNIEnv* env, jclass /*jcls*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jreadopt_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &db, &cf_handle, &readopt](
                    const ROCKSDB_NAMESPACE::Slice& key, std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, cf_handle, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_disposeInternalJni(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(handle);
  assert(wbwi != nullptr);
  delete wbwi;
}

/* WBWIRocksIterator below */

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_disposeInternalJni(JNIEnv* /*env*/,
                                                           jclass /*jobj*/,
                                                           jlong handle) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  assert(it != nullptr);
  delete it;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WBWIRocksIterator_isValid0Jni(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jlong handle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToFirst0Jni(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToLast0Jni(JNIEnv* /*env*/,
                                                       jclass /*jcls*/,
                                                       jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_next0Jni(JNIEnv* /*env*/,
                                                 jclass /*jcls*/,
                                                 jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_prev0Jni(JNIEnv* /*env*/,
                                                 jclass /*jcls*/,
                                                 jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle)->Prev();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seek0Jni(JNIEnv* env, jclass /*jcls*/,
                                                 jlong handle,
                                                 jbyteArray jtarget,
                                                 jint jtarget_len) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  jbyte* target = new jbyte[jtarget_len];
  env->GetByteArrayRegion(jtarget, 0, jtarget_len, target);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] target;
    return;
  }

  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  it->Seek(target_slice);

  delete[] target;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekDirect0Jni(
    JNIEnv* env, jclass /*jcls*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  auto seek = [&it](ROCKSDB_NAMESPACE::Slice& target_slice) {
    it->Seek(target_slice);
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekByteArray0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekByteArray0Jni(
    JNIEnv* env, jclass /*jcls*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  const std::unique_ptr<char[]> target(new char[jtarget_len]);
  if (target == nullptr) {
    jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
    env->ThrowNew(oom_class,
                  "Memory allocation failed in RocksDB JNI function");
    return;
  }
  env->GetByteArrayRegion(jtarget, jtarget_off, jtarget_len,
                          reinterpret_cast<jbyte*>(target.get()));

  ROCKSDB_NAMESPACE::Slice target_slice(target.get(), jtarget_len);

  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  it->Seek(target_slice);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrev0Jni(JNIEnv* env,
                                                        jclass /*jcls*/,
                                                        jlong handle,
                                                        jbyteArray jtarget,
                                                        jint jtarget_len) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  jbyte* target = new jbyte[jtarget_len];
  env->GetByteArrayRegion(jtarget, 0, jtarget_len, target);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] target;
    return;
  }

  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  it->SeekForPrev(target_slice);

  delete[] target;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrevDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrevDirect0Jni(
    JNIEnv* env, jclass /*jcls*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  auto seek_for_prev = [&it](ROCKSDB_NAMESPACE::Slice& target_slice) {
    it->SeekForPrev(target_slice);
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seek_for_prev, env, jtarget,
                                          jtarget_off, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrevByteArray0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrevByteArray0Jni(
    JNIEnv* env, jclass /*jcls*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  const std::unique_ptr<char[]> target(new char[jtarget_len]);
  if (target == nullptr) {
    jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
    env->ThrowNew(oom_class,
                  "Memory allocation failed in RocksDB JNI function");
    return;
  }
  env->GetByteArrayRegion(jtarget, jtarget_off, jtarget_len,
                          reinterpret_cast<jbyte*>(target.get()));

  ROCKSDB_NAMESPACE::Slice target_slice(target.get(), jtarget_len);

  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  it->SeekForPrev(target_slice);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_status0Jni(JNIEnv* env, jclass /*jcls*/,
                                                   jlong handle) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  ROCKSDB_NAMESPACE::Status s = it->status();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    entry1
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_WBWIRocksIterator_entry1(JNIEnv* env,
                                                     jclass /*jobj*/,
                                                     jlong handle) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::WBWIIterator*>(handle);
  const ROCKSDB_NAMESPACE::WriteEntry& we = it->Entry();

  jlong results[3];

  // set the type of the write entry
  results[0] = ROCKSDB_NAMESPACE::WriteTypeJni::toJavaWriteType(we.type);

  // NOTE: key_slice and value_slice will be freed by
  // org.rocksdb.DirectSlice#close

  auto* key_slice = new ROCKSDB_NAMESPACE::Slice(we.key.data(), we.key.size());
  results[1] = GET_CPLUSPLUS_POINTER(key_slice);
  if (we.type == ROCKSDB_NAMESPACE::kDeleteRecord ||
      we.type == ROCKSDB_NAMESPACE::kSingleDeleteRecord ||
      we.type == ROCKSDB_NAMESPACE::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    results[2] = 0;
  } else {
    auto* value_slice =
        new ROCKSDB_NAMESPACE::Slice(we.value.data(), we.value.size());
    results[2] = GET_CPLUSPLUS_POINTER(value_slice);
  }

  jlongArray jresults = env->NewLongArray(3);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, 3, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    refresh0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_refresh0Jni(JNIEnv* env,
                                                    jobject /*jobj*/,
                                                    jlong /*handle*/) {
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::Status::NotSupported("Refresh() is not supported");
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    refresh1
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WBWIRocksIterator_refresh1(JNIEnv* env, jobject /*jobj*/,
                                                 jlong /*handle*/,
                                                 jlong /*snapshot_handle*/) {
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::Status::NotSupported(
      "Refresh(Snapshot*) is not supported");
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}
