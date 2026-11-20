//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
// Created by rhubner on 29-Nov-23.
//

#include "jni_merge_operator_v2.h"

#include "include/org_rocksdb_MergeOperatorV2.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

jlong Java_org_rocksdb_MergeOperatorV2_toCString(JNIEnv* env, jclass,
                                                 jstring operator_name) {
  auto operator_name_utf = env->GetStringUTFChars(operator_name, nullptr);
  if (operator_name_utf == nullptr) {
    return 0;  // Exception
  }
  auto operator_name_len = env->GetStringUTFLength(operator_name);

  char* ret_value = new char[operator_name_len + 1];
  memcpy(ret_value, operator_name_utf, operator_name_len + 1);

  env->ReleaseStringUTFChars(operator_name, operator_name_utf);

  return GET_CPLUSPLUS_POINTER(ret_value);
}

jlong Java_org_rocksdb_MergeOperatorV2_newMergeOperator(
    JNIEnv* env, jobject java_merge_operator, jlong _operator_name) {
  char* operator_name = reinterpret_cast<char*>(_operator_name);

  auto* jni_merge_operator =
      new std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>(
          new rocksdb::JniMergeOperatorV2(env, java_merge_operator,
                                          operator_name));

  return GET_CPLUSPLUS_POINTER(jni_merge_operator);
}

void Java_org_rocksdb_MergeOperatorV2_disposeInternal(JNIEnv*, jclass,
                                                      jlong j_handle) {
  auto* jni_merge_operator =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          j_handle);
  delete jni_merge_operator;
}

namespace ROCKSDB_NAMESPACE {

JniMergeOperatorV2::JniMergeOperatorV2(JNIEnv* env, jobject java_merge_operator,
                                       char* _operator_name)
    : JniCallback(env, java_merge_operator) {
  operator_name = _operator_name;

  j_merge_class = env->GetObjectClass(java_merge_operator);
  if (j_merge_class == nullptr) {
    return;  // Exception
  }
  j_merge_class = static_cast<jclass>(env->NewGlobalRef(j_merge_class));
  if (j_merge_class == nullptr) {
    if (env->ExceptionCheck() == JNI_FALSE) {
      RocksDBExceptionJni::ThrowNew(
          env, "Unable to obtain GlobalRef for merge operator");
    }
    return;
  }

  j_merge_internal =
      env->GetMethodID(j_merge_class, "mergeInternal",
                       "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;[Ljava/nio/"
                       "ByteBuffer;)Lorg/rocksdb/MergeOperatorOutput;");
  if (j_merge_internal == nullptr) {
    return;
  }

  return_value_clazz = env->FindClass("org/rocksdb/MergeOperatorOutput");
  if (return_value_clazz == nullptr) {
    return;  // Exception
  }
  return_value_clazz =
      static_cast<jclass>(env->NewGlobalRef(return_value_clazz));
  if (return_value_clazz == nullptr) {
    return;  // Exception
  }

  return_value_method = env->GetMethodID(return_value_clazz, "getDirectValue",
                                         "()Ljava/nio/ByteBuffer;");
  if (return_value_method == nullptr) {
    return;
  }

  return_status_method =
      env->GetMethodID(return_value_clazz, "getOpStatus", "()I");
  if (return_status_method == nullptr) {
    return;
  }

  j_byte_buffer_class = ByteBufferJni::getJClass(env);
  if (j_byte_buffer_class == nullptr) {
    return;
  }
  j_byte_buffer_class =
      static_cast<jclass>(env->NewGlobalRef(j_byte_buffer_class));
  if (j_byte_buffer_class == nullptr) {
    return;  // Exception
  }

  byte_buffer_position =
      env->GetMethodID(j_byte_buffer_class, "position", "()I");
  if (byte_buffer_position == nullptr) {
    return;
  }

  byte_buffer_remaining =
      env->GetMethodID(j_byte_buffer_class, "remaining", "()I");
  if (byte_buffer_remaining == nullptr) {
    return;
  }

  return;
}

bool JniMergeOperatorV2::FullMergeV2(const MergeOperationInput& merge_in,
                                     MergeOperationOutput* merge_out) const {
  jboolean attached_thread = JNI_FALSE;
  auto env = getJniEnv(&attached_thread);

  auto j_operand_list =
      env->NewObjectArray(static_cast<jsize>(merge_in.operand_list.size()),
                          j_byte_buffer_class, nullptr);
  if (j_operand_list == nullptr) {
    return clean_and_return_error(attached_thread, merge_out);
  }

  for (auto i = 0u; i < merge_in.operand_list.size(); i++) {
    auto operand = merge_in.operand_list[i];
    auto byte_buffer = env->NewDirectByteBuffer(
        const_cast<void*>(reinterpret_cast<const void*>(operand.data())),
        operand.size());

    if (byte_buffer == nullptr) {
      return clean_and_return_error(attached_thread, merge_out);
    }
    env->SetObjectArrayElement(j_operand_list, i, byte_buffer);
  }

  auto key = env->NewDirectByteBuffer(
      const_cast<void*>(reinterpret_cast<const void*>(merge_in.key.data())),
      merge_in.key.size());
  if (key == nullptr) {
    return clean_and_return_error(attached_thread, merge_out);
  }

  jobject exising_value = nullptr;
  if (merge_in.existing_value != nullptr) {
    exising_value = env->NewDirectByteBuffer(
        const_cast<void*>(
            reinterpret_cast<const void*>(merge_in.existing_value->data())),
        merge_in.existing_value->size());
  }

  jobject result = env->CallObjectMethod(m_jcallback_obj, j_merge_internal, key,
                                         exising_value, j_operand_list);
  if (env->ExceptionCheck() == JNI_TRUE) {
    env->ExceptionClear();
    Error(merge_in.logger, "Unable to merge, Java code throw exception");
    return clean_and_return_error(attached_thread, merge_out);
  }

  if (result == nullptr) {
    Error(merge_in.logger, "Unable to merge, Java code return nullptr result");
    return clean_and_return_error(attached_thread, merge_out);
  }

  merge_out->op_failure_scope =
      javaToOpFailureScope(env->CallIntMethod(result, return_status_method));
  if (merge_out->op_failure_scope != MergeOperator::OpFailureScope::kDefault) {
    releaseJniEnv(attached_thread);
    return false;
  }

  auto result_byte_buff = env->CallObjectMethod(result, return_value_method);
  if (result_byte_buff == nullptr) {
    Error(merge_in.logger,
          "Unable to merge, Java code return nullptr ByteBuffer");
    return clean_and_return_error(attached_thread, merge_out);
  }

  auto result_byte_buff_data = env->GetDirectBufferAddress(result_byte_buff);

  auto position = env->CallIntMethod(result_byte_buff, byte_buffer_position);
  auto remaining = env->CallIntMethod(result_byte_buff, byte_buffer_remaining);

  merge_out->new_value.assign(
      static_cast<char*>(result_byte_buff_data) + position, remaining);

  releaseJniEnv(attached_thread);

  return true;
}

JniMergeOperatorV2::~JniMergeOperatorV2() {
  jboolean attached_thread = JNI_FALSE;
  auto env = getJniEnv(&attached_thread);
  env->DeleteGlobalRef(j_merge_class);
  env->DeleteGlobalRef(j_byte_buffer_class);
  env->DeleteGlobalRef(return_value_clazz);
  delete operator_name;
  releaseJniEnv(attached_thread);
}

bool JniMergeOperatorV2::clean_and_return_error(
    jboolean& attached_thread, MergeOperationOutput* merge_out) const {
  merge_out->op_failure_scope =
      MergeOperator::OpFailureScope::kOpFailureScopeMax;
  releaseJniEnv(attached_thread);
  return false;
}

MergeOperator::OpFailureScope JniMergeOperatorV2::javaToOpFailureScope(
    jint failure) const {
  switch (failure) {
    case 0:
      return MergeOperator::OpFailureScope::kDefault;
    case 1:
      return MergeOperator::OpFailureScope::kTryMerge;
    case 2:
      return MergeOperator::OpFailureScope::kMustMerge;
    case 3:
      return MergeOperator::OpFailureScope::kOpFailureScopeMax;
    default:
      return MergeOperator::OpFailureScope::kOpFailureScopeMax;
  }
}

const char* JniMergeOperatorV2::Name() const { return operator_name; }
}  // namespace ROCKSDB_NAMESPACE