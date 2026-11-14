//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
// Created by rhubner on 29-Nov-23.
//
#ifndef ROCKSDB_JNI_MERGE_OPERATOR_V2_H
#define ROCKSDB_JNI_MERGE_OPERATOR_V2_H

#include <jni.h>

#include "rocksdb/merge_operator.h"
#include "rocksjni/jnicallback.h"

namespace ROCKSDB_NAMESPACE {

class JniMergeOperatorV2 : public JniCallback, public MergeOperator {
 public:
  JniMergeOperatorV2(JNIEnv* env1, jobject java_merge_operator,
                     char* operator_name);
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;
  const char* Name() const override;
  ~JniMergeOperatorV2() override;

 private:
  MergeOperator::OpFailureScope javaToOpFailureScope(jint failure) const;
  bool clean_and_return_error(jboolean& attached_thread,
                              MergeOperationOutput* merge_out) const;
  jclass j_merge_class;
  jclass j_byte_buffer_class;
  jmethodID j_merge_internal;
  jclass return_value_clazz;
  jmethodID return_value_method;
  jmethodID return_status_method;
  jmethodID byte_buffer_position;
  jmethodID byte_buffer_remaining;
  char* operator_name;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_JNI_MERGE_OPERATOR_V2_H