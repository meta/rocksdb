// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::CompactRangeOptions.

#include <jni.h>

#include "include/org_rocksdb_CompactRangeOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "util/coding.h"

/**
 * @brief Class containing compact range options for Java API
 *
 * An object of this class is returned as the native handle for
 * ROCKSDB_NAMESPACE::CompactRangeOptions It contains objects for various
 * parameters which are passed by reference/pointer in CompactRangeOptions. We
 * maintain the lifetime of these parameters (`full_history_ts_low`, `canceled`)
 * by including their values in this class.
 */
class Java_org_rocksdb_CompactRangeOptions {
 public:
  ROCKSDB_NAMESPACE::CompactRangeOptions compactRangeOptions;

 private:
  std::string full_history_ts_low;
  std::atomic<bool> canceled;

 public:
  void set_full_history_ts_low(uint64_t start, uint64_t range) {
    full_history_ts_low = "";
    ROCKSDB_NAMESPACE::PutFixed64(&full_history_ts_low, start);
    ROCKSDB_NAMESPACE::PutFixed64(&full_history_ts_low, range);
    compactRangeOptions.full_history_ts_low =
        new ROCKSDB_NAMESPACE::Slice(full_history_ts_low);
  }

  bool read_full_history_ts_low(uint64_t* start, uint64_t* range) {
    if (compactRangeOptions.full_history_ts_low == nullptr) return false;
    ROCKSDB_NAMESPACE::Slice read_slice(
        compactRangeOptions.full_history_ts_low->ToStringView());
    if (!ROCKSDB_NAMESPACE::GetFixed64(&read_slice, start)) return false;
    return ROCKSDB_NAMESPACE::GetFixed64(&read_slice, range);
  }

  void set_canceled(bool value) {
    if (compactRangeOptions.canceled == nullptr) {
      canceled.store(value, std::memory_order_seq_cst);
      compactRangeOptions.canceled = &canceled;
    } else {
      compactRangeOptions.canceled->store(value, std::memory_order_seq_cst);
    }
  }

  bool get_canceled() {
    return compactRangeOptions.canceled &&
           compactRangeOptions.canceled->load(std::memory_order_seq_cst);
  }
};

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    newCompactRangeOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompactRangeOptions_newCompactRangeOptions(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* options = new Java_org_rocksdb_CompactRangeOptions();
  return GET_CPLUSPLUS_POINTER(&options->compactRangeOptions);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    exclusiveManualCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_exclusiveManualCompaction(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(
      options->compactRangeOptions.exclusive_manual_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setExclusiveManualCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setExclusiveManualCompaction(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle,
    jboolean exclusive_manual_compaction) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.exclusive_manual_compaction =
      static_cast<bool>(exclusive_manual_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    skipMissingBlobFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_skipMissingBlobFiles(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return options->compactRangeOptions.skip_missing_blob_files;
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setSkipMissingBlobFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setSkipMissingBlobFiles(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle,
    jboolean skip_missing_blob_files) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.skip_missing_blob_files =
      skip_missing_blob_files != JNI_FALSE;
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    bottommostLevelCompaction
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_bottommostLevelCompaction(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::BottommostLevelCompactionJni::
      toJavaBottommostLevelCompaction(
          options->compactRangeOptions.bottommost_level_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setBottommostLevelCompaction
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setBottommostLevelCompaction(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle,
    jint bottommost_level_compaction) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.bottommost_level_compaction =
      ROCKSDB_NAMESPACE::BottommostLevelCompactionJni::
          toCppBottommostLevelCompaction(bottommost_level_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    changeLevel
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_changeLevel(JNIEnv* /*env*/,
                                                          jclass /*jobj*/,
                                                          jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(options->compactRangeOptions.change_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setChangeLevel
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setChangeLevel(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle, jboolean change_level) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.change_level = static_cast<bool>(change_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    targetLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_targetLevel(JNIEnv* /*env*/,
                                                      jclass /*jobj*/,
                                                      jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->compactRangeOptions.target_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setTargetLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setTargetLevel(JNIEnv* /*env*/,
                                                         jclass /*jobj*/,
                                                         jlong jhandle,
                                                         jint target_level) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.target_level = static_cast<int>(target_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    targetPathId
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_targetPathId(JNIEnv* /*env*/,
                                                       jclass /*jobj*/,
                                                       jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->compactRangeOptions.target_path_id);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setTargetPathId
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setTargetPathId(JNIEnv* /*env*/,
                                                          jclass /*jobj*/,
                                                          jlong jhandle,
                                                          jint target_path_id) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.target_path_id =
      static_cast<uint32_t>(target_path_id);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    allowWriteStall
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_allowWriteStall(JNIEnv* /*env*/,
                                                              jclass /*jobj*/,
                                                              jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(options->compactRangeOptions.allow_write_stall);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setAllowWriteStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setAllowWriteStall(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle,
    jboolean allow_write_stall) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.allow_write_stall =
      static_cast<bool>(allow_write_stall);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_maxSubcompactions(JNIEnv* /*env*/,
                                                            jclass /*jobj*/,
                                                            jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->compactRangeOptions.max_subcompactions);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setMaxSubcompactions(
    JNIEnv* /*env*/, jclass /*jobj*/, jlong jhandle, jint max_subcompactions) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->compactRangeOptions.max_subcompactions =
      static_cast<uint32_t>(max_subcompactions);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setFullHistoryTSLow
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setFullHistoryTSLow(JNIEnv*, jclass,
                                                              jlong jhandle,
                                                              jlong start,
                                                              jlong range) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->set_full_history_ts_low(start, range);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    fullHistoryTSLow
 * Signature: (J)Lorg/rocksdb/CompactRangeOptions/Timestamp;
 */
jobject Java_org_rocksdb_CompactRangeOptions_fullHistoryTSLow(JNIEnv* env,
                                                              jclass,
                                                              jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  uint64_t start;
  uint64_t range;
  jobject result = nullptr;
  if (options->read_full_history_ts_low(&start, &range)) {
    result =
        ROCKSDB_NAMESPACE::CompactRangeOptionsTimestampJni::fromCppTimestamp(
            env, start, range);
  }

  return result;
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setCanceled
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setCanceled(JNIEnv*, jclass,
                                                      jlong jhandle,
                                                      jboolean jcanceled) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  options->set_canceled(jcanceled);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    canceled
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_canceled(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  return options->get_canceled();
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactRangeOptions_disposeInternalJni(JNIEnv* /*env*/,
                                                             jclass /*jcls*/,
                                                             jlong jhandle) {
  auto* options =
      reinterpret_cast<Java_org_rocksdb_CompactRangeOptions*>(jhandle);
  delete options;
}
