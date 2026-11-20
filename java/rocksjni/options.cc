// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Options.

#include "rocksdb/options.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <vector>

#include "include/org_rocksdb_ColumnFamilyOptions.h"
#include "include/org_rocksdb_ComparatorOptions.h"
#include "include/org_rocksdb_DBOptions.h"
#include "include/org_rocksdb_FlushOptions.h"
#include "include/org_rocksdb_Options.h"
#include "include/org_rocksdb_ReadOptions.h"
#include "include/org_rocksdb_WriteOptions.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"
#include "rocksjni/table_filter_jnicallback.h"
#include "rocksjni/table_properties_collector_factory.h"
#include "util/stderr_logger.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_Options_newOptions__(JNIEnv*, jclass) {
  auto* op = new ROCKSDB_NAMESPACE::Options();
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_Options_newOptions__JJ(JNIEnv*, jclass, jlong jdboptions,
                                              jlong jcfoptions) {
  auto* dbOpt =
      reinterpret_cast<const ROCKSDB_NAMESPACE::DBOptions*>(jdboptions);
  auto* cfOpt = reinterpret_cast<const ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
      jcfoptions);
  auto* op = new ROCKSDB_NAMESPACE::Options(*dbOpt, *cfOpt);
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    copyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_copyOptions(JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::Options(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_disposeInternalJni(JNIEnv*, jclass,
                                                 jlong handle) {
  auto* op = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(handle);
  assert(op != nullptr);
  delete op;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setIncreaseParallelism(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jint totalThreads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateIfMissing(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->create_if_missing =
      flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createIfMissing(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_if_missing;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateMissingColumnFamilies(JNIEnv*, jclass,
                                                             jlong jhandle,
                                                             jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createMissingColumnFamilies(JNIEnv*, jclass,
                                                              jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JI(JNIEnv*, jclass,
                                                      jlong jhandle,
                                                      jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->comparator =
          ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->comparator =
          ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JJB(JNIEnv*, jclass,
                                                       jlong jopt_handle,
                                                       jlong jcomparator_handle,
                                                       jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperatorName(JNIEnv* env, jclass,
                                                   jlong jhandle,
                                                   jstring jop_name) {
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);

  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperator(JNIEnv*, jclass, jlong jhandle,
                                               jlong mergeOperatorHandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionFilterHandle(
    JNIEnv*, jclass, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter*>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void JNICALL Java_org_rocksdb_Options_setCompactionFilterFactoryHandle(
    JNIEnv*, jclass, jlong jopt_handle, jlong jcompactionfilterfactory_handle) {
  auto* cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactory>*>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_Options_setWriteBufferSize(JNIEnv* env, jclass,
                                                 jlong jhandle,
                                                 jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->write_buffer_size =
        jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteBufferManager(
    JNIEnv*, jclass, jlong joptions_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jwrite_buffer_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle)
      ->write_buffer_manager = *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeBufferSize(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumber(
    JNIEnv*, jclass, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setStatistics(JNIEnv*, jclass, jlong jhandle,
                                            jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* pSptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni>*>(
          jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statistics(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>* pSptr =
        new std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>(sptr);
    return GET_CPLUSPLUS_POINTER(pSptr);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumber(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_errorIfExists(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->error_if_exists;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setErrorIfExists(JNIEnv*, jclass, jlong jhandle,
                                               jboolean error_if_exists) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidChecks(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->paranoid_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidChecks(JNIEnv*, jclass, jlong jhandle,
                                                jboolean paranoid_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    openFilesAsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_openFilesAsync(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->open_files_async;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setOpenFilesAsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setOpenFilesAsync(JNIEnv*, jclass, jlong jhandle,
                                                jboolean open_files_async) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->open_files_async =
      static_cast<bool>(open_files_async);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setEnv(JNIEnv*, jclass, jlong jhandle,
                                     jlong jenv) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTotalWalSize(JNIEnv*, jclass, jlong jhandle,
                                                 jlong jmax_total_wal_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTotalWalSize(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxOpenFiles(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_open_files;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxOpenFiles(JNIEnv*, jclass, jlong jhandle,
                                              jint max_open_files) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxFileOpeningThreads(
    JNIEnv*, jclass, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_file_opening_threads = static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxFileOpeningThreads(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useFsync(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseFsync(JNIEnv*, jclass, jlong jhandle,
                                          jboolean use_fsync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_setDbPaths(JNIEnv* env, jclass, jlong jhandle,
                                         jobjectArray jpaths,
                                         jlongArray jtarget_sizes) {
  std::vector<ROCKSDB_NAMESPACE::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        ROCKSDB_NAMESPACE::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbPathsLen(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_dbPaths(JNIEnv* env, jclass, jlong jhandle,
                                      jobjectArray jpaths,
                                      jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy == JNI_TRUE ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_dbLogDir(JNIEnv* env, jclass, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
          ->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setDbLogDir(JNIEnv* env, jclass, jlong jhandle,
                                          jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->db_log_dir.assign(
      log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_walDir(JNIEnv* env, jclass, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setWalDir(JNIEnv* env, jclass, jlong jhandle,
                                        jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_dir.assign(
      wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_deleteObsoleteFilesPeriodMicros(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDeleteObsoleteFilesPeriodMicros(JNIEnv*,
                                                                 jclass,
                                                                 jlong jhandle,
                                                                 jlong micros) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundCompactions(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundCompactions(JNIEnv*, jclass,
                                                          jlong jhandle,
                                                          jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxSubcompactions(JNIEnv*, jclass,
                                                   jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxSubcompactions(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_subcompactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundFlushes(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_flushes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundFlushes(
    JNIEnv*, jclass, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_flushes = static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundJobs(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_jobs;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundJobs(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jint max_background_jobs) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_background_jobs =
      static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxLogFileSize(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_log_file_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxLogFileSize(JNIEnv* env, jclass,
                                                jlong jhandle,
                                                jlong max_log_file_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_log_file_size =
        max_log_file_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logFileTimeToRoll(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogFileTimeToRoll(
    JNIEnv* env, jclass, jlong jhandle, jlong log_file_time_to_roll) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->log_file_time_to_roll = log_file_time_to_roll;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_keepLogFileNum(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setKeepLogFileNum(JNIEnv* env, jclass,
                                                jlong jhandle,
                                                jlong keep_log_file_num) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->keep_log_file_num =
        keep_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_recycleLogFileNum(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRecycleLogFileNum(JNIEnv* env, jclass,
                                                   jlong jhandle,
                                                   jlong recycle_log_file_num) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->recycle_log_file_num = recycle_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxManifestFileSize(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_manifest_file_size;
}

/*
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_memTableFactoryName(JNIEnv* env, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

static std::vector<ROCKSDB_NAMESPACE::DbPath>
rocksdb_convert_cf_paths_from_java_helper(JNIEnv* env, jobjectArray path_array,
                                          jlongArray size_array,
                                          jboolean* has_exception) {
  jboolean copy_str_has_exception;
  std::vector<std::string> paths = ROCKSDB_NAMESPACE::JniUtil::copyStrings(
      env, path_array, &copy_str_has_exception);
  if (JNI_TRUE == copy_str_has_exception) {
    // Exception thrown
    *has_exception = JNI_TRUE;
    return {};
  }

  if (static_cast<size_t>(env->GetArrayLength(size_array)) != paths.size()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env,
        ROCKSDB_NAMESPACE::Status::InvalidArgument(
            ROCKSDB_NAMESPACE::Slice("There should be a corresponding target "
                                     "size for every path and vice versa.")));
    *has_exception = JNI_TRUE;
    return {};
  }

  jlong* size_array_ptr = env->GetLongArrayElements(size_array, nullptr);
  if (nullptr == size_array_ptr) {
    // exception thrown: OutOfMemoryError
    *has_exception = JNI_TRUE;
    return {};
  }
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths;
  for (size_t i = 0; i < paths.size(); ++i) {
    jlong target_size = size_array_ptr[i];
    if (target_size < 0) {
      ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
          env,
          ROCKSDB_NAMESPACE::Status::InvalidArgument(ROCKSDB_NAMESPACE::Slice(
              "Path target size has to be positive.")));
      *has_exception = JNI_TRUE;
      env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);
      return {};
    }
    cf_paths.push_back(ROCKSDB_NAMESPACE::DbPath(
        paths[i], static_cast<uint64_t>(target_size)));
  }

  env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);

  return cf_paths;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_setCfPaths(JNIEnv* env, jclass, jlong jhandle,
                                         jobjectArray path_array,
                                         jlongArray size_array) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_cfPathsLen(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->cf_paths.size());
}

template <typename T>
static void rocksdb_convert_cf_paths_to_java_helper(JNIEnv* env, jlong jhandle,
                                                    jobjectArray jpaths,
                                                    jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<T*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath cf_path = opt->cf_paths[i];

    jstring jpath = env->NewStringUTF(cf_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(cf_path.target_size);

    env->DeleteLocalRef(jpath);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_cfPaths(JNIEnv* env, jclass, jlong jhandle,
                                      jobjectArray jpaths,
                                      jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<ROCKSDB_NAMESPACE::Options>(
      env, jhandle, jpaths, jtarget_sizes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxManifestFileSize(
    JNIEnv*, jclass, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_manifest_file_size = static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemTableFactory(JNIEnv*, jclass, jlong jhandle,
                                                 jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::MemTableRepFactory*>(
              jfactory_handle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRateLimiter(JNIEnv*, jclass, jlong jhandle,
                                             jlong jrate_limiter_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->rate_limiter =
      *pRateLimiter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSstFileManager(
    JNIEnv*, jclass, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogger
 * Signature: (JJB)V
 */
void Java_org_rocksdb_Options_setLogger(JNIEnv* env, jclass, jlong jhandle,
                                        jlong jlogger_handle,
                                        jbyte jlogger_type) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  switch (jlogger_type) {
    case 0x1:
      // JAVA_IMPLEMENTATION
      options->info_log =
          *(reinterpret_cast<
              std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
              jlogger_handle));
      break;
    case 0x2:
      // STDERR_IMPLEMENTATION
      options->info_log =
          *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>*>(
              jlogger_handle));
      break;
    default:
      ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
          env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                   ROCKSDB_NAMESPACE::Slice("Unknown value for LoggerType")));
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setInfoLogLevel(JNIEnv*, jclass, jlong jhandle,
                                              jbyte jlog_level) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->info_log_level =
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_infoLogLevel(JNIEnv*, jclass, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_tableCacheNumshardbits(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTableCacheNumshardbits(
    JNIEnv*, jclass, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->table_cache_numshardbits = static_cast<int>(table_cache_numshardbits);
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useFixedLengthPrefixExtractor(
    JNIEnv*, jclass, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useCappedPrefixExtractor(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walTtlSeconds(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalTtlSeconds(JNIEnv*, jclass, jlong jhandle,
                                               jlong WAL_ttl_seconds) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walSizeLimitMB(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalSizeLimitMB(JNIEnv*, jclass, jlong jhandle,
                                                jlong WAL_size_limit_MB) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBatchGroupSizeBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxWriteBatchGroupSizeBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_write_batch_group_size_bytes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_write_batch_group_size_bytes =
      static_cast<uint64_t>(jmax_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBatchGroupSizeBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxWriteBatchGroupSizeBytes(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->max_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_manifestPreallocationSize(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setManifestPreallocationSize(
    JNIEnv* env, jclass, jlong jhandle, jlong preallocation_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      preallocation_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTableFactory(JNIEnv*, jclass, jlong jhandle,
                                              jlong jtable_factory_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* table_factory =
      reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory*>(jtable_factory_handle);
  options->table_factory.reset(table_factory);
}

/*
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSstPartitionerFactory(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jlong factory_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory>*>(
      factory_handle);
  options->sst_partitioner_factory = *factory;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionThreadLimiter(
    JNIEnv*, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(
      jlimiter_handle);
  options->compaction_thread_limiter = *limiter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapReads(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapReads(JNIEnv*, jclass, jlong jhandle,
                                                jboolean allow_mmap_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapWrites(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapWrites(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean allow_mmap_writes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectReads(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectReads(JNIEnv*, jclass, jlong jhandle,
                                                jboolean use_direct_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectIoForFlushAndCompaction(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectIoForFlushAndCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jclass, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowFAllocate(JNIEnv*, jclass, jlong jhandle,
                                                jboolean jallow_fallocate) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowFAllocate(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_isFdCloseOnExec(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setIsFdCloseOnExec(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean is_fd_close_on_exec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->is_fd_close_on_exec =
      static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsDumpPeriodSec(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsDumpPeriodSec(
    JNIEnv*, jclass, jlong jhandle, jint jstats_dump_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_dump_period_sec =
      static_cast<unsigned int>(jstats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxCompactionTriggerWakeupSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxCompactionTriggerWakeupSeconds(
    JNIEnv*, jclass, jlong jhandle, jlong jval) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_compaction_trigger_wakeup_seconds = static_cast<uint64_t>(jval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxCompactionTriggerWakeupSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxCompactionTriggerWakeupSeconds(
    JNIEnv*, jclass, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
          ->max_compaction_trigger_wakeup_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsPersistPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsPersistPeriodSec(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_persist_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsPersistPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsPersistPeriodSec(
    JNIEnv*, jclass, jlong jhandle, jint jstats_persist_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_persist_period_sec =
      static_cast<unsigned int>(jstats_persist_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsHistoryBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statsHistoryBufferSize(JNIEnv*, jclass,
                                                      jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_history_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsHistoryBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setStatsHistoryBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jstats_history_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_history_buffer_size =
      static_cast<size_t>(jstats_history_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_adviseRandomOnOpen(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAdviseRandomOnOpen(
    JNIEnv*, jclass, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->advise_random_on_open = static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDbWriteBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbWriteBufferSize(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionReadaheadSize(
    JNIEnv*, jclass, jlong jhandle, jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_compactionReadaheadSize(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWritableFileMaxBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writableFileMaxBufferSize(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useAdaptiveMutex(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseAdaptiveMutex(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean use_adaptive_mutex) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bytesPerSync(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBytesPerSync(JNIEnv*, jclass, jlong jhandle,
                                              jlong bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalBytesPerSync(JNIEnv*, jclass, jlong jhandle,
                                                 jlong jwal_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walBytesPerSync(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStrictBytesPerSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setStrictBytesPerSync(
    JNIEnv*, jclass, jlong jhandle, jboolean jstrict_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->strict_bytes_per_sync = jstrict_bytes_per_sync == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    strictBytesPerSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_strictBytesPerSync(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->strict_bytes_per_sync);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static void rocksdb_set_event_listeners_helper(
    JNIEnv* env, jlongArray jlistener_array,
    std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>&
        listener_sptr_vec) {
  jlong* ptr_jlistener_array =
      env->GetLongArrayElements(jlistener_array, nullptr);
  if (ptr_jlistener_array == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  const jsize array_size = env->GetArrayLength(jlistener_array);
  listener_sptr_vec.clear();
  for (jsize i = 0; i < array_size; ++i) {
    const auto& listener_sptr =
        *reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>*>(
            ptr_jlistener_array[i]);
    listener_sptr_vec.push_back(listener_sptr);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEventListeners
 * Signature: (J[J)V
 */
void Java_org_rocksdb_Options_setEventListeners(JNIEnv* env, jclass,
                                                jlong jhandle,
                                                jlongArray jlistener_array) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  rocksdb_set_event_listeners_helper(env, jlistener_array, opt->listeners);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static jobjectArray rocksdb_get_event_listeners_helper(
    JNIEnv* env,
    const std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>&
        listener_sptr_vec) {
  jsize sz = static_cast<jsize>(listener_sptr_vec.size());
  jclass jlistener_clazz =
      ROCKSDB_NAMESPACE::AbstractEventListenerJni::getJClass(env);
  jobjectArray jlisteners = env->NewObjectArray(sz, jlistener_clazz, nullptr);
  if (jlisteners == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  for (jsize i = 0; i < sz; ++i) {
    const auto* jni_cb =
        static_cast<ROCKSDB_NAMESPACE::EventListenerJniCallback*>(
            listener_sptr_vec[i].get());
    env->SetObjectArrayElement(jlisteners, i, jni_cb->GetJavaObject());
  }
  return jlisteners;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    eventListeners
 * Signature: (J)[Lorg/rocksdb/AbstractEventListener;
 */
jobjectArray Java_org_rocksdb_Options_eventListeners(JNIEnv* env, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return rocksdb_get_event_listeners_helper(env, opt->listeners);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableThreadTracking(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->enable_thread_tracking = static_cast<bool>(jenable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableThreadTracking(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDelayedWriteRate(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_delayedWriteRate(JNIEnv*, jclass,
                                                jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnablePipelinedWrite(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enablePipelinedWrite(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUnorderedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUnorderedWrite(JNIEnv*, jclass, jlong jhandle,
                                                jboolean unordered_write) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->unordered_write =
      static_cast<bool>(unordered_write);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    unorderedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_unorderedWrite(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->unordered_write;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowConcurrentMemtableWrite(JNIEnv*, jclass,
                                                              jlong jhandle,
                                                              jboolean allow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowConcurrentMemtableWrite(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jclass, jlong jhandle, jboolean yield) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableWriteThreadAdaptiveYield(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadMaxYieldUsec(JNIEnv*, jclass,
                                                         jlong jhandle,
                                                         jlong max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_max_yield_usec = static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadMaxYieldUsec(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadSlowYieldUsec(JNIEnv*, jclass,
                                                          jlong jhandle,
                                                          jlong slow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_slow_yield_usec = static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadSlowYieldUsec(JNIEnv*, jclass,
                                                        jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jclass, jlong jhandle, jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_skipStatsUpdateOnDbOpen(JNIEnv*, jclass,
                                                          jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setWalRecoveryMode(
    JNIEnv*, jclass, jlong jhandle, jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->wal_recovery_mode =
      ROCKSDB_NAMESPACE::WALRecoveryModeJni::toCppWALRecoveryMode(
          jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_walRecoveryMode(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllow2pc(JNIEnv*, jclass, jlong jhandle,
                                          jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allow2pc(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRowCache(JNIEnv*, jclass, jlong jhandle,
                                          jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jrow_cache_handle);
  opt->row_cache = *row_cache;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalFilter(JNIEnv*, jclass, jlong jhandle,
                                           jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* wal_filter = reinterpret_cast<ROCKSDB_NAMESPACE::WalFilterJniCallback*>(
      jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDumpMallocStats(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_dumpMallocStats(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringRecovery(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidUnnecessaryBlockingIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidUnnecessaryBlockingIO(
    JNIEnv*, jclass, jlong jhandle, jboolean avoid_blocking_io) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_unnecessary_blocking_io = static_cast<bool>(avoid_blocking_io);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidUnnecessaryBlockingIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidUnnecessaryBlockingIO(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_unnecessary_blocking_io);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPersistStatsToDisk
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setPersistStatsToDisk(
    JNIEnv*, jclass, jlong jhandle, jboolean persist_stats_to_disk) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->persist_stats_to_disk = static_cast<bool>(persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    persistStatsToDisk
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_persistStatsToDisk(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteDbidToManifest
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setWriteDbidToManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jwrite_dbid_to_manifest) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->write_dbid_to_manifest = static_cast<bool>(jwrite_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeDbidToManifest
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_writeDbidToManifest(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->write_dbid_to_manifest);
}

void Java_org_rocksdb_Options_setTrackOptionsFileNumberInManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jtrack) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->track_options_file_number_in_manifest = static_cast<bool>(jtrack);
}

jboolean Java_org_rocksdb_Options_trackOptionsFileNumberInManifest(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->track_options_file_number_in_manifest);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogReadaheadSize(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jlong jlog_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->log_readahead_size = static_cast<size_t>(jlog_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logReasaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logReadaheadSize(JNIEnv*, jclass,
                                                jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->log_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBestEffortsRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setBestEffortsRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean jbest_efforts_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->best_efforts_recovery = static_cast<bool>(jbest_efforts_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bestEffortsRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_bestEffortsRecovery(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->best_efforts_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBgErrorResumeCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBgErrorResumeCount(
    JNIEnv*, jclass, jlong jhandle, jint jmax_bgerror_resume_count) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_bgerror_resume_count = static_cast<int>(jmax_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBgerrorResumeCount
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBgerrorResumeCount(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jint>(opt->max_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBgerrorResumeRetryInterval
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBgerrorResumeRetryInterval(
    JNIEnv*, jclass, jlong jhandle, jlong jbgerror_resume_retry_interval) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->bgerror_resume_retry_interval =
      static_cast<uint64_t>(jbgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bgerrorResumeRetryInterval
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bgerrorResumeRetryInterval(JNIEnv*, jclass,
                                                          jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->bgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDailyOffpeakTimeUTC
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_Options_setDailyOffpeakTimeUTC(JNIEnv* env, jclass,
                                                     jlong jhandle,
                                                     jstring jtimeutc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  jboolean has_exception;
  auto timeutc =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jtimeutc, &has_exception);
  if (has_exception == JNI_TRUE) {
    return;
  }
  opt->daily_offpeak_time_utc = timeutc;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dailyOffpeakTimeUTC
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Options_dailyOffpeakTimeUTC(JNIEnv* env, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return env->NewStringUTF(opt->daily_offpeak_time_utc.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringShutdown(
    JNIEnv*, jclass, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringShutdown(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowIngestBehind(
    JNIEnv*, jclass, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowIngestBehind(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setTwoWriteQueues(JNIEnv*, jclass, jlong jhandle,
                                                jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_twoWriteQueues(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setManualWalFlush(JNIEnv*, jclass, jlong jhandle,
                                                jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_manualWalFlush(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAtomicFlush(JNIEnv*, jclass, jlong jhandle,
                                             jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_atomicFlush(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_tableFactoryName(JNIEnv* env, jclass,
                                                  jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  ROCKSDB_NAMESPACE::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minWriteBufferNumberToMerge(JNIEnv*, jclass,
                                                          jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinWriteBufferNumberToMerge(
    JNIEnv*, jclass, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compressionType(JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      opts->compression);
}

/**
 * Helper method to convert a Java byte array of compression levels
 * to a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A std::unique_ptr to the vector, or std::unique_ptr(nullptr) if a JNI
 * exception occurs
 */
std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
rocksdb_compression_vector_helper(JNIEnv* env, jbyteArray jcompression_levels) {
  jsize len = env->GetArrayLength(jcompression_levels);
  jbyte* jcompression_level =
      env->GetByteArrayElements(jcompression_levels, nullptr);
  if (jcompression_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>();
  }

  auto* compression_levels =
      new std::vector<ROCKSDB_NAMESPACE::CompressionType>();
  std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
      uptr_compression_levels(compression_levels);

  for (jsize i = 0; i < len; i++) {
    jbyte jcl = jcompression_level[i];
    compression_levels->push_back(
        static_cast<ROCKSDB_NAMESPACE::CompressionType>(jcl));
  }

  env->ReleaseByteArrayElements(jcompression_levels, jcompression_level,
                                JNI_ABORT);

  return uptr_compression_levels;
}

/**
 * Helper method to convert a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 * to a Java byte array of compression levels
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A jbytearray or nullptr if an exception occurs
 */
jbyteArray rocksdb_compression_list_helper(
    JNIEnv* env,
    std::vector<ROCKSDB_NAMESPACE::CompressionType> compression_levels) {
  const size_t len = compression_levels.size();
  jbyte* jbuf = new jbyte[len];

  for (size_t i = 0; i < len; i++) {
    jbuf[i] = compression_levels[i];
  }

  // insert in java array
  jbyteArray jcompression_levels = env->NewByteArray(static_cast<jsize>(len));
  if (jcompression_levels == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] jbuf;
    return nullptr;
  }
  env->SetByteArrayRegion(jcompression_levels, 0, static_cast<jsize>(len),
                          jbuf);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jcompression_levels);
    delete[] jbuf;
    return nullptr;
  }

  delete[] jbuf;

  return jcompression_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_Options_setCompressionPerLevel(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jcompressionLevels) {
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_Options_compressionPerLevel(JNIEnv* env, jclass,
                                                        jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return rocksdb_compression_list_helper(env, options->compression_per_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type_value) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_bottommostCompressionType(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      options->bottommost_compression);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionOptions(
    JNIEnv*, jclass, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jbottommost_compression_options_handle);
  options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompressionOptions(
    JNIEnv*, jclass, jlong jhandle, jlong jcompression_options_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jcompression_options_handle);
  options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionStyle(JNIEnv*, jclass, jlong jhandle,
                                                 jbyte jcompaction_style) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionStyle(JNIEnv*, jclass, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      options->compaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTableFilesSizeFIFO(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTableFilesSizeFIFO(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_numLevels(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->num_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setNumLevels(JNIEnv*, jclass, jlong jhandle,
                                           jint jnum_levels) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->num_levels =
      static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroFileNumCompactionTrigger(JNIEnv*, jclass,
                                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroSlowdownWritesTrigger(JNIEnv*, jclass,
                                                             jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroStopWritesTrigger(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroStopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_targetFileSizeBase(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeBase(
    JNIEnv*, jclass, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_targetFileSizeMultiplier(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeMultiplier(
    JNIEnv*, jclass, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxBytesForLevelBase(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelBase(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_levelCompactionDynamicLevelBytes(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_maxBytesForLevelMultiplier(JNIEnv*, jclass,
                                                            jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplier(
    JNIEnv*, jclass, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_Options_maxCompactionBytes(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxCompactionBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_compaction_bytes =
      static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_arenaBlockSize(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setArenaBlockSize(JNIEnv* env, jclass,
                                                jlong jhandle,
                                                jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->arena_block_size =
        jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableAutoCompactions(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableAutoCompactions(
    JNIEnv*, jclass, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSequentialSkipInIterations(JNIEnv*, jclass,
                                                             jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSequentialSkipInIterations(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_inplaceUpdateSupport(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateSupport(
    JNIEnv*, jclass, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_inplaceUpdateNumLocks(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateNumLocks(
    JNIEnv* env, jclass, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_Options_memtablePrefixBloomSizeRatio(JNIEnv*, jclass,
                                                              jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jclass, jlong jhandle, jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    experimentalMempurgeThreshold
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_Options_experimentalMempurgeThreshold(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->experimental_mempurge_threshold;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setExperimentalMempurgeThreshold
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setExperimentalMempurgeThreshold(
    JNIEnv*, jclass, jlong jhandle, jdouble jexperimental_mempurge_threshold) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->experimental_mempurge_threshold =
      static_cast<double>(jexperimental_mempurge_threshold);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtableWholeKeyFiltering
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_memtableWholeKeyFiltering(JNIEnv*, jclass,
                                                            jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_whole_key_filtering;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtableWholeKeyFiltering
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setMemtableWholeKeyFiltering(
    JNIEnv*, jclass, jlong jhandle, jboolean jmemtable_whole_key_filtering) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_whole_key_filtering =
      static_cast<bool>(jmemtable_whole_key_filtering);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_bloomLocality(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bloom_locality;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBloomLocality(JNIEnv*, jclass, jlong jhandle,
                                               jint jbloom_locality) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bloom_locality =
      static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSuccessiveMerges(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSuccessiveMerges(
    JNIEnv* env, jclass, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_optimizeFiltersForHits(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setOptimizeFiltersForHits(
    JNIEnv*, jclass, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    oldDefaults
 * Signature: (JII)V
 */
void Java_org_rocksdb_Options_oldDefaults(JNIEnv*, jclass, jlong jhandle,
                                          jint major_version,
                                          jint minor_version) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->OldDefaults(
      major_version, minor_version);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_optimizeForSmallDb__J(JNIEnv*, jclass,
                                                    jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeForSmallDb__JJ(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jlong cache_handle) {
  auto* cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          cache_handle);
  auto* options_ptr = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* cf_options_ptr =
      static_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(options_ptr);
  cf_options_ptr->OptimizeForSmallDb(cache_sptr_ptr);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeForPointLookup(
    JNIEnv*, jclass, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeLevelStyleCompaction(
    JNIEnv*, jclass, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeUniversalStyleCompaction(
    JNIEnv*, jclass, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    prepareForBulkLoad
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_prepareForBulkLoad(JNIEnv*, jclass,
                                                 jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->PrepareForBulkLoad();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_memtableHugePageSize(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemtableHugePageSize(
    JNIEnv* env, jclass, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_softPendingCompactionBytesLimit(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_hardPendingCompactionBytesLimit(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setHardPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0FileNumCompactionTrigger(JNIEnv*, jclass,
                                                             jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0SlowdownWritesTrigger(JNIEnv*, jclass,
                                                          jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0StopWritesTrigger(JNIEnv*, jclass,
                                                      jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0StopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_Options_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jclass, jlong jhandle) {
  auto mbflma = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
                    ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }

  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jclass, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidFileChecks(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidFileChecks(
    JNIEnv*, jclass, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->paranoid_file_checks =
      static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionPriority(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionPriority(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setReportBgIoStats(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean jreport_bg_io_stats) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_reportBgIoStats(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<bool>(opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTtl(JNIEnv*, jclass, jlong jhandle,
                                     jlong jttl) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    ttl
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_ttl(JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->ttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPeriodicCompactionSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setPeriodicCompactionSeconds(
    JNIEnv*, jclass, jlong jhandle, jlong jperiodicCompactionSeconds) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->periodic_compaction_seconds =
      static_cast<uint64_t>(jperiodicCompactionSeconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    periodicCompactionSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_periodicCompactionSeconds(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->periodic_compaction_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsUniversal(
    JNIEnv*, jclass, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(
          jcompaction_options_universal_handle);
  opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsFIFO(
    JNIEnv*, jclass, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* opts_fifo = reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setForceConsistencyChecks(
    JNIEnv*, jclass, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->force_consistency_checks = static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_forceConsistencyChecks(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<bool>(opts->force_consistency_checks);
}

/// BLOB options

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableBlobFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableBlobFiles(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean jenable_blob_files) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->enable_blob_files = static_cast<bool>(jenable_blob_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableBlobFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableBlobFiles(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opts->enable_blob_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinBlobSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMinBlobSize(JNIEnv*, jclass, jlong jhandle,
                                             jlong jmin_blob_size) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->min_blob_size = static_cast<uint64_t>(jmin_blob_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minBlobSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_minBlobSize(JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->min_blob_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBlobFileSize(JNIEnv*, jclass, jlong jhandle,
                                              jlong jblob_file_size) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_file_size = static_cast<uint64_t>(jblob_file_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_blobFileSize(JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->blob_file_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setBlobCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jblob_compression_type_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_compression_type =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jblob_compression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_blobCompressionType(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      opts->blob_compression_type);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableBlobGarbageCollection
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableBlobGarbageCollection(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_blob_garbage_collection) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->enable_blob_garbage_collection =
      static_cast<bool>(jenable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableBlobGarbageCollection
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableBlobGarbageCollection(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opts->enable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobGarbageCollectionAgeCutoff
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setBlobGarbageCollectionAgeCutoff(
    JNIEnv*, jclass, jlong jhandle,
    jdouble jblob_garbage_collection_age_cutoff) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_garbage_collection_age_cutoff =
      static_cast<double>(jblob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobGarbageCollectionAgeCutoff
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_blobGarbageCollectionAgeCutoff(JNIEnv*, jclass,
                                                                jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jdouble>(opts->blob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobGarbageCollectionForceThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setBlobGarbageCollectionForceThreshold(
    JNIEnv*, jclass, jlong jhandle,
    jdouble jblob_garbage_collection_force_threshold) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_garbage_collection_force_threshold =
      static_cast<double>(jblob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobGarbageCollectionForceThreshold
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_blobGarbageCollectionForceThreshold(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jdouble>(opts->blob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setReadTriggeredCompactionThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setReadTriggeredCompactionThreshold(
    JNIEnv*, jclass, jlong jhandle, jdouble jval) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->read_triggered_compaction_threshold = static_cast<double>(jval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    readTriggeredCompactionThreshold
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_readTriggeredCompactionThreshold(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jdouble>(opts->read_triggered_compaction_threshold);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBlobCompactionReadaheadSize(
    JNIEnv*, jclass, jlong jhandle, jlong jblob_compaction_readahead_size) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_compaction_readahead_size =
      static_cast<uint64_t>(jblob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobCompactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_blobCompactionReadaheadSize(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->blob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlobFileStartingLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBlobFileStartingLevel(
    JNIEnv*, jclass, jlong jhandle, jint jblob_file_starting_level) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->blob_file_starting_level = jblob_file_starting_level;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blobFileStartingLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_blobFileStartingLevel(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jint>(opts->blob_file_starting_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPrepopulateBlobCache
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setPrepopulateBlobCache(
    JNIEnv*, jclass, jlong jhandle, jbyte jprepopulate_blob_cache_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->prepopulate_blob_cache =
      ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toCppPrepopulateBlobCache(
          jprepopulate_blob_cache_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    prepopulateBlobCache
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_prepopulateBlobCache(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toJavaPrepopulateBlobCache(
      opts->prepopulate_blob_cache);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtableMaxRangeDeletions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtableMaxRangeDeletions(
    JNIEnv*, jclass, jlong jhandle, jint jmemtable_max_range_deletions) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->memtable_max_range_deletions =
      static_cast<int32_t>(jmemtable_max_range_deletions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtableMaxRangeDeletions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_memtableMaxRangeDeletions(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jint>(opts->memtable_max_range_deletions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinTombstonesForRangeConversion
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinTombstonesForRangeConversion(
    JNIEnv*, jclass, jlong jhandle, jint jmin_tombstones_for_range_conversion) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->min_tombstones_for_range_conversion =
      static_cast<uint32_t>(jmin_tombstones_for_range_conversion);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minTombstonesForRangeConversion
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minTombstonesForRangeConversion(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jint>(opts->min_tombstones_for_range_conversion);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tablePropertiesCollectorFactory
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_Options_tablePropertiesCollectorFactory(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  const size_t size = opt->table_properties_collector_factories.size();
  jlongArray retVal = env->NewLongArray(static_cast<jsize>(size));
  if (retVal == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  jlong* buf = env->GetLongArrayElements(retVal, nullptr);
  if (buf == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  for (size_t i = 0; i < size; i++) {
    auto* wrapper = new TablePropertiesCollectorFactoriesJniWrapper();
    wrapper->table_properties_collector_factories =
        opt->table_properties_collector_factories[i];
    buf[i] = GET_CPLUSPLUS_POINTER(wrapper);
  }
  env->ReleaseLongArrayElements(retVal, buf, 0);
  return retVal;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTablePropertiesCollectorFactory
 * Signature: (J[J)V
 */
void Java_org_rocksdb_Options_setTablePropertiesCollectorFactory(
    JNIEnv* env, jclass, jlong jhandle, jlongArray j_factory_handles) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  const jsize size = env->GetArrayLength(j_factory_handles);

  jlong* buf = env->GetLongArrayElements(j_factory_handles, nullptr);
  if (buf == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  opt->table_properties_collector_factories.clear();
  for (jsize i = 0; i < size; i++) {
    auto* wrapper =
        reinterpret_cast<TablePropertiesCollectorFactoriesJniWrapper*>(buf[i]);
    opt->table_properties_collector_factories.emplace_back(
        wrapper->table_properties_collector_factories);
  }
  env->ReleaseLongArrayElements(j_factory_handles, buf, JNI_ABORT);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ColumnFamilyOptions

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptions(JNIEnv*,
                                                                  jclass) {
  auto* op = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    copyColumnFamilyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_copyColumnFamilyOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(
      *reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    getColumnFamilyOptionsFromProps
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_getColumnFamilyOptionsFromProps__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong cfg_handle, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(cfg_handle);
  auto* cf_options = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  ROCKSDB_NAMESPACE::Status status =
      ROCKSDB_NAMESPACE::GetColumnFamilyOptionsFromString(
          *config_options, ROCKSDB_NAMESPACE::ColumnFamilyOptions(), opt_string,
          cf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(cf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete cf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    getColumnFamilyOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_getColumnFamilyOptionsFromProps__Ljava_lang_String_2(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* cf_options = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  ROCKSDB_NAMESPACE::ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ROCKSDB_NAMESPACE::Status status =
      ROCKSDB_NAMESPACE::GetColumnFamilyOptionsFromString(
          config_options, ROCKSDB_NAMESPACE::ColumnFamilyOptions(), opt_string,
          cf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(cf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete cf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_disposeInternalJni(JNIEnv*, jclass,
                                                             jlong handle) {
  auto* cfo = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(handle);
  assert(cfo != nullptr);
  delete cfo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    oldDefaults
 * Signature: (JII)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_oldDefaults(JNIEnv*, jclass,
                                                      jlong jhandle,
                                                      jint major_version,
                                                      jint minor_version) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OldDefaults(major_version, minor_version);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForSmallDb__J(JNIEnv*, jclass,
                                                                jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForSmallDb__JJ(
    JNIEnv*, jclass, jlong jhandle, jlong cache_handle) {
  auto* cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          cache_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForSmallDb(cache_sptr_ptr);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForPointLookup(
    JNIEnv*, jclass, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeLevelStyleCompaction(
    JNIEnv*, jclass, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeUniversalStyleCompaction(
    JNIEnv*, jclass, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JI(
    JNIEnv*, jclass, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JJB(
    JNIEnv*, jclass, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperatorName(
    JNIEnv* env, jclass, jlong jhandle, jstring jop_name) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);
  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperator(
    JNIEnv*, jclass, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterHandle(
    JNIEnv*, jclass, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter*>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle(
    JNIEnv*, jclass, jlong jopt_handle, jlong jcompactionfilterfactory_handle) {
  auto* cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactoryJniCallback>*>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ColumnFamilyOptions_setWriteBufferSize(
    JNIEnv* env, jclass, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->write_buffer_size = jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_writeBufferSize(JNIEnv*, jclass,
                                                           jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxWriteBufferNumber(
    JNIEnv*, jclass, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_maxWriteBufferNumber(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemTableFactory(
    JNIEnv*, jclass, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::MemTableRepFactory*>(
              jfactory_handle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_memTableFactoryName(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useFixedLengthPrefixExtractor(
    JNIEnv*, jclass, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useCappedPrefixExtractor(
    JNIEnv*, jclass, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTableFactory(
    JNIEnv*, jclass, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->table_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory*>(jfactory_handle));
}

/*
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setSstPartitionerFactory(
    JNIEnv*, jclass, jlong jhandle, jlong factory_handle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory>*>(
      factory_handle);
  options->sst_partitioner_factory = *factory;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionThreadLimiter(
    JNIEnv*, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(
      jlimiter_handle);
  options->compaction_thread_limiter = *limiter;
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_tableFactoryName(JNIEnv* env,
                                                              jclass,
                                                              jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  ROCKSDB_NAMESPACE::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCfPaths(JNIEnv* env, jclass,
                                                     jlong jhandle,
                                                     jobjectArray path_array,
                                                     jlongArray size_array) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_cfPathsLen(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(opt->cf_paths.size());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_cfPaths(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jobjectArray jpaths,
                                                  jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<
      ROCKSDB_NAMESPACE::ColumnFamilyOptions>(env, jhandle, jpaths,
                                              jtarget_sizes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_minWriteBufferNumberToMerge(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge(
    JNIEnv*, jclass, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compressionType(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      cf_opts->compression);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionPerLevel(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jcompressionLevels) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyOptions_compressionPerLevel(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return rocksdb_compression_list_helper(env,
                                         cf_options->compression_per_level);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_options->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_bottommostCompressionType(
    JNIEnv*, jclass, jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      cf_options->bottommost_compression);
}
/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionOptions(
    JNIEnv*, jclass, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jbottommost_compression_options_handle);
  cf_options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionOptions(
    JNIEnv*, jclass, jlong jhandle, jlong jcompression_options_handle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jcompression_options_handle);
  cf_options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionStyle(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompaction_style) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionStyle(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      cf_options->compaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxTableFilesSizeFIFO(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxTableFilesSizeFIFO(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_numLevels(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->num_levels;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setNumLevels(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jint jnum_levels) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->num_levels = static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroFileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroSlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroStopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroStopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeBase(JNIEnv*, jclass,
                                                              jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeBase(
    JNIEnv*, jclass, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeMultiplier(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeMultiplier(
    JNIEnv*, jclass, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelBase(JNIEnv*, jclass,
                                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelBase(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_levelCompactionDynamicLevelBytes(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplier(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplier(
    JNIEnv*, jclass, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxCompactionBytes(JNIEnv*, jclass,
                                                              jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxCompactionBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_compaction_bytes = static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_arenaBlockSize(JNIEnv*, jclass,
                                                          jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setArenaBlockSize(
    JNIEnv* env, jclass, jlong jhandle, jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->arena_block_size = jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_disableAutoCompactions(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setDisableAutoCompactions(
    JNIEnv*, jclass, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSequentialSkipInIterations(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSequentialSkipInIterations(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateSupport(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateSupport(
    JNIEnv*, jclass, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateNumLocks(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateNumLocks(
    JNIEnv* env, jclass, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_memtablePrefixBloomSizeRatio(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jclass, jlong jhandle, jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    experimentalMempurgeThreshold
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_experimentalMempurgeThreshold(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->experimental_mempurge_threshold;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setExperimentalMempurgeThreshold
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setExperimentalMempurgeThreshold(
    JNIEnv*, jclass, jlong jhandle, jdouble jexperimental_mempurge_threshold) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->experimental_mempurge_threshold =
      static_cast<double>(jexperimental_mempurge_threshold);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtableWholeKeyFiltering
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_memtableWholeKeyFiltering(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_whole_key_filtering;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtableWholeKeyFiltering
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtableWholeKeyFiltering(
    JNIEnv*, jclass, jlong jhandle, jboolean jmemtable_whole_key_filtering) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_whole_key_filtering =
      static_cast<bool>(jmemtable_whole_key_filtering);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_bloomLocality(JNIEnv*, jclass,
                                                        jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->bloom_locality;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBloomLocality(
    JNIEnv*, jclass, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->bloom_locality = static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSuccessiveMerges(JNIEnv*, jclass,
                                                               jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSuccessiveMerges(
    JNIEnv* env, jclass, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_optimizeFiltersForHits(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setOptimizeFiltersForHits(
    JNIEnv*, jclass, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_memtableHugePageSize(JNIEnv*, jclass,
                                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtableHugePageSize(
    JNIEnv* env, jclass, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_softPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_hardPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setHardPendingCompactionBytesLimit(
    JNIEnv*, jclass, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0FileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0SlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0StopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0StopWritesTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray
Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jclass, jlong jhandle) {
  auto mbflma =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jclass, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* cf_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_paranoidFileChecks(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setParanoidFileChecks(
    JNIEnv*, jclass, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks = static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionPriority(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionPriority(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      cf_opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setReportBgIoStats(
    JNIEnv*, jclass, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_reportBgIoStats(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<bool>(cf_opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTtl(JNIEnv*, jclass, jlong jhandle,
                                                 jlong jttl) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    ttl
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_ColumnFamilyOptions_ttl(JNIEnv*, jclass, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(cf_opts->ttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setPeriodicCompactionSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setPeriodicCompactionSeconds(
    JNIEnv*, jclass, jlong jhandle, jlong jperiodicCompactionSeconds) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->periodic_compaction_seconds =
      static_cast<uint64_t>(jperiodicCompactionSeconds);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    periodicCompactionSeconds
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_ColumnFamilyOptions_periodicCompactionSeconds(JNIEnv*, jclass,
                                                               jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(cf_opts->periodic_compaction_seconds);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsUniversal(
    JNIEnv*, jclass, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(
          jcompaction_options_universal_handle);
  cf_opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsFIFO(
    JNIEnv*, jclass, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* opts_fifo = reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  cf_opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setForceConsistencyChecks(
    JNIEnv*, jclass, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->force_consistency_checks =
      static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_forceConsistencyChecks(
    JNIEnv*, jclass, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(cf_opts->force_consistency_checks);
}

/// BLOB options

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setEnableBlobFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setEnableBlobFiles(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_blob_files) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->enable_blob_files = static_cast<bool>(jenable_blob_files);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    enableBlobFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_enableBlobFiles(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(opts->enable_blob_files);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMinBlobSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMinBlobSize(JNIEnv*, jclass,
                                                         jlong jhandle,
                                                         jlong jmin_blob_size) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->min_blob_size = static_cast<uint64_t>(jmin_blob_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    minBlobSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_minBlobSize(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(opts->min_blob_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobFileSize(
    JNIEnv*, jclass, jlong jhandle, jlong jblob_file_size) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_file_size = static_cast<uint64_t>(jblob_file_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_blobFileSize(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(opts->blob_file_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jblob_compression_type_value) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_compression_type =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jblob_compression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_blobCompressionType(JNIEnv*, jclass,
                                                               jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      opts->blob_compression_type);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setEnableBlobGarbageCollection
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setEnableBlobGarbageCollection(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_blob_garbage_collection) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->enable_blob_garbage_collection =
      static_cast<bool>(jenable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    enableBlobGarbageCollection
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_enableBlobGarbageCollection(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(opts->enable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobGarbageCollectionAgeCutoff
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobGarbageCollectionAgeCutoff(
    JNIEnv*, jclass, jlong jhandle,
    jdouble jblob_garbage_collection_age_cutoff) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_garbage_collection_age_cutoff =
      static_cast<double>(jblob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobGarbageCollectionAgeCutoff
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_blobGarbageCollectionAgeCutoff(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jdouble>(opts->blob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobGarbageCollectionForceThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobGarbageCollectionForceThreshold(
    JNIEnv*, jclass, jlong jhandle,
    jdouble jblob_garbage_collection_force_threshold) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_garbage_collection_force_threshold =
      static_cast<double>(jblob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobGarbageCollectionForceThreshold
 * Signature: (J)D
 */
jdouble
Java_org_rocksdb_ColumnFamilyOptions_blobGarbageCollectionForceThreshold(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jdouble>(opts->blob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setReadTriggeredCompactionThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setReadTriggeredCompactionThreshold(
    JNIEnv*, jclass, jlong jhandle, jdouble jval) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->read_triggered_compaction_threshold = static_cast<double>(jval);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    readTriggeredCompactionThreshold
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_readTriggeredCompactionThreshold(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jdouble>(opts->read_triggered_compaction_threshold);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobCompactionReadaheadSize(
    JNIEnv*, jclass, jlong jhandle, jlong jblob_compaction_readahead_size) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_compaction_readahead_size =
      static_cast<uint64_t>(jblob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobCompactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_blobCompactionReadaheadSize(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(opts->blob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBlobFileStartingLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBlobFileStartingLevel(
    JNIEnv*, jclass, jlong jhandle, jint jblob_file_starting_level) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->blob_file_starting_level = jblob_file_starting_level;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    blobFileStartingLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_blobFileStartingLevel(JNIEnv*, jclass,
                                                                jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jint>(opts->blob_file_starting_level);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setPrepopulateBlobCache
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setPrepopulateBlobCache(
    JNIEnv*, jclass, jlong jhandle, jbyte jprepopulate_blob_cache_value) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->prepopulate_blob_cache =
      ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toCppPrepopulateBlobCache(
          jprepopulate_blob_cache_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    prepopulateBlobCache
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_prepopulateBlobCache(JNIEnv*, jclass,
                                                                jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toJavaPrepopulateBlobCache(
      opts->prepopulate_blob_cache);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtableMaxRangeDeletions
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtableMaxRangeDeletions(
    JNIEnv*, jclass, jlong jhandle, jint jmemtable_max_range_deletions) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->memtable_max_range_deletions = jmemtable_max_range_deletions;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtableMaxRangeDeletions
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_memtableMaxRangeDeletions(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jint>(opts->memtable_max_range_deletions);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMinTombstonesForRangeConversion
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMinTombstonesForRangeConversion(
    JNIEnv*, jclass, jlong jhandle, jint jmin_tombstones_for_range_conversion) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  opts->min_tombstones_for_range_conversion =
      static_cast<uint32_t>(jmin_tombstones_for_range_conversion);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    minTombstonesForRangeConversion
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_minTombstonesForRangeConversion(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jint>(opts->min_tombstones_for_range_conversion);
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DBOptions

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptions(JNIEnv*, jclass) {
  auto* dbop = new ROCKSDB_NAMESPACE::DBOptions();
  return GET_CPLUSPLUS_POINTER(dbop);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    copyDBOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_copyDBOptions(JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::DBOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt = new ROCKSDB_NAMESPACE::DBOptions(
      *reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    getDBOptionsFromProps
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_DBOptions_getDBOptionsFromProps__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong config_handle, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(config_handle);
  auto* db_options = new ROCKSDB_NAMESPACE::DBOptions();
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::GetDBOptionsFromString(
      *config_options, ROCKSDB_NAMESPACE::DBOptions(), opt_string, db_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if DBOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(db_options);
  } else {
    // if operation failed the DBOptions need to be deleted
    // again to prevent a memory leak.
    delete db_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    getDBOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_DBOptions_getDBOptionsFromProps__Ljava_lang_String_2(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  const ROCKSDB_NAMESPACE::DBOptions base_options;
  auto* db_options = new ROCKSDB_NAMESPACE::DBOptions();
  ROCKSDB_NAMESPACE::ConfigOptions config_options(base_options);
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::GetDBOptionsFromString(
      config_options, base_options, opt_string, db_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if DBOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(db_options);
  } else {
    // if operation failed the DBOptions need to be deleted
    // again to prevent a memory leak.
    delete db_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_disposeInternalJni(JNIEnv*, jclass,
                                                   jlong handle) {
  auto* dbo = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(handle);
  assert(dbo != nullptr);
  delete dbo;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_optimizeForSmallDb(JNIEnv*, jclass,
                                                   jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setEnv(JNIEnv*, jclass, jlong jhandle,
                                       jlong jenv_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setIncreaseParallelism(JNIEnv*, jclass,
                                                       jlong jhandle,
                                                       jint totalThreads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateIfMissing(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->create_if_missing =
      flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createIfMissing(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_if_missing;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateMissingColumnFamilies(JNIEnv*, jclass,
                                                               jlong jhandle,
                                                               jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createMissingColumnFamilies(JNIEnv*, jclass,
                                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setErrorIfExists(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean error_if_exists) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_errorIfExists(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->error_if_exists;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setParanoidChecks(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean paranoid_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_paranoidChecks(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->paranoid_checks;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setOpenFilesAsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setOpenFilesAsync(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean open_files_async) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->open_files_async =
      static_cast<bool>(open_files_async);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    openFilesAsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_openFilesAsync(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->open_files_async;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRateLimiter(JNIEnv*, jclass, jlong jhandle,
                                               jlong jrate_limiter_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->rate_limiter =
      *pRateLimiter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setSstFileManager(
    JNIEnv*, jclass, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogger
 * Signature: (JJB)V
 */
void Java_org_rocksdb_DBOptions_setLogger(JNIEnv* env, jclass, jlong jhandle,
                                          jlong jlogger_handle,
                                          jbyte jlogger_type) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  switch (jlogger_type) {
    case 0x1:
      // JAVA_IMPLEMENTATION
      options->info_log =
          *(reinterpret_cast<
              std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
              jlogger_handle));
      break;
    case 0x2:
      // STDERR_IMPLEMENTATION
      options->info_log =
          *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>*>(
              jlogger_handle));
      break;
    default:
      ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
          env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                   ROCKSDB_NAMESPACE::Slice("Unknown value for LoggerType")));
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setInfoLogLevel(JNIEnv*, jclass, jlong jhandle,
                                                jbyte jlog_level) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->info_log_level =
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_infoLogLevel(JNIEnv*, jclass, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxTotalWalSize(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jlong jmax_total_wal_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxTotalWalSize(JNIEnv*, jclass,
                                                 jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxOpenFiles(JNIEnv*, jclass, jlong jhandle,
                                                jint max_open_files) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxOpenFiles(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_open_files;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxFileOpeningThreads(
    JNIEnv*, jclass, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_file_opening_threads = static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxFileOpeningThreads(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setStatistics(JNIEnv*, jclass, jlong jhandle,
                                              jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* pSptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni>*>(
          jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_statistics(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>* pSptr =
        new std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>(sptr);
    return GET_CPLUSPLUS_POINTER(pSptr);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseFsync(JNIEnv*, jclass, jlong jhandle,
                                            jboolean use_fsync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useFsync(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_setDbPaths(JNIEnv* env, jclass, jlong jhandle,
                                           jobjectArray jpaths,
                                           jlongArray jtarget_sizes) {
  std::vector<ROCKSDB_NAMESPACE::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        ROCKSDB_NAMESPACE::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbPathsLen(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_dbPaths(JNIEnv* env, jclass, jlong jhandle,
                                        jobjectArray jpaths,
                                        jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy == JNI_TRUE ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setDbLogDir(JNIEnv* env, jclass, jlong jhandle,
                                            jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->db_log_dir.assign(
      log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_dbLogDir(JNIEnv* env, jclass,
                                            jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setWalDir(JNIEnv* env, jclass, jlong jhandle,
                                          jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, 0);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->wal_dir.assign(
      wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_walDir(JNIEnv* env, jclass, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv*, jclass, jlong jhandle, jlong micros) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_deleteObsoleteFilesPeriodMicros(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundCompactions(JNIEnv*, jclass,
                                                            jlong jhandle,
                                                            jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundCompactions(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxSubcompactions(JNIEnv*, jclass,
                                                     jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxSubcompactions(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_subcompactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundFlushes(
    JNIEnv*, jclass, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_flushes = static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundFlushes(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_flushes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundJobs(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jint max_background_jobs) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_jobs = static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundJobs(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_jobs;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxLogFileSize(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jlong max_log_file_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->max_log_file_size = max_log_file_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxLogFileSize(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_log_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogFileTimeToRoll(
    JNIEnv* env, jclass, jlong jhandle, jlong log_file_time_to_roll) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->log_file_time_to_roll = log_file_time_to_roll;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_logFileTimeToRoll(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setKeepLogFileNum(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jlong keep_log_file_num) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->keep_log_file_num = keep_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_keepLogFileNum(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRecycleLogFileNum(
    JNIEnv* env, jclass, jlong jhandle, jlong recycle_log_file_num) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->recycle_log_file_num = recycle_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_recycleLogFileNum(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxManifestFileSize(
    JNIEnv*, jclass, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_manifest_file_size = static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxManifestFileSize(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_manifest_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setTableCacheNumshardbits(
    JNIEnv*, jclass, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->table_cache_numshardbits = static_cast<int>(table_cache_numshardbits);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_tableCacheNumshardbits(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalTtlSeconds(JNIEnv*, jclass, jlong jhandle,
                                                 jlong WAL_ttl_seconds) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walTtlSeconds(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalSizeLimitMB(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jlong WAL_size_limit_MB) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walSizeLimitMB(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxWriteBatchGroupSizeBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxWriteBatchGroupSizeBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_write_batch_group_size_bytes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->max_write_batch_group_size_bytes =
      static_cast<uint64_t>(jmax_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxWriteBatchGroupSizeBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxWriteBatchGroupSizeBytes(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->max_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setManifestPreallocationSize(
    JNIEnv* env, jclass, jlong jhandle, jlong preallocation_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      preallocation_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_manifestPreallocationSize(JNIEnv*, jclass,
                                                           jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectReads(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectReads(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean use_direct_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectIoForFlushAndCompaction(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jclass, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowFAllocate(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean jallow_fallocate) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowFAllocate(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapReads(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean allow_mmap_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapReads(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapWrites(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jboolean allow_mmap_writes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapWrites(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setIsFdCloseOnExec(
    JNIEnv*, jclass, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->is_fd_close_on_exec = static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_isFdCloseOnExec(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setStatsDumpPeriodSec(
    JNIEnv*, jclass, jlong jhandle, jint jstats_dump_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_dump_period_sec =
      static_cast<unsigned int>(jstats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_statsDumpPeriodSec(JNIEnv*, jclass,
                                                   jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxCompactionTriggerWakeupSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxCompactionTriggerWakeupSeconds(
    JNIEnv*, jclass, jlong jhandle, jlong jval) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_compaction_trigger_wakeup_seconds = static_cast<uint64_t>(jval);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxCompactionTriggerWakeupSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxCompactionTriggerWakeupSeconds(
    JNIEnv*, jclass, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->max_compaction_trigger_wakeup_seconds);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsPersistPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setStatsPersistPeriodSec(
    JNIEnv*, jclass, jlong jhandle, jint jstats_persist_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_persist_period_sec =
      static_cast<unsigned int>(jstats_persist_period_sec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsPersistPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_statsPersistPeriodSec(JNIEnv*, jclass,
                                                      jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_persist_period_sec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsHistoryBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setStatsHistoryBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jstats_history_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_history_buffer_size =
      static_cast<size_t>(jstats_history_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsHistoryBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_statsHistoryBufferSize(JNIEnv*, jclass,
                                                        jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_history_buffer_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAdviseRandomOnOpen(
    JNIEnv*, jclass, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->advise_random_on_open = static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_adviseRandomOnOpen(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDbWriteBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteBufferManager(
    JNIEnv*, jclass, jlong jdb_options_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jwrite_buffer_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_options_handle)
      ->write_buffer_manager = *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbWriteBufferSize(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setCompactionReadaheadSize(
    JNIEnv*, jclass, jlong jhandle, jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_compactionReadaheadSize(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWritableFileMaxBufferSize(
    JNIEnv*, jclass, jlong jhandle, jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writableFileMaxBufferSize(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseAdaptiveMutex(
    JNIEnv*, jclass, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useAdaptiveMutex(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setBytesPerSync(JNIEnv*, jclass, jlong jhandle,
                                                jlong bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_bytesPerSync(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalBytesPerSync(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jlong jwal_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walBytesPerSync(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStrictBytesPerSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setStrictBytesPerSync(
    JNIEnv*, jclass, jlong jhandle, jboolean jstrict_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->strict_bytes_per_sync = jstrict_bytes_per_sync == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    strictBytesPerSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_strictBytesPerSync(JNIEnv*, jclass,
                                                       jlong jhandle) {
  return static_cast<jboolean>(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->strict_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEventListeners
 * Signature: (J[J)V
 */
void Java_org_rocksdb_DBOptions_setEventListeners(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jlongArray jlistener_array) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  rocksdb_set_event_listeners_helper(env, jlistener_array, opt->listeners);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    eventListeners
 * Signature: (J)[Lorg/rocksdb/AbstractEventListener;
 */
jobjectArray Java_org_rocksdb_DBOptions_eventListeners(JNIEnv* env, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return rocksdb_get_event_listeners_helper(env, opt->listeners);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDelayedWriteRate(JNIEnv*, jclass,
                                                    jlong jhandle,
                                                    jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_delayedWriteRate(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnablePipelinedWrite(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enablePipelinedWrite(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUnorderedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUnorderedWrite(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean junordered_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->unordered_write = junordered_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    unorderedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_unorderedWrite(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->unordered_write);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableThreadTracking(
    JNIEnv*, jclass, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->enable_thread_tracking = jenable_thread_tracking == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableThreadTracking(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowConcurrentMemtableWrite(
    JNIEnv*, jclass, jlong jhandle, jboolean allow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowConcurrentMemtableWrite(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jclass, jlong jhandle, jboolean yield) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableWriteThreadAdaptiveYield(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadMaxYieldUsec(JNIEnv*, jclass,
                                                           jlong jhandle,
                                                           jlong max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_max_yield_usec = static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadMaxYieldUsec(JNIEnv*, jclass,
                                                         jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadSlowYieldUsec(JNIEnv*, jclass,
                                                            jlong jhandle,
                                                            jlong slow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_slow_yield_usec = static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadSlowYieldUsec(JNIEnv*, jclass,
                                                          jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jclass, jlong jhandle, jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_skipStatsUpdateOnDbOpen(JNIEnv*, jclass,
                                                            jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setWalRecoveryMode(
    JNIEnv*, jclass, jlong jhandle, jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->wal_recovery_mode =
      ROCKSDB_NAMESPACE::WALRecoveryModeJni::toCppWALRecoveryMode(
          jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_walRecoveryMode(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllow2pc(JNIEnv*, jclass, jlong jhandle,
                                            jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allow2pc(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRowCache(JNIEnv*, jclass, jlong jhandle,
                                            jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jrow_cache_handle);
  opt->row_cache = *row_cache;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalFilter(JNIEnv*, jclass, jlong jhandle,
                                             jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* wal_filter = reinterpret_cast<ROCKSDB_NAMESPACE::WalFilterJniCallback*>(
      jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setDumpMallocStats(
    JNIEnv*, jclass, jlong jhandle, jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_dumpMallocStats(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringRecovery(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowIngestBehind(
    JNIEnv*, jclass, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowIngestBehind(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setTwoWriteQueues(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_twoWriteQueues(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setManualWalFlush(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_manualWalFlush(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAtomicFlush(JNIEnv*, jclass, jlong jhandle,
                                               jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_atomicFlush(JNIEnv*, jclass,
                                                jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringShutdown(
    JNIEnv*, jclass, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringShutdown(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidUnnecessaryBlockingIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidUnnecessaryBlockingIO(
    JNIEnv*, jclass, jlong jhandle, jboolean avoid_blocking_io) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_unnecessary_blocking_io = static_cast<bool>(avoid_blocking_io);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidUnnecessaryBlockingIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidUnnecessaryBlockingIO(JNIEnv*, jclass,
                                                               jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_unnecessary_blocking_io);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setPersistStatsToDisk
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setPersistStatsToDisk(
    JNIEnv*, jclass, jlong jhandle, jboolean persist_stats_to_disk) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->persist_stats_to_disk = static_cast<bool>(persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    persistStatsToDisk
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_persistStatsToDisk(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteDbidToManifest
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setWriteDbidToManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jwrite_dbid_to_manifest) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->write_dbid_to_manifest = static_cast<bool>(jwrite_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeDbidToManifest
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_writeDbidToManifest(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->write_dbid_to_manifest);
}

void Java_org_rocksdb_DBOptions_setTrackOptionsFileNumberInManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jtrack) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->track_options_file_number_in_manifest = static_cast<bool>(jtrack);
}

jboolean Java_org_rocksdb_DBOptions_trackOptionsFileNumberInManifest(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->track_options_file_number_in_manifest);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogReadaheadSize(JNIEnv*, jclass,
                                                    jlong jhandle,
                                                    jlong jlog_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->log_readahead_size = static_cast<size_t>(jlog_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    logReasaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_logReadaheadSize(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->log_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBestEffortsRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setBestEffortsRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean jbest_efforts_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->best_efforts_recovery = static_cast<bool>(jbest_efforts_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bestEffortsRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_bestEffortsRecovery(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->best_efforts_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBgErrorResumeCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBgErrorResumeCount(
    JNIEnv*, jclass, jlong jhandle, jint jmax_bgerror_resume_count) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->max_bgerror_resume_count = static_cast<int>(jmax_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBgerrorResumeCount
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBgerrorResumeCount(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jint>(opt->max_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBgerrorResumeRetryInterval
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setBgerrorResumeRetryInterval(
    JNIEnv*, jclass, jlong jhandle, jlong jbgerror_resume_retry_interval) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->bgerror_resume_retry_interval =
      static_cast<uint64_t>(jbgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bgerrorResumeRetryInterval
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_bgerrorResumeRetryInterval(JNIEnv*, jclass,
                                                            jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->bgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDailyOffpeakTimeUTC
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_DBOptions_setDailyOffpeakTimeUTC(JNIEnv* env, jclass,
                                                       jlong jhandle,
                                                       jstring jtimeutc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  const jsize jtimesz = env->GetStringUTFLength(jtimeutc);
  const char* timeutc = env->GetStringUTFChars(jtimeutc, nullptr);
  if (env->ExceptionCheck()) {
    return;
  }
  opt->daily_offpeak_time_utc = std::string(timeutc, jtimesz);
  env->ReleaseStringUTFChars(jtimeutc, timeutc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dailyOffpeakTimeUTC
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_DBOptions_dailyOffpeakTimeUTC(JNIEnv* env, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return env->NewStringUTF(opt->daily_offpeak_time_utc.c_str());
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::WriteOptions

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    newWriteOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteOptions_newWriteOptions(JNIEnv*, jclass) {
  auto* op = new ROCKSDB_NAMESPACE::WriteOptions();
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    copyWriteOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteOptions_copyWriteOptions(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::WriteOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disposeInternal
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_disposeInternalJni(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle);
  assert(write_options != nullptr);
  delete write_options;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setSync(JNIEnv*, jclass, jlong jhandle,
                                           jboolean jflag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->sync = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_sync(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->sync;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setDisableWAL
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setDisableWAL(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean jflag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->disableWAL =
      jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disableWAL
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_disableWAL(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->disableWAL;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setIgnoreMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setIgnoreMissingColumnFamilies(
    JNIEnv*, jclass, jlong jhandle, jboolean jignore_missing_column_families) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->ignore_missing_column_families =
      static_cast<bool>(jignore_missing_column_families);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    ignoreMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_ignoreMissingColumnFamilies(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->ignore_missing_column_families;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setNoSlowdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setNoSlowdown(JNIEnv*, jclass, jlong jhandle,
                                                 jboolean jno_slowdown) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->no_slowdown =
      static_cast<bool>(jno_slowdown);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    noSlowdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_noSlowdown(JNIEnv*, jclass,
                                                  jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->no_slowdown;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setLowPri
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setLowPri(JNIEnv*, jclass, jlong jhandle,
                                             jboolean jlow_pri) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->low_pri =
      static_cast<bool>(jlow_pri);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    lowPri
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_lowPri(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->low_pri;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    memtableInsertHintPerBatch
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_memtableInsertHintPerBatch(
    JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->memtable_insert_hint_per_batch;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setMemtableInsertHintPerBatch
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setMemtableInsertHintPerBatch(
    JNIEnv*, jclass, jlong jhandle, jboolean jmemtable_insert_hint_per_batch) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->memtable_insert_hint_per_batch =
      static_cast<bool>(jmemtable_insert_hint_per_batch);
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ReadOptions

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__(JNIEnv*, jclass) {
  auto* read_options = new ROCKSDB_NAMESPACE::ReadOptions();
  return GET_CPLUSPLUS_POINTER(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: (ZZ)J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__ZZ(
    JNIEnv*, jclass, jboolean jverify_checksums, jboolean jfill_cache) {
  auto* read_options = new ROCKSDB_NAMESPACE::ReadOptions(
      static_cast<bool>(jverify_checksums), static_cast<bool>(jfill_cache));
  return GET_CPLUSPLUS_POINTER(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    copyReadOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_copyReadOptions(JNIEnv*, jclass,
                                                   jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ReadOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ReadOptions_disposeInternalJni(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  assert(read_options != nullptr);
  delete read_options;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setVerifyChecksums
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setVerifyChecksums(
    JNIEnv*, jclass, jlong jhandle, jboolean jverify_checksums) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->verify_checksums =
      static_cast<bool>(jverify_checksums);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    verifyChecksums
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_verifyChecksums(JNIEnv*, jclass,
                                                      jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->verify_checksums;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setFillCache
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setFillCache(JNIEnv*, jclass, jlong jhandle,
                                               jboolean jfill_cache) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->fill_cache =
      static_cast<bool>(jfill_cache);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    fillCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_fillCache(JNIEnv*, jclass,
                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->fill_cache;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTailing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTailing(JNIEnv*, jclass, jlong jhandle,
                                             jboolean jtailing) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->tailing =
      static_cast<bool>(jtailing);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    tailing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_tailing(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->tailing;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    totalOrderSeek
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_totalOrderSeek(JNIEnv*, jclass,
                                                     jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->total_order_seek;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTotalOrderSeek
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTotalOrderSeek(
    JNIEnv*, jclass, jlong jhandle, jboolean jtotal_order_seek) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->total_order_seek =
      static_cast<bool>(jtotal_order_seek);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    prefixSameAsStart
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_prefixSameAsStart(JNIEnv*, jclass,
                                                        jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->prefix_same_as_start;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPrefixSameAsStart
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPrefixSameAsStart(
    JNIEnv*, jclass, jlong jhandle, jboolean jprefix_same_as_start) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->prefix_same_as_start = static_cast<bool>(jprefix_same_as_start);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    pinData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_pinData(JNIEnv*, jclass, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->pin_data;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPinData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPinData(JNIEnv*, jclass, jlong jhandle,
                                             jboolean jpin_data) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->pin_data =
      static_cast<bool>(jpin_data);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    backgroundPurgeOnIteratorCleanup
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_backgroundPurgeOnIteratorCleanup(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->background_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setBackgroundPurgeOnIteratorCleanup
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setBackgroundPurgeOnIteratorCleanup(
    JNIEnv*, jclass, jlong jhandle,
    jboolean jbackground_purge_on_iterator_cleanup) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->background_purge_on_iterator_cleanup =
      static_cast<bool>(jbackground_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_readaheadSize(JNIEnv*, jclass,
                                                 jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->readahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setReadaheadSize(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jlong jreadahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->readahead_size = static_cast<size_t>(jreadahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    maxSkippableInternalKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_maxSkippableInternalKeys(JNIEnv*, jclass,
                                                            jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->max_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setMaxSkippableInternalKeys
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setMaxSkippableInternalKeys(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_skippable_internal_keys) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->max_skippable_internal_keys =
      static_cast<uint64_t>(jmax_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    ignoreRangeDeletions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_ignoreRangeDeletions(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->ignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIgnoreRangeDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setIgnoreRangeDeletions(
    JNIEnv*, jclass, jlong jhandle, jboolean jignore_range_deletions) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->ignore_range_deletions = static_cast<bool>(jignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setSnapshot(JNIEnv*, jclass, jlong jhandle,
                                              jlong jsnapshot) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::Snapshot*>(jsnapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    snapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_snapshot(JNIEnv*, jclass, jlong jhandle) {
  auto& snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->snapshot;
  return GET_CPLUSPLUS_POINTER(snapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readTier
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ReadOptions_readTier(JNIEnv*, jclass, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->read_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadTier
 * Signature: (JB)V
 */
void Java_org_rocksdb_ReadOptions_setReadTier(JNIEnv*, jclass, jlong jhandle,
                                              jbyte jread_tier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->read_tier =
      static_cast<ROCKSDB_NAMESPACE::ReadTier>(jread_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateUpperBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateUpperBound(
    JNIEnv*, jclass, jlong jhandle, jlong jupper_bound_slice_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->iterate_upper_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jupper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateUpperBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateUpperBound(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto& upper_bound_slice_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
          ->iterate_upper_bound;
  return GET_CPLUSPLUS_POINTER(upper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateLowerBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateLowerBound(
    JNIEnv*, jclass, jlong jhandle, jlong jlower_bound_slice_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->iterate_lower_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jlower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateLowerBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateLowerBound(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto& lower_bound_slice_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
          ->iterate_lower_bound;
  return GET_CPLUSPLUS_POINTER(lower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTableFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setTableFilter(
    JNIEnv*, jclass, jlong jhandle, jlong jjni_table_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  auto* jni_table_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::TableFilterJniCallback*>(
          jjni_table_filter_handle);
  opt->table_filter = jni_table_filter->GetTableFilterFunction();
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    autoPrefixMode
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_autoPrefixMode(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->auto_prefix_mode);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setAutoPrefixMode
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setAutoPrefixMode(
    JNIEnv*, jclass, jlong jhandle, jboolean jauto_prefix_mode) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->auto_prefix_mode = static_cast<bool>(jauto_prefix_mode);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    timestamp
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_timestamp(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  auto& timestamp_slice_handle = opt->timestamp;
  return reinterpret_cast<jlong>(timestamp_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTimestamp
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setTimestamp(JNIEnv*, jclass, jlong jhandle,
                                               jlong jtimestamp_slice_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->timestamp =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jtimestamp_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterStartTs
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterStartTs(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  auto& iter_start_ts_handle = opt->iter_start_ts;
  return reinterpret_cast<jlong>(iter_start_ts_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterStartTs
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setIterStartTs(JNIEnv*, jclass, jlong jhandle,
                                                 jlong jiter_start_ts_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->iter_start_ts =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jiter_start_ts_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    deadline
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_deadline(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->deadline.count());
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setDeadline
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setDeadline(JNIEnv*, jclass, jlong jhandle,
                                              jlong jdeadline) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->deadline = std::chrono::microseconds(static_cast<int64_t>(jdeadline));
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    ioTimeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_ioTimeout(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->io_timeout.count());
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIoTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setIoTimeout(JNIEnv*, jclass, jlong jhandle,
                                               jlong jio_timeout) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->io_timeout =
      std::chrono::microseconds(static_cast<int64_t>(jio_timeout));
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    valueSizeSofLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_valueSizeSoftLimit(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->value_size_soft_limit);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setValueSizeSofLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setValueSizeSoftLimit(
    JNIEnv*, jclass, jlong jhandle, jlong jvalue_size_soft_limit) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->value_size_soft_limit = static_cast<uint64_t>(jvalue_size_soft_limit);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    asyncIo
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_asyncIo(JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->async_io);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setAsyncIo
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setAsyncIo(JNIEnv*, jobject, jlong jhandle,
                                             jboolean jasync_io) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->async_io = static_cast<bool>(jasync_io);
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ComparatorOptions

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    newComparatorOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ComparatorOptions_newComparatorOptions(JNIEnv*, jclass) {
  auto* comparator_opt = new ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions();
  return GET_CPLUSPLUS_POINTER(comparator_opt);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    reusedSynchronisationType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ComparatorOptions_reusedSynchronisationType(
    JNIEnv*, jclass, jlong jhandle) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  return ROCKSDB_NAMESPACE::ReusedSynchronisationTypeJni::
      toJavaReusedSynchronisationType(
          comparator_opt->reused_synchronisation_type);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setReusedSynchronisationType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ComparatorOptions_setReusedSynchronisationType(
    JNIEnv*, jclass, jlong jhandle, jbyte jreused_synhcronisation_type) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  comparator_opt->reused_synchronisation_type =
      ROCKSDB_NAMESPACE::ReusedSynchronisationTypeJni::
          toCppReusedSynchronisationType(jreused_synhcronisation_type);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    useDirectBuffer
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ComparatorOptions_useDirectBuffer(JNIEnv*, jclass,
                                                            jlong jhandle) {
  return static_cast<jboolean>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle)
          ->direct_buffer);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setUseDirectBuffer
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ComparatorOptions_setUseDirectBuffer(
    JNIEnv*, jclass, jlong jhandle, jboolean jdirect_buffer) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(jhandle)
      ->direct_buffer = jdirect_buffer == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    maxReusedBufferSize
 * Signature: (J)I
 */
jint Java_org_rocksdb_ComparatorOptions_maxReusedBufferSize(JNIEnv*, jclass,
                                                            jlong jhandle) {
  return static_cast<jint>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle)
          ->max_reused_buffer_size);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setMaxReusedBufferSize
 * Signature: (JI)V
 */
void Java_org_rocksdb_ComparatorOptions_setMaxReusedBufferSize(
    JNIEnv*, jclass, jlong jhandle, jint jmax_reused_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(jhandle)
      ->max_reused_buffer_size = static_cast<int32_t>(jmax_reused_buffer_size);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ComparatorOptions_disposeInternalJni(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  assert(comparator_opt != nullptr);
  delete comparator_opt;
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::FlushOptions

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    newFlushOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlushOptions_newFlushOptions(JNIEnv*, jclass) {
  auto* flush_opt = new ROCKSDB_NAMESPACE::FlushOptions();
  return GET_CPLUSPLUS_POINTER(flush_opt);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setWaitForFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setWaitForFlush(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jboolean jwait) {
  reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle)->wait =
      static_cast<bool>(jwait);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    waitForFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_waitForFlush(JNIEnv*, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle)->wait;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setAllowWriteStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setAllowWriteStall(
    JNIEnv*, jclass, jlong jhandle, jboolean jallow_write_stall) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  flush_options->allow_write_stall = jallow_write_stall == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    allowWriteStall
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_allowWriteStall(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  return static_cast<jboolean>(flush_options->allow_write_stall);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setListenerWait
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setListenerWait(JNIEnv*, jclass,
                                                   jlong jhandle,
                                                   jboolean jlistener_wait) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  flush_options->listener_wait = jlistener_wait == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    listenerWait
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_listenerWait(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  return static_cast<jboolean>(flush_options->listener_wait);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_FlushOptions_disposeInternalJni(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* flush_opt = reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  assert(flush_opt != nullptr);
  delete flush_opt;
}
