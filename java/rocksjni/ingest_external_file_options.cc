// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>

#include "include/org_rocksdb_IngestExternalFileOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    newIngestExternalFileOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_IngestExternalFileOptions_newIngestExternalFileOptions__(
    JNIEnv*, jclass) {
  auto* options = new ROCKSDB_NAMESPACE::IngestExternalFileOptions();
  return GET_CPLUSPLUS_POINTER(options);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    newIngestExternalFileOptions
 * Signature: (ZZZZ)J
 */
jlong Java_org_rocksdb_IngestExternalFileOptions_newIngestExternalFileOptions__ZZZZ(
    JNIEnv*, jclass, jboolean jmove_files, jboolean jsnapshot_consistency,
    jboolean jallow_global_seqno, jboolean jallow_blocking_flush) {
  auto* options = new ROCKSDB_NAMESPACE::IngestExternalFileOptions();
  options->move_files = static_cast<bool>(jmove_files);
  options->snapshot_consistency = static_cast<bool>(jsnapshot_consistency);
  options->allow_global_seqno = static_cast<bool>(jallow_global_seqno);
  options->allow_blocking_flush = static_cast<bool>(jallow_blocking_flush);
  return GET_CPLUSPLUS_POINTER(options);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    moveFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_moveFiles(JNIEnv*, jclass,
                                                              jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return static_cast<jboolean>(options->move_files);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setMoveFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setMoveFiles(
    JNIEnv*, jclass, jlong jhandle, jboolean jmove_files) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->move_files = static_cast<bool>(jmove_files);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    snapshotConsistency
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_snapshotConsistency(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return static_cast<jboolean>(options->snapshot_consistency);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setSnapshotConsistency
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setSnapshotConsistency(
    JNIEnv*, jclass, jlong jhandle, jboolean jsnapshot_consistency) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->snapshot_consistency = static_cast<bool>(jsnapshot_consistency);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    allowGlobalSeqNo
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_allowGlobalSeqNo(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return static_cast<jboolean>(options->allow_global_seqno);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setAllowGlobalSeqNo
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setAllowGlobalSeqNo(
    JNIEnv*, jclass, jlong jhandle, jboolean jallow_global_seqno) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->allow_global_seqno = static_cast<bool>(jallow_global_seqno);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    allowBlockingFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_allowBlockingFlush(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return static_cast<jboolean>(options->allow_blocking_flush);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setAllowBlockingFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setAllowBlockingFlush(
    JNIEnv*, jclass, jlong jhandle, jboolean jallow_blocking_flush) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->allow_blocking_flush = static_cast<bool>(jallow_blocking_flush);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    ingestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_ingestBehind(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return options->ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setIngestBehind(
    JNIEnv*, jclass, jlong jhandle, jboolean jingest_behind) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->ingest_behind = jingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    rejectTooOldFile
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_IngestExternalFileOptions_rejectTooOldFile(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  return static_cast<jboolean>(options->reject_too_old_file);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    setRejectTooOldFile
 * Signature: (JZ)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_setRejectTooOldFile(
    JNIEnv*, jclass, jlong jhandle, jboolean jreject_too_old_file) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  options->reject_too_old_file = static_cast<bool>(jreject_too_old_file);
}

/*
 * Class:     org_rocksdb_IngestExternalFileOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_IngestExternalFileOptions_disposeInternalJni(
    JNIEnv*, jclass, jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(jhandle);
  delete options;
}
