// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the structures for thread operation and state.
// Thread operations are used to describe high level action of a
// thread such as doing compaction or flush, while thread state
// are used to describe lower-level action such as reading /
// writing a file or waiting for a mutex.  Operations and states
// are designed to be independent.  Typically, a thread usually involves
// in one operation and one state at any specific point in time.

#pragma once

#include <string>

#include "rocksdb/thread_status.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NROCKSDB_THREAD_STATUS

// The structure that describes a major thread operation.
struct OperationInfo {
  const ThreadStatus::OperationType type;
  const std::string name;
};

// The global operation table.
//
// When updating a status of a thread, the pointer of the OperationInfo
// of the current ThreadStatusData will be pointing to one of the
// rows in this global table.
static const OperationInfo global_operation_table[] = {
    {ThreadStatus::OP_UNKNOWN, ""},
    {ThreadStatus::OP_COMPACTION, "Compaction"},
    {ThreadStatus::OP_FLUSH, "Flush"},
    {ThreadStatus::OP_DBOPEN, "DBOpen"},
    {ThreadStatus::OP_GET, "Get"},
    {ThreadStatus::OP_MULTIGET, "MultiGet"},
    {ThreadStatus::OP_DBITERATOR, "DBIterator"},
    {ThreadStatus::OP_VERIFY_DB_CHECKSUM, "VerifyDBChecksum"},
    {ThreadStatus::OP_VERIFY_FILE_CHECKSUMS, "VerifyFileChecksums"},
    {ThreadStatus::OP_GETENTITY, "GetEntity"},
    {ThreadStatus::OP_MULTIGETENTITY, "MultiGetEntity"},
    {ThreadStatus::OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST,
     "GetFileChecksumsFromCurrentManifest"},
    {ThreadStatus::OP_LAZY_RESOLVE, "LazyResolve"},

};

struct OperationStageInfo {
  const ThreadStatus::OperationStage stage;
  const std::string name;
};

// A table maintains the mapping from stage type to stage string.
// Note that the string must be changed accordingly when the
// associated function name changed.
static const OperationStageInfo global_op_stage_table[] = {
    {ThreadStatus::STAGE_UNKNOWN, ""},
    {ThreadStatus::STAGE_FLUSH_RUN, "FlushJob::Run"},
    {ThreadStatus::STAGE_FLUSH_WRITE_L0, "FlushJob::WriteLevel0Table"},
    {ThreadStatus::STAGE_COMPACTION_PREPARE, "CompactionJob::Prepare"},
    {ThreadStatus::STAGE_COMPACTION_RUN, "CompactionJob::Run"},
    {ThreadStatus::STAGE_COMPACTION_PROCESS_KV,
     "CompactionJob::ProcessKeyValueCompaction"},
    {ThreadStatus::STAGE_COMPACTION_INSTALL, "CompactionJob::Install"},
    {ThreadStatus::STAGE_COMPACTION_SYNC_FILE,
     "CompactionJob::FinishCompactionOutputFile"},
    {ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH,
     "MemTableList::PickMemtablesToFlush"},
    {ThreadStatus::STAGE_MEMTABLE_ROLLBACK,
     "MemTableList::RollbackMemtableFlush"},
    {ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS,
     "MemTableList::TryInstallMemtableFlushResults"},
};

// The structure that describes a state.
struct StateInfo {
  const ThreadStatus::StateType type;
  const std::string name;
};

// The global state table.
//
// When updating a status of a thread, the pointer of the StateInfo
// of the current ThreadStatusData will be pointing to one of the
// rows in this global table.
static const StateInfo global_state_table[] = {
    {ThreadStatus::STATE_UNKNOWN, ""},
    {ThreadStatus::STATE_MUTEX_WAIT, "Mutex Wait"},
};

struct OperationProperty {
  int code;
  std::string name;
};

static const OperationProperty compaction_operation_properties[] = {
    {ThreadStatus::COMPACTION_JOB_ID, "JobID"},
    {ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL, "InputOutputLevel"},
    {ThreadStatus::COMPACTION_PROP_FLAGS, "Manual/Deletion/Trivial"},
    {ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES, "TotalInputBytes"},
    {ThreadStatus::COMPACTION_BYTES_READ, "BytesRead"},
    {ThreadStatus::COMPACTION_BYTES_WRITTEN, "BytesWritten"},
};

static const OperationProperty flush_operation_properties[] = {
    {ThreadStatus::FLUSH_JOB_ID, "JobID"},
    {ThreadStatus::FLUSH_BYTES_MEMTABLES, "BytesMemtables"},
    {ThreadStatus::FLUSH_BYTES_WRITTEN, "BytesWritten"}};

#else

struct OperationInfo {};

struct StateInfo {};

#endif  // !NROCKSDB_THREAD_STATUS
}  // namespace ROCKSDB_NAMESPACE
