//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <string>
#include <utility>
#include <vector>

#include "db/internal_stats.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/seqno_to_time_mapping.h"
#include "db/table_properties_collector.h"
#include "db/version_set.h"
#include "logging/event_logger.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

struct FileMetaData;

class VersionSet;
class BlobFileAddition;
class BlobFileGarbage;
class SnapshotChecker;
class TableCache;
class TableBuilder;
class WritableFileWriter;
class BlobFileCompletionCallback;

// Convenience function for NewTableBuilder on the embedded table_factory.
TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file);

// Extract min/max timestamps from table properties and populate FileMetaData.
// This is used by both flush (BuildTable) and compaction (CompactionOutputs)
// to populate timestamp range in FileMetaData from the TimestampTableProperties
// collector output.
void ExtractTimestampFromTableProperties(const TableProperties& tp,
                                         FileMetaData* meta);

// Copies the exact external-blob reference descriptor into MANIFEST-owned file
// metadata. Absence is preserved for legacy/pre-activation SSTs.
void ExtractBlobReferenceDescriptorFromTableProperties(
    const TableProperties& tp, FileMetaData* meta);
Status ExtractBlobReferenceClassificationFromTableProperties(
    const TableProperties& tp, FileMetaData* meta);

// Build a Table file from the contents of *iter.  The generated file
// will be named according to number specified in meta. On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
//
// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown.
// @param flush_stats treat flush as level 0 compaction in internal stats
Status BuildTable(
    const std::string& dbname, VersionSet* versions,
    const ImmutableDBOptions& db_options, const TableBuilderOptions& tboptions,
    const FileOptions& file_options, TableCache* table_cache,
    InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, std::vector<BlobFileAddition>* blob_file_additions,
    std::vector<SequenceNumber> snapshots, SequenceNumber earliest_snapshot,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, SnapshotChecker* snapshot_checker,
    bool paranoid_file_checks, InternalStats* internal_stats,
    IOStatus* io_status, const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCreationReason blob_creation_reason,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    EventLogger* event_logger = nullptr, int job_id = 0,
    TableProperties* table_properties = nullptr,
    Env::WriteLifeTimeHint write_hint = Env::WLTH_NOT_SET,
    const std::string* full_history_ts_low = nullptr,
    BlobFileCompletionCallback* blob_callback = nullptr,
    Version* version = nullptr, uint64_t* memtable_payload_bytes = nullptr,
    uint64_t* memtable_garbage_bytes = nullptr,
    InternalStats::CompactionStats* flush_stats = nullptr,
    std::vector<BlobFileGarbage>* blob_file_garbages = nullptr,
    bool fast_sst_open = false);

}  // namespace ROCKSDB_NAMESPACE
