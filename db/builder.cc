//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/blob/blob_counting_iterator.h"
#include "db/blob/blob_file_builder.h"
#include "db/blob/blob_garbage_meter.h"
#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "seqno_to_time_mapping.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

class TableFactory;

TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  return tboptions.moptions.table_factory->NewTableBuilder(tboptions, file);
}

void ExtractTimestampFromTableProperties(const TableProperties& tp,
                                         FileMetaData* meta) {
  auto min_ts_iter = tp.user_collected_properties.find("rocksdb.timestamp_min");
  if (min_ts_iter != tp.user_collected_properties.end()) {
    meta->min_timestamp = min_ts_iter->second;
  }
  auto max_ts_iter = tp.user_collected_properties.find("rocksdb.timestamp_max");
  if (max_ts_iter != tp.user_collected_properties.end()) {
    meta->max_timestamp = max_ts_iter->second;
  }
}

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
    EventLogger* event_logger, int job_id, TableProperties* table_properties,
    Env::WriteLifeTimeHint write_hint, const std::string* full_history_ts_low,
    BlobFileCompletionCallback* blob_callback, Version* version,
    uint64_t* memtable_payload_bytes, uint64_t* memtable_garbage_bytes,
    InternalStats::CompactionStats* flush_stats,
    std::vector<BlobFileGarbage>* blob_file_garbages, bool fast_sst_open) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  auto& mutable_cf_options = tboptions.moptions;
  auto& ioptions = tboptions.ioptions;
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  OutputValidator output_validator(tboptions.internal_comparator,
                                   /*enable_hash=*/paranoid_file_checks);
  Status s;
  meta->fd.file_size = 0;
  if (blob_file_garbages != nullptr) {
    blob_file_garbages->clear();
  }

  InternalIterator* input = iter;
  std::unique_ptr<BlobGarbageMeter> blob_garbage_meter;
  std::unique_ptr<BlobCountingIterator> blob_counting_iter;
  if (blob_file_garbages != nullptr) {
    // Flush can drop blob-backed records via overwrite elision and flush-time
    // compaction filters. Track input and surviving output refs just like the
    // compaction path so the manifest learns about the new garbage.
    blob_garbage_meter.reset(new BlobGarbageMeter());
    blob_counting_iter.reset(
        new BlobCountingIterator(iter, blob_garbage_meter.get(),
                                 /*track_current_key_inflow=*/false));
    input = blob_counting_iter.get();
  }

  input->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&tboptions.internal_comparator,
                                       snapshots, full_history_ts_low));
  uint64_t num_unfragmented_tombstones = 0;
  uint64_t total_tombstone_payload_bytes = 0;
  for (auto& range_del_iter : range_del_iters) {
    num_unfragmented_tombstones +=
        range_del_iter->num_unfragmented_tombstones();
    total_tombstone_payload_bytes +=
        range_del_iter->total_tombstone_payload_bytes();
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  EventHelpers::NotifyTableFileCreationStarted(ioptions.listeners, dbname,
                                               tboptions.column_family_name,
                                               fname, job_id, tboptions.reason);
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);

  TableProperties tp;
  bool table_file_created = false;
  if (input->Valid() || !range_del_agg->IsEmpty()) {
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions.compaction_filter_factory != nullptr &&
        ioptions.compaction_filter_factory->ShouldFilterTableFileCreation(
            tboptions.reason)) {
      CompactionFilter::Context context;
      context.is_full_compaction = false;
      context.is_manual_compaction = false;
      context.column_family_id = tboptions.column_family_id;
      context.reason = tboptions.reason;
      compaction_filter =
          ioptions.compaction_filter_factory->CreateCompactionFilter(context);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s.PermitUncheckedError();
        return Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
      }
    }

    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<FSWritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = file_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      FileOptions fo_copy = file_options;
      fo_copy.open_contract = FileOpenContract::kNoReopenForWrite |
                              FileOpenContract::kNoReadersWhileOpenForWrite;
      fo_copy.write_hint = write_hint;
      IOStatus io_s = NewWritableFile(fs, fname, &file, fo_copy);
      assert(s.ok());
      s = io_s;
      if (io_status->ok()) {
        *io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname,
            tboptions.column_family_name, fname, job_id, meta->fd,
            kInvalidBlobFileNumber, tp, tboptions.reason, s, file_checksum,
            file_checksum_func_name);
        return s;
      }

      table_file_created = true;
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(tboptions.write_options.rate_limiter_priority);
      // Subsequent attempts to override the hint via SetWriteLifeTimeHint
      // with the very same value will be ignored by the fs.
      file->SetWriteLifeTimeHint(fo_copy.write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, io_tracer,
          ioptions.stats, Histograms::SST_WRITE_MICROS, ioptions.listeners,
          ioptions.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile),
          tmp_set.Contains(FileType::kTableFile)));

      builder = NewTableBuilder(tboptions, file_writer.get());
    }

    auto ucmp = tboptions.internal_comparator.user_comparator();
    MergeHelper merge(
        env, ucmp, ioptions.merge_operator.get(), compaction_filter.get(),
        ioptions.logger, true /* internal key corruption is not ok */,
        snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);

    std::unique_ptr<BlobFileBuilder> blob_file_builder(
        (mutable_cf_options.enable_blob_files &&
         tboptions.level_at_creation >=
             mutable_cf_options.blob_file_starting_level &&
         blob_file_additions)
            ? new BlobFileBuilder(
                  versions, fs, &ioptions, &mutable_cf_options, &file_options,
                  &(tboptions.write_options), tboptions.db_id,
                  tboptions.db_session_id, job_id, tboptions.column_family_id,
                  tboptions.column_family_name, write_hint, io_tracer,
                  blob_callback, blob_creation_reason,
                  ioptions.cf_paths.front().path, &blob_file_paths,
                  blob_file_additions)
            : nullptr);

    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        input, ucmp, &merge, kMaxSequenceNumber, &snapshots, earliest_snapshot,
        earliest_write_conflict_snapshot, job_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.stats), range_del_agg.get(),
        blob_file_builder.get(), ioptions.allow_data_in_errors,
        ioptions.enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        true /* must_count_input_entries */,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, db_options.info_log, full_history_ts_low,
        std::nullopt, version, tboptions.read_options.io_activity);

    if (version != nullptr) {
      ColumnFamilyData* const cfd = version->cfd();
      c_iter.SetBlobFetcher(
          version, cfd != nullptr ? cfd->blob_file_cache() : nullptr,
          tboptions.read_options.io_activity,
          tboptions.reason == TableFileCreationReason::kFlush ||
              tboptions.reason == TableFileCreationReason::kRecovery);
    }

    SequenceNumber smallest_preferred_seqno = kMaxSequenceNumber;
    std::string key_after_flush_buf;
    std::string value_buf;
    c_iter.SeekToFirst();
    for (; c_iter.Valid(); c_iter.Next()) {
      const Slice& key = c_iter.key();
      const Slice& value = c_iter.value();
      ParsedInternalKey ikey = c_iter.ikey();
      Slice key_after_flush = key;
      Slice value_after_flush = value;

      if (ikey.type == kTypeValuePreferredSeqno) {
        auto [unpacked_value, unix_write_time] =
            ParsePackedValueWithWriteTime(value);
        SequenceNumber preferred_seqno =
            seqno_to_time_mapping
                ? seqno_to_time_mapping->GetProximalSeqnoBeforeTime(
                      unix_write_time)
                : kMaxSequenceNumber;
        if (preferred_seqno < ikey.sequence) {
          value_after_flush =
              PackValueAndSeqno(unpacked_value, preferred_seqno, &value_buf);
          smallest_preferred_seqno =
              std::min(smallest_preferred_seqno, preferred_seqno);
        } else {
          // Cannot get a useful preferred seqno, convert it to a kTypeValue.
          key_after_flush_buf.assign(key.data(), key.size());
          UpdateInternalKey(&key_after_flush_buf, ikey.sequence, kTypeValue);
          ikey = ParsedInternalKey(ikey.user_key, ikey.sequence, kTypeValue);
          key_after_flush = key_after_flush_buf;
          value_after_flush = ParsePackedValueForValue(value);
        }
      }

      //  Generate a rolling 64-bit hash of the key and values
      //  Note :
      //  Here "key" integrates 'sequence_number'+'kType'+'user key'.
      s = output_validator.Add(key_after_flush, value_after_flush);
      if (!s.ok()) {
        break;
      }
      builder->Add(key_after_flush, value_after_flush);

      if (flush_stats) {
        flush_stats->num_output_records++;
      }

      if (blob_garbage_meter != nullptr) {
        s = blob_garbage_meter->ProcessOutFlow(key_after_flush,
                                               value_after_flush);
        if (!s.ok()) {
          break;
        }
      }

      s = meta->UpdateBoundaries(key_after_flush, value_after_flush,
                                 ikey.sequence, ikey.type);
      if (!s.ok()) {
        break;
      }

      // TODO(noetzli): Update stats after flush, too.
      // TODO(hx235): Replace `rate_limiter_priority` with `io_activity` for
      // flush IO in repair when we have an `Env::IOActivity` enum for it
      if ((tboptions.write_options.io_activity == Env::IOActivity::kFlush ||
           tboptions.write_options.io_activity == Env::IOActivity::kDBOpen ||
           tboptions.write_options.rate_limiter_priority == Env::IO_HIGH) &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      Slice last_tombstone_start_user_key{};
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        std::pair<InternalKey, Slice> kv = tombstone.Serialize();
        builder->Add(kv.first.Encode(), kv.second);
        if (flush_stats) {
          flush_stats->num_output_records++;
        }
        InternalKey tombstone_end = tombstone.SerializeEndKey();
        meta->UpdateBoundariesForRange(kv.first, tombstone_end, tombstone.seq_,
                                       tboptions.internal_comparator);
        if (version) {
          if (last_tombstone_start_user_key.empty() ||
              ucmp->CompareWithoutTimestamp(last_tombstone_start_user_key,
                                            range_del_it->start_key()) < 0) {
            SizeApproximationOptions approx_opts;
            approx_opts.files_size_error_margin = 0.1;
            meta->compensated_range_deletion_size += versions->ApproximateSize(
                approx_opts, tboptions.read_options, version, kv.first.Encode(),
                tombstone_end.Encode(), 0 /* start_level */, -1 /* end_level */,
                TableReaderCaller::kFlush);
          }
          last_tombstone_start_user_key = range_del_it->start_key();
        }
      }
    }

    TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
    TEST_SYNC_POINT_CALLBACK("BuildTable:BeforeCheckEmpty",
                             static_cast<TableBuilder*>(builder));
    const bool empty = builder->IsEmpty();
    if (flush_stats) {
      assert(c_iter.HasNumInputEntryScanned());
      flush_stats->num_input_records =
          c_iter.NumInputEntryScanned() + num_unfragmented_tombstones;
    }
    if (!s.ok() || empty) {
      builder->Abandon();
      // Propagate the builder's error when the builder is empty due to an
      // internal error (e.g., write fault injection causing all Add() calls to
      // return early). Without this, `s` would remain OK and the downstream
      // key count validation in flush_job.cc would produce a misleading
      // Corruption error instead of the actual builder error.
      if (s.ok() && !builder->status().ok()) {
        s = builder->status();
      }
    } else {
      SeqnoToTimeMapping relevant_mapping;
      if (seqno_to_time_mapping) {
        relevant_mapping.CopyFromSeqnoRange(
            *seqno_to_time_mapping,
            std::min(meta->fd.smallest_seqno, smallest_preferred_seqno),
            meta->fd.largest_seqno);
        relevant_mapping.SetCapacity(kMaxSeqnoTimePairsPerSST);
        relevant_mapping.Enforce(tboptions.file_creation_time);
      }
      builder->SetSeqnoTimeTableProperties(
          relevant_mapping,
          ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
              ? meta->file_creation_time
              : meta->oldest_ancestor_time);
      s = builder->Finish();
    }
    if (io_status->ok()) {
      *io_status = builder->io_status();
    }

    if (s.ok() && !empty) {
      if (flush_stats) {
        flush_stats->bytes_written_pre_comp = builder->PreCompressionSize();
        // Add worker CPU micros here. Caller needs to add CPU micros from
        // calling thread.
        flush_stats->cpu_micros += builder->GetWorkerCPUMicros();
      }
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->tail_size = builder->GetTailSize();
      meta->marked_for_compaction = builder->NeedCompact();
      meta->user_defined_timestamps_persisted =
          ioptions.persist_user_defined_timestamps;
      assert(meta->fd.GetFileSize() > 0);
      tp = builder
               ->GetTableProperties();  // refresh now that builder is finished
      ExtractTimestampFromTableProperties(tp, meta);
      if (memtable_payload_bytes != nullptr &&
          memtable_garbage_bytes != nullptr) {
        const CompactionIterationStats& ci_stats = c_iter.iter_stats();
        uint64_t total_payload_bytes = ci_stats.total_input_raw_key_bytes +
                                       ci_stats.total_input_raw_value_bytes +
                                       total_tombstone_payload_bytes;
        uint64_t total_payload_bytes_written =
            (tp.raw_key_size + tp.raw_value_size);
        // Prevent underflow, which may still happen at this point
        // since we only support inserts, deletes, and deleteRanges.
        if (total_payload_bytes_written <= total_payload_bytes) {
          *memtable_payload_bytes = total_payload_bytes;
          *memtable_garbage_bytes =
              total_payload_bytes - total_payload_bytes_written;
        } else {
          *memtable_payload_bytes = 0;
          *memtable_garbage_bytes = 0;
        }
      }
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
    IOOptions opts;
    *io_status =
        WritableFileWriter::PrepareIOOptions(tboptions.write_options, opts);
    if (s.ok() && io_status->ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
      *io_status = ioptions.use_fsync ? file_writer->Fsync(opts)
                                      : file_writer->Sync(opts);
    }
    TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
    if (s.ok() && io_status->ok() && !empty) {
      *io_status = file_writer->Close(opts);
    }
    if (s.ok() && io_status->ok() && !empty) {
      // Add the checksum information to file metadata.
      meta->file_checksum = file_writer->GetFileChecksum();
      meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      file_checksum = meta->file_checksum;
      file_checksum_func_name = meta->file_checksum_func_name;
      // Set unique_id only if db_id and db_session_id exist
      if (!tboptions.db_id.empty() && !tboptions.db_session_id.empty()) {
        if (!GetSstInternalUniqueId(tboptions.db_id, tboptions.db_session_id,
                                    meta->fd.GetNumber(), &(meta->unique_id))
                 .ok()) {
          // if failed to get unique id, just set it Null
          meta->unique_id = kNullUniqueId64x2;
        }
      }
    }

    if (s.ok()) {
      s = *io_status;
    }

    // TODO(yuzhangyu): handle the key copy in the blob when ts should be
    // stripped.
    if (blob_file_builder) {
      if (s.ok()) {
        s = blob_file_builder->Finish();
      } else {
        blob_file_builder->Abandon(s);
      }
      blob_file_builder.reset();
    }

    // TODO Also check the IO status when create the Iterator.

    TEST_SYNC_POINT("BuildTable:BeforeOutputValidation");
    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building.
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // the goal is to cache it here for further user reads.
      std::string* file_open_metadata_ptr =
          fast_sst_open ? &meta->file_open_metadata : nullptr;
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          tboptions.read_options, file_options, tboptions.internal_comparator,
          *meta, nullptr /* range_del_agg */, mutable_cf_options, nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          TableReaderCaller::kFlush, /*arena=*/nullptr,
          /*skip_filter=*/false, tboptions.level_at_creation,
          MaxFileSizeForL0MetaPin(mutable_cf_options),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key*/ nullptr,
          /*allow_unprepared_value*/ false,
          /*range_del_read_seqno=*/nullptr,
          /*range_del_iter=*/nullptr,
          /*maybe_pin_table_handle=*/false, file_open_metadata_ptr));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        OutputValidator file_validator(tboptions.internal_comparator,
                                       /*enable_hash=*/true);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          // Generate a rolling 64-bit hash of the key and values
          file_validator.Add(it->key(), it->value()).PermitUncheckedError();
        }
        s = it->status();
        if (s.ok() && !output_validator.CompareValidator(file_validator)) {
          s = Status::Corruption("Paranoid checksums do not match");
        }
      }
    }
  }

  // Check for input iterator errors
  if (!input->status().ok()) {
    s = input->status();
  }

  if (s.ok() && blob_file_garbages != nullptr &&
      blob_garbage_meter != nullptr) {
    for (const auto& pair : blob_garbage_meter->flows()) {
      const uint64_t blob_file_number = pair.first;
      const BlobGarbageMeter::BlobInOutFlow& flow = pair.second;
      if (flow.HasGarbage()) {
        blob_file_garbages->emplace_back(
            blob_file_number, flow.GetGarbageCount(), flow.GetGarbageBytes());
      }
    }
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    if (table_file_created) {
      IOOptions opts;
      Status prepare =
          WritableFileWriter::PrepareIOOptions(tboptions.write_options, opts);
      if (prepare.ok()) {
        // FIXME: track file for "slow" deletion, e.g. into the
        // VersionSet::obsolete_files_ pipeline
        Status ignored = fs->DeleteFile(fname, opts, dbg);
        ignored.PermitUncheckedError();
      }
      // Ensure we don't leak table cache entries when throwing away output
      // files. (The usual logic in PurgeObsoleteFiles is not applicable because
      // this function deletes the obsolete file itself, while they should
      // probably go into the VersionSet::obsolete_files_ pipeline.)
      TableCache::ReleaseObsolete(table_cache->get_cache().get(),
                                  meta->fd.GetNumber(), nullptr /*handle*/,
                                  mutable_cf_options.uncache_aggressiveness);
    }

    assert(blob_file_additions || blob_file_paths.empty());

    if (blob_file_additions) {
      for (const std::string& blob_file_path : blob_file_paths) {
        Status ignored = DeleteDBFile(&db_options, blob_file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
        ignored.PermitUncheckedError();
        TEST_SYNC_POINT("BuildTable::AfterDeleteFile");
      }
    }
  }

  Status status_for_listener = s;
  if (meta->fd.GetFileSize() == 0) {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, tboptions.column_family_name,
      fname, job_id, meta->fd, meta->oldest_blob_file_number, tp,
      tboptions.reason, status_for_listener, file_checksum,
      file_checksum_func_name);

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
