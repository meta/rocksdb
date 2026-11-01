//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The test uses an array to compare against values written to the database.
// Keys written to the array are in 1:1 correspondence to the actual values in
// the database according to the formula in the function GenerateValue.

// Space is reserved in the array from 0 to FLAGS_max_key and values are
// randomly written/deleted/read from those positions. During verification we
// compare all the positions in the array. To shorten/elongate the running
// time, you could change the settings: FLAGS_max_key, FLAGS_ops_per_thread,
// (sometimes also FLAGS_threads).
//
// NOTE that if FLAGS_test_batches_snapshots is set, the test will have
// different behavior. See comment of the flag for details.

#ifdef GFLAGS
#include <iostream>
#include <thread>

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_driver.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> legacy_env_wrapper_guard;
// Raw pointers for signal-safe crash callback. Signal handlers can only
// access file-static/global variables; can't capture StressTest instances.
static std::vector<ROCKSDB_NAMESPACE::FaultInjectionTestFS*>
    fault_fs_for_crash_flush;

template <typename... Args>
int ReturnValidationError(const Args&... args) {
  std::cerr << "Error: ";
  (std::cerr << ... << args) << '\n';
  return 1;
}

int ValidateNumDbsFlags() {
  if (FLAGS_num_dbs < 1) {
    return ReturnValidationError("--num_dbs must be >= 1");
  }
  if (FLAGS_num_dbs > 1) {
    if (FLAGS_clear_column_family_one_in > 0) {
      return ReturnValidationError(
          "--num_dbs > 1 incompatible with --clear_column_family_one_in");
    }
    if (FLAGS_test_multi_ops_txns) {
      return ReturnValidationError(
          "--num_dbs > 1 incompatible with --test_multi_ops_txns");
    }
  }
  return 0;
}

int ValidateFileScopedWalWriteFaultFlags() {
  if (!FLAGS_file_scope_wal_write_faults) {
    return 0;
  }
  if (FLAGS_write_fault_one_in <= 0) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults requires --write_fault_one_in > 0");
  }
  if (FLAGS_exclude_wal_from_write_fault_injection) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults requires "
        "--exclude_wal_from_write_fault_injection=false");
  }
  if (FLAGS_inject_error_severity != 1) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults requires "
        "--inject_error_severity=1");
  }
  if (FLAGS_test_batches_snapshots) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults does not support "
        "--test_batches_snapshots");
  }
  if (FLAGS_reopen > 0) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults does not support --reopen > 0");
  }
  if (!FLAGS_options_file.empty()) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults does not support --options_file");
  }
  if (FLAGS_sync_fault_injection || FLAGS_read_fault_one_in > 0 ||
      FLAGS_metadata_read_fault_one_in > 0 ||
      FLAGS_metadata_write_fault_one_in > 0 ||
      FLAGS_open_read_fault_one_in > 0 || FLAGS_open_write_fault_one_in > 0 ||
      FLAGS_open_metadata_read_fault_one_in > 0 ||
      FLAGS_open_metadata_write_fault_one_in > 0 ||
      FLAGS_secondary_cache_fault_one_in > 0) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults requires all non-WAL fault injection "
        "to be disabled");
  }
  if (FLAGS_disable_wal || FLAGS_manual_wal_flush_one_in > 0 || FLAGS_use_txn ||
      FLAGS_two_write_queues || FLAGS_enable_pipelined_write ||
      FLAGS_unordered_write || FLAGS_recycle_log_file_num != 0 ||
      FLAGS_WAL_ttl_seconds != 0 || FLAGS_WAL_size_limit_MB != 0 ||
      FLAGS_track_and_verify_wals || FLAGS_enable_blob_direct_write) {
    return ReturnValidationError(
        "--file_scope_wal_write_faults does not support disabled or manual "
        "WALs, transactions, two write queues, pipelined or unordered "
        "writes, WAL recycling, retention or tracking, or blob direct write");
  }
  return 0;
}

int ValidateCfLifecycleFlags() {
  if (FLAGS_cf_lifecycle_fault_target < 0 ||
      FLAGS_cf_lifecycle_fault_target > 2) {
    return ReturnValidationError(
        "--cf_lifecycle_fault_target must be 0, 1, or 2");
  }
  if (!FLAGS_test_cf_lifecycle) {
    if (FLAGS_cf_lifecycle_fault_target != 0) {
      return ReturnValidationError(
          "--cf_lifecycle_fault_target requires --test_cf_lifecycle");
    }
    return 0;
  }
  if (FLAGS_column_families != 2 || FLAGS_num_dbs != 1 || FLAGS_threads != 1 ||
      FLAGS_read_only || FLAGS_test_batches_snapshots ||
      FLAGS_test_cf_consistency || FLAGS_test_multi_ops_txns || FLAGS_use_txn ||
      FLAGS_use_optimistic_txn || FLAGS_best_efforts_recovery ||
      FLAGS_ttl != -1 || FLAGS_use_blob_db || FLAGS_enable_blob_files ||
      FLAGS_enable_blob_direct_write ||
      FLAGS_allow_setting_blob_options_dynamically || FLAGS_test_secondary ||
      FLAGS_remote_compaction_worker_threads != 0 ||
      FLAGS_verify_before_write || !FLAGS_skip_verifydb ||
      !FLAGS_verify_cpu_corruption_dir.empty() ||
      FLAGS_open_read_only_one_in > 0 || FLAGS_reopen != 0 ||
      FLAGS_reuse_manifest_on_open || !FLAGS_options_file.empty() ||
      !FLAGS_expected_values_dir.empty() || !FLAGS_env_uri.empty() ||
      !FLAGS_fs_uri.empty()) {
    return ReturnValidationError(
        "--test_cf_lifecycle requires exactly two column families, one worker "
        "thread, one local read-write plain DB, reopen=0, MANIFEST reuse "
        "disabled, and no remote compaction, "
        "batch/CF-consistency/multi-op transaction, best-efforts recovery, "
        "TTL, BlobDB or dynamic blob options, persistent/fresh-state "
        "verification, secondary/read-only opens, or options-file mode");
  }
  if (FLAGS_cf_lifecycle_fault_target != 0 &&
      (!FLAGS_track_options_file_number_in_manifest ||
       FLAGS_clear_column_family_one_in <= 0)) {
    return ReturnValidationError(
        "targeted CF lifecycle faults require tracked OPTIONS and "
        "--clear_column_family_one_in > 0");
  }
  if (FLAGS_sync_fault_injection || FLAGS_read_fault_one_in > 0 ||
      FLAGS_write_fault_one_in > 0 || FLAGS_metadata_read_fault_one_in > 0 ||
      FLAGS_metadata_write_fault_one_in > 0 ||
      FLAGS_open_read_fault_one_in > 0 || FLAGS_open_write_fault_one_in > 0 ||
      FLAGS_open_metadata_read_fault_one_in > 0 ||
      FLAGS_open_metadata_write_fault_one_in > 0 ||
      FLAGS_secondary_cache_fault_one_in > 0 ||
      FLAGS_file_scope_wal_write_faults) {
    return ReturnValidationError(
        "--test_cf_lifecycle requires unrelated fault injection to be "
        "disabled");
  }
  return 0;
}

int DestroyAllDbs() {
  bool all_ok = true;
  const int num_dbs = FLAGS_num_dbs;
  auto destroy_one = [&](const std::string& db_path) {
    Status s = DbStressDestroyDb(db_path);
    if (s.ok()) {
      fprintf(stdout, "Successfully destroyed db at %s\n", db_path.c_str());
    } else {
      fprintf(stderr, "Failed to destroy db at %s: %s\n", db_path.c_str(),
              s.ToString().c_str());
      all_ok = false;
    }
  };
  if (num_dbs == 1) {
    destroy_one(FLAGS_db);
  } else {
    for (int i = 0; i < num_dbs; i++) {
      destroy_one(FLAGS_db + "/db_" + std::to_string(i));
    }
    DestroyDir(raw_env, FLAGS_db);
  }
  return all_ok ? 0 : 1;
}

void RegisterCrashCallbacks(
    const std::vector<std::unique_ptr<StressTest>>& stress_tests, int num_dbs) {
  fault_fs_for_crash_flush.resize(num_dbs, nullptr);
  bool any_fault_fs = false;
  for (int i = 0; i < num_dbs; i++) {
    fault_fs_for_crash_flush[i] =
        stress_tests[i]->GetDbFaultInjectionFs().get();
    if (fault_fs_for_crash_flush[i]) {
      any_fault_fs = true;
    }
  }
  if (any_fault_fs) {
    port::RegisterCrashCallback([]() {
      for (auto* fs : fault_fs_for_crash_flush) {
        if (fs) {
          fs->FlushRecentInjectedErrors();
        }
      }
    });
  }
}

}  // namespace

KeyGenContext key_gen_ctx;

int db_stress_tool(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  RegisterDbStressBdwFlagValidators();
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_wbm_flush_policy <
          lossless_cast<int32_t>(WriteBufferFlushPolicy::kFlushOldest) ||
      FLAGS_wbm_flush_policy >
          lossless_cast<int32_t>(
              WriteBufferFlushPolicy::kFlushLargestAcrossDBs)) {
    return ReturnValidationError("--wbm_flush_policy must be 0, 1, or 2");
  }

  SanitizeDoubleParam(&FLAGS_bloom_bits);
  SanitizeDoubleParam(&FLAGS_memtable_prefix_bloom_size_ratio);
  SanitizeDoubleParam(&FLAGS_max_bytes_for_level_multiplier);

  // db_stress may read and write in-development draft format_versions that are
  // not yet published to users. Enable the TEST-only opt-in once, centrally,
  // for the whole run. It stays on regardless of this run's --format_version:
  // the crash test re-randomizes format_version per invocation on the same DB,
  // so a run picking an older version may still need to READ draft-version SSTs
  // written by an earlier run. The opt-in is inert unless an unsupported
  // format_version is actually requested.
  TEST_AllowUnsupportedFormatVersion() = true;

#ifndef NDEBUG
  if (FLAGS_mock_direct_io) {
    SetupSyncPointsToMockDirectIO();
  }
#endif
  compression_type_e = StringToCompressionType(FLAGS_compression_type.c_str());
  bottommost_compression_type_e =
      StringToCompressionType(FLAGS_bottommost_compression_type.c_str());
  checksum_type_e = StringToChecksumType(FLAGS_checksum_type.c_str());

  int env_opts = !FLAGS_env_uri.empty() + !FLAGS_fs_uri.empty();
  if (env_opts > 1) {
    return ReturnValidationError(
        "--env_uri and --fs_uri are mutually exclusive");
  }

  Status s = Env::CreateFromUri(ConfigOptions(), FLAGS_env_uri, FLAGS_fs_uri,
                                &raw_env, &env_guard);
  DB_STRESS_ASSERT_OK_MSG(s, "Error creating Env URI %s",
                          FLAGS_env_uri.c_str());

  // Handle --destroy_db_and_exit early
  if (FLAGS_destroy_db_and_exit) {
    return DestroyAllDbs();
  }

  // Handle --delete_dir_and_exit early, before other option validation
  if (!FLAGS_delete_dir_and_exit.empty()) {
    s = DestroyDir(raw_env, FLAGS_delete_dir_and_exit);
    if (s.ok()) {
      fprintf(stdout, "Successfully deleted directory %s\n",
              FLAGS_delete_dir_and_exit.c_str());
      return 0;
    } else {
      fprintf(stderr, "Failed to delete directory %s: %s\n",
              FLAGS_delete_dir_and_exit.c_str(), s.ToString().c_str());
      return 1;
    }
  }

  {
    int rc = ValidateNumDbsFlags();
    if (rc != 0) {
      return rc;
    }
  }
  {
    int rc = ValidateFileScopedWalWriteFaultFlags();
    if (rc != 0) {
      return rc;
    }
  }
  {
    int rc = ValidateCfLifecycleFlags();
    if (rc != 0) {
      return rc;
    }
  }

  if (FLAGS_use_full_external_table) {
    if (FLAGS_column_families != 1) {
      return ReturnValidationError(
          "use_full_external_table currently requires column_families=1");
    }
    if (FLAGS_user_timestamp_size > 0 || FLAGS_use_timed_put_one_in > 0) {
      return ReturnValidationError(
          "use_full_external_table does not support timestamped entries");
    }
    if (FLAGS_preclude_last_level_data_seconds != 0 ||
        FLAGS_preserve_internal_time_seconds != 0) {
      return ReturnValidationError(
          "use_full_external_table does not support sequence-number time "
          "tracking");
    }
    if (FLAGS_test_backward_scan ||
        (FLAGS_use_multiscan && FLAGS_multiscan_reverse)) {
      return ReturnValidationError(
          "use_full_external_table does not support backward iteration");
    }
  }

  if (!FLAGS_verify_cpu_corruption_dir.empty()) {
    // The full-keyspace read-back is only well-defined with a single writer,
    // and injected I/O faults would taint it -- so require --threads=1 and all
    // fault injection off (see the flag's comment).
    if (FLAGS_threads != 1) {
      return ReturnValidationError(
          "--verify_cpu_corruption_dir requires --threads=1");
    }
    if (FLAGS_read_fault_one_in != 0 || FLAGS_write_fault_one_in != 0 ||
        FLAGS_metadata_read_fault_one_in != 0 ||
        FLAGS_metadata_write_fault_one_in != 0 ||
        FLAGS_open_read_fault_one_in != 0 ||
        FLAGS_open_write_fault_one_in != 0 ||
        FLAGS_open_metadata_read_fault_one_in != 0 ||
        FLAGS_open_metadata_write_fault_one_in != 0 ||
        FLAGS_secondary_cache_fault_one_in != 0 || FLAGS_sync_fault_injection) {
      return ReturnValidationError(
          "--verify_cpu_corruption_dir requires all fault injection off "
          "(every *_fault_one_in = 0 and sync_fault_injection = false)");
    }
    // The read-back compares against the expected-state model, which only the
    // default (state-tracked) stress test maintains. The
    // batched/cf-consistency/ multi-ops-txns variants set
    // IsStateTracked()=false, so every key would look absent -- reject them
    // rather than report false losses.
    if (FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency ||
        FLAGS_test_multi_ops_txns) {
      return ReturnValidationError(
          "--verify_cpu_corruption_dir requires the default state-tracked "
          "stress test; it is incompatible with --test_batches_snapshots, "
          "--test_cf_consistency, and --test_multi_ops_txns");
    }
  }

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  raw_env->SetBackgroundThreads(FLAGS_max_background_compactions,
                                ROCKSDB_NAMESPACE::Env::Priority::LOW);
  raw_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);
  if (FLAGS_prefixpercent > 0 && FLAGS_prefix_size < 0) {
    return ReturnValidationError(
        "prefixpercent is non-zero while prefix_size is not positive!");
  }
  if (FLAGS_test_batches_snapshots && FLAGS_prefix_size <= 0) {
    return ReturnValidationError(
        "please specify prefix_size for test_batches_snapshots test!");
  }
  if (FLAGS_memtable_prefix_bloom_size_ratio > 0.0 && FLAGS_prefix_size < 0 &&
      !FLAGS_memtable_whole_key_filtering) {
    return ReturnValidationError(
        "please specify positive prefix_size or enable whole key filtering in "
        "order to use memtable_prefix_bloom_size_ratio");
  }
  if ((FLAGS_readpercent + FLAGS_prefixpercent + FLAGS_writepercent +
       FLAGS_delpercent + FLAGS_delrangepercent + FLAGS_iterpercent +
       FLAGS_customopspercent) != 100) {
    return ReturnValidationError(
        "Read(-readpercent=", FLAGS_readpercent,
        ")+Prefix(-prefixpercent=", FLAGS_prefixpercent,
        ")+Write(-writepercent=", FLAGS_writepercent,
        ")+Delete(-delpercent=", FLAGS_delpercent,
        ")+DeleteRange(-delrangepercent=", FLAGS_delrangepercent,
        ")+Iterate(-iterpercent=", FLAGS_iterpercent,
        ")+CustomOps(-customopspercent=", FLAGS_customopspercent,
        ") percents != 100!");
  }
  if (FLAGS_disable_wal == 1 && FLAGS_reopen > 0) {
    return ReturnValidationError(
        "Db cannot reopen safely with disable_wal set!");
  }
  if ((unsigned)FLAGS_reopen >= FLAGS_ops_per_thread) {
    return ReturnValidationError(
        "#DB-reopens should be < ops_per_thread\nProvided reopens = ",
        FLAGS_reopen, " and ops_per_thread = ", FLAGS_ops_per_thread);
  }
  if (FLAGS_test_batches_snapshots && FLAGS_delrangepercent > 0) {
    return ReturnValidationError(
        "nonzero delrangepercent unsupported in test_batches_snapshots mode");
  }
  if (FLAGS_active_width > FLAGS_max_key) {
    return ReturnValidationError("active_width can be at most max_key");
  } else if (FLAGS_active_width == 0) {
    FLAGS_active_width = FLAGS_max_key;
  }
  if (FLAGS_value_size_mult * kRandomValueMaxFactor > kValueMaxLen) {
    return ReturnValidationError("value_size_mult can be at most ",
                                 kValueMaxLen / kRandomValueMaxFactor);
  }
  if (FLAGS_use_merge && FLAGS_nooverwritepercent == 100) {
    return ReturnValidationError(
        "nooverwritepercent must not be 100 when using merge operands");
  }
  if (FLAGS_enable_blob_direct_write) {
    // Blob direct write is intentionally validated as a reduced-scope stress
    // feature. We allow the WAL-disabled crash-test profile, including
    // wide-column PutEntity/GetEntity coverage, but reject best-efforts
    // recovery, parallel memtable/write-queue variants, transactions, remote
    // compaction, and APIs/features that depend on active-file snapshotting or
    // unsupported blob option transitions.
    if (!FLAGS_enable_blob_files) {
      return ReturnValidationError(
          "enable_blob_direct_write requires enable_blob_files");
    }
    if (FLAGS_allow_concurrent_memtable_write) {
      return ReturnValidationError(
          "blob direct write stress requires "
          "allow_concurrent_memtable_write=0");
    }
    if (FLAGS_enable_pipelined_write) {
      return ReturnValidationError(
          "blob direct write stress does not support "
          "enable_pipelined_write");
    }
    if (FLAGS_unordered_write) {
      return ReturnValidationError(
          "blob direct write stress does not support unordered_write");
    }
    if (FLAGS_two_write_queues) {
      return ReturnValidationError(
          "blob direct write stress does not support two_write_queues");
    }
    if (FLAGS_use_blob_db) {
      return ReturnValidationError(
          "blob direct write is only supported with integrated BlobDB");
    }
    if (FLAGS_use_merge || FLAGS_use_full_merge_v1) {
      return ReturnValidationError(
          "blob direct write stress does not support merge");
    }
    if (FLAGS_experimental_mempurge_threshold > 0.0) {
      return ReturnValidationError(
          "blob direct write stress does not support MemPurge");
    }
    if (FLAGS_user_timestamp_size > 0) {
      return ReturnValidationError(
          "blob direct write stress does not support user-defined timestamps");
    }
    if (FLAGS_allow_setting_blob_options_dynamically ||
        FLAGS_enable_blob_garbage_collection) {
      return ReturnValidationError(
          "blob direct write stress does not support dynamic blob options or "
          "blob GC");
    }
    if (FLAGS_best_efforts_recovery) {
      return ReturnValidationError(
          "blob direct write stress supports disable_wal-based crash "
          "testing, not best-efforts recovery");
    }
    if (FLAGS_remote_compaction_worker_threads > 0) {
      return ReturnValidationError(
          "blob direct write stress does not support remote compaction");
    }
    if (FLAGS_use_txn || FLAGS_txn_write_policy != 0 ||
        FLAGS_use_optimistic_txn || FLAGS_test_multi_ops_txns ||
        FLAGS_commit_bypass_memtable_one_in > 0) {
      return ReturnValidationError(
          "blob direct write stress does not support TransactionDB modes");
    }
    if (FLAGS_test_secondary || FLAGS_backup_one_in > 0 ||
        FLAGS_checkpoint_one_in > 0 || FLAGS_get_live_files_apis_one_in > 0 ||
        FLAGS_ingest_external_file_one_in > 0) {
      return ReturnValidationError(
          "blob direct write stress does not support secondary, backup, "
          "checkpoint, get_live_files, or ingest_external_file modes");
    }
    if (FLAGS_ingest_wbwi_one_in > 0) {
      return ReturnValidationError(
          "blob direct write stress does not support "
          "IngestWriteBatchWithIndex");
    }
  }
  if (FLAGS_ingest_external_file_one_in > 0 &&
      FLAGS_nooverwritepercent == 100) {
    return ReturnValidationError(
        "nooverwritepercent must not be 100 when using file ingestion");
  }
  if (FLAGS_ingest_external_file_atomic_replace_one_in > 0) {
    if (FLAGS_ingest_external_file_one_in <= 0) {
      return ReturnValidationError(
          "ingest_external_file_one_in must be positive when testing atomic "
          "range replacement");
    }
    if (FLAGS_ingest_external_file_width < 2) {
      return ReturnValidationError(
          "ingest_external_file_width must be at least 2 when testing atomic "
          "range replacement");
    }
    if (FLAGS_compaction_style != kCompactionStyleUniversal) {
      return ReturnValidationError(
          "atomic range replacement ingestion requires universal "
          "compaction");
    }
    if (FLAGS_user_timestamp_size > 0) {
      return ReturnValidationError(
          "atomic range replacement ingestion does not support user-defined "
          "timestamps");
    }
    if (FLAGS_acquire_snapshot_one_in > 0) {
      return ReturnValidationError(
          "atomic range replacement ingestion is incompatible with acquired "
          "snapshots");
    }
  }
  if (FLAGS_clear_column_family_one_in > 0 && FLAGS_backup_one_in > 0) {
    return ReturnValidationError(
        "clear_column_family_one_in must be 0 when using backup");
  }
  if (FLAGS_clear_column_family_one_in > 0 &&
      FLAGS_subset_cf_checkpoint_one_in > 0) {
    // A subset checkpoint passes live handles from `column_families_` to the
    // Checkpoint API and validates the checkpoint's column family list against
    // `column_family_names_`; neither is stable while families are being
    // dropped and recreated underneath.
    return ReturnValidationError(
        "clear_column_family_one_in must be 0 when using "
        "subset_cf_checkpoint_one_in");
  }
  if (FLAGS_clear_column_family_one_in > 0 &&
      FLAGS_abort_and_resume_cf_compactions_one_in > 0) {
    fprintf(stderr,
            "Error: clear_column_family_one_in must be 0 when using "
            "per-column-family compaction abort\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_test_cf_consistency && FLAGS_disable_wal) {
    FLAGS_atomic_flush = true;
  }

  // Trie UDI uses zero-copy pointers into block data, which is
  // incompatible with mmap_read.
  if (FLAGS_use_trie_index && FLAGS_mmap_read) {
    return ReturnValidationError(
        "use_trie_index is incompatible with mmap_read. The trie index uses "
        "zero-copy pointers into block data which is unsafe with mmap'd "
        "reads.");
  }

  // TrieIndexFactory requires plain BytewiseComparator, but timestamps use
  // BytewiseComparator.u64ts.
  if (FLAGS_use_trie_index && FLAGS_user_timestamp_size > 0) {
    return ReturnValidationError(
        "use_trie_index is incompatible with user-defined timestamps. "
        "TrieIndexFactory requires BytewiseComparator but timestamps use "
        "BytewiseComparator.u64ts.");
  }

  if (FLAGS_read_only) {
    if (FLAGS_writepercent != 0 || FLAGS_delpercent != 0 ||
        FLAGS_delrangepercent != 0) {
      return ReturnValidationError(
          "updates are not supported in read only mode");
    } else if (FLAGS_checkpoint_one_in > 0 &&
               FLAGS_clear_column_family_one_in > 0) {
      fprintf(stdout,
              "Warn: checkpoint won't be validated since column families may "
              "be dropped.\n");
    }
  }

  if (FLAGS_open_read_only_one_in > 0) {
    if (FLAGS_read_only) {
      return ReturnValidationError(
          "open_read_only_one_in needs a read-write primary and is "
          "incompatible with read_only");
    }
    if (FLAGS_use_txn || FLAGS_use_optimistic_txn || FLAGS_use_blob_db ||
        FLAGS_ttl != -1) {
      return ReturnValidationError(
          "open_read_only_one_in is not supported with transactions, BlobDB, "
          "or TTL");
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    raw_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbstress";
    FLAGS_db = default_db_path;
  }

  // For num_dbs=1: --db and --expected_values_dir are paths used as-is.
  // For num_dbs>1: they are parent directories; C++ creates db_0/, db_1/, ...
  // subdirs underneath. DB::Open(create_if_missing) creates each DB dir.
  // Python (db_crashtest.py) also creates EV dirs; C++ creates them here as a
  // fallback for direct CLI usage.
  const int num_dbs = FLAGS_num_dbs;
  if (num_dbs > 1) {
    s = raw_env->CreateDirIfMissing(FLAGS_db);
    DB_STRESS_ASSERT_OK_MSG(s, "Failed to create directory %s",
                            FLAGS_db.c_str());
  }
  std::vector<std::string> db_paths;
  std::vector<std::string> ev_paths;
  for (int i = 0; i < num_dbs; i++) {
    std::string suffix = (num_dbs == 1) ? "" : "/db_" + std::to_string(i);
    db_paths.push_back(FLAGS_db + suffix);
    if (!FLAGS_expected_values_dir.empty()) {
      std::string ep = FLAGS_expected_values_dir + suffix;
      s = Env::Default()->CreateDirIfMissing(ep);
      DB_STRESS_ASSERT_OK_MSG(s, "Failed to create directory %s", ep.c_str());
      ev_paths.push_back(std::move(ep));
    }
  }
  if (ev_paths.empty()) {
    ev_paths.resize(num_dbs);
  }

  // Secondary paths: C++ owns creation entirely.
  std::vector<std::string> sec_paths;
  if (FLAGS_test_secondary || FLAGS_continuous_verification_interval > 0) {
    std::string sec_parent;
    if (!FLAGS_secondaries_base.empty()) {
      sec_parent = FLAGS_secondaries_base;
    } else {
      raw_env->GetTestDirectory(&sec_parent);
      sec_parent += "/dbstress_secondaries";
    }
    s = raw_env->CreateDirIfMissing(sec_parent);
    DB_STRESS_ASSERT_OK_MSG(s, "Failed to create directory %s",
                            sec_parent.c_str());
    for (int i = 0; i < num_dbs; i++) {
      std::string suffix = (num_dbs == 1) ? "" : "/db_" + std::to_string(i);
      std::string sec_path = sec_parent + suffix;
      s = raw_env->CreateDirIfMissing(sec_path);
      DB_STRESS_ASSERT_OK_MSG(s, "Failed to create directory %s",
                              sec_path.c_str());
      sec_paths.push_back(std::move(sec_path));
    }
  }
  if (sec_paths.empty()) {
    sec_paths.resize(num_dbs);
  }

  if (FLAGS_best_efforts_recovery &&
      !(FLAGS_skip_verifydb && FLAGS_disable_wal)) {
    return ReturnValidationError(
        "With best-efforts recovery, skip_verifydb and disable_wal should be "
        "set to true.");
  }
  if (FLAGS_skip_verifydb) {
    if (FLAGS_verify_db_one_in > 0) {
      return ReturnValidationError(
          "Must set -verify_db_one_in=0 if skip_verifydb is true.");
    }
    if (FLAGS_continuous_verification_interval > 0) {
      return ReturnValidationError(
          "Must set -continuous_verification_interval=0 if skip_verifydb is "
          "true.");
    }
  }
  if ((FLAGS_enable_compaction_filter || FLAGS_inplace_update_support) &&
      (FLAGS_acquire_snapshot_one_in > 0 || FLAGS_compact_range_one_in > 0 ||
       FLAGS_iterpercent > 0 || FLAGS_prefixpercent > 0 ||
       FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency ||
       FLAGS_check_multiget_consistency ||
       FLAGS_check_multiget_entity_consistency)) {
    return ReturnValidationError(
        "acquire_snapshot_one_in, compact_range_one_in, iterpercent, "
        "prefixpercent, test_batches_snapshots, test_cf_consistency, "
        "check_multiget_consistency, check_multiget_entity_consistency must "
        "all be 0 when using compaction filter or inplace update support");
  }
  if (FLAGS_test_multi_ops_txns) {
    CheckAndSetOptionsForMultiOpsTxnStressTest();
  }

  if (!FLAGS_use_txn && FLAGS_use_optimistic_txn) {
    return ReturnValidationError(
        "You cannot set use_optimistic_txn true while use_txn is false. Please "
        "set use_txn true if you want to use OptimisticTransactionDB");
  }

  if (FLAGS_create_timestamped_snapshot_one_in > 0) {
    if (!FLAGS_use_txn) {
      return ReturnValidationError(
          "timestamped snapshot supported only in TransactionDB");
    } else if (FLAGS_txn_write_policy != 0) {
      return ReturnValidationError(
          "timestamped snapshot supported only in write-committed");
    }
  }

  if (FLAGS_preserve_unverified_changes && FLAGS_reopen != 0) {
    return ReturnValidationError(
        "Reopen DB is incompatible with preserving unverified changes");
  }

  if (FLAGS_use_txn && !FLAGS_use_optimistic_txn &&
      FLAGS_sync_fault_injection && FLAGS_txn_write_policy != 0) {
    return ReturnValidationError(
        "For TransactionDB, correctness testing with unsync data loss is "
        "currently compatible with only write committed policy");
  }

  if (FLAGS_use_put_entity_one_in > 0 &&
      (FLAGS_use_full_merge_v1 || FLAGS_test_multi_ops_txns ||
       FLAGS_user_timestamp_size > 0)) {
    return ReturnValidationError(
        "Wide columns are incompatible with V1 Merge, the multi-op "
        "transaction test, and user-defined timestamps");
  }

#ifndef NDEBUG
  KillPoint* kp = KillPoint::GetInstance();
  kp->rocksdb_kill_odds = FLAGS_kill_random_test;
  kp->rocksdb_kill_exclude_prefixes = SplitString(FLAGS_kill_exclude_prefixes);
#endif

  unsigned int levels = FLAGS_max_key_len;
  std::vector<std::string> weights;
  uint64_t scale_factor = FLAGS_key_window_scale_factor;
  key_gen_ctx.window = scale_factor * 100;
  if (!FLAGS_key_len_percent_dist.empty()) {
    weights = SplitString(FLAGS_key_len_percent_dist);
    if (weights.size() != levels) {
      return ReturnValidationError(
          "Number of weights in key_len_dist should be equal to max_key_len");
    }

    uint64_t total_weight = 0;
    for (std::string& weight : weights) {
      uint64_t val = std::stoull(weight);
      key_gen_ctx.weights.emplace_back(val * scale_factor);
      total_weight += val;
    }
    if (total_weight != 100) {
      return ReturnValidationError(
          "Sum of all weights in key_len_dist should be 100");
    }
  } else {
    uint64_t keys_per_level = key_gen_ctx.window / levels;
    for (unsigned int level = 0; level + 1 < levels; ++level) {
      key_gen_ctx.weights.emplace_back(keys_per_level);
    }
    key_gen_ctx.weights.emplace_back(key_gen_ctx.window -
                                     keys_per_level * (levels - 1));
  }
  // Initialize shared resources (hot-key generator, block cache, WBM,
  // rate limiter) once so that all DB instances share them.
  InitializeHotKeyGenerator(FLAGS_hot_key_alpha);
  block_cache =
      StressTest::NewCache(FLAGS_cache_size, FLAGS_cache_numshardbits);
  if (FLAGS_use_write_buffer_manager) {
    wbm = std::make_shared<WriteBufferManager>(
        FLAGS_db_write_buffer_size, block_cache, false /* allow_stall */,
        lossless_cast<WriteBufferFlushPolicy>(FLAGS_wbm_flush_policy));
  }
  if (FLAGS_rate_limiter_bytes_per_sec > 0) {
    rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_rate_limiter_bytes_per_sec, 1000 /* refill_period_us */,
        10 /* fairness */,
        FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                  : RateLimiter::Mode::kWritesOnly));
  }

  // Phase 1: Create StressTest instances (one per DB).
  std::vector<std::unique_ptr<StressTest>> stress_tests(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    if (FLAGS_test_cf_consistency) {
      stress_tests[i].reset(CreateCfConsistencyStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else if (FLAGS_test_batches_snapshots) {
      stress_tests[i].reset(CreateBatchedOpsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else if (FLAGS_test_multi_ops_txns) {
      stress_tests[i].reset(CreateMultiOpsTxnsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else {
      stress_tests[i].reset(CreateNonBatchedOpsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    }
  }

  RegisterCrashCallbacks(stress_tests, num_dbs);

  // Phase 2: Create SharedState for every DB before any worker thread starts.
  std::vector<std::unique_ptr<SharedState>> shared_states(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    shared_states[i].reset(new SharedState(raw_env, stress_tests[i].get()));
  }

  // Phase 3: Launch each DB's stress test on its own thread.
  std::vector<int> results(num_dbs, 0);
  std::vector<std::thread> stress_test_runners;
  stress_test_runners.reserve(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    stress_test_runners.emplace_back([i, &results, &shared_states]() {
      results[i] = RunStressTest(shared_states[i].get()) ? 1 : 0;
    });
  }

  // Phase 4: Wait for all DB threads, collect results, clean up.
  bool all_passed = true;
  for (int i = 0; i < num_dbs; i++) {
    stress_test_runners[i].join();
    if (!results[i]) {
      all_passed = false;
    }
    stress_tests[i]->CleanUp();
  }
  for (auto& fs : fault_fs_for_crash_flush) {
    fs = nullptr;
  }
  return all_passed ? 0 : 1;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
