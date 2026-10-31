#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import argparse
import errno
import glob
import math
import os
import random
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time

_TOOLS_DIR = os.path.dirname(__file__)
if _TOOLS_DIR and _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

import fault_injection_log_parser

per_iteration_random_seed_override = 0
remain_argv = None
is_remote_db = False

_SIGTERM_STDOUT_MARKER = "Received signal 15 (Terminated)"
# Keep this timeout filter narrow: Poll/AbortIO only retry Linux
# -EINTR (-4) and -EAGAIN (-11), so terminal wait_cqe errors still fail.
_IGNORED_SIGTERM_STDERR_RE = re.compile(
    r"^(?:PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
    r"returned terminal error: -9\."
    r"|(?:Poll|AbortIO): io_uring_wait_cqe failed: -(?:4|11))$"
)
_IGNORED_IO_URING_INIT_STDERR_RE = re.compile(
    r"^[IE]\d{4} \d{2}:\d{2}:\d{2}\.\d+ +\d+ "
    r"IoUringBackend\.cpp:\d+\] "
    r"io_uring_queue_init_params\([^)]*\) failed .*$"
)
_IGNORED_CACHELIB_INFO_STDERR_RE = re.compile(
    r"^I\d{4} \d{2}:\d{2}:\d{2}\.\d+ +\d+ "
    r"(?:Allocator|BlockCache|Device|Driver|EnginePair|LruPolicy|NavySetup|"
    r"RegionManager|RejectRandomAP|SlabAllocator|SparseMapIndex)\.cpp:\d+\] .*$"
)
_NO_SPACE_SUBSTRINGS = (
    "no space left on device",
    "out of disk space",
    "out of space",
    "enospc",
)
_OUTPUT_PATH_RE = re.compile(r"(/[^\s]+)")
_TSAN_OPTIONS_ENV_VAR = "TSAN_OPTIONS"
_TSAN_SUPPRESSIONS_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "tsan_suppressions.txt")
)
DEFAULT_LIVENESS_TIMEOUT_SEC = 3600
DEFAULT_LIVENESS_NO_PROGRESS_TIMEOUT_SEC = 300
REMOTE_DB_LIVENESS_TIMEOUT_MULTIPLIER = 2
_FAULT_INJECTION_LOG_DIR_NAME = "fault_injection_logs"
_STRESS_DIAGNOSTICS_DIR_NAME = "rocksdb_crashtest_diagnostics"
_EXPECTED_CF_LIFECYCLE_FAULT_EXIT_CODE = 86
_CF_LIFECYCLE_FAULT_MARKERS = {
    1: "EXPECTED_CF_LIFECYCLE_FAULT target=options",
    2: "EXPECTED_CF_LIFECYCLE_FAULT target=manifest",
}
_CF_LIFECYCLE_SUCCESS_MARKER = "CF_LIFECYCLE_SUCCESS"
_CF_LIFECYCLE_MIN_COVERAGE = {"options": 1, "manifest": 1, "success": 2}
# fault target, tracking enabled, clear-CF frequency. A faulted run is always
# followed by a run that validates recovery before performing more topology
# changes. The disabled run covers enabled -> disabled recovery, while the last
# run covers enabled recovery and successful repeated lifecycle operations.
_CF_LIFECYCLE_SCHEDULE = (
    (1, 1, 1),
    (0, 0, 0),
    (2, 1, 1),
    (0, 1, 100),
)
_CF_LIFECYCLE_AUTHORITATIVE_FLAGS = (
    "cf_lifecycle_fault_target",
    "track_options_file_number_in_manifest",
    "clear_column_family_one_in",
    "destroy_db_initially",
    "max_key",
    "log2_keys_per_lock",
)
_MAX_STRESS_DIAGNOSTIC_PROCESS_GROUPS = 8
_STRESS_DIAGNOSTIC_FILE_RE = re.compile(
    r"^[A-Za-z0-9_.-]*\.pid_([0-9]+)\.thread_[0-9]+(?:"
    r"\.breadcrumbs\.txt(?:\.tmp)?|"
    r"\."
    r"(?:multi_scan|prefix_scan|prefix_scan_batched|"
    r"prefix_scan_cf_consistency)\.witness\.txt(?:\.tmp)?)$"
)
_REMOTE_DB_URI_FLAGS = ("--env_uri", "--fs_uri")
_MIN_WRITE_BUFFER_SIZE = 64 * 1024
_WBM_FLUSH_POLICIES = (0, 1, 2)
_WBM_FLUSH_LARGEST_ACROSS_DBS = 2


def get_random_seed(override):
    if override == 0:
        return random.randint(1, 2**64)
    else:
        return override


def quote_arg_for_display(arg):
    """
    Quote only the value after '=' for shell display.
    This makes the printed command safe to copy/paste into a Unix shell.
    Note: shlex is Unix-focused; Non-Unix shell users may need to adjust quoting after copying.
    """
    if "=" not in arg:
        return arg
    flag, value = arg.split("=", 1)
    return f"{flag}={shlex.quote(value)}"


def stress_cmd_env():
    env = os.environ.copy()
    if _TSAN_OPTIONS_ENV_VAR not in env and os.path.exists(_TSAN_SUPPRESSIONS_FILE):
        env[_TSAN_OPTIONS_ENV_VAR] = "suppressions=" + _TSAN_SUPPRESSIONS_FILE
    return env


def apply_cache_and_write_buffer_size_multiplier(params):
    """Scale selected per-iteration memory sizes while preserving randomization."""
    multiplier = params.pop("cache_and_write_buffer_size_multiplier", None)
    if multiplier is None:
        return
    if not math.isfinite(multiplier) or multiplier <= 0:
        raise ValueError(
            "cache_and_write_buffer_size_multiplier must be finite and greater than zero"
        )

    for name, minimum in (
        ("cache_size", 1),
        ("write_buffer_size", _MIN_WRITE_BUFFER_SIZE),
    ):
        value = params.get(name)
        if value is not None and value > 0:
            params[name] = max(int(value * multiplier), minimum)


def normalize_remote_db_args(args):
    """Normalize supported remote URI flags to --flag=value form."""
    normalized_args = []
    index = 0
    while index < len(args):
        arg = args[index]
        if (
            arg in _REMOTE_DB_URI_FLAGS
            and index + 1 < len(args)
            and args[index + 1]
            and not args[index + 1].startswith("-")
        ):
            normalized_args.append(arg + "=" + args[index + 1])
            index += 2
            continue
        normalized_args.append(arg)
        index += 1
    return normalized_args


def parse_remote_db_uri_arg(arg):
    flag, separator, value = arg.partition("=")
    if flag not in _REMOTE_DB_URI_FLAGS or not separator:
        return None
    return flag, value


def remote_db_enabled(args):
    effective_values = {}
    for arg in args:
        parsed_arg = parse_remote_db_uri_arg(arg)
        if parsed_arg is not None:
            flag, value = parsed_arg
            effective_values[flag] = value
    return any(effective_values.values())


def early_argument_parsing_before_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--initial_random_seed_override",
        default=0,
        type=int,
        help="Random seed used for initialize the test parameters at the beginning of stress test run",
    )
    # sometimes the failure appeared after a few iteration, to reproduce the error, we have to wait for the test to run
    # multiple iterations to reach the iteration that fails the test. By overriding the seed used within each iteration,
    # we could skip all the previous iterations.
    parser.add_argument(
        "--per_iteration_random_seed_override",
        default=0,
        type=int,
        help="Random seed used for initialize the test parameters in each iteration of the stress test run",
    )

    global remain_args
    args, remain_args = parser.parse_known_args()
    remain_args = normalize_remote_db_args(remain_args)
    init_random_seed = get_random_seed(args.initial_random_seed_override)
    global per_iteration_random_seed_override
    per_iteration_random_seed_override = args.per_iteration_random_seed_override
    global is_remote_db
    # Remote URI arguments are normalized above so all later consumers see
    # the same representation.
    is_remote_db = remote_db_enabled(remain_args)

    print(f"Start with random seed {init_random_seed}")
    random.seed(init_random_seed)


def apply_random_seed_per_iteration():
    per_iteration_random_seed = get_random_seed(per_iteration_random_seed_override)
    print(f"Use random seed for iteration {per_iteration_random_seed}")
    random.seed(per_iteration_random_seed)


# Random seed has to be setup before the rest of the script, so that the random
# value selected in the global variable uses the random seed specified. More
# arguments can also be parsed early.
early_argument_parsing_before_main()

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox,liveness}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox,liveness}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox,liveness}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox,liveness}_default_params <
#       txn_params < args
#   for ts:
#       default_params < {blackbox,whitebox,liveness}_default_params <
#       ts_params < args
#   for multiops_txn:
#       default_params < {blackbox,whitebox,liveness}_default_params <
#       multiops_txn_params < args
#   for full external table:
#       default_params < blackbox_default_params <
#       full_external_table_params < args
# Liveness mode reapplies liveness_fault_injection_params after args so the
# base liveness profile cannot accidentally turn fault injection back on.


# use_trie_index and index_mode must be the same across invocations so that all
# SSTs in a DB are opened with matching table options, so they are drawn once
# here. The custom index modes require the trie factory.
_USE_TRIE_INDEX = random.choice([0] * 15 + [1])
# 0=kStandardOnly, 1=kStandardDefault, 2=kCustomDefault, 3=kCustomOnly,
# 4=kStandardRequired. Without a factory, the default kStandardDefault builds
# only the standard index.
_INDEX_MODE = random.choice([0, 1, 2, 2, 3, 3, 4]) if _USE_TRIE_INDEX == 1 else 1


default_params = {
    "acquire_snapshot_one_in": lambda: random.choice([100, 10000]),
    "backup_max_size": 100 * 1024 * 1024,
    # Consider larger number when backups considered more stable
    "backup_one_in": lambda: random.choice([1000, 100000]),
    "batch_protection_bytes_per_key": lambda: random.choice([0, 8]),
    "memtable_protection_bytes_per_key": lambda: random.choice([0, 1, 2, 4, 8]),
    "block_protection_bytes_per_key": lambda: random.choice([0, 1, 2, 4, 8]),
    "block_size": 16384,
    "bloom_bits": lambda: random.choice(
        [random.randint(0, 19), random.lognormvariate(2.3, 1.3)]
    ),
    "cache_index_and_filter_blocks": lambda: random.randint(0, 1),
    "cache_size": lambda: random.choice([8388608, 33554432]),
    "charge_compression_dictionary_building_buffer": lambda: random.choice([0, 1]),
    "charge_filter_construction": lambda: random.choice([0, 1]),
    "charge_table_reader": lambda: random.choice([0, 1]),
    "charge_file_metadata": lambda: random.choice([0, 1]),
    "checkpoint_one_in": lambda: random.choice([0, 0, 10000, 1000000]),
    "checkpoint_engine_max_background_operations": lambda: random.choice([4, 8]),
    "checkpoint_engine_use_link_file_when_available": lambda: random.randint(0, 1),
    "parallel_checkpoint_one_in": lambda: random.choice([0, 1, 2]),
    "subset_cf_checkpoint_one_in": lambda: random.choice([0, 1, 2]),
    "compression_type": lambda: random.choice(
        ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
    ),
    "bottommost_compression_type": lambda: (
        "disable"
        if random.randint(0, 1) == 0
        else random.choice(["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"])
    ),
    "checksum_type": lambda: random.choice(
        ["kNoChecksum", "kCRC32c", "kxxHash", "kxxHash64", "kXXH3"]
    ),
    "compression_max_dict_bytes": lambda: 16384 * random.randint(0, 1),
    "compression_zstd_max_train_bytes": lambda: 65536 * random.randint(0, 1),
    "compression_parallel_threads": lambda: random.choice([1, 1, 2, 3, 4, 5, 8, 9, 16]),
    "compression_max_dict_buffer_bytes": lambda: (1 << random.randint(0, 40)) - 1,
    "compression_use_zstd_dict_trainer": lambda: random.randint(0, 1),
    "compression_checksum": lambda: random.randint(0, 1),
    "clear_column_family_one_in": 0,
    "compact_files_one_in": lambda: random.choice([1000, 1000000]),
    "compact_range_one_in": lambda: random.choice([1000, 1000000]),
    # Disabled because of various likely related failures with
    # "Cannot delete table file #N from level 0 since it is on level X"
    "promote_l0_one_in": 0,
    "compaction_pri": random.randint(0, 4),
    "key_may_exist_one_in": lambda: random.choice([100, 100000]),
    "data_block_index_type": lambda: random.choice([0, 1]),
    "decouple_partitioned_filters": lambda: random.choice([0, 1, 1]),
    "delpercent": 4,
    "delrangepercent": 1,
    "db": "",
    "destroy_db_initially": 0,
    "expected_values_dir": "",
    "stress_diagnostics_dir": "",
    "stress_diagnostics_breadcrumbs": 1,
    "stress_diagnostics_breadcrumb_entries": 256,
    "stress_diagnostics_scan_witness": 1,
    "stress_diagnostics_scan_witness_entries": 128,
    "num_dbs": 1,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    "enable_compaction_filter": lambda: random.choice([0, 0, 0, 1]),
    "enable_compaction_on_deletion_trigger": lambda: random.choice([0, 0, 0, 1]),
    # `inplace_update_support` is incompatible with a wide range of features.
    # The current sanitization process is not sufficient to reliably handle
    # this. While inplace_update_support is intended to stay on across runs
    # in default_params, in certain runs it can be toggled off by
    # sanitization due to some incompatible features being enabled. When
    # inplace_update_support is off, other incompatible features such as
    # delete range may be enabled and leave state in the DB. A later run
    # reads default_params again with inplace_update_support on, but now
    # operates on a DB containing delete range data from the previous run,
    # causing stress test failures. Temporarily disabled until the
    # sanitization can account for cross-run incompatibility.
    "inplace_update_support": 0,
    "flush_one_in": lambda: random.choice([1000, 1000000]),
    "manual_wal_flush_one_in": lambda: random.choice([0, 1000]),
    "sync_wal_one_in": 0,
    # Remote backends can surface transient infrastructure IO errors that are
    # not RocksDB bugs, so default to tolerating them whenever a remote
    # --env_uri / --fs_uri is in use. A caller can still pass 0 explicitly, and
    # finalize_and_sanitize() force-disables it for local DBs.
    "tolerate_non_injected_io_errors_for_remote_dbs": lambda: 1 if is_remote_db else 0,
    "file_checksum_impl": lambda: random.choice(["none", "crc32c", "xxh64", "big"]),
    "get_live_files_apis_one_in": lambda: random.choice([10000, 1000000]),
    "checkpoint_atomic_flush": lambda: random.choice([0, 1]),
    "get_all_column_family_metadata_one_in": lambda: random.choice([10000, 1000000]),
    # Note: the following two are intentionally disabled as the corresponding
    # APIs are not guaranteed to succeed.
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    # Temporarily disable hash index
    "index_type": lambda: random.choice([0, 0, 0, 2, 2, 3]),
    "index_block_search_type": lambda: random.choice([0, 1, 2]),
    "uniform_cv_threshold": lambda: random.choice([-1, 0.2, 1000]),
    "ingest_external_file_one_in": lambda: random.choice([1000, 1000000]),
    "ingest_external_file_prepare_commit_one_in": lambda: random.choice([0, 1, 2]),
    "ingest_external_file_use_file_info_one_in": lambda: random.choice([0, 1, 2]),
    "ingest_external_file_atomic_replace_one_in": lambda: random.choice([0, 5, 10]),
    "ingest_external_file_with_embedded_blobs": lambda: random.choice([0, 1]),
    "test_ingest_standalone_range_deletion_one_in": lambda: random.choice([0, 5, 10]),
    "iterpercent": 10,
    "lock_wal_one_in": lambda: random.choice([10000, 1000000]),
    "mark_for_compaction_one_file_in": lambda: 10 * random.randint(0, 1),
    "max_background_compactions": lambda: random.choice([2, 20]),
    "num_bottom_pri_threads": lambda: random.choice([0, 1, 20]),
    "max_bytes_for_level_base": 10485760,
    # max_key has to be the same across invocations for verification to work, hence no lambda
    "max_key": random.choice([100000, 25000000]),
    "max_sequential_skip_in_iterations": lambda: random.choice([1, 2, 8, 16]),
    "max_write_buffer_number": 3,
    "mmap_read": lambda: random.choice([0, 0, 1]),
    # Setting `nooverwritepercent > 0` is only possible because we do not vary
    # the random seed, so the same keys are chosen by every run for disallowing
    # overwrites.
    "nooverwritepercent": 1,
    "open_files": lambda: random.choice([-1, -1, -1, -1, 100, 500000]),
    "open_files_async": lambda: random.choice([0, 1]),
    "async_wal_precreate": lambda: random.choice([0, 1]),
    "optimize_filters_for_memory": lambda: random.randint(0, 1),
    "partition_filters": lambda: random.randint(0, 1),
    "partition_pinning": lambda: random.randint(0, 3),
    "rate_limit_auto_wal_flush": 0,
    "reset_stats_one_in": lambda: random.choice([10000, 1000000]),
    "pause_background_one_in": lambda: random.choice([10000, 1000000]),
    "disable_file_deletions_one_in": lambda: random.choice([10000, 1000000]),
    "disable_manual_compaction_one_in": lambda: random.choice([10000, 1000000]),
    "abort_and_resume_compactions_one_in": lambda: random.choice([0, 1000, 10000]),
    "abort_and_resume_cf_compactions_one_in": lambda: random.choice([10000, 1000000]),
    "prefix_size": lambda: random.choice([-1, 1, 5, 7, 8]),
    "prefixpercent": 5,
    "progress_reports": 0,
    "readpercent": 45,
    # See disabled DBWALTest.RecycleMultipleWalsCrash
    "recycle_log_file_num": 0,
    "snapshot_hold_ops": 100000,
    "sqfc_name": lambda: random.choice(["foo", "bar"]),
    # 0 = disable writing SstQueryFilters
    "sqfc_version": lambda: random.choice([0, 1, 1, 2, 2]),
    "sst_file_manager_bytes_per_sec": lambda: random.choice([0, 104857600]),
    "sst_file_manager_bytes_per_truncate": lambda: random.choice([0, 1048576]),
    "long_running_snapshots": lambda: random.randint(0, 1),
    "subcompactions": lambda: random.randint(1, 4),
    "target_file_size_base": lambda: random.choice([512 * 1024, 2048 * 1024]),
    "target_file_size_multiplier": 2,
    "test_batches_snapshots": random.randint(0, 1),
    "top_level_index_pinning": lambda: random.randint(0, 3),
    "unpartitioned_pinning": lambda: random.randint(0, 3),
    "use_direct_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_compaction_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_flush_and_compaction": lambda: random.randint(0, 1),
    "use_sqfc_for_range_queries": lambda: random.choice([0, 1, 1, 1]),
    "mock_direct_io": False,
    "cache_type": lambda: random.choice(
        [
            "lru_cache",
            "fixed_hyper_clock_cache",
            "auto_hyper_clock_cache",
            "auto_hyper_clock_cache",
            "tiered_lru_cache",
            "tiered_fixed_hyper_clock_cache",
            "tiered_auto_hyper_clock_cache",
            "tiered_auto_hyper_clock_cache",
        ]
    ),
    "uncache_aggressiveness": lambda: int(math.pow(10, 4.0 * random.random()) - 1.0),
    "use_full_merge_v1": lambda: random.randint(0, 1),
    "use_merge": lambda: random.randint(0, 1),
    # Drawn above, together with index_mode.
    "use_trie_index": _USE_TRIE_INDEX,
    "index_mode": _INDEX_MODE,
    # Format of trie indexes in new SSTs. Readers handle every format, so this
    # changes between invocations and DBs mix SSTs of both formats.
    "trie_index_format_version": lambda: random.choice([1, 2]),
    # ReadOptions::read_index: -1=auto (db_stress selects kPreferCustom when the
    # trie is the secondary index), 0=kDefault, 1=kBuiltin, 2=kPreferCustom.
    # finalize_and_sanitize() drops selectors the index mode cannot serve.
    "read_index": lambda: random.choice([-1, -1, 0, 1, 2]),
    # use_put_entity_one_in has to be the same across invocations for verification to work, hence no lambda
    "use_put_entity_one_in": random.choice([0] * 7 + [1, 5, 10]),
    "use_attribute_group": lambda: random.randint(0, 1),
    "use_multi_cf_iterator": lambda: random.randint(0, 1),
    # 999 -> use Bloom API
    "bloom_before_level": lambda: random.choice(
        [random.randint(-1, 2), random.randint(-1, 10), 0x7FFFFFFF - 1, 0x7FFFFFFF]
    ),
    "value_size_mult": 32,
    "verification_only": 0,
    "verify_checksum": 1,
    "write_buffer_size": lambda: random.choice([1024 * 1024, 4 * 1024 * 1024]),
    "writepercent": 35,
    "format_version": lambda: random.choice([2, 3, 4, 5, 6, 7, 8, 8]),
    "optimize_key_common_prefix": lambda: random.choice([0, 1, 2]),
    "separate_key_value_in_data_block": lambda: random.choice([0, 1, 1]),
    "index_block_restart_interval": lambda: random.choice(range(1, 16)),
    "use_multiget": lambda: random.randint(0, 1),
    "use_async_db_api": lambda: random.choice([0] * 5 + [1]),
    "use_get_entity": lambda: random.choice([0] * 7 + [1]),
    "use_multi_get_entity": lambda: random.choice([0] * 7 + [1]),
    # NOTE: only takes effect when open_files == -1 (required by the lazy API)
    "lazy_entity_read_one_in": lambda: random.choice([0, 0, 0, 10, 100]),
    "periodic_compaction_seconds": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "max_compaction_trigger_wakeup_seconds": lambda: random.choice([43200, 600, 30]),
    "read_triggered_compaction_threshold": lambda: random.choice([0.0, 0.001, 0.01]),
    "daily_offpeak_time_utc": lambda: random.choice(
        ["", "", "00:00-23:59", "04:00-08:00", "23:30-03:15"]
    ),
    "periodic_compaction_phase_recovery_percent": lambda: random.choice(
        [0, 25, 33, 50, 100]
    ),
    # 0 = never (used by some), 10 = often (for threading bugs), 600 = default
    "stats_dump_period_sec": lambda: random.choice([0, 10, 600]),
    "mutex_hold_log_threshold_us": lambda: random.choice([0, 100000, 25000, 500000]),
    "compaction_ttl": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "fifo_allow_compaction": lambda: random.randint(0, 1),
    # TODO(T260692223): FIFO compaction drops old SST files when total size
    # exceeds the limit, but the stress test expected state can't track these
    # drops. max_table_files_size is always active (default 1GB) so set it
    # very high. max_data_files_size when non-zero overrides max_table_files_size;
    # randomize between 0 (fallback to max_table_files_size) and very high.
    # Long-term, handle drops via OnCompactionBegin + SetPendingDel() as
    # concurrent deletes.
    "fifo_compaction_max_data_files_size_mb": lambda: random.choice(
        [0, 100 * 1024]  # 0 = disabled (defers to max_table_files_size), 100GB
    ),
    "fifo_compaction_max_table_files_size_mb": 100 * 1024,  # 100GB, always high
    "fifo_compaction_use_kv_ratio_compaction": lambda: random.randint(0, 1),
    # Test small max_manifest_file_size in a smaller chance, as most of the
    # time we wnat manifest history to be preserved to help debug
    "max_manifest_file_size": lambda: random.choice(
        [t * 2048 if t < 5 else 1024 * 1024 * 1024 for t in range(1, 30)]
    ),
    "verify_manifest_content_on_close": lambda: random.randint(0, 1),
    "max_manifest_space_amp_pct": lambda: random.choice([0, 10, 100, 1000]),
    # Sync mode might make test runs slower so running it in a smaller chance
    "sync": lambda: random.choice([1 if t == 0 else 0 for t in range(0, 20)]),
    "bytes_per_sync": lambda: random.choice([0, 262144]),
    # TODO(hx235): Enable `wal_bytes_per_sync` after fixing the DB recovery such
    # that it won't recover past the WAL data hole created by this option
    "wal_bytes_per_sync": 0,
    "compaction_readahead_size": lambda: random.choice([0, 0, 1024 * 1024]),
    "db_write_buffer_size": lambda: random.choice(
        [0, 0, 0, 1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024]
    ),
    "use_write_buffer_manager": lambda: random.randint(0, 1),
    "wbm_flush_policy": lambda: random.choice(_WBM_FLUSH_POLICIES),
    "avoid_unnecessary_blocking_io": random.randint(0, 1),
    "write_dbid_to_manifest": random.randint(0, 1),
    "track_options_file_number_in_manifest": lambda: random.randint(0, 1),
    "write_identity_file": random.randint(0, 1),
    "optimize_manifest_for_recovery": lambda: 1 if random.randint(1, 5) == 1 else 0,
    "reuse_manifest_on_open": lambda: 1 if random.randint(1, 5) == 1 else 0,
    "avoid_flush_during_recovery": lambda: random.choice(
        [1 if t == 0 else 0 for t in range(0, 8)]
    ),
    "enforce_write_buffer_manager_during_recovery": random.randint(0, 1),
    "max_write_batch_group_size_bytes": lambda: random.choice(
        [16, 64, 1024 * 1024, 16 * 1024 * 1024]
    ),
    "level_compaction_dynamic_level_bytes": lambda: random.randint(0, 1),
    "verify_checksum_one_in": lambda: random.choice([1000, 1000000]),
    "verify_file_checksums_one_in": lambda: random.choice([1000, 1000000]),
    "verify_db_one_in": lambda: random.choice([10000, 100000]),
    "continuous_verification_interval": 0,
    "max_key_len": 3,
    "key_len_percent_dist": "1,30,69",
    "error_recovery_with_no_fault_injection": lambda: random.randint(0, 1),
    "metadata_read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "metadata_write_fault_one_in": lambda: random.choice([0, 128, 1000]),
    "read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "write_fault_one_in": lambda: random.choice([0, 128, 1000]),
    "exclude_wal_from_write_fault_injection": 0,
    "file_scope_wal_write_faults": 0,
    "open_metadata_write_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_metadata_read_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_write_fault_one_in": lambda: random.choice([0, 0, 16]),
    "open_read_fault_one_in": lambda: random.choice([0, 0, 32]),
    "sync_fault_injection": lambda: random.randint(0, 1),
    "get_property_one_in": lambda: random.choice([100000, 1000000]),
    "get_properties_of_all_tables_one_in": lambda: random.choice([100000, 1000000]),
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "max_write_buffer_size_to_maintain": lambda: random.choice(
        [0, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024]
    ),
    "user_timestamp_size": 0,
    "secondary_cache_fault_one_in": lambda: random.choice([0, 0, 32]),
    "compressed_secondary_cache_size": lambda: random.choice([8388608, 16777216]),
    "prepopulate_block_cache": lambda: random.choice([0, 1, 2]),
    "memtable_prefix_bloom_size_ratio": lambda: random.choice([0.001, 0.01, 0.1, 0.5]),
    "memtable_whole_key_filtering": lambda: random.randint(0, 1),
    "detect_filter_construct_corruption": lambda: random.choice([0, 1]),
    "adaptive_readahead": lambda: random.choice([0, 1]),
    "async_io": lambda: random.choice([0, 1]),
    "wal_compression": lambda: random.choice(["none", "zstd"]),
    "verify_sst_unique_id_in_manifest": 1,  # always do unique_id verification
    "fast_sst_open": lambda: random.choice([0, 1]),
    "secondary_cache_uri": lambda: random.choice(
        [
            "",
            "",
            "",
            "compressed_secondary_cache://capacity=8388608;enable_custom_split_merge=true",
        ]
    ),
    "allow_data_in_errors": True,
    "enable_thread_tracking": lambda: random.choice([0, 1]),
    "readahead_size": lambda: random.choice([0, 16384, 524288]),
    "initial_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "max_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "num_file_reads_for_auto_readahead": lambda: random.choice([0, 1, 2]),
    "min_write_buffer_number_to_merge": lambda: random.choice([1, 2]),
    "preserve_internal_time_seconds": lambda: random.choice([0, 60, 3600, 36000]),
    "memtable_max_range_deletions": lambda: random.choice([0] * 6 + [100, 1000]),
    # 0 (disable) is the default and more commonly used value.
    "bottommost_file_compaction_delay": lambda: random.choice(
        [0, 0, 0, 600, 3600, 86400]
    ),
    "auto_readahead_size": lambda: random.choice([0, 1]),
    "verify_iterator_with_expected_state_one_in": 5,
    "allow_fallocate": lambda: random.choice([0, 1]),
    "table_cache_numshardbits": lambda: random.choice([6] * 3 + [-1] * 2 + [0]),
    "enable_write_thread_adaptive_yield": lambda: random.choice([0, 1]),
    "log_readahead_size": lambda: random.choice([0, 16 * 1024 * 1024]),
    "bgerror_resume_retry_interval": lambda: random.choice([100, 1000000]),
    "delete_obsolete_files_period_micros": lambda: random.choice(
        [6 * 60 * 60 * 1000000, 30 * 1000000]
    ),
    "max_log_file_size": lambda: random.choice([0, 1024 * 1024]),
    "log_file_time_to_roll": lambda: random.choice([0, 60]),
    "use_adaptive_mutex": lambda: random.choice([0, 1]),
    "advise_random_on_open": lambda: random.choice([0] + [1] * 3),
    "WAL_ttl_seconds": lambda: random.choice([0, 60]),
    "WAL_size_limit_MB": lambda: random.choice([0, 1]),
    "strict_bytes_per_sync": lambda: random.choice([0, 1]),
    "avoid_flush_during_shutdown": lambda: random.choice([0, 1]),
    "fill_cache": lambda: random.choice([0, 1]),
    "optimize_multiget_for_io": lambda: random.choice([0, 1]),
    "memtable_insert_hint_per_batch": lambda: random.choice([0, 1]),
    "dump_malloc_stats": lambda: random.choice([0, 1]),
    "stats_history_buffer_size": lambda: random.choice([0, 1024 * 1024]),
    "skip_stats_update_on_db_open": lambda: random.choice([0, 1]),
    "optimize_filters_for_hits": lambda: random.choice([0, 1]),
    "sample_for_compression": lambda: random.choice([0, 5]),
    "report_bg_io_stats": lambda: random.choice([0, 1]),
    "cache_index_and_filter_blocks_with_high_priority": lambda: random.choice([0, 1]),
    "use_delta_encoding": lambda: random.choice([0, 1]),
    "verify_compression": lambda: random.choice([0, 1]),
    "read_amp_bytes_per_bit": lambda: random.choice([0, 32]),
    "enable_index_compression": lambda: random.choice([0, 1]),
    "index_shortening": lambda: random.choice([0, 1, 2]),
    "metadata_charge_policy": lambda: random.choice([0, 1]),
    "use_adaptive_mutex_lru": lambda: random.choice([0, 1]),
    "manifest_preallocation_size": lambda: random.choice([0, 5 * 1024]),
    "enable_checksum_handoff": lambda: random.choice([0, 1]),
    "max_total_wal_size": lambda: random.choice([0] * 4 + [64 * 1024 * 1024]),
    "high_pri_pool_ratio": lambda: random.choice([0, 0.5]),
    "low_pri_pool_ratio": lambda: random.choice([0, 0.5]),
    "soft_pending_compaction_bytes_limit": lambda: random.choice(
        [1024 * 1024] + [64 * 1073741824] * 4
    ),
    "hard_pending_compaction_bytes_limit": lambda: random.choice(
        [2 * 1024 * 1024] + [256 * 1073741824] * 4
    ),
    "enable_sst_partitioner_factory": lambda: random.choice([0, 1]),
    "enable_do_not_compress_roles": lambda: random.choice([0, 1]),
    "block_align": lambda: random.choice([0, 1]),
    "super_block_alignment_size": lambda: random.choice(
        [0, 128 * 1024, 512 * 1024, 2 * 1024 * 1024]
    ),
    "super_block_alignment_space_overhead_ratio": lambda: random.choice([0, 32, 4096]),
    "lowest_used_cache_tier": lambda: random.choice([0, 1, 2]),
    "enable_custom_split_merge": lambda: random.choice([0, 1]),
    "adm_policy": lambda: random.choice([0, 1, 2, 3]),
    "last_level_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    "default_write_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    "default_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    # TODO(hx235): enable `enable_memtable_insert_with_hint_prefix_extractor`
    # after fixing the surfaced issue with delete range
    "enable_memtable_insert_with_hint_prefix_extractor": 0,
    "check_multiget_consistency": lambda: random.choice([0, 0, 0, 1]),
    "check_multiget_entity_consistency": lambda: random.choice([0, 0, 0, 1]),
    "use_timed_put_one_in": lambda: random.choice([0] * 7 + [1, 5, 10]),
    "universal_max_read_amp": lambda: random.choice([-1] * 3 + [0, 4, 10]),
    # verify_output_flags is a bitmask: bits 0-2 are verification types
    # (block checksum, iteration, file checksum), bits 10-11 are when to
    # enable (local compaction, remote compaction). 0x407 = all types +
    # local, 0xC07 = all types + local + remote, 0xFFFFFFFF = all.
    "verify_output_flags": lambda: random.choice([0] * 3 + [0x407, 0xC07, 0xFFFFFFFF]),
    "paranoid_memory_checks": lambda: random.choice([0] * 7 + [1]),
    "memtable_verify_per_key_checksum_on_seek": lambda: random.choice([0] * 7 + [1]),
    "memtable_batch_lookup_optimization": lambda: random.randint(0, 1),
    "allow_unprepared_value": lambda: random.choice([0, 1]),
    # TODO(hx235): enable `track_and_verify_wals` after stabalizing the stress test
    "track_and_verify_wals": lambda: random.choice([0]),
    "remote_compaction_worker_threads": lambda: random.choice([0, 8]),
    "allow_resumption_one_in": lambda: random.choice([0, 1, 2, 20]),
    "remote_compaction_failure_fall_back_to_local": lambda: random.choice([0, 1]),
    "use_separate_remote_compaction_pool": lambda: random.choice([0, 1]),
    "auto_refresh_iterator_with_snapshot": lambda: random.choice([0, 1]),
    "memtable_op_scan_flush_trigger": lambda: random.choice([0, 10, 100, 1000]),
    "memtable_avg_op_scan_flush_trigger": lambda: random.choice([0, 2, 20, 200]),
    "min_tombstones_for_range_conversion": lambda: random.choice([0, 2, 2, 4, 16]),
    "ingest_wbwi_one_in": lambda: random.choice([0, 0, 100, 500]),
    "universal_reduce_file_locking": lambda: random.randint(0, 1),
    "compression_manager": lambda: random.choice(
        ["mixed"] * 1
        + ["none"] * 2
        + ["randommixed"] * 2
        + ["custom"] * 3
    ),
    # AutoSkip compression is now a CompressionOptions feature (bool).
    "compression_auto_skip": lambda: random.choice([0] * 4 + [1]),
    "compression_auto_skip_min_sample_every": lambda: random.choice(
        [0, 16, 64, 256]
    ),
    # fixed within a run for easier debugging
    # actual frequency is lower after option sanitization
    "use_multiscan": random.choice([1] + [0] * 3),
    # By default, `statistics` use kExceptDetailedTimers level
    "statistics": random.choice([0, 1]),
    "multiscan_use_async_io": lambda: random.randint(0, 1),
}

_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
# If TEST_TMPDIR_EXPECTED is not specified, default value will be TEST_TMPDIR
# except on remote filesystem
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"
_DEBUG_LEVEL_ENV_VAR = "DEBUG_LEVEL"

stress_cmd = "./db_stress"


def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_db_parent_dir(test_name):
    # Returns the --db path. C++ owns all DB dir creation (supports remote env).
    test_dir_name = "rocksdb_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        return tempfile.mkdtemp(prefix=test_dir_name)
    return test_tmpdir + "/" + test_dir_name


ev_parent_dir_global = None


def get_ev_parent_dir():
    # Returns the --expected_values_dir path. Python owns all EV dir creation.
    global ev_parent_dir_global
    if ev_parent_dir_global is not None:
        return ev_parent_dir_global
    expected_dir_prefix = "rocksdb_crashtest_expected_"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    if not is_remote_db and (test_exp_tmpdir is None or test_exp_tmpdir == ""):
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        ev_parent_dir_global = tempfile.mkdtemp(prefix=expected_dir_prefix)
    else:
        ev_parent_dir_global = test_exp_tmpdir + "/rocksdb_crashtest_expected"
        os.makedirs(ev_parent_dir_global, exist_ok=True)
    return ev_parent_dir_global


multiops_txn_key_spaces_file = None


def setup_multiops_txn_key_spaces_file():
    global multiops_txn_key_spaces_file
    if multiops_txn_key_spaces_file is not None:
        return multiops_txn_key_spaces_file
    key_spaces_file_prefix = "rocksdb_crashtest_multiops_txn_key_spaces"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    if not is_remote_db and (test_exp_tmpdir is None or test_exp_tmpdir == ""):
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        multiops_txn_key_spaces_file = tempfile.mkstemp(prefix=key_spaces_file_prefix)[
            1
        ]
    else:
        if not os.path.exists(test_exp_tmpdir):
            os.mkdir(test_exp_tmpdir)
        multiops_txn_key_spaces_file = tempfile.mkstemp(
            prefix=key_spaces_file_prefix, dir=test_exp_tmpdir
        )[1]
    return multiops_txn_key_spaces_file


diagnostics_dir_global = None


def is_real_directory(path):
    try:
        return stat.S_ISDIR(os.lstat(path).st_mode)
    except OSError:
        return False


def create_stress_diagnostics_dir(test_tmpdir):
    if not test_tmpdir or not os.path.isdir(test_tmpdir):
        return tempfile.mkdtemp(prefix=_STRESS_DIAGNOSTICS_DIR_NAME)

    diagnostics_dir = os.path.join(test_tmpdir, _STRESS_DIAGNOSTICS_DIR_NAME)
    if not os.path.lexists(diagnostics_dir):
        os.mkdir(diagnostics_dir)
        return diagnostics_dir
    if not is_real_directory(diagnostics_dir):
        raise OSError(
            errno.ENOTDIR,
            "stress diagnostics path is not a real directory",
            diagnostics_dir,
        )
    return diagnostics_dir


def get_diagnostics_dir():
    # Diagnostics are local process artifacts, independent of whether the DB
    # itself uses a local or remote filesystem.
    global diagnostics_dir_global
    if diagnostics_dir_global is not None:
        return diagnostics_dir_global

    try:
        diagnostics_dir_global = create_stress_diagnostics_dir(
            os.environ.get(_TEST_DIR_ENV_VAR)
        )
    except OSError as error:
        print(f"Failed to create stress diagnostics directory: {error}")
        diagnostics_dir_global = None
        return ""
    return diagnostics_dir_global


def stress_diagnostics_enabled(params):
    return (
        params.get("stress_diagnostics_breadcrumbs", 0) == 1
        or params.get("stress_diagnostics_scan_witness", 0) == 1
    )


def set_default_stress_diagnostics_dir(params):
    if stress_diagnostics_enabled(params) and not params.get("stress_diagnostics_dir"):
        params["stress_diagnostics_dir"] = get_diagnostics_dir()


def cleanup_stress_diagnostics_dir():
    global diagnostics_dir_global
    if diagnostics_dir_global is None:
        return

    try:
        if is_real_directory(diagnostics_dir_global):
            with os.scandir(diagnostics_dir_global) as entries:
                for entry in entries:
                    if entry.is_file(
                        follow_symlinks=False
                    ) and _STRESS_DIAGNOSTIC_FILE_RE.fullmatch(entry.name):
                        os.remove(entry.path)
            try:
                os.rmdir(diagnostics_dir_global)
            except OSError as error:
                if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                    raise
    except OSError as error:
        print(
            "Failed to clean up stress diagnostics directory "
            f"{diagnostics_dir_global}: {error}"
        )
    finally:
        diagnostics_dir_global = None


def prune_stress_diagnostics_dir(current_pid):
    if diagnostics_dir_global is None or not is_real_directory(
        diagnostics_dir_global
    ):
        return

    try:
        process_files = {}
        with os.scandir(diagnostics_dir_global) as entries:
            for entry in entries:
                if not entry.is_file(follow_symlinks=False):
                    continue
                match = _STRESS_DIAGNOSTIC_FILE_RE.fullmatch(entry.name)
                if match is None:
                    continue
                pid = int(match.group(1))
                process_files.setdefault(pid, []).append(
                    (entry.stat(follow_symlinks=False).st_mtime_ns, entry.path)
                )

        if len(process_files) <= _MAX_STRESS_DIAGNOSTIC_PROCESS_GROUPS:
            return

        groups_by_age = sorted(
            process_files.items(),
            key=lambda item: max(mtime for mtime, _ in item[1]),
        )
        groups_to_remove = len(process_files) - _MAX_STRESS_DIAGNOSTIC_PROCESS_GROUPS
        for pid, files in groups_by_age:
            if groups_to_remove == 0:
                break
            if pid == current_pid:
                continue
            for _, path in files:
                os.remove(path)
            groups_to_remove -= 1
    except OSError as error:
        print(f"Failed to prune stress diagnostics directory: {error}")


def is_direct_io_supported(dbname):
    if is_remote_db:
        return False
    else:
        # Note: db dir might be removed on check_mode change. Re-create it
        os.makedirs(dbname, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=dbname) as f:
            try:
                os.open(f.name, os.O_DIRECT)
            except BaseException:
                return False
            return True


blackbox_default_params = {
    "disable_wal": lambda: random.choice([0, 0, 0, 1]),
    # total time for this script to test db_stress
    "duration": 6000,
    # time for one db_stress instance to run
    "interval": 120,
    # time for the final verification step
    "verify_timeout": 1200,
    # since we will be killing anyway, use large value for ops_per_thread
    "ops_per_thread": 100000000,
    "reopen": 0,
    "set_options_one_in": 1000,
}

whitebox_default_params = {
    # TODO: enable this at random once we figure out two things. First, we need
    # to ensure the kill odds in WAL-disabled runs result in regular crashing
    # before the fifteen minute timeout. When WAL is disabled there are very few
    # calls to write functions since writes to SST files are buffered and other
    # writes (e.g., MANIFEST) are infrequent. Crashing in reasonable time might
    # currently assume killpoints in write functions are reached frequently.
    #
    # Second, we need to make sure disabling WAL works with `-reopen > 0`.
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    "duration": 10000,
    "log2_keys_per_lock": 10,
    "ops_per_thread": 200000,
    "random_kill_odd": 888887,
    "reopen": 20,
}

# Liveness mode inherits the normal randomized stress-test matrix and mixed
# workload by default. The C++ watchdog tracks the active operation and
# per-operation completion counts, so liveness does not need to force an
# all-write workload to keep the failure signal understandable. Keep fault
# injection disabled, though: injected read/open/write/sync/cache faults can
# legitimately put the DB into background-error recovery or a no-progress write
# stall for longer than the liveness timeout. Those cases are covered by other
# test modes.
# The Python wrapper owns duration, so keep db_stress's operation budget high
# enough for long liveness runs. The C++ abort/resume path is additionally
# gated on completed compaction progress and backs off when write stall pressure
# is already visible.
no_fault_injection_params = {
    "error_recovery_with_no_fault_injection": 0,
    "exclude_wal_from_write_fault_injection": 0,
    "file_scope_wal_write_faults": 0,
    "metadata_read_fault_one_in": 0,
    "metadata_write_fault_one_in": 0,
    "open_metadata_read_fault_one_in": 0,
    "open_metadata_write_fault_one_in": 0,
    "open_read_fault_one_in": 0,
    "open_write_fault_one_in": 0,
    "read_fault_one_in": 0,
    "secondary_cache_fault_one_in": 0,
    "sync_fault_injection": 0,
    "write_fault_one_in": 0,
}

liveness_default_params = {
    "duration": DEFAULT_LIVENESS_TIMEOUT_SEC,
    "enable_thread_tracking": 1,
    "liveness_check_interval_sec": 1,
    "liveness_no_progress_timeout_sec": DEFAULT_LIVENESS_NO_PROGRESS_TIMEOUT_SEC,
    "ops_per_thread": 100000000,
    "progress_reports": 1,
}
liveness_default_params.update(no_fault_injection_params)


def liveness_default_timeout_sec():
    multiplier = REMOTE_DB_LIVENESS_TIMEOUT_MULTIPLIER if is_remote_db else 1
    return DEFAULT_LIVENESS_TIMEOUT_SEC * multiplier


def apply_liveness_remote_defaults(params, args):
    if not is_remote_db:
        return

    # Keep the wrapper duration in the same ratio as the no-progress timeout so
    # the longer remote DB timeout still has enough run time to exercise
    # liveness.
    if getattr(args, "duration", None) is None:
        params["duration"] = liveness_default_timeout_sec()
    if getattr(args, "liveness_no_progress_timeout_sec", None) is None:
        params["liveness_no_progress_timeout_sec"] = (
            DEFAULT_LIVENESS_NO_PROGRESS_TIMEOUT_SEC
            * REMOTE_DB_LIVENESS_TIMEOUT_MULTIPLIER
        )


simple_default_params = {
    "allow_concurrent_memtable_write": lambda: random.randint(0, 1),
    "column_families": 1,
    # TODO: re-enable once internal task T124324915 is fixed.
    # "experimental_mempurge_threshold": lambda: 10.0*random.random(),
    "max_background_compactions": 1,
    "max_bytes_for_level_base": 67108864,
    "memtablerep": "skip_list",
    "target_file_size_base": 16777216,
    "target_file_size_multiplier": 1,
    "test_batches_snapshots": 0,
    "write_buffer_size": 32 * 1024 * 1024,
    "level_compaction_dynamic_level_bytes": lambda: random.randint(0, 1),
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "test_secondary": lambda: random.choice([0, 1]),
    "open_read_only_one_in": lambda: random.choice([0, 0, 0, 16]),
}

blackbox_simple_default_params = {
    "open_files": -1,
    "set_options_one_in": 0,
}

whitebox_simple_default_params = {}

full_external_table_params = {
    # Backup verification reconstructs Options from strings, but the test-only
    # full external table factory is not registered for reconstruction.
    "backup_one_in": 0,
    # The db_stress full external table integration currently supports one CF.
    "column_families": 1,
    # The simple reader loads each open table into memory, so keep generated
    # files and the number held open small enough to bound the DB's memory use.
    "open_files": 100,
    "target_file_size_base": 1024 * 1024,
    "target_file_size_multiplier": 1,
    "write_buffer_size": 1024 * 1024,
    # RocksDB permits TTL only for factories identifying as block-based.
    "compaction_ttl": 0,
    # Exercise range deletions while keeping the workload at 100%.
    "delpercent": 4,
    "delrangepercent": 1,
    # Aggregated table properties are not supported by the simple factory.
    "get_property_one_in": 0,
    # This probe cannot distinguish checksum failures from injected read faults.
    "get_properties_of_all_tables_one_in": 0,
    # Embedded-blob SST writing requires direct block-based table options.
    "ingest_external_file_with_embedded_blobs": 0,
    # WAL-disabled WBWI state can be lost on a crash before the ingested
    # memtable is flushed, which the expected-state recovery cannot reconstruct.
    "ingest_wbwi_one_in": 0,
    # RocksDB permits periodic compaction only for block-based factories.
    "periodic_compaction_seconds": 0,
    # Full external tables do not persist sequence-number-to-time mappings.
    "preclude_last_level_data_seconds": 0,
    "preserve_internal_time_seconds": 0,
    # Remote-compaction workers reconstruct factories by name, but this test
    # factory is not registered.
    "remote_compaction_worker_threads": 0,
    # The randomized option table includes a block_based_table_factory update,
    # which is invalid when the active factory is external.
    "set_options_one_in": 0,
    # DBIter::Prev() requires value pinning that the external iterator adapter
    # does not currently propagate.
    "test_backward_scan": 0,
    # Select the table implementation under test.
    "use_full_external_table": 1,
    # TimedPut is not an entry type supported by external tables.
    "use_timed_put_one_in": 0,
    # External tables reject user-defined timestamps in keys and comparators.
    "user_timestamp_size": 0,
}

cf_consistency_params = {
    "disable_wal": lambda: random.randint(0, 1),
    "reopen": 0,
    "test_cf_consistency": 1,
    "test_secondary": lambda: random.randint(0, 1),
    # use small value for write_buffer_size so that RocksDB triggers flush
    # more frequently
    "write_buffer_size": 1024 * 1024,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    # Snapshots are used heavily in this test mode, while they are incompatible
    # with compaction filter, inplace_update_support
    "enable_compaction_filter": 0,
    "inplace_update_support": 0,
    # `CfConsistencyStressTest::TestIngestExternalFile()` is not implemented.
    "ingest_external_file_one_in": 0,
    # `CfConsistencyStressTest::TestIterateAgainstExpected()` is not implemented.
    "verify_iterator_with_expected_state_one_in": 0,
    "memtablerep": random.choice(["skip_list"] * 9 + ["vector"]),
}

cf_lifecycle_params = {
    "test_cf_lifecycle": 1,
    "cf_lifecycle_fault_target": 0,
    "column_families": 2,
    "threads": 1,
    "clear_column_family_one_in": 100,
    "track_options_file_number_in_manifest": 1,
    # FaultInjectionTestFS cannot wrap an existing MANIFEST reopened from a
    # prior process. Create a fresh MANIFEST at open so its subsequent CF edit
    # always crosses the targeted write-fault wrapper.
    "reuse_manifest_on_open": 0,
    # This profile deliberately does not use a persistent expected-values
    # directory because clear_column_family_one_in is incompatible with it.
    # Restrict the workload to writes and disable data verification so a
    # restart does not compare the retained DB contents against a fresh
    # in-memory expected state. The lifecycle consistency oracle still runs
    # before every writable open, including verification_only runs.
    "readpercent": 0,
    "prefixpercent": 0,
    "writepercent": 100,
    "delpercent": 0,
    "delrangepercent": 0,
    "iterpercent": 0,
    "customopspercent": 0,
    "nooverwritepercent": 0,
    "skip_verifydb": 1,
    "verify_db_one_in": 0,
    "continuous_verification_interval": 0,
    "verify_iterator_with_expected_state_one_in": 0,
    "acquire_snapshot_one_in": 0,
    "key_may_exist_one_in": 0,
    "use_multiget": 0,
    "use_get_entity": 0,
    "use_multi_get_entity": 0,
    "use_attribute_group": 0,
    "use_multi_cf_iterator": 0,
    "use_multiscan": 0,
    "use_async_db_api": 0,
    "use_merge": 0,
    "use_put_entity_one_in": 0,
    "use_timed_put_one_in": 0,
    "use_write_buffer_manager": 0,
    "wbm_flush_policy": 0,
    # Keep the profile focused on foreground writes and CF metadata changes.
    # Random maintenance APIs can outlive the blackbox SIGTERM grace period
    # and obscure the restart result with shutdown-only failures.
    "ingest_external_file_one_in": 0,
    "set_options_one_in": 0,
    "compact_files_one_in": 0,
    "compact_range_one_in": 0,
    "flush_one_in": 0,
    "mark_for_compaction_one_file_in": 0,
    "pause_background_one_in": 0,
    "disable_file_deletions_one_in": 0,
    "disable_manual_compaction_one_in": 0,
    "reset_stats_one_in": 0,
    "get_property_one_in": 0,
    "get_properties_of_all_tables_one_in": 0,
    "get_live_files_apis_one_in": 0,
    "get_all_column_family_metadata_one_in": 0,
    "verify_checksum_one_in": 0,
    "verify_file_checksums_one_in": 0,
    "verify_manifest_content_on_close": 0,
    "long_running_snapshots": 0,
    "periodic_compaction_seconds": 0,
    "compaction_ttl": 0,
    "preserve_internal_time_seconds": 0,
    "remote_compaction_worker_threads": 0,
    "manual_wal_flush_one_in": 0,
    "sync_wal_one_in": 0,
    "lock_wal_one_in": 0,
    "disable_wal": 0,
    "reopen": 0,
    "read_only": 0,
    "test_batches_snapshots": 0,
    "test_cf_consistency": 0,
    "test_multi_ops_txns": 0,
    "use_txn": 0,
    "use_optimistic_txn": 0,
    "ttl": -1,
    "use_blob_db": 0,
    "enable_blob_files": 0,
    "enable_blob_direct_write": 0,
    "test_secondary": 0,
    "open_read_only_one_in": 0,
    "backup_one_in": 0,
    "checkpoint_one_in": 0,
    "parallel_checkpoint_one_in": 0,
    "subset_cf_checkpoint_one_in": 0,
    "abort_and_resume_compactions_one_in": 0,
    "abort_and_resume_cf_compactions_one_in": 0,
    "num_dbs": 1,
}
cf_lifecycle_params.update(no_fault_injection_params)

# For pessimistic transaction db
txn_params = {
    "use_txn": 1,
    "use_optimistic_txn": 0,
    # Avoid lambda to set it once for the entire test
    # NOTE: often passed in from command line overriding this
    "txn_write_policy": random.randint(0, 2),
    "unordered_write": random.randint(0, 1),
    "use_per_key_point_lock_mgr": lambda: random.choice([0, 1]),
    # TODO: there is such a thing as transactions with WAL disabled. We should
    # cover that case.
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
    "create_timestamped_snapshot_one_in": random.choice([0, 20]),
    # Should not be used with TransactionDB which uses snapshot.
    "inplace_update_support": 0,
    # TimedPut is not supported in transaction
    "use_timed_put_one_in": 0,
    # txn commit with this option will create a new memtable, keep the
    # frequency low to reduce stalls
    "commit_bypass_memtable_one_in": random.choice([0] * 2 + [500, 1000]),
    "two_write_queues": lambda: random.choice([0, 1]),
}

# For optimistic transaction db
optimistic_txn_params = {
    "use_txn": 1,
    "use_optimistic_txn": 1,
    "occ_validation_policy": random.randint(0, 1),
    "share_occ_lock_buckets": random.randint(0, 1),
    "occ_lock_bucket_count": lambda: random.choice([10, 100, 500]),
    # Should not be used with OptimisticTransactionDB which uses snapshot.
    "inplace_update_support": 0,
    # TimedPut is not supported in transaction
    "use_timed_put_one_in": 0,
}

best_efforts_recovery_params = {
    "best_efforts_recovery": 1,
    "disable_wal": 1,
    "column_families": 1,
    "skip_verifydb": 1,
    "verify_db_one_in": 0,
}

blob_params = {
    "allow_setting_blob_options_dynamically": 1,
    # Enable blob files and GC with a 75% chance initially; note that they might still be
    # enabled/disabled during the test via SetOptions
    "enable_blob_files": lambda: random.choice([0] + [1] * 3),
    "min_blob_size": lambda: random.choice([0, 8, 16]),
    "blob_file_size": lambda: random.choice([1048576, 16777216, 268435456, 1073741824]),
    "blob_compression_type": lambda: random.choice(["none", "snappy", "lz4", "zstd"]),
    "enable_blob_garbage_collection": lambda: random.choice([0] + [1] * 3),
    "blob_garbage_collection_age_cutoff": lambda: random.choice(
        [0.0, 0.25, 0.5, 0.75, 1.0]
    ),
    "blob_garbage_collection_force_threshold": lambda: random.choice([0.5, 0.75, 1.0]),
    "blob_compaction_readahead_size": lambda: random.choice([0, 1048576, 4194304]),
    "blob_file_writable_file_max_buffer_size": lambda: random.choice(
        [0, 65536, 131072, 1048576]
    ),
    "blob_file_starting_level": lambda: random.choice(
        [0] * 4 + [1] * 3 + [2] * 2 + [3]
    ),
    "use_blob_cache": lambda: random.randint(0, 1),
    "use_shared_block_and_blob_cache": lambda: random.randint(0, 1),
    "blob_cache_size": lambda: random.choice([1048576, 2097152, 4194304, 8388608]),
    "prepopulate_blob_cache": lambda: random.randint(0, 1),
    # TODO Fix races when both Remote Compaction + BlobDB enabled
    "remote_compaction_worker_threads": 0,
}

blob_direct_write_params = {
    "enable_blob_files": 1,
    "enable_blob_direct_write": 1,
    "blob_direct_write_partitions": lambda: random.choice([1, 2, 4, 8]),
    "allow_setting_blob_options_dynamically": 0,
    # Keep the fixed-across-runs write mode within the reduced WAL-disabled
    # direct-write profile.
    "inplace_update_support": 0,
    "min_blob_size": lambda: random.choice([8, 16, 64]),
    "blob_file_size": lambda: random.choice([1048576, 16777216, 268435456]),
    "blob_file_writable_file_max_buffer_size": lambda: random.choice(
        [0, 65536, 131072, 1048576]
    ),
    "blob_compression_type": lambda: random.choice(["none", "snappy", "lz4", "zstd"]),
    "enable_blob_garbage_collection": 0,
    "blob_garbage_collection_age_cutoff": 0.0,
    "blob_garbage_collection_force_threshold": 1.0,
    "blob_compaction_readahead_size": 0,
    "blob_file_starting_level": 0,
    "use_blob_cache": lambda: random.randint(0, 1),
    "use_shared_block_and_blob_cache": lambda: random.randint(0, 1),
    "blob_cache_size": lambda: random.choice([1048576, 2097152, 4194304, 8388608]),
    "prepopulate_blob_cache": lambda: random.randint(0, 1),
    "remote_compaction_worker_threads": 0,
}

# Wide-column entity stress needs `use_put_entity_one_in` fixed across the
# repeated db_stress invocations in one crash-test run series, so keep it as a
# once-per-process random choice rather than a lambda.
blob_direct_write_get_entity_params = dict(blob_direct_write_params)
blob_direct_write_get_entity_params.update(
    {
        "use_put_entity_one_in": random.choice([1, 5, 10]),
        "use_get_entity": 1,
        "use_multi_get_entity": 0,
        "use_attribute_group": 0,
    }
)

blob_direct_write_multi_get_entity_params = dict(blob_direct_write_params)
blob_direct_write_multi_get_entity_params.update(
    {
        "use_put_entity_one_in": random.choice([1, 5, 10]),
        "use_get_entity": 0,
        "use_multi_get_entity": 1,
        "use_attribute_group": 0,
    }
)

ts_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "user_timestamp_size": 8,
    # Below flag is randomly picked once and kept consistent in following runs.
    "persist_user_defined_timestamps": random.choice([0, 1, 1]),
    "use_merge": 0,
    # Causing failures and not yet compatible
    "use_multiscan": 0,
    "use_full_merge_v1": 0,
    "use_txn": 0,
    "ingest_external_file_one_in": 0,
    # PutEntity with timestamps is not yet implemented
    "use_put_entity_one_in": 0,
    # TimedPut is not compatible with user-defined timestamps yet.
    "use_timed_put_one_in": 0,
    # TrieIndexFactory requires plain BytewiseComparator, but timestamps use
    # BytewiseComparator.u64ts.
    "use_trie_index": 0,
    # when test_best_efforts_recovery == true, disable_wal becomes 0.
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
}

tiered_params = {
    # For Leveled/Universal compaction (ignored for FIFO)
    # Bias toward times that can elapse during a crash test run series
    # NOTE: -1 means starting disabled but dynamically changing
    "preclude_last_level_data_seconds": lambda: random.choice(
        [-1, -1, 10, 60, 1200, 86400]
    ),
    "last_level_temperature": lambda: random.choice(["kCold", "kIce"]),
    # For FIFO compaction (ignored otherwise)
    "file_temperature_age_thresholds": lambda: random.choice(
        [
            "{{temperature=kWarm;age=10}:{temperature=kCool;age=30}:{temperature=kCold;age=100}:{temperature=kIce;age=300}}",
            "{{temperature=kWarm;age=30}:{temperature=kCold;age=300}}",
            "{{temperature=kCold;age=100}}",
        ]
    ),
    "allow_trivial_copy_when_change_temperature": lambda: random.choice([0, 1]),
    # tiered storage doesn't support blob db yet
    "enable_blob_files": 0,
    "use_blob_db": 0,
    "default_write_temperature": lambda: random.choice(["kUnknown", "kHot", "kWarm"]),
}

multiops_txn_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "test_multi_ops_txns": 1,
    "use_txn": 1,
    # Avoid lambda to set it once for the entire test
    # NOTE: often passed in from command line overriding this
    "txn_write_policy": random.randint(0, 2),
    "two_write_queues": lambda: random.choice([0, 1]),
    # TODO: enable write-prepared
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    "use_only_the_last_commit_time_batch_for_recovery": lambda: random.choice([0, 1]),
    "clear_column_family_one_in": 0,
    "column_families": 1,
    # TODO re-enable pipelined write (lambda: random.choice([0, 1]))
    "enable_pipelined_write": 0,
    # This test already acquires snapshots in reads
    "acquire_snapshot_one_in": 0,
    "backup_one_in": 0,
    "writepercent": 0,
    "delpercent": 0,
    "delrangepercent": 0,
    "customopspercent": 80,
    "readpercent": 5,
    "iterpercent": 15,
    "prefixpercent": 0,
    "verify_db_one_in": 1000,
    "continuous_verification_interval": 1000,
    "delay_snapshot_read_one_in": 3,
    # 65536 is the smallest possible value for write_buffer_size. Smaller
    # values will be sanitized to 65536 during db open. SetOptions currently
    # does not sanitize options, but very small write_buffer_size may cause
    # assertion failure in
    # https://github.com/meta/rocksdb/blob/7.0.fb/db/memtable.cc#L117.
    "write_buffer_size": 65536,
    # flush more frequently to generate more files, thus trigger more
    # compactions.
    "flush_one_in": 1000,
    "key_spaces_path": setup_multiops_txn_key_spaces_file(),
    "rollback_one_in": 4,
    # Re-enable once we have a compaction for MultiOpsTxnStressTest
    "enable_compaction_filter": 0,
    "create_timestamped_snapshot_one_in": 50,
    "sync_fault_injection": 0,
    "metadata_write_fault_one_in": 0,
    "manual_wal_flush_one_in": 0,
    # This test has aggressive flush frequency and small write buffer size.
    # Disabling write fault to avoid writes being stopped.
    "write_fault_one_in": 0,
    "metadata_write_fault_one_in": 0,
    # PutEntity in transactions is not yet implemented
    "use_put_entity_one_in": 0,
    "use_get_entity": 0,
    "use_multi_get_entity": 0,
    # `MultiOpsTxnsStressTest::TestIterateAgainstExpected()` is not implemented.
    "verify_iterator_with_expected_state_one_in": 0,
    # This test uses snapshot heavily which is incompatible with this option.
    "inplace_update_support": 0,
    # TimedPut not supported in transaction
    "use_timed_put_one_in": 0,
    # AttributeGroup not yet supported
    "use_attribute_group": 0,
    "commit_bypass_memtable_one_in": random.choice([0] * 4 + [100]),
}


def finalize_and_sanitize(src_params):
    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}
    apply_cache_and_write_buffer_size_multiplier(dest_params)
    if is_release_mode():
        dest_params["read_fault_one_in"] = 0
    if dest_params.get("compression_max_dict_bytes") == 0:
        dest_params["compression_zstd_max_train_bytes"] = 0
        dest_params["compression_max_dict_buffer_bytes"] = 0
    if dest_params.get("compression_type") != "zstd":
        dest_params["compression_zstd_max_train_bytes"] = 0
    if dest_params["mmap_read"] == 1:
        dest_params["use_direct_io_for_flush_and_compaction"] = 0
        dest_params["use_direct_reads"] = 0
        dest_params["use_direct_io_for_compaction_reads"] = 0
        dest_params["multiscan_use_async_io"] = 0
        dest_params["open_read_only_one_in"] = 0
    if dest_params.get("min_tombstones_for_range_conversion", 0) > 0:
        # SQFC range-query filtering installs ReadOptions::table_filter on
        # iterators. Read-write iterators reject table_filter when read-path
        # range tombstone conversion is enabled, because conversion must see
        # the full relevant SST set before synthesizing a memtable tombstone.
        dest_params["use_sqfc_for_range_queries"] = 0
        # Delete range not compatible with inplace_update_support
        dest_params["inplace_update_support"] = 0
    if (
        dest_params["use_direct_io_for_flush_and_compaction"] == 1
        or dest_params["use_direct_reads"] == 1
        or dest_params["use_direct_io_for_compaction_reads"] == 1
    ) and not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print(
                "{} does not support direct IO. Disabling use_direct_reads, "
                "use_direct_io_for_compaction_reads and "
                "use_direct_io_for_flush_and_compaction.\n".format(dest_params["db"])
            )
            dest_params["use_direct_reads"] = 0
            dest_params["use_direct_io_for_compaction_reads"] = 0
            dest_params["use_direct_io_for_flush_and_compaction"] = 0
        else:
            dest_params["mock_direct_io"] = True

    # Blob direct write requires concurrent read visibility of files still open
    # for writing (BlobFileReader calls GetFileSize() on active partition files).
    # Some remote file systems do not guarantee that writes from a WritableFile
    # are visible to a separate RandomAccessFile until the writer is closed,
    # causing "Malformed blob file" corruption. Disable BDW when a remote
    # --env_uri / --fs_uri is in use.
    if is_remote_db:
        dest_params["enable_blob_direct_write"] = 0

    # Not to accidentally ignore errors on local dbs
    # (e.g. errors on IO Uring would be categorized as IO error)
    if not is_remote_db:
        dest_params["tolerate_non_injected_io_errors_for_remote_dbs"] = 0

    if dest_params.get("enable_blob_direct_write", 0) == 1:
        # Keep blob direct write in its reduced-scope crash-test profile.
        #
        # Supported recovery shape:
        #   * clean shutdown / flush
        #   * crash restart that only relies on SST + manifest-visible blob
        #     state, including wide-column entities stored through PutEntity
        #
        # Unsupported here:
        #   * WAL replay / best-efforts recovery
        #   * broad dynamic blob option changes and blob GC
        #   * merge / transaction variants and parallel write queue modes
        #   * secondary / backup / checkpoint / ingest style APIs that reason
        #     about active files directly
        dest_params["enable_blob_files"] = 1
        dest_params["blob_direct_write_partitions"] = max(
            1, dest_params.get("blob_direct_write_partitions", 1)
        )
        # BDW can still run with multiple application threads as long as writes
        # stay on the ordered write path. Disable the write modes that would
        # let memtable/WAL publication diverge from the transformed blob-file
        # write ordering.
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["enable_pipelined_write"] = 0
        dest_params["two_write_queues"] = 0
        dest_params["unordered_write"] = 0
        # Keep inplace updates off. The later inplace_update_support
        # sanitization intentionally forces disable_wal=0 for its own recovery
        # assumptions, which would silently undo the BDW crash-test profile.
        dest_params["inplace_update_support"] = 0
        # Direct write is implemented only for integrated BlobDB, without
        # dynamic blob option changes or background blob GC.
        dest_params["use_blob_db"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["enable_blob_garbage_collection"] = 0
        dest_params["blob_garbage_collection_age_cutoff"] = 0.0
        dest_params["blob_garbage_collection_force_threshold"] = 1.0
        dest_params["blob_compaction_readahead_size"] = 0
        dest_params["blob_file_starting_level"] = 0
        dest_params["use_merge"] = 0
        dest_params["use_full_merge_v1"] = 0
        dest_params["use_timed_put_one_in"] = 0
        # Wide-column PutEntity/GetEntity/MultiGetEntity are now compatible
        # with this profile. AttributeGroup exercises a different path that
        # still stays disabled here.
        dest_params["use_attribute_group"] = 0
        # Direct write stress only supports the plain comparator / key encoding
        # path. User-defined timestamps and the TransactionDB-only timestamped
        # snapshot API are outside this feature envelope.
        dest_params["user_timestamp_size"] = 0
        dest_params["persist_user_defined_timestamps"] = 0
        dest_params["create_timestamped_snapshot_one_in"] = 0
        dest_params["use_txn"] = 0
        dest_params["txn_write_policy"] = 0
        dest_params["use_optimistic_txn"] = 0
        dest_params["test_multi_ops_txns"] = 0
        dest_params["commit_bypass_memtable_one_in"] = 0
        # Force the WAL-disabled crash-test profile. The generic disable_wal
        # sanitization below still applies, but keep the WAL-dependent stress
        # features explicitly off here so later sanitizers or explicit command
        # line overrides do not silently re-enable them.
        dest_params["disable_wal"] = 1
        dest_params["best_efforts_recovery"] = 0
        # Direct write v1 crash testing does not cover reopen with unlogged
        # data, manual/sync WAL persistence, or WAL metadata/locking APIs.
        dest_params["reopen"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        dest_params["sync_wal_one_in"] = 0
        dest_params["lock_wal_one_in"] = 0
        dest_params["get_sorted_wal_files_one_in"] = 0
        dest_params["get_current_wal_file_one_in"] = 0
        dest_params["track_and_verify_wals"] = 0
        dest_params["rate_limit_auto_wal_flush"] = 0
        dest_params["recycle_log_file_num"] = 0
        # Write/open fault injection currently assumes WAL-based recovery or
        # error-retry behavior that direct write v1 does not provide.
        dest_params["sync_fault_injection"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        dest_params["open_metadata_write_fault_one_in"] = 0
        dest_params["open_metadata_read_fault_one_in"] = 0
        dest_params["open_write_fault_one_in"] = 0
        dest_params["open_read_fault_one_in"] = 0
        # Remote compaction, secondary readers, file snapshot style APIs, and
        # ingest APIs are outside the initial direct-write feature envelope.
        dest_params["remote_compaction_worker_threads"] = 0
        dest_params["test_secondary"] = 0
        dest_params["backup_one_in"] = 0
        dest_params["checkpoint_one_in"] = 0
        dest_params["get_live_files_apis_one_in"] = 0
        dest_params["ingest_external_file_one_in"] = 0
        dest_params["ingest_wbwi_one_in"] = 0

    if dest_params.get("memtablerep") == "vector":
        dest_params["inplace_update_support"] = 0

    # only skip list memtable representation supports paranoid memory checks
    if dest_params.get("memtablerep") != "skip_list":
        dest_params["paranoid_memory_checks"] = 0
        dest_params["memtable_verify_per_key_checksum_on_seek"] = 0

    if dest_params["test_batches_snapshots"] == 1:
        dest_params["enable_compaction_filter"] = 0
        dest_params["inplace_update_support"] = 0
        # TODO(hx235): enable test_batches_snapshots with fault injection after stabilizing the CI
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        dest_params["use_multiscan"] = 0
        if dest_params["prefix_size"] < 0:
            dest_params["prefix_size"] = 1

    # BER disables WAL and tests unsynced data loss which
    # does not work with inplace_update_support. Integrated BlobDB is also
    # incompatible, so force blob-related toggles off even if they came from
    # command-line overrides or another preset.
    if dest_params.get("best_efforts_recovery") == 1:
        dest_params["inplace_update_support"] = 0
        dest_params["enable_blob_files"] = 0
        dest_params["enable_blob_garbage_collection"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["enable_blob_direct_write"] = 0

    # Remote Compaction Incompatible Tests and Features
    if dest_params.get("remote_compaction_worker_threads", 0) > 0:
        # Disable Incompatible Ones
        dest_params["inplace_update_support"] = 0
        dest_params["checkpoint_one_in"] = 0
        dest_params["use_timed_put_one_in"] = 0
        dest_params["test_secondary"] = 0
        dest_params["mmap_read"] = 0
        # skip_stats_update_on_db_open leaves num_entries and
        # num_range_deletions at 0 on the remote worker, which breaks
        # standalone range deletion file filtering in compaction and causes
        # input key count mismatch. This can happen even if standalone range
        # deletion ingestion is off in the current run, because such files
        # may have been ingested in a previous run with different options.
        # TODO: remove after the real fix lands.
        dest_params["skip_stats_update_on_db_open"] = 0

        # Disable database open fault injection to prevent test inefficiency described below.
        # When fault injection occurs during DB open, the db will wait for compaction
        # to finish to clean up the database before retrying without injected error.
        # However remote compaction threads are not yet created at that point
        # so the db has to wait for the timeout (currently 30 seconds) to fall back to
        # local compaction in order for the compaction to finish.
        #
        # TODO: Consider moving compaction thread creation earlier in the startup sequence
        # to allow db open fault injection testing without this performance penalty
        dest_params["open_metadata_write_fault_one_in"] = 0
        dest_params["open_metadata_read_fault_one_in"] = 0
        dest_params["open_write_fault_one_in"] = 0
        dest_params["open_read_fault_one_in"] = 0
        dest_params["sync_fault_injection"] = 0

    # UDI is compatible with all operation types and iteration directions
    # except mmap_read.
    if dest_params.get("use_trie_index") == 1:
        # Trie UDI uses zero-copy pointers into block data, which is
        # incompatible with mmap_read.
        dest_params["mmap_read"] = 0
        index_mode = dest_params.get("index_mode")
        if index_mode in (2, 3):
            # kCustomDefault/kCustomOnly reject partitioned index
            # (kTwoLevelIndexSearch) and partitioned filters.
            dest_params["index_type"] = random.choice([0, 0, 3])
            dest_params["partition_filters"] = 0
            # db_stress rebuilds the restored DB's options from strings that
            # do not carry the user_defined_index_factory, so the restored DB
            # cannot read through the trie, and cannot open kCustomOnly SSTs.
            dest_params["backup_one_in"] = 0
            # The secondary instance gets the trie through the column family
            # options, but it has not been crash tested with the trie as the
            # default read index.
            dest_params["test_secondary"] = 0
        if index_mode == 3:
            # kCustomOnly requires format_version >= 6.
            dest_params["format_version"] = max(6, dest_params["format_version"])
            # kCustomOnly SSTs have only a stub standard index, so db_stress
            # rejects kBuiltin reads.
            if dest_params.get("read_index") == 1:
                dest_params["read_index"] = -1
    # Multi-key operations are not currently compatible with transactions or
    # timestamp.
    if (
        dest_params.get("test_batches_snapshots") == 1
        or dest_params.get("use_txn") == 1
        or dest_params.get("user_timestamp_size", 0) > 0
    ):
        dest_params["ingest_external_file_one_in"] = 0
    if (
        dest_params.get("test_batches_snapshots") == 1
        or dest_params.get("use_txn") == 1
    ):
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
    # Since the value of inplace_update_support needs to be fixed across runs,
    # we disable other incompatible options here instead of disabling
    # inplace_update_support based on other option values, which may change
    # across runs.
    if dest_params["inplace_update_support"] == 1:
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
        dest_params["readpercent"] += dest_params["prefixpercent"]
        dest_params["prefixpercent"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        # inplace_update_support does not update sequence number. Our stress test recovery
        # logic for unsynced data loss relies on max sequence number stored
        # in MANIFEST, so they don't work together.
        dest_params["sync_fault_injection"] = 0
        dest_params["disable_wal"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
    if (
        dest_params.get("sync_fault_injection") == 1
        or dest_params.get("disable_wal") == 1
        or dest_params.get("manual_wal_flush_one_in", 0) > 0
    ):
        # File ingestion does not guarantee prefix-recoverability when unsynced
        # data can be lost. Ingesting a file syncs data immediately that is
        # newer than unsynced memtable data that can be lost on restart.
        #
        # Even if the above issue is fixed or worked around, our
        # trace-and-replay does not trace file ingestion, so in its current form
        # it would not recover the expected state to the correct point in time.
        dest_params["ingest_external_file_one_in"] = 0
        # The `DbStressCompactionFilter` can apply memtable updates to SST
        # files, which would be problematic when unsynced data can be lost in
        # crash recoveries.
        dest_params["enable_compaction_filter"] = 0
    # Remove the following once write-prepared/write-unprepared with/without
    # unordered write supports timestamped snapshots
    if dest_params.get("create_timestamped_snapshot_one_in", 0) > 0:
        dest_params["unordered_write"] = 0
        if dest_params.get("txn_write_policy", 0) != 0:
            dest_params["create_timestamped_snapshot_one_in"] = 0
    # Only under WritePrepared txns, unordered_write would provide the same guarnatees as vanilla rocksdb
    # unordered_write is only enabled with --txn, and txn_params disables inplace_update_support, so
    # setting allow_concurrent_memtable_write=1 won't conflcit with inplace_update_support.
    # don't overwrite txn_write_policy
    if dest_params.get("unordered_write", 0) == 1:
        if dest_params.get("txn_write_policy", 0) == 1:
            dest_params["allow_concurrent_memtable_write"] = 1
        else:
            dest_params["unordered_write"] = 0
    if dest_params.get("disable_wal", 0) == 1:
        # WAL-disabled stress runs do not support in-process reopen. Blob
        # direct write v1 relies on this path so crash testing stays within its
        # SST/blob-manifest recovery envelope rather than WAL replay.
        dest_params["atomic_flush"] = 1
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["reopen"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        # disableWAL and recycle_log_file_num options are not mutually
        # compatible at the moment
        dest_params["recycle_log_file_num"] = 0
    if dest_params.get("open_files", 1) != -1:
        # Compaction TTL and periodic compactions are only compatible
        # with open_files = -1
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params.get("compaction_style", 0) == 2:
        # Disable compaction TTL in FIFO compaction, because right
        # now assertion failures are triggered.
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
        # FIFO compaction is not supported with open_files_async
        dest_params["open_files_async"] = 0
        # Disable irrelevant tiering options
        dest_params["preclude_last_level_data_seconds"] = 0
        dest_params["last_level_temperature"] = "kUnknown"
    else:
        # Disable irrelevant tiering options
        dest_params["file_temperature_age_thresholds"] = ""
        # Disable FIFO-specific options for non-FIFO compaction styles
        dest_params["fifo_compaction_max_data_files_size_mb"] = 0
        dest_params["fifo_compaction_max_table_files_size_mb"] = 0
        dest_params["fifo_compaction_use_kv_ratio_compaction"] = 0
    if dest_params["partition_filters"] == 1:
        if dest_params["index_type"] != 2:
            dest_params["partition_filters"] = 0
    if dest_params.get("atomic_flush", 0) == 1:
        # disable pipelined write when atomic flush is used.
        dest_params["enable_pipelined_write"] = 0
    # Truncating SST files in primary DB is incompatible
    # with secondary DB since the latter can't read the shared
    # and truncated SST file correctly
    if (
        dest_params.get("sst_file_manager_bytes_per_sec", 0) == 0
        or dest_params.get("test_secondary") == 1
    ):
        dest_params["sst_file_manager_bytes_per_truncate"] = 0
    if dest_params.get("prefix_size") == -1:
        dest_params["readpercent"] += dest_params.get("prefixpercent", 20)
        dest_params["prefixpercent"] = 0
    elif dest_params.get("simple") and dest_params.get("test_type") == "blackbox":
        # `db_stress` randomizes iterate_lower_bound independently of the seek
        # target. With a configured prefix extractor this can violate
        # ReadOptions' same-prefix requirement, so disable random iterator
        # operations in simple blackbox mode.
        dest_params["readpercent"] += dest_params.get("iterpercent", 10)
        dest_params["iterpercent"] = 0
    if (
        dest_params.get("prefix_size") == -1
        and dest_params.get("memtable_whole_key_filtering") == 0
    ):
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
    if dest_params.get("two_write_queues") == 1:
        dest_params["enable_pipelined_write"] = 0
    if dest_params.get("best_efforts_recovery") == 1:
        dest_params["disable_wal"] = 1
        dest_params["enable_compaction_filter"] = 0
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["skip_verifydb"] = 1
        dest_params["verify_db_one_in"] = 0
    # For TransactionDB, correctness testing with unsync data loss is currently
    # compatible with only write committed policy
    if dest_params.get("use_txn") == 1 and dest_params.get("txn_write_policy", 0) != 0:
        dest_params["sync_fault_injection"] = 0
        dest_params["disable_wal"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        # Wide-column pessimistic transaction APIs are initially supported for
        # WriteCommitted only
        dest_params["use_put_entity_one_in"] = 0
        # MultiCfIterator is currently only compatible with write committed policy
        dest_params["use_multi_cf_iterator"] = 0
        # only works with write committed policy
        dest_params["commit_bypass_memtable_one_in"] = 0
        # not compatible with Remote Compaction yet
        dest_params["remote_compaction_worker_threads"] = 0
    # TODO(hx235): enable test_multi_ops_txns with fault injection after stabilizing the CI
    if dest_params.get("test_multi_ops_txns") == 1:
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        if dest_params.get("txn_write_policy", 0) != 0:
            # TODO: should any of this change for WUP (txn_write_policy==2)?
            dest_params["wp_snapshot_cache_bits"] = 1
            # try small wp_commit_cache_bits, e.g. 0 once we explore storing full
            # commit sequence numbers in commit cache
            dest_params["wp_commit_cache_bits"] = 10
            # pipeline write is not currnetly compatible with WritePrepared txns
            dest_params["enable_pipelined_write"] = 0
            # OpenReadOnly after checkpoint is not currently compatible with WritePrepared txns
            dest_params["checkpoint_one_in"] = 0
            # Required to be 1 in order to use commit-time-batch
            dest_params["use_only_the_last_commit_time_batch_for_recovery"] = 1
            dest_params["clear_wp_commit_cache_one_in"] = 10
            # sequence number can be advanced in SwitchMemtable::WriteRecoverableState() for WP.
            # disable it for now until we find another way to test LockWAL().
            dest_params["lock_wal_one_in"] = 0

    # Wide column stress tests require FullMergeV3
    if dest_params["use_put_entity_one_in"] != 0:
        dest_params["use_full_merge_v1"] = 0
    if dest_params["file_checksum_impl"] == "none":
        dest_params["verify_file_checksums_one_in"] = 0
    if dest_params["write_fault_one_in"] > 0:
        # background work may be disabled while DB is resuming after some error
        dest_params["max_write_buffer_number"] = max(
            dest_params["max_write_buffer_number"], 10
        )
    if dest_params["secondary_cache_uri"].find("compressed_secondary_cache") >= 0:
        dest_params["compressed_secondary_cache_size"] = 0
        dest_params["compressed_secondary_cache_ratio"] = 0.0
    if dest_params["cache_type"].find("tiered_") >= 0:
        if dest_params["compressed_secondary_cache_size"] > 0:
            dest_params["compressed_secondary_cache_ratio"] = float(
                dest_params["compressed_secondary_cache_size"]
                / (
                    dest_params["cache_size"]
                    + dest_params["compressed_secondary_cache_size"]
                )
            )
            dest_params["compressed_secondary_cache_size"] = 0
        else:
            dest_params["compressed_secondary_cache_ratio"] = 0.0
            dest_params["cache_type"] = dest_params["cache_type"].replace("tiered_", "")
    else:
        if dest_params["secondary_cache_uri"]:
            dest_params["compressed_secondary_cache_size"] = 0
            dest_params["compressed_secondary_cache_ratio"] = 0.0
    if dest_params["use_write_buffer_manager"]:
        if dest_params["cache_size"] <= 0 or dest_params["db_write_buffer_size"] <= 0:
            dest_params["use_write_buffer_manager"] = 0
    if not dest_params["use_write_buffer_manager"]:
        dest_params["wbm_flush_policy"] = 0
    if (
        dest_params["user_timestamp_size"] > 0
        and dest_params["persist_user_defined_timestamps"] == 0
    ):
        # Features that are not compatible with UDT in memtable only feature.
        dest_params["enable_blob_files"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["atomic_flush"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["block_protection_bytes_per_key"] = 0
        # TODO(yuzhangyu): make stress test logic handle this and enable testing
        # these APIs.
        # These operations need to compare side to side one operation with another.
        # It's hard to guarantee their consistency because when timestamps can be
        # collapsed, only operations using the same SuperVersion can be consistent
        # with each other. There is no external APIs to ensure that.
        dest_params["use_multiget"] = 0
        dest_params["use_multi_get_entity"] = 0
        dest_params["readpercent"] += dest_params.get("iterpercent", 10)
        dest_params["iterpercent"] = 0
        # Only best efforts recovery test support disabling wal and
        # disable atomic flush.
        if dest_params["test_best_efforts_recovery"] == 0:
            dest_params["disable_wal"] = 0
    if dest_params.get("user_timestamp_size", 0) > 0:
        # Interpolation search requires BytewiseComparator but user-defined
        # timestamps use BytewiseComparatorWithU64TsWrapper.
        dest_params["index_block_search_type"] = 0
        # TrieIndexFactory requires BytewiseComparator.
        dest_params["use_trie_index"] = 0
    if (
        dest_params.get("enable_compaction_filter", 0) == 1
        or dest_params.get("inplace_update_support", 0) == 1
    ):
        # Compaction filter, inplace update support are incompatible with snapshots. Need to avoid taking
        # snapshots, as well as avoid operations that use snapshots for
        # verification.
        dest_params["acquire_snapshot_one_in"] = 0
        dest_params["compact_range_one_in"] = 0
        # Redistribute to maintain 100% total
        dest_params["readpercent"] += dest_params.get(
            "iterpercent", 10
        ) + dest_params.get("prefixpercent", 20)
        dest_params["iterpercent"] = 0
        dest_params["prefixpercent"] = 0
        dest_params["check_multiget_consistency"] = 0
        dest_params["check_multiget_entity_consistency"] = 0
    if dest_params.get("disable_wal") == 0:
        if (
            dest_params.get("reopen", 0) > 0
            or (
                dest_params.get("manual_wal_flush_one_in")
                and dest_params.get("column_families") != 1
            )
            or (
                dest_params.get("use_txn") != 0
                and dest_params.get("use_optimistic_txn") == 0
            )
        ):
            # 1. Reopen with WAL currently requires persisting WAL data before closing for reopen.
            # Previous injected WAL write errors may not be cleared by the time of closing and ready
            # for persisting WAL.
            # To simplify, we disable any WAL write error injection.
            # TODO(hx235): support WAL write error injection with reopen
            #
            # 2. WAL write failure can drop buffered WAL data. This can cause
            # inconsistency when one CF has a successful flush during auto
            # recovery. Disable the fault injection in this path for now until
            # we have a fix that allows auto recovery.
            #
            # 3. Pessimistic transactions use 2PC, which can't auto-recover from WAL write errors.
            # This is because RocksDB cannot easily discard the corrupted WAL without risking the
            # loss of uncommitted prepared data within the same WAL.
            # Therefore disabling WAL write error injection in stress tests to prevent crashing
            # since stress test does not support injecting errors that can' be auto-recovered.
            #
            # TODO(hx235): support excluding WAL from metadata write fault injection so we don't
            # have to disable metadata write fault injection to other file
            dest_params["exclude_wal_from_write_fault_injection"] = 1
            dest_params["metadata_write_fault_one_in"] = 0
    # Disabling block align if mixed manager is being used
    if dest_params.get("compression_manager") == "custom":
        if dest_params.get("block_align") == 1:
            dest_params["block_align"] = 0
        if dest_params["format_version"] < 7:
            dest_params["format_version"] = 7
    elif (
        dest_params.get("compression_manager") == "mixed"
        or dest_params.get("compression_manager") == "randommixed"
    ):
        dest_params["block_align"] = 0
    else:
        # Enabling block_align with compression is not supported
        if dest_params.get("block_align") == 1:
            dest_params["compression_type"] = "none"
            dest_params["bottommost_compression_type"] = "none"
    if dest_params.get("compression_auto_skip", 0) == 1:
        # AutoSkip only does anything when compression is actually configured,
        # and it is incompatible with block_align (like other compression).
        dest_params["block_align"] = 0
        if dest_params.get("compression_type") == "none":
            dest_params["compression_type"] = random.choice(
                ["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
            )
        # Also ensure the bottommost level actually compresses, so AutoSkip is
        # exercised on bottommost output too (parity with the pre-integration
        # autoskip compression manager coverage).
        if dest_params.get("bottommost_compression_type") == "none":
            dest_params["bottommost_compression_type"] = random.choice(
                ["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
            )
    # If periodic_compaction_seconds is not set, daily_offpeak_time_utc doesn't do anything
    if dest_params.get("periodic_compaction_seconds") == 0:
        dest_params["daily_offpeak_time_utc"] = ""
    # `use_put_entity_one_in` cannot be enabled/disabled across runs, modify
    # `use_timed_put_one_in` option so that they make sense together.
    if dest_params.get("use_put_entity_one_in") == 1:
        dest_params["use_timed_put_one_in"] = 0
    elif (
        dest_params.get("use_put_entity_one_in", 0) > 1
        and dest_params.get("use_timed_put_one_in") == 1
    ):
        dest_params["use_timed_put_one_in"] = 3
    if (
        dest_params.get("write_dbid_to_manifest") == 0
        and dest_params.get("write_identity_file") == 0
    ):
        # At least one must be true
        dest_params["write_dbid_to_manifest"] = 1
    # Checkpoint creation skips flush if the WAL is locked, so enabling lock_wal_one_in
    # can cause checkpoint verification to fail. So make the two mutually exclusive.
    if dest_params.get("checkpoint_one_in") != 0:
        dest_params["lock_wal_one_in"] = 0
    if (
        dest_params.get("ingest_external_file_one_in") == 0
        or dest_params.get("delrangepercent") == 0
    ):
        dest_params["test_ingest_standalone_range_deletion_one_in"] = 0
    if (
        dest_params.get("ingest_external_file_one_in") == 0
        or (
            dest_params.get("ingest_external_file_width") is not None
            and dest_params["ingest_external_file_width"] < 2
        )
        or dest_params.get("compaction_style", 0) != 1
        or dest_params.get("user_timestamp_size", 0) > 0
        or dest_params.get("use_multiscan") == 1
    ):
        dest_params["ingest_external_file_atomic_replace_one_in"] = 0
    elif dest_params.get("ingest_external_file_atomic_replace_one_in", 0) > 0:
        dest_params["acquire_snapshot_one_in"] = 0
    # Embedded blobs in ingested files require ingestion to be enabled and
    # block-based table format_version >= 7.
    if (
        dest_params.get("ingest_external_file_one_in") == 0
        or dest_params.get("format_version", 2) < 7
    ):
        dest_params["ingest_external_file_with_embedded_blobs"] = 0
    if (
        dest_params.get("use_txn", 0) == 1
        and dest_params.get("commit_bypass_memtable_one_in", 0) > 0
    ):
        dest_params["enable_blob_files"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["use_put_entity_one_in"] = 0
        dest_params["use_get_entity"] = 0
        dest_params["use_multi_get_entity"] = 0
        dest_params["enable_pipelined_write"] = 0
        dest_params["use_attribute_group"] = 0
    if (
        dest_params.get("enable_pipelined_write", 0)
        or dest_params.get("unordered_write", 0)
        or dest_params.get("disable_wal", 0) == 0
        or dest_params.get("user_timestamp_size", 0)
    ):
        dest_params["ingest_wbwi_one_in"] = 0
    # Opening a read-only DB on the primary's directory needs a plain read-write
    # primary; it is not wired up for transactions, BlobDB, or TTL DBs.
    if (
        dest_params.get("use_txn", 0) == 1
        or dest_params.get("use_optimistic_txn", 0) == 1
        or dest_params.get("use_blob_db", 0) == 1
        or dest_params.get("ttl", -1) != -1
    ):
        dest_params["open_read_only_one_in"] = 0
    if dest_params.get("use_multiscan") == 1:
        dest_params["async_io"] = 0
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
        dest_params["prefix_size"] = -1
        dest_params["iterpercent"] += dest_params["prefixpercent"]
        dest_params["prefixpercent"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
        dest_params["max_sequential_skip_in_iterations"] = sys.maxsize
        dest_params["min_tombstones_for_range_conversion"] = 0
        # This option ingests a delete range that might partially overlap with
        # existing key range, which will cause a reseek that's currently not
        # supported by multiscan
        dest_params["test_ingest_standalone_range_deletion_one_in"] = 0
        dest_params["ingest_external_file_atomic_replace_one_in"] = 0
        # LevelIterator multiscan currently relies on num_entries and num_range_deletions,
        # which are not updated if skip_stats_update_on_db_open is true
        dest_params["skip_stats_update_on_db_open"] = 0
        dest_params["multiscan_max_prefetch_memory_bytes"] = random.choice(
            [0, 0, 64 * 1024, 256 * 1024]
        )

    # open_files_async requires skip_stats_update_on_db_open to avoid
    # synchronous I/O in UpdateAccumulatedStats during DB open
    if dest_params.get("skip_stats_update_on_db_open", 0) == 0:
        dest_params["open_files_async"] = 0

    # inplace update and key checksum verification during seek would cause race condition
    # Therefore, when inplace_update_support is enabled, disable memtable_verify_per_key_checksum_on_seek
    if dest_params["inplace_update_support"] == 1:
        dest_params["memtable_verify_per_key_checksum_on_seek"] = 0

    # allow_resumption requires remote compaction
    if dest_params.get("remote_compaction_worker_threads", 0) == 0:
        dest_params["allow_resumption_one_in"] = 0

    # When read-triggered compaction is enabled, use a short periodic trigger
    # interval so that the feature gets exercised on a quiet DB.
    if dest_params.get("read_triggered_compaction_threshold", 0) > 0:
        dest_params["max_compaction_trigger_wakeup_seconds"] = 20

    # Batch/snapshot stress relies on WAL-backed recovery semantics. Keep it
    # off for every finalized WAL-disabled profile, including blob direct
    # write presets that force disable_wal=1 earlier in sanitization.
    if dest_params.get("disable_wal", 0) == 1:
        dest_params["test_batches_snapshots"] = 0

    if dest_params.get("file_scope_wal_write_faults", 0) == 1:
        # Exercise the supported file-scoped WAL recovery envelope without
        # unrelated injected faults obscuring failures. The production path
        # deliberately fails closed for the options disabled here.
        dest_params["disable_wal"] = 0
        # IngestWriteBatchWithIndex only supports disableWAL=true; this profile
        # forces disable_wal=0 after the earlier ingest_wbwi_one_in guard has
        # already run, so re-disable it here to avoid an unsupported pairing.
        dest_params["ingest_wbwi_one_in"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        dest_params["use_txn"] = 0
        dest_params["txn_write_policy"] = 0
        dest_params["use_optimistic_txn"] = 0
        dest_params["test_multi_ops_txns"] = 0
        dest_params["test_batches_snapshots"] = 0
        dest_params["two_write_queues"] = 0
        dest_params["enable_pipelined_write"] = 0
        dest_params["unordered_write"] = 0
        dest_params["recycle_log_file_num"] = 0
        dest_params["WAL_ttl_seconds"] = 0
        dest_params["WAL_size_limit_MB"] = 0
        dest_params["track_and_verify_wals"] = 0
        dest_params["enable_blob_direct_write"] = 0
        dest_params["reopen"] = 0
        dest_params["exclude_wal_from_write_fault_injection"] = 0
        dest_params["inject_error_severity"] = 1
        if dest_params.get("write_fault_one_in", 0) <= 0:
            dest_params["write_fault_one_in"] = 128
        dest_params["max_write_buffer_number"] = max(
            dest_params["max_write_buffer_number"], 10
        )
        dest_params["sync_fault_injection"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["secondary_cache_fault_one_in"] = 0
        dest_params["open_metadata_read_fault_one_in"] = 0
        dest_params["open_metadata_write_fault_one_in"] = 0
        dest_params["open_read_fault_one_in"] = 0
        dest_params["open_write_fault_one_in"] = 0

    if dest_params.get("num_dbs", 1) > 1:
        # These features assume a single DB instance.
        # See ValidateNumDbsFlags() in db_stress_tool.cc for C++ guards.
        dest_params["clear_column_family_one_in"] = 0
        dest_params["test_multi_ops_txns"] = 0

    if dest_params.get("test_cf_lifecycle", 0):
        # LockColumnFamily() can acquire many expected-state key locks at once.
        # Bound that lock set so TSAN's deadlock detector can observe the CF
        # lifecycle path without exceeding its implementation-specific lock
        # limit. DB operations remain concurrent; this only coarsens the stress
        # harness's expected-state locking.
        max_key = max(1, dest_params["max_key"])
        log2_keys_per_lock = 0
        while max_key >> log2_keys_per_lock > 16:
            log2_keys_per_lock += 1
        dest_params["log2_keys_per_lock"] = log2_keys_per_lock

    # Occasionally shrink block, readahead and cache together so the
    # SeekToFirst()-after-Seek() iterator coverage can trim readahead across a
    # block boundary. Runs last so the tiered-cache split above sees the
    # original cache_size; clears use_write_buffer_manager so cache_size is the
    # block cache size.
    if (
        dest_params["prefix_size"] > 0
        and dest_params["auto_readahead_size"] == 1
        and random.choice([0, 0, 0, 1]) == 1
    ):
        dest_params["block_size"] = 1024
        dest_params["readahead_size"] = 4096
        dest_params["cache_size"] = 65536
        dest_params["use_write_buffer_manager"] = 0

    # Some sanitizers, such as user timestamps, can disable trie after the
    # UDI-specific rules above have run. Keep the final mode and per-read
    # selection valid without a custom factory.
    if dest_params.get("use_trie_index") != 1:
        if dest_params.get("index_mode", 1) in (2, 3, 4):
            dest_params["index_mode"] = 1
        if dest_params.get("read_index", -1) > 0:
            dest_params["read_index"] = -1

    # `db_stress` rejects either verification knob when `skip_verifydb` is set.
    # Must stay last: sanitization above can turn a verification knob back on
    # after `skip_verifydb` was decided.
    if dest_params.get("skip_verifydb", 0):
        dest_params["continuous_verification_interval"] = 0
        dest_params["verify_db_one_in"] = 0

    return dest_params


def gen_cmd_params(args, unknown_params=None):
    params = {}

    params.update(default_params)
    if args.test_type == "blackbox":
        params.update(blackbox_default_params)
    if args.test_type == "whitebox":
        params.update(whitebox_default_params)
    if args.test_type == "liveness":
        params.update(liveness_default_params)
    if args.simple:
        params.update(simple_default_params)
        if args.test_type == "blackbox":
            params.update(blackbox_simple_default_params)
        if args.test_type == "whitebox":
            params.update(whitebox_simple_default_params)
    if args.cf_consistency:
        params.update(cf_consistency_params)
    if args.cf_lifecycle:
        params.update(cf_lifecycle_params)
    if args.txn:
        params.update(txn_params)
    if args.optimistic_txn:
        params.update(optimistic_txn_params)
    if args.test_best_efforts_recovery:
        params.update(best_efforts_recovery_params)
    if args.enable_ts:
        params.update(ts_params)
    if args.test_multiops_txn:
        params.update(multiops_txn_params)
    if args.test_tiered_storage:
        params.update(tiered_params)
    if args.test_full_external_table:
        params.update(full_external_table_params)

    # These specialized profiles use fixed table configurations. Otherwise,
    # apply a blob feature override with a 10% chance.
    if (
        not args.test_best_efforts_recovery
        and not args.test_tiered_storage
        and not args.test_full_external_table
        and args.test_type != "liveness"
        and not args.cf_lifecycle
        and params.get("test_secondary", 0) == 0
        and random.choice([0] * 9 + [1]) == 1
    ):
        params.update(
            random.choice(
                [
                    blob_params,
                    blob_direct_write_params,
                    blob_direct_write_get_entity_params,
                    blob_direct_write_multi_get_entity_params,
                ]
            )
        )

    if "compaction_style" not in params:
        # Default to leveled compaction
        # TODO: Fix "Unsafe to store Seq later" with tiered+leveled and
        # enable that combination rather than falling back to universal.
        params["compaction_style"] = 0 if not args.test_tiered_storage else 1

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v

    # Reserve a small fraction of ordinary crash-test invocations for the
    # file-scoped WAL recovery state machine. Do this after merging command
    # line arguments so automatic selection cannot override an explicit opt-out
    # or an explicitly requested incompatible mode.
    explicit_incompatible_file_scope_flags = [
        "best_efforts_recovery",
        "disable_wal",
        "manual_wal_flush_one_in",
        "test_cf_consistency",
        "test_cf_lifecycle",
        "use_txn",
        "test_multi_ops_txns",
        "test_batches_snapshots",
        "two_write_queues",
        "enable_pipelined_write",
        "unordered_write",
        "recycle_log_file_num",
        "WAL_ttl_seconds",
        "WAL_size_limit_MB",
        "track_and_verify_wals",
        "enable_blob_direct_write",
        "reopen",
        "exclude_wal_from_write_fault_injection",
        "sync_fault_injection",
        "metadata_read_fault_one_in",
        "metadata_write_fault_one_in",
        "read_fault_one_in",
        "secondary_cache_fault_one_in",
        "open_metadata_read_fault_one_in",
        "open_metadata_write_fault_one_in",
        "open_read_fault_one_in",
        "open_write_fault_one_in",
    ]
    has_explicit_file_scope_conflict = any(
        getattr(args, flag, None) not in (None, 0)
        for flag in explicit_incompatible_file_scope_flags
    )
    # Unknown arguments are forwarded to db_stress after the generated flags,
    # so they can change the effective configuration without being visible in
    # `args`. Only auto-select this specialized profile when every option was
    # parsed and checked here. Explicit selection remains available.
    has_unparsed_file_scope_args = bool(unknown_params)
    if (
        getattr(args, "file_scope_wal_write_faults", None) is None
        and getattr(args, "write_fault_one_in", None) != 0
        and (
            getattr(args, "inject_error_severity", None) is None
            or getattr(args, "inject_error_severity") == 1
        )
        and not has_explicit_file_scope_conflict
        and not has_unparsed_file_scope_args
        and args.test_type != "liveness"
        and not args.cf_consistency
        and not args.cf_lifecycle
        and not args.txn
        and not args.optimistic_txn
        and not args.test_best_efforts_recovery
        and not args.enable_ts
        and not args.test_multiops_txn
        and not args.test_tiered_storage
        and params.get("test_batches_snapshots", 0) == 0
        and params.get("enable_blob_direct_write", 0) == 0
        and random.choice([0] * 19 + [1]) == 1
    ):
        params["file_scope_wal_write_faults"] = 1
    if args.test_type == "liveness":
        apply_liveness_remote_defaults(params, args)
        params.update(no_fault_injection_params)

    # DB paths must not change between the crash and recovery processes.
    wbm_flush_policy = params["wbm_flush_policy"]
    if callable(wbm_flush_policy):
        wbm_flush_policy = wbm_flush_policy()
        params["wbm_flush_policy"] = wbm_flush_policy
    use_write_buffer_manager = params["use_write_buffer_manager"]
    if (
        wbm_flush_policy == _WBM_FLUSH_LARGEST_ACROSS_DBS
        and (callable(use_write_buffer_manager) or use_write_buffer_manager)
    ):
        params["num_dbs"] = max(params.get("num_dbs", 1), 2)
    return params


def gen_cmd(params, unknown_params):
    finalzied_params = finalize_and_sanitize(params)
    num_dbs = finalzied_params.get("num_dbs", 1)

    # Python owns EV dir creation entirely (always local filesystem).
    # For num_dbs=1: --expected_values_dir is the EV path used as-is.
    # For num_dbs > 1: it is the parent dir; os.makedirs creates db_<i>
    # subdirs (and the parent implicitly).
    # C++ owns DB dir creation (see db_stress_tool.cc).
    ev_val = finalzied_params.get("expected_values_dir", "")
    if ev_val:
        if finalzied_params.get("destroy_db_initially", 0) and os.path.exists(ev_val):
            shutil.rmtree(ev_val, True)
        if num_dbs == 1:
            os.makedirs(ev_val, exist_ok=True)
        else:
            for i in range(num_dbs):
                os.makedirs(os.path.join(ev_val, "db_" + str(i)), exist_ok=True)
    unknown_params = list(unknown_params)
    lifecycle_mode_arg = (
        f"--test_cf_lifecycle={finalzied_params.get('test_cf_lifecycle', 0)}"
    )
    if "--" in unknown_params:
        unknown_params.insert(unknown_params.index("--"), lifecycle_mode_arg)
    else:
        unknown_params.append(lifecycle_mode_arg)
    cmd = (
        [stress_cmd]
        + [
            f"--{k}={v}"
            for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
            if k
            not in {
                "test_type",
                "simple",
                "duration",
                "interval",
                "random_kill_odd",
                "cf_consistency",
                "cf_lifecycle",
                "txn",
                "optimistic_txn",
                "test_best_efforts_recovery",
                "enable_ts",
                "test_multiops_txn",
                "stress_cmd",
                "test_tiered_storage",
                "test_full_external_table",
                "cleanup_cmd",
                "print_stderr_separately",
                "verify_timeout",
            }
            and v is not None
        ]
        + unknown_params
    )
    if finalzied_params.get("test_cf_lifecycle"):
        cmd.extend(
            f"--{name}={finalzied_params[name]}"
            for name in _CF_LIFECYCLE_AUTHORITATIVE_FLAGS
        )
    return cmd, finalzied_params


def human_readable_bytes(num_bytes):
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(num_bytes)
    unit_index = 0
    while value >= 1024.0 and unit_index + 1 < len(units):
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{num_bytes}{units[unit_index]}"
    return f"{value:.2f}{units[unit_index]}"


def message_matches_no_space(message):
    lowered = message.lower()
    return any(needle in lowered for needle in _NO_SPACE_SUBSTRINGS)


def output_matches_no_space(stdout, stderr):
    return message_matches_no_space("\n".join([stdout, stderr]))


def file_type_suffix(path):
    basename = os.path.basename(path)
    dot_pos = basename.find(".")
    if dot_pos <= 0:
        return "<no_ext>"
    return basename[dot_pos:]


def add_existing_directory(roots, seen, candidate):
    if not candidate:
        return
    normalized = os.path.normpath(candidate)
    if normalized in seen:
        return
    try:
        if not os.path.isdir(normalized):
            return
    except OSError:
        return
    roots.append(normalized)
    seen.add(normalized)


def collect_diagnostic_roots(base_paths, stdout, stderr):
    roots = []
    seen = set()
    for path in base_paths:
        add_existing_directory(roots, seen, path)

    for output in [stdout, stderr]:
        for match in _OUTPUT_PATH_RE.finditer(output):
            candidate = match.group(1).rstrip(",:;.)]}")
            if not candidate.startswith("/"):
                continue
            if os.path.isdir(candidate):
                add_existing_directory(roots, seen, candidate)
                continue
            parent = os.path.dirname(candidate)
            if parent:
                add_existing_directory(roots, seen, parent)

    pruned_roots = []
    for root in sorted(roots, key=lambda path: (len(path), path)):
        if any(
            root == existing or existing == os.sep or root.startswith(existing + os.sep)
            for existing in pruned_roots
        ):
            continue
        pruned_roots.append(root)
    return pruned_roots


def format_filesystem_usage(path):
    if not hasattr(os, "statvfs"):
        return f"  {path}: filesystem usage unavailable on this platform"

    try:
        stats = os.statvfs(path)
    except OSError as exc:
        return f"  {path}: failed to collect filesystem usage: {exc}"

    block_size = stats.f_frsize or stats.f_bsize
    total_bytes = stats.f_blocks * block_size
    available_bytes = stats.f_bavail * block_size
    used_bytes = max(total_bytes - available_bytes, 0)
    used_pct = 0.0 if total_bytes == 0 else 100.0 * used_bytes / total_bytes
    return (
        f"  {path}: total={human_readable_bytes(total_bytes)} "
        f"used={human_readable_bytes(used_bytes)} "
        f"avail={human_readable_bytes(available_bytes)} "
        f"use={used_pct:.1f}%"
    )


def new_directory_usage():
    return {
        "local_file_count": 0,
        "local_dir_count": 0,
        "local_bytes": 0,
        "subtree_file_count": 0,
        "subtree_bytes": 0,
        "local_suffixes": {},
    }


def collect_directory_usage(root):
    entries = []
    errors = []
    global_suffixes = {}

    def walk(dirpath):
        summary = new_directory_usage()
        try:
            with os.scandir(dirpath) as iterator:
                children = sorted(list(iterator), key=lambda entry: entry.name)
        except OSError as exc:
            errors.append((dirpath, f"failed to enumerate directory contents: {exc}"))
            return summary

        for child in children:
            try:
                if child.is_dir(follow_symlinks=False):
                    summary["local_dir_count"] += 1
                    child_summary = walk(child.path)
                    summary["subtree_file_count"] += child_summary["subtree_file_count"]
                    summary["subtree_bytes"] += child_summary["subtree_bytes"]
                    continue

                file_size = child.stat(follow_symlinks=False).st_size
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append((child.path, f"failed to stat child path: {exc}"))
                continue

            summary["local_file_count"] += 1
            summary["subtree_file_count"] += 1
            summary["local_bytes"] += file_size
            summary["subtree_bytes"] += file_size

            suffix = file_type_suffix(child.name)
            local_usage = summary["local_suffixes"].setdefault(
                suffix, {"count": 0, "bytes": 0}
            )
            local_usage["count"] += 1
            local_usage["bytes"] += file_size

            global_usage = global_suffixes.setdefault(suffix, {"count": 0, "bytes": 0})
            global_usage["count"] += 1
            global_usage["bytes"] += file_size

        entries.append((dirpath, summary))
        return summary

    root_summary = walk(root)
    return root_summary, entries, global_suffixes, errors


def sorted_suffix_usage(suffixes):
    return sorted(suffixes.items(), key=lambda item: (-item[1]["bytes"], item[0]))


def format_directory_usage(root):
    root_summary, entries, global_suffixes, errors = collect_directory_usage(root)
    lines = [
        "Directory usage for {}: subtree={} files={} descendant_dirs={}".format(
            root,
            human_readable_bytes(root_summary["subtree_bytes"]),
            root_summary["subtree_file_count"],
            max(len(entries) - 1, 0),
        )
    ]

    if not global_suffixes:
        lines.append(f"  No files found under {root}")
    else:
        lines.append("  Aggregate suffix totals:")
        for suffix, usage in sorted_suffix_usage(global_suffixes):
            lines.append(
                "    {} files={} bytes={}".format(
                    suffix,
                    usage["count"],
                    human_readable_bytes(usage["bytes"]),
                )
            )

        lines.append("  Per-directory suffix totals:")
        for dirpath, usage in sorted(
            entries,
            key=lambda item: (-item[1]["subtree_bytes"], item[0]),
        ):
            lines.append(
                "    {} subtree={} local={} local_files={} local_dirs={}".format(
                    dirpath,
                    human_readable_bytes(usage["subtree_bytes"]),
                    human_readable_bytes(usage["local_bytes"]),
                    usage["local_file_count"],
                    usage["local_dir_count"],
                )
            )
            for suffix, suffix_usage in sorted_suffix_usage(usage["local_suffixes"]):
                lines.append(
                    "      {} files={} bytes={}".format(
                        suffix,
                        suffix_usage["count"],
                        human_readable_bytes(suffix_usage["bytes"]),
                    )
                )

    if errors:
        lines.append("  Collection errors:")
        for path, error in errors:
            lines.append(f"    {path}: {error}")

    return lines


def build_out_of_space_diagnostics(
    stdout, stderr, diagnostic_paths=None, include_dev_shm=True
):
    if not output_matches_no_space(stdout, stderr):
        return ""

    roots = collect_diagnostic_roots(diagnostic_paths or [], stdout, stderr)
    lines = ["=== Out-of-space diagnostics ===", "Filesystem usage:"]
    if include_dev_shm and os.path.isdir("/dev/shm"):
        lines.append(format_filesystem_usage("/dev/shm"))
    for root in roots:
        lines.append(format_filesystem_usage(root))

    lines.append("Directory usage:")
    if not roots:
        lines.append("  no existing db_stress roots found")
    else:
        for root in roots:
            lines.extend(format_directory_usage(root))
    return "\n".join(lines) + "\n"


def diagnostic_paths(finalized_params):
    return [
        finalized_params.get("db"),
        finalized_params.get("expected_values_dir"),
        finalized_params.get("stress_diagnostics_dir"),
    ]


def execute_cmd(cmd, timeout=None, timeout_pstack=False, expected_to_timeout=True):
    child = subprocess.Popen(
        cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=stress_cmd_env()
    )
    print(
        "Running db_stress with pid=%d: %s\n\n"
        % (child.pid, " ".join(quote_arg_for_display(arg) for arg in cmd))
    )
    pid = child.pid

    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        if expected_to_timeout:
            print(
                "WARNING: db_stress ended before kill: exitcode=%d\n"
                % child.returncode
            )
        else:
            print("db_stress ended: exitcode=%d\n" % child.returncode)
    except subprocess.TimeoutExpired:
        hit_timeout = True
        if timeout_pstack:
            try:
                subprocess.call(["pstack", str(pid)], env=stress_cmd_env())
            except OSError as exc:
                print("WARNING: failed to run pstack for pid %d: %s\n" % (pid, exc))
        child.terminate()  # SIGTERM -- triggers TerminationHandler
        try:
            outs, errs = child.communicate(timeout=3)
            print("TERMINATED %d\n" % child.pid)
        except subprocess.TimeoutExpired:
            child.kill()
            print("KILLED %d (SIGTERM did not work)\n" % child.pid)
            outs, errs = child.communicate()

    prune_stress_diagnostics_dir(pid)
    return (
        hit_timeout,
        child.returncode,
        outs.decode("utf-8"),
        errs.decode("utf-8"),
        pid,
    )


def print_output_and_exit_on_error(
    stdout, stderr, print_stderr_separately=False, diagnostic_paths=None
):
    diagnostics = build_out_of_space_diagnostics(stdout, stderr, diagnostic_paths)
    if diagnostics:
        if stdout and not stdout.endswith("\n"):
            stdout += "\n"
        stdout += diagnostics

    print("stdout:\n", stdout)
    if len(stderr) == 0:
        return

    if print_stderr_separately:
        print("stderr:\n", stderr, file=sys.stderr)
    else:
        print("stderr:\n", stderr)

    sys.exit(2)


def print_run_output_and_exit_on_error(
    args, finalized_params, stdout, stderr, hit_timeout=False
):
    stdout, stderr = sanitize_known_stderr(
        stdout, stderr, hit_timeout, finalized_params
    )
    print_output_and_exit_on_error(
        stdout,
        stderr,
        args.print_stderr_separately,
        diagnostic_paths(finalized_params),
    )


def sanitize_known_stderr(stdout, stderr, hit_timeout, finalized_params):
    # Blackbox crash tests intentionally terminate db_stress with SIGTERM.
    ignore_sigterm_stderr = hit_timeout and _SIGTERM_STDOUT_MARKER in stdout
    # RocksDB falls back to synchronous reads if Folly cannot initialize io_uring.
    ignore_io_uring_init_stderr = finalized_params.get("use_async_db_api") == 1
    # cachelibwrapper construction and reset can emit normal CacheLib INFO to
    # stderr when its XLOG header is not suppressed. Filter only those INFO
    # sources so the harness can accept an intentional whitebox SIGTERM without
    # hiding warnings, errors, or unrelated output.
    secondary_cache_uri = finalized_params.get("secondary_cache_uri") or ""
    ignore_cachelib_info_stderr = secondary_cache_uri.startswith(
        "cachelibwrapper://"
    )
    if len(stderr) == 0 or not (
        ignore_sigterm_stderr
        or ignore_io_uring_init_stderr
        or ignore_cachelib_info_stderr
    ):
        return stdout, stderr

    kept_lines = []
    ignored_lines = []
    for line in stderr.splitlines(keepends=True):
        stripped_line = line.rstrip("\n")
        if (
            ignore_sigterm_stderr
            and _IGNORED_SIGTERM_STDERR_RE.fullmatch(stripped_line)
        ) or (
            ignore_io_uring_init_stderr
            and _IGNORED_IO_URING_INIT_STDERR_RE.fullmatch(stripped_line)
        ) or (
            ignore_cachelib_info_stderr
            and _IGNORED_CACHELIB_INFO_STDERR_RE.fullmatch(stripped_line)
        ):
            ignored_lines.append(line)
        else:
            kept_lines.append(line)

    if len(ignored_lines) == 0:
        return stdout, stderr

    if stdout and not stdout.endswith("\n"):
        stdout += "\n"
    stdout += "Ignored known stderr:\n"
    stdout += "".join(ignored_lines)

    stderr = "".join(kept_lines)
    if not stderr.strip():
        stderr = ""
    return stdout, stderr


def cleanup_after_success(db_arg, num_dbs=1):
    # Delegates to db_stress --destroy_db_and_exit so cleanup uses the same
    # Env (local or remote) that created the DB.
    # `db_arg` is the --db value (DB path for num_dbs=1, parent dir for num_dbs>1).
    cleanup_cmd_parts = [
        stress_cmd,
        "--destroy_db_and_exit=1",
        "--db=" + db_arg,
        "--num_dbs=" + str(num_dbs),
    ]
    # Pass through relevant arguments for remote DB access
    for arg in remain_args:
        if parse_remote_db_uri_arg(arg) is not None:
            cleanup_cmd_parts.append(arg)
    print("Running DB cleanup command - %s\n" % " ".join(cleanup_cmd_parts))
    ret = subprocess.call(cleanup_cmd_parts, env=stress_cmd_env())
    if ret != 0:
        print("ERROR: DB cleanup returned error %d\n" % ret)
        sys.exit(2)


def _fault_injection_log_dir():
    if is_remote_db:
        base_dir = "/tmp"
    else:
        base_dir = os.environ.get(_TEST_DIR_ENV_VAR) or "/tmp"
    return os.path.join(
        base_dir,
        _FAULT_INJECTION_LOG_DIR_NAME,
    )


def print_fault_injection_log(pid):
    # Fault injection logs are stored in TEST_TMPDIR (or /tmp) to survive
    # DB reopen cleanup, and to be included in crash-test artifacts.
    # Filter by pid to only print the log from the current run. Raw and decoded
    # logs are intentionally left behind for external artifact collection and
    # cleanup.
    log_dir = _fault_injection_log_dir()

    raw_pattern = os.path.join(log_dir, "fault_injection_%d_*.bin" % pid)
    for raw_log in sorted(glob.glob(raw_pattern)):
        decoded_log = raw_log + ".txt"
        try:
            entry_count = fault_injection_log_parser.decode_fault_injection_log(
                raw_log, decoded_log
            )
            print(
                "Fault injection log saved: raw=%s decoded=%s entries=%d"
                % (raw_log, decoded_log, entry_count)
            )
        except (OSError, ValueError) as exc:
            print(
                "WARNING: failed to decode fault injection log %s: %s\n"
                % (raw_log, exc)
            )


def liveness_timeout(cmd_params):
    duration = cmd_params.get("duration", 0)
    if duration is None or duration <= 0:
        return liveness_default_timeout_sec()
    return duration


def liveness_main(args, unknown_args):
    cmd_params = gen_cmd_params(args, unknown_args)
    db_parent_dir = get_db_parent_dir("liveness")
    num_dbs = cmd_params.get("num_dbs", 1)

    if not cmd_params.get("db"):
        cmd_params["db"] = db_parent_dir
    if is_remote_db and not cmd_params.get("expected_values_dir"):
        cmd_params["expected_values_dir"] = get_ev_parent_dir()
    set_default_stress_diagnostics_dir(cmd_params)

    apply_random_seed_per_iteration()
    wrapper_timeout = liveness_timeout(cmd_params)

    print(
        "Running liveness-test with \n"
        + "liveness_check_interval_sec="
        + str(cmd_params["liveness_check_interval_sec"])
        + "\n"
        + "liveness_no_progress_timeout_sec="
        + str(cmd_params["liveness_no_progress_timeout_sec"])
        + "\n"
        + "wrapper-duration="
        + str(wrapper_timeout)
        + "\n"
    )

    cmd, finalized_params = gen_cmd(dict(list(cmd_params.items())), unknown_args)
    hit_timeout, retcode, outs, errs, pid = execute_cmd(
        cmd, wrapper_timeout, False, False
    )

    print_fault_injection_log(pid)
    print_run_output_and_exit_on_error(
        args, finalized_params, outs, errs, hit_timeout
    )

    if hit_timeout:
        if retcode == -9:
            print("TEST FAILED. db_stress ignored SIGTERM on wrapper timeout\n")
            sys.exit(1)
        print("db_stress reached liveness wrapper duration\n")
        cleanup_after_success(cmd_params["db"], num_dbs)
        return
    if retcode != 0:
        print("TEST FAILED. db_stress exited with code %d\n" % retcode)
        sys.exit(1)

    cleanup_after_success(cmd_params["db"], num_dbs)


def cf_lifecycle_iteration_overrides(iteration):
    fault_target, tracking, clear_one_in = _CF_LIFECYCLE_SCHEDULE[
        iteration % len(_CF_LIFECYCLE_SCHEDULE)
    ]
    return {
        "cf_lifecycle_fault_target": fault_target,
        "track_options_file_number_in_manifest": tracking,
        "clear_column_family_one_in": clear_one_in,
    }


def expected_cf_lifecycle_fault_observed(params, hit_timeout, retcode, stdout):
    marker = _CF_LIFECYCLE_FAULT_MARKERS.get(
        params.get("cf_lifecycle_fault_target", 0)
    )
    return (
        marker is not None
        and not hit_timeout
        and retcode == _EXPECTED_CF_LIFECYCLE_FAULT_EXIT_CODE
        and marker in stdout.splitlines()
    )


def count_cf_lifecycle_successes(stdout):
    return sum(line == _CF_LIFECYCLE_SUCCESS_MARKER for line in stdout.splitlines())


def missing_cf_lifecycle_coverage(coverage):
    return [
        name
        for name, minimum in _CF_LIFECYCLE_MIN_COVERAGE.items()
        if coverage.get(name, 0) < minimum
    ]


def cf_lifecycle_final_verification_failed(args, hit_timeout, retcode):
    return args.cf_lifecycle and (hit_timeout or retcode != 0)


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args, unknown_args)
    db_parent_dir = get_db_parent_dir("blackbox")
    num_dbs = cmd_params.get("num_dbs", 1)

    if not cmd_params.get("db"):
        cmd_params["db"] = db_parent_dir
    if not args.cf_lifecycle and not cmd_params.get("expected_values_dir"):
        cmd_params["expected_values_dir"] = get_ev_parent_dir()
    set_default_stress_diagnostics_dir(cmd_params)

    exit_time = time.time() + cmd_params["duration"]

    print(
        "Running blackbox-crash-test with \n"
        + "interval_between_crash="
        + str(cmd_params["interval"])
        + "\n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    cf_lifecycle_iteration = 0
    cf_lifecycle_coverage = {"options": 0, "manifest": 0, "success": 0}

    while time.time() < exit_time:
        apply_random_seed_per_iteration()
        additional_opts = {}
        if args.cf_lifecycle:
            additional_opts = cf_lifecycle_iteration_overrides(
                cf_lifecycle_iteration
            )
        cmd, finalized_params = gen_cmd(
            dict(list(cmd_params.items()) + list(additional_opts.items())),
            unknown_args,
        )

        hit_timeout, retcode, outs, errs, pid = execute_cmd(cmd, cmd_params["interval"])

        print_fault_injection_log(pid)
        # Reset destroy_db_initially after each run (it may have been set by
        # command line for first run only)
        cmd_params["destroy_db_initially"] = 0

        fault_target = finalized_params.get("cf_lifecycle_fault_target", 0)
        if args.cf_lifecycle and fault_target != 0:
            print_run_output_and_exit_on_error(
                args, finalized_params, outs, errs, hit_timeout
            )
            if not expected_cf_lifecycle_fault_observed(
                finalized_params, hit_timeout, retcode, outs
            ):
                print(
                    "TEST FAILED. Targeted CF lifecycle fault was not "
                    "observed as expected\n"
                )
                sys.exit(2)
            target_name = "options" if fault_target == 1 else "manifest"
            cf_lifecycle_coverage[target_name] += 1
            cf_lifecycle_iteration += 1
            time.sleep(1)
            continue

        if not hit_timeout:
            print("Exit Before Killing")
            print_run_output_and_exit_on_error(
                args, finalized_params, outs, errs, hit_timeout
            )
            sys.exit(2)

        print_run_output_and_exit_on_error(
            args, finalized_params, outs, errs, hit_timeout
        )

        if args.cf_lifecycle:
            cf_lifecycle_coverage["success"] += count_cf_lifecycle_successes(outs)
        cf_lifecycle_iteration += 1

        time.sleep(1)  # time to stabilize before the next run

    if args.cf_lifecycle:
        missing_coverage = missing_cf_lifecycle_coverage(cf_lifecycle_coverage)
        if missing_coverage:
            print(
                "TEST FAILED. Missing CF lifecycle coverage: "
                + ", ".join(missing_coverage)
                + "\n"
            )
            sys.exit(2)
        print("CF lifecycle coverage: %s\n" % cf_lifecycle_coverage)

    # Run one final verification process. Lifecycle mode requires it to finish
    # before the wrapper timeout; ordinary profiles keep legacy timeout handling.
    print("Running final time for verification")
    cmd_params.update({"verification_only": 1})
    cmd_params.update({"skip_verifydb": 0})
    if args.cf_lifecycle:
        cmd_params.update(
            {
                "cf_lifecycle_fault_target": 0,
                "track_options_file_number_in_manifest": 1,
                "clear_column_family_one_in": 0,
                "skip_verifydb": 1,
            }
        )

    cmd, finalized_params = gen_cmd(dict(list(cmd_params.items())), unknown_args)
    hit_timeout, retcode, outs, errs, pid = execute_cmd(
        cmd, cmd_params["verify_timeout"], True
    )

    print_fault_injection_log(pid)

    # For the final run
    print_run_output_and_exit_on_error(
        args, finalized_params, outs, errs, hit_timeout
    )
    if cf_lifecycle_final_verification_failed(args, hit_timeout, retcode):
        print(
            "TEST FAILED. Final verification did not complete successfully: "
            "hit_timeout=%s retcode=%d\n" % (hit_timeout, retcode)
        )
        sys.exit(2)

    # we need to clean up after ourselves -- only do this on test success
    cleanup_after_success(cmd_params["db"], num_dbs)


# This python script runs db_stress multiple times. Some runs with
# kill_random_test that causes rocksdb to crash at various points in code.
def whitebox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args, unknown_args)
    db_parent_dir = get_db_parent_dir("whitebox")
    ev_parent_dir = get_ev_parent_dir()
    num_dbs = cmd_params.get("num_dbs", 1)

    if not cmd_params.get("db"):
        cmd_params["db"] = db_parent_dir
    if not cmd_params.get("expected_values_dir"):
        cmd_params["expected_values_dir"] = ev_parent_dir
    set_default_stress_diagnostics_dir(cmd_params)

    cur_time = time.time()
    exit_time = cur_time + cmd_params["duration"]
    half_time = cur_time + cmd_params["duration"] // 2

    print(
        "Running whitebox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    total_check_mode = 4
    check_mode = 0
    kill_random_test = cmd_params["random_kill_odd"]
    kill_mode = 0
    prev_compaction_style = -1
    succeeded = True
    hit_timeout = False
    while time.time() < exit_time:
        apply_random_seed_per_iteration()
        if check_mode == 0:
            additional_opts = {
                # use large ops per thread since we will kill it anyway
                "ops_per_thread": 100
                * cmd_params["ops_per_thread"],
            }
            # run with kill_random_test, with three modes.
            # Mode 0 covers all kill points. Mode 1 covers less kill points but
            # increases change of triggering them. Mode 2 covers even less
            # frequent kill points and further increases triggering change.
            if kill_mode == 0:
                additional_opts.update(
                    {
                        "kill_random_test": kill_random_test,
                    }
                )
            elif kill_mode == 1:
                if cmd_params.get("disable_wal", 0) == 1:
                    my_kill_odd = kill_random_test // 50 + 1
                else:
                    my_kill_odd = kill_random_test // 10 + 1
                additional_opts.update(
                    {
                        "kill_random_test": my_kill_odd,
                        "kill_exclude_prefixes": "WritableFileWriter::Append,"
                        + "WritableFileWriter::WriteBuffered",
                    }
                )
            elif kill_mode == 2:
                # TODO: May need to adjust random odds if kill_random_test
                # is too small.
                additional_opts.update(
                    {
                        "kill_random_test": (kill_random_test // 5000 + 1),
                        "kill_exclude_prefixes": "WritableFileWriter::Append,"
                        "WritableFileWriter::WriteBuffered,"
                        "PosixMmapFile::Allocate,WritableFileWriter::Flush",
                    }
                )
            # Run kill mode 0, 1 and 2 by turn.
            kill_mode = (kill_mode + 1) % 3
        elif check_mode == 1:
            # normal run with universal compaction mode
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"],
                "compaction_style": 1,
            }
            # Single level universal has a lot of special logic. Ensure we cover
            # it sometimes.
            if not args.test_tiered_storage and random.randint(0, 1) == 1:
                additional_opts["num_levels"] = 1
        elif check_mode == 2:
            # normal run with FIFO compaction mode
            # ops_per_thread is divided by 5 because FIFO compaction
            # style is quite a bit slower on reads with lot of files
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"] // 5,
                "compaction_style": 2,
            }
            # TODO: test transition from non-FIFO to FIFO with num_levels > 1.
            # See https://github.com/meta/rocksdb/pull/10348
            # For now, tiered storage FIFO (file_temperature_age_thresholds)
            # requires num_levels == 1 and non-tiered operates that way.
            if args.test_tiered_storage:
                additional_opts["num_levels"] = 1
        else:
            # normal run
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"],
            }

        cur_compaction_style = additional_opts.get(
            "compaction_style", cmd_params.get("compaction_style", 0)
        )
        if (
            prev_compaction_style != -1
            and prev_compaction_style != cur_compaction_style
        ):
            print(
                "`compaction_style` is changed in current run so `destroy_db_initially` is set to 1 as a short-term solution to avoid cycling through previous db of different compaction style."
                + "\n"
            )
            cmd_params["destroy_db_initially"] = 1
        prev_compaction_style = cur_compaction_style

        cmd, finalized_params = gen_cmd(
            dict(list(cmd_params.items()) + list(additional_opts.items())),
            unknown_args,
        )

        print(
            "Running:" + " ".join(cmd) + "\n"
        )  # noqa: E999 T25377293 Grandfathered in

        # If the running time is 15 minutes over the run time, explicit kill and
        # exit even if white box kill didn't hit. This is to guarantee run time
        # limit, as if it runs as a job, running too long will create problems
        # for job scheduling or execution.
        # TODO detect a hanging condition. The job might run too long as RocksDB
        # hits a hanging bug.
        hit_timeout, retncode, stdoutdata, stderrdata, pid = execute_cmd(
            cmd, exit_time - time.time() + 900
        )
        print_fault_injection_log(pid)

        # Reset destroy_db_initially after each run (it may have been set by
        # command line for first run, or set for various reasons for a step)
        cmd_params["destroy_db_initially"] = 0

        msg = "check_mode={}, kill option={}, exitcode={}\n".format(
            check_mode, additional_opts["kill_random_test"], retncode
        )

        print(msg)
        print_run_output_and_exit_on_error(
            args, finalized_params, stdoutdata, stderrdata
        )

        if hit_timeout:
            print("Killing the run for running too long")
            break

        succeeded = False
        if additional_opts["kill_random_test"] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            succeeded = True
        elif additional_opts["kill_random_test"] is not None and retncode <= 0:
            # When kill option is given, the test MIGHT kill itself.
            # If it does, negative retncode is expected. Otherwise 0.
            succeeded = True

        if not succeeded:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # Set next iteration to destroy DB (works for remote DB)
            cmd_params["destroy_db_initially"] = 1
            check_mode = (check_mode + 1) % total_check_mode

        time.sleep(1)  # time to stabilize after a kill

    # If successfully finished or timed out (we currently treat timed out test as passing)
    # Clean up after ourselves
    if succeeded or hit_timeout:
        cleanup_after_success(cmd_params["db"], num_dbs)


def main():
    global stress_cmd

    parser = argparse.ArgumentParser(
        description="This script runs and kills \
        db_stress multiple times"
    )
    parser.add_argument("test_type", choices=["blackbox", "whitebox", "liveness"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action="store_true")
    parser.add_argument("--cf_lifecycle", action="store_true")
    parser.add_argument("--txn", action="store_true")
    parser.add_argument("--optimistic_txn", action="store_true")
    parser.add_argument("--test_best_efforts_recovery", action="store_true")
    parser.add_argument("--enable_ts", action="store_true")
    parser.add_argument("--test_multiops_txn", action="store_true")
    parser.add_argument("--stress_cmd")
    parser.add_argument("--test_tiered_storage", action="store_true")
    parser.add_argument("--test_full_external_table", action="store_true")
    parser.add_argument("--cleanup_cmd")  # ignore old option for now
    parser.add_argument("--print_stderr_separately", action="store_true", default=False)
    parser.add_argument(
        "--cache_and_write_buffer_size_multiplier",
        type=float,
        help="Scale each iteration's cache_size and write_buffer_size",
    )

    all_params = dict(
        list(default_params.items())
        + list(blackbox_default_params.items())
        + list(whitebox_default_params.items())
        + list(liveness_default_params.items())
        + list(simple_default_params.items())
        + list(blackbox_simple_default_params.items())
        + list(whitebox_simple_default_params.items())
        + list(blob_params.items())
        + list(blob_direct_write_params.items())
        + list(blob_direct_write_get_entity_params.items())
        + list(blob_direct_write_multi_get_entity_params.items())
        + list(ts_params.items())
        + list(multiops_txn_params.items())
        + list(best_efforts_recovery_params.items())
        + list(cf_consistency_params.items())
        + list(cf_lifecycle_params.items())
        + list(tiered_params.items())
        + list(full_external_table_params.items())
        + list(txn_params.items())
        + list(optimistic_txn_params.items())
    )

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress

    args, unknown_args = parser.parse_known_args(remain_args)
    if (
        args.test_cf_lifecycle is not None
        or args.cf_lifecycle_fault_target is not None
    ):
        parser.error(
            "--test_cf_lifecycle and --cf_lifecycle_fault_target are managed "
            "by --cf_lifecycle"
        )
    if args.cf_lifecycle and args.test_type != "blackbox":
        parser.error("--cf_lifecycle currently requires blackbox mode")
    if args.cf_lifecycle and args.cf_consistency:
        parser.error("--cf_lifecycle cannot be combined with --cf_consistency")
    if args.cf_lifecycle and "--" in unknown_args:
        parser.error("--cf_lifecycle does not support the -- argument terminator")
    multiplier = args.cache_and_write_buffer_size_multiplier
    if multiplier is not None and (not math.isfinite(multiplier) or multiplier <= 0):
        parser.error(
            "--cache_and_write_buffer_size_multiplier must be finite and greater than zero"
        )
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not is_remote_db:
        isdir = False
        try:
            isdir = os.path.isdir(test_tmpdir)
            if not isdir:
                print(
                    "ERROR: %s env var is set to a non-existent directory: %s. Update it to correct directory path."
                    % (_TEST_DIR_ENV_VAR, test_tmpdir)
                )
                sys.exit(1)
        except OSError:
            pass

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.test_type == "blackbox":
        blackbox_crash_main(args, unknown_args)
    if args.test_type == "whitebox":
        whitebox_crash_main(args, unknown_args)
    if args.test_type == "liveness":
        liveness_main(args, unknown_args)
    # Delete the expected values base dir if test passes.
    # Per-DB subdirectories (db_0, db_1, ...) live under the base,
    # so rmtree of the parent cleans everything.
    if ev_parent_dir_global is not None:
        if os.path.exists(ev_parent_dir_global):
            shutil.rmtree(ev_parent_dir_global)
    if multiops_txn_key_spaces_file is not None:
        os.remove(multiops_txn_key_spaces_file)
    cleanup_stress_diagnostics_dir()


if __name__ == "__main__":
    main()
