#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

import importlib.util
import io
import os
import shutil
import struct
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest import mock


_DB_CRASHTEST_PATH = os.path.join(os.path.dirname(__file__), "db_crashtest.py")
_FAULT_INJECTION_LOG_PARSER_PATH = os.path.join(
    os.path.dirname(__file__), "fault_injection_log_parser.py"
)
_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"
_TSAN_OPTIONS_ENV_VAR = "TSAN_OPTIONS"


def load_db_crashtest_module(args=None):
    spec = importlib.util.spec_from_file_location(
        "db_crashtest_under_test", _DB_CRASHTEST_PATH
    )
    module = importlib.util.module_from_spec(spec)
    old_argv = sys.argv[:]
    try:
        sys.argv = [_DB_CRASHTEST_PATH] + list(args or [])
        spec.loader.exec_module(module)
    finally:
        sys.argv = old_argv
    return module


def load_fault_injection_log_parser_module():
    spec = importlib.util.spec_from_file_location(
        "fault_injection_log_parser_under_test", _FAULT_INJECTION_LOG_PARSER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DBCrashTestTest(unittest.TestCase):
    def setUp(self):
        self.test_tmpdir = tempfile.mkdtemp(prefix="db_crashtest_test_")
        self.expected_dir = os.path.join(
            self.test_tmpdir, "rocksdb_crashtest_expected"
        )
        self.old_test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
        self.old_test_expected_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)
        self.old_tsan_options = os.environ.get(_TSAN_OPTIONS_ENV_VAR)
        os.environ[_TEST_DIR_ENV_VAR] = self.test_tmpdir
        os.environ.pop(_TEST_EXPECTED_DIR_ENV_VAR, None)

    def tearDown(self):
        if self.old_test_tmpdir is None:
            os.environ.pop(_TEST_DIR_ENV_VAR, None)
        else:
            os.environ[_TEST_DIR_ENV_VAR] = self.old_test_tmpdir

        if self.old_test_expected_tmpdir is None:
            os.environ.pop(_TEST_EXPECTED_DIR_ENV_VAR, None)
        else:
            os.environ[_TEST_EXPECTED_DIR_ENV_VAR] = self.old_test_expected_tmpdir

        if self.old_tsan_options is None:
            os.environ.pop(_TSAN_OPTIONS_ENV_VAR, None)
        else:
            os.environ[_TSAN_OPTIONS_ENV_VAR] = self.old_tsan_options

        shutil.rmtree(self.test_tmpdir)

    def load_db_crashtest(self, args=None):
        return load_db_crashtest_module(args)

    def register_expected_values_dir_cleanup(self, db_crashtest):
        def cleanup_expected_values_dir():
            if db_crashtest.ev_parent_dir_global:
                shutil.rmtree(
                    db_crashtest.ev_parent_dir_global,
                    ignore_errors=True,
                )

        self.addCleanup(cleanup_expected_values_dir)

    def register_diagnostics_dir_cleanup(self, db_crashtest):
        def cleanup_diagnostics_dir():
            if db_crashtest.diagnostics_dir_global:
                shutil.rmtree(
                    db_crashtest.diagnostics_dir_global,
                    ignore_errors=True,
                )

        self.addCleanup(cleanup_diagnostics_dir)

    def load_fault_injection_log_parser(self):
        return load_fault_injection_log_parser_module()

    def build_params(self, base_params, overrides=None):
        params = dict(base_params)
        params["db"] = self.test_tmpdir
        params["wbm_flush_policy"] = 0
        if overrides:
            params.update(overrides)
        return params

    def build_mode_args(self, test_type="liveness", **overrides):
        args = {
            "test_type": test_type,
            "simple": False,
            "cf_consistency": False,
            "cf_lifecycle": False,
            "txn": False,
            "optimistic_txn": False,
            "test_best_efforts_recovery": False,
            "enable_ts": False,
            "test_multiops_txn": False,
            "test_tiered_storage": False,
            "test_full_external_table": False,
            "print_stderr_separately": False,
        }
        args.update(overrides)
        return SimpleNamespace(**args)

    def test_stress_cmd_env_defaults_tsan_suppressions(self):
        os.environ.pop(_TSAN_OPTIONS_ENV_VAR, None)
        db_crashtest = self.load_db_crashtest()

        env = db_crashtest.stress_cmd_env()

        self.assertEqual(
            "suppressions="
            + os.path.abspath(
                os.path.join(os.path.dirname(__file__), "tsan_suppressions.txt")
            ),
            env[_TSAN_OPTIONS_ENV_VAR],
        )

    def test_stress_cmd_env_preserves_tsan_options(self):
        os.environ[_TSAN_OPTIONS_ENV_VAR] = "halt_on_error=1"
        db_crashtest = self.load_db_crashtest()

        env = db_crashtest.stress_cmd_env()

        self.assertEqual("halt_on_error=1", env[_TSAN_OPTIONS_ENV_VAR])

    def test_generated_command_uses_async_db_api(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {"use_async_db_api": 1},
        )

        command, _ = db_crashtest.gen_cmd(params, [])

        self.assertIn("--use_async_db_api=1", command)
        self.assertFalse(
            any(arg.startswith("--use_coro_db_api=") for arg in command)
        )

    def test_stress_diagnostics_directory_creation_failure_is_non_fatal(self):
        db_crashtest = self.load_db_crashtest()
        diagnostics_path = os.path.join(
            self.test_tmpdir, "rocksdb_crashtest_diagnostics"
        )
        with open(diagnostics_path, "w", encoding="utf-8"):
            pass
        params = {
            "stress_diagnostics_breadcrumbs": 1,
            "stress_diagnostics_dir": "",
        }
        output = io.StringIO()

        with redirect_stdout(output):
            db_crashtest.set_default_stress_diagnostics_dir(params)

        self.assertEqual("", params["stress_diagnostics_dir"])
        self.assertIsNone(db_crashtest.diagnostics_dir_global)
        self.assertIn(
            "Failed to create stress diagnostics directory", output.getvalue()
        )

    def test_stress_diagnostics_cleanup_failure_is_non_fatal(self):
        db_crashtest = self.load_db_crashtest()
        db_crashtest.get_diagnostics_dir()
        output = io.StringIO()

        with mock.patch.object(
            db_crashtest.os,
            "rmdir",
            side_effect=OSError("injected cleanup failure"),
        ):
            with redirect_stdout(output):
                db_crashtest.cleanup_stress_diagnostics_dir()

        self.assertIsNone(db_crashtest.diagnostics_dir_global)
        self.assertIn(
            "Failed to clean up stress diagnostics directory", output.getvalue()
        )

    def test_stress_diagnostics_cleanup_preserves_unrelated_files(self):
        db_crashtest = self.load_db_crashtest()
        diagnostics_dir = db_crashtest.get_diagnostics_dir()
        unrelated_path = os.path.join(diagnostics_dir, "keep-me.txt")
        whitespace_path = os.path.join(
            diagnostics_dir, "keep me.pid_1.thread_0.breadcrumbs.txt"
        )
        unicode_path = os.path.join(
            diagnostics_dir, "keep_me_\N{SNOWMAN}.pid_1.thread_0.breadcrumbs.txt"
        )
        diagnostic_path = os.path.join(
            diagnostics_dir, "db.pid_1.thread_0.breadcrumbs.txt"
        )
        witness_path = os.path.join(
            diagnostics_dir,
            "db.pid_1.thread_0.prefix_scan_batched.witness.txt",
        )
        witness_temp_path = witness_path + ".tmp"
        for path in (unrelated_path, whitespace_path, unicode_path):
            with open(path, "w", encoding="utf-8"):
                pass
        for path in (diagnostic_path, witness_path, witness_temp_path):
            with open(path, "w", encoding="utf-8"):
                pass

        db_crashtest.cleanup_stress_diagnostics_dir()

        self.assertTrue(os.path.exists(unrelated_path))
        self.assertTrue(os.path.exists(whitespace_path))
        self.assertTrue(os.path.exists(unicode_path))
        self.assertFalse(os.path.exists(diagnostic_path))
        self.assertFalse(os.path.exists(witness_path))
        self.assertFalse(os.path.exists(witness_temp_path))
        self.assertIsNone(db_crashtest.diagnostics_dir_global)

    def test_stress_diagnostics_rejects_symlinked_auto_directory(self):
        db_crashtest = self.load_db_crashtest()
        diagnostics_path = os.path.join(
            self.test_tmpdir, "rocksdb_crashtest_diagnostics"
        )
        target_dir = os.path.join(self.test_tmpdir, "unrelated")
        os.mkdir(target_dir)
        target_file = os.path.join(
            target_dir, "db.pid_1.thread_0.breadcrumbs.txt"
        )
        with open(target_file, "w", encoding="utf-8"):
            pass
        os.symlink(target_dir, diagnostics_path)
        params = {
            "stress_diagnostics_breadcrumbs": 1,
            "stress_diagnostics_dir": "",
        }

        db_crashtest.set_default_stress_diagnostics_dir(params)
        db_crashtest.prune_stress_diagnostics_dir(current_pid=1)
        db_crashtest.cleanup_stress_diagnostics_dir()

        self.assertEqual("", params["stress_diagnostics_dir"])
        self.assertTrue(os.path.exists(target_file))
        self.assertIsNone(db_crashtest.diagnostics_dir_global)

    def test_stress_diagnostics_pruning_keeps_recent_processes(self):
        db_crashtest = self.load_db_crashtest()
        diagnostics_dir = db_crashtest.get_diagnostics_dir()
        self.register_diagnostics_dir_cleanup(db_crashtest)
        unrelated_path = os.path.join(diagnostics_dir, "keep-me.txt")
        misleading_path = os.path.join(diagnostics_dir, "notes.pid_1.txt")
        with open(unrelated_path, "w", encoding="utf-8"):
            pass
        with open(misleading_path, "w", encoding="utf-8"):
            pass

        for pid in range(10):
            path = os.path.join(
                diagnostics_dir, f"db.pid_{pid}.thread_0.breadcrumbs.txt"
            )
            with open(path, "w", encoding="utf-8"):
                pass
            os.utime(path, ns=(pid + 1, pid + 1))

        db_crashtest.prune_stress_diagnostics_dir(current_pid=9)

        self.assertTrue(os.path.exists(unrelated_path))
        self.assertTrue(os.path.exists(misleading_path))
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    diagnostics_dir, "db.pid_0.thread_0.breadcrumbs.txt"
                )
            )
        )
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    diagnostics_dir, "db.pid_1.thread_0.breadcrumbs.txt"
                )
            )
        )
        for pid in range(2, 10):
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        diagnostics_dir, f"db.pid_{pid}.thread_0.breadcrumbs.txt"
                    )
                )
            )

    def test_stress_diagnostics_pruning_ignores_explicit_directory(self):
        db_crashtest = self.load_db_crashtest()
        explicit_dir = os.path.join(self.test_tmpdir, "explicit_diagnostics")
        os.mkdir(explicit_dir)
        path = os.path.join(explicit_dir, "db.pid_1.thread_0.breadcrumbs.txt")
        with open(path, "w", encoding="utf-8"):
            pass

        db_crashtest.prune_stress_diagnostics_dir(current_pid=1)

        self.assertTrue(os.path.exists(path))

    def test_stress_diagnostics_defaults_to_local_artifact_dir(self):
        db_crashtest = self.load_db_crashtest()
        self.register_diagnostics_dir_cleanup(db_crashtest)
        params = db_crashtest.gen_cmd_params(self.build_mode_args("blackbox"))
        params["db"] = self.test_tmpdir

        db_crashtest.set_default_stress_diagnostics_dir(params)
        command, finalized = db_crashtest.gen_cmd(params, [])

        self.assertEqual(1, finalized["stress_diagnostics_breadcrumbs"])
        self.assertEqual(1, finalized["stress_diagnostics_scan_witness"])
        self.assertEqual(
            os.path.join(self.test_tmpdir, "rocksdb_crashtest_diagnostics"),
            finalized["stress_diagnostics_dir"],
        )
        self.assertTrue(os.path.isdir(finalized["stress_diagnostics_dir"]))
        self.assertIn("--stress_diagnostics_breadcrumbs=1", command)
        self.assertIn("--stress_diagnostics_scan_witness=1", command)
        self.assertIn(
            "--stress_diagnostics_dir=" + finalized["stress_diagnostics_dir"],
            command,
        )

    def test_cf_consistency_randomizes_secondary(self):
        db_crashtest = self.load_db_crashtest()

        params = db_crashtest.gen_cmd_params(
            self.build_mode_args("blackbox", cf_consistency=True)
        )

        with mock.patch.object(db_crashtest.random, "randint", side_effect=[0, 1]):
            self.assertEqual(0, params["test_secondary"]())
            self.assertEqual(1, params["test_secondary"]())

    def test_manifest_options_tracking_is_randomized_per_invocation(self):
        db_crashtest = self.load_db_crashtest()
        choose_tracking = db_crashtest.default_params[
            "track_options_file_number_in_manifest"
        ]

        with mock.patch.object(db_crashtest.random, "randint", side_effect=[0, 1]):
            self.assertEqual(0, choose_tracking())
            self.assertEqual(1, choose_tracking())

    def test_trie_index_format_version_is_randomized_per_invocation(self):
        # Readers handle every format, so DBs may mix SSTs of both formats.
        db_crashtest = self.load_db_crashtest()
        choose_version = db_crashtest.default_params["trie_index_format_version"]

        with mock.patch.object(db_crashtest.random, "choice", side_effect=min):
            self.assertEqual(1, choose_version())
        with mock.patch.object(db_crashtest.random, "choice", side_effect=max):
            self.assertEqual(2, choose_version())

    def test_cf_lifecycle_profile_and_schedule(self):
        db_crashtest = self.load_db_crashtest()
        params = db_crashtest.gen_cmd_params(
            self.build_mode_args("blackbox", cf_lifecycle=True)
        )
        finalized_params = db_crashtest.finalize_and_sanitize(
            self.build_params(params)
        )

        self.assertEqual(1, params["test_cf_lifecycle"])
        self.assertEqual(2, params["column_families"])
        self.assertEqual(1, params["threads"])
        self.assertEqual(0, params["metadata_write_fault_one_in"])
        self.assertEqual(0, params["reuse_manifest_on_open"])
        self.assertEqual(100, params["writepercent"])
        self.assertEqual(0, params["readpercent"])
        self.assertEqual(1, params["skip_verifydb"])
        self.assertEqual(0, params["verify_db_one_in"])
        self.assertEqual(0, params["verify_iterator_with_expected_state_one_in"])
        self.assertEqual(0, params["set_options_one_in"])
        self.assertEqual(0, params["compact_range_one_in"])
        self.assertEqual(0, params["remote_compaction_worker_threads"])
        self.assertEqual(1, finalized_params["num_dbs"])
        self.assertEqual(100, finalized_params["clear_column_family_one_in"])
        self.assertGreaterEqual(
            finalized_params["max_key"]
            >> finalized_params["log2_keys_per_lock"],
            1,
        )
        self.assertLessEqual(
            finalized_params["max_key"]
            >> finalized_params["log2_keys_per_lock"],
            16,
        )
        self.assertEqual(
            {
                "cf_lifecycle_fault_target": 1,
                "track_options_file_number_in_manifest": 1,
                "clear_column_family_one_in": 1,
            },
            db_crashtest.cf_lifecycle_iteration_overrides(0),
        )
        self.assertEqual(0, db_crashtest.cf_lifecycle_iteration_overrides(1)[
            "track_options_file_number_in_manifest"
        ])
        self.assertEqual(
            2,
            db_crashtest.cf_lifecycle_iteration_overrides(2)[
                "cf_lifecycle_fault_target"
            ],
        )
        self.assertEqual(
            100,
            db_crashtest.cf_lifecycle_iteration_overrides(3)[
                "clear_column_family_one_in"
            ],
        )

        params.update(db_crashtest.cf_lifecycle_iteration_overrides(1))
        params.update(
            {
                "destroy_db_initially": 0,
                "max_key": 10000,
                "log2_keys_per_lock": 0,
            }
        )
        cmd, _ = db_crashtest.gen_cmd(
            self.build_params(params),
            [
                "-track_options_file_number_in_manifest=true",
                "--cf_lifecycle_fault_target=2",
                "--clear_column_family_one_in=1",
                "-destroy_db_initially=true",
                "--max_key=1000000",
                "-log2_keys_per_lock=0",
            ],
        )
        self.assertEqual(
            [
                "--cf_lifecycle_fault_target=0",
                "--track_options_file_number_in_manifest=0",
                "--clear_column_family_one_in=0",
                "--destroy_db_initially=0",
                "--max_key=10000",
                "--log2_keys_per_lock=10",
            ],
            cmd[-6:],
        )

        params["destroy_db_initially"] = 1
        first_cmd, _ = db_crashtest.gen_cmd(
            self.build_params(params), ["-destroy_db_initially=false"]
        )
        self.assertEqual("--destroy_db_initially=1", first_cmd[-3])

    def test_generated_command_disables_unmanaged_lifecycle_mode(self):
        db_crashtest = self.load_db_crashtest()
        params = db_crashtest.gen_cmd_params(self.build_mode_args("blackbox"))

        cmd, _ = db_crashtest.gen_cmd(
            self.build_params(params),
            ["--flagfile=flags.txt", "-test_cf_lifecycle=1", "--", "ignored"],
        )

        terminator_index = cmd.index("--")
        self.assertEqual(
            ["-test_cf_lifecycle=1", "--test_cf_lifecycle=0"],
            cmd[terminator_index - 2 : terminator_index],
        )

    def test_cf_lifecycle_rejects_direct_low_level_controls(self):
        for control in (
            "--test_cf_lifecycle=1",
            "--cf_lifecycle_fault_target=1",
        ):
            with self.subTest(control=control):
                db_crashtest = self.load_db_crashtest(["liveness", control])
                with mock.patch.object(sys, "stderr", io.StringIO()):
                    with self.assertRaisesRegex(SystemExit, "2"):
                        db_crashtest.main()

    def test_cf_lifecycle_rejects_argument_terminator(self):
        db_crashtest = self.load_db_crashtest(
            ["blackbox", "--cf_lifecycle", "-user_override=true", "--"]
        )

        with mock.patch.object(sys, "stderr", io.StringIO()):
            with self.assertRaisesRegex(SystemExit, "2"):
                db_crashtest.main()

    def test_expected_cf_lifecycle_fault_requires_exit_and_marker(self):
        db_crashtest = self.load_db_crashtest()
        params = {"cf_lifecycle_fault_target": 1}
        marker = "EXPECTED_CF_LIFECYCLE_FAULT target=options"

        self.assertTrue(
            db_crashtest.expected_cf_lifecycle_fault_observed(
                params, False, 86, marker
            )
        )
        self.assertFalse(
            db_crashtest.expected_cf_lifecycle_fault_observed(
                params, True, 86, marker
            )
        )
        self.assertFalse(
            db_crashtest.expected_cf_lifecycle_fault_observed(
                params, False, 1, marker
            )
        )
        self.assertFalse(
            db_crashtest.expected_cf_lifecycle_fault_observed(
                params, False, 86, ""
            )
        )
        self.assertFalse(
            db_crashtest.expected_cf_lifecycle_fault_observed(
                params, False, 86, marker + "_extra"
            )
        )

    def test_cf_lifecycle_success_count_requires_exact_lines(self):
        db_crashtest = self.load_db_crashtest()

        self.assertEqual(
            2,
            db_crashtest.count_cf_lifecycle_successes(
                "CF_LIFECYCLE_SUCCESS\n"
                "NOT_CF_LIFECYCLE_SUCCESS\n"
                "CF_LIFECYCLE_SUCCESS\n"
            ),
        )

    def test_cf_lifecycle_requires_repeated_successful_recreation(self):
        db_crashtest = self.load_db_crashtest()

        self.assertEqual(
            ["success"],
            db_crashtest.missing_cf_lifecycle_coverage(
                {"options": 1, "manifest": 1, "success": 1}
            ),
        )
        self.assertEqual(
            [],
            db_crashtest.missing_cf_lifecycle_coverage(
                {"options": 1, "manifest": 1, "success": 2}
            ),
        )

    def test_final_verification_failure_is_lifecycle_only(self):
        db_crashtest = self.load_db_crashtest()
        ordinary_args = self.build_mode_args("blackbox")
        lifecycle_args = self.build_mode_args("blackbox", cf_lifecycle=True)

        self.assertFalse(
            db_crashtest.cf_lifecycle_final_verification_failed(
                ordinary_args, True, -15
            )
        )
        self.assertTrue(
            db_crashtest.cf_lifecycle_final_verification_failed(
                lifecycle_args, True, -15
            )
        )
        self.assertTrue(
            db_crashtest.cf_lifecycle_final_verification_failed(
                lifecycle_args, False, 1
            )
        )
        self.assertFalse(
            db_crashtest.cf_lifecycle_final_verification_failed(
                lifecycle_args, False, 0
            )
        )

    def test_skip_verifydb_disables_verification_knobs(self):
        db_crashtest = self.load_db_crashtest()

        unfinalized = db_crashtest.gen_cmd_params(
            self.build_mode_args(
                "blackbox",
                cf_consistency=True,
                test_best_efforts_recovery=True,
            )
        )
        unfinalized["db"] = self.test_tmpdir
        params = db_crashtest.finalize_and_sanitize(unfinalized)

        # `db_stress` rejects either knob when `skip_verifydb` is set.
        self.assertEqual(1, params["skip_verifydb"])
        self.assertEqual(0, params["continuous_verification_interval"])
        self.assertEqual(0, params["verify_db_one_in"])

    def test_stress_diagnostics_disabled_does_not_allocate_dir(self):
        db_crashtest = self.load_db_crashtest()
        params = {
            "stress_diagnostics_breadcrumbs": 0,
            "stress_diagnostics_scan_witness": 0,
            "stress_diagnostics_dir": "",
        }

        db_crashtest.set_default_stress_diagnostics_dir(params)

        self.assertEqual("", params["stress_diagnostics_dir"])
        self.assertIsNone(db_crashtest.diagnostics_dir_global)

    def test_stress_diagnostics_scan_witness_uses_independent_gate(self):
        db_crashtest = self.load_db_crashtest()
        self.register_diagnostics_dir_cleanup(db_crashtest)
        params = {
            "stress_diagnostics_breadcrumbs": 0,
            "stress_diagnostics_scan_witness": 1,
            "stress_diagnostics_dir": "",
        }

        db_crashtest.set_default_stress_diagnostics_dir(params)

        self.assertEqual(
            os.path.join(self.test_tmpdir, "rocksdb_crashtest_diagnostics"),
            params["stress_diagnostics_dir"],
        )

    def test_stress_diagnostics_paths_include_artifact_dir(self):
        db_crashtest = self.load_db_crashtest()
        diagnostics_dir = os.path.join(self.test_tmpdir, "diagnostics")

        self.assertEqual(
            ["/db", "/expected", diagnostics_dir],
            db_crashtest.diagnostic_paths(
                {
                    "db": "/db",
                    "expected_values_dir": "/expected",
                    "stress_diagnostics_dir": diagnostics_dir,
                }
            ),
        )

    def test_full_external_table_profile(self):
        db_crashtest = self.load_db_crashtest()
        args = self.build_mode_args(
            test_type="blackbox", test_full_external_table=True
        )

        # Choose the blob overlay whenever it is consulted. The full external
        # table profile must bypass that random overlay.
        with mock.patch.object(
            db_crashtest.random, "choice", side_effect=lambda values: values[-1]
        ):
            params = db_crashtest.gen_cmd_params(args)
        command_params = dict(params)
        command_params["enable_compaction_filter"] = 0
        command_params["db"] = self.test_tmpdir
        command, finalized = db_crashtest.gen_cmd(command_params, [])

        expected = {
            "backup_one_in": 0,
            "column_families": 1,
            "open_files": 100,
            "target_file_size_base": 1024 * 1024,
            "target_file_size_multiplier": 1,
            "write_buffer_size": 1024 * 1024,
            "compaction_ttl": 0,
            "delpercent": 4,
            "delrangepercent": 1,
            "get_property_one_in": 0,
            "get_properties_of_all_tables_one_in": 0,
            "ingest_external_file_with_embedded_blobs": 0,
            "ingest_wbwi_one_in": 0,
            "periodic_compaction_seconds": 0,
            "preclude_last_level_data_seconds": 0,
            "preserve_internal_time_seconds": 0,
            "remote_compaction_worker_threads": 0,
            "set_options_one_in": 0,
            "test_backward_scan": 0,
            "use_full_external_table": 1,
            "use_timed_put_one_in": 0,
            "user_timestamp_size": 0,
        }
        self.assertEqual(expected, db_crashtest.full_external_table_params)
        for key, value in expected.items():
            if key not in ("delpercent", "delrangepercent"):
                self.assertEqual(value, finalized.get(key))
        self.assertEqual(
            5, finalized["delpercent"] + finalized["delrangepercent"]
        )
        self.assertIn(finalized["delrangepercent"], (0, 1))
        for inherited_param in (
            "adaptive_readahead",
            "allow_unprepared_value",
            "async_io",
            "block_protection_bytes_per_key",
            "bottommost_compression_type",
            "check_multiget_consistency",
            "checksum_type",
            "compression_auto_skip",
            "compression_manager",
            "compression_type",
            "enable_checksum_handoff",
            "enable_compaction_filter",
            "file_checksum_impl",
            "ingest_external_file_one_in",
            "inplace_update_support",
            "mmap_read",
            "min_tombstones_for_range_conversion",
            "paranoid_file_checks",
            "test_batches_snapshots",
            "use_async_db_api",
            "use_merge",
            "use_multiget",
            "use_multiscan",
            "use_trie_index",
            "verify_checksum_one_in",
            "verify_file_checksums_one_in",
            "verify_manifest_content_on_close",
        ):
            self.assertIs(
                db_crashtest.default_params[inherited_param],
                params[inherited_param],
            )
        self.assertEqual(10 * 1024 * 1024, finalized["max_bytes_for_level_base"])
        self.assertEqual(1, finalized["verify_sst_unique_id_in_manifest"])
        self.assertGreater(finalized["acquire_snapshot_one_in"], 0)
        self.assertIn("--use_full_external_table=1", command)
        self.assertFalse(
            any(arg.startswith("--test_full_external_table=") for arg in command)
        )
        for fault_param in (
            "metadata_read_fault_one_in",
            "metadata_write_fault_one_in",
            "open_metadata_read_fault_one_in",
            "open_metadata_write_fault_one_in",
            "open_read_fault_one_in",
            "open_write_fault_one_in",
            "read_fault_one_in",
            "sync_fault_injection",
            "write_fault_one_in",
        ):
            self.assertIs(db_crashtest.default_params[fault_param], params[fault_param])

    def test_cache_and_write_buffer_size_multiplier_preserves_randomization(self):
        db_crashtest = self.load_db_crashtest()
        cache_sizes = iter([8 * 1024 * 1024, 32 * 1024 * 1024])
        params = self.build_params(
            db_crashtest.default_params,
            {
                "cache_size": lambda: next(cache_sizes),
                # Keep the block/readahead/cache override off (see
                # finalize_and_sanitize) so it does not replace cache_size.
                "prefix_size": -1,
                "test_batches_snapshots": 0,
                "write_buffer_size": 32 * 1024 * 1024,
                "cache_and_write_buffer_size_multiplier": 0.5,
            },
        )

        first_command, _ = db_crashtest.gen_cmd(params, [])
        second_command, _ = db_crashtest.gen_cmd(params, [])

        self.assertIn("--cache_size=4194304", first_command)
        self.assertIn("--cache_size=16777216", second_command)
        self.assertIn("--write_buffer_size=16777216", first_command)
        self.assertIn("--write_buffer_size=16777216", second_command)
        self.assertFalse(
            any(
                arg.startswith("--cache_and_write_buffer_size_multiplier=")
                for arg in first_command
            )
        )

    def test_wbm_flush_policy_coverage(self):
        db_crashtest = self.load_db_crashtest()
        for policy in db_crashtest._WBM_FLUSH_POLICIES:
            params = db_crashtest.gen_cmd_params(
                self.build_mode_args(
                    cache_size=8 * 1024 * 1024,
                    db=self.test_tmpdir,
                    db_write_buffer_size=1024 * 1024,
                    num_dbs=1,
                    use_write_buffer_manager=1,
                    wbm_flush_policy=policy,
                )
            )

            command, _ = db_crashtest.gen_cmd(params, [])

            self.assertIn(f"--wbm_flush_policy={policy}", command)
            expected_num_dbs = (
                2
                if policy == db_crashtest._WBM_FLUSH_LARGEST_ACROSS_DBS
                else 1
            )
            self.assertIn(f"--num_dbs={expected_num_dbs}", command)

    def test_cross_db_wbm_policy_keeps_db_topology_stable(self):
        db_crashtest = self.load_db_crashtest()
        policies = iter([db_crashtest._WBM_FLUSH_LARGEST_ACROSS_DBS, 0])
        params = db_crashtest.gen_cmd_params(
            self.build_mode_args(
                cache_size=8 * 1024 * 1024,
                db=self.test_tmpdir,
                db_write_buffer_size=1024 * 1024,
                num_dbs=1,
                use_write_buffer_manager=1,
                wbm_flush_policy=lambda: next(policies),
            )
        )

        first_command, _ = db_crashtest.gen_cmd(params, [])
        second_command, _ = db_crashtest.gen_cmd(params, [])

        self.assertEqual(
            db_crashtest._WBM_FLUSH_LARGEST_ACROSS_DBS,
            params["wbm_flush_policy"],
        )
        self.assertEqual(2, params["num_dbs"])
        for command in (first_command, second_command):
            self.assertIn("--wbm_flush_policy=2", command)
            self.assertIn("--num_dbs=2", command)

    def test_cross_db_wbm_policy_does_not_force_two_dbs_when_disabled(self):
        db_crashtest = self.load_db_crashtest()
        params = db_crashtest.gen_cmd_params(
            self.build_mode_args(
                db=self.test_tmpdir,
                num_dbs=1,
                use_write_buffer_manager=0,
                wbm_flush_policy=db_crashtest._WBM_FLUSH_LARGEST_ACROSS_DBS,
            )
        )

        command, _ = db_crashtest.gen_cmd(params, [])

        self.assertEqual(1, params["num_dbs"])
        self.assertIn("--wbm_flush_policy=0", command)
        self.assertIn("--num_dbs=1", command)

    def test_cache_and_write_buffer_size_multiplier_respects_write_buffer_minimum(
        self,
    ):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "write_buffer_size": 64 * 1024,
                "cache_and_write_buffer_size_multiplier": 0.5,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(64 * 1024, finalized["write_buffer_size"])
        self.assertNotIn("cache_and_write_buffer_size_multiplier", finalized)

    def test_get_ev_parent_dir_preserves_existing_contents(self):
        os.makedirs(self.expected_dir)
        marker = os.path.join(self.expected_dir, "marker")
        with open(marker, "w") as f:
            f.write("keep")

        db_crashtest = self.load_db_crashtest()

        expected_dir = db_crashtest.get_ev_parent_dir()

        self.assertEqual(self.expected_dir, expected_dir)
        self.assertTrue(os.path.exists(marker))

    def test_finalize_keeps_parallel_compression_with_trie_index(self):
        db_crashtest = self.load_db_crashtest()
        for use_udi_as_primary_index in (0, 1):
            with self.subTest(use_udi_as_primary_index=use_udi_as_primary_index):
                params = self.build_params(
                    db_crashtest.default_params,
                    {
                        "use_trie_index": 1,
                        "use_udi_as_primary_index": use_udi_as_primary_index,
                        "compression_parallel_threads": 4,
                    },
                )

                finalized = db_crashtest.finalize_and_sanitize(params)

                self.assertEqual(1, finalized["use_trie_index"])
                self.assertEqual(4, finalized["compression_parallel_threads"])

    def test_finalize_disables_test_batches_snapshots_when_disable_wal(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "disable_wal": 1,
                "test_batches_snapshots": 1,
                # finalize_and_sanitize() intentionally forces disable_wal=0
                # when inplace_update_support=1.
                "inplace_update_support": 0,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1, finalized["disable_wal"])
        self.assertEqual(0, finalized["test_batches_snapshots"])

    def test_finalize_disables_test_batches_snapshots_for_blob_direct_write(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            dict(
                list(db_crashtest.default_params.items())
                + list(db_crashtest.blob_direct_write_multi_get_entity_params.items())
            ),
            {
                "test_batches_snapshots": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1, finalized["enable_blob_direct_write"])
        self.assertEqual(1, finalized["disable_wal"])
        self.assertEqual(0, finalized["test_batches_snapshots"])

    def test_finalize_file_scoped_wal_fault_profile(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "file_scope_wal_write_faults": 1,
                "write_fault_one_in": 0,
                "max_write_buffer_number": 3,
                "disable_wal": 1,
                "manual_wal_flush_one_in": 10,
                "use_txn": 1,
                "two_write_queues": 1,
                "enable_pipelined_write": 1,
                "unordered_write": 1,
                "recycle_log_file_num": 1,
                "WAL_ttl_seconds": 60,
                "WAL_size_limit_MB": 1,
                "track_and_verify_wals": 1,
                "enable_blob_direct_write": 1,
                "test_batches_snapshots": 1,
                "reopen": 10,
                "exclude_wal_from_write_fault_injection": 1,
                "inject_error_severity": 2,
                "sync_fault_injection": 1,
                "metadata_read_fault_one_in": 10,
                "metadata_write_fault_one_in": 10,
                "read_fault_one_in": 10,
                "open_metadata_read_fault_one_in": 10,
                "open_metadata_write_fault_one_in": 10,
                "open_read_fault_one_in": 10,
                "open_write_fault_one_in": 10,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1, finalized["file_scope_wal_write_faults"])
        self.assertEqual(128, finalized["write_fault_one_in"])
        self.assertEqual(10, finalized["max_write_buffer_number"])
        self.assertEqual(0, finalized["test_batches_snapshots"])
        self.assertEqual(1, finalized["inject_error_severity"])
        for flag in [
            "disable_wal",
            "manual_wal_flush_one_in",
            "use_txn",
            "use_optimistic_txn",
            "test_multi_ops_txns",
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
        ]:
            self.assertEqual(0, finalized[flag], flag)

    def test_finalize_disables_ingest_wbwi_for_file_scoped_wal_faults(self):
        db_crashtest = self.load_db_crashtest()
        # Isolate the regression: the generic ingest_wbwi_one_in guard only
        # fires on disable_wal==0 / pipelined / unordered / timestamps, none of
        # which hold here. disable_wal starts at 1, so that guard leaves
        # ingest_wbwi_one_in untouched; the file-scoped WAL fault profile then
        # forces disable_wal=0 later, and must also disable the (now
        # unsupported) IngestWriteBatchWithIndex path.
        params = self.build_params(
            db_crashtest.default_params,
            {
                "file_scope_wal_write_faults": 1,
                "disable_wal": 1,
                "ingest_wbwi_one_in": 100,
                "enable_pipelined_write": 0,
                "unordered_write": 0,
                "user_timestamp_size": 0,
                "use_txn": 0,
                "two_write_queues": 0,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(0, finalized["disable_wal"])
        self.assertEqual(0, finalized["ingest_wbwi_one_in"])

    def test_auto_file_scoped_wal_faults_preserve_explicit_opt_outs(self):
        db_crashtest = self.load_db_crashtest()

        def choose_without_blob_profile(values):
            if values == [0] * 9 + [1]:
                return 0
            if values == [0] * 19 + [1]:
                return 1
            return values[0]

        for explicit_opt_out in [
            {"file_scope_wal_write_faults": 0},
            {"write_fault_one_in": 0},
            {"exclude_wal_from_write_fault_injection": 1},
            {"enable_blob_direct_write": 1},
        ]:
            with self.subTest(explicit_opt_out=explicit_opt_out):
                args = self.build_mode_args(
                    test_type="blackbox", **explicit_opt_out
                )
                with mock.patch.object(
                    db_crashtest.random,
                    "choice",
                    side_effect=choose_without_blob_profile,
                ):
                    params = db_crashtest.gen_cmd_params(args)

                self.assertEqual(0, params["file_scope_wal_write_faults"])

    def test_auto_file_scoped_wal_faults_skip_batch_mode(self):
        db_crashtest = self.load_db_crashtest()
        args = self.build_mode_args(test_type="blackbox")
        db_crashtest.default_params["test_batches_snapshots"] = 1

        def choose_without_blob_profile(values):
            if values == [0] * 9 + [1]:
                return 0
            if values == [0] * 19 + [1]:
                return 1
            return values[0]

        with mock.patch.object(
            db_crashtest.random,
            "choice",
            side_effect=choose_without_blob_profile,
        ):
            params = db_crashtest.gen_cmd_params(args)

        self.assertEqual(0, params["file_scope_wal_write_faults"])

    def test_auto_file_scoped_wal_faults_skip_unsupported_inputs(self):
        db_crashtest = self.load_db_crashtest()

        def choose_without_blob_profile(values):
            if values == [0] * 9 + [1]:
                return 0
            if values == [0] * 19 + [1]:
                return 1
            return values[0]

        cases = [
            ({"best_efforts_recovery": 1}, []),
            ({"test_cf_consistency": 1}, []),
            ({}, ["--options_file=/tmp/OPTIONS"]),
            ({}, ["-options_file", "/tmp/OPTIONS"]),
            ({}, ["-disable_wal=1"]),
            ({}, ["-test_cf_consistency=1"]),
        ]
        for overrides, unknown_params in cases:
            with self.subTest(overrides=overrides, unknown_params=unknown_params):
                args = self.build_mode_args(test_type="blackbox", **overrides)
                with mock.patch.object(
                    db_crashtest.random,
                    "choice",
                    side_effect=choose_without_blob_profile,
                ):
                    params = db_crashtest.gen_cmd_params(args, unknown_params)

                self.assertEqual(0, params["file_scope_wal_write_faults"])

    def test_auto_file_scoped_wal_faults_select_eligible_profile(self):
        db_crashtest = self.load_db_crashtest()
        args = self.build_mode_args(test_type="blackbox")
        db_crashtest.default_params["test_batches_snapshots"] = 0

        def choose_without_blob_profile(values):
            if values == [0] * 9 + [1]:
                return 0
            if values == [0] * 19 + [1]:
                return 1
            return values[0]

        with mock.patch.object(
            db_crashtest.random,
            "choice",
            side_effect=choose_without_blob_profile,
        ):
            params = db_crashtest.gen_cmd_params(args)

        self.assertEqual(1, params["file_scope_wal_write_faults"])

    def test_finalize_disables_sqfc_range_queries_with_range_conversion(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "test_batches_snapshots": 0,
                "use_multiscan": 0,
                "use_sqfc_for_range_queries": 1,
                "min_tombstones_for_range_conversion": 2,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(2, finalized["min_tombstones_for_range_conversion"])
        self.assertEqual(0, finalized["use_sqfc_for_range_queries"])

    def test_finalize_enables_atomic_replace_ingestion_for_universal(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "acquire_snapshot_one_in": 100,
                "compaction_style": 1,
                "disable_wal": 0,
                "enable_blob_direct_write": 0,
                "ingest_external_file_atomic_replace_one_in": 1,
                "ingest_external_file_one_in": 1,
                "manual_wal_flush_one_in": 0,
                "sync_fault_injection": 0,
                "test_batches_snapshots": 0,
                "user_timestamp_size": 0,
                "use_multiscan": 0,
            },
        )

        command, finalized = db_crashtest.gen_cmd(params, [])

        self.assertEqual(1, finalized["ingest_external_file_atomic_replace_one_in"])
        self.assertEqual(0, finalized["acquire_snapshot_one_in"])
        self.assertIn("--ingest_external_file_atomic_replace_one_in=1", command)

    def test_finalize_disables_atomic_replace_ingestion_when_incompatible(self):
        db_crashtest = self.load_db_crashtest()
        for overrides in (
            {"compaction_style": 0, "ingest_external_file_one_in": 1},
            {"compaction_style": 1, "ingest_external_file_one_in": 0},
            {
                "compaction_style": 1,
                "ingest_external_file_one_in": 1,
                "ingest_external_file_width": 1,
            },
            {
                "compaction_style": 1,
                "ingest_external_file_one_in": 1,
                "persist_user_defined_timestamps": 1,
                "user_timestamp_size": 8,
            },
            {
                "compaction_style": 1,
                "ingest_external_file_one_in": 1,
                "use_multiscan": 1,
            },
        ):
            with self.subTest(overrides=overrides):
                params = self.build_params(
                    db_crashtest.default_params,
                    {
                        "ingest_external_file_atomic_replace_one_in": 1,
                        **overrides,
                    },
                )

                finalized = db_crashtest.finalize_and_sanitize(params)

                self.assertEqual(
                    0, finalized["ingest_external_file_atomic_replace_one_in"]
                )

    def test_finalize_trie_index_mode_rules(self):
        db_crashtest = self.load_db_crashtest()
        for index_mode in range(5):
            with self.subTest(index_mode=index_mode):
                params = self.build_params(
                    db_crashtest.default_params,
                    {
                        "use_trie_index": 1,
                        "index_mode": index_mode,
                        "mmap_read": 1,
                        "index_type": 2,
                        "partition_filters": 1,
                        "backup_one_in": 1000,
                        "test_secondary": 1,
                        "remote_compaction_worker_threads": 0,
                        "format_version": 2,
                    },
                )

                finalized = db_crashtest.finalize_and_sanitize(params)

                self.assertEqual(index_mode, finalized["index_mode"])
                self.assertEqual(0, finalized["mmap_read"])
                if index_mode in (2, 3):
                    self.assertIn(finalized["index_type"], [0, 3])
                    self.assertEqual(0, finalized["partition_filters"])
                    self.assertEqual(0, finalized["backup_one_in"])
                    self.assertEqual(0, finalized["test_secondary"])
                else:
                    self.assertEqual(2, finalized["index_type"])
                    self.assertEqual(1, finalized["partition_filters"])
                    self.assertEqual(1000, finalized["backup_one_in"])
                    self.assertEqual(1, finalized["test_secondary"])
                if index_mode == 3:
                    self.assertGreaterEqual(finalized["format_version"], 6)

    def test_finalize_keeps_remote_compaction_in_trie_index_modes(self):
        db_crashtest = self.load_db_crashtest()
        for index_mode in range(5):
            with self.subTest(index_mode=index_mode):
                params = self.build_params(
                    db_crashtest.default_params,
                    {
                        "use_trie_index": 1,
                        "index_mode": index_mode,
                        "backup_one_in": 0,
                        "test_secondary": 0,
                        "remote_compaction_worker_threads": 8,
                        "enable_blob_direct_write": 0,
                        "use_txn": 0,
                    },
                )

                finalized = db_crashtest.finalize_and_sanitize(params)

                self.assertEqual(8, finalized["remote_compaction_worker_threads"])

    def test_finalize_read_index_rules(self):
        db_crashtest = self.load_db_crashtest()
        # (use_trie_index, index_mode, read_index, expected read_index)
        cases = [
            (1, 2, 1, 1),  # kBuiltin is valid with kCustomDefault
            (1, 3, 1, -1),  # kCustomOnly has only a stub standard index
            (1, 3, 2, 2),
            (0, 1, 1, -1),  # selectors need the trie factory
            (0, 1, 2, -1),
            (0, 1, 0, 0),
        ]
        for use_trie_index, index_mode, read_index, expected in cases:
            with self.subTest(
                use_trie_index=use_trie_index,
                index_mode=index_mode,
                read_index=read_index,
            ):
                params = self.build_params(
                    db_crashtest.default_params,
                    {
                        "use_trie_index": use_trie_index,
                        "index_mode": index_mode,
                        "read_index": read_index,
                    },
                )

                finalized = db_crashtest.finalize_and_sanitize(params)

                self.assertEqual(expected, finalized["read_index"])

    def test_finalize_resets_custom_index_mode_without_trie(self):
        db_crashtest = self.load_db_crashtest()
        for index_mode in (2, 3, 4):
            for overrides in (
                {"use_trie_index": 0},
                # Timestamps disable the trie after the trie rules run.
                {
                    "use_trie_index": 1,
                    "user_timestamp_size": 8,
                    "persist_user_defined_timestamps": 1,
                },
            ):
                with self.subTest(index_mode=index_mode, **overrides):
                    params = self.build_params(
                        db_crashtest.default_params,
                        dict(overrides, index_mode=index_mode, read_index=2),
                    )

                    finalized = db_crashtest.finalize_and_sanitize(params)

                    self.assertEqual(0, finalized["use_trie_index"])
                    self.assertEqual(1, finalized["index_mode"])
                    self.assertEqual(-1, finalized["read_index"])

    def test_sanitize_known_stderr_suppresses_only_known_sigterm_lines(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        stderr = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )

        filtered_stdout, filtered_stderr = db_crashtest.sanitize_known_stderr(
            stdout, stderr, True, {}
        )

        self.assertEqual("", filtered_stderr)
        self.assertEqual(
            stdout + "Ignored known stderr:\n" + stderr,
            filtered_stdout,
        )

    def test_sanitize_known_stderr_suppresses_retryable_wait_cqe(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"

        # This models blackbox crash test SIGTERM interrupting wait_cqe before
        # the C++ retry path can avoid stderr; only retryable codes are expected.
        for caller in ("Poll", "AbortIO"):
            for err in (-4, -11):
                stderr = f"{caller}: io_uring_wait_cqe failed: {err}\n"
                filtered_stdout, filtered_stderr = (
                    db_crashtest.sanitize_known_stderr(stdout, stderr, True, {})
                )

                self.assertEqual("", filtered_stderr)
                self.assertEqual(
                    stdout + "Ignored known stderr:\n" + stderr,
                    filtered_stdout,
                )

    def test_sanitize_known_stderr_preserves_terminal_wait_cqe(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        stderr = "Poll: io_uring_wait_cqe failed: -5\n"

        # This guards against hiding real io_uring failures: even after SIGTERM,
        # non-retryable wait_cqe errors must remain visible on stderr.
        filtered_stdout, filtered_stderr = db_crashtest.sanitize_known_stderr(
            stdout, stderr, True, {}
        )

        self.assertEqual(stderr, filtered_stderr)
        self.assertEqual(stdout, filtered_stdout)

    def test_sanitize_known_stderr_preserves_other_stderr(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        ignored_line = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )
        kept_line = "Different stderr line\n"
        stderr = ignored_line + kept_line

        filtered_stdout, filtered_stderr = db_crashtest.sanitize_known_stderr(
            stdout, stderr, True, {}
        )

        self.assertEqual(kept_line, filtered_stderr)
        self.assertEqual(
            stdout + "Ignored known stderr:\n" + ignored_line,
            filtered_stdout,
        )

    def test_sanitize_known_stderr_requires_timeout_and_sigterm_marker(self):
        db_crashtest = self.load_db_crashtest()
        stderr = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )

        filtered_stdout, filtered_stderr = db_crashtest.sanitize_known_stderr(
            "Received signal 15 (Terminated)\n", stderr, False, {}
        )
        self.assertEqual("Received signal 15 (Terminated)\n", filtered_stdout)
        self.assertEqual(stderr, filtered_stderr)

        filtered_stdout, filtered_stderr = db_crashtest.sanitize_known_stderr(
            "other stdout\n", stderr, True, {}
        )
        self.assertEqual("other stdout\n", filtered_stdout)
        self.assertEqual(stderr, filtered_stderr)

    def test_sanitize_known_stderr_suppresses_only_cachelib_info(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Crash-recovery verification passed :)\n"
        cachelib_params = {
            "secondary_cache_uri": (
                "cachelibwrapper://cacheName=fbcode_stress_crash;"
                "fileName=/tmp/fbcode_stress_crash_cache;size=104857600"
            )
        }
        cachelib_info = (
            "I1005 18:03:25.419455 26069 SlabAllocator.cpp:110] "
            "CacheLib slab ASAN poisoning is DISABLED\n"
            "I1005 18:03:25.437886 26069 Device.cpp:1153] Cache file: "
            "/tmp/fbcode_stress_crash_cache size: 104857600 truncate: 1\n"
            "I1005 18:03:25.438145 26069 Device.cpp:1220] Cache file "
            "/tmp/fbcode_stress_crash_cache is fallocated from 0 bytes to "
            "104857600 bytes\n"
            "I1005 18:03:25.438421 26069 Device.cpp:1038] Created device with "
            "num_devices 1 size 104857600 block_size 4096,stripe_size 0 "
            "max_write_size 0 max_io_size 0 io_engine sync qdepth 0,"
            "num_fdp_devices 0\n"
            "I1005 18:03:25.488981 26069 RejectRandomAP.cpp:38] "
            "RejectRandomAP: probability 1\n"
            "I1005 18:03:25.489110 26069 NavySetup.cpp:286] "
            "metadataSize: 524288\n"
            "I1005 18:03:25.489446 26069 LruPolicy.cpp:32] "
            "LRU policy: expected 6 regions\n"
            "I1005 18:03:25.498560 26069 SparseMapIndex.cpp:99] "
            "SparseMapIndex was created with numBucketMaps_ = 65536, "
            "totalMutexes_ = 1024\n"
            "I1005 18:03:25.498678 26069 RegionManager.cpp:60] 6 regions, "
            "16777216 bytes each, allowReadDuringReclaim true, directFlush false\n"
            "I1005 18:03:25.500776 26069 Allocator.cpp:41] Enable "
            "priority-based allocation for Allocator. Number of priorities: 1\n"
            "I1005 18:03:25.501036 26069 BlockCache.cpp:257] "
            "Block cache created\n"
            "I1005 18:03:25.501143 26069 EnginePair.cpp:341] "
            "Small item cache is noop.\n"
            "I1005 18:03:25.501237 26069 Driver.cpp:80] "
            "Max concurrent inserts: 1000000\n"
        )
        cachelib_error = (
            "E1005 18:03:25.501335 26069 Driver.cpp:256] Reset Navy failed\n"
        )
        unrelated_info = (
            "I1005 18:03:25.501380 26069 OtherComponent.cpp:10] "
            "Unrelated information\n"
        )
        cases = (
            (
                "cachelib info",
                cachelib_params,
                cachelib_info,
                stdout + "Ignored known stderr:\n" + cachelib_info,
                "",
            ),
            ("no cachelib uri", {}, cachelib_info, stdout, cachelib_info),
            (
                "cachelib error",
                cachelib_params,
                cachelib_error,
                stdout,
                cachelib_error,
            ),
            (
                "unrelated info",
                cachelib_params,
                unrelated_info,
                stdout,
                unrelated_info,
            ),
        )

        for name, params, stderr, expected_stdout, expected_stderr in cases:
            with self.subTest(name=name):
                filtered_stdout, filtered_stderr = (
                    db_crashtest.sanitize_known_stderr(
                        stdout, stderr, False, params
                    )
                )

                self.assertEqual(expected_stdout, filtered_stdout)
                self.assertEqual(expected_stderr, filtered_stderr)

    def test_print_run_output_tolerates_read_executor_io_uring_init_failure(self):
        db_crashtest = self.load_db_crashtest()
        args = self.build_mode_args()
        finalized_params = {"db": self.test_tmpdir, "use_async_db_api": 1}
        stdout = "Crash-recovery verification passed :)\n"
        stderr = (
            "I0903 14:05:39.470695 286445 IoUringBackend.cpp:527] "
            "io_uring_queue_init_params(512,1024) failed errno = "
            '0:"Success" 0x7d2e06295280 retrying with capacity = 512\n'
            "E0903 14:05:39.470759 286445 IoUringBackend.cpp:537] "
            "io_uring_queue_init_params(512,512) failed ret = "
            '-12:"Unknown error -12" 0x7d2e06295280\n'
        )
        output = io.StringIO()

        with redirect_stdout(output):
            db_crashtest.print_run_output_and_exit_on_error(
                args, finalized_params, stdout, stderr
            )

        self.assertIn("Ignored known stderr", output.getvalue())
        self.assertIn(stderr, output.getvalue())

    def test_output_matches_no_space_catches_known_failure_strings(self):
        db_crashtest = self.load_db_crashtest()
        open_and_compact_stdout = (
            "Failed to run OpenAndCompact(/dev/shm/rocksdb_test/db): "
            "IO error: No space left on device: While appending to file: "
            "/dev/shm/rocksdb_test/db/tmp_output_1/019471.sst: "
            "No space left on device\n"
        )
        verification_stderr = (
            "Verification failed: SetOptions failed: IO error: Unable to "
            "persist options.: IO error: No space left on device: While "
            "appending to file: /dev/shm/rocksdb_test/db/OPTIONS-084168.dbtmp: "
            "No space left on device\n"
        )

        self.assertTrue(
            db_crashtest.output_matches_no_space(open_and_compact_stdout, "")
        )
        self.assertTrue(
            db_crashtest.output_matches_no_space("", verification_stderr)
        )
        self.assertFalse(
            db_crashtest.output_matches_no_space("", "Permission denied\n")
        )

    def test_file_type_suffix_preserves_compound_suffixes(self):
        db_crashtest = self.load_db_crashtest()

        self.assertEqual(".sst.trash", db_crashtest.file_type_suffix("000123.sst.trash"))
        self.assertEqual(
            ".sst", db_crashtest.file_type_suffix("tmp_output_1/019471.sst")
        )
        self.assertEqual(".old.1", db_crashtest.file_type_suffix("/tmp/LOG.old.1"))
        self.assertEqual("<no_ext>", db_crashtest.file_type_suffix("/tmp/CURRENT"))

    def test_finalize_sanitizes_incompatible_flags_for_multi_db(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "num_dbs": 3,
                "clear_column_family_one_in": 10,
                "test_multi_ops_txns": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(0, finalized["clear_column_family_one_in"])
        self.assertEqual(0, finalized["test_multi_ops_txns"])

    def test_finalize_preserves_flags_for_single_db(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "num_dbs": 1,
                "clear_column_family_one_in": 10,
                "tolerate_non_injected_io_errors_for_remote_dbs": 0,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(10, finalized["clear_column_family_one_in"])
        self.assertEqual(0, finalized["tolerate_non_injected_io_errors_for_remote_dbs"])

    def test_finalize_shrinks_blocks_readahead_and_cache_together(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "auto_readahead_size": 1,
                "prefix_size": 7,
                "use_multiscan": 0,
            },
        )

        with mock.patch.object(
            db_crashtest.random, "choice", side_effect=lambda seq: seq[-1]
        ):
            finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1024, finalized["block_size"])
        self.assertEqual(4096, finalized["readahead_size"])
        self.assertEqual(65536, finalized["cache_size"])
        self.assertEqual(0, finalized["use_write_buffer_manager"])

    def test_finalize_keeps_default_sizes_without_prefix_extractor(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "auto_readahead_size": 1,
                "prefix_size": -1,
                "test_batches_snapshots": 0,
            },
        )

        with mock.patch.object(
            db_crashtest.random, "choice", side_effect=lambda seq: seq[-1]
        ):
            finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(
            db_crashtest.default_params["block_size"], finalized["block_size"]
        )

    def test_finalize_defaults_tolerate_non_injected_io_errors_for_remote_db(self):
        db_crashtest = self.load_db_crashtest()
        db_crashtest.is_remote_db = True
        params = self.build_params(db_crashtest.default_params)

        finalized = db_crashtest.finalize_and_sanitize(params)

        # Remote backends can return transient infrastructure IO errors that
        # are not RocksDB bugs, so remote runs tolerate them by default.
        self.assertEqual(1, finalized["tolerate_non_injected_io_errors_for_remote_dbs"])

    def test_finalize_tolerates_non_injected_io_errors_for_remote_db(self):
        db_crashtest = self.load_db_crashtest()
        db_crashtest.is_remote_db = True
        params = self.build_params(
            db_crashtest.default_params,
            {
                "enable_blob_direct_write": 1,
                "tolerate_non_injected_io_errors_for_remote_dbs": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(0, finalized["enable_blob_direct_write"])
        # Remote DBs keep the caller-provided value; it is not overridden.
        self.assertEqual(1, finalized["tolerate_non_injected_io_errors_for_remote_dbs"])

    def test_finalize_disables_tolerate_non_injected_io_errors_for_local_db(self):
        db_crashtest = self.load_db_crashtest()
        db_crashtest.is_remote_db = False
        params = self.build_params(
            db_crashtest.default_params,
            {
                "tolerate_non_injected_io_errors_for_remote_dbs": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        # Local DBs must never tolerate IO errors, even if requested, so genuine
        # local IO errors (e.g. io_uring failures) are not silently masked.
        self.assertEqual(0, finalized["tolerate_non_injected_io_errors_for_remote_dbs"])

    def test_build_out_of_space_diagnostics_summarizes_directory_suffixes(self):
        db_crashtest = self.load_db_crashtest()
        db_root = os.path.join(self.test_tmpdir, "rocksdb_crashtest_blackbox")
        remote_output_dir = os.path.join(db_root, "tmp_output_123")
        os.makedirs(remote_output_dir)

        files = {
            os.path.join(db_root, "CURRENT"): 7,
            os.path.join(db_root, "000001.sst.trash"): 3,
            os.path.join(remote_output_dir, "019471.sst"): 5,
        }
        for path, size in files.items():
            with open(path, "wb") as f:
                f.write(b"x" * size)

        diagnostics = db_crashtest.build_out_of_space_diagnostics(
            "",
            (
                "IO error: No space left on device: While appending to file: "
                f"{os.path.join(remote_output_dir, '019471.sst')}: "
                "No space left on device\n"
            ),
            [db_root],
            include_dev_shm=False,
        )

        self.assertIn("=== Out-of-space diagnostics ===", diagnostics)
        self.assertIn(f"Directory usage for {db_root}:", diagnostics)
        self.assertIn(".sst.trash files=1 bytes=3B", diagnostics)
        self.assertIn(".sst files=1 bytes=5B", diagnostics)
        self.assertIn("<no_ext> files=1 bytes=7B", diagnostics)
        self.assertIn(
            f"{remote_output_dir} subtree=5B local=5B local_files=1 local_dirs=0",
            diagnostics,
        )

    def test_liveness_params_keep_mixed_workload_and_enable_watchdog(self):
        db_crashtest = self.load_db_crashtest()

        # Liveness mode should inherit the normal mixed workload. The C++
        # watchdog tracks per-thread active operation type, so Python does not
        # need to force an all-write workload to keep the signal readable.
        params = db_crashtest.gen_cmd_params(self.build_mode_args())
        params["db"] = self.test_tmpdir
        finalized_params = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC, params["duration"])
        self.assertEqual(1, params["liveness_check_interval_sec"])
        self.assertEqual(300, params["liveness_no_progress_timeout_sec"])
        self.assertEqual(1, params["enable_thread_tracking"])
        self.assertEqual(1, params["progress_reports"])
        self.assertEqual(100000000, params["ops_per_thread"])
        for fault_param in [
            "error_recovery_with_no_fault_injection",
            "exclude_wal_from_write_fault_injection",
            "file_scope_wal_write_faults",
            "metadata_read_fault_one_in",
            "metadata_write_fault_one_in",
            "open_metadata_read_fault_one_in",
            "open_metadata_write_fault_one_in",
            "open_read_fault_one_in",
            "open_write_fault_one_in",
            "read_fault_one_in",
            "secondary_cache_fault_one_in",
            "sync_fault_injection",
            "write_fault_one_in",
        ]:
            self.assertEqual(0, finalized_params[fault_param])
        self.assertIn(
            finalized_params["abort_and_resume_compactions_one_in"],
            [0, 1000, 10000],
        )
        self.assertEqual(db_crashtest.default_params["readpercent"], params["readpercent"])
        self.assertEqual(
            db_crashtest.default_params["prefixpercent"], params["prefixpercent"]
        )
        self.assertEqual(
            db_crashtest.default_params["writepercent"], params["writepercent"]
        )
        self.assertGreaterEqual(params["writepercent"], 20)
        self.assertEqual(db_crashtest.default_params["delpercent"], params["delpercent"])
        self.assertEqual(
            db_crashtest.default_params["delrangepercent"], params["delrangepercent"]
        )
        self.assertEqual(db_crashtest.default_params["iterpercent"], params["iterpercent"])

        params = db_crashtest.gen_cmd_params(
            self.build_mode_args(read_fault_one_in=32, sync_fault_injection=1)
        )
        self.assertEqual(0, params["read_fault_one_in"])
        self.assertEqual(0, params["sync_fault_injection"])

    def test_liveness_command_passes_watchdog_flags_not_wrapper_duration(self):
        db_crashtest = self.load_db_crashtest()
        check_interval_sec = 2
        no_progress_timeout_sec = 7
        wrapper_duration_sec = 17
        args = self.build_mode_args(
            duration=wrapper_duration_sec,
            liveness_check_interval_sec=check_interval_sec,
            liveness_no_progress_timeout_sec=no_progress_timeout_sec,
        )
        params = db_crashtest.gen_cmd_params(args)
        params["db"] = self.test_tmpdir

        # The Python wrapper owns duration; db_stress only needs the watchdog
        # interval and no-progress timeout flags.
        cmd, _ = db_crashtest.gen_cmd(params, [])
        cmd_flags = {}
        for arg in cmd:
            if arg.startswith("--") and "=" in arg:
                key, value = arg[2:].split("=", 1)
                cmd_flags[key] = value

        self.assertEqual(
            str(check_interval_sec), cmd_flags["liveness_check_interval_sec"]
        )
        self.assertEqual(
            str(no_progress_timeout_sec),
            cmd_flags["liveness_no_progress_timeout_sec"],
        )
        self.assertNotIn("duration", cmd_flags)

    def test_liveness_timeout_zero_uses_default_duration(self):
        db_crashtest = self.load_db_crashtest()

        # A zero/None duration still needs a finite wrapper runtime.
        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC,
            db_crashtest.liveness_timeout({"duration": 0}),
        )
        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC,
            db_crashtest.liveness_timeout({"duration": None}),
        )
        self.assertEqual(30, db_crashtest.liveness_timeout({"duration": 30}))

    def test_liveness_remote_db_uses_longer_timeouts(self):
        db_crashtest = self.load_db_crashtest()
        db_crashtest.is_remote_db = True

        params = db_crashtest.gen_cmd_params(self.build_mode_args())

        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC
            * db_crashtest.REMOTE_DB_LIVENESS_TIMEOUT_MULTIPLIER,
            params["duration"],
        )
        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_NO_PROGRESS_TIMEOUT_SEC
            * db_crashtest.REMOTE_DB_LIVENESS_TIMEOUT_MULTIPLIER,
            params["liveness_no_progress_timeout_sec"],
        )
        self.assertEqual(
            params["duration"], db_crashtest.liveness_timeout({"duration": 0})
        )

        params = db_crashtest.gen_cmd_params(
            self.build_mode_args(duration=17, liveness_no_progress_timeout_sec=7)
        )

        self.assertEqual(17, params["duration"])
        self.assertEqual(7, params["liveness_no_progress_timeout_sec"])

    def test_liveness_wrapper_timeout_is_successful_end_of_run(self):
        db_crashtest = self.load_db_crashtest()
        execute_calls = []
        cleanups = []

        def fake_execute_cmd(cmd, timeout=None, timeout_pstack=False, expected_to_timeout=True):
            execute_calls.append((cmd, timeout, timeout_pstack, expected_to_timeout))
            return (
                True,
                -15,
                "Received signal 15 (Terminated)\n",
                "",
                123,
            )

        db_crashtest.execute_cmd = fake_execute_cmd
        db_crashtest.print_fault_injection_log = lambda pid: None
        db_crashtest.cleanup_after_success = lambda db_arg, num_dbs=1: cleanups.append(
            (db_arg, num_dbs)
        )

        db_crashtest.liveness_main(self.build_mode_args(duration=17), [])

        self.assertEqual(1, len(execute_calls))
        self.assertEqual(17, execute_calls[0][1])
        self.assertFalse(execute_calls[0][2])
        self.assertFalse(execute_calls[0][3])
        self.assertIn("--expected_values_dir=", execute_calls[0][0])
        self.assertEqual(1, len(cleanups))

    def test_remote_liveness_uses_local_expected_values_dir(self):
        db_crashtest = self.load_db_crashtest()
        os.environ[_TEST_DIR_ENV_VAR] = "/dev_test/rocksdb_crash_test/job123"
        db_crashtest.is_remote_db = True
        self.register_expected_values_dir_cleanup(db_crashtest)
        self.register_diagnostics_dir_cleanup(db_crashtest)
        execute_calls = []

        def fake_execute_cmd(
            cmd, timeout=None, timeout_pstack=False, expected_to_timeout=True
        ):
            execute_calls.append(cmd)
            return (
                True,
                -15,
                "Received signal 15 (Terminated)\n",
                "",
                123,
            )

        def expected_values_dir_from(cmd):
            values = [
                arg.split("=", 1)[1]
                for arg in cmd
                if arg.startswith("--expected_values_dir=")
            ]
            self.assertEqual(1, len(values))
            return values[0]

        db_crashtest.execute_cmd = fake_execute_cmd
        db_crashtest.print_fault_injection_log = lambda pid: None
        db_crashtest.cleanup_after_success = lambda db_arg, num_dbs=1: None

        db_crashtest.liveness_main(self.build_mode_args(duration=17), [])

        self.assertEqual(1, len(execute_calls))
        expected_values_dir = expected_values_dir_from(execute_calls[0])
        self.assertTrue(expected_values_dir)
        self.assertEqual(
            os.path.realpath(tempfile.gettempdir()),
            os.path.dirname(os.path.realpath(expected_values_dir)),
        )
        self.assertTrue(os.path.isdir(expected_values_dir))

        explicit_dir = os.path.join(self.test_tmpdir, "explicit_expected")
        db_crashtest.get_ev_parent_dir = lambda: self.fail(
            "explicit expected_values_dir should not allocate a default"
        )
        db_crashtest.liveness_main(
            self.build_mode_args(duration=17, expected_values_dir=explicit_dir), []
        )

        self.assertEqual(2, len(execute_calls))
        self.assertEqual(explicit_dir, expected_values_dir_from(execute_calls[1]))
        self.assertTrue(os.path.isdir(explicit_dir))

    def test_remote_uri_forms_are_normalized_for_workload_and_cleanup(self):
        os.environ[_TEST_DIR_ENV_VAR] = "/dev_test/rocksdb_crash_test/job123"
        uri = "remote://example/db"

        for flag in ("--env_uri", "--fs_uri"):
            for form in ("equals", "spaced"):
                with self.subTest(flag=flag, form=form):
                    remote_args = (
                        [flag + "=" + uri] if form == "equals" else [flag, uri]
                    )
                    db_crashtest = self.load_db_crashtest(
                        [
                            "liveness",
                            "--duration=17",
                            "--stress_cmd=/bin/db_stress",
                        ]
                        + remote_args
                    )
                    self.register_expected_values_dir_cleanup(db_crashtest)
                    workload_calls = []
                    cleanup_calls = []
                    expected_values_dirs = []

                    def fake_execute_cmd(
                        cmd,
                        timeout=None,
                        timeout_pstack=False,
                        expected_to_timeout=True,
                    ):
                        workload_calls.append(cmd)
                        expected_values_dirs.extend(
                            arg.split("=", 1)[1]
                            for arg in cmd
                            if arg.startswith("--expected_values_dir=")
                        )
                        return (
                            True,
                            -15,
                            "Received signal 15 (Terminated)\n",
                            "",
                            123,
                        )

                    def fake_cleanup_call(cmd, env=None):
                        cleanup_calls.append(cmd)
                        return 0

                    db_crashtest.execute_cmd = fake_execute_cmd
                    db_crashtest.print_fault_injection_log = lambda pid: None
                    with mock.patch.object(
                        db_crashtest.subprocess,
                        "call",
                        side_effect=fake_cleanup_call,
                    ):
                        db_crashtest.main()

                    normalized_arg = flag + "=" + uri
                    self.assertTrue(db_crashtest.is_remote_db)
                    self.assertEqual(1, db_crashtest.remain_args.count(normalized_arg))
                    self.assertEqual(1, len(workload_calls))
                    self.assertEqual(1, workload_calls[0].count(normalized_arg))
                    self.assertNotIn(flag, workload_calls[0])
                    self.assertEqual(1, len(cleanup_calls))
                    self.assertEqual(1, cleanup_calls[0].count(normalized_arg))
                    self.assertNotIn(flag, cleanup_calls[0])
                    self.assertEqual(1, len(expected_values_dirs))
                    self.assertTrue(expected_values_dirs[0])
                    self.assertEqual(
                        os.path.realpath(tempfile.gettempdir()),
                        os.path.dirname(os.path.realpath(expected_values_dirs[0])),
                    )

    def test_remote_uri_missing_value_does_not_consume_following_option(self):
        db_crashtest = self.load_db_crashtest()

        for flag in ("--env_uri", "--fs_uri"):
            for following_option in ("-x", "--duration=17"):
                with self.subTest(flag=flag, following_option=following_option):
                    args = [flag, following_option]
                    self.assertEqual(
                        args,
                        db_crashtest.normalize_remote_db_args(args),
                    )
                    self.assertFalse(db_crashtest.remote_db_enabled(args))

    def test_remote_uri_last_value_wins_and_cleanup_preserves_order(self):
        cases = (
            (["--env_uri=first", "--env_uri="], False),
            (["--env_uri=", "--env_uri=last"], True),
            (["--env_uri=first", "--fs_uri="], True),
            (["--env_uri=", "--fs_uri="], False),
        )

        for remote_args, expected_remote in cases:
            with self.subTest(remote_args=remote_args):
                db_crashtest = self.load_db_crashtest(
                    ["liveness"] + remote_args
                )
                db_crashtest.stress_cmd = "/bin/db_stress"

                self.assertEqual(expected_remote, db_crashtest.is_remote_db)
                self.assertEqual(remote_args, db_crashtest.remain_args[1:])
                with mock.patch.object(
                    db_crashtest.subprocess,
                    "call",
                    return_value=0,
                ) as cleanup_call:
                    db_crashtest.cleanup_after_success("/remote/db")
                self.assertEqual(remote_args, cleanup_call.call_args.args[0][4:])

    def test_liveness_can_target_transaction_lock_manager(self):
        db_crashtest = self.load_db_crashtest()

        # Transaction mode remains composable with liveness mode, which lets
        # the same watchdog cover point-lock-manager deadlocks/livelocks.
        params = db_crashtest.gen_cmd_params(self.build_mode_args(txn=True))

        self.assertEqual(1, params["use_txn"])
        self.assertEqual(0, params["use_optimistic_txn"])
        self.assertIn("use_per_key_point_lock_mgr", params)
        self.assertEqual(0, params.get("kill_random_test", 0))
        self.assertGreater(params["liveness_no_progress_timeout_sec"], 0)

    # Goal: verify db_crashtest decodes the headerless streaming binary fault
    # injection log format used on crash paths while keeping stdout concise.
    # The test writes a raw .bin file with two complete entries plus a
    # truncated tail, then checks the decoded text artifact and the summary
    # line printed to stdout.
    def test_print_fault_injection_log_decodes_streaming_raw_trace(self):
        db_crashtest = self.load_db_crashtest()
        fault_parser = self.load_fault_injection_log_parser()
        pid = 5151
        log_dir = os.path.join(self.test_tmpdir, "fault_injection_logs")
        os.makedirs(log_dir)
        raw_log = os.path.join(log_dir, f"fault_injection_{pid}_1.bin")
        decoded_log = raw_log + ".txt"

        entry0 = fault_parser.ENTRY_STRUCT.pack(
            123456789,
            17,
            7,
            4,
            0,
            0,
            b"abcd".ljust(48, b"\0"),
            fault_parser.DETAIL_KIND_OFFSET_SIZE_AND_HEAD,
            4,
            1,
            0,
            b"Append\0".ljust(32, b"\0"),
            b"/tmp/000001.log\0".ljust(72, b"\0"),
            b"injected write error\0".ljust(56, b"\0"),
        )
        entry1 = fault_parser.ENTRY_STRUCT.pack(
            123456790,
            23,
            0,
            6,
            0,
            0,
            b"/tmp/b".ljust(48, b"\0"),
            fault_parser.DETAIL_KIND_TWO_FILES,
            6,
            0,
            1,
            b"Rename\0".ljust(32, b"\0"),
            b"/tmp/a\0".ljust(72, b"\0"),
            b"injected metadata read error\0".ljust(56, b"\0"),
        )
        with open(raw_log, "wb") as f:
            f.write(entry0)
            f.write(entry1)
            f.write(b"tail")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            db_crashtest.print_fault_injection_log(pid)

        self.assertTrue(os.path.exists(decoded_log))
        with open(decoded_log) as f:
            decoded_text = f.read()

        self.assertIn(
            'Append("/tmp/000001.log", offset=7, size=4, head=[61 62 63 64])',
            decoded_text,
        )
        self.assertIn("IO error: injected write error [retryable]", decoded_text)
        self.assertIn(
            'Rename("/tmp/a", "/tmp/b") -> IO error: injected metadata read error [data_loss]',
            decoded_text,
        )
        self.assertIn("max=unbounded", decoded_text)
        self.assertIn(
            "Fault injection log saved: raw=%s decoded=%s entries=2"
            % (raw_log, decoded_log),
            stdout.getvalue(),
        )

    # Goal: verify fault-injection logs do not follow a remote TEST_TMPDIR.
    # The test marks the DB as remote, points TEST_TMPDIR at a remote-looking
    # path, and checks db_crashtest uses the same local staging root as
    # db_stress.
    def test_fault_injection_log_dir_uses_local_tmp_for_remote_db(self):
        db_crashtest = self.load_db_crashtest()
        os.environ[_TEST_DIR_ENV_VAR] = "/dev_test/rocksdb_crash_test/job123"
        db_crashtest.is_remote_db = True

        self.assertEqual(
            os.path.join("/tmp", "fault_injection_logs"),
            db_crashtest._fault_injection_log_dir(),
        )


if __name__ == "__main__":
    unittest.main()
