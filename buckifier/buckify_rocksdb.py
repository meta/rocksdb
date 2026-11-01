#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

try:
    from builtins import str
except ImportError:
    from __builtin__ import str
import fnmatch
import json
import os
import sys

from targets_builder import TARGETSBuilder, LiteralValue

from util import ColorString

# This script generates BUCK file for Buck.
# Buck is a build tool specifying dependencies among different build targets.
# User can pass extra dependencies as a JSON object via command line, and this
# script can include these dependencies in the generate BUCK file.
# Usage:
# $python3 buckifier/buckify_rocksdb.py
# (This generates a TARGET file without user-specified dependency for unit
# tests.)
# $python3 buckifier/buckify_rocksdb.py \
#        '{"fake": {
#                      "extra_deps": [":test_dep", "//fakes/module:mock1"],
#                      "extra_compiler_flags": ["-DFOO_BAR", "-Os"]
#                  }
#         }'
# (Generated BUCK file has test_dep and mock1 as dependencies for RocksDB
# unit tests, and will use the extra_compiler_flags to compile the unit test
# source.)

# tests to export as libraries for inclusion in other projects
_EXPORTED_TEST_LIBS = ["env_basic_test"]

_TEST_LIB_HEADERS = [
    "db/db_test_util.h",
    "db/db_with_timestamp_test_util.h",
    "table/mock_table.h",
    "test_util/mock_time_env.h",
    "test_util/secondary_cache_test_util.h",
    "test_util/simple_external_table_factory.h",
    "test_util/testharness.h",
    "test_util/testutil.h",
    "tools/block_cache_analyzer/block_cache_trace_analyzer.h",
    "tools/trace_analyzer_tool.h",
    "utilities/agg_merge/test_agg_merge.h",
    "utilities/cassandra/test_utils.h",
]

_TEST_HEADERS = {
    "configurable_test": ["options/configurable_test.h"],
    "persistent_cache_test": ["utilities/persistent_cache/persistent_cache_test.h"],
    "point_lock_manager_test": [
        "utilities/transactions/lock/point/point_lock_manager_test.h",
    ],
    "transaction_test": ["utilities/transactions/transaction_test.h"],
}

_STRESS_LIB_HEADERS = [
    "db_stress_tool/db_stress_common.h",
    "db_stress_tool/db_stress_compaction_service.h",
    "db_stress_tool/db_stress_compression_manager.h",
    "db_stress_tool/db_stress_driver.h",
    "db_stress_tool/db_stress_filters.h",
    "db_stress_tool/db_stress_listener.h",
    "db_stress_tool/db_stress_shared_state.h",
    "db_stress_tool/db_stress_test_base.h",
    "db_stress_tool/db_stress_wide_merge_operator.h",
    "db_stress_tool/expected_state.h",
    "db_stress_tool/expected_value.h",
    "db_stress_tool/multi_ops_txns_stress.h",
    "test_util/simple_external_table_factory.h",
    "test_util/testutil.h",
    "tools/block_cache_analyzer/block_cache_trace_analyzer.h",
    "tools/trace_analyzer_tool.h"
]

# Parse src.mk files as a Dictionary of
# VAR_NAME => list of files
def parse_src_mk(repo_path):
    src_mk = repo_path + "/src.mk"
    src_files = {}
    for line in open(src_mk):
        line = line.strip()
        if len(line) == 0 or line[0] == "#":
            continue
        if "=" in line:
            current_src = line.split("=")[0].strip()
            src_files[current_src] = []
        elif ".c" in line:
            src_path = line.split("\\")[0].strip()
            src_files[current_src].append(src_path)
    return src_files


# get all .cc / .c files
def get_cc_files(repo_path):
    cc_files = []
    for root, _dirnames, filenames in os.walk(
        repo_path
    ):  # noqa: B007 T25377293 Grandfathered in
        root = root[(len(repo_path) + 1) :]
        if "java" in root:
            # Skip java
            continue
        for filename in fnmatch.filter(filenames, "*.cc"):
            cc_files.append(os.path.join(root, filename))
        for filename in fnmatch.filter(filenames, "*.c"):
            cc_files.append(os.path.join(root, filename))
    return cc_files


# Get non_parallel tests from Makefile
def get_non_parallel_tests(repo_path):
    Makefile = repo_path + "/Makefile"

    s = set({})

    found_non_parallel_tests = False
    for line in open(Makefile):
        line = line.strip()
        if line.startswith("NON_PARALLEL_TEST ="):
            found_non_parallel_tests = True
        elif found_non_parallel_tests:
            if line.endswith("\\"):
                # remove the trailing \
                line = line[:-1]
                line = line.strip()
                s.add(line)
            else:
                # we consumed all the non_parallel tests
                break

    return s


# Parse extra dependencies passed by user from command line
def get_dependencies():
    deps_map = {"": {"extra_deps": [], "extra_compiler_flags": []}}
    if len(sys.argv) < 2:
        return deps_map

    def encode_dict(data):
        rv = {}
        for k, v in data.items():
            if isinstance(v, dict):
                v = encode_dict(v)
            rv[k] = v
        return rv

    extra_deps = json.loads(sys.argv[1], object_hook=encode_dict)
    for target_alias, deps in extra_deps.items():
        deps_map[target_alias] = deps
    return deps_map


# Prepare BUCK file for buck
def generate_buck(repo_path, deps_map):
    print(ColorString.info("Generating BUCK"))
    # parsed src.mk file
    src_mk = parse_src_mk(repo_path)
    # get all .cc files
    cc_files = get_cc_files(repo_path)
    # get non_parallel tests from Makefile
    non_parallel_tests = get_non_parallel_tests(repo_path)

    if src_mk is None or cc_files is None or non_parallel_tests is None:
        return False

    extra_argv = ""
    if len(sys.argv) >= 2:
        # Heuristically quote and canonicalize whitespace for inclusion
        # in how the file was generated.
        extra_argv = " '{}'".format(" ".join(sys.argv[1].split()))

    BUCK = TARGETSBuilder("%s/BUCK" % repo_path, extra_argv)

    # Add oncall("rocksdb_point_of_contact") at the top
    BUCK.add_oncall("rocksdb_point_of_contact")

    # rocksdb_lib
    BUCK.add_library(
        "rocksdb_lib",
        src_mk["LIB_SOURCES"] +
        # always add range_tree, it's only excluded on ppc64, which we don't use internally
        src_mk["RANGE_TREE_SOURCES"] + src_mk["TOOL_LIB_SOURCES"],
        deps=[
            "fbsource//xplat/folly:executor",
            "fbsource//xplat/folly/container:f14_hash",
            "fbsource//xplat/folly/coro:baton",
            "fbsource//xplat/folly/coro:blocking_wait",
            "fbsource//xplat/folly/coro:collect",
            "fbsource//xplat/folly/coro:coroutine",
            "fbsource//xplat/folly/coro:nothrow",
            "fbsource//xplat/folly/coro:task",
            "fbsource//xplat/folly/executors:io_executor",
            "fbsource//xplat/folly/executors:io_thread_pool_executor",
            "fbsource//xplat/folly/executors/thread_factory:named_thread_factory",
            "fbsource//xplat/folly/io/async:async_base",
            "fbsource//xplat/folly/io/async:event_base_manager",
            "fbsource//xplat/folly/io/async:io_uring_backend",
            "fbsource//xplat/folly/io/async:request_context",
            "fbsource//xplat/folly/synchronization:distributed_mutex",
        ],
        headers=LiteralValue("glob([\"**/*.h\"])")
    )
    # rocksdb_whole_archive_lib
    BUCK.add_library(
        "rocksdb_whole_archive_lib",
        [],
        deps=[
            ":rocksdb_lib",
        ],
        extra_external_deps="",
        link_whole=True,
    )
    # rocksdb_with_faiss_lib
    BUCK.add_library(
        "rocksdb_with_faiss_lib",
        src_mk.get("WITH_FAISS_LIB_SOURCES", []),
        deps=[
            "//faiss:faiss",
            ":rocksdb_lib",
        ],
    )
    test_lib_sources = (
        src_mk.get("MOCK_LIB_SOURCES", [])
        + src_mk.get("TEST_LIB_SOURCES", [])
        + src_mk.get("EXP_LIB_SOURCES", [])
        + src_mk.get("ANALYZER_LIB_SOURCES", [])
    )
    # Older release source lists may not compile every test helper yet.
    test_lib_headers = [
        header for header in _TEST_LIB_HEADERS
        if header[:-2] + ".cc" in test_lib_sources
    ]
    # rocksdb_test_lib
    BUCK.add_library(
        "rocksdb_test_lib",
        test_lib_sources,
        [":rocksdb_lib"],
        extra_test_libs=True,
        headers=test_lib_headers,
    )
    # rocksdb_with_faiss_test_lib
    BUCK.add_library(
        "rocksdb_with_faiss_test_lib",
        test_lib_sources,
        deps=[
            ":rocksdb_with_faiss_lib",
        ],
        extra_test_libs=True,
        headers=test_lib_headers,
    )
    # rocksdb_tools_lib
    BUCK.add_library(
        "rocksdb_tools_lib",
        src_mk.get("BENCH_LIB_SOURCES", [])
        + src_mk.get("ANALYZER_LIB_SOURCES", [])
        + ["test_util/testutil.cc"],
        [":rocksdb_lib"],
        headers=[
            "test_util/testutil.h",
            "tools/block_cache_analyzer/block_cache_trace_analyzer.h",
            "tools/simulated_hybrid_file_system.h",
            "tools/trace_analyzer_tool.h",
        ],
    )
    # rocksdb_cache_bench_tools_lib
    BUCK.add_library(
        "rocksdb_cache_bench_tools_lib",
        src_mk.get("CACHE_BENCH_LIB_SOURCES", []),
        [":rocksdb_lib"],
    )
    # rocksdb_point_lock_bench_tools_lib
    BUCK.add_library(
        "rocksdb_point_lock_bench_tools_lib",
        src_mk.get("POINT_LOCK_BENCH_LIB_SOURCES", []),
        [":rocksdb_lib"],
    )
    stress_sources = (
        src_mk.get("ANALYZER_LIB_SOURCES", [])
        + src_mk.get("STRESS_LIB_SOURCES", [])
        + ["test_util/testutil.cc"]
    )
    # rocksdb_stress_lib
    BUCK.add_rocksdb_library(
        "rocksdb_stress_lib",
        stress_sources,
        headers=[
            header for header in _STRESS_LIB_HEADERS
            if header[:-2] + ".cc" in stress_sources
        ],
    )
    # ldb binary
    BUCK.add_binary(
        "ldb", ["tools/ldb.cc"], [":rocksdb_tools_lib"]
    )
    BUCK.add_binary(
        "db_split_merge", ["tools/db_split_merge.cc"], [":rocksdb_lib"]
    )
    # db_stress binary
    BUCK.add_binary(
        "db_stress", ["db_stress_tool/db_stress.cc"], [":rocksdb_stress_lib"]
    )
    # db_bench binary
    BUCK.add_binary(
        "db_bench", ["tools/db_bench.cc"], [":rocksdb_tools_lib"]
    )
    # cache_bench binary
    BUCK.add_binary(
        "cache_bench", ["cache/cache_bench.cc"], [":rocksdb_cache_bench_tools_lib"]
    )
    # point_lock_bench binary
    BUCK.add_binary(
        "point_lock_bench",
        ["utilities/transactions/lock/point/point_lock_bench.cc"],
        [":rocksdb_point_lock_bench_tools_lib"]
    )
    # bench binaries
    for src in src_mk.get("MICROBENCH_SOURCES", []):
        name = src.rsplit("/", 1)[1].split(".")[0] if "/" in src else src.split(".")[0]
        BUCK.add_binary(name, [src], [], extra_bench_libs=True)
    print(f"Extra dependencies:\n{json.dumps(deps_map)}")

    # Dictionary test executable name -> relative source file path
    test_source_map = {}

    # c_test.c is added through BUCK.add_c_test(). If there
    # are more than one .c test file, we need to extend
    # BUCK.add_c_test() to include other C tests too.
    for test_src in src_mk.get("TEST_MAIN_SOURCES_C", []):
        if test_src != "db/c_test.c":
            print("Don't know how to deal with " + test_src)
            return False
    BUCK.add_c_test()

    try:
        with open(f"{repo_path}/buckifier/bench.json") as json_file:
            fast_fancy_bench_config_list = json.load(json_file)
            for config_dict in fast_fancy_bench_config_list:
                clean_benchmarks = {}
                benchmarks = config_dict["benchmarks"]
                for binary, benchmark_dict in benchmarks.items():
                    clean_benchmarks[binary] = {}
                    for benchmark, overloaded_metric_list in benchmark_dict.items():
                        clean_benchmarks[binary][benchmark] = []
                        for metric in overloaded_metric_list:
                            if not isinstance(metric, dict):
                                clean_benchmarks[binary][benchmark].append(metric)
                BUCK.add_fancy_bench_config(
                    config_dict["name"],
                    clean_benchmarks,
                    False,
                    config_dict["expected_runtime_one_iter"],
                    config_dict["sl_iterations"],
                    config_dict["regression_threshold"],
                )

        with open(f"{repo_path}/buckifier/bench-slow.json") as json_file:
            slow_fancy_bench_config_list = json.load(json_file)
            for config_dict in slow_fancy_bench_config_list:
                clean_benchmarks = {}
                benchmarks = config_dict["benchmarks"]
                for binary, benchmark_dict in benchmarks.items():
                    clean_benchmarks[binary] = {}
                    for benchmark, overloaded_metric_list in benchmark_dict.items():
                        clean_benchmarks[binary][benchmark] = []
                        for metric in overloaded_metric_list:
                            if not isinstance(metric, dict):
                                clean_benchmarks[binary][benchmark].append(metric)
            for config_dict in slow_fancy_bench_config_list:
                BUCK.add_fancy_bench_config(
                    config_dict["name"] + "_slow",
                    clean_benchmarks,
                    True,
                    config_dict["expected_runtime_one_iter"],
                    config_dict["sl_iterations"],
                    config_dict["regression_threshold"],
                )
    # it is better servicelab experiments break
    # than rocksdb github ci
    except Exception:
        pass

    BUCK.add_test_header()

    for test_src in src_mk.get("TEST_MAIN_SOURCES", []):
        test = test_src.split(".c")[0].strip().split("/")[-1].strip()
        test_source_map[test] = (test_src, False)
        print("" + test + " " + test_src)

    for test_src in src_mk.get("WITH_FAISS_TEST_MAIN_SOURCES", []):
        test = test_src.split(".c")[0].strip().split("/")[-1].strip()
        test_source_map[test] = (test_src, True)
        print("" + test + " " + test_src + " [FAISS]")

    for target_alias, deps in deps_map.items():
        for test, (test_src, with_faiss) in sorted(test_source_map.items()):
            if len(test) == 0:
                print(ColorString.warning("Failed to get test name for %s" % test_src))
                continue

            test_target_name = test if not target_alias else test + "_" + target_alias

            if test in _EXPORTED_TEST_LIBS:
                test_library = "%s_lib" % test_target_name
                BUCK.add_library(
                    test_library,
                    [test_src],
                    deps=[":rocksdb_test_lib"],
                    extra_test_libs=True,
                )
                BUCK.register_test(
                    test_target_name,
                    test_src,
                    deps=json.dumps(deps["extra_deps"] + [":" + test_library]),
                    extra_compiler_flags=json.dumps(deps["extra_compiler_flags"]),
                    source_headers=_TEST_HEADERS.get(test, []),
                )
            else:
                if with_faiss:
                    BUCK.register_test(
                        test_target_name,
                        test_src,
                        deps=json.dumps(deps["extra_deps"] + [":rocksdb_with_faiss_test_lib"]),
                        extra_compiler_flags=json.dumps(deps["extra_compiler_flags"]),
                        source_headers=_TEST_HEADERS.get(test, []),
                    )
                else:
                    BUCK.register_test(
                        test_target_name,
                        test_src,
                        deps=json.dumps(deps["extra_deps"] + [":rocksdb_test_lib"]),
                        extra_compiler_flags=json.dumps(deps["extra_compiler_flags"]),
                        source_headers=_TEST_HEADERS.get(test, []),
                    )
    BUCK.export_file("tools/db_crashtest.py")
    BUCK.export_file("tools/fault_injection_log_parser.py")

    print(ColorString.info("Generated BUCK Summary:"))
    print(ColorString.info("- %d libs" % BUCK.total_lib))
    print(ColorString.info("- %d binarys" % BUCK.total_bin))
    print(ColorString.info("- %d tests" % BUCK.total_test))
    return True


def get_rocksdb_path():
    # rocksdb = {script_dir}/..
    script_dir = os.path.dirname(sys.argv[0])
    script_dir = os.path.abspath(script_dir)
    rocksdb_path = os.path.abspath(os.path.join(script_dir, "../"))

    return rocksdb_path


def exit_with_error(msg):
    print(ColorString.error(msg))
    sys.exit(1)


def main():
    deps_map = get_dependencies()
    # Generate BUCK file for buck
    ok = generate_buck(get_rocksdb_path(), deps_map)
    if not ok:
        exit_with_error("Failed to generate BUCK files")


if __name__ == "__main__":
    main()
