#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# The RocksDB regression test script.
# REQUIREMENT: must be able to run make db_bench in the current directory
#
# This script will do the following things in order:
#
# 1. check out the specified rocksdb commit.
# 2. build db_bench using the specified commit
# 3. setup test directory $TEST_PATH.  If not specified, then the test directory
#    will be "/tmp/rocksdb/regression_test"
# 4. run set of benchmarks on the specified host
#    (can be either locally or remotely)
# 5. generate report in the $RESULT_PATH.  If RESULT_PATH is not specified,
#    RESULT_PATH will be set to $TEST_PATH/current_time
#
# = Examples =
# * Run the regression test using rocksdb commit abcdef that outputs results
#   and temp files in "/my/output/dir"
#r
#   TEST_PATH=/my/output/dir COMMIT_ID=abcdef ./tools/regression_test.sh
#
# * Run the regression test on a remost host under "/my/output/dir" directory
#   and stores the result locally in "/my/benchmark/results" using commit
#   abcdef and with the rocksdb options specified in /my/path/to/OPTIONS-012345
#   with 1000000000 keys in each benchmark in the regression test where each
#   key and value are 100 and 900 bytes respectively:
#
#   REMOTE_USER_AT_HOST=yhchiang@my.remote.host \
#       TEST_PATH=/my/output/dir \
#       RESULT_PATH=/my/benchmark/results \
#       COMMIT_ID=abcdef \
#       OPTIONS_FILE=/my/path/to/OPTIONS-012345 \
#       NUM_KEYS=1000000000 \
#       KEY_SIZE=100 \
#       VALUE_SIZE=900 \
#       ./tools/regression_test.sh
#
# = Regression test environmental parameters =
#   DEBUG: If true, then the script will not build db_bench if db_bench already
#       exists
#       Default: 0
#   TEST_MODE: If 1, run fillseqdeterminstic and benchmarks both
#       if 0, only run fillseqdeterministc
#       if 2, only run benchmarks
#       Default: 1
#   TEST_PATH: the root directory of the regression test.
#       Default: "/tmp/rocksdb/regression_test"
#       !!! NOTE !!! - a DB will also be saved in $TEST_PATH/../db
#   RESULT_PATH: the directory where the regression results will be generated.
#       Default: "$TEST_PATH/current_time"
#   REMOTE_USER_AT_HOST: If set, then test will run on the specified host under
#       TEST_PATH directory and outputs test results locally in RESULT_PATH
#       The REMOTE_USER_AT_HOST should follow the format user-id@host.name
#   DB_PATH: the path where the rocksdb database will be created during the
#       regression test.  Default:  $TEST_PATH/db
#   WAL_PATH: the path where the rocksdb WAL will be outputted.
#       Default:  $TEST_PATH/wal
#   OPTIONS_FILE:  If specified, then the regression test will use the specified
#       file to initialize the RocksDB options in its benchmarks.  Note that
#       this feature only work for commits after 88acd93 or rocksdb version
#       later than 4.9.
#   DELETE_TEST_PATH: If true, then the test directory will be deleted
#       after the script ends.
#       Default: 0
#
# = db_bench parameters =
#   NUM_THREADS:  The number of concurrent foreground threads that will issue
#       database operations in the benchmark.  Default: 16.
#   NUM_KEYS:  The key range that will be used in the entire regression test.
#       Default: 1G.
#   NUM_OPS:  The number of operations (reads, writes, or deletes) that will
#       be issued in EACH thread.
#       Default: $NUM_KEYS / $NUM_THREADS
#   KEY_SIZE:  The size of each key in bytes in db_bench.  Default: 100.
#   VALUE_SIZE:  The size of each value in bytes in db_bench.  Default: 900.
#   CACHE_SIZE:  The size of RocksDB block cache used in db_bench.  Default: 1G
#   STATISTICS:  If 1, then statistics is on in db_bench.  Default: 0.
#   COMPRESSION_RATIO:  The compression ratio of the key generated in db_bench.
#       Default: 0.5.
#   HISTOGRAM:  If 1, then the histogram feature on performance feature is on.
#   STATS_PER_INTERVAL:  If 1, then the statistics will be reported for every
#       STATS_INTERVAL_SECONDS seconds.  Default 1.
#   STATS_INTERVAL_SECONDS:  If STATS_PER_INTERVAL is set to 1, then statistics
#       will be reported for every STATS_INTERVAL_SECONDS.  Default 60.
#   MAX_BACKGROUND_FLUSHES:  The maximum number of concurrent flushes in
#       db_bench.  Default: 4.
#   MAX_BACKGROUND_COMPACTIONS:  The maximum number of concurrent compactions
#       in db_bench.  Default: 16.
#   NUM_HIGH_PRI_THREADS:  The number of high-pri threads available for
#       concurrent flushes in db_bench.  Default: 4.
#   NUM_LOW_PRI_THREADS:  The number of low-pri threads available for
#       concurrent compactions in db_bench.  Default: 16.
#   SEEK_NEXTS:  Controls how many Next() will be called after seek.
#       Default: 10.
#   SEED:  random seed that controls the randomness of the benchmark.
#       Default: $( date +%s )

#==============================================================================
#  CONSTANT
#==============================================================================
TITLE_FORMAT="%40s,%25s,%30s,%7s,%9s,%8s,"
TITLE_FORMAT+="%10s,%13s,%14s,%11s,%12s,"
TITLE_FORMAT+="%7s,%11s,"
TITLE_FORMAT+="%9s,%10s,%10s,%10s,%10s,%10s,%5s,"
TITLE_FORMAT+="%5s,%5s,%5s" # time
TITLE_FORMAT+="\n"

DATA_FORMAT="%40s,%25s,%30s,%7s,%9s,%8s,"
DATA_FORMAT+="%10s,%13.0f,%14s,%11s,%12s,"
DATA_FORMAT+="%7s,%11s,"
DATA_FORMAT+="%9.0f,%10.0f,%10.0f,%10.0f,%10.0f,%10.0f,%5.0f,"
DATA_FORMAT+="%5.0f,%5.0f,%5.0f" # time
DATA_FORMAT+="\n"

# In case of async_io, $1 is benchmark_asyncio
MAIN_PATTERN="${1%%_*}""[[:blank:]]+:.*[[:blank:]]+([0-9\.]+)[[:blank:]]+ops/sec"
PERC_PATTERN="Percentiles: P50: ([0-9\.]+) P75: ([0-9\.]+) "
PERC_PATTERN+="P99: ([0-9\.]+) P99.9: ([0-9\.]+) P99.99: ([0-9\.]+)"
#==============================================================================

function main {
  TEST_ROOT_DIR=${TEST_PATH:-"/tmp/rocksdb/regression_test"}
  init_arguments $TEST_ROOT_DIR
  arm_cleanup_trap $TEST_ROOT_DIR

  build_db_bench_and_ldb

  setup_test_directory
  if [ $TEST_MODE -le 1 ]; then
      echo "Building DB..."
      # compactall alone will not print ops or threads, which will fail update_report
     run_db_bench "fillseq,compactall" $NUM_KEYS 1 0 0
     # only save for future use on success
     test_remote "mv $DB_PATH $ORIGIN_PATH"
  fi
  if [ $TEST_MODE -ge 1 ]; then
      build_checkpoint

      # run_db_bench benchmark_name NUM_OPS NUM_THREADS USED_EXISTING_DB UPDATE_REPORT ASYNC_IO
      run_db_bench "seekrandom_asyncio" $NUM_OPS $NUM_THREADS  1 1 true
      run_db_bench "multireadrandom_asyncio" $NUM_OPS $NUM_THREADS  1 1 true
      run_db_bench "readrandom"
      run_db_bench "readwhilewriting"
      run_db_bench "deleterandom"
      run_db_bench "seekrandom"
      run_db_bench "seekrandomwhilewriting"
      run_db_bench "multireadrandom"
  fi

  finalize_cleanup $TEST_ROOT_DIR
  echo ""
  echo "Benchmark completed!  Results are available in $RESULT_PATH"
}

############################################################################
function arm_cleanup_trap {
  CLEANUP_DONE=0
  trap 'finalize_cleanup "$TEST_ROOT_DIR"' EXIT
}

function finalize_cleanup {
  if [ "${CLEANUP_DONE:-0}" -eq 0 ]; then
    cleanup_test_directory "$1"
    CLEANUP_DONE=1
  fi
}

############################################################################
function init_arguments {
  K=1024
  M=$((1024 * K))
  G=$((1024 * M))

  current_time=$(date +"%F-%H:%M:%S")
  RESULT_PATH=${RESULT_PATH:-"$1/results/$current_time"}
  COMMIT_ID=`hg id -i 2>/dev/null || git rev-parse HEAD 2>/dev/null || echo 'unknown'`
  SUMMARY_FILE="$RESULT_PATH/SUMMARY.csv"

  DB_PATH=${3:-"$1/db"}
  ORIGIN_PATH=${ORIGIN_PATH:-"$(dirname $(dirname $DB_PATH))/db"}
  WAL_PATH=${4:-""}
  if [ -z "$REMOTE_USER_AT_HOST" ]; then
    DB_BENCH_DIR=${5:-"."}
  else
    DB_BENCH_DIR=${5:-"$1/db_bench"}
  fi

  DEBUG=${DEBUG:-0}
  TEST_MODE=${TEST_MODE:-1}
  SCP=${SCP:-"scp"}
  SSH=${SSH:-"ssh"}
  NUM_THREADS=${NUM_THREADS:-16}
  NUM_KEYS=${NUM_KEYS:-$((1 * G))}  # key range
  NUM_OPS=${NUM_OPS:-$(($NUM_KEYS / $NUM_THREADS))}
  KEY_SIZE=${KEY_SIZE:-100}
  VALUE_SIZE=${VALUE_SIZE:-900}
  CACHE_SIZE=${CACHE_SIZE:-$((1 * G))}
  STATISTICS=${STATISTICS:-0}
  COMPRESSION_RATIO=${COMPRESSION_RATIO:-0.5}
  HISTOGRAM=${HISTOGRAM:-1}
  NUM_MULTI_DB=${NUM_MULTI_DB:-1}
  STATS_PER_INTERVAL=${STATS_PER_INTERVAL:-1}
  STATS_INTERVAL_SECONDS=${STATS_INTERVAL_SECONDS:-600}
  MAX_BACKGROUND_FLUSHES=${MAX_BACKGROUND_FLUSHES:-4}
  MAX_BACKGROUND_COMPACTIONS=${MAX_BACKGROUND_COMPACTIONS:-16}
  NUM_HIGH_PRI_THREADS=${NUM_HIGH_PRI_THREADS:-4}
  NUM_LOW_PRI_THREADS=${NUM_LOW_PRI_THREADS:-16}
  DELETE_TEST_PATH=${DELETE_TEST_PATH:-0}
  SEEK_NEXTS=${SEEK_NEXTS:-10}
  SEED=${SEED:-$( date +%s )}
  MULTIREAD_BATCH_SIZE=${MULTIREAD_BATCH_SIZE:-128}
  MULTIREAD_STRIDE=${MULTIREAD_STRIDE:-12}
  PERF_LEVEL=${PERF_LEVEL:-1}
}

# $1 --- benchmark name
# $2 --- number of operations.  Default: $NUM_OPS
# $3 --- number of threads.  Default $NUM_THREADS
# $4 --- use_existing_db.  Default: 1
# $5 --- update_report. Default: 1
# $6 --- async_io. Default: False
function run_db_bench {
  # Make sure no other db_bench is running. (Make sure command succeeds if pidof
  # command exists but finds nothing.)
  pids_cmd='pidof db_bench || pidof --version > /dev/null'
  # But first, make best effort to kill any db_bench that have run for more
  # than 12 hours, as that indicates a hung or runaway process.
  kill_old_cmd='for PID in $(pidof db_bench); do [ "$(($(stat -c %Y /proc/$PID) + 43200))" -lt "$(date +%s)" ] && echo "Killing old db_bench $PID" && kill $PID && sleep 5 && kill -9 $PID && sleep 5; done; pidof --version > /dev/null'
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    pids_cmd="$SSH $REMOTE_USER_AT_HOST '$pids_cmd'"
    kill_old_cmd="$SSH $REMOTE_USER_AT_HOST '$kill_old_cmd'"
  fi

  eval $kill_old_cmd
  exit_on_error $? "$kill_old_cmd"

  pids_output="$(eval $pids_cmd)"
  exit_on_error $? "$pids_cmd"

  if [ "$pids_output" != "" ]; then
    echo "Stopped regression_test.sh as there're still recent db_bench "
    echo "processes running: $pids_output"
    echo "Clean up test directory"
    cleanup_test_directory $TEST_ROOT_DIR
    exit 2
  fi

  # Build db_bench command
  ops=${2:-$NUM_OPS}
  threads=${3:-$NUM_THREADS}
  USE_EXISTING_DB=${4:-1}
  UPDATE_REPORT=${5:-1}
  async_io=${6:-false}
  seek_nexts=$SEEK_NEXTS

  if [ "$async_io" == "true" ]; then
    if ! [ -z "$SEEK_NEXTS_ASYNC_IO" ]; then
      seek_nexts=$SEEK_NEXTS_ASYNC_IO
    fi
  fi


  echo ""
  echo "======================================================================="
  echo "Benchmark $1"
  echo "======================================================================="
  echo ""
  db_bench_error=0
  options_file_arg=$(setup_options_file)
  echo "$options_file_arg"

  # In case of async_io, benchmark is benchmark_asyncio
  db_bench_type=${1%%_*}

  # use `which time` to avoid using bash's internal time command
  db_bench_cmd="\$(which time) -p $DB_BENCH_DIR/db_bench \
      --benchmarks=$db_bench_type --db=$DB_PATH --wal_dir=$WAL_PATH \
      --use_existing_db=$USE_EXISTING_DB \
      --perf_level=$PERF_LEVEL \
      --disable_auto_compactions \
      --threads=$threads \
      --num=$NUM_KEYS \
      --reads=$ops \
      --writes=$ops \
      --deletes=$ops \
      --key_size=$KEY_SIZE \
      --value_size=$VALUE_SIZE \
      --cache_size=$CACHE_SIZE \
      --statistics=$STATISTICS \
      $options_file_arg \
      --compression_ratio=$COMPRESSION_RATIO \
      --histogram=$HISTOGRAM \
      --seek_nexts=$seek_nexts \
      --stats_per_interval=$STATS_PER_INTERVAL \
      --stats_interval_seconds=$STATS_INTERVAL_SECONDS \
      --max_background_flushes=$MAX_BACKGROUND_FLUSHES \
      --num_multi_db=$NUM_MULTI_DB \
      --max_background_compactions=$MAX_BACKGROUND_COMPACTIONS \
      --num_high_pri_threads=$NUM_HIGH_PRI_THREADS \
      --num_low_pri_threads=$NUM_LOW_PRI_THREADS \
      --seed=$SEED \
      --multiread_batched=true \
      --batch_size=$MULTIREAD_BATCH_SIZE \
      --multiread_stride=$MULTIREAD_STRIDE \
      --async_io=$async_io"

  if [ "$async_io" == "true" ]; then
    db_bench_cmd="$db_bench_cmd $(set_async_io_parameters) "
  fi

  db_bench_cmd=" $db_bench_cmd 2>&1"

  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    echo "Running benchmark remotely on $REMOTE_USER_AT_HOST"
    db_bench_cmd="$SSH $REMOTE_USER_AT_HOST '$db_bench_cmd'"
  fi
  echo db_bench_cmd="$db_bench_cmd"

  # Run the db_bench command
  eval $db_bench_cmd | tee -a "$RESULT_PATH/$1"
  exit_on_error ${PIPESTATUS[0]} db_bench
  if [ $UPDATE_REPORT -ne 0 ]; then
    update_report "$1" "$RESULT_PATH/$1" $ops $threads
  fi
}

function set_async_io_parameters {
  options=" --duration=500"
  # Below parameters are used in case of async_io only.
  # 1. If you want to run below parameters for all benchmarks, it should be
  #    specify in OPTIONS_FILE instead of exporting them.
  # 2. Below exported var takes precedence over OPTIONS_FILE.
  if ! [ -z "$MAX_READAHEAD_SIZE" ]; then
    options="$options --max_auto_readahead_size=$MAX_READAHEAD_SIZE "
  fi
  if ! [ -z "$INITIAL_READAHEAD_SIZE" ]; then
    options="$options --initial_auto_readahead_size=$INITIAL_READAHEAD_SIZE "
  fi
  if ! [ -z "$NUM_READS_FOR_READAHEAD_SIZE" ]; then
    options="$options --num_file_reads_for_auto_readahead=$NUM_READS_FOR_READAHEAD_SIZE "
  fi
  echo $options
}

function build_checkpoint {
    echo "NUM_MULTI_DB=$NUM_MULTI_DB"
    if [ $NUM_MULTI_DB -gt 1 ]; then
        run_remote "rm -rf $DB_PATH"
        run_remote "mkdir -p $DB_PATH"
        for ((db_index = 0; db_index < NUM_MULTI_DB; db_index++)); do
            run_remote "test -d $ORIGIN_PATH/$db_index"
            echo "Building checkpoint: $ORIGIN_PATH/$db_index -> $DB_PATH/$db_index ..."
            run_remote "$DB_BENCH_DIR/ldb checkpoint --checkpoint_dir=$DB_PATH/$db_index --db=$ORIGIN_PATH/$db_index --try_load_options 2>&1"
            exit_on_error $?
        done
    else
        # checkpoint cannot build in directory already exists
        run_remote "rm -rf $DB_PATH"
        echo "Building checkpoint: $ORIGIN_PATH -> $DB_PATH ..."
        run_remote "$DB_BENCH_DIR/ldb checkpoint --checkpoint_dir=$DB_PATH --db=$ORIGIN_PATH --try_load_options 2>&1"
        exit_on_error $?
    fi
}

function multiply {
  echo "$1 * $2" | bc
}

# $1 --- name of the benchmark
# $2 --- the filename of the output log of db_bench
function update_report {
  # In case of async_io, benchmark is benchmark_asyncio
  db_bench_type=${1%%_*}

  main_result=`cat $2 | grep $db_bench_type`
  exit_on_error $?
  perc_statement=`cat $2 | grep Percentile`
  exit_on_error $?

  # Obtain micros / op

  [[ $main_result =~ $MAIN_PATTERN ]]
  ops_per_s=${BASH_REMATCH[1]}

  # Obtain percentile information
  [[ $perc_statement =~ $PERC_PATTERN ]]
  perc[0]=${BASH_REMATCH[1]}  # p50
  perc[1]=${BASH_REMATCH[2]}  # p75
  perc[2]=${BASH_REMATCH[3]}  # p99
  perc[3]=${BASH_REMATCH[4]}  # p99.9
  perc[4]=${BASH_REMATCH[5]}  # p99.99

  # Parse the output of the time command
  real_sec=`tail -3 $2 | grep real | awk '{print $2}'`
  user_sec=`tail -3 $2 | grep user | awk '{print $2}'`
  sys_sec=`tail -3 $2 | grep sys | awk '{print $2}'`

  (printf "$DATA_FORMAT" \
    $COMMIT_ID $1 $REMOTE_USER_AT_HOST $NUM_MULTI_DB $NUM_KEYS $KEY_SIZE $VALUE_SIZE \
       $(multiply $COMPRESSION_RATIO 100) \
       $3 $4 $CACHE_SIZE \
       $MAX_BACKGROUND_FLUSHES $MAX_BACKGROUND_COMPACTIONS \
       $ops_per_s \
       $(multiply ${perc[0]} 1000) \
       $(multiply ${perc[1]} 1000) \
       $(multiply ${perc[2]} 1000) \
       $(multiply ${perc[3]} 1000) \
       $(multiply ${perc[4]} 1000) \
       $DEBUG \
       $real_sec \
       $user_sec \
       $sys_sec \
       >> $SUMMARY_FILE)
  exit_on_error $?
}

function exit_on_error {
  if [ $1 -ne 0 ]; then
    echo ""
    echo "ERROR: Benchmark did not complete successfully."
    if ! [ -z "$2" ]; then
      echo "Failure command: $2"
    fi
    echo "Partial results are output to $RESULT_PATH"
    echo "ERROR" >> $SUMMARY_FILE
    exit $1
  fi
}

function build_db_bench_and_ldb {
  echo "Building db_bench & ldb ..."

  make clean
  exit_on_error $?

  DEBUG_LEVEL=0 make db_bench ldb -j32
  exit_on_error $?
}

function run_remote {
  test_remote "$1"
  exit_on_error $? "$1"
}

function test_remote {
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
      cmd="$SSH $REMOTE_USER_AT_HOST '$1'"
  else
      cmd="$1"
  fi
  eval "$cmd"
}

function run_local {
  eval "$1"
  exit_on_error $? "$1"
}

function setup_options_file {
 if ! [ -z "$OPTIONS_FILE" ]; then
    if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
      options_file="$DB_BENCH_DIR/OPTIONS_FILE"
      $SCP $OPTIONS_FILE $REMOTE_USER_AT_HOST:$options_file
    else
      options_file="$OPTIONS_FILE"
    fi
    echo "--options_file=$options_file"
  fi
  echo ""
}

function setup_test_directory {
  echo "Deleting old regression test directories and creating new ones"

  run_local 'test "$DB_PATH" != "."'
  run_remote "rm -rf $DB_PATH"

  if [ "$DB_BENCH_DIR" != "." ]; then
    run_remote "rm -rf $DB_BENCH_DIR"
  fi

  run_local 'test "$RESULT_PATH" != "."'
  run_local "rm -rf $RESULT_PATH"

  if ! [ -z "$WAL_PATH" ]; then
    run_remote "rm -rf $WAL_PATH"
    run_remote "mkdir -p $WAL_PATH"
  fi

  run_remote "mkdir -p $DB_PATH"

  run_remote "mkdir -p $DB_BENCH_DIR"
  run_remote "ls -l $DB_BENCH_DIR"

  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    run_local "$SCP db_bench $REMOTE_USER_AT_HOST:$DB_BENCH_DIR/."
    run_local "$SCP ldb $REMOTE_USER_AT_HOST:$DB_BENCH_DIR/."
  fi

  run_local "mkdir -p $RESULT_PATH"

  (printf $TITLE_FORMAT \
      "commit id" "benchmark" "user@host" "num-dbs" "key-range" "key-size" \
      "value-size" "compress-rate" "ops-per-thread" "num-threads" "cache-size" \
      "flushes" "compactions" \
      "ops-per-s" "p50" "p75" "p99" "p99.9" "p99.99" "debug" \
      "real-sec" "user-sec" "sys-sec" \
      >> $SUMMARY_FILE)
  exit_on_error $?
}

function cleanup_test_directory {

  if [ $DELETE_TEST_PATH -ne 0 ]; then
    echo "Clear old regression test directories and creating new ones"
    run_remote "rm -rf $DB_PATH"
    run_remote "rm -rf $WAL_PATH"
    if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
      run_remote "rm -rf $DB_BENCH_DIR"
    fi
    run_remote "rm -rf $1"
  else
    echo "------------ DEBUG MODE ------------"
    echo "DB  PATH: $DB_PATH"
    echo "WAL PATH: $WAL_PATH"
  fi
}

############################################################################

# shellcheck disable=SC2068
main $@
