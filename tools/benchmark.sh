#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: db_bench binary exists in the current directory

# Exit Codes
EXIT_INVALID_ARGS=1
EXIT_NOT_COMPACTION_TEST=2
EXIT_UNKNOWN_JOB=3
EXIT_INVALID_PATH=4

# Size Constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

function display_usage() {
  echo "usage: benchmark.sh [--help] <test>"
  echo ""
  echo "These are the available benchmark tests:"
  echo -e "\tbulkload"
  echo -e "\tfillseq_disable_wal\t\tSequentially fill the database with no WAL"
  echo -e "\tfillseq_enable_wal\t\tSequentially fill the database with WAL"
  echo -e "\toverwrite"
  echo -e "\tupdaterandom"
  echo -e "\treadrandom"
  echo -e "\tmergerandom"
  echo -e "\tfilluniquerandom"
  echo -e "\tmultireadrandom"
  echo -e "\tfwdrange"
  echo -e "\trevrange"
  echo -e "\treadwhilewriting"
  echo -e "\treadwhilemerging"
  echo -e "\tfwdrangewhilewriting"
  echo -e "\trevrangewhilewriting"
  echo -e "\tfwdrangewhilemerging"
  echo -e "\trevrangewhilemerging"
  echo -e "\trandomtransaction"
  echo -e "\tuniversal_compaction"
  echo -e "\tdebug"
  echo ""
  echo "Generic enviroment Variables:"
  echo -e "\tJOB_ID\t\t\t\tAn identifier for the benchmark job, will appear in the results"
  echo -e "\tDB_DIR\t\t\t\tPath to write the database data directory"
  echo -e "\tWAL_DIR\t\t\t\tPath to write the database WAL directory"
  echo -e "\tOUTPUT_DIR\t\t\tPath to write the benchmark results to (default: /tmp)"
  echo -e "\tNUM_KEYS\t\t\tThe number of keys to use in the benchmark"
  echo -e "\tKEY_SIZE\t\t\tThe size of the keys to use in the benchmark (default: 20 bytes)"
  echo -e "\tVALUE_SIZE\t\t\tThe size of the values to use in the benchmark (default: 400 bytes)"
  echo -e "\tBLOCK_SIZE\t\t\tThe size of the database blocks in the benchmark (default: 8 KB)"
  echo -e "\tDB_BENCH_NO_SYNC\t\tDisable fsync on the WAL"
  echo -e "\tNUMACTL\t\t\t\tWhen defined use numactl --interleave=all"
  echo -e "\tNUM_THREADS\t\t\tThe number of threads to use (default: 64)"
  echo -e "\tMB_WRITE_PER_SEC\t\t\tRate limit for background writer"
  echo -e "\tNUM_NEXTS_PER_SEEK\t\t(default: 10)"
  echo -e "\tCACHE_SIZE\t\t\tSize of the block cache (default: 16GB)"
  echo -e "\tCACHE_NUMSHARDBITS\t\t\tNumber of shards for the block cache is 2 ** cache_numshardbits (default: 6)"
  echo -e "\tCOMPRESSION_MAX_DICT_BYTES"
  echo -e "\tCOMPRESSION_TYPE\t\tDefault compression(default: zstd)"
  echo -e "\tBOTTOMMOST_COMPRESSION\t\t(default: none)"
  echo -e "\tMIN_LEVEL_TO_COMPRESS\t\tValue for min_level_to_compress for Leveled"
  echo -e "\tCOMPRESSION_SIZE_PERCENT\tValue for compression_size_percent for Universal"
  echo -e "\tDURATION\t\t\tNumber of seconds for which the test runs"
  echo -e "\tWRITES\t\t\t\tNumber of writes for which the test runs"
  echo -e "\tWRITE_BUFFER_SIZE_MB\t\tThe size of the write buffer in MB (default: 128)"
  echo -e "\tTARGET_FILE_SIZE_BASE_MB\tThe value for target_file_size_base in MB (default: 128)"
  echo -e "\tMAX_BYTES_FOR_LEVEL_BASE_MB\tThe value for max_bytes_for_level_base in MB (default: 128)"
  echo -e "\tMAX_BACKGROUND_JOBS\t\tThe value for max_background_jobs (default: 16)"
  echo -e "\tCACHE_INDEX_AND_FILTER_BLOCKS\tThe value for cache_index_and_filter_blocks (default: 0)"
  echo -e "\tUSE_O_DIRECT\t\t\tUse O_DIRECT for user reads and compaction"
  echo -e "\tBYTES_PER_SYNC\t\t\tValue for bytes_per_sync, set to zero when USE_O_DIRECT is true"
  echo -e "\tSTATS_INTERVAL_SECONDS\t\tValue for stats_interval_seconds"
  echo -e "\tREPORT_INTERVAL_SECONDS\t\tValue for report_interval_seconds"
  echo -e "\tSUBCOMPACTIONS\t\t\tValue for subcompactions"
  echo -e "\tCOMPACTION_STYLE\t\tOne of leveled, universal, blob. Default is leveled."
  echo -e "\nEnvironment variables (mostly) for leveled compaction:"
  echo -e "\tLEVEL0_FILE_NUM_COMPACTION_TRIGGER\t\tValue for level0_file_num_compaction_trigger"
  echo -e "\tLEVEL0_SLOWDOWN_WRITES_TRIGGER\t\t\tValue for level0_slowdown_writes_trigger"
  echo -e "\tLEVEL0_STOP_WRITES_TRIGGER\t\t\tValue for level0_stop_writes_trigger"
  echo -e "\tPER_LEVEL_FANOUT\t\t\t\tValue for max_bytes_for_level_multiplier"
  echo -e "\tSOFT_PENDING_COMPACTION_BYTES_LIMIT_IN_GB\tThe value for soft_pending_compaction_bytes_limit in GB"
  echo -e "\tHARD_PENDING_COMPACTION_BYTES_LIMIT_IN_GB\tThe value for hard_pending_compaction_bytes_limit in GB"
  echo -e "\nEnvironment variables for universal compaction:"
  echo -e "\tUNIVERSAL_MIN_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\tUNIVERSAL_MAX_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\tUNIVERSAL_SIZE_RATIO\t\tValue of size_ratio option for universal"
  echo -e "\tUNIVERSAL_MAX_SIZE_AMP\t\tmax_size_amplification_percent for universal"
  echo -e "\tUNIVERSAL_ALLOW_TRIVIAL_MOVE\tSet allow_trivial_move to true for universal, default is false"
  echo -e "\nOptions for integrated BlobDB"
  echo -e "\tMIN_BLOB_SIZE\tValue for min_blob_size"
  echo -e "\tBLOB_FILE_SIZE\tValue for blob_file_size"
  echo -e "\tBLOB_COMPRESSION_TYPE\tValue for blob_compression_type"
  echo -e "\tBLOB_GC_AGE_CUTOFF\tValue for blob_garbage_collection_age_cutoff"
  echo -e "\tBLOB_GC_FORCE_THRESHOLD\tValue for blob_garbage_collection_force_threshold"
  echo -e "\tBLOB_FILE_STARTING_LEVEL\t\tBlob file starting level (default: 0)"
  echo -e "\tUSE_BLOB_CACHE\t\t\tEnable blob cache (default: 1)"
  echo -e "\tUSE_SHARED_BLOCK_AND_BLOB_CACHE\t\t\tUse the same backing cache for block cache and blob cache (default: 1)"
  echo -e "\tBLOB_CACHE_SIZE\t\t\tSize of the blob cache (default: 16GB)"
  echo -e "\tBLOB_CACHE_NUMSHARDBITS\t\t\tNumber of shards for the blob cache is 2 ** blob_cache_numshardbits (default: 6)"
  echo -e "\tPREPOPULATE_BLOB_CACHE\t\t\tPre-populate hot/warm blobs in blob cache (default: 0)"
}

if [ $# -lt 1 ]; then
  display_usage
  exit $EXIT_INVALID_ARGS
fi
bench_cmd=$1
shift
bench_args=$*

if [[ "$bench_cmd" == "--help" ]]; then
  display_usage
  exit
fi

job_id=${JOB_ID}

if [ ! -x ./db_bench ]; then
  echo "./db_bench not found. Please make sure it exists in the current directory."
  exit $EXIT_INVALID_PATH
fi

# Make it easier to run only the compaction test. Getting valid data requires
# a number of iterations and having an ability to run the test separately from
# rest of the benchmarks helps.
if [ "$COMPACTION_TEST" == "1" -a "$bench_cmd" != "universal_compaction" ]; then
  echo "Skipping $1 because it's not a compaction test."
  exit $EXIT_NOT_COMPACTION_TEST
fi

if [ -z $DB_DIR ]; then
  echo "DB_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

if [ -z $WAL_DIR ]; then
  echo "WAL_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

output_dir=${OUTPUT_DIR:-/tmp}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

report="$output_dir/report.tsv"
schedule="$output_dir/schedule.txt"

# all multithreaded tests run with sync=1 unless
# $DB_BENCH_NO_SYNC is defined
syncval="1"
if [ ! -z $DB_BENCH_NO_SYNC ]; then
  echo "Turning sync off for all multithreaded tests"
  syncval="0";
fi

compaction_style=${COMPACTION_STYLE:-leveled}
if [ $compaction_style = "leveled" ]; then
  echo Use leveled compaction
elif [ $compaction_style = "universal" ]; then
  echo Use universal compaction
elif [ $compaction_style = "blob" ]; then
  echo Use blob compaction
else
  echo COMPACTION_STYLE is :: $COMPACTION_STYLE :: and must be one of leveled, universal, blob
  exit $EXIT_INVALID_ARGS
fi

num_threads=${NUM_THREADS:-64}
mb_written_per_sec=${MB_WRITE_PER_SEC:-0}
# Only for tests that do range scans
num_nexts_per_seek=${NUM_NEXTS_PER_SEEK:-10}
cache_size=${CACHE_SIZE:-$(( 16 * $G ))}
cache_numshardbits=${CACHE_NUMSHARDBITS:-6}
compression_max_dict_bytes=${COMPRESSION_MAX_DICT_BYTES:-0}
compression_type=${COMPRESSION_TYPE:-zstd}
min_level_to_compress=${MIN_LEVEL_TO_COMPRESS:-"-1"}
compression_size_percent=${COMPRESSION_SIZE_PERCENT:-"-1"}

duration=${DURATION:-0}
writes=${WRITES:-0}

num_keys=${NUM_KEYS:-8000000000}
key_size=${KEY_SIZE:-20}
value_size=${VALUE_SIZE:-400}
block_size=${BLOCK_SIZE:-8192}
write_buffer_mb=${WRITE_BUFFER_SIZE_MB:-128}
target_file_mb=${TARGET_FILE_SIZE_BASE_MB:-128}
l1_mb=${MAX_BYTES_FOR_LEVEL_BASE_MB:-1024}
max_background_jobs=${MAX_BACKGROUND_JOBS:-16}
stats_interval_seconds=${STATS_INTERVAL_SECONDS:-60}
report_interval_seconds=${REPORT_INTERVAL_SECONDS:-1}
subcompactions=${SUBCOMPACTIONS:-1}
per_level_fanout=${PER_LEVEL_FANOUT:-8}

cache_index_and_filter=${CACHE_INDEX_AND_FILTER_BLOCKS:-0}
if [[ $cache_index_and_filter -eq 0 ]]; then
  cache_meta_flags=""
elif [[ $cache_index_and_filter -eq 1 ]]; then
  cache_meta_flags="\
  --cache_index_and_filter_blocks=$cache_index_and_filter \
  --cache_high_pri_pool_ratio=0.5 --cache_low_pri_pool_ratio=0"
else
  echo CACHE_INDEX_AND_FILTER_BLOCKS was $CACHE_INDEX_AND_FILTER_BLOCKS but must be 0 or 1
  exit $EXIT_INVALID_ARGS
fi

soft_pending_arg=""
if [ ! -z $SOFT_PENDING_COMPACTION_BYTES_LIMIT_IN_GB ]; then
  soft_pending_bytes=$( echo $SOFT_PENDING_COMPACTION_BYTES_LIMIT_IN_GB | \
    awk '{ printf "%.0f", $1 * GB }' GB=$G )
  soft_pending_arg="--soft_pending_compaction_bytes_limit=$soft_pending_bytes"
fi

hard_pending_arg=""
if [ ! -z $HARD_PENDING_COMPACTION_BYTES_LIMIT_IN_GB ]; then
  hard_pending_bytes=$( echo $HARD_PENDING_COMPACTION_BYTES_LIMIT_IN_GB | \
    awk '{ printf "%.0f", $1 * GB }' GB=$G )
  hard_pending_arg="--hard_pending_compaction_bytes_limit=$hard_pending_bytes"
fi

o_direct_flags=""
if [ ! -z $USE_O_DIRECT ]; then
  # Some of these flags are only supported in new versions and --undefok makes that work
  o_direct_flags="--use_direct_reads --use_direct_io_for_flush_and_compaction --prepopulate_block_cache=1"
  bytes_per_sync=0
else
  bytes_per_sync=${BYTES_PER_SYNC:-$(( 1 * M ))}
fi

univ_min_merge_width=${UNIVERSAL_MIN_MERGE_WIDTH:-2}
univ_max_merge_width=${UNIVERSAL_MAX_MERGE_WIDTH:-20}
univ_size_ratio=${UNIVERSAL_SIZE_RATIO:-1}
univ_max_size_amp=${UNIVERSAL_MAX_SIZE_AMP:-200}

if [ ! -z $UNIVERSAL_ALLOW_TRIVIAL_MOVE ]; then
  univ_allow_trivial_move=1
else
  univ_allow_trivial_move=0
fi

min_blob_size=${MIN_BLOB_SIZE:-0}
blob_file_size=${BLOB_FILE_SIZE:-$(( 256 * $M ))}
blob_compression_type=${BLOB_COMPRESSION_TYPE:-${compression_type}}
blob_gc_age_cutoff=${BLOB_GC_AGE_CUTOFF:-"0.25"}
blob_gc_force_threshold=${BLOB_GC_FORCE_THRESHOLD:-1}
blob_file_starting_level=${BLOB_FILE_STARTING_LEVEL:-0}
use_blob_cache=${USE_BLOB_CACHE:-1}
use_shared_block_and_blob_cache=${USE_SHARED_BLOCK_AND_BLOB_CACHE:-1}
blob_cache_size=${BLOB_CACHE_SIZE:-$(( 16 * $G ))}
blob_cache_numshardbits=${BLOB_CACHE_NUMSHARDBITS:-6}
prepopulate_blob_cache=${PREPOPULATE_BLOB_CACHE:-0}

# This script still works back to RocksDB 6.0
undef_params="\
use_blob_cache,\
use_shared_block_and_blob_cache,\
blob_cache_size,blob_cache_numshardbits,\
prepopulate_blob_cache,\
multiread_batched,\
cache_low_pri_pool_ratio,\
prepopulate_block_cache"

const_params_base="
  --undefok=$undef_params \
  --db=$DB_DIR \
  --wal_dir=$WAL_DIR \
  \
  --num=$num_keys \
  --key_size=$key_size \
  --value_size=$value_size \
  --block_size=$block_size \
  --cache_size=$cache_size \
  --cache_numshardbits=$cache_numshardbits \
  --compression_max_dict_bytes=$compression_max_dict_bytes \
  --compression_ratio=0.5 \
  --compression_type=$compression_type \
  --bytes_per_sync=$bytes_per_sync \
  $cache_meta_flags \
  $o_direct_flags \
  --benchmark_write_rate_limit=$(( 1024 * 1024 * $mb_written_per_sec )) \
  \
  --write_buffer_size=$(( $write_buffer_mb * M)) \
  --target_file_size_base=$(( $target_file_mb * M)) \
  --max_bytes_for_level_base=$(( $l1_mb * M)) \
  \
  --verify_checksum=1 \
  --delete_obsolete_files_period_micros=$((60 * M)) \
  --max_bytes_for_level_multiplier=$per_level_fanout \
  \
  --statistics=0 \
  --stats_per_interval=1 \
  --stats_interval_seconds=$stats_interval_seconds \
  --report_interval_seconds=$report_interval_seconds \
  --histogram=1 \
  \
  --memtablerep=skip_list \
  --bloom_bits=10 \
  --open_files=-1 \
  --subcompactions=$subcompactions \
  \
  $bench_args"

level_const_params="
  $const_params_base \
  --compaction_style=0 \
  --num_levels=8 \
  --min_level_to_compress=$min_level_to_compress \
  --level_compaction_dynamic_level_bytes=true \
  --pin_l0_filter_and_index_blocks_in_cache=1 \
  $soft_pending_arg \
  $hard_pending_arg \
"

# These inherit level_const_params because the non-blob LSM tree uses leveled compaction.
blob_const_params="
  $level_const_params \
  --enable_blob_files=true \
  --min_blob_size=$min_blob_size \
  --blob_file_size=$blob_file_size \
  --blob_compression_type=$blob_compression_type \
  --enable_blob_garbage_collection=true \
  --blob_garbage_collection_age_cutoff=$blob_gc_age_cutoff \
  --blob_garbage_collection_force_threshold=$blob_gc_force_threshold \
  --blob_file_starting_level=$blob_file_starting_level \
  --use_blob_cache=$use_blob_cache \
  --use_shared_block_and_blob_cache=$use_shared_block_and_blob_cache \
  --blob_cache_size=$blob_cache_size \
  --blob_cache_numshardbits=$blob_cache_numshardbits \
  --prepopulate_blob_cache=$prepopulate_blob_cache \
"

# TODO:
#   pin_l0_filter_and..., is this OK?
univ_const_params="
  $const_params_base \
  --compaction_style=1 \
  --num_levels=40 \
  --universal_compression_size_percent=$compression_size_percent \
  --pin_l0_filter_and_index_blocks_in_cache=1 \
  --universal_min_merge_width=$univ_min_merge_width \
  --universal_max_merge_width=$univ_max_merge_width \
  --universal_size_ratio=$univ_size_ratio \
  --universal_max_size_amplification_percent=$univ_max_size_amp \
  --universal_allow_trivial_move=$univ_allow_trivial_move \
"

if [ $compaction_style == "leveled" ]; then
  const_params="$level_const_params"
  l0_file_num_compaction_trigger=${LEVEL0_FILE_NUM_COMPACTION_TRIGGER:-4}
  l0_slowdown_writes_trigger=${LEVEL0_SLOWDOWN_WRITES_TRIGGER:-20}
  l0_stop_writes_trigger=${LEVEL0_STOP_WRITES_TRIGGER:-30}
elif [ $compaction_style == "universal" ]; then
  const_params="$univ_const_params"
  l0_file_num_compaction_trigger=${LEVEL0_FILE_NUM_COMPACTION_TRIGGER:-8}
  l0_slowdown_writes_trigger=${LEVEL0_SLOWDOWN_WRITES_TRIGGER:-20}
  l0_stop_writes_trigger=${LEVEL0_STOP_WRITES_TRIGGER:-30}
else
  # compaction_style == "blob"
  const_params="$blob_const_params"
  l0_file_num_compaction_trigger=${LEVEL0_FILE_NUM_COMPACTION_TRIGGER:-4}
  l0_slowdown_writes_trigger=${LEVEL0_SLOWDOWN_WRITES_TRIGGER:-20}
  l0_stop_writes_trigger=${LEVEL0_STOP_WRITES_TRIGGER:-30}
fi

l0_config="
  --level0_file_num_compaction_trigger=$l0_file_num_compaction_trigger \
  --level0_slowdown_writes_trigger=$l0_slowdown_writes_trigger \
  --level0_stop_writes_trigger=$l0_stop_writes_trigger"

# You probably don't want to set both --writes and --duration
if [ $duration -gt 0 ]; then
  const_params="$const_params --duration=$duration"
fi
if [ $writes -gt 0 ]; then
  const_params="$const_params --writes=$writes"
fi

params_w="$l0_config \
          --max_background_jobs=$max_background_jobs \
          --max_write_buffer_number=8 \
          $const_params"

params_bulkload="--max_background_jobs=$max_background_jobs \
                 --max_write_buffer_number=8 \
                 --allow_concurrent_memtable_write=false \
                 --level0_file_num_compaction_trigger=$((10 * M)) \
                 --level0_slowdown_writes_trigger=$((10 * M)) \
                 --level0_stop_writes_trigger=$((10 * M)) \
                 $const_params "

params_fillseq="--allow_concurrent_memtable_write=false \
                $params_w "

#
# Tune values for level and universal compaction.
# For universal compaction, these level0_* options mean total sorted of runs in
# LSM. In level-based compaction, it means number of L0 files.
#
params_level_compact="$const_params \
                --max_background_flushes=4 \
                --max_write_buffer_number=4 \
                --level0_file_num_compaction_trigger=4 \
                --level0_slowdown_writes_trigger=16 \
                --level0_stop_writes_trigger=20"

params_univ_compact="$const_params \
                --max_background_flushes=4 \
                --max_write_buffer_number=4 \
                --level0_file_num_compaction_trigger=8 \
                --level0_slowdown_writes_trigger=16 \
                --level0_stop_writes_trigger=20"

tsv_header="ops_sec\tmb_sec\tlsm_sz\tblob_sz\tc_wgb\tw_amp\tc_mbps\tc_wsecs\tc_csecs\tb_rgb\tb_wgb\tusec_op\tp50\tp99\tp99.9\tp99.99\tpmax\tuptime\tstall%\tNstall\tu_cpu\ts_cpu\trss\ttest\tdate\tversion\tjob_id\tgithash"

function get_cmd() {
  output=$1

  numa=""
  if [ ! -z $NUMACTL ]; then
    numa="numactl --interleave=all "
  fi

  # Try to use timeout when duration is set because some tests (revrange*) hang
  # for some versions (v6.10, v6.11).
  timeout_cmd=""
  if [ $duration -gt 0 ]; then
    if hash timeout ; then
      timeout_cmd="timeout $(( $duration + 600 ))"
    fi
  fi

  echo "/usr/bin/time -f '%e %U %S' -o $output $numa $timeout_cmd"
}

function month_to_num() {
    local date_str=$1
    date_str="${date_str/Jan/01}"
    date_str="${date_str/Feb/02}"
    date_str="${date_str/Mar/03}"
    date_str="${date_str/Apr/04}"
    date_str="${date_str/May/05}"
    date_str="${date_str/Jun/06}"
    date_str="${date_str/Jul/07}"
    date_str="${date_str/Aug/08}"
    date_str="${date_str/Sep/09}"
    date_str="${date_str/Oct/10}"
    date_str="${date_str/Nov/11}"
    date_str="${date_str/Dec/12}"
    echo $date_str
}

function start_stats {
  output=$1
  iostat -y -mx 1  >& $output.io &
  vmstat 1 >& $output.vm &
  # tail -1 because "ps | grep db_bench" returns 2 entries and we want the second
  while :; do ps aux | grep db_bench | grep -v grep | tail -1; sleep 10; done >& $output.ps &
  # This sets a global value
  pspid=$!

  while :; do
    eval $( ls -l $DB_DIR 2> /dev/null | awk '
      BEGIN { gb = 1024*1024*1024 }
      /blob/{ bb += $5 } /sst/{ sb += $5 } { ab += $5 }
      END { printf "b_gb=%.1f s_gb=%.1f a_gb=%.1f", bb / gb, sb / gb, ab / gb }
    ')
    l_gb=$( ls -l $WAL_DIR 2> /dev/null | awk '/log/{ b += $5 } END { printf "%.1f", b / (1024*1024*1024) }' )
    ts=$( date +%H%M%S )
    echo -e "${a_gb}\t${s_gb}\t${l_gb}\t${b_gb}\t${ts}"
    sleep 10
  done >& $output.sizes &
  # This sets a global value
  szpid=$!
}

function stop_stats {
  output=$1
  kill $pspid
  kill $szpid
  killall iostat
  killall vmstat
  sleep 1
  gzip $output.io
  gzip $output.vm

  am=$( sort -nk 1,1 $output.sizes | tail -1 | awk '{ print $1 }' )
  sm=$( sort -nk 2,2 $output.sizes | tail -1 | awk '{ print $2 }' )
  lm=$( sort -nk 3,3 $output.sizes | tail -1 | awk '{ print $3 }' )
  bm=$( sort -nk 4,4 $output.sizes | tail -1 | awk '{ print $4 }' )
  echo -e "max sizes (GB): $am all, $sm sst, $lm log, $bm blob" >> $output.sizes
}

function units_as_gb {
  size=$1
  units=$2

  case $units in
    MB)
      echo "$size" | awk '{ printf "%.1f", $1 / 1024.0 }'
      ;;
    GB)
      echo "$size"
      ;;
    TB)
      echo "$size" | awk '{ printf "%.1f", $1 * 1024.0 }'
      ;;
    *)
      echo "NA"
      ;;
  esac
}

function summarize_result {
  test_out=$1
  test_name=$2
  bench_name=$3

  # In recent versions these can be found directly via db_bench --version, --build_info but
  # grepping from the log lets this work on older versions.
  version="$( grep "RocksDB version:" "$DB_DIR"/LOG | head -1 | awk '{ printf "%s", $5 }' )"
  git_hash="$( grep "Git sha" "$DB_DIR"/LOG | head -1 | awk '{ printf "%s", substr($5, 1, 10) }' )"

  # Note that this function assumes that the benchmark executes long enough so
  # that "Compaction Stats" is written to stdout at least once. If it won't
  # happen then empty output from grep when searching for "Sum" will cause
  # syntax errors.
  date=$( grep ^Date: $test_out | awk '{ print $6 "-" $3 "-" $4 "T" $5 }' )
  my_date=$( month_to_num $date )
  uptime=$( grep ^Uptime\(secs $test_out | tail -1 | awk '{ printf "%.0f", $2 }' )
  stall_pct=$( grep "^Cumulative stall" $test_out| tail -1  | awk '{  print $5 }' )
  nstall=$( grep ^Stalls\(count\):  $test_out | tail -1 | awk '{ print $2 + $6 + $10 + $14 + $18 + $20 }' )

  if ! grep ^"$bench_name" "$test_out" > /dev/null 2>&1 ; then
    echo -e "failed\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t$test_name\t$my_date\t$version\t$job_id\t$git_hash"
    return
  fi

  # Output formats
  # V1: readrandom   :      10.218 micros/op 3131616 ops/sec; 1254.3 MB/s (176144999 of 176144999 found)
  # The MB/s is mssing for multireadrandom
  # V1a: multireadrandom :      10.164 micros/op 3148272 ops/sec; (177099990 of 177099990 found)
  # V1: overwrite    :       7.939 micros/op 125963 ops/sec;   50.5 MB/s
  # V2: overwrite    :       7.854 micros/op 127320 ops/sec 1800.001 seconds 229176999 operations;   51.0 MB/s

  format_version=$( grep ^"$bench_name" "$test_out" \
    | awk '{ if (NF >= 10 && $8 == "seconds") { print "V2" } else { print "V1" } }' )
  if [ $format_version == "V1" ]; then
    ops_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $5 }' )
    usecs_op=$( grep ^"$bench_name" "$test_out" | awk '{ printf "%.1f", $3 }' )
    if [ "$bench_name" == "multireadrandom" ]; then
      mb_sec="NA"
    else
      mb_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $7 }' )
    fi
  else
    ops_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $5 }' )
    usecs_op=$( grep ^"$bench_name" "$test_out" | awk '{ printf "%.1f", $3 }' )
    mb_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $11 }' )
  fi

  # For RocksDB version 4.x there are fewer fields but this still parses correctly
  # Cumulative writes: 242M writes, 242M keys, 18M commit groups, 12.9 writes per commit group, ingest: 95.96 GB, 54.69 MB/s
  cum_writes_gb_orig=$( grep "^Cumulative writes" "$test_out" | tail -1 | awk '{ for (x=1; x<=NF; x++) { if ($x == "ingest:") { printf "%.1f", $(x+1) } } }' )
  cum_writes_units=$( grep "^Cumulative writes" "$test_out" | tail -1 | awk '{ for (x=1; x<=NF; x++) { if ($x == "ingest:") { print $(x+2) } } }' | sed 's/,//g' )
  cum_writes_gb=$( units_as_gb "$cum_writes_gb_orig" "$cum_writes_units" )

  # Cumulative compaction: 1159.74 GB write, 661.03 MB/s write, 1108.89 GB read, 632.04 MB/s read, 6284.3 seconds
  cmb_ps=$( grep "^Cumulative compaction" "$test_out" | tail -1 | awk '{ printf "%.1f", $6 }' )
  sum_wgb_orig=$( grep "^Cumulative compaction" "$test_out" | tail -1 | awk '{ printf "%.1f", $3 }' )
  sum_wgb_units=$( grep "^Cumulative compaction" "$test_out" | tail -1 | awk '{ print $4 }' )
  sum_wgb=$( units_as_gb "$sum_wgb_orig" "$sum_wgb_units" )

  # Flush(GB): cumulative 97.193, interval 1.247
  flush_wgb=$( grep "^Flush(GB)" "$test_out" | tail -1 | awk '{ print $3 }' | tr ',' ' ' | awk '{ print $1 }' )

  if [[ "$sum_wgb" == "NA" || \
        "$cum_writes_gb" == "NA" || \
        "$cum_writes_gb_orig" == "0.0" || \
        -z "$cum_writes_gb_orig" || \
        -z "$flush_wgb" ]]; then
    wamp="NA"
  else
    wamp=$( echo "( $sum_wgb + $flush_wgb ) / $cum_writes_gb" | bc -l | awk '{ printf "%.1f", $1 }' )
  fi

  c_wsecs=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.0f", $15 }' )
  c_csecs=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.0f", $16 }' )

  lsm_size=$( grep "^ Sum" "$test_out" | tail -1 | awk '{ printf "%.0f%s", $3, $4 }' )
  blob_size=$( grep "^Blob file count:" "$test_out" | tail -1 | awk '{ printf "%.0f%s", $7, $8 }' )
  # Remove the trailing comma from blob_size: 3.0GB, -> 3.0GB
  blob_size="${blob_size/,/}"

  b_rgb=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.0f", $21 }' )
  b_wgb=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.0f", $22 }' )

  p50=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.1f", $3 }' )
  p99=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $7 }' )
  p999=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $9 }' )
  p9999=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $11 }' )
  pmax=$( grep "^Min: " $test_out | grep Median: | grep Max: | awk '{ printf "%.0f", $6 }' )

  # Use the last line because there might be extra lines when the db_bench process exits with an error
  time_out="$test_out".time
  u_cpu=$( tail -1 "$time_out" | awk '{ printf "%.1f", $2 / 1000.0 }' )
  s_cpu=$( tail -1 "$time_out" | awk '{ printf "%.1f", $3 / 1000.0  }' )

  rss="NA"
  if [ -f $test_out.stats.ps ]; then
    rss=$( awk '{ printf "%.1f\n", $6 / (1024 * 1024) }' "$test_out".stats.ps | sort -n | tail -1 )
  fi

  # if the report TSV (Tab Separate Values) file does not yet exist, create it and write the header row to it
  if [ ! -f "$report" ]; then
    echo -e "# ops_sec - operations per second" >> "$report"
    echo -e "# mb_sec - ops_sec * size-of-operation-in-MB" >> "$report"
    echo -e "# lsm_sz - size of LSM tree" >> "$report"
    echo -e "# blob_sz - size of BlobDB logs" >> "$report"
    echo -e "# c_wgb - GB written by compaction" >> "$report"
    echo -e "# w_amp - Write-amplification as (bytes written by compaction / bytes written by memtable flush)" >> "$report"
    echo -e "# c_mbps - Average write rate for compaction" >> "$report"
    echo -e "# c_wsecs - Wall clock seconds doing compaction" >> "$report"
    echo -e "# c_csecs - CPU seconds doing compaction" >> "$report"
    echo -e "# b_rgb - Blob compaction read GB" >> "$report"
    echo -e "# b_wgb - Blob compaction write GB" >> "$report"
    echo -e "# usec_op - Microseconds per operation" >> "$report"
    echo -e "# p50, p99, p99.9, p99.99 - 50th, 99th, 99.9th, 99.99th percentile response time in usecs" >> "$report"
    echo -e "# pmax - max response time in usecs" >> "$report"
    echo -e "# uptime - RocksDB uptime in seconds" >> "$report"
    echo -e "# stall% - Percentage of time writes are stalled" >> "$report"
    echo -e "# Nstall - Number of stalls" >> "$report"
    echo -e "# u_cpu - #seconds/1000 of user CPU" >> "$report"
    echo -e "# s_cpu - #seconds/1000 of system CPU" >> "$report"
    echo -e "# rss - max RSS in GB for db_bench process" >> "$report"
    echo -e "# test - Name of test" >> "$report"
    echo -e "# date - Date/time of test" >> "$report"
    echo -e "# version - RocksDB version" >> "$report"
    echo -e "# job_id - User-provided job ID" >> "$report"
    echo -e "# githash - git hash at which db_bench was compiled" >> "$report"
    echo -e $tsv_header >> "$report"
  fi

  echo -e "$ops_sec\t$mb_sec\t$lsm_size\t$blob_size\t$sum_wgb\t$wamp\t$cmb_ps\t$c_wsecs\t$c_csecs\t$b_rgb\t$b_wgb\t$usecs_op\t$p50\t$p99\t$p999\t$p9999\t$pmax\t$uptime\t$stall_pct\t$nstall\t$u_cpu\t$s_cpu\t$rss\t$test_name\t$my_date\t$version\t$job_id\t$git_hash" \
    >> "$report"
}

function run_bulkload {
  # This runs with a vector memtable and the WAL disabled to load faster. It is still crash safe and the
  # client can discover where to restart a load after a crash. I think this is a good way to load.
  echo "Bulk loading $num_keys random keys"
  log_file_name=$output_dir/benchmark_bulkload_fillrandom.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=fillrandom,stats \
       --use_existing_db=0 \
       --disable_auto_compactions=1 \
       --sync=0 \
       $params_bulkload \
       --threads=1 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=1 \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name bulkload fillrandom

  echo "Compacting..."
  log_file_name=$output_dir/benchmark_bulkload_compact.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=compact,stats \
       --use_existing_db=1 \
       --disable_auto_compactions=1 \
       --sync=0 \
       $params_w \
       --threads=1 \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
}

#
# Parameter description:
#
# $1 - 1 if I/O statistics should be collected.
# $2 - compaction type to use (level=0, universal=1).
# $3 - number of subcompactions.
# $4 - number of maximum background compactions.
#
function run_manual_compaction_worker {
  # This runs with a vector memtable and the WAL disabled to load faster.
  # It is still crash safe and the client can discover where to restart a
  # load after a crash. I think this is a good way to load.
  echo "Bulk loading $num_keys random keys for manual compaction."

  log_file_name=$output_dir/benchmark_man_compact_fillrandom_$3.log

  if [ "$2" == "1" ]; then
    extra_params=$params_univ_compact
  else
    extra_params=$params_level_compact
  fi

  # Make sure that fillrandom uses the same compaction options as compact.
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=fillrandom,stats \
       --use_existing_db=0 \
       --disable_auto_compactions=0 \
       --sync=0 \
       $extra_params \
       --threads=$num_threads \
       --compaction_measure_io_stats=$1 \
       --compaction_style=$2 \
       --subcompactions=$3 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=1 \
       --max_background_compactions=$4 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"

  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd

  summarize_result $log_file_namefillrandom_output_file man_compact_fillrandom_$3 fillrandom

  echo "Compacting with $3 subcompactions specified ..."

  log_file_name=$output_dir/benchmark_man_compact_$3.log

  # This is the part we're really interested in. Given that compact benchmark
  # doesn't output regular statistics then we'll just use the time command to
  # measure how long this step takes.
  cmd="{ \
       time ./db_bench --benchmarks=compact,stats \
       --use_existing_db=1 \
       --disable_auto_compactions=0 \
       --sync=0 \
       $extra_params \
       --threads=$num_threads \
       --compaction_measure_io_stats=$1 \
       --compaction_style=$2 \
       --subcompactions=$3 \
       --max_background_compactions=$4 \
       ;}
       2>&1 | tee -a $log_file_name"

  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd

  # Can't use summarize_result here. One way to analyze the results is to run
  # "grep real" on the resulting log files.
}

function run_univ_compaction {
  # Always ask for I/O statistics to be measured.
  io_stats=1

  # Values: kCompactionStyleLevel = 0x0, kCompactionStyleUniversal = 0x1.
  compaction_style=1

  # Define a set of benchmarks.
  subcompactions=(1 2 4 8 16)
  max_background_compactions=(16 16 8 4 2)

  i=0
  total=${#subcompactions[@]}

  # Execute a set of benchmarks to cover variety of scenarios.
  while [ "$i" -lt "$total" ]
  do
    run_manual_compaction_worker $io_stats $compaction_style ${subcompactions[$i]} \
      ${max_background_compactions[$i]}
    ((i++))
  done
}

function run_fillseq {
  # This runs with a vector memtable. WAL can be either disabled or enabled
  # depending on the input parameter (1 for disabled, 0 for enabled). The main
  # benefit behind disabling WAL is to make loading faster. It is still crash
  # safe and the client can discover where to restart a load after a crash. I
  # think this is a good way to load.

  # Make sure that we'll have unique names for all the files so that data won't
  # be overwritten.
  if [ $1 == 1 ]; then
    log_file_name="${output_dir}/benchmark_fillseq.wal_disabled.v${value_size}.log"
    test_name=fillseq.wal_disabled.v${value_size}
  else
    log_file_name="${output_dir}/benchmark_fillseq.wal_enabled.v${value_size}.log"
    test_name=fillseq.wal_enabled.v${value_size}
  fi

  # For Leveled compaction hardwire this to 0 so that data that is trivial-moved
  # to larger levels (3, 4, etc) will be compressed.
  if [ $compaction_style == "leveled" ]; then
    comp_arg="--min_level_to_compress=0"
  elif [ $compaction_style == "universal" ]; then
    if [ ! -z $UNIVERSAL_ALLOW_TRIVIAL_MOVE ]; then
      # See GetCompressionFlush where compression_size_percent < 1 means use the default
      # compression which is needed because trivial moves are enabled
      comp_arg="--universal_compression_size_percent=-1"
    else
      # See GetCompressionFlush where compression_size_percent > 0 means no compression.
      # Don't set anything here because compression_size_percent is set in univ_const_params
      comp_arg=""
    fi
  else
    # compaction_style == "blob"
    comp_arg="--min_level_to_compress=0"
  fi

  echo "Loading $num_keys keys sequentially"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=fillseq,stats \
       $params_fillseq \
       $comp_arg \
       --use_existing_db=0 \
       --sync=0 \
       --threads=1 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=$1 \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats

  # The constant "fillseq" which we pass to db_bench is the benchmark name.
  summarize_result $log_file_name $test_name fillseq
}

function run_lsm {
  # This flushes the memtable and L0 to get the LSM tree into a deterministic
  # state for read-only tests that will follow.
  echo "Flush memtable, wait, compact L0, wait"
  job=$1

  if [ $job = flush_mt_l0 ]; then
    benchmarks=levelstats,flush,waitforcompaction,compact0,waitforcompaction,memstats,levelstats
  elif [ $job = waitforcompaction ]; then
    benchmarks=levelstats,waitforcompaction,memstats,levelstats
  else
    echo Job unknown: $job
    exit $EXIT_NOT_COMPACTION_TEST
  fi

  log_file_name=$output_dir/benchmark_${job}.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=$benchmarks,stats \
       --use_existing_db=1 \
       --sync=0 \
       $params_w \
       --threads=1 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  # waitforcompaction can hang with universal (compaction_style=1)
  # see bug https://github.com/meta/rocksdb/issues/9275
  eval $cmd
  stop_stats $log_file_name.stats
  # Don't summarize, the log doesn't have the output needed for it
}

function run_change {
  output_name=$1
  grep_name=$2
  benchmarks=$3
  echo "Do $num_keys random $output_name"
  log_file_name="$output_dir/benchmark_${output_name}.t${num_threads}.s${syncval}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=$benchmarks,stats \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name ${output_name}.t${num_threads}.s${syncval} $grep_name
}

function run_filluniquerandom {
  echo "Loading $num_keys unique keys randomly"
  log_file_name=$output_dir/benchmark_filluniquerandom.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=filluniquerandom,stats \
       --use_existing_db=0 \
       --sync=0 \
       $params_w \
       --threads=1 \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name filluniquerandom filluniquerandom
}

function run_readrandom {
  echo "Reading $num_keys random keys"
  log_file_name="${output_dir}/benchmark_readrandom.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=readrandom,stats \
       --use_existing_db=1 \
       $params_w \
       --threads=$num_threads \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name readrandom.t${num_threads} readrandom
}

function run_multireadrandom {
  echo "Multi-Reading $num_keys random keys"
  log_file_name="${output_dir}/benchmark_multireadrandom.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=multireadrandom,stats \
       --use_existing_db=1 \
       --threads=$num_threads \
       --batch_size=10 \
       $params_w \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name multireadrandom.t${num_threads} multireadrandom
}

function run_readwhile {
  operation=$1
  echo "Reading $num_keys random keys while $operation"
  log_file_name="${output_dir}/benchmark_readwhile${operation}.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench --benchmarks=readwhile${operation},stats \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name readwhile${operation}.t${num_threads} readwhile${operation}
}

function run_rangewhile {
  operation=$1
  full_name=$2
  reverse_arg=$3
  log_file_name="${output_dir}/benchmark_${full_name}.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  echo "Range scan $num_keys random keys while ${operation} for reverse_iter=${reverse_arg}"
  cmd="$time_cmd ./db_bench --benchmarks=seekrandomwhile${operation},stats \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seek_nexts=$num_nexts_per_seek \
       --reverse_iterator=$reverse_arg \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  echo $cmd | tee $log_file_name
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name ${full_name}.t${num_threads} seekrandomwhile${operation}
}

function run_range {
  full_name=$1
  reverse_arg=$2
  log_file_name="${output_dir}/benchmark_${full_name}.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  echo "Range scan $num_keys random keys for reverse_iter=${reverse_arg}"
  cmd="$time_cmd ./db_bench --benchmarks=seekrandom,stats \
       --use_existing_db=1 \
       $params_w \
       --threads=$num_threads \
       --seek_nexts=$num_nexts_per_seek \
       --reverse_iterator=$reverse_arg \
       --seed=$( date +%s ) \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name ${full_name}.t${num_threads} seekrandom
}

function run_randomtransaction {
  echo "..."
  log_file_name=$output_dir/benchmark_randomtransaction.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./db_bench $params_w --benchmarks=randomtransaction,stats \
       --num=$num_keys \
       --transaction_db \
       --threads=5 \
       --transaction_sets=5 \
       --report_file=${log_file_name}.r.csv \
       2>&1 | tee $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
}

function now() {
  echo `date +"%s"`
}


echo "===== Benchmark ====="

# Run!!!
IFS=',' read -a jobs <<< $bench_cmd
# shellcheck disable=SC2068
for job in ${jobs[@]}; do

  if [ $job != debug ]; then
    echo "Starting $job (ID: $job_id) at `date`" | tee -a $schedule
  fi

  start=$(now)
  if [ $job = bulkload ]; then
    run_bulkload
  elif [ $job = flush_mt_l0 ]; then
    run_lsm flush_mt_l0
  elif [ $job = waitforcompaction ]; then
    run_lsm waitforcompaction
  elif [ $job = fillseq_disable_wal ]; then
    run_fillseq 1
  elif [ $job = fillseq_enable_wal ]; then
    run_fillseq 0
  elif [ $job = overwrite ]; then
    run_change overwrite overwrite overwrite
  elif [ $job = overwritesome ]; then
    # This uses a different name for overwrite results so it can be run twice in one benchmark run.
    run_change overwritesome overwrite overwrite
  elif [ $job = overwriteandwait ]; then
    run_change overwriteandwait overwrite overwrite,waitforcompaction
  elif [ $job = updaterandom ]; then
    run_change updaterandom updaterandom updaterandom
  elif [ $job = mergerandom ]; then
    run_change mergerandom mergerandom mergerandom
  elif [ $job = filluniquerandom ]; then
    run_filluniquerandom
  elif [ $job = readrandom ]; then
    run_readrandom
  elif [ $job = multireadrandom ]; then
    run_multireadrandom
  elif [ $job = fwdrange ]; then
    run_range $job false
  elif [ $job = revrange ]; then
    run_range $job true
  elif [ $job = readwhilewriting ]; then
    run_readwhile writing
  elif [ $job = readwhilemerging ]; then
    run_readwhile merging
  elif [ $job = fwdrangewhilewriting ]; then
    run_rangewhile writing $job false
  elif [ $job = revrangewhilewriting ]; then
    run_rangewhile writing $job true
  elif [ $job = fwdrangewhilemerging ]; then
    run_rangewhile merging $job false
  elif [ $job = revrangewhilemerging ]; then
    run_rangewhile merging $job true
  elif [ $job = randomtransaction ]; then
    run_randomtransaction
  elif [ $job = universal_compaction ]; then
    run_univ_compaction
  elif [ $job = debug ]; then
    num_keys=1000; # debug
    echo "Setting num_keys to $num_keys"
  else
    echo "unknown job $job"
    exit $EXIT_UNKNOWN_JOB
  fi
  end=$(now)

  if [ $job != debug ]; then
    echo "Completed $job (ID: $job_id) in $((end-start)) seconds" | tee -a $schedule
  fi

  echo -e $tsv_header
  tail -1 $report

done
