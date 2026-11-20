#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: db_bench binary exists in the current directory

dbdir=$1
odir=$2

# Size Constants
K=1024
M=$((1024 * K))

# Dynamic loader configuration
ld_library_path=${LD_LIBRARY_PATH:-""}

# Benchmark configuration
duration_rw=${DURATION_RW:-65}
duration_ro=${DURATION_RO:-65}
num_keys=${NUM_KEYS:-1000000}
num_threads=${NUM_THREADS:-16}
key_size=${KEY_SIZE:-20}
value_size=${VALUE_SIZE:-400}
mb_write_per_sec=${MB_WRITE_PER_SEC:-2}
ci_tests_only=${CI_TESTS_ONLY:-"false"}

# RocksDB configuration
compression_type=${COMPRESSION_TYPE:-lz4}
subcompactions=${SUBCOMPACTIONS:-1}
write_buffer_size_mb=${WRITE_BUFFER_SIZE_MB:-32}
target_file_size_base_mb=${TARGET_FILE_SIZE_BASE_MB:-32}
max_bytes_for_level_base_mb=${MAX_BYTES_FOR_LEVEL_BASE_MB:-128}
max_background_jobs=${MAX_BACKGROUND_JOBS:-8}
stats_interval_seconds=${STATS_INTERVAL_SECONDS:-20}
cache_index_and_filter_blocks=${CACHE_INDEX_AND_FILTER_BLOCKS:-0}
# USE_O_DIRECT doesn't need a default
bytes_per_sync=${BYTES_PER_SYNC:-$(( 1 * M ))}
# CACHE_SIZE_MB doesn't need a default
min_level_to_compress=${MIN_LEVEL_TO_COMPRESS:-"-1"}

compaction_style=${COMPACTION_STYLE:-leveled}
if [ "$compaction_style" = "leveled" ]; then
  echo Use leveled compaction
elif [ "$compaction_style" = "universal" ]; then
  echo Use universal compaction
elif [ "$compaction_style" = "blob" ]; then
  echo Use blob compaction
else
  echo COMPACTION_STYLE is :: "$COMPACTION_STYLE" :: and must be one of leveled, universal, blob
  exit 1
fi

# Leveled compaction configuration
level0_file_num_compaction_trigger=${LEVEL0_FILE_NUM_COMPACTION_TRIGGER:-4}
level0_slowdown_writes_trigger=${LEVEL0_SLOWDOWN_WRITES_TRIGGER:-20}
level0_stop_writes_trigger=${LEVEL0_STOP_WRITES_TRIGGER:-30}
per_level_fanout=${PER_LEVEL_FANOUT:-8}

# Universal compaction configuration
universal_min_merge_width=${UNIVERSAL_MIN_MERGE_WIDTH:-2}
universal_max_merge_width=${UNIVERSAL_MAX_MERGE_WIDTH:-20}
universal_size_ratio=${UNIVERSAL_SIZE_RATIO:-1}
universal_max_size_amp=${UNIVERSAL_MAX_SIZE_AMP:-200}
universal_compression_size_percent=${UNIVERSAL_COMPRESSION_SIZE_PERCENT:-"-1"}

# Integrated BlobDB configuration

min_blob_size=${MIN_BLOB_SIZE:-0}
blob_file_size=${BLOB_FILE_SIZE:-$(( 256 * M ))}
blob_compression_type=${BLOB_COMPRESSION_TYPE:-${compression_type}}
blob_gc_age_cutoff=${BLOB_GC_AGE_CUTOFF:-"0.25"}
blob_gc_force_threshold=${BLOB_GC_FORCE_THRESHOLD:-1}

# Arguments for dynamic loading
base_args=( LD_LIBRARY_PATH="$ld_library_path" )

# Arguments used for all tests
base_args+=( NUM_KEYS="$num_keys" )
base_args+=( NUM_THREADS="$num_threads" )
base_args+=( KEY_SIZE="$key_size" )
base_args+=( VALUE_SIZE="$value_size" )

base_args+=( SUBCOMPACTIONS="$subcompactions" )
base_args+=( COMPRESSION_TYPE="$compression_type" )
base_args+=( WRITE_BUFFER_SIZE_MB="$write_buffer_size_mb" )
base_args+=( TARGET_FILE_SIZE_BASE_MB="$target_file_size_base_mb" )
base_args+=( MAX_BYTES_FOR_LEVEL_BASE_MB="$max_bytes_for_level_base_mb" )
base_args+=( MAX_BACKGROUND_JOBS="$max_background_jobs" )
base_args+=( STATS_INTERVAL_SECONDS="$stats_interval_seconds" )
base_args+=( CACHE_INDEX_AND_FILTER_BLOCKS="$cache_index_and_filter_blocks" )
base_args+=( COMPACTION_STYLE="$compaction_style" )
base_args+=( BYTES_PER_SYNC="$bytes_per_sync" )

if [ -n "$USE_O_DIRECT" ]; then
  base_args+=( USE_O_DIRECT=1 )
fi

if [ -n "$NUMA" ]; then
  base_args+=( NUMACTL=1 )
fi

if [ -n "$CACHE_SIZE_MB" ]; then
  cacheb=$(( CACHE_SIZE_MB * M ))
  base_args+=( CACHE_SIZE="$cacheb" )
fi

if [ "$compaction_style" == "leveled" ]; then
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER="$level0_file_num_compaction_trigger" )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER="$level0_slowdown_writes_trigger" )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER="$level0_stop_writes_trigger" )
  base_args+=( PER_LEVEL_FANOUT="$per_level_fanout" )
elif [ "$compaction_style" == "universal" ]; then
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER="$level0_file_num_compaction_trigger" )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER="$level0_slowdown_writes_trigger" )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER="$level0_stop_writes_trigger" )
  base_args+=( UNIVERSAL_MIN_MERGE_WIDTH="$universal_min_merge_width" )
  base_args+=( UNIVERSAL_MAX_MERGE_WIDTH="$universal_max_merge_width" )
  base_args+=( UNIVERSAL_SIZE_RATIO="$universal_size_ratio" )
  base_args+=( UNIVERSAL_MAX_SIZE_AMP="$universal_max_size_amp" )
  if [ -n "$UNIVERSAL_ALLOW_TRIVIAL_MOVE" ]; then
    base_args+=( UNIVERSAL_ALLOW_TRIVIAL_MOVE=1 )
  fi
else
  # Inherit settings for leveled because index uses leveled LSM
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER="$level0_file_num_compaction_trigger" )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER="$level0_slowdown_writes_trigger" )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER="$level0_stop_writes_trigger" )
  base_args+=( PER_LEVEL_FANOUT="$per_level_fanout" )
  # Then add BlobDB specific settings
  base_args+=( MIN_BLOB_SIZE="$min_blob_size" )
  base_args+=( BLOB_FILE_SIZE="$blob_file_size" )
  base_args+=( BLOB_COMPRESSION_TYPE="$blob_compression_type" )
  base_args+=( BLOB_GC_AGE_CUTOFF="$blob_gc_age_cutoff" )
  base_args+=( BLOB_GC_FORCE_THRESHOLD="$blob_gc_force_threshold" )
fi

function usage {
  echo "usage: benchmark_compare.sh db_dir output_dir version+"
  echo -e "\tdb_dir\t\tcreate RocksDB database in this directory"
  echo -e "\toutput_dir\twrite output from performance tests in this directory"
  echo -e "\tversion+\tspace separated sequence of RocksDB versions to test."
  echo -e "\nThis expects that db_bench.\$version exists in \$PWD for each version in the sequence."
  echo -e "An example value for version+ is 6.23.0 6.24.0"
  echo ""
  echo -e "Environment variables for options"
  echo -e "\tNUM_KEYS\t\t\tnumber of keys to load"
  echo -e "\tKEY_SIZE\t\t\tsize of key"
  echo -e "\tVALUE_SIZE\t\t\tsize of value"
  echo -e "\tCACHE_SIZE_MB\t\t\tsize of block cache in MB"
  echo -e "\tDURATION_RW\t\t\tnumber of seconds for which each test runs, except for read-only tests"
  echo -e "\tDURATION_RO\t\t\tnumber of seconds for which each read-only test runs"
  echo -e "\tMB_WRITE_PER_SEC\t\trate limit for writer that runs concurrent with queries for some tests"
  echo -e "\tNUM_THREADS\t\t\tnumber of user threads"
  echo -e "\tCOMPRESSION_TYPE\t\tcompression type (zstd, lz4, none, etc)"
  echo -e "\tMIN_LEVEL_TO_COMPRESS\t\tmin_level_to_compress for leveled"
  echo -e "\tWRITE_BUFFER_SIZE_MB\t\tsize of write buffer in MB"
  echo -e "\tTARGET_FILE_SIZE_BASE_MB\tvalue for target_file_size_base in MB"
  echo -e "\tMAX_BYTES_FOR_LEVEL_BASE_MB\tvalue for max_bytes_for_level_base in MB"
  echo -e "\tMAX_BACKGROUND_JOBS\t\tvalue for max_background_jobs"
  echo -e "\tCACHE_INDEX_AND_FILTER_BLOCKS\tvalue for cache_index_and_filter_blocks"
  echo -e "\tUSE_O_DIRECT\t\t\tUse O_DIRECT for user reads and compaction"
  echo -e "\tBYTES_PER_SYNC\t\t\tValue for bytes_per_sync"
  echo -e "\tSTATS_INTERVAL_SECONDS\t\tvalue for stats_interval_seconds"
  echo -e "\tSUBCOMPACTIONS\t\t\tvalue for subcompactions"
  echo -e "\tCOMPACTION_STYLE\t\tCompaction style to use, one of: leveled, universal, blob"
  echo -e "\tCI_TESTS_ONLY\t\tRun a subset of tests tailored to a CI regression job, one of: true, false (default)"
  echo ""
  echo -e "\tOptions specific to leveled compaction:"
  echo -e "\t\tLEVEL0_FILE_NUM_COMPACTION_TRIGGER\tvalue for level0_file_num_compaction_trigger"
  echo -e "\t\tLEVEL0_SLOWDOWN_WRITES_TRIGGER\t\tvalue for level0_slowdown_writes_trigger"
  echo -e "\t\tLEVEL0_STOP_WRITES_TRIGGER\t\tvalue for level0_stop_writes_trigger"
  echo -e "\t\tPER_LEVEL_FANOUT\t\t\tvalue for max_bytes_for_level_multiplier"
  echo ""
  echo -e "\tOptions specific to universal compaction:"
  echo -e "\t\tSee LEVEL0_*_TRIGGER above"
  echo -e "\t\tUNIVERSAL_MIN_MERGE_WIDTH\t\tvalue of min_merge_width option for universal"
  echo -e "\t\tUNIVERSAL_MAX_MERGE_WIDTH\t\tvalue of min_merge_width option for universal"
  echo -e "\t\tUNIVERSAL_SIZE_RATIO\t\t\tvalue of size_ratio option for universal"
  echo -e "\t\tUNIVERSAL_MAX_SIZE_AMP\t\t\tmax_size_amplification_percent for universal"
  echo -e "\t\tUNIVERSAL_ALLOW_TRIVIAL_MOVE\t\tSet allow_trivial_move to true for universal, default is false"
  echo -e "\t\tUNIVERSAL_COMPRESSION_SIZE_PERCENT\tpercentage of LSM tree that should be compressed"
  echo ""
  echo -e "\tOptions for integrated BlobDB:"
  echo -e "\t\tMIN_BLOB_SIZE\t\t\t\tvalue for min_blob_size"
  echo -e "\t\tBLOB_FILE_SIZE\t\t\t\tvalue for blob_file_size"
  echo -e "\t\tBLOB_COMPRESSION_TYPE\t\t\tvalue for blob_compression_type"
  echo -e "\t\tBLOB_GC_AGE_CUTOFF\t\t\tvalue for blog_garbage_collection_age_cutoff"
  echo -e "\t\tBLOB_GC_FORCE_THRESHOLD\t\t\tvalue for blog_garbage_collection_force_threshold"
}

function dump_env {
  echo "Base args" > "$odir"/args
  echo "${base_args[@]}" | tr ' ' '\n' >> "$odir"/args

  echo -e "\nOther args" >> "$odir"/args
  echo -e "dbdir\t$dbdir" >> "$odir"/args
  echo -e "duration_rw\t$duration_rw" >> "$odir"/args
  echo -e "duration_ro\t$duration_ro" >> "$odir"/args
  echo -e "per_level_fanout\t$per_level_fanout" >> "$odir"/args

  echo -e "\nargs_load:" >> "$odir"/args
  echo "${args_load[@]}" | tr ' ' '\n' >> "$odir"/args
  echo -e "\nargs_nolim:" >> "$odir"/args
  echo "${args_nolim[@]}" | tr ' ' '\n' >> "$odir"/args
  echo -e "\nargs_lim:" >> "$odir"/args
  echo "${args_lim[@]}" | tr ' ' '\n' >> "$odir"/args
}

if [ $# -lt 3 ]; then
  usage
  echo
  echo "Need at least 3 arguments"
  exit 1
fi

shift 2

mkdir -p "$odir"

echo Test versions: "$@"
echo Test versions: "$@" >> "$odir"/args

for v in "$@" ; do
  my_odir="$odir"/"$v"

  if [ -d "$my_odir" ]; then
    echo Exiting because the output directory exists: "$my_odir"
    exit 1
  fi

  args_common=("${base_args[@]}")

  args_common+=( OUTPUT_DIR="$my_odir" DB_DIR="$dbdir" WAL_DIR="$dbdir" DB_BENCH_NO_SYNC=1 )

  if [ "$compaction_style" == "leveled" ]; then
    args_common+=( MIN_LEVEL_TO_COMPRESS="$min_level_to_compress" )
  elif [ "$compaction_style" == "universal" ]; then
    args_common+=( UNIVERSAL=1 COMPRESSION_SIZE_PERCENT="$universal_compression_size_percent" )
  else
    args_common+=( MIN_LEVEL_TO_COMPRESS="$min_level_to_compress" )
  fi

  args_load=("${args_common[@]}")

  args_nolim=("${args_common[@]}")

  args_lim=("${args_nolim[@]}")
  args_lim+=( MB_WRITE_PER_SEC="$mb_write_per_sec" )

  dump_env

  echo Run benchmark for "$v" at "$( date )" with results at "$my_odir"
  rm -f db_bench
  echo ln -s db_bench."$v" db_bench
  ln -s db_bench."$v" db_bench

  find "$dbdir" -type f -exec rm \{\} \;

  # Load in key order
  echo env "${args_load[@]}" bash ./benchmark.sh fillseq_disable_wal
  env -i "${args_load[@]}" bash ./benchmark.sh fillseq_disable_wal

  # Read-only tests. The LSM tree shape is in a deterministic state if trivial move
  # was used during the load.

  # Add revrange with a fixed duration and hardwired number of keys and threads to give
  # compaction debt leftover from fillseq a chance at being removed. Not using waitforcompaction
  # here because it isn't supported on older db_bench versions.
  env -i "${args_nolim[@]}" DURATION=300 NUM_KEYS=100 NUM_THREADS=1 bash ./benchmark.sh revrange
  env -i "${args_nolim[@]}" DURATION="$duration_ro" bash ./benchmark.sh readrandom

  # Skipped for CI - a single essentail readrandom is enough to set up for other tests
  if [ "$ci_tests_only" != "true" ]; then
    env -i "${args_nolim[@]}" DURATION="$duration_ro" bash ./benchmark.sh fwdrange
    env -i "${args_lim[@]}"   DURATION="$duration_ro" bash ./benchmark.sh multireadrandom --multiread_batched
  else
    echo "CI_TESTS_ONLY is set, skipping optional read steps."
  fi

  # Write 10% of the keys. The goal is to randomize keys prior to Lmax
  p10=$( echo "$num_keys" "$num_threads" | awk '{ printf "%.0f", $1 / $2 / 10.0 }' )
  env -i "${args_nolim[@]}" WRITES="$p10"        bash ./benchmark.sh overwritesome

  if [ "$compaction_style" == "leveled" ]; then
    # These are not supported by older versions
    # Flush memtable & L0 to get LSM tree into deterministic state
    env -i "${args_nolim[@]}"                  bash ./benchmark.sh flush_mt_l0
  elif [ "$compaction_style" == "universal" ]; then
    # For universal don't compact L0 as can have too many sorted runs
    # waitforcompaction can hang, see https://github.com/meta/rocksdb/issues/9275
    # While this is disabled the test that follows will have more variance from compaction debt.
    # env -i "${args_nolim[@]}"                    bash ./benchmark.sh waitforcompaction
    echo TODO enable when waitforcompaction hang is fixed
  else
    # These are not supported by older versions
    # Flush memtable & L0 to get LSM tree into deterministic state
    env -i "${args_nolim[@]}"                  bash ./benchmark.sh flush_mt_l0
  fi

  # Read-mostly tests with a rate-limited writer
  env -i "${args_lim[@]}" DURATION="$duration_rw" bash ./benchmark.sh revrangewhilewriting
  env -i "${args_lim[@]}" DURATION="$duration_rw" bash ./benchmark.sh fwdrangewhilewriting
  env -i "${args_lim[@]}" DURATION="$duration_rw" bash ./benchmark.sh readwhilewriting

  # Write-only tests

  # This creates much compaction debt which will be a problem for tests added after it.
  # Also, the compaction stats measured at test end can underestimate write-amp depending
  # on how much compaction debt is allowed.
  if [ "$compaction_style" == "leveled" ] && ./db_bench --benchmarks=waitforcompaction ; then
    # Use waitforcompaction to get more accurate write-amp measurement
    env -i "${args_nolim[@]}" DURATION="$duration_rw" bash ./benchmark.sh overwriteandwait
  else
    # waitforcompaction hangs with universal, see https://github.com/meta/rocksdb/issues/9275
    env -i "${args_nolim[@]}" DURATION="$duration_rw" bash ./benchmark.sh overwrite
  fi

  cp "$dbdir"/LOG* "$my_odir"
  gzip -9 "$my_odir"/LOG*

done

# Generate a file that groups lines from the same test for all versions
basev=$1
nlines=$( awk '/^ops_sec/,/END/' "$odir"/"$basev"/report.tsv | grep -v ops_sec | wc -l )
hline=$( awk '/^ops_sec/ { print NR }' "$odir"/"$basev"/report.tsv )
sline=$(( hline + 1 ))
eline=$(( sline + nlines - 1 ))

sum_file="$odir"/summary.tsv

for v in "$@" ; do
  echo "$odir"/"$v"/report.tsv
done >> "$sum_file"
echo >> "$sum_file"

for x in $( seq "$sline" "$eline" ); do
  awk '{ if (NR == lno) { print $0 } }' lno="$hline" "$odir"/"$basev"/report.tsv >> "$sum_file"
  for v in "$@" ; do
    r="$odir"/"$v"/report.tsv
    awk '{ if (NR == lno) { print $0 } }' lno="$x" "$r" >> "$sum_file"
  done
echo >> "$sum_file"
done
