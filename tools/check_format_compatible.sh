#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# A shell script to build and run different versions of ldb to check for
# expected forward and backward compatibility with "current" version. The
# working copy must have no uncommitted changes.
#
# Usage: <SCRIPT> [ref_for_current]
# `ref_for_current` can be a revision, tag, commit or branch name. Default is HEAD.
#
# Return value 0 means all regression tests pass. 1 if not pass.
#
# Environment options:
#  SANITY_CHECK=1 - Do a syntax check and git checkout test as a sanity check
#    that the script hasn't been broken by e.g. adding a new release wrongly.
#  SHORT_TEST=1 - Test only the oldest branch for each kind of test. This is
#    a good choice for PR validation as it is relatively fast and will find
#    most issues.
#  LONG_TEST=1 - Test all branches known to build for this test, rather than
#    the default of randomly sampling the branches that aren't the oldest in
#    each set.
#  USE_SSH=1 - Connect to GitHub with ssh instead of https

if ! git diff-index --quiet HEAD; then
  echo "You have uncommitted changes. Aborting."
  exit 1
fi

current_checkout_name=${1:-HEAD}
# This allows the script to work even if with transient refs like "HEAD"
current_checkout_hash="$(git rev-parse --quiet --verify $current_checkout_name)"

if [ "$current_checkout_hash" == "" ]; then
  echo "Not a recognized ref: $current_checkout_name"
  exit 1
fi

# To restore to prior branch at the end
orig_branch="$(git rev-parse --abbrev-ref HEAD)"
tmp_branch=_tmp_format_compatible
tmp_origin=_tmp_origin

# Don't depend on what current "origin" might be
set -e
git remote remove $tmp_origin 2>/dev/null || true
if [ "$USE_SSH" ]; then
  git remote add $tmp_origin "git@github.com:meta/rocksdb.git"
else
  git remote add $tmp_origin "https://github.com/meta/rocksdb.git"
fi
git fetch $tmp_origin

# Used in building some ancient RocksDB versions where by default it tries to
# use a precompiled libsnappy.a checked in to the repo.
export SNAPPY_LDFLAGS=-lsnappy

cleanup() {
  echo "== Cleaning up"
  git reset --hard || true
  git checkout "$orig_branch" || true
  git branch -D $tmp_branch || true
  git remote remove $tmp_origin || true
}
trap cleanup EXIT # Always clean up, even on failure or Ctrl+C

scriptpath=`dirname ${BASH_SOURCE[0]}`

test_dir=${TEST_TMPDIR:-"/tmp"}"/rocksdb_format_compatible_$USER"
rm -rf ${test_dir:?}
# Also clean the noexec fallback dir from any prior run (see cs_test_dir below).
rm -rf "$PWD/tmp/cs"

# Prevent 'make clean' etc. from wiping out test_dir
export TEST_TMPDIR=$test_dir"/misc"

# For saving current version of scripts as we checkout different versions to test
script_copy_dir=$test_dir"/script_copy"
mkdir -p $script_copy_dir
cp -f $scriptpath/*.sh $script_copy_dir

# For shared raw input data
input_data_path=$test_dir"/test_data_input"
mkdir -p $input_data_path
# For external sst ingestion test
ext_test_dir=$test_dir"/ext"
mkdir -p $ext_test_dir
# For DB dump test
db_test_dir=$test_dir"/db"
mkdir -p $db_test_dir
# For backup/restore test (uses DB test)
bak_test_dir=$test_dir"/bak"
mkdir -p $bak_test_dir
# For remote compaction test.  The saved ldb binary must be executable.
# Detect noexec early and fall back to $PWD/tmp/cs (git-ignored).
cs_test_dir=$test_dir"/cs"
mkdir -p $cs_test_dir
if cp /bin/true $cs_test_dir/_exec_test 2>/dev/null && \
   ! $cs_test_dir/_exec_test 2>/dev/null; then
  echo "   $cs_test_dir is noexec, using $PWD/tmp/cs instead"
  cs_test_dir="$PWD/tmp/cs"
  mkdir -p $cs_test_dir
fi
rm -f $cs_test_dir/_exec_test

python_bin=$(which python3 || which python || echo python3)

if [ "$J" == "" ]; then
  # make parallelism
  J=32
fi

# Generate random files. We want enough files to cover a variety of compression
# settings and low enough entropy in the files to make compression highly
# likely, including for index blocks.
for i in {1..8}
do
  input_data[$i]=$input_data_path/data$i
  echo == Generating random input file ${input_data[$i]}
  $python_bin - <<EOF
import random
random.seed($i)
symbols=['a', 'b', 'c', 'd', 'e', 'f', 'g']
with open('${input_data[$i]}', 'w') as f:
  for i in range(1,1024):
    k = ""
    for j in range(1, random.randint(1,80)):
      k=k + symbols[random.randint(0, len(symbols) - 1)]
    vb = ""
    for j in range(1, random.randint(0,128)):
      vb = vb + symbols[random.randint(0, len(symbols) - 1)]
    v = ""
    for j in range(1, random.randint(1, 5)):
      v = v + vb
    print(k + " ==> " + v, file=f)
EOF
done

# Generate file(s) with sorted keys.
sorted_input_data=$input_data_path/sorted_data
echo == Generating file with sorted keys ${sorted_input_data}
$python_bin - <<EOF
with open('${sorted_input_data}', 'w') as f:
  for i in range(0,10):
    k = str(i)
    v = "value" + k
    print(k + " ==> " + v, file=f)
EOF

# Generate a file with uniformly distributed keys
uniform_input_data=$input_data_path/uniform_data
echo == Generating file with uniform keys ${uniform_input_data}
$python_bin - <<EOF
with open('${uniform_input_data}', 'w') as f:
  for i in range(0, 2000):
    k = f"{i:012d}"
    v = f"value{i:012d}"
    print(k + " ==> " + v, file=f)
EOF

# db_backward_only_refs defined below the rest

# To check for DB forward compatibility with loading options (old version
# reading data from new), as well as backward compatibility
declare -a db_forward_with_options_refs=("10.4.fb" "10.5.fb" "10.6.fb" "10.7.fb" "10.8.fb" "10.9.fb" "10.10.fb" "10.11.fb" "11.0.fb" "11.1.fb" "11.2.fb" "11.3.fb" "11.4.fb" "11.5.fb" "11.6.fb" "11.7.fb" "11.8.fb" "11.9.fb" "11.10.fb" "11.11.fb" "11.12.fb" "11.13.fb")
# To check for DB forward compatibility without loading options (in addition
# to the "with loading options" set), as well as backward compatibility
declare -a db_forward_no_options_refs=() # N/A at the moment

# To check for SST ingestion backward compatibility (new version reading
# data from old) (ldb ingest_extern_sst added in 5.16.x, back-ported to
# 5.14.x, 5.15.x)
declare -a ext_backward_only_refs=("5.14.fb" "5.15.fb" "5.16.fb" "5.17.fb" "5.18.fb" "6.0.fb" "6.1.fb" "6.2.fb" "6.3.fb" "6.4.fb" "6.5.fb" "6.6.fb" "6.7.fb" "6.8.fb" "6.9.fb" "6.10.fb" "6.11.fb" "6.12.fb" "6.13.fb" "6.14.fb" "6.15.fb" "6.16.fb" "6.17.fb" "6.18.fb" "6.19.fb" "6.20.fb" "6.21.fb" "6.22.fb" "6.23.fb" "6.24.fb" "6.25.fb" "6.26.fb" "6.27.fb" "6.28.fb" "6.29.fb" "7.0.fb" "7.1.fb" "7.2.fb" "7.3.fb" "7.4.fb" "7.5.fb" "7.6.fb" "7.7.fb" "7.8.fb" "7.9.fb" "7.10.fb" "8.0.fb" "8.1.fb" "8.2.fb" "8.3.fb" "8.4.fb" "8.5.fb" "8.6.fb" "8.7.fb" "8.8.fb" "8.9.fb" "8.10.fb" "8.11.fb" "9.0.fb" "9.1.fb" "9.2.fb" "9.3.fb" "9.4.fb" "9.5.fb" "9.6.fb" "9.7.fb" "9.8.fb" "9.9.fb" "9.10.fb" "9.11.fb" "10.0.fb" "10.1.fb" "10.2.fb" "10.3.fb")
# To check for SST ingestion forward compatibility (old version reading
# data from new) as well as backward compatibility
declare -a ext_forward_refs=("${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}")

# To check for backup backward compatibility (new version reading data
# from old) (ldb backup/restore added in 4.11.x)
declare -a bak_backward_only_refs=("4.11.fb" "4.12.fb" "4.13.fb" "5.0.fb" "5.1.fb" "5.2.fb" "5.3.fb" "5.4.fb" "5.5.fb" "5.6.fb" "5.7.fb" "5.8.fb" "5.9.fb" "5.10.fb" "5.11.fb" "5.12.fb" "5.13.fb" "${ext_backward_only_refs[@]}")
# To check for backup forward compatibility (old version reading data
# from new) as well as backward compatibility
declare -a bak_forward_refs=("${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}")

# Branches (git refs) to check for DB backward compatibility (new version
# reading data from old) (in addition to the "forward compatible" list)
# NOTE: format_version < 2 support was removed, so we only test back to 4.6.fb
# (when format_version=2 became the default)
declare -a db_backward_only_refs=("4.6.fb" "4.7.fb" "4.8.fb" "4.9.fb" "4.10.fb" "${bak_backward_only_refs[@]}")

validate_release_refs()
{
  local list_name="$1"
  local checkout_ref
  shift
  for checkout_ref in "$@"
  do
    if [[ ! "$checkout_ref" =~ ^[0-9]+\.[0-9]+\.fb$ ]]; then
      echo "Invalid ref in ${list_name}: '${checkout_ref}'" >&2
      exit 1
    fi
  done
}

validate_release_refs "db_backward_only_refs" "${db_backward_only_refs[@]}"
validate_release_refs "db_forward_no_options_refs" "${db_forward_no_options_refs[@]}"
validate_release_refs "db_forward_with_options_refs" "${db_forward_with_options_refs[@]}"
validate_release_refs "ext_backward_only_refs" "${ext_backward_only_refs[@]}"
validate_release_refs "ext_forward_refs" "${ext_forward_refs[@]}"
validate_release_refs "bak_backward_only_refs" "${bak_backward_only_refs[@]}"
validate_release_refs "bak_forward_refs" "${bak_forward_refs[@]}"

if [ "$SHORT_TEST" ]; then
  # Use only the first (if exists) of each list
  db_backward_only_refs=(${db_backward_only_refs[0]})
  db_forward_no_options_refs=(${db_forward_no_options_refs[0]})
  db_forward_with_options_refs=(${db_forward_with_options_refs[0]})
  ext_backward_only_refs=(${ext_backward_only_refs[0]})
  ext_forward_refs=(${ext_forward_refs[0]})
  bak_backward_only_refs=(${bak_backward_only_refs[0]})
  bak_forward_refs=(${bak_forward_refs[0]})
fi

# De-duplicate & accumulate
declare -a checkout_refs=()
# Always include first release on each list
for checkout_ref in "${db_backward_only_refs[0]}" "${db_forward_no_options_refs[0]}" "${db_forward_with_options_refs[0]}" "${ext_backward_only_refs[0]}" "${ext_forward_refs[0]}" "${bak_backward_only_refs[0]}" "${bak_forward_refs[0]}"
do
  if [ ! -e "$db_test_dir/$checkout_ref" ]; then
    mkdir -p "$db_test_dir/$checkout_ref"
    checkout_refs+=("$checkout_ref")
  fi
done
# Maybe include rest if not short test
if [ "$SHORT_TEST" == "" ]; then
  for checkout_ref in "${db_backward_only_refs[@]}" "${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}" "${ext_backward_only_refs[@]}" "${ext_forward_refs[@]}" "${bak_backward_only_refs[@]}" "${bak_forward_refs[@]}"
  do
    if [ ! -e "$db_test_dir/$checkout_ref" ]; then
      mkdir -p "$db_test_dir/$checkout_ref"
      # Randomly sample remaining releases, unless long test
      if [ "$LONG_TEST" ] || [ "$(($RANDOM % 3))" == "0" ]; then
        checkout_refs+=("$checkout_ref")
      fi
    fi
  done
fi

invoke_make()
{
    [ "$SANITY_CHECK" ] || make "$@"
}
generate_db()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/generate_random_db.sh "$1" "$2" "$3"
    if [ $? -ne 0 ]; then
        echo ==== Error loading data from $2 to $1 ====
        exit 1
    fi
    set -e
}

compare_db()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/verify_random_db.sh "$1" "$2" "$3" "$4" "$5"
    if [ $? -ne 0 ]; then
        echo ==== Read different content from $1 and $2 or error happened. ====
        exit 1
    fi
    set -e
}

compact_db()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/compact_db.sh "$1" "$2" "$3"
    if [ $? -ne 0 ]; then
        echo ==== Error compacting DB at $1 ====
        exit 1
    fi
    set -e
}

write_external_sst()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/write_external_sst.sh "$1" "$2" "$3"
    if [ $? -ne 0 ]; then
        echo ==== Error writing external SST file using data from $1 to $3 ====
        exit 1
    fi
    set -e
}

ingest_external_sst()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/ingest_external_sst.sh "$1" "$2"
    if [ $? -ne 0 ]; then
        echo ==== Error ingesting external SST in $2 to DB at $1 ====
        exit 1
    fi
    set -e
}

backup_db()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/backup_db.sh "$1" "$2"
    if [ $? -ne 0 ]; then
        echo ==== Error backing up DB $1 to $2 ====
        exit 1
    fi
    set -e
}

restore_db()
{
    set +e
    [ "$SANITY_CHECK" ] || bash "$script_copy_dir"/restore_db.sh "$1" "$2"
    if [ $? -ne 0 ]; then
        echo ==== Error restoring from $1 to $2 ====
        exit 1
    fi
    set -e
}

member_of_array()
{
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

# Run one cross-version remote compaction compatibility test.
# Generates a DB using the primary's ldb (so its OPTIONS file matches the
# primary's version), then runs primary and worker from potentially
# different versions. They coordinate via files in test_dir (see
# remote_compaction_primary/worker ldb commands for protocol details).
# Tests the wire format of CompactionServiceInput/Result between versions.
run_cs_compat_test()
{
  local test_label="$1"
  local primary_ldb="$2"
  local worker_ldb="$3"
  local cs_run_dir="$4"

  echo "== $test_label"
  rm -rf "$cs_run_dir" && mkdir -p "$cs_run_dir"
  generate_db $input_data_path "$cs_run_dir/db" "$primary_ldb"
  # Write keys spanning the full key range to create an L0 file that
  # overlaps with existing L0 files, ensuring CompactRange triggers a
  # real compaction (not a trivial move).
  printf "a ==> overlap_start\nzzzzzzz ==> overlap_end\n" | \
    $primary_ldb load --db="$cs_run_dir/db" --auto_compaction=false
  $worker_ldb remote_compaction_worker --db="$cs_run_dir/db" --job_dir="$cs_run_dir" &
  local worker_pid=$!
  local primary_exit=0
  $primary_ldb remote_compaction_primary --db="$cs_run_dir/db" --job_dir="$cs_run_dir" || primary_exit=$?
  local worker_exit=0
  wait $worker_pid || worker_exit=$?
  if [ $primary_exit -ne 0 ] || [ $worker_exit -ne 0 ]; then
    echo "==== Error running remote compaction: $test_label (primary=$primary_exit worker=$worker_exit) ===="
    kill $worker_pid 2>/dev/null || true
    exit 1
  fi
}

force_no_fbcode()
{
  # Not all branches recognize ROCKSDB_NO_FBCODE and we should not need
  # to patch old branches for changes to available FB compilers.
  sed -i -e 's|-d /mnt/gvfs/third-party|"$ROCKSDB_FORCE_FBCODE"|' build_tools/build_detect_platform
  # Fix a build issue affecting at least 4.2.fb
  if [ -e include/rocksdb/delete_scheduler.h ]; then
    sed -i -e 's|pragma once|pragma once\n#include <memory>|' include/rocksdb/delete_scheduler.h
  fi
}

# General structure from here:
# * Check out, build, and do stuff with the "current" branch.
# * For each older branch under consideration,
#   * Check out, build, and do stuff with it, potentially using data
#     generated from "current" branch.
# * (Again) check out, build, and do (other) stuff with the "current"
#    branch, potentially using data from older branches.
#
# This way, we only do at most n+1 checkout+build steps.  The one
# exception is the remote compaction test, which saves a copy of the
# current ldb binary before old-ref checkouts overwrite ./ldb, so both
# versions can run simultaneously as primary and worker.

# Decorate name
current_checkout_name="$current_checkout_name ($current_checkout_hash)"

echo "== Building $current_checkout_name debug"
git checkout -B $tmp_branch $current_checkout_hash
force_no_fbcode
invoke_make clean
DISABLE_WARNING_AS_ERROR=1 invoke_make ldb -j$J

# Save current ldb for cross-version remote compaction tests.  The old-ref
# checkout will overwrite ./ldb, so we save a copy now.
save_cs_current_ldb()
{
  cs_current_ldb=$cs_test_dir/current_ldb
  cp -f ./ldb $cs_current_ldb
  # Copy shared libs next to the binary for LD_LIBRARY_PATH.
  # Static builds have no .so files - cp fails harmlessly.
  cp -f ./librocksdb*.so* $cs_test_dir/ 2>/dev/null || true
  cs_current_ldb_cmd="env LD_LIBRARY_PATH=$cs_test_dir $cs_current_ldb"
  cs_ldb_output=$($cs_current_ldb_cmd --version 2>&1)
  cs_ldb_exit=$?
  return $cs_ldb_exit
}
echo "== Saving current ldb for remote compaction cross-version tests"
cs_current_ldb_cmd=""
if [ ! "$SANITY_CHECK" ]; then
  if ! save_cs_current_ldb; then
    echo "==== Error: saved current ldb cannot run from $cs_test_dir (exit=$cs_ldb_exit): $cs_ldb_output ===="
    exit 1
  fi
  # Smoke test before cross-version tests to catch integration failures early.
  run_cs_compat_test \
    "Remote compaction smoke test: current primary + current worker" \
    "$cs_current_ldb_cmd" "$cs_current_ldb_cmd" "$cs_test_dir/smoke"
fi

echo "== Using $current_checkout_name, generate DB with extern SST and ingest"
current_ext_test_dir=$ext_test_dir"/current"
write_external_sst $input_data_path ${current_ext_test_dir}_pointless $current_ext_test_dir
ingest_external_sst ${current_ext_test_dir}_ingest $current_ext_test_dir

echo "== Generating DB from $current_checkout_name ..."
current_db_test_dir=$db_test_dir"/current"
generate_db $input_data_path $current_db_test_dir

echo "== Creating backup of DB from $current_checkout_name ..."
current_bak_test_dir=$bak_test_dir"/current"
backup_db $current_db_test_dir $current_bak_test_dir

for checkout_ref in "${checkout_refs[@]}"
do
  echo "== Building $checkout_ref debug"
  git reset --hard $tmp_origin/$checkout_ref
  force_no_fbcode
  invoke_make clean
  DISABLE_WARNING_AS_ERROR=1 invoke_make ldb -j$J

  # We currently assume DB backward compatibility for every branch listed
  echo "== Use $checkout_ref to generate a DB ..."
  generate_db $input_data_path $db_test_dir/$checkout_ref

  if member_of_array "$checkout_ref" "${ext_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to generate DB with extern SST file"
    write_external_sst $input_data_path $ext_test_dir/${checkout_ref}_pointless $ext_test_dir/$checkout_ref
  fi

  if member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to ingest extern SST file and compare vs. $current_checkout_name"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $ext_test_dir/$checkout_ref
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1

    rm -rf ${ext_test_dir:?}/${checkout_ref}_ingest
    echo "== Use $checkout_ref to ingest extern SST file from $current_checkout_name"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $current_ext_test_dir
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${db_forward_no_options_refs[@]}" ||
    member_of_array "$checkout_ref" "${db_forward_with_options_refs[@]}"
  then
    echo "== Use $checkout_ref to open DB generated using $current_checkout_name..."
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 0

    echo "== Use $checkout_ref to compact a copy of DB generated using $current_checkout_name..."
    [ "$SANITY_CHECK" ] || cp -a $current_db_test_dir ${current_db_test_dir}_copy_for_${checkout_ref}
    compact_db ${current_db_test_dir}_copy_for_${checkout_ref} 0

    echo "== After compaction, re-verify DB copy originally from $current_checkout_name..."
    compare_db ${current_db_test_dir}_copy_for_${checkout_ref} $current_db_test_dir forward_${checkout_ref}_dump_after_compact.txt 0
  fi

  if member_of_array "$checkout_ref" "${db_forward_with_options_refs[@]}"
  then
    echo "== Use $checkout_ref to open DB generated using $current_checkout_name with its options..."
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${bak_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to backup DB"
    backup_db $db_test_dir/$checkout_ref $bak_test_dir/$checkout_ref
  fi

  if member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to restore DB from $current_checkout_name"
    rm -rf ${db_test_dir:?}/$checkout_ref
    restore_db $current_bak_test_dir $db_test_dir/$checkout_ref
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 0
  fi

  # Remote compaction format compatibility: test that primary and worker from
  # different versions can exchange CompactionServiceInput/Result.
  # Requires db_forward_with_options_refs (the old worker must be able to
  # read OPTIONS written by the current primary) and the remote compaction
  # related ldb commands.
  # Skipped in SANITY_CHECK mode (no ldb binary to test).
  if [ ! "$SANITY_CHECK" ] &&
    [ -n "$cs_current_ldb_cmd" ] &&
    member_of_array "$checkout_ref" "${db_forward_with_options_refs[@]}"
  then
    if ./ldb --help 2>&1 | grep -q remote_compaction_primary; then
      cs_old_ldb_cmd="./ldb"
      ref_dir=$cs_test_dir/$checkout_ref
      run_cs_compat_test \
        "Remote compaction compatibility: current primary + $checkout_ref worker" \
        "$cs_current_ldb_cmd" "$cs_old_ldb_cmd" "$ref_dir/test1"
      run_cs_compat_test \
        "Remote compaction compatibility: $checkout_ref primary + current worker" \
        "$cs_old_ldb_cmd" "$cs_current_ldb_cmd" "$ref_dir/test2"
    else
      echo "   remote_compaction commands not available at $checkout_ref, skipping"
    fi
  fi
done

echo "== Building $current_checkout_name debug (again, final)"
git reset --hard $current_checkout_hash
force_no_fbcode
invoke_make clean
DISABLE_WARNING_AS_ERROR=1 invoke_make ldb -j$J

for checkout_ref in "${checkout_refs[@]}"
do
  # We assume DB backward compatibility for every branch listed
  echo "== Use $current_checkout_name to open DB generated using $checkout_ref..."
  compare_db $db_test_dir/$checkout_ref $current_db_test_dir db_dump.txt 1 0

  echo "== Use $current_checkout_name to compact DB generated using $checkout_ref..."
  compact_db $db_test_dir/$checkout_ref 1 0

  echo "== After compaction, re-verify DB originally from $checkout_ref..."
  compare_db $db_test_dir/$checkout_ref $current_db_test_dir db_dump_after_compact.txt 1 0

  if member_of_array "$checkout_ref" "${ext_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    rm -rf ${ext_test_dir:?}/${checkout_ref}_ingest
    echo "== Use $current_checkout_name to ingest extern SST file from $checkout_ref"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $current_ext_test_dir
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${bak_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $current_checkout_name to restore DB from $checkout_ref"
    rm -rf ${db_test_dir:?}/$checkout_ref
    restore_db $bak_test_dir/$checkout_ref $db_test_dir/$checkout_ref
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir db_dump.txt 1 0
  fi

done

if [ "$SANITY_CHECK" ]; then
  echo "==== check_format_compatible.sh sanity check PASSED ===="
else
  echo ==== Compatibility Test PASSED ====
fi
