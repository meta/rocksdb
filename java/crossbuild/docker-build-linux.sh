#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -e
#set -x

# Set job parallelism to 1 (none) if it is not defined in the environment
if [ -z "${J}" ]; then
  J=1
fi

# just in-case this is run outside Docker
mkdir -p /rocksdb-local-build

rm -rf /rocksdb-local-build/*
cp -r /rocksdb-host/* /rocksdb-local-build
cd /rocksdb-local-build

# Detect and enable devtoolset if available
DEVTOOLSET=""
if hash scl 2>/dev/null; then
  # Check for devtoolsets in order of preference (newer first)
  if scl --list | grep -q 'devtoolset-12'; then
    DEVTOOLSET="devtoolset-12"
  elif scl --list | grep -q 'devtoolset-11'; then
    DEVTOOLSET="devtoolset-11"
  elif scl --list | grep -q 'devtoolset-10'; then
    DEVTOOLSET="devtoolset-10"
  elif scl --list | grep -q 'devtoolset-9'; then
    DEVTOOLSET="devtoolset-9"
  elif scl --list | grep -q 'devtoolset-8'; then
    DEVTOOLSET="devtoolset-8"
  elif scl --list | grep -q 'devtoolset-7'; then
    DEVTOOLSET="devtoolset-7"
  elif scl --list | grep -q 'devtoolset-2'; then
    DEVTOOLSET="devtoolset-2"
  fi
fi

# Build with devtoolset if available, otherwise use system compiler
# Add -Wno-error=restrict for GCC 12+ to avoid false positive warnings
if [ -n "$DEVTOOLSET" ]; then
  echo "Using $DEVTOOLSET"
  scl enable $DEVTOOLSET "bash -c 'make clean-not-downloaded'"
  scl enable $DEVTOOLSET "bash -c 'PORTABLE=1 J=$J make -j$J rocksdbjavastatic'"
else
  echo "No SCL devtoolset found, falling back to system compiler"
  make clean-not-downloaded
  PORTABLE=1 J=$J make -j$J rocksdbjavastatic
fi

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar java/target/rocksdbjni-*-linux*.jar.sha1 /rocksdb-java-target

