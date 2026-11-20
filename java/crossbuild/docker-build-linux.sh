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

# Use scl devtoolset if available
if hash scl 2>/dev/null; then
  if scl --list | grep -q 'devtoolset-8'; then
    # CentOS 6+
    scl enable devtoolset-8 'make clean-not-downloaded'
    scl enable devtoolset-8 "PORTABLE=1 J=$J make -j$J rocksdbjavastatic"
  elif scl --list | grep -q 'devtoolset-7'; then
    # CentOS 6+
    scl enable devtoolset-7 'make clean-not-downloaded'
    scl enable devtoolset-7 "PORTABLE=1 J=$J make -j$J rocksdbjavastatic"
  elif scl --list | grep -q 'devtoolset-2'; then
    # CentOS 5 or 6
    scl enable devtoolset-2 'make clean-not-downloaded'
    scl enable devtoolset-2 "PORTABLE=1 J=$J make -j$J rocksdbjavastatic"
  else
    echo "Could not find devtoolset"
    exit 1;
  fi
else
  make clean-not-downloaded
  PORTABLE=1 J=$J make -j$J rocksdbjavastatic
fi

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar java/target/rocksdbjni-*-linux*.jar.sha1 /rocksdb-java-target

