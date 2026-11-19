package org.rocksdb.ffm;

public interface DB {
  OpenDB open(String path) throws RocksFFMException;

  OpenDB open(RocksdbOptions options, String path) throws RocksFFMException;
}
