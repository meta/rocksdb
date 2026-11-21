package org.rocksdb.ffm;

public interface DBSpec {
  DB open(String path) throws RocksFFMException;

  DB open(RocksdbOptions options, String path) throws RocksFFMException;
}
