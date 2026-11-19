package org.rocksdb.ffm;

public interface Putter {
  void invoke() throws RocksFFMException;
}
