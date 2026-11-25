package org.rocksdb.ffm;

public interface ColumnFamily {

  ColumnFamilyHandle defaultFamilyHandle();

  ColumnFamilyHandle createColumnFamily(String columnFamilyName, RocksdbOptions columnFamilyOptions) throws RocksFFMException;
}
