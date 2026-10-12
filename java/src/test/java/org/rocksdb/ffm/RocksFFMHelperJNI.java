package org.rocksdb.ffm;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.rocksdb.*;

public class RocksFFMHelperJNI {
  void createDB(String db_path, Map<String, String> entries) {
    try (final RocksDB db = RocksDB.open(db_path)) {
      for (var entry : entries.entrySet()) {
        db.put(entry.getKey().getBytes(StandardCharsets.UTF_8),
            entry.getValue().getBytes(StandardCharsets.UTF_8));
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  void createCF(String db_path, String cf_name) {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, db_path)) {
      // create column family
      try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
               new ColumnFamilyDescriptor(cf_name.getBytes(), new ColumnFamilyOptions()))) {
        assert (columnFamilyHandle != null);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }
}
