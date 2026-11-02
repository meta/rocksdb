package org.rocksdb.ffm;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.ffm.RocksFFM.*;

public class RocksFFMTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  static String dbPath;

  final Map<String, String> values =
      Map.of("key1", "value1", "key2", "value2", "key3", "somethingElseEntirely");

  @Before
  public void setup() throws IOException {
    final RocksFFMHelperJNI helper = new RocksFFMHelperJNI();
    dbPath = dbFolder.newFolder().getAbsolutePath();
    helper.createDB(dbPath, values);
  }

  @Test
  public void read() throws RocksDBException {
    try (Arena arena = Arena.ofConfined()) {
      var db = RocksFFM.db(arena).open(dbPath);

      var get2 = db.get("key2");
      MemorySegment value2 = get2.invoke();
      assertThat(RocksFFMBasic.stringFromSegment(value2)).isEqualTo("value2");

      // value doesn't exist
      var get4 = db.get("key4");
      MemorySegment value4 = get4.invoke();
      assertThat(value4).isNull();
    }
  }
}
