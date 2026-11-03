package org.rocksdb.ffm;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.rocksdb.ffm.RocksFFM.segmentOf;
import static org.rocksdb.ffm.RocksFFM.stringOf;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RocksFFMTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  static String dbPath;

  final Map<String, String> values =
      Map.of("key1", "value1", "key2", "value2", "key3", "somethingElseEntirely");

  @Before
  public void setup() throws IOException {
    dbPath = dbFolder.newFolder().getAbsolutePath();
    /*
    final RocksFFMHelperJNI helper = new RocksFFMHelperJNI();
    helper.createDB(dbPath, values);

     */
  }

  @Test
  public void openNonExistent() {
    try (Arena arena = Arena.ofConfined()) {
      assertThatThrownBy(() -> RocksFFM.db(arena).open(dbPath))
          .isInstanceOf(RocksFFMException.class)
          .hasMessageContaining("does not exist (create_if_missing is false)");
    }
  }

  @Test
  public void read() throws RocksFFMException {
    try (Arena arena = Arena.ofConfined()) {
      var db = RocksFFM.db(arena).open(dbPath);

      var get2 = db.get(segmentOf(arena, "key2"));
      MemorySegment value2 = get2.invoke();
      assertThat(stringOf(value2)).isEqualTo("value2");

      // value doesn't exist
      var get4 = db.get(segmentOf(arena, "key4"));
      MemorySegment value4 = get4.invoke();
      assertThat(value4).isNull();
    }
  }

  @Test
  public void readWriteRead() throws RocksFFMException {
    try (Arena arena = Arena.ofConfined()) {
      var db = RocksFFM.db(arena).open(dbPath);
      var key4 = segmentOf(arena, "key4");

      // value doesn't exist
      var get4 = db.get(key4);
      MemorySegment value4 = get4.invoke();
      assertThat(value4).isNull();

      var put4 = db.put(key4, segmentOf(arena, "wroteValue4"));
      put4.invoke();

      var get4_2 = db.get(key4);
      MemorySegment value4_2 = get4_2.invoke();
      assertThat(stringOf(value4_2)).isEqualTo("wroteValue4");
    }
  }
}
