package org.rocksdb.ffm;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

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

  // C API returns its error(s) as a null-terminated string, which does not have a status code.
  // This is different from how we do it with the JNI-based API, which may cause issues.
  @Test
  public void failToOpen() {
    try (Arena arena = Arena.ofConfined()) {
      assertThatThrownBy(() -> RocksFFM.open(arena, dbPath.substring(0, dbPath.length() - 1)))
          .isInstanceOf(RocksDBException.class)
          .hasMessageContaining("does not exist (create_if_missing is false)");
    }
  }

  @Test
  public void read() throws RocksDBException {
    try (Arena arena = Arena.ofConfined()) {
      RocksFFM rocksFFM = RocksFFM.open(arena, dbPath);

      // value exists
      MemorySegment value2 = rocksFFM.get("key2");
      assertThat(RocksFFM.stringFromSegment(value2)).isEqualTo("value2");

      // value doesn't exist
      MemorySegment value4 = rocksFFM.get("key4");
      assertThat(value4).isNull();
    }
  }
}
