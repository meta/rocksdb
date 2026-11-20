package org.rocksdb.ffm.coredb;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.rocksdb.ffm.util.FFMSegment.segmentOf;
import static org.rocksdb.ffm.util.FFMSegment.stringOf;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ffm.RocksFFM;
import org.rocksdb.ffm.RocksFFMException;
import org.rocksdb.ffm.RocksdbOptions;
import org.rocksdb.ffm.RocksdbReadoptions;

public class RocksFFMTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  static String dbPath;

  @Before
  public void setup() throws IOException {
    dbPath = dbFolder.newFolder().getAbsolutePath();
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
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        rocksdbOptions.setCreateIfMissing((byte) 1);
        try (var db = RocksFFM.db(arena).open(rocksdbOptions, dbPath)) {
          try (var readOptions = new RocksdbReadoptions()) {
            var getter = db.get();
            MemorySegment value2 = getter.get(readOptions, segmentOf(arena, "key2"));
            assertThat(value2).isNull();

            // value doesn't exist
            MemorySegment value4 = getter.get(readOptions, segmentOf(arena, "key4"));
            assertThat(value4).isNull();
          }
        }
      }
    }
  }

  @Test
  public void readWriteRead() throws RocksFFMException {
    try (Arena arena = Arena.ofConfined()) {
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        rocksdbOptions.setCreateIfMissing((byte) 1);
        try (var db = RocksFFM.db(arena).open(rocksdbOptions, dbPath)) {
          try (var readOptions = new RocksdbReadoptions()) {
            // value doesn't exist
            var getter = db.get();
            MemorySegment value2 = getter.get(readOptions, segmentOf(arena, "key2"));
            assertThat(value2).isNull();

            var key4 = segmentOf(arena, "key4");
            var put4 = db.put(key4, segmentOf(arena, "wroteValue4"));
            put4.invoke();

            // value now exists exist
            MemorySegment value4 = getter.get(readOptions, segmentOf(arena, "key4"));
            assertThat(stringOf(value4)).isEqualTo("wroteValue4");
          }
        }
      }
    }
  }

  @Test
  public void walDir() {
    try (Arena arena = Arena.ofConfined()) {
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        String walDir = rocksdbOptions.getWalDir();
        assertThat(walDir).isEqualTo("");
      }
    }
  }
}
