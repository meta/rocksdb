package org.rocksdb.ffm.coredb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ffm.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.rocksdb.ffm.util.FFMSegment.segmentOf;
import static org.rocksdb.ffm.util.FFMSegment.stringOf;

public class RocksFFMMultiGetTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  static String dbPath;

  @Before
  public void setup() throws IOException {
    dbPath = dbFolder.newFolder().getAbsolutePath();
  }

  @Test
  public void readWriteRead() throws RocksFFMException {
    try (Arena arena = Arena.ofConfined()) {
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        rocksdbOptions.setCreateIfMissing((byte) 1);
        try (var db = RocksFFM.db(arena).open(rocksdbOptions, dbPath)) {

          try (RocksdbWriteoptions writeoptions = new RocksdbWriteoptions()) {
            var putter = db.put();
            putter.put(writeoptions, segmentOf(arena, "key2"), segmentOf(arena, "value2"));
            putter.put(writeoptions, segmentOf(arena, "key4"), segmentOf(arena, "value4"));
          }

          ColumnFamilyHandle defaultColumnFamilyHandle = db.columnFamily().defaultFamilyHandle();

          List<String> keys = List.of("key1", "key2", "key3", "key4", "key5");
          try (RocksdbReadoptions readoptions = new RocksdbReadoptions()) {
            var result = db.multiGet().get(readoptions, defaultColumnFamilyHandle, keys, false);
            assertThat(result.getCount()).isEqualTo(5);
            assertThat(result.getValue(0)).isEqualTo(MemorySegment.NULL);
            assertThat(stringOf(result.getValue(1))).isEqualTo("value2");
            assertThat(result.getValue(2)).isEqualTo(MemorySegment.NULL);
            assertThat(stringOf(result.getValue(3))).isEqualTo("value4");
            assertThat(result.getValue(4)).isEqualTo(MemorySegment.NULL);

            for (int i = 0; i < keys.size(); i++) {
              assertThat(result.isError(i)).isFalse();
            }

            for (int i = 0; i < keys.size(); i++) {
              assertThat(result.getError(i)).isEqualTo(null);
            }
          }
        }
      }
    }
  }

  // TODO (AP) - write to the new CF

  @Test
  public void readEmptyNewColumnFamily() throws RocksFFMException {
    try (Arena arena = Arena.ofConfined()) {
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        rocksdbOptions.setCreateIfMissing((byte) 1);
        try (var db = RocksFFM.db(arena).open(rocksdbOptions, dbPath)) {

          try (RocksdbWriteoptions writeoptions = new RocksdbWriteoptions()) {
            var putter = db.put();
            putter.put(writeoptions, segmentOf(arena, "key2"), segmentOf(arena, "value2"));
            putter.put(writeoptions, segmentOf(arena, "key4"), segmentOf(arena, "value4"));
          }

          try (RocksdbOptions columnFamilyOptions = new RocksdbOptions()) {
            try (ColumnFamilyHandle cf2 = db.columnFamily().createColumnFamily("CF2", columnFamilyOptions)) {
              List<String> keys = List.of("key1", "key2", "key3", "key4", "key5");

              try (RocksdbReadoptions readoptions = new RocksdbReadoptions()) {
                var result = db.multiGet().get(readoptions, cf2, keys, false);
                assertThat(result.getCount()).isEqualTo(5);
                for (int i = 0; i < keys.size(); i++) {
                  assertThat(result.isError(i)).isFalse();
                  assertThat(result.getValue(i)).isEqualTo(MemorySegment.NULL);
                }
              }
            }
          }
        }
      }
    }
  }

}
