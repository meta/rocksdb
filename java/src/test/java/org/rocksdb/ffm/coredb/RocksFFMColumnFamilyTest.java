package org.rocksdb.ffm.coredb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ffm.*;

import java.io.IOException;
import java.lang.foreign.Arena;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.rocksdb.ffm.util.FFMSegment.segmentOf;
import static org.rocksdb.ffm.util.FFMSegment.stringOf;

public class RocksFFMColumnFamilyTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  static String dbPath;

  @Before
  public void setup() throws IOException {
    dbPath = dbFolder.newFolder().getAbsolutePath();
  }

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

              try (RocksdbWriteoptions writeoptions = new RocksdbWriteoptions()) {
                var putter = db.put();
                putter.put(writeoptions, cf2, segmentOf(arena, "key3"), segmentOf(arena, "value3"));
                putter.put(writeoptions, cf2, segmentOf(arena, "key5"), segmentOf(arena, "value5"));
              }

              try (RocksdbReadoptions readoptions = new RocksdbReadoptions()) {
                var getter = db.get();
                assertThat(getter.get(readoptions, segmentOf(arena, "key1"))).isNull();
                assertThat(stringOf(getter.get(readoptions, segmentOf(arena, "key2")))).isEqualTo("value2");
                assertThat(getter.get(readoptions, segmentOf(arena, "key3"))).isNull();
                assertThat(stringOf(getter.get(readoptions, segmentOf(arena, "key4")))).isEqualTo("value4");
                assertThat(getter.get(readoptions, segmentOf(arena, "key5"))).isNull();
              }

              try (RocksdbReadoptions readoptions = new RocksdbReadoptions()) {
                var getter = db.get();
                assertThat(getter.get(readoptions, cf2, segmentOf(arena, "key1"))).isNull();
                assertThat(getter.get(readoptions, cf2, segmentOf(arena, "key2"))).isNull();
                assertThat(stringOf(getter.get(readoptions, cf2, segmentOf(arena, "key3")))).isEqualTo("value3");
                assertThat(getter.get(readoptions, cf2, segmentOf(arena, "key4"))).isNull();
                assertThat(stringOf(getter.get(readoptions, cf2, segmentOf(arena, "key5")))).isEqualTo("value5");
              }
            }
          }

        }
      }
    }
  }

}
