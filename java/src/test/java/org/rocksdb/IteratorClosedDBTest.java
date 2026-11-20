package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IteratorClosedDBTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ownedIterators() throws RocksDBException {
    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      byte[] key = {0x1};
      byte[] value = {0x2};
      db.put(key, value);

      RocksIterator it = db.newIterator();
      it.seekToFirst();
      assertThat(it.key()).isEqualTo(key);
      assertThat(it.value()).isEqualTo(value);

      it.next();
      assertThat(it.isValid()).isFalse();
    } // if iterator were still open when we close the DB, we would see a C++ assertion in
      // DEBUG_LEVEL=1
  }

  @Test
  public void shouldNotCrashJavaRocks() throws RocksDBException {
    try (Options options = new Options().setCreateIfMissing(true)) {
      RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
      byte[] key = {0x1};
      byte[] value = {0x2};
      db.put(key, value);

      RocksIterator it = db.newIterator();
      assertThat(it.isValid()).isFalse();
      it.seekToFirst();
      assertThat(it.isValid()).isTrue();

      byte[] valueOK = it.value();
      assertThat(valueOK).isEqualTo(value);

      // Close should work because iterator references are now cleaned up
      // Previously would have thrown an exception here (assertion failure in C++) -
      // when built with DEBUG_LEVEL=1
      // Because the outstanding iterator has a reference to the column family which is being closed
      db.close();

      try {
        byte[] valueShouldAssert = it.value();
        throw new RuntimeException("it.value() should cause an assertion");
      } catch (AssertionError ignored) {
      }

      // should assert
      try {
        boolean isValidShouldAssert = it.isValid();
        throw new RuntimeException("it.isValid() should cause an assertion");
      } catch (AssertionError ignored) {
      }

      // Multiple close() should be fine/no-op
      it.close();
      it.close();
    }
  }
}
