// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for IntComparator, but more generally
 * also for rocksdb::ComparatorJniCallback implementation.
 */
@RunWith(Parameterized.class)
public class IntComparatorTest {

  // test with 500 random integer keys
  private static final int TOTAL_KEYS = 500;
  private static final byte[][] keys = new byte[TOTAL_KEYS][4];

  @BeforeClass
  public static void prepareKeys() {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    final Random random = new Random();
    for (int i = 0; i < TOTAL_KEYS; i++) {
      final int ri = random.nextInt();
      buf.putInt(ri);
      buf.flip();
      final byte[] key = buf.array();

      // does key already exist (avoid duplicates)
      if (keyExists(key, i)) {
        i--; // loop round and generate a different key
      } else {
        System.arraycopy(key, 0, keys[i], 0, 4);
      }
    }
  }

  private static boolean keyExists(final byte[] key, final int limit) {
    for (int j = 0; j < limit; j++) {
      if (Arrays.equals(key, keys[j])) {
        return true;
      }
    }
    return false;
  }

  @Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        { "non-direct_reused64_mutex", false, 64, ReusedSynchronisationType.MUTEX },
        { "direct_reused64_mutex", true, 64, ReusedSynchronisationType.MUTEX },
        { "non-direct_reused64_adaptive-mutex", false, 64, ReusedSynchronisationType.ADAPTIVE_MUTEX },
        { "direct_reused64_adaptive-mutex", true, 64, ReusedSynchronisationType.ADAPTIVE_MUTEX },
        { "non-direct_reused64_thread-local", false, 64, ReusedSynchronisationType.THREAD_LOCAL },
        { "direct_reused64_thread-local", true, 64, ReusedSynchronisationType.THREAD_LOCAL },
        { "non-direct_noreuse", false, -1, null },
        { "direct_noreuse", true, -1, null }
    });
  }

  @Parameter(0)
  public String name;

  @Parameter(1)
  public boolean useDirectBuffer;

  @Parameter(2)
  public int maxReusedBufferSize;

  @Parameter(3)
  public ReusedSynchronisationType reusedSynchronisationType;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();


  @Test
  public void javaComparatorDefaultCf() throws RocksDBException {
    try (final ComparatorOptions options = new ComparatorOptions()
        .setUseDirectBuffer(useDirectBuffer)
        .setMaxReusedBufferSize(maxReusedBufferSize)
        // if reusedSynchronisationType == null we assume that maxReusedBufferSize <= 0 and so we just set ADAPTIVE_MUTEX, even though it won't be used
        .setReusedSynchronisationType(reusedSynchronisationType == null ? ReusedSynchronisationType.ADAPTIVE_MUTEX : reusedSynchronisationType);
        final IntComparator comparator = new IntComparator(options)) {

      // test the round-tripability of keys written and read with the Comparator
      testRoundtrip(FileSystems.getDefault().getPath(
          dbFolder.getRoot().getAbsolutePath()), comparator);
    }
  }

  @Test
  public void javaComparatorNamedCf() throws RocksDBException {
    try (final ComparatorOptions options = new ComparatorOptions()
        .setUseDirectBuffer(useDirectBuffer)
        .setMaxReusedBufferSize(maxReusedBufferSize)
        // if reusedSynchronisationType == null we assume that maxReusedBufferSize <= 0 and so we just set ADAPTIVE_MUTEX, even though it won't be used
        .setReusedSynchronisationType(reusedSynchronisationType == null ? ReusedSynchronisationType.ADAPTIVE_MUTEX : reusedSynchronisationType);
      final IntComparator comparator = new IntComparator(options)) {

      // test the round-tripability of keys written and read with the Comparator
      testRoundtripCf(FileSystems.getDefault().getPath(
          dbFolder.getRoot().getAbsolutePath()), comparator);
    }
  }

  /**
   * Test which stores random keys into the database
   * using an {@link IntComparator}
   * it then checks that these keys are read back in
   * ascending order
   *
   * @param db_path A path where we can store database
   *                files temporarily
   *
   * @param comparator the comparator
   *
   * @throws RocksDBException if a database error happens.
   */
  private void testRoundtrip(final Path db_path,
      final AbstractComparator comparator) throws RocksDBException {
    try (final Options opt = new Options()
             .setCreateIfMissing(true)
             .setComparator(comparator)) {

      // store TOTAL_KEYS into the db
      try (final RocksDB db = RocksDB.open(opt, db_path.toString())) {
        for (int i = 0; i < TOTAL_KEYS; i++) {
              db.put(keys[i], "value".getBytes(UTF_8));
        }
      }

      // re-open db and read from start to end
      // integer keys should be in ascending
      // order as defined by IntComparator
      final ByteBuffer key = ByteBuffer.allocate(4);
      try (final RocksDB db = RocksDB.open(opt, db_path.toString());
           final RocksIterator it = db.newIterator()) {
        it.seekToFirst();
        int lastKey = Integer.MIN_VALUE;
        int count = 0;
        for (it.seekToFirst(); it.isValid(); it.next()) {
          key.put(it.key());
          key.flip();
          final int thisKey = key.getInt();
          key.clear();
          assertThat(thisKey).isGreaterThan(lastKey);
          lastKey = thisKey;
          count++;
        }
        assertThat(count).isEqualTo(TOTAL_KEYS);
      }
    }
  }

  /**
   * Test which stores random keys into a column family
   * in the database
   * using an {@link IntComparator}
   * it then checks that these keys are read back in
   * ascending order
   *
   * @param db_path A path where we can store database
   *                files temporarily
   *
   * @param comparator the comparator
   *
   * @throws RocksDBException if a database error happens.
   */
  private void testRoundtripCf(final Path db_path,
      final AbstractComparator comparator) throws RocksDBException {

    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes(),
            new ColumnFamilyOptions()
                .setComparator(comparator))
    );

    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    try (final DBOptions opt = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)) {

      try (final RocksDB db = RocksDB.open(opt, db_path.toString(),
          cfDescriptors, cfHandles)) {
        try {
          assertThat(cfDescriptors.size()).isEqualTo(2);
          assertThat(cfHandles.size()).isEqualTo(2);

          for (int i = 0; i < TOTAL_KEYS; i++) {
            db.put(cfHandles.get(1), keys[i], "value".getBytes(UTF_8));
          }
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
          cfHandles.clear();
        }
      }

      // re-open db and read from start to end
      // integer keys should be in ascending
      // order as defined by SimpleIntComparator
      final ByteBuffer key = ByteBuffer.allocate(4);
      try (final RocksDB db = RocksDB.open(opt, db_path.toString(),
          cfDescriptors, cfHandles);
           final RocksIterator it = db.newIterator(cfHandles.get(1))) {
        try {
          assertThat(cfDescriptors.size()).isEqualTo(2);
          assertThat(cfHandles.size()).isEqualTo(2);

          it.seekToFirst();
          int lastKey = Integer.MIN_VALUE;
          int count = 0;
          for (it.seekToFirst(); it.isValid(); it.next()) {
            key.put(it.key());
            key.flip();
            final int thisKey = key.getInt();
            key.clear();
            assertThat(thisKey).isGreaterThan(lastKey);
            lastKey = thisKey;
            count++;
          }

          assertThat(count).isEqualTo(TOTAL_KEYS);

        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
          cfHandles.clear();
          for (final ColumnFamilyDescriptor cfDescriptor : cfDescriptors) {
            cfDescriptor.getOptions().close();
            cfDescriptor.close();
          }
        }
      }
    }
  }
}
