// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.BytewiseComparator;

/**
 * This test confirms that the following issues were in fact resolved
 * by a change made between 6.2.2 and 6.22.1,
 * to wit {@link <a href="https://github.com/meta/rocksdb/commit/7242dae7">...</a>}
 * which as part of its effect, changed the Java bytewise comparators.
 * <p>
 * {@link <a href="https://github.com/meta/rocksdb/issues/5891">...</a>}
 * {@link <a href="https://github.com/meta/rocksdb/issues/2001">...</a>}
 */
public class BytewiseComparatorRegressionTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public TemporaryFolder temporarySSTFolder = new TemporaryFolder();

  private static final byte[][] testData = {{10, -11, 13}, {10, 11, 12}, {10, 11, 14}};
  private static final byte[][] orderedData = {{10, 11, 12}, {10, 11, 14}, {10, -11, 13}};

  /**
   * {@link <a href="https://github.com/meta/rocksdb/issues/5891">...</a>}
   */
  @Test
  public void testJavaComparator() throws RocksDBException {
    final BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(comparator)) {
      performTest(options);
    }
  }

  @Test
  public void testDefaultComparator() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      performTest(options);
    }
  }

  /**
   * {@link <a href="https://github.com/meta/rocksdb/issues/5891">...</a>}
   */
  @Test
  public void testCppComparator() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(
             BuiltinComparator.BYTEWISE_COMPARATOR)) {
      performTest(options);
    }
  }

  private void performTest(final Options options) throws RocksDBException {
    try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      for (final byte[] item : testData) {
        db.put(item, item);
      }
      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        final ArrayList<byte[]> result = new ArrayList<>();
        while (iterator.isValid()) {
          result.add(iterator.key());
          iterator.next();
        }
        assertArrayEquals(orderedData, result.toArray());
      }
    }
  }

  private byte[] hexToByte(final String hexString) {
    final byte[] bytes = new byte[hexString.length() / 2];
    if (bytes.length * 2 < hexString.length()) {
      throw new RuntimeException("Hex string has odd length: " + hexString);
    }

    for (int i = 0; i < bytes.length; i++) {
      final int firstDigit = toDigit(hexString.charAt(i + i));
      final int secondDigit = toDigit(hexString.charAt(i + i + 1));
      bytes[i] = (byte) ((firstDigit << 4) + secondDigit);
    }

    return bytes;
  }

  private int toDigit(final char hexChar) {
    final int digit = Character.digit(hexChar, 16);
    if (digit == -1) {
      throw new IllegalArgumentException("Invalid Hexadecimal Character: " + hexChar);
    }
    return digit;
  }

  /**
   * {@link <a href="https://github.com/meta/rocksdb/issues/2001">...</a>}
   *
   * @throws RocksDBException if something goes wrong, or if the regression occurs
   * @throws IOException if we can't make the temporary file
   */
  @Test
  public void testSST() throws RocksDBException, IOException {
    final File tempSSTFile = temporarySSTFolder.newFile("test_file_with_weird_keys.sst");

    final EnvOptions envOpts = new EnvOptions();
    final Options opts = new Options();
    opts.setComparator(new BytewiseComparator(new ComparatorOptions()));
    final SstFileWriter writer = new SstFileWriter(envOpts, opts);
    writer.open(tempSSTFile.getAbsolutePath());
    final byte[] gKey =
        hexToByte("000000293030303030303030303030303030303030303032303736343730696E666F33");
    final byte[] wKey =
        hexToByte("0000008d3030303030303030303030303030303030303030303437363433696e666f34");
    writer.put(new Slice(gKey), new Slice("dummyV1"));
    writer.put(new Slice(wKey), new Slice("dummyV2"));
    writer.finish();
  }
}
