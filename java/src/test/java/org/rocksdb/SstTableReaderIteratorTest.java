// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.rocksdb.SstFileReaderTest.newSstFile;
import static org.rocksdb.util.ByteBufferAllocator.DIRECT;
import static org.rocksdb.util.ByteBufferAllocator.HEAP;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.SstFileReaderTest.KeyValueWithOp;
import org.rocksdb.SstFileReaderTest.OpType;
import org.rocksdb.util.ByteBufferAllocator;

@RunWith(Parameterized.class)
public class SstTableReaderIteratorTest {

  private static File sstFile;
  private static List<KeyValueWithOp> kvs;

  @ClassRule
  public static TemporaryFolder parentFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {"direct-direct", DIRECT, DIRECT},
        {"direct-indirect", DIRECT, HEAP},
        {"indirect-direct", HEAP, DIRECT},
        {"indirect-indirect", HEAP, HEAP},
    });
  }

  public SstTableReaderIteratorTest(String name, ByteBufferAllocator userByteBufferAllocator,
      ByteBufferAllocator internalByteBufferAllocator) throws RocksDBException, IOException {
    this.name = name;
    this.userByteBufferAllocator = userByteBufferAllocator;
    this.internalByteBufferAllocator = internalByteBufferAllocator;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    kvs = new ArrayList<>();
    sstFile = createSstFileWithKeys(kvs);
  }


  private static File createSstFileWithKeys(List<KeyValueWithOp> keyValues) throws IOException,
      RocksDBException {
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key11", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key12", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key21", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key22", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key31", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key32", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key33", "value33_merge", OpType.MERGE));
    return newSstFile(parentFolder, keyValues);
  }

  private final String name;

  private final ByteBufferAllocator userByteBufferAllocator;

  private final ByteBufferAllocator internalByteBufferAllocator;

  private void assertInternalKey(final Options options, final ParsedEntryInfo parsedEntryInfo,
      final SstFileReaderIterator sstFileReaderIterator, final ByteBuffer internalKey, ByteBuffer userKey,
      final String expectedUserKey, final EntryType expectedEntryType) {
    // Adding random 4 bytes before getting the internal key length.
    internalKey.putInt(1056);
    sstFileReaderIterator.key(internalKey);
    userKey.clear();
    // Adding random 4 bytes before getting the user key length.
    userKey.putInt(1039);
    assertEquals(4, userKey.position());
    assertEquals(128 - 4, userKey.remaining());
    parsedEntryInfo.parseEntry(options, internalKey);
    assertEquals(0, internalKey.remaining());
    byte[] expectedUserKeyBytes = expectedUserKey.getBytes();
    assertThat(expectedUserKeyBytes.length).isEqualTo(parsedEntryInfo.userKey(userKey));
    assertThat(userKey.position()).isEqualTo(4);
    assertThat(userKey.remaining()).isEqualTo(expectedUserKeyBytes.length);
    byte[] dst = new byte[expectedUserKeyBytes.length];
    userKey.get(dst);
    assertEquals(new String(expectedUserKeyBytes), new String(dst));
    assertEquals(expectedEntryType, parsedEntryInfo.getEntryType());
    internalKey.clear();
    userKey.clear();
    assertInternalKeyByteArray(options, parsedEntryInfo, sstFileReaderIterator.key(), expectedUserKey,
        expectedEntryType);
  }

  private void assertInternalKeyByteArray(final Options options, final ParsedEntryInfo parsedEntryInfo,
      final byte[] internalKey, final String expectedUserKey, final EntryType expectedEntryType) {
    parsedEntryInfo.parseEntry(options, internalKey);
    byte[] expectedUserKeyBytes = expectedUserKey.getBytes();
    assertArrayEquals(expectedUserKeyBytes, parsedEntryInfo.getUserKey());
    assertEquals(expectedEntryType, parsedEntryInfo.getEntryType());
  }

  private void seekTableIterator(final SstFileReaderIterator iterator, final ByteBuffer userKey,
                                 final ByteBuffer internalKey, final Options options) {
    byte[] userKeyArray = new byte[userKey.remaining()];
    // Adding random 4 bytes before getting the user key length.
    internalKey.putInt(1540);
    userKey.asReadOnlyBuffer().get(userKeyArray);
    int len = TypeUtil.getInternalKey(userKey, internalKey, options);
    byte[] internalKeyArray = TypeUtil.getInternalKey(userKeyArray, options);
    assertEquals(4, internalKey.position());
    assertEquals(len, internalKey.remaining());
    byte[] internalKeyArrayFromByteBuffer = new byte[internalKey.remaining()];
    internalKey.asReadOnlyBuffer().get(internalKeyArrayFromByteBuffer);
    assertArrayEquals(internalKeyArray, internalKeyArrayFromByteBuffer);
    iterator.seek(internalKey);
    assertThat(internalKey.position()).isEqualTo(len + 4);
    assertThat(internalKey.limit()).isEqualTo(len + 4);
    internalKey.clear();
  }

  private void seekTableIteratorForPrev(final SstFileReaderIterator iterator, final ByteBuffer userKey,
    final ByteBuffer internalKey, final Options options) {
    byte[] userKeyArray = new byte[userKey.remaining()];
    userKey.asReadOnlyBuffer().get(userKeyArray);
    internalKey.putInt(1540);
    int len = TypeUtil.getInternalKeyForPrev(userKey, internalKey, options);
    byte[] internalKeyArray = TypeUtil.getInternalKeyForPrev(userKeyArray, options);
    assertEquals(4, internalKey.position());
    assertEquals(len, internalKey.remaining());
    byte[] internalKeyArrayFromByteBuffer = new byte[internalKey.remaining()];
    internalKey.asReadOnlyBuffer().get(internalKeyArrayFromByteBuffer);
    assertArrayEquals(internalKeyArray, internalKeyArrayFromByteBuffer);
    iterator.seekForPrev(internalKey);
    assertThat(internalKey.position()).isEqualTo(len + 4);
    assertThat(internalKey.limit()).isEqualTo(len + 4);
    internalKey.clear();
  }

  @Test
  public void readSstFileTableIterator() throws RocksDBException {
    SstFileReaderIterator iterator = null;
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options);
         final ParsedEntryInfo parsedEntryInfo = new ParsedEntryInfo()) {
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      iterator = reader.newTableIterator();

      // Use the iterator to read sst file
      iterator.seekToFirst();

      // Verify Checksum
      reader.verifyChecksum();

      // Verify Table Properties
      assertEquals(reader.getTableProperties().getNumEntries(), 10);
      final ByteBuffer userByteBuffer = userByteBufferAllocator.allocate(128);
      final ByteBuffer internalKeyByteBuffer = internalByteBufferAllocator.allocate(128);

      // Check key and value
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer,
          "key1", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value1".getBytes());


      userByteBuffer.put("key1".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer,
          "key1", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      {
        userByteBuffer.clear();
        int length = iterator.key(userByteBuffer);
        final byte[] dst = new byte[length];
        userByteBuffer.get(dst);
        assertInternalKeyByteArray(options, parsedEntryInfo, dst, "key1", EntryType.kEntryPut);
      }

      {
        userByteBuffer.clear();
        userByteBuffer.put("PREFIX".getBytes());
        final ByteBuffer slice = userByteBuffer.slice();
        final byte[] dst = new byte[iterator.key(userByteBuffer)];
        slice.get(dst);
        assertInternalKeyByteArray(options, parsedEntryInfo, dst, "key1", EntryType.kEntryPut);
      }

      {
        userByteBuffer.clear();
        assertThat(iterator.value(userByteBuffer)).isEqualTo("value1".getBytes().length);
        final byte[] dst = new byte["value1".getBytes().length];
        userByteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("value1");
      }

      userByteBuffer.clear();
      userByteBuffer.put("key10".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key11", EntryType.kEntryDelete);

      userByteBuffer.clear();
      userByteBuffer.put("key1point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key12", EntryType.kEntryDelete);

      userByteBuffer.clear();
      userByteBuffer.put("key2point5".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key3", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value3".getBytes());

      userByteBuffer.clear();
      userByteBuffer.put("key2point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key22", EntryType.kEntryDelete);

      userByteBuffer.clear();
      internalKeyByteBuffer.put("PREFIX".getBytes());
      final ByteBuffer slice = internalKeyByteBuffer.slice();
      userByteBuffer.put("key1point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, slice, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key12", EntryType.kEntryDelete);

      userByteBuffer.clear();
      userByteBuffer.put("key3point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer, "key33",
          EntryType.kEntryMerge);
      assertThat(iterator.value()).isEqualTo("value33_merge".getBytes());
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  @Test
  public void testReadTableForwardIteratorWithLimits() throws RocksDBException {
    SstFileReaderIterator iterator = null;
    String lowerBound = "key1point5";
    String upperBound = "key3";
    ByteBuffer lowerBoundBuffer = ByteBuffer.allocateDirect(128);
    ByteBuffer upperBoundBuffer = ByteBuffer.allocateDirect(128);
    lowerBoundBuffer.putInt(10294);
    upperBoundBuffer.putInt(15948);
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options);
         final ParsedEntryInfo parsedEntryInfo = new ParsedEntryInfo();
         DirectSlice lowerSliceBound = new DirectSlice(lowerBoundBuffer);
         DirectSlice upperSliceBound = new DirectSlice(upperBoundBuffer)) {
      lowerBoundBuffer.put(TypeUtil.getInternalKey(lowerBound.getBytes(), options));
      upperBoundBuffer.put(TypeUtil.getInternalKey(upperBound.getBytes(), options));
      lowerBoundBuffer.flip();
      upperBoundBuffer.flip();
      lowerBoundBuffer.position(4);
      upperBoundBuffer.position(4);
      lowerSliceBound.removePrefix(4);
      upperSliceBound.removePrefix(4);
      lowerSliceBound.setLength(lowerBoundBuffer.remaining());
      upperSliceBound.setLength(upperBoundBuffer.remaining());
      final ByteBuffer userByteBuffer = userByteBufferAllocator.allocate(128);
      final ByteBuffer internalKeyByteBuffer = internalByteBufferAllocator.allocate(128);
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      iterator = reader.newTableIterator(lowerSliceBound, upperSliceBound);
      iterator.seekToFirst();
      Queue<KeyValueWithOp> expectedKeys = kvs.stream()
          .filter(kv -> lowerBound.compareTo(kv.getKey()) <= 0 &&
              upperBound.compareTo(kv.getKey()) > 0).collect(Collectors.toCollection(LinkedList::new));
      while (!expectedKeys.isEmpty()) {
        KeyValueWithOp expectedKey = expectedKeys.poll();
        assertThat(iterator.isValid()).isTrue();
        assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer,
            expectedKey.getKey(), expectedKey.getOpType().getEntryType());
        assertArrayEquals(expectedKey.getValue().getBytes(), iterator.value());
        iterator.next();
      }
      assertFalse(iterator.isValid());
    }
  }

  @Test
  public void testReadTableReverseIteratorWithLimits() throws RocksDBException {
    SstFileReaderIterator iterator = null;
    String lowerBound = "key1point5";
    String upperBound = "key3";
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options);
         final ParsedEntryInfo parsedEntryInfo = new ParsedEntryInfo();
         Slice lowerSliceBound = new Slice(TypeUtil.getInternalKey(lowerBound.getBytes(), options));
         Slice upperSliceBound = new Slice(TypeUtil.getInternalKey(upperBound.getBytes(), options));) {

      final ByteBuffer userByteBuffer = userByteBufferAllocator.allocate(128);
      final ByteBuffer internalKeyByteBuffer = internalByteBufferAllocator.allocate(128);
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      iterator = reader.newTableIterator(lowerSliceBound, upperSliceBound);
      iterator.seekToLast();
      Stack<KeyValueWithOp> expectedKeys = kvs.stream()
          .filter(kv -> lowerBound.compareTo(kv.getKey()) <= 0 &&
              upperBound.compareTo(kv.getKey()) > 0).collect(Collectors.toCollection(Stack::new));
      while (!expectedKeys.isEmpty()) {
        KeyValueWithOp expectedKey = expectedKeys.pop();
        assertThat(iterator.isValid()).isTrue();
        assertInternalKey(options, parsedEntryInfo, iterator, internalKeyByteBuffer, userByteBuffer,
            expectedKey.getKey(), expectedKey.getOpType().getEntryType());
        assertArrayEquals(expectedKey.getValue().getBytes(), iterator.value());
        iterator.prev();
      }
      assertFalse(iterator.isValid());
    }
  }
}
