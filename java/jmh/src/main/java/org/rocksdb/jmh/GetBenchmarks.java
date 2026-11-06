/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Benchmark)
public class GetBenchmarks {

  @Param({
      "no_column_family",
      "1_column_family",
      "20_column_families",
      "100_column_families"
  })
  String columnFamilyTestType;

  @Param({"1000", "100000"}) int keyCount;

  @Param({"12", "64", "128"}) int keySize;

  @Param({"64", "1024", "65536"}) int valueSize;

  Path dbDir;
  DBOptions options;
  ReadOptions readOptions;
  int cfs = 0;  // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();
  private ByteBuffer keyBuf;
  private ByteBuffer valueBuf;
  private byte[] keyArr;
  private byte[] valueArr;

  private RocksDBBuf dbBuf;
  private ByteBuf keyByteBuf;
  private ByteBuf valueByteBuf;

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-get-benchmarks");

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    readOptions = new ReadOptions();

    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    if ("1_column_family".equals(columnFamilyTestType)) {
      cfs = 1;
    } else if ("20_column_families".equals(columnFamilyTestType)) {
      cfs = 20;
    } else if ("100_column_families".equals(columnFamilyTestType)) {
      cfs = 100;
    }

    if (cfs > 0) {
      cfHandlesIdx = new AtomicInteger(1);
      for (int i = 1; i <= cfs; i++) {
        cfDescriptors.add(new ColumnFamilyDescriptor(ba("cf" + i)));
      }
    }

    final List<ColumnFamilyHandle> cfHandlesList = new ArrayList<>(cfDescriptors.size());
    db = RocksDB.open(options, dbDir.toAbsolutePath().toString(), cfDescriptors, cfHandlesList);
    cfHandles = cfHandlesList.toArray(new ColumnFamilyHandle[0]);

    // store initial data for retrieving via get
    keyArr = new byte[keySize];
    valueArr = new byte[valueSize];
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    for (int i = 0; i <= cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        final byte[] keyPrefix = ba("key" + j);
        final byte[] valuePrefix = ba("value" + j);
        System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
        System.arraycopy(valuePrefix, 0, valueArr, 0, valuePrefix.length);
        db.put(cfHandles[i], keyArr, valueArr);
      }
    }

    try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }

    keyBuf = ByteBuffer.allocateDirect(keySize);
    valueBuf = ByteBuffer.allocateDirect(valueSize);
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    keyBuf.put(keyArr);
    keyBuf.flip();
    valueBuf.put(valueArr);
    valueBuf.flip();

    dbBuf = new RocksDBBuf(db);
    keyByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(keySize);
    keyByteBuf.writeBytes(keyArr);
    valueByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(valueSize);
    if (dbBuf.get(cfHandles[0], readOptions, getKeyByteBuf(), valueByteBuf) != valueSize) {
      throw new IllegalStateException("preallocatedMemoryAddr would not read the stored keys");
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    readOptions.close();
    FileUtils.delete(dbDir);

    keyByteBuf.release();
    valueByteBuf.release();
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    } else if (cfs == 1) {
      return cfHandles[1];
    } else {
      int idx = cfHandlesIdx.getAndIncrement();
      if (idx > cfs) {
        cfHandlesIdx.set(1); // doesn't ensure a perfect distribution, but it's ok
        idx = 0;
      }
      return cfHandles[idx];
    }
  }

  /**
   * Takes the next position in the index.
   */
  private int next() {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + 1;
      if (nextIdx >= keyCount) {
        nextIdx = 0;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }
    return idx;
  }

  // String -> byte[]
  private byte[] getKeyArr() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final byte[] keyPrefix = ba("key" + keyIdx);
    System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
    Arrays.fill(keyArr, keyPrefix.length, MAX_LEN, (byte) 0x30);
    return keyArr;
  }

  // String -> ByteBuffer
  private ByteBuffer getKeyBuf() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keyBuf.put(i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keyBuf.put(i, (byte) 0x30);
    }
    // Reset position for future reading
    keyBuf.position(0);
    return keyBuf;
  }

  // String -> ByteBuf
  private ByteBuf getKeyByteBuf() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keyByteBuf.setByte(i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keyByteBuf.setByte(i, (byte) 0x30);
    }
    return keyByteBuf;
  }

  private byte[] getValueArr() {
    return valueArr;
  }

  private ByteBuffer getValueBuf() {
    return valueBuf;
  }

  @Benchmark
  public void get() throws RocksDBException {
    db.get(getColumnFamily(), getKeyArr());
  }

  @Benchmark
  public void preallocatedGet() throws RocksDBException {
    db.get(getColumnFamily(), getKeyArr(), getValueArr());
  }

  @Benchmark
  public void preallocatedByteBufferGet() throws RocksDBException {
    int res = db.get(getColumnFamily(), readOptions, getKeyBuf(), getValueBuf());
    // For testing correctness:
    //    assert res > 0;
    //    final byte[] ret = new byte[valueSize];
    //    valueBuf.get(ret);
    //    System.out.println(str(ret));
    //    valueBuf.flip();
  }

  @Benchmark
  public int preallocatedMemoryAddr() throws RocksDBException {
    return dbBuf.get(getColumnFamily(), readOptions, getKeyByteBuf(), valueByteBuf);
  }
}
