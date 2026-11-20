/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
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
public class PutBenchmarks {

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

  @Param({"16"}) int bufferListSize;

  Path dbDir;
  DBOptions options;
  int cfs = 0;  // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  List<byte[]> keyBuffers = new ArrayList<>(bufferListSize);
  List<byte[]> valueBuffers = new ArrayList<>(bufferListSize);
  List<ByteBuffer> keyBuffersBB = new ArrayList<>(bufferListSize);
  List<ByteBuffer> valueBuffersBB = new ArrayList<>(bufferListSize);
  RocksDBBuf dbBuf;
  List<ByteBuf> keyBuffersNetty = new ArrayList<>(bufferListSize);
  List<ByteBuf> valueBuffersNetty = new ArrayList<>(bufferListSize);

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-put-benchmarks");

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

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
    dbBuf = new RocksDBBuf(db);

    for (int i = 0; i < bufferListSize; i++) {
      final byte[] keyArr = new byte[keySize];
      Arrays.fill(keyArr, (byte) 0x30);
      keyBuffers.add(keyArr);
    }

    for (int i = 0; i < bufferListSize; i++) {
      final byte[] valueArr = new byte[valueSize];
      Arrays.fill(valueArr, (byte) 0x30);
      valueBuffers.add(valueArr);
    }

    for (int i = 0; i < bufferListSize; i++) {
      final ByteBuffer keyBB = ByteBuffer.allocateDirect(keySize);
      byte[] keyArr = new byte[keySize];
      Arrays.fill(keyArr, (byte) 0x30);
      keyBB.put(keyArr);
      keyBuffersBB.add(keyBB);
    }

    for (int i = 0; i < bufferListSize; i++) {
      final ByteBuffer valueBB = ByteBuffer.allocateDirect(valueSize);
      byte[] valueArr = new byte[valueSize];
      Arrays.fill(valueArr, (byte) 0x30);
      valueBB.put(valueArr);
      valueBuffersBB.add(valueBB);
    }

    for (int i = 0; i < bufferListSize; i++) {
      final ByteBuf keyBuf = PooledByteBufAllocator.DEFAULT.directBuffer(keySize);
      byte[] keyArr = new byte[keySize];
      Arrays.fill(keyArr, (byte) 0x30);
      keyBuf.writeBytes(keyArr);
      keyBuffersNetty.add(keyBuf);
    }

    for (int i = 0; i < bufferListSize; i++) {
      final ByteBuf valueBuf = PooledByteBufAllocator.DEFAULT.directBuffer(valueSize);
      byte[] valueArr = new byte[valueSize];
      Arrays.fill(valueArr, (byte) 0x30);
      valueBuf.writeBytes(valueArr);
      valueBuffersNetty.add(valueBuf);
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    FileUtils.delete(dbDir);

    keyBuffersNetty.forEach(ByteBuf::release);
    valueBuffersNetty.forEach(ByteBuf::release);
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    }  else if (cfs == 1) {
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

  @State(Scope.Benchmark)
  public static class Counter {
    private final AtomicInteger count = new AtomicInteger();

    public int next() {
      return count.getAndIncrement();
    }
  }

  private <T> T borrow(final List<T> buffers) {
    synchronized (buffers) {
      while (true) {
        if (buffers.isEmpty()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            return null;
          }
          continue;
        }
        return buffers.remove(0);
      }
    }
  }

  private <T> void repay(final List<T> buffers, final T buffer) {
    synchronized (buffers) {
      buffers.add(buffer);
    }
  }

  @Benchmark
  public void put(final Counter counter) throws RocksDBException {
    byte[] keyBuf = borrow(keyBuffers);
    byte[] valueBuf = borrow(valueBuffers);

    final int i = counter.next();
    final byte[] keyPrefix = ba("key" + i);
    final byte[] valuePrefix = ba("value" + i);
    System.arraycopy(keyPrefix, 0, keyBuf, 0, keyPrefix.length);
    System.arraycopy(valuePrefix, 0, valueBuf, 0, valuePrefix.length);
    db.put(getColumnFamily(), keyBuf, valueBuf);

    repay(keyBuffers, keyBuf);
    repay(valueBuffers, valueBuf);
  }

  @Benchmark
  public void putByteArrays(final Counter counter) throws RocksDBException {
    byte[] keyBuf = borrow(keyBuffers);
    byte[] valueBuf = borrow(valueBuffers);

    final int i = counter.next();
    final byte[] keyPrefix = ba("key" + i);
    final byte[] valuePrefix = ba("value" + i);
    System.arraycopy(keyPrefix, 0, keyBuf, 0, keyPrefix.length);
    System.arraycopy(valuePrefix, 0, valueBuf, 0, valuePrefix.length);
    db.put(getColumnFamily(), new WriteOptions(), keyBuf, valueBuf);

    repay(keyBuffers, keyBuf);
    repay(valueBuffers, valueBuf);
  }

  @Benchmark
  public void putByteBuffers(final Counter counter) throws RocksDBException {
    ByteBuffer keyBuf = borrow(keyBuffersBB);
    keyBuf.clear();
    ByteBuffer valueBuf = borrow(valueBuffersBB);
    valueBuf.clear();

    final int i = counter.next();
    final byte[] keyPrefix = ba("key" + i);
    final byte[] valuePrefix = ba("value" + i);
    keyBuf.put(keyPrefix, 0, keyPrefix.length);
    keyBuf.position(keySize);
    keyBuf.flip();
    valueBuf.put(valuePrefix, 0, valuePrefix.length);
    valueBuf.position(valueSize);
    valueBuf.flip();
    db.put(getColumnFamily(), new WriteOptions(), keyBuf, valueBuf);

    repay(keyBuffersBB, keyBuf);
    repay(valueBuffersBB, valueBuf);
  }

  @Benchmark
  public void putWithMemoryAddr(final Counter counter) throws RocksDBException {
    ByteBuf keyBuf = borrow(keyBuffersNetty);
    ByteBuf valueBuf = borrow(valueBuffersNetty);

    final int i = counter.next();
    final byte[] keyPrefix = ba("key" + i);
    final byte[] valuePrefix = ba("value" + i);
    keyBuf.setBytes(0, keyPrefix);
    valueBuf.setBytes(0, valuePrefix);
    dbBuf.put(getColumnFamily(), keyBuf, valueBuf);

    repay(keyBuffersNetty, keyBuf);
    repay(valueBuffersNetty, valueBuf);
  }
}
