/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Cost of adding entries to a {@link WriteBatch}, for each kind of key and value buffer. Every
 * benchmark writes the same entries: "key" or "value", then the entry number as 4 bytes, then
 * filler up to the configured size.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 10)
public class WriteBatchBenchmarks {
  private static final int N = 10_000;
  private static final byte[] KEY_PREFIX = "key".getBytes(UTF_8);
  private static final byte[] VALUE_PREFIX = "value".getBytes(UTF_8);

  @Param({"16", "64"}) int keySize;

  @Param({"16", "64", "1024"}) int valueSize;

  private byte[] filler;
  private byte[] keyArray;
  private byte[] valueArray;
  private ByteBuffer keyArrayBuffer;
  private ByteBuffer valueArrayBuffer;
  private ByteBuffer keyDirectBuffer;
  private ByteBuffer valueDirectBuffer;
  private ByteBuf keyByteBuf;
  private ByteBuf valueByteBuf;

  @Setup(Level.Trial)
  public void setup() {
    RocksDB.loadLibrary();

    filler = new byte[Math.max(keySize, valueSize)];
    Arrays.fill(filler, (byte) 0x30);

    keyArray = new byte[keySize];
    valueArray = new byte[valueSize];
    keyArrayBuffer = ByteBuffer.wrap(keyArray);
    valueArrayBuffer = ByteBuffer.wrap(valueArray);

    keyDirectBuffer = ByteBuffer.allocateDirect(keySize);
    valueDirectBuffer = ByteBuffer.allocateDirect(valueSize);

    keyByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(keySize);
    valueByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(valueSize);
  }

  @TearDown(Level.Trial)
  public void cleanup() {
    keyByteBuf.release();
    valueByteBuf.release();
  }

  /** Fills the buffer with an entry and flips it, ready to be read. */
  private void fill(final ByteBuffer buffer, final byte[] prefix, final int i) {
    buffer.clear();
    buffer.put(prefix).putInt(i).put(filler, 0, buffer.remaining());
    buffer.flip();
  }

  /** Fills the buffer with an entry; the entry is the readable bytes. */
  private void fill(final ByteBuf buffer, final byte[] prefix, final int i) {
    buffer.clear();
    buffer.writeBytes(prefix).writeInt(i).writeBytes(filler, 0, buffer.writableBytes());
  }

  /** New byte[] key and value for every entry. */
  @Benchmark
  @OperationsPerInvocation(N)
  public void put() throws RocksDBException {
    try (final WriteBatch wb = new WriteBatch()) {
      for (int i = 0; i < N; i++) {
        final byte[] key = new byte[keySize];
        final byte[] value = new byte[valueSize];
        fill(ByteBuffer.wrap(key), KEY_PREFIX, i);
        fill(ByteBuffer.wrap(value), VALUE_PREFIX, i);
        wb.put(key, value);
      }
    }
  }

  /** The same byte[] key and value, reused for every entry. */
  @Benchmark
  @OperationsPerInvocation(N)
  public void putCached() throws RocksDBException {
    try (final WriteBatch wb = new WriteBatch()) {
      for (int i = 0; i < N; i++) {
        fill(keyArrayBuffer, KEY_PREFIX, i);
        fill(valueArrayBuffer, VALUE_PREFIX, i);
        wb.put(keyArray, valueArray);
      }
    }
  }

  /** The same direct ByteBuffer key and value, reused for every entry. */
  @Benchmark
  @OperationsPerInvocation(N)
  public void putWithByteBuffer() throws RocksDBException {
    try (final WriteBatch wb = new WriteBatch()) {
      for (int i = 0; i < N; i++) {
        fill(keyDirectBuffer, KEY_PREFIX, i);
        fill(valueDirectBuffer, VALUE_PREFIX, i);
        wb.put(keyDirectBuffer, valueDirectBuffer);
      }
    }
  }

  /** The same Netty direct ByteBuf key and value, reused for every entry, passed by address. */
  @Benchmark
  @OperationsPerInvocation(N)
  public void putWithAddress() throws RocksDBException {
    try (final WriteBatch wb = new WriteBatch()) {
      final WriteBatchBuf wbBuf = new WriteBatchBuf(wb);
      for (int i = 0; i < N; i++) {
        fill(keyByteBuf, KEY_PREFIX, i);
        fill(valueByteBuf, VALUE_PREFIX, i);
        wbBuf.put(keyByteBuf, valueByteBuf);
      }
    }
  }
}
