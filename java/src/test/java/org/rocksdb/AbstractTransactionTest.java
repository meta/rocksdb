// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Base class of {@link TransactionTest} and {@link OptimisticTransactionTest}
 */
public abstract class AbstractTransactionTest {
  protected static final byte[] TXN_TEST_COLUMN_FAMILY = "txn_test_cf".getBytes();

  protected static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public abstract DBContainer startDb()
      throws RocksDBException;

  @Test
  public void setSnapshot() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.setSnapshot();
    }
  }

  @Test
  public void setSnapshotOnNextOperation() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.setSnapshotOnNextOperation();
      txn.put("key1".getBytes(), "value1".getBytes());
    }
  }

  @Test
  public void setSnapshotOnNextOperation_transactionNotifier() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {

      try(final TestTransactionNotifier notifier = new TestTransactionNotifier()) {
        txn.setSnapshotOnNextOperation(notifier);
        txn.put("key1".getBytes(), "value1".getBytes());

        txn.setSnapshotOnNextOperation(notifier);
        txn.put("key2".getBytes(), "value2".getBytes());

        assertThat(notifier.getCreatedSnapshots().size()).isEqualTo(2);
      }
    }
  }

  @Test
  public void getSnapshot() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.setSnapshot();
      final Snapshot snapshot = txn.getSnapshot();
      assertThat(snapshot.isOwningHandle()).isFalse();
    }
  }

  @Test
  public void getSnapshot_null() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      final Snapshot snapshot = txn.getSnapshot();
      assertThat(snapshot).isNull();
    }
  }

  @Test
  public void clearSnapshot() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.setSnapshot();
      txn.clearSnapshot();
    }
  }

  @Test
  public void clearSnapshot_none() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.clearSnapshot();
    }
  }

  @Test
  public void commit() throws RocksDBException {
    final byte[] k1 = "rollback-key1".getBytes(UTF_8);
    final byte[] v1 = "rollback-value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb()) {
      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(k1, v1);
        txn.commit();
      }

      try(final ReadOptions readOptions = new ReadOptions();
          final Transaction txn2 = dbContainer.beginTransaction()) {
        assertThat(txn2.get(readOptions, k1)).isEqualTo(v1);
      }
    }
  }

  @Test
  public void rollback() throws RocksDBException {
    final byte[] k1 = "rollback-key1".getBytes(UTF_8);
    final byte[] v1 = "rollback-value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb()) {
      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(k1, v1);
        txn.rollback();
      }

      try(final ReadOptions readOptions = new ReadOptions();
          final Transaction txn2 = dbContainer.beginTransaction()) {
        assertThat(txn2.get(readOptions, k1)).isNull();
      }
    }
  }

  @Test
  public void savePoint() throws RocksDBException {
    final byte[] k1 = "savePoint-key1".getBytes(UTF_8);
    final byte[] v1 = "savePoint-value1".getBytes(UTF_8);
    final byte[] k2 = "savePoint-key2".getBytes(UTF_8);
    final byte[] v2 = "savePoint-value2".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {


      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(k1, v1);

        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);

        txn.setSavePoint();

        txn.put(k2, v2);

        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
        assertThat(txn.get(readOptions, k2)).isEqualTo(v2);

        txn.rollbackToSavePoint();

        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
        assertThat(txn.get(readOptions, k2)).isNull();

        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        assertThat(txn2.get(readOptions, k1)).isEqualTo(v1);
        assertThat(txn2.get(readOptions, k2)).isNull();
      }
    }
  }

  @Test
  public void getPut_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      assertThat(txn.get(readOptions, testCf, k1)).isNull();
      assertThat(txn.get(testCf, readOptions, k1)).isNull();
      txn.put(testCf, k1, v1);
      assertThat(txn.get(readOptions, testCf, k1)).isEqualTo(v1);
      assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);
    }
  }

  @Test
  public void getPut() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.get(readOptions, k1)).isNull();
      txn.put(k1, v1);
      assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
    }
  }

  @Test
  public void getPutTargetBuffer_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final byte[] target = "overwrite1".getBytes(UTF_8);
      GetStatus status = txn.get(readOptions, testCf, k1, target);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.NotFound);
      assertThat(status.requiredSize).isEqualTo(0);
      txn.put(testCf, k1, v1);
      status = txn.get(readOptions, testCf, k1, target);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(status.requiredSize).isEqualTo(v1.length);
      assertThat(target).isEqualTo("value1ite1".getBytes());
    }
  }

  @Test
  public void getPutTargetBuffer() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final byte[] target = "overwrite1".getBytes(UTF_8);
      GetStatus status = txn.get(readOptions, k1, target);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.NotFound);
      assertThat(status.requiredSize).isEqualTo(0);
      txn.put(k1, v1);
      status = txn.get(readOptions, k1, target);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(status.requiredSize).isEqualTo(v1.length);
      assertThat(target).isEqualTo("value1ite1".getBytes());
    }
  }

  public void getPutByteBuffer(final Function<Integer, ByteBuffer> allocateBuffer)
      throws RocksDBException {
    final ByteBuffer k1 = allocateBuffer.apply(100).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = allocateBuffer.apply(100).put("value1".getBytes(UTF_8));
    v1.flip();
    final ByteBuffer vEmpty = allocateBuffer.apply(0);

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ByteBuffer vGet = allocateBuffer.apply(100);
      assertThat(txn.get(readOptions, k1, vGet).status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(k1, v1);

      final GetStatus getStatusError = txn.get(readOptions, k1, vEmpty);
      assertThat(getStatusError.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatusError.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);
      assertThat(vEmpty.position()).isEqualTo(0);
      assertThat(vEmpty.remaining()).isEqualTo(0);

      vGet.put("12345".getBytes(UTF_8));

      final GetStatus getStatus = txn.get(readOptions, k1, vGet);
      assertThat(getStatusError.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatusError.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);

      vGet.put("67890".getBytes(UTF_8));
      vGet.flip();
      final byte[] bytes = new byte[vGet.limit()];
      vGet.get(bytes);
      assertThat(new String(bytes, UTF_8)).isEqualTo("12345value167890");
    }
  }

  @Test
  public void getPutDirectByteBuffer() throws RocksDBException {
    getPutByteBuffer(ByteBuffer::allocateDirect);
  }

  @Test
  public void getPutIndirectByteBuffer() throws RocksDBException {
    getPutByteBuffer(ByteBuffer::allocate);
  }

  public void getPutByteBuffer_cf(final Function<Integer, ByteBuffer> allocateBuffer)
      throws RocksDBException {
    final ByteBuffer k1 = allocateBuffer.apply(100).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = allocateBuffer.apply(100).put("value1".getBytes(UTF_8));
    v1.flip();
    final ByteBuffer vEmpty = allocateBuffer.apply(0);

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final ByteBuffer vGet = allocateBuffer.apply(100);
      assertThat(txn.get(readOptions, testCf, k1, vGet).status.getCode())
          .isEqualTo(Status.Code.NotFound);
      txn.put(testCf, k1, v1);

      final GetStatus getStatusError = txn.get(readOptions, testCf, k1, vEmpty);
      assertThat(getStatusError.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatusError.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);
      assertThat(vEmpty.position()).isEqualTo(0);
      assertThat(vEmpty.remaining()).isEqualTo(0);

      vGet.put("12345".getBytes(UTF_8));
      final GetStatus getStatus = txn.get(readOptions, testCf, k1, vGet);
      assertThat(getStatus.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatus.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);
      vGet.put("67890".getBytes(UTF_8));
      vGet.flip();
      final byte[] bytes = new byte[vGet.limit()];
      vGet.get(bytes);
      assertThat(new String(bytes, UTF_8)).isEqualTo("12345value167890");
    }
  }

  @Test
  public void getPutDirectByteBuffer_cf() throws RocksDBException {
    getPutByteBuffer_cf(ByteBuffer::allocateDirect);
  }

  @Test
  public void getPutIndirectByteBuffer_cf() throws RocksDBException {
    getPutByteBuffer_cf(ByteBuffer::allocate);
  }

  @Test
  public void multiGetPut_cf() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGet(readOptions, cfList, keys)).isEqualTo(new byte[][] { null, null });

      txn.put(testCf, keys[0], values[0]);
      txn.put(testCf, keys[1], values[1]);
      assertThat(txn.multiGet(readOptions, cfList, keys)).isEqualTo(values);
    }
  }

  @Test
  public void multiGetPutAsList_cf() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGetAsList(readOptions, cfList, Arrays.asList(keys)))
          .containsExactly(null, null);

      txn.put(testCf, keys[0], values[0]);
      txn.put(testCf, keys[1], values[1]);
      assertThat(txn.multiGetAsList(readOptions, cfList, Arrays.asList(keys)))
          .containsExactly(values);
    }
  }

  @Test
  public void multiGetPut() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {

      assertThat(txn.multiGet(readOptions, keys)).isEqualTo(new byte[][] { null, null });

      txn.put(keys[0], values[0]);
      txn.put(keys[1], values[1]);
      assertThat(txn.multiGet(readOptions, keys)).isEqualTo(values);
    }
  }

  @Test
  public void multiGetPutAsList() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.multiGetAsList(readOptions, Arrays.asList(keys))).containsExactly(null, null);

      txn.put(keys[0], values[0]);
      txn.put(keys[1], values[1]);
      assertThat(txn.multiGetAsList(readOptions, Arrays.asList(keys))).containsExactly(values);
    }
  }

  @Test
  public void getForUpdate_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      assertThat(txn.getForUpdate(readOptions, testCf, k1, true)).isNull();
      txn.put(testCf, k1, v1);
      assertThat(txn.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);
    }
  }

  @Test
  public void getForUpdate() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getForUpdate(readOptions, k1, true)).isNull();
      txn.put(k1, v1);
      assertThat(txn.getForUpdate(readOptions, k1, true)).isEqualTo(v1);
    }
  }

  @Test
  public void getForUpdateByteArray_cf_doValidate() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final byte[] vNonExistent = new byte[1];
      final GetStatus sNonExistent =
          txn.getForUpdate(readOptions, testCf, k1, vNonExistent, true, true);
      assertThat(sNonExistent.status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(testCf, k1, v1);
      final byte[] vPartial = new byte[4];
      final GetStatus sPartial = txn.getForUpdate(readOptions, testCf, k1, vPartial, true, true);
      assertThat(sPartial.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sPartial.requiredSize).isEqualTo(v1.length);
      assertThat(vPartial).isEqualTo(Arrays.copyOfRange(v1, 0, vPartial.length));

      final byte[] vTotal = new byte[sPartial.requiredSize];
      final GetStatus sTotal = txn.getForUpdate(readOptions, testCf, k1, vTotal, true, true);
      assertThat(sTotal.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sTotal.requiredSize).isEqualTo(v1.length);
      assertThat(vTotal).isEqualTo(v1);
    }
  }

  public void getForUpdateByteArray_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final byte[] vNonExistent = new byte[1];
      final GetStatus sNonExistent = txn.getForUpdate(readOptions, testCf, k1, vNonExistent, true);
      assertThat(sNonExistent.status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(testCf, k1, v1);
      final byte[] vPartial = new byte[4];
      final GetStatus sPartial = txn.getForUpdate(readOptions, testCf, k1, vPartial, true);
      assertThat(sPartial.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sPartial.requiredSize).isEqualTo(v1.length);
      assertThat(vPartial).isEqualTo(Arrays.copyOfRange(v1, 0, vPartial.length));

      final byte[] vTotal = new byte[sPartial.requiredSize];
      final GetStatus sTotal = txn.getForUpdate(readOptions, testCf, k1, vTotal, true);
      assertThat(sTotal.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sTotal.requiredSize).isEqualTo(v1.length);
      assertThat(vTotal).isEqualTo(v1);
    }
  }

  @Test
  public void getForUpdateByteArray() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final byte[] vNonExistent = new byte[1];
      final GetStatus sNonExistent = txn.getForUpdate(readOptions, k1, vNonExistent, true);
      assertThat(sNonExistent.status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(k1, v1);
      final byte[] vPartial = new byte[4];
      final GetStatus sPartial = txn.getForUpdate(readOptions, k1, vPartial, true);
      assertThat(sPartial.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sPartial.requiredSize).isEqualTo(v1.length);
      assertThat(vPartial).isEqualTo(Arrays.copyOfRange(v1, 0, vPartial.length));

      final byte[] vTotal = new byte[sPartial.requiredSize];
      final GetStatus sTotal = txn.getForUpdate(readOptions, k1, vTotal, true);
      assertThat(sTotal.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(sTotal.requiredSize).isEqualTo(v1.length);
      assertThat(vTotal).isEqualTo(v1);
    }
  }

  @Test
  public void getForUpdateDirectByteBuffer() throws Exception {
    getForUpdateByteBuffer(ByteBuffer::allocateDirect);
  }

  @Test
  public void getForUpdateIndirectByteBuffer() throws Exception {
    getForUpdateByteBuffer(ByteBuffer::allocate);
  }

  public void getForUpdateByteBuffer(final Function<Integer, ByteBuffer> allocateBuffer)
      throws Exception {
    final ByteBuffer k1 = allocateBuffer.apply(20).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = allocateBuffer.apply(20).put("value1".getBytes(UTF_8));
    v1.flip();
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ByteBuffer v1Read1 = allocateBuffer.apply(20);
      final GetStatus getStatus1 = txn.getForUpdate(readOptions, k1, v1Read1, true);
      assertThat(getStatus1.status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(k1, v1);
      final ByteBuffer v1Read2 = allocateBuffer.apply(20);
      final GetStatus getStatus2 = txn.getForUpdate(readOptions, k1, v1Read2, true);
      assertThat(getStatus2.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatus2.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);
      assertThat(v1Read2).isEqualTo(allocateBuffer.apply(20).put("value1".getBytes(UTF_8)));
    }
  }

  @Test
  public void getForUpdateDirectByteBuffer_cf() throws Exception {
    getForUpdateByteBuffer_cf(ByteBuffer::allocateDirect);
  }

  @Test
  public void getForUpdateIndirectByteBuffer_cf() throws Exception {
    getForUpdateByteBuffer_cf(ByteBuffer::allocate);
  }

  public void getForUpdateByteBuffer_cf(final Function<Integer, ByteBuffer> allocateBuffer)
      throws Exception {
    final ByteBuffer k1 = allocateBuffer.apply(20).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = allocateBuffer.apply(20).put("value1".getBytes(UTF_8));
    v1.flip();
    final ByteBuffer k2 = allocateBuffer.apply(20).put("key2".getBytes(UTF_8));
    k2.flip();
    final ByteBuffer v2 = allocateBuffer.apply(20).put("value2".getBytes(UTF_8));
    v2.flip();
    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final ByteBuffer v1Read1 = allocateBuffer.apply(20);
      final GetStatus getStatus1 = txn.getForUpdate(readOptions, testCf, k1, v1Read1, true);
      assertThat(getStatus1.status.getCode()).isEqualTo(Status.Code.NotFound);
      txn.put(k1, v1);
      k1.flip();
      v1.flip();
      txn.put(testCf, k2, v2);
      k2.flip();
      v2.flip();
      final ByteBuffer v1Read2 = allocateBuffer.apply(20);
      final GetStatus getStatus2 = txn.getForUpdate(readOptions, testCf, k1, v1Read2, true);
      assertThat(getStatus2.status.getCode()).isEqualTo(Status.Code.NotFound);
      k1.flip();
      txn.put(testCf, k1, v1);
      k1.flip();
      v1.flip();
      final ByteBuffer v1Read3 = allocateBuffer.apply(20);
      final GetStatus getStatus3 = txn.getForUpdate(readOptions, testCf, k1, v1Read3, true);
      assertThat(getStatus3.status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(getStatus3.requiredSize).isEqualTo("value1".getBytes(UTF_8).length);
      assertThat(v1Read3).isEqualTo(allocateBuffer.apply(20).put("value1".getBytes(UTF_8)));
    }
  }

  @Test
  public void multiGetForUpdate_cf() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGetForUpdate(readOptions, cfList, keys))
          .isEqualTo(new byte[][] { null, null });

      txn.put(testCf, keys[0], values[0]);
      txn.put(testCf, keys[1], values[1]);
      assertThat(txn.multiGetForUpdate(readOptions, cfList, keys))
          .isEqualTo(values);
    }
  }

  @Test
  public void multiGetForUpdate() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.multiGetForUpdate(readOptions, keys)).isEqualTo(new byte[][]{null, null});

      txn.put(keys[0], values[0]);
      txn.put(keys[1], values[1]);
      assertThat(txn.multiGetForUpdate(readOptions, keys)).isEqualTo(values);
    }
  }

  @Test
  public void multiGetForUpdateAsList_cf() throws RocksDBException {
    final List<byte[]> keys = Arrays.asList("key1".getBytes(UTF_8), "key2".getBytes(UTF_8));
    final List<byte[]> values = Arrays.asList("value1".getBytes(UTF_8), "value2".getBytes(UTF_8));

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGetForUpdateAsList(readOptions, cfList, keys))
          .isEqualTo(Arrays.asList(null, null));

      txn.put(testCf, keys.get(0), values.get(0));
      txn.put(testCf, keys.get(1), values.get(1));
      final List<byte[]> result = txn.multiGetForUpdateAsList(readOptions, cfList, keys);
      assertThat(result.size()).isEqualTo(values.size());
      for (int i = 0; i < result.size(); i++) {
        assertThat(result.get(i)).isEqualTo(values.get(i));
      }
    }
  }

  @Test
  public void multiGetForUpdateAsList() throws RocksDBException {
    final List<byte[]> keys = Arrays.asList("key1".getBytes(UTF_8), "key2".getBytes(UTF_8));
    final List<byte[]> values = Arrays.asList("value1".getBytes(UTF_8), "value2".getBytes(UTF_8));

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final List<byte[]> nulls = new ArrayList<>();
      nulls.add(null);
      nulls.add(null);
      assertThat(txn.multiGetForUpdateAsList(readOptions, keys)).isEqualTo(nulls);

      txn.put(keys.get(0), values.get(0));
      txn.put(keys.get(1), values.get(1));
      final List<byte[]> result = txn.multiGetForUpdateAsList(readOptions, keys);
      assertThat(result.size()).isEqualTo(values.size());
      for (int i = 0; i < result.size(); i++) {
        assertThat(result.get(i)).isEqualTo(values.get(i));
      }
    }
  }

  @Test
  public void getIterator() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {

      final byte[] k1 = "key1".getBytes(UTF_8);
      final byte[] v1 = "value1".getBytes(UTF_8);

      txn.put(k1, v1);

      try(final RocksIterator iterator = txn.getIterator(readOptions)) {
        iterator.seek(k1);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo(k1);
        assertThat(iterator.value()).isEqualTo(v1);
      }

      try (final RocksIterator iterator = txn.getIterator()) {
        iterator.seek(k1);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo(k1);
        assertThat(iterator.value()).isEqualTo(v1);
      }
    }
  }

  @Test
  public void getIterator_cf() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();

      final byte[] k1 = "key1".getBytes(UTF_8);
      final byte[] v1 = "value1".getBytes(UTF_8);

      txn.put(testCf, k1, v1);

      try(final RocksIterator iterator = txn.getIterator(readOptions, testCf)) {
        iterator.seek(k1);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo(k1);
        assertThat(iterator.value()).isEqualTo(v1);
      }

      try (final RocksIterator iterator = txn.getIterator(testCf)) {
        iterator.seek(k1);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo(k1);
        assertThat(iterator.value()).isEqualTo(v1);
      }
    }
  }

  @Test
  public void merge_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, k1, v1);
      txn.merge(testCf, k1, v2);
      assertThat(txn.get(new ReadOptions(), testCf, k1)).isEqualTo("value1**value2".getBytes());
      assertThat(txn.get(testCf, new ReadOptions(), k1)).isEqualTo("value1**value2".getBytes());
    }
  }

  @Test
  public void merge() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(k1, v1);
      txn.merge(k1, v2);
      assertThat(txn.get(new ReadOptions(), k1)).isEqualTo("value1++value2".getBytes());
    }
  }

  @Test
  public void mergeDirectByteBuffer() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocateDirect(100).put("key1".getBytes(UTF_8));
    final ByteBuffer v1 = ByteBuffer.allocateDirect(100).put("value1".getBytes(UTF_8));
    final ByteBuffer v2 = ByteBuffer.allocateDirect(100).put("value2".getBytes(UTF_8));
    k1.flip();
    v1.flip();
    v2.flip();

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(k1, v1);
      k1.flip();
      v1.flip();
      txn.merge(k1, v2);
      assertThat(txn.get(new ReadOptions(), "key1".getBytes(UTF_8)))
          .isEqualTo("value1++value2".getBytes());
    }
  }

  public void mergeIndirectByteBuffer() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocate(100).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = ByteBuffer.allocate(100).put("value1".getBytes(UTF_8));
    v1.flip();
    final ByteBuffer v2 = ByteBuffer.allocate(100).put("value2".getBytes(UTF_8));
    v2.flip();

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(k1, v1);
      txn.merge(k1, v2);
      assertThat(txn.get(new ReadOptions(), "key1".getBytes(UTF_8)))
          .isEqualTo("value1++value2".getBytes());
    }
  }

  @Test
  public void mergeDirectByteBuffer_cf() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocateDirect(100).put("key1".getBytes(UTF_8));
    final ByteBuffer v1 = ByteBuffer.allocateDirect(100).put("value1".getBytes(UTF_8));
    final ByteBuffer v2 = ByteBuffer.allocateDirect(100).put("value2".getBytes(UTF_8));
    k1.flip();
    v1.flip();
    v2.flip();

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, k1, v1);
      k1.flip();
      v1.flip();
      txn.merge(testCf, k1, v2);
      assertThat(txn.get(new ReadOptions(), testCf, "key1".getBytes(UTF_8)))
          .isEqualTo("value1**value2".getBytes());
      assertThat(txn.get(testCf, new ReadOptions(), "key1".getBytes(UTF_8)))
          .isEqualTo("value1**value2".getBytes());
    }
  }
  public void mergeIndirectByteBuffer_cf() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocate(100).put("key1".getBytes(UTF_8));
    k1.flip();
    final ByteBuffer v1 = ByteBuffer.allocate(100).put("value1".getBytes(UTF_8));
    v1.flip();
    final ByteBuffer v2 = ByteBuffer.allocate(100).put("value2".getBytes(UTF_8));
    v2.flip();

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, k1, v1);
      txn.merge(testCf, k1, v2);
      assertThat(txn.get(new ReadOptions(), testCf, "key1".getBytes(UTF_8)))
          .isEqualTo("value1**value2".getBytes());
      assertThat(txn.get(testCf, new ReadOptions(), "key1".getBytes(UTF_8)))
          .isEqualTo("value1**value2".getBytes());
    }
  }

  @Test
  public void delete_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, k1, v1);
      assertThat(txn.get(readOptions, testCf, k1)).isEqualTo(v1);
      assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);

      txn.delete(testCf, k1);
      assertThat(txn.get(readOptions, testCf, k1)).isNull();
      assertThat(txn.get(testCf, readOptions, k1)).isNull();
    }
  }

  @Test
  public void delete() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(k1, v1);
      assertThat(txn.get(readOptions, k1)).isEqualTo(v1);

      txn.delete(k1);
      assertThat(txn.get(readOptions, k1)).isNull();
    }
  }

  @Test
  public void delete_parts_cf() throws RocksDBException {
    final byte[][] keyParts = new byte[][] {"ke".getBytes(UTF_8), "y1".getBytes(UTF_8)};
    final byte[][] valueParts = new byte[][] {"val".getBytes(UTF_8), "ue1".getBytes(UTF_8)};
    final byte[] key = concat(keyParts);
    final byte[] value = concat(valueParts);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, keyParts, valueParts);
      assertThat(txn.get(testCf, readOptions, key)).isEqualTo(value);
      assertThat(txn.get(readOptions, testCf, key)).isEqualTo(value);

      txn.delete(testCf, keyParts);

      assertThat(txn.get(readOptions, testCf, key)).isNull();
      assertThat(txn.get(testCf, readOptions, key)).isNull();
    }
  }

  @Test
  public void delete_parts() throws RocksDBException {
    final byte[][] keyParts = new byte[][] {"ke".getBytes(UTF_8), "y1".getBytes(UTF_8)};
    final byte[][] valueParts = new byte[][] {"val".getBytes(UTF_8), "ue1".getBytes(UTF_8)};
    final byte[] key = concat(keyParts);
    final byte[] value = concat(valueParts);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {

      txn.put(keyParts, valueParts);

      assertThat(txn.get(readOptions, key)).isEqualTo(value);

      txn.delete(keyParts);

      assertThat(txn.get(readOptions, key)).isNull();
    }
  }

  @Test
  public void getPutUntracked_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      assertThat(txn.get(readOptions, testCf, k1)).isNull();
      assertThat(txn.get(testCf, readOptions, k1)).isNull();
      txn.putUntracked(testCf, k1, v1);
      assertThat(txn.get(readOptions, testCf, k1)).isEqualTo(v1);
      assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);
    }
  }

  @Test
  public void getPutUntracked() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.get(readOptions, k1)).isNull();
      txn.putUntracked(k1, v1);
      assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
    }
  }

  @Deprecated
  @Test
  public void multiGetPutUntracked_cf() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();

      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGet(readOptions, cfList, keys)).isEqualTo(new byte[][] { null, null });
      txn.putUntracked(testCf, keys[0], values[0]);
      txn.putUntracked(testCf, keys[1], values[1]);
      assertThat(txn.multiGet(readOptions, cfList, keys)).isEqualTo(values);
    }
  }

  @Test
  public void multiGetPutUntrackedAsList_cf() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();

      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      assertThat(txn.multiGetAsList(readOptions, cfList, Arrays.asList(keys)))
          .containsExactly(null, null);
      txn.putUntracked(testCf, keys[0], values[0]);
      txn.putUntracked(testCf, keys[1], values[1]);
      assertThat(txn.multiGetAsList(readOptions, cfList, Arrays.asList(keys)))
          .containsExactly(values);
    }
  }

  @Deprecated
  @Test
  public void multiGetPutUntracked() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {

      assertThat(txn.multiGet(readOptions, keys)).isEqualTo(new byte[][] { null, null });
      txn.putUntracked(keys[0], values[0]);
      txn.putUntracked(keys[1], values[1]);
      assertThat(txn.multiGet(readOptions, keys)).isEqualTo(values);
    }
  }

  @Test
  public void multiGetPutAsListUntracked() throws RocksDBException {
    final byte[][] keys = new byte[][] {"key1".getBytes(UTF_8), "key2".getBytes(UTF_8)};
    final byte[][] values = new byte[][] {"value1".getBytes(UTF_8), "value2".getBytes(UTF_8)};

    try (final DBContainer dbContainer = startDb();
         final ReadOptions readOptions = new ReadOptions();
         final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.multiGetAsList(readOptions, Arrays.asList(keys))).containsExactly(null, null);
      txn.putUntracked(keys[0], values[0]);
      txn.putUntracked(keys[1], values[1]);
      assertThat(txn.multiGetAsList(readOptions, Arrays.asList(keys))).containsExactly(values);
    }
  }

  @Test
  public void mergeUntracked_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.mergeUntracked(testCf, k1, v1);
      txn.mergeUntracked(testCf, k1, v2);
      txn.commit();
    }
    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      assertThat(txn.get(new ReadOptions(), testCf, k1)).isEqualTo("value1**value2".getBytes());
    }
  }

  @Test
  public void mergeUntracked() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.mergeUntracked(k1, v1);
      txn.mergeUntracked(k1, v2);
      txn.commit();
    }
    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.get(new ReadOptions(), k1)).isEqualTo("value1++value2".getBytes());
    }
  }

  @Test
  public void mergeUntrackedByteBuffer() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocateDirect(20).put("key1".getBytes(UTF_8));
    final ByteBuffer v1 = ByteBuffer.allocateDirect(20).put("value1".getBytes(UTF_8));
    final ByteBuffer v2 = ByteBuffer.allocateDirect(20).put("value2".getBytes(UTF_8));
    k1.flip();
    v1.flip();
    v2.flip();

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      txn.mergeUntracked(k1, v1);
      k1.flip();
      v1.flip();
      txn.mergeUntracked(k1, v2);
      k1.flip();
      v2.flip();
      txn.commit();
    }

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ByteBuffer v = ByteBuffer.allocateDirect(20);
      final GetStatus status = txn.get(new ReadOptions(), k1, v);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
      k1.flip();
      v.flip();
      final int expectedLength = "value1++value2".length();
      assertThat(v.remaining()).isEqualTo(expectedLength);
      final byte[] vBytes = new byte[expectedLength];
      v.get(vBytes);
      assertThat(vBytes).isEqualTo("value1++value2".getBytes());
    }
  }

  @Test
  public void mergeUntrackedByteBuffer_cf() throws RocksDBException {
    final ByteBuffer k1 = ByteBuffer.allocateDirect(20).put("key1".getBytes(UTF_8));
    final ByteBuffer v1 = ByteBuffer.allocateDirect(20).put("value1".getBytes(UTF_8));
    final ByteBuffer v2 = ByteBuffer.allocateDirect(20).put("value2".getBytes(UTF_8));
    k1.flip();
    v1.flip();
    v2.flip();

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.mergeUntracked(testCf, k1, v1);
      k1.flip();
      v1.flip();
      txn.mergeUntracked(testCf, k1, v2);
      k1.flip();
      v2.flip();
      txn.commit();
    }

    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      final ByteBuffer v = ByteBuffer.allocateDirect(20);
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final GetStatus status = txn.get(new ReadOptions(), testCf, k1, v);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
      k1.flip();
      v.flip();
      final int expectedLength = "value1++value2".length();
      assertThat(v.remaining()).isEqualTo(expectedLength);
      final byte[] vBytes = new byte[expectedLength];
      v.get(vBytes);
      assertThat(vBytes).isEqualTo("value1**value2".getBytes());
    }
  }

  @Test
  public void deleteUntracked_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, k1, v1);
      assertThat(txn.get(readOptions, testCf, k1)).isEqualTo(v1);
      assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);

      txn.deleteUntracked(testCf, k1);
      assertThat(txn.get(readOptions, testCf, k1)).isNull();
      assertThat(txn.get(testCf, readOptions, k1)).isNull();
    }
  }

  @Test
  public void deleteUntracked() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(k1, v1);
      assertThat(txn.get(readOptions, k1)).isEqualTo(v1);

      txn.deleteUntracked(k1);
      assertThat(txn.get(readOptions, k1)).isNull();
    }
  }

  @Test
  public void deleteUntracked_parts_cf() throws RocksDBException {
    final byte[][] keyParts = new byte[][] {"ke".getBytes(UTF_8), "y1".getBytes(UTF_8)};
    final byte[][] valueParts = new byte[][] {"val".getBytes(UTF_8), "ue1".getBytes(UTF_8)};
    final byte[] key = concat(keyParts);
    final byte[] value = concat(valueParts);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(testCf, keyParts, valueParts);
      assertThat(txn.get(readOptions, testCf, key)).isEqualTo(value);
      assertThat(txn.get(testCf, readOptions, key)).isEqualTo(value);

      txn.deleteUntracked(testCf, keyParts);
      assertThat(txn.get(readOptions, testCf, key)).isNull();
      assertThat(txn.get(testCf, readOptions, key)).isNull();
    }
  }

  @Test
  public void deleteUntracked_parts() throws RocksDBException {
    final byte[][] keyParts = new byte[][] {"ke".getBytes(UTF_8), "y1".getBytes(UTF_8)};
    final byte[][] valueParts = new byte[][] {"val".getBytes(UTF_8), "ue1".getBytes(UTF_8)};
    final byte[] key = concat(keyParts);
    final byte[] value = concat(valueParts);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.put(keyParts, valueParts);
      assertThat(txn.get(readOptions, key)).isEqualTo(value);

      txn.deleteUntracked(keyParts);
      assertThat(txn.get(readOptions, key)).isNull();
    }
  }

  @Test
  public void putLogData() throws RocksDBException {
    final byte[] blob = "blobby".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.putLogData(blob);
    }
  }

  @Test
  public void enabledDisableIndexing() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.disableIndexing();
      txn.enableIndexing();
      txn.disableIndexing();
      txn.enableIndexing();
    }
  }

  @Test
  public void numKeys() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] k2 = "key2".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);
    final byte[] k3 = "key3".getBytes(UTF_8);
    final byte[] v3 = "value3".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      txn.put(k1, v1);
      txn.put(testCf, k2, v2);
      txn.merge(k3, v3);
      txn.delete(testCf, k2);

      assertThat(txn.getNumKeys()).isEqualTo(3);
      assertThat(txn.getNumPuts()).isEqualTo(2);
      assertThat(txn.getNumMerges()).isEqualTo(1);
      assertThat(txn.getNumDeletes()).isEqualTo(1);
    }
  }

  @Test
  public void elapsedTime() throws RocksDBException, InterruptedException {
    final long preStartTxnTime = System.currentTimeMillis();
    try (final DBContainer dbContainer = startDb();
         final Transaction txn = dbContainer.beginTransaction()) {
      Thread.sleep(2);

      final long txnElapsedTime = txn.getElapsedTime();
      assertThat(txnElapsedTime).isLessThan(System.currentTimeMillis() - preStartTxnTime);
      assertThat(txnElapsedTime).isGreaterThan(0);
    }
  }

  @Test
  public void getWriteBatch() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {

      txn.put(k1, v1);

      final WriteBatchWithIndex writeBatch = txn.getWriteBatch();
      assertThat(writeBatch).isNotNull();
      assertThat(writeBatch.isOwningHandle()).isFalse();
      assertThat(writeBatch.count()).isEqualTo(1);
    }
  }

  @Test
  public void setLockTimeout() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      txn.setLockTimeout(1000);
    }
  }

  @Test
  public void writeOptions() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final WriteOptions writeOptions = new WriteOptions()
        .setDisableWAL(true)
        .setSync(true);
        final Transaction txn = dbContainer.beginTransaction(writeOptions)) {

      txn.put(k1, v1);

      WriteOptions txnWriteOptions = txn.getWriteOptions();
      assertThat(txnWriteOptions).isNotNull();
      assertThat(txnWriteOptions.isOwningHandle()).isFalse();
      assertThat(txnWriteOptions).isNotSameAs(writeOptions);
      assertThat(txnWriteOptions.disableWAL()).isTrue();
      assertThat(txnWriteOptions.sync()).isTrue();

      txn.setWriteOptions(txnWriteOptions.setSync(false));
      txnWriteOptions = txn.getWriteOptions();
      assertThat(txnWriteOptions).isNotNull();
      assertThat(txnWriteOptions.isOwningHandle()).isFalse();
      assertThat(txnWriteOptions).isNotSameAs(writeOptions);
      assertThat(txnWriteOptions.disableWAL()).isTrue();
      assertThat(txnWriteOptions.sync()).isFalse();
    }
  }

  @Test
  public void undoGetForUpdate_cf() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      assertThat(txn.getForUpdate(readOptions, testCf, k1, true)).isNull();
      txn.put(testCf, k1, v1);
      assertThat(txn.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);
      txn.undoGetForUpdate(testCf, k1);
    }
  }

  @Test
  public void undoGetForUpdate() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getForUpdate(readOptions, k1, true)).isNull();
      txn.put(k1, v1);
      assertThat(txn.getForUpdate(readOptions, k1, true)).isEqualTo(v1);
      txn.undoGetForUpdate(k1);
    }
  }

  @Test
  public void rebuildFromWriteBatch() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);
    final byte[] k2 = "key2".getBytes(UTF_8);
    final byte[] v2 = "value2".getBytes(UTF_8);
    final byte[] k3 = "key3".getBytes(UTF_8);
    final byte[] v3 = "value3".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions();
        final Transaction txn = dbContainer.beginTransaction()) {

      txn.put(k1, v1);

      assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
      assertThat(txn.getNumKeys()).isEqualTo(1);

      try(final WriteBatch writeBatch = new WriteBatch()) {
        writeBatch.put(k2, v2);
        writeBatch.put(k3, v3);
        txn.rebuildFromWriteBatch(writeBatch);

        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
        assertThat(txn.get(readOptions, k2)).isEqualTo(v2);
        assertThat(txn.get(readOptions, k3)).isEqualTo(v3);
        assertThat(txn.getNumKeys()).isEqualTo(3);
      }
    }
  }

  @Test
  public void getCommitTimeWriteBatch() throws RocksDBException {
    final byte[] k1 = "key1".getBytes(UTF_8);
    final byte[] v1 = "value1".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {

      txn.put(k1, v1);
      final WriteBatch writeBatch = txn.getCommitTimeWriteBatch();

      assertThat(writeBatch).isNotNull();
      assertThat(writeBatch.isOwningHandle()).isFalse();
      assertThat(writeBatch.count()).isEqualTo(0);
    }
  }

  @Test
  public void logNumber() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getLogNumber()).isEqualTo(0);
      final long logNumber = rand.nextLong();
      txn.setLogNumber(logNumber);
      assertThat(txn.getLogNumber()).isEqualTo(logNumber);
    }
  }

  private static byte[] concat(final byte[][] bufs) {
    int resultLength = 0;
    for(final byte[] buf : bufs) {
      resultLength += buf.length;
    }

    final byte[] result = new byte[resultLength];
    int resultOffset = 0;
    for(final byte[] buf : bufs) {
      final int srcLength = buf.length;
      System.arraycopy(buf, 0, result, resultOffset, srcLength);
      resultOffset += srcLength;
    }

    return result;
  }

  private static class TestTransactionNotifier
      extends AbstractTransactionNotifier {
    private final List<Snapshot> createdSnapshots = new ArrayList<>();

    @Override
    public void snapshotCreated(final Snapshot newSnapshot) {
      createdSnapshots.add(newSnapshot);
    }

    public List<Snapshot> getCreatedSnapshots() {
      return createdSnapshots;
    }
  }

  protected abstract static class DBContainer implements AutoCloseable {
    protected final WriteOptions writeOptions;
    protected final List<ColumnFamilyHandle> columnFamilyHandles;
    protected final ColumnFamilyOptions columnFamilyOptions;
    protected final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    protected final DBOptions options;

    public DBContainer(final WriteOptions writeOptions,
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
        final List<ColumnFamilyHandle> columnFamilyHandles,
        final ColumnFamilyOptions columnFamilyOptions, final DBOptions options) {
      this.writeOptions = writeOptions;
      this.columnFamilyDescriptors = columnFamilyDescriptors;
      this.columnFamilyHandles = columnFamilyHandles;
      this.columnFamilyOptions = columnFamilyOptions;
      this.options = options;
    }

    public abstract Transaction beginTransaction();

    public abstract Transaction beginTransaction(
        final WriteOptions writeOptions);

    public ColumnFamilyHandle getTestColumnFamily() {
      return columnFamilyHandles.get(1);
    }

    @Override
    public abstract void close();
  }
}
