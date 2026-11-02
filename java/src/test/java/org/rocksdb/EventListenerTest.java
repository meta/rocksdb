//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.AbstractEventListener.EnabledEventCallback;
import org.rocksdb.test.TestableEventListener;
import org.rocksdb.util.Environment;

public class EventListenerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  void flushDb(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.flush(new FlushOptions());
      assertThat(wasCbCalled.get()).isTrue();
    }
  }

  @Test
  public void onFlushCompleted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onFlushCompletedListener = new AbstractEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB rocksDb, final FlushJobInfo flushJobInfo) {
        assertThat(flushJobInfo.getColumnFamilyName()).isNotNull();
        assertThat(flushJobInfo.getFlushReason()).isEqualTo(FlushReason.MANUAL_FLUSH);
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushCompletedListener, wasCbCalled);
  }

  @Test
  public void onFlushBegin() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onFlushBeginListener = new AbstractEventListener() {
      @Override
      public void onFlushBegin(final RocksDB rocksDb, final FlushJobInfo flushJobInfo) {
        assertThat(flushJobInfo.getColumnFamilyName()).isNotNull();
        assertThat(flushJobInfo.getFlushReason()).isEqualTo(FlushReason.MANUAL_FLUSH);
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushBeginListener, wasCbCalled);
  }

  @SuppressWarnings("deprecation")
  void deleteTableFile(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 1000;
    final int FILE_SIZE = 64000;
    final int NUM_FILES = 2;

    final int KEY_INTERVAL = 10000;
    /*
     * Intention of these options is to end up reliably with NUM_FILES files.
     * we will be deleting using deleteFilesInRange.
     * It is writing roughly number of keys that will fit in NUM_FILES files (target size).
     * It is writing interleaved so that files from memory on L0 will overlap.
     * Then compaction cleans everything, and we should end up with NUM_FILES files.
     */
    try (final Options opt = new Options()
                                 .setCreateIfMissing(true)
                                 .setListeners(Collections.singletonList(el))
                                 .setCompressionType(CompressionType.NO_COMPRESSION)
                                 .setTargetFileSizeBase(FILE_SIZE)
                                 .setWriteBufferSize(FILE_SIZE / 2)
                                 .setDisableAutoCompactions(true)
                                 .setLevelCompactionDynamicLevelBytes(false);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final int records = FILE_SIZE / (KEY_SIZE + VALUE_SIZE);

      // fill database with key/value pairs
      final byte[] value = new byte[VALUE_SIZE];
      int key_init = 0;
      for (int o = 0; o < NUM_FILES; ++o) {
        int int_key = key_init++;
        for (int i = 0; i < records; ++i) {
          int_key += KEY_INTERVAL;
          rand.nextBytes(value);

          db.put(String.format("%020d", int_key).getBytes(), value);
        }
      }
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }
      db.compactRange();
      db.deleteFilesInRanges(null, Arrays.asList(null, null), false /* includeEnd */);
    }
    assertThat(wasCbCalled.get()).isTrue();
  }

  @Test
  public void onTableFileDeleted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileDeletedListener = new AbstractEventListener() {
      @Override
      public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
        assertThat(tableFileDeletionInfo.getDbName()).isNotNull();
        wasCbCalled.set(true);
      }
    };
    deleteTableFile(onTableFileDeletedListener, wasCbCalled);
  }

  void compactRange(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.compactRange();
      assertThat(wasCbCalled.get()).isTrue();
    }
  }

  @Test
  public void onCompactionBegin() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onCompactionBeginListener = new AbstractEventListener() {
      @Override
      public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        assertThat(compactionJobInfo.compactionReason())
            .isEqualTo(CompactionReason.kManualCompaction);
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionBeginListener, wasCbCalled);
  }

  @Test
  public void onCompactionCompleted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onCompactionCompletedListener = new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(
          final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        assertThat(compactionJobInfo.compactionReason())
            .isEqualTo(CompactionReason.kManualCompaction);
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionCompletedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreated() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileCreatedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
        assertThat(tableFileCreationInfo.getReason()).isEqualTo(TableFileCreationReason.FLUSH);
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreatedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreationStarted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileCreationStartedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreationStarted(
          final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        assertThat(tableFileCreationBriefInfo.getReason()).isEqualTo(TableFileCreationReason.FLUSH);
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreationStartedListener, wasCbCalled);
  }

  void deleteColumnFamilyHandle(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      final ColumnFamilyHandle columnFamilyHandle = db.getDefaultColumnFamily();
      columnFamilyHandle.close();
      assertThat(wasCbCalled.get()).isTrue();
    }
  }

  @Test
  public void onColumnFamilyHandleDeletionStarted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onColumnFamilyHandleDeletionStartedListener =
        new AbstractEventListener() {
          @Override
          public void onColumnFamilyHandleDeletionStarted(
              final ColumnFamilyHandle columnFamilyHandle) {
            assertThat(columnFamilyHandle).isNotNull();
            wasCbCalled.set(true);
          }
        };
    deleteColumnFamilyHandle(onColumnFamilyHandleDeletionStartedListener, wasCbCalled);
  }

  void ingestExternalFile(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final String uuid = UUID.randomUUID().toString();
      final SstFileWriter sstFileWriter = new SstFileWriter(new EnvOptions(), opt);
      final Path externalFilePath = Paths.get(db.getName(), uuid);
      sstFileWriter.open(externalFilePath.toString());
      sstFileWriter.put("testKey".getBytes(), uuid.getBytes());
      sstFileWriter.finish();
      db.ingestExternalFile(
          Collections.singletonList(externalFilePath.toString()), new IngestExternalFileOptions());
      assertThat(wasCbCalled.get()).isTrue();
    }
  }

  @Test
  public void onExternalFileIngested() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onExternalFileIngestedListener = new AbstractEventListener() {
      @Override
      public void onExternalFileIngested(
          final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
        assertThat(db).isNotNull();
        wasCbCalled.set(true);
      }
    };
    ingestExternalFile(onExternalFileIngestedListener, wasCbCalled);
  }

  @Test
  public void testAllCallbacksInvocation() {
    final long TEST_LONG_VAL = -1;
    // Expected test data objects
    final Map<String, String> userCollectedPropertiesTestData =
        Collections.singletonMap("key", "value");
    final Map<String, String> readablePropertiesTestData = Collections.singletonMap("key", "value");
    final TableProperties tablePropertiesTestData = new TableProperties(TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, "columnFamilyName".getBytes(), "filterPolicyName",
        "comparatorName", "mergeOperatorName", "prefixExtractorName", "propertyCollectorsNames",
        "compressionName", userCollectedPropertiesTestData, readablePropertiesTestData);
    final FlushJobInfo flushJobInfoTestData = new FlushJobInfo(Integer.MAX_VALUE,
        "testColumnFamily", "/file/path", TEST_LONG_VAL, Integer.MAX_VALUE, true, true,
        TEST_LONG_VAL, TEST_LONG_VAL, tablePropertiesTestData, (byte) 0x0a);
    final Status statusTestData = new Status(Status.Code.Incomplete, Status.SubCode.NoSpace, null);
    final TableFileDeletionInfo tableFileDeletionInfoTestData =
        new TableFileDeletionInfo("dbName", "/file/path", Integer.MAX_VALUE, statusTestData);
    final TableFileCreationInfo tableFileCreationInfoTestData = new TableFileCreationInfo(
        TEST_LONG_VAL, tablePropertiesTestData, statusTestData, "dbName", "columnFamilyName",
        "/file/path", Integer.MAX_VALUE, TableFileCreationReason.MISC.getValue());
    final TableFileCreationBriefInfo tableFileCreationBriefInfoTestData =
        new TableFileCreationBriefInfo("dbName", "columnFamilyName", "/file/path",
            Integer.MAX_VALUE, TableFileCreationReason.MISC.getValue());
    final MemTableInfo memTableInfoTestData = new MemTableInfo(
        "columnFamilyName", TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL);
    final FileOperationInfo fileOperationInfoTestData = new FileOperationInfo("/file/path",
        TEST_LONG_VAL, 4096, 1_600_699_420_000_000_000L, 5_000_000_000L, statusTestData);
    final WriteStallInfo writeStallInfoTestData =
        new WriteStallInfo("columnFamilyName", (byte) 0x0, (byte) 0x1);
    final ExternalFileIngestionInfo externalFileIngestionInfoTestData =
        new ExternalFileIngestionInfo("columnFamilyName", "/external/file/path",
            "/internal/file/path", TEST_LONG_VAL, tablePropertiesTestData);

    final CapturingTestableEventListener listener = new CapturingTestableEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo) {
        super.onFlushCompleted(db, flushJobInfo);
        assertThat(flushJobInfo).isEqualTo(flushJobInfoTestData);
      }

      @Override
      public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
        super.onFlushBegin(db, flushJobInfo);
        assertThat(flushJobInfo).isEqualTo(flushJobInfoTestData);
      }

      @Override
      public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
        super.onTableFileDeleted(tableFileDeletionInfo);
        assertThat(tableFileDeletionInfo).isEqualTo(tableFileDeletionInfoTestData);
      }

      @Override
      public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        super.onCompactionBegin(db, compactionJobInfo);
        assertThat(new String(compactionJobInfo.columnFamilyName(), StandardCharsets.UTF_8))
            .isEqualTo("compactionColumnFamily");
        assertThat(compactionJobInfo.status()).isEqualTo(statusTestData);
        assertThat(compactionJobInfo.threadId()).isEqualTo(TEST_LONG_VAL);
        assertThat(compactionJobInfo.jobId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.baseInputLevel()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.outputLevel()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.inputFiles())
            .isEqualTo(Collections.singletonList("inputFile.sst"));
        assertThat(compactionJobInfo.outputFiles())
            .isEqualTo(Collections.singletonList("outputFile.sst"));
        assertThat(compactionJobInfo.tableProperties())
            .isEqualTo(Collections.singletonMap("tableProperties", tablePropertiesTestData));
        assertThat(compactionJobInfo.compactionReason()).isEqualTo(CompactionReason.kFlush);
        assertThat(compactionJobInfo.compression()).isEqualTo(CompressionType.SNAPPY_COMPRESSION);
      }

      @Override
      public void onCompactionCompleted(
          final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        super.onCompactionCompleted(db, compactionJobInfo);
        assertThat(new String(compactionJobInfo.columnFamilyName()))
            .isEqualTo("compactionColumnFamily");
        assertThat(compactionJobInfo.status()).isEqualTo(statusTestData);
        assertThat(compactionJobInfo.threadId()).isEqualTo(TEST_LONG_VAL);
        assertThat(compactionJobInfo.jobId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.baseInputLevel()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.outputLevel()).isEqualTo(Integer.MAX_VALUE);
        assertThat(compactionJobInfo.inputFiles())
            .isEqualTo(Collections.singletonList("inputFile.sst"));
        assertThat(compactionJobInfo.outputFiles())
            .isEqualTo(Collections.singletonList("outputFile.sst"));
        assertThat(compactionJobInfo.tableProperties())
            .isEqualTo(Collections.singletonMap("tableProperties", tablePropertiesTestData));
        assertThat(compactionJobInfo.compactionReason()).isEqualTo(CompactionReason.kFlush);
        assertThat(compactionJobInfo.compression()).isEqualTo(CompressionType.SNAPPY_COMPRESSION);
      }

      @Override
      public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
        super.onTableFileCreated(tableFileCreationInfo);
        assertThat(tableFileCreationInfo).isEqualTo(tableFileCreationInfoTestData);
      }

      @Override
      public void onTableFileCreationStarted(
          final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        super.onTableFileCreationStarted(tableFileCreationBriefInfo);
        assertThat(tableFileCreationBriefInfo).isEqualTo(tableFileCreationBriefInfoTestData);
      }

      @Override
      public void onMemTableSealed(final MemTableInfo memTableInfo) {
        super.onMemTableSealed(memTableInfo);
        assertThat(memTableInfo).isEqualTo(memTableInfoTestData);
      }

      @Override
      public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
        super.onColumnFamilyHandleDeletionStarted(columnFamilyHandle);
      }

      @Override
      public void onExternalFileIngested(
          final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
        super.onExternalFileIngested(db, externalFileIngestionInfo);
        assertThat(externalFileIngestionInfo).isEqualTo(externalFileIngestionInfoTestData);
      }

      @Override
      public void onBackgroundError(
          final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
        super.onBackgroundError(backgroundErrorReason, backgroundError);
      }

      @Override
      public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
        super.onStallConditionsChanged(writeStallInfo);
        assertThat(writeStallInfo).isEqualTo(writeStallInfoTestData);
      }

      @Override
      public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileReadFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileWriteFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileFlushFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileFlushFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileSyncFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileSyncFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileRangeSyncFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileTruncateFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileTruncateFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public void onFileCloseFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileCloseFinish(fileOperationInfo);
        assertThat(fileOperationInfo).isEqualTo(fileOperationInfoTestData);
      }

      @Override
      public boolean shouldBeNotifiedOnFileIO() {
        super.shouldBeNotifiedOnFileIO();
        return false;
      }

      @Override
      public boolean onErrorRecoveryBegin(
          final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
        super.onErrorRecoveryBegin(backgroundErrorReason, backgroundError);
        assertThat(backgroundErrorReason).isEqualTo(BackgroundErrorReason.FLUSH);
        assertThat(backgroundError).isEqualTo(statusTestData);
        return true;
      }

      @Override
      public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
        super.onErrorRecoveryCompleted(oldBackgroundError);
        assertThat(oldBackgroundError).isEqualTo(statusTestData);
      }
    };

    // test action
    listener.invokeAllCallbacks();

    // assert
    assertAllEventsCalled(listener);

    assertNoCallbackErrors(listener);
  }

  @Test
  public void testEnabledCallbacks() {
    final EnabledEventCallback[] enabledEvents = {
        EnabledEventCallback.ON_MEMTABLE_SEALED, EnabledEventCallback.ON_ERROR_RECOVERY_COMPLETED};

    final CapturingTestableEventListener listener =
        new CapturingTestableEventListener(enabledEvents);

    // test action
    listener.invokeAllCallbacks();

    // assert
    assertEventsCalled(listener, enabledEvents);
  }

  private static void assertAllEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener) {
    assertEventsCalled(capturingTestableEventListener, EnumSet.allOf(EnabledEventCallback.class));
  }

  private static void assertEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener,
      final EnabledEventCallback[] expected) {
    assertEventsCalled(capturingTestableEventListener, EnumSet.copyOf(Arrays.asList(expected)));
  }

  private static void assertNoCallbackErrors(
      final CapturingTestableEventListener capturingTestableEventListener) {
    for (final AssertionError error : capturingTestableEventListener.capturedAssertionErrors) {
      throw new Error("An assertion failed in callback", error);
    }
  }

  private static void assertEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener,
      final EnumSet<EnabledEventCallback> expected) {
    final ListenerEvents capturedEvents = capturingTestableEventListener.capturedListenerEvents;

    assertThat(capturedEvents.flushCompleted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FLUSH_COMPLETED));
    assertThat(capturedEvents.flushBegin)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FLUSH_BEGIN));
    assertThat(capturedEvents.tableFileDeleted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_TABLE_FILE_DELETED));
    assertThat(capturedEvents.compactionBegin)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_COMPACTION_BEGIN));
    assertThat(capturedEvents.compactionCompleted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_COMPACTION_COMPLETED));
    assertThat(capturedEvents.tableFileCreated)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_TABLE_FILE_CREATED));
    assertThat(capturedEvents.tableFileCreationStarted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_TABLE_FILE_CREATION_STARTED));
    assertThat(capturedEvents.memTableSealed)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_MEMTABLE_SEALED));
    assertThat(capturedEvents.columnFamilyHandleDeletionStarted)
        .isEqualTo(
            expected.contains(EnabledEventCallback.ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED));
    assertThat(capturedEvents.externalFileIngested)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_EXTERNAL_FILE_INGESTED));
    assertThat(capturedEvents.backgroundError)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_BACKGROUND_ERROR));
    assertThat(capturedEvents.stallConditionsChanged)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_STALL_CONDITIONS_CHANGED));
    assertThat(capturedEvents.fileReadFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_READ_FINISH));
    assertThat(capturedEvents.fileWriteFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_WRITE_FINISH));
    assertThat(capturedEvents.fileFlushFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_FLUSH_FINISH));
    assertThat(capturedEvents.fileSyncFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_SYNC_FINISH));
    assertThat(capturedEvents.fileRangeSyncFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_RANGE_SYNC_FINISH));
    assertThat(capturedEvents.fileTruncateFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_TRUNCATE_FINISH));
    assertThat(capturedEvents.fileCloseFinish)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_FILE_CLOSE_FINISH));
    assertThat(capturedEvents.shouldBeNotifiedOnFileIO)
        .isEqualTo(expected.contains(EnabledEventCallback.SHOULD_BE_NOTIFIED_ON_FILE_IO));
    assertThat(capturedEvents.errorRecoveryBegin)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_ERROR_RECOVERY_BEGIN));
    assertThat(capturedEvents.errorRecoveryCompleted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_ERROR_RECOVERY_COMPLETED));
    assertThat(capturedEvents.errorRecoveryCompleted)
        .isEqualTo(expected.contains(EnabledEventCallback.ON_ERROR_RECOVERY_COMPLETED));
  }

  /**
   * Members are volatile as they may be written
   * and read by different threads.
   */
  private static class ListenerEvents {
    volatile boolean flushCompleted;
    volatile boolean flushBegin;
    volatile boolean tableFileDeleted;
    volatile boolean compactionBegin;
    volatile boolean compactionCompleted;
    volatile boolean tableFileCreated;
    volatile boolean tableFileCreationStarted;
    volatile boolean memTableSealed;
    volatile boolean columnFamilyHandleDeletionStarted;
    volatile boolean externalFileIngested;
    volatile boolean backgroundError;
    volatile boolean stallConditionsChanged;
    volatile boolean fileReadFinish;
    volatile boolean fileWriteFinish;
    volatile boolean fileFlushFinish;
    volatile boolean fileSyncFinish;
    volatile boolean fileRangeSyncFinish;
    volatile boolean fileTruncateFinish;
    volatile boolean fileCloseFinish;
    volatile boolean shouldBeNotifiedOnFileIO;
    volatile boolean errorRecoveryBegin;
    volatile boolean errorRecoveryCompleted;
  }

  private static class CapturingObjectAssert<T> extends ObjectAssert<T> {
    private final List<AssertionError> assertionErrors;
    public CapturingObjectAssert(final T t, final List<AssertionError> assertionErrors) {
      super(t);
      this.assertionErrors = assertionErrors;
    }

    @Override
    public ObjectAssert<T> isEqualTo(final Object other) {
      try {
        return super.isEqualTo(other);
      } catch (final AssertionError error) {
        assertionErrors.add(error);
        throw error;
      }
    }

    @Override
    public ObjectAssert<T> isNotNull() {
      try {
        return super.isNotNull();
      } catch (final AssertionError error) {
        assertionErrors.add(error);
        throw error;
      }
    }
  }

  private static class CapturingTestableEventListener extends TestableEventListener {
    final ListenerEvents capturedListenerEvents = new ListenerEvents();

    final List<AssertionError> capturedAssertionErrors = new ArrayList<>();

    protected <T> AbstractObjectAssert<?, T> assertThat(final T actual) {
      return new CapturingObjectAssert<>(actual, capturedAssertionErrors);
    }

    public CapturingTestableEventListener() {}

    public CapturingTestableEventListener(final EnabledEventCallback... enabledEventCallbacks) {
      super(enabledEventCallbacks);
    }

    @Override
    public void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo) {
      capturedListenerEvents.flushCompleted = true;
    }

    @Override
    public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
      capturedListenerEvents.flushBegin = true;
    }

    @Override
    public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
      capturedListenerEvents.tableFileDeleted = true;
    }

    @Override
    public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
      capturedListenerEvents.compactionBegin = true;
    }

    @Override
    public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
      capturedListenerEvents.compactionCompleted = true;
    }

    @Override
    public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
      capturedListenerEvents.tableFileCreated = true;
    }

    @Override
    public void onTableFileCreationStarted(
        final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
      capturedListenerEvents.tableFileCreationStarted = true;
    }

    @Override
    public void onMemTableSealed(final MemTableInfo memTableInfo) {
      capturedListenerEvents.memTableSealed = true;
    }

    @Override
    public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
      capturedListenerEvents.columnFamilyHandleDeletionStarted = true;
    }

    @Override
    public void onExternalFileIngested(
        final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
      capturedListenerEvents.externalFileIngested = true;
    }

    @Override
    public void onBackgroundError(
        final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
      capturedListenerEvents.backgroundError = true;
    }

    @Override
    public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
      capturedListenerEvents.stallConditionsChanged = true;
    }

    @Override
    public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileReadFinish = true;
    }

    @Override
    public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileWriteFinish = true;
    }

    @Override
    public void onFileFlushFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileFlushFinish = true;
    }

    @Override
    public void onFileSyncFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileSyncFinish = true;
    }

    @Override
    public void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileRangeSyncFinish = true;
    }

    @Override
    public void onFileTruncateFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileTruncateFinish = true;
    }

    @Override
    public void onFileCloseFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileCloseFinish = true;
    }

    @Override
    public boolean shouldBeNotifiedOnFileIO() {
      capturedListenerEvents.shouldBeNotifiedOnFileIO = true;
      return false;
    }

    @Override
    public boolean onErrorRecoveryBegin(
        final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
      capturedListenerEvents.errorRecoveryBegin = true;
      return true;
    }

    @Override
    public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
      capturedListenerEvents.errorRecoveryCompleted = true;
    }
  }
}
