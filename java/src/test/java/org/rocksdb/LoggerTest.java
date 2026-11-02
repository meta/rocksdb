// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LoggerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void customLogger() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL).setCreateIfMissing(true);
        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      // Set custom logger to options
      options.setLogger(logger);

      try (final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // there should be more than zero received log messages in
        // debug level.
        assertThat(logMessageCounter.get()).isGreaterThan(0);
      }
    }
  }

  @Test
  public void warnLogger() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.WARN_LEVEL).setCreateIfMissing(true);

        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      // Set custom logger to options
      options.setLogger(logger);

      try (final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // there should be zero messages
        // using warn level as log level.
        assertThat(logMessageCounter.get()).isEqualTo(0);
      }
    }
  }


  @Test
  public void fatalLogger() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).setCreateIfMissing(true);

        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      // Set custom logger to options
      options.setLogger(logger);

      try (final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // there should be zero messages
        // using fatal level as log level.
        assertThat(logMessageCounter.get()).isEqualTo(0);
      }
    }
  }

  @Test
  public void dbOptionsLogger() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final DBOptions options =
             new DBOptions().setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).setCreateIfMissing(true);
        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      // Set custom logger to options
      options.setLogger(logger);

      final List<ColumnFamilyDescriptor> cfDescriptors =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (final RocksDB ignored = RocksDB.open(
               options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, cfHandles)) {
        try {
          // there should be zero messages
          // using fatal level as log level.
          assertThat(logMessageCounter.get()).isEqualTo(0);
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle : cfHandles) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void setWarnLogLevel() {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).setCreateIfMissing(true);
        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      assertThat(logger.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      logger.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
      assertThat(logger.infoLogLevel()).
          isEqualTo(InfoLogLevel.WARN_LEVEL);
    }
  }

  @Test
  public void setInfoLogLevel() {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).setCreateIfMissing(true);
        final Logger logger = new Logger(options.infoLogLevel()) {
          // Create new logger with max log level passed by options
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      assertThat(logger.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      logger.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
      assertThat(logger.infoLogLevel()).
          isEqualTo(InfoLogLevel.DEBUG_LEVEL);
    }
  }

  @Test
  public void changeLogLevelAtRuntime() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final Options options =
             new Options().setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).setCreateIfMissing(true);

        // Create new logger with max log level passed by options
        final Logger logger = new Logger(options.infoLogLevel()) {
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            assertThat(logMsg).isNotNull();
            assertThat(logMsg.length()).isGreaterThan(0);
            logMessageCounter.incrementAndGet();
          }
        }) {
      // Set custom logger to options
      options.setLogger(logger);

      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {

        // there should be zero messages
        // using fatal level as log level.
        assertThat(logMessageCounter.get()).isEqualTo(0);

        // change log level to debug level
        logger.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);

        db.put("key".getBytes(), "value".getBytes());
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
          db.flush(flushOptions);
        }

        // messages shall be received due to previous actions.
        assertThat(logMessageCounter.get()).isNotEqualTo(0);
      }
    }
  }

  @Test
  public void logLevelLogger() throws RocksDBException {
    final AtomicInteger logMessageCounter = new AtomicInteger();
    try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
         final Logger logger = new Logger(InfoLogLevel.FATAL_LEVEL) {
           // Create new logger with max log level passed by options
           @Override
           protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
             assertThat(logMsg).isNotNull();
             assertThat(logMsg.length()).isGreaterThan(0);
             logMessageCounter.incrementAndGet();
           }
         }) {
      // Set custom logger to options
      options.setLogger(logger);

      final List<ColumnFamilyDescriptor> cfDescriptors =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (final RocksDB ignored = RocksDB.open(
               options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, cfHandles)) {
        try {
          // there should be zero messages
          // using fatal level as log level.
          assertThat(logMessageCounter.get()).isEqualTo(0);
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle : cfHandles) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }
}
