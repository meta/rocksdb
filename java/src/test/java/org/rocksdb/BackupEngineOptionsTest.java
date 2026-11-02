// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import java.util.function.Consumer;
import org.junit.ClassRule;
import org.junit.Test;

public class BackupEngineOptionsTest {
  private static final String ARBITRARY_PATH = System.getProperty("java.io.tmpdir");

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void backupDir() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupDir()).isEqualTo(ARBITRARY_PATH);
    }
  }

  @Test
  public void env() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final Env env = new RocksMemEnv(Env.getDefault())) {
        backupEngineOptions.setBackupEnv(env);
        assertThat(backupEngineOptions.backupEnv()).isEqualTo(env);
      }
    }
  }

  @Test
  public void shareTableFiles() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setShareTableFiles(value);
      assertThat(backupEngineOptions.shareTableFiles()).isEqualTo(value);
    }
  }

  @Test
  public void infoLog() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.infoLog()).isNull();

      try (final Options options = new Options(); final Logger logger = new Logger(
                                                      options.infoLogLevel()) {
        @Override
        protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {}
      }) {
        backupEngineOptions.setInfoLog(logger);
        assertThat(backupEngineOptions.infoLog()).isEqualTo(logger);
      }
    }
  }

  @Test
  public void sync() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setSync(value);
      assertThat(backupEngineOptions.sync()).isEqualTo(value);
    }
  }

  @Test
  public void destroyOldData() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setDestroyOldData(value);
      assertThat(backupEngineOptions.destroyOldData()).isEqualTo(value);
    }
  }

  @Test
  public void backupLogFiles() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setBackupLogFiles(value);
      assertThat(backupEngineOptions.backupLogFiles()).isEqualTo(value);
    }
  }

  @Test
  public void backupRateLimit() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupEngineOptions.setBackupRateLimit(value);
      assertThat(backupEngineOptions.backupRateLimit()).isEqualTo(value);
      // negative will be mapped to 0
      backupEngineOptions.setBackupRateLimit(-1);
      assertThat(backupEngineOptions.backupRateLimit()).isEqualTo(0);
    }
  }

  @Test
  public void backupRateLimiter() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final RateLimiter backupRateLimiter =
              new RateLimiter(999)) {
        backupEngineOptions.setBackupRateLimiter(backupRateLimiter);
        assertThat(backupEngineOptions.backupRateLimiter()).isEqualTo(backupRateLimiter);
      }
    }
  }

  @Test
  public void restoreRateLimit() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupEngineOptions.setRestoreRateLimit(value);
      assertThat(backupEngineOptions.restoreRateLimit()).isEqualTo(value);
      // negative will be mapped to 0
      backupEngineOptions.setRestoreRateLimit(-1);
      assertThat(backupEngineOptions.restoreRateLimit()).isEqualTo(0);
    }
  }

  @Test
  public void restoreRateLimiter() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final RateLimiter restoreRateLimiter =
              new RateLimiter(911)) {
        backupEngineOptions.setRestoreRateLimiter(restoreRateLimiter);
        assertThat(backupEngineOptions.restoreRateLimiter()).isEqualTo(restoreRateLimiter);
      }
    }
  }

  @Test
  public void shareFilesWithChecksum() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setShareFilesWithChecksum(value);
      assertThat(backupEngineOptions.shareFilesWithChecksum()).isEqualTo(value);
    }
  }

  @Test
  public void maxBackgroundOperations() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final int value = rand.nextInt();
      backupEngineOptions.setMaxBackgroundOperations(value);
      assertThat(backupEngineOptions.maxBackgroundOperations()).isEqualTo(value);
    }
  }

  @Test
  public void callbackTriggerIntervalSize() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = rand.nextLong();
      backupEngineOptions.setCallbackTriggerIntervalSize(value);
      assertThat(backupEngineOptions.callbackTriggerIntervalSize()).isEqualTo(value);
    }
  }

  @Test
  public void failBackupDirIsNull() {
    assertThatThrownBy(() -> {
      try (final BackupEngineOptions ignored = new BackupEngineOptions(null)) {
        ; // no-op
      }
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void failBackupDirIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::backupDir);
  }

  @Test
  public void failSetShareTableFilesIfDisposed() {
    failOperationWithClosedOptions(options -> options.setShareTableFiles(true));
  }

  @Test
  public void failShareTableFilesIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::shareTableFiles);
  }

  @Test
  public void failSetSyncIfDisposed() {
    failOperationWithClosedOptions(options -> options.setSync(true));
  }

  @Test
  public void failSyncIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::sync);
  }

  @Test
  public void failSetDestroyOldDataIfDisposed() {
    failOperationWithClosedOptions(options -> options.setDestroyOldData(true));
  }

  @Test
  public void failDestroyOldDataIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::destroyOldData);
  }

  @Test
  public void failSetBackupLogFilesIfDisposed() {
    failOperationWithClosedOptions(options -> options.setBackupLogFiles(true));
  }

  @Test
  public void failBackupLogFilesIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::backupLogFiles);
  }

  @Test
  public void failSetBackupRateLimitIfDisposed() {
    failOperationWithClosedOptions(options -> options.setBackupRateLimit(1));
  }

  @Test
  public void failBackupRateLimitIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::backupRateLimit);
  }

  @Test
  public void failSetRestoreRateLimitIfDisposed() {
    failOperationWithClosedOptions(options -> options.setRestoreRateLimit(1));
  }

  @Test
  public void failRestoreRateLimitIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::restoreRateLimit);
  }

  @Test
  public void failSetShareFilesWithChecksumIfDisposed() {
    failOperationWithClosedOptions(options -> options.setShareFilesWithChecksum(true));
  }

  @Test
  public void failShareFilesWithChecksumIfDisposed() {
    failOperationWithClosedOptions(BackupEngineOptions::shareFilesWithChecksum);
  }

  private void failOperationWithClosedOptions(Consumer<BackupEngineOptions> operation) {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      backupEngineOptions.close();
      assertThatThrownBy(() -> operation.accept(backupEngineOptions))
          .isInstanceOf(AssertionError.class);
    }
  }
}
