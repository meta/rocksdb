//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

public class LoggerFromOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  // The log is meant to show up in a different directory from the DB..
  @Rule public TemporaryFolder logFolder = new TemporaryFolder();

  @Test
  public void openReadOnlyWithLoggerFromOptions() throws RocksDBException, IOException {
    // Create the DB and close it again
    try (final Options options = new Options().setCreateIfMissing(true);
        final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.flush(flushOptions);
    }

    LoggerFromOptions logger =
        LoggerFromOptions.CreateLoggerFromOptions(logFolder.getRoot().toString(), new DBOptions());
    logger.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);

    // Expect these configured options to be output in the log
    List<String> remainingMatches = new ArrayList<>(
        Arrays.asList("Options.max_log_file_size: 65536", "Options.log_file_time_to_roll: 2048",
            "Options.keep_log_file_num: 8", "Options.recycle_log_file_num: 3"));

    // Configure the log in order for the dump of log configuration to the log to be unique
    // Open the DB readonly and give it the new logger to use
    try (final Options options = new Options()
             .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)
             .setLogger(logger)
             .setMaxLogFileSize(1024L * 64L)
             .setRecycleLogFileNum(3)
             .setLogFileTimeToRoll(2048)
             .setKeepLogFileNum(8);
        final RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db.getDBOptions()).isNotNull();
    }

    // Look for evidence that the (readonly) DB has written to the logger we gave it
    File[] logFiles = Objects.requireNonNull(logFolder.getRoot().listFiles());
    assertThat(logFiles.length).isEqualTo(1);
    File logFile = logFiles[0];
    BufferedReader reader = new BufferedReader(new FileReader(logFile));
    reader.lines().forEach(s -> remainingMatches.removeIf(s::contains));
    assertThat(remainingMatches).as("Not all expected options have been observed in log").isEmpty();
  }
}
