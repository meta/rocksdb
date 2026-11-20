// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

public class OptionsUtil {
  /**
   * A static method to construct the DBOptions and ColumnFamilyDescriptors by
   * loading the latest RocksDB options file stored in the specified rocksdb
   * database.
   * <p>
   * Note that the all the pointer options (except table_factory, which will
   * be described in more details below) will be initialized with the default
   * values.  Developers can further initialize them after this function call.
   * Below is an example list of pointer options which will be initialized.
   * <p>
   * - env
   * - memtable_factory
   * - compaction_filter_factory
   * - prefix_extractor
   * - comparator
   * - merge_operator
   * - compaction_filter
   * <p>
   * For table_factory, this function further supports deserializing
   * BlockBasedTableFactory and its BlockBasedTableOptions except the
   * pointer options of BlockBasedTableOptions (flush_block_policy_factory,
   * and block_cache), which will be initialized with
   * default values.  Developers can further specify these three options by
   * casting the return value of TableFactoroy::GetOptions() to
   * BlockBasedTableOptions and making necessary changes.
   *
   * @param dbPath the path to the RocksDB.
   * @param configOptions {@link org.rocksdb.ConfigOptions} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadLatestOptions(final ConfigOptions configOptions, final String dbPath,
      final DBOptions dbOptions, final List<ColumnFamilyDescriptor> cfDescs)
      throws RocksDBException {
    loadLatestOptions(configOptions.nativeHandle_, dbPath, dbOptions.nativeHandle_, cfDescs);
    loadTableFormatConfig(cfDescs);
  }

  /**
   * Similar to LoadLatestOptions, this function constructs the DBOptions
   * and ColumnFamilyDescriptors based on the specified RocksDB Options file.
   * See LoadLatestOptions above.
   *
   * @param optionsFileName the RocksDB options file path.
   * @param configOptions {@link org.rocksdb.ConfigOptions} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(final ConfigOptions configOptions,
      final String optionsFileName, final DBOptions dbOptions,
      final List<ColumnFamilyDescriptor> cfDescs) throws RocksDBException {
    loadOptionsFromFile(
        configOptions.nativeHandle_, optionsFileName, dbOptions.nativeHandle_, cfDescs);
    loadTableFormatConfig(cfDescs);
  }

  /**
   * Returns the latest options file name under the specified RocksDB path.
   *
   * @param dbPath the path to the RocksDB.
   * @param env {@link org.rocksdb.Env} instance.
   * @return the latest options file name under the db path.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static String getLatestOptionsFileName(final String dbPath, final Env env)
      throws RocksDBException {
    return getLatestOptionsFileName(dbPath, env.nativeHandle_);
  }

  @SuppressWarnings("PMD.CloseResource")
  private static void loadTableFormatConfig(final List<ColumnFamilyDescriptor> cfDescs) {
    for (final ColumnFamilyDescriptor columnFamilyDescriptor : cfDescs) {
      final ColumnFamilyOptions columnFamilyOptions = columnFamilyDescriptor.getOptions();
      columnFamilyOptions.setFetchedTableFormatConfig(
          readTableFormatConfig(columnFamilyOptions.nativeHandle_));
    }
  }

  /**
   * Private constructor.
   * This class has only static methods and shouldn't be instantiated.
   */
  private OptionsUtil() {}

  // native methods
  private static native void loadLatestOptions(long cfgHandle, String dbPath, long dbOptionsHandle,
      List<ColumnFamilyDescriptor> cfDescs) throws RocksDBException;
  private static native void loadOptionsFromFile(long cfgHandle, String optionsFileName,
      long dbOptionsHandle, List<ColumnFamilyDescriptor> cfDescs) throws RocksDBException;
  private static native String getLatestOptionsFileName(String dbPath, long envHandle)
      throws RocksDBException;

  private static native TableFormatConfig readTableFormatConfig(final long nativeHandle_);
}
