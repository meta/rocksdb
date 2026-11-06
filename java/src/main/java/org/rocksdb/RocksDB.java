// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.rocksdb.util.BufferUtil.CheckBounds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the RocksDB library side and the call failed.
 */
public class RocksDB extends RocksObject {
  public static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes(UTF_8);
  public static final int NOT_FOUND = -1;

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static final AtomicReference<LibraryState> libraryLoaded =
      new AtomicReference<>(LibraryState.NOT_LOADED);

  static final String PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD =
      "Performance optimization for a very specific workload";

  static final String BB_ALL_DIRECT_OR_INDIRECT =
      "ByteBuffer parameters must all be direct, or must all be indirect and not read only";
  private ColumnFamilyHandle defaultColumnFamilyHandle_;
  private final ReadOptions defaultReadOptions_ = new ReadOptions();

  final List<ColumnFamilyHandle> ownedColumnFamilyHandles = new ArrayList<>();

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   * By default the method extracts the shared library for loading at
   * java.io.tmpdir, however, you can override this temporary location by
   * setting the environment variable ROCKSDB_SHAREDLIB_DIR.
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  public static void loadLibrary() {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
      // loading possibly necessary libraries.
      for (final CompressionType compressionType : CompressionType.values()) {
        try {
          if (compressionType.getLibraryName() != null) {
            System.loadLibrary(compressionType.getLibraryName());
          }
        } catch (final UnsatisfiedLinkError e) {
          // since it may be optional, we ignore its loading failure here.
        }
      }
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (final IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the RocksDB shared library",
            e);
      }

      final int encodedVersion = version();
      version = Version.fromEncodedVersion(encodedVersion);

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    waitForLibraryToBeLoaded();
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  public static void loadLibrary(final List<String> paths) {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      for (final CompressionType compressionType : CompressionType.values()) {
        if (compressionType.equals(CompressionType.NO_COMPRESSION)) {
          continue;
        }
        for (final String path : paths) {
          try {
            System.load(path + "/" + Environment.getSharedLibraryFileName(
                compressionType.getLibraryName()));
            break;
          } catch (final UnsatisfiedLinkError e) {
            // since they are optional, we ignore loading fails.
          }
        }
      }
      boolean success = false;
      UnsatisfiedLinkError err = null;
      for (final String path : paths) {
        try {
          System.load(path + "/" +
              Environment.getJniLibraryFileName("rocksdbjni"));
          success = true;
          break;
        } catch (final UnsatisfiedLinkError e) {
          err = e;
        }
      }
      if (!success) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw err;
      }

      final int encodedVersion = version();
      version = Version.fromEncodedVersion(encodedVersion);

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    waitForLibraryToBeLoaded();
  }

  private static void waitForLibraryToBeLoaded() {
    final long wait = 10; // Time to wait before re-checking if another thread loaded the library
    final long timeout =
        10 * 1000; // Maximum time to wait for another thread to load the library (10 seconds)
    long waited = 0;
    try {
      while (libraryLoaded.get() == LibraryState.LOADING) {
        Thread.sleep(wait);
        waited += wait;

        if (waited >= timeout) {
          throw new RuntimeException(
              "Exceeded timeout whilst trying to load the RocksDB shared library");
        }
      }
    } catch (final InterruptedException e) {
      // restore interrupted status
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted whilst trying to load the RocksDB shared library", e);
    }
  }

  public static Version rocksdbVersion() {
    return version;
  }

  public boolean isClosed() {
    return !owningHandle_.get();
  }

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ RocksDB object
   */
  protected RocksDB(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the default options w/ createIfMissing
   * set to true.
   *
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path) throws RocksDBException {
    RocksDB.loadLibrary();
    try (Options options = new Options()) {
      options.setCreateIfMissing(true);
      return open(options, path);
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    try (DBOptions options = new DBOptions()) {
      return open(options, path, columnFamilyDescriptors, columnFamilyHandles);
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path.
   *
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final Options options, final String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(open(options.nativeHandle_, path));
    db.storeOptionsInstance(options);
    db.storeDefaultColumnFamilyHandle(db.makeDefaultColumnFamilyHandle());
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param options {@link org.rocksdb.DBOptions} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    int defaultColumnFamilyIndex = -1;
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = cfDescriptor.getOptions().nativeHandle_;
      if (Arrays.equals(cfDescriptor.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
        defaultColumnFamilyIndex = i;
      }
    }
    if (defaultColumnFamilyIndex < 0) {
      throw new IllegalArgumentException(
          "You must provide the default column family in your columnFamilyDescriptors");
    }

    final long[] handles = open(options.nativeHandle_, path, cfNames,
        cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      final ColumnFamilyHandle columnFamilyHandle = // NOPMD - CloseResource
          new ColumnFamilyHandle(db, handles[i]);
      columnFamilyHandles.add(columnFamilyHandle);
    }

    db.ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    db.storeDefaultColumnFamilyHandle(columnFamilyHandles.get(defaultColumnFamilyIndex));
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path)
      throws RocksDBException {
    RocksDB.loadLibrary();
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    try (Options options = new Options()) {
      return openReadOnly(options, path);
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.
   *
   * @param options {@link Options} instance.
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final Options options, final String path)
      throws RocksDBException {
    return openReadOnly(options, path, false);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.
   *
   * @param options {@link Options} instance.
   * @param path the path to the RocksDB.
   * @param errorIfWalFileExists true to raise an error when opening the db
   *            if a non-empty Write Ahead Log file exists, false otherwise.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final Options options, final String path,
      final boolean errorIfWalFileExists) throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(openROnly(options.nativeHandle_, path, errorIfWalFileExists));
    db.storeOptionsInstance(options);
    db.storeDefaultColumnFamilyHandle(db.makeDefaultColumnFamilyHandle());
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    try (DBOptions options = new DBOptions()) {
      return openReadOnly(options, path, columnFamilyDescriptors, columnFamilyHandles, false);
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * <p>This open method allows to open RocksDB using a subset of available
   * column families</p>
   * <p>Options instance *should* not be disposed before all DBs using this
   * options instance have been closed. If user doesn't call options dispose
   * explicitly,then this options instance will be GC'd automatically.</p>
   *
   * @param options {@link DBOptions} instance.
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    return openReadOnly(options, path, columnFamilyDescriptors, columnFamilyHandles, false);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * <p>This open method allows to open RocksDB using a subset of available
   * column families</p>
   * <p>Options instance *should* not be disposed before all DBs using this
   * options instance have been closed. If user doesn't call options dispose
   * explicitly,then this options instance will be GC'd automatically.</p>
   *
   * @param options {@link DBOptions} instance.
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @param errorIfWalFileExists true to raise an error when opening the db
   *            if a non-empty Write Ahead Log file exists, false otherwise.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles, final boolean errorIfWalFileExists)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    int defaultColumnFamilyIndex = -1;
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = cfDescriptor.getOptions().nativeHandle_;
      if (Arrays.equals(cfDescriptor.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
        defaultColumnFamilyIndex = i;
      }
    }
    if (defaultColumnFamilyIndex < 0) {
      throw new IllegalArgumentException(
          "You must provide the default column family in your columnFamilyDescriptors");
    }

    final long[] handles =
        openROnly(options.nativeHandle_, path, cfNames, cfOptionHandles, errorIfWalFileExists);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      final ColumnFamilyHandle columnFamilyHandle = // NOPMD - CloseResource
          new ColumnFamilyHandle(db, handles[i]);
      columnFamilyHandles.add(columnFamilyHandle);
    }

    db.ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    db.storeDefaultColumnFamilyHandle(columnFamilyHandles.get(defaultColumnFamilyIndex));

    return db;
  }

  /**
   * Open DB as secondary instance with only the default column family.
   * <p>
   * The secondary instance can dynamically tail the MANIFEST of
   * a primary that must have already been created. User can call
   * {@link #tryCatchUpWithPrimary()} to make the secondary instance catch up
   * with primary (WAL tailing is NOT supported now) whenever the user feels
   * necessary. Column families created by the primary after the secondary
   * instance starts are currently ignored by the secondary instance.
   * Column families opened by secondary and dropped by the primary will be
   * dropped by secondary as well. However the user of the secondary instance
   * can still access the data of such dropped column family as long as they
   * do not destroy the corresponding column family handle.
   * WAL tailing is not supported at present, but will arrive soon.
   *
   * @param options the options to open the secondary instance.
   * @param path the path to the primary RocksDB instance.
   * @param secondaryPath points to a directory where the secondary instance
   *    stores its info log
   *
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openAsSecondary(final Options options, final String path,
      final String secondaryPath) throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(openAsSecondary(options.nativeHandle_, path, secondaryPath));
    db.storeOptionsInstance(options);
    db.storeDefaultColumnFamilyHandle(db.makeDefaultColumnFamilyHandle());
    return db;
  }

  /**
   * Open DB as secondary instance with column families.
   * You can open a subset of column families in secondary mode.
   * <p>
   * The secondary instance can dynamically tail the MANIFEST of
   * a primary that must have already been created. User can call
   * {@link #tryCatchUpWithPrimary()} to make the secondary instance catch up
   * with primary (WAL tailing is NOT supported now) whenever the user feels
   * necessary. Column families created by the primary after the secondary
   * instance starts are currently ignored by the secondary instance.
   * Column families opened by secondary and dropped by the primary will be
   * dropped by secondary as well. However the user of the secondary instance
   * can still access the data of such dropped column family as long as they
   * do not destroy the corresponding column family handle.
   * WAL tailing is not supported at present, but will arrive soon.
   *
   * @param options the options to open the secondary instance.
   * @param path the path to the primary RocksDB instance.
   * @param secondaryPath points to a directory where the secondary instance
   *    stores its info log.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   *
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openAsSecondary(final DBOptions options, final String path,
      final String secondaryPath, final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors.get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = cfDescriptor.getOptions().nativeHandle_;
    }

    final long[] handles =
        openAsSecondary(options.nativeHandle_, path, secondaryPath, cfNames, cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      final ColumnFamilyHandle columnFamilyHandle = // NOPMD - CloseResource
          new ColumnFamilyHandle(db, handles[i]);
      columnFamilyHandles.add(columnFamilyHandle);
    }

    db.ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    db.storeDefaultColumnFamilyHandle(db.makeDefaultColumnFamilyHandle());

    return db;
  }

  /**
   * This is similar to {@link #close()} except that it
   * throws an exception if any error occurs.
   * <p>
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   * <p>
   * See also {@link #close()}.
   *
   * @throws RocksDBException if an error occurs whilst closing.
   */
  public void closeE() throws RocksDBException {
    for (final ColumnFamilyHandle columnFamilyHandle : // NOPMD - CloseResource
        ownedColumnFamilyHandles) {
      columnFamilyHandle.close();
    }
    ownedColumnFamilyHandles.clear();

    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } finally {
        disposeInternal();
      }
    }
  }

  /**
   * This is similar to {@link #closeE()} except that it
   * silently ignores any errors.
   * <p>
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   * <p>
   * See also {@link #close()}.
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  @Override
  public void close() {
    for (final ColumnFamilyHandle columnFamilyHandle : // NOPMD - CloseResource
        ownedColumnFamilyHandles) {
      columnFamilyHandle.close();
    }
    ownedColumnFamilyHandles.clear();

    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } catch (final RocksDBException e) {
        // silently ignore the error report
      } finally {
        disposeInternal();
      }
    }
  }

  /**
   * Static method to determine all available column families for a
   * rocksdb database identified by path
   *
   * @param options Options for opening the database
   * @param path Absolute path to rocksdb database
   * @return List&lt;byte[]&gt; List containing the column family names
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static List<byte[]> listColumnFamilies(final Options options,
      final String path) throws RocksDBException {
    return Arrays.asList(RocksDB.listColumnFamilies(options.nativeHandle_,
        path));
  }

  /**
   * Creates a new column family with the name columnFamilyName and
   * allocates a ColumnFamilyHandle within an internal structure.
   * The ColumnFamilyHandle is automatically disposed with DB disposal.
   *
   * @param columnFamilyDescriptor column family to be created.
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public ColumnFamilyHandle createColumnFamily(
      final ColumnFamilyDescriptor columnFamilyDescriptor)
      throws RocksDBException {
    final ColumnFamilyHandle columnFamilyHandle = new ColumnFamilyHandle(this,
        createColumnFamily(nativeHandle_, columnFamilyDescriptor.getName(),
            columnFamilyDescriptor.getName().length,
            columnFamilyDescriptor.getOptions().nativeHandle_));
    ownedColumnFamilyHandles.add(columnFamilyHandle);
    return columnFamilyHandle;
  }

  /**
   * Bulk create column families with the same column family options.
   *
   * @param columnFamilyOptions the options for the column families.
   * @param columnFamilyNames the names of the column families.
   *
   * @return the handles to the newly created column families.
   *
   * @throws RocksDBException if an error occurs whilst creating
   *     the column families
   */
  public List<ColumnFamilyHandle> createColumnFamilies(
      final ColumnFamilyOptions columnFamilyOptions,
      final List<byte[]> columnFamilyNames) throws RocksDBException {
    final byte[][] cfNames = columnFamilyNames.toArray(
        new byte[0][]);
    final long[] cfHandles = createColumnFamilies(nativeHandle_,
        columnFamilyOptions.nativeHandle_, cfNames);
    final List<ColumnFamilyHandle> columnFamilyHandles =
        new ArrayList<>(cfHandles.length);
    for (final long cfHandle : cfHandles) {
      final ColumnFamilyHandle columnFamilyHandle = new ColumnFamilyHandle(this, cfHandle); // NOPMD
      columnFamilyHandles.add(columnFamilyHandle);
    }
    ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    return columnFamilyHandles;
  }

  /**
   * Bulk create column families with the same column family options.
   *
   * @param columnFamilyDescriptors the descriptions of the column families.
   *
   * @return the handles to the newly created column families.
   *
   * @throws RocksDBException if an error occurs whilst creating
   *     the column families
   */
  public List<ColumnFamilyHandle> createColumnFamilies(
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors)
      throws RocksDBException {
    final long[] cfOptsHandles = new long[columnFamilyDescriptors.size()];
    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor columnFamilyDescriptor
          = columnFamilyDescriptors.get(i);
      cfOptsHandles[i] = columnFamilyDescriptor.getOptions().nativeHandle_;
      cfNames[i] = columnFamilyDescriptor.getName();
    }
    final long[] cfHandles = createColumnFamilies(nativeHandle_,
        cfOptsHandles, cfNames);
    final List<ColumnFamilyHandle> columnFamilyHandles =
        new ArrayList<>(cfHandles.length);
    for (final long cfHandle : cfHandles) {
      final ColumnFamilyHandle columnFamilyHandle = new ColumnFamilyHandle(this, cfHandle); // NOPMD
      columnFamilyHandles.add(columnFamilyHandle);
    }
    ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    return columnFamilyHandles;
  }

  /**
   * Creates a new column family with the name columnFamilyName and
   * import external SST files specified in `metadata` allocates a
   * ColumnFamilyHandle within an internal structure.
   * The ColumnFamilyHandle is automatically disposed with DB disposal.
   *
   * @param columnFamilyDescriptor column family to be created.
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public ColumnFamilyHandle createColumnFamilyWithImport(
      final ColumnFamilyDescriptor columnFamilyDescriptor,
      final ImportColumnFamilyOptions importColumnFamilyOptions,
      final ExportImportFilesMetaData metadata) throws RocksDBException {
    List<ExportImportFilesMetaData> metadatas = new ArrayList<>();
    metadatas.add(metadata);
    return createColumnFamilyWithImport(
        columnFamilyDescriptor, importColumnFamilyOptions, metadatas);
  }

  public ColumnFamilyHandle createColumnFamilyWithImport(
      final ColumnFamilyDescriptor columnFamilyDescriptor,
      final ImportColumnFamilyOptions importColumnFamilyOptions,
      final List<ExportImportFilesMetaData> metadatas) throws RocksDBException {
    final int metadataNum = metadatas.size();
    final long[] metadataHandleList = new long[metadataNum];
    for (int i = 0; i < metadataNum; i++) {
      metadataHandleList[i] = metadatas.get(i).getNativeHandle();
    }
    final ColumnFamilyHandle columnFamilyHandle = new ColumnFamilyHandle(this,
        createColumnFamilyWithImport(nativeHandle_, columnFamilyDescriptor.getName(),
            columnFamilyDescriptor.getName().length,
            columnFamilyDescriptor.getOptions().nativeHandle_,
            importColumnFamilyOptions.nativeHandle_, metadataHandleList));
    ownedColumnFamilyHandles.add(columnFamilyHandle);
    return columnFamilyHandle;
  }

  /**
   * Drops the column family specified by {@code columnFamilyHandle}. This call
   * only records a drop record in the manifest and prevents the column
   * family from flushing and compacting.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void dropColumnFamily(final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    dropColumnFamily(nativeHandle_, columnFamilyHandle.nativeHandle_);
  }

  // Bulk drop column families. This call only records drop records in the
  // manifest and prevents the column families from flushing and compacting.
  // In case of error, the request may succeed partially. User may call
  // ListColumnFamilies to check the result.
  public void dropColumnFamilies(
      final List<ColumnFamilyHandle> columnFamilies) throws RocksDBException {
    final long[] cfHandles = new long[columnFamilies.size()];
    for (int i = 0; i < columnFamilies.size(); i++) {
      cfHandles[i] = columnFamilies.get(i).nativeHandle_;
    }
    dropColumnFamilies(nativeHandle_, cfHandles);
  }

  /**
   * Deletes native column family handle of given {@link ColumnFamilyHandle} Java object
   * and removes reference from {@link RocksDB#ownedColumnFamilyHandles}.
   *
   * @param columnFamilyHandle column family handle object.
   */
  public void destroyColumnFamilyHandle(final ColumnFamilyHandle columnFamilyHandle) {
    for (int i = 0; i < ownedColumnFamilyHandles.size(); ++i) {
      final ColumnFamilyHandle ownedHandle = ownedColumnFamilyHandles.get(i); // NOPMD
      if (ownedHandle.equals(columnFamilyHandle)) {
        columnFamilyHandle.close();
        ownedColumnFamilyHandles.remove(i);
        return;
      }
    }
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final byte[] key, final byte[] value)
      throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if errors happens in underlying native
   *     library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    put(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Set the database entry for "key" to "value" in the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   * <p>
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value" in the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must
   *     be non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length - offset)
   *
   * @throws RocksDBException thrown if errors happens in underlying native
   *     library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    put(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   * <p>
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see IllegalArgumentException
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted. Position and limit is used.
   *     Supports direct buffer only.
   * @param value the value associated with the specified key. Position and limit is used.
   *     Supports direct buffer only.
   * <p>
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see IllegalArgumentException
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts,
      final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, writeOpts.nativeHandle_, key, key.position(), key.remaining(), value,
          value.position(), value.remaining(), columnFamilyHandle.nativeHandle_);
    } else if (key.hasArray() && value.hasArray()) {
      put(nativeHandle_, writeOpts.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
          key.remaining(), value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted. Position and limit is used.
   *     Supports direct buffer only.
   * @param value the value associated with the specified key. Position and limit is used.
   *     Supports direct buffer only.
   * <p>
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see IllegalArgumentException
   */
  public void put(final WriteOptions writeOpts, final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, writeOpts.nativeHandle_, key, key.position(), key.remaining(), value,
          value.position(), value.remaining(), 0);
    } else if (key.hasArray() && value.hasArray()) {
      put(nativeHandle_, writeOpts.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
          key.remaining(), value.array(), value.arrayOffset() + value.position(),
          value.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    put(nativeHandle_, writeOpts.nativeHandle_, key, offset, len, value,
        vOffset, vLen, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *      non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *      non-negative and no larger than ("key".length - offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key, final int offset, final int len)
      throws RocksDBException {
    delete(nativeHandle_, key, offset, len);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used,
   *     must be non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("value".length - offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len)
      throws RocksDBException {
    delete(nativeHandle_, key, offset, len, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and no larger than ("key".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final byte[] key,
      final int offset, final int len) throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, offset, len);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and no larger than ("key".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key, final int offset,
      final int len)  throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value. It is using position and limit.
   *     Supports direct buffer only.
   * @param value the out-value to receive the retrieved value.
   *     It is using position and limit. Limit is set according to value size.
   *     Supports direct buffer only.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    final int result;
    if (key.isDirect() && value.isDirect()) {
      result = getDirect(nativeHandle_, opt.nativeHandle_, key, key.position(), key.remaining(),
          value, value.position(), value.remaining(), 0);
    } else if (key.hasArray() && value.hasArray()) {
      result =
          get(nativeHandle_, opt.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
              key.remaining(), value.array(), value.arrayOffset() + value.position(),
              value.remaining(), defaultColumnFamilyHandle_.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    if (result != NOT_FOUND) {
      value.limit(Math.min(value.limit(), value.position() + result));
    }
    key.position(key.limit());
    return result;
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value. It is using position and limit.
   *     Supports direct buffer only.
   * @param value the out-value to receive the retrieved value.
   *     It is using position and limit. Limit is set according to value size.
   *     Supports direct buffer only.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final ReadOptions opt,
      final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      final int result =
          getDirect(nativeHandle_, opt.nativeHandle_, key, key.position(), key.remaining(), value,
              value.position(), value.remaining(), columnFamilyHandle.nativeHandle_);
      if (result != NOT_FOUND) {
        value.position(Math.min(value.limit(), value.position() + result));
      }
      key.position(key.limit());
      return result;
    } else if (key.hasArray() && value.hasArray()) {
      final int result = get(nativeHandle_, opt.nativeHandle_, key.array(),
          key.arrayOffset() + key.position(), key.remaining(), value.array(),
          value.arrayOffset() + value.position(), value.remaining());
      if (result != NOT_FOUND) {
        value.position(Math.min(value.limit(), value.position() + result));
      }
      key.position(key.limit());
      return result;
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   * <p>
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   * <p>
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   * <p>
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   * <p>
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
      throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   * <p>
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   * <p>
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   * <p>
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final WriteOptions writeOpt, final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   * <p>
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   * <p>
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   * <p>
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt,
      final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   * <p>
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final byte[] beginKey, final byte[] endKey)
      throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   * <p>
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] beginKey, final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   * <p>
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void deleteRange(final WriteOptions writeOpt, final byte[] beginKey,
      final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0,
        beginKey.length, endKey, 0, endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   * <p>
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param beginKey First key to delete within database (included)
   * @param endKey Last key to delete within database (excluded)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] beginKey, final byte[] endKey)
      throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0,
        beginKey.length, endKey, 0, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }


  /**
   * Add merge operand for key/value pair.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for the
   *     specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for the
   *     specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and must be non-negative and no larger than
   *     ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final byte[] key, final int offset, final int len, final byte[] value,
      final int vOffset, final int vLen) throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Add merge operand for key/value pair in a ColumnFamily.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair in a ColumnFamily.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     must be non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len, final byte[] value,
      final int vOffset, final int vLen) throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("value".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final WriteOptions writeOpts,
      final byte[] key,  final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  public void merge(final WriteOptions writeOpts, final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      mergeDirect(nativeHandle_, writeOpts.nativeHandle_, key, key.position(), key.remaining(),
          value, value.position(), value.remaining(), 0);
    } else if (key.hasArray() && value.hasArray()) {
      merge(nativeHandle_, writeOpts.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
          key.remaining(), value.array(), value.arrayOffset() + value.position(),
          value.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  public void merge(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts,
      final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      mergeDirect(nativeHandle_, writeOpts.nativeHandle_, key, key.position(), key.remaining(),
          value, value.position(), value.remaining(), columnFamilyHandle.nativeHandle_);
    } else if (key.hasArray() && value.hasArray()) {
      merge(nativeHandle_, writeOpts.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
          key.remaining(), value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database. It is using position and limit.
   *     Supports direct buffer only.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final ByteBuffer key) throws RocksDBException {
    if (key.isDirect()) {
      deleteDirect(nativeHandle_, writeOpt.nativeHandle_, key, key.position(), key.remaining(), 0);
    } else if (key.hasArray()) {
      assert key.hasArray();
      delete(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database. It is using position and limit.
   *     Supports direct buffer only.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt,
      final ByteBuffer key) throws RocksDBException {
    if (key.isDirect()) {
      deleteDirect(nativeHandle_, writeOpt.nativeHandle_, key, key.position(), key.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else if (key.hasArray()) {
      delete(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for the
   *     specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(
      final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatch instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts, final WriteBatch updates)
      throws RocksDBException {
    write0(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatchWithIndex instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts,
      final WriteBatchWithIndex updates) throws RocksDBException {
    write1(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  // TODO(AR) we should improve the #get() API, returning -1 (RocksDB.NOT_FOUND) is not very nice
  // when we could communicate better status into, also the C++ code show that -2 could be returned

  /**
   * Get the value associated with the specified key within column family*
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final byte[] key, final byte[] value) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Get the value associated with the specified key within column family*
   *
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "value".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and and no larger than ("value".length -  offset)
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value) throws RocksDBException, IllegalArgumentException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     an no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final int offset, final int len, final byte[] value, final int vOffset,
      final int vLen) throws RocksDBException, IllegalArgumentException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final byte[] key,
      final byte[] value) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_,
               key, 0, key.length, value, 0, value.length);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final byte[] key, final int offset,
      final int len, final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, opt.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final byte[] value)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, and must be
   *     non-negative and no larger than ("value".length -  offset)
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    CheckBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len, value,
        vOffset, vLen, columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final byte[] key, final int offset,
      final int len) throws RocksDBException {
    CheckBounds(offset, len, key.length);
    return get(nativeHandle_, key, offset, len);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    return get(nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ReadOptions opt, final byte[] key)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ReadOptions opt, final byte[] key, final int offset,
      final int len) throws RocksDBException {
    CheckBounds(offset, len, key.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final int offset, final int len)
      throws RocksDBException {
    CheckBounds(offset, len, key.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Returns the snapshot-visible value together with timestamp and
   * newer-version metadata.
   *
   * @param opt read options
   * @param key key to read
   * @return metadata result; its value is null when the key is not found
   * @throws RocksDBException if the native read fails
   */
  public GetWithMetadataResult getWithMetadata(final ReadOptions opt, final byte[] key)
      throws RocksDBException {
    return getWithMetadata(nativeHandle_, opt.nativeHandle_, key, 0, key.length, 0);
  }

  /**
   * Returns metadata for a key in the specified column family.
   *
   * @param columnFamilyHandle column family containing the key
   * @param opt read options
   * @param key key to read
   * @return metadata result; its value is null when the key is not found
   * @throws RocksDBException if the native read fails
   */
  public GetWithMetadataResult getWithMetadata(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key) throws RocksDBException {
    return getWithMetadata(
        nativeHandle_, opt.nativeHandle_, key, 0, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Takes a list of keys, and returns a list of values for the given list of
   * keys. List will contain null for keys which could not be found.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final List<byte[]> keys)
      throws RocksDBException {
    assert (!keys.isEmpty());

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys List of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public List<byte[]> multiGetAsList(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException,
      IllegalArgumentException {
    assert (!keys.isEmpty());
    // Check if key size equals cfList size. If not an exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandleList.size()) {
        throw new IllegalArgumentException(
            "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths, cfHandles));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @param opt Read options.
   * @param keys of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final ReadOptions opt,
      final List<byte[]> keys) throws RocksDBException {
    assert (!keys.isEmpty());

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths));
  }

  /**
   * Returns values and metadata for the supplied keys.
   *
   * @param opt read options
   * @param keys keys to read
   * @return results in input order
   * @throws RocksDBException if a native read fails
   */
  public List<GetWithMetadataResult> multiGetWithMetadata(
      final ReadOptions opt, final List<byte[]> keys) throws RocksDBException {
    assert (!keys.isEmpty());
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for (int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }
    return Arrays.asList(multiGetWithMetadata(
        nativeHandle_, opt.nativeHandle_, keysArray, keyOffsets, keyLengths, null));
  }

  /**
   * Returns values and metadata for keys in the supplied column families.
   *
   * @param opt read options
   * @param columnFamilyHandles one column family for each key
   * @param keys keys to read
   * @return results in input order
   * @throws RocksDBException if a native read fails
   */
  public List<GetWithMetadataResult> multiGetWithMetadata(final ReadOptions opt,
      final List<ColumnFamilyHandle> columnFamilyHandles, final List<byte[]> keys)
      throws RocksDBException {
    if (keys.size() != columnFamilyHandles.size()) {
      throw new IllegalArgumentException("For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandles.size()];
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      cfHandles[i] = columnFamilyHandles.get(i).nativeHandle_;
    }
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for (int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }
    return Arrays.asList(multiGetWithMetadata(
        nativeHandle_, opt.nativeHandle_, keysArray, keyOffsets, keyLengths, cfHandles));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param opt Read options.
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public List<byte[]> multiGetAsList(final ReadOptions opt,
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException {
    assert (!keys.isEmpty());
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()){
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int[] keyOffsets = new int[keysArray.length];
    final int[] keyLengths = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths, cfHandles));
  }

  /**
   * Fetches a list of values for the given list of keys, all from the default column family.
   *
   * @param keys list of keys for which values need to be retrieved.
   * @param values list of buffers to return retrieved values in
   * @return list of number of bytes in DB for each requested key
   * this can be more than the size of the corresponding buffer; then the buffer will be filled
   * with the appropriate truncation of the database value.
   * @throws RocksDBException if error happens in underlying native library.
   * @throws IllegalArgumentException thrown if the number of passed keys and passed values
   * do not match.
   */
  public List<ByteBufferGetStatus> multiGetByteBuffers(
      final List<ByteBuffer> keys, final List<ByteBuffer> values) throws RocksDBException {
    try (ReadOptions readOptions = new ReadOptions()) {
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>(1);
      columnFamilyHandleList.add(getDefaultColumnFamily());
      return multiGetByteBuffers(readOptions, columnFamilyHandleList, keys, values);
    }
  }

  /**
   * Fetches a list of values for the given list of keys, all from the default column family.
   *
   * @param readOptions Read options
   * @param keys list of keys for which values need to be retrieved.
   * @param values list of buffers to return retrieved values in
   * @throws RocksDBException if error happens in underlying native library.
   * @throws IllegalArgumentException thrown if the number of passed keys and passed values
   * do not match.
   * @return the list of values for the given list of keys
   */
  public List<ByteBufferGetStatus> multiGetByteBuffers(final ReadOptions readOptions,
      final List<ByteBuffer> keys, final List<ByteBuffer> values) throws RocksDBException {
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>(1);
    columnFamilyHandleList.add(getDefaultColumnFamily());
    return multiGetByteBuffers(readOptions, columnFamilyHandleList, keys, values);
  }

  /**
   * Fetches a list of values for the given list of keys.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   * {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys list of keys for which values need to be retrieved.
   * @param values list of buffers to return retrieved values in
   * @throws RocksDBException if error happens in underlying native library.
   * @throws IllegalArgumentException thrown if the number of passed keys, passed values and
   * passed column family handles do not match.
   * @return the list of values for the given list of keys
   */
  public List<ByteBufferGetStatus> multiGetByteBuffers(
      final List<ColumnFamilyHandle> columnFamilyHandleList, final List<ByteBuffer> keys,
      final List<ByteBuffer> values) throws RocksDBException {
    try (ReadOptions readOptions = new ReadOptions()) {
      return multiGetByteBuffers(readOptions, columnFamilyHandleList, keys, values);
    }
  }

  /**
   * Fetches a list of values for the given list of keys.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param readOptions Read options
   * @param columnFamilyHandleList {@link java.util.List} containing
   * {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys list of keys for which values need to be retrieved.
   * @param values list of buffers to return retrieved values in
   * @throws RocksDBException if error happens in underlying native library.
   * @throws IllegalArgumentException thrown if the number of passed keys, passed values and
   * passed column family handles do not match.
   * @return the list of values for the given list of keys
   */
  public List<ByteBufferGetStatus> multiGetByteBuffers(final ReadOptions readOptions,
      final List<ColumnFamilyHandle> columnFamilyHandleList, final List<ByteBuffer> keys,
      final List<ByteBuffer> values) throws RocksDBException {
    assert (!keys.isEmpty());

    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandleList.size() && columnFamilyHandleList.size() > 1) {
      throw new IllegalArgumentException(
          "Wrong number of ColumnFamilyHandle(s) supplied. Provide 0, 1, or as many as there are key/value(s)");
    }

    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (values.size() != keys.size()) {
      throw new IllegalArgumentException("For each key there must be a corresponding value. "
          + keys.size() + " keys were supplied, but " + values.size() + " values were supplied.");
    }

    // TODO (AP) support indirect buffers
    for (final ByteBuffer key : keys) {
      if (!key.isDirect()) {
        throw new IllegalArgumentException("All key buffers must be direct byte buffers");
      }
    }

    // TODO (AP) support indirect buffers, though probably via a less efficient code path
    for (final ByteBuffer value : values) {
      if (!value.isDirect()) {
        throw new IllegalArgumentException("All value buffers must be direct byte buffers");
      }
    }

    final int numCFHandles = columnFamilyHandleList.size();
    final long[] cfHandles = new long[numCFHandles];
    for (int i = 0; i < numCFHandles; i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final int numValues = keys.size();

    final ByteBuffer[] keysArray = keys.toArray(new ByteBuffer[0]);
    final int[] keyOffsets = new int[numValues];
    final int[] keyLengths = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      // TODO (AP) add keysArray[i].arrayOffset() if the buffer is indirect
      // TODO (AP) because in that case we have to pass the array directly,
      // so that the JNI C++ code will not know to compensate for the array offset
      keyOffsets[i] = keysArray[i].position();
      keyLengths[i] = keysArray[i].limit();
    }
    final ByteBuffer[] valuesArray = values.toArray(new ByteBuffer[0]);
    final int[] valuesSizeArray = new int[numValues];
    final Status[] statusArray = new Status[numValues];

    multiGet(nativeHandle_, readOptions.nativeHandle_, cfHandles, keysArray, keyOffsets, keyLengths,
        valuesArray, valuesSizeArray, statusArray);

    final List<ByteBufferGetStatus> results = new ArrayList<>();
    for (int i = 0; i < numValues; i++) {
      final Status status = statusArray[i];
      if (status.getCode() == Status.Code.Ok) {
        final ByteBuffer value = valuesArray[i];
        value.position(Math.min(valuesSizeArray[i], value.capacity()));
        value.flip(); // prepare for read out
        results.add(new ByteBufferGetStatus(status, valuesSizeArray[i], value));
      } else if (status.getCode() == Status.Code.Incomplete) {
        assert valuesSizeArray[i] == -1;
        final ByteBuffer value = valuesArray[i];
        value.position(value.capacity());
        value.flip(); // prepare for read out
        results.add(new ByteBufferGetStatus(status, value.capacity(), value));
      } else {
        results.add(new ByteBufferGetStatus(status));
      }
    }

    return results;
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   * @param key byte array of a key to search for*
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final byte[] key) {
    return keyExists(key, 0, key.length);
  }
  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final byte[] key, final int offset, final int len) {
    return keyExists(null, null, key, offset, len);
  }
  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle, final byte[] key) {
    return keyExists(columnFamilyHandle, key, 0, key.length);
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final int offset, final int len) {
    return keyExists(columnFamilyHandle, null, key, offset, len);
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ReadOptions readOptions, final byte[] key) {
    return keyExists(readOptions, key, 0, key.length);
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(
      final ReadOptions readOptions, final byte[] key, final int offset, final int len) {
    return keyExists(null, readOptions, key, offset, len);
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final byte[] key) {
    return keyExists(columnFamilyHandle, readOptions, key, 0, key.length);
  }

  /**
   *  Check if a key exists in the database.
   *  This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   *  of a correct result, whether the key exists or not.
   *
   *  Internally it checks if the key may exist and then double checks with read operation
   *  that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   *  a false positive.
   *
   *  The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final byte[] key, final int offset, final int len) {
    checkBounds(offset, len, key.length);
    return keyExists(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        readOptions == null ? 0 : readOptions.nativeHandle_, key, offset, len);
  }

  /**
   * Check if a key exists in the database.
   * This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   * of a correct result, whether the key exists or not.
   *
   * Internally it checks if the key may exist and then double checks with read operation
   * that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   * a false positive.
   *
   * The code crosses the Java/JNI boundary only once.
   *
   * @param key ByteBuffer with key. Must be allocated as direct.
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ByteBuffer key) throws RocksDBException {
    return keyExists(null, null, key);
  }

  /**
   * Check if a key exists in the database.
   * This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   * of a correct result, whether the key exists or not.
   *
   * Internally it checks if the key may exist and then double checks with read operation
   * that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   * a false positive.
   *
   * The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key ByteBuffer with key. Must be allocated as direct.
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key)
      throws RocksDBException {
    return keyExists(columnFamilyHandle, null, key);
  }

  /**
   * Check if a key exists in the database.
   * This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   * of a correct result, whether the key exists or not.
   *
   * Internally it checks if the key may exist and then double checks with read operation
   * that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   * a false positive.
   *
   * The code crosses the Java/JNI boundary only once.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key ByteBuffer with key. Must be allocated as direct.
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ReadOptions readOptions, final ByteBuffer key)
      throws RocksDBException {
    return keyExists(null, readOptions, key);
  }

  /**
   * Check if a key exists in the database.
   * This method is not as lightweight as {@code keyMayExist} but it gives a 100% guarantee
   * of a correct result, whether the key exists or not.
   *
   * Internally it checks if the key may exist and then double checks with read operation
   * that confirms the key exists. This deals with the case where {@code keyMayExist} may return
   * a false positive.
   *
   * The code crosses the Java/JNI boundary only once.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param readOptions {@link ReadOptions} instance
   * @param key ByteBuffer with key. Must be allocated as direct.
   * @return true if key exist in database, otherwise false.
   */
  public boolean keyExists(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final ByteBuffer key) throws RocksDBException {
    assert key != null : "key ByteBuffer parameter cannot be null";

    boolean result;

    if (key.isDirect()) {
      result = keyExistsDirect(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_, key, key.position(), key.limit());

    } else if (key.hasArray()) {
      result = keyExists(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_, key.array(),
          key.arrayOffset() + key.position(), key.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    return result;
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(byte[])}. One way to make this lighter weight is to avoid
   * doing any IOs.
   *
   * @param key byte array of a key to search for
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final byte[] key,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(key, 0, key.length, valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(byte[], int, int)}. One way to make this lighter weight is to
   * avoid doing any IOs.
   *
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final byte[] key,
      final int offset, final int len,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist((ColumnFamilyHandle)null, key, offset, len, valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ColumnFamilyHandle,byte[])}. One way to make this lighter
   * weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(columnFamilyHandle, key, 0, key.length,
        valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ColumnFamilyHandle, byte[], int, int)}. One way to make this
   * lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final int offset, final int len,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(columnFamilyHandle, null, key, offset, len,
        valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a true negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ReadOptions, byte[])}. One way to make this
   * lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(
      final ReadOptions readOptions, final byte[] key,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(readOptions, key, 0, key.length,
        valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a true negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ReadOptions, byte[], int, int)}. One way to make this
   * lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(
      final ReadOptions readOptions,
      final byte[] key, final int offset, final int len,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(null, readOptions,
        key, offset, len, valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a true negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ColumnFamilyHandle, ReadOptions, byte[])}. One way to make this
   * lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(
      final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final byte[] key,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    return keyMayExist(columnFamilyHandle, readOptions,
        key, 0, key.length, valueHolder);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   * <p>
   * If the caller wants to obtain value when the key
   * is found in memory, then {@code valueHolder} must be set.
   * <p>
   * This check is potentially lighter-weight than invoking
   * {@link #get(ColumnFamilyHandle, ReadOptions, byte[], int, int)}.
   * One way to make this lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param valueHolder non-null to retrieve the value if it is found, or null
   *     if the value is not needed. If non-null, upon return of the function,
   *     the {@code value} will be set if it could be retrieved.
   *
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(
      final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions,
      final byte[] key, final int offset, final int len,
      /* @Nullable */ final Holder<byte[]> valueHolder) {
    CheckBounds(offset, len, key.length);
    if (valueHolder == null) {
      return keyMayExist(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_,
          key, offset, len);
    } else {
      final byte[][] result = keyMayExistFoundValue(
          nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_,
          key, offset, len);
      if (result[0][0] == 0x0) {
        valueHolder.setValue(null);
        return false;
      } else if (result[0][0] == 0x1) {
        valueHolder.setValue(null);
        return true;
      } else {
        valueHolder.setValue(result[1]);
        return true;
      }
    }
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   *
   * @param key bytebuffer containing the value of the key
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final ByteBuffer key) throws RocksDBException {
    return keyMayExist(null, (ReadOptions) null, key);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   *
   * @param columnFamilyHandle the {@link ColumnFamilyHandle} to look for the key in
   * @param key bytebuffer containing the value of the key
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key)
      throws RocksDBException {
    return keyMayExist(columnFamilyHandle, (ReadOptions) null, key);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   *
   * @param readOptions the {@link ReadOptions} to use when reading the key/value
   * @param key bytebuffer containing the value of the key
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final ReadOptions readOptions, final ByteBuffer key)
      throws RocksDBException {
    return keyMayExist(null, readOptions, key);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns {@link KeyMayExist.KeyMayExistEnum#kNotExist},
   * otherwise if it can with best effort retreive the value, it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithValue} otherwise it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithoutValue}. The choice not to return a value which might
   * exist is at the discretion of the implementation; the only guarantee is that {@link
   * KeyMayExist.KeyMayExistEnum#kNotExist} is an assurance that the key does not exist.
   *
   * @param key bytebuffer containing the value of the key
   * @param value bytebuffer which will receive a value if the key exists and a value is known
   * @return a {@link KeyMayExist} object reporting if key may exist and if a value is provided
   */
  public KeyMayExist keyMayExist(final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    return keyMayExist(null, null, key, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns {@link KeyMayExist.KeyMayExistEnum#kNotExist},
   * otherwise if it can with best effort retreive the value, it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithValue} otherwise it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithoutValue}. The choice not to return a value which might
   * exist is at the discretion of the implementation; the only guarantee is that {@link
   * KeyMayExist.KeyMayExistEnum#kNotExist} is an assurance that the key does not exist.
   *
   * @param columnFamilyHandle the {@link ColumnFamilyHandle} to look for the key in
   * @param key bytebuffer containing the value of the key
   * @param value bytebuffer which will receive a value if the key exists and a value is known
   * @return a {@link KeyMayExist} object reporting if key may exist and if a value is provided
   */
  public KeyMayExist keyMayExist(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    return keyMayExist(columnFamilyHandle, null, key, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns {@link KeyMayExist.KeyMayExistEnum#kNotExist},
   * otherwise if it can with best effort retreive the value, it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithValue} otherwise it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithoutValue}. The choice not to return a value which might
   * exist is at the discretion of the implementation; the only guarantee is that {@link
   * KeyMayExist.KeyMayExistEnum#kNotExist} is an assurance that the key does not exist.
   *
   * @param readOptions the {@link ReadOptions} to use when reading the key/value
   * @param key bytebuffer containing the value of the key
   * @param value bytebuffer which will receive a value if the key exists and a value is known
   * @return a {@link KeyMayExist} object reporting if key may exist and if a value is provided
   */
  public KeyMayExist keyMayExist(final ReadOptions readOptions, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    return keyMayExist(null, readOptions, key, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, otherwise it returns true if the key might exist.
   * That is to say that this method is probabilistic and may return false
   * positives, but never a false negative.
   *
   * @param columnFamilyHandle the {@link ColumnFamilyHandle} to look for the key in
   * @param readOptions the {@link ReadOptions} to use when reading the key/value
   * @param key bytebuffer containing the value of the key
   * @return false if the key definitely does not exist in the database,
   *     otherwise true.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final ByteBuffer key) throws RocksDBException {
    if (key.isDirect()) {
      final boolean result = keyMayExistDirect(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_, key, key.position(), key.limit());
      key.position(key.limit());
      return result;
    } else if (key.hasArray()) {
      final boolean result = keyMayExist(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_, key.array(),
          key.arrayOffset() + key.position(), key.limit());
      key.position(key.limit());
      return result;
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns {@link KeyMayExist.KeyMayExistEnum#kNotExist},
   * otherwise if it can with best effort retreive the value, it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithValue} otherwise it returns {@link
   * KeyMayExist.KeyMayExistEnum#kExistsWithoutValue}. The choice not to return a value which might
   * exist is at the discretion of the implementation; the only guarantee is that {@link
   * KeyMayExist.KeyMayExistEnum#kNotExist} is an assurance that the key does not exist.
   *
   * @param columnFamilyHandle the {@link ColumnFamilyHandle} to look for the key in
   * @param readOptions the {@link ReadOptions} to use when reading the key/value
   * @param key bytebuffer containing the value of the key
   * @param value bytebuffer which will receive a value if the key exists and a value is known
   * @return a {@link KeyMayExist} object reporting if key may exist and if a value is provided
   */
  public KeyMayExist keyMayExist(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions, final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    assert key != null : "key ByteBuffer parameter cannot be null";
    assert value
        != null
        : "value ByteBuffer parameter cannot be null. If you do not need the value, use a different version of the method";

    if (key.isDirect()) {
      final int[] result = keyMayExistDirectFoundValue(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
          readOptions == null ? 0 : readOptions.nativeHandle_, key, key.position(), key.remaining(),
          value, value.position(), value.remaining());
      final int valueLength = result[1];
      value.position(value.position() + Math.min(valueLength, value.remaining()));
      key.position(key.limit());
      return new KeyMayExist(KeyMayExist.KeyMayExistEnum.values()[result[0]], valueLength);
    } else if (key.hasArray() && value.hasArray()) {
      Holder<byte[]> holder = new Holder<>();
      final boolean ret = keyMayExist(columnFamilyHandle, readOptions, key.array(),
          key.arrayOffset() + key.position(), key.remaining(), holder);
      key.position(key.limit());
      if (ret) {
        if (holder.getValue() == null) {
          return new KeyMayExist(KeyMayExist.KeyMayExistEnum.kExistsWithoutValue, 0);
        } else {
          value.put(holder.getValue(), 0, Math.min(holder.getValue().length, value.remaining()));
          return new KeyMayExist(
              KeyMayExist.KeyMayExistEnum.kExistsWithValue, holder.getValue().length);
        }
      } else {
        return new KeyMayExist(KeyMayExist.KeyMayExistEnum.kNotExist, 0);
      }
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @return instance of iterator object.
   */
  public RocksIterator newIterator() {
    return new RocksIterator(this,
        iterator(nativeHandle_, defaultColumnFamilyHandle_.nativeHandle_,
            defaultReadOptions_.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ReadOptions readOptions) {
    return new RocksIterator(this,
        iterator(
            nativeHandle_, defaultColumnFamilyHandle_.nativeHandle_, readOptions.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of a
   * ColumnFamily. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(
      final ColumnFamilyHandle columnFamilyHandle) {
    return new RocksIterator(this,
        iterator(
            nativeHandle_, columnFamilyHandle.nativeHandle_, defaultReadOptions_.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of a
   * ColumnFamily. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions) {
    return new RocksIterator(
        this, iterator(nativeHandle_, columnFamilyHandle.nativeHandle_, readOptions.nativeHandle_));
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList)
      throws RocksDBException {
    return newIterators(columnFamilyHandleList, new ReadOptions());
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param readOptions {@link ReadOptions} instance.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final ReadOptions readOptions) throws RocksDBException {

    final long[] columnFamilyHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      columnFamilyHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final long[] iteratorRefs = iterators(nativeHandle_, columnFamilyHandles,
        readOptions.nativeHandle_);

    final List<RocksIterator> iterators = new ArrayList<>(
        columnFamilyHandleList.size());
    for (int i=0; i<columnFamilyHandleList.size(); i++){
      iterators.add(new RocksIterator(this, iteratorRefs[i]));
    }
    return iterators;
  }


  /**
   * <p>Return a handle to the current DB state. Iterators created with
   * this handle will all observe a stable snapshot of the current DB
   * state. The caller must call ReleaseSnapshot(result) when the
   * snapshot is no longer needed.</p>
   *
   * <p>nullptr will be returned if the DB fails to take a snapshot or does
   * not support snapshot.</p>
   *
   * @return Snapshot {@link Snapshot} instance
   */
  public Snapshot getSnapshot() {
    final long snapshotHandle = getSnapshot(nativeHandle_);
    if (snapshotHandle != 0) {
      return new Snapshot(snapshotHandle);
    }
    return null;
  }

  /**
   * Release a previously acquired snapshot.
   * <p>
   * The caller must not use "snapshot" after this call.
   *
   * @param snapshot {@link Snapshot} instance
   */
  public void releaseSnapshot(final Snapshot snapshot) {
    if (snapshot != null) {
      releaseSnapshot(nativeHandle_, snapshot.nativeHandle_);
    }
  }

  /**
   * DB implements can export properties about their state
   * via this method on a per column family level.
   *
   * <p>If {@code property} is a valid property understood by this DB
   * implementation, fills {@code value} with its current value and
   * returns true. Otherwise returns false.</p>
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * DB implementations can export properties about their state
   * via this method.  If "property" is a valid property understood by this
   * DB implementation, fills "*value" with its current value and returns
   * true.  Otherwise returns false.
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   *</ul>
   *
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(final String property) throws RocksDBException {
    return getProperty(null, property);
  }


  /**
   * Gets a property map.
   *
   * @param property to be fetched.
   *
   * @return the property map
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public Map<String, String> getMapProperty(final String property)
      throws RocksDBException {
    return getMapProperty(null, property);
  }

  /**
   * Gets a property map.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param property to be fetched.
   *
   * @return the property map
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public Map<String, String> getMapProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
                      final String property) throws RocksDBException {
    return getMapProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param property to be fetched.
   *
   * @return numerical property value.
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(final String property) throws RocksDBException {
    return getLongProperty(null, property);
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param property to be fetched.
   *
   * @return numerical property value
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getLongProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * Reset internal stats for DB and all column families.
   * <p>
   * Note this doesn't reset {@link Options#statistics()} as it is not
   * owned by DB.
   *
   * @throws RocksDBException if an error occurs whilst reseting the stats
   */
  public void resetStats() throws RocksDBException {
    resetStats(nativeHandle_);
  }

  /**
   * <p> Return sum of the getLongProperty of all the column families</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param property to be fetched.
   *
   * @return numerical property value
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getAggregatedLongProperty(final String property)
      throws RocksDBException {
    return getAggregatedLongProperty(nativeHandle_, property,
        property.length());
  }

  /**
   * Get the approximate file system space used by keys in each range.
   * <p>
   * Note that the returned sizes measure file system space usage, so
   * if the user data compresses by a factor of ten, the returned
   * sizes will be one-tenth the size of the corresponding user data size.
   * <p>
   * If {@code sizeApproximationFlags} defines whether the returned size
   * should include the recently written data in the mem-tables (if
   * the mem-table type supports it), data serialized to disk, or both.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param ranges the ranges over which to approximate sizes
   * @param sizeApproximationFlags flags to determine what to include in the
   *     approximation.
   *
   * @return the sizes
   */
  public long[] getApproximateSizes(
      /*@Nullable*/ final ColumnFamilyHandle columnFamilyHandle,
      final List<Range> ranges,
      final SizeApproximationFlag... sizeApproximationFlags) {

    byte flags = 0x0;
    for (final SizeApproximationFlag sizeApproximationFlag
        : sizeApproximationFlags) {
      flags |= sizeApproximationFlag.getValue();
    }

    return getApproximateSizes(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        toRangeSliceHandles(ranges), flags);
  }

  /**
   * Get the approximate file system space used by keys in each range for
   * the default column family.
   * <p>
   * Note that the returned sizes measure file system space usage, so
   * if the user data compresses by a factor of ten, the returned
   * sizes will be one-tenth the size of the corresponding user data size.
   * <p>
   * If {@code sizeApproximationFlags} defines whether the returned size
   * should include the recently written data in the mem-tables (if
   * the mem-table type supports it), data serialized to disk, or both.
   *
   * @param ranges the ranges over which to approximate sizes
   * @param sizeApproximationFlags flags to determine what to include in the
   *     approximation.
   *
   * @return the sizes.
   */
  public long[] getApproximateSizes(final List<Range> ranges,
      final SizeApproximationFlag... sizeApproximationFlags) {
    return getApproximateSizes(null, ranges, sizeApproximationFlags);
  }

  public static class CountAndSize {
    public final long count;
    public final long size;

    public CountAndSize(final long count, final long size) {
      this.count = count;
      this.size = size;
    }
  }

  /**
   * This method is similar to
   * {@link #getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)},
   * except that it returns approximate number of records and size in memtables.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param range the ranges over which to get the memtable stats
   *
   * @return the count and size for the range
   */
  public CountAndSize getApproximateMemTableStats(
      /*@Nullable*/ final ColumnFamilyHandle columnFamilyHandle,
      final Range range) {
    final long[] result = getApproximateMemTableStats(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        range.start.getNativeHandle(),
        range.limit.getNativeHandle());
    return new CountAndSize(result[0], result[1]);
  }

  /**
   * This method is similar to
   * {@link #getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)},
   * except that it returns approximate number of records and size in memtables.
   *
   * @param range the ranges over which to get the memtable stats
   *
   * @return the count and size for the range
   */
  public CountAndSize getApproximateMemTableStats(
    final Range range) {
    return getApproximateMemTableStats(null, range);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * </ul>
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange() throws RocksDBException {
    compactRange(null);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    compactRange(nativeHandle_, null, -1, null, -1, 0,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * </ul>
   *
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final byte[] begin, final byte[] end)
      throws RocksDBException {
    compactRange(null, begin, end);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end) throws RocksDBException {
    compactRange(nativeHandle_,
        begin, begin == null ? -1 : begin.length,
        end, end == null ? -1 : end.length,
        0, columnFamilyHandle == null ? 0: columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param compactRangeOptions options for the compaction
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end,
      final CompactRangeOptions compactRangeOptions) throws RocksDBException {
    compactRange(nativeHandle_,
        begin, begin == null ? -1 : begin.length,
        end, end == null ? -1 : end.length,
        compactRangeOptions.nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * ClipColumnFamily() will clip the entries in the CF according to the range
   * [begin_key, end_key). Returns OK on success, and a non-OK status on error.
   * Any entries outside this range will be completely deleted (including
   * tombstones).
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param beginKey First key to clip within database (inclusive)
   * @param endKey Last key to clip within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void clipColumnFamily(final ColumnFamilyHandle columnFamilyHandle, final byte[] beginKey,
      final byte[] endKey) throws RocksDBException {
    clipColumnFamily(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_, beginKey, 0,
        beginKey.length, endKey, 0, endKey.length);
  }

  /**
   * Change the options for the column family handle.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param mutableColumnFamilyOptions the options.
   *
   * @throws RocksDBException if an error occurs whilst setting the options
   */
  public void setOptions(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle,
      final MutableColumnFamilyOptions mutableColumnFamilyOptions)
      throws RocksDBException {
    setOptions(nativeHandle_, columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        mutableColumnFamilyOptions.getKeys(), mutableColumnFamilyOptions.getValues());
  }

  /**
   * Set performance level for rocksdb performance measurement.
   * @param level
   * @throws IllegalArgumentException for UNINITIALIZED and OUT_OF_BOUNDS values
   *    as they can't be used for settings.
   */
  public void setPerfLevel(final PerfLevel level) {
    if (level == PerfLevel.UNINITIALIZED) {
      throw new IllegalArgumentException("Unable to set UNINITIALIZED level");
    } else if (level == PerfLevel.OUT_OF_BOUNDS) {
      throw new IllegalArgumentException("Unable to set OUT_OF_BOUNDS level");
    } else {
      setPerfLevel(level.getValue());
    }
  }

  /**
   * Return current performance level measurement settings.
   * @return
   */
  public PerfLevel getPerfLevel() {
    byte level = getPerfLevelNative();
    return PerfLevel.getPerfLevel(level);
  }

  /**
   * Return perf context bound to this thread.
   * @return
   */
  public PerfContext getPerfContext() {
    long native_handle = getPerfContextNative();
    return new PerfContext(native_handle);
  }

  /**
   * Get the options for the column family handle
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   *
   * @return the options parsed from the options string return by RocksDB
   *
   * @throws RocksDBException if an error occurs while getting the options string, or parsing the
   *     resulting options string into options
   */
  public MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder getOptions(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle) throws RocksDBException {
    final String optionsString = getOptions(
        nativeHandle_, columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
    return MutableColumnFamilyOptions.parse(optionsString, true);
  }

  /**
   * Default column family options
   *
   * @return the options parsed from the options string return by RocksDB
   *
   * @throws RocksDBException if an error occurs while getting the options string, or parsing the
   *     resulting options string into options
   */
  public MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder getOptions()
      throws RocksDBException {
    return getOptions(null);
  }

  /**
   * Get the database options
   *
   * @return the DB options parsed from the options string return by RocksDB
   *
   * @throws RocksDBException if an error occurs while getting the options string, or parsing the
   *     resulting options string into options
   */
  public MutableDBOptions.MutableDBOptionsBuilder getDBOptions() throws RocksDBException {
    final String optionsString = getDBOptions(nativeHandle_);
    return MutableDBOptions.parse(optionsString, true);
  }

  /**
   * Change the options for the default column family handle.
   *
   * @param mutableColumnFamilyOptions the options.
   *
   * @throws RocksDBException if an error occurs whilst setting the options
   */
  public void setOptions(
      final MutableColumnFamilyOptions mutableColumnFamilyOptions)
      throws RocksDBException {
    setOptions(null, mutableColumnFamilyOptions);
  }

  /**
   * Set the options for the column family handle.
   *
   * @param mutableDBoptions the options.
   *
   * @throws RocksDBException if an error occurs whilst setting the options
   */
  public void setDBOptions(final MutableDBOptions mutableDBoptions)
      throws RocksDBException {
    setDBOptions(nativeHandle_,
        mutableDBoptions.getKeys(),
        mutableDBoptions.getValues());
  }

  /**
   * Takes a list of files specified by file names and
   * compacts them to the specified level.
   * <p>
   * Note that the behavior is different from
   * {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * in that CompactFiles() performs the compaction job using the CURRENT
   * thread.
   *
   * @param compactionOptions compaction options
   * @param inputFileNames the name of the files to compact
   * @param outputLevel the level to which they should be compacted
   * @param outputPathId the id of the output path, or -1
   * @param compactionJobInfo the compaction job info, this parameter
   *     will be updated with the info from compacting the files,
   *     can just be null if you don't need it.
   *
   * @return the list of compacted files
   *
   * @throws RocksDBException if an error occurs during compaction
   */
  public List<String> compactFiles(
      final CompactionOptions compactionOptions,
      final List<String> inputFileNames,
      final int outputLevel,
      final int outputPathId,
      /* @Nullable */ final CompactionJobInfo compactionJobInfo)
      throws RocksDBException {
    return compactFiles(compactionOptions, null, inputFileNames, outputLevel,
        outputPathId, compactionJobInfo);
  }

  /**
   * Takes a list of files specified by file names and
   * compacts them to the specified level.
   * <p>
   * Note that the behavior is different from
   * {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * in that CompactFiles() performs the compaction job using the CURRENT
   * thread.
   *
   * @param compactionOptions compaction options
   * @param columnFamilyHandle columnFamilyHandle, or null for the
   *     default column family
   * @param inputFileNames the name of the files to compact
   * @param outputLevel the level to which they should be compacted
   * @param outputPathId the id of the output path, or -1
   * @param compactionJobInfo the compaction job info, this parameter
   *     will be updated with the info from compacting the files,
   *     can just be null if you don't need it.
   *
   * @return the list of compacted files
   *
   * @throws RocksDBException if an error occurs during compaction
   */
  public List<String> compactFiles(
      final CompactionOptions compactionOptions,
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final List<String> inputFileNames,
      final int outputLevel,
      final int outputPathId,
      /* @Nullable */ final CompactionJobInfo compactionJobInfo)
      throws RocksDBException {
    return Arrays.asList(compactFiles(nativeHandle_, compactionOptions.nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        inputFileNames.toArray(new String[0]),
        outputLevel,
        outputPathId,
        compactionJobInfo == null ? 0 : compactionJobInfo.nativeHandle_));
  }

  /**
   * This function will cancel all currently running background processes.
   *
   * @param wait if true, wait for all background work to be cancelled before
   *   returning.
   *
   */
  public void cancelAllBackgroundWork(final boolean wait) {
    cancelAllBackgroundWork(nativeHandle_, wait);
  }

  /**
   * This function will wait until all currently running background processes
   * finish. After it returns, no background process will be run until
   * {@link #continueBackgroundWork()} is called
   *
   * @throws RocksDBException if an error occurs when pausing background work
   */
  public void pauseBackgroundWork() throws RocksDBException {
    pauseBackgroundWork(nativeHandle_);
  }

  /**
   * Resumes background work which was suspended by
   * previously calling {@link #pauseBackgroundWork()}
   *
   * @throws RocksDBException if an error occurs when resuming background work
   */
  public void continueBackgroundWork() throws RocksDBException {
    continueBackgroundWork(nativeHandle_);
  }

  /**
   * Abort all running and pending compaction jobs. This method will signal
   * all active compactions to terminate and wait for them to complete.
   * No new compactions will be scheduled until {@link #resumeAllCompactions()} is called.
   */
  public void abortAllCompactions() {
    abortAllCompactions(nativeHandle_);
  }

  /**
   * Resume compaction scheduling after {@link #abortAllCompactions()} was called.
   * Must be called the same number of times as {@link #abortAllCompactions()}.
   */
  public void resumeAllCompactions() {
    resumeAllCompactions(nativeHandle_);
  }

  /**
   * Enable automatic compactions for the given column
   * families if they were previously disabled.
   * <p>
   * The function will first set the
   * {@link ColumnFamilyOptions#disableAutoCompactions()} option for each
   * column family to false, after which it will schedule a flush/compaction.
   * <p>
   * NOTE: Setting disableAutoCompactions to 'false' through
   * {@link #setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   * does NOT schedule a flush/compaction afterwards, and only changes the
   * parameter itself within the column family option.
   *
   * @param columnFamilyHandles the column family handles
   *
   * @throws RocksDBException if an error occurs whilst enabling auto-compaction
   */
  public void enableAutoCompaction(
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    enableAutoCompaction(nativeHandle_,
        toNativeHandleList(columnFamilyHandles));
  }

  /**
   * Number of levels used for this DB.
   *
   * @return the number of levels
   */
  public int numberLevels() {
    return numberLevels(null);
  }

  /**
   * Number of levels used for a column family in this DB.
   *
   * @param columnFamilyHandle the column family handle, or null
   *     for the default column family
   *
   * @return the number of levels
   */
  public int numberLevels(/* @Nullable */final ColumnFamilyHandle columnFamilyHandle) {
    return numberLevels(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Maximum level to which a new compacted memtable is pushed if it
   * does not create overlap.
   *
   * @return the maximum level
   */
  @Deprecated
  public int maxMemCompactionLevel() {
    return maxMemCompactionLevel(null);
  }

  /**
   * Maximum level to which a new compacted memtable is pushed if it
   * does not create overlap.
   *
   * @param columnFamilyHandle the column family handle
   *
   * @return the maximum level
   */
  public int maxMemCompactionLevel(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle) {
      return maxMemCompactionLevel(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Number of files in level-0 that would stop writes.
   *
   * @return the number of files
   */
  public int level0StopWriteTrigger() {
    return level0StopWriteTrigger(null);
  }

  /**
   * Number of files in level-0 that would stop writes.
   *
   * @param columnFamilyHandle the column family handle
   *
   * @return the number of files
   */
  public int level0StopWriteTrigger(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle) {
    return level0StopWriteTrigger(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get DB name -- the exact same name that was provided as an argument to
   * as path to {@link #open(Options, String)}.
   *
   * @return the DB name
   */
  public String getName() {
    return getName(nativeHandle_);
  }

  /**
   * Get the Env object from the DB
   *
   * @return the env
   */
  public Env getEnv() {
    final long envHandle = getEnv(nativeHandle_);
    if (envHandle == Env.getDefault().nativeHandle_) {
      return Env.getDefault();
    } else {
      final Env env = new RocksEnv(envHandle);
      env.disOwnNativeHandle();  // we do not own the Env!
      return env;
    }
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions)
      throws RocksDBException {
    flush(flushOptions, Collections.singletonList(getDefaultColumnFamily()));
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions,
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    flush(flushOptions,
        columnFamilyHandle == null ? null : Collections.singletonList(columnFamilyHandle));
  }

  /**
   * Flushes multiple column families.
   * <p>
   * If atomic flush is not enabled, this is equivalent to calling
   * {@link #flush(FlushOptions, ColumnFamilyHandle)} multiple times.
   * <p>
   * If atomic flush is enabled, this will flush all column families
   * specified up to the latest sequence number at the time when flush is
   * requested.
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @param columnFamilyHandles column family handles.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions,
      /* @Nullable */ final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    flush(nativeHandle_, flushOptions.nativeHandle_,
        toNativeHandleList(columnFamilyHandles));
  }

  /**
   * Flush the WAL memory buffer to the file. If {@code sync} is true,
   * it calls {@link #syncWal()} afterwards.
   *
   * @param sync true to also fsync to disk.
   *
   * @throws RocksDBException if an error occurs whilst flushing
   */
  public void flushWal(final boolean sync) throws RocksDBException {
    flushWal(nativeHandle_, sync);
  }

  /**
   * Sync the WAL.
   * <p>
   * Note that {@link #write(WriteOptions, WriteBatch)} followed by
   * {@code #syncWal()} is not exactly the same as
   * {@link #write(WriteOptions, WriteBatch)} with
   * {@link WriteOptions#sync()} set to true; In the latter case the changes
   * won't be visible until the sync is done.
   * <p>
   * Currently only works if {@link Options#allowMmapWrites()} is set to false.
   *
   * @throws RocksDBException if an error occurs whilst syncing
   */
  public void syncWal() throws RocksDBException {
    syncWal(nativeHandle_);
  }

  /**
   * <p>The sequence number of the most recent transaction.</p>
   *
   * @return sequence number of the most
   *     recent transaction.
   */
  public long getLatestSequenceNumber() {
    return getLatestSequenceNumber(nativeHandle_);
  }

  /**
   * <p>Prevent file deletions. Compactions will continue to occur,
   * but no obsolete files will be deleted. Calling this multiple
   * times have the same effect as calling it once.</p>
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void disableFileDeletions() throws RocksDBException {
    disableFileDeletions(nativeHandle_);
  }

  /**
   * <p>EnableFileDeletions will only enable file deletion after
   * it's been called at least as many times as DisableFileDeletions(),
   * enabling the two methods to be called by two threads concurrently
   * without synchronization
   * -- i.e., file deletions will be enabled only after both
   * threads call EnableFileDeletions()</p>
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void enableFileDeletions() throws RocksDBException {
    enableFileDeletions(nativeHandle_);
  }

  public static class LiveFiles {
    /**
     * The valid size of the manifest file. The manifest file is an ever growing
     * file, but only the portion specified here is valid for this snapshot.
     */
    public final long manifestFileSize;

    /**
     * The files are relative to the {@link #getName()} and are not
     * absolute paths. Despite being relative paths, the file names begin
     * with "/".
     */
    public final List<String> files;

    LiveFiles(final long manifestFileSize, final List<String> files) {
      this.manifestFileSize = manifestFileSize;
      this.files = files;
    }
  }

  /**
   * Retrieve the list of all files in the database after flushing the memtable.
   * <p>
   * See {@link #getLiveFiles(boolean)}.
   *
   * @return the live files
   *
   * @throws RocksDBException if an error occurs whilst retrieving the list
   *     of live files
   */
  public LiveFiles getLiveFiles() throws RocksDBException {
    return getLiveFiles(true);
  }

  /**
   * Retrieve the list of all files in the database.
   * <p>
   * In case you have multiple column families, even if {@code flushMemtable}
   * is true, you still need to call {@link #getSortedWalFiles()}
   * after {@code #getLiveFiles(boolean)} to compensate for new data that
   * arrived to already-flushed column families while other column families
   * were flushing.
   * <p>
   * NOTE: Calling {@code #getLiveFiles(boolean)} followed by
   *     {@link #getSortedWalFiles()} can generate a lossless backup.
   *
   * @param flushMemtable set to true to flush before recoding the live
   *     files. Setting to false is useful when we don't want to wait for flush
   *     which may have to wait for compaction to complete taking an
   *     indeterminate time.
   *
   * @return the live files
   *
   * @throws RocksDBException if an error occurs whilst retrieving the list
   *     of live files
   */
  public LiveFiles getLiveFiles(final boolean flushMemtable)
      throws RocksDBException {
     final String[] result = getLiveFiles(nativeHandle_, flushMemtable);
     if (result == null) {
       return null;
     }
     final String[] files = Arrays.copyOf(result, result.length - 1);
     final long manifestFileSize = Long.parseLong(result[result.length - 1]);

     return new LiveFiles(manifestFileSize, Arrays.asList(files));
  }

  /**
   * Retrieve the sorted list of all wal files with earliest file first.
   *
   * @return the log files
   *
   * @throws RocksDBException if an error occurs whilst retrieving the list
   *     of sorted WAL files
   */
  public List<LogFile> getSortedWalFiles() throws RocksDBException {
    final LogFile[] logFiles = getSortedWalFiles(nativeHandle_);
    return Arrays.asList(logFiles);
  }

  /**
   * <p>Returns a {@link WalIterator} over the WriteBatches recorded in the
   * write-ahead log, starting from the one containing
   * {@code sequenceNumber}.</p>
   *
   * <p>Only writes that reached the WAL are returned. Writes made with
   * {@link WriteOptions#setDisableWAL(boolean)}, and sequence numbers consumed
   * by {@code ingestExternalFile}, are absent and leave permanent holes in the
   * sequence numbers seen here. Set {@code WAL_ttl_seconds} and/or
   * {@code WAL_size_limit_MB} large enough to cover how far behind a consumer
   * may fall, or the WAL will be cleared before the consumer reads it.</p>
   *
   * <p><strong>WARNING:</strong> if {@code sequenceNumber} itself is no longer
   * available, this returns an iterator positioned at the next available
   * write-batch and reports no error -- data is skipped silently. Since
   * recovering from a spent iterator means calling this method again, that
   * recovery is where data loss can slip in unnoticed. Callers resuming a
   * previous iterator should check that the first batch continues from where
   * that one stopped; see {@link WalIterator}.</p>
   *
   * @param sequenceNumber sequence number offset
   *
   * @return {@link org.rocksdb.WalIterator} instance. The declared type is the
   *     old name, retained so existing callers still compile; declare
   *     {@link WalIterator} in new code.
   *
   * @throws org.rocksdb.RocksDBException if iterator cannot be retrieved
   *     from native-side.
   */
  public TransactionLogIterator getUpdatesSince(final long sequenceNumber)
      throws RocksDBException {
    return new TransactionLogIterator(
        getUpdatesSince(nativeHandle_, sequenceNumber));
  }

  /**
   * Gets a list of all table files metadata.
   *
   * @return table files metadata.
   */
  public List<LiveFileMetaData> getLiveFilesMetaData() {
    return Arrays.asList(getLiveFilesMetaData(nativeHandle_));
  }

  /**
   * Obtains the meta data of the specified column family of the DB.
   *
   * @param columnFamilyHandle the column family
   *
   * @return the column family metadata
   */
  public ColumnFamilyMetaData getColumnFamilyMetaData(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle) {
    return getColumnFamilyMetaData(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Obtains the meta data of the default column family of the DB.
   *
   * @return the column family metadata
   */
  public ColumnFamilyMetaData getColumnFamilyMetaData() {
    return getColumnFamilyMetaData(null);
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   * <p>
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, getDefaultColumnFamily().nativeHandle_,
        filePathList.toArray(new String[0]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   * <p>
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param columnFamilyHandle The column family for the ingested files
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final ColumnFamilyHandle columnFamilyHandle,
      final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, columnFamilyHandle.nativeHandle_,
        filePathList.toArray(new String[0]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * Verify checksum
   *
   * @throws RocksDBException if the checksum is not valid
   */
  public void verifyChecksum() throws RocksDBException {
    verifyChecksum(nativeHandle_);
  }

  /**
   * Gets the handle for the default column family
   *
   * @return The handle of the default column family
   */
  public ColumnFamilyHandle getDefaultColumnFamily() {
    return defaultColumnFamilyHandle_;
  }

  /**
   * Create a handle for the default CF on open
   *
   * @return the default family handle
   */
  protected ColumnFamilyHandle makeDefaultColumnFamilyHandle() {
    final ColumnFamilyHandle cfHandle =
        new ColumnFamilyHandle(this, getDefaultColumnFamily(nativeHandle_));
    cfHandle.disOwnNativeHandle();
    return cfHandle;
  }

  /**
   * Get the properties of all tables.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   *
   * @return the properties
   *
   * @throws RocksDBException if an error occurs whilst getting the properties
   */
  public Map<String, TableProperties> getPropertiesOfAllTables(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    return getPropertiesOfAllTables(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the properties of all tables in the default column family.
   *
   * @return the properties
   *
   * @throws RocksDBException if an error occurs whilst getting the properties
   */
  public Map<String, TableProperties> getPropertiesOfAllTables()
      throws RocksDBException {
    return getPropertiesOfAllTables(null);
  }

  /**
   * Get the properties of tables in range.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   * @param ranges the ranges over which to get the table properties
   *
   * @return the properties
   *
   * @throws RocksDBException if an error occurs whilst getting the properties
   */
  public Map<String, TableProperties> getPropertiesOfTablesInRange(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle,
      final List<Range> ranges) throws RocksDBException {
    return getPropertiesOfTablesInRange(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        toRangeSliceHandles(ranges));
  }

  /**
   * Get the properties of tables in range for the default column family.
   *
   * @param ranges the ranges over which to get the table properties
   *
   * @return the properties
   *
   * @throws RocksDBException if an error occurs whilst getting the properties
   */
  public Map<String, TableProperties> getPropertiesOfTablesInRange(
      final List<Range> ranges) throws RocksDBException {
    return getPropertiesOfTablesInRange(null, ranges);
  }

  /**
   * Suggest the range to compact.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   *
   * @return the suggested range.
   *
   * @throws RocksDBException if an error occurs whilst suggesting the range
   */
  public Range suggestCompactRange(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    final long[] rangeSliceHandles = suggestCompactRange(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
    return new Range(new Slice(rangeSliceHandles[0]),
        new Slice(rangeSliceHandles[1]));
  }

  /**
   * Suggest the range to compact for the default column family.
   *
   * @return the suggested range.
   *
   * @throws RocksDBException if an error occurs whilst suggesting the range
   */
  public Range suggestCompactRange()
      throws RocksDBException {
    return suggestCompactRange(null);
  }

  /**
   * Promote L0.
   *
   * @param columnFamilyHandle the column family handle,
   *     or null for the default column family.
   * @param targetLevel the target level for L0
   *
   * @throws RocksDBException if an error occurs whilst promoting L0
   *
   * @deprecated this API may be removed in a future release.
   */
  @Deprecated
  public void promoteL0(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle, final int targetLevel)
      throws RocksDBException {
    promoteL0(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        targetLevel);
  }

  /**
   * Promote L0 for the default column family.
   *
   * @param targetLevel the target level for L0
   *
   * @throws RocksDBException if an error occurs whilst promoting L0
   *
   * @deprecated this API may be removed in a future release.
   */
  @Deprecated
  public void promoteL0(final int targetLevel) throws RocksDBException {
    promoteL0(null, targetLevel);
  }

  /**
   * Trace DB operations.
   * <p>
   * Use {@link #endTrace()} to stop tracing.
   *
   * @param traceOptions the options
   * @param traceWriter the trace writer
   *
   * @throws RocksDBException if an error occurs whilst starting the trace
   */
  public void startTrace(final TraceOptions traceOptions,
      final AbstractTraceWriter traceWriter) throws RocksDBException {
    startTrace(nativeHandle_, traceOptions.getMaxTraceFileSize(),
        traceWriter.nativeHandle_);
    /*
     * NOTE: {@link #startTrace(long, long, long) transfers the ownership
     * from Java to C++, so we must disown the native handle here.
     */
    traceWriter.disOwnNativeHandle();
  }

  /**
   * Stop tracing DB operations.
   * <p>
   * See {@link #startTrace(TraceOptions, AbstractTraceWriter)}
   *
   * @throws RocksDBException if an error occurs whilst ending the trace
   */
  public void endTrace() throws RocksDBException {
    endTrace(nativeHandle_);
  }

  /**
   * Make the secondary instance catch up with the primary by tailing and
   * replaying the MANIFEST and WAL of the primary.
   * Column families created by the primary after the secondary instance starts
   * will be ignored unless the secondary instance closes and restarts with the
   * newly created column families.
   * Column families that exist before secondary instance starts and dropped by
   * the primary afterwards will be marked as dropped. However, as long as the
   * secondary instance does not delete the corresponding column family
   * handles, the data of the column family is still accessible to the
   * secondary.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void tryCatchUpWithPrimary() throws RocksDBException {
    tryCatchUpWithPrimary(nativeHandle_);
  }

  /**
   * Delete files in multiple ranges at once.
   * Delete files in a lot of ranges one at a time can be slow, use this API for
   * better performance in that case.
   *
   * @param columnFamily - The column family for operation (null for default)
   * @param includeEnd - Whether ranges should include end
   * @param ranges - pairs of ranges (from1, to1, from2, to2, ...)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void deleteFilesInRanges(final ColumnFamilyHandle columnFamily, final List<byte[]> ranges,
      final boolean includeEnd) throws RocksDBException {
    if (ranges.isEmpty()) {
      return;
    }
    if ((ranges.size() % 2) != 0) {
      throw new IllegalArgumentException("Ranges size needs to be multiple of 2 "
          + "(from1, to1, from2, to2, ...), but is " + ranges.size());
    }

    final byte[][] rangesArray = ranges.toArray(new byte[ranges.size()][]);

    deleteFilesInRanges(nativeHandle_, columnFamily == null ? 0 : columnFamily.nativeHandle_,
        rangesArray, includeEnd);
  }

  /**
   * Static method to destroy the contents of the specified database.
   * Be very careful using this method.
   *
   * @param path the path to the Rocksdb database.
   * @param options {@link org.rocksdb.Options} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static void destroyDB(final String path, final Options options)
      throws RocksDBException {
    destroyDB(path, options.nativeHandle_);
  }

  private /* @Nullable */ long[] toNativeHandleList(
      /* @Nullable */ final List<? extends RocksObject> objectList) {
    if (objectList == null) {
      return new long[0];
    }
    final int len = objectList.size();
    final long[] handleList = new long[len];
    for (int i = 0; i < len; i++) {
      handleList[i] = objectList.get(i).nativeHandle_;
    }
    return handleList;
  }

  @SuppressWarnings({"PMD.ForLoopVariableCount", "PMD.AvoidReassigningLoopVariables"})
  private static long[] toRangeSliceHandles(final List<Range> ranges) {
    final long[] rangeSliceHandles = new long[ranges.size() * 2];
    for (int i = 0, j = 0; i < ranges.size(); i++) {
      final Range range = ranges.get(i);
      rangeSliceHandles[j++] = range.start.getNativeHandle();
      rangeSliceHandles[j++] = range.limit.getNativeHandle();
    }
    return rangeSliceHandles;
  }

  protected void storeOptionsInstance(final DBOptionsInterface<?> options) {
    options_ = options;
  }

  protected void storeDefaultColumnFamilyHandle(ColumnFamilyHandle columnFamilyHandle) {
    defaultColumnFamilyHandle_ = columnFamilyHandle;
  }

  private static void checkBounds(int offset, int len, int size) {
    if ((offset | len | (offset + len) | (size - (offset + len))) < 0) {
      throw new IndexOutOfBoundsException(String.format("offset(%d), len(%d), size(%d)", offset, len, size));
    }
  }

  // native methods
  private static native long open(final long optionsHandle, final String path)
      throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  private static native long[] open(final long optionsHandle, final String path,
      final byte[][] columnFamilyNames, final long[] columnFamilyOptions) throws RocksDBException;

  private static native long openROnly(final long optionsHandle, final String path,
      final boolean errorIfWalFileExists) throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  private static native long[] openROnly(final long optionsHandle, final String path,
      final byte[][] columnFamilyNames, final long[] columnFamilyOptions,
      final boolean errorIfWalFileExists) throws RocksDBException;

  private static native long openAsSecondary(final long optionsHandle, final String path,
      final String secondaryPath) throws RocksDBException;

  private static native long[] openAsSecondary(final long optionsHandle, final String path,
      final String secondaryPath, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions) throws RocksDBException;

  @Override
  protected void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native void closeDatabase(final long handle) throws RocksDBException;
  private static native byte[][] listColumnFamilies(final long optionsHandle, final String path)
      throws RocksDBException;
  private static native long createColumnFamily(final long handle, final byte[] columnFamilyName,
      final int columnFamilyNamelen, final long columnFamilyOptions) throws RocksDBException;
  private static native long[] createColumnFamilies(
      final long handle, final long columnFamilyOptionsHandle, final byte[][] columnFamilyNames)
      throws RocksDBException;
  private static native long[] createColumnFamilies(
      final long handle, final long[] columnFamilyOptionsHandles, final byte[][] columnFamilyNames)
      throws RocksDBException;
  private static native long createColumnFamilyWithImport(final long handle,
      final byte[] columnFamilyName, final int columnFamilyNamelen, final long columnFamilyOptions,
      final long importColumnFamilyOptions, final long[] metadataHandleList)
      throws RocksDBException;
  private static native void dropColumnFamily(final long handle, final long cfHandle)
      throws RocksDBException;
  private static native void dropColumnFamilies(final long handle, final long[] cfHandles)
      throws RocksDBException;
  private static native void put(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, int valueLength)
      throws RocksDBException;
  private static native void put(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;
  private static native void put(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength) throws RocksDBException;
  private static native void put(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength, final long cfHandle) throws RocksDBException;
  private static native void delete(final long handle, final byte[] key, final int keyOffset,
      final int keyLength) throws RocksDBException;
  private static native void delete(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;
  private static native void delete(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength) throws RocksDBException;
  private static native void delete(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle) throws RocksDBException;
  private static native void singleDelete(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;
  private static native void singleDelete(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;
  private static native void singleDelete(final long handle, final long writeOptHandle,
      final byte[] key, final int keyLen) throws RocksDBException;
  private static native void singleDelete(final long handle, final long writeOptHandle,
      final byte[] key, final int keyLen, final long cfHandle) throws RocksDBException;
  private static native void deleteRange(final long handle, final byte[] beginKey,
      final int beginKeyOffset, final int beginKeyLength, final byte[] endKey,
      final int endKeyOffset, final int endKeyLength) throws RocksDBException;
  private static native void deleteRange(final long handle, final byte[] beginKey,
      final int beginKeyOffset, final int beginKeyLength, final byte[] endKey,
      final int endKeyOffset, final int endKeyLength, final long cfHandle) throws RocksDBException;
  private static native void deleteRange(final long handle, final long writeOptHandle,
      final byte[] beginKey, final int beginKeyOffset, final int beginKeyLength,
      final byte[] endKey, final int endKeyOffset, final int endKeyLength) throws RocksDBException;
  private static native void deleteRange(final long handle, final long writeOptHandle,
      final byte[] beginKey, final int beginKeyOffset, final int beginKeyLength,
      final byte[] endKey, final int endKeyOffset, final int endKeyLength, final long cfHandle)
      throws RocksDBException;
  private static native void clipColumnFamily(final long handle, final long cfHandle,
      final byte[] beginKey, final int beginKeyOffset, final int beginKeyLength,
      final byte[] endKey, final int endKeyOffset, final int endKeyLength) throws RocksDBException;
  private static native void merge(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private static native void merge(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;
  private static native void merge(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength) throws RocksDBException;
  private static native void merge(final long handle, final long writeOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength, final long cfHandle) throws RocksDBException;
  private static native void mergeDirect(long handle, long writeOptHandle, ByteBuffer key,
      int keyOffset, int keyLength, ByteBuffer value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;

  private static native void write0(
      final long handle, final long writeOptHandle, final long wbHandle) throws RocksDBException;
  private static native void write1(
      final long handle, final long writeOptHandle, final long wbwiHandle) throws RocksDBException;
  private static native int get(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private static native int get(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;

  private static native int get(final long handle, final long readOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength) throws RocksDBException;

  private static native int get(final long handle, final long readOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength, final long cfHandle) throws RocksDBException;
  private static native byte[] get(final long handle, byte[] key, final int keyOffset,
      final int keyLength) throws RocksDBException;
  private static native byte[] get(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;
  private static native byte[] get(final long handle, final long readOptHandle, final byte[] key,
      final int keyOffset, final int keyLength) throws RocksDBException;
  private static native byte[] get(final long handle, final long readOptHandle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle) throws RocksDBException;
  private static native GetWithMetadataResult getWithMetadata(final long handle,
      final long readOptHandle, final byte[] key, final int keyOffset, final int keyLength,
      final long cfHandle) throws RocksDBException;
  private static native byte[][] multiGet(
      final long dbHandle, final byte[][] keys, final int[] keyOffsets, final int[] keyLengths);
  private static native byte[][] multiGet(final long dbHandle, final byte[][] keys,
      final int[] keyOffsets, final int[] keyLengths, final long[] columnFamilyHandles);
  private static native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths);
  private static native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);
  private static native GetWithMetadataResult[] multiGetWithMetadata(final long dbHandle,
      final long rOptHandle, final byte[][] keys, final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);

  private static native void multiGet(final long dbHandle, final long rOptHandle,
      final long[] columnFamilyHandles, final ByteBuffer[] keysArray, final int[] keyOffsets,
      final int[] keyLengths, final ByteBuffer[] valuesArray, final int[] valuesSizeArray,
      final Status[] statusArray);

  private static native boolean keyExists(final long handle, final long cfHandle,
      final long readOptHandle, final byte[] key, final int keyOffset, final int keyLength);

  private static native boolean keyExistsDirect(final long handle, final long cfHandle,
      final long readOptHandle, final ByteBuffer key, final int keyOffset, final int keyLength);

  private static native boolean keyMayExist(final long handle, final long cfHandle,
      final long readOptHandle, final byte[] key, final int keyOffset, final int keyLength);
  private static native byte[][] keyMayExistFoundValue(final long handle, final long cfHandle,
      final long readOptHandle, final byte[] key, final int keyOffset, final int keyLength);
  private static native void putDirect(long handle, long writeOptHandle, ByteBuffer key,
      int keyOffset, int keyLength, ByteBuffer value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;
  private static native long iterator(
      final long handle, final long cfHandle, final long readOptHandle);
  private static native long[] iterators(final long handle, final long[] columnFamilyHandles,
      final long readOptHandle) throws RocksDBException;

  private static native long getSnapshot(final long nativeHandle);
  private static native void releaseSnapshot(final long nativeHandle, final long snapshotHandle);
  private static native String getProperty(final long nativeHandle, final long cfHandle,
      final String property, final int propertyLength) throws RocksDBException;
  private static native Map<String, String> getMapProperty(final long nativeHandle,
      final long cfHandle, final String property, final int propertyLength) throws RocksDBException;
  private static native int getDirect(long handle, long readOptHandle, ByteBuffer key,
      int keyOffset, int keyLength, ByteBuffer value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;
  private static native boolean keyMayExistDirect(final long handle, final long cfHhandle,
      final long readOptHandle, final ByteBuffer key, final int keyOffset, final int keyLength);
  private static native int[] keyMayExistDirectFoundValue(final long handle, final long cfHhandle,
      final long readOptHandle, final ByteBuffer key, final int keyOffset, final int keyLength,
      final ByteBuffer value, final int valueOffset, final int valueLength);
  private static native void deleteDirect(long handle, long optHandle, ByteBuffer key,
      int keyOffset, int keyLength, long cfHandle) throws RocksDBException;
  private static native long getLongProperty(final long nativeHandle, final long cfHandle,
      final String property, final int propertyLength) throws RocksDBException;
  private static native void resetStats(final long nativeHandle) throws RocksDBException;
  private static native long getAggregatedLongProperty(
      final long nativeHandle, final String property, int propertyLength) throws RocksDBException;
  private static native long[] getApproximateSizes(final long nativeHandle,
      final long columnFamilyHandle, final long[] rangeSliceHandles, final byte includeFlags);
  private static native long[] getApproximateMemTableStats(final long nativeHandle,
      final long columnFamilyHandle, final long rangeStartSliceHandle,
      final long rangeLimitSliceHandle);
  private static native void compactRange(final long handle,
      /* @Nullable */ final byte[] begin, final int beginLen,
      /* @Nullable */ final byte[] end, final int endLen, final long compactRangeOptHandle,
      final long cfHandle) throws RocksDBException;
  private static native void setOptions(final long handle, final long cfHandle, final String[] keys,
      final String[] values) throws RocksDBException;
  private static native String getOptions(final long handle, final long cfHandle);
  private static native void setDBOptions(
      final long handle, final String[] keys, final String[] values) throws RocksDBException;
  private static native String getDBOptions(final long handle);
  private static native void setPerfLevel(final byte level);
  private static native byte getPerfLevelNative();

  private static native long getPerfContextNative();

  private static native String[] compactFiles(final long handle, final long compactionOptionsHandle,
      final long columnFamilyHandle, final String[] inputFileNames, final int outputLevel,
      final int outputPathId, final long compactionJobInfoHandle) throws RocksDBException;
  private static native void cancelAllBackgroundWork(final long handle, final boolean wait);
  private static native void pauseBackgroundWork(final long handle) throws RocksDBException;
  private static native void continueBackgroundWork(final long handle) throws RocksDBException;
  private static native void abortAllCompactions(final long handle);
  private static native void resumeAllCompactions(final long handle);
  private static native void enableAutoCompaction(
      final long handle, final long[] columnFamilyHandles) throws RocksDBException;
  private static native int numberLevels(final long handle, final long columnFamilyHandle);
  private static native int maxMemCompactionLevel(final long handle, final long columnFamilyHandle);
  private static native int level0StopWriteTrigger(
      final long handle, final long columnFamilyHandle);
  private static native String getName(final long handle);
  private static native long getEnv(final long handle);
  private static native void flush(final long handle, final long flushOptHandle,
      /* @Nullable */ final long[] cfHandles) throws RocksDBException;
  private static native void flushWal(final long handle, final boolean sync)
      throws RocksDBException;
  private static native void syncWal(final long handle) throws RocksDBException;
  private static native long getLatestSequenceNumber(final long handle);
  private static native void disableFileDeletions(long handle) throws RocksDBException;
  private static native void enableFileDeletions(long handle) throws RocksDBException;
  private static native String[] getLiveFiles(final long handle, final boolean flushMemtable)
      throws RocksDBException;
  private static native LogFile[] getSortedWalFiles(final long handle) throws RocksDBException;
  private static native long getUpdatesSince(final long handle, final long sequenceNumber)
      throws RocksDBException;
  private static native LiveFileMetaData[] getLiveFilesMetaData(final long handle);
  private static native ColumnFamilyMetaData getColumnFamilyMetaData(
      final long handle, final long columnFamilyHandle);
  private static native void ingestExternalFile(final long handle, final long columnFamilyHandle,
      final String[] filePathList, final int filePathListLen,
      final long ingestExternalFileOptionsHandle) throws RocksDBException;
  private static native void verifyChecksum(final long handle) throws RocksDBException;
  private static native long getDefaultColumnFamily(final long handle);
  private static native Map<String, TableProperties> getPropertiesOfAllTables(
      final long handle, final long columnFamilyHandle) throws RocksDBException;
  private static native Map<String, TableProperties> getPropertiesOfTablesInRange(
      final long handle, final long columnFamilyHandle, final long[] rangeSliceHandles);
  private static native long[] suggestCompactRange(final long handle, final long columnFamilyHandle)
      throws RocksDBException;
  private static native void promoteL0(final long handle, final long columnFamilyHandle,
      final int tragetLevel) throws RocksDBException;
  private static native void startTrace(final long handle, final long maxTraceFileSize,
      final long traceWriterHandle) throws RocksDBException;
  private static native void endTrace(final long handle) throws RocksDBException;
  private static native void tryCatchUpWithPrimary(final long handle) throws RocksDBException;
  private static native void deleteFilesInRanges(long handle, long cfHandle, final byte[][] ranges,
      boolean include_end) throws RocksDBException;

  private static native void destroyDB(final String path, final long optionsHandle)
      throws RocksDBException;

  private static native int version();

  protected DBOptionsInterface<?> options_;
  private static Version version;

  public static class Version {
    private final byte major;
    private final byte minor;
    private final byte patch;

    public Version(final byte major, final byte minor, final byte patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public int getPatch() {
      return patch;
    }

    @Override
    public String toString() {
      return getMajor() + "." + getMinor() + "." + getPatch();
    }

    private static Version fromEncodedVersion(final int encodedVersion) {
      final byte patch = (byte) (encodedVersion & 0xff);
      final byte minor = (byte) (encodedVersion >> 8 & 0xff);
      final byte major = (byte) (encodedVersion >> 16 & 0xff);

      return new Version(major, minor, patch);
    }
  }
}
