// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for all Env implementations in RocksDB.
 */
public abstract class Env extends RocksObject {
  private static final AtomicReference<RocksEnv> SINGULAR_DEFAULT_ENV = new AtomicReference<>(null);

  /**
   * <p>Returns the default environment suitable for the current operating
   * system.</p>
   *
   * <p>The result of {@code getDefault()} is a singleton whose ownership
   * belongs to rocksdb c++.  As a result, the returned RocksEnv will not
   * have the ownership of its c++ resource, and calling its dispose()/close()
   * will be no-op.</p>
   *
   * @return the default {@link org.rocksdb.RocksEnv} instance.
   */
  @SuppressWarnings({"PMD.CloseResource", "PMD.AssignmentInOperand"})
  public static Env getDefault() {
    RocksEnv defaultEnv;
    RocksEnv newDefaultEnv = null;

    while ((defaultEnv = SINGULAR_DEFAULT_ENV.get()) == null) {
      // construct the RocksEnv only once in this thread
      if (newDefaultEnv == null) {
        // load the library just in-case it isn't already loaded!
        RocksDB.loadLibrary();

        newDefaultEnv = new RocksEnv(getDefaultEnvInternal());

        /*
         * The Ownership of the Default Env belongs to C++
         *  and so we disown the native handle here so that
         * we cannot accidentally free it from Java.
         */
        newDefaultEnv.disOwnNativeHandle();
      }

      // use CAS to gracefully handle thread pre-emption
      SINGULAR_DEFAULT_ENV.compareAndSet(null, newDefaultEnv);
    }

    return defaultEnv;
  }

  /**
   * <p>Sets the number of background worker threads of the low priority
   * pool for this environment.</p>
   * <p>Default number: 1</p>
   *
   * @param number the number of threads
   *
   * @return current {@link RocksEnv} instance.
   */
  public Env setBackgroundThreads(final int number) {
    return setBackgroundThreads(number, Priority.LOW);
  }

  /**
   * <p>Gets the number of background worker threads of the pool
   * for this environment.</p>
   *
   * @param priority the priority id of a specified thread pool.
   *
   * @return the number of threads.
   */
  public int getBackgroundThreads(final Priority priority) {
    return getBackgroundThreads(nativeHandle_, priority.getValue());
  }

  /**
   * <p>Sets the number of background worker threads of the specified thread
   * pool for this environment.</p>
   *
   * @param number the number of threads
   * @param priority the priority id of a specified thread pool.
   *
   * <p>Default number: 1</p>
   * @return current {@link RocksEnv} instance.
   */
  public Env setBackgroundThreads(final int number, final Priority priority) {
    setBackgroundThreads(nativeHandle_, number, priority.getValue());
    return this;
  }

  /**
   * <p>Returns the length of the queue associated with the specified
   * thread pool.</p>
   *
   * @param priority the priority id of a specified thread pool.
   *
   * @return the thread pool queue length.
   */
  public int getThreadPoolQueueLen(final Priority priority) {
    return getThreadPoolQueueLen(nativeHandle_, priority.getValue());
  }

  /**
   * Enlarge number of background worker threads of a specific thread pool
   * for this environment if it is smaller than specified. 'LOW' is the default
   * pool.
   *
   * @param number the number of threads.
   * @param priority the priority id of a specified thread pool.
   *
   * @return current {@link RocksEnv} instance.
   */
  public Env incBackgroundThreadsIfNeeded(final int number,
    final Priority priority) {
    incBackgroundThreadsIfNeeded(nativeHandle_, number, priority.getValue());
    return this;
  }

  /**
   * Lower IO priority for threads from the specified pool.
   *
   * @param priority the priority id of a specified thread pool.
   *
   * @return current {@link RocksEnv} instance.
   */
  public Env lowerThreadPoolIOPriority(final Priority priority) {
    lowerThreadPoolIOPriority(nativeHandle_, priority.getValue());
    return this;
  }

  /**
   * Lower CPU priority for threads from the specified pool.
   *
   * @param priority the priority id of a specified thread pool.
   *
   * @return current {@link RocksEnv} instance.
   */
  public Env lowerThreadPoolCPUPriority(final Priority priority) {
    lowerThreadPoolCPUPriority(nativeHandle_, priority.getValue());
    return this;
  }

  /**
   * Returns the status of all threads that belong to the current Env.
   *
   * @return the status of all threads belong to this env.
   *
   * @throws RocksDBException if the thread list cannot be acquired.
   */
  public List<ThreadStatus> getThreadList() throws RocksDBException {
    return Arrays.asList(getThreadList(nativeHandle_));
  }

  protected Env(final long nativeHandle) {
    super(nativeHandle);
  }

  private static native long getDefaultEnvInternal();
  private static native void setBackgroundThreads(
      final long handle, final int number, final byte priority);
  private static native int getBackgroundThreads(final long handle, final byte priority);
  private static native int getThreadPoolQueueLen(final long handle, final byte priority);
  private static native void incBackgroundThreadsIfNeeded(
      final long handle, final int number, final byte priority);
  private static native void lowerThreadPoolIOPriority(final long handle, final byte priority);
  private static native void lowerThreadPoolCPUPriority(final long handle, final byte priority);
  private static native ThreadStatus[] getThreadList(final long handle) throws RocksDBException;
}
