//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public abstract class TablePropertiesCollectorFactory extends RocksObject {
  private TablePropertiesCollectorFactory(final long nativeHandle) {
    super(nativeHandle);
  }

  public static TablePropertiesCollectorFactory NewCompactOnDeletionCollectorFactory(
      final long sliding_window_size, final long deletion_trigger, final double deletion_ratio) {
    long handle =
        newCompactOnDeletionCollectorFactory(sliding_window_size, deletion_trigger, deletion_ratio);
    return new TablePropertiesCollectorFactory(handle) {
      @Override
      protected void disposeInternal(long handle) {
        deleteCompactOnDeletionCollectorFactory(handle);
      }
    };
  }

  /**
   * Internal API. Do not use.
   * @param nativeHandle
   * @return
   */
  static TablePropertiesCollectorFactory newWrapper(final long nativeHandle) {
    return new TablePropertiesCollectorFactory(nativeHandle) {
      @Override
      protected void disposeInternal(long handle) {
        deleteCompactOnDeletionCollectorFactory(handle);
      }
    };
  }

  private static native long newCompactOnDeletionCollectorFactory(
      final long slidingWindowSize, final long deletionTrigger, final double deletionRatio);

  private static native void deleteCompactOnDeletionCollectorFactory(final long handle);
}
