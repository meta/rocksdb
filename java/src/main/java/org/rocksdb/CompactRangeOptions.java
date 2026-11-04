// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

/**
 * CompactRangeOptions is used by CompactRange() call. In the documentation of the methods "the compaction" refers to
 * any compaction that is using this CompactRangeOptions.
 */
public class CompactRangeOptions extends RocksObject {
  private static final byte VALUE_kSkip = 0;
  private static final byte VALUE_kIfHaveCompactionFilter = 1;
  private static final byte VALUE_kForce = 2;
  private static final byte VALUE_kForceOptimized = 3;

  // For level based compaction, we can configure if we want to skip/force bottommost level
  // compaction. The order of this enum MUST follow the C++ layer. See BottommostLevelCompaction in
  // db/options.h
  public enum BottommostLevelCompaction {
    /**
     * Skip bottommost level compaction
     */
    kSkip(VALUE_kSkip),
    /**
     * Only compact bottommost level if there is a compaction filter. This is the default option
     */
    kIfHaveCompactionFilter(VALUE_kIfHaveCompactionFilter),
    /**
     * Always compact bottommost level
     */
    kForce(VALUE_kForce),
    /**
     * Always compact bottommost level but in bottommost level avoid
     * double-compacting files created in the same compaction
     */
    kForceOptimized(VALUE_kForceOptimized);

    private final byte value;

    BottommostLevelCompaction(final byte value) {
      this.value = value;
    }

    /**
     * <p>Returns the byte value of the enumerations value.</p>
     *
     * @return byte representation
     */
    public byte getValue() {
      return value;
    }

    /**
     * Returns the BottommostLevelCompaction for the given C++ rocks enum value.
     * @param bottommostLevelCompaction The value of the BottommostLevelCompaction
     * @return BottommostLevelCompaction instance, or null if none matches
     */
    public static BottommostLevelCompaction fromRocksId(final int bottommostLevelCompaction) {
      switch (bottommostLevelCompaction) {
        case VALUE_kSkip: return kSkip;
        case VALUE_kIfHaveCompactionFilter: return kIfHaveCompactionFilter;
        case VALUE_kForce: return kForce;
        case VALUE_kForceOptimized:
          return kForceOptimized;
        default: return null;
      }
    }
  }

  public static class Timestamp {
    public final long start;
    public final long range;

    public Timestamp(final long start, final long duration) {
      this.start = start;
      this.range = duration;
    }

    public Timestamp() {
      this.start = 0;
      this.range = 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Timestamp timestamp = (Timestamp) o;
      return start == timestamp.start && range == timestamp.range;
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, range);
    }
  }

  /**
   * Construct CompactRangeOptions.
   */
  public CompactRangeOptions() {
    super(newCompactRangeOptions());
  }

  /**
   * Returns whether the compaction is exclusive or other compactions may run concurrently at the same time.
   *
   * @return true if exclusive, false if concurrent
   */
  public boolean exclusiveManualCompaction() {
    return exclusiveManualCompaction(nativeHandle_);
  }

  /**
   * Sets whether the compaction is exclusive or other compaction are allowed run concurrently at the same time.
   *
   * @param exclusiveCompaction true if compaction should be exclusive
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setExclusiveManualCompaction(final boolean exclusiveCompaction) {
    setExclusiveManualCompaction(nativeHandle_, exclusiveCompaction);
    return this;
  }

  /**
   * Returns whether this compaction repairs references to missing integrated external blob files.
   *
   * @return true if repair is enabled; false by default
   */
  public boolean skipMissingBlobFiles() {
    return skipMissingBlobFiles(nativeHandle_);
  }

  /**
   * Enables offline repair of references to integrated external blob files absent from the
   * compaction's input Version. Lost scalar values become deletions; wide entities retain available
   * columns and become deletions when no columns remain. This deliberately discards unavailable
   * data. Legacy BlobDB is unsupported.
   *
   * <p>Selected SST files are rewritten, including bottommost files even with {@code kSkip}, and
   * repair runs locally. FIFO compaction is unsupported. The metadata-only check cannot detect disk
   * loss when blob metadata remains present. Filters, merges, and blob garbage collection can still
   * read payloads and propagate errors. This setting applies only to calls using this object.
   *
   * @param skipMissingBlobFiles true to enable repair
   * @return this CompactRangeOptions
   */
  public CompactRangeOptions setSkipMissingBlobFiles(final boolean skipMissingBlobFiles) {
    setSkipMissingBlobFiles(nativeHandle_, skipMissingBlobFiles);
    return this;
  }

  /**
   * Returns whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   * @return true, if compacted files will be moved to the minimum level
   */
  public boolean changeLevel() {
    return changeLevel(nativeHandle_);
  }

  /**
   * Whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   *
   * @param changeLevel If true, compacted files will be moved to the minimum level
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setChangeLevel(final boolean changeLevel) {
    setChangeLevel(nativeHandle_, changeLevel);
    return this;
  }

  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   * @return The target level for the compacted files
   */
  public int targetLevel() {
    return targetLevel(nativeHandle_);
  }


  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   *
   * @param targetLevel target level for the compacted files
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setTargetLevel(final int targetLevel) {
    setTargetLevel(nativeHandle_, targetLevel);
    return this;
  }

  /**
   * target_path_id for compaction output. Compaction outputs will be placed in options.db_paths[target_path_id].
   *
   * @return target_path_id
   */
  public int targetPathId() {
    return targetPathId(nativeHandle_);
  }

  /**
   * Compaction outputs will be placed in options.db_paths[target_path_id]. Behavior is undefined if target_path_id is
   * out of range.
   *
   * @param targetPathId target path id
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setTargetPathId(final int targetPathId) {
    setTargetPathId(nativeHandle_, targetPathId);
    return this;
  }

  /**
   * Returns the policy for compacting the bottommost level
   * @return The BottommostLevelCompaction policy
   */
  public BottommostLevelCompaction bottommostLevelCompaction() {
    return BottommostLevelCompaction.fromRocksId(bottommostLevelCompaction(nativeHandle_));
  }

  /**
   * Sets the policy for compacting the bottommost level
   *
   * @param bottommostLevelCompaction The policy for compacting the bottommost level
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setBottommostLevelCompaction(final BottommostLevelCompaction bottommostLevelCompaction) {
    setBottommostLevelCompaction(nativeHandle_, bottommostLevelCompaction.getValue());
    return this;
  }

  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   * enter write stall mode. Otherwise, it'll sleep until load is low enough.
   * @return true if compaction will execute immediately
   */
  public boolean allowWriteStall() {
    return allowWriteStall(nativeHandle_);
  }


  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   * enter write stall mode. Otherwise, it'll sleep until load is low enough.
   *
   * @return This CompactRangeOptions
   * @param allowWriteStall true if compaction should execute immediately
   */
  public CompactRangeOptions setAllowWriteStall(final boolean allowWriteStall) {
    setAllowWriteStall(nativeHandle_, allowWriteStall);
    return this;
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   * @return number of subcompactions
   */
  public int maxSubcompactions() {
    return maxSubcompactions(nativeHandle_);
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   *
   * @param maxSubcompactions number of subcompactions
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setMaxSubcompactions(final int maxSubcompactions) {
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
    return this;
  }

  public CompactRangeOptions setFullHistoryTSLow(final Timestamp tsLow) {
    setFullHistoryTSLow(nativeHandle_, tsLow.start, tsLow.range);
    return this;
  }

  public Timestamp fullHistoryTSLow() {
    return fullHistoryTSLow(nativeHandle_);
  }

  public CompactRangeOptions setCanceled(final boolean canceled) {
    setCanceled(nativeHandle_, canceled);
    return this;
  }

  public boolean canceled() {
    return canceled(nativeHandle_);
  }

  private static native long newCompactRangeOptions();
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native boolean exclusiveManualCompaction(final long handle);
  private static native void setExclusiveManualCompaction(
      final long handle, final boolean exclusive_manual_compaction);
  private static native boolean skipMissingBlobFiles(final long handle);
  private static native void setSkipMissingBlobFiles(
      final long handle, final boolean skipMissingBlobFiles);
  private static native boolean changeLevel(final long handle);
  private static native void setChangeLevel(final long handle, final boolean changeLevel);
  private static native int targetLevel(final long handle);
  private static native void setTargetLevel(final long handle, final int targetLevel);
  private static native int targetPathId(final long handle);
  private static native void setTargetPathId(final long handle, final int targetPathId);
  private static native int bottommostLevelCompaction(final long handle);
  private static native void setBottommostLevelCompaction(
      final long handle, final int bottommostLevelCompaction);
  private static native boolean allowWriteStall(final long handle);
  private static native void setAllowWriteStall(final long handle, final boolean allowWriteStall);
  private static native void setMaxSubcompactions(final long handle, final int maxSubcompactions);
  private static native int maxSubcompactions(final long handle);

  private static native void setFullHistoryTSLow(
      final long handle, final long timestampStart, final long timestampRange);

  private static native Timestamp fullHistoryTSLow(final long handle);

  private static native void setCanceled(final long handle, final boolean canceled);

  private static native boolean canceled(final long handle);
}
