// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.EnumSet;

/**
 * Statistics to analyze the performance of a db. Pointer for statistics object
 * is managed by Options class.
 */
public class Statistics extends RocksObject {

  public Statistics() {
    super(newStatisticsInstance());
  }

  public Statistics(final Statistics otherStatistics) {
    super(newStatistics(otherStatistics.nativeHandle_));
  }

  @SuppressWarnings("PMD.LooseCoupling")
  public Statistics(final EnumSet<HistogramType> ignoreHistograms) {
    super(newStatisticsInstance(toArrayValues(ignoreHistograms)));
  }

  @SuppressWarnings("PMD.LooseCoupling")
  public Statistics(
      final EnumSet<HistogramType> ignoreHistograms, final Statistics otherStatistics) {
    super(newStatistics(toArrayValues(ignoreHistograms), otherStatistics.nativeHandle_));
  }

  /**
   * Intentionally package-private.
   * <p>
   * Used from {@link DBOptions#statistics()}
   *
   * @param existingStatisticsHandle The C++ pointer to an existing statistics object
   */
  Statistics(final long existingStatisticsHandle) {
    super(existingStatisticsHandle);
  }

  @SuppressWarnings({"PMD.LooseCoupling", "PMD.AssignmentInOperand"})
  private static byte[] toArrayValues(final EnumSet<HistogramType> histogramTypes) {
    final byte[] values = new byte[histogramTypes.size()];
    int i = 0;
    for(final HistogramType histogramType : histogramTypes) {
      values[i++] = histogramType.getValue();
    }
    return values;
  }

  /**
   * Gets the current stats level.
   *
   * @return The stats level.
   */
  public StatsLevel statsLevel() {
    return StatsLevel.getStatsLevel(statsLevel(nativeHandle_));
  }

  /**
   * Sets the stats level.
   *
   * @param statsLevel The stats level to set.
   */
  public void setStatsLevel(final StatsLevel statsLevel) {
    setStatsLevel(nativeHandle_, statsLevel.getValue());
  }

  /**
   * Get the count for a ticker.
   *
   * @param tickerType The ticker to get the count for
   *
   * @return The count for the ticker
   */
  public long getTickerCount(final TickerType tickerType) {
    assert(isOwningHandle());
    return getTickerCount(nativeHandle_, tickerType.getValue());
  }

  /**
   * Get the count for a ticker and reset the tickers count.
   *
   * @param tickerType The ticker to get the count for
   *
   * @return The count for the ticker
   */
  public long getAndResetTickerCount(final TickerType tickerType) {
    assert(isOwningHandle());
    return getAndResetTickerCount(nativeHandle_, tickerType.getValue());
  }

  /**
   * Gets the histogram data for a particular histogram.
   *
   * @param histogramType The histogram to retrieve the data for
   *
   * @return The histogram data
   */
  public HistogramData getHistogramData(final HistogramType histogramType) {
    assert(isOwningHandle());
    return getHistogramData(nativeHandle_, histogramType.getValue());
  }

  /**
   * Gets a string representation of a particular histogram.
   *
   * @param histogramType The histogram to retrieve the data for
   *
   * @return A string representation of the histogram data
   */
  public String getHistogramString(final HistogramType histogramType) {
    assert(isOwningHandle());
    return getHistogramString(nativeHandle_, histogramType.getValue());
  }

  /**
   * Resets all ticker and histogram stats.
   *
   * @throws RocksDBException if an error occurs when resetting the statistics.
   */
  public void reset() throws RocksDBException {
    assert(isOwningHandle());
    reset(nativeHandle_);
  }

  /**
   * String representation of the statistic object.
   */
  @Override
  public String toString() {
    assert(isOwningHandle());
    return toString(nativeHandle_);
  }

  private static long newStatisticsInstance() {
    RocksDB.loadLibrary();
    return newStatistics();
  }
  private static native long newStatistics();
  private static native long newStatistics(final long otherStatisticsHandle);
  private static long newStatisticsInstance(final byte[] ignoreHistograms) {
    RocksDB.loadLibrary();
    return newStatistics(ignoreHistograms);
  }
  private static native long newStatistics(final byte[] ignoreHistograms);
  private static native long newStatistics(
      final byte[] ignoreHistograms, final long otherStatisticsHandle);

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native byte statsLevel(final long handle);
  private static native void setStatsLevel(final long handle, final byte statsLevel);
  private static native long getTickerCount(final long handle, final byte tickerType);
  private static native long getAndResetTickerCount(final long handle, final byte tickerType);
  private static native HistogramData getHistogramData(final long handle, final byte histogramType);
  private static native String getHistogramString(final long handle, final byte histogramType);
  private static native void reset(final long nativeHandle) throws RocksDBException;
  private static native String toString(final long nativeHandle);
}
