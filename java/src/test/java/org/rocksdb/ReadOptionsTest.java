// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ReadOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void altConstructor() {
    try (final ReadOptions opt = new ReadOptions(true, true)) {
      assertThat(opt.verifyChecksums()).isTrue();
      assertThat(opt.fillCache()).isTrue();
    }
  }

  @Test
  public void copyConstructor() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setVerifyChecksums(false);
      opt.setFillCache(false);
      opt.setIterateUpperBound(buildRandomSlice());
      opt.setIterateLowerBound(buildRandomSlice());
      opt.setTimestamp(buildRandomSlice());
      opt.setIterStartTs(buildRandomSlice());
      try (final ReadOptions other = new ReadOptions(opt)) {
        assertThat(opt.verifyChecksums()).isEqualTo(other.verifyChecksums());
        assertThat(opt.fillCache()).isEqualTo(other.fillCache());
        assertThat(Arrays.equals(opt.iterateUpperBound().data(), other.iterateUpperBound().data())).isTrue();
        assertThat(Arrays.equals(opt.iterateLowerBound().data(), other.iterateLowerBound().data())).isTrue();
        assertThat(Arrays.equals(opt.timestamp().data(), other.timestamp().data())).isTrue();
        assertThat(Arrays.equals(opt.iterStartTs().data(), other.iterStartTs().data())).isTrue();
      }
    }
  }

  @Test
  public void verifyChecksum() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksums(boolValue);
      assertThat(opt.verifyChecksums()).isEqualTo(boolValue);
    }
  }

  @Test
  public void fillCache() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setFillCache(boolValue);
      assertThat(opt.fillCache()).isEqualTo(boolValue);
    }
  }

  @Test
  public void tailing() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setTailing(boolValue);
      assertThat(opt.tailing()).isEqualTo(boolValue);
    }
  }

  @Test
  public void snapshot() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setSnapshot(null);
      assertThat(opt.snapshot()).isNull();
    }
  }

  @Test
  public void readTier() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setReadTier(ReadTier.BLOCK_CACHE_TIER);
      assertThat(opt.readTier()).isEqualTo(ReadTier.BLOCK_CACHE_TIER);
    }
  }

  @Test
  public void totalOrderSeek() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setTotalOrderSeek(true);
      assertThat(opt.totalOrderSeek()).isTrue();
    }
  }

  @Test
  public void prefixSameAsStart() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setPrefixSameAsStart(true);
      assertThat(opt.prefixSameAsStart()).isTrue();
    }
  }

  @Test
  public void pinData() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setPinData(true);
      assertThat(opt.pinData()).isTrue();
    }
  }

  @Test
  public void backgroundPurgeOnIteratorCleanup() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setBackgroundPurgeOnIteratorCleanup(true);
      assertThat(opt.backgroundPurgeOnIteratorCleanup()).isTrue();
    }
  }

  @Test
  public void readaheadSize() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final int intValue = rand.nextInt(2147483647);
      opt.setReadaheadSize(intValue);
      assertThat(opt.readaheadSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void ignoreRangeDeletions() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setIgnoreRangeDeletions(true);
      assertThat(opt.ignoreRangeDeletions()).isTrue();
    }
  }

  @Test
  public void iterateUpperBound() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Slice upperBound = buildRandomSlice();
      opt.setIterateUpperBound(upperBound);
      assertThat(Arrays.equals(upperBound.data(), opt.iterateUpperBound().data())).isTrue();
      opt.setIterateUpperBound(null);
      assertThat(opt.iterateUpperBound()).isNull();
    }
  }

  @Test
  public void iterateUpperBoundNull() {
    try (final ReadOptions opt = new ReadOptions()) {
      assertThat(opt.iterateUpperBound()).isNull();
    }
  }

  @Test
  public void iterateLowerBound() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Slice lowerBound = buildRandomSlice();
      opt.setIterateLowerBound(lowerBound);
      assertThat(Arrays.equals(lowerBound.data(), opt.iterateLowerBound().data())).isTrue();
      opt.setIterateLowerBound(null);
      assertThat(opt.iterateLowerBound()).isNull();
    }
  }

  @Test
  public void iterateLowerBoundNull() {
    try (final ReadOptions opt = new ReadOptions()) {
      assertThat(opt.iterateLowerBound()).isNull();
    }
  }

  @Test
  public void tableFilter() {
    try (final ReadOptions opt = new ReadOptions();
         final AbstractTableFilter allTablesFilter = new AllTablesFilter()) {
      opt.setTableFilter(allTablesFilter);
    }
  }

  @Test
  public void autoPrefixMode() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setAutoPrefixMode(true);
      assertThat(opt.autoPrefixMode()).isTrue();
    }
  }

  @Test
  public void timestamp() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Slice timestamp = buildRandomSlice();
      opt.setTimestamp(timestamp);
      assertThat(Arrays.equals(timestamp.data(), opt.timestamp().data())).isTrue();
      opt.setTimestamp(null);
      assertThat(opt.timestamp()).isNull();
    }
  }

  @Test
  public void iterStartTs() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Slice itertStartTsSlice = buildRandomSlice();
      opt.setIterStartTs(itertStartTsSlice);
      assertThat(Arrays.equals(itertStartTsSlice.data(), opt.iterStartTs().data())).isTrue();
      opt.setIterStartTs(null);
      assertThat(opt.iterStartTs()).isNull();
    }
  }

  @Test
  public void deadline() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setDeadline(1999L);
      assertThat(opt.deadline()).isEqualTo(1999L);
    }
  }

  @Test
  public void ioTimeout() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setIoTimeout(34555L);
      assertThat(opt.ioTimeout()).isEqualTo(34555L);
    }
  }

  @Test
  public void valueSizeSoftLimit() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setValueSizeSoftLimit(12134324L);
      assertThat(opt.valueSizeSoftLimit()).isEqualTo(12134324L);
    }
  }

  @Test
  public void asyncIo() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setAsyncIo(true);
      assertThat(opt.asyncIo()).isTrue();
    }
  }

  @Test
  public void failSetVerifyChecksumUninitialized() {
    failOperationWithClosedOptions(options -> options.setVerifyChecksums(true));
  }

  @Test
  public void failVerifyChecksumUninitialized() {
    failOperationWithClosedOptions(ReadOptions::verifyChecksums);
  }

  @Test
  public void failSetFillCacheUninitialized() {
    failOperationWithClosedOptions(options -> options.setFillCache(true));
  }

  @Test
  public void failFillCacheUninitialized() {
    failOperationWithClosedOptions(ReadOptions::fillCache);
  }

  @Test
  public void failSetTailingUninitialized() {
    failOperationWithClosedOptions(options -> options.setTailing(true));
  }

  @Test
  public void failTailingUninitialized() {
    failOperationWithClosedOptions(ReadOptions::tailing);
  }

  @Test
  public void failSetSnapshotUninitialized() {
    failOperationWithClosedOptions(options -> options.setSnapshot(null));
  }

  @Test
  public void failSnapshotUninitialized() {
    failOperationWithClosedOptions(ReadOptions::snapshot);
  }

  @Test
  public void failSetIterateUpperBoundUninitialized() {
    failOperationWithClosedOptions(options -> options.setIterateUpperBound(null));
  }

  @Test
  public void failIterateUpperBoundUninitialized() {
    failOperationWithClosedOptions(ReadOptions::iterateUpperBound);
  }

  @Test
  public void failSetIterateLowerBoundUninitialized() {
    failOperationWithClosedOptions(options -> options.setIterateLowerBound(null));
  }

  @Test
  public void failIterateLowerBoundUninitialized() {
    failOperationWithClosedOptions(ReadOptions::iterateLowerBound);
  }

  private ReadOptions setupUninitializedReadOptions(final ExpectedException exception) {
    final ReadOptions readOptions = new ReadOptions();
    readOptions.close();
    exception.expect(AssertionError.class);
    return readOptions;
  }

  private void failOperationWithClosedOptions(Consumer<ReadOptions> operation) {
    try (final ReadOptions readOptions = new ReadOptions()) {
      readOptions.close();
      assertThatThrownBy(() -> operation.accept(readOptions)).isInstanceOf(AssertionError.class);
    }
  }

  private Slice buildRandomSlice() {
    final Random rand = new Random();
    final byte[] sliceBytes = new byte[rand.nextInt(100) + 1];
    rand.nextBytes(sliceBytes);
    return new Slice(sliceBytes);
  }

  private static class AllTablesFilter extends AbstractTableFilter {
    @Override
    public boolean filter(final TableProperties tableProperties) {
      return true;
    }
  }
}
