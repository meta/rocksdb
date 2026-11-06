// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

public class RibbonFilterTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void ribbonFilter() {
    // Test default constructor
    try (final RibbonFilter ribbonFilter = new RibbonFilter()) {
      assertThat(ribbonFilter).isNotNull();
    }
  }

  @Test
  public void ribbonFilterWithBitsPerKey() {
    // Test constructor with bits per key
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0)) {
      assertThat(ribbonFilter).isNotNull();
    }
  }

  @Test
  public void ribbonFilterWithHybridMode() {
    // Test constructor with hybrid Bloom/Ribbon mode
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0, 1)) {
      assertThat(ribbonFilter).isNotNull();
    }
  }

  @Test
  public void ribbonFilterAlwaysRibbon() {
    // Test always use Ribbon (bloomBeforeLevel = -1)
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0, -1)) {
      assertThat(ribbonFilter).isNotNull();
    }
  }

  @Test
  public void ribbonFilterEquality() {
    // Test equals and hashCode
    try (final RibbonFilter filter1 = new RibbonFilter(10.0, 1);
        final RibbonFilter filter2 = new RibbonFilter(10.0, 1);
        final RibbonFilter filter3 = new RibbonFilter(10.0, 2)) {
      assertThat(filter1).isEqualTo(filter2);
      assertThat(filter1.hashCode()).isEqualTo(filter2.hashCode());
      assertThat(filter1).isNotEqualTo(filter3);
    }
  }

  @Test
  public void ribbonFilterWithTableConfig() {
    // Test using RibbonFilter with BlockBasedTableConfig
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0);
        final Options options = new Options()) {
      final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setFilterPolicy(ribbonFilter);
      options.setTableFormatConfig(tableConfig);
      assertThat(options).isNotNull();
    }
  }
}
