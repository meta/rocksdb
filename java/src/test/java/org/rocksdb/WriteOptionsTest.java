// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;

public class WriteOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.
          getPlatformSpecificRandomFactory();

  @Test
  public void writeOptions() {
    try (final WriteOptions writeOptions = new WriteOptions()) {

      writeOptions.setSync(true);
      assertThat(writeOptions.sync()).isTrue();
      writeOptions.setSync(false);
      assertThat(writeOptions.sync()).isFalse();

      writeOptions.setDisableWAL(true);
      assertThat(writeOptions.disableWAL()).isTrue();
      writeOptions.setDisableWAL(false);
      assertThat(writeOptions.disableWAL()).isFalse();


      writeOptions.setIgnoreMissingColumnFamilies(true);
      assertThat(writeOptions.ignoreMissingColumnFamilies()).isTrue();
      writeOptions.setIgnoreMissingColumnFamilies(false);
      assertThat(writeOptions.ignoreMissingColumnFamilies()).isFalse();

      writeOptions.setNoSlowdown(true);
      assertThat(writeOptions.noSlowdown()).isTrue();
      writeOptions.setNoSlowdown(false);
      assertThat(writeOptions.noSlowdown()).isFalse();

      writeOptions.setLowPri(true);
      assertThat(writeOptions.lowPri()).isTrue();
      writeOptions.setLowPri(false);
      assertThat(writeOptions.lowPri()).isFalse();

      writeOptions.setMemtableInsertHintPerBatch(true);
      assertThat(writeOptions.memtableInsertHintPerBatch()).isTrue();
      writeOptions.setMemtableInsertHintPerBatch(false);
      assertThat(writeOptions.memtableInsertHintPerBatch()).isFalse();

      // Test protectionBytesPerKey - supported values: 0, 8
      assertThat(writeOptions.protectionBytesPerKey()).isEqualTo(0);
      writeOptions.setProtectionBytesPerKey(8);
      assertThat(writeOptions.protectionBytesPerKey()).isEqualTo(8);
      writeOptions.setProtectionBytesPerKey(0);
      assertThat(writeOptions.protectionBytesPerKey()).isEqualTo(0);
    }
  }

  @Test
  public void copyConstructor() {
    final WriteOptions origOpts = new WriteOptions();
    origOpts.setDisableWAL(rand.nextBoolean());
    origOpts.setIgnoreMissingColumnFamilies(rand.nextBoolean());
    origOpts.setSync(rand.nextBoolean());
    origOpts.setMemtableInsertHintPerBatch(true);
    final WriteOptions copyOpts = new WriteOptions(origOpts);
    assertThat(origOpts.disableWAL()).isEqualTo(copyOpts.disableWAL());
    assertThat(origOpts.ignoreMissingColumnFamilies()).isEqualTo(
            copyOpts.ignoreMissingColumnFamilies());
    assertThat(origOpts.sync()).isEqualTo(copyOpts.sync());
    assertThat(origOpts.memtableInsertHintPerBatch())
        .isEqualTo(copyOpts.memtableInsertHintPerBatch());
  }
}
