// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.BeforeClass;
import org.junit.Test;

public class OptimisticTransactionOptionsDBTest {
  private static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @BeforeClass
  public static void beforeAll() {
    RocksDB.loadLibrary();
  }

  @Test
  public void lockBucketCount() {
    try (final OptimisticTransactionDBOptions options = new OptimisticTransactionDBOptions()) {
      final long lockBucketCount = rand.nextInt(Integer.MAX_VALUE) + 1;
      options.setOccLockBuckets(lockBucketCount);
      assertThat(options.getOccLockBuckets()).isEqualTo(lockBucketCount);
    }
  }

  @Test
  public void setOccValidationPolicy() {
    try (final OptimisticTransactionDBOptions options = new OptimisticTransactionDBOptions()) {
      options.setOccValidationPolicy(OccValidationPolicy.VALIDATE_SERIAL);
      assertThat(options.occValidationPolicy()).isEqualTo(OccValidationPolicy.VALIDATE_SERIAL);

      options.setOccValidationPolicy(OccValidationPolicy.VALIDATE_PARALLEL);
      assertThat(options.occValidationPolicy()).isEqualTo(OccValidationPolicy.VALIDATE_PARALLEL);
    }
  }
}
