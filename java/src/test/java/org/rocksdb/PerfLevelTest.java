//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.rocksdb.PerfLevel.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class PerfLevelTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  List<ColumnFamilyDescriptor> cfDescriptors;
  List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
  RocksDB db;

  @Before
  public void before() throws RocksDBException {
    cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    db = RocksDB.open(
        options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
  }

  @After
  public void after() {
    for (ColumnFamilyDescriptor cfDescriptor : cfDescriptors) {
      cfDescriptor.close();
    }
    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.close();
    }
    db.close();
  }
  @Test
  public void testForInvalidValues() {
    assertThatThrownBy(() -> db.setPerfLevel(UNINITIALIZED))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> db.setPerfLevel(OUT_OF_BOUNDS))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testAllPerfLevels() {
    for (PerfLevel level : new PerfLevel[] {DISABLE, ENABLE_COUNT, ENABLE_TIME_EXCEPT_FOR_MUTEX,
             ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX, ENABLE_TIME}) {
      db.setPerfLevel(level);
      assertThat(db.getPerfLevel()).isEqualTo(level);
    }
    db.setPerfLevel(DISABLE);
  }
}
