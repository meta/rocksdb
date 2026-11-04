//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestExternalFileOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE
      = new RocksNativeLibraryResource();

  public static final Random rand =
      PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createExternalSstFileInfoWithoutParameters() {
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions()) {
      assertThat(options).isNotNull();
    }
  }

  @Test
  public void createExternalSstFileInfoWithParameters() {
    final boolean moveFiles = rand.nextBoolean();
    final boolean snapshotConsistency = rand.nextBoolean();
    final boolean allowGlobalSeqNo = rand.nextBoolean();
    final boolean allowBlockingFlush = rand.nextBoolean();
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions(moveFiles, snapshotConsistency,
        allowGlobalSeqNo, allowBlockingFlush)) {
      assertThat(options).isNotNull();
      assertThat(options.moveFiles()).isEqualTo(moveFiles);
      assertThat(options.snapshotConsistency()).isEqualTo(snapshotConsistency);
      assertThat(options.allowGlobalSeqNo()).isEqualTo(allowGlobalSeqNo);
      assertThat(options.allowBlockingFlush()).isEqualTo(allowBlockingFlush);
    }
  }

  @Test
  public void moveFiles() {
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions()) {
      final boolean moveFiles = rand.nextBoolean();
      options.setMoveFiles(moveFiles);
      assertThat(options.moveFiles()).isEqualTo(moveFiles);
    }
  }

  @Test
  public void snapshotConsistency() {
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions()) {
      final boolean snapshotConsistency = rand.nextBoolean();
      options.setSnapshotConsistency(snapshotConsistency);
      assertThat(options.snapshotConsistency()).isEqualTo(snapshotConsistency);
    }
  }

  @Test
  public void allowGlobalSeqNo() {
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions()) {
      final boolean allowGlobalSeqNo = rand.nextBoolean();
      options.setAllowGlobalSeqNo(allowGlobalSeqNo);
      assertThat(options.allowGlobalSeqNo()).isEqualTo(allowGlobalSeqNo);
    }
  }

  @Test
  public void allowBlockingFlush() {
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions()) {
      final boolean allowBlockingFlush = rand.nextBoolean();
      options.setAllowBlockingFlush(allowBlockingFlush);
      assertThat(options.allowBlockingFlush()).isEqualTo(allowBlockingFlush);
    }
  }

  @Test
  public void ingestBehind() {
    try (final IngestExternalFileOptions options =
             new IngestExternalFileOptions()) {
      assertThat(options.ingestBehind()).isFalse();
      options.setIngestBehind(true);
      assertThat(options.ingestBehind()).isTrue();
    }
  }

  @Test
  public void verifyChecksumsBeforeIngest() {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      assertThat(options.verifyChecksumsBeforeIngest()).isFalse();
      assertThat(options.setVerifyChecksumsBeforeIngest(true)).isEqualTo(options);
      assertThat(options.verifyChecksumsBeforeIngest()).isTrue();
      assertThat(options.setVerifyChecksumsBeforeIngest(false)).isEqualTo(options);
      assertThat(options.verifyChecksumsBeforeIngest()).isFalse();
    }
  }
  @Test
  public void verifyChecksumsReadaheadSize() {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      assertThat(options.verifyChecksumsReadaheadSize()).isEqualTo(0L);
      assertThat(options.setVerifyChecksumsReadaheadSize(4096 * 1024L)).isEqualTo(options);
      assertThat(options.verifyChecksumsReadaheadSize()).isEqualTo(4096 * 1024L);
      assertThat(options.setVerifyChecksumsReadaheadSize(0)).isEqualTo(options);
      assertThat(options.verifyChecksumsReadaheadSize()).isEqualTo(0L);
    }
  }
  @Test
  public void verifyFileChecksum() {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      assertThat(options.verifyFileChecksum()).isTrue();
      assertThat(options.setVerifyFileChecksum(false)).isEqualTo(options);
      assertThat(options.verifyFileChecksum()).isFalse();
      assertThat(options.setVerifyFileChecksum(true)).isEqualTo(options);
      assertThat(options.verifyFileChecksum()).isTrue();
    }
  }

  @Test
  public void failIfNotLastLevel() {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      assertThat(options.failIfNotLastLevel()).isFalse();
      assertThat(options.setFailIfNotLastLevel(true)).isEqualTo(options);
      assertThat(options.failIfNotLastLevel()).isTrue();
      assertThat(options.setFailIfNotLastLevel(false)).isEqualTo(options);
      assertThat(options.failIfNotLastLevel()).isFalse();
    }
  }

  @Test
  public void linkFiles() {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      assertThat(options.linkFiles()).isFalse();
      assertThat(options.setLinkFiles(true)).isEqualTo(options);
      assertThat(options.linkFiles()).isTrue();
      assertThat(options.setLinkFiles(false)).isEqualTo(options);
      assertThat(options.linkFiles()).isFalse();
    }
  }
}
