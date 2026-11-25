package org.rocksdb;
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.util.List;

/**
 * IngestExternalFileOptions is used by
 * {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}.
 */
public class IngestExternalFileOptions extends RocksObject {

  public IngestExternalFileOptions() {
    super(newIngestExternalFileOptions());
  }

  /**
   * @param moveFiles {@link #setMoveFiles(boolean)}
   * @param snapshotConsistency {@link #setSnapshotConsistency(boolean)}
   * @param allowGlobalSeqNo {@link #setAllowGlobalSeqNo(boolean)}
   * @param allowBlockingFlush {@link #setAllowBlockingFlush(boolean)}
   */
  public IngestExternalFileOptions(final boolean moveFiles,
      final boolean snapshotConsistency, final boolean allowGlobalSeqNo,
      final boolean allowBlockingFlush) {
    super(newIngestExternalFileOptions(moveFiles, snapshotConsistency,
        allowGlobalSeqNo, allowBlockingFlush));
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @return true if files will be moved
   */
  public boolean moveFiles() {
    return moveFiles(nativeHandle_);
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @param moveFiles true if files should be moved instead of copied
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setMoveFiles(final boolean moveFiles) {
    setMoveFiles(nativeHandle_, moveFiles);
    return this;
  }

  /**
   * Returns true if ingestion will fall back to copying when hard linking
   * fails.
   *
   * @return true if ingestion will fall back to copying when hard linking
   *     fails.
   */
  public boolean failedMoveFallBackToCopy() {
    return failedMoveFallBackToCopy(nativeHandle_);
  }

  /**
   * If set to true, ingestion falls back to copying when hard linking fails.
   * This applies when {@link #moveFiles()} is true.
   *
   * @param failedMoveFallBackToCopy true if ingestion should fall back to
   *     copying when hard linking fails.
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setFailedMoveFallBackToCopy(
      final boolean failedMoveFallBackToCopy) {
    setFailedMoveFallBackToCopy(nativeHandle_, failedMoveFallBackToCopy);
    return this;
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @return true if snapshot consistency is assured
   */
  public boolean snapshotConsistency() {
    return snapshotConsistency(nativeHandle_);
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @param snapshotConsistency true if snapshot consistency is required
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setSnapshotConsistency(
      final boolean snapshotConsistency) {
    setSnapshotConsistency(nativeHandle_, snapshotConsistency);
    return this;
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @return true if global seq numbers are assured
   */
  public boolean allowGlobalSeqNo() {
    return allowGlobalSeqNo(nativeHandle_);
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @param allowGlobalSeqNo true if global seq numbers are required
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setAllowGlobalSeqNo(
      final boolean allowGlobalSeqNo) {
    setAllowGlobalSeqNo(nativeHandle_, allowGlobalSeqNo);
    return this;
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @return true if blocking flushes may occur
   */
  public boolean allowBlockingFlush() {
    return allowBlockingFlush(nativeHandle_);
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @param allowBlockingFlush true if blocking flushes are allowed
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setAllowBlockingFlush(
      final boolean allowBlockingFlush) {
    setAllowBlockingFlush(nativeHandle_, allowBlockingFlush);
    return this;
  }

  /**
   * Returns true if duplicate keys in the file being ingested are
   * to be skipped rather than overwriting existing data under that key.
   *
   * @return true if duplicate keys in the file being ingested are to be
   *     skipped, false otherwise.
   */
  public boolean ingestBehind() {
    return ingestBehind(nativeHandle_);
  }

  /**
   * Set to true if you would like duplicate keys in the file being ingested
   * to be skipped rather than overwriting existing data under that key.
   * <p>
   * Usecase: back-fill of some historical data in the database without
   * over-writing existing newer version of data.
   * <p>
   * This option could only be used if the DB has been running
   * with DBOptions#allowIngestBehind() == true since the dawn of time.
   * <p>
   * All files will be ingested at the bottommost level with seqno=0.
   * <p>
   * Default: false
   *
   * @param ingestBehind true if you would like duplicate keys in the file being
   *     ingested to be skipped.
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setIngestBehind(final boolean ingestBehind) {
    setIngestBehind(nativeHandle_, ingestBehind);
    return this;
  }

  /**
   * Returns true if ingestion rejects files that are already too old for
   * the column family.
   * <p>
   * See {@link #setRejectTooOldFile(boolean)}.
   *
   * @return true if too-old files are rejected.
   */
  public boolean rejectTooOldFile() {
    return rejectTooOldFile(nativeHandle_);
  }

  /**
   * If true, ingestion fails (and no files are ingested) if any file is
   * already too old for the column family to keep without recompaction or
   * deletion. A file is too old if the age of its contents, from the creation
   * time recorded in the file, exceeds the effective
   * periodic_compaction_seconds (leveled or universal compaction), or if the
   * age of its newest data, as estimated by FIFO TTL compaction (from the
   * newest key time recorded in the file, or else its oldest ancestor time, or
   * else its creation time), exceeds ttl (FIFO compaction). Nothing is
   * rejected if the applicable periodic_compaction_seconds or ttl is 0
   * (disabled), and a file without the recorded time is not rejected.
   * <p>
   * Default: false
   *
   * @param rejectTooOldFile true to reject files that are too old
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setRejectTooOldFile(final boolean rejectTooOldFile) {
    setRejectTooOldFile(nativeHandle_, rejectTooOldFile);
    return this;
  }

  private static native long newIngestExternalFileOptions();
  private static native long newIngestExternalFileOptions(final boolean moveFiles,
      final boolean snapshotConsistency, final boolean allowGlobalSeqNo,
      final boolean allowBlockingFlush);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);

  private static native boolean moveFiles(final long handle);
  private static native void setMoveFiles(final long handle, final boolean move_files);
  private static native boolean failedMoveFallBackToCopy(final long handle);
  private static native void setFailedMoveFallBackToCopy(
      final long handle, final boolean failedMoveFallBackToCopy);
  private static native boolean snapshotConsistency(final long handle);
  private static native void setSnapshotConsistency(
      final long handle, final boolean snapshotConsistency);
  private static native boolean allowGlobalSeqNo(final long handle);
  private static native void setAllowGlobalSeqNo(final long handle, final boolean allowGloablSeqNo);
  private static native boolean allowBlockingFlush(final long handle);
  private static native void setAllowBlockingFlush(
      final long handle, final boolean allowBlockingFlush);
  private static native boolean ingestBehind(final long handle);
  private static native void setIngestBehind(final long handle, final boolean ingestBehind);
  private static native boolean rejectTooOldFile(final long handle);
  private static native void setRejectTooOldFile(final long handle, final boolean rejectTooOldFile);
}
