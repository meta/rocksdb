//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

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
   * True iff the option to verify the checksums of each block of the
   * external SST file before ingestion has been set.
   *
   * @return true iff the option to verify the checksums of each block of the
   * external SST file before ingestion has been set.
   */
  public boolean verifyChecksumsBeforeIngest() {
    return verifyChecksumsBeforeIngest(nativeHandle_);
  }

  /**
   * Set to true if you would like to verify the checksums of each block of the
   * external SST file before ingestion.
   * <p>
   * Warning: setting this to true causes slowdown in file ingestion because
   * the external SST file has to be read.
   * <p>
   * Default: false
   *
   * @param verifyChecksumsBeforeIngest true if you would like to verify the checksums of each block
   *     of the
   * external SST file before ingestion.
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setVerifyChecksumsBeforeIngest(
      final boolean verifyChecksumsBeforeIngest) {
    setVerifyChecksumsBeforeIngest(nativeHandle_, verifyChecksumsBeforeIngest);
    return this;
  }

  /**
   * When verify_checksums_before_ingest = true, RocksDB uses default
   * readahead setting to scan the file while verifying checksums before
   * ingestion
   * <p>
   * Users can override the default value using this option.
   * Using a large readahead size (&gt; 2MB) can typically improve the performance
   * of forward iteration on spinning disks.
   *
   * @return the current value of readahead size (0 if it has not been set)
   */
  public long verifyChecksumsReadaheadSize() {
    return verifyChecksumsReadaheadSize(nativeHandle_);
  }

  /**
   * When verify_checksums_before_ingest = true, RocksDB uses default
   * readahead setting to scan the file while verifying checksums before
   * ingestion
   * <p>
   * Users can override the default value using this option.
   * Using a large readahead size (&gt; 2MB) can typically improve the performance
   * of forward iteration on spinning disks.
   *
   * @param verifyChecksumsReadaheadSize the value of readahead size to set
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setVerifyChecksumsReadaheadSize(
      final long verifyChecksumsReadaheadSize) {
    setVerifyChecksumsReadaheadSize(nativeHandle_, verifyChecksumsReadaheadSize);
    return this;
  }

  /**
   * Set to TRUE if user wants to verify the sst file checksum of ingested
   * files. The DB checksum function will generate the checksum of each
   * ingested file (if file_checksum_gen_factory is set) and compare the
   * checksum function name and checksum with the ingested checksum information.
   *
   * @return true iff the option to verify the sst file checksum of ingested files is set
   */
  public boolean verifyFileChecksum() {
    return verifyFileChecksum(nativeHandle_);
  }

  /**
   * Set to TRUE if user wants to verify the sst file checksum of ingested
   * files. The DB checksum function will generate the checksum of each
   * ingested file (if file_checksum_gen_factory is set) and compare the
   * checksum function name and checksum with the ingested checksum information.
   * <p>
   * If this option is set to True: 1) if DB does not enable checksum
   * (file_checksum_gen_factory == nullptr), the ingested checksum information
   * will be ignored; 2) If DB enable the checksum function, we calculate the
   * sst file checksum after the file is moved or copied and compare the
   * checksum and checksum name. If checksum or checksum function name does
   * not match, ingestion will be failed. If the verification is successful,
   * checksum and checksum function name will be stored in Manifest.
   * If this option is set to FALSE, 1) if DB does not enable checksum,
   * the ingested checksum information will be ignored; 2) if DB enable the
   * checksum, we only verify the ingested checksum function name and we
   * trust the ingested checksum. If the checksum function name matches, we
   * store the checksum in Manifest. DB does not calculate the checksum during
   * ingestion. However, if no checksum information is provided with the
   * ingested files, DB will generate the checksum and store in the Manifest.
   *
   * @param verifyFileChecksum true iff user wants to verify the sst file checksum of ingested files
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setVerifyFileChecksum(final boolean verifyFileChecksum) {
    setVerifyFileChecksum(nativeHandle_, verifyFileChecksum);
    return this;
  }

  /**
   * Set to TRUE if user wants file to be ingested to the last level. An
   * error of Status::TryAgain() will be returned if a file cannot fit in the
   * last level when calling {@link RocksDB#ingestExternalFile(List, IngestExternalFileOptions)}
   * or {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}.
   * The user should clear
   * the last level in the overlapping range before re-attempt.
   * <p>
   * @return true iff failIfNotLastLevel option has been set
   */
  public boolean failIfNotLastLevel() {
    return failIfNotLastLevel(nativeHandle_);
  }

  /**
   * Set to TRUE if user wants file to be ingested to the last level. An
   * error of Status::TryAgain() will be returned if a file cannot fit in the
   * last level when calling
   * DB::IngestExternalFile()/DB::IngestExternalFiles(). The user should clear
   * the last level in the overlapping range before re-attempt.
   * <p>
   * ingestBehind takes precedence over failIfNotLastLevel.
   * <p>
   * This method is named for "last" instead of "bottommost" (in the C++ API)
   * as that API notes that "bottommost" is obsolete/confusing terminology to refer to last level
   *
   * @param failIfNotLastLevel true iff user wants file to be ingested to the last level
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setFailIfNotLastLevel(final boolean failIfNotLastLevel) {
    setFailIfNotLastLevel(nativeHandle_, failIfNotLastLevel);
    return this;
  }

  /**
   * True if the files will be linked instead of copying them.
   * Same as moveFiles except that input files will NOT be unlinked.
   * Only one of `moveFiles` and `linkFiles` can be set at the same time.
   *
   * @return true if files will be moved
   */
  public boolean linkFiles() {
    return linkFiles(nativeHandle_);
  }

  /**
   * Can be set to true to link the files instead of copying them.
   * Same as moveFiles except that input files will NOT be unlinked.
   * Only one of `moveFiles` and `linkFiles` can be set at the same time.
   *
   * @param linkFiles true if files should be linked instead of copied
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setLinkFiles(final boolean linkFiles) {
    setLinkFiles(nativeHandle_, linkFiles);
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
  private static native boolean verifyChecksumsBeforeIngest(final long handle);
  private static native void setVerifyChecksumsBeforeIngest(
      final long handle, final boolean verifyChecksumsBeforeIngest);
  private static native long verifyChecksumsReadaheadSize(final long handle);
  private static native void setVerifyChecksumsReadaheadSize(
      final long handle, final long verifyChecksumsReadaheadSize);
  private static native boolean verifyFileChecksum(final long handle);
  private static native void setVerifyFileChecksum(
      final long handle, final boolean verifyFileChecksum);
  private static native boolean failIfNotLastLevel(final long handle);
  private static native void setFailIfNotLastLevel(
      final long handle, final boolean failIfNotLastLevel);

  private static native boolean linkFiles(final long handle);
  private static native void setLinkFiles(final long handle, final boolean linkFiles);
}
