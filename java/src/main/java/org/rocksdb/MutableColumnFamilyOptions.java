// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;

public class MutableColumnFamilyOptions extends AbstractMutableOptions {
  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   * <p>
   * See {@link #builder()} and {@link #parse(String)}.
   */
  private MutableColumnFamilyOptions(final String[] keys,
      final String[] values) {
    super(keys, values);
  }

  /**
   * Creates a builder which allows you
   * to set MutableColumnFamilyOptions in a fluent
   * manner
   *
   * @return A builder for MutableColumnFamilyOptions
   */
  public static MutableColumnFamilyOptionsBuilder builder() {
    return new MutableColumnFamilyOptionsBuilder();
  }

  /**
   * Parses a String representation of MutableColumnFamilyOptions
   * <p>
   * The format is: key1=value1;key2=value2;key3=value3 etc
   * <p>
   * For int[] values, each int should be separated by a colon, e.g.
   * <p>
   * key1=value1;intArrayKey1=1:2:3
   *
   * @param str The string representation of the mutable column family options
   * @param ignoreUnknown what to do if the key is not one of the keys we expect
   *
   * @return A builder for the mutable column family options
   */
  public static MutableColumnFamilyOptionsBuilder parse(
      final String str, final boolean ignoreUnknown) {
    Objects.requireNonNull(str);

    final List<OptionString.Entry> parsedOptions = OptionString.Parser.parse(str);
    return new MutableColumnFamilyOptionsBuilder().fromParsed(parsedOptions, ignoreUnknown);
  }

  public static MutableColumnFamilyOptionsBuilder parse(final String str) {
    return parse(str, false);
  }

  private interface MutableColumnFamilyOptionKey extends MutableOptionKey {}

  public enum MemtableOption implements MutableColumnFamilyOptionKey {
    write_buffer_size(ValueType.LONG),
    arena_block_size(ValueType.LONG),
    memtable_prefix_bloom_size_ratio(ValueType.DOUBLE),
    memtable_whole_key_filtering(ValueType.BOOLEAN),
    @Deprecated memtable_prefix_bloom_bits(ValueType.INT),
    @Deprecated memtable_prefix_bloom_probes(ValueType.INT),
    memtable_huge_page_size(ValueType.LONG),
    max_successive_merges(ValueType.LONG),
    @Deprecated filter_deletes(ValueType.BOOLEAN),
    max_write_buffer_number(ValueType.INT),
    inplace_update_num_locks(ValueType.LONG),
    experimental_mempurge_threshold(ValueType.DOUBLE),
    min_tombstones_for_range_conversion(ValueType.INT);

    private final ValueType valueType;
    MemtableOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum CompactionOption implements MutableColumnFamilyOptionKey {
    disable_auto_compactions(ValueType.BOOLEAN),
    soft_pending_compaction_bytes_limit(ValueType.LONG),
    hard_pending_compaction_bytes_limit(ValueType.LONG),
    level0_file_num_compaction_trigger(ValueType.INT),
    level0_slowdown_writes_trigger(ValueType.INT),
    level0_stop_writes_trigger(ValueType.INT),
    max_compaction_bytes(ValueType.LONG),
    target_file_size_base(ValueType.LONG),
    target_file_size_multiplier(ValueType.INT),
    max_bytes_for_level_base(ValueType.LONG),
    max_bytes_for_level_multiplier(ValueType.INT),
    max_bytes_for_level_multiplier_additional(ValueType.INT_ARRAY),
    ttl(ValueType.LONG),
    periodic_compaction_seconds(ValueType.LONG),
    read_triggered_compaction_threshold(ValueType.DOUBLE);

    private final ValueType valueType;
    CompactionOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum BlobOption implements MutableColumnFamilyOptionKey {
    enable_blob_files(ValueType.BOOLEAN),
    min_blob_size(ValueType.LONG),
    blob_file_size(ValueType.LONG),
    blob_compression_type(ValueType.ENUM),
    enable_blob_garbage_collection(ValueType.BOOLEAN),
    blob_garbage_collection_age_cutoff(ValueType.DOUBLE),
    blob_garbage_collection_force_threshold(ValueType.DOUBLE),
    blob_compaction_readahead_size(ValueType.LONG),
    blob_file_starting_level(ValueType.INT),
    prepopulate_blob_cache(ValueType.ENUM);

    private final ValueType valueType;
    BlobOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum MiscOption implements MutableColumnFamilyOptionKey {
    max_sequential_skip_in_iterations(ValueType.LONG),
    paranoid_file_checks(ValueType.BOOLEAN),
    report_bg_io_stats(ValueType.BOOLEAN),
    compression(ValueType.ENUM),
    memtable_protection_bytes_per_key(ValueType.INT);

    private final ValueType valueType;
    MiscOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public static class MutableColumnFamilyOptionsBuilder
      extends AbstractMutableOptionsBuilder<MutableColumnFamilyOptions, MutableColumnFamilyOptionsBuilder, MutableColumnFamilyOptionKey>
      implements MutableColumnFamilyOptionsInterface<MutableColumnFamilyOptionsBuilder> {
    private static final Map<String, MutableColumnFamilyOptionKey> ALL_KEYS_LOOKUP =
        new HashMap<>();
    static {
      for(final MutableColumnFamilyOptionKey key : MemtableOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : CompactionOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : MiscOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for (final MutableColumnFamilyOptionKey key : BlobOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }
    }

    private MutableColumnFamilyOptionsBuilder() {
      super();
    }

    @Override
    protected MutableColumnFamilyOptionsBuilder self() {
      return this;
    }

    @Override
    protected Map<String, MutableColumnFamilyOptionKey> allKeys() {
      return ALL_KEYS_LOOKUP;
    }

    @Override
    protected MutableColumnFamilyOptions build(final String[] keys,
        final String[] values) {
      return new MutableColumnFamilyOptions(keys, values);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setWriteBufferSize(
        final long writeBufferSize) {
      return setLong(MemtableOption.write_buffer_size, writeBufferSize);
    }

    @Override
    public long writeBufferSize() {
      return getLong(MemtableOption.write_buffer_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setArenaBlockSize(
        final long arenaBlockSize) {
      return setLong(MemtableOption.arena_block_size, arenaBlockSize);
    }

    @Override
    public long arenaBlockSize() {
      return getLong(MemtableOption.arena_block_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtablePrefixBloomSizeRatio(
        final double memtablePrefixBloomSizeRatio) {
      return setDouble(MemtableOption.memtable_prefix_bloom_size_ratio,
          memtablePrefixBloomSizeRatio);
    }

    @Override
    public double memtablePrefixBloomSizeRatio() {
      return getDouble(MemtableOption.memtable_prefix_bloom_size_ratio);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtableWholeKeyFiltering(
        final boolean memtableWholeKeyFiltering) {
      return setBoolean(MemtableOption.memtable_whole_key_filtering, memtableWholeKeyFiltering);
    }

    @Override
    public boolean memtableWholeKeyFiltering() {
      return getBoolean(MemtableOption.memtable_whole_key_filtering);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtableHugePageSize(
        final long memtableHugePageSize) {
      return setLong(MemtableOption.memtable_huge_page_size,
          memtableHugePageSize);
    }

    @Override
    public long memtableHugePageSize() {
      return getLong(MemtableOption.memtable_huge_page_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSuccessiveMerges(
        final long maxSuccessiveMerges) {
      return setLong(MemtableOption.max_successive_merges, maxSuccessiveMerges);
    }

    @Override
    public long maxSuccessiveMerges() {
      return getLong(MemtableOption.max_successive_merges);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxWriteBufferNumber(
        final int maxWriteBufferNumber) {
      return setInt(MemtableOption.max_write_buffer_number,
          maxWriteBufferNumber);
    }

    @Override
    public int maxWriteBufferNumber() {
      return getInt(MemtableOption.max_write_buffer_number);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setInplaceUpdateNumLocks(
        final long inplaceUpdateNumLocks) {
      return setLong(MemtableOption.inplace_update_num_locks,
          inplaceUpdateNumLocks);
    }

    @Override
    public long inplaceUpdateNumLocks() {
      return getLong(MemtableOption.inplace_update_num_locks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setExperimentalMempurgeThreshold(
        final double experimentalMempurgeThreshold) {
      return setDouble(
          MemtableOption.experimental_mempurge_threshold, experimentalMempurgeThreshold);
    }

    @Override
    public double experimentalMempurgeThreshold() {
      return getDouble(MemtableOption.experimental_mempurge_threshold);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMinTombstonesForRangeConversion(
        final int minTombstonesForRangeConversion) {
      return setInt(
          MemtableOption.min_tombstones_for_range_conversion, minTombstonesForRangeConversion);
    }

    @Override
    public int minTombstonesForRangeConversion() {
      return getInt(MemtableOption.min_tombstones_for_range_conversion);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setDisableAutoCompactions(
        final boolean disableAutoCompactions) {
      return setBoolean(CompactionOption.disable_auto_compactions,
          disableAutoCompactions);
    }

    @Override
    public boolean disableAutoCompactions() {
      return getBoolean(CompactionOption.disable_auto_compactions);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setSoftPendingCompactionBytesLimit(
        final long softPendingCompactionBytesLimit) {
      return setLong(CompactionOption.soft_pending_compaction_bytes_limit,
          softPendingCompactionBytesLimit);
    }

    @Override
    public long softPendingCompactionBytesLimit() {
      return getLong(CompactionOption.soft_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setHardPendingCompactionBytesLimit(
        final long hardPendingCompactionBytesLimit) {
      return setLong(CompactionOption.hard_pending_compaction_bytes_limit,
          hardPendingCompactionBytesLimit);
    }

    @Override
    public long hardPendingCompactionBytesLimit() {
      return getLong(CompactionOption.hard_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0FileNumCompactionTrigger(
        final int level0FileNumCompactionTrigger) {
      return setInt(CompactionOption.level0_file_num_compaction_trigger,
          level0FileNumCompactionTrigger);
    }

    @Override
    public int level0FileNumCompactionTrigger() {
      return getInt(CompactionOption.level0_file_num_compaction_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0SlowdownWritesTrigger(
        final int level0SlowdownWritesTrigger) {
      return setInt(CompactionOption.level0_slowdown_writes_trigger,
          level0SlowdownWritesTrigger);
    }

    @Override
    public int level0SlowdownWritesTrigger() {
      return getInt(CompactionOption.level0_slowdown_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0StopWritesTrigger(
        final int level0StopWritesTrigger) {
      return setInt(CompactionOption.level0_stop_writes_trigger,
          level0StopWritesTrigger);
    }

    @Override
    public int level0StopWritesTrigger() {
      return getInt(CompactionOption.level0_stop_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxCompactionBytes(final long maxCompactionBytes) {
      return setLong(CompactionOption.max_compaction_bytes, maxCompactionBytes);
    }

    @Override
    public long maxCompactionBytes() {
      return getLong(CompactionOption.max_compaction_bytes);
    }


    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeBase(
        final long targetFileSizeBase) {
      return setLong(CompactionOption.target_file_size_base,
          targetFileSizeBase);
    }

    @Override
    public long targetFileSizeBase() {
      return getLong(CompactionOption.target_file_size_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeMultiplier(
        final int targetFileSizeMultiplier) {
      return setInt(CompactionOption.target_file_size_multiplier,
          targetFileSizeMultiplier);
    }

    @Override
    public int targetFileSizeMultiplier() {
      return getInt(CompactionOption.target_file_size_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelBase(
        final long maxBytesForLevelBase) {
      return setLong(CompactionOption.max_bytes_for_level_base,
          maxBytesForLevelBase);
    }

    @Override
    public long maxBytesForLevelBase() {
      return getLong(CompactionOption.max_bytes_for_level_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplier(
        final double maxBytesForLevelMultiplier) {
      return setDouble(CompactionOption.max_bytes_for_level_multiplier, maxBytesForLevelMultiplier);
    }

    @Override
    public double maxBytesForLevelMultiplier() {
      return getDouble(CompactionOption.max_bytes_for_level_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplierAdditional(
        final int[] maxBytesForLevelMultiplierAdditional) {
      return setIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional,
          maxBytesForLevelMultiplierAdditional);
    }

    @Override
    public int[] maxBytesForLevelMultiplierAdditional() {
      return getIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSequentialSkipInIterations(
        final long maxSequentialSkipInIterations) {
      return setLong(MiscOption.max_sequential_skip_in_iterations,
          maxSequentialSkipInIterations);
    }

    @Override
    public long maxSequentialSkipInIterations() {
      return getLong(MiscOption.max_sequential_skip_in_iterations);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setParanoidFileChecks(
        final boolean paranoidFileChecks) {
      return setBoolean(MiscOption.paranoid_file_checks, paranoidFileChecks);
    }

    @Override
    public boolean paranoidFileChecks() {
      return getBoolean(MiscOption.paranoid_file_checks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setCompressionType(
        final CompressionType compressionType) {
      return setEnum(MiscOption.compression, compressionType);
    }

    @Override
    public CompressionType compressionType() {
      return getEnum(MiscOption.compression);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setReportBgIoStats(
        final boolean reportBgIoStats) {
      return setBoolean(MiscOption.report_bg_io_stats, reportBgIoStats);
    }

    @Override
    public boolean reportBgIoStats() {
      return getBoolean(MiscOption.report_bg_io_stats);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setTtl(final long ttl) {
      return setLong(CompactionOption.ttl, ttl);
    }

    @Override
    public long ttl() {
      return getLong(CompactionOption.ttl);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setPeriodicCompactionSeconds(
        final long periodicCompactionSeconds) {
      return setLong(CompactionOption.periodic_compaction_seconds, periodicCompactionSeconds);
    }

    @Override
    public long periodicCompactionSeconds() {
      return getLong(CompactionOption.periodic_compaction_seconds);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setEnableBlobFiles(final boolean enableBlobFiles) {
      return setBoolean(BlobOption.enable_blob_files, enableBlobFiles);
    }

    @Override
    public boolean enableBlobFiles() {
      return getBoolean(BlobOption.enable_blob_files);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMinBlobSize(final long minBlobSize) {
      return setLong(BlobOption.min_blob_size, minBlobSize);
    }

    @Override
    public long minBlobSize() {
      return getLong(BlobOption.min_blob_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobFileSize(final long blobFileSize) {
      return setLong(BlobOption.blob_file_size, blobFileSize);
    }

    @Override
    public long blobFileSize() {
      return getLong(BlobOption.blob_file_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobCompressionType(
        final CompressionType compressionType) {
      return setEnum(BlobOption.blob_compression_type, compressionType);
    }

    @Override
    public CompressionType blobCompressionType() {
      return getEnum(BlobOption.blob_compression_type);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setEnableBlobGarbageCollection(
        final boolean enableBlobGarbageCollection) {
      return setBoolean(BlobOption.enable_blob_garbage_collection, enableBlobGarbageCollection);
    }

    @Override
    public boolean enableBlobGarbageCollection() {
      return getBoolean(BlobOption.enable_blob_garbage_collection);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobGarbageCollectionAgeCutoff(
        final double blobGarbageCollectionAgeCutoff) {
      return setDouble(
          BlobOption.blob_garbage_collection_age_cutoff, blobGarbageCollectionAgeCutoff);
    }

    @Override
    public double blobGarbageCollectionAgeCutoff() {
      return getDouble(BlobOption.blob_garbage_collection_age_cutoff);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobGarbageCollectionForceThreshold(
        final double blobGarbageCollectionForceThreshold) {
      return setDouble(
          BlobOption.blob_garbage_collection_force_threshold, blobGarbageCollectionForceThreshold);
    }

    @Override
    public double blobGarbageCollectionForceThreshold() {
      return getDouble(BlobOption.blob_garbage_collection_force_threshold);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setReadTriggeredCompactionThreshold(
        final double readTriggeredCompactionThreshold) {
      return setDouble(
          CompactionOption.read_triggered_compaction_threshold, readTriggeredCompactionThreshold);
    }

    @Override
    public double readTriggeredCompactionThreshold() {
      return getDouble(CompactionOption.read_triggered_compaction_threshold);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobCompactionReadaheadSize(
        final long blobCompactionReadaheadSize) {
      return setLong(BlobOption.blob_compaction_readahead_size, blobCompactionReadaheadSize);
    }

    @Override
    public long blobCompactionReadaheadSize() {
      return getLong(BlobOption.blob_compaction_readahead_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setBlobFileStartingLevel(
        final int blobFileStartingLevel) {
      return setInt(BlobOption.blob_file_starting_level, blobFileStartingLevel);
    }

    @Override
    public int blobFileStartingLevel() {
      return getInt(BlobOption.blob_file_starting_level);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setPrepopulateBlobCache(
        final PrepopulateBlobCache prepopulateBlobCache) {
      return setEnum(BlobOption.prepopulate_blob_cache, prepopulateBlobCache);
    }

    @Override
    public PrepopulateBlobCache prepopulateBlobCache() {
      return getEnum(BlobOption.prepopulate_blob_cache);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtableProtectionBytesPerKey(
        final int memtableProtectionBytesPerKey) {
      return setInt(MiscOption.memtable_protection_bytes_per_key, memtableProtectionBytesPerKey);
    }

    @Override
    public int memtableProtectionBytesPerKey() {
      return getInt(MiscOption.memtable_protection_bytes_per_key);
    }
  }
}
