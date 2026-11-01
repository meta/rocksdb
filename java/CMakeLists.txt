cmake_minimum_required(VERSION 3.11)

set(JAVA_JUNIT_VERSION "4.13.1")
set(JAVA_HAMCR_VERSION "2.2")
set(JAVA_MOCKITO_VERSION "1.10.19")
set(JAVA_CGLIB_VERSION "3.3.0")
set(JAVA_ASSERTJ_VERSION "2.9.0")


if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
    message("Please consider switching to CMake 3.11.4 or newer")
endif()

set(CMAKE_JAVA_COMPILE_FLAGS -source 8)

set(JNI_NATIVE_SOURCES
        rocksjni/backup_engine_options.cc
        rocksjni/backupenginejni.cc
        rocksjni/cache.cc
        rocksjni/cassandra_compactionfilterjni.cc
        rocksjni/cassandra_value_operator.cc
        rocksjni/checkpoint.cc
        rocksjni/clock_cache.cc
        rocksjni/columnfamilyhandle.cc
        rocksjni/compact_range_options.cc
        rocksjni/compaction_filter.cc
        rocksjni/compaction_filter_factory.cc
        rocksjni/compaction_filter_factory_jnicallback.cc
        rocksjni/compaction_job_info.cc
        rocksjni/compaction_job_stats.cc
        rocksjni/compaction_options.cc
        rocksjni/compaction_options_fifo.cc
        rocksjni/compaction_options_universal.cc
        rocksjni/comparator.cc
        rocksjni/comparatorjnicallback.cc
        rocksjni/compression_options.cc
        rocksjni/concurrent_task_limiter.cc
        rocksjni/config_options.cc
        rocksjni/env.cc
        rocksjni/env_options.cc
        rocksjni/event_listener.cc
        rocksjni/event_listener_jnicallback.cc
        rocksjni/export_import_files_metadatajni.cc
        rocksjni/filter.cc
        rocksjni/hyper_clock_cache.cc
        rocksjni/import_column_family_options.cc
        rocksjni/ingest_external_file_options.cc
        rocksjni/iterator.cc
        rocksjni/jni_multiget_helpers.cc
        rocksjni/jni_perf_context.cc
        rocksjni/jnicallback.cc
        rocksjni/loggerjnicallback.cc
        rocksjni/lru_cache.cc
        rocksjni/memory_util.cc
        rocksjni/memtablejni.cc
        rocksjni/merge_operator.cc
        rocksjni/native_comparator_wrapper_test.cc
        rocksjni/optimistic_transaction_db.cc
        rocksjni/optimistic_transaction_options.cc
        rocksjni/options.cc
        rocksjni/options_util.cc
        rocksjni/persistent_cache.cc
        rocksjni/ratelimiterjni.cc
        rocksjni/remove_emptyvalue_compactionfilterjni.cc
        rocksjni/restorejni.cc
        rocksjni/rocks_callback_object.cc
        rocksjni/rocksdb_exception_test.cc
        rocksjni/rocksjni.cc
        rocksjni/slice.cc
        rocksjni/snapshot.cc
        rocksjni/sst_file_manager.cc
        rocksjni/sst_file_reader_iterator.cc
        rocksjni/sst_file_readerjni.cc
        rocksjni/sst_file_writerjni.cc
        rocksjni/sst_partitioner.cc
        rocksjni/statistics.cc
        rocksjni/statisticsjni.cc
        rocksjni/stderr_logger.cc
        rocksjni/table.cc
        rocksjni/table_filter.cc
        rocksjni/table_filter_jnicallback.cc
        rocksjni/table_properties_collector_factory.cc
        rocksjni/testable_event_listener.cc
        rocksjni/thread_status.cc
        rocksjni/trace_writer.cc
        rocksjni/trace_writer_jnicallback.cc
        rocksjni/transaction.cc
        rocksjni/transaction_db.cc
        rocksjni/transaction_db_options.cc
        rocksjni/transaction_notifier.cc
        rocksjni/transaction_notifier_jnicallback.cc
        rocksjni/transaction_options.cc
        rocksjni/ttl.cc
        rocksjni/wal_filter.cc
        rocksjni/wal_filter_jnicallback.cc
        rocksjni/wal_iterator.cc
        rocksjni/write_batch.cc
        rocksjni/write_batch_test.cc
        rocksjni/write_batch_with_index.cc
        rocksjni/write_buffer_manager.cc
        rocksjni/writebatchhandlerjnicallback.cc
)

set(JAVA_MAIN_CLASSES
  src/main/java/org/rocksdb/AbstractCompactionFilter.java
  src/main/java/org/rocksdb/AbstractCompactionFilterFactory.java
  src/main/java/org/rocksdb/AbstractComparator.java
  src/main/java/org/rocksdb/AbstractComparatorJniBridge.java
  src/main/java/org/rocksdb/AbstractEventListener.java
  src/main/java/org/rocksdb/AbstractImmutableNativeReference.java
  src/main/java/org/rocksdb/AbstractMutableOptions.java
  src/main/java/org/rocksdb/AbstractNativeReference.java
  src/main/java/org/rocksdb/AbstractRocksIterator.java
  src/main/java/org/rocksdb/AbstractSlice.java
  src/main/java/org/rocksdb/AbstractTableFilter.java
  src/main/java/org/rocksdb/AbstractTraceWriter.java
  src/main/java/org/rocksdb/AbstractTransactionNotifier.java
  src/main/java/org/rocksdb/AbstractWalFilter.java
  src/main/java/org/rocksdb/AbstractWriteBatch.java
  src/main/java/org/rocksdb/AdvancedColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/AdvancedMutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/BackgroundErrorReason.java
  src/main/java/org/rocksdb/BackupEngine.java
  src/main/java/org/rocksdb/BackupEngineOptions.java
  src/main/java/org/rocksdb/BackupInfo.java
  src/main/java/org/rocksdb/BlockBasedTableConfig.java
  src/main/java/org/rocksdb/BloomFilter.java
  src/main/java/org/rocksdb/BuiltinComparator.java
  src/main/java/org/rocksdb/ByteBufferGetStatus.java
  src/main/java/org/rocksdb/Cache.java
  src/main/java/org/rocksdb/CassandraCompactionFilter.java
  src/main/java/org/rocksdb/CassandraValueMergeOperator.java
  src/main/java/org/rocksdb/Checkpoint.java
  src/main/java/org/rocksdb/ChecksumType.java
  src/main/java/org/rocksdb/ClockCache.java
  src/main/java/org/rocksdb/ColumnFamilyDescriptor.java
  src/main/java/org/rocksdb/ColumnFamilyHandle.java
  src/main/java/org/rocksdb/ColumnFamilyMetaData.java
  src/main/java/org/rocksdb/ColumnFamilyOptions.java
  src/main/java/org/rocksdb/ColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/CompactRangeOptions.java
  src/main/java/org/rocksdb/CompactionJobInfo.java
  src/main/java/org/rocksdb/CompactionJobStats.java
  src/main/java/org/rocksdb/CompactionOptions.java
  src/main/java/org/rocksdb/CompactionOptionsFIFO.java
  src/main/java/org/rocksdb/CompactionOptionsUniversal.java
  src/main/java/org/rocksdb/CompactionPriority.java
  src/main/java/org/rocksdb/CompactionReason.java
  src/main/java/org/rocksdb/CompactionStopStyle.java
  src/main/java/org/rocksdb/CompactionStyle.java
  src/main/java/org/rocksdb/ComparatorOptions.java
  src/main/java/org/rocksdb/ComparatorType.java
  src/main/java/org/rocksdb/CompressionOptions.java
  src/main/java/org/rocksdb/CompressionType.java
  src/main/java/org/rocksdb/ConcurrentTaskLimiter.java
  src/main/java/org/rocksdb/ConcurrentTaskLimiterImpl.java
  src/main/java/org/rocksdb/ConfigOptions.java
  src/main/java/org/rocksdb/DBOptions.java
  src/main/java/org/rocksdb/DBOptionsInterface.java
  src/main/java/org/rocksdb/DataBlockIndexType.java
  src/main/java/org/rocksdb/DbPath.java
  src/main/java/org/rocksdb/DirectSlice.java
  src/main/java/org/rocksdb/EncodingType.java
  src/main/java/org/rocksdb/Env.java
  src/main/java/org/rocksdb/EnvOptions.java
  src/main/java/org/rocksdb/EventListener.java
  src/main/java/org/rocksdb/Experimental.java
  src/main/java/org/rocksdb/ExportImportFilesMetaData.java
  src/main/java/org/rocksdb/ExternalFileIngestionInfo.java
  src/main/java/org/rocksdb/FileOperationInfo.java
  src/main/java/org/rocksdb/Filter.java
  src/main/java/org/rocksdb/FilterPolicyType.java
  src/main/java/org/rocksdb/FlushJobInfo.java
  src/main/java/org/rocksdb/FlushOptions.java
  src/main/java/org/rocksdb/FlushReason.java
  src/main/java/org/rocksdb/GetStatus.java
  src/main/java/org/rocksdb/GetWithMetadataResult.java
  src/main/java/org/rocksdb/HashLinkedListMemTableConfig.java
  src/main/java/org/rocksdb/HashSkipListMemTableConfig.java
  src/main/java/org/rocksdb/HistogramData.java
  src/main/java/org/rocksdb/HistogramType.java
  src/main/java/org/rocksdb/Holder.java
  src/main/java/org/rocksdb/HyperClockCache.java
  src/main/java/org/rocksdb/ImportColumnFamilyOptions.java
  src/main/java/org/rocksdb/IndexShorteningMode.java
  src/main/java/org/rocksdb/IndexSearchType.java
  src/main/java/org/rocksdb/IndexType.java
  src/main/java/org/rocksdb/InfoLogLevel.java
  src/main/java/org/rocksdb/IngestExternalFileOptions.java
  src/main/java/org/rocksdb/KeyMayExist.java
  src/main/java/org/rocksdb/LRUCache.java
  src/main/java/org/rocksdb/LevelMetaData.java
  src/main/java/org/rocksdb/LiveFileMetaData.java
  src/main/java/org/rocksdb/LogFile.java
  src/main/java/org/rocksdb/Logger.java
  src/main/java/org/rocksdb/LoggerInterface.java
  src/main/java/org/rocksdb/LoggerType.java
  src/main/java/org/rocksdb/MemTableConfig.java
  src/main/java/org/rocksdb/MemTableInfo.java
  src/main/java/org/rocksdb/MemoryUsageType.java
  src/main/java/org/rocksdb/MemoryUtil.java
  src/main/java/org/rocksdb/MergeOperator.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptions.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/MutableDBOptions.java
  src/main/java/org/rocksdb/MutableDBOptionsInterface.java
  src/main/java/org/rocksdb/MutableOptionKey.java
  src/main/java/org/rocksdb/MutableOptionValue.java
  src/main/java/org/rocksdb/NativeComparatorWrapper.java
  src/main/java/org/rocksdb/NativeLibraryLoader.java
  src/main/java/org/rocksdb/OperationStage.java
  src/main/java/org/rocksdb/OperationType.java
  src/main/java/org/rocksdb/OptimisticTransactionDB.java
  src/main/java/org/rocksdb/OptimisticTransactionOptions.java
  src/main/java/org/rocksdb/OptimizeKeyCommonPrefix.java
  src/main/java/org/rocksdb/OptionString.java
  src/main/java/org/rocksdb/Options.java
  src/main/java/org/rocksdb/OptionsUtil.java
  src/main/java/org/rocksdb/PerfContext.java
  src/main/java/org/rocksdb/PerfLevel.java
  src/main/java/org/rocksdb/PersistentCache.java
  src/main/java/org/rocksdb/PlainTableConfig.java
  src/main/java/org/rocksdb/PrepopulateBlobCache.java
  src/main/java/org/rocksdb/Priority.java
  src/main/java/org/rocksdb/Range.java
  src/main/java/org/rocksdb/RateLimiter.java
  src/main/java/org/rocksdb/RateLimiterMode.java
  src/main/java/org/rocksdb/ReadOptions.java
  src/main/java/org/rocksdb/ReadTier.java
  src/main/java/org/rocksdb/RemoveEmptyValueCompactionFilter.java
  src/main/java/org/rocksdb/RestoreOptions.java
  src/main/java/org/rocksdb/ReusedSynchronisationType.java
  src/main/java/org/rocksdb/RocksCallbackObject.java
  src/main/java/org/rocksdb/RocksDB.java
  src/main/java/org/rocksdb/RocksDBException.java
  src/main/java/org/rocksdb/RocksEnv.java
  src/main/java/org/rocksdb/RocksIterator.java
  src/main/java/org/rocksdb/RocksIteratorInterface.java
  src/main/java/org/rocksdb/RocksMemEnv.java
  src/main/java/org/rocksdb/RocksMutableObject.java
  src/main/java/org/rocksdb/RocksObject.java
  src/main/java/org/rocksdb/SanityLevel.java
  src/main/java/org/rocksdb/SizeApproximationFlag.java
  src/main/java/org/rocksdb/SkipListMemTableConfig.java
  src/main/java/org/rocksdb/Slice.java
  src/main/java/org/rocksdb/Snapshot.java
  src/main/java/org/rocksdb/SstFileManager.java
  src/main/java/org/rocksdb/SstFileMetaData.java
  src/main/java/org/rocksdb/SstFileReader.java
  src/main/java/org/rocksdb/SstFileReaderIterator.java
  src/main/java/org/rocksdb/SstFileWriter.java
  src/main/java/org/rocksdb/SstPartitionerFactory.java
  src/main/java/org/rocksdb/SstPartitionerFixedPrefixFactory.java
  src/main/java/org/rocksdb/StateType.java
  src/main/java/org/rocksdb/Statistics.java
  src/main/java/org/rocksdb/StatisticsCollector.java
  src/main/java/org/rocksdb/StatisticsCollectorCallback.java
  src/main/java/org/rocksdb/StatsCollectorInput.java
  src/main/java/org/rocksdb/StatsLevel.java
  src/main/java/org/rocksdb/Status.java
  src/main/java/org/rocksdb/StringAppendOperator.java
  src/main/java/org/rocksdb/TableFileCreationBriefInfo.java
  src/main/java/org/rocksdb/TableFileCreationInfo.java
  src/main/java/org/rocksdb/TableFileCreationReason.java
  src/main/java/org/rocksdb/TableFileDeletionInfo.java
  src/main/java/org/rocksdb/TableFilter.java
  src/main/java/org/rocksdb/TableFormatConfig.java
  src/main/java/org/rocksdb/TableProperties.java
  src/main/java/org/rocksdb/TablePropertiesCollectorFactory.java
  src/main/java/org/rocksdb/ThreadStatus.java
  src/main/java/org/rocksdb/ThreadType.java
  src/main/java/org/rocksdb/TickerType.java
  src/main/java/org/rocksdb/TimedEnv.java
  src/main/java/org/rocksdb/TraceOptions.java
  src/main/java/org/rocksdb/TraceWriter.java
  src/main/java/org/rocksdb/Transaction.java
  src/main/java/org/rocksdb/TransactionDB.java
  src/main/java/org/rocksdb/TransactionDBOptions.java
  src/main/java/org/rocksdb/TransactionLogIterator.java
  src/main/java/org/rocksdb/TransactionOptions.java
  src/main/java/org/rocksdb/TransactionalDB.java
  src/main/java/org/rocksdb/TransactionalOptions.java
  src/main/java/org/rocksdb/TtlDB.java
  src/main/java/org/rocksdb/TxnDBWritePolicy.java
  src/main/java/org/rocksdb/UInt64AddOperator.java
  src/main/java/org/rocksdb/VectorMemTableConfig.java
  src/main/java/org/rocksdb/WALRecoveryMode.java
  src/main/java/org/rocksdb/WBWIRocksIterator.java
  src/main/java/org/rocksdb/WalFileType.java
  src/main/java/org/rocksdb/WalFilter.java
  src/main/java/org/rocksdb/WalIterator.java
  src/main/java/org/rocksdb/WalProcessingOption.java
  src/main/java/org/rocksdb/WriteBatch.java
  src/main/java/org/rocksdb/WriteBatchInterface.java
  src/main/java/org/rocksdb/WriteBatchWithIndex.java
  src/main/java/org/rocksdb/WriteBufferManager.java
  src/main/java/org/rocksdb/WriteBufferManagerFlushPolicy.java
  src/main/java/org/rocksdb/WriteOptions.java
  src/main/java/org/rocksdb/WriteStallCondition.java
  src/main/java/org/rocksdb/WriteStallInfo.java
  src/main/java/org/rocksdb/util/BufferUtil.java
  src/main/java/org/rocksdb/util/ByteUtil.java
  src/main/java/org/rocksdb/util/BytewiseComparator.java
  src/main/java/org/rocksdb/util/Environment.java
  src/main/java/org/rocksdb/util/IntComparator.java
  src/main/java/org/rocksdb/util/ReverseBytewiseComparator.java
  src/main/java/org/rocksdb/util/SizeUnit.java
  src/main/java/org/rocksdb/util/StdErrLogger.java
  src/test/java/org/rocksdb/NativeComparatorWrapperTest.java
  src/test/java/org/rocksdb/RocksDBExceptionTest.java
  src/test/java/org/rocksdb/RocksNativeLibraryResource.java
  src/test/java/org/rocksdb/WriteBatchTest.java
  src/test/java/org/rocksdb/test/TestableEventListener.java
  src/test/java/org/rocksdb/util/CapturingWriteBatchHandler.java
  src/test/java/org/rocksdb/util/WriteBatchGetter.java
)

set(JAVA_TEST_CLASSES
  src/test/java/org/rocksdb/AbstractTransactionTest.java
  src/test/java/org/rocksdb/BackupEngineOptionsTest.java
  src/test/java/org/rocksdb/BackupEngineTest.java
  src/test/java/org/rocksdb/BlobOptionsTest.java
  src/test/java/org/rocksdb/BlockBasedTableConfigTest.java
  src/test/java/org/rocksdb/BuiltinComparatorTest.java
  src/test/java/org/rocksdb/ByteBufferUnsupportedOperationTest.java
  src/test/java/org/rocksdb/BytewiseComparatorRegressionTest.java
  src/test/java/org/rocksdb/CheckPointTest.java
  src/test/java/org/rocksdb/ClockCacheTest.java
  src/test/java/org/rocksdb/ColumnFamilyOptionsTest.java
  src/test/java/org/rocksdb/ColumnFamilyTest.java
  src/test/java/org/rocksdb/CompactRangeOptionsTest.java
  src/test/java/org/rocksdb/CompactionFilterFactoryTest.java
  src/test/java/org/rocksdb/CompactionJobInfoTest.java
  src/test/java/org/rocksdb/CompactionJobStatsTest.java
  src/test/java/org/rocksdb/CompactionOptionsFIFOTest.java
  src/test/java/org/rocksdb/CompactionOptionsTest.java
  src/test/java/org/rocksdb/CompactionOptionsUniversalTest.java
  src/test/java/org/rocksdb/CompactionPriorityTest.java
  src/test/java/org/rocksdb/CompactionStopStyleTest.java
  src/test/java/org/rocksdb/ComparatorOptionsTest.java
  src/test/java/org/rocksdb/CompressionOptionsTest.java
  src/test/java/org/rocksdb/CompressionTypesTest.java
  src/test/java/org/rocksdb/ConcurrentTaskLimiterTest.java
  src/test/java/org/rocksdb/DBOptionsTest.java
  src/test/java/org/rocksdb/DefaultEnvTest.java
  src/test/java/org/rocksdb/DirectSliceTest.java
  src/test/java/org/rocksdb/EnvOptionsTest.java
  src/test/java/org/rocksdb/EventListenerTest.java
  src/test/java/org/rocksdb/FilterTest.java
  src/test/java/org/rocksdb/FlushOptionsTest.java
  src/test/java/org/rocksdb/FlushTest.java
  src/test/java/org/rocksdb/HyperClockCacheTest.java
  src/test/java/org/rocksdb/ImportColumnFamilyTest.java
  src/test/java/org/rocksdb/InfoLogLevelTest.java
  src/test/java/org/rocksdb/IngestExternalFileOptionsTest.java
  src/test/java/org/rocksdb/KeyExistsTest.java
  src/test/java/org/rocksdb/KeyMayExistTest.java
  src/test/java/org/rocksdb/LRUCacheTest.java
  src/test/java/org/rocksdb/LoggerTest.java
  src/test/java/org/rocksdb/MemTableTest.java
  src/test/java/org/rocksdb/MemoryUtilTest.java
  src/test/java/org/rocksdb/MergeCFVariantsTest.java
  src/test/java/org/rocksdb/MergeTest.java
  src/test/java/org/rocksdb/MergeVariantsTest.java
  src/test/java/org/rocksdb/MixedOptionsTest.java
  src/test/java/org/rocksdb/MultiColumnRegressionTest.java
  src/test/java/org/rocksdb/MultiGetManyKeysTest.java
  src/test/java/org/rocksdb/MultiGetTest.java
  src/test/java/org/rocksdb/MutableColumnFamilyOptionsTest.java
  src/test/java/org/rocksdb/MutableDBOptionsTest.java
  src/test/java/org/rocksdb/MutableOptionsGetSetTest.java
  src/test/java/org/rocksdb/NativeLibraryLoaderTest.java
  src/test/java/org/rocksdb/OptimisticTransactionDBTest.java
  src/test/java/org/rocksdb/OptimisticTransactionOptionsTest.java
  src/test/java/org/rocksdb/OptimisticTransactionTest.java
  src/test/java/org/rocksdb/OptionsTest.java
  src/test/java/org/rocksdb/OptionsUtilTest.java
  src/test/java/org/rocksdb/PerfContextTest.java
  src/test/java/org/rocksdb/PerfLevelTest.java
  src/test/java/org/rocksdb/PlainTableConfigTest.java
  src/test/java/org/rocksdb/PlatformRandomHelper.java
  src/test/java/org/rocksdb/PutCFVariantsTest.java
  src/test/java/org/rocksdb/PutMultiplePartsTest.java
  src/test/java/org/rocksdb/PutVariantsTest.java
  src/test/java/org/rocksdb/RateLimiterTest.java
  src/test/java/org/rocksdb/ReadOnlyTest.java
  src/test/java/org/rocksdb/ReadOptionsTest.java
  src/test/java/org/rocksdb/RocksDBTest.java
  src/test/java/org/rocksdb/RocksIteratorTest.java
  src/test/java/org/rocksdb/RocksMemEnvTest.java
  src/test/java/org/rocksdb/SecondaryDBTest.java
  src/test/java/org/rocksdb/SliceTest.java
  src/test/java/org/rocksdb/SnapshotTest.java
  src/test/java/org/rocksdb/SstFileManagerTest.java
  src/test/java/org/rocksdb/SstFileReaderTest.java
  src/test/java/org/rocksdb/SstFileWriterTest.java
  src/test/java/org/rocksdb/SstPartitionerTest.java
  src/test/java/org/rocksdb/StatisticsCollectorTest.java
  src/test/java/org/rocksdb/StatisticsTest.java
  src/test/java/org/rocksdb/StatsCallbackMock.java
  src/test/java/org/rocksdb/TableFilterTest.java
  src/test/java/org/rocksdb/TimedEnvTest.java
  src/test/java/org/rocksdb/TransactionDBOptionsTest.java
  src/test/java/org/rocksdb/TransactionDBTest.java
  src/test/java/org/rocksdb/TransactionOptionsTest.java
  src/test/java/org/rocksdb/TransactionTest.java
  src/test/java/org/rocksdb/TtlDBTest.java
  src/test/java/org/rocksdb/Types.java
  src/test/java/org/rocksdb/VerifyChecksumsTest.java
  src/test/java/org/rocksdb/WALRecoveryModeTest.java
  src/test/java/org/rocksdb/WalFilterTest.java
  src/test/java/org/rocksdb/WalIteratorTest.java
  src/test/java/org/rocksdb/WriteBatchHandlerTest.java
  src/test/java/org/rocksdb/WriteBatchThreadedTest.java
  src/test/java/org/rocksdb/WriteBatchWithIndexTest.java
  src/test/java/org/rocksdb/WriteOptionsTest.java
  src/test/java/org/rocksdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/rocksdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/rocksdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/rocksdb/test/RocksJunitRunner.java
  src/test/java/org/rocksdb/test/TestableEventListener.java
  src/test/java/org/rocksdb/test/TestableEventListener.java
  src/test/java/org/rocksdb/util/ByteBufferAllocator.java
  src/test/java/org/rocksdb/util/BytewiseComparatorIntTest.java
  src/test/java/org/rocksdb/util/BytewiseComparatorTest.java
  src/test/java/org/rocksdb/util/DirectByteBufferAllocator.java
  src/test/java/org/rocksdb/util/EnvironmentTest.java
  src/test/java/org/rocksdb/util/HeapByteBufferAllocator.java
  src/test/java/org/rocksdb/util/IntComparatorTest.java
  src/test/java/org/rocksdb/util/JNIComparatorTest.java
  src/test/java/org/rocksdb/util/ReverseBytewiseComparatorIntTest.java
  src/test/java/org/rocksdb/util/SizeUnitTest.java
  src/test/java/org/rocksdb/util/StdErrLoggerTest.java
  src/test/java/org/rocksdb/util/TestUtil.java
)

set(JAVA_TEST_RUNNING_CLASSES
  org.rocksdb.BackupEngineOptionsTest
  org.rocksdb.BackupEngineTest
  org.rocksdb.BlobOptionsTest
  org.rocksdb.BlockBasedTableConfigTest
  org.rocksdb.BuiltinComparatorTest
  org.rocksdb.ByteBufferUnsupportedOperationTest
  org.rocksdb.BytewiseComparatorRegressionTest
  org.rocksdb.CheckPointTest
  org.rocksdb.ClockCacheTest
  org.rocksdb.ColumnFamilyOptionsTest
  org.rocksdb.ColumnFamilyTest
  org.rocksdb.CompactRangeOptionsTest
  org.rocksdb.CompactionFilterFactoryTest
  org.rocksdb.CompactionJobInfoTest
  org.rocksdb.CompactionJobStatsTest
  org.rocksdb.CompactionOptionsFIFOTest
  org.rocksdb.CompactionOptionsTest
  org.rocksdb.CompactionOptionsUniversalTest
  org.rocksdb.CompactionPriorityTest
  org.rocksdb.CompactionStopStyleTest
  org.rocksdb.ComparatorOptionsTest
  org.rocksdb.CompressionOptionsTest
  org.rocksdb.CompressionTypesTest
  org.rocksdb.ConcurrentTaskLimiterTest
  org.rocksdb.DBOptionsTest
  org.rocksdb.DefaultEnvTest
  org.rocksdb.DirectSliceTest
  org.rocksdb.EnvOptionsTest
  org.rocksdb.EventListenerTest
  org.rocksdb.FilterTest
  org.rocksdb.FlushOptionsTest
  org.rocksdb.FlushTest
  org.rocksdb.HyperClockCacheTest
  org.rocksdb.ImportColumnFamilyTest
  org.rocksdb.InfoLogLevelTest
  org.rocksdb.IngestExternalFileOptionsTest
  org.rocksdb.KeyExistsTest
  org.rocksdb.KeyMayExistTest
  org.rocksdb.LRUCacheTest
  org.rocksdb.LoggerTest
  org.rocksdb.MemTableTest
  org.rocksdb.MemoryUtilTest
  org.rocksdb.MergeCFVariantsTest
  org.rocksdb.MergeTest
  org.rocksdb.MergeVariantsTest
  org.rocksdb.MixedOptionsTest
  org.rocksdb.MultiColumnRegressionTest
  org.rocksdb.MultiGetManyKeysTest
  org.rocksdb.MultiGetTest
  org.rocksdb.MutableColumnFamilyOptionsTest
  org.rocksdb.MutableDBOptionsTest
  org.rocksdb.MutableOptionsGetSetTest
  org.rocksdb.NativeComparatorWrapperTest
  org.rocksdb.NativeLibraryLoaderTest
  org.rocksdb.OptimisticTransactionDBTest
  org.rocksdb.OptimisticTransactionOptionsTest
  org.rocksdb.OptimisticTransactionTest
  org.rocksdb.OptionsTest
  org.rocksdb.OptionsUtilTest
  org.rocksdb.PerfContextTest
  org.rocksdb.PerfLevelTest
  org.rocksdb.PlainTableConfigTest
  org.rocksdb.PutCFVariantsTest
  org.rocksdb.PutMultiplePartsTest
  org.rocksdb.PutVariantsTest
  org.rocksdb.RateLimiterTest
  org.rocksdb.ReadOnlyTest
  org.rocksdb.ReadOptionsTest
  org.rocksdb.RocksDBExceptionTest
  org.rocksdb.RocksDBTest
  org.rocksdb.RocksIteratorTest
  org.rocksdb.RocksMemEnvTest
  org.rocksdb.SecondaryDBTest
  org.rocksdb.SliceTest
  org.rocksdb.SnapshotTest
  org.rocksdb.SstFileManagerTest
  org.rocksdb.SstFileReaderTest
  org.rocksdb.SstFileWriterTest
  org.rocksdb.SstPartitionerTest
  org.rocksdb.StatisticsCollectorTest
  org.rocksdb.StatisticsTest
  org.rocksdb.TableFilterTest
  org.rocksdb.TimedEnvTest
  org.rocksdb.TransactionDBOptionsTest
  org.rocksdb.TransactionDBTest
  org.rocksdb.TransactionOptionsTest
  org.rocksdb.TransactionTest
  org.rocksdb.TtlDBTest
  org.rocksdb.VerifyChecksumsTest
  org.rocksdb.WALRecoveryModeTest
  org.rocksdb.WalFilterTest
  org.rocksdb.WalIteratorTest
  org.rocksdb.WriteBatchHandlerTest
  org.rocksdb.WriteBatchTest
  org.rocksdb.WriteBatchThreadedTest
  org.rocksdb.WriteBatchWithIndexTest
  org.rocksdb.WriteOptionsTest
  org.rocksdb.util.BytewiseComparatorIntTest
  org.rocksdb.util.BytewiseComparatorTest
  org.rocksdb.util.EnvironmentTest
  org.rocksdb.util.IntComparatorTest
  org.rocksdb.util.JNIComparatorTest
  org.rocksdb.util.ReverseBytewiseComparatorIntTest
  org.rocksdb.util.SizeUnitTest
  org.rocksdb.util.StdErrLoggerTest
)

include(FindJava)
include(UseJava)
find_package(JNI)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-${JAVA_JUNIT_VERSION}.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-${JAVA_HAMCR_VERSION}.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-${JAVA_MOCKITO_VERSION}.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-${JAVA_CGLIB_VERSION}.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-${JAVA_ASSERTJ_VERSION}.jar)
set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR} ${JAVA_HAMCR_JAR} ${JAVA_MOCKITO_JAR} ${JAVA_CGLIB_JAR} ${JAVA_ASSERTJ_JAR})
message("CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
message("MINGW: ${MINGW}")

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(JAVA_RUN_TESTCLASSPATH ${JAVA_JUNIT_JAR}$<SEMICOLON>${JAVA_HAMCR_JAR}$<SEMICOLON>${JAVA_MOCKITO_JAR}$<SEMICOLON>${JAVA_CGLIB_JAR}$<SEMICOLON>${JAVA_ASSERTJ_JAR})
else()
  set(JAVA_RUN_TESTCLASSPATH ${JAVA_JUNIT_JAR}:${JAVA_HAMCR_JAR}:${JAVA_MOCKITO_JAR}:${JAVA_CGLIB_JAR}:${JAVA_ASSERTJ_JAR})
endif()

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)
file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})

if(${Java_VERSION_MINOR} VERSION_LESS_EQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1")
  message(FATAL_ERROR "Detected Java 7 or older (${Java_VERSION_STRING}), minimum required version in now Java 8")
endif()

if(${Java_VERSION_MAJOR} VERSION_GREATER_EQUAL "10" AND ${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Java 10 and newer don't have javah, but the alternative GENERATE_NATIVE_HEADERS requires CMake 3.11.4 or newer
  message(FATAL_ERROR "Detected Java 10 or newer (${Java_VERSION_STRING}), to build with CMake please upgrade CMake to 3.11.4 or newer")

elseif(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake
  message("Using an old CMAKE (${CMAKE_VERSION}) - JNI headers generated in separate step")
  add_jar(
      rocksdbjni_classes
      SOURCES ${JAVA_MAIN_CLASSES}
  )

else ()
  # Java 1.8 or newer prepare the JAR...
  message("Preparing Jar for JDK ${Java_VERSION_STRING}")
  message("JAVA_TESTCLASSPATH=${JAVA_TESTCLASSPATH}")
  add_jar(
      rocksdbjni_classes
      SOURCES ${JAVA_MAIN_CLASSES}
      INCLUDE_JARS ${ROCKSDBJNI_CLASSES_JAR_FILE} ${JAVA_TESTCLASSPATH}
      GENERATE_NATIVE_HEADERS rocksdbjni_headers DESTINATION ${JNI_OUTPUT_DIR}
  )

endif()

add_jar(
    rocksdbjni_test_classes
    SOURCES
    ${JAVA_MAIN_CLASSES}
    ${JAVA_TEST_CLASSES}
    INCLUDE_JARS ${JAVA_TESTCLASSPATH}
    GENERATE_NATIVE_HEADERS rocksdbjni_test_headers DESTINATION ${JNI_OUTPUT_DIR}
)

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_DEPS_URL)
  set(DEPS_URL ${CUSTOM_DEPS_URL}/)
else ()
  # Hosted as assets on the "third-party-deps" GitHub release. (maven.org has a
  # history of failing in CI builds.) To add or update a jar, upload it here
  # (intentionally unlisted): https://github.com/meta/rocksdb/releases/edit/third-party-deps
  set(DEPS_URL "https://github.com/meta/rocksdb/releases/download/third-party-deps")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${DEPS_URL}/junit-${JAVA_JUNIT_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${DEPS_URL}/hamcrest-${JAVA_HAMCR_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${DEPS_URL}/mockito-all-${JAVA_MOCKITO_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${DEPS_URL}/cglib-${JAVA_CGLIB_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${DEPS_URL}/assertj-core-${JAVA_ASSERTJ_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake ONLY generate JNI headers, otherwise JNI is handled in add_jar step above
  message("Preparing JNI headers for old CMake (${CMAKE_VERSION})")
  set(NATIVE_JAVA_CLASSES
          org.rocksdb.AbstractCompactionFilter
          org.rocksdb.AbstractCompactionFilterFactory
          org.rocksdb.AbstractComparator
          org.rocksdb.AbstractEventListener
          org.rocksdb.AbstractImmutableNativeReference
          org.rocksdb.AbstractNativeReference
          org.rocksdb.AbstractRocksIterator
          org.rocksdb.AbstractSlice
          org.rocksdb.AbstractTableFilter
          org.rocksdb.AbstractTraceWriter
          org.rocksdb.AbstractTransactionNotifier
          org.rocksdb.AbstractWalFilter
          org.rocksdb.BackupEngineOptions
          org.rocksdb.BackupEngine
          org.rocksdb.BlockBasedTableConfig
          org.rocksdb.BloomFilter
          org.rocksdb.CassandraCompactionFilter
          org.rocksdb.CassandraValueMergeOperator
          org.rocksdb.Checkpoint
          org.rocksdb.ClockCache
          org.rocksdb.Cache
          org.rocksdb.ColumnFamilyHandle
          org.rocksdb.ColumnFamilyOptions
          org.rocksdb.CompactionJobInfo
          org.rocksdb.CompactionJobStats
          org.rocksdb.CompactionOptions
          org.rocksdb.CompactionOptionsFIFO
          org.rocksdb.CompactionOptionsUniversal
          org.rocksdb.CompactRangeOptions
          org.rocksdb.ComparatorOptions
          org.rocksdb.CompressionOptions
          org.rocksdb.ConcurrentTaskLimiterImpl
          org.rocksdb.ConfigOptions
          org.rocksdb.DBOptions
          org.rocksdb.DirectSlice
          org.rocksdb.Env
          org.rocksdb.EnvOptions
          org.rocksdb.Filter
          org.rocksdb.FlushOptions
          org.rocksdb.HashLinkedListMemTableConfig
          org.rocksdb.HashSkipListMemTableConfig
          org.rocksdb.HyperClockCache
          org.rocksdb.IngestExternalFileOptions
          org.rocksdb.Logger
          org.rocksdb.LRUCache
          org.rocksdb.MemoryUtil
          org.rocksdb.MemTableConfig
          org.rocksdb.NativeComparatorWrapper
          org.rocksdb.NativeLibraryLoader
          org.rocksdb.OptimisticTransactionDB
          org.rocksdb.OptimisticTransactionOptions
          org.rocksdb.Options
          org.rocksdb.OptionsUtil
          org.rocksdb.PersistentCache
          org.rocksdb.PlainTableConfig
          org.rocksdb.RateLimiter
          org.rocksdb.ReadOptions
          org.rocksdb.RemoveEmptyValueCompactionFilter
          org.rocksdb.RestoreOptions
          org.rocksdb.RocksCallbackObject
          org.rocksdb.RocksDB
          org.rocksdb.RocksEnv
          org.rocksdb.RocksIterator
          org.rocksdb.RocksIteratorInterface
          org.rocksdb.RocksMemEnv
          org.rocksdb.RocksMutableObject
          org.rocksdb.RocksObject
          org.rocksdb.SkipListMemTableConfig
          org.rocksdb.Slice
          org.rocksdb.Snapshot
          org.rocksdb.SstFileManager
          org.rocksdb.SstFileWriter
          org.rocksdb.SstFileReader
          org.rocksdb.SstFileReaderIterator
          org.rocksdb.SstPartitionerFactory
          org.rocksdb.SstPartitionerFixedPrefixFactory
          org.rocksdb.Statistics
          org.rocksdb.StringAppendOperator
          org.rocksdb.TableFormatConfig
          org.rocksdb.ThreadStatus
          org.rocksdb.TimedEnv
          org.rocksdb.Transaction
          org.rocksdb.TransactionDB
          org.rocksdb.TransactionDBOptions
          org.rocksdb.TransactionOptions
          org.rocksdb.WalIterator
          org.rocksdb.TtlDB
          org.rocksdb.UInt64AddOperator
          org.rocksdb.VectorMemTableConfig
          org.rocksdb.WBWIRocksIterator
          org.rocksdb.WriteBatch
          org.rocksdb.WriteBatch.Handler
          org.rocksdb.WriteBatchInterface
          org.rocksdb.WriteBatchWithIndex
          org.rocksdb.WriteOptions
          org.rocksdb.NativeComparatorWrapperTest
          org.rocksdb.RocksDBExceptionTest
          org.rocksdb.SnapshotTest
          org.rocksdb.WriteBatchTest
          org.rocksdb.WriteBatchTestInternalHelper
          org.rocksdb.WriteBufferManager
          org.rocksdb.test.TestableEventListener
  )

  create_javah(
    TARGET rocksdbjni_headers
    CLASSES ${NATIVE_JAVA_CLASSES}
    CLASSPATH rocksdbjni_classes ${JAVA_TESTCLASSPATH}
    OUTPUT_DIR ${JNI_OUTPUT_DIR}
  )
endif()

if(NOT MSVC)
  set_property(TARGET ${ROCKSDB_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(ROCKSDBJNI_STATIC_LIB rocksdbjni${ARTIFACT_SUFFIX})
if(MINGW)
  # Build mingw only as staic library.
  add_library(${ROCKSDBJNI_STATIC_LIB} ${JNI_NATIVE_SOURCES})
else()
  add_library(${ROCKSDBJNI_STATIC_LIB} SHARED ${JNI_NATIVE_SOURCES})
endif()
add_dependencies(${ROCKSDBJNI_STATIC_LIB} rocksdbjni_headers)
target_link_libraries(${ROCKSDBJNI_STATIC_LIB} ${ROCKSDB_STATIC_LIB} ${ROCKSDB_LIB})

if(NOT MINGW)
  set(ROCKSDBJNI_SHARED_LIB rocksdbjni-shared${ARTIFACT_SUFFIX})
  add_library(${ROCKSDBJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
  add_dependencies(${ROCKSDBJNI_SHARED_LIB} rocksdbjni_headers)
  target_link_libraries(${ROCKSDBJNI_SHARED_LIB} ${ROCKSDB_STATIC_LIB} ${ROCKSDB_LIB})

  set_target_properties(
    ${ROCKSDBJNI_SHARED_LIB}
    PROPERTIES
    COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
    COMPILE_PDB_NAME ${ROCKSDBJNI_STATIC_LIB}.pdb
  )
endif()

# Javadoc Jar
set(ROCKSDB_JAVADOC_JAR rocksdbjni-${CMAKE_PROJECT_VERSION}-javadoc.jar)
create_javadoc(rocksdb
        PACKAGES org.rocksdb org.rocksdb.util
        SOURCEPATH "${PROJECT_SOURCE_DIR}/java/src/main/java"
        WINDOWTITLE "RocksDB Java API JavaDoc"
        AUTHOR FALSE
        USE FALSE
        VERSION TRUE
)
add_custom_target(rocksdb_javadocs_jar ALL
  COMMAND ${Java_JAR_EXECUTABLE} cvf ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAVADOC_JAR} -C ${CMAKE_CURRENT_BINARY_DIR}/javadoc/rocksdb .
  BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAVADOC_JAR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS rocksdb_javadoc
)

# Sources Jar
set(ROCKSDB_SOURCES_JAR rocksdbjni-${CMAKE_PROJECT_VERSION}-sources.jar)
add_custom_target(rocksdb_sources_jar ALL
        ${Java_JAR_EXECUTABLE} cvf ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_SOURCES_JAR} -C ${PROJECT_SOURCE_DIR}/java/src/main/java/ .
        BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_SOURCES_JAR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

set(bitness 32)
if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(bitness 64)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    set_target_properties(
            ${ROCKSDBJNI_STATIC_LIB}
            PROPERTIES
            OUTPUT_NAME librocksdbjni-win${bitness}
    )
    set(ROCKSDB_JAR rocksdbjni-${CMAKE_PROJECT_VERSION}-win${bitness}.jar)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set_target_properties(
            ${ROCKSDBJNI_STATIC_LIB}
            PROPERTIES
            OUTPUT_NAME "rocksdbjni-linux${bitness}"
    )
    set(ROCKSDB_JAR rocksdbjni-${CMAKE_PROJECT_VERSION}-linux${bitness}.jar)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set_target_properties(
            ${ROCKSDBJNI_STATIC_LIB}
            PROPERTIES
            OUTPUT_NAME rocksdbjni-osx-${CMAKE_SYSTEM_PROCESSOR}
    )
    set_target_properties(
            ${ROCKSDBJNI_STATIC_LIB}
            PROPERTIES
            SUFFIX ".jnilib"
    )
    set(ROCKSDB_JAR rocksdbjni-${CMAKE_PROJECT_VERSION}-osx.jar)
else()
    set(ROCKSDB_JAR rocksdb-${CMAKE_PROJECT_VERSION}.jar)
endif()

get_target_property(ROCKS_JAR_FILE rocksdbjni_classes JAR_FILE)

add_custom_target(rocksdbjava ALL
        COMMAND ${CMAKE_COMMAND} -E copy ${ROCKS_JAR_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAR}
        COMMAND ${Java_JAR_EXECUTABLE} -uf ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAR} -C $<TARGET_FILE_DIR:${ROCKSDBJNI_STATIC_LIB}> $<TARGET_FILE_NAME:${ROCKSDBJNI_STATIC_LIB}>
        COMMAND ${Java_JAR_EXECUTABLE} -uf ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAR} -C ${CMAKE_CURRENT_SOURCE_DIR} HISTORY-JAVA.md
        DEPENDS ${ROCKSDBJNI_STATIC_LIB} rocksdbjni_classes
        BYPRODUCTS ${ROCKSDB_JAR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )

enable_testing()
get_target_property(ROCKSDBJNI_CLASSES_TEST_JAR_FILE rocksdbjni_test_classes JAR_FILE)
foreach (CLAZZ ${JAVA_TEST_RUNNING_CLASSES})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    add_test(
      NAME jtest_${CLAZZ}
            COMMAND ${Java_JAVA_EXECUTABLE} ${JVMARGS} -ea -Xcheck:jni -Djava.library.path=${PROJECT_BINARY_DIR}/java/${CMAKE_BUILD_TYPE} -classpath ${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAR}$<SEMICOLON>${JAVA_RUN_TESTCLASSPATH}$<SEMICOLON>${ROCKSDBJNI_CLASSES_TEST_JAR_FILE} org.rocksdb.test.RocksJunitRunner ${CLAZZ}
    )
  else()
    add_test(
      NAME jtest_${CLAZZ}
            COMMAND ${Java_JAVA_EXECUTABLE} ${JVMARGS} -ea -Xcheck:jni -Djava.library.path=${PROJECT_BINARY_DIR}/java -classpath ${JAVA_RUN_TESTCLASSPATH}:${ROCKSDBJNI_CLASSES_TEST_JAR_FILE}:${CMAKE_CURRENT_BINARY_DIR}/${ROCKSDB_JAR} org.rocksdb.test.RocksJunitRunner ${CLAZZ}
    )
  endif()
endforeach(CLAZZ)
