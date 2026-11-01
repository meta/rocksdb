//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/statistics.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>

#include "monitoring/statistics_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {BLOCK_CACHE_MISS, "rocksdb.block.cache.miss"},
    {BLOCK_CACHE_HIT, "rocksdb.block.cache.hit"},
    {BLOCK_CACHE_ADD, "rocksdb.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "rocksdb.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "rocksdb.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "rocksdb.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_ADD, "rocksdb.block.cache.index.add"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "rocksdb.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_FILTER_MISS, "rocksdb.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "rocksdb.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_ADD, "rocksdb.block.cache.filter.add"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "rocksdb.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_DATA_MISS, "rocksdb.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "rocksdb.block.cache.data.hit"},
    {BLOCK_CACHE_DATA_ADD, "rocksdb.block.cache.data.add"},
    {BLOCK_CACHE_DATA_BYTES_INSERT, "rocksdb.block.cache.data.bytes.insert"},
    {BLOCK_CACHE_BYTES_READ, "rocksdb.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "rocksdb.block.cache.bytes.write"},
    {BLOCK_CACHE_COMPRESSION_DICT_MISS,
     "rocksdb.block.cache.compression.dict.miss"},
    {BLOCK_CACHE_COMPRESSION_DICT_HIT,
     "rocksdb.block.cache.compression.dict.hit"},
    {BLOCK_CACHE_COMPRESSION_DICT_ADD,
     "rocksdb.block.cache.compression.dict.add"},
    {BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
     "rocksdb.block.cache.compression.dict.bytes.insert"},
    {BLOCK_CACHE_ADD_REDUNDANT, "rocksdb.block.cache.add.redundant"},
    {BLOCK_CACHE_INDEX_ADD_REDUNDANT,
     "rocksdb.block.cache.index.add.redundant"},
    {BLOCK_CACHE_FILTER_ADD_REDUNDANT,
     "rocksdb.block.cache.filter.add.redundant"},
    {BLOCK_CACHE_DATA_ADD_REDUNDANT, "rocksdb.block.cache.data.add.redundant"},
    {BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT,
     "rocksdb.block.cache.compression.dict.add.redundant"},
    {SECONDARY_CACHE_HITS, "rocksdb.secondary.cache.hits"},
    {SECONDARY_CACHE_FILTER_HITS, "rocksdb.secondary.cache.filter.hits"},
    {SECONDARY_CACHE_INDEX_HITS, "rocksdb.secondary.cache.index.hits"},
    {SECONDARY_CACHE_DATA_HITS, "rocksdb.secondary.cache.data.hits"},
    {COMPRESSED_SECONDARY_CACHE_DUMMY_HITS,
     "rocksdb.compressed.secondary.cache.dummy.hits"},
    {COMPRESSED_SECONDARY_CACHE_HITS,
     "rocksdb.compressed.secondary.cache.hits"},
    {COMPRESSED_SECONDARY_CACHE_PROMOTIONS,
     "rocksdb.compressed.secondary.cache.promotions"},
    {COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS,
     "rocksdb.compressed.secondary.cache.promotion.skips"},
    {BLOOM_FILTER_USEFUL, "rocksdb.bloom.filter.useful"},
    {BLOOM_FILTER_FULL_POSITIVE, "rocksdb.bloom.filter.full.positive"},
    {BLOOM_FILTER_FULL_TRUE_POSITIVE,
     "rocksdb.bloom.filter.full.true.positive"},
    {BLOOM_FILTER_PREFIX_CHECKED, "rocksdb.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "rocksdb.bloom.filter.prefix.useful"},
    {BLOOM_FILTER_PREFIX_TRUE_POSITIVE,
     "rocksdb.bloom.filter.prefix.true.positive"},
    {PERSISTENT_CACHE_HIT, "rocksdb.persistent.cache.hit"},
    {PERSISTENT_CACHE_MISS, "rocksdb.persistent.cache.miss"},
    {SIM_BLOCK_CACHE_HIT, "rocksdb.sim.block.cache.hit"},
    {SIM_BLOCK_CACHE_MISS, "rocksdb.sim.block.cache.miss"},
    {MEMTABLE_HIT, "rocksdb.memtable.hit"},
    {MEMTABLE_MISS, "rocksdb.memtable.miss"},
    {GET_HIT_L0, "rocksdb.l0.hit"},
    {GET_HIT_L1, "rocksdb.l1.hit"},
    {GET_HIT_L2_AND_UP, "rocksdb.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "rocksdb.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "rocksdb.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_RANGE_DEL, "rocksdb.compaction.key.drop.range_del"},
    {COMPACTION_KEY_DROP_USER, "rocksdb.compaction.key.drop.user"},
    {COMPACTION_RANGE_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.range_del.drop.obsolete"},
    {COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.optimized.del.drop.obsolete"},
    {COMPACTION_CANCELLED, "rocksdb.compaction.cancelled"},
    {COMPACTION_ABORTED, "rocksdb.compaction.aborted"},
    {NUMBER_KEYS_WRITTEN, "rocksdb.number.keys.written"},
    {NUMBER_KEYS_READ, "rocksdb.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "rocksdb.number.keys.updated"},
    {BYTES_WRITTEN, "rocksdb.bytes.written"},
    {BYTES_READ, "rocksdb.bytes.read"},
    {NUMBER_DB_SEEK, "rocksdb.number.db.seek"},
    {NUMBER_DB_NEXT, "rocksdb.number.db.next"},
    {NUMBER_DB_PREV, "rocksdb.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "rocksdb.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "rocksdb.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "rocksdb.number.db.prev.found"},
    {ITER_BYTES_READ, "rocksdb.db.iter.bytes.read"},
    {NUMBER_ITER_SKIP, "rocksdb.number.iter.skip"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "rocksdb.number.reseeks.iteration"},
    {NO_ITERATOR_CREATED, "rocksdb.num.iterator.created"},
    {NO_ITERATOR_DELETED, "rocksdb.num.iterator.deleted"},
    {NO_FILE_OPENS, "rocksdb.no.file.opens"},
    {NO_FILE_ERRORS, "rocksdb.no.file.errors"},
    {STALL_MICROS, "rocksdb.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "rocksdb.db.mutex.wait.micros"},
    {NUMBER_MULTIGET_CALLS, "rocksdb.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "rocksdb.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "rocksdb.number.multiget.bytes.read"},
    {NUMBER_MULTIGET_KEYS_FOUND, "rocksdb.number.multiget.keys.found"},
    {NUMBER_MERGE_FAILURES, "rocksdb.number.merge.failures"},
    {GET_UPDATES_SINCE_CALLS, "rocksdb.getupdatessince.calls"},
    {WAL_FILE_SYNCED, "rocksdb.wal.synced"},
    {WAL_FILE_BYTES, "rocksdb.wal.bytes"},
    {WAL_PRECREATE_HIT, "rocksdb.wal.precreate.hit"},
    {WAL_PRECREATE_MISS, "rocksdb.wal.precreate.miss"},
    {WAL_PRECREATE_WAITED, "rocksdb.wal.precreate.waited"},
    {WAL_PRECREATE_WAIT_MICROS, "rocksdb.wal.precreate.wait.micros"},
    {WAL_PRECREATE_FAILED, "rocksdb.wal.precreate.failed"},
    {WRITE_DONE_BY_SELF, "rocksdb.write.self"},
    {WRITE_DONE_BY_OTHER, "rocksdb.write.other"},
    {WRITE_WITH_WAL, "rocksdb.write.wal"},
    {COMPACT_READ_BYTES, "rocksdb.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "rocksdb.compact.write.bytes"},
    {FLUSH_WRITE_BYTES, "rocksdb.flush.write.bytes"},
    {COMPACT_READ_BYTES_MARKED, "rocksdb.compact.read.marked.bytes"},
    {COMPACT_READ_BYTES_PERIODIC, "rocksdb.compact.read.periodic.bytes"},
    {COMPACT_READ_BYTES_TTL, "rocksdb.compact.read.ttl.bytes"},
    {COMPACT_WRITE_BYTES_MARKED, "rocksdb.compact.write.marked.bytes"},
    {COMPACT_WRITE_BYTES_PERIODIC, "rocksdb.compact.write.periodic.bytes"},
    {COMPACT_WRITE_BYTES_TTL, "rocksdb.compact.write.ttl.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "rocksdb.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "rocksdb.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "rocksdb.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "rocksdb.number.superversion_cleanups"},
    {NUMBER_BLOCK_COMPRESSED, "rocksdb.number.block.compressed"},
    {NUMBER_BLOCK_DECOMPRESSED, "rocksdb.number.block.decompressed"},
    {BYTES_COMPRESSED_FROM, "rocksdb.bytes.compressed.from"},
    {BYTES_COMPRESSED_TO, "rocksdb.bytes.compressed.to"},
    {BYTES_COMPRESSION_BYPASSED, "rocksdb.bytes.compression_bypassed"},
    {BYTES_COMPRESSION_REJECTED, "rocksdb.bytes.compression.rejected"},
    {NUMBER_BLOCK_COMPRESSION_BYPASSED,
     "rocksdb.number.block_compression_bypassed"},
    {NUMBER_BLOCK_COMPRESSION_REJECTED,
     "rocksdb.number.block_compression_rejected"},
    {BYTES_DECOMPRESSED_FROM, "rocksdb.bytes.decompressed.from"},
    {BYTES_DECOMPRESSED_TO, "rocksdb.bytes.decompressed.to"},
    {MERGE_OPERATION_TOTAL_TIME, "rocksdb.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "rocksdb.filter.operation.time.nanos"},
    {COMPACTION_CPU_TOTAL_TIME, "rocksdb.compaction.total.time.cpu_micros"},
    {ROW_CACHE_HIT, "rocksdb.row.cache.hit"},
    {ROW_CACHE_MISS, "rocksdb.row.cache.miss"},
    {READ_AMP_ESTIMATE_USEFUL_BYTES, "rocksdb.read.amp.estimate.useful.bytes"},
    {READ_AMP_TOTAL_READ_BYTES, "rocksdb.read.amp.total.read.bytes"},
    {NUMBER_RATE_LIMITER_DRAINS, "rocksdb.number.rate_limiter.drains"},
    {RATE_LIMITER_BYTES_READ, "rocksdb.rate.limiter.bytes.read"},
    {RATE_LIMITER_BYTES_WRITE, "rocksdb.rate.limiter.bytes.write"},
    {RATE_LIMITER_REQUESTS_READ, "rocksdb.rate.limiter.requests.read"},
    {RATE_LIMITER_REQUESTS_WRITE, "rocksdb.rate.limiter.requests.write"},
    {RATE_LIMITER_DELAYED_REQUESTS_READ,
     "rocksdb.rate.limiter.delayed.requests.read"},
    {RATE_LIMITER_DELAYED_REQUESTS_WRITE,
     "rocksdb.rate.limiter.delayed.requests.write"},
    {RATE_LIMITER_TOTAL_WAIT_MICROS_READ,
     "rocksdb.rate.limiter.total.wait.micros.read"},
    {RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE,
     "rocksdb.rate.limiter.total.wait.micros.write"},
    {BLOB_DB_NUM_PUT, "rocksdb.blobdb.num.put"},
    {BLOB_DB_NUM_WRITE, "rocksdb.blobdb.num.write"},
    {BLOB_DB_NUM_GET, "rocksdb.blobdb.num.get"},
    {BLOB_DB_NUM_MULTIGET, "rocksdb.blobdb.num.multiget"},
    {BLOB_DB_NUM_SEEK, "rocksdb.blobdb.num.seek"},
    {BLOB_DB_NUM_NEXT, "rocksdb.blobdb.num.next"},
    {BLOB_DB_NUM_PREV, "rocksdb.blobdb.num.prev"},
    {BLOB_DB_NUM_KEYS_WRITTEN, "rocksdb.blobdb.num.keys.written"},
    {BLOB_DB_NUM_KEYS_READ, "rocksdb.blobdb.num.keys.read"},
    {BLOB_DB_BYTES_WRITTEN, "rocksdb.blobdb.bytes.written"},
    {BLOB_DB_BYTES_READ, "rocksdb.blobdb.bytes.read"},
    {BLOB_DB_WRITE_INLINED_DEPRECATED, "rocksdb.blobdb.write.inlined"},
    {BLOB_DB_WRITE_INLINED_TTL_DEPRECATED, "rocksdb.blobdb.write.inlined.ttl"},
    {BLOB_DB_WRITE_BLOB, "rocksdb.blobdb.write.blob"},
    {BLOB_DB_WRITE_BLOB_TTL, "rocksdb.blobdb.write.blob.ttl"},
    {BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "rocksdb.blobdb.blob.file.bytes.written"},
    {BLOB_DB_BLOB_FILE_BYTES_READ, "rocksdb.blobdb.blob.file.bytes.read"},
    {BLOB_DB_BLOB_FILE_SYNCED, "rocksdb.blobdb.blob.file.synced"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
     "rocksdb.blobdb.blob.index.expired.count"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, "rocksdb.blobdb.blob.index.expired.size"},
    {BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
     "rocksdb.blobdb.blob.index.evicted.count"},
    {BLOB_DB_BLOB_INDEX_EVICTED_SIZE, "rocksdb.blobdb.blob.index.evicted.size"},
    {BLOB_DB_GC_NUM_FILES, "rocksdb.blobdb.gc.num.files"},
    {BLOB_DB_GC_NUM_NEW_FILES, "rocksdb.blobdb.gc.num.new.files"},
    {BLOB_DB_GC_FAILURES, "rocksdb.blobdb.gc.failures"},
    {BLOB_DB_GC_NUM_KEYS_RELOCATED, "rocksdb.blobdb.gc.num.keys.relocated"},
    {BLOB_DB_GC_BYTES_RELOCATED, "rocksdb.blobdb.gc.bytes.relocated"},
    {BLOB_DB_FIFO_NUM_FILES_EVICTED, "rocksdb.blobdb.fifo.num.files.evicted"},
    {BLOB_DB_FIFO_NUM_KEYS_EVICTED, "rocksdb.blobdb.fifo.num.keys.evicted"},
    {BLOB_DB_FIFO_BYTES_EVICTED, "rocksdb.blobdb.fifo.bytes.evicted"},
    {BLOB_DB_CACHE_MISS, "rocksdb.blobdb.cache.miss"},
    {BLOB_DB_CACHE_HIT, "rocksdb.blobdb.cache.hit"},
    {BLOB_DB_CACHE_ADD, "rocksdb.blobdb.cache.add"},
    {BLOB_DB_CACHE_ADD_FAILURES, "rocksdb.blobdb.cache.add.failures"},
    {BLOB_DB_CACHE_BYTES_READ, "rocksdb.blobdb.cache.bytes.read"},
    {BLOB_DB_CACHE_BYTES_WRITE, "rocksdb.blobdb.cache.bytes.write"},
    {TXN_PREPARE_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.prepare"},
    {TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
     "rocksdb.txn.overhead.mutex.old.commit.map"},
    {TXN_DUPLICATE_KEY_OVERHEAD, "rocksdb.txn.overhead.duplicate.key"},
    {TXN_SNAPSHOT_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.snapshot"},
    {TXN_GET_TRY_AGAIN, "rocksdb.txn.get.tryagain"},
    {FILES_MARKED_TRASH, "rocksdb.files.marked.trash"},
    {FILES_DELETED_FROM_TRASH_QUEUE, "rocksdb.files.marked.trash.deleted"},
    {FILES_DELETED_IMMEDIATELY, "rocksdb.files.deleted.immediately"},
    {ERROR_HANDLER_BG_ERROR_COUNT, "rocksdb.error.handler.bg.error.count"},
    {ERROR_HANDLER_BG_IO_ERROR_COUNT,
     "rocksdb.error.handler.bg.io.error.count"},
    {ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT,
     "rocksdb.error.handler.bg.retryable.io.error.count"},
    {ERROR_HANDLER_AUTORESUME_COUNT, "rocksdb.error.handler.autoresume.count"},
    {ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT,
     "rocksdb.error.handler.autoresume.retry.total.count"},
    {ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT,
     "rocksdb.error.handler.autoresume.success.count"},
    {MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
     "rocksdb.memtable.payload.bytes.at.flush"},
    {MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
     "rocksdb.memtable.garbage.bytes.at.flush"},
    {VERIFY_CHECKSUM_READ_BYTES, "rocksdb.verify_checksum.read.bytes"},
    {BACKUP_READ_BYTES, "rocksdb.backup.read.bytes"},
    {BACKUP_WRITE_BYTES, "rocksdb.backup.write.bytes"},
    {REMOTE_COMPACT_READ_BYTES, "rocksdb.remote.compact.read.bytes"},
    {REMOTE_COMPACT_WRITE_BYTES, "rocksdb.remote.compact.write.bytes"},
    {REMOTE_COMPACT_RESUMED_BYTES, "rocksdb.remote.compact.resumed.bytes"},
    {HOT_FILE_READ_BYTES, "rocksdb.hot.file.read.bytes"},
    {WARM_FILE_READ_BYTES, "rocksdb.warm.file.read.bytes"},
    {COOL_FILE_READ_BYTES, "rocksdb.cool.file.read.bytes"},
    {COLD_FILE_READ_BYTES, "rocksdb.cold.file.read.bytes"},
    {ICE_FILE_READ_BYTES, "rocksdb.ice.file.read.bytes"},
    {HOT_FILE_READ_COUNT, "rocksdb.hot.file.read.count"},
    {WARM_FILE_READ_COUNT, "rocksdb.warm.file.read.count"},
    {COOL_FILE_READ_COUNT, "rocksdb.cool.file.read.count"},
    {COLD_FILE_READ_COUNT, "rocksdb.cold.file.read.count"},
    {ICE_FILE_READ_COUNT, "rocksdb.ice.file.read.count"},
    {LAST_LEVEL_READ_BYTES, "rocksdb.last.level.read.bytes"},
    {LAST_LEVEL_READ_COUNT, "rocksdb.last.level.read.count"},
    {NON_LAST_LEVEL_READ_BYTES, "rocksdb.non.last.level.read.bytes"},
    {NON_LAST_LEVEL_READ_COUNT, "rocksdb.non.last.level.read.count"},
    {LAST_LEVEL_SEEK_FILTERED, "rocksdb.last.level.seek.filtered"},
    {LAST_LEVEL_SEEK_FILTER_MATCH, "rocksdb.last.level.seek.filter.match"},
    {LAST_LEVEL_SEEK_DATA, "rocksdb.last.level.seek.data"},
    {LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER,
     "rocksdb.last.level.seek.data.useful.no.filter"},
    {LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH,
     "rocksdb.last.level.seek.data.useful.filter.match"},
    {NON_LAST_LEVEL_SEEK_FILTERED, "rocksdb.non.last.level.seek.filtered"},
    {NON_LAST_LEVEL_SEEK_FILTER_MATCH,
     "rocksdb.non.last.level.seek.filter.match"},
    {NON_LAST_LEVEL_SEEK_DATA, "rocksdb.non.last.level.seek.data"},
    {NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER,
     "rocksdb.non.last.level.seek.data.useful.no.filter"},
    {NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH,
     "rocksdb.non.last.level.seek.data.useful.filter.match"},
    {BLOCK_CHECKSUM_COMPUTE_COUNT, "rocksdb.block.checksum.compute.count"},
    {BLOCK_CHECKSUM_MISMATCH_COUNT, "rocksdb.block.checksum.mismatch.count"},
    {MULTIGET_COROUTINE_COUNT, "rocksdb.multiget.coroutine.count"},
    {READ_ASYNC_MICROS, "rocksdb.read.async.micros"},
    {ASYNC_READ_ERROR_COUNT, "rocksdb.async.read.error.count"},
    {TABLE_OPEN_PREFETCH_TAIL_MISS, "rocksdb.table.open.prefetch.tail.miss"},
    {TABLE_OPEN_PREFETCH_TAIL_HIT, "rocksdb.table.open.prefetch.tail.hit"},
    {TIMESTAMP_FILTER_TABLE_CHECKED, "rocksdb.timestamp.filter.table.checked"},
    {TIMESTAMP_FILTER_TABLE_FILTERED,
     "rocksdb.timestamp.filter.table.filtered"},
    {READAHEAD_TRIMMED, "rocksdb.readahead.trimmed"},
    {FIFO_MAX_SIZE_COMPACTIONS, "rocksdb.fifo.max.size.compactions"},
    {FIFO_TTL_COMPACTIONS, "rocksdb.fifo.ttl.compactions"},
    {FIFO_CHANGE_TEMPERATURE_COMPACTIONS,
     "rocksdb.fifo.change_temperature.compactions"},
    {PREFETCH_BYTES, "rocksdb.prefetch.bytes"},
    {PREFETCH_BYTES_USEFUL, "rocksdb.prefetch.bytes.useful"},
    {PREFETCH_HITS, "rocksdb.prefetch.hits"},
    {SST_FOOTER_CORRUPTION_COUNT, "rocksdb.footer.corruption.count"},
    {FILE_READ_CORRUPTION_RETRY_COUNT,
     "rocksdb.file.read.corruption.retry.count"},
    {FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT,
     "rocksdb.file.read.corruption.retry.success.count"},
    {NUMBER_WBWI_INGEST, "rocksdb.number.wbwi.ingest"},
    {SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT,
     "rocksdb.sst.user.defined.index.load.fail.count"},
    {MULTISCAN_PREPARE_CALLS, "rocksdb.multiscan.prepare.calls"},
    {MULTISCAN_PREPARE_ERRORS, "rocksdb.multiscan.prepare.errors"},
    {MULTISCAN_BLOCKS_PREFETCHED, "rocksdb.multiscan.blocks.prefetched"},
    {MULTISCAN_BLOCKS_FROM_CACHE, "rocksdb.multiscan.blocks.from.cache"},
    {MULTISCAN_PREFETCH_BYTES, "rocksdb.multiscan.prefetch.bytes"},
    {MULTISCAN_PREFETCH_BLOCKS_WASTED,
     "rocksdb.multiscan.prefetch.blocks.wasted"},
    {MULTISCAN_IO_REQUESTS, "rocksdb.multiscan.io.requests"},
    {MULTISCAN_IO_COALESCED_NONADJACENT,
     "rocksdb.multiscan.io.coalesced.nonadjacent"},
    {MULTISCAN_SEEK_ERRORS, "rocksdb.multiscan.seek.errors"},
    {PREFETCH_MEMORY_BYTES_GRANTED, "rocksdb.prefetch.memory.bytes.granted"},
    {PREFETCH_MEMORY_BYTES_RELEASED, "rocksdb.prefetch.memory.bytes.released"},
    {PREFETCH_MEMORY_REQUESTS_BLOCKED,
     "rocksdb.prefetch.memory.requests.blocked"},
    {READ_PATH_RANGE_TOMBSTONES_INSERTED,
     "rocksdb.read.path.range.tombstones.inserted"},
    {READ_PATH_RANGE_TOMBSTONES_DISCARDED,
     "rocksdb.read.path.range.tombstones.discarded"},
    {FILE_OPEN_METADATA_RETRIEVED, "rocksdb.file.open.metadata.retrieved"},
    {FILE_OPEN_METADATA_PASSED, "rocksdb.file.open.metadata.passed"},
    {MANIFEST_VALIDATION_FAILURE_COUNT,
     "rocksdb.manifest.validation.failure.count"},
    {FLUSH_REASON_WRITE_BUFFER_FULL, "rocksdb.flush.reason.write_buffer_full"},
    {FLUSH_REASON_WRITE_BUFFER_MANAGER,
     "rocksdb.flush.reason.write_buffer_manager"},
    {FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS,
     "rocksdb.flush.reason.memtable_max_range_deletions"},
    {ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL,
     "rocksdb.atomic_flush.request.reason.write_buffer_full"},
    {ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER,
     "rocksdb.atomic_flush.request.reason.write_buffer_manager"},
    {ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS,
     "rocksdb.atomic_flush.request.reason.memtable_max_range_deletions"},
    {ATOMIC_FLUSH_REQUEST_REASON_OTHER,
     "rocksdb.atomic_flush.request.reason.other"},
    {CHECKPOINT_READ_BYTES, "rocksdb.checkpoint.read.bytes"},
    {CHECKPOINT_WRITE_BYTES, "rocksdb.checkpoint.write.bytes"},
    {FILE_SUBMIT_ASYNC_READ_FALLBACK,
     "rocksdb.file.submit.async.read.fallback"},
    {BLOB_DB_LAZY_READ_COUNT, "rocksdb.blobdb.lazy.read.count"},
    {BLOB_DB_LAZY_READ_BYTES, "rocksdb.blobdb.lazy.read.bytes"},
    {BLOB_DB_LAZY_PARTIAL_READ_COUNT, "rocksdb.blobdb.lazy.partial.read.count"},
    {BLOB_DB_LAZY_PARTIAL_BYTES_SAVED,
     "rocksdb.blobdb.lazy.partial.bytes.saved"},
    {REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT,
     "rocksdb.remote.compact.input.blob.file.count"},
    {REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT,
     "rocksdb.remote.compact.blob.result.declined.count"},
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
    {DB_GET, "rocksdb.db.get.micros"},
    {DB_WRITE, "rocksdb.db.write.micros"},
    {COMPACTION_TIME, "rocksdb.compaction.times.micros"},
    {COMPACTION_CPU_TIME, "rocksdb.compaction.times.cpu_micros"},
    {SUBCOMPACTION_SETUP_TIME, "rocksdb.subcompaction.setup.times.micros"},
    {TABLE_SYNC_MICROS, "rocksdb.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "rocksdb.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "rocksdb.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "rocksdb.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "rocksdb.table.open.io.micros"},
    {DB_MULTIGET, "rocksdb.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "rocksdb.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "rocksdb.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "rocksdb.write.raw.block.micros"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "rocksdb.numfiles.in.singlecompaction"},
    {DB_SEEK, "rocksdb.db.seek.micros"},
    {WRITE_STALL, "rocksdb.db.write.stall"},
    {SST_READ_MICROS, "rocksdb.sst.read.micros"},
    {FILE_READ_FLUSH_MICROS, "rocksdb.file.read.flush.micros"},
    {FILE_READ_COMPACTION_MICROS, "rocksdb.file.read.compaction.micros"},
    {FILE_READ_DB_OPEN_MICROS, "rocksdb.file.read.db.open.micros"},
    {FILE_READ_GET_MICROS, "rocksdb.file.read.get.micros"},
    {FILE_READ_MULTIGET_MICROS, "rocksdb.file.read.multiget.micros"},
    {FILE_READ_DB_ITERATOR_MICROS, "rocksdb.file.read.db.iterator.micros"},
    {FILE_READ_VERIFY_DB_CHECKSUM_MICROS,
     "rocksdb.file.read.verify.db.checksum.micros"},
    {FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS,
     "rocksdb.file.read.verify.file.checksums.micros"},
    {SST_WRITE_MICROS, "rocksdb.sst.write.micros"},
    {FILE_WRITE_FLUSH_MICROS, "rocksdb.file.write.flush.micros"},
    {FILE_WRITE_COMPACTION_MICROS, "rocksdb.file.write.compaction.micros"},
    {FILE_WRITE_DB_OPEN_MICROS, "rocksdb.file.write.db.open.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "rocksdb.num.subcompactions.scheduled"},
    {BYTES_PER_READ, "rocksdb.bytes.per.read"},
    {BYTES_PER_WRITE, "rocksdb.bytes.per.write"},
    {BYTES_PER_MULTIGET, "rocksdb.bytes.per.multiget"},
    {COMPRESSION_TIMES_NANOS, "rocksdb.compression.times.nanos"},
    {DECOMPRESSION_TIMES_NANOS, "rocksdb.decompression.times.nanos"},
    {READ_NUM_MERGE_OPERANDS, "rocksdb.read.num.merge_operands"},
    {BLOB_DB_KEY_SIZE, "rocksdb.blobdb.key.size"},
    {BLOB_DB_VALUE_SIZE, "rocksdb.blobdb.value.size"},
    {BLOB_DB_WRITE_MICROS, "rocksdb.blobdb.write.micros"},
    {BLOB_DB_GET_MICROS, "rocksdb.blobdb.get.micros"},
    {BLOB_DB_MULTIGET_MICROS, "rocksdb.blobdb.multiget.micros"},
    {BLOB_DB_SEEK_MICROS, "rocksdb.blobdb.seek.micros"},
    {BLOB_DB_NEXT_MICROS, "rocksdb.blobdb.next.micros"},
    {BLOB_DB_PREV_MICROS, "rocksdb.blobdb.prev.micros"},
    {BLOB_DB_BLOB_FILE_WRITE_MICROS, "rocksdb.blobdb.blob.file.write.micros"},
    {BLOB_DB_BLOB_FILE_READ_MICROS, "rocksdb.blobdb.blob.file.read.micros"},
    {BLOB_DB_BLOB_FILE_SYNC_MICROS, "rocksdb.blobdb.blob.file.sync.micros"},
    {BLOB_DB_COMPRESSION_MICROS, "rocksdb.blobdb.compression.micros"},
    {BLOB_DB_DECOMPRESSION_MICROS, "rocksdb.blobdb.decompression.micros"},
    {FLUSH_TIME, "rocksdb.db.flush.micros"},
    {SST_BATCH_SIZE, "rocksdb.sst.batch.size"},
    {MULTIGET_IO_BATCH_SIZE, "rocksdb.multiget.io.batch.size"},
    {NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
     "rocksdb.num.index.and.filter.blocks.read.per.level"},
    {NUM_SST_READ_PER_LEVEL, "rocksdb.num.sst.read.per.level"},
    {NUM_LEVEL_READ_PER_MULTIGET, "rocksdb.num.level.read.per.multiget"},
    {ERROR_HANDLER_AUTORESUME_RETRY_COUNT,
     "rocksdb.error.handler.autoresume.retry.count"},
    {ASYNC_READ_BYTES, "rocksdb.async.read.bytes"},
    {POLL_WAIT_MICROS, "rocksdb.poll.wait.micros"},
    {COMPACTION_PREFETCH_BYTES, "rocksdb.compaction.prefetch.bytes"},
    {PREFETCHED_BYTES_DISCARDED, "rocksdb.prefetched.bytes.discarded"},
    {ASYNC_PREFETCH_ABORT_MICROS, "rocksdb.async.prefetch.abort.micros"},
    {TABLE_OPEN_PREFETCH_TAIL_READ_BYTES,
     "rocksdb.table.open.prefetch.tail.read.bytes"},
    {NUM_OP_PER_TRANSACTION, "rocksdb.num.op.per.transaction"},
    {MULTISCAN_PREPARE_ITERATORS,
     "rocksdb.multiscan.op.prepare.iterators.micros"},
    {MULTISCAN_PREPARE_MICROS, "rocksdb.multiscan.prepare.micros"},
    {MULTISCAN_BLOCKS_PER_PREPARE, "rocksdb.multiscan.blocks.per.prepare"},
    {IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS,
     "rocksdb.iodispatcher.async.read.observed.completion.micros"},
    {IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS,
     "rocksdb.iodispatcher.async.read.poll.wait.micros"},
    {IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS,
     "rocksdb.iodispatcher.async.read.prefetch.lead.micros"},
    {BLOCK_KEY_DISTRIBUTION_CV, "rocksdb.block.key.distribution.cv"},
    {INGEST_EXTERNAL_FILE_PREPARE_TIME,
     "rocksdb.ingest.external.file.prepare.micros"},
    {INGEST_EXTERNAL_FILE_RUN_TIME, "rocksdb.ingest.external.file.run.micros"},
    {RATE_LIMITER_WAIT_MICROS_READ, "rocksdb.rate.limiter.wait.micros.read"},
    {RATE_LIMITER_WAIT_MICROS_WRITE, "rocksdb.rate.limiter.wait.micros.write"},
    {FLUSH_MEMTABLE_MEMORY_BYTES, "rocksdb.flush.memtable.memory.bytes"},
    {FLUSH_MEMTABLE_TOTAL_DATA_SIZE,
     "rocksdb.flush.memtable.total.data.size.bytes"},
    {FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES,
     "rocksdb.flush.write_buffer_full.memtable.memory.bytes"},
    {FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES,
     "rocksdb.flush.write_buffer_manager.memtable.memory.bytes"},
    {OPEN_AND_COMPACT_DB_OPEN_MICROS,
     "rocksdb.open.and.compact.db.open.micros"},
};

std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl>(nullptr);
}

static int RegisterBuiltinStatistics(ObjectLibrary& library,
                                     const std::string& /*arg*/) {
  library.AddFactory<Statistics>(
      StatisticsImpl::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Statistics>* guard,
         std::string* /* errmsg */) {
        guard->reset(new StatisticsImpl(nullptr));
        return guard->get();
      });
  return 1;
}

Status Statistics::CreateFromString(const ConfigOptions& config_options,
                                    const std::string& id,
                                    std::shared_ptr<Statistics>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinStatistics(*(ObjectLibrary::Default().get()), "");
  });
  Status s;
  if (id == "" || id == StatisticsImpl::kClassName()) {
    result->reset(new StatisticsImpl(nullptr));
  } else if (id == kNullptrString) {
    result->reset();
  } else {
    s = LoadSharedObject<Statistics>(config_options, id, result);
  }
  return s;
}

static std::unordered_map<std::string, OptionTypeInfo> stats_type_info = {
    {"inner", OptionTypeInfo::AsCustomSharedPtr<Statistics>(
                  0, OptionVerificationType::kByNameAllowFromNull,
                  OptionTypeFlags::kCompareNever)},
};

StatisticsImpl::StatisticsImpl(std::shared_ptr<Statistics> stats)
    : stats_(std::move(stats)) {
  RegisterOptions("StatisticsOptions", &stats_, &stats_type_info);
}

StatisticsImpl::~StatisticsImpl() = default;

uint64_t StatisticsImpl::getTickerCount(uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

uint64_t StatisticsImpl::getTickerCountLocked(uint32_t tickerType) const {
  assert(tickerType < TICKER_ENUM_MAX);
  uint64_t res = 0;
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res += per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType];
  }
  return res;
}

void StatisticsImpl::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

std::unique_ptr<HistogramImpl> StatisticsImpl::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

std::string StatisticsImpl::getHistogramString(uint32_t histogramType) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

void StatisticsImpl::setTickerCount(uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

void StatisticsImpl::setTickerCountLocked(uint32_t tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    if (core_idx == 0) {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = count;
    } else {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = 0;
    }
  }
}

uint64_t StatisticsImpl::getAndResetTickerCount(uint32_t tickerType) {
  uint64_t sum = 0;
  {
    MutexLock lock(&aggregate_lock_);
    assert(tickerType < TICKER_ENUM_MAX);
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      sum +=
          per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType].exchange(
              0, std::memory_order_relaxed);
    }
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, 0);
  }
  return sum;
}

void StatisticsImpl::recordTick(uint32_t tickerType, uint64_t count) {
  if (get_stats_level() <= StatsLevel::kExceptTickers) {
    return;
  }
  if (tickerType < TICKER_ENUM_MAX) {
    per_core_stats_.Access()->tickers_[tickerType].fetch_add(
        count, std::memory_order_relaxed);
    if (stats_) {
      stats_->recordTick(tickerType, count);
    }
  } else {
    assert(false);
  }
}

void StatisticsImpl::recordInHistogram(uint32_t histogramType, uint64_t value) {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  if (get_stats_level() <= StatsLevel::kExceptHistogramOrTimers) {
    return;
  }
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_ENUM_MAX) {
    stats_->recordInHistogram(histogramType, value);
  }
}

Status StatisticsImpl::Reset() {
  MutexLock lock(&aggregate_lock_);
  for (uint32_t i = 0; i < TICKER_ENUM_MAX; ++i) {
    setTickerCountLocked(i, 0);
  }
  for (uint32_t i = 0; i < HISTOGRAM_ENUM_MAX; ++i) {
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      per_core_stats_.AccessAtCore(core_idx)->histograms_[i].Clear();
    }
  }
  return Status::OK();
}

namespace {

// a buffer size used for temp string buffers
const int kTmpStrBufferSize = 200;

}  // namespace

std::string StatisticsImpl::ToString() const {
  MutexLock lock(&aggregate_lock_);
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n",
             t.second.c_str(), getTickerCountLocked(t.first));
    res.append(buffer);
  }
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    HistogramData hData;
    getHistogramImplLocked(h.first)->Data(&hData);
    // don't handle failures - buffer should always be big enough and arguments
    // should be provided correctly
    int ret =
        snprintf(buffer, kTmpStrBufferSize,
                 "%s P50 : %f P95 : %f P99 : %f P100 : %f COUNT : %" PRIu64
                 " SUM : %" PRIu64 "\n",
                 h.second.c_str(), hData.median, hData.percentile95,
                 hData.percentile99, hData.max, hData.count, hData.sum);
    if (ret < 0 || ret >= kTmpStrBufferSize) {
      assert(false);
      continue;
    }
    res.append(buffer);
  }
  res.shrink_to_fit();
  return res;
}

bool StatisticsImpl::getTickerMap(
    std::map<std::string, uint64_t>* stats_map) const {
  assert(stats_map);
  if (!stats_map) {
    return false;
  }
  stats_map->clear();
  MutexLock lock(&aggregate_lock_);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    (*stats_map)[t.second.c_str()] = getTickerCountLocked(t.first);
  }
  return true;
}

bool StatisticsImpl::HistEnabledForType(uint32_t type) const {
  return type < HISTOGRAM_ENUM_MAX;
}

}  // namespace ROCKSDB_NAMESPACE
