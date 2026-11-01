//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_table_builder.h"

#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "block_cache.h"
#include "builtin_index_factory.h"
#include "cache/cache_entry_roles.h"
#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_constants.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "memory/memory_allocator_impl.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/types.h"
#include "rocksdb/wide_columns.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/index_builder.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/block_based/user_defined_index_wrapper.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/range_del_block.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/bit_fields.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/random.h"
#include "util/semaphore.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

constexpr size_t kBlockTrailerSize = BlockBasedTable::kBlockTrailerSize;

void NotifyFileAppendForTest(const Slice& data,
                             const Crc32cChecksum* crc32c_checksum) {
#ifndef NDEBUG
  std::pair<Slice, const Crc32cChecksum*> append_info(data, crc32c_checksum);
  TEST_SYNC_POINT_CALLBACK("BlockBasedTableBuilder::Append:Before",
                           &append_info);
#else
  (void)data;
  (void)crc32c_checksum;
#endif
}

// ===================== AutoSkip compression =====================
// Fixed-point scale for the continuous compression-ratio estimate q_hat and the
// threshold derived from max_compressed_bytes_per_kb. A stored/uncompressed
// ratio in [0,1] maps to [0, kAutoSkipScale]. Using a fine scale (2^20) so the
// integer EWMA step never stalls short of the threshold (the threshold is at
// most 1023/1024 -> ~kAutoSkipScale).
constexpr uint32_t kAutoSkipScale = 1u << 20;
// Asymmetric EWMA on the estimated stored ratio q_hat (lower ratio == better
// compression). Slightly faster toward better compression (so we resume
// attempting reasonably promptly when compressible data appears) than toward
// worse (build confidence before skipping), expressed as right-shift amounts
// (alpha_fast = 1/8, alpha_slow = 1/16).
//
// The fast side is deliberately NOT very aggressive: with a large alpha_fast a
// single "compressible but not great" sample (ratio just under the bar) would
// yank q_hat below the threshold and flip the whole file back to
// attempt-everything, causing us to under-skip fine-grained marginal-payoff
// mixes whose *aggregate* ratio does not clear the bar. A gentler alpha_fast
// lets q_hat track the true aggregate, so such mixes are skipped as intended,
// while genuinely compressible regions (large diff) still recover within a
// region at negligible cost. Tentative; candidates for a tuning study (e.g.
// remote compaction workers).
constexpr int kAutoSkipAlphaFastShift = 3;
constexpr int kAutoSkipAlphaSlowShift = 4;
// q_hat starts at the best possible ratio ("attempt by default") absent
// carryover, so a fresh file never skips a compressible workload before
// observing it.
constexpr uint32_t kAutoSkipInitialQHat = 0;
// Default nominal sampling interval (skipped data blocks between samples) when
// auto_skip_min_sample_every is left 0.
constexpr uint32_t kAutoSkipDefaultMinSampleEvery = 64;

// Inter-file carryover of the estimate, living on the emit/compaction thread so
// a sustained-incompressible compaction stays confident across the files it
// emits without re-ramping each file. A single slot suffices: consecutive files
// from one flush/compaction thread almost always use the same compression type.
// The slot records the (preferred) CompressionType its estimate was produced
// with, so a file inherits carryover only when its type matches; a type change
// (or an unset slot, marked by kNoCompression) simply starts fresh, so
// different algorithms don't contaminate each other.
//
// Because this is a bare thread_local on a pooled flush/compaction thread, the
// estimate also carries across unrelated jobs that happen to reuse the thread.
// That is fine: job-to-job carryover is not meaningfully different from the
// file-to-file recovery/hysteresis we already rely on within a job -- a fresh
// job samples early (the per-worker sample countdown starts at 0) and keeps
// sampling, so a stale estimate self-corrects quickly (fast toward better
// compression). Scoping the carryover more tightly (per job/CF, with a TTL,
// etc.) would add complexity for nuance that may not pay off, and if it does,
// only negligibly.
struct AutoSkipCarry {
  uint32_t q_hat = kAutoSkipInitialQHat;
  // Preferred CompressionType this estimate was produced with; kNoCompression
  // marks the slot unset/invalid.
  CompressionType type = kNoCompression;
};
// Accessor for the per-thread carryover slot. A function-local thread_local
// (rather than a namespace-scope thread_local) keeps this mutable per-thread
// state out of the global namespace (clang-tidy
// cppcoreguidelines-avoid-non-const-global-variables).
AutoSkipCarry& AutoSkipCarryTLS() {
  thread_local AutoSkipCarry carry{};
  return carry;
}

// Whether data blocks should store the common user-key prefix once (see
// BlockBasedTableOptions::optimize_key_common_prefix). Requires
// format_version >= 8, delta encoding, and no user-defined-timestamp stripping.
bool UseCommonPrefixForDataBlock(const BlockBasedTableOptions& table_options,
                                 const Comparator* ucmp, size_t ts_sz) {
  if (table_options.format_version < 8 || !table_options.use_delta_encoding ||
      ts_sz != 0) {
    return false;
  }
  const bool is_bytewise =
      ucmp == BytewiseComparator() || ucmp == ReverseBytewiseComparator();
  switch (table_options.optimize_key_common_prefix) {
    case BlockBasedTableOptions::OptimizeKeyCommonPrefix::kDisabled:
      return false;
    case BlockBasedTableOptions::OptimizeKeyCommonPrefix::kIfFastSeek:
      return is_bytewise;
    case BlockBasedTableOptions::OptimizeKeyCommonPrefix::kEnabled:
      // (Reverse-)bytewise get the space savings + suffix-seek speedup; other
      // comparators get the space savings only, read via the DataBlockIter
      // full-key reconstruction path.
      return true;
  }
  return false;
}

// Whether index-like blocks (leaf/partition index, and the partitioned index /
// partitioned filter top-level) should store the common user-key prefix once.
// Identical to the data-block gate, but additionally requires
// super_block_alignment_size == 0: super block alignment triggers
// skip_delta_encoding on index entries, which stores a non-restart key with
// shared==0 that the reader cannot distinguish from a stripped restart key.
// Like data blocks, kEnabled extends stripping to custom comparators; the
// reader reconstructs probed restart keys for non-(reverse-)bytewise index
// blocks (IndexBlockIter::SeekImpl falls through to the reconstructing binary
// search).
bool UseCommonPrefixForIndex(const BlockBasedTableOptions& table_options,
                             const Comparator* ucmp, size_t ts_sz) {
  return table_options.super_block_alignment_size == 0 &&
         UseCommonPrefixForDataBlock(table_options, ucmp, ts_sz);
}

// Create a filter block builder based on its type.
FilterBlockBuilder* CreateFilterBlockBuilder(
    const ImmutableCFOptions& /*opt*/, const MutableCFOptions& mopt,
    const FilterBuildingContext& context,
    const bool use_delta_encoding_for_index_values,
    IndexFactoryBuilder* const index_builder, size_t ts_sz,
    const bool persist_user_defined_timestamps, bool use_common_prefix) {
  const BlockBasedTableOptions& table_opt = context.table_options;
  assert(table_opt.filter_policy);  // precondition

  FilterBitsBuilder* filter_bits_builder =
      BloomFilterPolicy::GetBuilderFromContext(context);
  if (filter_bits_builder == nullptr) {
    return nullptr;
  } else {
    if (table_opt.partition_filters) {
      PartitionedIndexBuilder* partitioned_index_builder = nullptr;
      if (index_builder != nullptr) {
        partitioned_index_builder =
            static_cast_with_check<BuiltinIndexFactoryBuilder>(index_builder)
                ->GetPartitionedIndexBuilder();
      }
      // Partitioned filters require a partitioned index; the options
      // validation in Rep's constructor rejects any other combination.
      assert(partitioned_index_builder != nullptr);
      // Since after partition cut request from filter builder it takes time
      // until index builder actully cuts the partition, until the end of a
      // data block potentially with many keys, we take the lower bound as
      // partition size.
      assert(table_opt.block_size_deviation <= 100);
      auto partition_size =
          static_cast<uint32_t>(((table_opt.metadata_block_size *
                                  (100 - table_opt.block_size_deviation)) +
                                 99) /
                                100);
      partition_size = std::max(partition_size, static_cast<uint32_t>(1));
      return new PartitionedFilterBlockBuilder(
          mopt.prefix_extractor.get(), table_opt.whole_key_filtering,
          filter_bits_builder, table_opt.index_block_restart_interval,
          use_delta_encoding_for_index_values, table_opt.format_version,
          partitioned_index_builder, partition_size, ts_sz,
          persist_user_defined_timestamps,
          table_opt.decouple_partitioned_filters, use_common_prefix);
    } else {
      return new FullFilterBlockBuilder(mopt.prefix_extractor.get(),
                                        table_opt.whole_key_filtering,
                                        filter_bits_builder);
    }
  }
}

// A convenience function for populating the Compressor* fields; see ~Rep()
Compressor* MaybeCloneSpecialized(
    Compressor* compressor, CacheEntryRole block_type,
    Compressor::DictConfigArgs&& dict_config = Compressor::DictDisabled{}) {
  auto specialized =
      compressor->MaybeCloneSpecialized(block_type, std::move(dict_config));
  if (specialized) {
    // Caller is responsible for freeing when distinct
    return specialized.release();
  } else {
    return compressor;
  }
}
}  // namespace

// Defined outside the anonymous namespace above (it is a static member of
// BlockBasedTableBuilder) but relies on that namespace's AutoSkip carryover.
// Lets tools that build many independent files on one thread (e.g. sst_dump
// --command=recompress) clear the AutoSkip inter-file estimate between files,
// so each is measured without inheriting the previous file's estimate.
void BlockBasedTableBuilder::ResetThreadLocalAutoSkipCarryover() {
  AutoSkipCarryTLS() = AutoSkipCarry{};
}

// kBlockBasedTableMagicNumber was picked by running
//    echo rocksdb.table.block_based | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by other
// .cc files
// for that reason we declare it extern in the header but to get the space
// allocated
// it must be not extern in one place.
const uint64_t kBlockBasedTableMagicNumber = 0x88e241b785f4cff7ull;

// A collector that collects properties of interest to block-based table.
// For now this class looks heavy-weight since we only write one additional
// property.
// But in the foreseeable future, we will add more and more properties that are
// specific to block-based table.
class BlockBasedTableBuilder::BlockBasedTablePropertiesCollector
    : public InternalTblPropColl {
 public:
  explicit BlockBasedTablePropertiesCollector(
      BlockBasedTableOptions::IndexType index_type, bool whole_key_filtering,
      bool prefix_filtering, bool decoupled_partitioned_filters)
      : index_type_(index_type),
        whole_key_filtering_(whole_key_filtering),
        prefix_filtering_(prefix_filtering),
        decoupled_partitioned_filters_(decoupled_partitioned_filters) {}

  Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                     uint64_t /*file_size*/) override {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    // Intentionally left blank. No interest in collecting stats for
    // blocks.
  }

  Status Finish(UserCollectedProperties* properties) override {
    std::string val;
    PutFixed32(&val, static_cast<uint32_t>(index_type_));
    properties->insert({BlockBasedTablePropertyNames::kIndexType, val});
    properties->insert({BlockBasedTablePropertyNames::kWholeKeyFiltering,
                        whole_key_filtering_ ? kPropTrue : kPropFalse});
    properties->insert({BlockBasedTablePropertyNames::kPrefixFiltering,
                        prefix_filtering_ ? kPropTrue : kPropFalse});
    if (decoupled_partitioned_filters_) {
      properties->insert(
          {BlockBasedTablePropertyNames::kDecoupledPartitionedFilters,
           kPropTrue});
    }
    return Status::OK();
  }

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override {
    return "BlockBasedTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    // Intentionally left blank.
    return UserCollectedProperties();
  }

 private:
  BlockBasedTableOptions::IndexType index_type_;
  bool whole_key_filtering_;
  bool prefix_filtering_;
  bool decoupled_partitioned_filters_;
};

struct BlockBasedTableBuilder::WorkingAreaPair {
  Compressor::ManagedWorkingArea compress;
  Decompressor::ManagedWorkingArea verify;
};

// ParallelCompressionRep essentially defines a framework for parallelizing
// block generation ("emit"), block compression, and block writing to storage.
// The synchronization is lock-free/wait-free, so thread waiting only happens
// when work-order dependencies are unsatisfied, though sleeping/idle threads
// might be kept idle when it seems unlikely they would improve throughput by
// waking them up (essentially auto-tuned parallelism). But because all threads
// are capable of 2 out of 3 kinds of work, in a quasi-work-stealing system,
// running threads can usually expect that compatible work is available.
//
// This is currently activated with CompressionOptions::parallel_threads > 1
// but that is a somewhat crude API that would ideally be adapted along with
// the implementation in the future to allow threads to serve multiple
// flush/compaction jobs, though the available improvement might be small.
// Even within the scope of a single file it might be nice to use a general
// framework for distributing work across threads, but (a) different threads
// are limited to which work they can do because of technical challenges, (b)
// being largely CPU bound on small work units means such a framework would
// likely have big overheads compared to this hand-optimized solution.
struct BlockBasedTableBuilder::ParallelCompressionRep {
  // The framework has two kinds of threads: the calling thread from
  // flush/compaction/SstFileWriter is called the "emit thread" (kEmitter).
  // Other threads cannot generally take over "emit" work because that is
  // largely happening up the call stack from BlockBasedTableBuilder.
  // The emit thread can also take on compression work in a quasi-work-stealing
  // manner when the buffer for emitting new blocks is full.
  //
  // When parallelism is enabled, there are also "worker" threads that
  // can handle compressing blocks and (one worker thread at a time) write them
  // to the SST file (and handle other single-threaded wrap-up of each block).
  //
  // NOTE: when parallelism is enabled, the emit thread is not permitted to
  // write to the SST file because that is the potential "output" bottleneck,
  // and it's generally bad for parallelism to allow the only thread that can
  // serve the "input" bottleneck (emit work) to also spend exclusive time on
  // the output bottleneck.
  enum class ThreadKind {
    kEmitter,
    kWorker,
  };

  // ThreadState allows each thread to track its work assignment. In addition to
  // the cases already mentioned, kEmitting, kCompressing, and kWriting to the
  // SST file writer,
  // * Threads can enter the kIdle state so that they can sleep when no work is
  // available for them, to be woken up when appropriate.
  // * The kEnd state means the thread is not doing any more work items, which
  // for worker threads means they will end soon.
  // * The kCompressingAndWriting state means a worker can compress and write a
  // block without additional state updates because the same block to be
  // compressed is the next to be written.
  enum class ThreadState {
    /* BEGIN Emitter only states */
    kEmitting,
    /* END Emitter only states */
    /* BEGIN states for emitter and worker */
    kIdle,
    kCompressing,
    kEnd,
    /* END states for emitter and worker */
    /* BEGIN Worker only states */
    kCompressingAndWriting,
    kWriting,
    /* END Worker only states */
  };

  // BlockRep instances are used and reused in a ring buffer (below), so that
  // many blocks can be in an intermediate state between serialized into
  // uncompressed bytes and written to the SST file. Notably, each block is
  // "emitted" in uncompressed form into a BlockRep, compressed (at least
  // attempted, when configured) for updated BlockRep, and then written from the
  // BlockRep to the writer for the SST file bytes.
  struct ALIGN_AS(CACHE_LINE_SIZE) BlockRep {
    // Uncompressed block contents
    std::string uncompressed;
    GrowableBuffer compressed;
    CompressionType compression_type = kNoCompression;
    uint32_t contents_checksum = 0;
    // Set by the emit thread: this data block's compression was auto-skipped,
    // so no worker should compress it -- StateTransition auto-completes it
    // straight to the write stage. Set on every emit (true=skip,
    // false=attempt).
    //
    // Atomic purely for TSAN suppression (and technical UB). In cases where a
    // race would be reported, a later atomic CAS would discover that (among
    // other things) the value read was potentially stale; in that case, the
    // read value and all downstream decisions based on it are abandoned.
    // See StateTransition(). Specifically, if a thread is lagging, it could
    // read this value after the slot was opened for recycling and would
    // discover its speculated state transition is stale and rejected by the
    // CAS. If the CAS succeeds, the value of this field was published by the
    // emit thread with acquire/release of atomic_state and not modified
    // elsewhere.
    RelaxedAtomic<bool> auto_skip_bypassed{false};
    std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry> prepared_index_entry;
    bool index_entry_prepared = false;

    // Staged entries, one per Rep::custom_indexes element in matching
    // order. Empty unless every custom builder supports parallel.
    std::vector<std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry>>
        custom_prepared_entries;
    bool custom_entries_prepared = false;
  };

  // Ring buffer of emitted blocks that may or may not yet be compressed.
  std::unique_ptr<BlockRep[]> ring_buffer;
  // log_2(ring buffer size), where ring buffer size must be a power of two
  const int ring_buffer_nbits;
  // ring buffer size - 1, to function as a bit mask for ring buffer positions
  // (e.g. given the ordinal number of a block)
  const uint32_t ring_buffer_mask;
  // Number of threads in worker_threads. (Emit thread doesn't count)
  const uint32_t num_worker_threads;

  // Rough upper bound on the sst file size contribution from blocks emitted
  // into the parallel compression ring buffer but not yet written. Tracks
  // uncompressed size, with trailer, until a block is compressed, then
  // compressed size until the block is written. (TODO: does not currently
  // account for block_align)
  RelaxedAtomic<uint64_t> estimated_inflight_size{0};
  // Thread objects for worker threads
  std::vector<port::Thread> worker_threads;
  // Working areas for data_block_compressor for each worker thread
  std::vector<WorkingAreaPair> working_areas;

  // Semaphores for threads to sleep when there's no available work for them
  // and to wake back up when someone determines there is available work (most
  // likely). Split between worker threads and emit thread because they can do
  // different kinds of work.
  CountingSemaphore idle_worker_sem{0};
  BinarySemaphore idle_emit_sem{0};

  // Primary atomic state of parallel compression, which includes a number of
  // state fields that are best updated atomically to avoid locking and/or to
  // simplify the interesting interleavings that have to be considered and
  // accommodated.
  struct State : public BitFields<uint64_t, State> {};
  ALIGN_AS(CACHE_LINE_SIZE) BitFieldsAtomic<State> atomic_state;

  // The first field is a bit for each ring buffer slot (max 32) for whether
  // that slot is ready to be claimed for writing by a worker thread. Because
  // compressions might finish out-of-order, we need to track individually
  // whether they are finished, though this field doesn't differentiate
  // "compression completed" from "compression not started" because that can be
  // inferred from NextToCompress. A block might not enter this state, because
  // the same thread that compresses it can also immediately write the block if
  // it notices that the block is next to write.
  using NeedsWriter = UnsignedBitField<State, 32, NoPrevBitField>;
  // Track how many worker threads are in an idle state because there was no
  // available work and haven't been selected to wake back up.
  using IdleWorkerCount = UnsignedBitField<State, 5, NeedsWriter>;
  // Track whether the emit thread is an idle state because there was no
  // available work and hasn't been triggered to wake back up. The nature of
  // available work and atomic CAS assignment of work ensures at least one
  // thread is kept out of the idle state.
  using IdleEmitFlag = BoolBitField<State, IdleWorkerCount>;
  // Track whether threads should end when they finish available work because no
  // more blocks will be emitted.
  using NoMoreToEmitFlag = BoolBitField<State, IdleEmitFlag>;
  // Track whether threads should abort ASAP because of an error.
  using AbortFlag = BoolBitField<State, NoMoreToEmitFlag>;
  // Track three "NextTo" counters for the positions of the next block to write,
  // to start compression, and to emit into the ring buffer. If these counters
  // never overflowed / wrapped around, we would have next_to_write <=
  // next_to_compress <= next_to_emit because a block must be emitted before
  // compressed, and compressed (at least attempted) before writing. We need to
  // track more than ring_buffer_nbits of these counters to be able to
  // distinguish an empty ring buffer (next_to_write == next_to_emit) from a
  // full ring buffer (next_to_write != next_to_emit but equal under
  // ring_buffer_mask).
  using NextToWrite = UnsignedBitField<State, 8, AbortFlag>;
  using NextToCompress = UnsignedBitField<State, 8, NextToWrite>;
  using NextToEmit = UnsignedBitField<State, 8, NextToCompress>;
  static_assert(NextToEmit::kEndBit == 64);

  // BEGIN fields for use by the emit thread only. These can't live on the stack
  // because the emit thread frequently returns out of BlockBasedTableBuilder.
  ALIGN_AS(CACHE_LINE_SIZE)
  ThreadState emit_thread_state = ThreadState::kEmitting;
  // Ring buffer index that emit thread is operating on (for emitting and
  // compressing states)
  uint32_t emit_slot = 0;
  // Including some data to inform when to wake up idle worker threads (see
  // implementation for details)
  int32_t emit_counter_toward_wake_up = 0;
  int32_t emit_counter_for_wake_up = 0;
  static constexpr int32_t kMaxWakeupInterval = 8;
  // END fields for use by the emit thread only

  // TSAN on GCC has bugs that report false positives on this watchdog code.
  // Other efforts to work around the bug have failed, so to avoid those false
  // positive reports, we simply disable the watchdog when running under GCC
  // TSAN.
#if !defined(NDEBUG) && !(defined(__GNUC__) && defined(__SANITIZE_THREAD__))
#define BBTB_PC_WATCHDOG 1
#endif
#ifdef BBTB_PC_WATCHDOG
  // These are for an extra "watchdog" thread in DEBUG builds that heuristically
  // checks for the most likely deadlock conditions. False positives and false
  // negatives are technically possible.
  std::thread watchdog_thread;
  std::mutex watchdog_mutex;
  std::condition_variable watchdog_cv;
  bool shutdown_watchdog = false;
  RelaxedAtomic<uint32_t> live_workers{0};
  RelaxedAtomic<uint32_t> idling_workers{0};
  RelaxedAtomic<bool> live_emit{0};
  RelaxedAtomic<bool> idling_emit{0};
#endif  // BBTB_PC_WATCHDOG

  int ComputeRingBufferNbits(uint32_t parallel_threads) {
    // Ring buffer size is a power of two not to exceed 32 but otherwise
    // at least twice the number of threads.
    if (parallel_threads >= 9) {
      return 5;
    } else if (parallel_threads >= 5) {
      return 4;
    } else if (parallel_threads >= 3) {
      return 3;
    } else {
      assert(parallel_threads > 1);
      return 2;
    }
  }

  explicit ParallelCompressionRep(uint32_t parallel_threads)
      : ring_buffer_nbits(ComputeRingBufferNbits(parallel_threads)),
        ring_buffer_mask((uint32_t{1} << ring_buffer_nbits) - 1),
        num_worker_threads(std::min(parallel_threads, ring_buffer_mask)) {
    assert(num_worker_threads <= IdleWorkerCount::kMask);

    ring_buffer = std::make_unique<BlockRep[]>(ring_buffer_mask + 1);

    // Start by aggressively waking up idle workers
    emit_counter_for_wake_up = -static_cast<int32_t>(num_worker_threads);
  }
  // No copying allowed
  ParallelCompressionRep(const ParallelCompressionRep&) = delete;
  ParallelCompressionRep& operator=(const ParallelCompressionRep&) = delete;
  ParallelCompressionRep(ParallelCompressionRep&&) = delete;
  ParallelCompressionRep& operator=(ParallelCompressionRep&&) = delete;

  ~ParallelCompressionRep() {
#ifndef NDEBUG
    auto state = atomic_state.Load();
    if (state.Get<AbortFlag>() == false) {
      // Should be clear / cancelled out with normal shutdown
      assert(state.Get<NeedsWriter>() == 0);

      // Ring buffer reached empty state
      assert(state.Get<NextToWrite>() == state.Get<NextToCompress>());
      assert(state.Get<NextToCompress>() == state.Get<NextToEmit>());

      // Everything cancels out in inflight size
      assert(estimated_inflight_size.LoadRelaxed() == 0);
    }
    // All idling metadata cleaned up, properly tracked
    assert(state.Get<IdleWorkerCount>() == 0);
    assert(state.Get<IdleEmitFlag>() == false);

    // No excess in semaphores
    assert(!idle_emit_sem.TryAcquire());
    assert(!idle_worker_sem.TryAcquire());
#endif  // !NDEBUG
  }

  // The primary function for a thread transitioning from one state or work
  // assignment to the next. `slot` refers to a position in the ring buffer
  // for assigned emit, compression, or write work.
  //
  // Because both the emit thread and worker threads can work on compression,
  // this is a quasi-work-stealing parallel algorithm. (Enabling other threads
  // to do emit work would be quite challenging, and allowing the emit thread
  // to handle writes could create a bottle-neck.)
  //
  // This function is basically a CAS loop trying to pick the next piece of work
  // for this thread and retrying if CAS fails. This function also handles
  // thread idling when that's the appropriate assignment, continuing the loop
  // looking for productive work when woken from an idle state.
  //
  // Precondition: thread_state is appropriate for thread_kind and not kEnd. It
  // must match the previously returned state for that thread, and is only kIdle
  // for the thread on startup (though the kIdle state is used internal to the
  // function).
  //
  // Postcondition: thread_state is appropriate for thread_kind and not kIdle.
  // Except for kEnd state, the calling thread has exclusive access to
  // ring_buffer[slot] until next StateTransition().
  template <ThreadKind thread_kind>
  void StateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    assert(slot <= ring_buffer_mask);
    // Last known value for atomic_state
    State seen_state = atomic_state.Load();

    for (;;) {
      if (seen_state.Get<AbortFlag>()) {
        thread_state = ThreadState::kEnd;
        return;
      }

      assert(static_cast<uint8_t>(seen_state.Get<NextToEmit>() -
                                  seen_state.Get<NextToCompress>()) <=
             ring_buffer_mask + 1);
      assert(static_cast<uint8_t>(seen_state.Get<NextToCompress>() -
                                  seen_state.Get<NextToWrite>()) <=
             ring_buffer_mask + 1);
      assert(static_cast<uint8_t>(seen_state.Get<NextToEmit>() -
                                  seen_state.Get<NextToWrite>()) <=
             ring_buffer_mask + 1);

      // Draft of the next proposed atomic_state. Start by marking completion of
      // the current thread's last work.
      State next_state = seen_state;
      bool wake_idle = false;
      switch (thread_state) {
        case ThreadState::kEmitting: {
          assert(thread_kind == ThreadKind::kEmitter);
          assert(slot == (next_state.Get<NextToEmit>() & ring_buffer_mask));
          next_state.Ref<NextToEmit>() += 1;
          // Check whether to wake up idle worker thread
          if (next_state.Get<IdleWorkerCount>() > 0 &&
              // The number of blocks for which compression hasn't started
              // is well over the number of active threads.
              static_cast<uint8_t>(next_state.Get<NextToEmit>() -
                                   next_state.Get<NextToCompress>()) >=
                  (ring_buffer_mask + 1) / 4 +
                      (num_worker_threads -
                       next_state.Get<IdleWorkerCount>())) {
            // At first, emit_counter_for_wake_up is negative to aggressively
            // wake up idle worker threads. Then it backs off the interval at
            // which we wake up, up to some maximum that attempts to balance
            // maximum throughput and minimum CPU overhead.
            if (emit_counter_toward_wake_up >= emit_counter_for_wake_up) {
              // We reached a threshold to justify a wake-up.
              wake_idle = true;
              // Adjust idle count assuming we are going to own waking it up,
              // so no one else can duplicate that. (The idle count is really
              // the number idling for which no one yet owns waking them up.)
              next_state.Ref<IdleWorkerCount>() -= 1;
              // Reset the counter toward the threshold for wake-up
              emit_counter_toward_wake_up = 0;
              // Raise the threshold (up to some limit) to stabilize the number
              // of active threads after some ramp-up period.
              emit_counter_for_wake_up =
                  std::min(emit_counter_for_wake_up + 1,
                           static_cast<int32_t>(num_worker_threads +
                                                kMaxWakeupInterval));
            } else {
              // Advance closer to the threshold for justifying a wake-up
              emit_counter_toward_wake_up++;
            }
          }
          break;
        }
        case ThreadState::kIdle:
          // NOTE: thread that signalled to wake up already updated idle count
          // or marker. This is required to avoid overflow on the semaphore,
          // especially the binary semaphore for idle_emit_sem, and likely
          // desirable to avoid spurious/extra Release().
          break;
        case ThreadState::kCompressing:
          next_state.Ref<NeedsWriter>() |= uint32_t{1} << slot;
          if constexpr (thread_kind == ThreadKind::kEmitter) {
            if (next_state.Get<IdleWorkerCount>() == num_worker_threads) {
              // Work is available for a worker thread and none are running
              wake_idle = true;
              // Adjust idle count assuming we are going to own waking it up
              next_state.Ref<IdleWorkerCount>() -= 1;
            }
          }
          break;
        case ThreadState::kEnd:
          // Should have already recognized the end state
          assert(thread_state != ThreadState::kEnd);
          return;
        case ThreadState::kCompressingAndWriting:
        case ThreadState::kWriting:
          assert(thread_kind == ThreadKind::kWorker);
          assert((next_state.Get<NextToWrite>() & ring_buffer_mask) == slot);
          assert(next_state.Get<NextToCompress>() !=
                 next_state.Get<NextToWrite>());
          assert(next_state.Get<NextToEmit>() != next_state.Get<NextToWrite>());
          assert((next_state.Get<NeedsWriter>() & (uint32_t{1} << slot)) == 0);
          next_state.Ref<NextToWrite>() += 1;
          if (next_state.Get<IdleEmitFlag>()) {
            wake_idle = true;
            // Clear idle emit flag assuming we are going to own waking it up
            next_state.Set<IdleEmitFlag>(false);
          }
          break;
      }

      // Find the next state, depending on the kind of thread
      ThreadState next_thread_state = ThreadState::kEnd;
      uint32_t next_slot = 0;
      if constexpr (thread_kind == ThreadKind::kEmitter) {
        // First priority is emitting more uncompressed blocks, if there's
        // room in the ring buffer.
        if (static_cast<uint8_t>(next_state.Get<NextToEmit>() -
                                 next_state.Get<NextToWrite>()) <=
            ring_buffer_mask) {
          // There is room
          next_thread_state = ThreadState::kEmitting;
          next_slot = next_state.Get<NextToEmit>() & ring_buffer_mask;
        }
      }
      // Auto-complete any leading auto-skip-bypassed blocks: they need no
      // compression, so advance NextToCompress past them and mark each
      // writer-ready, WITHOUT dispatching a compression worker. Done here
      // (after the emitter's emit priority, before the worker's write priority)
      // so a worker still picks up writing them and the emitter never writes.
      if (next_thread_state == ThreadState::kEnd) {
        while (next_state.Get<NextToCompress>() !=
               next_state.Get<NextToEmit>()) {
          uint32_t cslot = next_state.Get<NextToCompress>() & ring_buffer_mask;
          if (!ring_buffer[cslot].auto_skip_bypassed.LoadRelaxed()) {
            break;
          }
          next_state.Ref<NeedsWriter>() |= uint32_t{1} << cslot;
          next_state.Ref<NextToCompress>() += 1;
        }
      }
      if constexpr (thread_kind == ThreadKind::kWorker) {
        // First priority is writing next block to write, if it needs a writer
        // assigned to it
        uint32_t next_to_write_slot =
            next_state.Get<NextToWrite>() & ring_buffer_mask;
        uint32_t needs_writer_bit = uint32_t{1} << next_to_write_slot;
        if (next_state.Get<NeedsWriter>() & needs_writer_bit) {
          // Clear the "needs writer" marker on the slot
          next_state.Ref<NeedsWriter>() &= ~needs_writer_bit;
          // Take ownership of writing it
          next_thread_state = ThreadState::kWriting;
          next_slot = next_to_write_slot;
        }
      }

      // If didn't find higher priority work
      if (next_thread_state == ThreadState::kEnd) {
        if (next_state.Get<NextToCompress>() != next_state.Get<NextToEmit>()) {
          // Compression work is available, select that
          if (thread_kind == ThreadKind::kWorker &&
              next_state.Get<NextToCompress>() ==
                  next_state.Get<NextToWrite>()) {
            next_thread_state = ThreadState::kCompressingAndWriting;
          } else {
            next_thread_state = ThreadState::kCompressing;
          }
          next_slot = next_state.Get<NextToCompress>() & ring_buffer_mask;
          next_state.Ref<NextToCompress>() += 1;
        } else if constexpr (thread_kind == ThreadKind::kEmitter) {
          // Emitter thread goes idle. But if we just auto-completed skipped
          // blocks (creating writer work) and all workers are idle, wake one to
          // write them -- the emit thread never writes, so otherwise no one
          // would.
          if (!wake_idle && next_state.Get<NeedsWriter>() != 0 &&
              next_state.Get<IdleWorkerCount>() == num_worker_threads) {
            wake_idle = true;
            next_state.Ref<IdleWorkerCount>() -= 1;
          }
          next_thread_state = ThreadState::kIdle;
          assert(next_state.Get<IdleEmitFlag>() == false);
          assert(next_state.Get<NoMoreToEmitFlag>() == false);
          next_state.Set<IdleEmitFlag>(true);
        } else if (next_state.Get<NoMoreToEmitFlag>()) {
          // Worker thread shall not idle if we are done emitting. At least
          // one worker will remain unblocked to finish writing
          next_thread_state = ThreadState::kEnd;
        } else {
          // Worker thread goes idle
          next_thread_state = ThreadState::kIdle;
          assert(next_state.Get<IdleWorkerCount>() < IdleWorkerCount::kMask);
          next_state.Ref<IdleWorkerCount>() += 1;
        }
      }
      assert(thread_state != ThreadState::kEnd);

      // Attempt to atomically apply the desired/computed state transition
      if (atomic_state.CasWeak(seen_state, next_state)) {
        // Success
        thread_state = next_thread_state;
        slot = next_slot;
        seen_state = next_state;
        if (wake_idle) {
          if constexpr (thread_kind == ThreadKind::kEmitter) {
            idle_worker_sem.Release();
          } else {
            idle_emit_sem.Release();
          }
        }
        if (thread_state != ThreadState::kIdle) {
          // Successfully transitioned to another useful state
          return;
        }
        // Handle idle state
        if constexpr (thread_kind == ThreadKind::kEmitter) {
#ifdef BBTB_PC_WATCHDOG
          idling_emit.StoreRelaxed(true);
          Defer decr{[this]() { idling_emit.StoreRelaxed(false); }};
#endif  // BBTB_PC_WATCHDOG

          // Likely go to sleep
          idle_emit_sem.Acquire();
        } else {
#ifdef BBTB_PC_WATCHDOG
          // Tracking for watchdog
          idling_workers.FetchAddRelaxed(1);
          Defer decr{[this]() { idling_workers.FetchSubRelaxed(1); }};
#endif  // BBTB_PC_WATCHDOG

          // Likely go to sleep
          idle_worker_sem.Acquire();
        }
        // Update state after sleep
        seen_state = atomic_state.Load();
      }
      // else loop and try again
    }
  }

  void EmitterStateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    StateTransition<ThreadKind::kEmitter>(thread_state, slot);
  }

  void WorkerStateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    StateTransition<ThreadKind::kWorker>(thread_state, slot);
  }

  // Exactly wake all idling threads (for an end state)
  void WakeAllIdle() {
    State old_state;
    State new_state;
    auto transform =
        IdleEmitFlag::ClearTransform() + IdleWorkerCount::ClearTransform();
    atomic_state.Apply(transform, &old_state, &new_state);
    assert(new_state.Get<IdleEmitFlag>() == false);
    assert(new_state.Get<IdleWorkerCount>() == 0);
    if (old_state.Get<IdleEmitFlag>()) {
      idle_emit_sem.Release();
    }
    idle_worker_sem.Release(old_state.Get<IdleWorkerCount>());
  }

  // Called by emit thread if it is decided no more blocks will be emitted into
  // this SST file.
  void SetNoMoreToEmit(/*in/out*/ ThreadState& thread_state,
                       /*in/out*/ uint32_t& slot) {
    (void)slot;
    State old_state;
    atomic_state.Apply(NoMoreToEmitFlag::SetTransform(), &old_state);
    assert(old_state.Get<NoMoreToEmitFlag>() == false);
    assert(slot == BitwiseAnd(old_state.Get<NextToEmit>(), ring_buffer_mask));
    assert(thread_state == ThreadState::kEmitting);
    thread_state = ThreadState::kEnd;
    WakeAllIdle();
  }

  // Called by any thread to abort parallel compression, etc. because of an
  // error.
  void SetAbort(/*in/out*/ ThreadState& thread_state) {
    State old_state;
    atomic_state.Apply(AbortFlag::SetTransform(), &old_state);
    if (old_state.Get<AbortFlag>() == false) {
      // First to set abort. Wake all workers and emitter
      WakeAllIdle();
    }
    thread_state = ThreadState::kEnd;
  }

#ifdef BBTB_PC_WATCHDOG
  // Logic for the extra "watchdog" thread in DEBUG builds that heuristically
  // checks for the most likely deadlock conditions.
  //
  // Some ways to manually validate the watchdog:
  // * Insert
  //      if (Random::GetTLSInstance()->OneIn(100)) {
  //        sleep(100);
  //      }
  //   after either of the calls to semaphore Acquire above.
  // * Miss some Release()s in WakeAllIdle()
  //
  // and run table_test unit tests.
  void BGWatchdog() {
    int count_toward_deadlock_judgment = 0;
    for (;;) {
      // Check for termination condition: All workers and emit thread have
      // completed.
      if (live_workers.LoadRelaxed() == 0 && live_emit.LoadRelaxed() == false) {
        return;
      }

      // Check for potential deadlock condition
      if (idling_workers.LoadRelaxed() < live_workers.LoadRelaxed() ||
          (live_emit.LoadRelaxed() && !idling_emit.LoadRelaxed())) {
        // Someone is working, all good
        count_toward_deadlock_judgment = 0;
      } else {
        // Could be a deadlock state, but could also be a transient
        // state where someone has woken up but not cleared their idling flag.
        // Give it plenty of time and watchdog thread wake-ups before
        // declaring deadlock.
        count_toward_deadlock_judgment++;
        if (count_toward_deadlock_judgment >= 70) {
          fprintf(stderr,
                  "Error: apparent deadlock in parallel compression. "
                  "Aborting. %u / %u, %d / %d, %llx\n",
                  (unsigned)idling_workers.LoadRelaxed(),
                  (unsigned)live_workers.LoadRelaxed(),
                  (int)idling_emit.LoadRelaxed(), (int)live_emit.LoadRelaxed(),
                  (long long)atomic_state.Load().underlying);
          std::terminate();
        }
      }

      // Sleep for 1s at a time unless we are woken up because other threads
      // ended.
      std::unique_lock<std::mutex> lock(watchdog_mutex);
      if (!shutdown_watchdog) {
        watchdog_cv.wait_for(lock, std::chrono::seconds{1});
      }
    }
  }
#endif  // BBTB_PC_WATCHDOG
};

struct WarmCacheConfig {
  const bool enabled;
  const Cache::Priority priority;

  static WarmCacheConfig Compute(
      BlockBasedTableOptions::PrepopulateBlockCache mode,
      TableFileCreationReason reason) {
    bool enabled = false;
    Cache::Priority priority = Cache::Priority::LOW;
    switch (mode) {
      case BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly:
        enabled = (reason == TableFileCreationReason::kFlush);
        break;
      case BlockBasedTableOptions::PrepopulateBlockCache::kFlushAndCompaction:
        enabled = (reason == TableFileCreationReason::kFlush ||
                   reason == TableFileCreationReason::kCompaction);
        if (reason == TableFileCreationReason::kCompaction) {
          priority = Cache::Priority::BOTTOM;
        }
        break;
      case BlockBasedTableOptions::PrepopulateBlockCache::kDisable:
        break;
      default:
        assert(false);
    }
    return {enabled, priority};
  }
};

struct BlockBasedTableBuilder::Rep {
  const ImmutableOptions ioptions;
  // BEGIN from MutableCFOptions
  std::shared_ptr<const SliceTransform> prefix_extractor;
  // END from MutableCFOptions
  const WriteOptions write_options;
  const BlockBasedTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  // Size in bytes for the user-defined timestamps.
  size_t ts_sz;
  // When `ts_sz` > 0 and this flag is false, the user-defined timestamp in the
  // user key will be stripped when creating the block based table. This
  // stripping happens for all user keys, including the keys in data block,
  // index block for data block, index block for index block (if index type is
  // `kTwoLevelIndexSearch`), index for filter blocks (if using partitioned
  // filters), the `first_internal_key` in `IndexValue`, the `end_key` for range
  // deletion entries.
  // As long as the user keys are sorted when added via `Add` API, their logic
  // ordering won't change after timestamps are stripped. However, for each user
  // key to be logically equivalent before and after timestamp is stripped, the
  // user key should contain the minimum timestamp.
  bool persist_user_defined_timestamps;
  WritableFileWriter* file;
  // The current offset is only written by the current designated writer thread
  // but can be read by other threads to estimate current file size
  RelaxedAtomic<uint64_t> offset{0};
  size_t alignment;
  BlockBuilder data_block;
  // Buffers uncompressed data blocks to replay later. Needed when
  // compression dictionary is enabled so we can finalize the dictionary before
  // compressing any data blocks.
  std::vector<std::string> data_block_buffers;
  RangeDelBlockBuilder range_del_block;

  InternalKeySliceTransform internal_prefix_transform;
  std::unique_ptr<IndexFactoryBuilder> index_builder;
  // Checked alias of index_builder, or nullptr in kCustomOnly mode.
  // index_builder is always a BuiltinIndexFactoryBuilder when present, so
  // the hot paths use this instead of repeating the downcast.
  BuiltinIndexFactoryBuilder* builtin_index_builder = nullptr;
  std::string index_separator_scratch;

  // Custom indexes built alongside the built-in index.
  struct CustomIndex {
    // Meta block key is kIndexFactoryMetaPrefix + name.
    std::string name;
    std::unique_ptr<IndexFactoryBuilder> builder;
    // Owned by this CustomIndex so a Slice returned from
    // AddIndexEntry that references this buffer isn't invalidated by
    // another builder's call.
    std::string separator_scratch;
  };
  std::vector<CustomIndex> custom_indexes;

  // Forwards OnKeyAdded per key to the built-in builder (full internal
  // key) and each custom builder (parsed user key + ValueType).
  // index_builder is null only in kCustomOnly mode; custom_indexes is
  // empty when no user_defined_index_factory is configured.
  void ForwardOnKeyAddedToAll(const Slice& internal_key,
                              const std::optional<Slice>& value) {
    if (builtin_index_builder != nullptr) {
      builtin_index_builder->OnKeyAddedInternal(internal_key, value);
    }
    if (custom_indexes.empty()) {
      return;
    }
    // Parse failure on a builder-constructed internal key is unreachable;
    // surface as Corruption to avoid divergence (built-in builder above
    // already received the key).
    ParsedInternalKey pkey;
    Status parse_s = ParseInternalKey(internal_key, &pkey, false);
    assert(parse_s.ok());
    if (UNLIKELY(!parse_s.ok())) {
      SetStatus(Status::Corruption(
          "Failed to parse internal_key in ForwardOnKeyAddedToAll: " +
          parse_s.ToString()));
      return;
    }
    IndexFactoryBuilder::ValueType vt;
    switch (pkey.type) {
      case kTypeValue:
      case kTypeValuePreferredSeqno:
        vt = IndexFactoryBuilder::ValueType::kValue;
        break;
      case kTypeDeletion:
      case kTypeSingleDeletion:
      case kTypeDeletionWithTimestamp:
        vt = IndexFactoryBuilder::ValueType::kDelete;
        break;
      case kTypeMerge:
        vt = IndexFactoryBuilder::ValueType::kMerge;
        break;
      case kTypeBlobIndex:
      case kTypeWideColumnEntity:
        vt = IndexFactoryBuilder::ValueType::kOther;
        break;
      default:
        assert(false);
        vt = IndexFactoryBuilder::ValueType::kOther;
    }
    // kTypeValuePreferredSeqno packs the user value with a preferred
    // seqno; extract the user value portion before forwarding.
    Slice user_value;
    if (value.has_value()) {
      user_value = (pkey.type == kTypeValuePreferredSeqno)
                       ? ParsePackedValueForValue(*value)
                       : *value;
    }
    for (auto& ci : custom_indexes) {
      ci.builder->OnKeyAdded(pkey.user_key, vt, user_value);
    }
  }

  // Forwards AddIndexEntry to all index builders. The built-in builder
  // takes the direct (no-parse) path; the parse runs only when at least
  // one custom index needs the user-key form.
  void ForwardAddIndexEntryToAll(const Slice& last_internal_key,
                                 const Slice* first_internal_key_next,
                                 const BlockHandle& handle,
                                 bool skip_delta_encoding) {
    if (builtin_index_builder != nullptr) {
      builtin_index_builder->AddIndexEntryDirect(
          last_internal_key, first_internal_key_next, handle,
          &index_separator_scratch, skip_delta_encoding);
    }
    if (custom_indexes.empty()) {
      return;
    }
    // Parse failure on a builder-constructed internal key is unreachable;
    // surface as Corruption if it ever happens to avoid built-in vs
    // custom index divergence (the built-in entry was staged above).
    ParsedInternalKey last_pkey;
    Status parse_s = ParseInternalKey(last_internal_key, &last_pkey, false);
    assert(parse_s.ok());
    if (UNLIKELY(!parse_s.ok())) {
      SetStatus(Status::Corruption(
          "Failed to parse last_internal_key in ForwardAddIndexEntryToAll: " +
          parse_s.ToString()));
      return;
    }
    IndexFactoryBuilder::IndexEntryContext ctx;
    ctx.last_key_tag = PackSequenceAndType(last_pkey.sequence, last_pkey.type);
    ctx.skip_delta_encoding = skip_delta_encoding;
    ParsedInternalKey next_pkey;
    const Slice* next_user = nullptr;
    if (first_internal_key_next != nullptr) {
      Status next_parse_s =
          ParseInternalKey(*first_internal_key_next, &next_pkey, false);
      assert(next_parse_s.ok());
      if (UNLIKELY(!next_parse_s.ok())) {
        SetStatus(
            Status::Corruption("Failed to parse first_internal_key_next in "
                               "ForwardAddIndexEntryToAll: " +
                               next_parse_s.ToString()));
        return;
      }
      next_user = &next_pkey.user_key;
      ctx.first_key_tag =
          PackSequenceAndType(next_pkey.sequence, next_pkey.type);
    }
    IndexFactoryBuilder::BlockHandle pub{handle.offset(), handle.size()};
    for (auto& ci : custom_indexes) {
      ci.builder->AddIndexEntry(last_pkey.user_key, next_user, pub,
                                &ci.separator_scratch, ctx);
    }
  }

  std::string last_ikey;  // Internal key or empty (unset)
  bool uses_explicit_compression_manager = false;

  uint64_t sample_for_compression;
  RelaxedAtomic<uint64_t> compressible_input_data_bytes{0};
  RelaxedAtomic<uint64_t> uncompressible_input_data_bytes{0};
  RelaxedAtomic<uint64_t> num_data_blocks_compression_rejected{0};
  RelaxedAtomic<uint64_t> num_data_blocks_compression_bypassed{0};

  // ===================== AutoSkip compression state =====================
  // See CompressAndVerifyBlock. Active iff auto_skip is true (which also
  // requires a data-block compressor to exist). auto_skip_threshold_q and q_hat
  // are estimated stored/uncompressed ratios in kAutoSkipScale fixed point.
  bool auto_skip = false;
  // Skip data blocks once the estimate q_hat exceeds this (i.e. attempting is
  // not achieving at least this ratio). Derived from
  // max_compressed_bytes_per_kb (the per-block "worth keeping" bar), scaled to
  // kAutoSkipScale units.
  uint32_t auto_skip_threshold_q = 0;
  uint32_t auto_skip_min_sample_every = 0;
  // Emit-thread-only countdown of skipped data blocks before the next forced
  // compression "sample" in the skip regime. 0 => sample the next skip
  // candidate, then draw a fresh randomized gap. Only the emit thread makes
  // auto-skip decisions (serial: inline in WriteBlock; parallel: at emit time
  // in EmitBlockForParallel), so this needs no synchronization.
  uint32_t auto_skip_skips_until_sample = 0;
  // Preferred CompressionType this file's carryover is keyed to, or
  // kNoCompression if not seeded.
  CompressionType auto_skip_carry_type = kNoCompression;
  // Shared, continuously-updated estimate of the aggregate stored/uncompressed
  // ratio q_hat (kAutoSkipScale units; lower == better compression), read and
  // updated by all compression workers and the emit thread. Given its own full
  // cache line via CacheAlignedWrapper -- which pads to CACHE_LINE_SIZE so both
  // neighbors are isolated -- to avoid false sharing with the hot per-block
  // counters that precede it and the sampling counters that follow it.
  CacheAlignedWrapper<std::atomic<uint32_t>> auto_skip_q_hat{
      {kAutoSkipInitialQHat}};

  RelaxedAtomic<uint64_t> sampled_input_data_bytes{0};
  RelaxedAtomic<uint64_t> sampled_output_slow_data_bytes{0};
  RelaxedAtomic<uint64_t> sampled_output_fast_data_bytes{0};
  // Effective values are computed in the constructor body after consulting the
  // (data-block) Compressor via MaybeOverrideCompressionOptions(). They are
  // poisoned here (0 == no parallelism / no threads; a negative threshold makes
  // any block "not worth compressing") so that any use before that computation
  // surfaces as visibly wrong behavior rather than silently applying the
  // pre-override configured value.
  uint32_t compression_parallel_threads = 0;
  int max_compressed_bytes_per_kb = -1;
  // Dictionary guidance for data blocks (from GetDictGuidance())
  Compressor::DictConfig data_block_dict_guidance;

  // *** Compressors & decompressors - Yes, it seems like a lot here but ***
  // *** these are distinct fields to minimize extra conditionals and    ***
  // *** field reads on hot code paths. And to avoid interlocked         ***
  // *** instructions associated with shared_ptr.                        ***

  // A compressor for blocks in general, without dictionary compression
  std::unique_ptr<Compressor> basic_compressor;
  // Built-in compressors for compression size sampling
  std::unique_ptr<Compressor> fast_sample_compressor;
  std::unique_ptr<Compressor> slow_sample_compressor;
  // A compressor for data blocks, which might be tuned differently and might
  // use dictionary compression (when applicable). See ~Rep() for some details.
  UnownedPtr<Compressor> data_block_compressor = nullptr;
  // A compressor for index blocks, which might be tuned differently from
  // basic_compressor. See ~Rep() for some details.
  UnownedPtr<Compressor> index_block_compressor = nullptr;
  // A decompressor corresponding to basic_compressor (when non-nullptr).
  // Used for verification and cache warming.
  std::shared_ptr<Decompressor> basic_decompressor;
  // When needed, a decompressor for verifying compression using a
  // dictionary sampled/trained from this file.
  std::unique_ptr<Decompressor> verify_decompressor_with_dict;
  // When non-nullptr, compression should be verified with this corresponding
  // decompressor, except for data blocks. (Points to same as basic_decompressor
  // when verify_compression is set.)
  UnownedPtr<Decompressor> verify_decompressor;
  // Once configured/determined, points to one of the above Decompressors to use
  // in verifying data blocks.
  UnownedPtr<Decompressor> data_block_verify_decompressor;

  // Set of compression types used for blocks in this file (mixing compression
  // algorithms in a single file is allowed, using a CompressionManager)
  SmallEnumSet<CompressionType, kDisableCompressionOption>
      compression_types_used;

  // Working area for basic_compressor when compression_parallel_threads==1
  WorkingAreaPair index_block_working_area;
  // Working area for data_block_compressor, for emit/compaction thread
  WorkingAreaPair data_block_working_area;

  size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state = State::kUnbuffered;
  // `kBuffered` state is allowed only as long as the buffering of uncompressed
  // data blocks (see `data_block_buffers`) does not exceed `buffer_limit`.
  uint64_t buffer_limit = 0;
  std::shared_ptr<CacheReservationManager>
      compression_dict_buffer_cache_res_mgr;
  std::unique_ptr<BlobReferenceMetadataCollector> blob_reference_collector;
  std::shared_ptr<const BlobReferenceSstSummary> blob_reference_summary;
  std::unique_ptr<BlobRelocationDirectoryBuilder>
      blob_relocation_directory_builder;
  std::optional<BlobRelocationFileMetadata> blob_relocation_file_metadata;
  const bool use_delta_encoding_for_index_values;
  std::unique_ptr<FilterBlockBuilder> filter_builder;
  OffsetableCacheKey base_cache_key;
  const TableFileCreationReason reason;
  const bool target_file_size_is_upper_bound;

  BlockHandle pending_handle;  // Handle to add to index block

  GrowableBuffer single_threaded_compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  std::vector<std::unique_ptr<InternalTblPropColl>> table_properties_collectors;

  std::unique_ptr<ParallelCompressionRep> pc_rep;
  RelaxedAtomic<uint64_t> worker_cpu_micros{0};
  const WarmCacheConfig warm_cache_config;
  BlockCreateContext create_context;

  // The size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t tail_size;

  // The total size of all blocks in this file before they are compressed.
  // This is used for logging compaction stats.
  uint64_t pre_compression_size = 0;

  // Embedded-blob SST support. When `embedded_blob_options` is set the builder
  // is in embedded mode: index value delta encoding is disabled and eligible
  // large values are written inline as same-file blob records as values are
  // added (so they may be interleaved with data blocks), with table entries
  // rewritten to same-file BlobIndex references. This is a private owned copy:
  // the caller (e.g. SstFileWriter::OpenWithEmbeddedBlobs) may free its own
  // copy as soon as the builder is constructed, so we must not alias it.
  std::unique_ptr<const EmbeddedBlobSstBuilderOptions> embedded_blob_options;

  // Mutable state for embedded blob writing, lazily allocated when the first
  // blob record is written. Its presence is the signal that the file contains
  // embedded blobs (driving the presence property). Holds the diagnostic
  // counters plus scratch buffers reused across Add() calls to avoid per-entry
  // allocation.
  struct EmbeddedBlobState {
    EmbeddedBlobStats stats;
    std::string key_buf;
    std::string blob_index_buf;
    std::string entity_buf;
  };
  std::unique_ptr<EmbeddedBlobState> embedded_blob_state;

  // See class Footer
  uint32_t base_context_checksum;

  uint64_t get_offset() { return offset.LoadRelaxed(); }
  void set_offset(uint64_t o) { offset.StoreRelaxed(o); }

  bool IsParallelCompressionActive() const { return pc_rep != nullptr; }

  bool AutoSkipEnabled() const { return auto_skip; }

  // Draw a randomized sampling gap uniform in [(1-e)N, (1+e)N], e = 1/4, so
  // samples don't cluster or fall in a predictable pattern.
  uint32_t AutoSkipDrawGap() {
    uint32_t n = auto_skip_min_sample_every;
    uint32_t spread = n / 4;  // e = 0.25
    uint32_t jitter = static_cast<uint32_t>(
        Random::GetTLSInstance()->Uniform(static_cast<int>(2 * spread + 1)));
    return n - spread + jitter;
  }

  // Emit-thread-only auto-skip decision for an ELIGIBLE data block (caller has
  // already checked AutoSkipEnabled() and size < kCompressionSizeLimit).
  // Returns true to bypass compression, advancing the sample countdown.
  // Attempts while q_hat is at or below the bar (paying off), otherwise skips
  // except for periodic samples that keep the estimate fresh and detect a
  // return to compressible data.
  bool AutoSkipDecideBypassEligible() {
    if (auto_skip_q_hat.obj_.load(std::memory_order_relaxed) <=
        auto_skip_threshold_q) {
      return false;  // attempt regime
    }
    if (auto_skip_skips_until_sample == 0) {
      auto_skip_skips_until_sample = AutoSkipDrawGap();
      return false;  // periodic sample: attempt
    }
    auto_skip_skips_until_sample--;
    return true;  // skip
  }

  // Bypass bookkeeping for an eligible data block whose compression was
  // auto-skipped. Mirrors what CompressAndVerifyBlock records for a bypassed
  // block, for the parallel path where skipped blocks never reach it. May be
  // called from the emit thread; the counters/stats are thread-safe.
  void AutoSkipRecordBypass(size_t uncompressed_size) {
    compressible_input_data_bytes.FetchAddRelaxed(uncompressed_size);
    uncompressible_input_data_bytes.FetchAddRelaxed(kBlockTrailerSize);
    num_data_blocks_compression_bypassed.FetchAddRelaxed(1);
    RecordTick(ioptions.stats, NUMBER_BLOCK_COMPRESSION_BYPASSED);
    RecordTick(ioptions.stats, BYTES_COMPRESSION_BYPASSED, uncompressed_size);
  }

  // Asymmetric EWMA update of the shared q_hat toward this block's observed
  // stored ratio obs (kAutoSkipScale units; lower == better compression). Fast
  // toward better compression (obs < q_hat), slow toward worse. Relaxed
  // load/store: a lost update under concurrency only costs a little precision
  // (acceptable within the limits of parallel compression).
  void AutoSkipUpdateQHat(uint32_t obs) {
    int32_t cur = static_cast<int32_t>(
        auto_skip_q_hat.obj_.load(std::memory_order_relaxed));
    int32_t diff = static_cast<int32_t>(obs) - cur;
    int shift = diff < 0 ? kAutoSkipAlphaFastShift : kAutoSkipAlphaSlowShift;
    int32_t step = diff >= 0 ? (diff >> shift) : -((-diff) >> shift);
    if (step == 0) {
      // Granularity floor so the estimate can converge all the way to the
      // observation (and thus cross the threshold) rather than stalling.
      step = diff > 0 ? 1 : (diff < 0 ? -1 : 0);
    }
    auto_skip_q_hat.obj_.store(static_cast<uint32_t>(cur + step),
                               std::memory_order_relaxed);
  }

  // Resolve AutoSkip options into internal fixed-point form. The skip threshold
  // reuses max_compressed_bytes_per_kb (the per-block "worth keeping" bar): a
  // per-1024 ratio scaled to kAutoSkipScale (2^20) fixed point (x * 2^20 / 2^10
  // == x << 10). Requires max_compressed_bytes_per_kb to be set already.
  void AutoSkipSetup(const CompressionOptions& opts) {
    auto_skip = opts.auto_skip;
    if (!auto_skip) {
      return;
    }
    auto_skip_threshold_q = static_cast<uint32_t>(max_compressed_bytes_per_kb)
                            << 10;
    int n = opts.auto_skip_min_sample_every;
    auto_skip_min_sample_every =
        n > 0 ? static_cast<uint32_t>(n) : kAutoSkipDefaultMinSampleEvery;
  }

  // Seed the estimate from this emit/compaction thread's carryover for the data
  // block compressor's preferred type (if any). No-op until a data-block
  // compressor exists (e.g. dictionary sampling defers this to
  // MaybeEnterUnbuffered). Emit-thread only.
  void AutoSkipSeed() {
    if (!AutoSkipEnabled() || !data_block_compressor) {
      return;
    }
    auto_skip_carry_type = data_block_compressor->GetPreferredCompressionType();
    const AutoSkipCarry& c = AutoSkipCarryTLS();
    // Inherit the carried estimate only if it was produced for the same
    // compression type; otherwise start fresh.
    auto_skip_q_hat.obj_.store(
        c.type == auto_skip_carry_type ? c.q_hat : kAutoSkipInitialQHat,
        std::memory_order_relaxed);
  }

  // Persist the final estimate back to thread-local carryover. Emit-thread
  // only, after parallel workers have joined.
  void AutoSkipWriteBack() {
    if (auto_skip_carry_type == kNoCompression) {
      return;
    }
    AutoSkipCarryTLS() =
        AutoSkipCarry{auto_skip_q_hat.obj_.load(std::memory_order_relaxed),
                      auto_skip_carry_type};
  }

  Status GetStatus() { return GetIOStatus(); }

  bool StatusOk() {
    // The OK case is optimized with an atomic. Relaxed is sufficient because
    // if a thread other than the emit/compaction thread sets to non-OK it
    // will synchronize that in aborting parallel compression.
    bool ok = io_status_ok.LoadRelaxed();
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    if (ok) {
      std::lock_guard<std::mutex> lock(io_status_mutex);
      // Double-check
      if (io_status_ok.LoadRelaxed()) {
        io_status.PermitUncheckedError();
        assert(io_status.ok());
      } else {
        ok = false;
      }
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return ok;
  }

  IOStatus GetIOStatus() {
    // See StatusOk, which is optimized to avoid Status object copies
    if (LIKELY(io_status_ok.LoadRelaxed())) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
      std::lock_guard<std::mutex> lock(io_status_mutex);
      // Double-check
      if (io_status_ok.LoadRelaxed()) {
        io_status.PermitUncheckedError();
        assert(io_status.ok());
      } else {
        return io_status;
      }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
      return IOStatus::OK();
    } else {
      std::lock_guard<std::mutex> lock(io_status_mutex);
      return io_status;
    }
  }

  // Avoid copying Status and IOStatus objects as much as possible.
  // Never erase an existing I/O status that is not OK.
  void SetStatus(Status&& s) {
    if (UNLIKELY(!s.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(status_to_io_status(std::move(s)));
    }
  }
  void SetStatus(const Status& s) {
    if (UNLIKELY(!s.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(status_to_io_status(Status(s)));
    }
  }
  void SetIOStatus(IOStatus&& ios) {
    if (UNLIKELY(!ios.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(std::move(ios));
    }
  }
  void SetIOStatus(const IOStatus& ios) {
    if (UNLIKELY(!ios.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(IOStatus(ios));
    }
  }

  void SetFailedIOStatus(IOStatus&& ios) {
    assert(!ios.ok());
    // Because !s.ok() is rare, locking is acceptable even in non-parallel case.
    std::lock_guard<std::mutex> lock(io_status_mutex);
    // Double-check
    if (io_status.ok()) {
      io_status = std::move(ios);
      io_status_ok.StoreRelaxed(false);
    }
  }

  Rep(const BlockBasedTableOptions& table_opt, const TableBuilderOptions& tbo,
      WritableFileWriter* f)
      : ioptions(tbo.ioptions),
        prefix_extractor(tbo.moptions.prefix_extractor),
        write_options(tbo.write_options),
        table_options(table_opt),
        internal_comparator(tbo.internal_comparator),
        ts_sz(tbo.internal_comparator.user_comparator()->timestamp_size()),
        persist_user_defined_timestamps(
            tbo.ioptions.persist_user_defined_timestamps),
        file(f),
        alignment(table_options.block_align
                      ? std::min(static_cast<size_t>(table_options.block_size),
                                 kDefaultPageSize)
                      : 0),
        data_block(table_options.block_restart_interval,
                   table_options.use_delta_encoding,
                   false /* use_value_delta_encoding */,
                   tbo.internal_comparator.user_comparator()
                           ->CanKeysWithDifferentByteContentsBeEqual()
                       ? BlockBasedTableOptions::kDataBlockBinarySearch
                       : table_options.data_block_index_type,
                   table_options.data_block_hash_table_util_ratio, ts_sz,
                   persist_user_defined_timestamps, false /* is_user_key */,
                   table_options.separate_key_value_in_data_block,
                   tbo.ioptions.stats, -1.0 /* uniform_cv_threshold */,
                   UseCommonPrefixForDataBlock(
                       table_options, tbo.internal_comparator.user_comparator(),
                       ts_sz)),
        range_del_block(ts_sz, persist_user_defined_timestamps),
        internal_prefix_transform(prefix_extractor.get()),
        sample_for_compression(tbo.moptions.sample_for_compression),
        use_delta_encoding_for_index_values(
            table_opt.format_version >= 4 && !table_opt.block_align &&
            // Embedded blob records are interleaved between data blocks, which
            // breaks the "next block starts where the last one ended"
            // contiguity that plain index value-delta encoding assumes.
            // format_version >= 8 handles non-contiguous index entries with the
            // in-value escape (see IndexValue::EncodeTo), so value-delta
            // encoding is safe with embedded blobs there; older versions must
            // fall back to full handles.
            (tbo.embedded_blob_options == nullptr ||
             FormatVersionUsesValueDeltaEscape(table_opt.format_version))),
        reason(tbo.reason),
        target_file_size_is_upper_bound(
            tbo.moptions.target_file_size_is_upper_bound),
        flush_block_policy(
            table_options.flush_block_policy_factory->NewFlushBlockPolicy(
                table_options, data_block)),
        warm_cache_config(WarmCacheConfig::Compute(
            table_options.prepopulate_block_cache, reason)),
        create_context(
            &table_options, &ioptions, ioptions.stats,
            /*decompressor=*/nullptr,
            tbo.moptions.block_protection_bytes_per_key,
            tbo.internal_comparator.user_comparator(),
            !use_delta_encoding_for_index_values,
            table_opt.index_type ==
                BlockBasedTableOptions::kBinarySearchWithFirstKey,
            FormatVersionUsesValueDeltaEscape(table_opt.format_version),
            table_options.block_restart_interval,
            table_options.index_block_restart_interval),
        tail_size(0),
        embedded_blob_options(
            tbo.embedded_blob_options
                ? std::make_unique<EmbeddedBlobSstBuilderOptions>(
                      *tbo.embedded_blob_options)
                : nullptr) {
    FilterBuildingContext filter_context(table_options);

    filter_context.info_log = ioptions.logger;
    filter_context.column_family_name = tbo.column_family_name;
    filter_context.db_name = tbo.db_name;
    filter_context.reason = reason;

    // Only populate other fields if known to be in LSM rather than
    // generating external SST file
    switch (reason) {
      case TableFileCreationReason::kFlush:
      case TableFileCreationReason::kCompaction:
      case TableFileCreationReason::kRecovery:
        filter_context.compaction_style = ioptions.compaction_style;
        filter_context.num_levels = ioptions.num_levels;
        filter_context.level_at_creation = tbo.level_at_creation;
        filter_context.is_bottommost = tbo.is_bottommost;
        filter_context.is_remote_compaction = tbo.is_remote_compaction;
        assert(filter_context.level_at_creation < filter_context.num_levels);
        // Record informational LSM-at-creation context.
        props.lsm_info_at_creation =
            LsmInfoAtCreation::Applicable(tbo.level_at_creation,
                                          tbo.is_bottommost)
                .Encode();
        break;
      case TableFileCreationReason::kSstFileWriter:
      case TableFileCreationReason::kMisc:
        // These files have no LSM position; record that explicitly so it is
        // distinguishable from "unknown" (older files with no property).
        props.lsm_info_at_creation =
            LsmInfoAtCreation::NotApplicable().Encode();
        break;
    }

    auto* compression_manager = tbo.moptions.compression_manager.get();
    if (compression_manager == nullptr) {
      uses_explicit_compression_manager = false;
      compression_manager = GetBuiltinV2CompressionManager().get();
    } else {
      uses_explicit_compression_manager = true;
    }
    assert(compression_manager);

    basic_compressor = compression_manager->GetCompressorForSST(
        filter_context, tbo.compression_opts, tbo.compression_type);
    if (basic_compressor) {
      if (table_options.enable_index_compression) {
        index_block_compressor = MaybeCloneSpecialized(
            basic_compressor.get(), CacheEntryRole::kIndexBlock);
        index_block_working_area.compress =
            index_block_compressor->ObtainWorkingArea();
      }
      data_block_dict_guidance =
          basic_compressor->GetDictGuidance(CacheEntryRole::kDataBlock);
      if (auto* sampling =
              std::get_if<Compressor::DictSampling>(&data_block_dict_guidance);
          sampling != nullptr && sampling->max_sample_bytes > 0) {
        // Sampling mode: collect samples up to max_sample_bytes
        state = State::kBuffered;
        if (tbo.target_file_size == 0) {
          buffer_limit = tbo.compression_opts.max_dict_buffer_bytes;
        } else if (tbo.compression_opts.max_dict_buffer_bytes == 0) {
          buffer_limit = tbo.target_file_size;
        } else {
          buffer_limit = std::min(tbo.target_file_size,
                                  tbo.compression_opts.max_dict_buffer_bytes);
        }
      } else if (auto* predef = std::get_if<Compressor::DictPreDefined>(
                     &data_block_dict_guidance);
                 predef != nullptr && !predef->dict_data.empty()) {
        // Pre-defined dictionary mode: use it immediately, no buffering
        data_block_compressor = MaybeCloneSpecialized(
            basic_compressor.get(), CacheEntryRole::kDataBlock,
            Compressor::DictPreDefined{std::string{predef->dict_data}});
        data_block_working_area.compress =
            data_block_compressor->ObtainWorkingArea();
      } else {
        assert(std::holds_alternative<Compressor::DictSampling>(
                   data_block_dict_guidance) ||
               std::holds_alternative<Compressor::DictPreDefined>(
                   data_block_dict_guidance) ||
               std::holds_alternative<Compressor::DictDisabled>(
                   data_block_dict_guidance));
        // No distinct data block compressor using dictionary, but
        // implementation might still want to specialize for data blocks
        data_block_compressor = MaybeCloneSpecialized(
            basic_compressor.get(), CacheEntryRole::kDataBlock);
        data_block_working_area.compress =
            data_block_compressor->ObtainWorkingArea();
      }
      basic_decompressor = basic_compressor->GetOptimizedDecompressor();
      if (basic_decompressor == nullptr) {
        // Optimized version not available
        basic_decompressor = compression_manager->GetDecompressor();
      }
      create_context.decompressor = basic_decompressor.get();

      if (table_options.verify_compression) {
        verify_decompressor = basic_decompressor.get();
        if (table_options.enable_index_compression) {
          index_block_working_area.verify =
              verify_decompressor->ObtainWorkingArea(
                  index_block_compressor->GetPreferredCompressionType());
        }
        if (state == State::kUnbuffered) {
          assert(data_block_compressor);
          data_block_verify_decompressor = verify_decompressor.get();
          data_block_working_area.verify =
              data_block_verify_decompressor->ObtainWorkingArea(
                  data_block_compressor->GetPreferredCompressionType());
        }
      }
    }

    // Let the Compressor override the subset of CompressionOptions that the
    // builder is responsible for applying (parallel threads, the auto-skip
    // heuristic, and the compression-worthwhile threshold), rather than the
    // Compressor itself. These primarily govern how *data* blocks are
    // compressed, so consult the data-block compressor. In dictionary-sampling
    // mode the data-block compressor is not created until
    // MaybeEnterUnbuffered(), so fall back to the basic compressor (parallel
    // compression does not start in that mode regardless). When there is no
    // compressor at all, AutoSkip has nothing to skip, so force it off to keep
    // the hot path free of pointless work and consistent with the "requires a
    // data-block compressor" invariant. See
    // Compressor::MaybeOverrideCompressionOptions().
    CompressionOptions compression_opts = tbo.compression_opts;
    Compressor* compression_opts_source = data_block_compressor
                                              ? data_block_compressor.get()
                                              : basic_compressor.get();
    if (compression_opts_source) {
      compression_opts_source->MaybeOverrideCompressionOptions(
          &compression_opts);
    } else {
      compression_opts.auto_skip = false;
    }

    // Record the compression options actually in effect: the configured options
    // as possibly overridden by the Compressor just above, plus some pseudo-
    // option debugging info (underscore prefix marks these as special). The
    // per-block compression type actually recorded can differ from
    // tbo.compression_type (e.g. LZ4 vs LZ4HC is selected by compression level;
    // the compression manager can override), so this preserves the originally
    // configured choice.
    props.compression_options = CompressionOptionsToString(compression_opts);
    props.compression_options.append("_type=");
    props.compression_options.append(
        std::to_string(static_cast<int>(tbo.compression_type)));
    props.compression_options.append("; ");
    if (uses_explicit_compression_manager) {
      // Persist the manager's full details (its id plus any configured
      // options), not just GetId(). Informational only. ToString() yields the
      // bare id when there are no options, or a brace-wrapped
      // "{id=...;opt=...}" when there are, so the nested ";"/"=" never collide
      // with the surrounding compression_options pseudo-option string.
      ConfigOptions cfg;
      props.compression_options.append("_compression_manager=");
      props.compression_options.append(compression_manager->ToString(cfg));
      props.compression_options.append("; ");
    }

    compression_parallel_threads = compression_opts.parallel_threads;

    // Sanitize to only allowing compression when it saves space.
    max_compressed_bytes_per_kb =
        std::min(int{1023}, compression_opts.max_compressed_bytes_per_kb);

    // Resolve the auto-skip options into internal form. Reads the sanitized
    // max_compressed_bytes_per_kb set just above.
    AutoSkipSetup(compression_opts);

    // Hard structural constraints override any recommendation:
    // partition_filters without decoupling and embedded-blob SSTs require
    // single-threaded compression. The UDI parallel-support check happens after
    // builder construction (each IndexFactoryBuilder votes via
    // SupportsParallelAddEntry).
    if ((table_opt.partition_filters &&
         !table_opt.decouple_partitioned_filters) ||
        embedded_blob_options) {
      compression_parallel_threads = 1;
    }

    // Seed AutoSkip from this thread's inter-file carryover (no-op if disabled
    // or if the data-block compressor isn't set up yet, e.g. dictionary
    // sampling, which seeds later in MaybeEnterUnbuffered).
    AutoSkipSeed();

    if (sample_for_compression > 0) {
      auto builtin = GetBuiltinV2CompressionManager();
      if (builtin->SupportsCompressionType(kLZ4Compression)) {
        fast_sample_compressor = builtin->GetCompressor({}, kLZ4Compression);
      } else if (builtin->SupportsCompressionType(kSnappyCompression)) {
        fast_sample_compressor = builtin->GetCompressor({}, kSnappyCompression);
      }
      if (builtin->SupportsCompressionType(kZSTD)) {
        slow_sample_compressor = builtin->GetCompressor({}, kZSTD);
      } else if (builtin->SupportsCompressionType(kZlibCompression)) {
        slow_sample_compressor = builtin->GetCompressor({}, kZlibCompression);
      }
      // NOTE: even if both sampling compressors are nullptr, we still populate
      // the table properties with placeholder info
    }

    const auto compress_dict_build_buffer_charged =
        table_options.cache_usage_options.options_overrides
            .at(CacheEntryRole::kCompressionDictionaryBuildingBuffer)
            .charged;
    if (table_options.block_cache &&
        (compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kEnabled ||
         compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kFallback)) {
      compression_dict_buffer_cache_res_mgr =
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kCompressionDictionaryBuildingBuffer>>(
              table_options.block_cache);
    } else {
      compression_dict_buffer_cache_res_mgr = nullptr;
    }

    // SstFileWriter output has no DB file number and may lack a known CF, so
    // it cannot carry repair metadata. It stays in the legacy form.
    if (tbo.moptions.enable_blob_indirection &&
        tbo.reason != TableFileCreationReason::kSstFileWriter &&
        tbo.column_family_id !=
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) {
      std::shared_ptr<CacheReservationManager> reservation_manager =
          GetBlobReferenceReservationManager(table_options);
      blob_reference_collector =
          std::make_unique<BlobReferenceMetadataCollector>(
              std::move(reservation_manager), tbo.column_family_id,
              tbo.cur_file_num, /*route_snapshot_fingerprint_high=*/0,
              /*route_snapshot_fingerprint_low=*/0);
    }

    // Common user-key prefix feature (format_version >= 8) for index-like
    // blocks: gated uniformly for all index tiers (leaf/partition and the
    // partitioned index / partitioned filter top-level). See
    // UseCommonPrefixForIndex.
    const bool use_common_prefix_index = UseCommonPrefixForIndex(
        table_options, internal_comparator.user_comparator(), ts_sz);

    const bool is_custom_default =
        table_options.GetEffectiveIndexMode() ==
        BlockBasedTableOptions::IndexMode::kCustomDefault;
    SetStatus(ValidateIndexMode(table_options.GetEffectiveIndexMode()));
    const bool is_custom_only = table_options.GetEffectiveIndexMode() ==
                                BlockBasedTableOptions::IndexMode::kCustomOnly;
    const bool builds_custom_index =
        table_options.GetEffectiveIndexMode() !=
        BlockBasedTableOptions::IndexMode::kStandardOnly;

    if ((is_custom_default || is_custom_only) &&
        table_options.user_defined_index_factory == nullptr) {
      SetStatus(
          Status::InvalidArgument("index_mode kCustomDefault/kCustomOnly "
                                  "requires "
                                  "user_defined_index_factory to be set"));
    }
    if (is_custom_only && table_options.format_version < 6) {
      SetStatus(Status::InvalidArgument(
          "index_mode kCustomOnly requires format_version >= 6; got " +
          std::to_string(table_options.format_version)));
    }
    if (builds_custom_index &&
        table_options.user_defined_index_factory != nullptr) {
      // See IndexFactoryOptions for why the two can't be combined. Repeated
      // from BlockBasedTableFactory::ValidateOptions because SstFileWriter and
      // SetOptions never reach it. Reopening this needs ts_sz and
      // persist_user_defined_timestamps plumbed into IndexFactoryOptions.
      if (ts_sz > 0) {
        SetStatus(Status::InvalidArgument(
            "user_defined_index_factory is incompatible with user-defined "
            "timestamps"));
      }
      if ((is_custom_default || is_custom_only) &&
          table_options.index_type ==
              BlockBasedTableOptions::kTwoLevelIndexSearch) {
        SetStatus(Status::InvalidArgument(
            "index_mode kCustomDefault/kCustomOnly is incompatible with "
            "partitioned index (kTwoLevelIndexSearch)"));
      } else if ((is_custom_default || is_custom_only) &&
                 table_options.partition_filters) {
        SetStatus(Status::InvalidArgument(
            "index_mode kCustomDefault/kCustomOnly is incompatible with "
            "partitioned filters"));
      }
    }

    // kCustomOnly skips the built-in builder; a minimal empty index
    // block is written by WriteIndexBlock to satisfy the SST footer.
    const bool build_standard_index =
        table_options.GetEffectiveIndexMode() !=
        BlockBasedTableOptions::IndexMode::kCustomOnly;
    if (build_standard_index) {
      BuiltinIndexFactoryConfig builtin_config;
      builtin_config.internal_comparator = &internal_comparator;
      builtin_config.internal_prefix_transform =
          &this->internal_prefix_transform;
      builtin_config.use_delta_encoding_for_index_values =
          use_delta_encoding_for_index_values;
      builtin_config.table_options = &table_options;
      builtin_config.ts_sz = ts_sz;
      builtin_config.persist_user_defined_timestamps =
          persist_user_defined_timestamps;
      builtin_config.stats = ioptions.stats;
      builtin_config.use_common_prefix = use_common_prefix_index;

      IndexFactoryOptions builtin_opts;
      builtin_opts.comparator = internal_comparator.user_comparator();
      Status s = NewBuiltinIndexFactoryBuilder(table_options.index_type,
                                               builtin_config, builtin_opts,
                                               index_builder);
      if (!s.ok()) {
        SetStatus(s);
      } else {
        builtin_index_builder =
            static_cast_with_check<BuiltinIndexFactoryBuilder>(
                index_builder.get());
      }
    }

    if (builds_custom_index &&
        table_options.user_defined_index_factory != nullptr) {
      IndexFactoryOptions custom_opts;
      custom_opts.comparator = internal_comparator.user_comparator();
      CustomIndex ci;
      ci.name = table_options.user_defined_index_factory->Name();
      Status s = table_options.user_defined_index_factory->NewBuilder(
          custom_opts, ci.builder);
      if (!s.ok()) {
        SetStatus(s);
      } else if (ci.builder != nullptr) {
        custom_indexes.push_back(std::move(ci));
      } else {
        SetStatus(Status::InvalidArgument(
            "IndexFactory::NewBuilder returned OK with null builder for " +
            ci.name));
      }
    }

    // Parallel compression requires every index builder to implement the
    // two-phase prepare/finish protocol. Fall back to single-threaded
    // compression if any of them declines, and name it: the throughput
    // difference is large and otherwise invisible.
    if (compression_parallel_threads > 1) {
      const char* declining = nullptr;
      if (index_builder && !index_builder->SupportsParallelAddEntry()) {
        declining = "the built-in index";
      } else {
        for (const auto& ci : custom_indexes) {
          // A builder that claims parallel support but cannot hand back a
          // prepared entry would pass nullptr into PrepareAddEntry on the emit
          // thread. Treat that half-implemented contract as declining, the same
          // as an honest false, so the SST still builds single-threaded. The
          // probe costs one throwaway allocation per builder per SST and only
          // catches a builder that returns null up front; a builder that starts
          // returning null later is caught in MaybeStartParallelCompression.
          if (!ci.builder->SupportsParallelAddEntry() ||
              ci.builder->CreatePreparedAddEntry() == nullptr) {
            declining = ci.name.c_str();
            break;
          }
        }
      }
      if (declining != nullptr) {
        ROCKS_LOG_WARN(ioptions.logger,
                       "compression_parallel_threads=%u requested but %s does "
                       "not support parallel AddEntry; using 1 thread",
                       compression_parallel_threads, declining);
        compression_parallel_threads = 1;
      }
    }

    if (!StatusOk()) {
      // Rejected index options can leave a partitioned filter without its
      // index.
      filter_builder.reset();
    } else if (ioptions.optimize_filters_for_hits && tbo.is_bottommost) {
      // Apply optimize_filters_for_hits setting here when applicable by
      // skipping filter generation
      filter_builder.reset();
    } else if (!table_options.filter_policy) {
      // Null filter_policy -> no filter
      filter_builder.reset();
    } else {
      filter_builder.reset(CreateFilterBlockBuilder(
          ioptions, tbo.moptions, filter_context,
          use_delta_encoding_for_index_values, index_builder.get(), ts_sz,
          persist_user_defined_timestamps, use_common_prefix_index));
    }

    assert(tbo.internal_tbl_prop_coll_factories);
    for (auto& factory : *tbo.internal_tbl_prop_coll_factories) {
      assert(factory);

      std::unique_ptr<InternalTblPropColl> collector{
          factory->CreateInternalTblPropColl(
              tbo.column_family_id, tbo.level_at_creation,
              tbo.ioptions.num_levels,
              tbo.last_level_inclusive_max_seqno_threshold)};
      if (collector) {
        table_properties_collectors.emplace_back(std::move(collector));
      }
    }
    table_properties_collectors.emplace_back(
        std::make_unique<BlockBasedTablePropertiesCollector>(
            table_options.index_type, table_options.whole_key_filtering,
            prefix_extractor != nullptr,
            table_options.decouple_partitioned_filters));
    if (ts_sz > 0 && persist_user_defined_timestamps) {
      table_properties_collectors.emplace_back(
          std::make_unique<TimestampTablePropertiesCollector>(
              tbo.internal_comparator.user_comparator()));
    }

    // These are only needed for populating table properties
    props.column_family_id = tbo.column_family_id;
    props.column_family_name = tbo.column_family_name;
    props.oldest_key_time = tbo.oldest_key_time;
    props.newest_key_time = tbo.newest_key_time;
    props.file_creation_time = tbo.file_creation_time;
    props.orig_file_number = tbo.cur_file_num;
    props.db_id = tbo.db_id;
    props.db_session_id = tbo.db_session_id;
    props.db_host_id = ioptions.db_host_id;
    props.format_version = table_options.format_version;
    props.data_block_restart_interval = table_options.block_restart_interval;
    props.index_block_restart_interval =
        table_options.index_block_restart_interval;
    props.separate_key_value_in_data_block =
        table_options.separate_key_value_in_data_block ? 1 : 0;
    if (!ReifyDbHostIdProperty(ioptions.env, &props.db_host_id).ok()) {
      ROCKS_LOG_INFO(ioptions.logger, "db_host_id property will not be set");
    }
    // Default is UINT64_MAX for unknown. Setting it to 0 here
    // to allow updating it by taking max in BlockBasedTableBuilder::Add().
    props.key_largest_seqno = 0;
    // Default is UINT64_MAX for unknown.
    props.key_smallest_seqno = UINT64_MAX;
    PrePopulateCompressionProperties(compression_manager);

    if (FormatVersionUsesContextChecksum(table_options.format_version)) {
      // Must be non-zero and semi- or quasi-random
      // TODO: ideally guaranteed different for related files (e.g. use file
      // number and db_session, for benefit of SstFileWriter)
      do {
        base_context_checksum = Random::GetTLSInstance()->Next();
      } while (UNLIKELY(base_context_checksum == 0));
    } else {
      base_context_checksum = 0;
    }

    if (alignment > 0 && basic_compressor) {
      // With better sanitization in `CompactionPicker::CompactFiles()`, we
      // would not need to handle this case here and could change it to an
      // assertion instead.
      SetStatus(Status::InvalidArgument(
          "Enable block_align, but compression enabled"));
    }
  }

  ~Rep() {
    // Delete working areas before their compressors.
    index_block_working_area = {};
    data_block_working_area = {};
    // Must have been cleaned up by StopParallelCompression
    assert(pc_rep == nullptr);
    // Delete specialized compressors if they were distinct (avoiding extra
    // fields and interlocked instructions with shared_ptr)
    if (data_block_compressor.get() != basic_compressor.get()) {
      delete data_block_compressor.get();
    }
    if (index_block_compressor.get() != basic_compressor.get()) {
      delete index_block_compressor.get();
    }
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  Rep(Rep&&) = delete;
  Rep& operator=(Rep&&) = delete;

  void PrePopulateCompressionProperties(
      UnownedPtr<CompressionManager> compression_manager) {
    if (FormatVersionUsesCompressionManagerName(table_options.format_version)) {
      assert(compression_manager);
      // Use newer compression_name property
      props.compression_name.reserve(32);
      // If compression is disabled, use empty manager name
      if (basic_compressor) {
        props.compression_name.append(compression_manager->CompatibilityName());
      }
      props.compression_name.push_back(';');
      // Rest of property to be filled out at the end of building the file
    } else {
      // Use legacy compression_name property, populated at the end of
      // building the file. Not compatible with compression managers using
      // custom algorithms / compression types.
      assert(
          Slice(compression_manager->CompatibilityName())
              .compare(GetBuiltinV2CompressionManager()->CompatibilityName()) ==
          0);
    }
  }
  void PostPopulateCompressionProperties() {
    // Do not include "no compression" in the set. It's not really useful
    // information whether there are any uncompressed blocks. Some kinds of
    // blocks are never compressed anyway.
    compression_types_used.Remove(kNoCompression);
    size_t ctype_count = compression_types_used.count();

    if (uses_explicit_compression_manager) {
      // Stuff some extra debugging info as extra pseudo-options. Using
      // underscore prefix to indicate they are special.
      std::string& compression_options = props.compression_options;
      compression_options.append("_compressor=");
      compression_options.append(data_block_compressor
                                     ? data_block_compressor->GetId()
                                     : std::string{});
      compression_options.append("; ");
    } else {
      // No explicit compression manager
      assert(compression_types_used.count() <= 1);
    }

    std::string& compression_name = props.compression_name;
    if (FormatVersionUsesCompressionManagerName(table_options.format_version)) {
      // Fill in extended field of "compression name" property, which is the
      // set of compression types used, sorted by unsigned byte and then hex
      // encoded with two digits each (so that table properties are human
      // readable).
      assert(*compression_name.rbegin() == ';');
      size_t pos = compression_name.size();
      // Make space for the field contents
      compression_name.append(ctype_count * 2, '\0');
      char* ptr = compression_name.data() + pos;
      // Populate the field contents
      for (CompressionType t : compression_types_used) {
        PutBaseChars<16>(&ptr, /*n=*/2, static_cast<unsigned char>(t),
                         /*uppercase=*/true);
      }
      assert(ptr == compression_name.data() + pos + ctype_count * 2);
      // Allow additional fields in the future
      compression_name.push_back(';');
    } else {
      // Use legacy compression naming. To adhere to requirements described in
      // TableProperties::compression_name, we might have to replace the name
      // based on the legacy configured compression type.
      assert(compression_name.empty());
      if (ctype_count == 0) {
        // We could get a slight performance boost in the reader by marking
        // the file as "no compression" if compression is configured but
        // consistently rejected, but that would give misleading info for
        // debugging purposes. So instead we record the configured compression
        // type, matching the historical behavior.
        if (data_block_compressor) {
          compression_name = CompressionTypeToString(
              data_block_compressor->GetPreferredCompressionType());
        } else {
          assert(basic_compressor == nullptr);
          compression_name = CompressionTypeToString(kNoCompression);
        }
      } else if (compression_types_used.Contains(kZSTD)) {
        compression_name = CompressionTypeToString(kZSTD);
      } else {
        compression_name =
            CompressionTypeToString(*compression_types_used.begin());
      }
    }
  }

 private:
  // Synchronize io_status to be readable/writable across threads, but
  // optimize for the OK case
  std::mutex io_status_mutex;
  RelaxedAtomic<bool> io_status_ok{true};
  IOStatus io_status;
};

BlockBasedTableBuilder::BlockBasedTableBuilder(
    const BlockBasedTableOptions& table_options, const TableBuilderOptions& tbo,
    WritableFileWriter* file) {
  BlockBasedTableOptions sanitized_table_options(table_options);
  auto ucmp = tbo.internal_comparator.user_comparator();
  assert(ucmp);
  (void)ucmp;  // avoids unused variable error.
  rep_ = std::make_unique<Rep>(sanitized_table_options, tbo, file);

  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::BlockBasedTableBuilder:PreSetupBaseCacheKey",
      const_cast<TableProperties*>(&rep_->props));

  BlockBasedTable::SetupBaseCacheKey(&rep_->props, tbo.db_session_id,
                                     tbo.cur_file_num, &rep_->base_cache_key);

  MaybeStartParallelCompression();
  if (!rep_->IsParallelCompressionActive() && rep_->basic_compressor) {
    rep_->single_threaded_compressed_output.ResetForSize(
        table_options.block_size);
  }
}

BlockBasedTableBuilder::~BlockBasedTableBuilder() {
  // Catch errors where caller forgot to call Finish()
  assert(rep_->state == Rep::State::kClosed);
}

void BlockBasedTableBuilder::Add(const Slice& ikey, const Slice& value) {
  Rep* r = rep_.get();
  assert(r != nullptr);
  if (UNLIKELY(r == nullptr)) {
    return;
  }
  assert(rep_->state != Rep::State::kClosed);
  if (UNLIKELY(!ok())) {
    return;
  }
  if (UNLIKELY(value.size() > std::numeric_limits<uint32_t>::max())) {
    // NOTE:
    // * WriteBatch and SstFileWriter check input value sizes
    // * MergeHelper checks merge result size
    // * AddWithLastKey() below requires < 4GB and we have seen random
    // corruptions here
    r->SetStatus(Status::Corruption(
        "BlockBasedBuilder::Add() received a value sized >= 4GB. This could be "
        "a random corruption. size=" +
        std::to_string(value.size())));
    return;
  }
  ValueType value_type;
  SequenceNumber seq;
  UnPackSequenceAndType(ExtractInternalKeyFooter(ikey), &seq, &value_type);
  r->props.key_largest_seqno = std::max(r->props.key_largest_seqno, seq);
  r->props.key_smallest_seqno = std::min(r->props.key_smallest_seqno, seq);

  // For embedded-blob SSTs, large value payloads are written inline as
  // same-file blob records and the table entry is rewritten to reference them
  // before the normal flush/add logic. entry_ikey / entry_value alias the
  // inputs unless rewriting occurs.
  Slice entry_ikey = ikey;
  Slice entry_value = value;

  if (IsValueType(value_type)) {
    if (r->embedded_blob_options) {
      if (!MaybeExtractEmbeddedBlobs(seq, value_type, &entry_ikey,
                                     &entry_value)) {
        return;  // failed status already recorded
      }
    }
#ifndef NDEBUG
    if (r->props.num_entries > r->props.num_range_deletions) {
      assert(r->internal_comparator.Compare(entry_ikey, Slice(r->last_ikey)) >
             0);
    }
    bool skip = false;
    TEST_SYNC_POINT_CALLBACK("BlockBasedTableBuilder::Add::skip", (void*)&skip);
    if (skip) {
      return;
    }
#endif  // !NDEBUG

    auto should_flush = r->flush_block_policy->Update(entry_ikey, entry_value);
    if (should_flush) {
      assert(!r->data_block.empty());
      Flush(/*first_key_in_next_block=*/&entry_ikey);
    }

    // Note: PartitionedFilterBlockBuilder with
    // decouple_partitioned_filters=false requires key being added to filter
    // builder after being added to and "finished" in the index builder, so
    // forces no parallel compression (logic in Rep constructor).
    if (r->state == Rep::State::kUnbuffered) {
      if (r->filter_builder != nullptr) {
        r->filter_builder->AddWithPrevKey(
            ExtractUserKeyAndStripTimestamp(entry_ikey, r->ts_sz),
            r->last_ikey.empty()
                ? Slice{}
                : ExtractUserKeyAndStripTimestamp(r->last_ikey, r->ts_sz));
      }
    }

    auto* const blob_reference_collector = r->blob_reference_collector.get();
    if (blob_reference_collector != nullptr) {
      auto& collector = *blob_reference_collector;
      const uint64_t user_key_size = ExtractUserKey(entry_ikey).size();
      ValueType final_value_type;
      SequenceNumber ignored_seq;
      UnPackSequenceAndType(ExtractInternalKeyFooter(entry_ikey), &ignored_seq,
                            &final_value_type);
      Status reference_status;
      if (final_value_type == kTypeBlobIndex) {
        BlobIndex blob_index;
        reference_status = blob_index.DecodeFrom(entry_value);
        if (reference_status.ok()) {
          reference_status = collector.Add(blob_index, user_key_size);
        }
      } else if (final_value_type == kTypeWideColumnEntity) {
        reference_status = WideColumnSerialization::ForEachBlobIndex(
            entry_value, [&](const BlobIndex& blob_index) {
              return collector.Add(blob_index, user_key_size);
            });
      }
      if (!reference_status.ok()) {
        r->SetStatus(reference_status);
        return;
      }
    }

    // NOTE: WriteBatch guarantees keys < 4GB; value size checked above
    r->data_block.AddWithLastKey(entry_ikey, entry_value, r->last_ikey);
    r->last_ikey.assign(entry_ikey.data(), entry_ikey.size());
    assert(!r->last_ikey.empty());
    if (r->state == Rep::State::kBuffered) {
      // Buffered keys will be replayed from data_block_buffers during
      // `Finish()` once compression dictionary has been finalized.
    } else {
      r->ForwardOnKeyAddedToAll(entry_ikey, entry_value);
    }
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(entry_ikey, entry_value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);

  } else if (value_type == kTypeRangeDeletion) {
    // NOTE: WriteBatch guarantees keys < 4GB; here 'value' is also a key
    r->range_del_block.Add(ikey, value);
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(ikey, value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);
  } else {
    assert(false);
    r->SetStatus(Status::InvalidArgument(
        "BlockBasedBuilder::Add() received a key with invalid value type " +
        std::to_string(static_cast<unsigned int>(value_type))));
    return;
  }

  r->props.num_entries++;
  r->props.raw_key_size += entry_ikey.size();
  if (!r->persist_user_defined_timestamps) {
    r->props.raw_key_size -= r->ts_sz;
  }
  r->props.raw_value_size += entry_value.size();
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion ||
      value_type == kTypeDeletionWithTimestamp) {
    r->props.num_deletions++;
  } else if (value_type == kTypeRangeDeletion) {
    r->props.num_deletions++;
    r->props.num_range_deletions++;
  } else if (value_type == kTypeMerge) {
    r->props.num_merge_operands++;
  }
}

TableBuilderAddResult BlockBasedTableBuilder::TryAdd(
    const Slice& ikey, const Slice& value, TableBuilderAddContext* context) {
  assert(context != nullptr);
  context->new_table_reason = TableBuilderNewTableReason::kNone;
  Add(ikey, value);
  return TableBuilderAddResult::kAdded;
}

void BlockBasedTableBuilder::Flush(const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(rep_->state != Rep::State::kClosed);
  if (UNLIKELY(!ok())) {
    return;
  }
  if (r->data_block.empty()) {
    return;
  }
  Slice uncompressed_block_data = r->data_block.Finish();

  // NOTE: compression sampling is done here in the same thread as building
  // the uncompressed block because of the requirements to call table
  // property collectors:
  // * BlockAdd function expects block_compressed_bytes_{fast,slow} for
  //   historical reasons. Probably a hassle to remove.
  // * Collector is not thread safe so calls need to be
  // serialized/synchronized.
  // * Ideally, AddUserKey and BlockAdd calls need to line up such that a
  //   reported block corresponds to all the keys reported since the previous
  //   block.

  // If requested, we sample one in every N block with a
  // fast and slow compression algorithm and report the stats.
  // The users can use these stats to decide if it is worthwhile
  // enabling compression and they also get a hint about which
  // compression algorithm wil be beneficial.
  if (r->sample_for_compression > 0 &&
      Random::GetTLSInstance()->OneIn(
          static_cast<int>(r->sample_for_compression))) {
    GrowableBuffer sampled_output;
    sampled_output.ResetForSize(uncompressed_block_data.size());
    size_t fast_size = uncompressed_block_data.size();
    size_t slow_size = uncompressed_block_data.size();

    // Sampling with a fast compression algorithm
    if (r->fast_sample_compressor) {
      CompressionType result_type = kNoCompression;
      Status s = r->fast_sample_compressor->CompressBlock(
          uncompressed_block_data, sampled_output.data(), &fast_size,
          &result_type, /*working_area=*/nullptr);
      if (!s.ok() || result_type == kNoCompression) {
        // For accounting, fall back on no compression
        fast_size = uncompressed_block_data.size();
      }
    }

    // Sampling with a slow but high-compression algorithm
    if (r->slow_sample_compressor) {
      CompressionType result_type = kNoCompression;
      Status s = r->slow_sample_compressor->CompressBlock(
          uncompressed_block_data, sampled_output.data(), &slow_size,
          &result_type, /*working_area=*/nullptr);
      if (!s.ok() || result_type == kNoCompression) {
        // For accounting, fall back on no compression
        slow_size = uncompressed_block_data.size();
      }
    }

    // NOTE: Currently compression sampling is only enabled for data block.
    r->sampled_input_data_bytes.FetchAddRelaxed(uncompressed_block_data.size());
    r->sampled_output_slow_data_bytes.FetchAddRelaxed(slow_size);
    r->sampled_output_fast_data_bytes.FetchAddRelaxed(fast_size);

    NotifyCollectTableCollectorsOnBlockAdd(r->table_properties_collectors,
                                           uncompressed_block_data.size(),
                                           slow_size, fast_size);
  } else {
    NotifyCollectTableCollectorsOnBlockAdd(
        r->table_properties_collectors, uncompressed_block_data.size(),
        0 /*block_compressed_bytes_slow*/, 0 /*block_compressed_bytes_fast*/);
  }

  if (rep_->state == Rep::State::kBuffered) {
    std::string uncompressed_block_holder;
    uncompressed_block_holder.reserve(rep_->table_options.block_size);
    r->data_block.SwapAndReset(uncompressed_block_holder);
    assert(uncompressed_block_data.size() == uncompressed_block_holder.size());
    rep_->data_block_buffers.emplace_back(std::move(uncompressed_block_holder));
    rep_->data_begin_offset += uncompressed_block_data.size();
    MaybeEnterUnbuffered(first_key_in_next_block);
  } else {
    // Increment num_data_blocks when a data block is finalized in the
    // emit thread to avoid data races with write worker threads
    ++r->props.num_data_blocks;

    // Notify filter builder that a data block has been finalized
    // This must happen on the emit thread before the block is added to the
    // ring buffer to avoid race conditions with worker threads
    if (r->filter_builder) {
      r->filter_builder->OnDataBlockFinalized(r->props.num_data_blocks);
    }

    if (r->IsParallelCompressionActive()) {
      EmitBlockForParallel(r->data_block.MutableBuffer(), r->last_ikey,
                           first_key_in_next_block);
    } else {
      EmitBlock(r->data_block.MutableBuffer(), r->last_ikey,
                first_key_in_next_block);
    }
    r->data_block.Reset();
  }
}

void BlockBasedTableBuilder::EmitBlockForParallel(
    std::string& uncompressed, const Slice& last_key_in_current_block,
    const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  assert(uncompressed.size() > 0);
  auto& pc_rep = *r->pc_rep;
  // Can emit the uncompressed block into the ring buffer
  assert(pc_rep.emit_thread_state ==
         ParallelCompressionRep::ThreadState::kEmitting);
  auto* block_rep = &pc_rep.ring_buffer[pc_rep.emit_slot];
  pc_rep.estimated_inflight_size.FetchAddRelaxed(uncompressed.size() +
                                                 kBlockTrailerSize);
  std::swap(uncompressed, block_rep->uncompressed);

  // Reset before any conditional staging below: BlockReps are reused via
  // the ring buffer, and a stale `true` flag would cause BGWorker to
  // commit data from the slot's previous iteration.
  block_rep->index_entry_prepared = false;
  block_rep->custom_entries_prepared = false;

  // Stage the built-in index entry (skipped only in kCustomOnly).
  // PrepareAddEntryDirect bypasses ParseInternalKey + repack.
  if (r->builtin_index_builder != nullptr) {
    r->builtin_index_builder->PrepareAddEntryDirect(
        last_key_in_current_block, first_key_in_next_block,
        block_rep->prepared_index_entry.get());
    block_rep->index_entry_prepared = true;
  }

  // Stage entries for custom indexes. Every builder reachable here
  // supports the parallel protocol (verified at Rep construction).
  //
  // On failure this leaves custom_entries_prepared false and falls
  // through rather than returning: the slot has already been claimed and
  // charged to estimated_inflight_size, so the emitter must still run the
  // state transition below or the pipeline stalls holding the block.
  if (!r->custom_indexes.empty()) {
    // Test hook: skip_custom_prepare=true takes the same path as a parse
    // failure, exercising the stale-flag reset above.
    bool skip_custom_prepare = false;
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::EmitBlockForParallel:SkipCustomPrepare",
        &skip_custom_prepare);
    if (!skip_custom_prepare) {
      block_rep->custom_entries_prepared = [&]() {
        ParsedInternalKey last_pkey;
        Status parse_s = ParseInternalKey(last_key_in_current_block, &last_pkey,
                                          /*log_err_key=*/false);
        assert(parse_s.ok());
        if (UNLIKELY(!parse_s.ok())) {
          r->SetStatus(
              Status::Corruption("Failed to parse last_key_in_current_block in "
                                 "EmitBlockForParallel: " +
                                 parse_s.ToString()));
          return false;
        }
        IndexFactoryBuilder::IndexEntryContext ctx;
        ctx.last_key_tag =
            PackSequenceAndType(last_pkey.sequence, last_pkey.type);
        const Slice* next_user = nullptr;
        ParsedInternalKey next_pkey;
        if (first_key_in_next_block != nullptr) {
          Status next_parse_s =
              ParseInternalKey(*first_key_in_next_block, &next_pkey,
                               /*log_err_key=*/false);
          assert(next_parse_s.ok());
          if (UNLIKELY(!next_parse_s.ok())) {
            r->SetStatus(
                Status::Corruption("Failed to parse first_key_in_next_block in "
                                   "EmitBlockForParallel: " +
                                   next_parse_s.ToString()));
            return false;
          }
          next_user = &next_pkey.user_key;
          ctx.first_key_tag =
              PackSequenceAndType(next_pkey.sequence, next_pkey.type);
        }
        for (size_t i = 0; i < r->custom_indexes.size(); i++) {
          r->custom_indexes[i].builder->PrepareAddEntry(
              last_pkey.user_key, next_user, ctx,
              block_rep->custom_prepared_entries[i].get());
        }
        return true;
      }();
    }
  }

  block_rep->compressed.Reset();
  block_rep->compression_type = kNoCompression;
  block_rep->contents_checksum = 0;
  // Emit thread makes the auto-skip decision here so skipped blocks are never
  // dispatched to a worker for (no-op) compression; StateTransition
  // auto-completes them straight to the write stage.
  bool auto_skip_bypassed =
      r->AutoSkipEnabled() &&
      block_rep->uncompressed.size() < kCompressionSizeLimit &&
      r->AutoSkipDecideBypassEligible();
  block_rep->auto_skip_bypassed.StoreRelaxed(auto_skip_bypassed);
  if (auto_skip_bypassed) {
    r->AutoSkipRecordBypass(block_rep->uncompressed.size());
  }

  // Might need to take up some compression work before we are able to
  // resume emitting the next uncompressed block.
  for (;;) {
    pc_rep.EmitterStateTransition(pc_rep.emit_thread_state, pc_rep.emit_slot);

    if (pc_rep.emit_thread_state ==
        ParallelCompressionRep::ThreadState::kCompressing) {
      // Took up some compression work to help unblock ourself
      block_rep = &pc_rep.ring_buffer[pc_rep.emit_slot];
      Status s = CompressAndVerifyBlock(
          block_rep->uncompressed, /*is_data_block=*/true,
          r->data_block_working_area, &block_rep->compressed,
          &block_rep->compression_type, &block_rep->contents_checksum);
      if (UNLIKELY(!s.ok())) {
        r->SetStatus(s);
        pc_rep.SetAbort(pc_rep.emit_thread_state);
        break;
      }
    } else {
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kCompressingAndWriting);
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kWriting);
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kIdle);
      // Either emitting or end state.
      // Detect nothing more to emit and set if so.
      if (first_key_in_next_block == nullptr &&
          pc_rep.emit_thread_state ==
              ParallelCompressionRep::ThreadState::kEmitting) {
        pc_rep.SetNoMoreToEmit(pc_rep.emit_thread_state, pc_rep.emit_slot);
      }
      break;
    }
  }
}
void BlockBasedTableBuilder::EmitBlock(std::string& uncompressed,
                                       const Slice& last_key_in_current_block,
                                       const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  // Single-threaded context only
  assert(!r->IsParallelCompressionActive());
  assert(uncompressed.size() > 0);
  // When data blocks are aligned with super block alignment, delta encoding
  // needs to be skipped for the first block after padding.
  bool skip_delta_encoding = false;
  WriteBlock(uncompressed, &r->pending_handle, BlockType::kData,
             &skip_delta_encoding);
  if (LIKELY(ok())) {
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    r->ForwardAddIndexEntryToAll(last_key_in_current_block,
                                 first_key_in_next_block, r->pending_handle,
                                 skip_delta_encoding);
  }
}

void BlockBasedTableBuilder::WriteBlock(const Slice& uncompressed_block_data,
                                        BlockHandle* handle,
                                        BlockType block_type,
                                        bool* skip_delta_encoding) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  // Single-threaded context only
  assert(!r->IsParallelCompressionActive());
  CompressionType type = kNoCompression;
  uint32_t block_contents_crc32c = 0;
  bool is_data_block = block_type == BlockType::kData;
  // NOTE: only index and data blocks are currently compressed
  assert(is_data_block || block_type == BlockType::kIndex);
  // Serial path: the emit thread makes the auto-skip decision here, inline. A
  // bypassed block is stored raw without ever attempting compression (symmetric
  // with the parallel path, which folds skips in StateTransition).
  if (is_data_block && r->AutoSkipEnabled() &&
      uncompressed_block_data.size() < kCompressionSizeLimit &&
      r->AutoSkipDecideBypassEligible()) {
    r->AutoSkipRecordBypass(uncompressed_block_data.size());
  } else {
    Status compress_status = CompressAndVerifyBlock(
        uncompressed_block_data, is_data_block,
        is_data_block ? r->data_block_working_area
                      : r->index_block_working_area,
        &r->single_threaded_compressed_output, &type, &block_contents_crc32c);
    r->SetStatus(compress_status);
    if (UNLIKELY(!ok())) {
      return;
    }
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WriteBlock:TamperWithCompressedData",
        &r->single_threaded_compressed_output);
  }
  WriteMaybeCompressedBlock(
      type == kNoCompression ? uncompressed_block_data
                             : Slice(r->single_threaded_compressed_output),
      type, handle, block_type, &uncompressed_block_data, skip_delta_encoding,
      type == kNoCompression || r->table_options.checksum != kCRC32c
          ? nullptr
          : &block_contents_crc32c);
  r->single_threaded_compressed_output.Reset();
  if (is_data_block) {
    r->props.data_size = r->get_offset();
    r->props.uncompressed_data_size += uncompressed_block_data.size();
  }
}

uint64_t BlockBasedTableBuilder::GetWorkerCPUMicros() const {
  return rep_->worker_cpu_micros.LoadRelaxed();
}

void BlockBasedTableBuilder::BGWorker(WorkingAreaPair& working_area) {
  // Record CPU usage of this thread
  const uint64_t start_cpu_micros =
      rep_->ioptions.env->GetSystemClock()->CPUMicros();
  Defer log_cpu{[this, start_cpu_micros]() {
    rep_->worker_cpu_micros.FetchAddRelaxed(
        rep_->ioptions.env->GetSystemClock()->CPUMicros() - start_cpu_micros);
  }};

  auto& pc_rep = *rep_->pc_rep;
#ifdef BBTB_PC_WATCHDOG
  pc_rep.live_workers.FetchAddRelaxed(1);
  Defer decr{[&pc_rep]() { pc_rep.live_workers.FetchSubRelaxed(1); }};
#endif  // BBTB_PC_WATCHDOG
  ParallelCompressionRep::ThreadState thread_state =
      ParallelCompressionRep::ThreadState::kIdle;
  uint32_t slot = 0;
  // Workers should avoid checking the shared status (e.g. ok()) to minimize
  // potential data dependencies across threads. If another thread hits an
  // error, we will pick up the kEnd state from the abort.
  IOStatus ios;
  do {
    pc_rep.WorkerStateTransition(thread_state, slot);
    ParallelCompressionRep::BlockRep* block_rep = &pc_rep.ring_buffer[slot];
    auto compress_fn = [this, block_rep, &ios, &working_area]() {
      ios = status_to_io_status(CompressAndVerifyBlock(
          block_rep->uncompressed, /*is_data_block=*/true, working_area,
          &block_rep->compressed, &block_rep->compression_type,
          &block_rep->contents_checksum));
    };
    auto write_fn = [this, block_rep, &ios]() {
      Slice compressed = block_rep->compressed;
      Slice uncompressed = block_rep->uncompressed;
      bool skip_delta_encoding = false;
      const bool has_precomputed_block_contents_crc32c =
          block_rep->compression_type != kNoCompression &&
          rep_->table_options.checksum == kCRC32c;
      ios = WriteMaybeCompressedBlockImpl(
          block_rep->compression_type == kNoCompression ? uncompressed
                                                        : compressed,
          block_rep->compression_type, &rep_->pending_handle, BlockType::kData,
          &uncompressed, &skip_delta_encoding,
          has_precomputed_block_contents_crc32c ? &block_rep->contents_checksum
                                                : nullptr);
      if (LIKELY(ios.ok())) {
        rep_->props.data_size = rep_->get_offset();
        rep_->props.uncompressed_data_size += block_rep->uncompressed.size();

        // The parallel pipeline state machine serializes write_fn so
        // FinishAddEntry runs on a single writer thread at a time. That
        // makes it safe to use each builder's per-builder scratch buffer
        // without locking.
        IndexFactoryBuilder::BlockHandle pub_handle{
            rep_->pending_handle.offset(), rep_->pending_handle.size()};
        // Built-in index (null only in kCustomOnly mode).
        if (rep_->index_builder && block_rep->index_entry_prepared) {
          rep_->index_builder->FinishAddEntry(
              pub_handle, block_rep->prepared_index_entry.get(),
              &rep_->index_separator_scratch, skip_delta_encoding);
        }
        // Custom indexes that opted into the parallel protocol.
        if (block_rep->custom_entries_prepared) {
          for (size_t i = 0; i < rep_->custom_indexes.size(); i++) {
            rep_->custom_indexes[i].builder->FinishAddEntry(
                pub_handle, block_rep->custom_prepared_entries[i].get(),
                &rep_->custom_indexes[i].separator_scratch,
                skip_delta_encoding);
          }
        }
      }
    };
    switch (thread_state) {
      case ParallelCompressionRep::ThreadState::kEnd:
        // All done
        assert(ios.ok());
        return;
      case ParallelCompressionRep::ThreadState::kCompressing:
        compress_fn();
        break;
      case ParallelCompressionRep::ThreadState::kCompressingAndWriting:
        compress_fn();
        if (LIKELY(ios.ok())) {
          write_fn();
        }
        break;
      case ParallelCompressionRep::ThreadState::kWriting:
        write_fn();
        break;
      case ParallelCompressionRep::ThreadState::kEmitting:
        // Shouldn't happen
        assert(thread_state != ParallelCompressionRep::ThreadState::kEmitting);
        break;
      case ParallelCompressionRep::ThreadState::kIdle:
        // Shouldn't happen
        assert(thread_state != ParallelCompressionRep::ThreadState::kIdle);
        break;
      default:
        assert(false);
        break;
    }
  } while (LIKELY(ios.ok()));
  // Hit an error, so abort
  rep_->SetIOStatus(ios);
  pc_rep.SetAbort(thread_state);
}

Status BlockBasedTableBuilder::CompressAndVerifyBlock(
    const Slice& uncompressed_block_data, bool is_data_block,
    WorkingAreaPair& working_area, GrowableBuffer* compressed_output,
    CompressionType* result_compression_type, uint32_t* result_checksum) {
  Rep* r = rep_.get();
  Status status;
  *result_checksum = 0;

  UnownedPtr<Compressor> compressor = nullptr;
  Decompressor* verify_decomp = nullptr;
  if (is_data_block) {
    compressor = r->data_block_compressor;
    verify_decomp = r->data_block_verify_decompressor.get();
  } else {
    compressor = r->index_block_compressor;
    verify_decomp = r->verify_decompressor.get();
  }

  compressed_output->Reset();
  CompressionType type = kNoCompression;
  const bool compression_eligible =
      uncompressed_block_data.size() < kCompressionSizeLimit;
  // AutoSkip: this function is only ever called to ATTEMPT compression -- the
  // skip/attempt decision (and any bypass) is handled by the caller (emit
  // thread). We "learn" (update q_hat) only when auto-skip is on and we attempt
  // compression on an eligible data block.
  const bool auto_skip_learn = is_data_block && compressor &&
                               r->AutoSkipEnabled() && compression_eligible;
  if (LIKELY(compression_eligible)) {
    if (compressor) {
      StopWatchNano timer(
          r->ioptions.clock,
          ShouldReportDetailedTime(r->ioptions.env, r->ioptions.stats));

      size_t max_compressed_size = static_cast<size_t>(
          (static_cast<uint64_t>(r->max_compressed_bytes_per_kb) *
           uncompressed_block_data.size()) >>
          10);
      compressed_output->ResetForSize(max_compressed_size);
      status = compressor->CompressBlock(
          uncompressed_block_data, compressed_output->data(),
          &compressed_output->MutableSize(), &type, &working_area.compress);

      // Post-condition of Compressor::CompressBlock
      assert(type == kNoCompression || status.ok());
      assert(type == kNoCompression ||
             r->table_options.verify_compression == (verify_decomp != nullptr));

      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::CompressAndVerifyBlock:TamperWithResultType",
          &type);

      if (type != kNoCompression) {
        if (r->table_options.checksum == kCRC32c) {
          *result_checksum = crc32c::Value(compressed_output->data(),
                                           compressed_output->size());
        }
        TEST_SYNC_POINT_CALLBACK(
            "BlockBasedTableBuilder::CompressAndVerifyBlock:"
            "TamperWithCompressedDataBeforeVerify",
            compressed_output);
      }

      // Some of the compression algorithms are known to be unreliable. If
      // the verify_compression flag is set then try to de-compress the
      // compressed data and compare to the input.
      if (verify_decomp && type != kNoCompression) {
        BlockContents contents;
        Status uncompress_status = DecompressBlockData(
            compressed_output->data(), compressed_output->size(), type,
            *verify_decomp, &contents, r->ioptions,
            /*allocator=*/nullptr, &working_area.verify);

        if (LIKELY(uncompress_status.ok())) {
          bool data_match = contents.data.compare(uncompressed_block_data) == 0;
          if (!data_match) {
            // The result of the compression was invalid. abort.
            const char* const msg =
                "Decompressed block did not match pre-compression block";
            ROCKS_LOG_ERROR(r->ioptions.logger, "%s", msg);
            status = Status::Corruption(msg);
            type = kNoCompression;
          }
        } else {
          // Decompression reported an error. abort.
          status = Status::Corruption(std::string("Could not decompress: ") +
                                      uncompress_status.getState());
          type = kNoCompression;
        }
      }
      if (timer.IsStarted()) {
        RecordTimeToHistogram(r->ioptions.stats, COMPRESSION_TIMES_NANOS,
                              timer.ElapsedNanos());
      }
    }
  }
  if (is_data_block) {
    // Data blocks eligible for compression (normal size) count as
    // "compressible input" whether or not compression was attempted this time.
    // An auto-skip bypass is a CPU optimization, not a claim that the bytes are
    // incompressible, so it must not bias the sample_for_compression size
    // estimate, which extrapolates the randomly-sampled fast/slow ratio over
    // compressible_input_data_bytes (the sampling itself is independent of
    // auto-skip). Only ineligible (too-big) blocks and block trailers count as
    // uncompressible.
    if (LIKELY(compression_eligible)) {
      r->compressible_input_data_bytes.FetchAddRelaxed(
          uncompressed_block_data.size());
      r->uncompressible_input_data_bytes.FetchAddRelaxed(kBlockTrailerSize);
    } else {
      r->uncompressible_input_data_bytes.FetchAddRelaxed(
          uncompressed_block_data.size() + kBlockTrailerSize);
    }
  }

  // AutoSkip: learn from this attempt's outcome (only when we actually
  // attempted; skipped/bypassed blocks carry no compressibility signal). The
  // observation is the realized stored ratio: the achieved ratio when the block
  // was kept compressed, or 1.0 (stored raw) when compression was rejected or
  // otherwise not kept -- so q_hat tracks the aggregate stored size auto-skip
  // would produce by attempting.
  if (auto_skip_learn) {
    uint32_t obs = kAutoSkipScale;  // stored raw (ratio 1.0)
    if (type != kNoCompression) {
      uint64_t scaled = static_cast<uint64_t>(compressed_output->size()) *
                        kAutoSkipScale / uncompressed_block_data.size();
      obs = scaled < kAutoSkipScale ? static_cast<uint32_t>(scaled)
                                    : kAutoSkipScale;
    }
    r->AutoSkipUpdateQHat(obs);
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (type == kNoCompression) {
    bool compression_attempted = !compressed_output->empty();
    RecordTick(r->ioptions.stats, compression_attempted
                                      ? NUMBER_BLOCK_COMPRESSION_REJECTED
                                      : NUMBER_BLOCK_COMPRESSION_BYPASSED);
    RecordTick(r->ioptions.stats,
               compression_attempted ? BYTES_COMPRESSION_REJECTED
                                     : BYTES_COMPRESSION_BYPASSED,
               uncompressed_block_data.size());
    if (is_data_block) {
      if (compression_attempted) {
        r->num_data_blocks_compression_rejected.FetchAddRelaxed(1);
      } else {
        r->num_data_blocks_compression_bypassed.FetchAddRelaxed(1);
      }
    }
  } else {
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_COMPRESSED);
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_FROM,
               uncompressed_block_data.size());
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_TO,
               compressed_output->size());
    if (r->IsParallelCompressionActive() && is_data_block) {
      r->pc_rep->estimated_inflight_size.FetchSubRelaxed(
          uncompressed_block_data.size() - compressed_output->size());
    }
  }
  *result_compression_type = type;
  return status;
}

void BlockBasedTableBuilder::WriteMaybeCompressedBlock(
    const Slice& block_contents, CompressionType comp_type, BlockHandle* handle,
    BlockType block_type, const Slice* uncompressed_block_data,
    bool* skip_delta_encoding,
    const uint32_t* precomputed_block_contents_crc32c) {
  // Must have pre-checked status in single-threaded context
  assert(status().ok());
  assert(io_status().ok());
  rep_->SetIOStatus(WriteMaybeCompressedBlockImpl(
      block_contents, comp_type, handle, block_type, uncompressed_block_data,
      skip_delta_encoding, precomputed_block_contents_crc32c));
}

IOStatus BlockBasedTableBuilder::WriteMaybeCompressedBlockImpl(
    const Slice& block_contents, CompressionType comp_type, BlockHandle* handle,
    BlockType block_type, const Slice* uncompressed_block_data,
    bool* skip_delta_encoding,
    const uint32_t* precomputed_block_contents_crc32c) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compression_type: uint8
  //    checksum: uint32
  Rep* r = rep_.get();
  bool is_data_block = block_type == BlockType::kData;
  // For data block, skip_delta_encoding must be non null
  if (is_data_block) {
    assert(skip_delta_encoding != nullptr);
  }
  if (skip_delta_encoding != nullptr) {
    *skip_delta_encoding = false;
  }
  IOOptions io_options;
  // Always return io_s for NRVO
  IOStatus io_s =
      WritableFileWriter::PrepareIOOptions(r->write_options, io_options);
  if (UNLIKELY(!io_s.ok())) {
    return io_s;
  }
  // Old, misleading name of this function: WriteRawBlock
  StopWatch sw(r->ioptions.clock, r->ioptions.stats, WRITE_RAW_BLOCK_MICROS);

  auto offset = r->get_offset();
  // try to align the data block page to the super alignment size, if enabled
  if ((r->table_options.super_block_alignment_size != 0) && is_data_block) {
    auto super_block_alignment_mask =
        r->table_options.super_block_alignment_size - 1;
    if ((r->table_options.super_block_alignment_space_overhead_ratio != 0) &&
        (offset & (~super_block_alignment_mask)) !=
            ((offset + block_contents.size()) &
             (~super_block_alignment_mask))) {
      auto allowed_max_padding_size =
          r->table_options.super_block_alignment_size /
          r->table_options.super_block_alignment_space_overhead_ratio;
      // new block would cross the super block boundary
      auto pad_bytes = r->table_options.super_block_alignment_size -
                       (offset & super_block_alignment_mask);
      if (pad_bytes < allowed_max_padding_size) {
        io_s = r->file->Pad(io_options, pad_bytes, allowed_max_padding_size);
        if (UNLIKELY(!io_s.ok())) {
          r->SetIOStatus(io_s);
          return io_s;
        }
        r->pre_compression_size += pad_bytes;
        offset += pad_bytes;
        r->set_offset(offset);
        if (skip_delta_encoding != nullptr) {
          // Skip delta encoding in index block builder when a super block
          // alignment padding is added for data block.
          *skip_delta_encoding = true;
        }
        TEST_SYNC_POINT(
            "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
            "SuperBlockAlignment");
      } else {
        TEST_SYNC_POINT(
            "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
            "SuperBlockAlignmentPaddingBytesExceedLimit");
      }
    }
  }

  handle->set_offset(offset);
  handle->set_size(block_contents.size());
  if (uncompressed_block_data == nullptr) {
    uncompressed_block_data = &block_contents;
    assert(comp_type == kNoCompression);
  }
  assert(precomputed_block_contents_crc32c == nullptr ||
         r->table_options.checksum == kCRC32c);

  r->compression_types_used.Add(comp_type);
  std::array<char, kBlockTrailerSize> trailer;
  const char compression_type = comp_type;
  trailer[0] = compression_type;
  uint32_t block_contents_crc32c = 0;
  uint32_t checksum = 0;
  if (r->table_options.checksum == kCRC32c) {
    block_contents_crc32c =
        precomputed_block_contents_crc32c != nullptr
            ? *precomputed_block_contents_crc32c
            : crc32c::Value(block_contents.data(), block_contents.size());
    checksum = crc32c::Mask(crc32c::Extend(
        block_contents_crc32c, &compression_type, sizeof(compression_type)));
  } else {
    checksum = ComputeBuiltinChecksumWithLastByte(
        r->table_options.checksum, block_contents.data(), block_contents.size(),
        /*last_byte*/ comp_type);
  }
  checksum += ChecksumModifierForContext(r->base_context_checksum, offset);

  {
    if (r->table_options.checksum == kCRC32c) {
      Crc32cChecksum block_contents_checksum(block_contents_crc32c);
      NotifyFileAppendForTest(block_contents, &block_contents_checksum);
      io_s =
          r->file->Append(block_contents, io_options, block_contents_checksum);
    } else {
      NotifyFileAppendForTest(block_contents, nullptr);
      io_s = r->file->Append(block_contents, io_options);
    }
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  if (block_type == BlockType::kFilter) {
    io_s = status_to_io_status(
        r->filter_builder->MaybePostVerifyFilter(block_contents));
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  uint32_t trailer_crc32c = 0;
  if (r->table_options.checksum == kCRC32c) {
    char encoded_checksum[sizeof(uint32_t)];
    EncodeFixed32(encoded_checksum, checksum);
    trailer_crc32c =
        crc32c::Extend(0, &compression_type, sizeof(compression_type));
    trailer_crc32c = crc32c::Extend(trailer_crc32c, encoded_checksum,
                                    sizeof(encoded_checksum));
  }
  EncodeFixed32(trailer.data() + 1, checksum);
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:TamperWithChecksum",
      trailer.data());
  {
    const Slice trailer_to_write(trailer.data(), trailer.size());
    if (r->table_options.checksum == kCRC32c) {
      Crc32cChecksum trailer_checksum(trailer_crc32c);
      NotifyFileAppendForTest(trailer_to_write, &trailer_checksum);
      io_s = r->file->Append(trailer_to_write, io_options, trailer_checksum);
    } else {
      NotifyFileAppendForTest(trailer_to_write, nullptr);
      io_s = r->file->Append(trailer_to_write, io_options);
    }
    if UNLIKELY (!io_s.ok()) {
      return io_s;
    }
  }

  if (r->warm_cache_config.enabled) {
    io_s = status_to_io_status(
        InsertBlockInCacheHelper(*uncompressed_block_data, handle, block_type));
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  r->pre_compression_size +=
      uncompressed_block_data->size() + kBlockTrailerSize;
  r->set_offset(r->get_offset() + block_contents.size() + kBlockTrailerSize);
  if (r->table_options.block_align && is_data_block) {
    size_t pad_bytes =
        (r->alignment -
         ((block_contents.size() + kBlockTrailerSize) & (r->alignment - 1))) &
        (r->alignment - 1);

    io_s = r->file->Pad(io_options, pad_bytes, kDefaultPageSize);
    if (LIKELY(io_s.ok())) {
      r->pre_compression_size += pad_bytes;
      r->set_offset(r->get_offset() + pad_bytes);
    } else {
      return io_s;
    }
  }

  if (r->IsParallelCompressionActive() && is_data_block) {
    r->pc_rep->estimated_inflight_size.FetchSubRelaxed(block_contents.size() +
                                                       kBlockTrailerSize);
  }
  return io_s;
}

void BlockBasedTableBuilder::MaybeStartParallelCompression() {
  if (rep_->compression_parallel_threads <= 1) {
    return;
  }
  // Although in theory having a separate thread for writing to the SST file
  // could help to hide the latency associated with writing, it is more often
  // the case that the latency comes in large units for rare calls to write that
  // flush downstream buffers, including in WritableFileWriter. The buffering
  // provided by the compression ring buffer is almost negligible for hiding
  // that latency. So even with some optimizations, turning on the parallel
  // framework when compression is disabled just eats more CPU with little-to-no
  // improvement in throughput.
  if (!rep_->data_block_compressor) {
    // Force the generally best configuration for no compression: no parallelism
    return;
  }
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::MaybeStartParallelCompression:Started",
      &rep_->compression_parallel_threads);
  rep_->pc_rep = std::make_unique<ParallelCompressionRep>(
      rep_->compression_parallel_threads);
  auto& pc_rep = *rep_->pc_rep;
  // index_builder is null only in kCustomOnly mode.
  if (rep_->index_builder) {
    for (uint32_t i = 0; i <= pc_rep.ring_buffer_mask; i++) {
      pc_rep.ring_buffer[i].prepared_index_entry =
          rep_->index_builder->CreatePreparedAddEntry();
    }
  }
  // Every custom builder reachable here supports the parallel protocol and
  // returned a non-null prepared entry when probed at Rep construction. One
  // that starts returning null after that would be dereferenced on the emit
  // thread, so fall back to serial compression instead. No worker thread has
  // started yet, so clearing pc_rep is all it takes.
  if (!rep_->custom_indexes.empty()) {
    for (uint32_t i = 0; i <= pc_rep.ring_buffer_mask; i++) {
      auto& slot = pc_rep.ring_buffer[i];
      slot.custom_prepared_entries.reserve(rep_->custom_indexes.size());
      for (auto& ci : rep_->custom_indexes) {
        std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry> entry =
            ci.builder->CreatePreparedAddEntry();
        if (UNLIKELY(entry == nullptr)) {
          assert(false);
          rep_->pc_rep.reset();
          return;
        }
        slot.custom_prepared_entries.emplace_back(std::move(entry));
      }
    }
  }
  pc_rep.worker_threads.reserve(pc_rep.num_worker_threads);
  pc_rep.working_areas.resize(pc_rep.num_worker_threads);
  for (uint32_t i = 0; i < pc_rep.num_worker_threads; i++) {
    auto& wa = pc_rep.working_areas[i];
    if (rep_->data_block_compressor) {
      wa.compress = rep_->data_block_compressor->ObtainWorkingArea();
    }
    if (rep_->data_block_verify_decompressor) {
      wa.verify = rep_->data_block_verify_decompressor->ObtainWorkingArea(
          rep_->data_block_compressor->GetPreferredCompressionType());
    }
    pc_rep.worker_threads.emplace_back([this, &wa] { BGWorker(wa); });
  }
#ifdef BBTB_PC_WATCHDOG
  // Start watchdog thread
  pc_rep.watchdog_thread = std::thread([&pc_rep] { pc_rep.BGWatchdog(); });
  pc_rep.live_emit.StoreRelaxed(true);
#endif  // BBTB_PC_WATCHDOG
}

void BlockBasedTableBuilder::StopParallelCompression(bool abort) {
  auto& pc_rep = *rep_->pc_rep;
  if (abort) {
    pc_rep.SetAbort(pc_rep.emit_thread_state);
  } else if (pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kEnd) {
    // In case we didn't do a final flush with no next key, which might have
    // been skipped if !ok() was set after the start of Finish()
    assert(rep_->props.num_data_blocks == 0 || !ok());
    pc_rep.SetNoMoreToEmit(pc_rep.emit_thread_state, pc_rep.emit_slot);
  }
#ifdef BBTB_PC_WATCHDOG
  pc_rep.live_emit.StoreRelaxed(false);
#endif  // BBTB_PC_WATCHDOG
  assert(pc_rep.emit_thread_state == ParallelCompressionRep::ThreadState::kEnd);
  for (auto& thread : pc_rep.worker_threads) {
    thread.join();
  }
#ifdef BBTB_PC_WATCHDOG
  // Wake & shutdown watchdog thread
  {
    std::unique_lock<std::mutex> lock(pc_rep.watchdog_mutex);
    pc_rep.shutdown_watchdog = true;
    pc_rep.watchdog_cv.notify_all();
  }
  pc_rep.watchdog_thread.join();
#endif  // BBTB_PC_WATCHDOG
  rep_->pc_rep.reset();
}

Status BlockBasedTableBuilder::status() const { return rep_->GetStatus(); }

IOStatus BlockBasedTableBuilder::io_status() const {
  return rep_->GetIOStatus();
}

bool BlockBasedTableBuilder::ok() const { return rep_->StatusOk(); }

Status BlockBasedTableBuilder::WriteEmbeddedBlobRecord(
    const Slice& payload, CompressionType compression,
    uint64_t* record_offset) {
  Rep* r = rep_.get();
  assert(r->embedded_blob_options);
  // Inline blob writing requires single-threaded writes so the running table
  // offset matches the file position (enforced in the Rep constructor).
  assert(!r->IsParallelCompressionActive());

  // Embedded blob payloads share the 32-bit value-size limit with the rest of
  // the table format; enforcing it here keeps record sizes in range and gives
  // an early, cheap corruption check.
  if (payload.size() > kMaxSimpleGen2BlobPayloadSize) {
    return Status::InvalidArgument("Embedded blob value is too large");
  }

  if (!r->embedded_blob_state) {
    r->embedded_blob_state = std::make_unique<Rep::EmbeddedBlobState>();
  }

  // The builder's running table offset equals the current file size, so the
  // record begins exactly here.
  const uint64_t offset = r->get_offset();
  IOStatus io_s = WriteSimpleGen2BlobRecord(
      r->file, r->write_options, r->table_options.checksum,
      r->base_context_checksum, offset, payload, compression);
  if (!io_s.ok()) {
    r->SetIOStatus(io_s);
    return io_s;
  }

  const uint64_t record_size = payload.size() + kSimpleGen2BlobTrailerSize;
  r->set_offset(offset + record_size);
  r->embedded_blob_state->stats.blob_count++;
  r->embedded_blob_state->stats.payload_bytes += payload.size();

  BlobIndex::EncodeBlob(&r->embedded_blob_state->blob_index_buf,
                        kCurrentFileBlobIndexFileNumber, offset, payload.size(),
                        compression);
  if (record_offset != nullptr) {
    *record_offset = offset;
  }
  return Status::OK();
}

Status BlockBasedTableBuilder::StartBlobRelocationFile() {
  Rep* r = rep_.get();
  if (!ok()) {
    return status();
  }
  if (r->blob_relocation_directory_builder != nullptr ||
      r->props.num_entries != 0 || r->get_offset() != 0 ||
      r->embedded_blob_options == nullptr) {
    return Status::InvalidArgument(
        "Blob relocation file must start on a fresh embedded table builder");
  }
  if (r->props.orig_file_number == 0 || r->props.db_session_id.empty()) {
    return Status::InvalidArgument(
        "Blob relocation file requires persisted file identity");
  }
  if (r->internal_comparator.user_comparator() != BytewiseComparator()) {
    return Status::InvalidArgument(
        "Blob relocation file requires the bytewise comparator without "
        "timestamps");
  }
  assert(!r->IsParallelCompressionActive());

  IOOptions io_options;
  IOStatus io_status =
      WritableFileWriter::PrepareIOOptions(r->write_options, io_options);
  if (io_status.ok()) {
    io_status = r->file->Append(
        Slice(kBlobRelocationFilePrefix, kBlobRelocationFilePrefixSize),
        io_options);
  }
  if (!io_status.ok()) {
    r->SetIOStatus(io_status);
    return io_status;
  }
  r->set_offset(kBlobRelocationFilePrefixSize);
  r->blob_relocation_directory_builder =
      std::make_unique<BlobRelocationDirectoryBuilder>();
  return Status::OK();
}

Status BlockBasedTableBuilder::AddBlobRelocation(uint64_t origin_file_number,
                                                 uint64_t original_value_offset,
                                                 const Slice& stored_payload,
                                                 CompressionType compression) {
  Rep* r = rep_.get();
  if (r->blob_relocation_directory_builder == nullptr) {
    return Status::InvalidArgument("Blob relocation file has not been started");
  }
  if (!ok()) {
    return status();
  }
  if (stored_payload.size() > kMaxSimpleGen2BlobPayloadSize) {
    return Status::InvalidArgument("Blob relocation value is too large");
  }
  if (!CompressionTypeSupported(compression)) {
    return Status::InvalidArgument(
        "Blob relocation compression type is not supported");
  }

  const uint64_t relocated_value_offset = r->get_offset();
  Status add_status = r->blob_relocation_directory_builder->Add(
      origin_file_number, original_value_offset, relocated_value_offset,
      stored_payload.size(), compression);
  if (!add_status.ok()) {
    r->SetStatus(add_status);
    return add_status;
  }
  uint64_t written_value_offset = 0;
  add_status = WriteEmbeddedBlobRecord(stored_payload, compression,
                                       &written_value_offset);
  if (!add_status.ok()) {
    return add_status;
  }
  assert(written_value_offset == relocated_value_offset);

  const std::string user_key =
      EncodeBlobRelocationKey(origin_file_number, original_value_offset);
  const InternalKey internal_key(user_key, 0, kTypeBlobIndex);
  Add(internal_key.Encode(), r->embedded_blob_state->blob_index_buf);
  return status();
}

Status BlockBasedTableBuilder::GetBlobRelocationFileMetadata(
    BlobRelocationFileMetadata* metadata) const {
  if (metadata == nullptr) {
    return Status::InvalidArgument("Null blob relocation metadata output");
  }
  if (!ok()) {
    return status();
  }
  if (!rep_->blob_relocation_file_metadata.has_value()) {
    return Status::InvalidArgument("Blob relocation file is not finished");
  }
  *metadata = *rep_->blob_relocation_file_metadata;
  metadata->tail_size = GetTailSize();
  return Status::OK();
}

Status BlockBasedTableBuilder::BuildEmbeddedWideColumnEntity(const Slice& value,
                                                             bool* rewritten) {
  Rep* r = rep_.get();
  assert(rewritten != nullptr);
  *rewritten = false;

  WideColumns columns;
  Slice input = value;
  Status s = WideColumnSerialization::DeserializeSimple(input, columns);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  blob_columns.reserve(columns.size());

  const uint64_t min_blob_size = r->embedded_blob_options->min_blob_size;
  for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx) {
    const Slice& column_value = columns[column_idx].value();
    if (column_value.size() < min_blob_size) {
      continue;
    }
    s = WriteEmbeddedBlobRecord(column_value, kNoCompression,
                                /*record_offset=*/nullptr);
    if (!s.ok()) {
      return s;
    }
    BlobIndex blob_index;
    s = blob_index.DecodeFrom(r->embedded_blob_state->blob_index_buf);
    if (!s.ok()) {
      return s;
    }
    blob_columns.emplace_back(column_idx, blob_index);
  }

  if (blob_columns.empty()) {
    return Status::OK();
  }

  // At least one record was written above, so the state exists.
  r->embedded_blob_state->entity_buf.clear();
  s = WideColumnSerialization::SerializeV2(columns, blob_columns,
                                           r->embedded_blob_state->entity_buf);
  if (!s.ok()) {
    return s;
  }
  *rewritten = true;
  return Status::OK();
}

bool BlockBasedTableBuilder::MaybeExtractEmbeddedBlobs(SequenceNumber seq,
                                                       ValueType value_type,
                                                       Slice* key,
                                                       Slice* value) {
  Rep* r = rep_.get();
  assert(r->embedded_blob_options);
  assert(key != nullptr);
  assert(value != nullptr);

  if (value_type == kTypeValue) {
    if (value->size() < r->embedded_blob_options->min_blob_size) {
      return true;  // leave small values inline
    }
    Status s = WriteEmbeddedBlobRecord(*value, kNoCompression,
                                       /*record_offset=*/nullptr);
    if (!s.ok()) {
      r->SetStatus(std::move(s));
      return false;
    }
    // Rewrite the internal key footer type to kTypeBlobIndex in scratch (the
    // input key is not mutated in place).
    std::string& key_buf = r->embedded_blob_state->key_buf;
    key_buf.assign(key->data(), key->size());
    assert(key_buf.size() >= kNumInternalBytes);
    EncodeFixed64(&key_buf[key_buf.size() - kNumInternalBytes],
                  PackSequenceAndType(seq, kTypeBlobIndex));
    *key = Slice(key_buf);
    *value = Slice(r->embedded_blob_state->blob_index_buf);
    return true;
  }

  if (value_type == kTypeWideColumnEntity) {
    bool rewritten = false;
    Status s = BuildEmbeddedWideColumnEntity(*value, &rewritten);
    if (!s.ok()) {
      r->SetStatus(std::move(s));
      return false;
    }
    if (rewritten) {
      *value = Slice(r->embedded_blob_state->entity_buf);
    }
    return true;
  }

  return true;
}

Status BlockBasedTableBuilder::InsertBlockInCacheHelper(
    const Slice& block_contents, const BlockHandle* handle,
    BlockType block_type) {
  Cache* block_cache = rep_->table_options.block_cache.get();
  Status s;
  auto helper =
      GetCacheItemHelper(block_type, rep_->ioptions.lowest_used_cache_tier);
  if (block_cache && helper && helper->create_cb) {
    CacheKey key = BlockBasedTable::GetCacheKey(rep_->base_cache_key, *handle);
    size_t charge;
    // NOTE: data blocks (and everything else) will be warmed in decompressed
    // state, so does not need a dictionary-aware decompressor. The only thing
    // needing a decompressor here (in create_context) is warming the
    // (de)compression dictionary, which will clone and save a dict-based
    // decompressor from the corresponding non-dict decompressor.
    s = WarmInCache(block_cache, key.AsSlice(), block_contents,
                    &rep_->create_context, helper,
                    rep_->warm_cache_config.priority, &charge);
    if (LIKELY(s.ok())) {
      BlockBasedTable::UpdateCacheInsertionMetrics(
          block_type, nullptr /*get_context*/, charge, s.IsOkOverwritten(),
          rep_->ioptions.stats);
    } else {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_ADD_FAILURES);
    }
  }
  return s;
}

void BlockBasedTableBuilder::WriteFilterBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (rep_->filter_builder == nullptr || rep_->filter_builder->IsEmpty()) {
    // No filter block needed
    return;
  }
  if (!rep_->last_ikey.empty()) {
    // We might have been using AddWithPrevKey, so need PrevKeyBeforeFinish
    // to be safe. And because we are re-synchronized after buffered/parallel
    // operation, rep_->last_ikey is accurate.
    rep_->filter_builder->PrevKeyBeforeFinish(
        ExtractUserKeyAndStripTimestamp(rep_->last_ikey, rep_->ts_sz));
  }
  BlockHandle filter_block_handle;
  bool is_partitioned_filter = rep_->table_options.partition_filters;
  if (LIKELY(ok())) {
    rep_->props.num_filter_entries +=
        rep_->filter_builder->EstimateEntriesAdded();
    Status s = Status::Incomplete();
    while (LIKELY(ok()) && s.IsIncomplete()) {
      // filter_data is used to store the transferred filter data payload from
      // FilterBlockBuilder and deallocate the payload by going out of scope.
      // Otherwise, the payload will unnecessarily remain until
      // BlockBasedTableBuilder is deallocated.
      //
      // See FilterBlockBuilder::Finish() for more on the difference in
      // transferred filter data payload among different FilterBlockBuilder
      // subtypes.
      std::unique_ptr<const char[]> filter_owner;
      Slice filter_content;
      s = rep_->filter_builder->Finish(filter_block_handle, &filter_content,
                                       &filter_owner);

      assert(s.ok() || s.IsIncomplete() || s.IsCorruption());
      if (s.IsCorruption()) {
        rep_->SetStatus(s);
        break;
      }

      rep_->props.filter_size += filter_content.size();

      BlockType btype = is_partitioned_filter && /* last */ s.ok()
                            ? BlockType::kFilterPartitionIndex
                            : BlockType::kFilter;
      WriteMaybeCompressedBlock(filter_content, kNoCompression,
                                &filter_block_handle, btype);
    }
    rep_->filter_builder->ResetFilterBitsBuilder();
  }
  if (LIKELY(ok())) {
    // Add mapping from "<filter_block_prefix>.Name" to location
    // of filter data.
    std::string key;
    key = is_partitioned_filter ? BlockBasedTable::kPartitionedFilterBlockPrefix
                                : BlockBasedTable::kFullFilterBlockPrefix;
    key.append(rep_->table_options.filter_policy->CompatibilityName());
    meta_index_builder->Add(key, filter_block_handle);
  }
}

// Adapter that bridges built-in index writer callbacks to
// BlockBasedTableBuilder's private
// block-write helpers and the meta-index builder. Defined out-of-line as
// a private nested class so it can access BlockBasedTableBuilder's private
// WriteBlock / WriteMaybeCompressedBlock methods.
class BlockBasedTableBuilder::IndexBlockWriterImpl
    : public BuiltinIndexBlockWriter {
 public:
  IndexBlockWriterImpl(BlockBasedTableBuilder* builder,
                       MetaIndexBuilder* meta_builder, bool compress)
      : builder_(builder), meta_builder_(meta_builder), compress_(compress) {}

  Status WriteBlock(const Slice& contents,
                    IndexFactoryBuilder::BlockHandle* handle,
                    bool compress_this) override {
    BlockHandle internal_handle;
    // Honor the SST setting and the per-block compression request.
    if (compress_ && compress_this) {
      builder_->WriteBlock(contents, &internal_handle, BlockType::kIndex);
    } else {
      builder_->WriteMaybeCompressedBlock(contents, kNoCompression,
                                          &internal_handle, BlockType::kIndex);
    }
    if (!builder_->ok()) {
      return builder_->status();
    }
    handle->offset = internal_handle.offset();
    handle->size = internal_handle.size();
    return Status::OK();
  }

  void AddMetaBlock(const std::string& name,
                    const IndexFactoryBuilder::BlockHandle& handle) override {
    BlockHandle internal_handle;
    internal_handle.set_offset(handle.offset);
    internal_handle.set_size(handle.size);
    meta_builder_->Add(name, internal_handle);
  }

 private:
  BlockBasedTableBuilder* builder_;
  MetaIndexBuilder* meta_builder_;
  bool compress_;
};

void BlockBasedTableBuilder::WriteIndexBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* index_block_handle) {
  if (UNLIKELY(!ok())) {
    return;
  }

  // Use the FinishAndWrite protocol which handles:
  // - Multi-partition writes for partitioned indexes
  // - Auxiliary meta blocks (e.g., hash index prefix blocks)
  // - Single-block writes for simple indexes
  // The writer callback adapts between IndexFactoryBuilder::BlockHandle and
  // the internal BlockHandle.
  bool compress = rep_->table_options.enable_index_compression;

  if (rep_->builtin_index_builder != nullptr) {
    // Normal path: built-in index is present.
    IndexBlockWriterImpl writer(this, meta_index_builder, compress);
    IndexFactoryBuilder::BlockHandle final_handle{0, 0};
    Status s = rep_->builtin_index_builder->FinishAndWrite(
        &writer, &final_handle, compress);
    if (!s.ok()) {
      rep_->SetStatus(s);
      return;
    }
    // Convert the public handle back to internal handle
    index_block_handle->set_offset(final_handle.offset);
    index_block_handle->set_size(final_handle.size);

    if (LIKELY(ok())) {
      // index_size reports logical (uncompressed) top-level index size +
      // trailer, not the on-disk compressed size. Compaction estimators
      // and other callers depend on this contract.
      rep_->props.index_size =
          rep_->builtin_index_builder->IndexSize() + kBlockTrailerSize;
      rep_->props.num_uniform_blocks =
          rep_->builtin_index_builder->NumUniformIndexBlocks();
    }
  } else {
    // index_mode=kCustomOnly: no built-in index builder.
    // Write a minimal empty index block to satisfy the SST footer.
    // Only index_size is set here (it needs the just-written handle); the
    // other stub properties are written by WritePropertiesBlock.
    BlockBuilder empty_index_block(
        1 /* block_restart_interval */, false /* use_delta_encoding */,
        false /* use_value_delta_encoding */,
        BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
        0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
        true /* persist_user_defined_timestamps */, false /* is_user_key */,
        false /* use_separated_kv_storage */, nullptr /* statistics */,
        -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);
    Slice empty_contents = empty_index_block.Finish();
    WriteMaybeCompressedBlock(empty_contents, kNoCompression,
                              index_block_handle, BlockType::kIndex);
    if (LIKELY(ok())) {
      // Stub block is uncompressed: handle.size() is the exact data size.
      rep_->props.index_size = index_block_handle->size() + kBlockTrailerSize;
    }
  }
  // If success and need to record in metaindex rather than footer...
  if (LIKELY(ok()) && !FormatVersionUsesIndexHandleInFooter(
                          rep_->table_options.format_version)) {
    meta_index_builder->Add(kIndexBlockName, *index_block_handle);
  }

  // Custom indexes serialize into one self-contained block. Multi-block
  // writing remains internal to built-in indexes because the public custom
  // reader API receives only this top-level block.
  for (auto& ci : rep_->custom_indexes) {
    if (UNLIKELY(!ok())) {
      break;
    }
    Slice contents;
    Status cs = ci.builder->Finish(&contents);
    if (!cs.ok()) {
      rep_->SetStatus(cs);
      break;
    }
    const BlockBasedTableOptions::IndexMode mode =
        rep_->table_options.GetEffectiveIndexMode();
    if (contents.empty() &&
        mode == BlockBasedTableOptions::IndexMode::kCustomOnly) {
      rep_->SetStatus(Status::InvalidArgument(
          "IndexFactory " + ci.name +
          " returned an empty index in a mode that requires it"));
      break;
    }
    BlockHandle custom_handle;
    WriteMaybeCompressedBlock(contents, kNoCompression, &custom_handle,
                              BlockType::kUserDefinedIndex);
    if (LIKELY(ok())) {
      meta_index_builder->Add(std::string(kIndexFactoryMetaPrefix) + ci.name,
                              custom_handle);
      if (rep_->builtin_index_builder == nullptr) {
        // kCustomOnly wrote a few-byte stub where the standard index goes, so
        // index_size would otherwise read as roughly zero for the one mode
        // where the custom index is the whole index. Add the block that
        // actually serves reads on top of the stub already counted above, so
        // the property covers every index byte in the file. The block is
        // written uncompressed, so handle.size() is the logical size that
        // index_size is contracted to report.
        rep_->props.index_size += custom_handle.size() + kBlockTrailerSize;
      }
    }
  }
}

void BlockBasedTableBuilder::WritePropertiesBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle properties_block_handle;
  if (LIKELY(ok())) {
    PropertyBlockBuilder property_block_builder;
    rep_->props.filter_policy_name =
        rep_->table_options.filter_policy != nullptr
            ? rep_->table_options.filter_policy->Name()
            : "";
    // rep_->props.index_size is set in WriteIndexBlock(), which has direct
    // access to the index block handle (needed for accurate kCustomOnly stub
    // reporting).
    const bool is_blob_relocation_file =
        rep_->blob_relocation_directory_builder != nullptr;
    rep_->props.comparator_name =
        is_blob_relocation_file ? BytewiseComparator()->Name()
                                : (rep_->ioptions.user_comparator != nullptr
                                       ? rep_->ioptions.user_comparator->Name()
                                       : "nullptr");
    rep_->props.merge_operator_name =
        rep_->ioptions.merge_operator != nullptr
            ? rep_->ioptions.merge_operator->Name()
            : "nullptr";
    rep_->props.prefix_extractor_name =
        rep_->prefix_extractor ? rep_->prefix_extractor->AsString() : "nullptr";
    std::string property_collectors_names = "[";
    for (size_t i = 0;
         i < rep_->ioptions.table_properties_collector_factories.size(); ++i) {
      if (i != 0) {
        property_collectors_names += ",";
      }
      property_collectors_names +=
          rep_->ioptions.table_properties_collector_factories[i]->Name();
    }
    property_collectors_names += "]";
    rep_->props.property_collectors_names = property_collectors_names;

    rep_->PostPopulateCompressionProperties();

    if (rep_->builtin_index_builder != nullptr) {
      // Normal path: built-in index is present.
      if (rep_->table_options.index_type ==
          BlockBasedTableOptions::kTwoLevelIndexSearch) {
        rep_->props.index_partitions =
            rep_->builtin_index_builder->NumPartitions();
        rep_->props.top_level_index_size =
            rep_->builtin_index_builder->TopLevelIndexSize(
                rep_->offset.LoadRelaxed());
      }
      rep_->props.index_key_is_user_key =
          !rep_->builtin_index_builder->separator_is_key_plus_seq();
      // kCustomOnly takes the else branch below, so kCustomDefault is the
      // only mode here that makes the custom index primary.
      if (rep_->table_options.GetEffectiveIndexMode() ==
              BlockBasedTableOptions::IndexMode::kCustomDefault &&
          rep_->table_options.user_defined_index_factory != nullptr) {
        rep_->props.udi_is_primary_index = 1;
      }
      // The standard index is always fully populated (even in primary mode),
      // so delta encoding applies normally.
      rep_->props.index_value_is_delta_encoded =
          rep_->use_delta_encoding_for_index_values;
    } else {
      // kCustomOnly stub properties; index_size is written by
      // WriteIndexBlock (it needs the just-written block handle).
      rep_->props.index_key_is_user_key = 1;
      rep_->props.udi_is_primary_index = 1;
      rep_->props.standard_index_is_stub = 1;
    }
    if (rep_->sampled_input_data_bytes.LoadRelaxed() > 0) {
      rep_->props.slow_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(
              rep_->sampled_output_slow_data_bytes.LoadRelaxed()) /
              rep_->sampled_input_data_bytes.LoadRelaxed() *
              rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed() + 0.5);
      rep_->props.fast_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(
              rep_->sampled_output_fast_data_bytes.LoadRelaxed()) /
              rep_->sampled_input_data_bytes.LoadRelaxed() *
              rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed() + 0.5);
    } else if (rep_->sample_for_compression > 0) {
      // We tried to sample but none were found. Assume worst-case
      // (compression ratio 1.0) so data is complete and aggregatable.
      rep_->props.slow_compression_estimated_data_size =
          rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed();
      rep_->props.fast_compression_estimated_data_size =
          rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed();
    }
    rep_->props.user_defined_timestamps_persisted =
        is_blob_relocation_file ? false : rep_->persist_user_defined_timestamps;

    if (rep_->embedded_blob_state) {
      std::string encoded_stats;
      EncodeEmbeddedBlobStats(rep_->embedded_blob_state->stats, &encoded_stats);
      rep_->props.user_collected_properties[kEmbeddedBlobSstStatsPropertyName] =
          std::move(encoded_stats);
    }

    rep_->props.num_data_blocks_compression_rejected =
        rep_->num_data_blocks_compression_rejected.LoadRelaxed();
    rep_->props.num_data_blocks_compression_bypassed =
        rep_->num_data_blocks_compression_bypassed.LoadRelaxed();

    assert(IsEmpty() || rep_->props.key_largest_seqno != UINT64_MAX);
    // Add basic properties
    property_block_builder.AddTableProperty(rep_->props);

    // Add use collected properties
    NotifyCollectTableCollectorsOnFinish(
        rep_->table_properties_collectors, rep_->ioptions.logger,
        &property_block_builder, rep_->props.user_collected_properties,
        rep_->props.readable_properties);

    Slice block_data = property_block_builder.Finish();
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:BlockData", &block_data);
    WriteMaybeCompressedBlock(block_data, kNoCompression,
                              &properties_block_handle, BlockType::kProperties);
  }
  if (LIKELY(ok())) {
#ifndef NDEBUG
    {
      uint64_t props_block_offset = properties_block_handle.offset();
      uint64_t props_block_size = properties_block_handle.size();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
          &props_block_offset);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
          &props_block_size);
    }
#endif  // !NDEBUG

    const std::string* properties_block_meta = &kPropertiesBlockName;
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:Meta",
        &properties_block_meta);
    meta_index_builder->Add(*properties_block_meta, properties_block_handle);
  }
}

void BlockBasedTableBuilder::WriteBlobReferenceMetadata(
    MetaIndexBuilder* meta_index_builder) {
  auto* const blob_reference_collector = rep_->blob_reference_collector.get();
  if (blob_reference_collector == nullptr) {
    return;
  }
  auto& collector = *blob_reference_collector;
  if (meta_index_builder == nullptr) {
    rep_->SetStatus(Status::InvalidArgument("Null metadata index builder"));
    collector.ReleaseWorkingMemory().PermitUncheckedError();
    return;
  }

  const auto release_working_memory = [&]() {
    Status release_status = collector.ReleaseWorkingMemory();
    if (!release_status.ok()) {
      rep_->SetStatus(release_status);
    }
  };
  if (!ok()) {
    release_working_memory();
    return;
  }

  Status status =
      collector.Finish([&](const Slice& block, BlockHandle* handle) -> Status {
        WriteMaybeCompressedBlock(block, kNoCompression, handle,
                                  BlockType::kUserDefinedIndex);
        return rep_->GetStatus();
      });
  if (!status.ok()) {
    rep_->SetStatus(status);
    release_working_memory();
    return;
  }

  BlockHandle index_handle;
  WriteMaybeCompressedBlock(collector.index_block(), kNoCompression,
                            &index_handle, BlockType::kUserDefinedIndex);
  if (!ok()) {
    release_working_memory();
    return;
  }
  meta_index_builder->Add(kBlobReferenceIndexBlockName, index_handle);

  BlockHandle directory_handle;
  WriteMaybeCompressedBlock(collector.directory_block(), kNoCompression,
                            &directory_handle, BlockType::kUserDefinedIndex);
  if (!ok()) {
    release_working_memory();
    return;
  }
  meta_index_builder->Add(kBlobReferenceDirectoryBlockName, directory_handle);

  auto* descriptor = collector.mutable_descriptor();
  descriptor->index_handle = index_handle;
  descriptor->directory_handle = directory_handle;
  std::string encoded_descriptor;
  descriptor->EncodeTo(&encoded_descriptor);
  const auto [unused, inserted] = rep_->props.user_collected_properties.emplace(
      kBlobReferenceDescriptorPropertyName, std::move(encoded_descriptor));
  (void)unused;
  if (!inserted) {
    rep_->SetStatus(Status::Corruption(
        "Duplicate blob reference descriptor table property"));
  }
  if (ok()) {
    status = collector.TakeSstSummary(&rep_->blob_reference_summary);
    if (!status.ok()) {
      rep_->SetStatus(status);
    }
  } else {
    release_working_memory();
  }
}

std::shared_ptr<const BlobReferenceSstSummary>
BlockBasedTableBuilder::GetBlobReferenceSstSummary() const {
  return ok() ? rep_->blob_reference_summary : nullptr;
}

void BlockBasedTableBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder) {
  Slice compression_dict;
  if (rep_->data_block_compressor) {
    compression_dict = rep_->data_block_compressor->GetSerializedDict();
  }
  if (!compression_dict.empty()) {
    BlockHandle compression_dict_block_handle;
    if (LIKELY(ok())) {
      WriteMaybeCompressedBlock(compression_dict, kNoCompression,
                                &compression_dict_block_handle,
                                BlockType::kCompressionDictionary);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
          &compression_dict);
    }
    if (LIKELY(ok())) {
      meta_index_builder->Add(kCompressionDictBlockName,
                              compression_dict_block_handle);
    }
  }
}

void BlockBasedTableBuilder::WriteRangeDelBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (LIKELY(ok()) && !rep_->range_del_block.empty()) {
    BlockHandle range_del_block_handle;
    WriteMaybeCompressedBlock(rep_->range_del_block.Finish(), kNoCompression,
                              &range_del_block_handle,
                              BlockType::kRangeDeletion);
    meta_index_builder->Add(kRangeDelBlockName, range_del_block_handle);
  }
}

void BlockBasedTableBuilder::WriteFooter(BlockHandle& metaindex_block_handle,
                                         BlockHandle& index_block_handle) {
  assert(LIKELY(ok()));
  Rep* r = rep_.get();
  IOOptions io_options;
  IOStatus ios =
      WritableFileWriter::PrepareIOOptions(r->write_options, io_options);
  if (!ios.ok()) {
    r->SetIOStatus(ios);
    return;
  }
  // TEST-only hook: inject a gap of the requested number of bytes between the
  // metaindex block and the footer, to exercise format_version >= 8 footer
  // metaindex-gap decoding. The gap is reserved for future file checksum data;
  // no production writer emits one yet. Compiled out in release builds.
  size_t footer_gap = 0;
  TEST_SYNC_POINT_CALLBACK("BlockBasedTableBuilder::WriteFooter:MetaindexGap",
                           &footer_gap);
  if (footer_gap > 0) {
    std::string gap_bytes(footer_gap, '\0');
    NotifyFileAppendForTest(Slice(gap_bytes), nullptr);
    ios = r->file->Append(Slice(gap_bytes), io_options);
    if (!ios.ok()) {
      r->SetIOStatus(ios);
      return;
    }
    r->pre_compression_size += footer_gap;
    r->set_offset(r->get_offset() + footer_gap);
  }
  FooterBuilder footer;
  uint64_t incompatible_features =
      r->table_options.GetEffectiveIndexMode() ==
              BlockBasedTableOptions::IndexMode::kCustomOnly
          ? kFooterFeatureRequireUserDefinedIndex
          : 0;
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::WriteFooter:IncompatibleFeatures",
      &incompatible_features);
  Status s = footer.Build(
      kBlockBasedTableMagicNumber, r->table_options.format_version,
      r->get_offset(), r->table_options.checksum, metaindex_block_handle,
      index_block_handle, r->base_context_checksum, incompatible_features);
  if (!s.ok()) {
    r->SetStatus(s);
    return;
  }
  NotifyFileAppendForTest(footer.GetSlice(), nullptr);
  ios = r->file->Append(footer.GetSlice(), io_options);
  if (ios.ok()) {
    r->pre_compression_size += footer.GetSlice().size();
    r->set_offset(r->get_offset() + footer.GetSlice().size());
  } else {
    r->SetIOStatus(ios);
  }
}

void BlockBasedTableBuilder::MaybeEnterUnbuffered(
    const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kBuffered);
  // Don't yet enter unbuffered (early return) if none of the conditions are
  // met
  if (first_key_in_next_block != nullptr) {
    bool exceeds_buffer_limit =
        (r->buffer_limit != 0 && r->data_begin_offset > r->buffer_limit);
    if (!exceeds_buffer_limit) {
      bool exceeds_global_block_cache_limit = false;
      // Increase cache charging for the last buffered data block
      // only if the block is not going to be unbuffered immediately
      // and there exists a cache reservation manager
      if (r->compression_dict_buffer_cache_res_mgr != nullptr) {
        Status s =
            r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
                r->data_begin_offset);
        exceeds_global_block_cache_limit = s.IsMemoryLimit();
      }
      if (!exceeds_global_block_cache_limit) {
        return;
      }
    }
  }

  // Enter Unbuffered state
  r->state = Rep::State::kUnbuffered;
  const size_t kNumBlocksBuffered = r->data_block_buffers.size();
  if (kNumBlocksBuffered == 0) {
    // The below code is neither safe nor necessary for handling zero data
    // blocks.
    // For PostPopulateCompressionProperties()
    assert(!r->data_block_compressor);
    r->data_block_compressor = r->basic_compressor.get();
    r->AutoSkipSeed();
    return;
  }

  // Abstract algebra teaches us that a finite cyclic group (such as the
  // additive group of integers modulo N) can be generated by a number that is
  // coprime with N. Since N is variable (number of buffered data blocks), we
  // must then pick a prime number in order to guarantee coprimeness with any
  // N.
  //
  // One downside of this approach is the spread will be poor when
  // `kPrimeGeneratorRemainder` is close to zero or close to
  // `kNumBlocksBuffered`.
  //
  // Picked a random number between one and one trillion and then chose the
  // next prime number greater than or equal to it.
  const uint64_t kPrimeGenerator = 545055921143ull;
  // Can avoid repeated division by just adding the remainder repeatedly.
  const size_t kPrimeGeneratorRemainder = static_cast<size_t>(
      kPrimeGenerator % static_cast<uint64_t>(kNumBlocksBuffered));
  const size_t kInitSampleIdx = kNumBlocksBuffered / 2;

  Compressor::DictSamples samples;
  size_t buffer_idx = kInitSampleIdx;
  // Get max_sample_bytes from the DictSampling guidance
  auto* sampling =
      std::get_if<Compressor::DictSampling>(&r->data_block_dict_guidance);
  assert(sampling != nullptr);
  size_t max_sample_bytes = sampling->max_sample_bytes;
  for (size_t i = 0;
       i < kNumBlocksBuffered && samples.sample_data.size() < max_sample_bytes;
       ++i) {
    size_t copy_len = std::min(max_sample_bytes - samples.sample_data.size(),
                               r->data_block_buffers[buffer_idx].size());
    samples.sample_data.append(r->data_block_buffers[buffer_idx], 0, copy_len);
    samples.sample_lens.emplace_back(copy_len);

    buffer_idx += kPrimeGeneratorRemainder;
    if (buffer_idx >= kNumBlocksBuffered) {
      buffer_idx -= kNumBlocksBuffered;
    }
  }

  assert(samples.sample_data.size() > 0);

  // final sample data block flushed, now we can generate dictionary (or it
  // might opt not to use a dictionary and that's ok)
  r->data_block_compressor =
      MaybeCloneSpecialized(r->basic_compressor.get(),
                            CacheEntryRole::kDataBlock, std::move(samples));
  r->AutoSkipSeed();

  Slice serialized_dict = r->data_block_compressor->GetSerializedDict();
  if (r->verify_decompressor) {
    if (serialized_dict.empty()) {
      // No dictionary
      r->data_block_verify_decompressor = r->verify_decompressor.get();
    } else {
      // Get an updated dictionary-aware decompressor for verification.
      Status s = r->verify_decompressor->MaybeCloneForDict(
          serialized_dict, &r->verify_decompressor_with_dict);
      // Dictionary support must be present on the decompressor side if it's
      // on the compressor side.
      assert(r->verify_decompressor_with_dict);
      if (r->verify_decompressor_with_dict) {
        r->data_block_verify_decompressor =
            r->verify_decompressor_with_dict.get();
        assert(s.ok());
      } else {
        assert(!s.ok());
        r->SetStatus(s);
      }
    }
  }

  auto get_iterator_for_block = [&r](size_t i) {
    auto& data_block = r->data_block_buffers[i];
    assert(!data_block.empty());

    Block reader{
        BlockContents{data_block}, 0 /* read_amp_bytes_per_bit */,
        nullptr /* statistics */,
        static_cast<uint32_t>(r->table_options.block_restart_interval)};
    DataBlockIter* iter = reader.NewDataIterator(
        r->internal_comparator.user_comparator(), kDisableGlobalSequenceNumber,
        nullptr /* iter */, nullptr /* stats */,
        false /*  block_contents_pinned */, r->persist_user_defined_timestamps);

    iter->SeekToFirst();
    assert(iter->Valid());
    return std::unique_ptr<DataBlockIter>(iter);
  };

  std::unique_ptr<DataBlockIter> iter = nullptr;
  std::unique_ptr<DataBlockIter> next_block_iter = nullptr;

  for (size_t i = 0; ok() && i < r->data_block_buffers.size(); ++i) {
    if (iter == nullptr) {
      iter = get_iterator_for_block(i);
      assert(iter != nullptr);
    };

    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (r->filter_builder != nullptr) {
        // NOTE: AddWithPrevKey here would only save key copying if prev is
        // pinned (iter->IsKeyPinned()), which is probably rare with delta
        // encoding. OK to go from Add() here to AddWithPrevKey() in
        // unbuffered operation.
        r->filter_builder->Add(ExtractUserKeyAndStripTimestamp(key, r->ts_sz));
      }
      r->ForwardOnKeyAddedToAll(key, iter->value());
    }

    Slice first_key_in_loop_next_block;
    const Slice* first_key_in_loop_next_block_ptr;
    if (i + 1 < r->data_block_buffers.size()) {
      next_block_iter = get_iterator_for_block(i + 1);
      first_key_in_loop_next_block = next_block_iter->key();
      first_key_in_loop_next_block_ptr = &first_key_in_loop_next_block;
    } else {
      first_key_in_loop_next_block_ptr = first_key_in_next_block;
    }

    auto& data_block = r->data_block_buffers[i];
    iter->SeekToLast();
    assert(iter->Valid());
    if (r->IsParallelCompressionActive()) {
      EmitBlockForParallel(data_block, iter->key(),
                           first_key_in_loop_next_block_ptr);

    } else {
      EmitBlock(data_block, iter->key(), first_key_in_loop_next_block_ptr);
    }
    std::swap(iter, next_block_iter);
  }
  r->data_block_buffers.clear();
  r->data_begin_offset = 0;
  // Release all reserved cache for data block buffers
  if (r->compression_dict_buffer_cache_res_mgr != nullptr) {
    Status s = r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
        r->data_begin_offset);
    s.PermitUncheckedError();
  }
}

Status BlockBasedTableBuilder::Finish() {
  Rep* r = rep_.get();
  assert(r->state != Rep::State::kClosed);

#ifndef NDEBUG
  {
    // This sync point callback is a simple approximation of a failure detected
    // in parallel compression after the start of calling Finish() but before
    // Finish() calls Flush()
    IOStatus s = rep_->GetIOStatus();
    TEST_SYNC_POINT_CALLBACK("BlockBasedTableBuilder::Finish:ParallelIOStatus",
                             &s);
    if (!s.ok()) {
      rep_->SetIOStatus(s);
    }
  }
#endif  // !NDEBUG
  // To make sure properties block is able to keep the accurate size of index
  // block, we will finish writing all index entries first, in Flush().
  Flush(/*first_key_in_next_block=*/nullptr);
  if (rep_->state == Rep::State::kBuffered) {
    MaybeEnterUnbuffered(nullptr);
  }
  assert(r->state == Rep::State::kUnbuffered);
  if (r->IsParallelCompressionActive()) {
    StopParallelCompression(/*abort=*/false);
  }

  // Persist the AutoSkip estimate for the next file this thread emits (workers,
  // if any, have joined so the estimate is now quiescent).
  r->AutoSkipWriteBack();

  if (ok() && r->blob_relocation_directory_builder != nullptr) {
    BlobRelocationDirectory directory;
    Status directory_status =
        r->blob_relocation_directory_builder->Finish(&directory);
    if (directory_status.ok()) {
      std::string encoded_directory;
      directory.EncodeTo(&encoded_directory);
      r->blob_relocation_file_metadata = BlobRelocationFileMetadata{
          directory.directory_digest(),
          static_cast<uint64_t>(directory.origins().size()),
          directory.total_entries(), directory.total_record_bytes(),
          encoded_directory};
      // Finish() keeps the Rep alive until this method returns;
      // NULLSAFECLANG cannot carry that ownership invariant through `r`.
      // @lint-ignore NULLSAFECLANG nullable-dereference
      r->props.user_collected_properties[kBlobRelocationDirectoryPropertyName] =
          std::move(encoded_directory);
    } else {
      r->SetStatus(std::move(directory_status));
    }
  }

  // These GC-only blocks remain addressable through the metaindex, but do not
  // belong to the ordinary open-time metadata prefetch tail.
  MetaIndexBuilder meta_index_builder;
  WriteBlobReferenceMetadata(&meta_index_builder);
  r->props.tail_start_offset = r->offset.LoadRelaxed();

  uint64_t last_estimated_tail_size = EstimatedTailSize();

  // Write meta blocks, metaindex block and footer in the following order.
  //    1. [meta block: filter]
  //    2. [meta block: index]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: properties]
  //    6. [metaindex block]
  //    7. Footer
  BlockHandle metaindex_block_handle;
  BlockHandle index_block_handle;
  WriteFilterBlock(&meta_index_builder);
  WriteIndexBlock(&meta_index_builder, &index_block_handle);
  WriteCompressionDictBlock(&meta_index_builder);
  WriteRangeDelBlock(&meta_index_builder);
  WritePropertiesBlock(&meta_index_builder);
  if (LIKELY(ok())) {
    // flush the meta index block
    WriteMaybeCompressedBlock(meta_index_builder.Finish(), kNoCompression,
                              &metaindex_block_handle, BlockType::kMetaIndex);
  }
  if (LIKELY(ok())) {
    WriteFooter(metaindex_block_handle, index_block_handle);
  }
  r->state = Rep::State::kClosed;
  r->tail_size = r->offset.LoadRelaxed() - r->props.tail_start_offset;

  // Assert tail size estimation is an overestimate only when tail size
  // estimation option is enabled for compaction files with supported
  // index/filter types:
  // - Shortened indexes (kBinarySearch, kBinarySearchWithFirstKey)
  // - Partitioned indexes (kTwoLevelIndexSearch)
  // - Full filters
  // - Partitioned filters
  if (r->target_file_size_is_upper_bound &&
      r->reason == TableFileCreationReason::kCompaction &&
      r->table_options.index_type != BlockBasedTableOptions::kHashSearch) {
    ROCKS_LOG_WARN(r->ioptions.info_log,
                   "File number: %" PRIu64 ", Estimated tail size = %" PRIu64
                   " bytes, Actual tail size = %" PRIu64 " bytes",
                   r->props.orig_file_number, last_estimated_tail_size,
                   r->tail_size);
    assert(r->tail_size <= last_estimated_tail_size);
  }

  return r->GetStatus();
}

void BlockBasedTableBuilder::Abandon() {
  assert(rep_->state != Rep::State::kClosed);
  if (rep_->IsParallelCompressionActive()) {
    StopParallelCompression(/*abort=*/true);
  }
  if (rep_->blob_reference_collector) {
    rep_->blob_reference_collector->ReleaseWorkingMemory()
        .PermitUncheckedError();
  }
  rep_->state = Rep::State::kClosed;
  rep_->GetIOStatus().PermitUncheckedError();
}

#ifndef NDEBUG
void BlockBasedTableBuilder::TEST_InjectIOError() {
  rep_->SetIOStatus(IOStatus::IOError("Injected IOError for testing"));
}
#endif  // !NDEBUG

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

bool BlockBasedTableBuilder::IsEmpty() const {
  return rep_->props.num_entries == 0 && rep_->props.num_range_deletions == 0;
}

uint64_t BlockBasedTableBuilder::PreCompressionSize() const {
  return rep_->pre_compression_size;
}

uint64_t BlockBasedTableBuilder::FileSize() const {
  return rep_->offset.LoadRelaxed();
}

uint64_t BlockBasedTableBuilder::EstimatedFileSize() const {
  if (rep_->IsParallelCompressionActive()) {
    // Use upper bound on "inflight" data size to estimate
    return FileSize() + rep_->pc_rep->estimated_inflight_size.LoadRelaxed();
  } else {
    return FileSize();
  }
}

uint64_t BlockBasedTableBuilder::EstimatedTailSize() const {
  uint64_t estimated_tail_size = 0;

  // 1. Estimate index size (built-in + custom indexes)
  if (rep_->builtin_index_builder != nullptr) {
    estimated_tail_size +=
        rep_->builtin_index_builder->CurrentIndexSizeEstimate();
  }
  for (const auto& ci : rep_->custom_indexes) {
    estimated_tail_size += ci.builder->EstimatedSize();
  }

  // 2. Estimate filter size
  if (rep_->filter_builder) {
    estimated_tail_size += rep_->filter_builder->CurrentFilterSizeEstimate();
  }

  // 3. Estimate compression dictionary size
  if (rep_->data_block_compressor) {
    Slice dict = rep_->data_block_compressor->GetSerializedDict();
    if (!dict.empty()) {
      estimated_tail_size += dict.size();
    }
  }

  // 4. Estimate range deletion block size
  if (!rep_->range_del_block.empty()) {
    estimated_tail_size += rep_->range_del_block.CurrentSizeEstimate();
  }

  // 5. Conservatively bound exact external-blob reference metadata.
  if (rep_->blob_reference_collector) {
    estimated_tail_size +=
        rep_->blob_reference_collector->EstimatedEncodedSize();
  }

  // 6. Estimate properties block size conservatively (~1-2KB)
  estimated_tail_size += 2048;

  // 7. Estimate meta-index block size conservatively (~1KB)
  estimated_tail_size += 1024;

  // 8. Add footer size
  estimated_tail_size += Footer::kMaxEncodedLength;

  return estimated_tail_size;
}

uint64_t BlockBasedTableBuilder::GetTailSize() const { return rep_->tail_size; }

bool BlockBasedTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties BlockBasedTableBuilder::GetTableProperties() const {
  return rep_->props;
}

std::string BlockBasedTableBuilder::GetFileChecksum() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* BlockBasedTableBuilder::GetFileChecksumFuncName() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}
void BlockBasedTableBuilder::SetSeqnoTimeTableProperties(
    const SeqnoToTimeMapping& relevant_mapping, uint64_t oldest_ancestor_time) {
  assert(rep_->props.seqno_to_time_mapping.empty());
  relevant_mapping.EncodeTo(rep_->props.seqno_to_time_mapping);
  rep_->props.oldest_ancestor_time = oldest_ancestor_time;
}

const std::string BlockBasedTable::kObsoleteFilterBlockPrefix = "filter.";
const std::string BlockBasedTable::kFullFilterBlockPrefix = "fullfilter.";
const std::string BlockBasedTable::kPartitionedFilterBlockPrefix =
    "partitionedfilter.";

}  // namespace ROCKSDB_NAMESPACE
