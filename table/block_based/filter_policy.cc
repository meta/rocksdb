//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/filter_policy.h"

#include <array>
#include <atomic>
#include <climits>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>

#include "cache/cache_reservation_manager.h"
#include "logging/logging.h"
#include "port/lang.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "util/atomic.h"
#include "util/bloom_impl.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/ribbon_config.h"
#include "util/ribbon_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Metadata trailer size for built-in filters. (This is separate from
// block-based table block trailer.)
//
// Originally this was 1 byte for num_probes and 4 bytes for number of
// cache lines in the Bloom filter, but now the first trailer byte is
// usually an implementation marker and remaining 4 bytes have various
// meanings.
static constexpr uint32_t kMetadataLen = 5;

Slice FinishAlwaysFalse(std::unique_ptr<const char[]>* /*buf*/) {
  // Missing metadata, treated as zero entries
  return Slice(nullptr, 0);
}

Slice FinishAlwaysTrue(std::unique_ptr<const char[]>* /*buf*/) {
  return Slice("\0\0\0\0\0\0", 6);
}

// Base class for filter builders using the XXH3 preview hash,
// also known as Hash64 or GetSliceHash64.
class XXPH3FilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit XXPH3FilterBitsBuilder(
      std::atomic<int64_t>* aggregate_rounding_balance,
      std::shared_ptr<CacheReservationManager> cache_res_mgr,
      bool detect_filter_construct_corruption)
      : aggregate_rounding_balance_(aggregate_rounding_balance),
        cache_res_mgr_(cache_res_mgr),
        detect_filter_construct_corruption_(
            detect_filter_construct_corruption) {}

  ~XXPH3FilterBitsBuilder() override = default;

  void AddKey(const Slice& key) override {
    uint64_t hash = GetSliceHash64(key);
    // Especially with prefixes, it is common to have repetition,
    // though only adjacent repetition, which we want to immediately
    // recognize and collapse for estimating true filter space
    // requirements.
    if (hash_entries_info_.entries.empty() ||
        hash != hash_entries_info_.entries.back()) {
      AddHash(hash);
    }
  }

  void AddKeyAndAlt(const Slice& key, const Slice& alt) override {
    uint64_t key_hash = GetSliceHash64(key);
    uint64_t alt_hash = GetSliceHash64(alt);
    std::optional<uint64_t> prev_key_hash;
    std::optional<uint64_t> prev_alt_hash = hash_entries_info_.prev_alt_hash;

    if (!hash_entries_info_.entries.empty()) {
      prev_key_hash = hash_entries_info_.entries.back();
    }

#ifdef ROCKSDB_VALGRIND_RUN
    // Valgrind can report uninitialized FPs on std::optional usage. See e.g.
    // https://stackoverflow.com/q/51616179
    if (!prev_key_hash.has_value()) {
      std::memset((void*)&prev_key_hash, 0, sizeof(prev_key_hash));
      prev_key_hash.reset();
    }
    if (!prev_alt_hash.has_value()) {
      std::memset((void*)&prev_alt_hash, 0, sizeof(prev_key_hash));
      prev_alt_hash.reset();
    }
#endif

    // Add alt first, so that entries.back() always contains previous key
    // ASSUMING a change from one alt to the next implies a change to
    // corresponding key
    if (alt_hash != prev_alt_hash && alt_hash != key_hash &&
        alt_hash != prev_key_hash) {
      AddHash(alt_hash);
    }
    // Overwrite prev_alt_hash for cases like alt_hash == key_hash
    hash_entries_info_.prev_alt_hash = alt_hash;
    // NOTE: checking key_hash != prev_alt_hash for cases like
    // key == prefix(key) at the end of a prefix grouping as in reverse
    // byte-wise comparator
    if (key_hash != prev_key_hash && key_hash != prev_alt_hash) {
      AddHash(key_hash);
    }
  }

  // Returns an estimate of the number of entries added to the
  // filter. This method is thread-safe and can be safely called
  // from background threads during parallel compression.
  size_t EstimateEntriesAdded() override {
    return hash_entries_info_.entries_count.LoadRelaxed();
  }

  Status MaybePostVerify(const Slice& filter_content) override;

 protected:
  static constexpr uint32_t kMetadataLen = 5;

  // Number of hash entries to accumulate before charging their memory usage to
  // the cache when cache charging is available
  static const std::size_t kUint64tHashEntryCacheResBucketSize =
      CacheReservationManagerImpl<
          CacheEntryRole::kFilterConstruction>::GetDummyEntrySize() /
      sizeof(uint64_t);

  void AddHash(uint64_t hash) {
    if (detect_filter_construct_corruption_) {
      hash_entries_info_.xor_checksum ^= hash;
    }
    hash_entries_info_.entries.push_back(hash);
    hash_entries_info_.entries_count.FetchAddRelaxed(1);
    if (cache_res_mgr_ &&
        // Traditional rounding to whole bucket size
        ((hash_entries_info_.entries.size() %
          kUint64tHashEntryCacheResBucketSize) ==
         kUint64tHashEntryCacheResBucketSize / 2)) {
      hash_entries_info_.cache_res_bucket_handles.emplace_back(nullptr);
      Status s = cache_res_mgr_->MakeCacheReservation(
          kUint64tHashEntryCacheResBucketSize * sizeof(hash),
          &hash_entries_info_.cache_res_bucket_handles.back());
      s.PermitUncheckedError();
    }
  }

  // For delegating between XXPH3FilterBitsBuilders
  void SwapEntriesWith(XXPH3FilterBitsBuilder* other) {
    assert(other != nullptr);
    hash_entries_info_.Swap(&(other->hash_entries_info_));
  }

  void ResetEntries() { hash_entries_info_.Reset(); }

  virtual size_t RoundDownUsableSpace(size_t available_size) = 0;

  // To choose size using malloc_usable_size, we have to actually allocate.
  size_t AllocateMaybeRounding(size_t target_len_with_metadata,
                               size_t num_entries,
                               std::unique_ptr<char[]>* buf) {
    // Return value set to a default; overwritten in some cases
    size_t rv = target_len_with_metadata;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    if (aggregate_rounding_balance_ != nullptr) {
      // Do optimize_filters_for_memory, using malloc_usable_size.
      // Approach: try to keep FP rate balance better than or on
      // target (negative aggregate_rounding_balance_). We can then select a
      // lower bound filter size (within reasonable limits) that gets us as
      // close to on target as possible. We request allocation for that filter
      // size and use malloc_usable_size to "round up" to the actual
      // allocation size.

      // Although it can be considered bad practice to use malloc_usable_size
      // to access an object beyond its original size, this approach should be
      // quite general: working for all allocators that properly support
      // malloc_usable_size.

      // Race condition on balance is OK because it can only cause temporary
      // skew in rounding up vs. rounding down, as long as updates are atomic
      // and relative.
      int64_t balance = aggregate_rounding_balance_->load();

      double target_fp_rate =
          EstimatedFpRate(num_entries, target_len_with_metadata);
      double rv_fp_rate = target_fp_rate;

      if (balance < 0) {
        // See formula for BloomFilterPolicy::aggregate_rounding_balance_
        double for_balance_fp_rate =
            -balance / double{0x100000000} + target_fp_rate;

        // To simplify, we just try a few modified smaller sizes. This also
        // caps how much we vary filter size vs. target, to avoid outlier
        // behavior from excessive variance.
        size_t target_len = target_len_with_metadata - kMetadataLen;
        assert(target_len < target_len_with_metadata);  // check underflow
        for (uint64_t maybe_len_rough :
             {uint64_t{3} * target_len / 4, uint64_t{13} * target_len / 16,
              uint64_t{7} * target_len / 8, uint64_t{15} * target_len / 16}) {
          size_t maybe_len_with_metadata =
              RoundDownUsableSpace(maybe_len_rough + kMetadataLen);
          double maybe_fp_rate =
              EstimatedFpRate(num_entries, maybe_len_with_metadata);
          if (maybe_fp_rate <= for_balance_fp_rate) {
            rv = maybe_len_with_metadata;
            rv_fp_rate = maybe_fp_rate;
            break;
          }
        }
      }

      // Filter blocks are loaded into block cache with their block trailer.
      // We need to make sure that's accounted for in choosing a
      // fragmentation-friendly size.
      const size_t kExtraPadding = BlockBasedTable::kBlockTrailerSize;
      size_t requested = rv + kExtraPadding;

      // Allocate and get usable size
      buf->reset(new char[requested]);
      size_t usable = malloc_usable_size(buf->get());

      if (usable - usable / 4 > requested) {
        // Ratio greater than 4/3 is too much for utilizing, if it's
        // not a buggy or mislinked malloc_usable_size implementation.
        // Non-linearity of FP rates with bits/key means rapidly
        // diminishing returns in overall accuracy for additional
        // storage on disk.
        // Nothing to do, except assert that the result is accurate about
        // the usable size. (Assignment never used.)
        assert(((*buf)[usable - 1] = 'x'));
      } else if (usable > requested) {
        rv = RoundDownUsableSpace(usable - kExtraPadding);
        assert(rv <= usable - kExtraPadding);
        rv_fp_rate = EstimatedFpRate(num_entries, rv);
      } else {
        // Too small means bad malloc_usable_size
        assert(usable == requested);
      }
      memset(buf->get(), 0, rv);

      // Update balance
      int64_t diff = static_cast<int64_t>((rv_fp_rate - target_fp_rate) *
                                          double{0x100000000});
      *aggregate_rounding_balance_ += diff;
    } else {
      buf->reset(new char[rv]());
    }
#else
    (void)num_entries;
    buf->reset(new char[rv]());
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return rv;
  }

  // TODO: Ideally we want to verify the hash entry
  // as it is added to the filter and eliminate this function
  // for speeding up and leaving fewer spaces for undetected memory/CPU
  // corruption. For Ribbon Filter, it's bit harder.
  // Possible solution:
  // pass a custom iterator that tracks the xor checksum as
  // it iterates to ResetAndFindSeedToSolve
  Status MaybeVerifyHashEntriesChecksum() {
    if (!detect_filter_construct_corruption_) {
      return Status::OK();
    }

    uint64_t actual_hash_entries_xor_checksum = 0;
    for (uint64_t h : hash_entries_info_.entries) {
      actual_hash_entries_xor_checksum ^= h;
    }

    if (actual_hash_entries_xor_checksum == hash_entries_info_.xor_checksum) {
      return Status::OK();
    } else {
      // Since these hash entries are corrupted and they will not be used
      // anymore, we can reset them and release memory.
      ResetEntries();
      return Status::Corruption("Filter's hash entries checksum mismatched");
    }
  }

  // See BloomFilterPolicy::aggregate_rounding_balance_. If nullptr,
  // always "round up" like historic behavior.
  std::atomic<int64_t>* aggregate_rounding_balance_;

  // For reserving memory used in (new) Bloom and Ribbon Filter construction
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;

  // For managing cache charge for final filter in (new) Bloom and Ribbon
  // Filter construction
  std::deque<std::unique_ptr<CacheReservationManager::CacheReservationHandle>>
      final_filter_cache_res_handles_;

  bool detect_filter_construct_corruption_;

  struct HashEntriesInfo {
    // A deque avoids unnecessary copying of already-saved values
    // and has near-minimal peak memory use.
    std::deque<uint64_t> entries;

    // Tracks the number of entries added for thread-safe
    // size estimation.
    RelaxedAtomic<size_t> entries_count{0};

    // If cache_res_mgr_ != nullptr,
    // it manages cache charge for buckets of hash entries in (new) Bloom
    // or Ribbon Filter construction.
    // Otherwise, it is empty.
    std::deque<std::unique_ptr<CacheReservationManager::CacheReservationHandle>>
        cache_res_bucket_handles;

    // If detect_filter_construct_corruption_ == true,
    // it records the xor checksum of hash entries.
    // Otherwise, it is 0.
    uint64_t xor_checksum = 0;

    // A single-element cache to help AddKeyAndAlt
    std::optional<uint64_t> prev_alt_hash;

    void Swap(HashEntriesInfo* other) {
      assert(other != nullptr);
      std::swap(entries, other->entries);
      entries_count.StoreRelaxed(
          other->entries_count.ExchangeRelaxed(entries_count.LoadRelaxed()));
      std::swap(cache_res_bucket_handles, other->cache_res_bucket_handles);
      std::swap(xor_checksum, other->xor_checksum);
      std::swap(prev_alt_hash, other->prev_alt_hash);
    }

    void Reset() {
      entries.clear();
      entries_count.StoreRelaxed(0);
      cache_res_bucket_handles.clear();
      xor_checksum = 0;
      prev_alt_hash = {};
    }
  };

  HashEntriesInfo hash_entries_info_;
};

// #################### FastLocalBloom implementation ################## //
// ############## also known as format_version=5 Bloom filter ########## //

// See description in FastLocalBloomImpl
class FastLocalBloomBitsBuilder : public XXPH3FilterBitsBuilder {
 public:
  // Non-null aggregate_rounding_balance implies optimize_filters_for_memory
  explicit FastLocalBloomBitsBuilder(
      const int millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance,
      std::shared_ptr<CacheReservationManager> cache_res_mgr,
      bool detect_filter_construct_corruption)
      : XXPH3FilterBitsBuilder(aggregate_rounding_balance, cache_res_mgr,
                               detect_filter_construct_corruption),
        millibits_per_key_(millibits_per_key) {
    assert(millibits_per_key >= 1000);
  }

  // No Copy allowed
  FastLocalBloomBitsBuilder(const FastLocalBloomBitsBuilder&) = delete;
  void operator=(const FastLocalBloomBitsBuilder&) = delete;
  FastLocalBloomBitsBuilder(FastLocalBloomBitsBuilder&&) = delete;
  void operator=(FastLocalBloomBitsBuilder&&) = delete;

  ~FastLocalBloomBitsBuilder() override = default;

  using FilterBitsBuilder::Finish;

  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    return Finish(buf, nullptr);
  }

  Slice Finish(std::unique_ptr<const char[]>* buf, Status* status) override {
    size_t num_entries = hash_entries_info_.entries.size();
    if (num_entries == 0) {
      // This case migrated from FullFilterBlockBuilder::Finish
      if (status) {
        *status = Status::OK();
      }
      return FinishAlwaysFalse(buf);
    }

    size_t len_with_metadata = CalculateSpace(num_entries);

    std::unique_ptr<char[]> mutable_buf;
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        final_filter_cache_res_handle;
    len_with_metadata =
        AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);
    // Cache charging for mutable_buf
    if (cache_res_mgr_) {
      Status s = cache_res_mgr_->MakeCacheReservation(
          len_with_metadata * sizeof(char), &final_filter_cache_res_handle);
      s.PermitUncheckedError();
    }

    assert(mutable_buf);
    assert(len_with_metadata >= kMetadataLen);

    // Max size supported by implementation
    assert(len_with_metadata <= 0xffffffffU);

    // Compute num_probes after any rounding / adjustments
    int num_probes = GetNumProbes(num_entries, len_with_metadata);

    uint32_t len = static_cast<uint32_t>(len_with_metadata - kMetadataLen);
    if (len > 0) {
      TEST_SYNC_POINT_CALLBACK(
          "XXPH3FilterBitsBuilder::Finish::"
          "TamperHashEntries",
          &hash_entries_info_.entries);
      AddAllEntries(mutable_buf.get(), len, num_probes);
      Status verify_hash_entries_checksum_status =
          MaybeVerifyHashEntriesChecksum();
      if (!verify_hash_entries_checksum_status.ok()) {
        if (status) {
          *status = verify_hash_entries_checksum_status;
        }
        return FinishAlwaysTrue(buf);
      }
    }

    bool keep_entries_for_postverify = detect_filter_construct_corruption_;
    if (!keep_entries_for_postverify) {
      ResetEntries();
    }

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    mutable_buf[len] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation
    mutable_buf[len + 1] = static_cast<char>(0);
    // num_probes (and 0 in upper bits for 64-byte block size)
    mutable_buf[len + 2] = static_cast<char>(num_probes);
    // rest of metadata stays zero

    auto TEST_arg_pair __attribute__((__unused__)) =
        std::make_pair(&mutable_buf, len_with_metadata);
    TEST_SYNC_POINT_CALLBACK("XXPH3FilterBitsBuilder::Finish::TamperFilter",
                             &TEST_arg_pair);

    Slice rv(mutable_buf.get(), len_with_metadata);
    *buf = std::move(mutable_buf);
    final_filter_cache_res_handles_.push_back(
        std::move(final_filter_cache_res_handle));
    if (status) {
      *status = Status::OK();
    }
    return rv;
  }

  size_t ApproximateNumEntries(size_t bytes) override {
    size_t bytes_no_meta =
        bytes >= kMetadataLen ? RoundDownUsableSpace(bytes) - kMetadataLen : 0;
    return static_cast<size_t>(uint64_t{8000} * bytes_no_meta /
                               millibits_per_key_);
  }

  size_t CalculateSpace(size_t num_entries) override {
    // If not for cache line blocks in the filter, what would the target
    // length in bytes be?
    size_t raw_target_len = static_cast<size_t>(
        (uint64_t{num_entries} * millibits_per_key_ + 7999) / 8000);

    if (raw_target_len >= size_t{0xffffffc0}) {
      // Max supported for this data structure implementation
      raw_target_len = size_t{0xffffffc0};
    }

    // Round up to nearest multiple of 64 (block size). This adjustment is
    // used for target FP rate only so that we don't receive complaints about
    // lower FP rate vs. historic Bloom filter behavior.
    return ((raw_target_len + 63) & ~size_t{63}) + kMetadataLen;
  }

  double EstimatedFpRate(size_t keys, size_t len_with_metadata) override {
    if (len_with_metadata <= kMetadataLen) {
      return keys > 0 ? 1.0 : 0.0;
    }
    int num_probes = GetNumProbes(keys, len_with_metadata);
    return FastLocalBloomImpl::EstimatedFpRate(
        keys, len_with_metadata - kMetadataLen, num_probes, /*hash bits*/ 64);
  }

 protected:
  size_t RoundDownUsableSpace(size_t available_size) override {
    size_t rv = available_size - kMetadataLen;

    if (rv >= size_t{0xffffffc0}) {
      // Max supported for this data structure implementation
      rv = size_t{0xffffffc0};
    }

    // round down to multiple of 64 (block size)
    rv &= ~size_t{63};

    return rv + kMetadataLen;
  }

 private:
  // Compute num_probes after any rounding / adjustments
  int GetNumProbes(size_t keys, size_t len_with_metadata) {
    uint64_t millibits = uint64_t{len_with_metadata - kMetadataLen} * 8000;
    int actual_millibits_per_key =
        static_cast<int>(millibits / std::max(keys, size_t{1}));
    // BEGIN XXX/TODO(peterd): preserving old/default behavior for now to
    // minimize unit test churn. Remove this some time.
    if (!aggregate_rounding_balance_) {
      actual_millibits_per_key = millibits_per_key_;
    }
    // END XXX/TODO
    return FastLocalBloomImpl::ChooseNumProbes(actual_millibits_per_key);
  }

  void AddAllEntries(char* data, uint32_t len, int num_probes) {
    // Simple version without prefetching:
    //
    // for (auto h : hash_entries_info_.entries) {
    //   FastLocalBloomImpl::AddHash(Lower32of64(h), Upper32of64(h), len,
    //                               num_probes, data);
    // }

    const size_t num_entries = hash_entries_info_.entries.size();
    constexpr size_t kBufferMask = 7;
    static_assert(((kBufferMask + 1) & kBufferMask) == 0,
                  "Must be power of 2 minus 1");

    std::array<uint32_t, kBufferMask + 1> hashes;
    std::array<uint32_t, kBufferMask + 1> byte_offsets;

    // Prime the buffer
    size_t i = 0;
    std::deque<uint64_t>::iterator hash_entries_it =
        hash_entries_info_.entries.begin();
    for (; i <= kBufferMask && i < num_entries; ++i) {
      uint64_t h = *hash_entries_it;
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
      ++hash_entries_it;
    }

    // Process and buffer
    for (; i < num_entries; ++i) {
      uint32_t& hash_ref = hashes[i & kBufferMask];
      uint32_t& byte_offset_ref = byte_offsets[i & kBufferMask];
      // Process (add)
      FastLocalBloomImpl::AddHashPrepared(hash_ref, num_probes,
                                          data + byte_offset_ref);
      // And buffer
      uint64_t h = *hash_entries_it;
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offset_ref);
      hash_ref = Upper32of64(h);
      ++hash_entries_it;
    }

    // Finish processing
    for (i = 0; i <= kBufferMask && i < num_entries; ++i) {
      FastLocalBloomImpl::AddHashPrepared(hashes[i], num_probes,
                                          data + byte_offsets[i]);
    }
  }

  // Target allocation per added key, in thousandths of a bit.
  int millibits_per_key_;
};

// See description in FastLocalBloomImpl
class FastLocalBloomBitsReader : public BuiltinFilterBitsReader {
 public:
  FastLocalBloomBitsReader(const char* data, int num_probes, uint32_t len_bytes)
      : data_(data), num_probes_(num_probes), len_bytes_(len_bytes) {}

  // No Copy allowed
  FastLocalBloomBitsReader(const FastLocalBloomBitsReader&) = delete;
  void operator=(const FastLocalBloomBitsReader&) = delete;
  FastLocalBloomBitsReader(FastLocalBloomBitsReader&&) = delete;
  void operator=(FastLocalBloomBitsReader&&) = delete;

  ~FastLocalBloomBitsReader() override = default;

  bool MayMatch(const Slice& key) override {
    uint64_t h = GetSliceHash64(key);
    uint32_t byte_offset;
    FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                    /*out*/ &byte_offset);
    return FastLocalBloomImpl::HashMayMatchPrepared(Upper32of64(h), num_probes_,
                                                    data_ + byte_offset);
  }

  void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      uint64_t h = GetSliceHash64(*keys[i]);
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = FastLocalBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i]);
    }
  }

  bool HashMayMatch(const uint64_t h) override {
    return FastLocalBloomImpl::HashMayMatch(Lower32of64(h), Upper32of64(h),
                                            len_bytes_, num_probes_, data_);
  }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t len_bytes_;
};

// ##################### Ribbon filter implementation ################### //

// Implements concept RehasherTypesAndSettings in ribbon_impl.h
struct Standard128RibbonRehasherTypesAndSettings {
  // These are schema-critical. Any change almost certainly changes
  // underlying data.
  static constexpr bool kIsFilter = true;
  static constexpr bool kHomogeneous = false;
  static constexpr bool kFirstCoeffAlwaysOne = true;
  static constexpr bool kUseSmash = false;
  using CoeffRow = ROCKSDB_NAMESPACE::Unsigned128;
  using Hash = uint64_t;
  using Seed = uint32_t;
  // Changing these doesn't necessarily change underlying data,
  // but might affect supported scalability of those dimensions.
  using Index = uint32_t;
  using ResultRow = uint32_t;
  // Save a conditional in Ribbon queries
  static constexpr bool kAllowZeroStarts = false;
};

using Standard128RibbonTypesAndSettings =
    ribbon::StandardRehasherAdapter<Standard128RibbonRehasherTypesAndSettings>;

class Standard128RibbonBitsBuilder : public XXPH3FilterBitsBuilder {
 public:
  explicit Standard128RibbonBitsBuilder(
      double desired_one_in_fp_rate, int bloom_millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance,
      std::shared_ptr<CacheReservationManager> cache_res_mgr,
      bool detect_filter_construct_corruption, Logger* info_log)
      : XXPH3FilterBitsBuilder(aggregate_rounding_balance, cache_res_mgr,
                               detect_filter_construct_corruption),
        desired_one_in_fp_rate_(desired_one_in_fp_rate),
        info_log_(info_log),
        bloom_fallback_(bloom_millibits_per_key, aggregate_rounding_balance,
                        cache_res_mgr, detect_filter_construct_corruption) {
    assert(desired_one_in_fp_rate >= 1.0);
  }

  // No Copy allowed
  Standard128RibbonBitsBuilder(const Standard128RibbonBitsBuilder&) = delete;
  void operator=(const Standard128RibbonBitsBuilder&) = delete;
  Standard128RibbonBitsBuilder(Standard128RibbonBitsBuilder&&) = delete;
  void operator=(Standard128RibbonBitsBuilder&&) = delete;

  ~Standard128RibbonBitsBuilder() override = default;

  using FilterBitsBuilder::Finish;

  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    return Finish(buf, nullptr);
  }

  Slice Finish(std::unique_ptr<const char[]>* buf, Status* status) override {
    if (hash_entries_info_.entries.size() > kMaxRibbonEntries) {
      ROCKS_LOG_WARN(
          info_log_, "Too many keys for Ribbon filter: %llu",
          static_cast<unsigned long long>(hash_entries_info_.entries.size()));
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_info_.entries.empty());
      return bloom_fallback_.Finish(buf, status);
    }
    if (hash_entries_info_.entries.size() == 0) {
      // Save a conditional in Ribbon queries by using alternate reader
      // for zero entries added.
      if (status) {
        *status = Status::OK();
      }
      return FinishAlwaysFalse(buf);
    }
    uint32_t num_entries =
        static_cast<uint32_t>(hash_entries_info_.entries.size());
    uint32_t num_slots;
    size_t len_with_metadata;

    CalculateSpaceAndSlots(num_entries, &len_with_metadata, &num_slots);

    // Bloom fall-back indicator
    if (num_slots == 0) {
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_info_.entries.empty());
      return bloom_fallback_.Finish(buf, status);
    }

    uint32_t entropy = 0;
    if (!hash_entries_info_.entries.empty()) {
      entropy = Lower32of64(hash_entries_info_.entries.front());
    }

    BandingType banding;
    std::size_t bytes_banding = ribbon::StandardBanding<
        Standard128RibbonTypesAndSettings>::EstimateMemoryUsage(num_slots);
    Status status_banding_cache_res = Status::OK();

    // Cache charging for banding
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        banding_res_handle;
    if (cache_res_mgr_) {
      status_banding_cache_res = cache_res_mgr_->MakeCacheReservation(
          bytes_banding, &banding_res_handle);
    }

    if (status_banding_cache_res.IsMemoryLimit()) {
      ROCKS_LOG_WARN(info_log_,
                     "Cache charging for Ribbon filter banding failed due "
                     "to cache full");
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_info_.entries.empty());
      // Release cache for banding since the banding won't be allocated
      banding_res_handle.reset();
      return bloom_fallback_.Finish(buf, status);
    }

    TEST_SYNC_POINT_CALLBACK(
        "XXPH3FilterBitsBuilder::Finish::"
        "TamperHashEntries",
        &hash_entries_info_.entries);

    bool success = banding.ResetAndFindSeedToSolve(
        num_slots, hash_entries_info_.entries.begin(),
        hash_entries_info_.entries.end(),
        /*starting seed*/ entropy & 255, /*seed mask*/ 255);
    if (!success) {
      ROCKS_LOG_WARN(
          info_log_, "Too many re-seeds (256) for Ribbon filter, %llu / %llu",
          static_cast<unsigned long long>(hash_entries_info_.entries.size()),
          static_cast<unsigned long long>(num_slots));
      SwapEntriesWith(&bloom_fallback_);
      assert(hash_entries_info_.entries.empty());
      return bloom_fallback_.Finish(buf, status);
    }

    Status verify_hash_entries_checksum_status =
        MaybeVerifyHashEntriesChecksum();
    if (!verify_hash_entries_checksum_status.ok()) {
      ROCKS_LOG_WARN(info_log_, "Verify hash entries checksum error: %s",
                     verify_hash_entries_checksum_status.getState());
      if (status) {
        *status = verify_hash_entries_checksum_status;
      }
      return FinishAlwaysTrue(buf);
    }

    bool keep_entries_for_postverify = detect_filter_construct_corruption_;
    if (!keep_entries_for_postverify) {
      ResetEntries();
    }

    uint32_t seed = banding.GetOrdinalSeed();
    assert(seed < 256);

    std::unique_ptr<char[]> mutable_buf;
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        final_filter_cache_res_handle;
    len_with_metadata =
        AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);
    // Cache charging for mutable_buf
    if (cache_res_mgr_) {
      Status s = cache_res_mgr_->MakeCacheReservation(
          len_with_metadata * sizeof(char), &final_filter_cache_res_handle);
      s.PermitUncheckedError();
    }

    SolnType soln(mutable_buf.get(), len_with_metadata);
    soln.BackSubstFrom(banding);
    uint32_t num_blocks = soln.GetNumBlocks();
    // This should be guaranteed:
    // num_entries < 2^30
    // => (overhead_factor < 2.0)
    // num_entries * overhead_factor == num_slots < 2^31
    // => (num_blocks = num_slots / 128)
    // num_blocks < 2^24
    assert(num_blocks < 0x1000000U);

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -2 = Marker for Standard128 Ribbon
    mutable_buf[len_with_metadata - 5] = static_cast<char>(-2);
    // Hash seed
    mutable_buf[len_with_metadata - 4] = static_cast<char>(seed);
    // Number of blocks, in 24 bits
    // (Along with bytes, we can derive other settings)
    mutable_buf[len_with_metadata - 3] = static_cast<char>(num_blocks & 255);
    mutable_buf[len_with_metadata - 2] =
        static_cast<char>((num_blocks >> 8) & 255);
    mutable_buf[len_with_metadata - 1] =
        static_cast<char>((num_blocks >> 16) & 255);

    auto TEST_arg_pair __attribute__((__unused__)) =
        std::make_pair(&mutable_buf, len_with_metadata);
    TEST_SYNC_POINT_CALLBACK("XXPH3FilterBitsBuilder::Finish::TamperFilter",
                             &TEST_arg_pair);

    Slice rv(mutable_buf.get(), len_with_metadata);
    *buf = std::move(mutable_buf);
    final_filter_cache_res_handles_.push_back(
        std::move(final_filter_cache_res_handle));
    if (status) {
      *status = Status::OK();
    }
    return rv;
  }

  // Setting num_slots to 0 means "fall back on Bloom filter."
  // And note this implementation does not support num_entries or num_slots
  // beyond uint32_t; see kMaxRibbonEntries.
  void CalculateSpaceAndSlots(size_t num_entries,
                              size_t* target_len_with_metadata,
                              uint32_t* num_slots) {
    if (num_entries > kMaxRibbonEntries) {
      // More entries than supported by this Ribbon
      *num_slots = 0;  // use Bloom
      *target_len_with_metadata = bloom_fallback_.CalculateSpace(num_entries);
      return;
    }
    uint32_t entropy = 0;
    if (!hash_entries_info_.entries.empty()) {
      entropy = Upper32of64(hash_entries_info_.entries.front());
    }

    *num_slots = NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
    *target_len_with_metadata =
        SolnType::GetBytesForOneInFpRate(*num_slots, desired_one_in_fp_rate_,
                                         /*rounding*/ entropy) +
        kMetadataLen;

    // Consider possible Bloom fallback for small filters
    if (*num_slots < 1024) {
      size_t bloom = bloom_fallback_.CalculateSpace(num_entries);
      if (bloom < *target_len_with_metadata) {
        *num_slots = 0;  // use Bloom
        *target_len_with_metadata = bloom;
        return;
      }
    }
  }

  size_t CalculateSpace(size_t num_entries) override {
    if (num_entries == 0) {
      // See FinishAlwaysFalse
      return 0;
    }
    size_t target_len_with_metadata;
    uint32_t num_slots;
    CalculateSpaceAndSlots(num_entries, &target_len_with_metadata, &num_slots);
    (void)num_slots;
    return target_len_with_metadata;
  }

  // This is a somewhat ugly but reasonably fast and reasonably accurate
  // reversal of CalculateSpace.
  size_t ApproximateNumEntries(size_t bytes) override {
    size_t len_no_metadata =
        RoundDownUsableSpace(std::max(bytes, size_t{kMetadataLen})) -
        kMetadataLen;

    if (!(desired_one_in_fp_rate_ > 1.0)) {
      // Effectively asking for 100% FP rate, or NaN etc.
      // Note that NaN is neither < 1.0 nor > 1.0
      return kMaxRibbonEntries;
    }

    // Find a slight under-estimate for actual average bits per slot
    double min_real_bits_per_slot;
    if (desired_one_in_fp_rate_ >= 1.0 + std::numeric_limits<uint32_t>::max()) {
      // Max of 32 solution columns (result bits)
      min_real_bits_per_slot = 32.0;
    } else {
      // Account for mix of b and b+1 solution columns being slightly
      // suboptimal vs. ideal log2(1/fp_rate) bits.
      uint32_t rounded = static_cast<uint32_t>(desired_one_in_fp_rate_);
      int upper_bits_per_key = 1 + FloorLog2(rounded);
      double fp_rate_for_upper = std::pow(2.0, -upper_bits_per_key);
      double portion_lower =
          (1.0 / desired_one_in_fp_rate_ - fp_rate_for_upper) /
          fp_rate_for_upper;
      min_real_bits_per_slot = upper_bits_per_key - portion_lower;
      assert(min_real_bits_per_slot > 0.0);
      assert(min_real_bits_per_slot <= 32.0);
    }

    // An overestimate, but this should only be O(1) slots away from truth.
    double max_slots = len_no_metadata * 8.0 / min_real_bits_per_slot;

    // Let's not bother accounting for overflow to Bloom filter
    // (Includes NaN case)
    if (!(max_slots < ConfigHelper::GetNumSlots(kMaxRibbonEntries))) {
      return kMaxRibbonEntries;
    }

    // Set up for short iteration
    uint32_t slots = static_cast<uint32_t>(max_slots);
    slots = SolnType::RoundUpNumSlots(slots);

    // Assert that we have a valid upper bound on slots
    assert(SolnType::GetBytesForOneInFpRate(
               SolnType::RoundUpNumSlots(slots + 1), desired_one_in_fp_rate_,
               /*rounding*/ 0) > len_no_metadata);

    // Iterate up to a few times to rather precisely account for small effects
    for (int i = 0; slots > 0; ++i) {
      size_t reqd_bytes =
          SolnType::GetBytesForOneInFpRate(slots, desired_one_in_fp_rate_,
                                           /*rounding*/ 0);
      if (reqd_bytes <= len_no_metadata) {
        break;  // done
      }
      if (i >= 2) {
        // should have been enough iterations
        assert(false);
        break;
      }
      slots = SolnType::RoundDownNumSlots(slots - 1);
    }

    uint32_t num_entries = ConfigHelper::GetNumToAdd(slots);

    // Consider possible Bloom fallback for small filters
    if (slots < 1024) {
      size_t bloom = bloom_fallback_.ApproximateNumEntries(bytes);
      if (bloom > num_entries) {
        return bloom;
      } else {
        return num_entries;
      }
    } else {
      return std::min(num_entries, kMaxRibbonEntries);
    }
  }

  double EstimatedFpRate(size_t num_entries,
                         size_t len_with_metadata) override {
    if (len_with_metadata <= kMetadataLen) {
      return num_entries > 0 ? 1.0 : 0.0;
    }
    if (num_entries > kMaxRibbonEntries) {
      // More entries than supported by this Ribbon
      return bloom_fallback_.EstimatedFpRate(num_entries, len_with_metadata);
    }
    uint32_t num_slots =
        NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
    SolnType fake_soln(nullptr, len_with_metadata);
    fake_soln.ConfigureForNumSlots(num_slots);
    return fake_soln.ExpectedFpRate();
  }

  Status MaybePostVerify(const Slice& filter_content) override {
    bool fall_back = (bloom_fallback_.EstimateEntriesAdded() > 0);
    return fall_back ? bloom_fallback_.MaybePostVerify(filter_content)
                     : XXPH3FilterBitsBuilder::MaybePostVerify(filter_content);
  }

 protected:
  size_t RoundDownUsableSpace(size_t available_size) override {
    size_t rv = available_size - kMetadataLen;

    // round down to multiple of 16 (segment size)
    rv &= ~size_t{15};

    return rv + kMetadataLen;
  }

 private:
  using TS = Standard128RibbonTypesAndSettings;
  using SolnType = ribbon::SerializableInterleavedSolution<TS>;
  using BandingType = ribbon::StandardBanding<TS>;
  using ConfigHelper = ribbon::BandingConfigHelper1TS<ribbon::kOneIn20, TS>;

  static uint32_t NumEntriesToNumSlots(uint32_t num_entries) {
    uint32_t num_slots1 = ConfigHelper::GetNumSlots(num_entries);
    return SolnType::RoundUpNumSlots(num_slots1);
  }

  // Approximate num_entries to ensure number of bytes fits in 32 bits, which
  // is not an inherent limitation but does ensure somewhat graceful Bloom
  // fallback for crazy high number of entries, since the Bloom implementation
  // does not support number of bytes bigger than fits in 32 bits. This is
  // within an order of magnitude of implementation limit on num_slots
  // fitting in 32 bits, and even closer for num_blocks fitting in 24 bits
  // (for filter metadata).
  static constexpr uint32_t kMaxRibbonEntries = 950000000;  // ~ 1 billion

  // A desired value for 1/fp_rate. For example, 100 -> 1% fp rate.
  double desired_one_in_fp_rate_;

  // For warnings, or can be nullptr
  Logger* info_log_;

  // For falling back on Bloom filter in some exceptional cases and
  // very small filter cases
  FastLocalBloomBitsBuilder bloom_fallback_;
};

class Standard128RibbonBitsReader : public BuiltinFilterBitsReader {
 public:
  Standard128RibbonBitsReader(const char* data, size_t len_bytes,
                              uint32_t num_blocks, uint32_t seed)
      : soln_(const_cast<char*>(data), len_bytes) {
    soln_.ConfigureForNumBlocks(num_blocks);
    hasher_.SetOrdinalSeed(seed);
  }

  // No Copy allowed
  Standard128RibbonBitsReader(const Standard128RibbonBitsReader&) = delete;
  void operator=(const Standard128RibbonBitsReader&) = delete;
  Standard128RibbonBitsReader(Standard128RibbonBitsReader&&) = delete;
  void operator=(Standard128RibbonBitsReader&&) = delete;

  ~Standard128RibbonBitsReader() override = default;

  bool MayMatch(const Slice& key) override {
    uint64_t h = GetSliceHash64(key);
    return soln_.FilterQuery(h, hasher_);
  }

  void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    struct SavedData {
      uint64_t seeded_hash;
      uint32_t segment_num;
      uint32_t num_columns;
      uint32_t start_bits;
    };
    std::array<SavedData, MultiGetContext::MAX_BATCH_SIZE> saved;
    for (int i = 0; i < num_keys; ++i) {
      ribbon::InterleavedPrepareQuery(
          GetSliceHash64(*keys[i]), hasher_, soln_, &saved[i].seeded_hash,
          &saved[i].segment_num, &saved[i].num_columns, &saved[i].start_bits);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = ribbon::InterleavedFilterQuery(
          saved[i].seeded_hash, saved[i].segment_num, saved[i].num_columns,
          saved[i].start_bits, hasher_, soln_);
    }
  }

  bool HashMayMatch(const uint64_t h) override {
    return soln_.FilterQuery(h, hasher_);
  }

 private:
  using TS = Standard128RibbonTypesAndSettings;
  ribbon::SerializableInterleavedSolution<TS> soln_;
  ribbon::StandardHasher<TS> hasher_;
};

// ##################### Legacy Bloom implementation ################### //

using LegacyBloomImpl = LegacyLocalityBloomImpl</*ExtraRotates*/ false>;

class LegacyBloomBitsBuilder : public FilterBitsBuilder {
 public:
  explicit LegacyBloomBitsBuilder(const int bits_per_key, Logger* info_log);

  // No Copy allowed
  LegacyBloomBitsBuilder(const LegacyBloomBitsBuilder&) = delete;
  void operator=(const LegacyBloomBitsBuilder&) = delete;
  LegacyBloomBitsBuilder(LegacyBloomBitsBuilder&&) = delete;
  void operator=(LegacyBloomBitsBuilder&&) = delete;

  ~LegacyBloomBitsBuilder() override;

  void AddKey(const Slice& key) override;
  void AddKeyAndAlt(const Slice& key, const Slice& alt) override;

  size_t EstimateEntriesAdded() override { return hash_entries_.size(); }

  using FilterBitsBuilder::Finish;

  Slice Finish(std::unique_ptr<const char[]>* buf) override;

  size_t CalculateSpace(size_t num_entries) override {
    uint32_t dont_care1;
    uint32_t dont_care2;
    return CalculateSpace(num_entries, &dont_care1, &dont_care2);
  }

  double EstimatedFpRate(size_t keys, size_t len_with_metadata) override {
    if (len_with_metadata <= kMetadataLen) {
      return keys > 0 ? 1.0 : 0.0;
    }
    return LegacyBloomImpl::EstimatedFpRate(
        keys, len_with_metadata - kMetadataLen, num_probes_);
  }

  size_t ApproximateNumEntries(size_t bytes) override;

 private:
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;
  // A single-element cache to help AddKeyAndAlt. (-1 == empty)
  int64_t prev_alt_hash_ = -1;

  Logger* info_log_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(size_t num_entries, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Implementation-specific variant of public CalculateSpace
  uint32_t CalculateSpace(size_t num_entries, uint32_t* total_bits,
                          uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
};

LegacyBloomBitsBuilder::LegacyBloomBitsBuilder(const int bits_per_key,
                                               Logger* info_log)
    : bits_per_key_(bits_per_key),
      num_probes_(LegacyNoLocalityBloomImpl::ChooseNumProbes(bits_per_key_)),
      info_log_(info_log) {
  assert(bits_per_key_);
}

LegacyBloomBitsBuilder::~LegacyBloomBitsBuilder() = default;

void LegacyBloomBitsBuilder::AddKey(const Slice& key) {
  uint32_t hash = BloomHash(key);
  if (hash_entries_.empty() || hash_entries_.back() != hash) {
    hash_entries_.push_back(hash);
  }
}

void LegacyBloomBitsBuilder::AddKeyAndAlt(const Slice& key, const Slice& alt) {
  // Modified from XXPH3FilterBitsBuilder::AddKeyAndAlt
  uint32_t key_hash = BloomHash(key);
  uint32_t alt_hash = BloomHash(alt);

  int64_t prev_key_hash = -1;
  int64_t prev_alt_hash = prev_alt_hash_;
  if (!hash_entries_.empty()) {
    prev_key_hash = hash_entries_.back();
  }
  if (alt_hash != prev_alt_hash && alt_hash != key_hash &&
      alt_hash != prev_key_hash) {
    hash_entries_.push_back(alt_hash);
  }
  prev_alt_hash_ = alt_hash;
  if (key_hash != prev_key_hash && key_hash != prev_alt_hash) {
    hash_entries_.push_back(key_hash);
  }
}

Slice LegacyBloomBitsBuilder::Finish(std::unique_ptr<const char[]>* buf) {
  size_t num_entries = hash_entries_.size();
  if (num_entries == 0) {
    // This case migrated from FullFilterBlockBuilder::Finish
    return FinishAlwaysFalse(buf);
  }

  uint32_t total_bits;
  uint32_t num_lines;
  char* data =
      ReserveSpace(static_cast<int>(num_entries), &total_bits, &num_lines);
  assert(data);

  if (total_bits != 0 && num_lines != 0) {
    for (auto h : hash_entries_) {
      AddHash(h, data, num_lines, total_bits);
    }

    // Check for excessive entries for 32-bit hash function
    if (num_entries >= /* minimum of 3 million */ 3000000U) {
      // More specifically, we can detect that the 32-bit hash function
      // is causing significant increase in FP rate by comparing current
      // estimated FP rate to what we would get with a normal number of
      // keys at same memory ratio.
      double est_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          num_entries, total_bits / 8, num_probes_);
      double vs_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          1U << 16, (1U << 16) * bits_per_key_ / 8, num_probes_);

      if (est_fp_rate >= 1.50 * vs_fp_rate) {
        // For more details, see
        // https://github.com/meta/rocksdb/wiki/RocksDB-Bloom-Filter
        ROCKS_LOG_WARN(
            info_log_,
            "Using legacy SST/BBT Bloom filter with excessive key count "
            "(%.1fM @ %dbpk), causing estimated %.1fx higher filter FP rate. "
            "Consider using new Bloom with format_version>=5, smaller SST "
            "file size, or partitioned filters.",
            num_entries / 1000000.0, bits_per_key_, est_fp_rate / vs_fp_rate);
      }
    }
  }
  // See BloomFilterPolicy::GetFilterBitsReader for metadata
  data[total_bits / 8] = static_cast<char>(num_probes_);
  EncodeFixed32(data + total_bits / 8 + 1, static_cast<uint32_t>(num_lines));

  const char* const_data = data;
  buf->reset(const_data);
  hash_entries_.clear();
  prev_alt_hash_ = -1;

  return Slice(data, total_bits / 8 + kMetadataLen);
}

size_t LegacyBloomBitsBuilder::ApproximateNumEntries(size_t bytes) {
  assert(bits_per_key_);
  assert(bytes > 0);

  uint64_t total_bits_tmp = bytes * 8;
  // total bits, including temporary computations, cannot exceed 2^32
  // for compatibility
  total_bits_tmp = std::min(total_bits_tmp, uint64_t{0xffff0000});

  uint32_t high = static_cast<uint32_t>(total_bits_tmp) /
                      static_cast<uint32_t>(bits_per_key_) +
                  1;
  uint32_t low = 1;
  uint32_t n = high;
  for (; n >= low; n--) {
    if (CalculateSpace(n) <= bytes) {
      break;
    }
  }
  return n;
}

uint32_t LegacyBloomBitsBuilder::GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_lines =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_lines an odd number to make sure more bits are involved
  // when determining which block.
  if (num_lines % 2 == 0) {
    num_lines++;
  }
  return num_lines * (CACHE_LINE_SIZE * 8);
}

uint32_t LegacyBloomBitsBuilder::CalculateSpace(size_t num_entries,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entries != 0) {
    size_t total_bits_tmp = num_entries * bits_per_key_;
    // total bits, including temporary computations, cannot exceed 2^32
    // for compatibility
    total_bits_tmp = std::min(total_bits_tmp, size_t{0xffff0000});

    *total_bits =
        GetTotalBitsForLocality(static_cast<uint32_t>(total_bits_tmp));
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += kMetadataLen;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}

char* LegacyBloomBitsBuilder::ReserveSpace(size_t num_entries,
                                           uint32_t* total_bits,
                                           uint32_t* num_lines) {
  uint32_t sz = CalculateSpace(num_entries, total_bits, num_lines);
  char* data = new char[sz];
  memset(data, 0, sz);
  return data;
}

inline void LegacyBloomBitsBuilder::AddHash(uint32_t h, char* data,
                                            uint32_t num_lines,
                                            uint32_t total_bits) {
#ifdef NDEBUG
  static_cast<void>(total_bits);
#endif
  assert(num_lines > 0 && total_bits > 0);

  LegacyBloomImpl::AddHash(h, num_lines, num_probes_, data,
                           ConstexprFloorLog2(CACHE_LINE_SIZE));
}

class LegacyBloomBitsReader : public BuiltinFilterBitsReader {
 public:
  LegacyBloomBitsReader(const char* data, int num_probes, uint32_t num_lines,
                        uint32_t log2_cache_line_size)
      : data_(data),
        num_probes_(num_probes),
        num_lines_(num_lines),
        log2_cache_line_size_(log2_cache_line_size) {}

  // No Copy allowed
  LegacyBloomBitsReader(const LegacyBloomBitsReader&) = delete;
  void operator=(const LegacyBloomBitsReader&) = delete;
  LegacyBloomBitsReader(LegacyBloomBitsReader&&) = delete;
  void operator=(LegacyBloomBitsReader&&) = delete;

  ~LegacyBloomBitsReader() override = default;

  // "contents" contains the data built by a preceding call to
  // FilterBitsBuilder::Finish. MayMatch must return true if the key was
  // passed to FilterBitsBuilder::AddKey. This method may return true or false
  // if the key was not on the list, but it should aim to return false with a
  // high probability.
  bool MayMatch(const Slice& key) override {
    uint32_t hash = BloomHash(key);
    uint32_t byte_offset;
    LegacyBloomImpl::PrepareHashMayMatch(
        hash, num_lines_, data_, /*out*/ &byte_offset, log2_cache_line_size_);
    return LegacyBloomImpl::HashMayMatchPrepared(
        hash, num_probes_, data_ + byte_offset, log2_cache_line_size_);
  }

  void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      hashes[i] = BloomHash(*keys[i]);
      LegacyBloomImpl::PrepareHashMayMatch(hashes[i], num_lines_, data_,
                                           /*out*/ &byte_offsets[i],
                                           log2_cache_line_size_);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = LegacyBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i],
          log2_cache_line_size_);
    }
  }

  bool HashMayMatch(const uint64_t /* h */) override { return false; }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t num_lines_;
  const uint32_t log2_cache_line_size_;
};

class AlwaysTrueFilter : public BuiltinFilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return true; }
  using FilterBitsReader::MayMatch;  // inherit overload
  bool HashMayMatch(const uint64_t) override { return true; }
  using BuiltinFilterBitsReader::HashMayMatch;  // inherit overload
};

class AlwaysFalseFilter : public BuiltinFilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return false; }
  using FilterBitsReader::MayMatch;  // inherit overload
  bool HashMayMatch(const uint64_t) override { return false; }
  using BuiltinFilterBitsReader::HashMayMatch;  // inherit overload
};

Status XXPH3FilterBitsBuilder::MaybePostVerify(const Slice& filter_content) {
  Status s = Status::OK();

  if (!detect_filter_construct_corruption_) {
    return s;
  }

  std::unique_ptr<BuiltinFilterBitsReader> bits_reader(
      BuiltinFilterPolicy::GetBuiltinFilterBitsReader(filter_content));

  for (uint64_t h : hash_entries_info_.entries) {
    // The current approach will not detect corruption from XXPH3Filter to
    // AlwaysTrueFilter, which can lead to performance cost later due to
    // AlwaysTrueFilter not filtering anything. But this cost is acceptable
    // given the extra implementation complixity to detect such case.
    bool may_match = bits_reader->HashMayMatch(h);
    if (!may_match) {
      s = Status::Corruption("Corrupted filter content");
      break;
    }
  }

  ResetEntries();
  return s;
}
}  // namespace

const char* BuiltinFilterPolicy::kClassName() {
  return "rocksdb.internal.BuiltinFilter";
}

bool BuiltinFilterPolicy::IsInstanceOf(const std::string& name) const {
  if (name == kClassName()) {
    return true;
  } else {
    return FilterPolicy::IsInstanceOf(name);
  }
}

static const char* kBuiltinFilterMetadataName = "rocksdb.BuiltinBloomFilter";

const char* BuiltinFilterPolicy::CompatibilityName() const {
  return kBuiltinFilterMetadataName;
}

BloomLikeFilterPolicy::BloomLikeFilterPolicy(double bits_per_key)
    : warned_(false), aggregate_rounding_balance_(0) {
  // Sanitize bits_per_key
  if (bits_per_key < 0.5) {
    // Round down to no filter
    bits_per_key = 0;
  } else if (bits_per_key < 1.0) {
    // Minimum 1 bit per key (equiv) when creating filter
    bits_per_key = 1.0;
  } else if (!(bits_per_key < 100.0)) {  // including NaN
    bits_per_key = 100.0;
  }

  // Includes a nudge toward rounding up, to ensure on all platforms
  // that doubles specified with three decimal digits after the decimal
  // point are interpreted accurately.
  millibits_per_key_ = static_cast<int>(bits_per_key * 1000.0 + 0.500001);

  // For now configure Ribbon filter to match Bloom FP rate and save
  // memory. (Ribbon bits per key will be ~30% less than Bloom bits per key
  // for same FP rate.)
  desired_one_in_fp_rate_ =
      1.0 / BloomMath::CacheLocalFpRate(
                bits_per_key,
                FastLocalBloomImpl::ChooseNumProbes(millibits_per_key_),
                /*cache_line_bits*/ 512);

  // For better or worse, this is a rounding up of a nudged rounding up,
  // e.g. 7.4999999999999 will round up to 8, but that provides more
  // predictability against small arithmetic errors in floating point.
  whole_bits_per_key_ = (millibits_per_key_ + 500) / 1000;
}

BloomLikeFilterPolicy::~BloomLikeFilterPolicy() = default;
const char* BloomLikeFilterPolicy::kClassName() {
  return "rocksdb.internal.BloomLikeFilter";
}

bool BloomLikeFilterPolicy::IsInstanceOf(const std::string& name) const {
  if (name == kClassName()) {
    return true;
  } else {
    return BuiltinFilterPolicy::IsInstanceOf(name);
  }
}

const char* ReadOnlyBuiltinFilterPolicy::kClassName() {
  return kBuiltinFilterMetadataName;
}

std::string BloomLikeFilterPolicy::GetId() const {
  return Name() + GetBitsPerKeySuffix();
}

BloomFilterPolicy::BloomFilterPolicy(double bits_per_key)
    : BloomLikeFilterPolicy(bits_per_key) {}

FilterBitsBuilder* BloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (GetMillibitsPerKey() == 0) {
    // "No filter" special case
    return nullptr;
  } else if (context.table_options.format_version < 5) {
    return GetLegacyBloomBuilderWithContext(context);
  } else {
    return GetFastLocalBloomBuilderWithContext(context);
  }
}

const char* BloomFilterPolicy::kClassName() { return "bloomfilter"; }
const char* BloomFilterPolicy::kNickName() { return "rocksdb.BloomFilter"; }

std::string BloomFilterPolicy::GetId() const {
  // Including ":false" for better forward-compatibility with 6.29 and earlier
  // which required a boolean `use_block_based_builder` parameter
  return BloomLikeFilterPolicy::GetId() + ":false";
}

FilterBitsBuilder* BloomLikeFilterPolicy::GetFastLocalBloomBuilderWithContext(
    const FilterBuildingContext& context) const {
  bool offm = context.table_options.optimize_filters_for_memory;
  const auto options_overrides_iter =
      context.table_options.cache_usage_options.options_overrides.find(
          CacheEntryRole::kFilterConstruction);
  const auto filter_construction_charged =
      options_overrides_iter !=
              context.table_options.cache_usage_options.options_overrides.end()
          ? options_overrides_iter->second.charged
          : context.table_options.cache_usage_options.options.charged;

  std::shared_ptr<CacheReservationManager> cache_res_mgr;
  if (context.table_options.block_cache &&
      filter_construction_charged ==
          CacheEntryRoleOptions::Decision::kEnabled) {
    cache_res_mgr = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kFilterConstruction>>(
        context.table_options.block_cache);
  }
  return new FastLocalBloomBitsBuilder(
      millibits_per_key_, offm ? &aggregate_rounding_balance_ : nullptr,
      cache_res_mgr, context.table_options.detect_filter_construct_corruption);
}

FilterBitsBuilder* BloomLikeFilterPolicy::GetLegacyBloomBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (whole_bits_per_key_ >= 14 && context.info_log &&
      !warned_.load(std::memory_order_relaxed)) {
    warned_ = true;
    const char* adjective;
    if (whole_bits_per_key_ >= 20) {
      adjective = "Dramatic";
    } else {
      adjective = "Significant";
    }
    // For more details, see
    // https://github.com/meta/rocksdb/wiki/RocksDB-Bloom-Filter
    ROCKS_LOG_WARN(context.info_log,
                   "Using legacy Bloom filter with high (%d) bits/key. "
                   "%s filter space and/or accuracy improvement is available "
                   "with format_version>=5.",
                   whole_bits_per_key_, adjective);
  }
  return new LegacyBloomBitsBuilder(whole_bits_per_key_, context.info_log);
}

FilterBitsBuilder*
BloomLikeFilterPolicy::GetStandard128RibbonBuilderWithContext(
    const FilterBuildingContext& context) const {
  // FIXME: code duplication with GetFastLocalBloomBuilderWithContext
  bool offm = context.table_options.optimize_filters_for_memory;
  const auto options_overrides_iter =
      context.table_options.cache_usage_options.options_overrides.find(
          CacheEntryRole::kFilterConstruction);
  const auto filter_construction_charged =
      options_overrides_iter !=
              context.table_options.cache_usage_options.options_overrides.end()
          ? options_overrides_iter->second.charged
          : context.table_options.cache_usage_options.options.charged;

  std::shared_ptr<CacheReservationManager> cache_res_mgr;
  if (context.table_options.block_cache &&
      filter_construction_charged ==
          CacheEntryRoleOptions::Decision::kEnabled) {
    cache_res_mgr = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kFilterConstruction>>(
        context.table_options.block_cache);
  }
  return new Standard128RibbonBitsBuilder(
      desired_one_in_fp_rate_, millibits_per_key_,
      offm ? &aggregate_rounding_balance_ : nullptr, cache_res_mgr,
      context.table_options.detect_filter_construct_corruption,
      context.info_log);
}

std::string BloomLikeFilterPolicy::GetBitsPerKeySuffix() const {
  std::string rv = ":" + std::to_string(millibits_per_key_ / 1000);
  int frac = millibits_per_key_ % 1000;
  if (frac > 0) {
    rv.push_back('.');
    rv.push_back(static_cast<char>('0' + (frac / 100)));
    frac %= 100;
    if (frac > 0) {
      rv.push_back(static_cast<char>('0' + (frac / 10)));
      frac %= 10;
      if (frac > 0) {
        rv.push_back(static_cast<char>('0' + frac));
      }
    }
  }
  return rv;
}

FilterBitsBuilder* BuiltinFilterPolicy::GetBuilderFromContext(
    const FilterBuildingContext& context) {
  if (context.table_options.filter_policy) {
    return context.table_options.filter_policy->GetBuilderWithContext(context);
  } else {
    return nullptr;
  }
}

// For testing only, but always constructable with internal names
namespace test {

const char* LegacyBloomFilterPolicy::kClassName() {
  return "rocksdb.internal.LegacyBloomFilter";
}

FilterBitsBuilder* LegacyBloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (GetMillibitsPerKey() == 0) {
    // "No filter" special case
    return nullptr;
  }
  return GetLegacyBloomBuilderWithContext(context);
}

const char* FastLocalBloomFilterPolicy::kClassName() {
  return "rocksdb.internal.FastLocalBloomFilter";
}

FilterBitsBuilder* FastLocalBloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (GetMillibitsPerKey() == 0) {
    // "No filter" special case
    return nullptr;
  }
  return GetFastLocalBloomBuilderWithContext(context);
}

const char* Standard128RibbonFilterPolicy::kClassName() {
  return "rocksdb.internal.Standard128RibbonFilter";
}

FilterBitsBuilder* Standard128RibbonFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (GetMillibitsPerKey() == 0) {
    // "No filter" special case
    return nullptr;
  }
  return GetStandard128RibbonBuilderWithContext(context);
}

}  // namespace test

BuiltinFilterBitsReader* BuiltinFilterPolicy::GetBuiltinFilterBitsReader(
    const Slice& contents) {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  if (len_with_meta <= kMetadataLen) {
    // filter is empty or broken. Treat like zero keys added.
    return new AlwaysFalseFilter();
  }

  // Legacy Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | byte for num_probes or            |
  //               |   marker for new implementations  |
  //         len+1 +-----------------------------------+
  //               | four bytes for number of cache    |
  //               |   lines                           |
  // len_with_meta +-----------------------------------+

  int8_t raw_num_probes =
      static_cast<int8_t>(contents.data()[len_with_meta - kMetadataLen]);
  // NB: *num_probes > 30 and < 128 probably have not been used, because of
  // BloomFilterPolicy::initialize, unless directly calling
  // LegacyBloomBitsBuilder as an API, but we are leaving those cases in
  // limbo with LegacyBloomBitsReader for now.

  if (raw_num_probes < 1) {
    // Note: < 0 (or unsigned > 127) indicate special new implementations
    // (or reserved for future use)
    switch (raw_num_probes) {
      case 0:
        // Treat as zero probes (always FP)
        return new AlwaysTrueFilter();
      case -1:
        // Marker for newer Bloom implementations
        return GetBloomBitsReader(contents);
      case -2:
        // Marker for Ribbon implementations
        return GetRibbonBitsReader(contents);
      default:
        // Reserved (treat as zero probes, always FP, for now)
        return new AlwaysTrueFilter();
    }
  }
  // else attempt decode for LegacyBloomBitsReader

  int num_probes = raw_num_probes;
  assert(num_probes >= 1);
  assert(num_probes <= 127);

  uint32_t len = len_with_meta - kMetadataLen;
  assert(len > 0);

  uint32_t num_lines = DecodeFixed32(contents.data() + len_with_meta - 4);
  uint32_t log2_cache_line_size;

  if (num_lines * CACHE_LINE_SIZE == len) {
    // Common case
    log2_cache_line_size = ConstexprFloorLog2(CACHE_LINE_SIZE);
  } else if (num_lines == 0 || len % num_lines != 0) {
    // Invalid (no solution to num_lines * x == len)
    // Treat as zero probes (always FP) for now.
    return new AlwaysTrueFilter();
  } else {
    // Determine the non-native cache line size (from another system)
    log2_cache_line_size = 0;
    while ((num_lines << log2_cache_line_size) < len) {
      ++log2_cache_line_size;
    }
    if ((num_lines << log2_cache_line_size) != len) {
      // Invalid (block size not a power of two)
      // Treat as zero probes (always FP) for now.
      return new AlwaysTrueFilter();
    }
  }
  // if not early return
  return new LegacyBloomBitsReader(contents.data(), num_probes, num_lines,
                                   log2_cache_line_size);
}

// Read metadata to determine what kind of FilterBitsReader is needed
// and return a new one.
FilterBitsReader* BuiltinFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  return BuiltinFilterPolicy::GetBuiltinFilterBitsReader(contents);
}

BuiltinFilterBitsReader* BuiltinFilterPolicy::GetRibbonBitsReader(
    const Slice& contents) {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - kMetadataLen;

  assert(len > 0);  // precondition

  uint32_t seed = static_cast<uint8_t>(contents.data()[len + 1]);
  uint32_t num_blocks = static_cast<uint8_t>(contents.data()[len + 2]);
  num_blocks |= static_cast<uint8_t>(contents.data()[len + 3]) << 8;
  num_blocks |= static_cast<uint8_t>(contents.data()[len + 4]) << 16;
  if (num_blocks < 2) {
    // Not supported
    // num_blocks == 1 is not used because num_starts == 1 is problematic
    // for the hashing scheme. num_blocks == 0 is unused because there's
    // already a concise encoding of an "always false" filter.
    // Return something safe:
    return new AlwaysTrueFilter();
  }
  return new Standard128RibbonBitsReader(contents.data(), len, num_blocks,
                                         seed);
}

// For newer Bloom filter implementations
BuiltinFilterBitsReader* BuiltinFilterPolicy::GetBloomBitsReader(
    const Slice& contents) {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - kMetadataLen;

  assert(len > 0);  // precondition

  // New Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | char{-1} byte -> new Bloom filter |
  //         len+1 +-----------------------------------+
  //               | byte for subimplementation        |
  //               |   0: FastLocalBloom               |
  //               |   other: reserved                 |
  //         len+2 +-----------------------------------+
  //               | byte for block_and_probes         |
  //               |   0 in top 3 bits -> 6 -> 64-byte |
  //               |   reserved:                       |
  //               |   1 in top 3 bits -> 7 -> 128-byte|
  //               |   2 in top 3 bits -> 8 -> 256-byte|
  //               |   ...                             |
  //               |   num_probes in bottom 5 bits,    |
  //               |     except 0 and 31 reserved      |
  //         len+3 +-----------------------------------+
  //               | two bytes reserved                |
  //               |   possibly for hash seed          |
  // len_with_meta +-----------------------------------+

  // Read more metadata (see above)
  char sub_impl_val = contents.data()[len_with_meta - 4];
  char block_and_probes = contents.data()[len_with_meta - 3];
  int log2_block_bytes = ((block_and_probes >> 5) & 7) + 6;

  int num_probes = (block_and_probes & 31);
  if (num_probes < 1 || num_probes > 30) {
    // Reserved / future safe
    return new AlwaysTrueFilter();
  }

  uint16_t rest = DecodeFixed16(contents.data() + len_with_meta - 2);
  if (rest != 0) {
    // Reserved, possibly for hash seed
    // Future safe
    return new AlwaysTrueFilter();
  }

  if (sub_impl_val == 0) {        // FastLocalBloom
    if (log2_block_bytes == 6) {  // Only block size supported for now
      return new FastLocalBloomBitsReader(contents.data(), num_probes, len);
    }
  }
  // otherwise
  // Reserved / future safe
  return new AlwaysTrueFilter();
}

const FilterPolicy* NewBloomFilterPolicy(double bits_per_key,
                                         bool /*use_block_based_builder*/) {
  // NOTE: use_block_based_builder now ignored so block-based filter is no
  // longer accessible in public API.
  return new BloomFilterPolicy(bits_per_key);
}

RibbonFilterPolicy::RibbonFilterPolicy(double bloom_equivalent_bits_per_key,
                                       int bloom_before_level)
    : BloomLikeFilterPolicy(bloom_equivalent_bits_per_key),
      bloom_before_level_(bloom_before_level) {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"bloom_before_level",
       {offsetof(class RibbonFilterPolicy, bloom_before_level_),
        OptionType::kAtomicInt, OptionVerificationType::kNormal,
        OptionTypeFlags::kMutable}},
  };
  RegisterOptions(this, &type_info);
}

FilterBitsBuilder* RibbonFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (GetMillibitsPerKey() == 0) {
    // "No filter" special case
    return nullptr;
  }
  // Treat unknown same as bottommost, INT_MAX - 1.
  // INT_MAX is reserved for "always use Bloom".
  int levelish = INT_MAX - 1;

  int bloom_before_level = bloom_before_level_.load(std::memory_order_relaxed);
  if (bloom_before_level < INT_MAX) {
    switch (context.compaction_style) {
      case kCompactionStyleLevel:
      case kCompactionStyleUniversal: {
        if (context.reason == TableFileCreationReason::kFlush) {
          // Treat flush as level -1
          assert(context.level_at_creation == 0);
          levelish = -1;
        } else if (context.level_at_creation == -1) {
          // Unknown level
          assert(levelish == INT_MAX - 1);
        } else {
          levelish = context.level_at_creation;
        }
        break;
      }
      case kCompactionStyleFIFO:
      case kCompactionStyleNone:
        // Treat as bottommost
        assert(levelish == INT_MAX - 1);
        break;
    }
  } else {
    // INT_MAX == always Bloom
    assert(levelish < bloom_before_level);
  }
  if (levelish < bloom_before_level) {
    return GetFastLocalBloomBuilderWithContext(context);
  } else {
    return GetStandard128RibbonBuilderWithContext(context);
  }
}

const char* RibbonFilterPolicy::kClassName() { return "ribbonfilter"; }
const char* RibbonFilterPolicy::kNickName() { return "rocksdb.RibbonFilter"; }
const char* RibbonFilterPolicy::kName() { return "RibbonFilterPolicy"; }

std::string RibbonFilterPolicy::GetId() const {
  return BloomLikeFilterPolicy::GetId() + ":" +
         std::to_string(bloom_before_level_.load(std::memory_order_acquire));
}

FilterPolicy* NewRibbonFilterPolicy(double bloom_equivalent_bits_per_key,
                                    int bloom_before_level) {
  return new RibbonFilterPolicy(bloom_equivalent_bits_per_key,
                                bloom_before_level);
}

FilterBuildingContext::FilterBuildingContext(
    const BlockBasedTableOptions& _table_options)
    : table_options(_table_options) {}

FilterPolicy::~FilterPolicy() = default;

std::shared_ptr<const FilterPolicy> BloomLikeFilterPolicy::Create(
    const std::string& name, double bits_per_key) {
  if (name == test::LegacyBloomFilterPolicy::kClassName()) {
    return std::make_shared<test::LegacyBloomFilterPolicy>(bits_per_key);
  } else if (name == test::FastLocalBloomFilterPolicy::kClassName()) {
    return std::make_shared<test::FastLocalBloomFilterPolicy>(bits_per_key);
  } else if (name == test::Standard128RibbonFilterPolicy::kClassName()) {
    return std::make_shared<test::Standard128RibbonFilterPolicy>(bits_per_key);
  } else if (name == BloomFilterPolicy::kClassName()) {
    // For testing
    return std::make_shared<BloomFilterPolicy>(bits_per_key);
  } else if (name == RibbonFilterPolicy::kClassName()) {
    // For testing
    return std::make_shared<RibbonFilterPolicy>(bits_per_key,
                                                /*bloom_before_level*/ 0);
  } else {
    return nullptr;
  }
}

namespace {
static ObjectLibrary::PatternEntry FilterPatternEntryWithBits(
    const char* name) {
  return ObjectLibrary::PatternEntry(name, false).AddNumber(":", false);
}

template <typename T>
T* NewBuiltinFilterPolicyWithBits(const std::string& uri) {
  const std::vector<std::string> vals = StringSplit(uri, ':');
  double bits_per_key = ParseDouble(vals[1]);
  return new T(bits_per_key);
}
static int RegisterBuiltinFilterPolicies(ObjectLibrary& library,
                                         const std::string& /*arg*/) {
  library.AddFactory<const FilterPolicy>(
      ReadOnlyBuiltinFilterPolicy::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new ReadOnlyBuiltinFilterPolicy());
        return guard->get();
      });

  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(BloomFilterPolicy::kClassName())
          .AnotherName(BloomFilterPolicy::kNickName()),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(NewBuiltinFilterPolicyWithBits<BloomFilterPolicy>(uri));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(BloomFilterPolicy::kClassName())
          .AnotherName(BloomFilterPolicy::kNickName())
          .AddSuffix(":false"),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(NewBuiltinFilterPolicyWithBits<BloomFilterPolicy>(uri));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(BloomFilterPolicy::kClassName())
          .AnotherName(BloomFilterPolicy::kNickName())
          .AddSuffix(":true"),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        const std::vector<std::string> vals = StringSplit(uri, ':');
        double bits_per_key = ParseDouble(vals[1]);
        // NOTE: This case previously configured the deprecated block-based
        // filter, but old ways of configuring that now map to full filter. We
        // defer to the corresponding API to ensure consistency in case that
        // change is reverted.
        guard->reset(NewBloomFilterPolicy(bits_per_key, true));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(RibbonFilterPolicy::kClassName())
          .AnotherName(RibbonFilterPolicy::kNickName()),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        const std::vector<std::string> vals = StringSplit(uri, ':');
        double bits_per_key = ParseDouble(vals[1]);
        guard->reset(NewRibbonFilterPolicy(bits_per_key));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(RibbonFilterPolicy::kClassName())
          .AnotherName(RibbonFilterPolicy::kNickName())
          .AddNumber(":", true),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        const std::vector<std::string> vals = StringSplit(uri, ':');
        double bits_per_key = ParseDouble(vals[1]);
        int bloom_before_level = ParseInt(vals[2]);
        guard->reset(NewRibbonFilterPolicy(bits_per_key, bloom_before_level));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(test::LegacyBloomFilterPolicy::kClassName()),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(
            NewBuiltinFilterPolicyWithBits<test::LegacyBloomFilterPolicy>(uri));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(
          test::FastLocalBloomFilterPolicy::kClassName()),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(
            NewBuiltinFilterPolicyWithBits<test::FastLocalBloomFilterPolicy>(
                uri));
        return guard->get();
      });
  library.AddFactory<const FilterPolicy>(
      FilterPatternEntryWithBits(
          test::Standard128RibbonFilterPolicy::kClassName()),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(
            NewBuiltinFilterPolicyWithBits<test::Standard128RibbonFilterPolicy>(
                uri));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
}  // namespace

Status FilterPolicy::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<const FilterPolicy>* policy) {
  if (value == kNullptrString || value.empty()) {
    policy->reset();
    return Status::OK();
  } else if (value == ReadOnlyBuiltinFilterPolicy::kClassName()) {
    *policy = std::make_shared<ReadOnlyBuiltinFilterPolicy>();
    return Status::OK();
  }

  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      Customizable::GetOptionsMap(options, policy->get(), value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
    static std::once_flag loaded;
    std::call_once(loaded, [&]() {
      RegisterBuiltinFilterPolicies(*(ObjectLibrary::Default().get()), "");
    });
    status = options.registry->NewSharedObject(id, policy);
  }
  if (options.ignore_unsupported_options && status.IsNotSupported()) {
    return Status::OK();
  } else if (status.ok()) {
    status = Customizable::ConfigureNewObject(
        options, const_cast<FilterPolicy*>(policy->get()), opt_map);
  }
  return status;
}

const std::vector<std::string>& BloomLikeFilterPolicy::GetAllFixedImpls() {
  STATIC_AVOID_DESTRUCTION(std::vector<std::string>, impls){
      // Match filter_bench -impl=x ordering
      test::LegacyBloomFilterPolicy::kClassName(),
      test::FastLocalBloomFilterPolicy::kClassName(),
      test::Standard128RibbonFilterPolicy::kClassName(),
  };
  return impls;
}

}  // namespace ROCKSDB_NAMESPACE
