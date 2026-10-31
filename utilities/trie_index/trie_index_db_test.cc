//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// DB-level tests for the trie-based User Defined Index (UDI). Validates that
// a DB opened with the trie UDI factory correctly handles all operation types
// (Put, Delete, Merge, SingleDelete, PutEntity, TimedPut, DeleteRange) through
// flush and compaction, and that the resulting SST files are readable with
// correct data.
//
// These tests complement the SST-level tests in trie_index_test.cc (which use
// SstFileWriter and are limited to Put/Delete/Merge) by exercising the full
// DB path including CompactionIterator, memtable flush, and the UDI builder
// wrapper's ValueType mapping and kTypeValuePreferredSeqno handling.

#include <atomic>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/experimental.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wide_columns.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// Encodes an integer as an 8-byte big-endian key body, matching the pattern
// used by db_stress's test_batches_snapshots mode.
static std::string MakeKeyBody(int k) {
  std::string key_body(8, '\0');
  uint64_t val = static_cast<uint64_t>(k);
  for (int i = 7; i >= 0; --i) {
    key_body[i] = static_cast<char>(val & 0xff);
    val >>= 8;
  }
  return key_body;
}

static std::string MakeStressKey(int prefix, int middle, int suffix) {
  return MakeKeyBody(prefix) + MakeKeyBody(middle) + MakeKeyBody(suffix);
}

static void AppendBigEndian64(uint64_t val, std::string* key) {
  PutFixed64(key, val);
  char* int_data = &((*key)[key->size() - sizeof(uint64_t)]);
  for (size_t i = 0; i < sizeof(uint64_t) / 2; ++i) {
    std::swap(int_data[i], int_data[sizeof(uint64_t) - 1 - i]);
  }
}

// Matches db_stress's default key generation for:
//   max_key_len=3, key_len_percent_dist=1,30,69, key_window_scale_factor=10
static std::string MakeStressKey(int64_t val) {
  static constexpr uint64_t kWindow = 1000;
  static constexpr uint64_t kWeights[] = {10, 300, 690};
  static constexpr size_t kNumWeights = 3;

  uint64_t window_idx = static_cast<uint64_t>(val) / kWindow;
  uint64_t offset = static_cast<uint64_t>(val) % kWindow;
  std::string key;
  key.reserve(3 * sizeof(uint64_t));

  for (size_t level = 0; level < kNumWeights; ++level) {
    const uint64_t weight = kWeights[level];
    uint64_t pfx = (level == 0) ? window_idx * weight : 0;
    pfx += offset >= weight ? weight - 1 : offset;
    AppendBigEndian64(pfx, &key);
    if (offset < weight) {
      if (offset < weight - 1 && level + 1 < kNumWeights) {
        key.append(static_cast<size_t>(offset & 0x7), 'x');
      }
      break;
    }
    offset -= weight;
  }

  return key;
}

static std::string MakePaddedValue(int version, int key_num) {
  std::string value =
      "value_v" + std::to_string(version) + "_k" + std::to_string(key_num);
  value.append(128, static_cast<char>('a' + (version % 26)));
  return value;
}

class StressLikeVariableWidthExtractor
    : public experimental::KeySegmentsExtractor {
 public:
  const char* Name() const override { return "StressLikeVariableWidth"; }

  std::string GetId() const override { return Name(); }

  void Extract(const Slice& key_or_bound, KeyKind /*kind*/,
               Result* result) const override {
    const uint32_t len = static_cast<uint32_t>(key_or_bound.size());
    bool prev_non_zero = false;
    for (uint32_t i = 0; i < len; ++i) {
      if ((prev_non_zero && key_or_bound[i] == 0) || i + 1 == len) {
        result->segment_ends.push_back(i + 1);
      }
      prev_non_zero = key_or_bound[i] != 0;
    }
  }
};

static std::shared_ptr<experimental::SstQueryFilterConfigsManager::Factory>
MakeStressLikeSqfcFactory() {
  using experimental::MakeSharedBytewiseMinMaxSQFC;
  using experimental::SelectKeySegment;
  using experimental::SstQueryFilterConfigs;
  using experimental::SstQueryFilterConfigsManager;

  auto extractor = std::make_shared<StressLikeVariableWidthExtractor>();
  auto filter0 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(0));
  auto filter2 = MakeSharedBytewiseMinMaxSQFC(SelectKeySegment(2));

  SstQueryFilterConfigs configs{{filter0, filter2}, extractor};
  SstQueryFilterConfigsManager::Data data = {{1, {{"foo", configs}}}};

  std::shared_ptr<SstQueryFilterConfigsManager> manager;
  EXPECT_OK(SstQueryFilterConfigsManager::MakeShared(data, &manager));
  EXPECT_TRUE(manager);

  std::shared_ptr<SstQueryFilterConfigsManager::Factory> factory;
  EXPECT_OK(manager->MakeSharedFactory("foo", 1, &factory));
  EXPECT_TRUE(factory);
  return factory;
}

// Counts table builds that start the parallel compression pipeline. Tests that
// configure parallel_threads > 1 check it so they fail, rather than passing
// serially, if a builder declines the parallel protocol.
class ParallelCompressionStartCounter {
 public:
  ParallelCompressionStartCounter() {
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::MaybeStartParallelCompression:Started",
        [this](void* /*arg*/) { started_.fetch_add(1); });
    SyncPoint::GetInstance()->EnableProcessing();
  }
  ~ParallelCompressionStartCounter() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  int started() const { return started_.load(); }

 private:
  std::atomic<int> started_{0};
};

// Parameterized on IndexMode (kStandardDefault, kCustomDefault, and
// kCustomOnly) and on the trie format version written. All tests run in every
// combination to ensure full coverage.
class TrieIndexDBTest
    : public testing::TestWithParam<
          std::tuple<BlockBasedTableOptions::IndexMode, uint32_t>> {
 protected:
  bool IsPrimaryMode() const {
    return GetIndexMode() ==
               BlockBasedTableOptions::IndexMode::kCustomDefault ||
           GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly;
  }
  BlockBasedTableOptions::IndexMode GetIndexMode() const {
    return std::get<0>(GetParam());
  }
  uint32_t GetFormatVersion() const { return std::get<1>(GetParam()); }

  void SetUp() override {
    trie_factory_ = std::make_shared<TrieIndexFactory>(GetFormatVersion());
    dbname_ = test::PerThreadDBPath("trie_index_db_test");
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void TearDown() override {
    if (db_) {
      EXPECT_OK(db_->Close());
      db_.reset();
    }
    EXPECT_OK(DestroyDB(dbname_, last_options_));
  }

  // Opens a DB using the parameterized UDI mode.
  Status OpenDB(int block_size = 0) {
    return OpenDBImpl(block_size, GetIndexMode());
  }

  void VerifySharedPrefixCompaction();

  // Puts keys "key_000000" through "key_<num_keys - 1>", each with `value`,
  // and returns them in key order.
  std::vector<std::string> PutNumberedKeys(int num_keys,
                                           const std::string& value) {
    std::vector<std::string> keys;
    keys.reserve(static_cast<size_t>(num_keys));
    for (int i = 0; i < num_keys; i++) {
      char key[16];
      snprintf(key, sizeof(key), "key_%06d", i);
      keys.emplace_back(key);
      EXPECT_OK(db_->Put(WriteOptions(), keys.back(), value));
    }
    return keys;
  }

  // Explicitly opens as primary -- used by the backward compatibility test.
  Status OpenDBPrimary(int block_size = 0) {
    return OpenDBImpl(block_size,
                      BlockBasedTableOptions::IndexMode::kCustomDefault);
  }

  // Explicitly opens as secondary -- used by the backward compatibility test.
  Status OpenDBSecondary(int block_size = 0) {
    return OpenDBImpl(block_size,
                      BlockBasedTableOptions::IndexMode::kStandardDefault);
  }

  Status OpenDBImpl(int block_size,
                    BlockBasedTableOptions::IndexMode index_mode) {
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    table_options.index_mode = index_mode;
    if (block_size > 0) {
      table_options.block_size = block_size;
    }
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    last_options_ = options_;
    return DB::Open(options_, dbname_, &db_);
  }

  // Returns a ReadOptions that routes reads through the built-in binary
  // search index. In secondary mode, this is the default. In primary mode,
  // kBuiltin overrides the custom index routing.
  ReadOptions StandardIndexReadOptions() const {
    ReadOptions ro;
    if (IsPrimaryMode()) {
      ro.read_index = ReadOptions::ReadIndex::kBuiltin;
    }
    return ro;
  }

  // Returns a ReadOptions that routes reads through the trie. In primary
  // mode, a bare ReadOptions already uses the trie, so read_index
  // is not set. In secondary mode, read_index is set explicitly.
  ReadOptions TrieIndexReadOptions() const {
    ReadOptions ro;
    if (!IsPrimaryMode()) {
      ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    }
    return ro;
  }

  // Collects all visible keys via forward scan using the given ReadOptions.
  std::vector<std::string> ScanAllKeys(const ReadOptions& ro) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  }

  // Collects all visible keys via forward scan using the default index for
  // the current mode: standard index for kStandardDefault/kCustomDefault, trie
  // index for kCustomOnly (where standard-index reads fail loudly).
  std::vector<std::string> ScanAllKeys() {
    if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
      return ScanAllKeys(TrieIndexReadOptions());
    }
    return ScanAllKeys(StandardIndexReadOptions());
  }

  // Collects all visible (key, value) pairs via forward scan.
  std::vector<std::pair<std::string, std::string>> ScanAllKeyValues(
      const ReadOptions& ro) {
    std::vector<std::pair<std::string, std::string>> kvs;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      kvs.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return kvs;
  }

  // Verifies that forward scan via SeekToFirst+Next AND reverse scan via
  // SeekToLast+Prev both produce the expected key set through both the
  // standard index and the trie index.
  // In kCustomOnly mode, standard-index reads fail loudly because only a
  // footer-satisfying stub exists, so we skip the standard index comparison.
  void VerifyScanBothIndexes(const std::vector<std::string>& expected_keys) {
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      SCOPED_TRACE("standard index forward");
      ASSERT_EQ(ScanAllKeys(StandardIndexReadOptions()), expected_keys);
    }
    {
      SCOPED_TRACE("trie index forward");
      ASSERT_EQ(ScanAllKeys(TrieIndexReadOptions()), expected_keys);
    }
    // Reverse scan must produce the reversed key set.
    std::vector<std::string> expected_reverse(expected_keys.rbegin(),
                                              expected_keys.rend());
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      SCOPED_TRACE("standard index reverse");
      ASSERT_EQ(ReverseScanAllKeys(StandardIndexReadOptions()),
                expected_reverse);
    }
    {
      SCOPED_TRACE("trie index reverse");
      ASSERT_EQ(ReverseScanAllKeys(TrieIndexReadOptions()), expected_reverse);
    }
  }

  // Verifies that forward scan via SeekToFirst+Next AND reverse scan via
  // SeekToLast+Prev both produce the expected (key, value) pairs through
  // both indexes.
  void VerifyScanBothIndexes(
      const std::vector<std::pair<std::string, std::string>>& expected_kvs) {
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      SCOPED_TRACE("standard index forward");
      ASSERT_EQ(ScanAllKeyValues(StandardIndexReadOptions()), expected_kvs);
    }
    {
      SCOPED_TRACE("trie index forward");
      ASSERT_EQ(ScanAllKeyValues(TrieIndexReadOptions()), expected_kvs);
    }
    // Reverse scan must produce the reversed pairs.
    std::vector<std::pair<std::string, std::string>> expected_reverse(
        expected_kvs.rbegin(), expected_kvs.rend());
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      SCOPED_TRACE("standard index reverse");
      ASSERT_EQ(ReverseScanAllKeyValues(StandardIndexReadOptions()),
                expected_reverse);
    }
    {
      SCOPED_TRACE("trie index reverse");
      ASSERT_EQ(ReverseScanAllKeyValues(TrieIndexReadOptions()),
                expected_reverse);
    }
  }

  // Returns the list of ReadOptions to test through both indexes, skipping the
  // standard index in kCustomOnly mode, where only a footer-satisfying stub
  // exists and standard-index reads fail loudly.
  std::vector<ReadOptions> BothIndexReadOptions() const {
    if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
      return {TrieIndexReadOptions()};
    }
    return {StandardIndexReadOptions(), TrieIndexReadOptions()};
  }

  // Verifies that a point Get returns the expected value through both indexes.
  void VerifyGetBothIndexes(const std::string& key,
                            const std::string& expected_value) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::string value;
      ASSERT_OK(db_->Get(ro, key, &value));
      ASSERT_EQ(value, expected_value);
    }
  }

  // Verifies that a point Get returns NotFound through both indexes.
  void VerifyGetNotFoundBothIndexes(const std::string& key) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::string value;
      ASSERT_TRUE(db_->Get(ro, key, &value).IsNotFound());
    }
  }

  // Verifies Get with a snapshot through both indexes.
  void VerifyGetBothIndexes(const Snapshot* snap, const std::string& key,
                            const std::string& expected_value) {
    for (auto base_ro : BothIndexReadOptions()) {
      SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      base_ro.snapshot = snap;
      std::string value;
      ASSERT_OK(db_->Get(base_ro, key, &value));
      ASSERT_EQ(value, expected_value);
    }
  }

  // Verifies Get returns NotFound with a snapshot through both indexes.
  void VerifyGetNotFoundBothIndexes(const Snapshot* snap,
                                    const std::string& key) {
    for (auto base_ro : BothIndexReadOptions()) {
      SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      base_ro.snapshot = snap;
      std::string value;
      ASSERT_TRUE(db_->Get(base_ro, key, &value).IsNotFound());
    }
  }

  // Verifies that forward and reverse scans with a snapshot produce the
  // expected (key, value) pairs through both indexes.
  void VerifyScanBothIndexes(
      const Snapshot* snap,
      const std::vector<std::pair<std::string, std::string>>& expected_kvs) {
    for (auto base_ro : BothIndexReadOptions()) {
      SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index forward"
                       : "builtin index forward");
      base_ro.snapshot = snap;
      ASSERT_EQ(ScanAllKeyValues(base_ro), expected_kvs);
    }
    // Reverse scan at the same snapshot must produce reversed pairs.
    std::vector<std::pair<std::string, std::string>> expected_reverse(
        expected_kvs.rbegin(), expected_kvs.rend());
    for (auto base_ro : BothIndexReadOptions()) {
      SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index reverse"
                       : "builtin index reverse");
      base_ro.snapshot = snap;
      ASSERT_EQ(ReverseScanAllKeyValues(base_ro), expected_reverse);
    }
  }

  // Verifies that Seek to a specific key through both indexes returns the
  // same result.
  void VerifySeekBothIndexes(const std::string& seek_key,
                             const std::string& expected_key,
                             const std::string& expected_value) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->Seek(seek_key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
      ASSERT_OK(iter->status());
    }
  }

  // Verifies that Seek with a snapshot through both indexes returns the
  // same result.
  void VerifySeekBothIndexes(const Snapshot* snap, const std::string& seek_key,
                             const std::string& expected_key,
                             const std::string& expected_value) {
    for (auto base_ro : BothIndexReadOptions()) {
      SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      base_ro.snapshot = snap;
      std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
      iter->Seek(seek_key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
      ASSERT_OK(iter->status());
    }
  }

  // Opens without UDI factory (standard index only). Used to test graceful
  // degradation when reopening a DB that has UDI SSTs.
  Status OpenDBWithoutUDI(int block_size = 0) {
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    // Deliberately no user_defined_index_factory.
    if (block_size > 0) {
      table_options.block_size = block_size;
    }
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    last_options_ = options_;
    return DB::Open(options_, dbname_, &db_);
  }

  // Verify prefix-scan lockstep across `num_prefixes` iterators.
  //
  // Creates one iterator per prefix digit (0..num_prefixes-1), seeks each to
  // its prefix, then walks all in lockstep asserting key bodies match. When
  // `use_upper_bounds` is true, even-numbered iterators get an upper bound
  // set to the next prefix. When `verify_values` is true, value bodies are
  // also cross-checked.
  //
  // Returns the number of keys walked (per-prefix).
  uint64_t VerifyPrefixScanLockstep(const ReadOptions& base_ro,
                                    int num_prefixes, bool use_upper_bounds,
                                    bool verify_values,
                                    const std::string& trace_context = "") {
    std::vector<std::unique_ptr<Iterator>> iters(num_prefixes);
    std::vector<std::string> prefixes(num_prefixes);
    std::vector<Slice> prefix_slices(num_prefixes);
    std::vector<ReadOptions> ro_copies(num_prefixes);
    std::vector<std::string> upper_bounds(num_prefixes);
    std::vector<Slice> ub_slices(num_prefixes);

    for (int d = 0; d < num_prefixes; ++d) {
      prefixes[d] = std::to_string(d);
      prefix_slices[d] = Slice(prefixes[d]);
      ro_copies[d] = base_ro;
      if (use_upper_bounds && d % 2 == 0) {
        upper_bounds[d] = prefixes[d];
        upper_bounds[d].back()++;
        ub_slices[d] = upper_bounds[d];
        ro_copies[d].iterate_upper_bound = &ub_slices[d];
      }
      iters[d].reset(db_->NewIterator(ro_copies[d]));
      iters[d]->Seek(prefix_slices[d]);
    }

    uint64_t count = 0;
    while (iters[0]->Valid() && iters[0]->key().starts_with(prefix_slices[0])) {
      count++;
      std::vector<std::string> keys(num_prefixes);
      std::vector<std::string> values(num_prefixes);
      for (int d = 0; d < num_prefixes; ++d) {
        EXPECT_TRUE(iters[d]->Valid())
            << trace_context << " iter " << d << " invalid at step " << count;
        EXPECT_TRUE(iters[d]->key().starts_with(prefix_slices[d]))
            << trace_context << " iter " << d << " out of prefix at step "
            << count;
        if (!iters[d]->Valid()) {
          return count;
        }
        keys[d] = iters[d]->key().ToString();
        values[d] = iters[d]->value().ToString();
      }

      std::string key0_body = keys[0].substr(1);
      for (int d = 1; d < num_prefixes; ++d) {
        EXPECT_EQ(key0_body, keys[d].substr(1))
            << trace_context << " key body mismatch at step " << count
            << " iter " << d;
      }

      if (verify_values) {
        std::string val0 = values[0];
        if (!val0.empty()) {
          val0.pop_back();
        }
        for (int d = 1; d < num_prefixes; ++d) {
          std::string vald = values[d];
          if (!vald.empty()) {
            vald.pop_back();
          }
          EXPECT_EQ(val0, vald) << trace_context << " value mismatch at step "
                                << count << " iter " << d;
        }
      }

      for (int d = 0; d < num_prefixes; ++d) {
        iters[d]->Next();
      }
    }

    EXPECT_OK(iters[0]->status());
    for (int d = 1; d < num_prefixes; ++d) {
      EXPECT_TRUE(!iters[d]->Valid() ||
                  !iters[d]->key().starts_with(prefix_slices[d]))
          << trace_context << " iter " << d
          << " still has keys after iter 0 finished";
      EXPECT_OK(iters[d]->status());
    }

    return count;
  }

  // Collects all visible keys via reverse scan (SeekToLast + Prev).
  std::vector<std::string> ReverseScanAllKeys(const ReadOptions& ro) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    for (; iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  }

  // Collects all visible (key, value) pairs via reverse scan.
  std::vector<std::pair<std::string, std::string>> ReverseScanAllKeyValues(
      const ReadOptions& ro) {
    std::vector<std::pair<std::string, std::string>> kvs;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    for (; iter->Valid(); iter->Prev()) {
      kvs.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return kvs;
  }

  // Verifies SeekForPrev through both indexes.
  void VerifySeekForPrevBothIndexes(const std::string& target,
                                    const std::string& expected_key,
                                    const std::string& expected_value) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->SeekForPrev(target);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
      ASSERT_OK(iter->status());
    }
  }

  // Verifies SeekForPrev returns invalid (target before all keys).
  void VerifySeekForPrevNotFoundBothIndexes(const std::string& target) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->SeekForPrev(target);
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  }

  // Writes sequential keys "key_NNNN" with values "val_NNNN" for i in
  // [start, end).
  void WriteSequentialKeys(int start, int end) {
    for (int i = start; i < end; i++) {
      char key[16];
      char val[16];
      snprintf(key, sizeof(key), "key_%04d", i);
      snprintf(val, sizeof(val), "val_%04d", i);
      ASSERT_OK(db_->Put(WriteOptions(), key, val));
    }
  }

  // Forward prefix scan: collect all keys with the given prefix.
  std::vector<std::string> PrefixScanKeys(const ReadOptions& ro,
                                          const std::string& prefix) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (iter->key().ToString().substr(0, prefix.size()) != prefix) {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  }

  // Reverse prefix scan: collect all keys with the given prefix via
  // SeekForPrev from prefix + "\xff".
  std::vector<std::string> ReversePrefixScanKeys(const ReadOptions& ro,
                                                 const std::string& prefix) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->SeekForPrev(prefix + "\xff"); iter->Valid(); iter->Prev()) {
      if (iter->key().ToString().substr(0, prefix.size()) != prefix) {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  }

  std::shared_ptr<TrieIndexFactory> trie_factory_;
  std::string dbname_;
  Options options_;
  Options last_options_;
  std::unique_ptr<DB> db_;
};

// ============================================================================
// Flush tests
// ============================================================================

TEST_P(TrieIndexDBTest, CompactionTailSizeIsUpperBound) {
  options_.disable_auto_compactions = true;
  options_.compression = kNoCompression;
  options_.target_file_size_is_upper_bound = true;
  ASSERT_OK(OpenDB(1));
  const std::string key(1024, 'a');
  for (const char* value : {"old", "new"}) {
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  std::string value;
  ASSERT_OK(db_->Get(TrieIndexReadOptions(), key, &value));
  EXPECT_EQ(value, "new");
}

void TrieIndexDBTest::VerifySharedPrefixCompaction() {
  options_.disable_auto_compactions = true;
  options_.target_file_size_is_upper_bound = true;
  options_.target_file_size_base = 256 << 10;
  ASSERT_OK(OpenDB(512));
  const std::string prefix(512, 'p');
  constexpr int kKeyCount = 2048;
  for (char version : {'a', 'b'}) {
    for (int i = 0; i < kKeyCount; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), prefix + MakeKeyBody(i),
                         std::string(64, version)));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  // About 1.2 MiB of data (plus a similarly sized standard index in dual
  // modes) should fit in a small number of 256 KiB SSTs. Charging the shared
  // prefix once per block used to produce dozens.
  EXPECT_GT(files.size(), 1U);
  EXPECT_LE(files.size(), 12U);
  for (const auto& file : files) {
    EXPECT_LE(file.size, options_.target_file_size_base);
  }
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB(512));
  for (int i = 0; i < kKeyCount; ++i) {
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes(prefix + MakeKeyBody(i), std::string(64, 'b')));
  }
}

TEST_P(TrieIndexDBTest, SharedPrefixDoesNotCutCompactionFilesEarly) {
  options_.compression = kNoCompression;
  ASSERT_NO_FATAL_FAILURE(VerifySharedPrefixCompaction());
}

TEST_P(TrieIndexDBTest, SharedPrefixWithParallelCompression) {
  if (!Zlib_Supported()) {
    ROCKSDB_GTEST_SKIP("Zlib not linked into this build");
    return;
  }
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  // Exercise the compression pipeline but retain uncompressed blocks so
  // the file-size assertions match the serial case regardless of codec ratio.
  options_.compression_opts.max_compressed_bytes_per_kb = 1;
  ParallelCompressionStartCounter parallel_starts;
  ASSERT_NO_FATAL_FAILURE(VerifySharedPrefixCompaction());
  EXPECT_GT(parallel_starts.started(), 0);
}

TEST_P(TrieIndexDBTest, FlushWithAllOperationTypes) {
  // Write every supported operation type via the DB API, flush, and verify
  // reads return correct results through both the standard index and the trie
  // UDI. This exercises the full path from memtable through
  // CompactionIterator, BlockBasedTableBuilder, and the UDI builder wrapper's
  // MapToUDIValueType for each internal ValueType.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // kTypeValue
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "val_put"));
  // kTypeMerge
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "val_merge"));
  // kTypeDeletion (bare tombstone -- no prior value for this key)
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03_del"));
  // kTypeSingleDeletion (preceded by a Put; both cancel out with no snapshot)
  ASSERT_OK(db_->Put(WriteOptions(), "key_04_sdel", "val_sdel"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_04_sdel"));
  // kTypeWideColumnEntity (with a default column so Get() returns a value)
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_05_entity",
                           WideColumns{{"", "default_val"}, {"col1", "val1"}}));
  // Another kTypeValue to anchor the end of the key range
  ASSERT_OK(db_->Put(WriteOptions(), "key_06_put", "val_put2"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Scan via both indexes. Expected visible keys after flush:
  //   key_01_put    -- Put (visible)
  //   key_02_merge  -- Merge single operand (visible)
  //   key_03_del    -- bare Delete tombstone (hidden by DBIter)
  //   key_04_sdel   -- Put + SingleDelete cancel out (hidden)
  //   key_05_entity -- PutEntity (visible)
  //   key_06_put    -- Put (visible)
  {
    std::vector<std::string> expected = {"key_01_put", "key_02_merge",
                                         "key_05_entity", "key_06_put"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  // Point lookups via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_merge", "val_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03_del"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04_sdel"));
  // PutEntity: Get() returns the value of the default column ("").
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05_entity", "default_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06_put", "val_put2"));
}

TEST_P(TrieIndexDBTest, TimedPutFlush) {
  // TimedPut produces kTypeValuePreferredSeqno entries during flush when
  // preclude_last_level_data_seconds > 0. The UDI wrapper strips the packed
  // preferred seqno suffix via ParsePackedValueForValue() before forwarding
  // to the plugin builder. This test verifies that path end-to-end through
  // both the standard index and the trie UDI.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.compaction_style = kCompactionStyleUniversal;
  // Required for kTypeValuePreferredSeqno to survive the flush path: the
  // seqno_to_time_mapping must be available so a preferred seqno can be
  // computed. With write_unix_time=0, GetProximalSeqnoBeforeTime(0) returns 0,
  // which is < any real seqno, so the entry stays as kTypeValuePreferredSeqno.
  options_.preclude_last_level_data_seconds = 10000;
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Regular Put alongside the TimedPut to verify they coexist.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "val_put"));

  // TimedPut via WriteBatch (there is no DB::TimedPut method).
  {
    WriteBatch wb;
    ASSERT_OK(wb.TimedPut(db_->DefaultColumnFamily(), "key_02_timed",
                          "val_timed", /*write_unix_time=*/0));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }

  // Merge to verify mixed types work with TimedPut in the same flush.
  ASSERT_OK(db_->Merge(WriteOptions(), "key_03_merge", "val_merge"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookups via both indexes -- the packed seqno must be transparent.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_timed", "val_timed"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03_merge", "val_merge"));

  // Scan via both indexes -- all three keys visible in order.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "val_put"},
        {"key_02_timed", "val_timed"},
        {"key_03_merge", "val_merge"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// Compaction tests
// ============================================================================

TEST_P(TrieIndexDBTest, CompactionWithMixedOpsAndSnapshots) {
  // Multiple flushes followed by compaction with a snapshot held. The snapshot
  // forces compaction to preserve multiple versions of the same user key,
  // exercising the UDI builder's handling of duplicate user keys with different
  // sequence numbers and value types. Verified through both indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: initial values.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_bb", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_cc", "m1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions so compaction preserves them.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: updates that create new versions.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "v2"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_bb"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_cc", "m2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact all levels. Both versions of each key are preserved because the
  // snapshot prevents garbage collection of the older versions.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view (no snapshot): key_aa=v2, key_bb deleted, key_cc="m1,m2".
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_aa", "v2"}, {"key_cc", "m1,m2"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_aa", "v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_cc", "m1,m2"));

  // Snapshot view: key_aa=v1, key_bb=v1, key_cc="m1".
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_aa", "v1"}, {"key_bb", "v1"}, {"key_cc", "m1"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snap, expected));
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_aa", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_bb", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_cc", "m1"));

  db_->ReleaseSnapshot(snap);
}

TEST_P(TrieIndexDBTest, CompactionWithAllOperationTypes) {
  // Exercises all operation types (Put, Delete, Merge, SingleDelete, PutEntity)
  // across two flushes with a snapshot, then compacts. Verified through both
  // indexes. This ensures the UDI builder handles the full range of value types
  // in compaction output, and that both the current and snapshot views are
  // correct.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: initial values with diverse types.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "m1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_03_sd_target", "sd_val"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_04_entity", WideColumns{{"", "e1"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_05_del_target", "del_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: updates each key with a different operation type.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "v2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "m2"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_03_sd_target"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_04_entity", WideColumns{{"", "e2"}}));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_05_del_target"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view via both indexes: key_01=v2, key_02="m1,m2", key_03 SD'd,
  // key_04=e2, key_05 deleted.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_merge", "m1,m2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03_sd_target"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04_entity", "e2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_05_del_target"));

  // Current view scan via both indexes: only key_01, key_02, key_04 visible.
  {
    std::vector<std::string> expected = {"key_01_put", "key_02_merge",
                                         "key_04_entity"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  // Snapshot view via both indexes: all original flush 1 values visible.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_01_put", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_02_merge", "m1"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_03_sd_target", "sd_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_04_entity", "e1"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_05_del_target", "del_val"));

  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "v1"},
        {"key_02_merge", "m1"},
        {"key_03_sd_target", "sd_val"},
        {"key_04_entity", "e1"},
        {"key_05_del_target", "del_val"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snap, expected));
  }

  db_->ReleaseSnapshot(snap);
}

TEST_P(TrieIndexDBTest, TimedPutCompaction) {
  // Verifies that kTypeValuePreferredSeqno entries survive compaction and the
  // UDI builder correctly strips the packed seqno during compaction output.
  // Verified through both indexes.
  options_.compaction_style = kCompactionStyleUniversal;
  options_.preclude_last_level_data_seconds = 10000;
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: TimedPut + regular Put.
  {
    WriteBatch wb;
    ASSERT_OK(wb.TimedPut(db_->DefaultColumnFamily(), "key_01_timed",
                          "timed_v1", /*write_unix_time=*/0));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_put", "put_v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: overwrite both keys with regular Puts.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_timed", "put_v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_put", "put_v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact: the snapshot forces both versions of key_01_timed to be kept.
  // The older version is kTypeValuePreferredSeqno with packed seqno.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view via both indexes: both keys have the newer value.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_timed", "put_v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_put", "put_v2"));
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "put_v2"}, {"key_02_put", "put_v2"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  // Snapshot view via both indexes: key_01 has the original TimedPut value
  // (packed seqno must be transparent), key_02 has its original value.
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_01_timed", "timed_v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_02_put", "put_v1"));
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "timed_v1"}, {"key_02_put", "put_v1"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snap, expected));
  }

  db_->ReleaseSnapshot(snap);
}

TEST_P(TrieIndexDBTest, CrossFlushSingleDelete) {
  // Verifies that a SingleDelete in a later SST correctly cancels a Put from
  // an earlier SST after compaction with the trie UDI active. Verified through
  // both indexes.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: Puts.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "val_aa"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_bb", "val_bb"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_cc", "val_cc"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Flush 2: SingleDelete key_bb (targets the Put from flush 1).
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_bb"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Before compaction: key_bb is already hidden by the merging iterator.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));

  // After compaction: SingleDelete + Put fully cancel out, key_bb is gone.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));

  // Remaining keys unaffected via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_aa", "val_aa"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_cc", "val_cc"));

  {
    std::vector<std::string> expected = {"key_aa", "key_cc"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// Iteration tests
// ============================================================================

TEST_P(TrieIndexDBTest, ReverseIteration) {
  // Verifies that reverse iteration (SeekToLast, Prev, SeekForPrev) works
  // correctly with mixed operation types through BOTH the standard binary
  // search index and the trie UDI index.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "m1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "v4"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "e5"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "v6"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Visible keys: key_01, key_02, key_04, key_05, key_06 (key_03 deleted).

  // Scan via both indexes.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01", "v1"},
        {"key_02", "m1"},
        {"key_04", "v4"},
        {"key_05", "e5"},
        {"key_06", "v6"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  // Point lookups via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "m1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04", "v4"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "e5"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "v6"));

  // Seek via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("key_04", "key_04", "v4"));
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("key_05", "key_05", "e5"));

  // SeekForPrev to an exact visible key via both indexes.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekForPrevBothIndexes("key_04", "key_04", "v4"));

  // SeekForPrev to a deleted key -- should land on key_02.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekForPrevBothIndexes("key_03", "key_02", "m1"));

  // SeekForPrev to a key between existing keys.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekForPrevBothIndexes("key_04_5", "key_04", "v4"));

  // SeekForPrev before all keys -- should be invalid.
  ASSERT_NO_FATAL_FAILURE(VerifySeekForPrevNotFoundBothIndexes("key_00"));

  // Prev from a Seek position in the middle of the range -- both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("key_05");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_05");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_04");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_02");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_01");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// ============================================================================
// DeleteRange test
// ============================================================================

TEST_P(TrieIndexDBTest, DeleteRangeWithTrieUDI) {
  // Verifies that DeleteRange (kTypeRangeDeletion) works correctly alongside
  // the trie UDI. Range deletions go to a separate range_del_block (not
  // through OnKeyAdded), but we verify that reads correctly filter out
  // range-deleted keys when the trie UDI is active. Scans and point
  // lookups verified through both indexes.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  for (int i = 1; i <= 10; i++) {
    char key_buf[16];
    char val_buf[16];
    snprintf(key_buf, sizeof(key_buf), "key_%02d", i);
    snprintf(val_buf, sizeof(val_buf), "val_%02d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key_buf, val_buf));
  }

  // DeleteRange [key_04, key_08) -- deletes key_04 through key_07.
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "key_04", "key_08"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Scan via both indexes: key_01..key_03 and key_08..key_10 visible.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_03",
                                         "key_08", "key_09", "key_10"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  // Point lookups via both indexes for deleted keys.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_07"));

  // Point lookups via both indexes for surviving keys at boundaries.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "val_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_08", "val_08"));
}

// ============================================================================
// DB reopen test
// ============================================================================

TEST_P(TrieIndexDBTest, ReopenWithMixedOperationTypes) {
  // Writes all operation types, flushes, closes the DB, reopens, and verifies
  // all data reads correctly from cold SST files through both indexes. This
  // exercises the read path on a freshly opened DB where no memtable data
  // exists.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "val_put"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "val_merge"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "sd_target"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_04"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "entity_val"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "val_put2"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Close the DB. All data is now only in SST files.
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen with the same trie UDI configuration.
  ASSERT_OK(OpenDB());

  // Point lookups on cold data via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "val_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "entity_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "val_put2"));

  // Scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_05",
                                         "key_06"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// Ingest external file test
// ============================================================================

TEST_P(TrieIndexDBTest, IngestExternalFileWithTrieUDI) {
  // Creates an SST with SstFileWriter using the trie UDI, then ingests it
  // into a live DB that also has trie UDI configured. Verifies that both the
  // existing DB data and the ingested data are correctly readable through both
  // indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Write some data directly to the DB and flush.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "db_val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_05", "db_val5"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Create an SST with SstFileWriter using trie UDI, containing mixed ops.
  std::string sst_path = dbname_ + "/ingest.sst";
  {
    Options sst_options;
    sst_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    sst_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileWriter writer(EnvOptions(), sst_options);
    ASSERT_OK(writer.Open(sst_path));
    ASSERT_OK(writer.Put("key_02", "ingest_val2"));
    ASSERT_OK(writer.Merge("key_03", "ingest_merge3"));
    ASSERT_OK(writer.Delete("key_04"));
    ASSERT_OK(writer.Put("key_06", "ingest_val6"));
    ASSERT_OK(writer.Finish());
  }

  // Ingest into the live DB.
  IngestExternalFileOptions ingest_opts;
  ASSERT_OK(db_->IngestExternalFile({sst_path}, ingest_opts));

  // Point lookups via both indexes -- combined DB + ingested data.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "db_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "ingest_val2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "ingest_merge3"));
  // key_04: ingested Delete tombstone, no prior value -- NotFound.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "db_val5"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "ingest_val6"));

  // Scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_03", "key_05",
                                         "key_06"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// WriteBatch test
// ============================================================================

TEST_P(TrieIndexDBTest, WriteBatchWithMixedOperations) {
  // Verifies that a single WriteBatch containing multiple operation types
  // (Put, Delete, Merge, SingleDelete, PutEntity) works correctly with the
  // trie UDI. Verified through both indexes. Real-world workloads typically
  // batch multiple operations.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Pre-populate a key that the batch's Delete will target.
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_del", "pre_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Build a WriteBatch with all operation types.
  WriteBatch wb;
  ASSERT_OK(wb.Put(db_->DefaultColumnFamily(), "key_01_put", "batch_put"));
  ASSERT_OK(wb.Delete(db_->DefaultColumnFamily(), "key_02_del"));
  ASSERT_OK(wb.Merge(db_->DefaultColumnFamily(), "key_03_merge", "batch_m"));
  // Put + SingleDelete within the same batch -- they cancel out.
  ASSERT_OK(wb.Put(db_->DefaultColumnFamily(), "key_04_sd", "sd_target"));
  ASSERT_OK(wb.SingleDelete(db_->DefaultColumnFamily(), "key_04_sd"));
  ASSERT_OK(wb.PutEntity(db_->DefaultColumnFamily(), "key_05_entity",
                         WideColumns{{"", "batch_entity"}}));

  ASSERT_OK(db_->Write(WriteOptions(), &wb));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookups via both indexes. Expected visible keys: key_01 (Put),
  // key_03 (Merge), key_05 (PutEntity). key_02 deleted, key_04
  // Put+SingleDelete cancel.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "batch_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_02_del"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03_merge", "batch_m"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04_sd"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes("key_05_entity", "batch_entity"));

  // Scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01_put", "key_03_merge",
                                         "key_05_entity"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// Large-scale test
// ============================================================================

TEST_P(TrieIndexDBTest, LargeMixedOperationsAcrossBlocks) {
  // Large-scale test with many keys of different operation types and a small
  // block size. This stresses block boundary handling in the trie UDI across
  // Put, Delete, Merge, SingleDelete, and PutEntity entries. Verified through
  // both indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  // Small block size forces many data blocks, exercising the trie index's
  // AddIndexEntry at frequent block boundaries.
  ASSERT_OK(OpenDB(/*block_size=*/128));

  const int kNumKeys = 500;
  // Track keys expected to be visible after flush (non-deleted, non-SD'd).
  std::vector<std::string> expected_visible;

  for (int i = 0; i < kNumKeys; i++) {
    char key_buf[32];
    snprintf(key_buf, sizeof(key_buf), "key_%06d", i);
    std::string key(key_buf);

    // Distribute operation types:
    //   i%10 in [0,3] -> Put         (40%)
    //   i%10 in [4,5] -> Delete      (20%)
    //   i%10 in [6,7] -> Merge       (20%)
    //   i%10 == 8     -> SingleDelete (10%, preceded by Put -- both cancel)
    //   i%10 == 9     -> PutEntity   (10%)
    int type = i % 10;
    if (type <= 3) {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "val_%06d", i);
      ASSERT_OK(db_->Put(WriteOptions(), key, val_buf));
      expected_visible.push_back(key);
    } else if (type <= 5) {
      ASSERT_OK(db_->Delete(WriteOptions(), key));
      // Bare tombstone -- not visible.
    } else if (type <= 7) {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "merge_%06d", i);
      ASSERT_OK(db_->Merge(WriteOptions(), key, val_buf));
      expected_visible.push_back(key);
    } else if (type == 8) {
      ASSERT_OK(db_->Put(WriteOptions(), key, "to_be_deleted"));
      ASSERT_OK(db_->SingleDelete(WriteOptions(), key));
      // Put + SingleDelete cancel -- not visible.
    } else {
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                               WideColumns{{"", "entity_val"}}));
      expected_visible.push_back(key);
    }
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Scan via both indexes -- verify exactly the expected visible keys.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected_visible));

  // Spot-check: Seek to every 10th visible key via both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (size_t i = 0; i < expected_visible.size(); i += 10) {
      iter->Seek(expected_visible[i]);
      ASSERT_TRUE(iter->Valid()) << "Seek failed for " << expected_visible[i];
      ASSERT_EQ(iter->key().ToString(), expected_visible[i]);
    }
    ASSERT_OK(iter->status());
  }
}

// ============================================================================
// Seqno side-table tests (same user key spanning data block boundaries)
// ============================================================================

TEST_P(TrieIndexDBTest, SameUserKeyAcrossBlockBoundaries) {
  // Forces the same user key to appear in multiple data blocks by writing many
  // versions with snapshots held to prevent garbage collection, using a tiny
  // block_size. This exercises the trie's seqno side-table: the trie stores
  // only one separator per user key, and the side-table records the seqno +
  // overflow block count so that Seek() can find the correct data block for
  // each version.
  //
  // Without the seqno side-table, reads through the trie index return
  // incorrect data when multiple versions of the same key span different
  // data blocks, because the trie cannot distinguish which block contains
  // which version.
  options_.disable_auto_compactions = true;
  // Tiny block_size (64 bytes) forces each version of the key into its own
  // data block, creating same-user-key block boundaries that the trie must
  // handle via the seqno side-table.
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write multiple versions of the same key, holding snapshots so all versions
  // survive the flush to a single SST file.
  const std::string key = "same_key";
  constexpr int kNumVersions = 10;
  std::vector<const Snapshot*> snaps;
  for (int i = 0; i < kNumVersions; i++) {
    std::string val = "ver_" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current view: latest version visible.
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(key, "ver_" + std::to_string(kNumVersions - 1)));

  // Each snapshot should see the version written at or before its creation.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[i], key, expected_val));
  }

  // Forward scan with each snapshot should return exactly one key with the
  // correct version.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    std::vector<std::pair<std::string, std::string>> expected = {
        {key, expected_val}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snaps[i], expected));
  }

  // Seek to the key through the trie index with each snapshot -- the trie's
  // post-seek correction must advance through overflow blocks to find the
  // correct version for each seqno.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    SCOPED_TRACE("snap=" + std::to_string(i));
    ASSERT_NO_FATAL_FAILURE(
        VerifySeekBothIndexes(snaps[i], key, key, expected_val));
  }

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

TEST_P(TrieIndexDBTest, SameUserKeyPutThenDeleteAcrossBlocks) {
  // Same user key with a Put followed by a Delete, where both entries land in
  // different data blocks. A snapshot pins the Put version. After compaction,
  // the current view shows NotFound while the snapshot view shows the Put.
  // This tests the seqno side-table with mixed value types for the same key.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write a Put, take snapshot, then Delete.
  ASSERT_OK(db_->Put(WriteOptions(), "del_key", "put_value"));
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(db_->Delete(WriteOptions(), "del_key"));

  // Add surrounding keys to create more data blocks and exercise trie
  // separators around the duplicated key.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_before", "before_val"));
  ASSERT_OK(db_->Put(WriteOptions(), "zzz_after", "after_val"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current view: del_key is deleted.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_key"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("aaa_before", "before_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("zzz_after", "after_val"));

  // Snapshot view: del_key is visible with the Put value.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "del_key", "put_value"));

  // Seek to del_key with snapshot through both indexes.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes(snap, "del_key", "del_key", "put_value"));

  // Compact to merge the Put + Delete. Snapshot prevents GC of the Put.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // After compaction, same behavior.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_key"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "del_key", "put_value"));

  db_->ReleaseSnapshot(snap);
}

TEST_P(TrieIndexDBTest, SameUserKeyManyVersionsSeekCorrectness) {
  // Writes many versions of three different keys (with snapshots), using a
  // tiny block_size to force same-user-key block boundaries. Verifies that
  // Seek + Get through the trie index returns the correct version for each
  // snapshot, testing the seqno side-table's overflow handling with multiple
  // keys interleaved in the SST.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/64));

  const std::vector<std::string> keys = {"key_aaa", "key_mmm", "key_zzz"};
  constexpr int kVersionsPerKey = 8;
  // snaps[v] is taken after writing version v of all keys.
  std::vector<const Snapshot*> snaps;

  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string val = k + "_v" + std::to_string(v);
      ASSERT_OK(db_->Put(WriteOptions(), k, val));
    }
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify each snapshot sees the correct version of each key via Get and Seek.
  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string expected_val = k + "_v" + std::to_string(v);
      SCOPED_TRACE("key=" + k + " v=" + std::to_string(v));
      ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[v], k, expected_val));
      ASSERT_NO_FATAL_FAILURE(
          VerifySeekBothIndexes(snaps[v], k, k, expected_val));
    }
  }

  // Compact and re-verify. Compaction must preserve all versions because
  // snapshots are held.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string expected_val = k + "_v" + std::to_string(v);
      ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[v], k, expected_val));
    }
  }

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

TEST_P(TrieIndexDBTest,
       AutoPrefixBoundsSnapshotIteratorMatchesStandardIndexWithHiddenVersions) {
  options_.compression = kNoCompression;
  options_.disable_auto_compactions = true;
  options_.max_sequential_skip_in_iterations = 2;
  options_.prefix_extractor.reset(NewFixedPrefixTransform(8));
  ASSERT_OK(OpenDB(/*block_size=*/64));

  auto large_value = [](char ch) { return std::string(96, ch); };

  const std::string lower_bound = MakeStressKey(0x45, 0x12B, 0x00A3);
  const std::string before = MakeStressKey(0xB3, 0x12B, 0x012E);
  const std::string repeated = MakeStressKey(0xB3, 0x12B, 0x012F);
  const std::string expected_1 = MakeStressKey(0xB3, 0x12B, 0x0131);
  const std::string expected_2 = MakeStressKey(0xB3, 0x12B, 0x0132);
  const std::string upper_bound = MakeStressKey(0x1D5, 0x12B, 0x0200);

  ASSERT_OK(db_->Put(WriteOptions(), before, large_value('a')));
  for (int version = 0; version < 8; ++version) {
    ASSERT_OK(db_->Put(WriteOptions(), repeated,
                       large_value(static_cast<char>('b' + version))));
  }
  ASSERT_OK(db_->Put(WriteOptions(), expected_1, large_value('m')));
  ASSERT_OK(db_->Put(WriteOptions(), expected_2, large_value('n')));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snapshot = db_->GetSnapshot();
  const Slice lower_bound_slice(lower_bound);
  const Slice upper_bound_slice(upper_bound);

  const auto build_read_options = [&](ReadOptions::ReadIndex read_index) {
    ReadOptions ro;
    ro.snapshot = snapshot;
    ro.auto_prefix_mode = true;
    ro.allow_unprepared_value = true;
    ro.auto_refresh_iterator_with_snapshot = true;
    ro.iterate_lower_bound = &lower_bound_slice;
    ro.iterate_upper_bound = &upper_bound_slice;
    ro.read_index = read_index;
    return ro;
  };

  const std::vector<std::pair<std::string, std::string>> expected = {
      {before, large_value('a')},
      {repeated, large_value('i')},
      {expected_1, large_value('m')},
      {expected_2, large_value('n')},
  };

  const ReadOptions::ReadIndex read_indices[] = {
      ReadOptions::ReadIndex::kDefault, ReadOptions::ReadIndex::kPreferCustom};
  for (const auto read_index : read_indices) {
    SCOPED_TRACE(read_index == ReadOptions::ReadIndex::kDefault
                     ? "builtin index"
                     : "custom index");
    const ReadOptions ro = build_read_options(read_index);
    ASSERT_EQ(ScanAllKeyValues(ro), expected);

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek(before);
    for (const auto& [expected_key, expected_value] : expected) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  db_->ReleaseSnapshot(snapshot);
}

TEST_P(TrieIndexDBTest, AutoRefreshSnapshotNextAcrossSameUserKeyBoundaries) {
  options_.create_if_missing = true;
  options_.disable_auto_compactions = true;

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  table_options.block_size = 64;
  table_options.separate_key_value_in_data_block = true;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  const std::vector<std::string> keys = {"key_aaa", "key_mmm", "key_zzz"};
  constexpr int kVersionsPerKey = 12;
  std::vector<const Snapshot*> snaps;

  for (int v = 0; v < kVersionsPerKey; ++v) {
    for (const auto& key : keys) {
      std::string value =
          key + "_v" + std::to_string(v) + "_padding_to_force_more_data_blocks";
      ASSERT_OK(db_->Put(WriteOptions(), key, value));
    }
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snap = snaps.back();
  const std::string expected_mmm = "key_mmm_v" +
                                   std::to_string(kVersionsPerKey - 1) +
                                   "_padding_to_force_more_data_blocks";

  ReadOptions std_ro;
  std_ro.snapshot = snap;
  std_ro.auto_refresh_iterator_with_snapshot = true;
  std_ro.allow_unprepared_value = true;

  ReadOptions trie_ro = std_ro;
  trie_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;

  std::unique_ptr<Iterator> std_iter(db_->NewIterator(std_ro));
  std::unique_ptr<Iterator> trie_iter(db_->NewIterator(trie_ro));

  std_iter->Seek("key_aaa");
  trie_iter->Seek("key_aaa");
  ASSERT_TRUE(std_iter->Valid());
  ASSERT_TRUE(trie_iter->Valid());
  ASSERT_EQ(std_iter->key().ToString(), "key_aaa");
  ASSERT_EQ(trie_iter->key().ToString(), "key_aaa");
  ASSERT_TRUE(std_iter->PrepareValue());
  ASSERT_TRUE(trie_iter->PrepareValue());

  std::string std_sv_before;
  std::string trie_sv_before;
  ASSERT_OK(std_iter->GetProperty("rocksdb.iterator.super-version-number",
                                  &std_sv_before));
  ASSERT_OK(trie_iter->GetProperty("rocksdb.iterator.super-version-number",
                                   &trie_sv_before));

  // Bump SuperVersion after the iterators are already positioned so the next
  // Next() must reconcile against the held snapshot.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "key_after_" + std::to_string(i),
                       "value_after_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  std_iter->Next();
  trie_iter->Next();
  ASSERT_TRUE(std_iter->Valid());
  ASSERT_TRUE(trie_iter->Valid());
  ASSERT_EQ(std_iter->key().ToString(), "key_mmm");
  ASSERT_EQ(trie_iter->key().ToString(), "key_mmm");
  ASSERT_TRUE(std_iter->PrepareValue());
  ASSERT_TRUE(trie_iter->PrepareValue());
  ASSERT_EQ(std_iter->value().ToString(), expected_mmm);
  ASSERT_EQ(trie_iter->value().ToString(), expected_mmm);

  std::string std_sv_after;
  std::string trie_sv_after;
  ASSERT_OK(std_iter->GetProperty("rocksdb.iterator.super-version-number",
                                  &std_sv_after));
  ASSERT_OK(trie_iter->GetProperty("rocksdb.iterator.super-version-number",
                                   &trie_sv_after));
  ASSERT_LT(std::stoull(std_sv_before), std::stoull(std_sv_after));
  ASSERT_LT(std::stoull(trie_sv_before), std::stoull(trie_sv_after));

  for (auto* held : snaps) {
    db_->ReleaseSnapshot(held);
  }
}

TEST_P(TrieIndexDBTest,
       AutoRefreshSnapshotNextAfterCompactionAcrossSameUserKeyBoundaries) {
  options_.create_if_missing = true;
  options_.disable_auto_compactions = true;

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  table_options.block_size = 64;
  table_options.separate_key_value_in_data_block = true;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  const std::vector<std::string> keys = {"key_aaa", "key_mmm", "key_zzz"};
  constexpr int kVersionsPerKey = 12;
  std::vector<const Snapshot*> snaps;

  for (int v = 0; v < kVersionsPerKey; ++v) {
    for (const auto& key : keys) {
      std::string value =
          key + "_v" + std::to_string(v) + "_padding_to_force_more_data_blocks";
      ASSERT_OK(db_->Put(WriteOptions(), key, value));
    }
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snap = snaps.back();
  const std::string expected_mmm = "key_mmm_v" +
                                   std::to_string(kVersionsPerKey - 1) +
                                   "_padding_to_force_more_data_blocks";

  ReadOptions std_ro;
  std_ro.snapshot = snap;
  std_ro.auto_refresh_iterator_with_snapshot = true;
  std_ro.allow_unprepared_value = true;

  ReadOptions trie_ro = std_ro;
  trie_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;

  std::unique_ptr<Iterator> std_iter(db_->NewIterator(std_ro));
  std::unique_ptr<Iterator> trie_iter(db_->NewIterator(trie_ro));

  std_iter->Seek("key_aaa");
  trie_iter->Seek("key_aaa");
  ASSERT_TRUE(std_iter->Valid());
  ASSERT_TRUE(trie_iter->Valid());
  ASSERT_TRUE(std_iter->PrepareValue());
  ASSERT_TRUE(trie_iter->PrepareValue());

  std::string std_sv_before;
  std::string trie_sv_before;
  ASSERT_OK(std_iter->GetProperty("rocksdb.iterator.super-version-number",
                                  &std_sv_before));
  ASSERT_OK(trie_iter->GetProperty("rocksdb.iterator.super-version-number",
                                   &trie_sv_before));

  // Rewrite the SST containing the multi-version keys while the iterators are
  // open. The held snapshot keeps all versions live across compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std_iter->Next();
  trie_iter->Next();
  ASSERT_TRUE(std_iter->Valid());
  ASSERT_TRUE(trie_iter->Valid());
  ASSERT_EQ(std_iter->key().ToString(), "key_mmm");
  ASSERT_EQ(trie_iter->key().ToString(), "key_mmm");
  ASSERT_TRUE(std_iter->PrepareValue());
  ASSERT_TRUE(trie_iter->PrepareValue());
  ASSERT_EQ(std_iter->value().ToString(), expected_mmm);
  ASSERT_EQ(trie_iter->value().ToString(), expected_mmm);

  std::string std_sv_after;
  std::string trie_sv_after;
  ASSERT_OK(std_iter->GetProperty("rocksdb.iterator.super-version-number",
                                  &std_sv_after));
  ASSERT_OK(trie_iter->GetProperty("rocksdb.iterator.super-version-number",
                                   &trie_sv_after));
  ASSERT_LT(std::stoull(std_sv_before), std::stoull(std_sv_after));
  ASSERT_LT(std::stoull(trie_sv_before), std::stoull(trie_sv_after));

  for (auto* held : snaps) {
    db_->ReleaseSnapshot(held);
  }
}

TEST_P(TrieIndexDBTest,
       AutoRefreshSnapshotStressLikeSingleCfCoalescingIterator) {
  auto sqfc_factory = MakeStressLikeSqfcFactory();

  options_.create_if_missing = true;
  options_.disable_auto_compactions = true;
  options_.prefix_extractor.reset(NewFixedPrefixTransform(5));
  options_.table_properties_collector_factories.emplace_back(sqfc_factory);

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  table_options.block_size = 128;
  table_options.separate_key_value_in_data_block = true;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  constexpr int kRangeStart = 36745;
  constexpr int kRangeEnd = 36755;
  constexpr int kFillerStart = kRangeStart - 40;
  constexpr int kFillerEnd = kRangeEnd + 40;
  constexpr int kFinalVersion = 8;

  FlushOptions flush_opts;
  flush_opts.wait = true;

  for (int version = 0; version <= kFinalVersion; ++version) {
    for (int key_num = kFillerStart; key_num < kFillerEnd; ++key_num) {
      ASSERT_OK(db_->Put(WriteOptions(), MakeStressKey(key_num),
                         MakePaddedValue(version, key_num)));
    }
    ASSERT_OK(db_->Flush(flush_opts));
  }

  const Snapshot* snap = db_->GetSnapshot();
  const std::string lb = MakeStressKey(kRangeStart);
  const std::string ub = MakeStressKey(kRangeEnd);
  // Owns the table filter for as long as any iterator created by make_iter (and
  // returned to the caller) uses it. ReadOptions::table_filter is a pointer to
  // a caller-owned std::function.
  const std::function<bool(const TableProperties&)> range_filter =
      sqfc_factory->GetTableFilterForRangeQuery(lb, ub);

  auto expected_value = [&](int key_num) {
    return MakePaddedValue(kFinalVersion, key_num);
  };

  auto make_iter = [&](bool use_trie, bool use_coalescing) {
    ReadOptions ro;
    ro.snapshot = snap;
    ro.allow_unprepared_value = true;
    ro.auto_refresh_iterator_with_snapshot = true;
    ro.table_filter = &range_filter;
    if (use_trie) {
      ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    }
    if (use_coalescing) {
      return db_->NewCoalescingIterator(ro, {db_->DefaultColumnFamily()});
    }
    return std::unique_ptr<Iterator>(db_->NewIterator(ro));
  };

  auto assert_iter_state = [&](const char* label, Iterator* iter, int key_num) {
    SCOPED_TRACE(label);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), MakeStressKey(key_num));
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value().ToString(), expected_value(key_num));
  };

  auto std_iter = make_iter(/*use_trie=*/false, /*use_coalescing=*/false);
  auto trie_iter = make_iter(/*use_trie=*/true, /*use_coalescing=*/false);
  auto std_coalescing = make_iter(/*use_trie=*/false, /*use_coalescing=*/true);
  auto trie_coalescing = make_iter(/*use_trie=*/true, /*use_coalescing=*/true);

  const std::string seek_key = MakeStressKey(kRangeStart);
  std_iter->Seek(seek_key);
  trie_iter->Seek(seek_key);
  std_coalescing->Seek(seek_key);
  trie_coalescing->Seek(seek_key);

  assert_iter_state("standard iterator before refresh", std_iter.get(),
                    kRangeStart);
  assert_iter_state("trie iterator before refresh", trie_iter.get(),
                    kRangeStart);
  assert_iter_state("standard coalescing iterator before refresh",
                    std_coalescing.get(), kRangeStart);
  assert_iter_state("trie coalescing iterator before refresh",
                    trie_coalescing.get(), kRangeStart);

  for (int i = 0; i < 100; ++i) {
    const int key_num = 90000 + i;
    ASSERT_OK(db_->Put(WriteOptions(), MakeStressKey(key_num),
                       MakePaddedValue(100 + i, key_num)));
  }
  ASSERT_OK(db_->Flush(flush_opts));

  for (int key_num = kRangeStart + 1; key_num < kRangeEnd; ++key_num) {
    std_iter->Next();
    trie_iter->Next();
    std_coalescing->Next();
    trie_coalescing->Next();

    assert_iter_state("standard iterator after refresh", std_iter.get(),
                      key_num);
    assert_iter_state("trie iterator after refresh", trie_iter.get(), key_num);
    assert_iter_state("standard coalescing iterator after refresh",
                      std_coalescing.get(), key_num);
    assert_iter_state("trie coalescing iterator after refresh",
                      trie_coalescing.get(), key_num);
  }

  db_->ReleaseSnapshot(snap);
}

// ============================================================================
// MultiGet test
// ============================================================================

TEST_P(TrieIndexDBTest, MultiGetWithTrieUDI) {
  // Verifies that the batched MultiGet API works correctly with the trie UDI.
  // MultiGet is a separate code path from single Get and uses batched block
  // lookups, so it needs dedicated testing.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Write a mix of operation types.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "val_01"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "merge_02"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "val_04"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "entity_05"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "val_06"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // MultiGet through both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");

    std::vector<Slice> mg_keys = {"key_01",         "key_02", "key_03",
                                  "key_04",         "key_05", "key_06",
                                  "key_nonexistent"};
    std::vector<std::string> mg_values(mg_keys.size());
    std::vector<Status> mg_statuses = db_->MultiGet(ro, mg_keys, &mg_values);

    ASSERT_EQ(mg_statuses.size(), mg_keys.size());
    ASSERT_OK(mg_statuses[0]);
    ASSERT_EQ(mg_values[0], "val_01");
    ASSERT_OK(mg_statuses[1]);
    ASSERT_EQ(mg_values[1], "merge_02");
    ASSERT_TRUE(mg_statuses[2].IsNotFound());
    ASSERT_OK(mg_statuses[3]);
    ASSERT_EQ(mg_values[3], "val_04");
    ASSERT_OK(mg_statuses[4]);
    ASSERT_EQ(mg_values[4], "entity_05");
    ASSERT_OK(mg_statuses[5]);
    ASSERT_EQ(mg_values[5], "val_06");
    ASSERT_TRUE(mg_statuses[6].IsNotFound());
  }
}

// ============================================================================
// WAL replay / crash recovery test
// ============================================================================

TEST_P(TrieIndexDBTest, WALReplayRecovery) {
  // Writes data without flushing, then closes and reopens the DB. The data
  // must be recovered from the WAL and then flushed. This tests that the trie
  // UDI builder handles entries replayed from the WAL correctly.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  // WAL is enabled by default (WriteOptions::disableWAL = false).
  ASSERT_OK(OpenDB());

  // Write data -- do NOT flush. Data lives only in the WAL + memtable.
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_01", "wal_val_01"));
  ASSERT_OK(db_->Merge(WriteOptions(), "wal_key_02", "wal_merge"));
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_03", "wal_val_03"));
  ASSERT_OK(db_->Delete(WriteOptions(), "wal_key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_04", "wal_val_04"));

  // Close and reopen -- triggers WAL replay.
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB());

  // After WAL replay, data should be in a memtable. Flush to create SST with
  // the trie UDI.
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify data through both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_01", "wal_val_01"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_02", "wal_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("wal_key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_04", "wal_val_04"));

  // Forward scan.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"wal_key_01", "wal_val_01"},
        {"wal_key_02", "wal_merge"},
        {"wal_key_04", "wal_val_04"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }
}

// ============================================================================
// Multiple column families test
// ============================================================================

TEST_P(TrieIndexDBTest, MultipleColumnFamilies) {
  // Opens a DB with multiple column families, each using the trie UDI. Writes
  // different data to each CF, flushes, and verifies reads through both indexes
  // for each CF. This tests that the UDI builder/reader are correctly isolated
  // per-CF.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  options_.create_if_missing = true;

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  // Open with default CF first.
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  // Create two additional CFs with the same trie UDI options.
  ColumnFamilyHandle* cf1 = nullptr;
  ColumnFamilyHandle* cf2 = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_one", &cf1));
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_two", &cf2));

  // Write different data to each CF.
  ASSERT_OK(db_->Put(WriteOptions(), "default_key", "default_val"));
  ASSERT_OK(db_->Put(WriteOptions(), cf1, "cf1_key_a", "cf1_val_a"));
  ASSERT_OK(db_->Merge(WriteOptions(), cf1, "cf1_key_b", "cf1_merge"));
  ASSERT_OK(db_->Put(WriteOptions(), cf2, "cf2_key_x", "cf2_val_x"));
  ASSERT_OK(db_->Delete(WriteOptions(), cf2, "cf2_key_y"));
  ASSERT_OK(db_->Put(WriteOptions(), cf2, "cf2_key_z", "cf2_val_z"));

  // Flush all CFs.
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Flush(FlushOptions(), cf1));
  ASSERT_OK(db_->Flush(FlushOptions(), cf2));

  // Verify default CF.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::string value;
    ASSERT_OK(db_->Get(ro, "default_key", &value));
    ASSERT_EQ(value, "default_val");
  }

  // Verify cf_one through both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::string value;
    ASSERT_OK(db_->Get(ro, cf1, "cf1_key_a", &value));
    ASSERT_EQ(value, "cf1_val_a");
    ASSERT_OK(db_->Get(ro, cf1, "cf1_key_b", &value));
    ASSERT_EQ(value, "cf1_merge");
  }

  // Verify cf_two through both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::string value;
    ASSERT_OK(db_->Get(ro, cf2, "cf2_key_x", &value));
    ASSERT_EQ(value, "cf2_val_x");
    ASSERT_TRUE(db_->Get(ro, cf2, "cf2_key_y", &value).IsNotFound());
    ASSERT_OK(db_->Get(ro, cf2, "cf2_key_z", &value));
    ASSERT_EQ(value, "cf2_val_z");
  }

  // Forward scan on each CF via both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");

    // cf_one scan.
    std::unique_ptr<Iterator> it1(db_->NewIterator(ro, cf1));
    it1->SeekToFirst();
    ASSERT_TRUE(it1->Valid());
    ASSERT_EQ(it1->key().ToString(), "cf1_key_a");
    it1->Next();
    ASSERT_TRUE(it1->Valid());
    ASSERT_EQ(it1->key().ToString(), "cf1_key_b");
    it1->Next();
    ASSERT_FALSE(it1->Valid());
    ASSERT_OK(it1->status());

    // cf_two scan.
    std::unique_ptr<Iterator> it2(db_->NewIterator(ro, cf2));
    it2->SeekToFirst();
    ASSERT_TRUE(it2->Valid());
    ASSERT_EQ(it2->key().ToString(), "cf2_key_x");
    it2->Next();
    ASSERT_TRUE(it2->Valid());
    ASSERT_EQ(it2->key().ToString(), "cf2_key_z");
    it2->Next();
    ASSERT_FALSE(it2->Valid());
    ASSERT_OK(it2->status());
  }

  // Clean up CF handles before closing.
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf1));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf2));

  // Close the DB. Need to clear db_ first since TearDown will also close.
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen with all CFs to verify persistence.
  {
    std::vector<ColumnFamilyDescriptor> cf_descs = {
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, options_),
        ColumnFamilyDescriptor("cf_one", options_),
        ColumnFamilyDescriptor("cf_two", options_)};
    std::vector<ColumnFamilyHandle*> cf_handles;
    std::unique_ptr<DB> reopen_db;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cf_handles, &reopen_db));
    db_ = std::move(reopen_db);

    // Verify data survives reopen.
    auto ro = TrieIndexReadOptions();
    std::string value;
    ASSERT_OK(db_->Get(ro, cf_handles[0], "default_key", &value));
    ASSERT_EQ(value, "default_val");
    ASSERT_OK(db_->Get(ro, cf_handles[1], "cf1_key_a", &value));
    ASSERT_EQ(value, "cf1_val_a");
    ASSERT_OK(db_->Get(ro, cf_handles[2], "cf2_key_z", &value));
    ASSERT_EQ(value, "cf2_val_z");

    for (auto* h : cf_handles) {
      ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
    }
  }
}

// ---------------------------------------------------------------------------
// BatchedPrefixScan: reproduces the test_batches_snapshots pattern.
//
// Writes batches of 10 keys {digit+key_body : value_body+digit} for digit in
// 0..9, exactly as the crash-test stress tool does. Then scans each prefix
// concurrently (same snapshot) and checks that:
//   (a) all 10 iterators yield the same key bodies in lockstep, and
//   (b) the values stripped of the trailing digit are identical across
//   prefixes.
//
// We run with both the standard index and the trie index and compare.
// ---------------------------------------------------------------------------
TEST_P(TrieIndexDBTest, BatchedPrefixScan) {
  // Small block size to force many data blocks (and thus many trie entries).
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumBatches = 200;
  const int kNumPrefixes = 10;
  Random rnd(42);

  // Phase 1: Write batches.
  for (int b = 0; b < kNumBatches; ++b) {
    WriteBatch batch;
    std::string key_body = MakeKeyBody(b);
    std::string value_body = rnd.RandomString(20);

    for (int d = kNumPrefixes - 1; d >= 0; --d) {
      std::string k = std::to_string(d) + key_body;
      std::string v = value_body + std::to_string(d);
      ASSERT_OK(batch.Put(k, v));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  }

  // Flush so data is in SSTs with trie index.
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Phase 2: Prefix scan with both indexes.
  for (const auto& base_ro_template : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro_template.read_index != ReadOptions::ReadIndex::kDefault
                     ? "trie index"
                     : "standard index");

    const Snapshot* snap = db_->GetSnapshot();
    ReadOptions base_ro = base_ro_template;
    base_ro.snapshot = snap;

    uint64_t count = VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                                              /*use_upper_bounds=*/true,
                                              /*verify_values=*/true);
    ASSERT_EQ(count, static_cast<uint64_t>(kNumBatches))
        << "expected " << kNumBatches << " entries per prefix";

    db_->ReleaseSnapshot(snap);
  }
}

// Same as above but with multiple flushes, compaction, and a DB reopen
// in between to simulate the crash-recovery path.
TEST_P(TrieIndexDBTest, BatchedPrefixScanAfterReopen) {
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumBatches = 100;
  const int kNumPrefixes = 10;
  Random rnd(123);

  for (int b = 0; b < kNumBatches; ++b) {
    WriteBatch batch;
    std::string key_body = MakeKeyBody(b);
    std::string value_body = rnd.RandomString(20);

    for (int d = kNumPrefixes - 1; d >= 0; --d) {
      std::string k = std::to_string(d) + key_body;
      std::string v = value_body + std::to_string(d);
      ASSERT_OK(batch.Put(k, v));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));

    // Flush every 20 batches to create multiple SSTs.
    if ((b + 1) % 20 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact to merge SSTs.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Close and reopen (simulating recovery).
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB(/*block_size=*/256));

  // Prefix scan with trie index after reopen.
  ReadOptions base_ro = TrieIndexReadOptions();
  const Snapshot* snap = db_->GetSnapshot();
  base_ro.snapshot = snap;

  uint64_t count =
      VerifyPrefixScanLockstep(base_ro, kNumPrefixes, /*use_upper_bounds=*/true,
                               /*verify_values=*/false);
  ASSERT_EQ(count, static_cast<uint64_t>(kNumBatches));
  db_->ReleaseSnapshot(snap);
}

// Test with overwrites: multiple writes to the same key body, ensuring
// the latest value is consistent across all prefixes.
TEST_P(TrieIndexDBTest, BatchedPrefixScanWithOverwrites) {
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumKeys = 50;
  const int kNumOverwrites = 5;
  const int kNumPrefixes = 10;
  Random rnd(999);

  // Write each key body multiple times.
  for (int round = 0; round < kNumOverwrites; ++round) {
    for (int k = 0; k < kNumKeys; ++k) {
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      std::string value_body = rnd.RandomString(20);

      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        std::string v = value_body + std::to_string(d);
        ASSERT_OK(batch.Put(key, v));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Flush after each round.
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  // Now verify with both indexes.
  for (const auto& base_ro_template : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro_template.read_index != ReadOptions::ReadIndex::kDefault
                     ? "trie index"
                     : "standard index");

    const Snapshot* snap = db_->GetSnapshot();
    ReadOptions base_ro = base_ro_template;
    base_ro.snapshot = snap;

    uint64_t count = VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                                              /*use_upper_bounds=*/false,
                                              /*verify_values=*/true);
    ASSERT_EQ(count, static_cast<uint64_t>(kNumKeys));
    db_->ReleaseSnapshot(snap);
  }
}

// Stress-like test: write + delete + rewrite many keys, flush between rounds,
// then verify prefix scan consistency. Simulates the crash test pattern that
// triggered failures.
TEST_P(TrieIndexDBTest, BatchedPrefixScanStressLike) {
  ASSERT_OK(OpenDB(/*block_size=*/4096));

  const int kMaxKey = 10000;
  const int kNumPrefixes = 10;
  const int kNumRounds = 20;
  Random rnd(7777);

  for (int round = 0; round < kNumRounds; ++round) {
    // Write a batch of random keys
    int num_writes = 100 + rnd.Uniform(200);
    for (int w = 0; w < num_writes; ++w) {
      int k = rnd.Uniform(kMaxKey);
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      std::string value_body = rnd.RandomString(rnd.Uniform(60) + 4);
      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        std::string v = value_body + std::to_string(d);
        ASSERT_OK(batch.Put(key, v));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Delete some random keys
    int num_deletes = 50 + rnd.Uniform(100);
    for (int w = 0; w < num_deletes; ++w) {
      int k = rnd.Uniform(kMaxKey);
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        ASSERT_OK(batch.Delete(key));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Flush every few rounds
    if (round % 3 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    // Verify prefix scan consistency with trie index.
    {
      ReadOptions base_ro = TrieIndexReadOptions();
      const Snapshot* snap = db_->GetSnapshot();
      base_ro.snapshot = snap;

      VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                               /*use_upper_bounds=*/false,
                               /*verify_values=*/true,
                               "round=" + std::to_string(round));

      db_->ReleaseSnapshot(snap);
    }
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithTrieIndex) {
  // Verifies that prefix iteration (total_order_seek=false with a prefix
  // extractor) returns identical results through the trie and standard indexes.
  options_.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Write keys with two different prefixes across two SSTs.
  ASSERT_OK(db_->Put(WriteOptions(), "aaaa1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaaa2", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaaa3", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbbb1", "v4"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbbb2", "v5"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Put(WriteOptions(), "aaaa4", "v6"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbbb3", "v7"));
  ASSERT_OK(db_->Put(WriteOptions(), "cccc1", "v8"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  auto PrefixScan = [&](const ReadOptions& ro, const std::string& target) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->Seek(target); iter->Valid(); iter->Next()) {
      if (iter->key().size() >= 4 && target.size() >= 4 &&
          iter->key().ToString().substr(0, 4) != target.substr(0, 4)) {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  };

  auto ReversePrefixScan = [&](const ReadOptions& ro,
                               const std::string& target) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->SeekForPrev(target); iter->Valid(); iter->Prev()) {
      if (iter->key().size() >= 4 && target.size() >= 4 &&
          iter->key().ToString().substr(0, 4) != target.substr(0, 4)) {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  };

  // Forward prefix scans.
  {
    std::vector<std::string> expected = {"aaaa1", "aaaa2", "aaaa3", "aaaa4"};
    for (const auto& ro : BothIndexReadOptions()) {
      ASSERT_EQ(PrefixScan(ro, "aaaa"), expected);
    }
  }
  {
    std::vector<std::string> expected = {"bbbb1", "bbbb2", "bbbb3"};
    for (const auto& ro : BothIndexReadOptions()) {
      ASSERT_EQ(PrefixScan(ro, "bbbb"), expected);
    }
  }
  {
    std::vector<std::string> expected = {"cccc1"};
    for (const auto& ro : BothIndexReadOptions()) {
      ASSERT_EQ(PrefixScan(ro, "cccc"), expected);
    }
  }

  // Reverse prefix scans.
  {
    std::vector<std::string> expected = {"aaaa4", "aaaa3", "aaaa2", "aaaa1"};
    for (const auto& ro : BothIndexReadOptions()) {
      ASSERT_EQ(ReversePrefixScan(ro, "aaaa\xff"), expected);
    }
  }
  {
    std::vector<std::string> expected = {"bbbb3", "bbbb2", "bbbb1"};
    for (const auto& ro : BothIndexReadOptions()) {
      ASSERT_EQ(ReversePrefixScan(ro, "bbbb\xff"), expected);
    }
  }

  // Direction switching within a prefix.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("aaaa2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "aaaa2");
    iter->Next();
    ASSERT_EQ(iter->key().ToString(), "aaaa3");
    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "aaaa2");
    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "aaaa1");
    iter->Next();
    ASSERT_EQ(iter->key().ToString(), "aaaa2");
    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithUpperBound) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 50; i++) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "aaaa%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  for (int i = 0; i < 20; i++) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "bbbb%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string upper = "aaaa0025";
  Slice upper_bound(upper);

  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.iterate_upper_bound = &upper_bound;
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    for (iter->Seek("aaaa"); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys.size(), 25u);
    ASSERT_EQ(keys.front(), "aaaa0000");
    ASSERT_EQ(keys.back(), "aaaa0024");
  }

  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.iterate_upper_bound = &upper_bound;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "aaaa0024");
    ASSERT_OK(iter->status());
  }

  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.iterate_upper_bound = &upper_bound;
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->SeekForPrev("aaaa0024");
    for (; iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys.size(), 25u);
    ASSERT_EQ(keys.front(), "aaaa0024");
    ASSERT_EQ(keys.back(), "aaaa0000");
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationDirectionSwitchStress) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/64));

  const char* prefixes[] = {"aaa", "bbb", "ccc"};
  for (int sst = 0; sst < 3; sst++) {
    for (const char* pfx : prefixes) {
      for (int i = 0; i < 10; i++) {
        char key[16];
        char val[16];
        snprintf(key, sizeof(key), "%s%02d_%d", pfx, i, sst);
        snprintf(val, sizeof(val), "v%d_%d", i, sst);
        ASSERT_OK(db_->Put(WriteOptions(), key, val));
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  for (const char* pfx : prefixes) {
    SCOPED_TRACE(pfx);
    // Collect keys through each available index and verify consistency.
    std::vector<std::string> ref_keys;
    for (const auto& ro : BothIndexReadOptions()) {
      std::vector<std::string> cur_keys;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      for (iter->Seek(pfx); iter->Valid(); iter->Next()) {
        if (iter->key().ToString().substr(0, 3) != std::string(pfx)) {
          break;
        }
        cur_keys.push_back(iter->key().ToString());
      }
      ASSERT_OK(iter->status());
      if (ref_keys.empty()) {
        ref_keys = cur_keys;
      } else {
        ASSERT_EQ(ref_keys, cur_keys);
      }
    }
    ASSERT_FALSE(ref_keys.empty());

    size_t mid = ref_keys.size() / 2;
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom"
                       : "builtin");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->Seek(ref_keys[mid]);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), ref_keys[mid]);
      for (int i = 1; i <= 3 && mid + i < ref_keys.size(); i++) {
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), ref_keys[mid + i]);
      }
      size_t pos = std::min(mid + 3, ref_keys.size() - 1);
      for (int i = 1; i <= 2 && pos >= 1; i++) {
        iter->Prev();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), ref_keys[pos - i]);
      }
      ASSERT_OK(iter->status());
    }
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithDeletesAndMerges) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  ASSERT_OK(db_->Put(WriteOptions(), "aaa01", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa02", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa03", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa04", "v4"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa05", "v5"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbb01", "b1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Delete(WriteOptions(), "aaa02"));
  ASSERT_OK(db_->Merge(WriteOptions(), "aaa03", ",m1"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "aaa04"));
  ASSERT_OK(db_->Merge(WriteOptions(), "aaa05", ",m2"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa06", "v6"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  auto PrefixScan = [&](const ReadOptions& ro) {
    std::vector<std::pair<std::string, std::string>> result;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->Seek("aaa"); iter->Valid(); iter->Next()) {
      if (iter->key().ToString().substr(0, 3) != "aaa") {
        break;
      }
      result.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return result;
  };

  auto trie_result = PrefixScan(TrieIndexReadOptions());
  ASSERT_EQ(trie_result.size(), 4u);
  ASSERT_EQ(trie_result[0].first, "aaa01");
  ASSERT_EQ(trie_result[1],
            std::make_pair(std::string("aaa03"), std::string("v3,,m1")));
  ASSERT_EQ(trie_result[2],
            std::make_pair(std::string("aaa05"), std::string("v5,,m2")));
  ASSERT_EQ(trie_result[3].first, "aaa06");
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(PrefixScan(StandardIndexReadOptions()), trie_result);
  }

  auto trie_rev = ReversePrefixScanKeys(TrieIndexReadOptions(), "aaa");
  ASSERT_EQ(trie_rev.size(), 4u);
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(ReversePrefixScanKeys(StandardIndexReadOptions(), "aaa"),
              trie_rev);
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationAfterCompaction) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 30; i++) {
    char key[16];
    char val[16];
    const char* pfx = (i % 3 == 0) ? "aaa" : (i % 3 == 1) ? "bbb" : "ccc";
    snprintf(key, sizeof(key), "%s%04d", pfx, i);
    snprintf(val, sizeof(val), "val%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (int i = 1; i < 30; i += 6) {
    char key[16];
    snprintf(key, sizeof(key), "bbb%04d", i);
    ASSERT_OK(db_->Delete(WriteOptions(), key));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (const char* pfx : {"aaa", "bbb", "ccc"}) {
    SCOPED_TRACE(pfx);
    auto trie_keys = PrefixScanKeys(TrieIndexReadOptions(), pfx);
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      ASSERT_EQ(PrefixScanKeys(StandardIndexReadOptions(), pfx), trie_keys);
    }
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithSnapshots) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  ASSERT_OK(db_->Put(WriteOptions(), "aaa01", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa02", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbb01", "b1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snap1 = db_->GetSnapshot();

  ASSERT_OK(db_->Delete(WriteOptions(), "aaa01"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa03", "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snap2 = db_->GetSnapshot();

  ASSERT_OK(db_->Put(WriteOptions(), "aaa04", "v4"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  auto PrefixScanAt = [&](const ReadOptions& base_ro, const Snapshot* snap,
                          const std::string& prefix) {
    ReadOptions ro = base_ro;
    ro.snapshot = snap;
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (iter->key().ToString().substr(0, 3) != prefix) {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  };

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(PrefixScanAt(ro, snap1, "aaa"),
              (std::vector<std::string>{"aaa01", "aaa02"}));
    ASSERT_EQ(PrefixScanAt(ro, snap2, "aaa"),
              (std::vector<std::string>{"aaa02", "aaa03"}));
    ASSERT_EQ(PrefixScanAt(ro, nullptr, "aaa"),
              (std::vector<std::string>{"aaa02", "aaa03", "aaa04"}));
  }

  db_->ReleaseSnapshot(snap1);
  db_->ReleaseSnapshot(snap2);
}

TEST_P(TrieIndexDBTest, PrefixIterationEmptyPrefix) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "aaa01", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "ccc01", "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("bbb");
    if (iter->Valid()) {
      ASSERT_NE(iter->key().ToString().substr(0, 3), "bbb");
    }
    ASSERT_OK(iter->status());
    iter->SeekForPrev("bbb\xff");
    if (iter->Valid()) {
      ASSERT_NE(iter->key().ToString().substr(0, 3), "bbb");
    }
    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithLowerBound) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 20; i++) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "aaa%04d", i);
    snprintf(val, sizeof(val), "val%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string lower = "aaa0010";
  Slice lower_bound(lower);

  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.iterate_lower_bound = &lower_bound;
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    for (iter->Seek("aaa"); iter->Valid(); iter->Next()) {
      if (iter->key().ToString().substr(0, 3) != "aaa") {
        break;
      }
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys.size(), 10u);
    ASSERT_EQ(keys.front(), "aaa0010");
    ASSERT_EQ(keys.back(), "aaa0019");
  }

  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.iterate_lower_bound = &lower_bound;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "aaa0010");
    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationWithDeleteRange) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 20; i++) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "aaa%04d", i);
    snprintf(val, sizeof(val), "val%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "aaa0005", "aaa0015"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  auto trie_keys = PrefixScanKeys(TrieIndexReadOptions(), "aaa");
  ASSERT_EQ(trie_keys.size(), 10u);
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(PrefixScanKeys(StandardIndexReadOptions(), "aaa"), trie_keys);
  }

  auto trie_rev = ReversePrefixScanKeys(TrieIndexReadOptions(), "aaa");
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(ReversePrefixScanKeys(StandardIndexReadOptions(), "aaa"),
              trie_rev);
  }
}

TEST_P(TrieIndexDBTest, PrefixIterationMemtablePlusSST) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  ASSERT_OK(db_->Put(WriteOptions(), "aaa01", "sst1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa03", "sst3"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa05", "sst5"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Put(WriteOptions(), "aaa02", "mem2"));
  ASSERT_OK(db_->Delete(WriteOptions(), "aaa03"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa04", "mem4"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa06", "mem6"));

  auto PrefixScan = [&](const ReadOptions& ro) {
    std::vector<std::pair<std::string, std::string>> result;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (iter->Seek("aaa"); iter->Valid(); iter->Next()) {
      if (iter->key().ToString().substr(0, 3) != "aaa") {
        break;
      }
      result.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return result;
  };

  auto trie_result = PrefixScan(TrieIndexReadOptions());
  ASSERT_EQ(trie_result.size(), 5u);
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(PrefixScan(StandardIndexReadOptions()), trie_result);
  }

  auto trie_rev = ReversePrefixScanKeys(TrieIndexReadOptions(), "aaa");
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ASSERT_EQ(ReversePrefixScanKeys(StandardIndexReadOptions(), "aaa"),
              trie_rev);
  }
}

// ---------------------------------------------------------------------------
// Verifies that the trie builder does NOT call FindShortSuccessor() on the
// last block's separator key. FindShortSuccessor would produce a shorter
// key covering a wider range than the actual data. For example, if the
// last key's user key is "9\xff\xff", FindShortSuccessor produces ":"
// (0x3A), making the trie claim it covers keys up to ":". A seek for
// "9\xff\xff\x01" would then find a block via the trie but not via the
// standard index, causing iterators to desynchronize.
// ---------------------------------------------------------------------------
TEST_P(TrieIndexDBTest, LastBlockSeparatorNotShortened) {
  // Use a small block size so each key lands in its own block.
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write keys where the last key has trailing 0xFF bytes, which
  // FindShortSuccessor would shorten by incrementing the byte before the
  // 0xFF suffix ("9\xff\xff" -> ":").
  ASSERT_OK(db_->Put(WriteOptions(), "1aaa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "5bbb", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), std::string("9\xff\xff", 3), "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The key "9\xff\xff\x01" is lexicographically after "9\xff\xff" but
  // before ":" (0x3A). If FindShortSuccessor were applied, the trie would
  // return a valid block for this key. Both indexes must correctly say
  // "not found".
  std::string seek_target = std::string("9\xff\xff\x01", 4);

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek(seek_target);
    ASSERT_TRUE(!iter->Valid()) << "Expected no key at or after seek_target, "
                                << "but got: " << iter->key().ToString(true);
    ASSERT_OK(iter->status());
  }

  // Also verify the actual last key is still findable.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek(std::string("9\xff\xff", 3));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), std::string("9\xff\xff", 3));
    ASSERT_EQ(iter->value().ToString(), "v3");

    // After this key, there should be nothing more.
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Variant: tests that when deletes remove the last key, seeking past the last
// remaining key correctly returns "not found" with both indexes.
TEST_P(TrieIndexDBTest, LastBlockSeparatorWithDeletes) {
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write and flush initial data.
  ASSERT_OK(db_->Put(WriteOptions(), "1aaa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "5bbb", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), std::string("9\xff\xff", 3), "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Delete the last key and flush (creates a tombstone in a new SST).
  ASSERT_OK(db_->Delete(WriteOptions(), std::string("9\xff\xff", 3)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Now seeking for the deleted key should yield "5bbb" or nothing,
  // depending on the seek target. Both indexes must agree.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    // Seek to the deleted key -- should skip it and land on nothing (it was
    // the last key).
    iter->Seek(std::string("9\xff\xff", 3));
    ASSERT_TRUE(!iter->Valid())
        << "Deleted key should not be visible, but got: "
        << iter->key().ToString(true);
    ASSERT_OK(iter->status());

    // Seek to a key between "5bbb" and the deleted key -- should find "5bbb"
    // or nothing depending on order. Actually, "6" > "5bbb" and "6" <
    // "9\xff\xff", so seeking "6" should find nothing since there's no key
    // >= "6" that's still alive.
    iter->Seek("6");
    ASSERT_TRUE(!iter->Valid()) << "No live key >= '6' should exist, but got: "
                                << iter->key().ToString(true);
    ASSERT_OK(iter->status());
  }

  // Compact to merge the tombstone, then verify again.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "1aaa");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "5bbb");
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Single-entry SST: the trie has exactly one leaf. Validates that Seek,
// SeekToFirst, Next, and Get all work with a one-block, one-key SST.
TEST_P(TrieIndexDBTest, SingleEntrySST) {
  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(WriteOptions(), "only_key", "only_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookup.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("only_key", "only_val"));

  // Forward scan: exactly one result.
  ASSERT_NO_FATAL_FAILURE(
      VerifyScanBothIndexes(std::vector<std::pair<std::string, std::string>>{
          {"only_key", "only_val"}}));

  // Seek to the exact key.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes("only_key", "only_key", "only_val"));

  // Seek before the key -- should land on it.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("a", "only_key", "only_val"));

  // Seek past the key -- should be invalid.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("z");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Deletion-only SST: flush a Put, then flush a Delete for that key so the
// second SST contains only a tombstone. After compaction, the key is gone.
TEST_P(TrieIndexDBTest, DeletionOnlySST) {
  ASSERT_OK(OpenDB());

  // Flush 1: a real Put.
  ASSERT_OK(db_->Put(WriteOptions(), "del_target", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Flush 2: only a Delete -- this creates an SST whose only entry is a
  // tombstone (the trie still builds an index for the block containing it).
  ASSERT_OK(db_->Delete(WriteOptions(), "del_target"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The tombstone hides the Put.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_target"));

  // Forward scan: nothing visible.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(std::vector<std::string>{}));

  // Compact to merge: key is fully removed.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_target"));
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(std::vector<std::string>{}));
}

// All-same-key SST: multiple versions of the same user key (via snapshots)
// land in the same SST, possibly spanning multiple blocks. Validates that
// the trie's same-key-run handling (seqno-based separators) works at the
// DB level through both indexes.
TEST_P(TrieIndexDBTest, AllSameKeySST) {
  options_.disable_auto_compactions = true;
  // Small block size to force multiple blocks for the same user key.
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write several versions of the same key with snapshots to prevent GC.
  std::vector<const Snapshot*> snaps;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), "same_key", "val_" + std::to_string(i)));
    snaps.push_back(db_->GetSnapshot());
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Latest value is visible.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("same_key", "val_9"));

  // Forward scan: only the latest version is visible (without snapshot).
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{{"same_key", "val_9"}}));

  // Each snapshot should see the correct version.
  for (int i = 0; i < 10; i++) {
    SCOPED_TRACE("snapshot " + std::to_string(i));
    std::string expected = "val_" + std::to_string(i);
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes(snaps[i], "same_key", expected));

    // Forward scan with snapshot.
    ASSERT_NO_FATAL_FAILURE(
        VerifyScanBothIndexes(snaps[i], {{"same_key", expected}}));
  }

  // Seek with earliest snapshot -- should find the earliest version.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes(snaps[0], "same_key", "same_key", "val_0"));

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

// Operations on a completely empty DB: nothing should crash, and after
// creating + deleting all data, the DB should correctly return nothing.
TEST_P(TrieIndexDBTest, EmptyDBOperations) {
  ASSERT_OK(OpenDB());

  // Get / Seek / SeekToFirst on empty memtable (no SSTs yet).
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("anything"));
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Seek("anything");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Create an SST, delete its only key, compact -> DB has no live data but
  // the trie code path was exercised during flush.
  ASSERT_OK(db_->Put(WriteOptions(), "temp", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Delete(WriteOptions(), "temp"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("temp"));
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Focused seek-pattern tests: before all data, between blocks, exact match,
// after all data, and empty-key seek.
TEST_P(TrieIndexDBTest, SeekEdgeCases) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write keys with deliberate gaps.
  for (const auto& k : {"bbb", "ddd", "fff", "hhh"}) {
    ASSERT_OK(db_->Put(WriteOptions(), k, std::string("v_") + k));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    // Before first key.
    iter->Seek("aaa");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");

    // Exact first key.
    iter->Seek("bbb");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");

    // Between keys.
    iter->Seek("ccc");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "ddd");

    // Between keys (eee -> fff).
    iter->Seek("eee");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "fff");

    // Exact last key.
    iter->Seek("hhh");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "hhh");

    // After last key.
    iter->Seek("zzz");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    // Empty key (smallest possible key for BytewiseComparator).
    iter->Seek("");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");
  }
}

// PutEntity + GetEntity through the trie index read path.
TEST_P(TrieIndexDBTest, GetEntityWithTrieUDI) {
  ASSERT_OK(OpenDB());

  // PutEntity with wide columns.
  WideColumns columns{
      {kDefaultWideColumnName, "default_val"},
      {"col_a", "val_a"},
      {"col_b", "val_b"},
  };
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "entity_key", columns));
  // Also a regular Put to verify GetEntity reads it as a single default column.
  ASSERT_OK(db_->Put(WriteOptions(), "regular_key", "regular_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");

    // GetEntity on a PutEntity key.
    PinnableWideColumns result;
    ASSERT_OK(
        db_->GetEntity(ro, db_->DefaultColumnFamily(), "entity_key", &result));
    ASSERT_EQ(result.columns().size(), 3u);
    ASSERT_EQ(result.columns()[0].name(), kDefaultWideColumnName);
    ASSERT_EQ(result.columns()[0].value(), "default_val");
    ASSERT_EQ(result.columns()[1].name(), "col_a");
    ASSERT_EQ(result.columns()[1].value(), "val_a");
    ASSERT_EQ(result.columns()[2].name(), "col_b");
    ASSERT_EQ(result.columns()[2].value(), "val_b");

    // GetEntity on a regular Put key returns single default column.
    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(ro, db_->DefaultColumnFamily(), "regular_key",
                             &result2));
    ASSERT_EQ(result2.columns().size(), 1u);
    ASSERT_EQ(result2.columns()[0].name(), kDefaultWideColumnName);
    ASSERT_EQ(result2.columns()[0].value(), "regular_val");

    // GetEntity on nonexistent key.
    PinnableWideColumns result3;
    ASSERT_TRUE(
        db_->GetEntity(ro, db_->DefaultColumnFamily(), "no_such_key", &result3)
            .IsNotFound());
  }
}

// Multiple overlapping L0 SSTs: the level iterator must coordinate trie
// iterators across multiple SST files with overlapping key ranges.
TEST_P(TrieIndexDBTest, OverlappingL0SSTs) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // SST1: keys 00..49.
  for (int i = 0; i < 50; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst1_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // SST2: keys 25..74 (overlapping with SST1).
  for (int i = 25; i < 75; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst2_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // SST3: keys 50..99 (overlapping with SST2).
  for (int i = 50; i < 100; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst3_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify: latest writer wins for overlapping keys.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    auto kvs = ScanAllKeyValues(ro);
    ASSERT_EQ(kvs.size(), 100u);
    for (int i = 0; i < 100; i++) {
      char key[16];
      snprintf(key, sizeof(key), "key_%03d", i);
      ASSERT_EQ(kvs[i].first, key);
      if (i < 25) {
        ASSERT_EQ(kvs[i].second, "sst1_" + std::to_string(i));
      } else if (i < 50) {
        ASSERT_EQ(kvs[i].second, "sst2_" + std::to_string(i));
      } else {
        ASSERT_EQ(kvs[i].second, "sst3_" + std::to_string(i));
      }
    }
  }

  // Compact all L0 -> L1, re-verify.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeyValues(ro).size(), 100u);
  }
}

// CompactRange with a sub-range: only part of the key space is compacted.
TEST_P(TrieIndexDBTest, CompactRangeSubset) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 26; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%c", 'a' + i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "val_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact only the middle range [key_f, key_p).
  std::string begin = "key_f";
  std::string end = "key_p";
  Slice begin_s(begin);
  Slice end_s(end);
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, &begin_s, &end_s));

  // All 26 keys should still be readable.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeys(ro).size(), 26u);
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_a", "val_0"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_z", "val_25"));
}

// Write keys, delete all of them, compact. The DB should be empty.
TEST_P(TrieIndexDBTest, AllKeysDeletedCompaction) {
  ASSERT_OK(OpenDB());

  for (int i = 0; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "val"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Delete all keys.
  for (int i = 0; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    ASSERT_OK(db_->Delete(WriteOptions(), key));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Before compaction: tombstones hide all keys.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeys(ro).size(), 0u);
  }

  // After compaction: all tombstones and data are gone.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeys(ro).size(), 0u);
  }
}

// Keys with special byte values: 0x00, 0xFF, embedded nulls, very short keys.
// These exercise trie byte-traversal edge cases.
TEST_P(TrieIndexDBTest, BinaryKeyEdgeCases) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // All keys in sorted order (BytewiseComparator).
  std::vector<std::pair<std::string, std::string>> kvs = {
      {std::string("\x00", 1), "val_null"},
      {std::string("\x00\x00\x00", 3), "val_triple_null"},
      {std::string("\x01", 1), "val_0x01"},
      {"a", "val_a"},
      {std::string("a\x00"
                   "b",
                   3),
       "val_a_null_b"},
      {"mid", "val_mid"},
      {std::string("\xfe", 1), "val_0xfe"},
      {std::string("\xff", 1), "val_0xff"},
      {std::string("\xff\xff\xff", 3), "val_triple_ff"},
  };

  for (const auto& kv : kvs) {
    ASSERT_OK(db_->Put(WriteOptions(), kv.first, kv.second));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan: all keys in order through both indexes.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    auto actual = ScanAllKeyValues(ro);
    ASSERT_EQ(actual.size(), kvs.size());
    for (size_t i = 0; i < kvs.size(); i++) {
      SCOPED_TRACE("key index " + std::to_string(i));
      ASSERT_EQ(actual[i].first, kvs[i].first);
      ASSERT_EQ(actual[i].second, kvs[i].second);
    }
  }

  // Point lookups for boundary keys.
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(std::string("\x00", 1), "val_null"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(std::string("\xff\xff\xff", 3), "val_triple_ff"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(std::string("a\x00"
                                                           "b",
                                                           3),
                                               "val_a_null_b"));

  // Seek to embedded-null key.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes(
      std::string("\x00", 1), std::string("\x00", 1), "val_null"));
}

// Puts with empty string values.
TEST_P(TrieIndexDBTest, EmptyValuePuts) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", ""));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "non_empty"));
  ASSERT_OK(db_->Put(WriteOptions(), "key3", ""));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key1", ""));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key2", "non_empty"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key3", ""));

  ASSERT_NO_FATAL_FAILURE(
      VerifyScanBothIndexes(std::vector<std::pair<std::string, std::string>>{
          {"key1", ""}, {"key2", "non_empty"}, {"key3", ""}}));
}

// Zlib compression: data blocks are compressed, UDI block is not.
// Verifies that reads through the trie index work with compressed data.
TEST_P(TrieIndexDBTest, CompressionZlib) {
  if (!Zlib_Supported()) {
    ROCKSDB_GTEST_SKIP("Zlib not linked");
    return;
  }
  options_.compression = kZlibCompression;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 100; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    // Compressible value (repeated pattern).
    ASSERT_OK(db_->Put(WriteOptions(), key, std::string(200, 'A' + (i % 26))));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeys(ro).size(), 100u);
  }

  // Spot-check a few keys.
  for (int i : {0, 49, 99}) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes(key, std::string(200, 'A' + (i % 26))));
  }
}

// Iterator stability: an iterator pinned to a snapshot should not see data
// written after the iterator was created, even after flush.
TEST_P(TrieIndexDBTest, IteratorStabilityDuringFlush) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Open iterator (implicitly pins a snapshot).
  auto ro = TrieIndexReadOptions();
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key1");

  // Write + flush new data while iterator is open.
  ASSERT_OK(db_->Put(WriteOptions(), "key3", "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Existing iterator should NOT see key3.
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // New iterator should see all three keys.
  std::unique_ptr<Iterator> iter2(db_->NewIterator(ro));
  iter2->SeekToFirst();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key1");
  iter2->Next();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key2");
  iter2->Next();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key3");
  ASSERT_OK(iter2->status());
}

// iterate_upper_bound without prefix scan: the iterator should stop at the
// upper bound.
TEST_P(TrieIndexDBTest, IteratorUpperBound) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  for (const auto& k : {"aa", "bb", "cc", "dd", "ee", "ff"}) {
    ASSERT_OK(db_->Put(WriteOptions(), k, std::string("v_") + k));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");

    // Upper bound = "dd" -> should see aa, bb, cc only.
    std::string ub_str = "dd";
    Slice ub(ub_str);
    ReadOptions ro = base_ro;
    ro.iterate_upper_bound = &ub;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    std::vector<std::string> keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys, (std::vector<std::string>{"aa", "bb", "cc"}));

    // Upper bound = "aa" -> should see nothing.
    std::string ub2_str = "aa";
    Slice ub2(ub2_str);
    ReadOptions ro2 = base_ro;
    ro2.iterate_upper_bound = &ub2;
    std::unique_ptr<Iterator> iter2(db_->NewIterator(ro2));
    iter2->SeekToFirst();
    ASSERT_FALSE(iter2->Valid());
    ASSERT_OK(iter2->status());

    // Upper bound after all data -> should see everything.
    std::string ub3_str = "zz";
    Slice ub3(ub3_str);
    ReadOptions ro3 = base_ro;
    ro3.iterate_upper_bound = &ub3;
    std::unique_ptr<Iterator> iter3(db_->NewIterator(ro3));
    std::vector<std::string> all_keys;
    for (iter3->SeekToFirst(); iter3->Valid(); iter3->Next()) {
      all_keys.push_back(iter3->key().ToString());
    }
    ASSERT_OK(iter3->status());
    ASSERT_EQ(all_keys.size(), 6u);
  }
}

// Combined snapshot + upper_bound: iterator sees the snapshot's view of data,
// bounded by iterate_upper_bound.
TEST_P(TrieIndexDBTest, IteratorSnapshotAndUpperBound) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "old_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_b", "old_b"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_c", "old_c"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_d", "old_d"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snap = db_->GetSnapshot();

  // Overwrite some keys after the snapshot.
  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "new_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_c", "new_c"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_e", "new_e"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");

    std::string ub_str = "key_d";
    Slice ub(ub_str);
    ReadOptions ro = base_ro;
    ro.snapshot = snap;
    ro.iterate_upper_bound = &ub;

    auto kvs = ScanAllKeyValues(ro);
    // Snapshot view: old values. Upper bound excludes key_d and key_e.
    ASSERT_EQ(kvs.size(), 3u);
    ASSERT_EQ(kvs[0],
              std::make_pair(std::string("key_a"), std::string("old_a")));
    ASSERT_EQ(kvs[1],
              std::make_pair(std::string("key_b"), std::string("old_b")));
    ASSERT_EQ(kvs[2],
              std::make_pair(std::string("key_c"), std::string("old_c")));
  }
  db_->ReleaseSnapshot(snap);
}

// VerifyChecksum goes through SeekToFirst+Next on the index iterator.
TEST_P(TrieIndexDBTest, VerifyChecksumWithTrieUDI) {
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 50; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // VerifyChecksum with default ReadOptions (standard index).
  ASSERT_OK(db_->VerifyChecksum());

  // VerifyChecksum with trie ReadOptions.
  ASSERT_OK(db_->VerifyChecksum(TrieIndexReadOptions()));
}

// Many small SSTs from frequent flushes: exercises trie iteration across
// many L0 files without compaction.
TEST_P(TrieIndexDBTest, ManySmallSSTs) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // 50 flushes, 2 keys each -> 50 SSTs.
  for (int f = 0; f < 50; f++) {
    char k1[16];
    char k2[16];
    snprintf(k1, sizeof(k1), "key_%04d", f * 2);
    snprintf(k2, sizeof(k2), "key_%04d", f * 2 + 1);
    ASSERT_OK(db_->Put(WriteOptions(), k1, "v" + std::to_string(f * 2)));
    ASSERT_OK(db_->Put(WriteOptions(), k2, "v" + std::to_string(f * 2 + 1)));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  // Verify all 100 keys are readable.
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    auto keys = ScanAllKeys(ro);
    ASSERT_EQ(keys.size(), 100u);
  }

  // Spot-check first and last.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_0000", "v0"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_0099", "v99"));

  // Compact everything into one SST, re-verify.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault ? "custom"
                                                                   : "builtin");
    ASSERT_EQ(ScanAllKeys(ro).size(), 100u);
  }
}

// Merge values accumulate across multiple compaction rounds.
TEST_P(TrieIndexDBTest, MergeAcrossMultipleCompactions) {
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(OpenDB());

  // Round 1: Put base value.
  ASSERT_OK(db_->Put(WriteOptions(), "key", "base"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base"));

  // Round 2: Merge "m1".
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "m1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base,m1"));

  // Round 3: Merge "m2".
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "m2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base,m1,m2"));

  // Forward scan also returns the accumulated value.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{{"key", "base,m1,m2"}}));
}

// Graceful degradation: reopen a DB that was written with UDI, but without
// the UDI factory configured. Reads should fall back to the standard index.
TEST_P(TrieIndexDBTest, ReopenWithoutTrieUDI) {
  // In primary mode, reopening without UDI still works because the standard
  // index is always fully populated. This test validates the secondary-mode
  // behavior where the UDI block is optional and reads fall back to the
  // standard index.
  if (IsPrimaryMode()) {
    ROCKSDB_GTEST_BYPASS("Not applicable in primary mode");
    return;
  }
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "val_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_b", "val_b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen WITHOUT UDI. The SST has a UDI meta block, but it's ignored.
  ASSERT_OK(OpenDBWithoutUDI());

  // Reads via standard index should work (UDI meta block is just ignored).
  std::string val;
  ASSERT_OK(db_->Get(ReadOptions(), "key_a", &val));
  ASSERT_EQ(val, "val_a");
  ASSERT_OK(db_->Get(ReadOptions(), "key_b", &val));
  ASSERT_EQ(val, "val_b");

  // Forward scan.
  auto keys = ScanAllKeys(ReadOptions());
  ASSERT_EQ(keys.size(), 2u);
  ASSERT_EQ(keys[0], "key_a");
  ASSERT_EQ(keys[1], "key_b");
}

// Mixed SSTs: some written with UDI, some without. Both should be readable
// through both index paths.
TEST_P(TrieIndexDBTest, EmptyUserKey) {
  ASSERT_OK(OpenDB(/*block_size=*/256));
  // The empty key's value fills a data block, so that block's separator is
  // the empty key.
  ASSERT_OK(db_->Put(WriteOptions(), "", std::string(1000, 'E')));
  std::vector<std::string> expected_keys = {""};
  for (char c = 'b'; c <= 'f'; c++) {
    expected_keys.emplace_back(1, c);
    ASSERT_OK(
        db_->Put(WriteOptions(), expected_keys.back(), std::string(300, c)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("", std::string(1000, 'E')));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("d", std::string(300, 'd')));
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected_keys));
}

TEST_P(TrieIndexDBTest, OnlyEmptyUserKey) {
  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(WriteOptions(), "", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("", "v"));
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes({""}));
}

// SSTs of both trie format versions are read by a factory writing either.
TEST_P(TrieIndexDBTest, MixedFormatVersions) {
  options_.disable_auto_compactions = true;
  const uint32_t other_version = GetFormatVersion() == 1 ? 2 : 1;
  std::vector<std::string> expected_keys;
  // The two SSTs interleave so that every Seek and Next consults both.
  for (uint32_t version : {GetFormatVersion(), other_version}) {
    trie_factory_ = std::make_shared<TrieIndexFactory>(version);
    ASSERT_OK(OpenDB(/*block_size=*/128));
    for (int i = version == GetFormatVersion() ? 0 : 1; i < 400; i += 2) {
      char key[16];
      snprintf(key, sizeof(key), "key_%04d", i);
      ASSERT_OK(db_->Put(WriteOptions(), key, "v" + std::to_string(version)));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->Close());
    db_.reset();
  }
  for (int i = 0; i < 400; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    expected_keys.emplace_back(key);
  }

  for (uint32_t version : {GetFormatVersion(), other_version}) {
    SCOPED_TRACE("reader writes format " + std::to_string(version));
    trie_factory_ = std::make_shared<TrieIndexFactory>(version);
    ASSERT_OK(OpenDB(/*block_size=*/128));
    ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(
        "key_0100", "v" + std::to_string(GetFormatVersion())));
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes("key_0101", "v" + std::to_string(other_version)));
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected_keys));
    // Rewrites both SSTs in the format this factory writes.
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected_keys));
    ASSERT_OK(db_->Close());
    db_.reset();
  }
}

TEST_P(TrieIndexDBTest, OpenRejectsNonBytewiseComparator) {
  options_.comparator = ReverseBytewiseComparator();
  ASSERT_EQ(OpenDB().code(), Status::Code::kNotSupported);
}

TEST_P(TrieIndexDBTest, OpenRejectsUnknownFormatVersion) {
  trie_factory_ = std::make_shared<TrieIndexFactory>(3);
  ASSERT_EQ(OpenDB().code(), Status::Code::kInvalidArgument);
}

TEST_P(TrieIndexDBTest, MixedSSTsWithAndWithoutUDI) {
  // This test mixes SSTs with and without UDI by reopening without UDI.
  // In primary mode, SSTs can still be read without UDI (the standard
  // index is always fully populated). This test validates the secondary-
  // mode mixed-SST fallback path.
  if (IsPrimaryMode()) {
    ROCKSDB_GTEST_BYPASS("Not applicable in primary mode");
    return;
  }
  options_.disable_auto_compactions = true;

  // Phase 1: Write with UDI -> SST1 has UDI + standard index.
  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "udi_val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_02", "udi_val2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Phase 2: Reopen WITHOUT UDI, write more -> SST2 has only standard index.
  ASSERT_OK(OpenDBWithoutUDI());
  ASSERT_OK(db_->Put(WriteOptions(), "key_03", "noudi_val3"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "noudi_val4"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Phase 3: Reopen WITH UDI again. SST1 uses trie, SST2 falls back to
  // standard index (UDI block missing -> logged warning, graceful fallback).
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // All 4 keys should be readable through both index paths.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "udi_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "udi_val2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "noudi_val3"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04", "noudi_val4"));

  ASSERT_NO_FATAL_FAILURE(
      VerifyScanBothIndexes({"key_01", "key_02", "key_03", "key_04"}));

  // Compact: merges UDI + non-UDI SSTs -> new SST has UDI.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(
      VerifyScanBothIndexes({"key_01", "key_02", "key_03", "key_04"}));
}

// TransactionDB commit: Put + Delete inside a transaction, then commit.
TEST_P(TrieIndexDBTest, TransactionCommit) {
  options_.create_if_missing = true;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(
      TransactionDB::Open(options_, TransactionDBOptions(), dbname_, &txn_db));
  db_.reset(txn_db);

  // Pre-populate a key.
  ASSERT_OK(txn_db->Put(WriteOptions(), "pre_key", "pre_val"));
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Begin transaction: Put + Delete + Commit.
  std::unique_ptr<Transaction> txn(
      txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn->Put("txn_key1", "txn_val1"));
  ASSERT_OK(txn->Delete("pre_key"));
  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Verify through both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("txn_key1", "txn_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("pre_key"));
}

// TransactionDB rollback: writes should be discarded. Rollback writes DELETE
// entries to WAL. Verifies the UDI builder correctly handles DELETE entries
// replayed from the WAL during recovery.
TEST_P(TrieIndexDBTest, TransactionRollback) {
  options_.create_if_missing = true;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(
      TransactionDB::Open(options_, TransactionDBOptions(), dbname_, &txn_db));
  db_.reset(txn_db);

  // Pre-populate data and flush.
  ASSERT_OK(txn_db->Put(WriteOptions(), "keep_key", "keep_val"));
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Begin transaction, write, then ROLLBACK.
  std::unique_ptr<Transaction> txn(
      txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn->Put("rollback_key", "rollback_val"));
  ASSERT_OK(txn->Delete("keep_key"));
  ASSERT_OK(txn->Rollback());

  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Original data should be unchanged. Rolled-back writes should not appear.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("keep_key", "keep_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("rollback_key"));

  // Forward scan: only the original key.
  ASSERT_NO_FATAL_FAILURE(
      VerifyScanBothIndexes(std::vector<std::pair<std::string, std::string>>{
          {"keep_key", "keep_val"}}));
}

// total_order_seek with prefix_extractor: a common stress-test configuration.
// With total_order_seek=true, SeekToFirst and full forward scan should work
// correctly even when a prefix extractor is configured.
TEST_P(TrieIndexDBTest, TotalOrderSeekWithPrefixExtractor) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Keys with different prefixes.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_2", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbb_1", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "ccc_1", "v4"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // With total_order_seek=true, scan all keys across prefixes.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.total_order_seek = true;
    auto keys = ScanAllKeys(base_ro);
    ASSERT_EQ(keys.size(), 4u);
    ASSERT_EQ(keys[0], "aaa_1");
    ASSERT_EQ(keys[1], "aaa_2");
    ASSERT_EQ(keys[2], "bbb_1");
    ASSERT_EQ(keys[3], "ccc_1");

    // Seek across prefix boundary.
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->Seek("aab");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb_1");
    ASSERT_OK(iter->status());
  }

  // auto_prefix_mode: let RocksDB decide per-seek.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.auto_prefix_mode = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->Seek("bbb_1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb_1");
    ASSERT_OK(iter->status());
  }
}

// ============================================================================
// Multi-level SST + DeleteRange randomized test
//
// Historically bug-prone area: range tombstones interact with data across
// LSM levels (L0, L1, L2+), and the trie index must correctly handle
// seek/scan when blocks are partially or entirely covered by range deletions
// at different levels.
//
// Strategy:
// 1. Populate bottommost level with baseline data (flush + compact)
// 2. Write overlapping data and DeleteRanges to L0 (multiple rounds)
// 3. Partial compactions to create data at intermediate levels
// 4. Verify reads match between standard and trie index after each mutation
// 5. Snapshot before large DeleteRange, verify snapshot preserves state
// 6. Re-insert into deleted ranges, compact, and re-verify
// ============================================================================
TEST_P(TrieIndexDBTest, MultiLevelDeleteRangeRandomized) {
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  options_.disable_auto_compactions = true;
  // Small block size forces many data blocks (and thus many trie entries).
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kMaxKey = 500;

  auto format_key = [](int k) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%05d", k);
    return std::string(buf);
  };

  // Core correctness check: forward scan via both indexes must match.
  // In kCustomOnly mode, standard-index reads fail loudly because only a
  // footer-satisfying stub exists, so we verify the trie scan succeeds.
  auto verify_scan_consistency = [&]() {
    auto trie_kvs = ScanAllKeyValues(TrieIndexReadOptions());
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      auto standard_kvs = ScanAllKeyValues(StandardIndexReadOptions());
      ASSERT_EQ(standard_kvs, trie_kvs)
          << "Scan mismatch: standard=" << standard_kvs.size()
          << " trie=" << trie_kvs.size();
    }
  };

  // Phase 1: Populate bottommost level with baseline data.
  for (int i = 0; i < 200; i++) {
    int k = rnd.Uniform(kMaxKey);
    ASSERT_OK(db_->Put(WriteOptions(), format_key(k),
                       "base_" + rnd.RandomString(20)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 2: Write overlapping data + DeleteRanges across multiple rounds.
  // Each round creates L0 SSTs with a mix of Puts and DeleteRanges,
  // with occasional partial compactions to push data to intermediate levels.
  for (int round = 0; round < 5; round++) {
    SCOPED_TRACE("round=" + std::to_string(round));

    // Write some new/updated keys.
    int num_writes = 30 + rnd.Uniform(70);
    for (int i = 0; i < num_writes; i++) {
      int k = rnd.Uniform(kMaxKey);
      ASSERT_OK(
          db_->Put(WriteOptions(), format_key(k),
                   "r" + std::to_string(round) + "_" + rnd.RandomString(15)));
    }

    // Issue 1-3 random DeleteRanges per round.
    int num_ranges = 1 + rnd.Uniform(3);
    for (int r = 0; r < num_ranges; r++) {
      int range_start = rnd.Uniform(kMaxKey - 10);
      int range_end = range_start + 5 + rnd.Uniform(50);
      if (range_end > kMaxKey) {
        range_end = kMaxKey;
      }
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 format_key(range_start),
                                 format_key(range_end)));
    }

    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

    // On odd rounds, do a partial compaction to push some data down,
    // creating a multi-level structure where range tombstones at L0
    // must shadow data at L1/L2.
    if (round % 2 == 1) {
      int compact_start = rnd.Uniform(kMaxKey / 2);
      int compact_end = compact_start + kMaxKey / 4;
      std::string start_key = format_key(compact_start);
      std::string end_key = format_key(compact_end);
      Slice s(start_key);
      Slice e(end_key);
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &s, &e));
      ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());
    }
  }

  // Phase 3: Snapshot, then delete a large range. The snapshot must
  // preserve the pre-deletion state while current reads see the deletion.
  const Snapshot* snap = db_->GetSnapshot();
  auto snap_kvs = ScanAllKeyValues(
      GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly
          ? TrieIndexReadOptions()
          : StandardIndexReadOptions());

  int big_start = rnd.Uniform(kMaxKey / 4);
  int big_end = big_start + kMaxKey / 3;
  if (big_end > kMaxKey) {
    big_end = kMaxKey;
  }
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             format_key(big_start), format_key(big_end)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current state should reflect the deletion.
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Snapshot state should be unchanged.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snap, snap_kvs));

  db_->ReleaseSnapshot(snap);

  // Phase 4: Re-insert keys into the deleted range, creating a pattern
  // where range tombstones and live data coexist at different levels.
  for (int i = big_start; i < big_end && i < kMaxKey; i += 3) {
    ASSERT_OK(db_->Put(WriteOptions(), format_key(i),
                       "reinserted_" + rnd.RandomString(10)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 5: Full compaction -- all range tombstones should be resolved.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 6: Point lookups for a sample of keys -- both indexes must agree.
  for (int i = 0; i < kMaxKey; i += 7) {
    std::string key = format_key(i);
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      std::string std_val;
      std::string trie_val;
      Status s1 = db_->Get(StandardIndexReadOptions(), key, &std_val);
      Status s2 = db_->Get(TrieIndexReadOptions(), key, &trie_val);
      ASSERT_EQ(s1.code(), s2.code()) << "Status mismatch for " << key;
      if (s1.ok()) {
        ASSERT_EQ(std_val, trie_val) << "Value mismatch for " << key;
      }
    } else {
      std::string trie_val;
      // Just verify trie reads don't crash in kCustomOnly mode.
      // The key may or may not exist (delete ranges active), so ignore
      // NotFound but check for unexpected errors.
      Status s = db_->Get(TrieIndexReadOptions(), key, &trie_val);
      ASSERT_TRUE(s.ok() || s.IsNotFound()) << s.ToString();
    }
  }
}

// ============================================================================
// Reverse iteration / direction switching tests
// ============================================================================

TEST_P(TrieIndexDBTest, ScanAfterCompactionWithDeletes) {
  // Forward and reverse scans after compaction with deletions -- verifies that
  // both scan directions produce correct results on compacted SSTs through
  // both indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(OpenDB());

  std::vector<std::string> expected;
  for (int i = 0; i < 20; i++) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
    expected.emplace_back(key);
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Delete some keys and flush again.
  ASSERT_OK(db_->Delete(WriteOptions(), "key_0003"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_0007"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_0015"));
  expected.erase(std::remove(expected.begin(), expected.end(), "key_0003"),
                 expected.end());
  expected.erase(std::remove(expected.begin(), expected.end(), "key_0007"),
                 expected.end());
  expected.erase(std::remove(expected.begin(), expected.end(), "key_0015"),
                 expected.end());
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact everything.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // VerifyScanBothIndexes checks forward+reverse through both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
}

TEST_P(TrieIndexDBTest, ScanLargeDatasetAcrossBlocks) {
  // Forward and reverse scans on a larger dataset spanning many blocks.
  ASSERT_OK(OpenDB(/*block_size=*/128));

  std::vector<std::pair<std::string, std::string>> expected;
  for (int i = 0; i < 500; i++) {
    char key[16];
    char val[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(val, sizeof(val), "val_%04d_%s", i, "padding");
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
    expected.emplace_back(key, val);
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // VerifyScanBothIndexes checks forward+reverse through both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
}

TEST_P(TrieIndexDBTest, SeekForPrevAfterCompaction) {
  // SeekForPrev on a compacted dataset -- verifies boundary correctness.
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 100; i += 2) {
    char key[16];
    char val[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // SeekForPrev to odd keys (not present) -- should land on the previous even.
  for (int i = 1; i < 100; i += 2) {
    char target[16];
    char expected_key[16];
    char expected_val[16];
    snprintf(target, sizeof(target), "key_%04d", i);
    snprintf(expected_key, sizeof(expected_key), "key_%04d", i - 1);
    snprintf(expected_val, sizeof(expected_val), "val_%04d", i - 1);
    ASSERT_NO_FATAL_FAILURE(
        VerifySeekForPrevBothIndexes(target, expected_key, expected_val));
  }
}

TEST_P(TrieIndexDBTest, PrevAfterSeekToFirstBothIndexes) {
  // Prev immediately after SeekToFirst should invalidate the iterator.
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, ForwardThenReverseDirection) {
  // Interleave forward and reverse iteration to test direction switching.
  ASSERT_OK(OpenDB(/*block_size=*/64));

  WriteSequentialKeys(0, 50);
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    // Seek to middle, go forward a few, then reverse.
    iter->Seek("key_0025");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0025");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0026");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0027");

    // Now reverse.
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0026");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0025");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0024");

    // Forward again.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0025");

    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, SeekToLastSingleEntry) {
  // SeekToLast on a single-entry SST.
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "only_key", "only_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : BothIndexReadOptions()) {
    SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom index"
                     : "builtin index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "only_key");
    ASSERT_EQ(iter->value().ToString(), "only_val");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

TEST_P(TrieIndexDBTest, ScanWithSnapshots) {
  // Forward and reverse scans at different snapshot points must produce
  // consistent results through both indexes.
  ASSERT_OK(OpenDB());
  options_.disable_auto_compactions = true;

  ASSERT_OK(db_->Put(WriteOptions(), "a", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snap1 = db_->GetSnapshot();

  ASSERT_OK(db_->Delete(WriteOptions(), "b"));
  ASSERT_OK(db_->Put(WriteOptions(), "d", "v4"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Scan at snap1: a, b, c (all present, no d).
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"a", "v1"}, {"b", "v2"}, {"c", "v3"}};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(snap1, expected));
  }

  // Scan at current: a, c, d (b deleted).
  {
    std::vector<std::string> expected = {"a", "c", "d"};
    ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(expected));
  }

  db_->ReleaseSnapshot(snap1);
}

TEST_P(TrieIndexDBTest, SeekForPrevVariableLengthKeys) {
  // Verifies SeekForPrev with variable-length keys that mimic
  // the stress test's key format: 8-byte big-endian key number followed by
  // optional padding bytes (0x78 = 'x'). Variable lengths exercise the
  // trie's handling of keys where one is a prefix of another.
  // Use crash test parameters: block_size=16384, prefix_size=1.
  options_.prefix_extractor.reset(NewFixedPrefixTransform(1));
  ASSERT_OK(OpenDB(/*block_size=*/16384));

  std::vector<std::string> keys;
  for (int key_num = 0; key_num < 100; key_num++) {
    // Vary key length: 8, 16, or 24 bytes (like max_key_len=1,2,3)
    for (int extra = 0; extra < 3; extra++) {
      std::string key(8 + extra * 8, '\0');
      // Big-endian key number in first 8 bytes
      for (int b = 0; b < 8; b++) {
        key[b] = static_cast<char>(
            (static_cast<uint64_t>(key_num) >> (56 - b * 8)) & 0xFF);
      }
      // Fill remaining bytes with padding (0x78 = 'x') and a secondary
      // number to ensure uniqueness
      for (size_t b = 8; b < key.size(); b++) {
        key[b] = (b < key.size() - 1) ? 0x78 : static_cast<char>(extra);
      }
      keys.push_back(key);
      ASSERT_OK(db_->Put(WriteOptions(), key, "val"));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more keys in a second flush to create overlapping L0 SSTs.
  for (int key_num = 50; key_num < 150; key_num++) {
    std::string key(8, '\0');
    for (int b = 0; b < 8; b++) {
      key[b] = static_cast<char>(
          (static_cast<uint64_t>(key_num) >> (56 - b * 8)) & 0xFF);
    }
    // Overwrite with different padding length
    key += "yy";
    ASSERT_OK(db_->Put(WriteOptions(), key, "val2"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Also delete some keys to create tombstones.
  for (int key_num = 20; key_num < 40; key_num++) {
    std::string key(8, '\0');
    for (int b = 0; b < 8; b++) {
      key[b] = static_cast<char>(
          (static_cast<uint64_t>(key_num) >> (56 - b * 8)) & 0xFF);
    }
    ASSERT_OK(db_->Delete(WriteOptions(), key));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // SeekForPrev for every key -- trie must match standard index.
  for (const auto& key : keys) {
    for (const auto& ro : BothIndexReadOptions()) {
      SCOPED_TRACE(ro.read_index != ReadOptions::ReadIndex::kDefault
                       ? "custom index"
                       : "builtin index");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->SeekForPrev(key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
    }
    if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kCustomOnly) {
      std::string std_result;
      std::string trie_result;
      {
        std::unique_ptr<Iterator> iter(
            db_->NewIterator(StandardIndexReadOptions()));
        iter->SeekForPrev(key);
        ASSERT_TRUE(iter->Valid());
        std_result = iter->key().ToString();
        ASSERT_OK(iter->status());
      }
      {
        std::unique_ptr<Iterator> iter(
            db_->NewIterator(TrieIndexReadOptions()));
        iter->SeekForPrev(key);
        ASSERT_TRUE(iter->Valid());
        trie_result = iter->key().ToString();
        ASSERT_OK(iter->status());
      }
      ASSERT_EQ(std_result, trie_result)
          << "SeekForPrev(" << key << ") diverged";
    }
  }

  // SeekForPrev for keys between existing keys.
  for (int i = 0; i < 200; i++) {
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "k%04da", i);
    std::string target(buf, len);

    if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
      // Just verify trie works in kCustomOnly mode.
      std::unique_ptr<Iterator> iter(db_->NewIterator(TrieIndexReadOptions()));
      iter->SeekForPrev(target);
      ASSERT_OK(iter->status());
      continue;
    }
    std::string std_result;
    std::string trie_result;
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(StandardIndexReadOptions()));
      iter->SeekForPrev(target);
      if (iter->Valid()) {
        std_result = iter->key().ToString();
      }
      ASSERT_OK(iter->status());
    }
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(TrieIndexReadOptions()));
      iter->SeekForPrev(target);
      if (iter->Valid()) {
        trie_result = iter->key().ToString();
      }
      ASSERT_OK(iter->status());
    }
    ASSERT_EQ(std_result, trie_result)
        << "SeekForPrev(" << target << ") diverged";
  }
}

// ============================================================================
// Primary UDI backward compatibility test -- uses explicit mode, not
// parameterized, because it tests the upgrade path from secondary to primary.
// ============================================================================

TEST_P(TrieIndexDBTest, PrimaryUDIBackwardCompatibility) {
  // Verifies that SSTs written with UDI as secondary (both indexes present)
  // can be read correctly when the DB is reopened with
  // index_mode=kCustomDefault. This is the upgrade path: old SSTs have both
  // indexes, new config says "use UDI as primary for all reads."
  ASSERT_OK(OpenDBSecondary(/*block_size=*/128));

  WriteSequentialKeys(0, 50);
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify via trie (secondary mode -- explicit ReadOptions).
  auto trie_keys = ScanAllKeys(TrieIndexReadOptions());
  ASSERT_EQ(trie_keys.size(), 50u);

  // Close and reopen with primary UDI config.
  EXPECT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDBPrimary(/*block_size=*/128));

  // Now all reads automatically use UDI -- no ReadOptions::read_index needed.
  ReadOptions ro;
  std::vector<std::string> keys;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys, trie_keys);

  // Point lookups also work.
  std::string value;
  ASSERT_OK(db_->Get(ro, "key_0025", &value));
  ASSERT_EQ(value, "val_0025");
}

// ============================================================================
// Migration path and rollback tests
// ============================================================================

TEST_P(TrieIndexDBTest, MigrationFullPath) {
  // Tests the complete recommended migration path:
  // Step 1: No UDI -> Step 2: UDI secondary -> Step 3: Compact all SSTs ->
  // Step 4: UDI primary
  // In kCustomOnly mode, standard-index reads fail loudly because only a
  // footer-satisfying stub exists, so the mixed-mode migration path is not
  // applicable.
  if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ROCKSDB_GTEST_BYPASS("Not applicable in kCustomOnly mode");
    return;
  }

  // Step 1: Start without UDI. Write some data.
  ASSERT_OK(OpenDBWithoutUDI(/*block_size=*/128));
  WriteSequentialKeys(0, 30);
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Step 2: Enable UDI as secondary. Write more data.
  ASSERT_OK(OpenDBSecondary(/*block_size=*/128));
  WriteSequentialKeys(30, 50);
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify reads work through both indexes for the new SST.
  std::string value;
  ASSERT_OK(db_->Get(TrieIndexReadOptions(), "key_0035", &value));
  ASSERT_EQ(value, "val_0035");
  // Old SST (no UDI) readable through standard index.
  ASSERT_OK(db_->Get(ReadOptions(), "key_0010", &value));
  ASSERT_EQ(value, "val_0010");

  // Step 3: Compact everything to rewrite all SSTs with UDI.
  // Use bottommost_level_compaction to ensure ALL SSTs are rewritten.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // After compaction, all data is in SSTs with both indexes.
  auto all_keys = ScanAllKeys(TrieIndexReadOptions());
  ASSERT_EQ(all_keys.size(), 50u);

  // Reopen in secondary mode to ensure the MANIFEST is clean -- old SST
  // files from step 1 (without UDI) are fully purged.
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDBSecondary(/*block_size=*/128));
  ASSERT_EQ(ScanAllKeys(TrieIndexReadOptions()).size(), 50u);
  ASSERT_OK(db_->Close());
  db_.reset();

  // Step 4: Enable UDI as primary.
  ASSERT_OK(OpenDBPrimary(/*block_size=*/128));

  // All reads go through UDI automatically -- no read_index needed.
  ReadOptions ro;
  std::vector<std::string> keys;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys, all_keys);

  // Point lookups from all original data.
  ASSERT_OK(db_->Get(ro, "key_0000", &value));
  ASSERT_EQ(value, "val_0000");
  ASSERT_OK(db_->Get(ro, "key_0049", &value));
  ASSERT_EQ(value, "val_0049");
}

TEST_P(TrieIndexDBTest, MigrationPrimaryRejectsPreUDISSTs) {
  // Verifies that enabling index_mode=kCustomDefault on a DB with SSTs
  // that have no UDI block fails at open time (not silently).
  options_.disable_auto_compactions = true;

  // Write SSTs without UDI.
  ASSERT_OK(OpenDBWithoutUDI(/*block_size=*/128));
  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "val_a"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Try to open with UDI as primary -- should fail because the SST has no
  // UDI block.
  Status s = OpenDBPrimary(/*block_size=*/128);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_P(TrieIndexDBTest, RollbackFromPrimaryToSecondary) {
  // Tests the rollback path: primary -> compact with secondary -> remove UDI.

  // Start in primary mode. Write data.
  ASSERT_OK(OpenDBPrimary(/*block_size=*/128));
  WriteSequentialKeys(0, 30);
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify data is readable in primary mode.
  ReadOptions ro;
  std::string value;
  ASSERT_OK(db_->Get(ro, "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  ASSERT_OK(db_->Close());
  db_.reset();

  // Rollback step 1: Reopen as secondary (with UDI factory still set).
  // Primary UDI routing is purely config-driven, so reopening as secondary
  // immediately reverts all reads to the standard index path. The trie is
  // still accessible via explicit ReadOptions::read_index.
  ASSERT_OK(OpenDBSecondary(/*block_size=*/128));

  // Verify the primary-mode SST is readable through both paths.
  // Explicit trie read:
  ASSERT_OK(db_->Get(TrieIndexReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  // Standard index read (default ReadOptions, no read_index):
  ASSERT_OK(db_->Get(ReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");

  // Full scan to verify all data is accessible.
  auto all_keys = ScanAllKeys(ReadOptions());
  ASSERT_EQ(all_keys.size(), 30u);

  // Rollback step 2: Compact to rewrite all SSTs with both indexes.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Verify data is readable through trie after compaction.
  ASSERT_OK(db_->Get(TrieIndexReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");

  // Reopen to ensure old SSTs are fully purged and only the compacted
  // output (with both indexes) remains.
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDBSecondary(/*block_size=*/128));

  // Now all SSTs were written by secondary mode -- standard index works.
  ASSERT_OK(db_->Get(ReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  ASSERT_EQ(ScanAllKeys(ReadOptions()), all_keys);
  ASSERT_OK(db_->Close());
  db_.reset();

  // Rollback step 3: Remove UDI entirely. SSTs have both indexes so
  // the standard index works.
  ASSERT_OK(OpenDBWithoutUDI(/*block_size=*/128));
  ASSERT_OK(db_->Get(ReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  ASSERT_EQ(ScanAllKeys(ReadOptions()), all_keys);
}

// An SST written in kCustomOnly has only a stub where the standard index would
// be, so the trie is the only way to read it. Lowering index_mode afterwards
// must not brick those files: both kCustomDefault and kStandardDefault keep
// serving default reads by routing to the trie. Only an explicit kBuiltin
// request fails, because there is no standard index to honor it with.
TEST_P(TrieIndexDBTest, CustomOnlySSTsSurviveLoweredIndexMode) {
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kStandardDefault) {
    ROCKSDB_GTEST_SKIP("Sets its own index modes");
    return;
  }
  ASSERT_OK(OpenDBImpl(/*block_size=*/128,
                       BlockBasedTableOptions::IndexMode::kCustomOnly));
  WriteSequentialKeys(0, 30);
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions trie_ro;
  trie_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  const std::vector<std::string> expected_keys = ScanAllKeys(trie_ro);
  ASSERT_EQ(expected_keys.size(), 30u);
  ASSERT_OK(db_->Close());
  db_.reset();

  // Lower to kCustomDefault: the trie is still primary, so nothing changes.
  ASSERT_OK(OpenDBImpl(/*block_size=*/128,
                       BlockBasedTableOptions::IndexMode::kCustomDefault));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  ASSERT_EQ(ScanAllKeys(ReadOptions()), expected_keys);
  ASSERT_OK(db_->Close());
  db_.reset();

  // Lower to kStandardDefault: the standard index is nominally primary, but
  // these SSTs only carry a stub, so default reads still route to the trie.
  ASSERT_OK(OpenDBImpl(/*block_size=*/128,
                       BlockBasedTableOptions::IndexMode::kStandardDefault));
  ASSERT_OK(db_->Get(ReadOptions(), "key_0015", &value));
  ASSERT_EQ(value, "val_0015");
  ASSERT_EQ(ScanAllKeys(ReadOptions()), expected_keys);

  // An explicit standard-index read cannot be served by a stub.
  ReadOptions builtin_ro;
  builtin_ro.read_index = ReadOptions::ReadIndex::kBuiltin;
  ASSERT_NOK(db_->Get(builtin_ro, "key_0015", &value));
  std::unique_ptr<Iterator> iter(db_->NewIterator(builtin_ro));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsInvalidArgument());
}

TEST_P(TrieIndexDBTest, RollbackFromPrimaryWithoutCompactSucceeds) {
  // Verifies that removing UDI from primary-mode SSTs WITHOUT compacting
  // first still works. The standard index is always fully populated (even
  // in primary mode), so reads fall back to the standard index correctly.
  // In kCustomOnly mode, standard-index reads fail loudly because only a
  // footer-satisfying stub exists, so this rollback path is not applicable.
  if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ROCKSDB_GTEST_BYPASS("Not applicable in kCustomOnly mode");
    return;
  }
  options_.disable_auto_compactions = true;

  // Write SSTs in primary mode.
  ASSERT_OK(OpenDBPrimary(/*block_size=*/128));
  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "val_a"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Open without UDI -- reads fall back to the standard index.
  ASSERT_OK(OpenDBWithoutUDI(/*block_size=*/128));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key_a", &value));
  ASSERT_EQ(value, "val_a");

  // Scan also works.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key_a");
  ASSERT_OK(iter->status());
}

TEST_P(TrieIndexDBTest, PrimaryModeTableProperties) {
  // Verifies primary-mode-specific behavior: the udi_is_primary_index table
  // property is set (informational, does not affect read routing), and
  // reads work without setting ReadOptions::read_index.
  if (!IsPrimaryMode()) {
    ROCKSDB_GTEST_BYPASS("Only applicable in primary mode");
    return;
  }
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "val2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify the informational table property is set.
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_FALSE(props.empty());
  for (const auto& p : props) {
    ASSERT_EQ(p.second->udi_is_primary_index, 1u);
  }

  // Reads work with default ReadOptions (no read_index needed).
  ReadOptions ro;
  std::string value;
  ASSERT_OK(db_->Get(ro, "key1", &value));
  ASSERT_EQ(value, "val1");
}

TEST_P(TrieIndexDBTest, EstimatedSizeNonZero) {
  // Verifies that TrieIndexBuilder::EstimatedSize() returns non-zero after
  // adding entries, ensuring compaction file sizing works. In kCustomOnly
  // mode, the standard index is a stub with zero size -- the trie's size is
  // tracked separately in the meta block, not in the index_size property.
  if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomOnly) {
    ROCKSDB_GTEST_BYPASS("index_size property tracks standard index only");
    return;
  }
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Write enough data to produce multiple blocks.
  WriteSequentialKeys(0, 200);
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Check that the SST's index size is non-zero in table properties.
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_FALSE(props.empty());
  for (const auto& p : props) {
    ASSERT_GT(p.second->index_size, 0u);
  }
}

// ============================================================================
// Issue #14561 DB-level reproducer: non-boundary separator seek correctness
// ============================================================================

TEST_P(TrieIndexDBTest, NonBoundarySeparatorSeekCorrectness) {
  // DB-level reproducer for GitHub issue #14561. When FindShortestSeparator
  // cannot shorten the separator (e.g., "acc" -> "acd" stays "acc") and
  // seqno encoding is active from an earlier same-user-key boundary, the
  // post-seek correction must not advance past the correct block.
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write a same-user-key pair to trigger seqno encoding.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa", std::string(100, 'a')));
  ManagedSnapshot snap_for_encoding(db_.get());
  ASSERT_OK(db_->Put(WriteOptions(), "aaa", std::string(100, 'A')));

  // Write keys where shortening fails: "acc" -> "acd" stays "acc".
  ASSERT_OK(db_->Put(WriteOptions(), "acc", std::string(100, 'c')));
  ASSERT_OK(db_->Put(WriteOptions(), "acd", std::string(100, 'd')));
  ManagedSnapshot read_snap(db_.get());
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Seek for "acc" should find "acc" through both indexes.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.snapshot = read_snap.snapshot();
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->Seek("acc");
    ASSERT_TRUE(iter->Valid()) << "Seek(acc) should be valid";
    ASSERT_EQ(iter->key().ToString(), "acc");
    ASSERT_OK(iter->status());
  }

  // Also verify point Get works.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.snapshot = read_snap.snapshot();
    std::string value;
    ASSERT_OK(db_->Get(base_ro, "acc", &value));
    ASSERT_EQ(value, std::string(100, 'c'));
  }
}

// ============================================================================
// Multi-CF iterator with trie index
// ============================================================================

TEST_P(TrieIndexDBTest, MultiCFCoalescingIterator) {
  // Exercises the trie index through NewCoalescingIterator with multiple
  // distinct column families. This covers the trie + use_multi_cf_iterator
  // gap identified in issue #14560.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  options_.create_if_missing = true;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode = GetIndexMode();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  // Create two additional CFs.
  ColumnFamilyHandle* cf1 = nullptr;
  ColumnFamilyHandle* cf2 = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_one", &cf1));
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_two", &cf2));

  // Write different keys to each CF (with some overlap in key space).
  for (int i = 0; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "default_" + std::to_string(i)));
    ASSERT_OK(db_->Put(WriteOptions(), cf1, key, "cf1_" + std::to_string(i)));
    ASSERT_OK(db_->Put(WriteOptions(), cf2, key, "cf2_" + std::to_string(i)));
  }

  // Flush all CFs.
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Flush(FlushOptions(), cf1));
  ASSERT_OK(db_->Flush(FlushOptions(), cf2));

  // Create coalescing iterator across all CFs.
  ReadOptions ro = TrieIndexReadOptions();
  std::vector<ColumnFamilyHandle*> cfhs = {db_->DefaultColumnFamily(), cf1,
                                           cf2};
  std::unique_ptr<Iterator> coal_iter = db_->NewCoalescingIterator(ro, cfhs);
  ASSERT_NE(coal_iter, nullptr);

  // Forward scan: should see all 20 keys.
  coal_iter->SeekToFirst();
  int count = 0;
  std::string prev_key;
  while (coal_iter->Valid()) {
    std::string key = coal_iter->key().ToString();
    if (!prev_key.empty()) {
      ASSERT_GT(key, prev_key) << "Keys must be in order";
    }
    prev_key = key;
    count++;
    coal_iter->Next();
  }
  ASSERT_OK(coal_iter->status());
  ASSERT_EQ(count, 20);

  // Reverse scan.
  coal_iter->SeekToLast();
  count = 0;
  prev_key.clear();
  while (coal_iter->Valid()) {
    std::string key = coal_iter->key().ToString();
    if (!prev_key.empty()) {
      ASSERT_LT(key, prev_key) << "Reverse keys must be in order";
    }
    prev_key = key;
    count++;
    coal_iter->Prev();
  }
  ASSERT_OK(coal_iter->status());
  ASSERT_EQ(count, 20);

  // Seek to specific key.
  coal_iter->Seek("key_0010");
  ASSERT_TRUE(coal_iter->Valid());
  ASSERT_EQ(coal_iter->key().ToString(), "key_0010");
  ASSERT_OK(coal_iter->status());

  delete cf1;
  delete cf2;
}

// ============================================================================
// GetEntity with explicit snapshot comparison
// ============================================================================

TEST_P(TrieIndexDBTest, GetEntityWithExplicitSnapshotComparison) {
  // Compares GetEntity results between standard and trie indexes using an
  // explicit snapshot. This covers the gap identified in issue #14562.
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Write PutEntity and regular Put.
  WideColumns columns{
      {kDefaultWideColumnName, "default_val_v1"},
      {"col_a", "val_a_v1"},
  };
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "entity_key", columns));
  ASSERT_OK(db_->Put(WriteOptions(), "regular_key", "regular_val_v1"));

  // Take an explicit snapshot.
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  // Overwrite both keys.
  WideColumns columns_v2{
      {kDefaultWideColumnName, "default_val_v2"},
      {"col_a", "val_a_v2"},
      {"col_b", "val_b_v2"},
  };
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "entity_key", columns_v2));
  ASSERT_OK(db_->Put(WriteOptions(), "regular_key", "regular_val_v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Read at snapshot through both indexes -- should see v1 data.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.snapshot = snap;

    // GetEntity on PutEntity key at snapshot.
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(base_ro, db_->DefaultColumnFamily(), "entity_key",
                             &result));
    ASSERT_EQ(result.columns().size(), 2u);
    ASSERT_EQ(result.columns()[0].value(), "default_val_v1");
    ASSERT_EQ(result.columns()[1].value(), "val_a_v1");

    // GetEntity on regular Put key at snapshot.
    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(base_ro, db_->DefaultColumnFamily(), "regular_key",
                             &result2));
    ASSERT_EQ(result2.columns().size(), 1u);
    ASSERT_EQ(result2.columns()[0].value(), "regular_val_v1");

    // GetEntity on nonexistent key.
    PinnableWideColumns result3;
    ASSERT_TRUE(db_->GetEntity(base_ro, db_->DefaultColumnFamily(),
                               "no_such_key", &result3)
                    .IsNotFound());
  }

  // Read without snapshot -- should see v2 data.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");

    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(base_ro, db_->DefaultColumnFamily(), "entity_key",
                             &result));
    ASSERT_EQ(result.columns().size(), 3u);
    ASSERT_EQ(result.columns()[0].value(), "default_val_v2");
    ASSERT_EQ(result.columns()[1].value(), "val_a_v2");
    ASSERT_EQ(result.columns()[2].value(), "val_b_v2");
  }

  db_->ReleaseSnapshot(snap);
}

// ============================================================================
// Same-user-key across blocks with Prev (reverse iteration through overflow)
// ============================================================================

TEST_P(TrieIndexDBTest, ReverseIterationAcrossSameUserKeyBlocks) {
  // Verifies that reverse iteration correctly traverses through same-user-key
  // blocks (overflow runs). This exercises the PrevAndGetResult overflow
  // decrement path at the DB level.
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write multiple versions of the same key to force them across blocks.
  std::vector<const Snapshot*> snapshots;
  for (int i = 0; i < 8; i++) {
    ASSERT_OK(
        db_->Put(WriteOptions(), "same_key", "value_v" + std::to_string(i)));
    snapshots.push_back(db_->GetSnapshot());
  }
  // Write surrounding keys.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa", "before"));
  ASSERT_OK(db_->Put(WriteOptions(), "zzz", "after"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan at latest snapshot should see: aaa, same_key(v7), zzz.
  std::vector<std::string> expected = {"aaa", "same_key", "zzz"};
  VerifyScanBothIndexes(expected);

  // At an older snapshot, same_key should have the older value.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    base_ro.snapshot = snapshots[3];
    std::string value;
    ASSERT_OK(db_->Get(base_ro, "same_key", &value));
    ASSERT_EQ(value, "value_v3");
  }

  // Reverse scan through trie should produce zzz, same_key, aaa.
  for (auto base_ro : BothIndexReadOptions()) {
    SCOPED_TRACE(base_ro.read_index != ReadOptions::ReadIndex::kDefault
                     ? "custom"
                     : "builtin");
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "zzz");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "same_key");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "aaa");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  for (auto* snap : snapshots) {
    db_->ReleaseSnapshot(snap);
  }
}

// ============================================================================
// Prefetch boundary comparison with custom index wrapper
// ============================================================================

TEST_P(TrieIndexDBTest, PrefetchWithCustomIndexWrapper) {
  // Exercises the Prefetch() code path with the custom index wrapper active.
  // In primary mode, Prefetch() uses the UDI wrapper whose key() returns an
  // internal key (user key + 8-byte trailer). The Prefetch boundary comparison
  // must use user_key() (not key()) when index_key_is_user_key=1 to avoid
  // including the trailer in the comparison.
  //
  // This test writes keys with NO same-user-key boundaries (each key is
  // unique), so index_key_is_user_key=1 in the SST properties. This is the
  // condition that triggers the user-key comparison path in Prefetch.
  if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kStandardDefault) {
    // In secondary mode, Prefetch uses the standard IndexBlockIter (the
    // wrapper falls through for internal ReadOptions). The bug only
    // manifests in primary mode where the wrapper intercepts all reads.
    ROCKSDB_GTEST_BYPASS("Only applicable in primary modes");
    return;
  }
  ASSERT_OK(OpenDB(/*block_size=*/256));

  // Write unique keys (no same-user-key boundaries) to ensure
  // index_key_is_user_key=1.
  for (int i = 0; i < 500; i++) {
    char key[32];
    snprintf(key, sizeof(key), "prefetch_key_%06d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, std::string(200, 'v')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify SST properties. In kCustomDefault mode, index_key_is_user_key=1
  // because the standard builder sees no same-user-key boundaries (all keys
  // unique). In kCustomOnly mode, the standard index is a stub; this test only
  // needs the custom-default assertion because primary mode is covered below by
  // the reopen-time prefetch checks.
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_FALSE(props.empty());
  if (GetIndexMode() == BlockBasedTableOptions::IndexMode::kCustomDefault) {
    for (const auto& p : props) {
      ASSERT_EQ(p.second->index_key_is_user_key, 1u)
          << "kCustomDefault with unique keys: expected user-key-only "
             "separators";
    }
  }

  // Close and reopen to trigger Prefetch during Open.
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB(/*block_size=*/256));

  // Exercises the Prefetch code path with the custom index wrapper in
  // primary mode. The wrapper's key() includes an 8-byte internal key
  // trailer that Prefetch's boundary comparison must handle correctly
  // via user_key(). An incorrect comparison would cause wrong prefetch
  // boundaries (performance issue, not data correctness). Verify the
  // reopen succeeds and all data is readable.
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->SeekToFirst();
  int count = 0;
  while (iter->Valid()) {
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 500);

  // Also verify point lookups work (uses a different read path).
  std::string value;
  ASSERT_OK(db_->Get(ro, "prefetch_key_000000", &value));
  ASSERT_EQ(value, std::string(200, 'v'));
  ASSERT_OK(db_->Get(ro, "prefetch_key_000499", &value));
  ASSERT_EQ(value, std::string(200, 'v'));
}

// End-to-end parallel compression: a DB with the trie UDI factory and
// compression_opts.parallel_threads > 1 must start the parallel pipeline (the
// trie builder opts into PrepareAddEntry / FinishAddEntry) and produce SSTs
// that read back correctly in all three modes. Any divergence between the
// parallel and serial trie construction surfaces as missing or extra keys.
TEST_P(TrieIndexDBTest, ParallelCompressionWithTrieIndex) {
  // Compression must be on for the parallel pipeline to actually run;
  // with no compressor the table builder silently falls back to serial.
  // Use Zlib because it's reliably linked across test build configs
  // where Snappy / LZ4 / ZSTD may not be.
  if (!Zlib_Supported()) {
    ROCKSDB_GTEST_SKIP("Zlib not linked into this build");
    return;
  }
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  options_.write_buffer_size = 1 << 20;  // small buffer -> multiple flushes

  ASSERT_OK(OpenDB());

  ParallelCompressionStartCounter parallel_starts;
  // Enough keys to produce many data blocks -> many index entries flowing
  // through the parallel pipeline.
  const std::string value(200, 'v');
  const std::vector<std::string> keys = PutNumberedKeys(500, value);
  ASSERT_OK(db_->Flush(FlushOptions()));
  EXPECT_GT(parallel_starts.started(), 0);

  ASSERT_NO_FATAL_FAILURE(VerifyScanBothIndexes(keys));
  for (size_t i : {0, 7, 123, 250, 499}) {
    ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(keys[i], value));
  }
}

// Trie + parallel compression on top of the kHashSearch built-in index.
//
// kCustomDefault / kCustomOnly restrict index_type to kBinarySearch /
// kBinarySearchWithFirstKey (the UDI wrapper layout is incompatible with
// hash / partitioned indexes). kStandardDefault has no such restriction,
// so the trie can coexist with any built-in index_type. The
// parameterized ParallelCompressionWithTrieIndex above covers the
// kBinarySearch case; this test covers kHashSearch. kTwoLevelIndexSearch
// is excluded because partition_filters constraints force
// parallel_threads = 1.
TEST_P(TrieIndexDBTest, ParallelCompressionWithHashStandardIndexAndTrieUdi) {
  if (GetIndexMode() != BlockBasedTableOptions::IndexMode::kStandardDefault) {
    ROCKSDB_GTEST_SKIP("Sets its own index modes");
    return;
  }
  if (!Zlib_Supported()) {
    ROCKSDB_GTEST_SKIP("Zlib not linked into this build");
    return;
  }

  options_.create_if_missing = true;
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  options_.write_buffer_size = 1 << 20;
  options_.prefix_extractor.reset(NewFixedPrefixTransform(4));

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  ParallelCompressionStartCounter parallel_starts;
  // Enough keys to fill multiple blocks -> multiple iterations through
  // the parallel ring buffer for both the hash builder and the trie
  // builder.
  const std::string value(200, 'v');
  const std::vector<std::string> keys = PutNumberedKeys(500, value);
  ASSERT_OK(db_->Flush(FlushOptions()));
  EXPECT_GT(parallel_starts.started(), 0);

  // This test is not parameterized, so the mode-aware read helpers do not
  // apply. Default reads use the hash index; kPreferCustom uses the trie.
  std::vector<ReadOptions> read_options(2);
  read_options[1].read_index = ReadOptions::ReadIndex::kPreferCustom;
  for (const ReadOptions& ro : read_options) {
    SCOPED_TRACE(ro.read_index == ReadOptions::ReadIndex::kPreferCustom
                     ? "trie index"
                     : "hash index");
    ASSERT_EQ(ScanAllKeys(ro), keys);
    for (size_t i : {0, 7, 123, 250, 499}) {
      std::string v;
      ASSERT_OK(db_->Get(ro, keys[i], &v));
      ASSERT_EQ(v, value);
    }
  }
}

// Format 1 is only supported on little-endian hosts.
std::vector<uint32_t> SupportedFormatVersions() {
  if (port::kLittleEndian) {
    return {1, 2};
  }
  return {2};
}

// Run all parameterized tests in all three custom UDI modes:
// - kStandardDefault: UDI is secondary, reads require read_index
// - kCustomDefault: UDI is primary, all reads use the trie by default
// - kCustomOnly: UDI is primary, standard-index reads fail loudly
// and with both trie format versions.
INSTANTIATE_TEST_CASE_P(
    AllIndexModes, TrieIndexDBTest,
    ::testing::Combine(
        ::testing::Values(BlockBasedTableOptions::IndexMode::kStandardDefault,
                          BlockBasedTableOptions::IndexMode::kCustomDefault,
                          BlockBasedTableOptions::IndexMode::kCustomOnly),
        ::testing::ValuesIn(SupportedFormatVersions())));

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
