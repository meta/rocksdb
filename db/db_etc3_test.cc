//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "rocksdb/convenience.h"
#include "rocksdb/experimental.h"
#include "rocksdb/metadata.h"
#include "rocksdb/sst_file_writer.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

class DBEtc3Test : public DBTestBase {
 public:
  DBEtc3Test() : DBTestBase("db_etc3_test", /*env_do_fsync=*/true) {}
};

namespace {
// The configuration in which RocksDB 11.10 through 11.13 could write a common
// prefix extending past a user key: format_version=8 (with the default
// optimize_key_common_prefix) and ReverseBytewiseComparator, which orders a
// user key after longer user keys that extend it.
Options PreviouslyBuggyCommonPrefixOptions(
    Options options, BlockBasedTableOptions table_options) {
  options.comparator = ReverseBytewiseComparator();
  options.disable_auto_compactions = true;
  table_options.format_version = 8;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  return options;
}

// Makes BlockBuilder compute the common prefix as RocksDB 11.10 through 11.13
// did, which could extend it past a user key.
void SimulateOldCommonPrefixWriter() {
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBuilder::AddWithLastKeyImpl:CommonPrefixUserKeySize",
      [](void* arg) {
        *static_cast<size_t*>(arg) = std::numeric_limits<size_t>::max();
      });
}

void StopSimulatingOldCommonPrefixWriter() {
  SyncPoint::GetInstance()->ClearCallBack(
      "BlockBuilder::AddWithLastKeyImpl:CommonPrefixUserKeySize");
}

// Data and index blocks found on load to have a last user key shorter than
// their common prefix.
struct ShortLastUserKeyBlockCounts {
  std::atomic<int> data{0};
  std::atomic<int> index{0};
};

void CountShortLastUserKeyBlocks(ShortLastUserKeyBlockCounts* counts) {
  SyncPoint::GetInstance()->SetCallBack(
      "Block::InitializeDataBlockCommonPrefixCheck:LastUserKeyShorter",
      [counts](void*) { counts->data.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "Block::InitializeIndexBlockCommonPrefixCheck:LastUserKeyShorter",
      [counts](void*) { counts->index.fetch_add(1); });
}

const std::string kPutLonger(
    "ab\x01"
    "c",
    4);
}  // namespace

TEST_F(DBEtc3Test, CommonKeyPrefixWithShorterKeyAfterLongerKey) {
  DestroyAndReopen(PreviouslyBuggyCommonPrefixOptions(
      CurrentOptions(), BlockBasedTableOptions()));
  ShortLastUserKeyBlockCounts counts;
  CountShortLastUserKeyBlocks(&counts);
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("xy", "old"));
  ASSERT_OK(Flush());
  const std::string del_long(
      "xy\x00"
      "c",
      4);
  ASSERT_OK(Put(del_long, "long"));
  ASSERT_OK(Delete("xy"));
  ASSERT_OK(Flush());
  EXPECT_EQ("long", Get(del_long));
  EXPECT_EQ("NOT_FOUND", Get("xy"));

  ASSERT_OK(Put(kPutLonger, "long"));
  ASSERT_OK(Put("ab", "short"));
  ASSERT_OK(Flush());
  EXPECT_EQ("long", Get(kPutLonger));
  EXPECT_EQ("short", Get("ab"));

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("ab");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("ab", iter->key().ToString());
  ASSERT_OK(iter->status());

  EXPECT_EQ(0, counts.data.load());
  EXPECT_EQ(0, counts.index.load());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBEtc3Test, CommonKeyPrefixReadsBlocksWithShorterKey) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.max_subcompactions = 2;
  options.target_file_size_base = 1024;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1000;
  table_options.block_size_deviation = 0;
  DestroyAndReopen(PreviouslyBuggyCommonPrefixOptions(options, table_options));
  ShortLastUserKeyBlockCounts counts;
  CountShortLastUserKeyBlocks(&counts);
  SimulateOldCommonPrefixWriter();
  SyncPoint::GetInstance()->EnableProcessing();

  // Data block [kPutLonger, "ab"] with common prefix "ab\x01".
  const std::string big(2000, 'v');
  ASSERT_OK(Put("c", big));
  ASSERT_OK(Put(kPutLonger, "x"));
  ASSERT_OK(Put("ab", big));
  ASSERT_OK(Put("aa", big));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "y"));
  ASSERT_OK(Flush());
  StopSimulatingOldCommonPrefixWriter();

  auto verify = [&]() {
    EXPECT_EQ(big, Get("ab"));
    EXPECT_EQ("x", Get(kPutLonger));
    EXPECT_EQ(std::vector<std::string>({big, "x"}),
              MultiGet({"ab", kPutLonger}));
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    // "ab\x01" equals the common prefix, so it can be compared by suffix.
    for (const Slice& target :
         {Slice("ab"), Slice("ab\x00", 3), Slice("ab\x01", 3)}) {
      iter->Seek(target);
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ("ab", iter->key().ToString());
    }
    iter->SeekForPrev("ab");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("ab", iter->key().ToString());
    iter->SeekForPrev(Slice("ab\x01", 3));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kPutLonger, iter->key().ToString());
    ASSERT_OK(iter->status());
  };
  verify();
  EXPECT_GT(counts.data.load(), 0);

  // With subcompactions, a compaction starts by seeking to a key.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  verify();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBEtc3Test, CommonKeyPrefixIteratesBlocksWithShorterKey) {
  // Iteration seeks internally after skipping many versions of a key
  // (max_sequential_skip_in_iterations) and when reversing.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(
      PreviouslyBuggyCommonPrefixOptions(options, BlockBasedTableOptions()));
  ShortLastUserKeyBlockCounts counts;
  CountShortLastUserKeyBlocks(&counts);
  SimulateOldCommonPrefixWriter();
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("aa", "va"));
  ASSERT_OK(Flush());
  // Data block [kPutLonger, "ab"] with common prefix "ab\x01".
  ASSERT_OK(Put(kPutLonger, "vl"));
  ASSERT_OK(Put("ab", "v0"));
  ASSERT_OK(Flush());
  StopSimulatingOldCommonPrefixWriter();
  const Snapshot* snap = db_->GetSnapshot();
  for (int i = 1; i <= 10; ++i) {
    ASSERT_OK(Put("ab", "v" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  auto scan = [&](const Snapshot* s, bool forward) {
    ReadOptions ro;
    ro.snapshot = s;
    std::unique_ptr<Iterator> it(db_->NewIterator(ro));
    std::string out;
    for (forward ? it->SeekToFirst() : it->SeekToLast(); it->Valid();
         forward ? it->Next() : it->Prev()) {
      out +=
          it->key().ToString(/*hex=*/true) + "=" + it->value().ToString() + " ";
    }
    EXPECT_OK(it->status());
    return out;
  };
  // Hex keys: "c", kPutLonger, "ab", "aa".
  EXPECT_EQ("63=vc 61620163=vl 6162=v10 6161=va ", scan(nullptr, true));
  EXPECT_EQ("63=vc 61620163=vl 6162=v0 6161=va ", scan(snap, true));
  EXPECT_EQ("6161=va 6162=v10 61620163=vl 63=vc ", scan(nullptr, false));
  EXPECT_EQ("6161=va 6162=v0 61620163=vl 63=vc ", scan(snap, false));
  EXPECT_GT(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  EXPECT_GT(counts.data.load(), 0);
  db_->ReleaseSnapshot(snap);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBEtc3Test, CommonKeyPrefixIndexBlockWithShorterKey) {
  // One key per data block. Two versions of "ab" in adjacent data blocks make
  // the index keys internal keys, so the old writer can store a common prefix
  // in the index block (with more than one key per restart interval) that
  // extends past a user key.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;
  table_options.index_block_restart_interval = 16;
  DestroyAndReopen(PreviouslyBuggyCommonPrefixOptions(options, table_options));
  ShortLastUserKeyBlockCounts counts;
  CountShortLastUserKeyBlocks(&counts);
  SimulateOldCommonPrefixWriter();
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(kPutLonger, "x"));
  ASSERT_OK(Put("ab", "v1"));
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put("ab", "v2"));
  ASSERT_OK(Flush());
  StopSimulatingOldCommonPrefixWriter();

  auto verify = [&]() {
    EXPECT_EQ("v2", Get("ab"));
    EXPECT_EQ("v1", Get("ab", snap));
    EXPECT_EQ("x", Get(kPutLonger));
    EXPECT_EQ(std::vector<std::string>({"v2", "x"}),
              MultiGet({"ab", kPutLonger}));
    for (const Snapshot* s : {static_cast<const Snapshot*>(nullptr), snap}) {
      ReadOptions ro;
      ro.snapshot = s;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      for (const Slice& target : {Slice("ab"), Slice("ab\x01", 3)}) {
        iter->Seek(target);
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ("ab", iter->key().ToString());
        EXPECT_EQ(s ? "v1" : "v2", iter->value().ToString());
      }
      iter->SeekForPrev(Slice("ab\x01", 3));
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(kPutLonger, iter->key().ToString());
      ASSERT_OK(iter->status());
    }
  };
  verify();
  EXPECT_GT(counts.index.load(), 0);
  EXPECT_EQ(0, counts.data.load());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  verify();
  db_->ReleaseSnapshot(snap);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBEtc3Test, CommonKeyPrefixRandomizedReverse) {
  // Random keys, often prefixes of each other, from both the old and the
  // current writer, checked against a model.
  struct RevLess {
    bool operator()(const std::string& a, const std::string& b) const {
      return Slice(a).compare(Slice(b)) > 0;
    }
  };
  const uint32_t base_seed = static_cast<uint32_t>(env_->NowMicros());
  ShortLastUserKeyBlockCounts counts;
  for (int round = 0; round < 60; ++round) {
    const uint32_t seed = base_seed + round;
    SCOPED_TRACE("seed=" + std::to_string(seed));
    Random rnd(seed);
    const bool old_writer = rnd.OneIn(2);
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    options.max_subcompactions = 1 + rnd.Uniform(3);
    const uint8_t kProtectionBytes[] = {0, 1, 2, 4, 8};
    options.block_protection_bytes_per_key = kProtectionBytes[rnd.Uniform(5)];
    BlockBasedTableOptions table_options;
    // Small blocks make it more likely that the old writer's prefix extends
    // past a user key: all keys of the block must extend that user key.
    table_options.block_size = 64 + rnd.Uniform(rnd.OneIn(2) ? 64 : 512);
    table_options.block_size_deviation = 0;
    const int kRestartIntervals[] = {1, 2, 4, 16};
    table_options.block_restart_interval = kRestartIntervals[rnd.Uniform(4)];
    table_options.index_block_restart_interval =
        kRestartIntervals[rnd.Uniform(4)];
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    // Small index partitions make it more likely that the old writer's prefix
    // extends past a user key in index blocks.
    switch (rnd.Uniform(4)) {
      case 0:
        break;
      case 1:
        table_options.index_type =
            BlockBasedTableOptions::kBinarySearchWithFirstKey;
        break;
      default:
        table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_options.metadata_block_size = 64 + rnd.Uniform(64);
        table_options.partition_filters = true;
        table_options.decouple_partitioned_filters = rnd.OneIn(2);
        break;
    }
    table_options.cache_index_and_filter_blocks = rnd.OneIn(2);
    // Warming the cache while building a table does not check blocks (only
    // blocks from the old writer need it), so only with the current writer.
    if (!old_writer && rnd.OneIn(2)) {
      table_options.prepopulate_block_cache =
          rnd.OneIn(2)
              ? BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly
              : BlockBasedTableOptions::PrepopulateBlockCache::
                    kFlushAndCompaction;
    }
    table_options.separate_key_value_in_data_block = rnd.OneIn(3);
    if (rnd.OneIn(3)) {
      table_options.data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinaryAndHash;
    }
    const Options db_options =
        PreviouslyBuggyCommonPrefixOptions(options, table_options);
    DestroyAndReopen(db_options);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    CountShortLastUserKeyBlocks(&counts);
    if (old_writer) {
      SimulateOldCommonPrefixWriter();
    }
    SyncPoint::GetInstance()->EnableProcessing();
    const int flagged_before = counts.data.load() + counts.index.load();

    auto rand_key = [&](bool allow_no_prefix) {
      static const char kAlphabet[] = {'\x00', '\x01', '\x02', 'a', 'b'};
      std::string k;
      if (!allow_no_prefix || !rnd.OneIn(4)) {
        k = "pp";
      }
      const int len = static_cast<int>(rnd.Uniform(6));
      for (int i = 0; i < len; ++i) {
        k.push_back(kAlphabet[rnd.Uniform(5)]);
      }
      return k;
    };
    std::map<std::string, std::string, RevLess> model;
    std::vector<std::string> keys;
    // Snapshots keep older versions, which can make index keys internal keys.
    std::vector<const Snapshot*> snapshots;
    Defer release_snapshots([&]() {
      for (const Snapshot* s : snapshots) {
        db_->ReleaseSnapshot(s);
      }
    });
    const int num_files = 1 + static_cast<int>(rnd.Uniform(3));
    for (int f = 0; f < num_files; ++f) {
      const int n = 20 + static_cast<int>(rnd.Uniform(300));
      for (int i = 0; i < n; ++i) {
        std::string k = rand_key(/*allow_no_prefix=*/false);
        keys.push_back(k);
        if (rnd.OneIn(5)) {
          ASSERT_OK(Delete(k));
          model.erase(k);
        } else {
          std::string v = "v" + std::to_string(rnd.Next());
          ASSERT_OK(Put(k, v));
          model[k] = v;
        }
        if (rnd.OneIn(20)) {
          snapshots.push_back(db_->GetSnapshot());
        }
      }
      ASSERT_OK(Flush());
    }
    // Sometimes also ingest a file whose keys overlap the DB, so it is
    // assigned a global sequence number (applied to block keys when seeking).
    if (rnd.OneIn(2)) {
      std::map<std::string, std::string, RevLess> file_kvs;
      const int n = 10 + static_cast<int>(rnd.Uniform(100));
      for (int i = 0; i < n; ++i) {
        file_kvs[rand_key(/*allow_no_prefix=*/false)] =
            "i" + std::to_string(rnd.Next());
      }
      const std::string file = dbname_ + "/ingest.sst";
      SstFileWriter writer(EnvOptions(), db_options);
      ASSERT_OK(writer.Open(file));
      for (const auto& kv : file_kvs) {
        ASSERT_OK(writer.Put(kv.first, kv.second));
        keys.push_back(kv.first);
        model[kv.first] = kv.second;
      }
      ASSERT_OK(writer.Finish());
      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = true;
      ASSERT_OK(db_->IngestExternalFile({file}, ifo));
    }

    auto verify = [&]() {
      for (const auto& k : keys) {
        auto it = model.find(k);
        const std::string expected =
            it == model.end() ? "NOT_FOUND" : it->second;
        ASSERT_EQ(expected, Get(k)) << "key=" << Slice(k).ToString(true);
      }
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      auto mit = model.begin();
      for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++mit) {
        ASSERT_TRUE(mit != model.end());
        ASSERT_EQ(Slice(mit->first).ToString(true), iter->key().ToString(true));
      }
      ASSERT_OK(iter->status());
      ASSERT_TRUE(mit == model.end());
      auto rit = model.rbegin();
      for (iter->SeekToLast(); iter->Valid(); iter->Prev(), ++rit) {
        ASSERT_TRUE(rit != model.rend());
        ASSERT_EQ(Slice(rit->first).ToString(true), iter->key().ToString(true));
      }
      ASSERT_OK(iter->status());
      ASSERT_TRUE(rit == model.rend());
      for (int p = 0; p < 400; ++p) {
        const std::string target = rand_key(/*allow_no_prefix=*/true);
        SCOPED_TRACE("target=" + Slice(target).ToString(true));
        iter->Seek(target);
        auto lb = model.lower_bound(target);
        if (lb == model.end()) {
          ASSERT_FALSE(iter->Valid());
        } else {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(Slice(lb->first).ToString(true),
                    iter->key().ToString(true));
        }
        ASSERT_OK(iter->status());
        iter->SeekForPrev(target);
        auto ub = model.upper_bound(target);
        if (ub == model.begin()) {
          ASSERT_FALSE(iter->Valid());
        } else {
          --ub;
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(Slice(ub->first).ToString(true),
                    iter->key().ToString(true));
        }
        ASSERT_OK(iter->status());
      }
    };
    verify();
    if (HasFatalFailure()) {
      return;
    }
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    verify();
    if (HasFatalFailure()) {
      return;
    }
    if (!old_writer) {
      // Blocks from the current writer never need the check to flag them.
      ASSERT_EQ(flagged_before, counts.data.load() + counts.index.load());
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Some blocks from the old writer needed the full-key search.
  EXPECT_GT(counts.data.load() + counts.index.load(), 0);
}

TEST_F(DBEtc3Test, ManifestRollOver) {
  do {
    Options options;
    // Force new manifest on each manifest write
    options.max_manifest_file_size = 0;
    options.max_manifest_space_amp_pct = 0;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    {
      ASSERT_OK(Put(1, "key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      // Re-open should always re-create manifest file
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      ASSERT_EQ(std::string(1000, '1'), Get(1, "key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "key3"));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBEtc3Test, AutoTuneManifestSize) {
  // Ensure we have auto-tuning beyond max_manifest_file_size by default
  ASSERT_EQ(DBOptions{}.max_manifest_space_amp_pct, 500);

  Options options = CurrentOptions();
  // Test setup: create many flushed files. Keep level compaction semantics so
  // DeleteFilesInRanges can remove non-L0 files, but prevent automatic
  // compactions and write stalls from adding unrelated behavior.
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = 100;
  options.level0_stop_writes_trigger = 200;

  // Test strategy: use large column family names to control the rough amount
  // of payload added to the MANIFEST. Drop manifest entries do not include the
  // CF name, so they are small.
  //
  // Most CF helper calls piggy-back a background manifest write so the main
  // auto-tuning phases continue to test the unrelaxed background threshold
  // even though CF manipulation itself is foreground. Phase-specific foreground
  // checks disable that piggy-backed background write.
  uint64_t prev_manifest_num = 0;
  uint64_t cur_manifest_num = 0;
  std::deque<ColumnFamilyHandle*> handles;
  int counter = 5;
  auto UpdateManifestNumsFrom = [&](uint64_t before_manifest_num) {
    prev_manifest_num = before_manifest_num;
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  };
  auto BackgroundManifestWriteFn = [&]() {
    uint64_t before_manifest_num = cur_manifest_num;
    ASSERT_OK(Put("x", std::to_string(counter++)));
    ASSERT_OK(Flush());
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto AddCfFn = [&](bool include_background_manifest_write = true) {
    uint64_t before_manifest_num = cur_manifest_num;
    std::string name = "cf" + std::to_string(counter++);
    name.resize(1000, 'a');
    ASSERT_OK(db_->CreateColumnFamily(options, name, &handles.emplace_back()));
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
    if (include_background_manifest_write) {
      BackgroundManifestWriteFn();
    }
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto DropCfFn = [&](bool include_background_manifest_write = true) {
    uint64_t before_manifest_num = cur_manifest_num;
    ASSERT_OK(db_->DropColumnFamily(handles.front()));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
    handles.pop_front();
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
    if (include_background_manifest_write) {
      BackgroundManifestWriteFn();
    }
    UpdateManifestNumsFrom(before_manifest_num);
  };

  // ---- Phase 1: foreground threshold relaxation is bounded ----
  //
  // Foreground operations should only get about 25% extra headroom, not an
  // unbounded threshold. With 1000-byte CF names and a 3000-byte normal limit,
  // the relaxed limit should allow the first four foreground-only CF additions
  // but require rotation on the fifth.
  options.max_manifest_file_size = 3000;
  options.max_manifest_space_amp_pct = 0;
  DestroyAndReopen(options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  prev_manifest_num = cur_manifest_num;
  for (int i = 1; i <= 4; ++i) {
    AddCfFn(/*include_background_manifest_write=*/false);
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  AddCfFn(/*include_background_manifest_write=*/false);
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  while (!handles.empty()) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
    handles.pop_front();
  }

  // ---- Phase 2: no auto-tuning means frequent rotation ----
  //
  options.max_manifest_file_size = 1000000;
  options.max_manifest_space_amp_pct = 0;  // no auto-tuning yet
  DestroyAndReopen(options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  prev_manifest_num = cur_manifest_num;

  // With the generous minimum manifest size, should not be rotated.
  AddCfFn();
  AddCfFn();
  AddCfFn();
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Lower the minimum while still disabling auto-tuning.
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_file_size", "3000"}}));

  // Takes effect on the next manifest write
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // Now we have to rewrite the whole manifest on each write because the
  // compacted size exceeds the "max" size.
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  DropCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // ---- Phase 3: auto-tuning raises the background threshold ----
  //
  // Enabling auto-tuning should fix this, immediately for next manifest writes.
  // This will allow up to roughly double the size of the compacted manifest,
  // which now includes CF entries plus the piggy-backed background writes.
  ASSERT_EQ(handles.size(), 4U);
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "95"}}));

  // Auto-tuning lets several more CF names accumulate in the MANIFEST before
  // the piggy-backed background write crosses the threshold.
  for (int i = 1; i <= 3; ++i) {
    if ((i % 2) == 1) {
      DropCfFn();
    }
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // We now have a different last compacted manifest size, so the next
  // threshold should be based on the newly compacted MANIFEST.
  ASSERT_EQ(handles.size(), 6U);

  DropCfFn();
  DropCfFn();
  for (int i = 1; i <= 4; ++i) {
    DropCfFn();
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  // We should be able to dynamically change the auto-tuning still based on
  // the last "compacted" manifest size.
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "51"}}));
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  // And the "compacted" manifest size has reset again, so should be changed
  // again sooner.
  ASSERT_EQ(handles.size(), 4U);
  for (int i = 1; i <= 2; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }

  // ---- Phase 4: foreground operations use relaxed threshold ----
  //
  // The current MANIFEST is now large enough for the next background manifest
  // write to rotate it, but still small enough for foreground operations to use
  // their 25% extra headroom. Assert that each foreground operation stays on
  // the same MANIFEST, then verify the next background write rotates.
  const std::string external_files_dir = dbname_ + "/external_files";
  ASSERT_OK(env_->CreateDirIfMissing(external_files_dir));
  auto WriteExternalSstFile = [&](const std::string& file_name,
                                  const std::string& key,
                                  const std::string& value) {
    const std::string file_path = external_files_dir + "/" + file_name;
    Status ignored = env_->DeleteFile(file_path);
    ignored.PermitUncheckedError();

    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(file_path));
    ASSERT_OK(sst_file_writer.Put(key, value));
    ASSERT_OK(sst_file_writer.Finish());
  };
  auto IngestExternalFileForegroundManifestWriteFn =
      [&](std::string* ingested_key) {
        uint64_t before_manifest_num = cur_manifest_num;
        const std::string key = "z" + std::to_string(counter++);
        const std::string value = "v" + std::to_string(counter++);
        const std::string file_name =
            "ingest_" + std::to_string(counter++) + ".sst";
        const std::string file_path = external_files_dir + "/" + file_name;
        WriteExternalSstFile(file_name, key, value);

        ASSERT_OK(
            db_->IngestExternalFile({file_path}, IngestExternalFileOptions()));
        ASSERT_EQ(value, Get(key));
        *ingested_key = key;
        Status ignored = env_->DeleteFile(file_path);
        ignored.PermitUncheckedError();
        UpdateManifestNumsFrom(before_manifest_num);
      };
  auto CreateColumnFamilyWithImportForegroundManifestWriteFn = [&]() {
    uint64_t before_manifest_num = cur_manifest_num;
    const std::string cf_name = "import_cf" + std::to_string(counter++);
    const std::string key = "import" + std::to_string(counter++);
    const std::string value = "v" + std::to_string(counter++);
    const std::string file_name =
        "import_" + std::to_string(counter++) + ".sst";
    const std::string file_path = external_files_dir + "/" + file_name;
    WriteExternalSstFile(file_name, key, value);

    LiveFileMetaData file_metadata;
    file_metadata.name = file_name;
    file_metadata.db_path = external_files_dir;
    file_metadata.smallest_seqno = 0;
    file_metadata.largest_seqno = 0;
    file_metadata.level = 0;
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(file_metadata);
    metadata.db_comparator_name = options.comparator->Name();

    ColumnFamilyHandle* import_handle = nullptr;
    ASSERT_OK(db_->CreateColumnFamilyWithImport(options, cf_name,
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_handle));
    ASSERT_NE(import_handle, nullptr);
    handles.push_back(import_handle);

    std::string result;
    ASSERT_OK(db_->Get(ReadOptions(), import_handle, key, &result));
    ASSERT_EQ(value, result);
    Status ignored = env_->DeleteFile(file_path);
    ignored.PermitUncheckedError();
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto DeleteFilesInRangesForegroundManifestWriteFn =
      [&](const std::string& key) {
        uint64_t before_manifest_num = cur_manifest_num;
        const std::string limit = key + "\xff";
        std::vector<RangeOpt> ranges;
        ranges.emplace_back(key, limit);
        ASSERT_OK(DeleteFilesInRanges(db_.get(), db_->DefaultColumnFamily(),
                                      ranges.data(), ranges.size(),
                                      /*include_end=*/false));

        std::string result;
        ASSERT_TRUE(db_->Get(ReadOptions(), key, &result).IsNotFound());
        UpdateManifestNumsFrom(before_manifest_num);
      };

  // Column family manipulation.
  AddCfFn(/*include_background_manifest_write=*/false);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // SetOptions should not write to the MANIFEST. If that regresses, the write
  // should be treated as a background write and rotate here.
  {
    ASSERT_OK(db_->SetOptions({{"level0_slowdown_writes_trigger", "101"}}));
    UpdateManifestNumsFrom(cur_manifest_num);
  }
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // External file ingestion.
  std::string ingested_key;
  IngestExternalFileForegroundManifestWriteFn(&ingested_key);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Imported column family creation.
  CreateColumnFamilyWithImportForegroundManifestWriteFn();
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Physical file deletion by range.
  DeleteFilesInRangesForegroundManifestWriteFn(ingested_key);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Background flush.
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // ---- Phase 5: persisted compacted manifest size survives close/reopen ----
  // Close with CFs still live. Reopen with reuse_manifest_on_open so the
  // manifest is NOT rewritten from scratch. The persisted compacted size
  // should be loaded and used for auto-tuning.
  // At this point we have 8 CF handles plus default.
  ASSERT_EQ(handles.size(), 8U);

  // Collect CF names for reopen, then release handles (Close needs this)
  std::vector<std::string> cf_names = {"default"};
  for (auto* h : handles) {
    cf_names.push_back(h->GetName());
  }
  for (auto* h : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles.clear();

  Close();
  // Use a max_manifest_file_size below the reused manifest size. Auto-tuning
  // with the persisted compacted size at 200% amp keeps the tuned threshold
  // high enough. Without persistence, the threshold would be
  // max(max_manifest_file_size, 0 * anything) = max_manifest_file_size.
  //
  // With max_manifest_file_size set to 3000, missing persisted compacted size
  // would keep tuned = 3000 and the first AddCf would rotate because the
  // reused manifest is already larger. With persisted compacted size loaded,
  // the tuned threshold is based on the compacted size, so no rotation until
  // the manifest grows well beyond the minimum.
  options.max_manifest_file_size = 3000;
  options.max_manifest_space_amp_pct = 200;
  options.reuse_manifest_on_open = true;
  uint64_t manifest_num_before_reopen = cur_manifest_num;
  ReopenWithColumnFamilies(cf_names, options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();

  // With persistence: the manifest file number should NOT have changed
  // during reopen, because the persisted compacted size keeps the tuned
  // threshold high enough. Without persistence, last_compacted = 0, so
  // tuned = max_manifest_file_size = 3000, and the first LogAndApply
  // during Open rotates the manifest because it is already larger than 3000.
  ASSERT_EQ(manifest_num_before_reopen, cur_manifest_num);

  // Adding CFs should still not trigger rotation because the tuned threshold
  // from the persisted compacted size exceeds the current manifest size.
  for (int i = 1; i <= 4; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  for (int i = 1; i <= 4; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }

  // Wrap up
  while (!handles.empty()) {
    DropCfFn(/*include_background_manifest_write=*/false);
  }
}

// Tests for the DB ownership metadata recorded for SST files
// (FileMetaData::db_ownership_start_time and data_file_provenance)
class DBOwnershipTest : public DBEtc3Test {
 protected:
  uint64_t Now() {
    int64_t now = 0;
    EXPECT_OK(env_->GetCurrentTime(&now));
    return static_cast<uint64_t>(now);
  }

  // The MANIFEST record and level of the file in the default column family
  // whose smallest user key is `key`
  std::pair<FileMetaData, int> FileStartingWith(const std::string& key) {
    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    for (size_t level = 0; level < metadata.size(); ++level) {
      for (const auto& f : metadata[level]) {
        if (f.smallest.user_key() == key) {
          return {f, static_cast<int>(level)};
        }
      }
    }
    ADD_FAILURE() << "No file starting with " << key;
    return {FileMetaData(), -1};
  }

  static DataFileProvenance Decode(const FileMetaData& f) {
    return DataFileProvenance::DecodeFrom(f.data_file_provenance);
  }

  static DataFileProvenance OriginOnly(DataFileProvenance::Origin origin) {
    DataFileProvenance info;
    info.origin = origin;
    return info;
  }

  // Expects `after` to be the same file as `before`, with the same contents
  // and DB ownership, plus DataFileProvenance flag bits `added_flags`
  static void ExpectSameFile(const FileMetaData& before,
                             const FileMetaData& after, uint32_t added_flags) {
    EXPECT_EQ(after.fd.GetNumber(), before.fd.GetNumber());
    EXPECT_EQ(after.file_creation_time, before.file_creation_time);
    EXPECT_EQ(after.db_ownership_start_time, before.db_ownership_start_time);
    EXPECT_EQ(after.data_file_provenance,
              before.data_file_provenance | added_flags);
  }
};

TEST_F(DBOwnershipTest, FlushAndCompactionOutputs) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  env_->SetMockSleep();
  DestroyAndReopen(options);

  const uint64_t start = Now();
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const FileMetaData flushed = FileStartingWith("a").first;
  EXPECT_GE(flushed.db_ownership_start_time, start);
  EXPECT_EQ(flushed.db_ownership_start_time, flushed.file_creation_time);
  EXPECT_EQ(Decode(flushed), OriginOnly(DataFileProvenance::Origin::kFlush));

  // Reported in public metadata
  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_EQ(live_files.size(), 1U);
  EXPECT_EQ(live_files[0].db_ownership_start_time,
            flushed.db_ownership_start_time);
  EXPECT_EQ(live_files[0].data_file_provenance, flushed.data_file_provenance);
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 1U);
  EXPECT_EQ(cf_meta.levels[0].files[0].db_ownership_start_time,
            flushed.db_ownership_start_time);
  EXPECT_EQ(cf_meta.levels[0].files[0].data_file_provenance,
            flushed.data_file_provenance);

  // Flushed during WAL recovery
  ASSERT_OK(Put("b", "1"));
  Reopen(options);
  const FileMetaData recovered = FileStartingWith("b").first;
  EXPECT_GE(recovered.db_ownership_start_time, start);
  EXPECT_EQ(recovered.db_ownership_start_time, recovered.file_creation_time);
  EXPECT_EQ(Decode(recovered), OriginOnly(DataFileProvenance::Origin::kFlush));

  // Compactions that take a long time to finish after creating their output
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
      [&](void*) { env_->MockSleepForSeconds(1000); });
  SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // The DB has held the output since creating it
  const FileMetaData compacted = FileStartingWith("a").first;
  EXPECT_EQ(compacted.db_ownership_start_time, compacted.file_creation_time);
  EXPECT_LE(compacted.db_ownership_start_time + 1000, Now());
  EXPECT_EQ(Decode(compacted),
            OriginOnly(DataFileProvenance::Origin::kCompaction));
}

TEST_F(DBOwnershipTest, MovesPreserveOwnership) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = true;
  env_->SetMockSleep();
  CompactRangeOptions force_compact;
  force_compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  {
    SCOPED_TRACE("PromoteL0");
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    const FileMetaData before = FileStartingWith("a").first;
    env_->MockSleepForSeconds(1000);
    ASSERT_OK(
        experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily(), 2));
    const auto& [after, level] = FileStartingWith("a");
    EXPECT_EQ(level, 2);
    ExpectSameFile(before, after, kProvenanceMovedBetweenLevels);
  }
  {
    SCOPED_TRACE("Trivial move");
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    const FileMetaData before = FileStartingWith("a").first;
    env_->MockSleepForSeconds(1000);
    std::atomic<int> trivial_moves{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction:TrivialMove",
        [&](void*) { ++trivial_moves; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_GT(trivial_moves.load(), 0);
    const auto& [after, level] = FileStartingWith("a");
    EXPECT_GT(level, 0);
    ExpectSameFile(before, after, kProvenanceMovedBetweenLevels);
  }
  {
    SCOPED_TRACE("CompactRange with change_level, and reopen");
    DestroyAndReopen(options);
    // A compaction output that has not been moved
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("a", "2"));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(force_compact, nullptr, nullptr));
    const auto& [before, before_level] = FileStartingWith("a");
    ASSERT_EQ(Decode(before),
              OriginOnly(DataFileProvenance::Origin::kCompaction));
    ASSERT_GT(before_level, 1);
    env_->MockSleepForSeconds(1000);
    CompactRangeOptions refit;
    refit.change_level = true;
    refit.target_level = 1;
    ASSERT_OK(db_->CompactRange(refit, nullptr, nullptr));
    const auto& [after, level] = FileStartingWith("a");
    EXPECT_EQ(level, 1);
    ExpectSameFile(before, after, kProvenanceMovedBetweenLevels);

    // Preserved by replaying the MANIFEST and then by the new MANIFEST written
    // on open
    for (int i = 0; i < 2; ++i) {
      Reopen(options);
      ExpectSameFile(after, FileStartingWith("a").first, 0);
    }
  }
  {
    SCOPED_TRACE("Level migration on open");
    options.level_compaction_dynamic_level_bytes = false;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("a", "2"));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(force_compact, nullptr, nullptr));
    const auto& [before, before_level] = FileStartingWith("a");
    ASSERT_EQ(Decode(before),
              OriginOnly(DataFileProvenance::Origin::kCompaction));
    ASSERT_LT(before_level, options.num_levels - 1);
    env_->MockSleepForSeconds(1000);
    options.level_compaction_dynamic_level_bytes = true;
    // Required for the migration
    options.disable_auto_compactions = false;
    Reopen(options);
    const auto& [after, level] = FileStartingWith("a");
    EXPECT_EQ(level, options.num_levels - 1);
    ExpectSameFile(before, after, kProvenanceMovedBetweenLevels);
  }
}

TEST_F(DBOwnershipTest, FIFOTemperatureChangeCopy) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.level0_file_num_compaction_trigger = 2;
  CompactionOptionsFIFO fifo_options;
  fifo_options.file_temperature_age_thresholds = {{Temperature::kCold, 1000}};
  fifo_options.max_table_files_size = 100000000;
  fifo_options.allow_trivial_copy_when_change_temperature = true;
  options.compaction_options_fifo = fifo_options;
  env_->SetMockSleep();
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  const FileMetaData before = FileStartingWith("a").first;
  ASSERT_NE(before.temperature, Temperature::kCold);

  env_->MockSleepForSeconds(2000);
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  const FileMetaData after = FileStartingWith("a").first;
  ASSERT_EQ(after.temperature, Temperature::kCold);
  EXPECT_NE(after.fd.GetNumber(), before.fd.GetNumber());
  // Same contents...
  EXPECT_EQ(after.file_creation_time, before.file_creation_time);
  // ...in a new file for the DB to hold
  EXPECT_GE(after.db_ownership_start_time,
            before.db_ownership_start_time + 2000);
  EXPECT_EQ(after.data_file_provenance,
            before.data_file_provenance | kProvenanceTemperatureChangedByCopy);
}

TEST_F(DBOwnershipTest, TemperatureUpdatedFromFileSystem) {
  auto test_fs = std::make_shared<FileTemperatureTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, test_fs));
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env.get();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const FileMetaData before = FileStartingWith("a").first;
  ASSERT_NE(before.temperature, Temperature::kCold);

  test_fs->OverrideSstFileTemperature(before.fd.GetNumber(),
                                      Temperature::kCold);
  Close();
  experimental::UpdateManifestForFilesStateOptions update_opts;
  update_opts.update_temperatures = true;
  ASSERT_OK(experimental::UpdateManifestForFilesState(
      options, dbname_,
      {ColumnFamilyDescriptor(kDefaultColumnFamilyName, options)},
      update_opts));
  Reopen(options);
  const FileMetaData after = FileStartingWith("a").first;
  EXPECT_EQ(after.temperature, Temperature::kCold);
  ExpectSameFile(before, after, kProvenanceTemperatureUpdatedFromFileSystem);
  Close();
}

TEST_F(DBOwnershipTest, Repair) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const FileMetaData flushed = FileStartingWith("a").first;
  // Left in the WAL, for repair to write to a table
  ASSERT_OK(Put("b", "1"));
  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);

  // Ownership of tables from before the repair was recorded in the lost
  // MANIFEST
  const FileMetaData repaired = FileStartingWith("a").first;
  // Restored from the table property, recorded when the flush finished
  EXPECT_GE(repaired.file_creation_time, flushed.file_creation_time);
  EXPECT_EQ(repaired.db_ownership_start_time, kUnknownDbOwnershipStartTime);
  DataFileProvenance expected;
  expected.recorded_by_repair = true;
  EXPECT_EQ(Decode(repaired), expected);

  const FileMetaData from_wal = FileStartingWith("b").first;
  EXPECT_GT(from_wal.file_creation_time, 0U);
  EXPECT_EQ(from_wal.db_ownership_start_time, from_wal.file_creation_time);
  expected.origin = DataFileProvenance::Origin::kFlush;
  EXPECT_EQ(Decode(from_wal), expected);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
