//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "plugin/trie/trie_rep.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/random.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Internal-key order with the default bytewise user comparator: user key
// ascending, then the whole 8-byte seq/type footer descending.
struct RefKeyCmp {
  bool operator()(const std::string& a, const std::string& b) const {
    const size_t alen = a.size() - kNumInternalBytes;
    const size_t blen = b.size() - kNumInternalBytes;
    const size_t common = std::min(alen, blen);
    const int r = memcmp(a.data(), b.data(), common);
    if (r != 0) {
      return r < 0;
    }
    if (alen != blen) {
      return alen < blen;
    }
    return DecodeFixed64(a.data() + alen) > DecodeFixed64(b.data() + blen);
  }
};

// A user comparator with deliberately non-bytewise order, to exercise the
// TrieRepFactory fallback to SkipListRep.
class ReverseUserComparator : public Comparator {
 public:
  const char* Name() const override { return "test.ReverseUserComparator"; }
  int Compare(const Slice& a, const Slice& b) const override {
    const size_t common = std::min(a.size(), b.size());
    const int r = memcmp(a.data(), b.data(), common);
    if (r != 0) {
      return -r;
    }
    if (a.size() != b.size()) {
      return (a.size() < b.size()) ? 1 : -1;
    }
    return 0;
  }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

class TrieRepTest : public testing::Test {
 public:
  TrieRepTest() : icmp_(BytewiseComparator()), cmp_(icmp_) {
    rep_.reset(factory_->CreateMemTableRep(cmp_, &arena_,
                                           /*transform=*/nullptr,
                                           /*logger=*/nullptr));
  }

  // Encodes a memtable entry the way MemTable::Add lays it out.
  static std::string EncodeEntry(const Slice& user_key, SequenceNumber seq,
                                 ValueType type, const Slice& value) {
    std::string buf;
    const size_t ikey_size = user_key.size() + kNumInternalBytes;
    PutVarint32(&buf, static_cast<uint32_t>(ikey_size));
    buf.append(user_key.data(), user_key.size());
    char footer[8];
    EncodeFixed64(footer, PackSequenceAndType(seq, type));
    buf.append(footer, 8);
    PutVarint32(&buf, static_cast<uint32_t>(value.size()));
    buf.append(value.data(), value.size());
    return buf;
  }

  // Inserts content into a rep the way MemTable::Add does: allocate through
  // the rep (some reps stash metadata around the buffer), then insert.
  // Returns the arena-owned buffer.
  static const char* InsertRaw(MemTableRep* rep, const std::string& content,
                               bool concurrent = false) {
    char* buf = nullptr;
    KeyHandle h = rep->Allocate(content.size(), &buf);
    memcpy(buf, content.data(), content.size());
    const bool is_new =
        concurrent ? rep->InsertKeyConcurrently(h) : rep->InsertKey(h);
    EXPECT_TRUE(is_new);
    return buf;
  }

  // Builds a memtable entry the way MemTable::Add lays it out, allocates and
  // fills it through the rep, and records it in the model, without inserting.
  KeyHandle PrepareEntry(const Slice& user_key, SequenceNumber seq,
                         ValueType type, const Slice& value) {
    storage_.push_back(EncodeEntry(user_key, seq, type, value));
    const std::string& stored = storage_.back();
    char* buf = nullptr;
    KeyHandle h = rep_->Allocate(stored.size(), &buf);
    memcpy(buf, stored.data(), stored.size());
    live_buffers_.insert(buf);
    const size_t ikey_size = user_key.size() + kNumInternalBytes;
    model_[std::string(GetLengthPrefixedSlice(stored.data()).data(),
                       ikey_size)] = stored;
    return h;
  }

  // Prepares and inserts an entry. Returns the stored bytes (owned by the
  // fixture's oracle).
  const std::string& InsertEntry(const Slice& user_key, SequenceNumber seq,
                                 ValueType type, const Slice& value,
                                 bool concurrent = false) {
    KeyHandle h = PrepareEntry(user_key, seq, type, value);
    EXPECT_TRUE(concurrent ? rep_->InsertKeyConcurrently(h)
                           : rep_->InsertKey(h));
    return storage_.back();
  }

  // Collects the full iteration order as internal-key strings.
  std::vector<std::string> ForwardOrder() {
    std::vector<std::string> out;
    std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const Slice ikey = GetLengthPrefixedSlice(it->key());
      out.emplace_back(ikey.data(), ikey.size());
    }
    return out;
  }

  std::vector<std::string> BackwardOrder() {
    std::vector<std::string> out;
    std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
      const Slice ikey = GetLengthPrefixedSlice(it->key());
      out.emplace_back(ikey.data(), ikey.size());
    }
    return out;
  }

  std::vector<std::string> ModelOrder() {
    std::vector<std::string> out;
    out.reserve(model_.size());
    for (const auto& kv : model_) {
      out.push_back(kv.first);
    }
    return out;
  }

  void CheckFullOrder() {
    ASSERT_EQ(ForwardOrder(), ModelOrder());
    std::vector<std::string> backward = BackwardOrder();
    std::vector<std::string> forward = ModelOrder();
    ASSERT_EQ(backward.size(), forward.size());
    for (size_t i = 0; i < forward.size(); ++i) {
      ASSERT_EQ(backward[i], forward[forward.size() - 1 - i]);
    }
    // Every entry is findable by exact lookup.
    int missing = 0;
    for (const auto& kv : model_) {
      if (!rep_->Contains(kv.second.data())) {
        if (missing < 5) {
          std::string hex;
          for (unsigned char c : kv.first) {
            char h[4];
            snprintf(h, sizeof(h), "%02x", c);
            hex += h;
          }
          fprintf(stderr, "MISSING key(%zu): %s\n", kv.first.size(),
                  hex.c_str());
        }
        ++missing;
      }
    }
    ASSERT_EQ(missing, 0);
  }

  // Seek and SeekForPrev to `target` must land where the model says.
  void CheckSeekMatchesModel(MemTableRep::Iterator* it,
                             const std::string& target) {
    const Slice target_slice(target);
    auto ge = model_.lower_bound(target);
    it->Seek(target_slice, nullptr);
    if (ge == model_.end()) {
      ASSERT_FALSE(it->Valid()) << "target size " << target.size();
    } else {
      ASSERT_TRUE(it->Valid()) << "target size " << target.size();
      ASSERT_EQ(Slice(ge->first), GetLengthPrefixedSlice(it->key()));
    }
    auto le = model_.upper_bound(target);
    it->SeekForPrev(target_slice, nullptr);
    if (le == model_.begin()) {
      ASSERT_FALSE(it->Valid()) << "target size " << target.size();
    } else {
      --le;
      ASSERT_TRUE(it->Valid()) << "target size " << target.size();
      ASSERT_EQ(Slice(le->first), GetLengthPrefixedSlice(it->key()));
    }
  }

  // Get/GetAndValidate callback state: every entry offered for `user`.
  struct Collected {
    std::string user;
    std::vector<const char*> entries;
  };

  static bool CollectSameUserKey(void* arg, const char* entry) {
    Collected* collected = static_cast<Collected*>(arg);
    if (ExtractUserKey(GetLengthPrefixedSlice(entry)) != collected->user) {
      return false;
    }
    collected->entries.push_back(entry);
    return true;
  }

  InternalKeyComparator icmp_;
  MemTable::KeyComparator cmp_;
  Arena arena_;
  std::unique_ptr<MemTableRepFactory> factory_{NewTrieRepFactory()};
  std::unique_ptr<MemTableRep> rep_;
  // Oracle copies of every entry (Contains probes read these).
  std::deque<std::string> storage_;
  // Arena-owned buffers actually handed to the rep (sample membership).
  std::unordered_set<const char*> live_buffers_;
  std::map<std::string, std::string, RefKeyCmp> model_;
};

TEST_F(TrieRepTest, Empty) {
  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  ASSERT_FALSE(it->Valid());
  it->SeekToFirst();
  ASSERT_FALSE(it->Valid());
  it->SeekToLast();
  ASSERT_FALSE(it->Valid());
  // Bounds are internal keys (user key + 8-byte footer), as all callers pass.
  const InternalKey start("a", kMaxSequenceNumber, kTypeValue);
  const InternalKey end("z", kMaxSequenceNumber, kTypeValue);
  ASSERT_EQ(rep_->ApproximateNumEntries(start.Encode(), end.Encode()),
            static_cast<uint64_t>(0));
  std::unordered_set<const char*> sample;
  rep_->UniqueRandomSample(0, 10, &sample);
  ASSERT_TRUE(sample.empty());
}

TEST_F(TrieRepTest, SequentialInserts) {
  for (int i = 0; i < 5000; ++i) {
    char buf[8];
    EncodeFixed64(buf, static_cast<uint64_t>(i));
    InsertEntry(Slice(buf, 8), static_cast<SequenceNumber>(i + 1), kTypeValue,
                "v");
  }
  CheckFullOrder();
  ASSERT_EQ(rep_->ApproximateMemoryUsage(), 0u);
}

TEST_F(TrieRepTest, SharedPrefixesAndVersions) {
  Random rnd(301);
  // 8-byte shared prefixes with random suffixes, several versions each,
  // plus keys that are strict prefixes of each other.
  for (int p = 0; p < 50; ++p) {
    std::string prefix = "prefix" + std::to_string(1000 + p);
    for (int s = 0; s < 20; ++s) {
      std::string key = prefix + "_suffix_" + std::to_string(rnd.Next() % 7);
      for (int v = 0; v < 3; ++v) {
        const ValueType t = (v == 2) ? kTypeDeletion : kTypeValue;
        InsertEntry(key, static_cast<SequenceNumber>(p * 100 + s * 3 + v + 1),
                    t, "value-" + std::to_string(v));
      }
    }
  }
  InsertEntry("a", 100000, kTypeValue, "v");
  InsertEntry("ab", 100001, kTypeValue, "v");
  InsertEntry("abc", 100002, kTypeValue, "v");
  InsertEntry("abd", 100003, kTypeValue, "v");
  InsertEntry("b", 100004, kTypeValue, "v");
  CheckFullOrder();
}

TEST_F(TrieRepTest, DuplicateInsertRejected) {
  const std::string& e = InsertEntry("dup", 7, kTypeValue, "v");
  std::string copy = e;
  ASSERT_FALSE(rep_->InsertKey(const_cast<char*>(copy.data())));
  ASSERT_FALSE(rep_->InsertKeyConcurrently(const_cast<char*>(copy.data())));
  CheckFullOrder();
}

TEST_F(TrieRepTest, SeekMatchesModel) {
  Random rnd(7007);
  for (int i = 0; i < 3000; ++i) {
    char buf[16];
    EncodeFixed64(buf, rnd.Next() % 500);
    EncodeFixed64(buf + 8, rnd.Next() % 500);
    InsertEntry(Slice(buf, 16), static_cast<SequenceNumber>(i + 1), kTypeValue,
                "v");
  }
  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  // Exact hits, gaps, truncated keys, and low/high boundary targets. Every
  // target stays a valid internal key (>= 8 bytes), as all callers provide.
  for (int t = 0; t < 500; ++t) {
    std::string target;
    const int kind = t % 5;
    if (kind == 0) {
      auto m = model_.begin();
      std::advance(m, rnd.Next() % model_.size());
      target = m->first;
    } else if (kind == 1) {
      char buf[16];
      EncodeFixed64(buf, rnd.Next() % 500);
      EncodeFixed64(buf + 8, rnd.Next() % 500);
      target.assign(buf, 16);
      char footer[8];
      EncodeFixed64(footer,
                    PackSequenceAndType(rnd.Next() % 4000 + 1, kTypeValue));
      target.append(footer, 8);
    } else if (kind == 2) {
      // Smallest plausible key: empty user key.
      char footer[8];
      EncodeFixed64(footer, PackSequenceAndType(1, kTypeValue));
      target.assign(footer, 8);
    } else if (kind == 3) {
      // Largest plausible key.
      target = std::string("\xff\xff\xff\xff\xff\xff\xff\xff", 8);
      char footer[8];
      // Same value as kValueTypeForSeek (see dbformat.cc); spelled out
      // because the extern symbol is not visible to this TU in this build.
      EncodeFixed64(footer, PackSequenceAndType(kMaxSequenceNumber,
                                                kTypeValuePreferredSeqno));
      target.append(footer, 8);
    } else {
      auto m = model_.begin();
      std::advance(m, rnd.Next() % model_.size());
      // Truncate the user part but keep a valid footer.
      const size_t ulen = m->first.size() - kNumInternalBytes;
      const size_t keep = rnd.Next() % (ulen + 1);
      target = m->first.substr(0, keep);
      target.append(m->first.data() + ulen, kNumInternalBytes);
    }
    CheckSeekMatchesModel(it.get(), target);
  }
}

TEST_F(TrieRepTest, EntrySlotsSplitAtAnyDepth) {
  // A child slot points straight at an entry until a second key shares its
  // path; these keys force that split after a long shared prefix, inside
  // escaped 0x00 bytes, between strict-prefix user keys, and in the footer.
  const std::string base(200, 'k');
  InsertEntry(base + "1", 1, kTypeValue, "v");
  InsertEntry(base + "2", 2, kTypeValue, "v");
  InsertEntry(base, 3, kTypeValue, "v");
  InsertEntry(base + std::string("\0", 1), 4, kTypeValue, "v");
  InsertEntry(base + std::string("\0\0", 2), 5, kTypeValue, "v");
  InsertEntry(base + std::string("\0\x01", 2), 6, kTypeValue, "v");
  for (SequenceNumber s = 10; s < 20; ++s) {
    InsertEntry(base + "1", s, kTypeValue, "v");
  }
  // Same sequence number, different type: differs only in the last byte.
  InsertEntry(base + "1", 10, kTypeDeletion, "v");
  CheckFullOrder();

  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  for (const auto& kv : model_) {
    const std::string& ikey = kv.first;
    const size_t ulen = ikey.size() - kNumInternalBytes;
    CheckSeekMatchesModel(it.get(), ikey);
    // Same user key, other footers: between versions of one key.
    for (SequenceNumber s :
         {SequenceNumber{0}, SequenceNumber{15}, kMaxSequenceNumber}) {
      std::string target = ikey.substr(0, ulen);
      char footer[8];
      EncodeFixed64(footer, PackSequenceAndType(s, kTypeValue));
      target.append(footer, 8);
      CheckSeekMatchesModel(it.get(), target);
    }
  }
}

TEST_F(TrieRepTest, DeepTrieSpillsTraversalStack) {
  // "xy", "xxy", "xxxy", ...: each key branches off the next one at a
  // successive byte, so the trie is one level deeper per key, past the
  // traversal stack's inline capacity.
  constexpr int kDepth = 40;
  for (int d = 1; d <= kDepth; ++d) {
    InsertEntry(std::string(d, 'x') + "y", static_cast<SequenceNumber>(d),
                kTypeValue, "v");
  }
  CheckFullOrder();
  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  for (const auto& kv : model_) {
    CheckSeekMatchesModel(it.get(), kv.first);
  }
}

TEST_F(TrieRepTest, SparseNodeGrowsInLabelOrder) {
  // One node gains children one at a time in shuffled label order, passing
  // through the small, large, and dense node kinds; order and seeks must
  // hold at every size.
  std::vector<char> labels;
  for (char c = 'a'; c <= 't'; ++c) {
    labels.push_back(c);
  }
  RandomShuffle(labels.begin(), labels.end(), /*seed=*/42);
  SequenceNumber seq = 0;
  char footer[8];
  EncodeFixed64(footer, PackSequenceAndType(kMaxSequenceNumber, kTypeValue));
  for (char c : labels) {
    InsertEntry(std::string("k") + c, ++seq, kTypeValue, "v");
    CheckFullOrder();
    std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
    // Targets below, between, and above the labels present so far.
    for (char t = 'a' - 1; t <= 't' + 1; ++t) {
      CheckSeekMatchesModel(it.get(),
                            std::string("k") + t + std::string(footer, 8));
    }
  }
}

TEST_F(TrieRepTest, GetSeesVersionsInOrder) {
  InsertEntry("userkey", 10, kTypeValue, "v10");
  InsertEntry("userkey", 20, kTypeValue, "v20");
  InsertEntry("userkey", 30, kTypeDeletion, "v30");
  InsertEntry("other", 25, kTypeValue, "other");
  // Mirrors MemTable::SaveValue: stop at the first different user key and
  // only surface versions visible to the snapshot.
  struct Seen {
    std::string user;
    SequenceNumber snapshot;
    std::vector<SequenceNumber> seqs;
  };
  auto callback = [](void* arg, const char* entry) -> bool {
    Seen* seen = static_cast<Seen*>(arg);
    const Slice ikey = GetLengthPrefixedSlice(entry);
    const Slice user(ikey.data(), ikey.size() - kNumInternalBytes);
    if (user != seen->user) {
      return false;
    }
    SequenceNumber seq;
    ValueType type;
    UnPackSequenceAndType(DecodeFixed64(ikey.data() + ikey.size() - 8), &seq,
                          &type);
    if (seq <= seen->snapshot) {
      seen->seqs.push_back(seq);
    }
    return true;
  };
  // Snapshot at 25 sees seq 20 and 10, newest first.
  Seen seen{"userkey", 25, {}};
  LookupKey lkey("userkey", 25);
  rep_->Get(lkey, &seen, callback);
  ASSERT_EQ(seen.seqs, std::vector<SequenceNumber>({20, 10}));
  // Snapshot at max sees everything, newest first.
  Seen seen_all{"userkey", kMaxSequenceNumber, {}};
  LookupKey lkey_all("userkey", kMaxSequenceNumber);
  rep_->Get(lkey_all, &seen_all, callback);
  ASSERT_EQ(seen_all.seqs, std::vector<SequenceNumber>({30, 20, 10}));
  // Missing key yields nothing.
  Seen seen_missing{"absent", kMaxSequenceNumber, {}};
  LookupKey lkey_missing("absent", kMaxSequenceNumber);
  rep_->Get(lkey_missing, &seen_missing, callback);
  ASSERT_TRUE(seen_missing.seqs.empty());
}

TEST_F(TrieRepTest, InsertWhileIteratorOpen) {
  for (int i = 0; i < 100; ++i) {
    char key[8];
    snprintf(key, sizeof(key), "k%03d", i);
    InsertEntry(key, static_cast<SequenceNumber>(i + 1), kTypeValue, "v");
  }
  const std::vector<std::string> before = ModelOrder();

  // Inserting on the iterating thread must not block, and the iterator must
  // keep moving forward over at least the keys present when it was opened.
  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  std::vector<std::string> seen;
  SequenceNumber seq = 1000;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const Slice ikey = GetLengthPrefixedSlice(it->key());
    if (!seen.empty()) {
      ASSERT_TRUE(RefKeyCmp()(seen.back(), ikey.ToString()));
    }
    seen.push_back(ikey.ToString());
    const std::string user = ExtractUserKey(ikey).ToString();
    if (user.size() == 4) {
      // Only original keys trigger inserts, so the loop terminates.
      seq += 3;
      InsertEntry(user + "a", seq, kTypeValue, "v");
      InsertEntry("a" + std::to_string(seq), seq, kTypeValue, "v");
      InsertEntry("z" + std::to_string(seq), seq, kTypeValue, "v");
    }
  }
  ASSERT_TRUE(std::includes(seen.begin(), seen.end(), before.begin(),
                            before.end(), RefKeyCmp()));
  CheckFullOrder();
}

TEST_F(TrieRepTest, ConcurrentReadersAndWriters) {
  // RocksDB inserts from one thread at a time (TrieRepFactory requires
  // allow_concurrent_memtable_write=false); several writers here also check
  // that the writer mutex keeps unexpected concurrent inserts correct.
  constexpr int kWriters = 4;
  constexpr int kReaders = 4;
  constexpr int kPerWriter = 5000;
  // The fixture's Arena is not thread-safe, so every entry is allocated up
  // front; only InsertKeyConcurrently runs on the writer threads.
  Random rnd(1234);
  std::vector<std::vector<KeyHandle>> handles(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    for (int i = 0; i < kPerWriter; ++i) {
      char key[16];
      EncodeFixed64(key, rnd.Next());
      EncodeFixed64(key + 8, rnd.Next() % 64);
      handles[w].push_back(
          PrepareEntry(Slice(key, sizeof(key)),
                       static_cast<SequenceNumber>(w * kPerWriter + i + 1),
                       kTypeValue, "v"));
    }
  }

  std::atomic<bool> writers_done{false};
  std::atomic<int> failures{0};
  std::vector<port::Thread> readers;
  readers.reserve(kReaders);
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&, r] {
      std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
      // At least one full pass each way, then keep going until writers stop.
      bool forward = (r % 2 == 0);
      int passes = 0;
      while (passes < 2 || !writers_done.load()) {
        const char* prev = nullptr;
        if (forward) {
          it->SeekToFirst();
        } else {
          it->SeekToLast();
        }
        for (; it->Valid(); forward ? it->Next() : it->Prev()) {
          if (prev != nullptr && (forward ? cmp_(prev, it->key()) >= 0
                                          : cmp_(prev, it->key()) <= 0)) {
            failures.fetch_add(1);
          }
          prev = it->key();
        }
        forward = !forward;
        ++passes;
      }
    });
  }
  std::vector<port::Thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (KeyHandle h : handles[w]) {
        if (!rep_->InsertKeyConcurrently(h)) {
          failures.fetch_add(1);
        }
      }
    });
  }
  for (auto& t : writers) {
    t.join();
  }
  writers_done.store(true);
  for (auto& t : readers) {
    t.join();
  }
  ASSERT_EQ(failures.load(), 0);
  CheckFullOrder();
}

TEST_F(TrieRepTest, ValidatingCallsMatchPlainCalls) {
  Random rnd(4242);
  for (int i = 0; i < 2000; ++i) {
    InsertEntry("user" + std::to_string(rnd.Next() % 300),
                static_cast<SequenceNumber>(i + 1), kTypeValue, "v");
  }
  int validated = 0;
  const std::function<Status(const char*, bool)> count_ok =
      [&validated](const char*, bool) {
        ++validated;
        return Status::OK();
      };
  for (int u = 0; u < 300; ++u) {
    const std::string user = "user" + std::to_string(u);
    LookupKey lkey(user, kMaxSequenceNumber);
    Collected plain{user, {}};
    Collected checked{user, {}};
    rep_->Get(lkey, &plain, CollectSameUserKey);
    ASSERT_OK(rep_->GetAndValidate(lkey, &checked, CollectSameUserKey,
                                   /*allow_data_in_errors=*/true,
                                   /*detect_key_out_of_order=*/true, count_ok));
    ASSERT_EQ(plain.entries, checked.entries);
  }
  ASSERT_GT(validated, 0);

  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  std::vector<std::string> forward;
  Status s;
  for (it->SeekToFirst(); it->Valid() && s.ok();
       s = it->NextAndValidate(/*allow_data_in_errors=*/true)) {
    forward.push_back(GetLengthPrefixedSlice(it->key()).ToString());
  }
  ASSERT_OK(s);
  ASSERT_EQ(forward, ModelOrder());
  std::vector<std::string> backward;
  for (it->SeekToLast(); it->Valid() && s.ok();
       s = it->PrevAndValidate(/*allow_data_in_errors=*/true)) {
    backward.push_back(GetLengthPrefixedSlice(it->key()).ToString());
  }
  ASSERT_OK(s);
  ASSERT_EQ(backward, BackwardOrder());
}

TEST_F(TrieRepTest, ValidationReportsCorruption) {
  std::vector<char*> buffers;
  for (int i = 0; i < 100; ++i) {
    char key[8];
    snprintf(key, sizeof(key), "k%03d", i);
    KeyHandle h =
        PrepareEntry(key, static_cast<SequenceNumber>(i + 1), kTypeValue, "v");
    ASSERT_TRUE(rep_->InsertKey(h));
    buffers.push_back(static_cast<char*>(h));
  }
  LookupKey lkey("k050", kMaxSequenceNumber);

  // A failing checksum callback surfaces as the callback's status.
  const std::function<Status(const char*, bool)> reject_k050 =
      [](const char* entry, bool) {
        return ExtractUserKey(GetLengthPrefixedSlice(entry)) == Slice("k050")
                   ? Status::Corruption("bad checksum")
                   : Status::OK();
      };
  Collected rejected{"k050", {}};
  ASSERT_TRUE(rep_->GetAndValidate(lkey, &rejected, CollectSameUserKey,
                                   /*allow_data_in_errors=*/false,
                                   /*detect_key_out_of_order=*/false,
                                   reject_k050)
                  .IsCorruption());

  // Overwrite k050's key bytes (after the 1-byte length) with "k999": the
  // entry no longer matches its trie path, and it is out of order with its
  // neighbours, so seeks and scans in both directions report corruption.
  memcpy(buffers[50] + 1, "k999", 4);
  Collected corrupted{"k050", {}};
  ASSERT_TRUE(rep_->GetAndValidate(lkey, &corrupted, CollectSameUserKey,
                                   /*allow_data_in_errors=*/true,
                                   /*detect_key_out_of_order=*/true,
                                   /*key_validation_callback=*/nullptr)
                  .IsCorruption());
  std::unique_ptr<MemTableRep::Iterator> it(rep_->GetIterator());
  Status s;
  for (it->SeekToFirst(); it->Valid() && s.ok();
       s = it->NextAndValidate(/*allow_data_in_errors=*/true)) {
  }
  ASSERT_TRUE(s.IsCorruption());
  s = Status::OK();
  for (it->SeekToLast(); it->Valid() && s.ok();
       s = it->PrevAndValidate(/*allow_data_in_errors=*/true)) {
  }
  ASSERT_TRUE(s.IsCorruption());
}

// MemTable::Get uses GetAndValidate when either option is set and treats any
// non-Corruption status as "not in this memtable", so an unsupported
// GetAndValidate would silently skip the memtable and miss recent writes.
TEST(TrieRepDBTest, ReadsWithMemtableValidationSeeMemtable) {
  for (bool paranoid : {false, true}) {
    for (bool checksum_on_seek : {false, true}) {
      SCOPED_TRACE("paranoid=" + std::to_string(paranoid) +
                   " checksum_on_seek=" + std::to_string(checksum_on_seek));
      Options options;
      options.create_if_missing = true;
      options.memtable_factory.reset(NewTrieRepFactory());
      options.allow_concurrent_memtable_write = false;
      options.paranoid_memory_checks = paranoid;
      options.memtable_verify_per_key_checksum_on_seek = checksum_on_seek;
      options.memtable_protection_bytes_per_key = checksum_on_seek ? 8 : 0;
      const std::string dbname = test::PerThreadDBPath("trierep_validation");
      ASSERT_OK(DestroyDB(dbname, options));
      std::unique_ptr<DB> db;
      ASSERT_OK(DB::Open(options, dbname, &db));
      ASSERT_OK(db->Put(WriteOptions(), "key", "v1"));
      ASSERT_OK(db->Put(WriteOptions(), "key", "v2"));
      std::string value;
      ASSERT_OK(db->Get(ReadOptions(), "key", &value));
      ASSERT_EQ(value, "v2");
      std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
      it->Seek("key");
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(it->value(), "v2");
      it->Next();
      ASSERT_FALSE(it->Valid());
      ASSERT_OK(it->status());
      it.reset();
      db.reset();
      ASSERT_OK(DestroyDB(dbname, options));
    }
  }
}

std::string RandomTrickyKey(Random* rnd);

void CheckDBMatchesModel(DB* db,
                         const std::map<std::string, std::string>& model,
                         const Snapshot* snapshot = nullptr) {
  ReadOptions ro;
  ro.snapshot = snapshot;
  for (const auto& kv : model) {
    std::string value;
    ASSERT_OK(db->Get(ro, kv.first, &value));
    ASSERT_EQ(value, kv.second);
  }
  std::unique_ptr<Iterator> it(db->NewIterator(ro));
  auto expected = model.begin();
  for (it->SeekToFirst(); it->Valid(); it->Next(), ++expected) {
    ASSERT_TRUE(expected != model.end());
    ASSERT_EQ(it->key(), expected->first);
    ASSERT_EQ(it->value(), expected->second);
  }
  ASSERT_OK(it->status());
  ASSERT_TRUE(expected == model.end());
  auto rexpected = model.rbegin();
  for (it->SeekToLast(); it->Valid(); it->Prev(), ++rexpected) {
    ASSERT_TRUE(rexpected != model.rend());
    ASSERT_EQ(it->key(), rexpected->first);
  }
  ASSERT_OK(it->status());
  ASSERT_TRUE(rexpected == model.rend());
}

TEST(TrieRepDBTest, StoreKeysOnceMatchesModel) {
  TrieRepOptions trie_options;
  trie_options.store_keys_once = true;
  Options options;
  options.create_if_missing = true;
  options.memtable_factory.reset(NewTrieRepFactory(trie_options));
  options.allow_concurrent_memtable_write = false;
  options.memtable_protection_bytes_per_key = 8;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  const std::string dbname = test::PerThreadDBPath("trierep_store_keys_once");
  ASSERT_OK(DestroyDB(dbname, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname, &db));

  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::map<std::string, std::string> model;
  auto random_ops = [&](int count) {
    for (int i = 0; i < count; ++i) {
      const std::string key = "tenant/table/" + RandomTrickyKey(&rnd);
      switch (rnd.Uniform(4)) {
        case 0:
          ASSERT_OK(db->Delete(WriteOptions(), key));
          model.erase(key);
          break;
        case 1: {
          ASSERT_OK(db->Merge(WriteOptions(), key, "m"));
          auto found = model.find(key);
          model[key] = found == model.end() ? "m" : found->second + ",m";
          break;
        }
        default: {
          const std::string value = "v" + std::to_string(i);
          ASSERT_OK(db->Put(WriteOptions(), key, value));
          model[key] = value;
        }
      }
    }
  };
  random_ops(2000);
  CheckDBMatchesModel(db.get(), model);
  const Snapshot* snapshot = db->GetSnapshot();
  const std::map<std::string, std::string> at_snapshot = model;
  random_ops(1000);
  CheckDBMatchesModel(db.get(), model);
  CheckDBMatchesModel(db.get(), at_snapshot, snapshot);
  ASSERT_OK(db->Flush(FlushOptions()));
  CheckDBMatchesModel(db.get(), model);
  random_ops(1000);
  CheckDBMatchesModel(db.get(), model);
  db->ReleaseSnapshot(snapshot);
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(TrieRepDBTest, StoreKeysOnceFallsBackForInplaceUpdates) {
  TrieRepOptions trie_options;
  trie_options.store_keys_once = true;
  Options options;
  options.create_if_missing = true;
  options.memtable_factory.reset(NewTrieRepFactory(trie_options));
  options.allow_concurrent_memtable_write = false;
  options.inplace_update_support = true;
  const std::string dbname = test::PerThreadDBPath("trierep_keys_once_inplace");
  ASSERT_OK(DestroyDB(dbname, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname, &db));
  ASSERT_OK(db->Put(WriteOptions(), "key", "value1"));
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key", &value));
  ASSERT_EQ(value, "value");
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(TrieRepDBTest, RequiresSingleWriter) {
  Options options;
  options.create_if_missing = true;
  options.memtable_factory.reset(NewTrieRepFactory());
  const std::string dbname = test::PerThreadDBPath("trierep_single_writer");
  ASSERT_OK(DestroyDB(dbname, options));
  std::unique_ptr<DB> db;
  ASSERT_TRUE(options.allow_concurrent_memtable_write);
  ASSERT_TRUE(DB::Open(options, dbname, &db).IsInvalidArgument());
  options.allow_concurrent_memtable_write = false;
  ASSERT_OK(DB::Open(options, dbname, &db));
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

#ifndef NDEBUG
// Counts log calls, to observe TrieRep's contention warning.
class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override {
    count_.fetch_add(1, std::memory_order_relaxed);
  }
  int count() const { return count_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int> count_{0};
};

TEST_F(TrieRepTest, ConcurrentInsertIsSerializedAndReported) {
  CountingLogger logger;
  std::unique_ptr<MemTableRep> rep(factory_->CreateMemTableRep(
      cmp_, &arena_, /*transform=*/nullptr, &logger));
  rep_.swap(rep);
  KeyHandle first = PrepareEntry("a", 1, kTypeValue, "v");
  KeyHandle second = PrepareEntry("b", 2, kTypeValue, "v");
  // Whichever insert takes the lock first waits inside it until the other
  // has found the lock taken, so the second insert is always contended.
  SyncPoint::GetInstance()->LoadDependency(
      {{"TrieRep::InsertKey:Contended", "TrieRep::InsertKey:Locked"}});
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread t1([&] { ASSERT_TRUE(rep_->InsertKeyConcurrently(first)); });
  port::Thread t2([&] { ASSERT_TRUE(rep_->InsertKeyConcurrently(second)); });
  t1.join();
  t2.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->ClearTrace();
  ASSERT_EQ(logger.count(), 1);
  CheckFullOrder();
}
#endif  // !NDEBUG

TEST_F(TrieRepTest, ApproximateCountsAndSample) {
  for (int i = 0; i < 1000; ++i) {
    char buf[8];
    EncodeFixed64(buf, static_cast<uint64_t>(i));
    InsertEntry(Slice(buf, 8), static_cast<SequenceNumber>(i + 1), kTypeValue,
                "v");
  }
  std::vector<std::string> order = ModelOrder();
  ASSERT_EQ(rep_->ApproximateNumEntries(order.front(), order.back()),
            order.size());
  ASSERT_EQ(rep_->ApproximateNumEntries(order[100], order[199]), 100u);
  std::unordered_set<const char*> sample;
  rep_->UniqueRandomSample(static_cast<uint64_t>(order.size()), 50, &sample);
  ASSERT_EQ(sample.size(), 50u);
  for (const char* entry : sample) {
    ASSERT_GT(live_buffers_.count(entry), 0u);
  }
}

TEST_F(TrieRepTest, FactoryBasics) {
  std::unique_ptr<MemTableRepFactory> factory(NewTrieRepFactory());
  ASSERT_STREQ(factory->Name(), TrieRepFactory::kClassName());
  ASSERT_FALSE(factory->IsInsertConcurrentlySupported());
  ASSERT_TRUE(factory->CanHandleDuplicatedKey());
  ASSERT_TRUE(factory->IsInstanceOf("trie"));
  ASSERT_TRUE(factory->IsInstanceOf("TrieRepFactory"));
}

TEST_F(TrieRepTest, FactoryFallsBackWithoutBytewise) {
  ReverseUserComparator reverse;
  InternalKeyComparator ricmp(&reverse);
  MemTable::KeyComparator rcmp(ricmp);
  std::unique_ptr<MemTableRepFactory> factory(NewTrieRepFactory());
  std::unique_ptr<MemTableRep> fallback(
      factory->CreateMemTableRep(rcmp, &arena_, /*transform=*/nullptr,
                                 /*logger=*/nullptr));
  ASSERT_TRUE(fallback != nullptr);
  // The fallback must honor the reverse order (a real trie would not).
  auto put = [&](const Slice& k, SequenceNumber s) {
    InsertRaw(fallback.get(), EncodeEntry(k, s, kTypeValue, "v"));
  };
  put("aaa", 1);
  put("mmm", 2);
  put("zzz", 3);
  std::unique_ptr<MemTableRep::Iterator> it(fallback->GetIterator());
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(GetLengthPrefixedSlice(it->key())), Slice("zzz"));
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(GetLengthPrefixedSlice(it->key())), Slice("mmm"));
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(GetLengthPrefixedSlice(it->key())), Slice("aaa"));
  it->Next();
  ASSERT_FALSE(it->Valid());
}

// Inserts into a key-owning rep the way MemTable::Add does: the rep takes the
// key, the caller writes the value part.
void InsertOwned(MemTableRep* rep, const std::string& user_key,
                 SequenceNumber seq, const std::string& value) {
  const size_t value_part_len = VarintLength(value.size()) + value.size();
  char* value_part = nullptr;
  KeyHandle h =
      rep->AllocateWithKey(user_key, PackSequenceAndType(seq, kTypeValue),
                           value_part_len, &value_part);
  char* p = EncodeVarint32(value_part, static_cast<uint32_t>(value.size()));
  memcpy(p, value.data(), value.size());
  ASSERT_TRUE(rep->InsertKey(h));
}

std::string InternalKeyString(const std::string& user_key, SequenceNumber seq) {
  std::string ikey = user_key;
  PutFixed64(&ikey, PackSequenceAndType(seq, kTypeValue));
  return ikey;
}

// User keys built to stress key rebuilding: shared prefixes, zero bytes
// (escaped in trie paths), and keys that are prefixes of other keys.
std::string RandomTrickyKey(Random* rnd) {
  static const std::string kPrefixes[] = {"", "a", std::string("ab\0", 3),
                                          "abcdefgh", "abcdefghij"};
  static const char kBytes[] = {'\0', '\x01', 'a', 'b', '\xff'};
  std::string key = kPrefixes[rnd->Uniform(5)];
  const int extra = static_cast<int>(rnd->Uniform(7));
  for (int i = 0; i < extra; ++i) {
    key.push_back(kBytes[rnd->Uniform(5)]);
  }
  return key;
}

struct CollectedEntries {
  std::vector<std::pair<std::string, std::string>> entries;
};

bool CollectEntry(void* arg, const MemTableRep::EntryView& entry) {
  static_cast<CollectedEntries*>(arg)->entries.emplace_back(
      entry.internal_key.ToString(),
      GetLengthPrefixedSlice(entry.value_part).ToString());
  return true;
}

TEST_F(TrieRepTest, StoreKeysOnceMatchesModel) {
  TrieRepOptions options;
  options.store_keys_once = true;
  std::unique_ptr<MemTableRepFactory> factory(NewTrieRepFactory(options));
  std::unique_ptr<MemTableRep> rep(factory->CreateMemTableRep(
      cmp_, &arena_, /*transform=*/nullptr, /*logger=*/nullptr));
  ASSERT_TRUE(rep->OwnsKeys());

  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::map<std::string, std::string, RefKeyCmp> model;
  SequenceNumber seq = 0;
  for (int i = 0; i < 2000; ++i) {
    const std::string user_key = RandomTrickyKey(&rnd);
    const std::string value = "v" + std::to_string(i);
    InsertOwned(rep.get(), user_key, ++seq, value);
    model[InternalKeyString(user_key, seq)] = value;
  }

  // Full order both ways, with values.
  std::vector<std::pair<std::string, std::string>> expected(model.begin(),
                                                            model.end());
  std::vector<std::pair<std::string, std::string>> forward;
  std::unique_ptr<MemTableRep::Iterator> it(rep->GetIterator());
  ASSERT_FALSE(it->IsKeyPinned());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const MemTableRep::EntryView e = it->entry();
    forward.emplace_back(e.internal_key.ToString(),
                         GetLengthPrefixedSlice(e.value_part).ToString());
  }
  ASSERT_EQ(forward, expected);
  std::vector<std::pair<std::string, std::string>> backward;
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    const MemTableRep::EntryView e = it->entry();
    backward.emplace_back(e.internal_key.ToString(),
                          GetLengthPrefixedSlice(e.value_part).ToString());
  }
  std::reverse(backward.begin(), backward.end());
  ASSERT_EQ(backward, expected);

  for (int i = 0; i < 500; ++i) {
    const std::string target =
        InternalKeyString(RandomTrickyKey(&rnd), rnd.Uniform(2500));
    auto ge = model.lower_bound(target);
    it->Seek(target, nullptr);
    ASSERT_EQ(it->Valid(), ge != model.end());
    if (it->Valid()) {
      ASSERT_EQ(it->entry().internal_key.ToString(), ge->first);
    }
    auto le = model.upper_bound(target);
    it->SeekForPrev(target, nullptr);
    ASSERT_EQ(it->Valid(), le != model.begin());
    if (it->Valid()) {
      ASSERT_EQ(it->entry().internal_key.ToString(), std::prev(le)->first);
    }
  }

  // Point lookups offer exactly the versions of the user key, newest first.
  for (int i = 0; i < 500; ++i) {
    const std::string user_key = RandomTrickyKey(&rnd);
    std::vector<std::pair<std::string, std::string>> versions;
    for (auto v =
             model.lower_bound(InternalKeyString(user_key, kMaxSequenceNumber));
         v != model.end() && ExtractUserKey(v->first) == user_key; ++v) {
      versions.emplace_back(*v);
    }
    CollectedEntries collected;
    rep->GetEntries(LookupKey(user_key, kMaxSequenceNumber), &collected,
                    CollectEntry);
    ASSERT_EQ(collected.entries, versions);
  }

  for (const auto& kv : model) {
    std::string memtable_key;
    PutLengthPrefixedSlice(&memtable_key, kv.first);
    ASSERT_TRUE(rep->Contains(memtable_key.data()));
  }
  ASSERT_EQ(
      rep->ApproximateNumEntries(model.begin()->first, model.rbegin()->first),
      model.size());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
