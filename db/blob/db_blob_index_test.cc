//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_test_util.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void CorruptPinnedBlobIndexOnCleanup(void* arg1, void* /*arg2*/) {
  auto* blob_index = static_cast<std::string*>(arg1);
  assert(blob_index != nullptr);
  assert(!blob_index->empty());
  (*blob_index)[0] = static_cast<char>(BlobIndex::Type::kUnknown);
}

}  // namespace

TEST(BlobIndexTest, SameFileBlobIndex) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/123, /*size=*/456, kNoCompression);

  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_TRUE(blob_index.IsSameFile());
  EXPECT_EQ(blob_index.file_number(), kCurrentFileBlobIndexFileNumber);
  EXPECT_NE(blob_index.DebugString(false).find("file:same"), std::string::npos);
}

TEST(BlobIndexTest, EmptyBlobIndexIsCorruption) {
  BlobIndex blob_index;
  ASSERT_EQ(blob_index.DecodeFrom(Slice()).code(), Status::Code::kCorruption);
}

// kTypeBlobIndex is a value type used by BlobDB only. The base rocksdb
// should accept the value type on write, and report not supported value
// for reads, unless caller request for it explicitly. The base rocksdb
// doesn't understand format of actual blob index (the value).
class DBBlobIndexTest : public DBTestBase {
 public:
  enum Tier {
    kMemtable = 0,
    kImmutableMemtables = 1,
    kL0SstFile = 2,
    kLnSstFile = 3,
  };
  const std::vector<Tier> kAllTiers = {Tier::kMemtable,
                                       Tier::kImmutableMemtables,
                                       Tier::kL0SstFile, Tier::kLnSstFile};

  DBBlobIndexTest() : DBTestBase("db_blob_index_test", /*env_do_fsync=*/true) {}

  ColumnFamilyHandle* cfh() { return dbfull()->DefaultColumnFamily(); }
  ColumnFamilyHandleImpl* cfh_impl() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(cfh());
  }
  ColumnFamilyData* cfd() { return cfh_impl()->cfd(); }

  Status PutBlobIndex(WriteBatch* batch, const Slice& key,
                      const Slice& blob_index) {
    return WriteBatchInternal::PutBlobIndex(batch, cfd()->GetID(), key,
                                            blob_index);
  }

  Status Write(WriteBatch* batch) {
    return dbfull()->Write(WriteOptions(), batch);
  }

  std::string GetImpl(const Slice& key, bool* is_blob_index = nullptr,
                      const Snapshot* snapshot = nullptr) {
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    PinnableSlice value;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = cfh();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = is_blob_index;
    auto s = dbfull()->GetImpl(read_options, key, get_impl_options);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    if (s.IsCorruption()) {
      return "CORRUPTION";
    }
    if (s.IsNotSupported()) {
      return "NOT_SUPPORTED";
    }
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }

  std::string GetBlobIndex(const Slice& key,
                           const Snapshot* snapshot = nullptr) {
    bool is_blob_index = false;
    std::string value = GetImpl(key, &is_blob_index, snapshot);
    if (!is_blob_index) {
      return "NOT_BLOB";
    }
    return value;
  }

  ArenaWrappedDBIter* GetBlobIterator() {
    DBImpl* db_impl = dbfull();
    return db_impl->NewIteratorImpl(
        ReadOptions(), cfh_impl(), cfd()->GetReferencedSuperVersion(db_impl),
        db_impl->GetLatestSequenceNumber(), nullptr /*read_callback*/,
        true /*expose_blob_index*/);
  }

  bool MaybeResolveDirectWriteValueForTest(
      const ReadOptions& read_options, const Slice& key,
      bool resolve_direct_write_value, const Version* current,
      PinnableSlice* value, PinnableWideColumns* columns, Status* s,
      bool* is_blob_index, bool* value_found = nullptr) {
    VersionBlobFetcher blob_fetcher(current, read_options,
                                    cfd()->blob_file_cache(),
                                    /*allow_write_path_fallback=*/true);
    return DBImpl::MaybeResolveDirectWriteValue(
        read_options, key, resolve_direct_write_value, blob_fetcher, value,
        columns, s, is_blob_index, value_found);
  }

  Status MaybeResolveMemtableBlobValueForTest(
      const Slice& key, const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, bool* did_resolve, bool* is_blob_index,
      bool* value_found = nullptr) {
    return DBImpl::MaybeResolveMemtableBlobValue(key, blob_fetcher, value,
                                                 columns, did_resolve,
                                                 is_blob_index, value_found);
  }

  Status PostprocessMemtableValueReadForTest(
      const Slice& key, bool resolve_blob_backed_memtable_value,
      const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, Status memtable_read_status,
      bool* is_blob_index, bool* value_found = nullptr) {
    return DBImpl::PostprocessMemtableValueRead(
        key, /*timestamp=*/nullptr, resolve_blob_backed_memtable_value,
        blob_fetcher, value, columns, std::move(memtable_read_status),
        is_blob_index, value_found);
  }

  Options GetTestOptions() {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.disable_auto_compactions = true;
    // Disable auto flushes.
    options.max_write_buffer_number = 10;
    options.min_write_buffer_number_to_merge = 10;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    return options;
  }

  Options GetBlobTestOptions() {
    return wide_column_test_util::GetOptionsForBlobTest(GetTestOptions());
  }

  void MoveDataTo(Tier tier) {
    switch (tier) {
      case Tier::kMemtable:
        break;
      case Tier::kImmutableMemtables:
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
        break;
      case Tier::kL0SstFile:
        ASSERT_OK(Flush());
        break;
      case Tier::kLnSstFile:
        ASSERT_OK(Flush());
        ASSERT_OK(Put("a", "dummy"));
        ASSERT_OK(Put("z", "dummy"));
        ASSERT_OK(Flush());
        ASSERT_OK(
            dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
        ASSERT_EQ("0,1", FilesPerLevel());
        break;
    }
  }
};

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. We should be able to write kTypeBlobIndex to memtables and
// SST files.
TEST_F(DBBlobIndexTest, Write) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    std::vector<std::pair<std::string, std::string>> key_values;

    constexpr size_t num_key_values = 5;

    key_values.reserve(num_key_values);

    for (size_t i = 1; i <= num_key_values; ++i) {
      std::string key = "key" + std::to_string(i);

      std::string blob_index;
      BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210,
                                  "blob" + std::to_string(i));

      key_values.emplace_back(std::move(key), std::move(blob_index));
    }

    for (const auto& key_value : key_values) {
      WriteBatch batch;
      ASSERT_OK(PutBlobIndex(&batch, key_value.first, key_value.second));
      ASSERT_OK(Write(&batch));
    }

    MoveDataTo(tier);

    for (const auto& key_value : key_values) {
      ASSERT_EQ(GetBlobIndex(key_value.first), key_value.second);
    }
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should be able to return blob index if is_blob_index is
// provided, otherwise it should return Status::NotSupported (when reading from
// memtable) or Status::Corruption (when reading from SST). Reading from SST
// returns Corruption because we can't differentiate between the application
// accidentally opening the base DB of a stacked BlobDB and actual corruption
// when using the integrated BlobDB.
TEST_F(DBBlobIndexTest, Get) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    WriteBatch batch;
    ASSERT_OK(batch.Put("key", "value"));
    ASSERT_OK(PutBlobIndex(&batch, "blob_key", blob_index));
    ASSERT_OK(Write(&batch));

    MoveDataTo(tier);

    // Verify normal value
    bool is_blob_index = false;
    PinnableSlice value;
    ASSERT_EQ("value", Get("key"));
    ASSERT_EQ("value", GetImpl("key"));
    ASSERT_EQ("value", GetImpl("key", &is_blob_index));
    ASSERT_FALSE(is_blob_index);

    // Verify blob index
    if (tier <= kImmutableMemtables) {
      ASSERT_TRUE(Get("blob_key", &value).IsNotSupported());
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("blob_key"));
    } else {
      ASSERT_TRUE(Get("blob_key", &value).IsCorruption());
      ASSERT_EQ("CORRUPTION", GetImpl("blob_key"));
    }
    ASSERT_EQ(blob_index, GetImpl("blob_key", &is_blob_index));
    ASSERT_TRUE(is_blob_index);
  }
}

TEST_F(DBBlobIndexTest,
       MaybeResolveDirectWriteValueDecodesPinnedBlobIndexBeforeReset) {
  DestroyAndReopen(GetTestOptions());

  std::string blob_index;
  BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                        /*size=*/789, kNoCompression);

  PinnableSlice value;
  value.PinSlice(Slice(blob_index), CorruptPinnedBlobIndexOnCleanup,
                 &blob_index, nullptr);

  Status s = Status::OK();
  bool is_blob_index = true;
  ASSERT_TRUE(MaybeResolveDirectWriteValueForTest(
      ReadOptions(), Slice("key"), /*resolve_direct_write_value=*/true,
      /*current=*/nullptr, &value, /*columns=*/nullptr, &s, &is_blob_index));

  ASSERT_FALSE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(s.IsIOError() || s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(is_blob_index);
  ASSERT_EQ(static_cast<char>(BlobIndex::Type::kUnknown), blob_index.front());
}

TEST_F(DBBlobIndexTest,
       MaybeResolveMemtableBlobValueWithoutFetcherFailsClosed) {
  // Goal: if a readonly/secondary memtable hit produces a blob-backed payload
  // but no BlobFetcher is available, the helper must fail closed instead of
  // handing raw blob-index bytes back to the caller as if they were the value.
  std::string blob_index;
  BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                        /*size=*/789, kNoCompression);

  PinnableSlice value;
  value.GetSelf()->assign(blob_index.data(), blob_index.size());
  value.PinSelf();

  bool did_resolve = false;
  bool is_blob_index = true;
  Status s = MaybeResolveMemtableBlobValueForTest(
      Slice("key"), /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr,
      &did_resolve, &is_blob_index);

  ASSERT_TRUE(did_resolve);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(value.empty());
  ASSERT_FALSE(is_blob_index);
}

TEST_F(DBBlobIndexTest, PostprocessMemtableValueReadClearsOutputsOnError) {
  // Regression test for the "swallowed status" FIXME: on any error,
  // PostprocessMemtableValueRead must clear the outputs and return the status,
  // never leaving a half-populated result behind a non-OK status.

  // Case 1: the incoming memtable read status is already non-OK. The resolve
  // path is skipped; the finalization block clears outputs and returns it.
  {
    PinnableSlice value;
    value.GetSelf()->assign("stale", 5);
    value.PinSelf();

    bool is_blob_index = false;
    Status s = PostprocessMemtableValueReadForTest(
        Slice("key"), /*resolve_blob_backed_memtable_value=*/true,
        /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr,
        Status::Corruption("read failed"), &is_blob_index);

    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(value.empty());
  }

  // Case 2: the incoming status is OK but the blob-resolve path itself fails (a
  // blob-backed value with no fetcher). This is the actual scenario the FIXME
  // referred to -- previously the non-OK status from the resolve path was
  // dropped. The failure must propagate and the raw blob-index bytes must be
  // cleared rather than handed back as if they were the value.
  {
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                          /*size=*/789, kNoCompression);

    PinnableSlice value;
    value.GetSelf()->assign(blob_index.data(), blob_index.size());
    value.PinSelf();

    bool is_blob_index = true;
    Status s = PostprocessMemtableValueReadForTest(
        Slice("key"), /*resolve_blob_backed_memtable_value=*/true,
        /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr, Status::OK(),
        &is_blob_index);

    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(value.empty());
  }
}

TEST_F(DBBlobIndexTest, ReadOnlyGetImplReturnsBlobIndexWhenRequested) {
  // Goal: cover the internal read-only GetImpl contract when the caller
  // explicitly asks for raw blob-index bytes via `is_blob_index`. Recovery
  // keeps the blob index in the memtable, and the read-only path must preserve
  // the encoded index instead of eagerly resolving or rejecting it.
  Options options = GetTestOptions();

  DestroyAndReopen(options);

  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /*expiration=*/9876543210, "blob");

  WriteBatch batch;
  ASSERT_OK(PutBlobIndex(&batch, "blob_key", blob_index));
  ASSERT_OK(Write(&batch));

  Close();
  options.avoid_flush_during_recovery = true;
  ASSERT_OK(ReadOnlyReopen(options));

  bool is_blob_index = false;
  ASSERT_EQ(blob_index, GetImpl("blob_key", &is_blob_index));
  ASSERT_TRUE(is_blob_index);
}

// Regression test for a same-file (embedded) BlobIndex being exposed
// unresolved through an iterator. With index_type=kBinarySearchWithFirstKey
// and allow_unprepared_value (the default for DB iterators), the block-based
// table iterator can sit in the is_at_first_key_from_index_ state and return
// the raw kTypeBlobIndex internal key from the index, while value() resolves
// the same-file blob to its plain payload. DBIter then reads the stale
// kTypeBlobIndex type and routes the already-resolved plain value through the
// blob-index path, corrupting/misreading it.
//
// Forcing one entry per data block makes every embedded blob the first key of
// a block, reliably triggering the deferred-first-key state on both SeekToFirst
// and forward block transitions.
TEST_F(DBBlobIndexTest, EmbeddedBlobIteratorWithFirstKeyIndex) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  // Soft limit of 1 byte starts a new data block after every entry.
  bbto.block_size = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Build an embedded-blob SST whose large values are stored as same-file blob
  // records (table entries become same-file BlobIndex references).
  const std::string sst_path = dbname_ + "/embedded_first_key.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');
  const std::string big3(1024, 'z');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Put("k3", big3));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Point lookups resolve same-file blobs before exposing them, so Get works.
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  ASSERT_EQ(Get("k3"), big3);

  // Iterator path: the bug manifests here.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k3");
  EXPECT_EQ(iter->value(), big3);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Seek directly to a block whose first key is an embedded blob.
  iter->Seek("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);
}

// Backward iteration (Prev / SeekForPrev) over an embedded-blob SST must work,
// including for wide-column entities whose same-file blob columns are resolved
// into an iterator-owned buffer. DBIter requires the underlying iterator's
// value to be pinned for backward iteration; EmbeddedBlobResolvingIterator
// therefore pins resolved values -- both whole-value blob payloads and rebuilt
// wide-column values -- and hands their cleanup to the PinnedIteratorsManager
// across repositioning. Before that, backward iteration over the wide-column
// entity returned NotSupported (or tripped the IsValuePinned() assertion).
TEST_F(DBBlobIndexTest, EmbeddedBlobBackwardIteration) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_backward.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // Whole-value same-file blobs.
  const std::string big1(1024, 'x');
  const std::string big3(1024, 'z');
  // Wide-column entity: a small (inline) default column plus a large
  // (embedded, same-file blob) non-default column -- the mixed wide-column
  // path whose resolved value lands in an iterator-owned buffer.
  const std::string k2_default(2, 'd');
  const std::string k2_big_col(1024, 'c');
  const WideColumns k2_columns{{kDefaultWideColumnName, k2_default},
                               {"big", k2_big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.PutEntity("k2", k2_columns));
  ASSERT_OK(writer.Put("k3", big3));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

  // Backward scan via SeekToLast + Prev.
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k3");
  EXPECT_EQ(iter->value(), big3);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), k2_default);
  EXPECT_EQ(iter->columns(), k2_columns);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // SeekForPrev directly onto the wide-column entity, then step back.
  iter->SeekForPrev("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), k2_default);
  EXPECT_EQ(iter->columns(), k2_columns);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);
}

// Regression test for T277566778. If resolving a same-file ("embedded") blob
// fails during compaction (e.g. a blob-region read fault), the error must be
// surfaced as-is. It must NOT be masked by feeding the raw, unresolved
// same-file BlobIndex downstream, where FileMetaData::UpdateBoundaries would
// (correctly) reject file number 0 and report a misleading "Invalid blob file
// number" corruption. The EmbeddedBlobResolvingIterator must not expose an
// unresolved same-file value on error: for eager (compaction) callers it
// resolves during positioning so the error is visible via status()/Valid()
// before value().
TEST_F(DBBlobIndexTest, EmbeddedBlobResolveErrorDuringCompactionNotMasked) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_resolve_err.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // Wide-column entity with a large (embedded, same-file blob) column: the
  // mixed path whose resolution reads the blob region during compaction.
  const std::string big_col(1024, 'c');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k2", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Add keys straddling the entity so a real compaction (not a trivial move)
  // reads and resolves the embedded entity.
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Flush());

  // Inject a resolution failure only during the compaction below, simulating a
  // blob-region read fault.
  const Status kInjected = Status::IOError("injected embedded blob read error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // The compaction must fail with the injected read error, NOT a masked
  // "Invalid blob file number" corruption.
  ASSERT_NOK(s);
  EXPECT_TRUE(s.IsIOError()) << s.ToString();
  EXPECT_EQ(s.ToString().find("Invalid blob file number"), std::string::npos)
      << s.ToString();
}

// Regression test for T277310719, the whole-value (kTypeBlobIndex) sibling of
// the wide-column-entity case above (T277566778). Both share one root cause: on
// a blob-region resolution error during compaction, the
// EmbeddedBlobResolvingIterator must not fall back to exposing the raw,
// unresolved same-file value.
//
// The whole-value variant is more dangerous than the entity variant because it
// escapes the FileMetaData::UpdateBoundaries tripwire: ResolveKeyType()
// rewrites the key type kTypeBlobIndex -> kTypeValue (no I/O, always succeeds),
// so on a masked resolution error the emitted entry is {kTypeValue, raw
// BlobIndex bytes}. UpdateBoundaries only scans kTypeBlobIndex/entity types, so
// unlike the entity variant it does NOT reject it -- the masked error slips
// through, a corrupt {kTypeValue, BlobIndex} record is written to the
// compaction output, and a later point lookup reads those raw BlobIndex bytes
// back as the value (in db_stress this surfaced as the
// ExpectedValue::IsValueBaseValid assertion, not "Invalid blob file number").
// The eager (compaction) resolver must surface the error via status()/Valid()
// before value() is consumed.
TEST_F(DBBlobIndexTest,
       EmbeddedBlobResolveErrorWholeValueDuringCompactionNotMasked) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_resolve_err_wholevalue.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // A large whole value is stored as a same-file blob, so the table entry
  // becomes a same-file (file number 0) BlobIndex (kTypeBlobIndex) -- the
  // whole-value path whose resolution reads the blob region during compaction.
  const std::string big_val(1024, 'b');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k2", big_val));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Add keys straddling the embedded blob so a real compaction (not a trivial
  // move) reads and resolves it.
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Flush());

  // Inject a resolution failure only during the compaction below, simulating a
  // blob-region read fault.
  const Status kInjected = Status::IOError("injected embedded blob read error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // The compaction must fail with the injected read error. Before the fix it
  // instead succeeded (masking the error) and persisted a {kTypeValue, raw
  // BlobIndex} record; it must not do that, and it must not report a misleading
  // "Invalid blob file number" corruption either.
  ASSERT_NOK(s);
  EXPECT_TRUE(s.IsIOError()) << s.ToString();
  EXPECT_EQ(s.ToString().find("Invalid blob file number"), std::string::npos)
      << s.ToString();
}

// When a blob cache is configured, same-file ("embedded") blob reads should go
// through BlobSource: the first read misses + inserts + does a disk read, and
// the second read of the same blob is served from the blob cache. This holds on
// both the Get() and the iterator paths.
TEST_F(DBBlobIndexTest, EmbeddedBlobBlobCacheHitMiss) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);

  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_cache.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  Statistics* const stats = options.statistics.get();

  // First Get: blob cache miss -> disk read -> cache insert.
  ASSERT_OK(stats->Reset());
  get_perf_context()->Reset();
  SetPerfLevel(kEnableCount);
  ASSERT_EQ(Get("k1"), big1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 1);
  EXPECT_GT(stats->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ), 0);
  EXPECT_EQ(get_perf_context()->blob_read_count, 1);

  // Second Get of the same key: blob cache hit, no disk read, no insert.
  ASSERT_OK(stats->Reset());
  get_perf_context()->Reset();
  ASSERT_EQ(Get("k1"), big1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ), 0);
  EXPECT_EQ(get_perf_context()->blob_read_count, 0);
  EXPECT_EQ(get_perf_context()->blob_cache_hit_count, 1);

  // Iterator path: first read of a fresh key misses + inserts.
  ASSERT_OK(stats->Reset());
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("k2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(iter->value(), big2);
  }
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 1);

  // Iterator path: second read of the same key hits the blob cache.
  ASSERT_OK(stats->Reset());
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("k2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(iter->value(), big2);
  }
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 0);

  SetPerfLevel(kDisable);
}

// With the blob cache and block cache pointing at the same Cache, the SST-like
// cache-key scheme (offset >> 2 on the SST's base key) keeps embedded blob
// records disjoint from the SST's data blocks. Reading embedded blobs must not
// alias or evict data blocks, and values must remain correct.
TEST_F(DBBlobIndexTest, EmbeddedBlobSharedBlockAndBlobCache) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  LRUCacheOptions co;
  co.capacity = 16 << 20;
  auto shared_cache = NewLRUCache(co);
  options.blob_cache = shared_cache;

  BlockBasedTableOptions bbto;
  bbto.block_cache = shared_cache;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_shared.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  Statistics* const stats = options.statistics.get();

  // Values are correct with a shared cache, and re-reads hit both the block
  // cache (data blocks) and the blob cache (embedded payloads) independently.
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  EXPECT_GT(stats->getTickerCount(BLOB_DB_CACHE_HIT), 0);
  EXPECT_GT(stats->getTickerCount(BLOCK_CACHE_HIT), 0);

  // Iterator yields correct values too.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Point GetEntity / MultiGetEntity of a wide-column entity whose non-default
// columns are embedded (same-file) blobs must return the correct columns and
// resolve them zero-copy: each resolved blob column value points straight at
// its pinned blob-cache buffer (identical across reads, and in a separate
// buffer per column), rather than being copied into one re-serialized entity.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityZeroCopy) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_zerocopy.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string small_default = "d";  // inline default column
  const std::string big1(1024, 'a');      // same-file blob column
  const std::string big2(2048, 'b');      // same-file blob column
  const WideColumns columns{
      {kDefaultWideColumnName, small_default}, {"big1", big1}, {"big2", big2}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  const char* big1_ptr = nullptr;
  const char* big2_ptr = nullptr;
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);

    // columns() is sorted by name -> [default(""), big1, big2].
    big1_ptr = result.columns()[1].value().data();
    big2_ptr = result.columns()[2].value().data();
    // Multi-buffer: each blob column lives in its own backing buffer, so the
    // two values are not laid out contiguously (as they would be in a single
    // re-serialized V1 entity).
    ASSERT_NE(big1_ptr + big1.size(), big2_ptr);

    // Pointer stability across move (address-stable backing nodes).
    PinnableWideColumns moved(std::move(result));
    ASSERT_EQ(moved.columns(), columns);
    ASSERT_EQ(moved.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(moved.columns()[2].value().data(), big2_ptr);
  }

  // A second read serves the same pinned blob-cache buffers (zero-copy): the
  // resolved column values point at the very same addresses, which a per-read
  // re-serialization would not.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
    ASSERT_EQ(result.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(result.columns()[2].value().data(), big2_ptr);
  }

  // MultiGetEntity: same correctness and zero-copy (cache-hit) guarantee.
  {
    const std::array<Slice, 1> keys{Slice("k")};
    std::array<PinnableWideColumns, 1> results;
    std::array<Status, 1> statuses;
    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                        keys.data(), results.data(), statuses.data());
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns);
    ASSERT_EQ(results[0].columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(results[0].columns()[2].value().data(), big2_ptr);
  }
}

// A wide-column entity whose default column is itself an embedded (same-file)
// blob must resolve correctly on the plain Get path (default-column
// extraction) as well as GetEntity, matching the values written.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetDefaultColumn) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_default.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_default(1024, 'x');  // same-file blob default column
  const std::string small_attr = "s";        // inline non-default column
  const WideColumns columns{{kDefaultWideColumnName, big_default},
                            {"attr", small_attr}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Plain Get returns the (blob-backed) default column value.
  PinnableSlice value;
  ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "k", &value));
  ASSERT_EQ(value, big_default);

  // GetEntity returns all columns.
  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k", &result));
  ASSERT_EQ(result.columns(), columns);
}

// Merge over an embedded-blob wide-column base entity must resolve the base
// entity's same-file default column before merging, and GetMergeOperands must
// return the resolved default column as the base operand.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnMergeBase) {
  Options options = GetTestOptions();  // merge_operator = StringAppend(',')
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_merge.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_default(1024, 'D');  // same-file blob default column
  const WideColumns columns{{kDefaultWideColumnName, big_default},
                            {"attr", "s"}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Newer merge operand lives in the memtable; the embedded entity base lives
  // in the ingested SST, so the merge resolves the base via the same-file path.
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), "k", "op1"));

  PinnableSlice value;
  ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "k", &value));
  ASSERT_EQ(value, big_default + "," + "op1");

  // GetMergeOperands returns the resolved base default column plus the operand.
  std::array<PinnableSlice, 8> operands;
  GetMergeOperandsOptions get_merge_opts;
  get_merge_opts.expected_max_number_of_operands =
      static_cast<int>(operands.size());
  int num_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k", operands.data(), &get_merge_opts,
                                  &num_operands));
  ASSERT_EQ(num_operands, 2);
  ASSERT_EQ(operands[0], big_default);
  ASSERT_EQ(operands[1], "op1");
}

// With read_tier == kBlockCacheTier, an unresolved same-file blob column of a
// wide-column entity must surface Incomplete (never a raw same-file BlobIndex).
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityBlockCacheTier) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_blockcache.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big1}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Warm the block cache so the data block read succeeds under kBlockCacheTier
  // and the lookup actually reaches same-file blob column resolution.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  PinnableWideColumns result;
  Status s =
      db_->GetEntity(read_options, db_->DefaultColumnFamily(), "k", &result);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

// Companion to the test above (genuine miss -> Incomplete): with a blob cache
// configured, a warm read populates the embedded record into the blob cache, so
// a subsequent kBlockCacheTier GetEntity serves it from cache instead of
// returning Incomplete. This exercises the non-lazy Get/GetEntity embedded path
// (get_context -> EmbeddedAwareBlobFetcher -> GetSameFileBlob whole ->
// ResolveEmbeddedBlobCached), which shares the block-cache-tier behavior with
// the lazy resolver (a same-file column already in the blob cache is served).
TEST_F(DBBlobIndexTest,
       EmbeddedBlobWideColumnGetEntityBlockCacheTierServesCached) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_blockcache_hit.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big1}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Warm both the block cache (data block) and the blob cache (embedded record)
  // with a full read.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  // The embedded record is now cached, so a block-cache-only read serves it.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(read_options, db_->DefaultColumnFamily(), "k", &result));
  ASSERT_EQ(result.columns(), columns);
}

// A corrupt embedded blob record backing a wide-column column must surface an
// error on GetEntity, not expose the raw same-file BlobIndex.
// A failure resolving an embedded blob record backing a wide-column column
// (e.g. a bad record or blob-region read fault) must surface on GetEntity, not
// expose the raw same-file BlobIndex. The failure is injected via a sync point
// so the test is independent of the on-disk byte layout (e.g. encrypted env).
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityCorruptionSurfaced) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_corrupt.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_col(1024, 'Z');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  const Status kInjected = Status::Corruption("injected embedded blob error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableWideColumns result;
  const Status s =
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k", &result);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

class PlainBlobValueFilterV3 : public CompactionFilter {
 public:
  PlainBlobValueFilterV3(std::atomic<int>* filter_call_count,
                         std::string* observed_value)
      : filter_call_count_(filter_call_count),
        observed_value_(observed_value) {}

  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* /*existing_columns*/,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type != ValueType::kValue || existing_value == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);
    *observed_value_ = existing_value->ToString();
    return Decision::kRemove;
  }

  const char* Name() const override { return "PlainBlobValueFilterV3"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
};

class PlainBlobValueFilterV3Factory : public CompactionFilterFactory {
 public:
  PlainBlobValueFilterV3Factory(TableFileCreationReason reason,
                                std::atomic<int>* filter_call_count,
                                std::string* observed_value)
      : reason_(reason),
        filter_call_count_(filter_call_count),
        observed_value_(observed_value) {}

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == reason_;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<PlainBlobValueFilterV3>(filter_call_count_,
                                                    observed_value_);
  }

  const char* Name() const override { return "PlainBlobValueFilterV3Factory"; }

 private:
  TableFileCreationReason reason_;
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
};

TEST_F(DBBlobIndexTest, DirectWritePlainBlobFilterV3FlushUsesResolvedValue) {
  std::atomic<int> filter_call_count{0};
  std::string observed_value;

  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetTestOptions());
  options.compaction_filter_factory =
      std::make_shared<PlainBlobValueFilterV3Factory>(
          TableFileCreationReason::kFlush, &filter_call_count, &observed_value);

  DestroyAndReopen(options);

  const std::string key = "flush_blob_key";
  const std::string large_value(10 * 1024, 'F');

  ASSERT_OK(Put(key, large_value));
  ASSERT_OK(Flush());

  ASSERT_EQ("NOT_FOUND", Get(key));
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(observed_value, large_value);
}

class PlainBlobValueFilterV4 : public CompactionFilter {
 public:
  PlainBlobValueFilterV4(std::atomic<int>* filter_call_count,
                         std::string* observed_value,
                         std::atomic<bool>* saw_nonnull_blob_resolver)
      : filter_call_count_(filter_call_count),
        observed_value_(observed_value),
        saw_nonnull_blob_resolver_(saw_nonnull_blob_resolver) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* /*existing_columns*/,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kValue || existing_value == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);
    *observed_value_ = existing_value->ToString();
    saw_nonnull_blob_resolver_->store(blob_resolver != nullptr,
                                      std::memory_order_relaxed);
    return Decision::kRemove;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "PlainBlobValueFilterV4"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
  std::atomic<bool>* saw_nonnull_blob_resolver_;
};

class PlainBlobValueFilterV4Factory : public CompactionFilterFactory {
 public:
  PlainBlobValueFilterV4Factory(TableFileCreationReason reason,
                                std::atomic<int>* filter_call_count,
                                std::string* observed_value,
                                std::atomic<bool>* saw_nonnull_blob_resolver)
      : reason_(reason),
        filter_call_count_(filter_call_count),
        observed_value_(observed_value),
        saw_nonnull_blob_resolver_(saw_nonnull_blob_resolver) {}

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == reason_;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<PlainBlobValueFilterV4>(
        filter_call_count_, observed_value_, saw_nonnull_blob_resolver_);
  }

  const char* Name() const override { return "PlainBlobValueFilterV4Factory"; }

 private:
  TableFileCreationReason reason_;
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
  std::atomic<bool>* saw_nonnull_blob_resolver_;
};

// Blob direct-write forces a clean-shutdown flush on close so active blob
// files are registered before reopen. Use flush to exercise the non-compaction
// plain-blob FilterV4 path.
TEST_F(DBBlobIndexTest, DirectWritePlainBlobFilterV4FlushUsesResolvedValue) {
  std::atomic<int> filter_call_count{0};
  std::string observed_value;
  std::atomic<bool> saw_nonnull_blob_resolver{false};

  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetTestOptions());
  options.compaction_filter_factory =
      std::make_shared<PlainBlobValueFilterV4Factory>(
          TableFileCreationReason::kFlush, &filter_call_count, &observed_value,
          &saw_nonnull_blob_resolver);

  DestroyAndReopen(options);

  const std::string key = "flush_v4_blob_key";
  const std::string large_value(10 * 1024, 'R');

  ASSERT_OK(Put(key, large_value));
  ASSERT_OK(Flush());

  ASSERT_EQ("NOT_FOUND", Get(key));
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(observed_value, large_value);
  ASSERT_FALSE(saw_nonnull_blob_resolver.load(std::memory_order_relaxed));
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should NOT return Status::NotSupported/Status::Corruption
// if blob index is updated with a normal value. See the test case above for
// more details.
TEST_F(DBBlobIndexTest, Updated) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    WriteBatch batch;
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(PutBlobIndex(&batch, "key" + std::to_string(i), blob_index));
    }
    ASSERT_OK(Write(&batch));
    // Avoid blob values from being purged.
    const Snapshot* snapshot = dbfull()->GetSnapshot();
    ASSERT_OK(Put("key1", "new_value"));
    ASSERT_OK(Merge("key2", "a"));
    ASSERT_OK(Merge("key2", "b"));
    ASSERT_OK(Merge("key2", "c"));
    ASSERT_OK(Delete("key3"));
    ASSERT_OK(SingleDelete("key4"));
    ASSERT_OK(Delete("key5"));
    ASSERT_OK(Merge("key5", "a"));
    ASSERT_OK(Merge("key5", "b"));
    ASSERT_OK(Merge("key5", "c"));
    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(), cfh(), "key6", "key9"));
    MoveDataTo(tier);
    for (int i = 0; i < 10; i++) {
      ASSERT_EQ(blob_index, GetBlobIndex("key" + std::to_string(i), snapshot));
    }
    ASSERT_EQ("new_value", Get("key1"));
    if (tier <= kImmutableMemtables) {
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("key2"));
    } else {
      ASSERT_EQ("CORRUPTION", GetImpl("key2"));
    }
    ASSERT_EQ("NOT_FOUND", Get("key3"));
    ASSERT_EQ("NOT_FOUND", Get("key4"));
    ASSERT_EQ("a,b,c", GetImpl("key5"));
    for (int i = 6; i < 9; i++) {
      ASSERT_EQ("NOT_FOUND", Get("key" + std::to_string(i)));
    }
    ASSERT_EQ(blob_index, GetBlobIndex("key9"));
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. When a blob iterator is used, it should set the
// expose_blob_index flag for the underlying DBIter, and retrieve/return the
// corresponding blob value. If a regular DBIter is created (i.e.
// expose_blob_index is not set), it should return Status::Corruption.
TEST_F(DBBlobIndexTest, Iterate) {
  const std::vector<std::vector<ValueType>> data = {
      /*00*/ {kTypeValue},
      /*01*/ {kTypeBlobIndex},
      /*02*/ {kTypeValue},
      /*03*/ {kTypeBlobIndex, kTypeValue},
      /*04*/ {kTypeValue},
      /*05*/ {kTypeValue, kTypeBlobIndex},
      /*06*/ {kTypeValue},
      /*07*/ {kTypeDeletion, kTypeBlobIndex},
      /*08*/ {kTypeValue},
      /*09*/ {kTypeSingleDeletion, kTypeBlobIndex},
      /*10*/ {kTypeValue},
      /*11*/ {kTypeMerge, kTypeMerge, kTypeMerge, kTypeBlobIndex},
      /*12*/ {kTypeValue},
      /*13*/
      {kTypeMerge, kTypeMerge, kTypeMerge, kTypeDeletion, kTypeBlobIndex},
      /*14*/ {kTypeValue},
      /*15*/ {kTypeBlobIndex},
      /*16*/ {kTypeValue},
  };

  auto get_key = [](int index) {
    char buf[20];
    snprintf(buf, sizeof(buf), "%02d", index);
    return "key" + std::string(buf);
  };

  auto get_value = [&](int index, int version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, Status::Code expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status().code());
    if (expected_status == Status::kOk) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto create_normal_iterator = [&]() -> Iterator* {
    return dbfull()->NewIterator(ReadOptions());
  };

  auto create_blob_iterator = [&]() -> Iterator* { return GetBlobIterator(); };

  auto check_is_blob = [&](bool is_blob) {
    return [is_blob](Iterator* iterator) {
      ASSERT_EQ(is_blob, static_cast<ArenaWrappedDBIter*>(iterator)->IsBlob());
    };
  };

  auto verify = [&](int index, Status::Code expected_status,
                    const Slice& forward_value, const Slice& backward_value,
                    const std::function<Iterator*()>& create_iterator,
                    const std::function<void(Iterator*)>& extra_check =
                        nullptr) {
    // Seek
    auto* iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index));
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Next
    iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index - 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Next();
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // SeekForPrev
    iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->SeekForPrev(get_key(index));
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Prev
    iterator = create_iterator();
    iterator->Seek(get_key(index + 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Prev();
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;
  };

  for (auto tier : {Tier::kMemtable} /*kAllTiers*/) {
    // Avoid values from being purged.
    std::vector<const Snapshot*> snapshots;
    DestroyAndReopen(GetTestOptions());

    // fill data
    for (int i = 0; i < static_cast<int>(data.size()); i++) {
      for (int j = static_cast<int>(data[i].size()) - 1; j >= 0; j--) {
        std::string key = get_key(i);
        std::string value = get_value(i, j);
        WriteBatch batch;
        switch (data[i][j]) {
          case kTypeValue:
            ASSERT_OK(Put(key, value));
            break;
          case kTypeDeletion:
            ASSERT_OK(Delete(key));
            break;
          case kTypeSingleDeletion:
            ASSERT_OK(SingleDelete(key));
            break;
          case kTypeMerge:
            ASSERT_OK(Merge(key, value));
            break;
          case kTypeBlobIndex:
            ASSERT_OK(PutBlobIndex(&batch, key, value));
            ASSERT_OK(Write(&batch));
            break;
          default:
            FAIL();
        };
      }
      snapshots.push_back(dbfull()->GetSnapshot());
    }
    ASSERT_OK(
        dbfull()->DeleteRange(WriteOptions(), cfh(), get_key(15), get_key(16)));
    snapshots.push_back(dbfull()->GetSnapshot());
    MoveDataTo(tier);

    // Normal iterator
    verify(1, Status::kCorruption, "", "", create_normal_iterator);
    verify(3, Status::kCorruption, "", "", create_normal_iterator);
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_normal_iterator);
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_normal_iterator);
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_normal_iterator);
    verify(11, Status::kCorruption, "", "", create_normal_iterator);
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_normal_iterator);
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_normal_iterator);

    // Iterator with blob support
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    // Iterator with blob support and using seek.
    ASSERT_OK(dbfull()->SetOptions(
        cfh(), {{"max_sequential_skip_in_iterations", "0"}}));
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    for (auto* snapshot : snapshots) {
      dbfull()->ReleaseSnapshot(snapshot);
    }
  }
}

// Use shared test utilities for Wide Column + Blob integration tests
using wide_column_test_util::GenerateLargeValue;
using wide_column_test_util::GenerateSmallValue;

// Test 1: Full roundtrip test: PutEntity with large columns -> flush ->
// compaction with blob extraction -> read back with blob resolution -> verify
TEST_F(DBBlobIndexTest, EntityBlobFlushCompactionRoundtrip) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.statistics = CreateDBStatistics();

  DestroyAndReopen(options);

  // Create entities with large column values that will become blobs
  constexpr char key1[] = "entity_key1";
  constexpr char key2[] = "entity_key2";
  constexpr char key3[] = "entity_key3";

  // Large value for default column (will become blob)
  std::string large_default_value = GenerateLargeValue(100);
  // Large value for non-default columns
  std::string large_attr1_value = GenerateLargeValue(150);
  std::string large_attr2_value = GenerateLargeValue(200);
  // Small value that stays inline
  std::string small_value = GenerateSmallValue();

  // Entity 1: Large default column + large attribute
  WideColumns columns1{{kDefaultWideColumnName, large_default_value},
                       {"attr1", large_attr1_value}};

  // Entity 2: No default column, only large attributes
  WideColumns columns2{{"attr1", large_attr1_value},
                       {"attr2", large_attr2_value}};

  // Entity 3: Mixed - small default, large attribute
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", large_attr1_value}};

  // Write entities
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));

  // Verify from memtable
  auto verify = [&]() {
    // Verify entity 1
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &result));
      ASSERT_EQ(result.columns(), columns1);

      // Also verify Get returns the default column value
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
      ASSERT_EQ(value, large_default_value);
    }

    // Verify entity 2
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns2);

      // Get should return empty for entity without default column
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &value));
      ASSERT_TRUE(value.empty());
    }

    // Verify entity 3
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                               &result));
      ASSERT_EQ(result.columns(), columns3);

      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key3, &value));
      ASSERT_EQ(value, small_value);
    }

    // Verify via iterator
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      iter->SeekToFirst();

      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key1);
      ASSERT_EQ(iter->columns(), columns1);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key2);
      ASSERT_EQ(iter->columns(), columns2);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key3);
      ASSERT_EQ(iter->columns(), columns3);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  // Verify from memtable
  verify();

  // Flush to create SST files (and potentially blob files)
  ASSERT_OK(Flush());
  verify();

  // Add more data and flush again to create multiple files
  constexpr char key4[] = "entity_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string large_value4_default = GenerateLargeValue(120);
  std::string large_value4_attr1 = GenerateLargeValue(80);
  WideColumns columns4{{kDefaultWideColumnName, large_value4_default},
                       {"attr1", large_value4_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));
  ASSERT_OK(Flush());

  // Compact to trigger blob extraction and GC
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Explicit verification: blob files were actually created after compaction
  {
    // Verify at least one blob file exists
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_GE(blob_files.size(), 1)
        << "Expected at least one blob file after compaction with entity data";

    // Verify using DB property
    uint64_t num_blob_files = 0;
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kNumBlobFiles, &num_blob_files));
    ASSERT_GE(num_blob_files, 1)
        << "kNumBlobFiles property should report at least one blob file";

    // Verify blob file size is non-zero (data was written)
    uint64_t total_blob_file_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                    &total_blob_file_size));
    ASSERT_GT(total_blob_file_size, 0)
        << "Total blob file size should be greater than zero";

    // Verify blob statistics - confirm data was written to blob files
    uint64_t blob_bytes_written =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_WRITTEN);
    ASSERT_GT(blob_bytes_written, 0)
        << "Expected non-zero bytes written to blob files";
  }

  // Verify original entities after compaction (verify() only checks key1-3,
  // so we verify them individually since key4 was added)
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    ASSERT_EQ(result.columns(), columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result));
    ASSERT_EQ(result.columns(), columns2);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                             &result));
    ASSERT_EQ(result.columns(), columns3);
  }

  // Verify key4 after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key4,
                             &result));
    ASSERT_EQ(result.columns(), columns4);
  }

  // Multiple flush/compaction cycles
  for (int cycle = 0; cycle < 3; ++cycle) {
    std::string cycle_key = "cycle_key" + std::to_string(cycle);
    // Store large values in persistent strings to avoid dangling Slice
    // references
    std::string cycle_default_value = GenerateLargeValue(100);
    std::string cycle_attr_value = GenerateLargeValue(50);
    WideColumns cycle_columns{{kDefaultWideColumnName, cycle_default_value},
                              {"cycle_attr", cycle_attr_value}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             cycle_key, cycle_columns));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Verify
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             cycle_key, &result));
    ASSERT_EQ(result.columns(), cycle_columns);
  }

  Close();
}

// Test 2: Entity blob GC respects snapshots - blobs referenced by snapshotted
// entities should not be GC'd
TEST_F(DBBlobIndexTest, EntityBlobGCWithSnapshot) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  constexpr char key[] = "snapshot_key";
  std::string original_value = GenerateLargeValue(100);
  std::string updated_value = GenerateLargeValue(150);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string original_attr1_value = GenerateLargeValue(80);
  std::string updated_attr1_value = GenerateLargeValue(120);

  WideColumns original_columns{{kDefaultWideColumnName, original_value},
                               {"attr1", original_attr1_value}};
  WideColumns updated_columns{{kDefaultWideColumnName, updated_value},
                              {"attr1", updated_attr1_value}};

  // Write original entity
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           original_columns));
  ASSERT_OK(Flush());

  // Take a snapshot
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);

  // Update the entity with new values
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           updated_columns));
  ASSERT_OK(Flush());

  // Compact - this should trigger GC, but the old blobs should be preserved
  // due to the snapshot
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  // Verify snapshot still sees original value
  {
    ReadOptions read_options;
    read_options.snapshot = snapshot;

    PinnableWideColumns result;
    ASSERT_OK(
        db_->GetEntity(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result.columns(), original_columns);

    // Also verify Get with snapshot
    PinnableSlice value;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &value));
    ASSERT_EQ(value, original_value);
  }

  // Release snapshot and compact again - now old blobs can be GC'd
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value still accessible
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  Close();
}

// Test 3: DB recovery replays unflushed WAL entries for entities with blob
// columns.
TEST_F(DBBlobIndexTest, EntityBlobRecoveryReplaysUnflushedWAL) {
  Options options = GetBlobTestOptions();
  options.avoid_flush_during_shutdown = true;

  DestroyAndReopen(options);

  // Write entities with various column configurations
  constexpr char key1[] = "recovery_key1";
  constexpr char key2[] = "recovery_key2";
  constexpr char key3[] = "recovery_key3";

  std::string large_value1 = GenerateLargeValue(100);
  std::string large_value2 = GenerateLargeValue(150);
  std::string small_value = GenerateSmallValue();
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns1_attr1 = GenerateLargeValue(80);
  std::string columns3_attr1 = GenerateLargeValue(200);

  WideColumns columns1{{kDefaultWideColumnName, large_value1},
                       {"attr1", columns1_attr1}};
  WideColumns columns2{{"attr1", large_value2}, {"attr2", small_value}};
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", columns3_attr1}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(Flush());

  // Compact to ensure blob extraction
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Write more data to WAL (not flushed) so recovery has live memtable data to
  // replay.
  constexpr char key4[] = "recovery_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns4_default = GenerateLargeValue(120);
  WideColumns columns4{{kDefaultWideColumnName, columns4_default},
                       {"attr1", "unflushed_attr"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));

  // Skip shutdown flush so key4 stays in WAL and must be recovered on reopen.
  Close();
  Reopen(options);

  // Verify all data is recovered correctly
  auto verify_entity = [&](const std::string& key,
                           const WideColumns& expected) {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), expected);
  };

  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);

  // Verify via Get for entities with default column
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
    ASSERT_EQ(value, large_value1);
  }

  // Verify via iterator
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 4);
  }

  // Add another unflushed entity so reopening with
  // `avoid_flush_during_recovery` also has WAL-backed recovery work to do.
  constexpr char key5[] = "recovery_key5";
  std::string columns5_default = GenerateLargeValue(140);
  WideColumns columns5{{kDefaultWideColumnName, columns5_default},
                       {"attr1", "recovered_with_avoid_flush"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key5,
                           columns5));

  Close();
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  // All data should still be accessible
  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);
  verify_entity(key5, columns5);

  Close();
}

// Test 5: Entities with blob columns work correctly mixed with regular Put
// operations that also use blobs
TEST_F(DBBlobIndexTest, EntityBlobMixedWithRegularPut) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  // Regular Put with large value (becomes blob)
  constexpr char put_key1[] = "put_key1";
  std::string put_value1 = GenerateLargeValue(100);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     put_value1));

  // Entity with large columns
  constexpr char entity_key1[] = "entity_key1";
  std::string entity_default = GenerateLargeValue(120);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns1_attr1 = GenerateLargeValue(80);
  WideColumns entity_columns1{{kDefaultWideColumnName, entity_default},
                              {"attr1", entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, entity_columns1));

  // Regular Put with small value (stays inline)
  constexpr char put_key2[] = "put_key2";
  std::string put_value2 = GenerateSmallValue();
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key2,
                     put_value2));

  // Entity without default column
  constexpr char entity_key2[] = "entity_key2";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns2_attr1 = GenerateLargeValue(90);
  std::string entity_columns2_attr2 = GenerateLargeValue(110);
  WideColumns entity_columns2{{"attr1", entity_columns2_attr1},
                              {"attr2", entity_columns2_attr2}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key2, entity_columns2));

  // More regular Put
  constexpr char put_key3[] = "put_key3";
  std::string put_value3 = GenerateLargeValue(200);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key3,
                     put_value3));

  // Flush
  ASSERT_OK(Flush());

  // Verify all values
  auto verify = [&]() {
    // Regular Put values
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1,
                         &value));
      ASSERT_EQ(value, put_value1);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2,
                         &value));
      ASSERT_EQ(value, put_value2);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3,
                         &value));
      ASSERT_EQ(value, put_value3);
    }

    // Entity values
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key1, &result));
      ASSERT_EQ(result.columns(), entity_columns1);

      // Get on entity returns default column
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), entity_key1,
                         &value));
      ASSERT_EQ(value, entity_default);
    }
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key2, &result));
      ASSERT_EQ(result.columns(), entity_columns2);
    }

    // GetEntity on regular Put returns single default column
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               put_key1, &result));
      ASSERT_EQ(result.columns().size(), 1);
      ASSERT_EQ(result.columns()[0].name(), kDefaultWideColumnName);
      ASSERT_EQ(result.columns()[0].value(), put_value1);
    }

    // Iterator should see all keys in order
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      std::vector<std::string> expected_keys = {entity_key1, entity_key2,
                                                put_key1, put_key2, put_key3};
      int idx = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ASSERT_OK(iter->status());
        ASSERT_LT(idx, static_cast<int>(expected_keys.size()));
        ASSERT_EQ(iter->key(), expected_keys[idx]);
        idx++;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(idx, static_cast<int>(expected_keys.size()));
    }
  };

  verify();

  // Add more data and compact
  constexpr char put_key4[] = "put_key4";
  std::string put_value4 = GenerateLargeValue(150);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key4,
                     put_value4));

  constexpr char entity_key3[] = "entity_key3";
  // Store values in persistent strings to avoid dangling Slice references
  std::string entity_columns3_default = GenerateLargeValue(130);
  std::string entity_columns3_attr1 = GenerateSmallValue();
  WideColumns entity_columns3{{kDefaultWideColumnName, entity_columns3_default},
                              {"attr1", entity_columns3_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key3, entity_columns3));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify original entries after compaction (verify() only checks the original
  // 5 keys, so we verify them individually since put_key4 and entity_key3 were
  // added)
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, put_value1);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2, &value));
    ASSERT_EQ(value, put_value2);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3, &value));
    ASSERT_EQ(value, put_value3);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), entity_columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key2, &result));
    ASSERT_EQ(result.columns(), entity_columns2);
  }

  // Verify new entries after compaction
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key4, &value));
    ASSERT_EQ(value, put_value4);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key3, &result));
    ASSERT_EQ(result.columns(), entity_columns3);
  }

  // Update some values and verify GC works correctly
  std::string updated_put_value1 = GenerateLargeValue(180);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     updated_put_value1));

  std::string updated_entity_default = GenerateLargeValue(190);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string updated_entity_columns1_attr1 = GenerateLargeValue(85);
  WideColumns updated_entity_columns1{
      {kDefaultWideColumnName, updated_entity_default},
      {"attr1", updated_entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, updated_entity_columns1));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify updated values
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, updated_put_value1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), updated_entity_columns1);
  }

  Close();
}

// Test 6: Verify lazy loading - when a compaction filter only accesses inline
// columns and uses the blob_resolver to check IsBlobColumn(), blob values
// are NOT read from blob files.
class LazyLoadingSmallColumnFilter : public CompactionFilter {
 public:
  LazyLoadingSmallColumnFilter(std::atomic<int>* filter_call_count,
                               std::string* last_small_col_value,
                               std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    // Only process wide-column entities
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to check which columns are blobs and only access
      // inline columns. This demonstrates lazy loading - we never call
      // ResolveColumn on blob columns.
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          (*resolver_check_count_)++;
          if (!blob_resolver->IsBlobColumn(i)) {
            // Only access inline columns
            const auto& col = (*existing_columns)[i];
            if (col.name() == "small_col") {
              *last_small_col_value_ = col.value().ToString();
            }
          }
        }
      } else {
        // Fallback if no resolver - just access small_col
        for (const auto& col : *existing_columns) {
          if (col.name() == "small_col") {
            *last_small_col_value_ = col.value().ToString();
            break;
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "LazyLoadingSmallColumnFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

class LazyLoadingSmallColumnFilterFactory : public CompactionFilterFactory {
 public:
  LazyLoadingSmallColumnFilterFactory(std::atomic<int>* filter_call_count,
                                      std::string* last_small_col_value,
                                      std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<LazyLoadingSmallColumnFilter>(
        filter_call_count_, last_small_col_value_, resolver_check_count_);
  }

  const char* Name() const override {
    return "LazyLoadingSmallColumnFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterSkipsBlobs) {
  // Test: When a compaction filter uses blob_resolver->IsBlobColumn() to check
  // which columns are blobs and DOESN'T call ResolveColumn on blob columns,
  // no blob I/O should occur.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter checks IsBlobColumn() and only accesses small_col
  // - Result: No blob bytes should be read!

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(last_small_col_value, small_value);

  // Verify resolver was used to check columns
  ASSERT_GE(resolver_check_count.load(), 2)
      << "Expected resolver to be used to check at least 2 columns";

  // Check blob read statistics after compaction
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);

  // With lazy loading, when filter only accesses inline columns via
  // IsBlobColumn() check, NO blob should be read!
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter uses IsBlobColumn() "
         "to skip blob columns. Got "
      << blob_bytes_read << " bytes.";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

// Test 7: Verify that when a compaction filter uses the blob_resolver to
// resolve blob columns, the blob values are correctly fetched.
class BlobResolvingFilter : public CompactionFilter {
 public:
  BlobResolvingFilter(std::atomic<int>* filter_call_count,
                      std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to resolve the blob column
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          const auto& col = (*existing_columns)[i];
          if (col.name() == "large_col" && blob_resolver->IsBlobColumn(i)) {
            Slice resolved_value;
            Status s = blob_resolver->ResolveColumn(i, &resolved_value);
            if (s.ok()) {
              *resolved_large_col_value_ = resolved_value.ToString();
            }
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "BlobResolvingFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

class BlobResolvingFilterFactory : public CompactionFilterFactory {
 public:
  BlobResolvingFilterFactory(std::atomic<int>* filter_call_count,
                             std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingFilter>(filter_call_count_,
                                                 resolved_large_col_value_);
  }

  const char* Name() const override { return "BlobResolvingFilterFactory"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

class BlobResolvingErrorIgnoringFilter : public CompactionFilter {
 public:
  BlobResolvingErrorIgnoringFilter(std::atomic<int>* filter_call_count,
                                   std::atomic<int>* resolve_error_count,
                                   std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kWideColumnEntity ||
        existing_columns == nullptr || blob_resolver == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);

    for (size_t i = 0; i < existing_columns->size(); ++i) {
      const auto& col = (*existing_columns)[i];
      if (col.name() == "large_col" && blob_resolver->IsBlobColumn(i)) {
        Slice resolved_value;
        const Status s = blob_resolver->ResolveColumn(i, &resolved_value);
        if (!s.ok()) {
          ++(*resolve_error_count_);
          *resolve_error_status_ = s.ToString();
        }
        break;
      }
    }

    // Even if the filter chooses kKeep after seeing a resolver error, the
    // compaction path should still fail and surface that read error.
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override {
    return "BlobResolvingErrorIgnoringFilter";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

class BlobResolvingErrorIgnoringFilterFactory : public CompactionFilterFactory {
 public:
  BlobResolvingErrorIgnoringFilterFactory(std::atomic<int>* filter_call_count,
                                          std::atomic<int>* resolve_error_count,
                                          std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingErrorIgnoringFilter>(
        filter_call_count_, resolve_error_count_, resolve_error_status_);
  }

  const char* Name() const override {
    return "BlobResolvingErrorIgnoringFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterResolvesBlobs) {
  // Test: When a compaction filter uses blob_resolver->ResolveColumn() to
  // fetch blob values, the values are correctly resolved.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter calls ResolveColumn on large_col
  // - Result: Blob bytes should be read and value should match

  std::atomic<int> filter_call_count{0};
  std::string resolved_large_col_value;

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<BlobResolvingFilterFactory>(&filter_call_count,
                                                   &resolved_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // Verify the resolved value matches the original large value
  ASSERT_EQ(resolved_large_col_value, large_value)
      << "Expected resolved blob value to match original large_value";

  // Check blob read statistics after compaction - should have read the blob
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_GE(blob_bytes_read, large_value.size())
      << "Expected at least " << large_value.size()
      << " bytes read from blob when filter resolves blob column";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterMissingBlobFailsCompaction) {
  // Goal: keep lazy FilterV4 resolver failures aligned with the eager FilterV3
  // path. The filter calls ResolveColumn() on a blob-backed column, then
  // returns kKeep after observing the error. Compaction must still fail and
  // latch bg_error instead of silently keeping the entry.
  std::atomic<int> filter_call_count{0};
  std::atomic<int> resolve_error_count{0};
  std::string resolve_error_status;

  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = false;
  options.paranoid_checks = true;
  options.compaction_filter_factory =
      std::make_shared<BlobResolvingErrorIgnoringFilterFactory>(
          &filter_call_count, &resolve_error_count, &resolve_error_status);

  DestroyAndReopen(options);

  constexpr char key[] = "missing_blob_key_v4";
  const std::string large_value(10 * 1024, 'L');
  WideColumns columns{{"large_col", large_value}, {"small_col", "small"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const auto blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));

  const Status status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_FALSE(status.ok())
      << "Compaction should fail when FilterV4 lazy blob resolution hits a "
         "read error";
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(resolve_error_count.load(), 1);
  ASSERT_FALSE(resolve_error_status.empty());

  const Status bg_error = dbfull()->TEST_GetBGError();
  ASSERT_FALSE(bg_error.ok());
  ASSERT_TRUE(bg_error.IsCorruption() || bg_error.IsIOError() ||
              bg_error.IsNotFound())
      << bg_error.ToString();
  ASSERT_GE(static_cast<int>(bg_error.severity()),
            static_cast<int>(Status::Severity::kHardError));
}

// Test 8: Verify backward compatibility - old filters that don't
// use the blob_resolver still work correctly by accessing inline columns.
// With lazy loading, blob columns will have blob indices (not actual values),
// but inline columns work correctly.
TEST_F(DBBlobIndexTest, EntityBlobCompactionFilterAccessesOnlySmallColumn) {
  // This test verifies backward compatibility: filters that don't use
  // the blob_resolver can still access inline columns correctly.
  //
  // With lazy loading, blob columns contain blob indices (not actual values),
  // but inline columns have the correct values. Since this filter
  // only accesses the inline small_col, it should work correctly.

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  // Reuse the LazyLoadingSmallColumnFilter which uses IsBlobColumn() check
  // to only access inline columns
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // The filter accessed small_col which is inline, so it should get the
  // correct value
  ASSERT_EQ(last_small_col_value, small_value);

  // With lazy loading, no blob bytes should be read since filter doesn't
  // access blob columns
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter only accesses inline columns";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

TEST_F(DBBlobIndexTest, IntegratedBlobIterate) {
  const std::vector<std::vector<std::string>> data = {
      /*00*/ {"Put"},
      /*01*/ {"Put", "Merge", "Merge", "Merge"},
      /*02*/ {"Put"}};

  auto get_key = [](size_t index) { return ("key" + std::to_string(index)); };

  auto get_value = [&](size_t index, size_t version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, const Status& expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status());
    if (expected_status.ok()) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto verify = [&](size_t index, const Status& expected_status,
                    const Slice& expected_value) {
    // Seek
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Next
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index - 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Next();
      check_iterator(iterator, expected_status, expected_value);
    }
    // SeekForPrev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->SeekForPrev(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Prev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      iterator->Seek(get_key(index + 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Prev();
      check_iterator(iterator, expected_status, expected_value);
    }
  };

  Options options = GetTestOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  DestroyAndReopen(options);

  // fill data
  for (size_t i = 0; i < data.size(); i++) {
    for (size_t j = 0; j < data[i].size(); j++) {
      std::string key = get_key(i);
      std::string value = get_value(i, j);
      if (data[i][j] == "Put") {
        ASSERT_OK(Put(key, value));
        ASSERT_OK(Flush());
      } else if (data[i][j] == "Merge") {
        ASSERT_OK(Merge(key, value));
        ASSERT_OK(Flush());
      }
    }
  }

  std::string expected_value = get_value(1, 0) + "," + get_value(1, 1) + "," +
                               get_value(1, 2) + "," + get_value(1, 3);
  Status expected_status;
  verify(1, expected_status, expected_value);

  // Test DBIter::FindValueForCurrentKeyUsingSeek flow.
  ASSERT_OK(dbfull()->SetOptions(cfh(),
                                 {{"max_sequential_skip_in_iterations", "0"}}));
  verify(1, expected_status, expected_value);
}

// FilterV3-only compaction filter that inspects entity column values.
// Does NOT override FilterV4, so SupportsFilterV4() returns false (default).
// This tests backward compatibility: blob columns should be eagerly resolved
// before the filter sees them, so the filter sees actual blob values (not
// raw BlobIndex bytes).
class FilterV3OnlyEntityFilter : public CompactionFilter {
 public:
  explicit FilterV3OnlyEntityFilter(std::atomic<int>* filter_call_count,
                                    std::string* observed_large_col_value)
      : filter_call_count_(filter_call_count),
        observed_large_col_value_(observed_large_col_value) {}

  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type == ValueType::kWideColumnEntity && existing_columns) {
      (*filter_call_count_)++;
      for (const auto& col : *existing_columns) {
        if (col.name() == "large_col") {
          *observed_large_col_value_ = col.value().ToString();
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "FilterV3OnlyEntityFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_large_col_value_;
};

class FilterV3OnlyEntityFilterFactory : public CompactionFilterFactory {
 public:
  FilterV3OnlyEntityFilterFactory(std::atomic<int>* filter_call_count,
                                  std::string* observed_large_col_value)
      : filter_call_count_(filter_call_count),
        observed_large_col_value_(observed_large_col_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<FilterV3OnlyEntityFilter>(
        filter_call_count_, observed_large_col_value_);
  }

  const char* Name() const override {
    return "FilterV3OnlyEntityFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_large_col_value_;
};

TEST_F(DBBlobIndexTest, EntityBlobFilterV3BackwardCompatibility) {
  // Test: A FilterV3-only compaction filter (no FilterV4 override) should see
  // resolved blob values, not raw BlobIndex bytes. The compaction path should
  // eagerly resolve blob columns before calling the filter.

  std::atomic<int> filter_call_count{0};
  std::string observed_large_col_value;

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<FilterV3OnlyEntityFilterFactory>(
          &filter_call_count, &observed_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";
  std::string small_value(5, 's');
  // Large value that will be stored as blob (> min_blob_size of 10)
  std::string large_value(10 * 1024, 'L');

  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Compact to trigger the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Filter should have been called
  ASSERT_GE(filter_call_count.load(), 1);

  // The FilterV3 filter should have seen the actual blob value, not raw
  // BlobIndex bytes. BlobIndex bytes would be much shorter than the original
  // value.
  ASSERT_EQ(observed_large_col_value, large_value);
}

TEST_F(DBBlobIndexTest, EntityBlobFilterV3MissingBlobFailsCompaction) {
  std::atomic<int> filter_call_count{0};
  std::string observed_large_col_value;

  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<FilterV3OnlyEntityFilterFactory>(
          &filter_call_count, &observed_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "missing_blob_key";
  const std::string large_value(10 * 1024, 'L');
  WideColumns columns{{"large_col", large_value}, {"small_col", "small"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const auto blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));

  const Status status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_FALSE(status.ok()) << "Compaction should fail when a FilterV3-only "
                               "filter cannot eagerly resolve a blob-backed "
                               "entity";
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_EQ(filter_call_count.load(), 0);
  ASSERT_TRUE(observed_large_col_value.empty());
}

// FilterV3-only filter that removes entities with blob columns.
// Tests that kRemove works correctly with eagerly resolved blob values.
class FilterV3RemoveEntityFilter : public CompactionFilter {
 public:
  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type == ValueType::kWideColumnEntity && existing_columns) {
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "FilterV3RemoveEntityFilter"; }
};

class FilterV3RemoveEntityFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<FilterV3RemoveEntityFilter>();
  }

  const char* Name() const override {
    return "FilterV3RemoveEntityFilterFactory";
  }
};

TEST_F(DBBlobIndexTest, EntityBlobFilterV3Remove) {
  // Test: A FilterV3-only filter returning kRemove should correctly delete
  // entities with eagerly resolved blob columns.

  Options options = GetBlobTestOptions();
  options.compaction_filter_factory =
      std::make_shared<FilterV3RemoveEntityFilterFactory>();

  DestroyAndReopen(options);

  constexpr char key[] = "remove_key";
  std::string large_value(10 * 1024, 'R');
  std::string small_value(5, 's');

  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Compact to trigger the filter -- entity should be removed
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Key should be gone
  PinnableSlice result;
  Status s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

// Compaction filter that drops wide column entities based on a TTL column,
// using FilterV4 with SupportsFilterV4()=true to avoid loading blob values.
class TTLBasedEntityDropFilter : public CompactionFilter {
 public:
  explicit TTLBasedEntityDropFilter(bool enabled) : enabled_(enabled) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (!enabled_) {
      return Decision::kKeep;
    }
    if (value_type != ValueType::kWideColumnEntity || !existing_columns) {
      return Decision::kKeep;
    }
    // Read only the "ttl" column (inline, not a blob) to decide.
    // Skip blob columns entirely -- no blob I/O.
    for (size_t i = 0; i < existing_columns->size(); ++i) {
      if (blob_resolver && blob_resolver->IsBlobColumn(i)) {
        continue;  // skip blob columns
      }
      const auto& col = (*existing_columns)[i];
      if (col.name() == "ttl") {
        // TTL value "expired" means drop
        if (col.value() == "expired") {
          return Decision::kRemove;
        }
      }
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "TTLBasedEntityDropFilter"; }

  void SetEnabled(bool enabled) { enabled_ = enabled; }

 private:
  bool enabled_;
};

class TTLBasedEntityDropFilterFactory : public CompactionFilterFactory {
 public:
  TTLBasedEntityDropFilterFactory() : enabled_(false) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<TTLBasedEntityDropFilter>(enabled_.load());
  }

  const char* Name() const override {
    return "TTLBasedEntityDropFilterFactory";
  }

  void SetEnabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_relaxed);
  }

 private:
  std::atomic<bool> enabled_;
};

TEST_F(DBBlobIndexTest, PassiveGCForWideColumnEntitiesWithBlobColumns) {
  // Reproduce a production scenario where passive GC fails to remove blob
  // files referenced by dropped wide column entities.
  //
  // Setup:
  //   - Universal compaction, passive GC only (no active GC)
  //   - Small blob_file_size so one flush creates multiple blob files
  //   - PutEntity with wide columns: one large "data" column stored in blob,
  //     one small "ttl" column stored inline
  //   - FilterV4 drops entities based on TTL without loading blob values
  //   - One entity has long TTL (survives), many have "expired" TTL
  //   - After compaction, blob files that only contained dropped entities'
  //     blobs should become all-garbage and be removed
  //
  // Expected behavior: blob files with garbage >= total and empty linked_ssts
  // should be dropped by passive GC.

  auto filter_factory = std::make_shared<TTLBasedEntityDropFilterFactory>();

  Options options = GetBlobTestOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.blob_file_size = 500;  // Small: ~1 entity per blob file
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory = filter_factory;
  options.num_levels = 7;

  DestroyAndReopen(options);

  // Large value for the "data" column (will be stored as blob)
  const std::string large_data(200, 'D');
  const std::string small_ttl_alive = "alive";
  const std::string small_ttl_expired = "expired";

  // Disable filter for initial load.
  filter_factory->SetEnabled(false);

  // Insert entities: one survivor, then many expired.
  // Sorted key order determines blob file assignment.
  // Survivor goes to the first blob file.
  ASSERT_OK(db_->PutEntity(
      WriteOptions(), db_->DefaultColumnFamily(), "aaa_survivor",
      WideColumns{{"data", large_data}, {"ttl", small_ttl_alive}}));

  // Expired entities fill subsequent blob files
  for (int i = 0; i < 8; i++) {
    char key[32];
    snprintf(key, sizeof(key), "drop_%02d", i);
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), key,
        WideColumns{{"data", large_data}, {"ttl", small_ttl_expired}}));
  }
  ASSERT_OK(Flush());

  // First compaction without filter (all entities survive)
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  const auto blob_files_initial = GetBlobFileNumbers();
  ASSERT_GE(blob_files_initial.size(), 3)
      << "Expected multiple blob files from entities with large columns";

  // Enable filter and trigger second compaction with overlapping data.
  filter_factory->SetEnabled(true);
  ASSERT_OK(db_->PutEntity(
      WriteOptions(), db_->DefaultColumnFamily(), "drop_00",
      WideColumns{{"data", large_data}, {"ttl", small_ttl_expired}}));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  const auto blob_files_after = GetBlobFileNumbers();

  // Verify survivor is readable.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             "aaa_survivor", &result));
    ASSERT_EQ(result.columns().size(), 2);
  }

  // Verify dropped entities are gone.
  for (int i = 0; i < 8; i++) {
    char key[32];
    snprintf(key, sizeof(key), "drop_%02d", i);
    PinnableSlice val;
    ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &val)
                    .IsNotFound())
        << "Expected " << key << " to be dropped by TTL filter";
  }

  // The key assertion: blob files that only contained dropped entities'
  // blobs should be removed. The count should decrease.
  ASSERT_LT(blob_files_after.size(), blob_files_initial.size())
      << "Passive GC failed: blob files not removed after entities dropped. "
      << "Before: " << blob_files_initial.size()
      << ", After: " << blob_files_after.size()
      << ". BlobGarbageMeter likely does not track kTypeWideColumnEntity.";

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
