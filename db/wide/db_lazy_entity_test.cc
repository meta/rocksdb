//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Tests for the lazy blob resolution + partial (byte-range) column read API
// (DB::GetEntityLazy / LazyWideColumns / DB::MultiGetEntityLazy /
// LazyWideColumnsBatch).

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_gen2_format.h"
#include "db/db_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "file/random_access_file_reader.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// FileSystem wrapper that records the ReadOptions::io_activity of the most
// recent read to a blob file (*.blob), so tests can assert how deferred lazy
// resolve reads are attributed. Separate-file blob payloads live in .blob
// files, so this isolates a deferred ResolveColumn read from the SST reads done
// by GetEntityLazy itself.
class BlobReadIOActivityFS : public FileSystemWrapper {
 public:
  explicit BlobReadIOActivityFS(std::shared_ptr<FileSystem> base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "BlobReadIOActivityFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    const IOStatus s =
        target()->NewRandomAccessFile(fname, file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
    const bool is_blob_file =
        fname.size() >= 5 && fname.compare(fname.size() - 5, 5, ".blob") == 0;
    const bool is_sst_file =
        fname.size() >= 4 && fname.compare(fname.size() - 4, 4, ".sst") == 0;
    if (is_blob_file || is_sst_file) {
      *result = std::make_unique<RecordingFile>(std::move(file), this,
                                                /*is_blob=*/is_blob_file);
    } else {
      *result = std::move(file);
    }
    return s;
  }

  void ResetLastBlobReadIOActivity() {
    last_blob_read_io_activity_.store(
        static_cast<uint8_t>(Env::IOActivity::kUnknown));
  }
  Env::IOActivity last_blob_read_io_activity() const {
    return static_cast<Env::IOActivity>(last_blob_read_io_activity_.load());
  }

  // Counters distinguishing a coalesced blob read (one MultiRead over N blobs)
  // from per-blob reads (N separate Read calls). A coalesced batch of blobs in
  // one blob file (separate-file) or one SST (embedded) issues a single
  // MultiRead; the non-coalesced single-read paths (GetBlob / GetBlobRange /
  // GetSimpleGen2Blob*) issue individual Read calls. (Reads issued *inside* the
  // underlying FS's MultiRead go to the wrapped file, not this wrapper's Read,
  // so they are not double-counted here.) Separate-file blob reads hit .blob
  // files; embedded (same-file) reads hit .sst files, hence separate counters.
  void ResetBlobReadCounts() {
    blob_read_count_.store(0);
    blob_multiread_count_.store(0);
    sst_multiread_count_.store(0);
    sst_multiread_overlap_count_.store(0);
    std::lock_guard<std::mutex> lock(sst_multiread_requests_mutex_);
    sst_multiread_requests_.clear();
    sst_multiread_scratches_.clear();
  }
  uint64_t blob_read_count() const { return blob_read_count_.load(); }
  uint64_t blob_multiread_count() const { return blob_multiread_count_.load(); }
  uint64_t sst_multiread_count() const { return sst_multiread_count_.load(); }

  // The number of SST MultiRead requests that overlap an earlier request of the
  // same call, after sorting each call's requests by offset.
  uint64_t sst_multiread_overlap_count() const {
    return sst_multiread_overlap_count_.load();
  }

  // The (offset, length) requests of each SST MultiRead call since the last
  // ResetBlobReadCounts, one inner list per call, in the order given.
  std::vector<std::vector<std::pair<uint64_t, size_t>>> sst_multiread_requests()
      const {
    std::lock_guard<std::mutex> lock(sst_multiread_requests_mutex_);
    return sst_multiread_requests_;
  }

  // The scratch pointer of each request in sst_multiread_requests(), in the
  // same layout. It shows which buffer each request reads into.
  std::vector<std::vector<const char*>> sst_multiread_scratches() const {
    std::lock_guard<std::mutex> lock(sst_multiread_requests_mutex_);
    return sst_multiread_scratches_;
  }

  // When set, each SST MultiRead call passes its requests to `hook` before it
  // reads. Set it only while no read runs.
  void SetSstMultiReadHook(
      std::function<void(const FSReadRequest*, size_t)> hook) {
    std::lock_guard<std::mutex> lock(sst_multiread_requests_mutex_);
    sst_multiread_hook_ = std::move(hook);
  }

  // When set, every SST MultiRead returns an IOError without reading.
  void SetFailSstMultiRead(bool fail) { fail_sst_multiread_.store(fail); }

  // After each SST MultiRead, the request at `index` in the call gets an
  // IOError of its own, so one request fails while the call succeeds. A
  // negative index turns this off.
  void SetFailSstReadRequest(int index) { fail_sst_read_request_.store(index); }

  // After each SST MultiRead, the request at `index` in the call returns
  // `missing_bytes` fewer bytes than asked, as a short read does. A negative
  // index turns this off.
  void SetShortSstReadRequest(int index, size_t missing_bytes) {
    short_sst_read_missing_bytes_.store(missing_bytes);
    short_sst_read_request_.store(index);
  }

 private:
  class RecordingFile : public FSRandomAccessFileOwnerWrapper {
   public:
    RecordingFile(std::unique_ptr<FSRandomAccessFile>&& file,
                  BlobReadIOActivityFS* fs, bool is_blob)
        : FSRandomAccessFileOwnerWrapper(std::move(file)),
          fs_(fs),
          is_blob_(is_blob) {}

    IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                  Slice* result, char* scratch,
                  IODebugContext* dbg) const override {
      if (is_blob_) {
        fs_->last_blob_read_io_activity_.store(
            static_cast<uint8_t>(options.io_activity));
        fs_->blob_read_count_.fetch_add(1);
      }
      return FSRandomAccessFileOwnerWrapper::Read(offset, n, options, result,
                                                  scratch, dbg);
    }

    IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                       const IOOptions& options, IODebugContext* dbg) override {
      if (is_blob_) {
        fs_->last_blob_read_io_activity_.store(
            static_cast<uint8_t>(options.io_activity));
        fs_->blob_multiread_count_.fetch_add(1);
      } else {
        fs_->sst_multiread_count_.fetch_add(1);
        // FSRandomAccessFile::MultiRead requires requests that do not overlap.
        // The file system in this test can handle overlaps, so count them
        // instead, to catch a caller that breaks the rule.
        std::vector<std::pair<uint64_t, size_t>> ranges;
        std::vector<const char*> scratches;
        for (size_t i = 0; i < num_reqs; ++i) {
          ranges.emplace_back(reqs[i].offset, reqs[i].len);
          scratches.push_back(reqs[i].scratch);
        }
        {
          std::lock_guard<std::mutex> lock(fs_->sst_multiread_requests_mutex_);
          fs_->sst_multiread_requests_.push_back(ranges);
          fs_->sst_multiread_scratches_.push_back(scratches);
          if (fs_->sst_multiread_hook_) {
            fs_->sst_multiread_hook_(reqs, num_reqs);
          }
        }
        std::sort(ranges.begin(), ranges.end());
        // A request can overlap any earlier request, not only the one just
        // before it, so compare with the largest end offset so far.
        uint64_t max_end = 0;
        for (size_t i = 0; i < ranges.size(); ++i) {
          if (i > 0 && ranges[i].first < max_end) {
            fs_->sst_multiread_overlap_count_.fetch_add(1);
          }
          max_end = std::max(max_end, ranges[i].first + ranges[i].second);
        }
        if (fs_->fail_sst_multiread_.load()) {
          return IOStatus::IOError("Injected SST MultiRead error");
        }
        IOStatus s = FSRandomAccessFileOwnerWrapper::MultiRead(reqs, num_reqs,
                                                               options, dbg);
        if (!s.ok()) {
          return s;
        }
        const int fail_index = fs_->fail_sst_read_request_.load();
        if (fail_index >= 0 && static_cast<size_t>(fail_index) < num_reqs) {
          reqs[fail_index].status =
              IOStatus::IOError("Injected SST read request error");
        }
        const int short_index = fs_->short_sst_read_request_.load();
        if (short_index >= 0 && static_cast<size_t>(short_index) < num_reqs) {
          Slice& result = reqs[short_index].result;
          const size_t missing = std::min(
              result.size(), fs_->short_sst_read_missing_bytes_.load());
          result = Slice(result.data(), result.size() - missing);
        }
        return s;
      }
      return FSRandomAccessFileOwnerWrapper::MultiRead(reqs, num_reqs, options,
                                                       dbg);
    }

   private:
    BlobReadIOActivityFS* fs_;
    bool is_blob_;
  };

  std::atomic<uint8_t> last_blob_read_io_activity_{
      static_cast<uint8_t>(Env::IOActivity::kUnknown)};
  std::atomic<uint64_t> blob_read_count_{0};
  std::atomic<uint64_t> blob_multiread_count_{0};
  std::atomic<uint64_t> sst_multiread_count_{0};
  std::atomic<uint64_t> sst_multiread_overlap_count_{0};
  std::atomic<bool> fail_sst_multiread_{false};
  std::atomic<int> fail_sst_read_request_{-1};
  std::atomic<int> short_sst_read_request_{-1};
  std::atomic<size_t> short_sst_read_missing_bytes_{0};
  mutable std::mutex sst_multiread_requests_mutex_;
  std::vector<std::vector<std::pair<uint64_t, size_t>>> sst_multiread_requests_;
  std::vector<std::vector<const char*>> sst_multiread_scratches_;
  std::function<void(const FSReadRequest*, size_t)> sst_multiread_hook_;
};

// A blob cache memory allocator that zero-fills each block and remembers the
// blocks that are allocated now. A test uses it to check that a read buffer
// belongs to the allocator.
class TrackingMemoryAllocator : public MemoryAllocator {
 public:
  static const char* kClassName() { return "TrackingMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }

  void* Allocate(size_t size) override {
    char* block = new char[size];
    memset(block, 0, size);
    std::lock_guard<std::mutex> lock(mutex_);
    blocks_[block] = size;
    return block;
  }

  void Deallocate(void* p) override {
    char* block = static_cast<char*>(p);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      blocks_.erase(block);
    }
    delete[] block;
  }

  // True when [p, p + len) lies inside one block that is allocated now.
  bool Owns(const char* p, size_t len) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = blocks_.upper_bound(p);
    if (it == blocks_.begin()) {
      return false;
    }
    --it;
    return p + len <= it->first + it->second;
  }

 private:
  mutable std::mutex mutex_;
  std::map<const char*, size_t> blocks_;
};
}  // namespace

class DBLazyEntityTest : public DBTestBase {
 protected:
  DBLazyEntityTest()
      : DBTestBase("db_lazy_entity_test", /*env_do_fsync=*/false) {}

  // Options for the lazy API: blob files enabled with a small threshold so
  // large columns become blob references, statistics on so tests can assert
  // exactly which blobs were read, and max_open_files == -1 (required by the
  // lazy API so table readers are immortal and same-file refs stay resolvable).
  Options GetLazyTestOptions() {
    Options options =
        wide_column_test_util::GetOptionsForBlobTest(GetDefaultOptions());
    options.max_open_files = -1;
    options.statistics = CreateDBStatistics();
    return options;
  }

  // Like GetLazyTestOptions but with a blob cache configured, so tests can
  // observe blob-cache hits / (non-)insertions on the partial-read path.
  Options GetLazyTestOptionsWithBlobCache() {
    Options options = GetLazyTestOptions();
    LRUCacheOptions co;
    co.capacity = 8 << 20;  // 8MB
    options.blob_cache = NewLRUCache(co);
    return options;
  }

  uint64_t BlobBytesRead(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  }

  uint64_t BlobCacheAdds(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_CACHE_ADD);
  }

  uint64_t LazyReadCount(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT);
  }
  uint64_t LazyReadBytes(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_LAZY_READ_BYTES);
  }
  uint64_t LazyPartialReadCount(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_LAZY_PARTIAL_READ_COUNT);
  }
  uint64_t LazyPartialBytesSaved(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_LAZY_PARTIAL_BYTES_SAVED);
  }

  // Opens a DB with `options` on `fs`, ingests the embedded values "ek0"
  // (2000 bytes) and "ek1" (3000 bytes), and resolves the whole "data" column
  // of the entities of "ek0", "ek0" and "ek1" in one MultiResolve call. "ek0"
  // twice gives two entities that point to the same embedded record. Checks
  // every value, and that the SST reads were one MultiRead call with two
  // requests: the duplicate shares one of them. The statistics and the read
  // counts of `fs` are reset before MultiResolve. Returns the number of reads.
  size_t ResolveDuplicateEmbeddedRecord(Options& options,
                                        BlobReadIOActivityFS* fs) {
    DestroyAndReopen(options);
    const std::string value0(2000, 'a');
    const std::string value1(3000, 'b');
    IngestEmbeddedEntities(
        options, {{"ek0", WideColumns{{kDefaultWideColumnName, "inline"},
                                      {"data", value0}}},
                  {"ek1", WideColumns{{kDefaultWideColumnName, "inline"},
                                      {"data", value1}}}});

    const std::array<Slice, 3> key_slices{Slice("ek0"), Slice("ek0"),
                                          Slice("ek1")};
    const std::array<const std::string*, 3> expected{&value0, &value0, &value1};
    LazyWideColumnsBatch batch;
    std::array<Status, 3> get_statuses;
    db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                            key_slices.size(), key_slices.data(), &batch,
                            get_statuses.data());
    for (const Status& s : get_statuses) {
      EXPECT_OK(s);
    }
    EXPECT_EQ(batch.size(), key_slices.size());
    if (batch.size() != key_slices.size()) {
      return 0;
    }

    fs->ResetBlobReadCounts();
    EXPECT_OK(options.statistics->Reset());

    std::array<PinnableSlice, 3> results;
    std::array<Status, 3> statuses;
    std::array<LazyColumnReadRequest, 3> reads;
    for (size_t i = 0; i < reads.size(); ++i) {
      reads[i].column = &batch[i][1];  // embedded "data" column
      reads[i].result = &results[i];
      reads[i].status = &statuses[i];
    }
    EXPECT_OK(batch.MultiResolve(reads.size(), reads.data()));

    for (size_t i = 0; i < reads.size(); ++i) {
      SCOPED_TRACE("read " + std::to_string(i));
      EXPECT_OK(statuses[i]);
      EXPECT_EQ(results[i], *expected[i]);
    }
    EXPECT_EQ(fs->sst_multiread_overlap_count(), 0U);
    const auto calls = fs->sst_multiread_requests();
    EXPECT_EQ(calls.size(), 1U);
    if (!calls.empty()) {
      EXPECT_EQ(calls[0].size(), 2U);
    }
    return reads.size();
  }

  // Warms the blob-file reader for a column with a 1-byte partial read: opening
  // the blob file reads its header + footer, a one-time cost. Tests that assert
  // an exact BLOB_DB_BLOB_FILE_BYTES_READ delta call this and then reset stats,
  // so only the bytes of the measured read are counted.
  void WarmBlobFileReader(LazyWideColumns& lazy, size_t column_index) {
    PinnableSlice warm;
    ASSERT_OK(lazy.ResolveColumnRange(lazy[column_index], /*offset=*/0,
                                      /*length=*/1, &warm));
  }

  // Returns a value where each byte depends on its position, so a read from a
  // wrong offset returns different bytes. The pattern repeats every 251 bytes.
  // 251 is a prime, so the pattern does not repeat at the power-of-two
  // distances that block and page sizes use.
  static std::string PositionDependentValue(size_t size) {
    std::string value(size, '\0');
    for (size_t i = 0; i < size; ++i) {
      value[i] = static_cast<char>(i % 251);
    }
    return value;
  }

  // Writes an SST containing a single embedded (same-file blob) wide-column
  // entity and ingests it into the default column family. Columns at least
  // `min_blob_size` bytes become same-file blob references; smaller ones stay
  // inline. Used by the Phase 2 (embedded range read) tests.
  void IngestEmbeddedEntity(const Options& options, const std::string& key,
                            const WideColumns& columns,
                            uint64_t min_blob_size = 64) {
    const std::string sst_path = dbname_ + "/embedded_" + key + ".sst";
    SstFileWriterEmbeddedBlobOptions embedded_blob_options;
    embedded_blob_options.min_blob_size = min_blob_size;
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
    ASSERT_OK(writer.PutEntity(key, columns));
    ASSERT_OK(writer.Finish());
    ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));
  }

  // Writes a single SST holding several embedded (same-file blob) wide-column
  // entities (keys must be given in sorted order) and ingests it, so all their
  // embedded blob references live in one SST -- used to exercise cross-key
  // coalescing of embedded reads within a single SameFileBlobReader.
  void IngestEmbeddedEntities(
      const Options& options,
      const std::vector<std::pair<std::string, WideColumns>>& entities,
      uint64_t min_blob_size = 64) {
    const std::string sst_path = dbname_ + "/embedded_multi.sst";
    SstFileWriterEmbeddedBlobOptions embedded_blob_options;
    embedded_blob_options.min_blob_size = min_blob_size;
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
    for (const auto& entity : entities) {
      ASSERT_OK(writer.PutEntity(entity.first, entity.second));
    }
    ASSERT_OK(writer.Finish());
    ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));
  }
};

// Enumerate a lazily-read entity without triggering any blob I/O: inline
// columns expose their bytes immediately, blob columns show up as unresolved
// references with known (uncompressed) logical sizes, and no blob bytes are
// read until something is pulled.
TEST_F(DBLazyEntityTest, EnumerateColumnsWithoutResolvingBlobs) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";  // < min_blob_size: stays inline
  const std::string big1(200, 'a');    // >= min_blob_size: blob reference
  const std::string big2(300, 'b');    // >= min_blob_size: blob reference
  const WideColumns columns{
      {kDefaultWideColumnName, small}, {"attr1", big1}, {"attr2", big2}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  ASSERT_EQ(lazy.size(), 3U);

  // Column 0: the inline default column -- bytes available with no I/O.
  ASSERT_EQ(lazy[0].name(), kDefaultWideColumnName);
  ASSERT_FALSE(lazy[0].is_reference());
  ASSERT_EQ(*lazy[0].inline_value(), small);

  // Columns 1 and 2: unresolved blob references with known logical sizes
  // (uncompressed), reported without reading the blob payloads.
  ASSERT_EQ(lazy[1].name(), "attr1");
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_TRUE(lazy[1].logical_size().has_value());
  ASSERT_EQ(*lazy[1].logical_size(), big1.size());

  ASSERT_EQ(lazy[2].name(), "attr2");
  ASSERT_TRUE(lazy[2].is_reference());
  ASSERT_TRUE(lazy[2].logical_size().has_value());
  ASSERT_EQ(*lazy[2].logical_size(), big2.size());

  // Nothing was pulled, so no blob payload was read.
  ASSERT_EQ(BlobBytesRead(options), 0U);
}

// Pull a single byte range from one blob column; only that column's blob is
// read, and the returned slice is exactly the requested sub-span.
TEST_F(DBLazyEntityTest, ResolveSingleColumnRange) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  std::string big(200, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('0' + (i % 10));
  }
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Read the 50 bytes at offset 100 of the "data" column (index 1).
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/100,
                                    /*length=*/50, &range));
  ASSERT_EQ(range, Slice(big).ToString().substr(100, 50));

  // An offset at/past the end clamps to empty (not an error).
  PinnableSlice past_end;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/big.size(),
                                    /*length=*/10, &past_end));
  ASSERT_TRUE(past_end.empty());
}

// Phase 1: a byte-range read of an uncompressed separate-file blob reads only
// the requested bytes from storage (not the whole column, and not the record
// header/key), and returns exactly the requested sub-span.
TEST_F(DBLazyEntityTest, PartialReadReadsOnlyRequestedBytes) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  std::string big(4000, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('0' + (i % 10));
  }
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Warm the blob-file reader (opening it reads the file header + footer, a
  // one-time cost that would otherwise be counted below). Partial reads are not
  // cached, so the measured read still hits the file.
  WarmBlobFileReader(lazy, /*column_index=*/1);
  ASSERT_OK(options.statistics->Reset());

  constexpr uint64_t kOffset = 1000;
  constexpr size_t kLength = 100;
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], kOffset, kLength, &range));
  ASSERT_EQ(range, big.substr(kOffset, kLength));

  // Only the requested bytes were read from the blob file -- not the whole
  // 4000-byte column, and not the record header/key.
  ASSERT_EQ(BlobBytesRead(options), kLength);
}

// Phase 1: length == kLazyWholeColumn with a nonzero offset is a partial read
// from the offset to the end -- only the remaining bytes are read.
TEST_F(DBLazyEntityTest, PartialReadToEndFromOffset) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  std::string big(4000, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('0' + (i % 10));
  }
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Warm the blob-file reader (see PartialReadReadsOnlyRequestedBytes).
  WarmBlobFileReader(lazy, /*column_index=*/1);
  ASSERT_OK(options.statistics->Reset());

  constexpr uint64_t kOffset = 3000;
  PinnableSlice range;
  ASSERT_OK(
      lazy.ResolveColumnRange(lazy[1], kOffset, kLazyWholeColumn, &range));
  ASSERT_EQ(range, big.substr(kOffset));
  ASSERT_EQ(BlobBytesRead(options), big.size() - kOffset);
}

// Phase 1: a partial read never inserts into the blob cache (which is keyed by
// (file, offset) and holds the whole record). A subsequent whole-column read
// does populate the cache, confirming the cache is otherwise functional.
TEST_F(DBLazyEntityTest, PartialReadDoesNotFillBlobCache) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  // A partial read reads only the range and adds nothing to the cache.
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    PinnableSlice range;
    ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000,
                                      /*length=*/100, &range));
    ASSERT_EQ(range, big.substr(1000, 100));
  }
  ASSERT_EQ(BlobCacheAdds(options), 0U);

  // A whole-column read of the same column does populate the cache.
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    PinnableSlice whole;
    ASSERT_OK(lazy.ResolveColumn(lazy[1], &whole));
    ASSERT_EQ(whole, big);
  }
  ASSERT_EQ(BlobCacheAdds(options), 1U);
}

// Phase 1: when the whole value is already in the blob cache, a partial read is
// served by slicing the cached value -- no blob-file I/O.
TEST_F(DBLazyEntityTest, PartialReadServedFromCachedWholeValueDoesNoIO) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Warm the blob cache with a whole-column read.
  {
    LazyWideColumns warm;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &warm));
    PinnableSlice whole;
    ASSERT_OK(warm.ResolveColumn(warm[1], &whole));
    ASSERT_EQ(whole, big);
  }

  ASSERT_OK(options.statistics->Reset());

  // A fresh lazy read + partial read: the whole value is cached, so the range
  // is sliced out of the cache with no blob-file I/O.
  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000, /*length=*/100,
                                    &range));
  ASSERT_EQ(range, big.substr(1000, 100));
  ASSERT_EQ(BlobBytesRead(options), 0U);
}

// Phase 1: a byte-range read of a *compressed* blob cannot read a sub-range in
// isolation, so it resolves (and decompresses) the whole column and slices the
// range. Its logical size is unknown before resolution.
TEST_F(DBLazyEntityTest, CompressedColumnRangeResolvesWholeAndSlices) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not supported");
    return;
  }
  Options options = GetLazyTestOptions();
  options.blob_compression_type = kSnappyCompression;
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');  // highly compressible
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // A compressed reference reports an unknown logical size before resolution.
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_FALSE(lazy[1].logical_size().has_value());

  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000, /*length=*/100,
                                    &range));
  ASSERT_EQ(range, big.substr(1000, 100));
}

// Phase 1: force_verify on a partial read prioritizes checksum verification
// over I/O efficiency -- it reads (and checks) the whole record even though
// only a sub-range was requested.
TEST_F(DBLazyEntityTest, ForceVerifyPartialReadDoesFullVerifiedRead) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  PinnableSlice range;
  Status status;
  LazyColumnReadRequest read;
  read.column = &lazy[1];
  read.offset = 1000;
  read.length = 100;
  read.force_verify = true;
  read.result = &range;
  read.status = &status;
  ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_OK(status);
  ASSERT_EQ(range, big.substr(1000, 100));

  // A verified read covers the whole record (value + key + header), so strictly
  // more than the requested 100 bytes are read from the blob file.
  ASSERT_GT(BlobBytesRead(options), big.size());
}

// Phase 1: force_verify verifies the record's checksum even when
// ReadOptions::verify_checksums is off. With verification forced on, a
// corrupted blob record surfaces as Corruption; without force_verify the same
// partial read skips verification and succeeds.
TEST_F(DBLazyEntityTest, ForceVerifyVerifiesEvenWhenVerifyChecksumsOff) {
  Options options = GetLazyTestOptions();  // no blob cache: reads hit the file
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Corrupt the value bytes after they are read from the file, so a checksum
  // verification (if it runs) fails. Flip a byte in place to keep the record
  // size intact (a strict sub-range read of the requested bytes would not
  // trigger this on the whole-record path, which is the point).
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:TamperWithResult", [](void* arg) {
        Slice* const record_slice = static_cast<Slice*>(arg);
        ASSERT_NE(record_slice, nullptr);
        ASSERT_FALSE(record_slice->empty());
        char* const data = const_cast<char*>(record_slice->data());
        data[record_slice->size() - 1] ^= 0xff;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ro;
  ro.verify_checksums = false;  // globally off
  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ro, db_->DefaultColumnFamily(), key, &lazy));

  // With force_verify, the whole record is read and its checksum verified, so
  // the corruption is detected.
  PinnableSlice range;
  Status status;
  LazyColumnReadRequest read;
  read.column = &lazy[1];
  read.offset = 1000;
  read.length = 100;
  read.force_verify = true;
  read.result = &range;
  read.status = &status;
  ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Phase 2: a byte-range read of an uncompressed embedded (same-file) blob reads
// only the requested bytes from the SST -- the embedded counterpart of
// PartialReadReadsOnlyRequestedBytes.
TEST_F(DBLazyEntityTest, EmbeddedPartialReadReadsOnlyRequestedBytes) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string key = "entity";
  std::string big(4000, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('0' + (i % 10));
  }
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  IngestEmbeddedEntity(options, key, columns);

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // The blob column is an unresolved same-file reference with a known
  // (uncompressed) logical size.
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_TRUE(lazy[1].logical_size().has_value());
  ASSERT_EQ(*lazy[1].logical_size(), big.size());

  ASSERT_OK(options.statistics->Reset());

  constexpr uint64_t kOffset = 1000;
  constexpr size_t kLength = 100;
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], kOffset, kLength, &range));
  ASSERT_EQ(range, big.substr(kOffset, kLength));

  // Only the requested bytes were read from the SST's embedded blob region --
  // not the whole 4000-byte payload, and not the record trailer.
  ASSERT_EQ(BlobBytesRead(options), kLength);
}

// Phase 2: with the whole embedded payload already in the blob cache, a partial
// read slices the cached value with no disk I/O.
TEST_F(DBLazyEntityTest,
       EmbeddedPartialReadServedFromCachedWholeValueDoesNoIO) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  const std::string key = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  IngestEmbeddedEntity(options, key, columns);

  // Warm the blob cache with a whole-column read.
  {
    LazyWideColumns warm;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &warm));
    PinnableSlice whole;
    ASSERT_OK(warm.ResolveColumn(warm[1], &whole));
    ASSERT_EQ(whole, big);
  }

  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000, /*length=*/100,
                                    &range));
  ASSERT_EQ(range, big.substr(1000, 100));
  ASSERT_EQ(BlobBytesRead(options), 0U);
}

// Phase 2: force_verify on an embedded partial read forces the whole-record
// (verifying) path even when ReadOptions::verify_checksums is off -- so more
// than the requested bytes are read from the SST.
TEST_F(DBLazyEntityTest, EmbeddedForceVerifyReadsWholeRecordWithVerifyOff) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string key = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  IngestEmbeddedEntity(options, key, columns);

  ReadOptions ro;
  ro.verify_checksums = false;  // globally off
  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ro, db_->DefaultColumnFamily(), key, &lazy));

  ASSERT_OK(options.statistics->Reset());

  PinnableSlice range;
  Status status;
  LazyColumnReadRequest read;
  read.column = &lazy[1];
  read.offset = 1000;
  read.length = 100;
  read.force_verify = true;
  read.result = &range;
  read.status = &status;
  ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_OK(status);
  ASSERT_EQ(range, big.substr(1000, 100));

  // force_verify forces the whole embedded record (payload + trailer) to be
  // read and verified even though verify_checksums is off, so strictly more
  // than the requested 100 bytes are read.
  ASSERT_GT(BlobBytesRead(options), big.size());
}

// Phase 1/2 statistics: an actual partial read (separate-file) bumps the lazy
// partial-read tickers by the requested length and the bytes it avoided.
TEST_F(DBLazyEntityTest, PartialReadUpdatesLazyStats) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  constexpr uint64_t kOffset = 1000;
  constexpr size_t kLength = 100;
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], kOffset, kLength, &range));
  ASSERT_EQ(range, big.substr(kOffset, kLength));

  // A partial read is both a lazy read and a partial read.
  ASSERT_EQ(LazyReadCount(options), 1U);
  ASSERT_EQ(LazyReadBytes(options), kLength);
  ASSERT_EQ(LazyPartialReadCount(options), 1U);
  ASSERT_EQ(LazyPartialBytesSaved(options), big.size() - kLength);
}

// The same lazy stats are recorded for an embedded (same-file) partial read.
TEST_F(DBLazyEntityTest, EmbeddedPartialReadUpdatesLazyStats) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string key = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  IngestEmbeddedEntity(options, key, columns);
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  constexpr uint64_t kOffset = 1000;
  constexpr size_t kLength = 100;
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], kOffset, kLength, &range));
  ASSERT_EQ(range, big.substr(kOffset, kLength));

  ASSERT_EQ(LazyReadCount(options), 1U);
  ASSERT_EQ(LazyReadBytes(options), kLength);
  ASSERT_EQ(LazyPartialReadCount(options), 1U);
  ASSERT_EQ(LazyPartialBytesSaved(options), big.size() - kLength);
}

// Lazy read stats count every lazy storage read (whole-column and partial), but
// the partial tickers count only actual partial reads: a whole-column read and
// a force_verify (full verified) read bump the lazy-read tickers without the
// partial ones, and a cache-hit slice bumps neither (no storage read).
TEST_F(DBLazyEntityTest, LazyStatsCountReadsAndPartialsSeparately) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  // A whole-column read is a lazy read, but not a partial read.
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    PinnableSlice whole;
    ASSERT_OK(lazy.ResolveColumn(lazy[1], &whole));
  }
  ASSERT_EQ(LazyReadCount(options), 1U);
  ASSERT_GE(LazyReadBytes(options), big.size());  // whole record (+ header)
  ASSERT_EQ(LazyPartialReadCount(options), 0U);
  ASSERT_EQ(LazyPartialBytesSaved(options), 0U);

  // A force_verify partial request becomes a full verified read: a lazy read,
  // not a partial read. (Fresh column so the blob cache does not serve it.)
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "e2",
                           columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                                 "e2", &lazy));
    PinnableSlice v;
    Status s;
    LazyColumnReadRequest read;
    read.column = &lazy[1];
    read.offset = 1000;
    read.length = 100;
    read.force_verify = true;
    read.result = &v;
    read.status = &s;
    ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
    ASSERT_OK(s);
  }
  ASSERT_EQ(LazyReadCount(options), 1U);
  ASSERT_EQ(LazyPartialReadCount(options), 0U);

  // A partial read served from the blob cache does no storage read, so it
  // counts as neither a lazy read nor a partial read.
  ASSERT_OK(options.statistics->Reset());
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    PinnableSlice range;
    ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000, /*length=*/100,
                                      &range));
    ASSERT_EQ(range, big.substr(1000, 100));
  }
  ASSERT_EQ(LazyReadCount(options), 0U);
  ASSERT_EQ(LazyReadBytes(options), 0U);
  ASSERT_EQ(LazyPartialReadCount(options), 0U);
}

// A non-lazy read (ordinary GetEntity) does not touch the lazy read stats: the
// lazy tickers are gated on Env::IOActivity::kLazyResolve.
TEST_F(DBLazyEntityTest, NonLazyReadsDoNotUpdateLazyStats) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  // An ordinary (eager) GetEntity resolves the blob column via the normal path.
  PinnableWideColumns eager;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key, &eager));
  ASSERT_GT(BlobBytesRead(options), 0U);  // it did read the blob
  ASSERT_EQ(LazyReadCount(options), 0U);
  ASSERT_EQ(LazyReadBytes(options), 0U);
  ASSERT_EQ(LazyPartialReadCount(options), 0U);
}

// Deferred lazy resolve reads are attributed to Env::IOActivity::kLazyResolve,
// distinct from the kGetEntity of the initial entity read.
TEST_F(DBLazyEntityTest, LazyResolveReadsUseLazyResolveIOActivity) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  // Close the DB before the local Env is destroyed (even on an early ASSERT).
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptions();
  options.env = env.get();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // The initial entity read touched only the SST, not the .blob file.
  fs->ResetLastBlobReadIOActivity();

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(value, big);

  // The deferred blob-file read carried the lazy-resolve activity.
  ASSERT_EQ(fs->last_blob_read_io_activity(), Env::IOActivity::kLazyResolve);
}

// The headline example: resolve *many blobs in a single call*. One MultiResolve
// submits reads for several blob columns at once; the engine coalesces them
// (and, later, issues them asynchronously) under the hood, filling each
// request's out-params independently.
TEST_F(DBLazyEntityTest, MultiResolveResolvesManyBlobsInOneCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string v1(200, 'a');
  const std::string v2(250, 'b');
  const std::string v3(300, 'c');
  const WideColumns columns{
      {kDefaultWideColumnName, "inline"}, {"c1", v1}, {"c2", v2}, {"c3", v3}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Describe all three blob reads up front, then resolve them together in a
  // single call. Each read points at its own result/status out-params.
  std::array<PinnableSlice, 3> results;
  std::array<Status, 3> statuses;
  std::vector<LazyColumnReadRequest> reads(3);
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].column = &lazy[i + 1];  // columns 1..3 are the blob columns
    reads[i].offset = 0;
    reads[i].length = kLazyWholeColumn;
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(lazy.MultiResolve(reads));  // std::vector sugar overload

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], v1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], v2);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], v3);
}

// A single MultiResolve can also mix multiple ranges from one column with reads
// of other columns -- all coalesced into one pass.
TEST_F(DBLazyEntityTest, MultiResolveMixesRangesAndColumns) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string a(400, 'a');
  const std::string b(200, 'b');
  const WideColumns columns{
      {kDefaultWideColumnName, "inline"}, {"a", a}, {"b", b}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Two disjoint ranges of column "a" (index 1) plus the whole of column "b"
  // (index 2), resolved together.
  std::array<PinnableSlice, 3> results;
  std::array<Status, 3> statuses;
  std::array<LazyColumnReadRequest, 3> reads;

  reads[0] = LazyColumnReadRequest{&lazy[1],      /*offset=*/0,
                                   /*length=*/50, /*force_verify=*/false,
                                   &results[0],   &statuses[0]};
  reads[1] = LazyColumnReadRequest{&lazy[1],       /*offset=*/300,
                                   /*length=*/100, /*force_verify=*/false,
                                   &results[1],    &statuses[1]};
  reads[2] = LazyColumnReadRequest{&lazy[2],
                                   /*offset=*/0,
                                   kLazyWholeColumn,
                                   /*force_verify=*/false,
                                   &results[2],
                                   &statuses[2]};

  ASSERT_OK(lazy.MultiResolve(reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], a.substr(0, 50));
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], a.substr(300, 100));
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], b);
}

// The cross-key peer: MultiGetEntityLazy returns a LazyWideColumnsBatch (N
// per-key entities), and one LazyWideColumnsBatch::MultiResolve call resolves
// blobs across all of them. Each read references its entity by index, so it
// cannot span batches.
TEST_F(DBLazyEntityTest, BatchResolveAcrossKeysInOneCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string key1 = "k1";
  const std::string key2 = "k2";
  const std::string v1(200, 'a');
  const std::string v2(250, 'b');
  const WideColumns columns1{{kDefaultWideColumnName, "inline1"}, {"data", v1}};
  const WideColumns columns2{{kDefaultWideColumnName, "inline2"}, {"data", v2}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  const std::array<Slice, 2> keys{Slice(key1), Slice(key2)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> get_statuses;
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch,
                          get_statuses.data());
  ASSERT_OK(get_statuses[0]);
  ASSERT_OK(get_statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  // No blobs read yet -- only inline columns + references were materialized.
  ASSERT_EQ(BlobBytesRead(options), 0U);

  // One batch resolves the "data" blob column of both keys together. Each read
  // names its target column, which identifies the owning entity.
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;
  std::array<LazyColumnReadRequest, 2> reads;
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].column = &batch[i][1];  // "data" column of entity i
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], v1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], v2);
}

// Cross-key coalescing: N whole-column separate-file reads across keys (all in
// one blob file) are served by a single coalesced MultiRead, and the fetched
// values are cached (a repeat read does no further blob I/O).
TEST_F(DBLazyEntityTest, BatchCoalescesWholeColumnSeparateFileReads) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptions();
  options.env = env.get();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(2000 + i, static_cast<char>('a' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());  // one flush -> one blob file holding all N blobs

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }
  ASSERT_EQ(batch.size(), kNumKeys);

  // Warm the (shared) blob-file reader so the coalescing assertion below counts
  // only the value read, not the one-time header/footer open.
  PinnableSlice warm;
  ASSERT_OK(batch[0].ResolveColumnRange(batch[0][1], /*offset=*/0, /*length=*/1,
                                        &warm));
  fs->ResetBlobReadCounts();

  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    reads[i].column = &batch[i][1];  // "data" column of entity i
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
  // All N whole-column reads coalesced into one MultiRead over the blob file;
  // no per-blob Read calls.
  ASSERT_EQ(fs->blob_multiread_count(), 1U);
  ASSERT_EQ(fs->blob_read_count(), 0U);

  // The coalesced whole reads were cached: reading the same columns again does
  // no further blob I/O.
  fs->ResetBlobReadCounts();
  std::vector<PinnableSlice> results2(kNumKeys);
  std::vector<Status> statuses2(kNumKeys);
  std::vector<LazyColumnReadRequest> reads2(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    reads2[i].column = &batch[i][1];
    reads2[i].result = &results2[i];
    reads2[i].status = &statuses2[i];
  }
  ASSERT_OK(batch.MultiResolve(reads2.size(), reads2.data()));
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses2[i]);
    ASSERT_EQ(results2[i], values[i]);
  }
  ASSERT_EQ(fs->blob_multiread_count(), 0U);
  ASSERT_EQ(fs->blob_read_count(), 0U);
}

// Cross-key coalescing of byte-range (partial) separate-file reads: N sub-range
// reads across keys in one blob file become a single coalesced MultiRead, save
// the un-read bytes, and (being partial) never populate the blob cache.
TEST_F(DBLazyEntityTest, BatchCoalescesRangeSeparateFileReads) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptionsWithBlobCache();
  options.env = env.get();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  constexpr size_t kValueSize = 3000;
  constexpr size_t kRangeLen = 100;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(kValueSize, static_cast<char>('a' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }

  // Warm the shared blob-file reader (opening it reads header/footer).
  PinnableSlice warm;
  ASSERT_OK(batch[0].ResolveColumnRange(batch[0][1], /*offset=*/0, /*length=*/1,
                                        &warm));
  fs->ResetBlobReadCounts();
  // Reset stats after warming so the assertions below count only the coalesced
  // batch reads (not the 1-byte warm partial read).
  ASSERT_OK(options.statistics->Reset());
  const uint64_t cache_adds_before = BlobCacheAdds(options);

  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    reads[i].column = &batch[i][1];
    reads[i].offset = 0;
    reads[i].length = kRangeLen;
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i].substr(0, kRangeLen));
  }
  // All N range reads coalesced into a single MultiRead; no per-blob Reads.
  ASSERT_EQ(fs->blob_multiread_count(), 1U);
  ASSERT_EQ(fs->blob_read_count(), 0U);
  // Partial reads never fill the blob cache.
  ASSERT_EQ(BlobCacheAdds(options), cache_adds_before);
  // Bytes saved: each read fetched kRangeLen instead of the whole value.
  ASSERT_EQ(LazyPartialBytesSaved(options),
            kNumKeys * (kValueSize - kRangeLen));
}

// Cross-key coalescing of embedded (same-file) reads: several keys whose
// embedded blobs live in one SST are resolved by a single coalesced MultiRead
// over that SST.
TEST_F(DBLazyEntityTest, BatchCoalescesEmbeddedReads) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptions();
  options.env = env.get();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<std::pair<std::string, WideColumns>> entities;
  entities.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("ek" + std::to_string(i));
    values.emplace_back(2000 + i, static_cast<char>('a' + i));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    entities.emplace_back(
        keys[i],
        WideColumns{{kDefaultWideColumnName, "inline"}, {"data", values[i]}});
  }
  IngestEmbeddedEntities(options, entities);

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }
  ASSERT_EQ(batch.size(), kNumKeys);

  // Reset after the point lookups; the embedded records are read only now.
  fs->ResetBlobReadCounts();

  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    reads[i].column = &batch[i][1];  // embedded "data" column
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
  // All embedded records (same SST) coalesced into one MultiRead over the SST.
  ASSERT_EQ(fs->sst_multiread_count(), 1U);
}

// Regression test: the batch may present reads in an order that does not match
// ascending record offset. RandomAccessFileReader::MultiRead requires ascending
// offsets (it asserts, and in direct-I/O mode merges only consecutive
// requests), so the embedded batch reader must sort internally. Here the reads
// are issued in reverse key order (descending record offset); the values must
// still resolve correctly in one coalesced read.
TEST_F(DBLazyEntityTest, BatchCoalescesEmbeddedReadsOutOfOrder) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptions();
  options.env = env.get();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 4;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<std::pair<std::string, WideColumns>> entities;
  entities.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("ek" + std::to_string(i));
    values.emplace_back(2000 + i, static_cast<char>('a' + i));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    entities.emplace_back(
        keys[i],
        WideColumns{{kDefaultWideColumnName, "inline"}, {"data", values[i]}});
  }
  // Keys are written in ascending order, so their embedded records have
  // ascending file offsets.
  IngestEmbeddedEntities(options, entities);

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }

  fs->ResetBlobReadCounts();

  // Issue the reads in reverse key order, i.e. descending record offset, so the
  // batch reader receives them unsorted.
  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    const size_t key_index = kNumKeys - 1 - i;
    reads[i].column = &batch[key_index][1];  // embedded "data" column
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  for (size_t i = 0; i < kNumKeys; ++i) {
    const size_t key_index = kNumKeys - 1 - i;
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[key_index]);
  }
  // Still one coalesced MultiRead over the SST despite the unsorted input.
  ASSERT_EQ(fs->sst_multiread_count(), 1U);
}

// Tests of batched embedded range reads that check the requests each SST
// MultiRead call gets. FSRandomAccessFile::MultiRead requires requests that do
// not overlap, so MultiReadGen2 merges overlapping ranges into one request and
// sends all requests of a group in one call. Each test reads ranges of one
// embedded value of kValueSize bytes. The value bytes depend on their position,
// so a slice taken from a wrong place in a merged read gives wrong bytes.
class DBLazyEntityEmbeddedMultiReadTest : public DBLazyEntityTest {
 public:
  ~DBLazyEntityEmbeddedMultiReadTest() override {
    // The batch pins DB state, so release it before the DB closes, and close
    // the DB before recording_env_ and fs_ go away.
    batch_.Reset();
    Close();
  }

 protected:
  // Large enough for the ranges of the tests below, which end at or before
  // offset 4000.
  static constexpr size_t kValueSize = 4000;

  // Opens a DB on the recording file system, ingests one entity whose "data"
  // column is an embedded value, loads it into batch_ and resets the counters.
  // With `use_direct_reads`, the SST is read with direct I/O.
  void OpenAndLoadEntity(bool use_direct_reads) {
    fs_ = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
    recording_env_ = NewCompositeEnv(fs_);
    options_ = GetLazyTestOptions();
    options_.env = recording_env_.get();
    options_.use_direct_reads = use_direct_reads;
    DestroyAndReopen(options_);

    value_ = PositionDependentValue(kValueSize);
    IngestEmbeddedEntity(
        options_, "entity",
        WideColumns{{kDefaultWideColumnName, "inline"}, {"data", value_}});

    const std::array<Slice, 1> key_slices{Slice("entity")};
    std::array<Status, 1> get_statuses;
    db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                            key_slices.size(), key_slices.data(), &batch_,
                            get_statuses.data());
    ASSERT_OK(get_statuses[0]);
    ASSERT_EQ(batch_.size(), 1U);

    fs_->ResetBlobReadCounts();
    ASSERT_OK(options_.statistics->Reset());
  }

  // Reads the (offset, length) `ranges` of the "data" column in one
  // MultiResolve call. Fills results_ and statuses_ and returns the dispatch
  // status.
  Status ResolveRanges(const std::vector<std::pair<uint64_t, size_t>>& ranges) {
    results_ = std::vector<PinnableSlice>(ranges.size());
    statuses_ = std::vector<Status>(ranges.size());
    std::vector<LazyColumnReadRequest> reads(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
      reads[i].column = &batch_[0][1];  // embedded "data" column
      reads[i].offset = ranges[i].first;
      reads[i].length = ranges[i].second;
      reads[i].result = &results_[i];
      reads[i].status = &statuses_[i];
    }
    return batch_.MultiResolve(reads.size(), reads.data());
  }

  // Checks that every range read OK and got its bytes of the value.
  void ExpectAllRangesRead(
      const std::vector<std::pair<uint64_t, size_t>>& ranges) {
    ASSERT_EQ(statuses_.size(), ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
      SCOPED_TRACE("range " + std::to_string(i));
      ASSERT_OK(statuses_[i]);
      ASSERT_EQ(results_[i], value_.substr(ranges[i].first, ranges[i].second));
    }
  }

  // Checks that the SST reads were one MultiRead call with requests of these
  // (offset, length) pairs. The offsets are relative to the first request,
  // because the file offset of the value depends on the SST layout.
  void ExpectOneCall(
      const std::vector<std::pair<uint64_t, size_t>>& relative_requests) {
    const auto calls = fs_->sst_multiread_requests();
    ASSERT_EQ(calls.size(), 1U);
    ASSERT_EQ(fs_->sst_multiread_overlap_count(), 0U);
    const auto& requests = calls[0];
    ASSERT_EQ(requests.size(), relative_requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
      SCOPED_TRACE("request " + std::to_string(i));
      ASSERT_EQ(requests[i].first - requests[0].first,
                relative_requests[i].first);
      ASSERT_EQ(requests[i].second, relative_requests[i].second);
    }
  }

  // Opens the DB's only SST file for direct calls of the batch readers, which
  // MultiResolve cannot reach for some inputs (for example, empty ranges).
  // Also sets sst_name_.
  std::unique_ptr<RandomAccessFileReader> OpenSstReader() {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    EXPECT_EQ(files.size(), 1U);
    sst_name_ = files[0].directory + "/" + files[0].relative_filename;
    std::unique_ptr<FSRandomAccessFile> file;
    EXPECT_OK(fs_->NewRandomAccessFile(sst_name_, FileOptions(), &file,
                                       /*dbg=*/nullptr));
    return std::make_unique<RandomAccessFileReader>(std::move(file), sst_name_);
  }

  std::shared_ptr<BlobReadIOActivityFS> fs_;
  // The Env on fs_. DBTestBase::env_ stays the Env of the base fixture.
  std::unique_ptr<Env> recording_env_;
  Options options_;
  std::string sst_name_;
  std::string value_;
  LazyWideColumnsBatch batch_;
  std::vector<PinnableSlice> results_;
  std::vector<Status> statuses_;
};

// An exact duplicate, a partial overlap and two disjoint ranges of one value:
// - [0, 100) twice and [50, 150) merge into one request [0, 150).
// - [1000, 1200) and [3000, 3010) overlap nothing, so each is its own request.
// All three requests go in one MultiRead call.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, OverlappingRangesMergeIntoOneRead) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  const std::vector<std::pair<uint64_t, size_t>> ranges{
      {0, 100}, {0, 100}, {50, 100}, {1000, 200}, {3000, 10}};
  ASSERT_OK(ResolveRanges(ranges));

  ASSERT_NO_FATAL_FAILURE(ExpectAllRangesRead(ranges));
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 150}, {1000, 200}, {3000, 10}}));
  // Read statistics count each range, so the ranges that share a read count
  // too.
  ASSERT_EQ(LazyReadCount(options_), ranges.size());
  ASSERT_EQ(BlobBytesRead(options_), 100 + 100 + 100 + 200 + 10);
}

// Buffered reads of 128 distinct prefixes [0, 1), [0, 2), ..., [0, 128) of one
// value. They are more than the 32 entries that the lists in MultiReadGen2 hold
// without a heap allocation, and they all overlap, so they merge into one
// request [0, 128). The test also reads [1000, 1010), which overlaps nothing,
// to check that the merged request and an unrelated request share the call.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, StrictPrefixesShareOneRead) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  constexpr size_t kNumPrefixes = 128;
  std::vector<std::pair<uint64_t, size_t>> ranges;
  ranges.reserve(kNumPrefixes + 1);
  for (size_t length = 1; length <= kNumPrefixes; ++length) {
    ranges.emplace_back(0, length);
  }
  ranges.emplace_back(1000, 10);
  ASSERT_OK(ResolveRanges(ranges));

  ASSERT_NO_FATAL_FAILURE(ExpectAllRangesRead(ranges));
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, kNumPrefixes}, {1000, 10}}));
}

// Ranges that do not overlap go to one MultiRead, however many there are. This
// test reads 40 ranges of one value, more than the 32 entries that the lists in
// MultiReadGen2 hold without a heap allocation.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, ManyDisjointRangesUseOneMultiRead) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  // Range i, for i = 0, 1, ..., 39, is [i * 50, i * 50 + 20). The ranges start
  // 50 bytes apart and are 20 bytes long, so a 30-byte gap separates each
  // range from the next, and no two ranges overlap or touch.
  constexpr size_t kNumRanges = 40;
  std::vector<std::pair<uint64_t, size_t>> ranges;
  ranges.reserve(kNumRanges);
  for (size_t i = 0; i < kNumRanges; ++i) {
    ranges.emplace_back(i * 50, 20);
  }
  ASSERT_OK(ResolveRanges(ranges));

  ASSERT_NO_FATAL_FAILURE(ExpectAllRangesRead(ranges));
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall(ranges));
}

// Direct I/O. The reader reads aligned pages into its own buffer, so each
// range copies its slice from there, also from a merged read. The ranges merge
// into five reads: [0, 150) (from [0, 100) twice, [50, 150) and [20, 30)),
// [1000, 1200) (from [1000, 1200) and [1100, 1150)), [2000, 2064) (from 64
// prefixes), [3000, 3010) and [3990, 4000). All of them go in one MultiRead
// call. The reader aligns and merges them before the file system sees them, so
// the test checks the five reads at the MultiReadGen2:Requests sync point, and
// that the file system gets one call with no overlapping request.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, DirectReadsCopyMergedSlices) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct I/O not supported");
    return;
  }
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/true));

  std::vector<std::pair<uint64_t, size_t>> ranges{
      {0, 100},    {0, 100},   {50, 100},  {20, 10},
      {1000, 200}, {1100, 50}, {3000, 10}, {3990, 10}};
  for (size_t length = 1; length <= 64; ++length) {
    ranges.emplace_back(2000, length);
  }
  std::vector<std::pair<uint64_t, size_t>> reads;
  SyncPoint::GetInstance()->SetCallBack(
      "MultiReadGen2:Requests", [&](void* arg) {
        for (const FSReadRequest& req :
             *static_cast<std::vector<FSReadRequest>*>(arg)) {
          reads.emplace_back(req.offset, req.len);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer clear_sync_point([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(ResolveRanges(ranges));

  ASSERT_NO_FATAL_FAILURE(ExpectAllRangesRead(ranges));
  // The offsets are relative to the first read, as in ExpectOneCall.
  const std::vector<std::pair<uint64_t, size_t>> expected_reads{
      {0, 150}, {1000, 200}, {2000, 64}, {3000, 10}, {3990, 10}};
  ASSERT_EQ(reads.size(), expected_reads.size());
  for (size_t i = 0; i < reads.size(); ++i) {
    SCOPED_TRACE("read " + std::to_string(i));
    ASSERT_EQ(reads[i].first - reads[0].first, expected_reads[i].first);
    ASSERT_EQ(reads[i].second, expected_reads[i].second);
  }
  ASSERT_EQ(fs_->sst_multiread_requests().size(), 1U);
  ASSERT_EQ(fs_->sst_multiread_overlap_count(), 0U);
}

// Buffered reads. When one range of a merged request covers the whole request,
// the request reads straight into that range's buffer, and the other ranges
// copy from it. Only partial overlaps need a scratch buffer. The requests are:
// - [0, 40) from the nested prefixes [0, 1), [0, 2), ..., [0, 40). [0, 40)
//   covers the request.
// - [500, 600) from [500, 600) twice and [520, 530). Either copy of [500, 600)
//   covers the request.
// - [1000, 1010), a range of its own.
// - [2000, 2150) from [2000, 2100) and [2050, 2150). No range covers it, so it
//   reads into scratch.
// Each range result keeps the buffer it was read into, so the test compares
// the scratch pointer of each request with the result buffers.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, CoveringRangeReadsIntoItsBuffer) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  std::vector<std::pair<uint64_t, size_t>> ranges{
      {500, 100}, {520, 10}, {500, 100}, {1000, 10}, {2000, 100}, {2050, 100}};
  constexpr size_t kNumPrefixes = 40;
  for (size_t length = 1; length <= kNumPrefixes; ++length) {
    ranges.emplace_back(0, length);
  }
  ASSERT_OK(ResolveRanges(ranges));

  ASSERT_NO_FATAL_FAILURE(ExpectAllRangesRead(ranges));
  ASSERT_NO_FATAL_FAILURE(
      ExpectOneCall({{0, kNumPrefixes}, {500, 100}, {1000, 10}, {2000, 150}}));
  const auto scratches = fs_->sst_multiread_scratches()[0];
  ASSERT_EQ(scratches[0], results_.back().data());  // [0, 40)
  ASSERT_TRUE(scratches[1] == results_[0].data() ||
              scratches[1] == results_[2].data());  // [500, 600)
  ASSERT_EQ(scratches[2], results_[3].data());      // [1000, 1010)
  for (const PinnableSlice& result : results_) {
    ASSERT_NE(scratches[3], result.data());  // [2000, 2150)
  }
}

// A batch of only empty ranges needs no bytes, so ReadSimpleGen2BlobRanges
// gives each range OK and makes no MultiRead call. MultiResolve answers empty
// ranges before it reaches this reader, so the test calls the reader directly
// on the SST file.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, AllEmptyRangesMakeNoCall) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));
  const std::unique_ptr<RandomAccessFileReader> reader = OpenSstReader();

  // Two empty ranges, at payload offsets 0 and 50, of a 100-byte payload at
  // file offset 0. The values are examples: no byte is read.
  char buf[1];
  std::array<Status, 2> statuses{Status::Incomplete(), Status::Incomplete()};
  std::array<SimpleGen2RangeReadRequest, 2> reqs;
  for (size_t i = 0; i < reqs.size(); ++i) {
    reqs[i].payload_size = 100;
    reqs[i].range_offset = i * 50;
    reqs[i].range_length = 0;
    reqs[i].buf = buf;
    reqs[i].status = &statuses[i];
  }
  ReadSimpleGen2BlobRanges(ReadOptions(), reader.get(), reqs.size(),
                           reqs.data());

  for (const Status& s : statuses) {
    ASSERT_OK(s);
  }
  ASSERT_TRUE(fs_->sst_multiread_requests().empty());
}

// An empty range whose offset lies inside a merged read needs no bytes, so it
// gets OK even when that read fails. The test reads [0, 100), an empty range
// at offset 50, [50, 150) and [500, 510) of a 1000-byte payload at file offset
// 0 of the SST. The values are examples. The empty range sorts between [0, 100)
// and [50, 150), inside the index range of their merged read [0, 150).
// [500, 510) is a second read, so the batch always uses MultiRead. MultiResolve
// answers empty ranges before it reaches this reader, so the test calls the
// reader directly.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, EmptyRangeInsideMergedReadGetsOK) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));
  const std::unique_ptr<RandomAccessFileReader> reader = OpenSstReader();
  std::string file_bytes;
  ASSERT_OK(ReadFileToString(env_, sst_name_, &file_bytes));
  ASSERT_GE(file_bytes.size(), 510U);

  for (const bool fail_read : {false, true}) {
    SCOPED_TRACE(fail_read ? "failed read" : "good read");
    fs_->ResetBlobReadCounts();
    const std::array<std::pair<uint64_t, size_t>, 4> ranges{
        {{0, 100}, {50, 0}, {50, 100}, {500, 10}}};
    std::array<std::string, 4> bufs{std::string(100, 'x'), std::string(1, 'x'),
                                    std::string(100, 'x'),
                                    std::string(10, 'x')};
    std::array<Status, 4> statuses;
    std::array<SimpleGen2RangeReadRequest, 4> reqs;
    for (size_t i = 0; i < reqs.size(); ++i) {
      reqs[i].payload_size = 1000;
      reqs[i].range_offset = ranges[i].first;
      reqs[i].range_length = ranges[i].second;
      reqs[i].buf = bufs[i].data();
      reqs[i].status = &statuses[i];
    }
    fs_->SetFailSstReadRequest(fail_read ? 0 : -1);
    ReadSimpleGen2BlobRanges(ReadOptions(), reader.get(), reqs.size(),
                             reqs.data());
    fs_->SetFailSstReadRequest(-1);

    ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 150}, {500, 10}}));
    ASSERT_OK(statuses[1]);
    ASSERT_EQ(bufs[1], "x");  // no byte written
    ASSERT_OK(statuses[3]);
    ASSERT_EQ(bufs[3], file_bytes.substr(500, 10));
    for (size_t i : {0, 2}) {
      SCOPED_TRACE("range " + std::to_string(i));
      if (fail_read) {
        ASSERT_EQ(statuses[i].code(), Status::kIOError)
            << statuses[i].ToString();
      } else {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(bufs[i], file_bytes.substr(ranges[i].first, 100));
      }
    }
  }
}

// A range whose end offset does not fit in 64 bits gets Corruption that names
// the file and the offset, and the other ranges of the batch still read. The
// test calls the reader directly, because a valid blob index cannot give such
// an offset. The two other ranges are two reads, so the batch always uses
// MultiRead.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, RangeEndOverflowGetsCorruption) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));
  const std::unique_ptr<RandomAccessFileReader> reader = OpenSstReader();

  // Range 0: a 100-byte payload at file offset UINT64_MAX - 50, so the range
  // end passes UINT64_MAX. Ranges 1 and 2: [0, 10) and [100, 110) of a 200-byte
  // payload at file offset 0.
  constexpr uint64_t kOverflowOffset =
      std::numeric_limits<uint64_t>::max() - 50;
  std::array<std::string, 3> bufs{std::string(100, 'x'), std::string(10, 'x'),
                                  std::string(10, 'x')};
  std::array<Status, 3> statuses;
  std::array<SimpleGen2RangeReadRequest, 3> reqs;
  reqs[0].record_offset = kOverflowOffset;
  reqs[0].payload_size = 100;
  reqs[0].range_length = 100;
  reqs[1].payload_size = 200;
  reqs[1].range_length = 10;
  reqs[2].payload_size = 200;
  reqs[2].range_offset = 100;
  reqs[2].range_length = 10;
  for (size_t i = 0; i < reqs.size(); ++i) {
    reqs[i].buf = bufs[i].data();
    reqs[i].status = &statuses[i];
  }
  ReadSimpleGen2BlobRanges(ReadOptions(), reader.get(), reqs.size(),
                           reqs.data());

  ASSERT_EQ(statuses[0].code(), Status::kCorruption) << statuses[0].ToString();
  ASSERT_EQ(statuses[0].ToString(), "Corruption: Blob read range overflow in " +
                                        sst_name_ + " at offset " +
                                        std::to_string(kOverflowOffset));
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 10}, {100, 10}}));
}

// Errors. [0, 100) twice and [50, 150) merge into one request, and
// [1000, 1100) is a second request in the same call.
// - When only the merged request fails, all three of its ranges get the error,
//   and [1000, 1100) keeps its bytes.
// - When the whole call fails, every range gets the error.
// In both cases the dispatch status of MultiResolve is OK, because the errors
// are per-read results.
TEST_F(DBLazyEntityEmbeddedMultiReadTest, MergedReadErrorGoesToAllMembers) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  const std::vector<std::pair<uint64_t, size_t>> ranges{
      {0, 100}, {0, 100}, {50, 100}, {1000, 100}};

  fs_->SetFailSstReadRequest(0);
  ASSERT_OK(ResolveRanges(ranges));
  fs_->SetFailSstReadRequest(-1);
  for (size_t i = 0; i < 3; ++i) {
    SCOPED_TRACE("range " + std::to_string(i));
    ASSERT_EQ(statuses_[i].code(), Status::kIOError) << statuses_[i].ToString();
  }
  ASSERT_OK(statuses_[3]);
  ASSERT_EQ(results_[3], value_.substr(1000, 100));
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 150}, {1000, 100}}));

  fs_->ResetBlobReadCounts();
  fs_->SetFailSstMultiRead(true);
  ASSERT_OK(ResolveRanges(ranges));
  fs_->SetFailSstMultiRead(false);
  for (size_t i = 0; i < ranges.size(); ++i) {
    SCOPED_TRACE("range " + std::to_string(i));
    ASSERT_EQ(statuses_[i].code(), Status::kIOError) << statuses_[i].ToString();
  }
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 150}, {1000, 100}}));
}

// Short read. [0, 100) twice, [20, 30) and [50, 150) merge into one request
// [0, 150), and [1000, 1100) is a second request. The merged request returns
// 40 bytes less than asked, so only [0, 110) arrives:
// - [0, 100) twice and [20, 30) lie inside the returned bytes and get OK.
// - [50, 150) reaches past the end and gets Corruption.
// - [1000, 1100) is not affected.
TEST_F(DBLazyEntityEmbeddedMultiReadTest,
       ShortMergedReadFailsOnlyMembersPastEnd) {
  ASSERT_NO_FATAL_FAILURE(OpenAndLoadEntity(/*use_direct_reads=*/false));

  const std::vector<std::pair<uint64_t, size_t>> ranges{
      {0, 100}, {0, 100}, {20, 10}, {50, 100}, {1000, 100}};
  fs_->SetShortSstReadRequest(0, /*missing_bytes=*/40);
  ASSERT_OK(ResolveRanges(ranges));
  fs_->SetShortSstReadRequest(-1, 0);

  for (size_t i : std::vector<size_t>{0, 1, 2, 4}) {
    SCOPED_TRACE("range " + std::to_string(i));
    ASSERT_OK(statuses_[i]);
    ASSERT_EQ(results_[i], value_.substr(ranges[i].first, ranges[i].second));
  }
  ASSERT_EQ(statuses_[3].code(), Status::kCorruption)
      << statuses_[3].ToString();
  ASSERT_NO_FATAL_FAILURE(ExpectOneCall({{0, 150}, {1000, 100}}));
}

// Whole embedded reads: two entities of the same key point to the same record.
// The two requests for that record share one file read, so the MultiRead gets
// no duplicate.
TEST_F(DBLazyEntityTest, BatchEmbeddedDuplicateRecordReadOnce) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  Options options = GetLazyTestOptions();
  options.env = env.get();
  const size_t num_reads = ResolveDuplicateEmbeddedRecord(options, fs.get());
  // Read statistics count each request, so the duplicate counts too.
  ASSERT_EQ(LazyReadCount(options), num_reads);
}

// Whole embedded reads fill the blob cache through its memory allocator (for
// example, one that keeps blob bytes out of core dumps). The duplicate record
// must also be read into a buffer from that allocator, not into a plain heap
// buffer. The test allocator zero-fills its blocks, so the read buffers are
// initialized before the read. A hook checks, during the MultiRead call, that
// each request reads into a block of the allocator.
TEST_F(DBLazyEntityTest, BatchEmbeddedDuplicateRecordUsesCacheAllocator) {
  auto fs = std::make_shared<BlobReadIOActivityFS>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fs));
  Defer close_db_on_exit([this]() { Close(); });

  auto allocator = std::make_shared<TrackingMemoryAllocator>();
  Options options = GetLazyTestOptions();
  options.env = env.get();
  LRUCacheOptions co;
  co.capacity = 8 << 20;  // 8 MB, room for both values
  co.memory_allocator = allocator;
  options.blob_cache = NewLRUCache(co);

  size_t num_checked = 0;
  // The hook uses the locals above, so clear it before they go away, also on
  // an early return.
  Defer clear_hook([&fs]() { fs->SetSstMultiReadHook(nullptr); });
  fs->SetSstMultiReadHook([&](const FSReadRequest* reqs, size_t num_reqs) {
    for (size_t i = 0; i < num_reqs; ++i) {
      EXPECT_TRUE(allocator->Owns(reqs[i].scratch, reqs[i].len))
          << "request " << i;
      ++num_checked;
    }
  });
  ASSERT_NO_FATAL_FAILURE(ResolveDuplicateEmbeddedRecord(options, fs.get()));
  ASSERT_EQ(num_checked, 2U);
}

// A single batch MultiResolve mixing an inline column (served immediately), a
// whole blob read, and a byte-range blob read across keys returns the right
// bytes for each.
TEST_F(DBLazyEntityTest, BatchMixedResolveAcrossKeys) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string v0(2000, 'a');
  const std::string v1(2500, 'b');
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "k0",
                     {{kDefaultWideColumnName, "inline0"}, {"data", v0}}));
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "k1",
                     {{kDefaultWideColumnName, "inline1"}, {"data", v1}}));
  ASSERT_OK(Flush());

  const std::array<Slice, 2> keys{Slice("k0"), Slice("k1")};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> get_statuses;
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch,
                          get_statuses.data());
  ASSERT_OK(get_statuses[0]);
  ASSERT_OK(get_statuses[1]);

  // Reads: k0 inline default column, k0 whole "data", k1 "data" range [10,60).
  std::array<PinnableSlice, 3> results;
  std::array<Status, 3> statuses;
  std::array<LazyColumnReadRequest, 3> reads;
  reads[0].column = &batch[0][0];  // inline default column
  reads[0].result = &results[0];
  reads[0].status = &statuses[0];
  reads[1].column = &batch[0][1];  // k0 whole "data"
  reads[1].result = &results[1];
  reads[1].status = &statuses[1];
  reads[2].column = &batch[1][1];  // k1 "data" range
  reads[2].offset = 10;
  reads[2].length = 50;
  reads[2].result = &results[2];
  reads[2].status = &statuses[2];

  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], "inline0");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], v0);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], v1.substr(10, 50));
}

// Batched MultiGetEntityLazy + one cross-key MultiResolve returns the same
// bytes as resolving each key with a separate GetEntityLazy.
TEST_F(DBLazyEntityTest, BatchedResolveMatchesPerKey) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 4;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(1500 + 37 * i, static_cast<char>('a' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());

  // Per-key reference via separate GetEntityLazy calls.
  for (size_t i = 0; i < kNumKeys; ++i) {
    LazyWideColumns single;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                                 keys[i], &single));
    PinnableSlice ref;
    ASSERT_OK(single.ResolveColumnRange(single[1], /*offset=*/5, /*length=*/200,
                                        &ref));
    ASSERT_EQ(ref, values[i].substr(5, 200));
  }

  // Batched path.
  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
    reads[i].column = &batch[i][1];
    reads[i].offset = 5;
    reads[i].length = 200;
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i].substr(5, 200));
  }
}

// Regression test: a single blob file can hold more references than the
// MultiGet batch limit (MultiGetContext::MAX_BATCH_SIZE).
// Version::MultiGetBlobLazy must split per-file requests into batches no larger
// than that limit, since the lower-level BlobFileReader::MultiGetBlob[Range]
// asserts it and BlobSource tracks cache hits in a 64-bit mask. Here all keys
// are flushed into one blob file and resolved (whole, then byte-range) in one
// batch each; kNumKeys exceeds both 32 and 64.
TEST_F(DBLazyEntityTest, BatchManyKeysInOneBlobFileExceedsBatchLimit) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 70;  // > MAX_BATCH_SIZE (32) and > 64
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(kNumKeys);
  values.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(1000 + i, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());  // one flush -> one blob file holding all kNumKeys blobs

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }
  ASSERT_EQ(batch.size(), kNumKeys);

  // Whole-column reads of every key in one batch.
  {
    std::vector<PinnableSlice> results(kNumKeys);
    std::vector<Status> statuses(kNumKeys);
    std::vector<LazyColumnReadRequest> reads(kNumKeys);
    for (size_t i = 0; i < kNumKeys; ++i) {
      reads[i].column = &batch[i][1];
      reads[i].result = &results[i];
      reads[i].status = &statuses[i];
    }
    ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i]);
    }
  }

  // Byte-range reads of every key in one batch.
  {
    std::vector<PinnableSlice> results(kNumKeys);
    std::vector<Status> statuses(kNumKeys);
    std::vector<LazyColumnReadRequest> reads(kNumKeys);
    for (size_t i = 0; i < kNumKeys; ++i) {
      reads[i].column = &batch[i][1];
      reads[i].offset = 10;
      reads[i].length = 50;
      reads[i].result = &results[i];
      reads[i].status = &statuses[i];
    }
    ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i].substr(10, 50));
    }
  }
}

// Regression test: a force_verify read must not be silently skipped when the
// same column is also read (whole, unverified) in the same batch. Coalescing
// must not adopt unverified bytes into the resolver cache ahead of the
// force_verify read. A corrupted record is detected because the force_verify
// read runs a full verified read rather than slicing the coalesced cache entry.
TEST_F(DBLazyEntityTest, BatchForceVerifyNotSkippedByCoalescedRead) {
  Options options = GetLazyTestOptions();  // no blob cache: reads hit the file
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Corrupt the value bytes after they are read from the file so a checksum
  // verification (if it runs) fails.
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:TamperWithResult", [](void* arg) {
        Slice* const record_slice = static_cast<Slice*>(arg);
        ASSERT_NE(record_slice, nullptr);
        ASSERT_FALSE(record_slice->empty());
        char* const data = const_cast<char*>(record_slice->data());
        data[record_slice->size() - 1] ^= 0xff;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ro;
  ro.verify_checksums = false;  // globally off; force_verify overrides per-read
  const std::array<Slice, 1> key_slices{Slice(key)};
  LazyWideColumnsBatch batch;
  std::array<Status, 1> get_statuses;
  db_->MultiGetEntityLazy(ro, db_->DefaultColumnFamily(), key_slices.size(),
                          key_slices.data(), &batch, get_statuses.data());
  ASSERT_OK(get_statuses[0]);

  // reads[0] forces verification of the "data" column; reads[1] reads the same
  // column whole without verification. reads[1] must not cause reads[0] to be
  // served from an unverified coalesced cache entry.
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;
  std::array<LazyColumnReadRequest, 2> reads;
  reads[0].column = &batch[0][1];
  reads[0].force_verify = true;
  reads[0].result = &results[0];
  reads[0].status = &statuses[0];
  reads[1].column = &batch[0][1];  // same column, whole, unverified
  reads[1].result = &results[1];
  reads[1].status = &statuses[1];
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  // The force_verify read detected the corruption.
  ASSERT_TRUE(statuses[0].IsCorruption()) << statuses[0].ToString();
  // The unverified whole read of the same column runs no verification, so it
  // still succeeds (returning the unverified bytes) rather than surfacing the
  // corruption.
  ASSERT_OK(statuses[1]);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Regression test: a force_verify read must not be served from the shared blob
// cache after a non-verifying coalesced read of a *duplicate key* filled it.
// The two keys are distinct entities that resolve to the same physical blob
// record and blob-cache key, so the non-verifying whole fetch must not run
// (caching unverified bytes) ahead of the force_verify read. Requires a blob
// cache (that is the shared state at risk); without one each entity's resolver
// is independent and the reordering is harmless.
TEST_F(DBLazyEntityTest, BatchForceVerifyNotSkippedAcrossDuplicateKeys) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Corrupt the value bytes on each file read so a checksum verification (if it
  // runs) fails; a cache hit that skips verification would not surface it.
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:TamperWithResult", [](void* arg) {
        Slice* const record_slice = static_cast<Slice*>(arg);
        ASSERT_NE(record_slice, nullptr);
        ASSERT_FALSE(record_slice->empty());
        char* const data = const_cast<char*>(record_slice->data());
        data[record_slice->size() - 1] ^= 0xff;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ro;
  ro.verify_checksums = false;  // globally off; force_verify overrides per-read
  // Duplicate keys: two distinct entities that resolve to the same physical
  // blob record and cache key.
  const std::array<Slice, 2> key_slices{Slice(key), Slice(key)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> get_statuses;
  db_->MultiGetEntityLazy(ro, db_->DefaultColumnFamily(), key_slices.size(),
                          key_slices.data(), &batch, get_statuses.data());
  ASSERT_OK(get_statuses[0]);
  ASSERT_OK(get_statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  // reads[0] force-verifies entity 0's "data"; reads[1] reads entity 1's "data"
  // whole without verification. The duplicate key shares the physical record,
  // so reads[1] must not coalesce a non-verifying fetch that caches unverified
  // bytes ahead of reads[0].
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;
  std::array<LazyColumnReadRequest, 2> reads;
  reads[0].column = &batch[0][1];
  reads[0].force_verify = true;
  reads[0].result = &results[0];
  reads[0].status = &statuses[0];
  reads[1].column = &batch[1][1];  // duplicate key's same column, unverified
  reads[1].result = &results[1];
  reads[1].status = &statuses[1];
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  // The force_verify read ran a real verified read and caught the corruption
  // rather than being served unverified bytes from the shared blob cache.
  ASSERT_TRUE(statuses[0].IsCorruption()) << statuses[0].ToString();
  // The unverified whole read runs no verification, so it still succeeds
  // (returning unverified bytes).
  ASSERT_OK(statuses[1]);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Two reads of the same embedded record in one batch share one file read, which
// is a plain Read because the batch needs only one file read. The test counts
// the plain reads at the MultiReadGen2:SingleRead sync point. The read
// statistics still count each of the two reads with its own bytes, for whole
// values and for ranges.
TEST_F(DBLazyEntityTest, BatchEmbeddedDuplicateReadCountsEach) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);
  size_t single_reads = 0;
  SyncPoint::GetInstance()->SetCallBack("MultiReadGen2:SingleRead",
                                        [&](void* /*arg*/) { ++single_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer clear_sync_point([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  const std::string value(2000, 'd');
  IngestEmbeddedEntity(
      options, "ek",
      WideColumns{{kDefaultWideColumnName, "inline"}, {"data", value}});
  const std::array<Slice, 2> key_slices{Slice("ek"), Slice("ek")};
  for (const bool range : {false, true}) {
    SCOPED_TRACE(range);
    LazyWideColumnsBatch batch;
    std::array<Status, 2> get_statuses;
    db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                            key_slices.size(), key_slices.data(), &batch,
                            get_statuses.data());
    ASSERT_OK(get_statuses[0]);
    ASSERT_OK(get_statuses[1]);
    const uint64_t reads_before = LazyReadCount(options);
    const uint64_t bytes_before = BlobBytesRead(options);
    single_reads = 0;
    std::array<PinnableSlice, 2> results;
    std::array<Status, 2> statuses;
    std::array<LazyColumnReadRequest, 2> reads;
    for (size_t i = 0; i < reads.size(); ++i) {
      reads[i].column = &batch[i][1];
      if (range) {
        reads[i].offset = 10;
        reads[i].length = 100;
      }
      reads[i].result = &results[i];
      reads[i].status = &statuses[i];
    }
    ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
    for (size_t i = 0; i < reads.size(); ++i) {
      ASSERT_OK(statuses[i]);
      EXPECT_EQ(results[i], range ? value.substr(10, 100) : value);
    }
    EXPECT_EQ(single_reads, 1U);
    EXPECT_EQ(LazyReadCount(options), reads_before + 2);
    EXPECT_EQ(BlobBytesRead(options) - bytes_before,
              2 * (range ? 100U : value.size() + 5U));
  }
}

// A read that omits its status out-param is a caller error (there is nowhere to
// report that read's outcome), so MultiResolve fails the whole call with
// InvalidArgument rather than silently resolving.
TEST_F(DBLazyEntityTest, BatchNullStatusReturnsInvalidArgument) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(kNumKeys);
  values.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(3000, static_cast<char>('a' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
  }

  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    reads[i].column = &batch[i][1];
    reads[i].offset = 100;
    reads[i].length = 80;
    reads[i].result = &results[i];
    reads[i].status = nullptr;  // caller error: no status out-param
  }
  const Status s = batch.MultiResolve(reads.size(), reads.data());
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}
// result has been moved.
TEST_F(DBLazyEntityTest, ResultOutlivesGetEntityLazyCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'z');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns moved;
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    // Move the result out of the inner scope; the SuperVersion pin travels with
    // it, so the reference stays resolvable.
    moved = std::move(lazy);
  }

  PinnableSlice value;
  ASSERT_OK(moved.ResolveColumn(moved[1], &value));
  ASSERT_EQ(value, big);
}

// Columns that are never pulled are never read from storage.
TEST_F(DBLazyEntityTest, UnpulledColumnsAreNeverRead) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string wanted(200, 'w');
  const std::string unwanted(5000, 'u');
  const WideColumns columns{{kDefaultWideColumnName, "inline"},
                            {"unwanted", unwanted},
                            {"wanted", wanted}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Sorted order: ["", "unwanted", "wanted"] -> pull only index 2.
  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[2], &value));
  ASSERT_EQ(value, wanted);

  // Only ~200 bytes (the "wanted" blob) should have been read, never the 5000
  // bytes of "unwanted".
  ASSERT_GT(BlobBytesRead(options), 0U);
  ASSERT_LT(BlobBytesRead(options), unwanted.size());
}

// A repeated resolve of the same column reads its blob from storage only once;
// the second resolve is served from the cached bytes with no additional I/O.
TEST_F(DBLazyEntityTest, RepeatedResolutionReadsBlobOnce) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  PinnableSlice v1;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &v1));
  ASSERT_EQ(Slice(v1), big);
  const uint64_t after_first = BlobBytesRead(options);
  ASSERT_GT(after_first, 0U);

  // Resolving the same column again reuses the cached bytes -- no more blob
  // I/O.
  PinnableSlice v2;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &v2));
  ASSERT_EQ(Slice(v2), big);
  ASSERT_EQ(BlobBytesRead(options), after_first);
}

// A memtable-resident entity (never flushed) reads back correctly through the
// lazy API. Blobs are only created at flush, so the large value lives inline in
// the memtable: the column is not a reference and resolves with no blob I/O.
TEST_F(DBLazyEntityTest, MemtableResidentEntity) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  // No Flush(): the entity stays in the memtable.
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_FALSE(lazy[1].is_reference());  // inline in memtable, not a blob ref

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
  ASSERT_EQ(BlobBytesRead(options), 0U);  // no SST/blob files exist yet
}

// The lazy API requires max_open_files == -1 (immortal table readers) so
// embedded/same-file references stay resolvable lazily; otherwise it declines.
TEST_F(DBLazyEntityTest, RequiresMaxOpenFilesMinusOne) {
  Options options = GetLazyTestOptions();
  options.max_open_files = 1000;  // not -1
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string data(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", data}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  const Status s =
      db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key, &lazy);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Under ReadTier::kBlockCacheTier (taken from the originating GetEntityLazy()
// call), resolving a reference whose blob bytes are not cached yields
// Incomplete instead of doing I/O.
TEST_F(DBLazyEntityTest, BlockCacheTierYieldsIncompleteOnMiss) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Warm the SST blocks into the block cache with a normal lazy read. Because
  // it is lazy, the "data" blob is left unresolved, so the blob cache stays
  // cold.
  {
    LazyWideColumns warm;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &warm));
  }

  // A cache-only lazy read: the entity is served from the warm block cache, but
  // resolving the blob would require I/O, so GetColumn returns Incomplete.
  ReadOptions block_cache_only;
  block_cache_only.read_tier = kBlockCacheTier;
  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(block_cache_only, db_->DefaultColumnFamily(),
                               key, &lazy));

  PinnableSlice value;
  const Status s = lazy.ResolveColumn(lazy[1], &value);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

// The embedded (same-file) read path probes the blob cache before honoring
// read_tier == kBlockCacheTier, so a cached embedded record serves a
// block-cache-only read -- whole column or sub-range -- instead of returning
// Incomplete. Regression test for the former quirk where
// ValidateEmbeddedBlobIndex rejected kBlockCacheTier up front, before the cache
// probe (unlike the separate-file range path, which always probed first).
TEST_F(DBLazyEntityTest, EmbeddedBlockCacheTierServedFromCachedValue) {
  Options options = GetLazyTestOptionsWithBlobCache();
  DestroyAndReopen(options);

  const std::string key = "entity";
  const std::string big(4000, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  IngestEmbeddedEntity(options, key, columns);

  // Warm the blob cache with a whole-column read of the embedded record.
  {
    LazyWideColumns warm;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &warm));
    PinnableSlice whole;
    ASSERT_OK(warm.ResolveColumn(warm[1], &whole));
    ASSERT_EQ(whole, big);
  }

  ASSERT_OK(options.statistics->Reset());

  ReadOptions block_cache_only;
  block_cache_only.read_tier = kBlockCacheTier;

  // Sub-range read on a fresh result (resolver cache empty) exercises the
  // embedded range path's blob-cache probe under kBlockCacheTier.
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(block_cache_only, db_->DefaultColumnFamily(),
                                 key, &lazy));
    PinnableSlice range;
    ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/1000, /*length=*/100,
                                      &range));
    ASSERT_EQ(range, big.substr(1000, 100));
  }

  // Whole-column read on a fresh result, also served from the cache.
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(block_cache_only, db_->DefaultColumnFamily(),
                                 key, &lazy));
    PinnableSlice whole;
    ASSERT_OK(lazy.ResolveColumn(lazy[1], &whole));
    ASSERT_EQ(whole, big);
  }

  // Everything came from the blob cache: no disk blob bytes were read.
  ASSERT_EQ(BlobBytesRead(options), 0U);
}

// A column that belongs to a different result is rejected with InvalidArgument
// on the per-read status (columns identify their owning result, so there is no
// untyped index to run out of range).
TEST_F(DBLazyEntityTest, ForeignColumnIsInvalidArgument) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string data(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", data}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  LazyWideColumns other;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &other));

  // Populate `value` with a successful resolve, then confirm a failed resolve
  // into the same output resets it (leaves no stale bytes).
  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_FALSE(value.empty());

  // Resolving `other`'s column through `lazy` is rejected, and clears `value`.
  const Status s = lazy.ResolveColumn(other[0], &value);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(value.empty());

  // A null column is likewise rejected, reported on the per-read status.
  PinnableSlice null_value;
  Status null_status;
  LazyColumnReadRequest read;
  read.column = nullptr;
  read.result = &null_value;
  read.status = &null_status;
  ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_TRUE(null_status.IsInvalidArgument()) << null_status.ToString();
}

// A reused LazyWideColumns is reset (left empty) when a later GetEntityLazy
// call fails argument validation, rather than retaining the previous result.
TEST_F(DBLazyEntityTest, ReusedResultResetOnEarlyReturn) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string value(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"},
                            {"data", value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);

  // Reuse the same result for a call that fails argument validation (an
  // io_activity that is neither kUnknown nor kGetEntity); it must be reset to
  // empty, not left holding the previous entity.
  ReadOptions bad_read_options;
  bad_read_options.io_activity = Env::IOActivity::kGet;
  const Status s = db_->GetEntityLazy(bad_read_options,
                                      db_->DefaultColumnFamily(), key, &lazy);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(lazy.empty());
  ASSERT_EQ(lazy.size(), 0U);
}

// A reused LazyWideColumnsBatch is reset (left empty) on MultiGetEntityLazy's
// early-return paths (num_keys == 0 and argument errors), not left stale.
TEST_F(DBLazyEntityTest, ReusedBatchResetOnEarlyReturn) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string k1 = "k1";
  const std::string k2 = "k2";
  const std::string d1(200, 'a');
  const std::string d2(200, 'b');
  const WideColumns c1{{kDefaultWideColumnName, "i1"}, {"data", d1}};
  const WideColumns c2{{kDefaultWideColumnName, "i2"}, {"data", d2}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), k1, c1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), k2, c2));
  ASSERT_OK(Flush());

  const std::array<Slice, 2> keys{Slice(k1), Slice(k2)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> statuses;

  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  // Reuse with num_keys == 0: the batch must be reset (empty), not stale.
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          /*num_keys=*/0, keys.data(), &batch, statuses.data());
  ASSERT_TRUE(batch.empty());
  ASSERT_EQ(batch.size(), 0U);

  // Repopulate, then reuse for a call that fails argument validation; still
  // reset to empty.
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  ReadOptions bad_read_options;
  bad_read_options.io_activity = Env::IOActivity::kGet;
  db_->MultiGetEntityLazy(bad_read_options, db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_TRUE(statuses[0].IsInvalidArgument()) << statuses[0].ToString();
  ASSERT_TRUE(statuses[1].IsInvalidArgument()) << statuses[1].ToString();
  ASSERT_TRUE(batch.empty());
}

// LazyWideColumnsBatch::MultiResolve rejects a null column and a column that
// belongs to a different result (not this batch), each on its per-read status,
// while still resolving the valid reads in the same call.
TEST_F(DBLazyEntityTest, BatchRejectsForeignAndNullColumns) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const std::array<Slice, 1> keys{Slice(key)};
  LazyWideColumnsBatch batch;
  std::array<Status, 1> get_statuses;
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch,
                          get_statuses.data());
  ASSERT_OK(get_statuses[0]);

  // A standalone result whose column does not belong to this batch.
  LazyWideColumns standalone;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &standalone));

  PinnableSlice v_null;
  PinnableSlice v_foreign;
  PinnableSlice v_ok;
  Status s_null;
  Status s_foreign;
  Status s_ok;
  std::vector<LazyColumnReadRequest> reads(3);
  reads[0].column = nullptr;  // null
  reads[0].result = &v_null;
  reads[0].status = &s_null;
  reads[1].column = &standalone[1];  // foreign: belongs to `standalone`
  reads[1].result = &v_foreign;
  reads[1].status = &s_foreign;
  reads[2].column = &batch[0][1];  // valid: "data" of this batch's entity
  reads[2].result = &v_ok;
  reads[2].status = &s_ok;

  ASSERT_OK(batch.MultiResolve(reads));  // std::vector sugar overload
  ASSERT_TRUE(s_null.IsInvalidArgument()) << s_null.ToString();
  ASSERT_TRUE(s_foreign.IsInvalidArgument()) << s_foreign.ToString();
  ASSERT_OK(s_ok);
  ASSERT_EQ(Slice(v_ok), big);
}

// Regression test: an empty (default-constructed) LazyWideColumnsBatch has a
// null rep_, and a standalone column (from GetEntityLazy, not part of any
// batch) has a null owning_batch_rep_. MultiResolve must still reject the
// standalone column with InvalidArgument -- it must not fall through the
// nullptr == nullptr comparison and resolve a column that does not belong to
// this batch.
TEST_F(DBLazyEntityTest, EmptyBatchRejectsStandaloneColumn) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns standalone;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &standalone));

  // An empty batch owns no columns, so resolving any column through it must
  // fail rather than resolve the standalone column.
  LazyWideColumnsBatch empty_batch;
  ASSERT_TRUE(empty_batch.empty());

  PinnableSlice value;
  Status status;
  LazyColumnReadRequest read;
  read.column = &standalone[1];
  read.result = &value;
  read.status = &status;
  ASSERT_OK(empty_batch.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  ASSERT_TRUE(value.empty());
}

// Wide-column entities do not support user-defined timestamps: PutEntity()
// rejects a UDT column family (see DBWideBasicTest.PutEntityTimestampError), so
// no entity can exist there. GetEntityLazy() inherits this feature-wide
// limitation and behaves at parity with GetEntity() on such a column family --
// it performs the same read-timestamp validation (both go through the same
// internal read path) and, for a plain value, returns a single default column.
TEST_F(DBLazyEntityTest, UserTimestampParityWithGetEntity) {
  Options options = GetLazyTestOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  ColumnFamilyHandle* const cfh = db_->DefaultColumnFamily();
  constexpr char key[] = "entity";

  // Entities cannot be written under UDT.
  const WideColumns columns{{kDefaultWideColumnName, "value"}, {"attr", "x"}};
  ASSERT_EQ(db_->PutEntity(WriteOptions(), cfh, key, columns).code(),
            Status::kInvalidArgument);

  // Write a plain value with a timestamp.
  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), cfh, key, write_ts, "plain"));
  ASSERT_OK(Flush());

  // Without a read timestamp, GetEntity and GetEntityLazy both reject the read
  // (the column family has a user-defined timestamp) -- i.e. at parity.
  {
    PinnableWideColumns eager;
    ASSERT_EQ(db_->GetEntity(ReadOptions(), cfh, key, &eager).code(),
              Status::kInvalidArgument);
    LazyWideColumns lazy;
    ASSERT_EQ(db_->GetEntityLazy(ReadOptions(), cfh, key, &lazy).code(),
              Status::kInvalidArgument);
  }

  // With a read timestamp, both succeed and return the plain value as a single
  // default column.
  std::string read_ts_buf;
  const Slice read_ts = EncodeU64Ts(2, &read_ts_buf);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;

  PinnableWideColumns eager;
  ASSERT_OK(db_->GetEntity(read_opts, cfh, key, &eager));

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(read_opts, cfh, key, &lazy));
  ASSERT_EQ(lazy.size(), eager.columns().size());
  ASSERT_EQ(lazy.size(), 1U);
  ASSERT_EQ(lazy[0].name(), kDefaultWideColumnName);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[0], &value));
  ASSERT_EQ(Slice(value), eager.columns()[0].value());
  ASSERT_EQ(Slice(value), "plain");
}

// A wrong-width read timestamp must be rejected up front (InvalidArgument for
// every key), before MultiCFSnapshot's collapsed-history check -- which assumes
// a correctly-sized timestamp and would otherwise assert in debug builds (or
// read past the timestamp buffer in release) when full_history_ts_low is set.
TEST_F(DBLazyEntityTest, BatchWrongWidthTimestampRejected) {
  Options options = GetLazyTestOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  ColumnFamilyHandle* const cfh = db_->DefaultColumnFamily();

  // Write a plain value with a valid (8-byte) timestamp.
  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "k", write_ts, "plain"));
  ASSERT_OK(Flush());

  // Set full_history_ts_low so the collapsed-history check would run (and, on a
  // wrong-width timestamp, trip the size assertion) if reached before the
  // width validation this test covers.
  std::string ts_low;
  PutFixed64(&ts_low, 1);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(cfh, ts_low));

  // A 1-byte timestamp does not match the column family's 8-byte width.
  const std::string bad_ts_storage(1, '\x01');
  const Slice bad_ts(bad_ts_storage);
  ReadOptions ro;
  ro.timestamp = &bad_ts;

  const std::array<Slice, 2> keys{Slice("k"), Slice("k")};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> statuses;
  db_->MultiGetEntityLazy(ro, cfh, keys.size(), keys.data(), &batch,
                          statuses.data());
  for (const Status& s : statuses) {
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }
}

// The lazy read API must be forwarded through StackableDB wrappers (e.g.
// TransactionDB) to the underlying DB, not fall through to the DB base class
// default (Status::NotSupported). Exercises committed data read outside a
// transaction, including resolving a blob-backed column through the wrapper.
TEST_F(DBLazyEntityTest, StackableDBForwardsLazyReads) {
  Options options = GetLazyTestOptions();
  const std::string txn_dbname = dbname_ + "_txn";
  ASSERT_OK(DestroyDB(txn_dbname, options));

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, TransactionDBOptions(), txn_dbname,
                                &txn_db));
  ASSERT_NE(txn_db, nullptr);
  std::unique_ptr<TransactionDB> txn_db_guard(txn_db);

  ColumnFamilyHandle* const cfh = txn_db->DefaultColumnFamily();

  constexpr char key[] = "entity";
  const std::string small = "inline";  // < min_blob_size: stays inline
  const std::string big(200, 'a');     // >= min_blob_size: blob reference
  const WideColumns columns{{kDefaultWideColumnName, small}, {"attr", big}};

  // Commit the entity via a transaction, then read it back as committed data.
  {
    std::unique_ptr<Transaction> txn(
        txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
    ASSERT_OK(txn->PutEntity(cfh, key, columns));
    ASSERT_OK(txn->Commit());
  }
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Single-key: forwards to the underlying DB (not NotSupported), and the
  // blob-backed column resolves through the wrapper.
  {
    LazyWideColumns lazy;
    ASSERT_OK(txn_db->GetEntityLazy(ReadOptions(), cfh, key, &lazy));
    ASSERT_EQ(lazy.size(), 2U);

    PinnableSlice default_value;
    ASSERT_OK(lazy.ResolveColumn(lazy[0], &default_value));
    ASSERT_EQ(Slice(default_value), small);

    PinnableSlice attr_value;
    ASSERT_OK(lazy.ResolveColumn(lazy[1], &attr_value));
    ASSERT_EQ(Slice(attr_value), big);
  }

  // Batch analogue via the wrapper.
  {
    const std::array<Slice, 1> keys{Slice(key)};
    LazyWideColumnsBatch batch;
    std::array<Status, 1> statuses;
    txn_db->MultiGetEntityLazy(ReadOptions(), cfh, keys.size(), keys.data(),
                               &batch, statuses.data(),
                               /*sorted_input=*/false);
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(batch.size(), 1U);
    ASSERT_EQ(batch[0].size(), 2U);

    PinnableSlice attr_value;
    ASSERT_OK(batch[0].ResolveColumn(batch[0][1], &attr_value));
    ASSERT_EQ(Slice(attr_value), big);
  }
}

// The lazy read API works on a read-only DB instance (opened with
// max_open_files == -1): the entity enumerates without blob I/O and its blob
// column resolves lazily after the call returns (the result's pin keeps it
// valid), matching the eager value.
TEST_F(DBLazyEntityTest, ReadOnlyInstance) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, small}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reopen the same DB read-only; GetEntityLazy must be served by the read-only
  // GetImpl override (which forwards the lazy signal and transfers the pin).
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_FALSE(lazy[0].is_reference());
  ASSERT_EQ(*lazy[0].inline_value(), small);
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_EQ(lazy[1].name(), "data");

  // Enumeration alone reads no blob bytes.
  ASSERT_EQ(BlobBytesRead(options), 0U);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
  ASSERT_GT(BlobBytesRead(options), 0U);
}

// The lazy read API works on a secondary DB instance (opened with
// max_open_files == -1) after catching up to the primary.
TEST_F(DBLazyEntityTest, SecondaryInstance) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";
  const std::string big(250, 'b');
  const WideColumns columns{{kDefaultWideColumnName, small}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const std::string secondary_path = dbname_ + "_secondary";
  ASSERT_OK(DestroyDB(secondary_path, options));
  std::unique_ptr<DB> secondary;
  ASSERT_OK(DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(secondary->GetEntityLazy(
      ReadOptions(), secondary->DefaultColumnFamily(), key, &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_EQ(BlobBytesRead(options), 0U);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
}

// Batched MultiGetEntityLazy works on a read-only instance: the read-only
// GetImpl override must honor the batch's shared SuperVersion / sequence number
// and hand back the resolution Version even though no per-key pin is taken.
TEST_F(DBLazyEntityTest, ReadOnlyInstanceBatched) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(kNumKeys);
  values.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(200 + i, static_cast<char>('a' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(ReadOnlyReopen(options));

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), kNumKeys,
                          key_slices.data(), &batch, get_statuses.data());
  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
    reads[i].column = &batch[i][1];
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}

// Batched MultiGetEntityLazy works on a secondary instance (same requirement on
// its GetImpl override as read-only).
TEST_F(DBLazyEntityTest, SecondaryInstanceBatched) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr size_t kNumKeys = 3;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(kNumKeys);
  values.reserve(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys.push_back("k" + std::to_string(i));
    values.emplace_back(250 + i, static_cast<char>('b' + i));
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), keys.back(),
        {{kDefaultWideColumnName, "inline"}, {"data", values.back()}}));
  }
  ASSERT_OK(Flush());

  const std::string secondary_path = dbname_ + "_secondary";
  ASSERT_OK(DestroyDB(secondary_path, options));
  std::unique_ptr<DB> secondary;
  ASSERT_OK(DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());

  std::vector<Slice> key_slices(keys.begin(), keys.end());
  LazyWideColumnsBatch batch;
  std::vector<Status> get_statuses(kNumKeys);
  secondary->MultiGetEntityLazy(ReadOptions(), secondary->DefaultColumnFamily(),
                                kNumKeys, key_slices.data(), &batch,
                                get_statuses.data());
  std::vector<PinnableSlice> results(kNumKeys);
  std::vector<Status> statuses(kNumKeys);
  std::vector<LazyColumnReadRequest> reads(kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(get_statuses[i]);
    reads[i].column = &batch[i][1];
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }
  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}
// Verify that MultiGetEntityLazy sets IOOptions::io_activity to kMultiGetEntity
// for its reads, so it matches the thread's operation type. This reproduces the
// scenario from T283693234 where the thread operation was left stale at
// OP_GETENTITY after a consistency check, causing a mismatch with the
// kMultiGetEntity activity that MultiGetEntityLazy correctly propagates.
TEST_F(DBLazyEntityTest, MultiGetEntityLazyIOActivity) {
  Options options = GetLazyTestOptions();
  options.enable_thread_tracking = true;
  DestroyAndReopen(options);

  constexpr char key1[] = "k1";
  constexpr char key2[] = "k2";
  const std::string val(200, 'x');
  const WideColumns cols{{kDefaultWideColumnName, val}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1, cols));
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2, cols));
  ASSERT_OK(Flush());

  // Simulate the stress test scenario: thread operation is set to
  // OP_MULTIGETENTITY (as in the stress test OperateDb loop).
  ThreadStatusUtil::SetThreadOperation(
      ThreadStatus::OperationType::OP_MULTIGETENTITY);

  std::array<Slice, 2> keys{Slice(key1), Slice(key2)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> statuses;

  // MultiGetEntityLazy should set io_activity = kMultiGetEntity internally,
  // which matches OP_MULTIGETENTITY. If the thread op were stale at
  // OP_GETENTITY, this would trigger the assertion in debug builds.
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  for (const auto& s : statuses) {
    ASSERT_OK(s);
  }
  ASSERT_EQ(batch.size(), 2U);

  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
