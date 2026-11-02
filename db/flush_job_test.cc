//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/flush_job.h"

#include <algorithm>
#include <array>
#include <map>
#include <string>

#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cache.h"
#include "rocksdb/file_system.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string ValueWithWriteTime(std::string val, uint64_t write_time = 0) {
  std::string result = val;
  PutFixed64(&result, write_time);
  return result;
}
std::string ValueWithPreferredSeqno(std::string val,
                                    SequenceNumber preferred_seqno = 0) {
  std::string result = val;
  PutFixed64(&result, preferred_seqno);
  return result;
}
}  // namespace

// TODO(icanadi) Mock out everything else:
// 1. VersionSet
// 2. Memtable
class FlushJobTestBase : public testing::Test {
 protected:
  FlushJobTestBase(std::string dbname, const Comparator* ucmp)
      : env_(Env::Default()),
        fs_(env_->GetFileSystem()),
        dbname_(std::move(dbname)),
        ucmp_(ucmp),
        options_(),
        db_options_(options_),
        column_family_names_({kDefaultColumnFamilyName, "foo", "bar"}),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        shutting_down_(false),
        mock_table_factory_(new mock::MockTableFactory()) {}

 public:
  ~FlushJobTestBase() override {
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "db is still in %s\n", dbname_.c_str());
    } else {
      // destroy versions_ to release all file handles
      versions_.reset();
      EXPECT_OK(DestroyDir(env_, dbname_));
    }
  }

 protected:
  void NewDB() {
    ASSERT_OK(
        SetIdentityFile(WriteOptions(), env_, dbname_, Temperature::kUnknown));
    VersionEdit new_db;

    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    autovector<VersionEdit> new_cfs;
    SequenceNumber last_seq = 1;
    uint32_t cf_id = 1;
    for (size_t i = 1; i != column_family_names_.size(); ++i) {
      VersionEdit new_cf;
      new_cf.AddColumnFamily(column_family_names_[i]);
      new_cf.SetColumnFamily(cf_id++);
      new_cf.SetComparatorName(ucmp_->Name());
      new_cf.SetPersistUserDefinedTimestamps(persist_udt_);
      new_cf.SetLogNumber(0);
      new_cf.SetNextFile(2);
      new_cf.SetLastSequence(last_seq++);
      new_cfs.emplace_back(new_cf);
    }

    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);

    {
      log::Writer log(std::move(file_writer), 0, false);
      std::string record;
      new_db.EncodeTo(&record);
      s = log.AddRecord(WriteOptions(), record);
      ASSERT_OK(s);

      for (const auto& e : new_cfs) {
        record.clear();
        e.EncodeTo(&record);
        s = log.AddRecord(WriteOptions(), record);
        ASSERT_OK(s);
      }
    }
    ASSERT_OK(s);
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(WriteOptions(), fs_.get(), dbname_, 1,
                       Temperature::kUnknown, nullptr);
    ASSERT_OK(s);
  }

  void SetUp() override {
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));

    // TODO(icanadi) Remove this once we mock out VersionSet
    NewDB();

    db_options_.env = env_;
    db_options_.fs = fs_;
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    db_options_.statistics = CreateDBStatistics();

    cf_options_.comparator = ucmp_;
    cf_options_.persist_user_defined_timestamps = persist_udt_;
    cf_options_.paranoid_file_checks = paranoid_file_checks_;

    std::vector<ColumnFamilyDescriptor> column_families;
    cf_options_.table_factory = mock_table_factory_;
    for (const auto& cf_name : column_family_names_) {
      column_families.emplace_back(cf_name, cf_options_);
    }

    versions_.reset(new VersionSet(
        dbname_, &db_options_, MutableDBOptions{options_}, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        test::kUnitTestDbId, /*db_session_id=*/"",
        /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*read_only=*/false));
    EXPECT_OK(versions_->Recover(column_families, false));
  }

  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string dbname_;
  const Comparator* const ucmp_;
  EnvOptions env_options_;
  Options options_;
  ImmutableDBOptions db_options_;
  const std::vector<std::string> column_family_names_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  ColumnFamilyOptions cf_options_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;

  bool persist_udt_ = true;
  bool paranoid_file_checks_ = false;

  std::shared_ptr<SeqnoToTimeMapping> empty_seqno_to_time_mapping_;
};

class FlushJobTest : public FlushJobTestBase {
 public:
  FlushJobTest()
      : FlushJobTestBase(test::PerThreadDBPath("flush_job_test"),
                         BytewiseComparator()) {}
};

class FlushJobMergeFilterGuardTest : public FlushJobTest {
 public:
  class Filter : public CompactionFilter {
   public:
    Filter(InstrumentedMutex* mutex, std::atomic<int>* destructions)
        : mutex_(mutex), destructions_(destructions) {}
    ~Filter() override {
      const bool unlocked = mutex_->TryLock();
      EXPECT_TRUE(unlocked);
      if (unlocked) {
        mutex_->Unlock();
      }
      destructions_->fetch_add(1);
    }
    const char* Name() const override { return "FlushJobMergeFilterGuard"; }
    MergeOrder GetMergeOrder() const override {
      const bool unlocked = mutex_->TryLock();
      EXPECT_TRUE(unlocked);
      if (unlocked) {
        mutex_->Unlock();
      }
      return MergeOrder::kMergeBeforeFilter;
    }

   private:
    InstrumentedMutex* const mutex_;
    std::atomic<int>* const destructions_;
  };

  class Factory : public CompactionFilterFactory {
   public:
    Factory(InstrumentedMutex* mutex, std::atomic<int>* destructions)
        : mutex_(mutex), destructions_(destructions) {}
    const char* Name() const override {
      return "FlushJobMergeFilterGuardFactory";
    }
    bool ShouldFilterTableFileCreation(
        TableFileCreationReason reason) const override {
      return reason == TableFileCreationReason::kFlush;
    }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& /*context*/) override {
      creations.fetch_add(1);
      return std::make_unique<Filter>(mutex_, destructions_);
    }
    std::atomic<int> creations{0};

   private:
    InstrumentedMutex* const mutex_;
    std::atomic<int>* const destructions_;
  };

  void SetUp() override {
    factory_ = std::make_shared<Factory>(&mutex_, &destructions_);
    cf_options_.compaction_filter_factory = factory_;
    cf_options_.experimental_mempurge_threshold = 2.0;
    FlushJobTest::SetUp();
  }

  std::shared_ptr<Factory> factory_;
  std::atomic<int> destructions_{0};
};

TEST_F(FlushJobMergeFilterGuardTest, MergeBeforeFilterMemPurgeReacquiresMutex) {
  JobContext context(0);
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  MemTable* mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                            kMaxSequenceNumber);
  mem->Ref();
  ASSERT_OK(mem->Add(1, kTypeValue, "a", "value", nullptr));
  mem->ConstructFragmentedRangeTombstones();
  autovector<ReadOnlyMemTable*> to_delete;
  cfd->imm()->Add(mem, &to_delete);
  for (ReadOnlyMemTable* old : to_delete) {
    delete old;
  }
  EventLogger logger(db_options_.info_log.get());
  context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob job(dbname_, cfd, db_options_, cfd->GetLatestMutableCFOptions(),
               kMaxSequenceNumber, env_options_, versions_.get(), &mutex_,
               &shutting_down_, &context, FlushReason::kWriteBufferFull,
               nullptr, nullptr, nullptr, kNoCompression,
               db_options_.statistics.get(), &logger, true, true, true,
               Env::Priority::USER, nullptr, empty_seqno_to_time_mapping_);
  {
    InstrumentedMutexLock lock(&mutex_);
    job.PickMemTable();
    ASSERT_TRUE(job.Run().IsInvalidArgument());
    mutex_.AssertHeld();
  }
  // MemPurge rejects first, then the existing SST fallback also rejects.
  ASSERT_EQ(2, factory_->creations.load());
  ASSERT_EQ(2, destructions_.load());
  ASSERT_EQ(0, cfd->current()->storage_info()->NumLevelFiles(0));
  context.Clean();
}

TEST_F(FlushJobTest, Empty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  EventLogger event_logger(db_options_.info_log.get());
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, cfd->GetLatestMutableCFOptions(),
                     std::numeric_limits<uint64_t>::max() /* memtable_id */,
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     &job_context, FlushReason::kTest, nullptr, nullptr,
                     nullptr, kNoCompression, nullptr, &event_logger, false,
                     true /* sync_output_directory */,
                     true /* write_manifest */, Env::Priority::USER,
                     nullptr /*IOTracer*/, empty_seqno_to_time_mapping_);
  {
    InstrumentedMutexLock l(&mutex_);
    flush_job.PickMemTable();
    ASSERT_OK(flush_job.Run());
  }
  job_context.Clean();
}

TEST_F(FlushJobTest, NonEmpty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);
  new_mem->Ref();
  auto inserted_keys = mock::MakeMockFile();
  // Test data:
  //   seqno [    1,    2 ... 8998, 8999, 9000, 9001, 9002 ... 9999 ]
  //   key   [ 1001, 1002 ... 9998, 9999,    0,    1,    2 ...  999 ]
  //   range-delete "9995" -> "9999" at seqno 10000
  //   blob references with seqnos 10001..10006
  for (int i = 1; i < 10000; ++i) {
    std::string key(std::to_string((i + 1000) % 10000));
    std::string value("value" + key);
    ASSERT_OK(new_mem->Add(SequenceNumber(i), kTypeValue, key, value,
                           nullptr /* kv_prot_info */));
    if ((i + 1000) % 10000 < 9995) {
      InternalKey internal_key(key, SequenceNumber(i), kTypeValue);
      inserted_keys.push_back({internal_key.Encode().ToString(), value});
    }
  }

  {
    MemTablePostProcessInfo post_process_info;
    ASSERT_OK(new_mem->Add(SequenceNumber(10000), kTypeRangeDeletion, "9995",
                           "9999a", nullptr /* kv_prot_info */,
                           true /* allow_concurrent */, &post_process_info));
    new_mem->BatchPostProcess(post_process_info);
    InternalKey internal_key("9995", SequenceNumber(10000), kTypeRangeDeletion);
    inserted_keys.push_back({internal_key.Encode().ToString(), "9999a"});
  }

  // Note: the first two blob references will not be considered when resolving
  // the oldest blob file referenced (the first one is inlined TTL, while the
  // second one is TTL and thus points to a TTL blob file).
  constexpr std::array<uint64_t, 6> blob_file_numbers{
      {kInvalidBlobFileNumber, 5, 103, 17, 102, 101}};
  for (size_t i = 0; i < blob_file_numbers.size(); ++i) {
    std::string key(std::to_string(i + 10001));
    std::string blob_index;
    if (i == 0) {
      BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 1234567890ULL,
                                  "foo");
    } else if (i == 1) {
      BlobIndex::EncodeBlobTTL(&blob_index, /* expiration */ 1234567890ULL,
                               blob_file_numbers[i], /* offset */ i << 10,
                               /* size */ i << 20, kNoCompression);
    } else {
      BlobIndex::EncodeBlob(&blob_index, blob_file_numbers[i],
                            /* offset */ i << 10, /* size */ i << 20,
                            kNoCompression);
    }

    const SequenceNumber seq(i + 10001);
    ASSERT_OK(new_mem->Add(seq, kTypeBlobIndex, key, blob_index,
                           nullptr /* kv_prot_info */));

    InternalKey internal_key(key, seq, kTypeBlobIndex);
    inserted_keys.push_back({internal_key.Encode().ToString(), blob_index});
  }
  mock::SortKVVector(&inserted_keys);

  autovector<ReadOnlyMemTable*> to_delete;
  new_mem->ConstructFragmentedRangeTombstones();
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, cfd->GetLatestMutableCFOptions(),
                     std::numeric_limits<uint64_t>::max() /* memtable_id */,
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     &job_context, FlushReason::kTest, nullptr, nullptr,
                     nullptr, kNoCompression, db_options_.statistics.get(),
                     &event_logger, true, true /* sync_output_directory */,
                     true /* write_manifest */, Env::Priority::USER,
                     nullptr /*IOTracer*/, empty_seqno_to_time_mapping_);

  HistogramData hist;
  FileMetaData file_meta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr, &file_meta));
  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);

  ASSERT_EQ(std::to_string(0), file_meta.smallest.user_key().ToString());
  ASSERT_EQ("9999a", file_meta.largest.user_key().ToString());
  ASSERT_EQ(1, file_meta.fd.smallest_seqno);
  ASSERT_EQ(10006, file_meta.fd.largest_seqno);
  ASSERT_EQ(17, file_meta.oldest_blob_file_number);
  mock_table_factory_->AssertSingleFile(inserted_keys);
  job_context.Clean();
}

TEST_F(FlushJobTest, FlushMemTablesSingleColumnFamily) {
  const size_t num_mems = 2;
  const size_t num_mems_to_flush = 1;
  const size_t num_keys_per_table = 100;
  JobContext job_context(0);
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  std::vector<uint64_t> memtable_ids;
  std::vector<MemTable*> new_mems;
  for (size_t i = 0; i != num_mems; ++i) {
    MemTable* mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                              kMaxSequenceNumber);
    mem->SetID(i);
    mem->Ref();
    new_mems.emplace_back(mem);
    memtable_ids.push_back(mem->GetID());

    for (size_t j = 0; j < num_keys_per_table; ++j) {
      std::string key(std::to_string(j + i * num_keys_per_table));
      std::string value("value" + key);
      ASSERT_OK(mem->Add(SequenceNumber(j + i * num_keys_per_table), kTypeValue,
                         key, value, nullptr /* kv_prot_info */));
    }
  }

  autovector<ReadOnlyMemTable*> to_delete;
  for (auto mem : new_mems) {
    mem->ConstructFragmentedRangeTombstones();
    cfd->imm()->Add(mem, &to_delete);
  }

  EventLogger event_logger(db_options_.info_log.get());

  assert(memtable_ids.size() == num_mems);
  uint64_t smallest_memtable_id = memtable_ids.front();
  uint64_t flush_memtable_id = smallest_memtable_id + num_mems_to_flush - 1;
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      cfd->GetLatestMutableCFOptions(), flush_memtable_id, env_options_,
      versions_.get(), &mutex_, &shutting_down_, &job_context,
      FlushReason::kTest, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, empty_seqno_to_time_mapping_);
  HistogramData hist;
  FileMetaData file_meta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr /* prep_tracker */, &file_meta));
  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);

  ASSERT_EQ(std::to_string(0), file_meta.smallest.user_key().ToString());
  ASSERT_EQ("99", file_meta.largest.user_key().ToString());
  ASSERT_EQ(0, file_meta.fd.smallest_seqno);
  ASSERT_EQ(SequenceNumber(num_mems_to_flush * num_keys_per_table - 1),
            file_meta.fd.largest_seqno);
  ASSERT_EQ(kInvalidBlobFileNumber, file_meta.oldest_blob_file_number);

  for (auto m : to_delete) {
    delete m;
  }
  to_delete.clear();
  job_context.Clean();
}

TEST_F(FlushJobTest, FlushMemtablesMultipleColumnFamilies) {
  autovector<ColumnFamilyData*> all_cfds;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    all_cfds.push_back(cfd);
  }
  const std::vector<size_t> num_memtables = {2, 1, 3};
  assert(num_memtables.size() == column_family_names_.size());
  const size_t num_keys_per_memtable = 1000;
  JobContext job_context(0);
  std::vector<uint64_t> memtable_ids;
  std::vector<SequenceNumber> smallest_seqs;
  std::vector<SequenceNumber> largest_seqs;
  autovector<ReadOnlyMemTable*> to_delete;
  SequenceNumber curr_seqno = 0;
  size_t k = 0;
  for (auto cfd : all_cfds) {
    smallest_seqs.push_back(curr_seqno);
    for (size_t i = 0; i != num_memtables[k]; ++i) {
      MemTable* mem = cfd->ConstructNewMemtable(
          cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
      mem->SetID(i);
      mem->Ref();

      for (size_t j = 0; j != num_keys_per_memtable; ++j) {
        std::string key(std::to_string(j + i * num_keys_per_memtable));
        std::string value("value" + key);
        ASSERT_OK(mem->Add(curr_seqno++, kTypeValue, key, value,
                           nullptr /* kv_prot_info */));
      }
      mem->ConstructFragmentedRangeTombstones();
      cfd->imm()->Add(mem, &to_delete);
    }
    largest_seqs.push_back(curr_seqno - 1);
    memtable_ids.push_back(num_memtables[k++] - 1);
  }

  EventLogger event_logger(db_options_.info_log.get());
  std::vector<std::unique_ptr<FlushJob>> flush_jobs;
  k = 0;
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  for (auto cfd : all_cfds) {
    std::vector<SequenceNumber> snapshot_seqs;
    flush_jobs.emplace_back(new FlushJob(
        dbname_, cfd, db_options_, cfd->GetLatestMutableCFOptions(),
        memtable_ids[k], env_options_, versions_.get(), &mutex_,
        &shutting_down_, &job_context, FlushReason::kTest, nullptr, nullptr,
        nullptr, kNoCompression, db_options_.statistics.get(), &event_logger,
        true, false /* sync_output_directory */, false /* write_manifest */,
        Env::Priority::USER, nullptr /*IOTracer*/,
        empty_seqno_to_time_mapping_));
    k++;
  }
  HistogramData hist;
  std::vector<FileMetaData> file_metas;
  // Call reserve to avoid auto-resizing
  file_metas.reserve(flush_jobs.size());
  mutex_.Lock();
  for (auto& job : flush_jobs) {
    job->PickMemTable();
  }
  for (auto& job : flush_jobs) {
    FileMetaData meta;
    // Run will release and re-acquire  mutex
    ASSERT_OK(job->Run(nullptr /**/, &meta));
    file_metas.emplace_back(meta);
  }
  autovector<FileMetaData*> file_meta_ptrs;
  for (auto& meta : file_metas) {
    file_meta_ptrs.push_back(&meta);
  }
  autovector<const autovector<ReadOnlyMemTable*>*> mems_list;
  for (size_t i = 0; i != all_cfds.size(); ++i) {
    const auto& mems = flush_jobs[i]->GetMemTables();
    mems_list.push_back(&mems);
  }
  autovector<std::list<std::unique_ptr<FlushJobInfo>>*>
      committed_flush_jobs_info;
  for (auto& job : flush_jobs) {
    committed_flush_jobs_info.push_back(job->GetCommittedFlushJobsInfo());
  }

  Status s = InstallMemtableAtomicFlushResults(
      nullptr /* imm_lists */, all_cfds, mems_list, versions_.get(),
      nullptr /* prep_tracker */, &mutex_, file_meta_ptrs,
      committed_flush_jobs_info, &job_context.memtables_to_free,
      nullptr /* db_directory */, nullptr /* log_buffer */);
  ASSERT_OK(s);

  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);
  k = 0;
  for (const auto& file_meta : file_metas) {
    ASSERT_EQ(std::to_string(0), file_meta.smallest.user_key().ToString());
    ASSERT_EQ("999", file_meta.largest.user_key()
                         .ToString());  // max key by bytewise comparator
    ASSERT_EQ(smallest_seqs[k], file_meta.fd.smallest_seqno);
    ASSERT_EQ(largest_seqs[k], file_meta.fd.largest_seqno);
    // Verify that imm is empty
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              all_cfds[k]->imm()->GetEarliestMemTableID());
    ASSERT_EQ(0, all_cfds[k]->imm()->GetLatestMemTableID(
                     false /* for_atomic_flush */));
    ++k;
  }

  for (auto m : to_delete) {
    delete m;
  }
  to_delete.clear();
  job_context.Clean();
}

TEST_F(FlushJobTest, Snapshots) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);

  std::set<SequenceNumber> snapshots_set;
  int keys = 10000;
  int max_inserts_per_keys = 8;

  Random rnd(301);
  for (int i = 0; i < keys / 2; ++i) {
    snapshots_set.insert(rnd.Uniform(keys * (max_inserts_per_keys / 2)) + 1);
  }
  // set has already removed the duplicate snapshots
  std::vector<SequenceNumber> snapshots(snapshots_set.begin(),
                                        snapshots_set.end());

  new_mem->Ref();
  SequenceNumber current_seqno = 0;
  auto inserted_keys = mock::MakeMockFile();
  for (int i = 1; i < keys; ++i) {
    std::string key(std::to_string(i));
    int insertions = rnd.Uniform(max_inserts_per_keys);
    for (int j = 0; j < insertions; ++j) {
      std::string value(rnd.HumanReadableString(10));
      auto seqno = ++current_seqno;
      ASSERT_OK(new_mem->Add(SequenceNumber(seqno), kTypeValue, key, value,
                             nullptr /* kv_prot_info */));
      // a key is visible only if:
      // 1. it's the last one written (j == insertions - 1)
      // 2. there's a snapshot pointing at it
      bool visible = (j == insertions - 1) ||
                     (snapshots_set.find(seqno) != snapshots_set.end());
      if (visible) {
        InternalKey internal_key(key, seqno, kTypeValue);
        inserted_keys.push_back({internal_key.Encode().ToString(), value});
      }
    }
  }
  mock::SortKVVector(&inserted_keys);

  autovector<ReadOnlyMemTable*> to_delete;
  new_mem->ConstructFragmentedRangeTombstones();
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber,
                                  std::move(snapshots));
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, cfd->GetLatestMutableCFOptions(),
                     std::numeric_limits<uint64_t>::max() /* memtable_id */,
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     &job_context, FlushReason::kTest, nullptr, nullptr,
                     nullptr, kNoCompression, db_options_.statistics.get(),
                     &event_logger, true, true /* sync_output_directory */,
                     true /* write_manifest */, Env::Priority::USER,
                     nullptr /*IOTracer*/, empty_seqno_to_time_mapping_);
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run());
  mutex_.Unlock();
  mock_table_factory_->AssertSingleFile(inserted_keys);
  HistogramData hist;
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);
  job_context.Clean();
}

TEST_F(FlushJobTest, GetRateLimiterPriorityForWrite) {
  // Prepare a FlushJob that flush MemTables of Single Column Family.
  const size_t num_mems = 2;
  const size_t num_mems_to_flush = 1;
  const size_t num_keys_per_table = 100;
  JobContext job_context(0);
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  std::vector<uint64_t> memtable_ids;
  std::vector<MemTable*> new_mems;
  for (size_t i = 0; i != num_mems; ++i) {
    MemTable* mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                              kMaxSequenceNumber);
    mem->SetID(i);
    mem->Ref();
    new_mems.emplace_back(mem);
    memtable_ids.push_back(mem->GetID());

    for (size_t j = 0; j < num_keys_per_table; ++j) {
      std::string key(std::to_string(j + i * num_keys_per_table));
      std::string value("value" + key);
      ASSERT_OK(mem->Add(SequenceNumber(j + i * num_keys_per_table), kTypeValue,
                         key, value, nullptr /* kv_prot_info */));
    }
  }

  autovector<ReadOnlyMemTable*> to_delete;
  for (auto mem : new_mems) {
    mem->ConstructFragmentedRangeTombstones();
    cfd->imm()->Add(mem, &to_delete);
  }

  EventLogger event_logger(db_options_.info_log.get());

  assert(memtable_ids.size() == num_mems);
  uint64_t smallest_memtable_id = memtable_ids.front();
  uint64_t flush_memtable_id = smallest_memtable_id + num_mems_to_flush - 1;
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      cfd->GetLatestMutableCFOptions(), flush_memtable_id, env_options_,
      versions_.get(), &mutex_, &shutting_down_, &job_context,
      FlushReason::kTest, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, empty_seqno_to_time_mapping_);

  // When the state from WriteController is normal.
  ASSERT_EQ(flush_job.GetRateLimiterPriority(), Env::IO_HIGH);

  WriteController* write_controller =
      flush_job.versions_->GetColumnFamilySet()->write_controller();

  {
    // When the state from WriteController is Delayed.
    std::unique_ptr<WriteControllerToken> delay_token =
        write_controller->GetDelayToken(1000000);
    ASSERT_EQ(flush_job.GetRateLimiterPriority(), Env::IO_USER);
  }

  {
    // When the state from WriteController is Stopped.
    std::unique_ptr<WriteControllerToken> stop_token =
        write_controller->GetStopToken();
    ASSERT_EQ(flush_job.GetRateLimiterPriority(), Env::IO_USER);
  }
}

TEST_F(FlushJobTest, ReplaceTimedPutWriteTimeWithPreferredSeqno) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);
  new_mem->Ref();
  std::shared_ptr<SeqnoToTimeMapping> seqno_to_time_mapping =
      std::make_shared<SeqnoToTimeMapping>();
  // Seqno: 10,       11, ... 20,
  // Time:  ...  500      ...      600
  // GetProximalSeqnoBeforeTime(500) -> 10
  // GetProximalSeqnoBeforeTime(600) -> 20
  seqno_to_time_mapping->Append(10, 500);
  seqno_to_time_mapping->Append(20, 600);

  ASSERT_OK(new_mem->Add(SequenceNumber(15), kTypeValuePreferredSeqno, "bar",
                         ValueWithWriteTime("bval", 500),
                         nullptr /*kv_prot_info*/));
  ASSERT_OK(new_mem->Add(SequenceNumber(18), kTypeValuePreferredSeqno, "foo",
                         ValueWithWriteTime("fval", 600),
                         nullptr /*kv_prot_info*/));

  auto inserted_entries = mock::MakeMockFile();
  InternalKey smallest_internal_key("bar", SequenceNumber(15),
                                    kTypeValuePreferredSeqno);
  inserted_entries.push_back({smallest_internal_key.Encode().ToString(),
                              ValueWithPreferredSeqno("bval", 10)});
  InternalKey largest_internal_key("foo", SequenceNumber(18), kTypeValue);
  inserted_entries.push_back(
      {largest_internal_key.Encode().ToString(), "fval"});
  autovector<ReadOnlyMemTable*> to_delete;
  new_mem->ConstructFragmentedRangeTombstones();
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, cfd->GetLatestMutableCFOptions(),
                     std::numeric_limits<uint64_t>::max() /* memtable_id */,
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     &job_context, FlushReason::kTest, nullptr, nullptr,
                     nullptr, kNoCompression, db_options_.statistics.get(),
                     &event_logger, true, true /* sync_output_directory */,
                     true /* write_manifest */, Env::Priority::USER,
                     nullptr /*IOTracer*/, seqno_to_time_mapping);

  FileMetaData file_meta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr, &file_meta));
  mutex_.Unlock();

  ASSERT_EQ(smallest_internal_key.Encode().ToString(),
            file_meta.smallest.Encode().ToString());
  ASSERT_EQ(largest_internal_key.Encode().ToString(),
            file_meta.largest.Encode().ToString());
  mock_table_factory_->AssertSingleFile(inserted_entries);
  job_context.Clean();
}

// Test parameters:
// param 0): paranoid file check
// param 1): user-defined timestamp test mode
class FlushJobTimestampTest
    : public FlushJobTestBase,
      public testing::WithParamInterface<
          std::tuple<bool, test::UserDefinedTimestampTestMode>> {
 public:
  FlushJobTimestampTest()
      : FlushJobTestBase(test::PerThreadDBPath("flush_job_ts_gc_test"),
                         test::BytewiseComparatorWithU64TsWrapper()) {}

  void AddKeyValueToMemtable(MemTable* memtable, std::string key, uint64_t ts,
                             SequenceNumber seq, ValueType value_type,
                             Slice value) {
    std::string key_str(std::move(key));
    PutFixed64(&key_str, ts);
    ASSERT_OK(memtable->Add(seq, value_type, key_str, value,
                            nullptr /* kv_prot_info */));
  }

 protected:
  void SetUp() override {
    paranoid_file_checks_ = std::get<0>(GetParam());
    auto udt_test_mode = std::get<1>(GetParam());
    persist_udt_ = test::ShouldPersistUDT(udt_test_mode);
    FlushJobTestBase::SetUp();
  }
  static constexpr uint64_t kStartTs = 10;
  static constexpr SequenceNumber kStartSeq = 0;
  SequenceNumber curr_seq_{kStartSeq};
  std::atomic<uint64_t> curr_ts_{kStartTs};

  void CheckFileMetaData(ColumnFamilyData* cfd,
                         const InternalKey& expected_smallest,
                         const InternalKey& expected_largest,
                         const FileMetaData* meta_from_flush) const {
    ASSERT_EQ(expected_smallest.Encode(), meta_from_flush->smallest.Encode());
    ASSERT_EQ(expected_largest.Encode(), meta_from_flush->largest.Encode());

    const VersionStorageInfo* storage_info = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& l0_files = storage_info->LevelFiles(0);

    ASSERT_EQ(l0_files.size(), 1);
    auto installed_file_meta = l0_files[0];
    ASSERT_EQ(expected_smallest.Encode(),
              installed_file_meta->smallest.Encode());
    ASSERT_EQ(expected_largest.Encode(), installed_file_meta->largest.Encode());
  }
  void CheckFullHistoryTsLow(ColumnFamilyData* cfd,
                             const std::string& expected_full_history_ts_low) {
    ASSERT_EQ(expected_full_history_ts_low, cfd->GetFullHistoryTsLow());
  }
};

TEST_P(FlushJobTimestampTest, AllKeysExpired) {
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  autovector<ReadOnlyMemTable*> to_delete;

  {
    MemTable* new_mem = cfd->ConstructNewMemtable(
        cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
    new_mem->Ref();
    for (int i = 0; i < 100; ++i) {
      uint64_t ts = curr_ts_.fetch_add(1);
      SequenceNumber seq = (curr_seq_++);
      AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                            ValueType::kTypeValue, "0_value");
    }
    uint64_t ts = curr_ts_.fetch_add(1);
    SequenceNumber seq = (curr_seq_++);
    AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                          ValueType::kTypeDeletionWithTimestamp, "");
    new_mem->ConstructFragmentedRangeTombstones();
    cfd->imm()->Add(new_mem, &to_delete);
  }

  std::vector<SequenceNumber> snapshots;
  JobContext job_context(0);
  EventLogger event_logger(db_options_.info_log.get());
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  cfd->SetFullHistoryTsLow(full_history_ts_low);
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  FlushJob flush_job(
      dbname_, cfd, db_options_, cfd->GetLatestMutableCFOptions(),
      std::numeric_limits<uint64_t>::max() /* memtable_id */, env_options_,
      versions_.get(), &mutex_, &shutting_down_, &job_context,
      FlushReason::kTest, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, empty_seqno_to_time_mapping_,
      /*db_id=*/"",
      /*db_session_id=*/"", full_history_ts_low);

  FileMetaData fmeta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(/*prep_tracker=*/nullptr, &fmeta));
  mutex_.Unlock();

  {
    std::string key = test::EncodeInt(0);
    if (!persist_udt_) {
      // When `AdvancedColumnFamilyOptions.persist_user_defined_timestamps` flag
      // is set to false. The user-defined timestamp is stripped from user key
      // during flush, making the user key logically containing the minimum
      // timestamp.
      key.append(test::EncodeInt(0));
    } else {
      key.append(test::EncodeInt(curr_ts_.load(std::memory_order_relaxed) - 1));
    }
    InternalKey ikey(key, curr_seq_ - 1, ValueType::kTypeDeletionWithTimestamp);
    CheckFileMetaData(cfd, ikey, ikey, &fmeta);
    CheckFullHistoryTsLow(cfd, full_history_ts_low);
  }

  job_context.Clean();
  ASSERT_TRUE(to_delete.empty());
}

TEST_P(FlushJobTimestampTest, NoKeyExpired) {
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  autovector<ReadOnlyMemTable*> to_delete;

  {
    MemTable* new_mem = cfd->ConstructNewMemtable(
        cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
    new_mem->Ref();
    for (int i = 0; i < 100; ++i) {
      uint64_t ts = curr_ts_.fetch_add(1);
      SequenceNumber seq = (curr_seq_++);
      AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                            ValueType::kTypeValue, "0_value");
    }
    new_mem->ConstructFragmentedRangeTombstones();
    cfd->imm()->Add(new_mem, &to_delete);
  }

  std::vector<SequenceNumber> snapshots;
  JobContext job_context(0);
  job_context.InitSnapshotContext(nullptr, nullptr, kMaxSequenceNumber, {});
  EventLogger event_logger(db_options_.info_log.get());
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 0);
  cfd->SetFullHistoryTsLow(full_history_ts_low);
  FlushJob flush_job(
      dbname_, cfd, db_options_, cfd->GetLatestMutableCFOptions(),
      std::numeric_limits<uint64_t>::max() /* memtable_id */, env_options_,
      versions_.get(), &mutex_, &shutting_down_, &job_context,
      FlushReason::kTest, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, empty_seqno_to_time_mapping_,
      /*db_id=*/"",
      /*db_session_id=*/"", full_history_ts_low);

  FileMetaData fmeta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(/*prep_tracker=*/nullptr, &fmeta));
  mutex_.Unlock();

  {
    std::string ukey = test::EncodeInt(0);
    std::string smallest_key;
    std::string largest_key;
    std::string expected_full_history_ts_low;
    if (!persist_udt_) {
      // When `AdvancedColumnFamilyOptions.persist_user_defined_timestamps` flag
      // is set to false. The user-defined timestamp is stripped from user key
      // during flush, making the user key logically containing the minimum
      // timestamp, which is hardcoded to be all zeros for now.
      smallest_key = ukey + test::EncodeInt(0);
      largest_key = ukey + test::EncodeInt(0);
      // When not all keys have expired and `persist_user_defined_timestamps` is
      // false. UDTs will be removed during flush, `full_history_ts_low` should
      // be automatically increased to above the effective cutoff UDT in the
      // flush.
      PutFixed64(&expected_full_history_ts_low, curr_ts_.fetch_add(1));
    } else {
      smallest_key =
          ukey + test::EncodeInt(curr_ts_.load(std::memory_order_relaxed) - 1);
      largest_key = ukey + test::EncodeInt(kStartTs);
      expected_full_history_ts_low = full_history_ts_low;
    }
    InternalKey smallest(smallest_key, curr_seq_ - 1, ValueType::kTypeValue);
    if (!persist_udt_) {
      InternalKey largest(largest_key, curr_seq_ - 1, ValueType::kTypeValue);
      CheckFileMetaData(cfd, smallest, largest, &fmeta);
      CheckFullHistoryTsLow(cfd, expected_full_history_ts_low);
    } else {
      InternalKey largest(largest_key, kStartSeq, ValueType::kTypeValue);
      CheckFileMetaData(cfd, smallest, largest, &fmeta);
      CheckFullHistoryTsLow(cfd, expected_full_history_ts_low);
    }
  }
  job_context.Clean();
  ASSERT_TRUE(to_delete.empty());
}

// Param 0: paranoid file check
// Param 1: test mode for the user-defined timestamp feature
INSTANTIATE_TEST_CASE_P(
    FlushJobTimestampTest, FlushJobTimestampTest,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Values(
            test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
            test::UserDefinedTimestampTestMode::kNormal)));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
