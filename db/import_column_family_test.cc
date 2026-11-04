//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <functional>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class ImportColumnFamilyTest : public DBTestBase {
 public:
  ImportColumnFamilyTest()
      : DBTestBase("import_column_family_test", /*env_do_fsync=*/true) {
    sst_files_dir_ = dbname_ + "/sst_files/";
    export_files_dir_ = test::PerThreadDBPath(env_, "export");
    export_files_dir2_ = test::PerThreadDBPath(env_, "export2");

    DestroyAndRecreateExternalSSTFilesDir();
    import_cfh_ = nullptr;
    import_cfh2_ = nullptr;
    metadata_ptr_ = nullptr;
    metadata_ptr2_ = nullptr;
  }

  ~ImportColumnFamilyTest() override {
    if (import_cfh_) {
      EXPECT_OK(db_->DropColumnFamily(import_cfh_));
      EXPECT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
      import_cfh_ = nullptr;
    }
    if (import_cfh2_) {
      EXPECT_OK(db_->DropColumnFamily(import_cfh2_));
      EXPECT_OK(db_->DestroyColumnFamilyHandle(import_cfh2_));
      import_cfh2_ = nullptr;
    }
    if (metadata_ptr_) {
      delete metadata_ptr_;
      metadata_ptr_ = nullptr;
    }

    if (metadata_ptr2_) {
      delete metadata_ptr2_;
      metadata_ptr2_ = nullptr;
    }
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    EXPECT_OK(DestroyDir(env_, export_files_dir_));
    EXPECT_OK(DestroyDir(env_, export_files_dir2_));
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    EXPECT_OK(env_->CreateDir(sst_files_dir_));
    EXPECT_OK(DestroyDir(env_, export_files_dir_));
    EXPECT_OK(DestroyDir(env_, export_files_dir2_));
  }

  LiveFileMetaData LiveFileMetaDataInit(std::string name, std::string path,
                                        int level,
                                        SequenceNumber smallest_seqno,
                                        SequenceNumber largest_seqno) {
    LiveFileMetaData metadata;
    metadata.name = name;
    metadata.db_path = path;
    metadata.smallest_seqno = smallest_seqno;
    metadata.largest_seqno = largest_seqno;
    metadata.level = level;
    return metadata;
  }

 protected:
  std::string sst_files_dir_;
  std::string export_files_dir_;
  std::string export_files_dir2_;
  ColumnFamilyHandle* import_cfh_;
  ColumnFamilyHandle* import_cfh2_;
  ExportImportFilesMetaData* metadata_ptr_;
  ExportImportFilesMetaData* metadata_ptr2_;
};

TEST_F(ImportColumnFamilyTest, ImportSSTFileWriterFiles) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  SstFileWriter sfw_unknown(EnvOptions(), options);

  // cf1.sst
  const std::string cf1_sst_name = "cf1.sst";
  const std::string cf1_sst = sst_files_dir_ + cf1_sst_name;
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K1", "V1"));
  ASSERT_OK(sfw_cf1.Put("K2", "V2"));
  ASSERT_OK(sfw_cf1.Finish());

  // cf_unknown.sst
  const std::string unknown_sst_name = "cf_unknown.sst";
  const std::string unknown_sst = sst_files_dir_ + unknown_sst_name;
  ASSERT_OK(sfw_unknown.Open(unknown_sst));
  ASSERT_OK(sfw_unknown.Put("K3", "V1"));
  ASSERT_OK(sfw_unknown.Put("K4", "V2"));
  ASSERT_OK(sfw_unknown.Finish());

  {
    // Import sst file corresponding to cf1 onto a new cf and verify
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(
        LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);

    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K1", &value));
    ASSERT_EQ(value, "V1");
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K2", &value));
    ASSERT_EQ(value, "V2");
    ASSERT_OK(db_->DropColumnFamily(import_cfh_));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
    import_cfh_ = nullptr;
  }

  {
    // Import sst file corresponding to unknown cf onto a new cf and verify
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(
        LiveFileMetaDataInit(unknown_sst_name, sst_files_dir_, 0, 20, 29));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "yoyo", ImportColumnFamilyOptions(), metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);

    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K3", &value));
    ASSERT_EQ(value, "V1");
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K4", &value));
    ASSERT_EQ(value, "V2");
  }
  EXPECT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
  import_cfh_ = nullptr;

  // verify sst unique id during reopen
  options.verify_sst_unique_id_in_manifest = true;
  ReopenWithColumnFamilies({"default", "koko", "yoyo"}, options);
}

TEST_F(ImportColumnFamilyTest, ImportSSTFileWriterFilesWithOverlap) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);

  // file3.sst
  const std::string file3_sst_name = "file3.sst";
  const std::string file3_sst = sst_files_dir_ + file3_sst_name;
  ASSERT_OK(sfw_cf1.Open(file3_sst));
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file2.sst
  const std::string file2_sst_name = "file2.sst";
  const std::string file2_sst = sst_files_dir_ + file2_sst_name;
  ASSERT_OK(sfw_cf1.Open(file2_sst));
  for (int i = 0; i < 100; i += 2) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_overwrite1"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file1a.sst
  const std::string file1a_sst_name = "file1a.sst";
  const std::string file1a_sst = sst_files_dir_ + file1a_sst_name;
  ASSERT_OK(sfw_cf1.Open(file1a_sst));
  for (int i = 0; i < 52; i += 4) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_overwrite2"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file1b.sst
  const std::string file1b_sst_name = "file1b.sst";
  const std::string file1b_sst = sst_files_dir_ + file1b_sst_name;
  ASSERT_OK(sfw_cf1.Open(file1b_sst));
  for (int i = 52; i < 100; i += 4) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_overwrite2"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file0a.sst
  const std::string file0a_sst_name = "file0a.sst";
  const std::string file0a_sst = sst_files_dir_ + file0a_sst_name;
  ASSERT_OK(sfw_cf1.Open(file0a_sst));
  for (int i = 0; i < 100; i += 16) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_overwrite3"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file0b.sst
  const std::string file0b_sst_name = "file0b.sst";
  const std::string file0b_sst = sst_files_dir_ + file0b_sst_name;
  ASSERT_OK(sfw_cf1.Open(file0b_sst));
  for (int i = 0; i < 100; i += 16) {
    ASSERT_OK(sfw_cf1.Put(Key(i), Key(i) + "_overwrite4"));
  }
  ASSERT_OK(sfw_cf1.Finish());

  // Import sst files and verify
  ExportImportFilesMetaData metadata;
  metadata.files.push_back(
      LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 3, 10, 19));
  metadata.files.push_back(
      LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 2, 20, 29));
  metadata.files.push_back(
      LiveFileMetaDataInit(file1a_sst_name, sst_files_dir_, 1, 30, 34));
  metadata.files.push_back(
      LiveFileMetaDataInit(file1b_sst_name, sst_files_dir_, 1, 35, 39));
  metadata.files.push_back(
      LiveFileMetaDataInit(file0a_sst_name, sst_files_dir_, 0, 40, 49));
  metadata.files.push_back(
      LiveFileMetaDataInit(file0b_sst_name, sst_files_dir_, 0, 50, 59));
  metadata.db_comparator_name = options.comparator->Name();

  ASSERT_OK(db_->CreateColumnFamilyWithImport(
      options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value));
    if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }

  for (int i = 0; i < 100; i += 5) {
    ASSERT_OK(
        db_->Put(WriteOptions(), import_cfh_, Key(i), Key(i) + "_overwrite5"));
  }

  // Flush and check again
  ASSERT_OK(db_->Flush(FlushOptions(), import_cfh_));
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value));
    if (i % 5 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite5");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }

  // Compact and check again.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), import_cfh_, nullptr, nullptr));
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value));
    if (i % 5 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite5");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }
}

TEST_F(ImportColumnFamilyTest, ImportSSTFileWriterFilesWithRangeTombstone) {
  // Test for a bug where import file's smallest and largest key did not
  // consider range tombstone.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  // cf1.sst
  const std::string cf1_sst_name = "cf1.sst";
  const std::string cf1_sst = sst_files_dir_ + cf1_sst_name;
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K1", "V1"));
  ASSERT_OK(sfw_cf1.Put("K2", "V2"));
  ASSERT_OK(sfw_cf1.DeleteRange("K3", "K4"));
  ASSERT_OK(sfw_cf1.DeleteRange("K7", "K9"));

  ASSERT_OK(sfw_cf1.Finish());

  // Import sst file corresponding to cf1 onto a new cf and verify
  ExportImportFilesMetaData metadata;
  metadata.files.push_back(
      LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 0, 19));
  metadata.db_comparator_name = options.comparator->Name();

  ASSERT_OK(db_->CreateColumnFamilyWithImport(
      options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  ColumnFamilyMetaData import_cf_meta;
  db_->GetColumnFamilyMetaData(import_cfh_, &import_cf_meta);
  ASSERT_EQ(import_cf_meta.file_count, 1);
  const SstFileMetaData* file_meta = nullptr;
  for (const auto& level_meta : import_cf_meta.levels) {
    if (!level_meta.files.empty()) {
      file_meta = level_meta.files.data();
      break;
    }
  }
  ASSERT_TRUE(file_meta != nullptr);
  InternalKey largest;
  largest.DecodeFrom(file_meta->largest);
  ASSERT_EQ(largest.user_key(), "K9");

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K1", &value));
  ASSERT_EQ(value, "V1");
  ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K2", &value));
  ASSERT_EQ(value, "V2");
  ASSERT_OK(db_->DropColumnFamily(import_cfh_));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
  import_cfh_ = nullptr;
}

TEST_F(ImportColumnFamilyTest, ImportExportedSSTFromAnotherCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1));

  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite"));
  }

  // Flush to create L0 file.
  ASSERT_OK(Flush(1));
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite2"));
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              *metadata_ptr_, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  import_options.move_files = true;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "yoyo", import_options,
                                              *metadata_ptr_, &import_cfh2_));
  ASSERT_NE(import_cfh2_, nullptr);
  delete metadata_ptr_;
  metadata_ptr_ = nullptr;

  std::string value1;
  std::string value2;

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Get(1, Key(i)), value1);
  }

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2));
    ASSERT_EQ(Get(1, Key(i)), value2);
  }

  // Modify keys in cf1 and verify.
  for (int i = 0; i < 25; i++) {
    ASSERT_OK(db_->Delete(WriteOptions(), import_cfh_, Key(i)));
  }
  for (int i = 25; i < 50; i++) {
    ASSERT_OK(
        db_->Put(WriteOptions(), import_cfh_, Key(i), Key(i) + "_overwrite3"));
  }
  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(
        db_->Get(ReadOptions(), import_cfh_, Key(i), &value1).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Key(i) + "_overwrite3", value1);
  }
  for (int i = 50; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Key(i) + "_overwrite2", value1);
  }

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2));
    ASSERT_EQ(Get(1, Key(i)), value2);
  }

  // Compact and check again.
  ASSERT_OK(db_->Flush(FlushOptions(), import_cfh_));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), import_cfh_, nullptr, nullptr));

  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(
        db_->Get(ReadOptions(), import_cfh_, Key(i), &value1).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Key(i) + "_overwrite3", value1);
  }
  for (int i = 50; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Key(i) + "_overwrite2", value1);
  }

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2));
    ASSERT_EQ(Get(1, Key(i)), value2);
  }
}

TEST_F(ImportColumnFamilyTest, ImportExportedSSTFromAnotherDB) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1));

  // Compact to create a L1 file.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite"));
  }

  // Flush to create L0 file.
  ASSERT_OK(Flush(1));

  for (int i = 0; i < 25; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite2"));
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  // Create a new db and import the files.
  std::unique_ptr<DB> db_copy;
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                  ImportColumnFamilyOptions(),
                                                  *metadata_ptr_, &cfh));
  ASSERT_NE(cfh, nullptr);

  for (int i = 0; i < 100; ++i) {
    std::string value;
    ASSERT_OK(db_copy->Get(ReadOptions(), cfh, Key(i), &value));
    ASSERT_EQ(Get(1, Key(i)), value);
  }
  ASSERT_OK(db_copy->DropColumnFamily(cfh));
  ASSERT_OK(db_copy->DestroyColumnFamilyHandle(cfh));
  db_copy.reset();
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
}

TEST_F(ImportColumnFamilyTest,
       ImportExportedSSTFromAnotherCFWithRangeTombstone) {
  // Test for a bug where import file's smallest and largest key did not
  // consider range tombstone.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 10; i < 20; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1 /* cf */));
  MoveFilesToLevel(1 /* level */, 1 /* cf */);
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), handles_[1], Key(0), Key(25)));
  ASSERT_OK(Put(1, Key(1), "t"));
  ASSERT_OK(Flush(1));
  // Tests importing a range tombstone only file
  ASSERT_OK(db_->DeleteRange(WriteOptions(), handles_[1], Key(0), Key(2)));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              *metadata_ptr_, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  import_options.move_files = true;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "yoyo", import_options,
                                              *metadata_ptr_, &import_cfh2_));
  ASSERT_NE(import_cfh2_, nullptr);
  delete metadata_ptr_;
  metadata_ptr_ = nullptr;

  std::string value1;
  std::string value2;
  ReadOptions ro_latest;
  ReadOptions ro_snapshot;
  ro_snapshot.snapshot = snapshot;

  for (int i = 10; i < 20; ++i) {
    ASSERT_TRUE(db_->Get(ro_latest, import_cfh_, Key(i), &value1).IsNotFound());
    ASSERT_OK(db_->Get(ro_snapshot, import_cfh_, Key(i), &value1));
    ASSERT_EQ(Get(1, Key(i), snapshot), value1);
  }
  ASSERT_TRUE(db_->Get(ro_latest, import_cfh_, Key(1), &value1).IsNotFound());

  for (int i = 10; i < 20; ++i) {
    ASSERT_TRUE(
        db_->Get(ro_latest, import_cfh2_, Key(i), &value1).IsNotFound());

    ASSERT_OK(db_->Get(ro_snapshot, import_cfh2_, Key(i), &value2));
    ASSERT_EQ(Get(1, Key(i), snapshot), value2);
  }
  ASSERT_TRUE(db_->Get(ro_latest, import_cfh2_, Key(1), &value1).IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(ImportColumnFamilyTest, LevelFilesOverlappingAtEndpoints) {
  // Imports a column family containing a level where two files overlap at their
  // endpoints. "Overlap" means the largest user key in one file is the same as
  // the smallest user key in the second file.
  const int kFileBytes = 128 << 10;  // 128KB
  const int kValueBytes = 1 << 10;   // 1KB
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 2;
  CreateAndReopenWithCF({"koko"}, options);

  Random rnd(301);
  // Every key is snapshot protected to ensure older versions will not be
  // dropped during compaction.
  std::vector<const Snapshot*> snapshots;
  snapshots.reserve(kFileBytes / kValueBytes * kNumFiles);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      ASSERT_OK(Put(1, "key", value));
      snapshots.push_back(db_->GetSnapshot());
    }
    ASSERT_OK(Flush(1));
  }

  // Compact to create overlapping L1 files.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  // Create a new db and import the files.
  std::unique_ptr<DB> db_copy;
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                  ImportColumnFamilyOptions(),
                                                  *metadata_ptr_, &cfh));
  ASSERT_NE(cfh, nullptr);

  {
    std::string value;
    ASSERT_OK(db_copy->Get(ReadOptions(), cfh, "key", &value));
  }
  ASSERT_OK(db_copy->DropColumnFamily(cfh));
  ASSERT_OK(db_copy->DestroyColumnFamilyHandle(cfh));
  db_copy.reset();
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
  for (const Snapshot* snapshot : snapshots) {
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(ImportColumnFamilyTest, ImportColumnFamilyNegativeTest) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  {
    // Create column family with existing cf name.
    ExportImportFilesMetaData metadata;
    metadata.db_comparator_name = options.comparator->Name();
    Status s = db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "koko",
                                                 ImportColumnFamilyOptions(),
                                                 metadata, &import_cfh_);
    ASSERT_TRUE(std::strstr(s.getState(), "Column family already exists"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with no files specified.
    ExportImportFilesMetaData metadata;
    metadata.db_comparator_name = options.comparator->Name();
    Status s = db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                 ImportColumnFamilyOptions(),
                                                 metadata, &import_cfh_);
    ASSERT_TRUE(std::strstr(s.getState(), "The list of files is empty"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with overlapping keys in sst files.
    ExportImportFilesMetaData metadata;
    SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Finish());
    const std::string file2_sst_name = "file2.sst";
    const std::string file2_sst = sst_files_dir_ + file2_sst_name;
    ASSERT_OK(sfw_cf1.Open(file2_sst));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Put("K3", "V3"));
    ASSERT_OK(sfw_cf1.Finish());

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.files.push_back(
        LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_NOK(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                 ImportColumnFamilyOptions(),
                                                 metadata, &import_cfh_));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with a mismatching comparator, should fail with appropriate error.
    ExportImportFilesMetaData metadata;
    Options mismatch_options = CurrentOptions();
    mismatch_options.comparator = ReverseBytewiseComparator();
    SstFileWriter sfw_cf1(EnvOptions(), mismatch_options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Finish());

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = mismatch_options.comparator->Name();

    Status s = db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "coco",
                                                 ImportColumnFamilyOptions(),
                                                 metadata, &import_cfh_);
    ASSERT_TRUE(std::strstr(s.getState(), "Comparator name mismatch"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with non existent sst file should fail with appropriate error
    ExportImportFilesMetaData metadata;
    SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Finish());
    const std::string file3_sst_name = "file3.sst";

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.files.push_back(
        LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    Status s = db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                 ImportColumnFamilyOptions(),
                                                 metadata, &import_cfh_);
    ASSERT_TRUE(std::strstr(s.getState(), "No such file or directory"));
    ASSERT_EQ(import_cfh_, nullptr);

    // Test successful import after a failure with the same CF name. Ensures
    // there is no side effect with CF when there is a failed import
    metadata.files.pop_back();
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);
  }
}

TEST_F(ImportColumnFamilyTest, ImportMultiColumnFamilyTest) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1));

  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite"));
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  Checkpoint* checkpoint1;
  Checkpoint* checkpoint2;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint1));
  ASSERT_OK(checkpoint1->ExportColumnFamily(handles_[1], export_files_dir_,
                                            &metadata_ptr_));

  // Create a new db and import the files.
  std::unique_ptr<DB> db_copy;
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* copy_cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamily(options, "koko", &copy_cfh));
  WriteOptions wo;
  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(db_copy->Put(wo, copy_cfh, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(db_copy->Flush(FlushOptions()));
  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(db_copy->Put(wo, copy_cfh, Key(i), Key(i) + "_overwrite"));
  }
  ASSERT_OK(db_copy->Flush(FlushOptions()));
  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(db_copy->Put(wo, copy_cfh, Key(i), Key(i) + "_overwrite2"));
  }
  ASSERT_OK(db_copy->Flush(FlushOptions()));

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Checkpoint::Create(db_copy.get(), &checkpoint2));
  ASSERT_OK(checkpoint2->ExportColumnFamily(copy_cfh, export_files_dir2_,
                                            &metadata_ptr2_));

  ASSERT_NE(metadata_ptr_, nullptr);
  ASSERT_NE(metadata_ptr2_, nullptr);
  delete checkpoint1;
  delete checkpoint2;
  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;

  std::vector<const ExportImportFilesMetaData*> metadatas = {metadata_ptr_,
                                                             metadata_ptr2_};
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              metadatas, &import_cfh_));

  std::string value1;
  std::string value2;
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Get(1, Key(i)), value1);
  }

  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_OK(db_copy->Get(ReadOptions(), copy_cfh, Key(i), &value2));
    ASSERT_EQ(value1, value2);
  }

  ASSERT_OK(db_copy->DropColumnFamily(copy_cfh));
  ASSERT_OK(db_copy->DestroyColumnFamilyHandle(copy_cfh));
  db_copy.reset();
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
}

TEST_F(ImportColumnFamilyTest, ImportMultiColumnFamilyWithOverlap) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }

  Checkpoint* checkpoint1;
  Checkpoint* checkpoint2;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint1));
  ASSERT_OK(checkpoint1->ExportColumnFamily(handles_[1], export_files_dir_,
                                            &metadata_ptr_));

  // Create a new db and import the files.
  std::unique_ptr<DB> db_copy;
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* copy_cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamily(options, "koko", &copy_cfh));
  WriteOptions wo;
  for (int i = 50; i < 150; ++i) {
    ASSERT_OK(db_copy->Put(wo, copy_cfh, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(db_copy->Flush(FlushOptions()));

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Checkpoint::Create(db_copy.get(), &checkpoint2));
  ASSERT_OK(checkpoint2->ExportColumnFamily(copy_cfh, export_files_dir2_,
                                            &metadata_ptr2_));

  ASSERT_NE(metadata_ptr_, nullptr);
  ASSERT_NE(metadata_ptr2_, nullptr);
  delete checkpoint1;
  delete checkpoint2;
  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;

  std::vector<const ExportImportFilesMetaData*> metadatas = {metadata_ptr_,
                                                             metadata_ptr2_};

  ASSERT_EQ(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              metadatas, &import_cfh_),
            Status::InvalidArgument("CFs have overlapping ranges"));

  ASSERT_OK(db_copy->DropColumnFamily(copy_cfh));
  ASSERT_OK(db_copy->DestroyColumnFamilyHandle(copy_cfh));
  db_copy.reset();
  ASSERT_OK(DestroyDir(env_, dbname_ + "/db_copy"));
}

TEST_F(ImportColumnFamilyTest, ImportMultiColumnFamilySeveralFilesWithOverlap) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  const std::string file1_sst_name = "file1.sst";
  const std::string file1_sst = sst_files_dir_ + file1_sst_name;
  ASSERT_OK(sfw_cf1.Open(file1_sst));
  ASSERT_OK(sfw_cf1.Put("K1", "V1"));
  ASSERT_OK(sfw_cf1.Put("K2", "V2"));
  ASSERT_OK(sfw_cf1.Finish());

  SstFileWriter sfw_cf2(EnvOptions(), options, handles_[1]);
  const std::string file2_sst_name = "file2.sst";
  const std::string file2_sst = sst_files_dir_ + file2_sst_name;
  ASSERT_OK(sfw_cf2.Open(file2_sst));
  ASSERT_OK(sfw_cf2.Put("K2", "V2"));
  ASSERT_OK(sfw_cf2.Put("K3", "V3"));
  ASSERT_OK(sfw_cf2.Finish());

  ColumnFamilyHandle* second_cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "toto", &second_cfh));

  SstFileWriter sfw_cf3(EnvOptions(), options, second_cfh);
  const std::string file3_sst_name = "file3.sst";
  const std::string file3_sst = sst_files_dir_ + file3_sst_name;
  ASSERT_OK(sfw_cf3.Open(file3_sst));
  ASSERT_OK(sfw_cf3.Put("K3", "V3"));
  ASSERT_OK(sfw_cf3.Put("K4", "V4"));
  ASSERT_OK(sfw_cf3.Finish());

  SstFileWriter sfw_cf4(EnvOptions(), options, second_cfh);
  const std::string file4_sst_name = "file4.sst";
  const std::string file4_sst = sst_files_dir_ + file4_sst_name;
  ASSERT_OK(sfw_cf4.Open(file4_sst));
  ASSERT_OK(sfw_cf4.Put("K4", "V4"));
  ASSERT_OK(sfw_cf4.Put("K5", "V5"));
  ASSERT_OK(sfw_cf4.Finish());

  ExportImportFilesMetaData metadata1;
  ExportImportFilesMetaData metadata2;
  metadata1.files.push_back(
      LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 1, 2));
  metadata1.files.push_back(
      LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 1, 3, 4));
  metadata1.db_comparator_name = options.comparator->Name();
  metadata2.files.push_back(
      LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 1, 1, 2));
  metadata2.files.push_back(
      LiveFileMetaDataInit(file4_sst_name, sst_files_dir_, 1, 3, 4));
  metadata2.db_comparator_name = options.comparator->Name();

  std::vector<const ExportImportFilesMetaData*> metadatas{&metadata1,
                                                          &metadata2};

  ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                              ImportColumnFamilyOptions(),
                                              metadatas, &import_cfh_),
            Status::InvalidArgument("CFs have overlapping ranges"));
  ASSERT_EQ(import_cfh_, nullptr);

  ASSERT_OK(db_->DropColumnFamily(second_cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_cfh));
}

TEST_F(ImportColumnFamilyTest, AssignEpochNumberToMultipleCF) {
  // Test ingesting CFs where L0 files could have the same epoch number.
  Options options = CurrentOptions();
  options.level_compaction_dynamic_level_bytes = true;
  options.max_background_jobs = 8;
  // Always allow parallel compaction
  options.soft_pending_compaction_bytes_limit = 10;
  env_->SetBackgroundThreads(2, Env::LOW);
  env_->SetBackgroundThreads(0, Env::BOTTOM);
  CreateAndReopenWithCF({"CF1", "CF2"}, options);

  // CF1:
  // L6: [0, 99], [100, 199]
  // CF2:
  // L6: [1000, 1099], [1100, 1199]
  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
    ASSERT_OK(Put(2, Key(1000 + i), Key(1000 + i) + "_val"));
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
    ASSERT_OK(Put(2, Key(1000 + i), Key(1000 + i) + "_val"));
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));
  MoveFilesToLevel(6, 1);
  MoveFilesToLevel(6, 2);

  // CF1:
  // level 0 epoch: 5 file num 30 smallest key000010 - key000019
  // level 0 epoch: 4 file num 27 smallest key000000 - key000009
  // level 0 epoch: 3 file num 23 smallest key000100 - key000199
  // level 6 epoch: 2 file num 20 smallest key000000 - key000099
  // level 6 epoch: 1 file num 17 smallest key000100 - key000199
  // CF2:
  // level 0 epoch: 5 file num 31 smallest key001010 - key001019
  // level 0 epoch: 4 file num 28 smallest key001000 - key001009
  // level 0 epoch: 3 file num 25 smallest key001020 - key001029
  // level 6 epoch: 2 file num 21 smallest key001000 - key001099
  // level 6 epoch: 1 file num 18 smallest key001100 - key001199
  for (int i = 100; i < 200; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1));
  for (int i = 20; i < 30; ++i) {
    ASSERT_OK(Put(2, Key(i + 1000), Key(i + 1000) + "_val"));
  }
  ASSERT_OK(Flush(2));

  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
    ASSERT_OK(Put(2, Key(i + 1000), Key(i + 1000) + "_val"));
    if (i % 10 == 9) {
      ASSERT_OK(Flush(1));
      ASSERT_OK(Flush(2));
    }
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));

  // Create a CF by importing these two CF1 and CF2.
  // Then two compactions will be triggerred, one to compact from L0
  // to L6 (files #23 and #17), and another to do intra-L0 compaction
  // for the rest of the L0 files. Before a bug fix, we used to
  // directly use the epoch numbers from the ingested files in the new CF.
  // This means different files from different CFs can have the same epoch
  // number. If the intra-L0 compaction finishes first, it can cause a
  // corruption where two L0 files can have the same epoch number but
  // with overlapping key range.
  Checkpoint* checkpoint1;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint1));
  ASSERT_OK(checkpoint1->ExportColumnFamily(handles_[1], export_files_dir_,
                                            &metadata_ptr_));
  ASSERT_OK(checkpoint1->ExportColumnFamily(handles_[2], export_files_dir2_,
                                            &metadata_ptr2_));
  ASSERT_NE(metadata_ptr_, nullptr);
  ASSERT_NE(metadata_ptr2_, nullptr);

  std::atomic_int compaction_counter = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun",
      [&compaction_counter](void*) {
        compaction_counter++;
        if (compaction_counter == 1) {
          // Wait for the next compaction to finish
          TEST_SYNC_POINT("WaitForSecondCompaction");
        }
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction:AfterCompaction",
        "WaitForSecondCompaction"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;
  std::vector<const ExportImportFilesMetaData*> metadatas = {metadata_ptr_,
                                                             metadata_ptr2_};
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "CF3", import_options,
                                              metadatas, &import_cfh_));
  WaitForCompactOptions o;
  ASSERT_OK(db_->WaitForCompact(o));
  delete checkpoint1;
}

TEST_F(ImportColumnFamilyTest, ImportAndGetSstSize) {
  Options options = CurrentOptions();
  std::shared_ptr<SstFileManager> sst_manager;
  sst_manager.reset(NewSstFileManager(options.env));
  options.sst_file_manager = sst_manager;
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  // cf1.sst
  const std::string cf1_sst_name = "cf1.sst";
  const std::string cf1_sst = sst_files_dir_ + cf1_sst_name;
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K1", "V1"));
  ASSERT_OK(sfw_cf1.Put("K2", "V2"));
  ASSERT_OK(sfw_cf1.Finish());
  {
    // Import sst file corresponding to cf1 onto a new cf and verify
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(
        LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);

    // sst size
    ASSERT_GT(sst_manager->GetTotalSize(), 1 << 10);

    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K1", &value));
    ASSERT_EQ(value, "V1");
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, "K2", &value));
    ASSERT_EQ(value, "V2");
    ASSERT_OK(db_->DropColumnFamily(import_cfh_));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
    import_cfh_ = nullptr;
  }
  // Import sst files exported and verify sst size
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_val"));
  }
  ASSERT_OK(Flush(1));

  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(1, Key(i), Key(i) + "_overwrite"));
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto",
                                              ImportColumnFamilyOptions(),
                                              *metadata_ptr_, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);
  delete metadata_ptr_;
  metadata_ptr_ = nullptr;
  std::string value1;
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), import_cfh_, Key(i), &value1));
    ASSERT_EQ(Get(1, Key(i)), value1);
  }
  ASSERT_GT(sst_manager->GetTotalSize(), 8 << 10);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
