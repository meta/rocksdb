//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/mock_table.h"

#include "db/dbformat.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "port/port.h"
#include "rocksdb/table_properties.h"
#include "table/get_context.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE::mock {

KVVector MakeMockFile(std::initializer_list<KVPair> l) { return KVVector(l); }

void SortKVVector(KVVector* kv_vector, const Comparator* ucmp) {
  InternalKeyComparator icmp(ucmp);
  std::sort(kv_vector->begin(), kv_vector->end(),
            [icmp](const KVPair& a, const KVPair& b) -> bool {
              return icmp.Compare(a.first, b.first) < 0;
            });
}

class MockTableIterator : public InternalIterator {
 public:
  explicit MockTableIterator(const KVVector& table) : table_(table) {
    itr_ = table_.end();
  }

  bool Valid() const override { return itr_ != table_.end(); }

  void SeekToFirst() override { itr_ = table_.begin(); }

  void SeekToLast() override {
    itr_ = table_.end();
    --itr_;
  }

  void Seek(const Slice& target) override {
    KVPair target_pair(target.ToString(), "");
    InternalKeyComparator icmp(BytewiseComparator());
    itr_ = std::lower_bound(table_.begin(), table_.end(), target_pair,
                            [icmp](const KVPair& a, const KVPair& b) -> bool {
                              return icmp.Compare(a.first, b.first) < 0;
                            });
  }

  void SeekForPrev(const Slice& target) override {
    KVPair target_pair(target.ToString(), "");
    InternalKeyComparator icmp(BytewiseComparator());
    itr_ = std::upper_bound(table_.begin(), table_.end(), target_pair,
                            [icmp](const KVPair& a, const KVPair& b) -> bool {
                              return icmp.Compare(a.first, b.first) < 0;
                            });
    Prev();
  }

  void Next() override { ++itr_; }

  void Prev() override {
    if (itr_ == table_.begin()) {
      itr_ = table_.end();
    } else {
      --itr_;
    }
  }

  Slice key() const override { return Slice(itr_->first); }

  Slice value() const override { return Slice(itr_->second); }

  Status status() const override { return Status::OK(); }

 private:
  const KVVector& table_;
  KVVector::const_iterator itr_;
};

class MockTableBuilder : public TableBuilder {
 public:
  MockTableBuilder(uint32_t id, MockTableFileSystem* file_system,
                   MockTableFactory::MockCorruptionMode corrupt_mode =
                       MockTableFactory::kCorruptNone,
                   size_t key_value_size = 1)
      : id_(id),
        file_system_(file_system),
        corrupt_mode_(corrupt_mode),
        key_value_size_(key_value_size) {
    table_ = MakeMockFile({});
  }

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MockTableBuilder() override = default;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override {
    if (corrupt_mode_ == MockTableFactory::kCorruptValue) {
      // Corrupt the value
      table_.push_back({key.ToString(), value.ToString() + " "});
      corrupt_mode_ = MockTableFactory::kCorruptNone;
    } else if (corrupt_mode_ == MockTableFactory::kCorruptKey) {
      table_.push_back({key.ToString() + " ", value.ToString()});
      corrupt_mode_ = MockTableFactory::kCorruptNone;
    } else if (corrupt_mode_ == MockTableFactory::kCorruptReorderKey) {
      if (prev_key_.empty()) {
        prev_key_ = key.ToString();
        prev_value_ = value.ToString();
      } else {
        table_.push_back({key.ToString(), value.ToString()});
        table_.push_back({prev_key_, prev_value_});
        corrupt_mode_ = MockTableFactory::kCorruptNone;
      }
    } else {
      table_.push_back({key.ToString(), value.ToString()});
    }
  }

  // Return non-ok iff some error has been detected.
  Status status() const override { return Status::OK(); }

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override { return IOStatus::OK(); }

  Status Finish() override {
    MutexLock lock_guard(&file_system_->mutex);
    file_system_->files.insert({id_, table_});
    return Status::OK();
  }

  void Abandon() override {}

  uint64_t NumEntries() const override { return table_.size(); }

  uint64_t FileSize() const override { return table_.size() * key_value_size_; }

  TableProperties GetTableProperties() const override {
    return TableProperties();
  }

  // Get file checksum
  std::string GetFileChecksum() const override { return kUnknownFileChecksum; }
  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override {
    return kUnknownFileChecksumFuncName;
  }

 private:
  uint32_t id_;
  std::string prev_key_;
  std::string prev_value_;
  MockTableFileSystem* file_system_;
  int corrupt_mode_;
  KVVector table_;
  size_t key_value_size_;
};

InternalIterator* MockTableReader::NewIterator(
    const ReadOptions&, const SliceTransform* /* prefix_extractor */,
    Arena* /*arena*/, bool /*skip_filters*/, TableReaderCaller /*caller*/,
    size_t /*compaction_readahead_size*/, bool /* allow_unprepared_value */) {
  return new MockTableIterator(table_);
}

Status MockTableReader::Get(const ReadOptions&, const Slice& key,
                            GetContext* get_context,
                            const SliceTransform* /*prefix_extractor*/,
                            bool /*skip_filters*/) {
  std::unique_ptr<MockTableIterator> iter(new MockTableIterator(table_));
  for (iter->Seek(key); iter->Valid(); iter->Next()) {
    ParsedInternalKey parsed_key;
    Status pik_status =
        ParseInternalKey(iter->key(), &parsed_key, true /* log_err_key */);
    if (!pik_status.ok()) {
      return pik_status;
    }

    bool dont_care __attribute__((__unused__));
    Status read_status;
    bool ret = get_context->SaveValue(parsed_key, iter->value(), &dont_care,
                                      &read_status);
    if (!read_status.ok()) {
      return read_status;
    }
    if (!ret) {
      break;
    }
  }
  return Status::OK();
}

MockTableFactory::MockTableFactory()
    : next_id_(1), corrupt_mode_(MockTableFactory::kCorruptNone) {}

Status MockTableFactory::NewTableReader(
    const ReadOptions& /*ro*/,
    const TableReaderOptions& /*table_reader_options*/,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t /*file_size*/,
    std::unique_ptr<TableReader>* table_reader,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  uint32_t id;
  Status s = GetIDFromFile(file.get(), &id);
  if (!s.ok()) {
    return s;
  }

  MutexLock lock_guard(&file_system_.mutex);

  auto it = file_system_.files.find(id);
  if (it == file_system_.files.end()) {
    return Status::IOError("Mock file not found");
  }

  table_reader->reset(new MockTableReader(it->second));

  return Status::OK();
}

TableBuilder* MockTableFactory::NewTableBuilder(
    const TableBuilderOptions& /*table_builder_options*/,
    WritableFileWriter* file) const {
  uint32_t id;
  Status s = GetAndWriteNextID(file, &id);
  assert(s.ok());

  return new MockTableBuilder(id, &file_system_, corrupt_mode_,
                              key_value_size_);
}

Status MockTableFactory::CreateMockTable(Env* env, const std::string& fname,
                                         KVVector file_contents) {
  std::unique_ptr<WritableFileWriter> file_writer;
  Status s = WritableFileWriter::Create(env->GetFileSystem(), fname,
                                        FileOptions(), &file_writer, nullptr);
  if (!s.ok()) {
    return s;
  }
  uint32_t id;
  s = GetAndWriteNextID(file_writer.get(), &id);
  if (s.ok()) {
    file_system_.files.insert({id, std::move(file_contents)});
  }
  return s;
}

Status MockTableFactory::GetAndWriteNextID(WritableFileWriter* file,
                                           uint32_t* next_id) const {
  *next_id = next_id_.fetch_add(1);
  char buf[4];
  EncodeFixed32(buf, *next_id);
  return file->Append(Slice(buf, 4), IOOptions());
}

Status MockTableFactory::GetIDFromFile(RandomAccessFileReader* file,
                                       uint32_t* id) const {
  char buf[4];
  Slice result;
  Status s = file->Read(0, 4, IOOptions(), &result, buf, nullptr);
  assert(result.size() == 4);
  *id = DecodeFixed32(buf);
  return s;
}

void MockTableFactory::AssertSingleFile(const KVVector& file_contents) {
  ASSERT_EQ(file_system_.files.size(), 1U);
  ASSERT_EQ(file_contents, file_system_.files.begin()->second);
}

void MockTableFactory::AssertLatestFiles(
    const std::vector<KVVector>& files_contents) {
  ASSERT_GE(file_system_.files.size(), files_contents.size());
  auto it = file_system_.files.rbegin();
  for (auto expect = files_contents.rbegin(); expect != files_contents.rend();
       expect++, it++) {
    ASSERT_TRUE(it != file_system_.files.rend());
    if (*expect != it->second) {
      std::cout << "Wrong content! Content of file, expect:" << std::endl;
      for (const auto& kv : *expect) {
        ParsedInternalKey ikey;
        std::string key;
        std::string value;
        std::tie(key, value) = kv;
        ASSERT_OK(ParseInternalKey(Slice(key), &ikey, true /* log_err_key */));
        std::cout << ikey.DebugString(true, false) << " -> " << value
                  << std::endl;
      }
      std::cout << "actual:" << std::endl;
      for (const auto& kv : it->second) {
        ParsedInternalKey ikey;
        std::string key;
        std::string value;
        std::tie(key, value) = kv;
        ASSERT_OK(ParseInternalKey(Slice(key), &ikey, true /* log_err_key */));
        std::cout << ikey.DebugString(true, false) << " -> " << value
                  << std::endl;
      }
      FAIL();
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE::mock
