//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/simple_external_table_factory.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#include "db/dbformat.h"
#include "file/file_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint64_t kSimpleExternalTableMagic = 0x31544c4241545845ULL;
constexpr size_t kSimpleExternalTableFooterSize =
    3 * sizeof(uint64_t) + sizeof(uint32_t);

template <ExternalTableMode Mode>
class SimpleExternalTableIterator final : public ExternalTableIteratorBase {
 public:
  SimpleExternalTableIterator(const SimpleExternalTableEntries& entries,
                              const CompareInterface* comparator, Status status)
      : entries_(entries),
        comparator_(comparator),
        position_(entries.size()),
        status_(std::move(status)) {
    TEST_SYNC_POINT_CALLBACK("SimpleExternalTableIterator::Constructor",
                             &status_);
  }

  bool Valid() const override { return position_ < entries_.size(); }

  void SeekToFirst() override {
    if (scan_options_ != nullptr) {
      status_ = Status::InvalidArgument();
      position_ = entries_.size();
    } else if (status_.ok()) {
      position_ = entries_.empty() ? entries_.size() : 0;
    }
  }

  void SeekToLast() override {
    if (scan_options_ != nullptr) {
      status_ = Status::InvalidArgument();
      position_ = entries_.size();
    } else if (status_.ok()) {
      position_ = entries_.empty() ? entries_.size() : entries_.size() - 1;
    }
  }

  void Seek(const Slice& target) override {
    if (!status_.ok()) {
      return;
    }
    auto iter =
        std::lower_bound(entries_.begin(), entries_.end(), target,
                         [this](const auto& entry, const Slice& key) {
                           return comparator_->Compare(entry.first, key) < 0;
                         });
    position_ = static_cast<size_t>(iter - entries_.begin());
    end_of_file_ = !Valid();
    if (scan_options_ != nullptr) {
      if (scan_index_ >= num_options_ ||
          target != scan_options_[scan_index_].range.start.value()) {
        status_ = Status::InvalidArgument();
        position_ = entries_.size();
        return;
      }
      if (Valid() && scan_options_[scan_index_].range.limit.has_value() &&
          comparator_->Compare(
              key(), scan_options_[scan_index_].range.limit.value()) >= 0) {
        position_ = entries_.size();
        end_of_file_ = false;
      }
      ++scan_index_;
    }
  }

  void SeekForPrev(const Slice& target) override {
    if (!status_.ok()) {
      return;
    }
    auto iter =
        std::upper_bound(entries_.begin(), entries_.end(), target,
                         [this](const Slice& key, const auto& entry) {
                           return comparator_->Compare(key, entry.first) < 0;
                         });
    position_ = iter == entries_.begin()
                    ? entries_.size()
                    : static_cast<size_t>(iter - entries_.begin() - 1);
  }

  void Next() override {
    if (Valid()) {
      ++position_;
      end_of_file_ = !Valid();
      if (Valid() && scan_options_ != nullptr &&
          scan_options_[scan_index_ - 1].range.limit.has_value() &&
          comparator_->Compare(
              key(), scan_options_[scan_index_ - 1].range.limit.value()) >= 0) {
        position_ = entries_.size();
        end_of_file_ = false;
      }
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    Next();
    result->key = Valid() ? key() : Slice();
    result->bound_check_result = Valid() && scan_options_ != nullptr
                                     ? IterBoundCheck::kInbound
                                 : scan_options_ != nullptr && !end_of_file_
                                     ? IterBoundCheck::kOutOfBound
                                     : IterBoundCheck::kUnknown;
    result->value_prepared = Valid();
    return Valid();
  }

  void Prev() override {
    if (!Valid()) {
      return;
    }
    position_ = position_ == 0 ? entries_.size() : position_ - 1;
  }

  Slice key() const override { return entries_[position_].first; }
  Slice value() const override { return entries_[position_].second; }
  Status status() const override { return status_; }
  bool PrepareValue() override { return Valid(); }
  IterBoundCheck UpperBoundCheckResult() override {
    return Valid() && scan_options_ != nullptr ? IterBoundCheck::kInbound
           : scan_options_ != nullptr && !end_of_file_
               ? IterBoundCheck::kOutOfBound
               : IterBoundCheck::kUnknown;
  }
  void Prepare(const ScanOptions scan_options[], size_t num_options) override {
    scan_options_ = scan_options;
    num_options_ = num_options;
    scan_index_ = 0;
  }

 private:
  const SimpleExternalTableEntries& entries_;
  const CompareInterface* comparator_;
  size_t position_;
  const ScanOptions* scan_options_ = nullptr;
  size_t num_options_ = 0;
  size_t scan_index_ = 0;
  bool end_of_file_ = false;
  Status status_;
};

template <ExternalTableMode Mode>
class SimpleExternalTableBuilder final : public ExternalTableBuilderBase {
 public:
  SimpleExternalTableBuilder(FSWritableFile* file,
                             const WriteOptions& write_options)
      : file_(file) {
    assert(file_ != nullptr);
    status_ = PrepareIOFromWriteOptions(write_options, io_options_);
  }

  ~SimpleExternalTableBuilder() override { status_.PermitUncheckedError(); }

  void Add(const Slice& key, const Slice& value) override {
    if (!status_.ok()) {
      return;
    }
    std::string record;
    PutFixed32(&record, static_cast<uint32_t>(key.size()));
    PutFixed32(&record, static_cast<uint32_t>(value.size()));
    record.append(key.data(), key.size());
    record.append(value.data(), value.size());
    Append(record, /*include_in_checksum=*/true);
  }

  Status status() const override { return status_; }

  Status Finish() override {
    Append(range_deletion_block_, /*include_in_checksum=*/true);
    Append(properties_block_, /*include_in_checksum=*/true);

    std::string footer;
    PutFixed64(&footer, range_deletion_block_.size());
    PutFixed64(&footer, properties_block_.size());
    PutFixed64(&footer, kSimpleExternalTableMagic);
    Append(footer, /*include_in_checksum=*/true);

    std::string checksum;
    PutFixed32(&checksum, contents_checksum_);
    Append(checksum, /*include_in_checksum=*/false);
    return status_;
  }

  void Abandon() override {}
  uint64_t FileSize() const override { return file_size_; }
  Status PutPropertiesBlock(const Slice& block) override {
    Status status;
    TEST_SYNC_POINT_CALLBACK("SimpleExternalTableBuilder::PutPropertiesBlock",
                             &status);
    if (status.ok()) {
      properties_block_.assign(block.data(), block.size());
    }
    return status;
  }
  Status PutRangeDeletionBlock(const Slice& block) override {
    range_deletion_block_.assign(block.data(), block.size());
    return Status::OK();
  }
  TableProperties GetTableProperties() const override {
    TableProperties properties;
    TEST_SYNC_POINT_CALLBACK("SimpleExternalTableBuilder::GetTableProperties",
                             &properties);
    return properties;
  }

 private:
  void Append(const Slice& data, bool include_in_checksum) {
    if (!status_.ok()) {
      return;
    }
    status_ = file_->Append(data, io_options_, /*dbg=*/nullptr);
    if (status_.ok()) {
      file_size_ += data.size();
      if (include_in_checksum) {
        contents_checksum_ =
            crc32c::Extend(contents_checksum_, data.data(), data.size());
      }
    }
  }

  FSWritableFile* file_;
  IOOptions io_options_;
  std::string range_deletion_block_;
  std::string properties_block_;
  uint64_t file_size_ = 0;
  uint32_t contents_checksum_ = 0;
  Status status_;
};

}  // namespace

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::ReadContents(
    const ReadOptions& read_options, std::string* contents) const {
  if (file_size_ > std::numeric_limits<size_t>::max()) {
    return Status::Corruption("Simple external table is too large");
  }
  IOOptions io_options;
  Status status = PrepareIOFromReadOptions(
      read_options, SystemClock::Default().get(), io_options);
  if (!status.ok()) {
    return status;
  }
  contents->resize(static_cast<size_t>(file_size_));
  Slice result;
  status = file_->Read(0, contents->size(), io_options, &result,
                       contents->data(), /*dbg=*/nullptr);
  if (!status.ok()) {
    return status;
  }
  if (result.size() != contents->size()) {
    return Status::Corruption("Truncated simple external table");
  }
  if (result.data() != contents->data()) {
    contents->assign(result.data(), result.size());
  }
  return Status::OK();
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::DecodeFooter(
    const Slice& contents, uint64_t* range_deletion_size,
    uint64_t* properties_size, uint32_t* contents_checksum) const {
  if (contents.size() < kSimpleExternalTableFooterSize) {
    return Status::Corruption("Missing simple external table footer");
  }
  const char* footer =
      contents.data() + contents.size() - kSimpleExternalTableFooterSize;
  *range_deletion_size = DecodeFixed64(footer);
  *properties_size = DecodeFixed64(footer + sizeof(uint64_t));
  const uint64_t magic = DecodeFixed64(footer + 2 * sizeof(uint64_t));
  *contents_checksum = DecodeFixed32(footer + 3 * sizeof(uint64_t));
  const uint64_t footer_offset =
      contents.size() - kSimpleExternalTableFooterSize;
  if (magic != kSimpleExternalTableMagic ||
      *range_deletion_size > footer_offset ||
      *properties_size > footer_offset - *range_deletion_size) {
    return Status::Corruption("Invalid simple external table footer");
  }
  return Status::OK();
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::VerifyContentsChecksum(
    const Slice& contents) const {
  uint64_t range_deletion_size = 0;
  uint64_t properties_size = 0;
  uint32_t contents_checksum = 0;
  Status status = DecodeFooter(contents, &range_deletion_size, &properties_size,
                               &contents_checksum);
  if (!status.ok()) {
    return status;
  }
  const size_t checksummed_size = contents.size() - sizeof(uint32_t);
  const uint32_t actual_checksum =
      crc32c::Value(contents.data(), checksummed_size);
  return actual_checksum == contents_checksum
             ? Status::OK()
             : Status::Corruption("Simple external table checksum mismatch");
}

template <ExternalTableMode Mode>
SimpleExternalTableReader<Mode>::SimpleExternalTableReader(
    const ReadOptions& read_options, const ExternalTableOptions& options,
    std::unique_ptr<FSRandomAccessFile>&& file, uint64_t file_size)
    : file_(std::move(file)),
      file_size_(file_size),
      key_comparator_(Mode == ExternalTableMode::kFull
                          ? options.internal_key_comparator
                          : options.comparator),
      user_comparator_(options.comparator),
      verify_checksums_(read_options.verify_checksums) {
  assert(file_ != nullptr);
  std::string contents;
  status_ = ReadContents(read_options, &contents);
  if (!status_.ok()) {
    return;
  }

  uint64_t range_deletion_size = 0;
  uint64_t properties_size = 0;
  uint32_t contents_checksum = 0;
  status_ = DecodeFooter(contents, &range_deletion_size, &properties_size,
                         &contents_checksum);
  if (!status_.ok()) {
    return;
  }
  properties_offset_ =
      contents.size() - kSimpleExternalTableFooterSize - properties_size;
  properties_block_.assign(contents.data() + properties_offset_,
                           static_cast<size_t>(properties_size));
  const uint64_t range_deletion_offset =
      properties_offset_ - range_deletion_size;
  range_deletion_block_.assign(contents.data() + range_deletion_offset,
                               static_cast<size_t>(range_deletion_size));

  Slice records(contents.data(), static_cast<size_t>(range_deletion_offset));
  while (!records.empty()) {
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    if (!GetFixed32(&records, &key_size) ||
        !GetFixed32(&records, &value_size) || key_size > records.size() ||
        value_size > records.size() - key_size) {
      status_ = Status::Corruption("Invalid simple external table record");
      return;
    }
    std::string key(records.data(), key_size);
    records.remove_prefix(key_size);
    std::string value(records.data(), value_size);
    records.remove_prefix(value_size);
    entries_.emplace_back(std::move(key), std::move(value));
  }
  checksum_status_ = VerifyContentsChecksum(contents);
}

template <ExternalTableMode Mode>
SimpleExternalTableReader<Mode>::~SimpleExternalTableReader() {
  checksum_status_.PermitUncheckedError();
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::status() const {
  return status_;
}

template <ExternalTableMode Mode>
ExternalTableIteratorBase* SimpleExternalTableReader<Mode>::NewIterator(
    const ReadOptions& read_options,
    const SliceTransform* /*prefix_extractor*/) {
  TEST_SYNC_POINT_CALLBACK("SimpleExternalTableReader::NewIterator",
                           const_cast<ReadOptions*>(&read_options));
  Status status =
      read_options.verify_checksums ? checksum_status_ : Status::OK();
  return new SimpleExternalTableIterator<Mode>(entries_, key_comparator_,
                                               std::move(status));
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::Get(
    const ReadOptions& read_options, const Slice& key,
    const SliceTransform* /*prefix_extractor*/,
    typename SimpleExternalTableReader<Mode>::GetArgument result) {
  TEST_SYNC_POINT_CALLBACK("SimpleExternalTableReader::Get",
                           const_cast<ReadOptions*>(&read_options));
  if (read_options.verify_checksums && !checksum_status_.ok()) {
    return checksum_status_;
  }
  auto iter = std::lower_bound(
      entries_.begin(), entries_.end(), key,
      [this](const auto& entry, const Slice& lookup_key) {
        return key_comparator_->Compare(entry.first, lookup_key) < 0;
      });
  if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
    if (iter == entries_.end() ||
        key_comparator_->Compare(iter->first, key) != 0) {
      return Status::NotFound();
    }
    result->PinSelf(iter->second);
    return Status::OK();
  } else {
    bool found = false;
    while (iter != entries_.end() &&
           user_comparator_->EqualWithoutTimestamp(ExtractUserKey(iter->first),
                                                   ExtractUserKey(key))) {
      found = true;
      PinnableSlice value;
      value.PinSelf(iter->second);
      bool continue_reading = false;
      Status status = result->Save(iter->first, &value, &continue_reading);
      if (!status.ok() || !continue_reading) {
        return status;
      }
      ++iter;
    }
    return found ? Status::OK() : Status::NotFound();
  }
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::GetPropertiesBlock(
    std::unique_ptr<char[]>* block, uint64_t* size) {
  Status status;
  if (properties_block_.empty()) {
    *size = 0;
    status = Status::NotSupported();
  } else {
    *block = std::make_unique<char[]>(properties_block_.size());
    memcpy(block->get(), properties_block_.data(), properties_block_.size());
    *size = properties_block_.size();
  }
  TEST_SYNC_POINT_CALLBACK("SimpleExternalTableReader::GetPropertiesBlock",
                           &status);
  return status;
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::GetRangeDeletionBlock(
    std::unique_ptr<char[]>* block, uint64_t* size) {
  if (verify_checksums_ && !checksum_status_.ok()) {
    return checksum_status_;
  }
  if (range_deletion_block_.empty()) {
    *size = 0;
    return Status::NotSupported();
  }
  *block = std::make_unique<char[]>(range_deletion_block_.size());
  memcpy(block->get(), range_deletion_block_.data(),
         range_deletion_block_.size());
  *size = range_deletion_block_.size();
  return Status::OK();
}

template <ExternalTableMode Mode>
std::shared_ptr<const TableProperties>
SimpleExternalTableReader<Mode>::GetTableProperties() const {
  return std::make_shared<TableProperties>();
}

template <ExternalTableMode Mode>
Status SimpleExternalTableReader<Mode>::VerifyChecksum(
    const ReadOptions& read_options) {
  std::string contents;
  Status status = ReadContents(read_options, &contents);
  return status.ok() ? VerifyContentsChecksum(contents) : status;
}

template <ExternalTableMode Mode>
SimpleExternalTableFactoryBase<Mode>::SimpleExternalTableFactoryBase() =
    default;

template <ExternalTableMode Mode>
SimpleExternalTableFactoryBase<Mode>::~SimpleExternalTableFactoryBase() =
    default;

template <ExternalTableMode Mode>
const char* SimpleExternalTableFactoryBase<Mode>::Name() const {
  if constexpr (Mode == ExternalTableMode::kFull) {
    return "SimpleFullExternalTableFactory";
  }
  return "SimpleExternalTableFactory";
}

template <ExternalTableMode Mode>
Status SimpleExternalTableFactoryBase<Mode>::FileIsValidTable(
    const IOOptions& io_options, FSRandomAccessFile& file, uint64_t file_size,
    bool* matches) const {
  *matches = false;
  if (file_size < kSimpleExternalTableFooterSize) {
    return Status::OK();
  }
  uint64_t magic_number;
  Status status = ReadTableMagicNumber(
      io_options, file, file_size - sizeof(uint32_t), &magic_number);
  *matches = status.ok() && magic_number == kSimpleExternalTableMagic;
  return status;
}

template <ExternalTableMode Mode>
Status SimpleExternalTableFactoryBase<Mode>::NewTableReader(
    const ReadOptions& read_options, const std::string& /*file_path*/,
    const ExternalTableOptions& table_options,
    std::unique_ptr<FSRandomAccessFile>&& file, uint64_t file_size,
    std::unique_ptr<ExternalTableReaderBase<Mode>>* table_reader) const {
  if (file == nullptr || (Mode == ExternalTableMode::kFull &&
                          table_options.internal_key_comparator == nullptr)) {
    return Status::InvalidArgument("Missing simple external table arguments");
  }
  TEST_SYNC_POINT_CALLBACK("SimpleExternalTableFactory::NewTableReader:File",
                           file.get());
  auto reader = std::make_unique<SimpleExternalTableReader<Mode>>(
      read_options, table_options, std::move(file), file_size);
  Status status = reader->status();
  if (status.ok()) {
    *table_reader = std::move(reader);
  }
  return status;
}

template <ExternalTableMode Mode>
ExternalTableBuilderBase* SimpleExternalTableFactoryBase<Mode>::NewTableBuilder(
    const ExternalTableBuilderOptions& builder_options,
    const std::string& /*file_path*/, FSWritableFile* file) const {
  if (file == nullptr) {
    return nullptr;
  }
  return new SimpleExternalTableBuilder<Mode>(file,
                                              builder_options.write_options);
}

template <ExternalTableMode Mode>
bool SimpleExternalTableFactoryBase<Mode>::IsDeleteRangeSupported() const {
  return true;
}

template class SimpleExternalTableReader<
    ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
template class SimpleExternalTableReader<ExternalTableMode::kFull>;
template class SimpleExternalTableFactoryBase<
    ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
template class SimpleExternalTableFactoryBase<ExternalTableMode::kFull>;

}  // namespace ROCKSDB_NAMESPACE
