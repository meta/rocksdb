//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "file/writable_file_writer.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "util/defer.h"
#include "util/mutexlock.h"

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int argc, char** argv);
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

namespace ROCKSDB_NAMESPACE {
class FileSystem;
class MemTableRepFactory;
class ObjectLibrary;
class Random;
class SequentialFile;
class SequentialFileReader;

namespace test {

extern const uint32_t kDefaultFormatVersion;
extern const std::set<uint32_t> kFooterFormatVersionsToTest;

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
enum RandomKeyType : char { RANDOM, LARGEST, SMALLEST, MIDDLE };
std::string RandomKey(Random* rnd, int len,
                      RandomKeyType type = RandomKeyType::RANDOM);

enum class UserDefinedTimestampTestMode {
  // Test does not enable user-defined timestamp feature.
  kNone,
  // Test enables user-defined timestamp feature. Write/read with min timestamps
  kNormal,
  // Test enables user-defined timestamp feature. Write/read with min timestamps
  // Set `persist_user_defined_timestamps` to false.
  kStripUserDefinedTimestamp,
};

const std::vector<UserDefinedTimestampTestMode>& GetUDTTestModes();

bool IsUDTEnabled(const UserDefinedTimestampTestMode& test_mode);

bool ShouldPersistUDT(const UserDefinedTimestampTestMode& test_mode);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
Slice CompressibleString(Random* rnd, double compressed_to_fraction, int len,
                         std::string* dst);

inline std::string CompressibleString(Random* rnd,
                                      double compressed_to_fraction, int len) {
  std::string dst;
  CompressibleString(rnd, compressed_to_fraction, len, &dst);
  return dst;
}

#ifndef NDEBUG
// An internal comparator that just forward comparing results from the
// user comparator in it. Can be used to test entities that have no dependency
// on internal key structure but consumes InternalKeyComparator, like
// BlockBasedTable.
class PlainInternalKeyComparator : public InternalKeyComparator {
 public:
  explicit PlainInternalKeyComparator(const Comparator* c)
      : InternalKeyComparator(c) {}

  ~PlainInternalKeyComparator() override {}

  int Compare(const Slice& a, const Slice& b) const override {
    return user_comparator()->Compare(a, b);
  }
};
#endif

// A test comparator which compare two strings in this way:
// (1) first compare prefix of 8 bytes in alphabet order,
// (2) if two strings share the same prefix, sort the other part of the string
//     in the reverse alphabet order.
// This helps simulate the case of compounded key of [entity][timestamp] and
// latest timestamp first.
class SimpleSuffixReverseComparator : public Comparator {
 public:
  SimpleSuffixReverseComparator() {}
  static const char* kClassName() { return "SimpleSuffixReverseComparator"; }
  const char* Name() const override { return kClassName(); }

  int Compare(const Slice& a, const Slice& b) const override {
    Slice prefix_a = Slice(a.data(), 8);
    Slice prefix_b = Slice(b.data(), 8);
    int prefix_comp = prefix_a.compare(prefix_b);
    if (prefix_comp != 0) {
      return prefix_comp;
    } else {
      Slice suffix_a = Slice(a.data() + 8, a.size() - 8);
      Slice suffix_b = Slice(b.data() + 8, b.size() - 8);
      return -(suffix_a.compare(suffix_b));
    }
  }
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};

// Returns a user key comparator that can be used for comparing two uint64_t
// slices. Instead of comparing slices byte-wise, it compares all the 8 bytes
// at once. Assumes same endian-ness is used though the database's lifetime.
// Symantics of comparison would differ from Bytewise comparator in little
// endian machines.
const Comparator* Uint64Comparator();

// A wrapper api for getting the ComparatorWithU64Ts<BytewiseComparator>
const Comparator* BytewiseComparatorWithU64TsWrapper();

// A wrapper api for getting the ComparatorWithU64Ts<ReverseBytewiseComparator>
const Comparator* ReverseBytewiseComparatorWithU64TsWrapper();

class StringSink : public FSWritableFile {
 public:
  std::string contents_;

  explicit StringSink(Slice* reader_contents = nullptr)
      : FSWritableFile(), reader_contents_(reader_contents), last_flush_(0) {
    if (reader_contents_ != nullptr) {
      *reader_contents_ = Slice(contents_.data(), 0);
    }
  }

  const std::string& contents() const { return contents_; }

  IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    contents_.resize(static_cast<size_t>(size));
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    if (reader_contents_ != nullptr) {
      assert(reader_contents_->size() <= last_flush_);
      size_t offset = last_flush_ - reader_contents_->size();
      *reader_contents_ =
          Slice(contents_.data() + offset, contents_.size() - offset);
      last_flush_ = contents_.size();
    }

    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  using FSWritableFile::Append;
  IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    contents_.append(slice.data(), slice.size());
    return IOStatus::OK();
  }
  void Drop(size_t bytes) {
    if (reader_contents_ != nullptr) {
      contents_.resize(contents_.size() - bytes);
      *reader_contents_ =
          Slice(reader_contents_->data(), reader_contents_->size() - bytes);
      last_flush_ = contents_.size();
    }
  }

  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return contents_.size();
  }

 private:
  Slice* reader_contents_;
  size_t last_flush_;
};

// A wrapper around a StringSink to give it a RandomRWFile interface
class RandomRWStringSink : public FSRandomRWFile {
 public:
  explicit RandomRWStringSink(StringSink* ss) : ss_(ss) {}

  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& /*opts*/,
                 IODebugContext* /*dbg*/) override {
    if (offset + data.size() > ss_->contents_.size()) {
      ss_->contents_.resize(static_cast<size_t>(offset) + data.size(), '\0');
    }

    char* pos = const_cast<char*>(ss_->contents_.data() + offset);
    memcpy(pos, data.data(), data.size());
    return IOStatus::OK();
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                Slice* result, char* /*scratch*/,
                IODebugContext* /*dbg*/) const override {
    *result = Slice(nullptr, 0);
    if (offset < ss_->contents_.size()) {
      size_t str_res_sz =
          std::min(static_cast<size_t>(ss_->contents_.size() - offset), n);
      *result = Slice(ss_->contents_.data() + offset, str_res_sz);
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  const std::string& contents() const { return ss_->contents(); }

 private:
  StringSink* ss_;
};

// Like StringSink, this writes into a string.  Unlink StringSink, it
// has some initial content and overwrites it, just like a recycled
// log file.
class OverwritingStringSink : public FSWritableFile {
 public:
  explicit OverwritingStringSink(Slice* reader_contents)
      : FSWritableFile(), reader_contents_(reader_contents), last_flush_(0) {}

  const std::string& contents() const { return contents_; }

  IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    contents_.resize(static_cast<size_t>(size));
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    if (last_flush_ < contents_.size()) {
      assert(reader_contents_->size() >= contents_.size());
      memcpy((char*)reader_contents_->data() + last_flush_,
             contents_.data() + last_flush_, contents_.size() - last_flush_);
      last_flush_ = contents_.size();
    }
    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  using FSWritableFile::Append;
  IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    contents_.append(slice.data(), slice.size());
    return IOStatus::OK();
  }
  void Drop(size_t bytes) {
    contents_.resize(contents_.size() - bytes);
    if (last_flush_ > contents_.size()) {
      last_flush_ = contents_.size();
    }
  }

  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return contents_.size();
  }

 private:
  std::string contents_;
  Slice* reader_contents_;
  size_t last_flush_;
};

class StringSource : public FSRandomAccessFile {
 public:
  explicit StringSource(const Slice& contents, uint64_t uniq_id = 0,
                        bool mmap = false)
      : contents_(contents.data(), contents.size()),
        uniq_id_(uniq_id),
        mmap_(mmap),
        total_reads_(0) {}

  ~StringSource() override {}

  uint64_t Size() const { return contents_.size(); }

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    // If we are using mmap_, it is equivalent to performing a prefetch
    if (mmap_) {
      return IOStatus::OK();
    } else {
      return IOStatus::NotSupported("Prefetch not supported");
    }
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    total_reads_++;
    if (offset > contents_.size()) {
      return IOStatus::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - static_cast<size_t>(offset);
    }
    if (!mmap_) {
      memcpy(scratch, &contents_[static_cast<size_t>(offset)], n);
      *result = Slice(scratch, n);
    } else {
      *result = Slice(&contents_[static_cast<size_t>(offset)], n);
    }
    return IOStatus::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    if (max_size < 20) {
      return 0;
    }

    char* rid = id;
    rid = EncodeVarint64(rid, uniq_id_);
    rid = EncodeVarint64(rid, 0);
    return static_cast<size_t>(rid - id);
  }

  int total_reads() const { return total_reads_; }

  void set_total_reads(int tr) { total_reads_ = tr; }

  IOStatus GetFileSize(uint64_t* file_size) override {
    *file_size = contents_.size();
    return IOStatus::OK();
  }

 private:
  std::string contents_;
  uint64_t uniq_id_;
  bool mmap_;
  mutable int total_reads_;
};

class NullLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override {}
  size_t GetLogFileSize() const override { return 0; }
};

// Corrupts key by changing the type
void CorruptKeyType(InternalKey* ikey);

std::string KeyStr(const std::string& user_key, const SequenceNumber& seq,
                   const ValueType& t, bool corrupt = false);

std::string KeyStr(uint64_t ts, const std::string& user_key,
                   const SequenceNumber& seq, const ValueType& t,
                   bool corrupt = false);

class SleepingBackgroundTask {
 public:
  SleepingBackgroundTask()
      : bg_cv_(&mutex_),
        should_sleep_(true),
        done_with_sleep_(false),
        sleeping_(false) {}

  ~SleepingBackgroundTask() {
    MutexLock l(&mutex_);
    should_sleep_ = false;
    while (sleeping_) {
      assert(!should_sleep_);
      bg_cv_.SignalAll();
      bg_cv_.Wait();
    }
  }

  bool IsSleeping() {
    MutexLock l(&mutex_);
    return sleeping_;
  }
  void DoSleep() {
    MutexLock l(&mutex_);
    sleeping_ = true;
    bg_cv_.SignalAll();
    while (should_sleep_) {
      bg_cv_.Wait();
    }
    sleeping_ = false;
    done_with_sleep_ = true;
    bg_cv_.SignalAll();
  }
  void WaitUntilSleeping() {
    MutexLock l(&mutex_);
    while (!sleeping_ || !should_sleep_) {
      bg_cv_.Wait();
    }
  }
  // Waits for the status to change to sleeping,
  // otherwise times out.
  // wait_time is in microseconds.
  // Returns true when times out, false otherwise.
  bool TimedWaitUntilSleeping(uint64_t wait_time);

  void WakeUp() {
    MutexLock l(&mutex_);
    should_sleep_ = false;
    bg_cv_.SignalAll();
  }
  void WaitUntilDone() {
    MutexLock l(&mutex_);
    while (!done_with_sleep_) {
      bg_cv_.Wait();
    }
  }
  // Similar to TimedWaitUntilSleeping.
  // Waits until the task is done.
  bool TimedWaitUntilDone(uint64_t wait_time);

  bool WokenUp() {
    MutexLock l(&mutex_);
    return should_sleep_ == false;
  }

  void Reset() {
    MutexLock l(&mutex_);
    should_sleep_ = true;
    done_with_sleep_ = false;
  }

  static void DoSleepTask(void* arg) {
    static_cast<SleepingBackgroundTask*>(arg)->DoSleep();
  }

 private:
  port::Mutex mutex_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  bool should_sleep_;
  bool done_with_sleep_;
  bool sleeping_;
};

// Filters merge operands and values that are equal to `num`.
class FilterNumber : public CompactionFilter {
 public:
  explicit FilterNumber(uint64_t num) : num_(num) {}

  std::string last_merge_operand_key() { return last_merge_operand_key_; }

  bool Filter(int /*level*/, const ROCKSDB_NAMESPACE::Slice& /*key*/,
              const ROCKSDB_NAMESPACE::Slice& value, std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  bool FilterMergeOperand(
      int /*level*/, const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Slice& value) const override {
    last_merge_operand_key_ = key.ToString();
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  const char* Name() const override { return "FilterBadMergeOperand"; }

 private:
  mutable std::string last_merge_operand_key_;
  uint64_t num_;
};

inline std::string EncodeInt(uint64_t x) {
  std::string result;
  PutFixed64(&result, x);
  return result;
}

class SeqStringSource : public FSSequentialFile {
 public:
  SeqStringSource(const std::string& data, std::atomic<int>* read_count)
      : data_(data), offset_(0), read_count_(read_count) {}
  ~SeqStringSource() override {}
  IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    std::string output;
    if (offset_ < data_.size()) {
      n = std::min(data_.size() - offset_, n);
      memcpy(scratch, data_.data() + offset_, n);
      offset_ += n;
      *result = Slice(scratch, n);
    } else {
      return IOStatus::InvalidArgument(
          "Attempt to read when it already reached eof.");
    }
    (*read_count_)++;
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    if (offset_ >= data_.size()) {
      return IOStatus::InvalidArgument(
          "Attempt to read when it already reached eof.");
    }
    // TODO(yhchiang): Currently doesn't handle the overflow case.
    offset_ += static_cast<size_t>(n);
    return IOStatus::OK();
  }

 private:
  std::string data_;
  size_t offset_;
  std::atomic<int>* read_count_;
};

class StringFS : public FileSystemWrapper {
 public:
  class StringSink : public FSWritableFile {
   public:
    explicit StringSink(std::string* contents)
        : FSWritableFile(), contents_(contents) {}
    IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
      contents_->resize(static_cast<size_t>(size));
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*opts*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*opts*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }

    using FSWritableFile::Append;
    IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
      contents_->append(slice.data(), slice.size());
      return IOStatus::OK();
    }

    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      if (contents_ != nullptr) {
        return contents_->size();
      }
      return 0;
    }

   private:
    std::string* contents_;
  };

  explicit StringFS(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  ~StringFS() override {}

  static const char* kClassName() { return "StringFS"; }
  const char* Name() const override { return kClassName(); }

  const std::string& GetContent(const std::string& f) { return files_[f]; }

  const IOStatus WriteToNewFile(const std::string& file_name,
                                const std::string& content) {
    std::unique_ptr<FSWritableFile> r;
    FileOptions file_opts;
    IOOptions io_opts;

    auto s = NewWritableFile(file_name, file_opts, &r, nullptr);
    if (s.ok()) {
      s = r->Append(content, io_opts, nullptr);
    }
    if (s.ok()) {
      s = r->Flush(io_opts, nullptr);
    }
    if (s.ok()) {
      s = r->Close(io_opts, nullptr);
    }
    assert(!s.ok() || files_[file_name] == content);
    return s;
  }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return IOStatus::NotFound("The specified file does not exist", f);
    }
    r->reset(new SeqStringSource(iter->second, &num_seq_file_read_));
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& /*f*/,
                               const FileOptions& /*options*/,
                               std::unique_ptr<FSRandomAccessFile>* /*r*/,
                               IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus NewWritableFile(const std::string& f, const FileOptions& /*options*/,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter != files_.end()) {
      return IOStatus::IOError("The specified file already exists", f);
    }
    r->reset(new StringSink(&files_[f]));
    return IOStatus::OK();
  }
  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*options*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus FileExists(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    if (files_.find(f) == files_.end()) {
      return IOStatus::NotFound();
    }
    return IOStatus::OK();
  }

  IOStatus GetChildren(const std::string& /*dir*/, const IOOptions& /*options*/,
                       std::vector<std::string>* /*r*/,
                       IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus DeleteFile(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    files_.erase(f);
    return IOStatus::OK();
  }

  IOStatus CreateDir(const std::string& /*d*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus CreateDirIfMissing(const std::string& /*d*/,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus DeleteDir(const std::string& /*d*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetFileSize(const std::string& f, const IOOptions& /*options*/,
                       uint64_t* s, IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return IOStatus::NotFound("The specified file does not exist:", f);
    }
    *s = iter->second.size();
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*file_mtime*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus RenameFile(const std::string& /*s*/, const std::string& /*t*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LinkFile(const std::string& /*s*/, const std::string& /*t*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LockFile(const std::string& /*f*/, const IOOptions& /*options*/,
                    FileLock** /*l*/, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus UnlockFile(FileLock* /*l*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  std::atomic<int> num_seq_file_read_;

 protected:
  std::unordered_map<std::string, std::string> files_;
};

// A compressor that essentially implements a custom compression algorithm
// by leveraging an existing compression algorithm and putting a custom header
// on it to detect any attempts to decompress it with the wrong compression
// type or dictionary.
template <CompressionType kCompression>
struct CompressorCustomAlg : public CompressorWrapper {
  static bool Supported() { return LZ4_Supported(); }

  explicit CompressorCustomAlg(
      std::unique_ptr<Compressor> wrapped =
          GetBuiltinV2CompressionManager()->GetCompressor({}, kLZ4Compression))
      : CompressorWrapper(std::move(wrapped)),
        dictionary_hash_(GetSliceHash(wrapped_->GetSerializedDict())) {
    static_assert(kCompression > kLastBuiltinCompression);
  }

  const char* Name() const override { return "CompressorCustomAlg"; }

  CompressionType GetPreferredCompressionType() const override {
    return kCompression;
  }

  std::unique_ptr<Compressor> Clone() const override {
    return std::make_unique<CompressorCustomAlg>(wrapped_->Clone());
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* working_area) override {
    size_t allowed_output_size = *compressed_output_size;
    Status s = wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                       compressed_output_size,
                                       out_compression_type, working_area);
    if (s.ok() && *out_compression_type != kNoCompression) {
      assert(*out_compression_type == kLZ4Compression);
      if (*compressed_output_size + 5 > allowed_output_size) {
        *out_compression_type = kNoCompression;
        return Status::OK();
      }
      // Generate & insert header
      std::memmove(compressed_output + 5, compressed_output,
                   *compressed_output_size);
      compressed_output[0] = lossless_cast<char>(kCompression);
      EncodeFixed32(&compressed_output[1], dictionary_hash_);
      *compressed_output_size += 5;
      *out_compression_type = kCompression;
    }
    return s;
  }

  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictConfigArgs&& dict_config) const override {
    auto clone =
        wrapped_->CloneMaybeSpecialized(block_type, std::move(dict_config));
    return std::make_unique<CompressorCustomAlg>(std::move(clone));
  }

 protected:
  uint32_t dictionary_hash_;
};

// A decompressor suitable for all the instantiable CompressorCustomAlg
// implementations. Can be configured to check that it is only used to
// decompress certain types using SetAllowedTypes().
struct DecompressorCustomAlg : public DecompressorWrapper {
  using TypeSet = SmallEnumSet<CompressionType, kDisableCompressionOption>;

  explicit DecompressorCustomAlg(
      std::shared_ptr<Decompressor> wrapped =
          GetBuiltinV2CompressionManager()->GetDecompressor())
      : DecompressorWrapper(std::move(wrapped)),
        dictionary_hash_(GetSliceHash(wrapped_->GetSerializedDict())),
        allowed_types_(TypeSet::All()) {}

  const char* Name() const override { return "DecompressorCustomAlg"; }

  Status MaybeCloneForDict(const Slice& serialized_dict,
                           std::unique_ptr<Decompressor>* out) override {
    Status s = wrapped_->MaybeCloneForDict(serialized_dict, out);
    if (s.ok()) {
      assert(*out != nullptr);
      auto clone = std::make_unique<DecompressorCustomAlg>(std::move(*out));
      clone->SetAllowedTypes(allowed_types_);
      *out = std::move(clone);
      assert(out->get()->GetSerializedDict() == serialized_dict);
    } else {
      assert(*out == nullptr);
    }
    return s;
  }

  Status ExtractUncompressedSize(Args& args) override {
    if (args.compression_type >= kFirstCustomCompression &&
        args.compression_type <= kLastCustomCompression) {
      assert(args.compressed_data.size() > 0);
      assert(args.compressed_data[0] ==
             lossless_cast<char>(args.compression_type));
      assert(DecodeFixed32(args.compressed_data.data() + 1) ==
             dictionary_hash_);
      // Strip off our header because ExtractUncompressedSize() is also going
      // to strip off the uncompressed size data.
      args.compressed_data.remove_prefix(5);
      // It's ok to modify other parts of args if we restore to original
      SaveAndRestore<CompressionType> save_compression_type(
          &args.compression_type, kLZ4Compression);
      return wrapped_->ExtractUncompressedSize(args);
    } else {
      // Also support built-in compressions
      return wrapped_->ExtractUncompressedSize(args);
    }
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (args.compression_type >= kFirstCustomCompression &&
        args.compression_type <= kLastCustomCompression) {
      // Also allowed to copy args and modify
      Args modified_args = args;
      modified_args.compression_type = kLZ4Compression;
      return wrapped_->DecompressBlock(modified_args, uncompressed_output);
    } else {
      // Also support built-in compressions
      return wrapped_->DecompressBlock(args, uncompressed_output);
    }
  }

  void SetAllowedTypes(const CompressionType* types_begin,
                       const CompressionType* types_end) {
    TypeSet allowed_types;
    for (auto type = types_begin; type != types_end; ++type) {
      allowed_types.Add(*type);
    }
    allowed_types_ = std::move(allowed_types);
  }

  void SetAllowedTypes(TypeSet allowed_types) {
    allowed_types_ = std::move(allowed_types);
  }

 protected:
  uint32_t dictionary_hash_;
  SmallEnumSet<CompressionType, kDisableCompressionOption> allowed_types_;
};

// Randomly initialize the given DBOptions
void RandomInitDBOptions(DBOptions* db_opt, Random* rnd);

// Randomly initialize the given ColumnFamilyOptions
// Note that the caller is responsible for releasing non-null
// cf_opt->compaction_filter.
void RandomInitCFOptions(ColumnFamilyOptions* cf_opt, DBOptions&, Random* rnd);

// A dummy merge operator which can change its name
class ChanglingMergeOperator : public MergeOperator {
 public:
  explicit ChanglingMergeOperator(const std::string& name)
      : name_(name + "MergeOperator") {}
  ~ChanglingMergeOperator() override {}

  void SetName(const std::string& name) { name_ = name; }

  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    return false;
  }
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    return false;
  }
  static const char* kClassName() { return "ChanglingMergeOperator"; }
  const char* NickName() const override { return kNickName(); }
  static const char* kNickName() { return "Changling"; }
  bool IsInstanceOf(const std::string& id) const override {
    if (id == kClassName()) {
      return true;
    } else {
      return MergeOperator::IsInstanceOf(id);
    }
  }

  const char* Name() const override { return name_.c_str(); }

 protected:
  std::string name_;
};

// Returns a dummy merge operator with random name.
MergeOperator* RandomMergeOperator(Random* rnd);

// A dummy compaction filter which can change its name
class ChanglingCompactionFilter : public CompactionFilter {
 public:
  explicit ChanglingCompactionFilter(const std::string& name)
      : name_(name + "CompactionFilter") {}
  ~ChanglingCompactionFilter() override {}

  void SetName(const std::string& name) { name_ = name; }

  bool Filter(int /*level*/, const Slice& /*key*/,
              const Slice& /*existing_value*/, std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return false;
  }

  static const char* kClassName() { return "ChanglingCompactionFilter"; }
  const char* NickName() const override { return kNickName(); }
  static const char* kNickName() { return "Changling"; }

  bool IsInstanceOf(const std::string& id) const override {
    if (id == kClassName()) {
      return true;
    } else {
      return CompactionFilter::IsInstanceOf(id);
    }
  }

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
};

// Returns a dummy compaction filter with a random name.
CompactionFilter* RandomCompactionFilter(Random* rnd);

// A dummy compaction filter factory which can change its name
class ChanglingCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit ChanglingCompactionFilterFactory(const std::string& name)
      : name_(name + "CompactionFilterFactory") {}
  ~ChanglingCompactionFilterFactory() override {}

  void SetName(const std::string& name) { name_ = name; }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>();
  }

  // Returns a name that identifies this compaction filter factory.
  const char* Name() const override { return name_.c_str(); }
  static const char* kClassName() { return "ChanglingCompactionFilterFactory"; }
  const char* NickName() const override { return kNickName(); }
  static const char* kNickName() { return "Changling"; }

  bool IsInstanceOf(const std::string& id) const override {
    if (id == kClassName()) {
      return true;
    } else {
      return CompactionFilterFactory::IsInstanceOf(id);
    }
  }

 protected:
  std::string name_;
};

// The factory for the hacky skip list mem table that triggers flush after
// number of entries exceeds a threshold.
MemTableRepFactory* NewSpecialSkipListFactory(int num_entries_per_flush);

CompressionType RandomCompressionType(Random* rnd);

void RandomCompressionTypeVector(const size_t count,
                                 std::vector<CompressionType>* types,
                                 Random* rnd);

CompactionFilterFactory* RandomCompactionFilterFactory(Random* rnd);

const SliceTransform* RandomSliceTransform(Random* rnd, int pre_defined = -1);

TableFactory* RandomTableFactory(Random* rnd, int pre_defined = -1);

std::string RandomName(Random* rnd, const size_t len);

bool IsDirectIOSupported(Env* env, const std::string& dir);

bool IsPrefetchSupported(const std::shared_ptr<FileSystem>& fs,
                         const std::string& dir);

// Return the number of lines where a given pattern was found in a file.
size_t GetLinesCount(const std::string& fname, const std::string& pattern);

Status CorruptFile(Env* env, const std::string& fname, int offset,
                   int bytes_to_corrupt, bool verify_checksum = true);
Status TruncateFile(Env* env, const std::string& fname, uint64_t length);

// Try and delete a directory if it exists
Status TryDeleteDir(Env* env, const std::string& dirname);

// Delete a directory if it exists
void DeleteDir(Env* env, const std::string& dirname);

// Find the FileType from the file path
FileType GetFileType(const std::string& path);

// Get the file number given the file path
uint64_t GetFileNumber(const std::string& path);

// Creates an Env from the system environment by looking at the system
// environment variables.
Status CreateEnvFromSystem(const ConfigOptions& options, Env** result,
                           std::shared_ptr<Env>* guard);

// Registers the testutil classes with the ObjectLibrary
int RegisterTestObjects(ObjectLibrary& library, const std::string& /*arg*/);

// Register the testutil classes with the default ObjectRegistry/Library
void RegisterTestLibrary(const std::string& arg = "");

struct ReadOptionsNoIo : public ReadOptions {
  ReadOptionsNoIo() { read_tier = ReadTier::kBlockCacheTier; }
};
extern const ReadOptionsNoIo kReadOptionsNoIo;

extern const std::string kUnitTestDbId;

}  // namespace test
}  // namespace ROCKSDB_NAMESPACE
