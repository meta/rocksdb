//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <unordered_map>

#include "rocksdb/file_checksum.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

// Converts a 32-bit checksum value (e.g. a CRC32C) to its hex string
// representation, matching the on-disk/backup-metadata encoding. Shared by the
// backup and copy engines.
inline std::string ChecksumInt32ToHex(uint32_t checksum_value) {
  std::string checksum_str;
  PutFixed32(&checksum_str, EndianSwapValue(checksum_value));
  return Slice(checksum_str).ToString(/*hex=*/true);
}

// This is the class to generate the file checksum based on Crc32. It
// will be used as the default checksum method for SST file checksum
class FileChecksumGenCrc32c : public FileChecksumGenerator {
 public:
  explicit FileChecksumGenCrc32c(const FileChecksumGenContext& /*context*/) {
    checksum_ = 0;
  }

  void Update(const char* data, size_t n) override {
    checksum_ = crc32c::Extend(checksum_, data, n);
  }

  void Finalize() override {
    assert(checksum_str_.empty());
    // Store as big endian raw bytes
    PutFixed32(&checksum_str_, EndianSwapValue(checksum_));
  }

  std::string GetChecksum() const override {
    assert(!checksum_str_.empty());
    return checksum_str_;
  }

  const char* Name() const override { return "FileChecksumCrc32c"; }

 private:
  uint32_t checksum_;
  std::string checksum_str_;
};

class FileChecksumGenCrc32cFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty() ||
        context.requested_checksum_func_name == "FileChecksumCrc32c") {
      return std::unique_ptr<FileChecksumGenerator>(
          new FileChecksumGenCrc32c(context));
    } else {
      return nullptr;
    }
  }

  static const char* kClassName() { return "FileChecksumGenCrc32cFactory"; }
  const char* Name() const override { return kClassName(); }
};

// The default implementaion of FileChecksumList
class FileChecksumListImpl : public FileChecksumList {
 public:
  FileChecksumListImpl() {}
  void reset() override;

  size_t size() const override;

  Status GetAllFileChecksums(
      std::vector<uint64_t>* file_numbers, std::vector<std::string>* checksums,
      std::vector<std::string>* checksum_func_names) override;

  Status SearchOneFileChecksum(uint64_t file_number, std::string* checksum,
                               std::string* checksum_func_name) override;

  Status InsertOneFileChecksum(uint64_t file_number,
                               const std::string& checksum,
                               const std::string& checksum_func_name) override;

  Status RemoveOneFileChecksum(uint64_t file_number) override;

 private:
  // Key is the file number, the first portion of the value is checksum, the
  // second portion of the value is checksum function name.
  std::unordered_map<uint64_t, std::pair<std::string, std::string>>
      checksum_map_;
};

}  // namespace ROCKSDB_NAMESPACE
