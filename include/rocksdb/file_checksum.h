// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// The unknown file checksum.
constexpr char kUnknownFileChecksum[] = "";
// The unknown sst file checksum function name.
// Indicates that the file metadata says that no checksum factory was configured
// when the file was written.
constexpr char kUnknownFileChecksumFuncName[] = "Unknown";
// Used when opening a file and there is no file checksum metadata to propagate
// at all.
constexpr char kNoFileChecksumFuncName[] = "Unavailable";
// The standard DB file checksum function name.
// This is the name of the checksum function returned by
// GetFileChecksumGenCrc32cFactory();
constexpr char kStandardDbFileChecksumFuncName[] = "FileChecksumCrc32c";

struct FileChecksumGenContext {
  std::string file_name;
  // The name of the requested checksum generator.
  // Checksum factories may use or ignore requested_checksum_func_name,
  // and checksum factories written before this field was available are still
  // compatible.
  std::string requested_checksum_func_name;
};

// FileChecksumGenerator is the class to generates the checksum value
// for each file when the file is written to the file system.
// Implementations may assume that
// * Finalize is called at most once during the life of the object
// * All calls to Update come before Finalize
// * All calls to GetChecksum come after Finalize
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FileChecksumGenerator {
 public:
  virtual ~FileChecksumGenerator() = default;

  // Update the current result after process the data. For different checksum
  // functions, the temporal results may be stored and used in Update to
  // include the new data.
  virtual void Update(const char* data, size_t n) = 0;

  // Generate the final results if no further new data will be updated.
  virtual void Finalize() = 0;

  // Get the checksum. The result should not be the empty string and may
  // include arbitrary bytes, including non-printable characters.
  virtual std::string GetChecksum() const = 0;

  // Returns a name that identifies the current file checksum function.
  virtual const char* Name() const = 0;
};

// Create the FileChecksumGenerator object for each SST file.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FileChecksumGenFactory : public Customizable {
 public:
  ~FileChecksumGenFactory() override = default;
  static const char* Type() { return "FileChecksumGenFactory"; }
  static Status CreateFromString(
      const ConfigOptions& options, const std::string& value,
      std::shared_ptr<FileChecksumGenFactory>* result);

  // Create a new FileChecksumGenerator. Recommended to return nullptr if the
  // requested function name is not recognized.
  virtual std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) = 0;

  // Return the name of this FileChecksumGenFactory.
  const char* Name() const override = 0;
};

// FileChecksumList stores the checksum information of a list of files (e.g.,
// SST files). The FileChecksumList can be used to store the checksum
// information of all SST file getting  from the MANIFEST, which are
// the checksum information of all valid SST file of a DB instance. It can
// also be used to store the checksum information of a list of SST files to
// be ingested.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FileChecksumList {
 public:
  virtual ~FileChecksumList() = default;

  // Clean the previously stored file checksum information.
  virtual void reset() = 0;

  // Get the number of checksums in the checksum list
  virtual size_t size() const = 0;

  // Return all the file checksum information being stored in a unordered_map.
  // File_number is the key, the first part of the value is checksum value,
  // and the second part of the value is checksum function name.
  virtual Status GetAllFileChecksums(
      std::vector<uint64_t>* file_numbers, std::vector<std::string>* checksums,
      std::vector<std::string>* checksum_func_names) = 0;

  // Given the file_number, it searches if the file checksum information is
  // stored.
  virtual Status SearchOneFileChecksum(uint64_t file_number,
                                       std::string* checksum,
                                       std::string* checksum_func_name) = 0;

  // Insert the checksum information of one file to the FileChecksumList.
  virtual Status InsertOneFileChecksum(
      uint64_t file_number, const std::string& checksum,
      const std::string& checksum_func_name) = 0;

  // Remove the checksum information of one SST file.
  virtual Status RemoveOneFileChecksum(uint64_t file_number) = 0;
};

// Create a new file checksum list.
FileChecksumList* NewFileChecksumList();

// Return a shared_ptr of the builtin Crc32c based file checksum generator
// factory object, which can be shared to create the Crc32c based checksum
// generator object.
// Note: this implementation is compatible with many other crc32c checksum
// implementations and uses big-endian encoding of the result, unlike most
// other crc32c checksums in RocksDB, which alter the result with
// crc32c::Mask and use little-endian encoding.
extern std::shared_ptr<FileChecksumGenFactory>
GetFileChecksumGenCrc32cFactory();

}  // namespace ROCKSDB_NAMESPACE
