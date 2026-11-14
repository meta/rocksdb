//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <set>

#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

// The classes and functions in this header file is used for dumping out the
// blocks in a block cache, storing or transfering the blocks to another
// destination host, and load these blocks to the secondary cache at destination
// host.
// NOTE that: The classes, functions, and data structures are EXPERIMENTAL! They
// my be changed in the future when the development continues.

// The major and minor version number of the data format to be stored/trandfered
// via CacheDumpWriter and read out via CacheDumpReader
static const int kCacheDumpMajorVersion = 0;
static const int kCacheDumpMinorVersion = 1;

// NOTE that: this class is EXPERIMENTAL! May be changed in the future!
// This is an abstract class to write or transfer the data that is created by
// CacheDumper. We pack one block with its block type, dump time, block key in
// the block cache, block len, block crc32c checksum and block itself as a unit
// and it is stored via WritePacket. Before we call WritePacket, we must call
// WriteMetadata once, which stores the sequence number, block unit checksum,
// and block unit size.
// We provide file based CacheDumpWriter to store the metadata and its package
// sequentially in a file as the defualt implementation. Users can implement
// their own CacheDumpWriter to store/transfer the data. For example, user can
// create a subclass which transfer the metadata and package on the fly.
class CacheDumpWriter {
 public:
  virtual ~CacheDumpWriter() = default;

  // Called ONCE before the calls to WritePacket
  virtual IOStatus WriteMetadata(const Slice& metadata) = 0;
  virtual IOStatus WritePacket(const Slice& data) = 0;
  virtual IOStatus Close() = 0;
};

// NOTE that: this class is EXPERIMENTAL! May be changed in the future!
// This is an abstract class to read or receive the data that is stored
// or transferred by CacheDumpWriter. Note that, ReadMetadata must be called
// once before we call a ReadPacket.
class CacheDumpReader {
 public:
  virtual ~CacheDumpReader() = default;
  // Called ONCE before the calls to ReadPacket
  virtual IOStatus ReadMetadata(std::string* metadata) = 0;
  // Sets data to empty string on EOF
  virtual IOStatus ReadPacket(std::string* data) = 0;
  // (Close not needed)
};

// CacheDumpOptions is the option for CacheDumper and CacheDumpedLoader. Any
// dump or load process related control variables can be added here.
struct CacheDumpOptions {
  SystemClock* clock;
  // Deadline for dumper or loader in microseconds
  std::chrono::microseconds deadline = std::chrono::microseconds::zero();
  // Max size bytes for dumper or loader
  uint64_t max_size_bytes = 0;
};

// NOTE that: this class is EXPERIMENTAL! May be changed in the future!
// This the class to dump out the block in the block cache, store/transfer them
// via CacheDumpWriter. In order to dump out the blocks belonging to a certain
// DB or a list of DB (block cache can be shared by many DB), user needs to call
// SetDumpFilter to specify a list of DB to filter out the blocks that do not
// belong to those DB.
// A typical use case is: when we migrate a DB instance from host A to host B.
// We need to reopen the DB at host B after all the files are copied to host B.
// At this moment, the block cache at host B does not have any block from this
// migrated DB. Therefore, the read performance can be low due to cache warm up.
// By using CacheDumper before we shut down the DB at host A and using
// CacheDumpedLoader at host B before we reopen the DB, we can warmup the cache
// ahead. This function can be used in other use cases also.
class CacheDumper {
 public:
  virtual ~CacheDumper() = default;
  // Only dump the blocks in the block cache that belong to the DBs in this list
  virtual Status SetDumpFilter(const std::vector<DB*>& db_list) {
    (void)db_list;
    return Status::NotSupported("SetDumpFilter is not supported");
  }
  // The main function to dump out all the blocks that satisfy the filter
  // condition from block cache to a certain CacheDumpWriter in one shot. This
  // process may take some time.
  virtual IOStatus DumpCacheEntriesToWriter() {
    return IOStatus::NotSupported("DumpCacheEntriesToWriter is not supported");
  }
};

// NOTE that: this class is EXPERIMENTAL! May be changed in the future!
// This is the class to load the dumped blocks to the destination cache. For now
// we only load the blocks to the SecondaryCache. In the future, we may plan to
// support loading to the block cache.
class CacheDumpedLoader {
 public:
  virtual ~CacheDumpedLoader() = default;
  virtual IOStatus RestoreCacheEntriesToSecondaryCache() {
    return IOStatus::NotSupported(
        "RestoreCacheEntriesToSecondaryCache is not supported");
  }
};

// Get the writer which stores all the metadata and data sequentially to a file
IOStatus NewToFileCacheDumpWriter(const std::shared_ptr<FileSystem>& fs,
                                  const FileOptions& file_opts,
                                  const std::string& file_name,
                                  std::unique_ptr<CacheDumpWriter>* writer);

// Get the reader which read out the metadata and data sequentially from a file
IOStatus NewFromFileCacheDumpReader(const std::shared_ptr<FileSystem>& fs,
                                    const FileOptions& file_opts,
                                    const std::string& file_name,
                                    std::unique_ptr<CacheDumpReader>* reader);

// Get the default cache dumper
Status NewDefaultCacheDumper(const CacheDumpOptions& dump_options,
                             const std::shared_ptr<Cache>& cache,
                             std::unique_ptr<CacheDumpWriter>&& writer,
                             std::unique_ptr<CacheDumper>* cache_dumper);

// Get the default cache dump loader
Status NewDefaultCacheDumpedLoader(
    const CacheDumpOptions& dump_options,
    const BlockBasedTableOptions& toptions,
    const std::shared_ptr<SecondaryCache>& secondary_cache,
    std::unique_ptr<CacheDumpReader>&& reader,
    std::unique_ptr<CacheDumpedLoader>* cache_dump_loader);

}  // namespace ROCKSDB_NAMESPACE
