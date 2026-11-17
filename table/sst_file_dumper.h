// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "port/mmap.h"
#include "rocksdb/advanced_options.h"

namespace ROCKSDB_NAMESPACE {

class SstFileDumper {
 public:
  explicit SstFileDumper(const Options& options, const std::string& file_name,
                         Temperature file_temp, size_t readahead_size,
                         bool verify_checksum, bool output_hex,
                         bool decode_blob_index,
                         const EnvOptions& soptions = EnvOptions(),
                         bool silent = false,
                         bool show_sequence_number_type = false);

  // read_num_limit limits the total number of keys read. If read_num_limit = 0,
  // then there is no limit. If read_num_limit = 0 or
  // std::numeric_limits<uint64_t>::max(), has_from and has_to are false, then
  // the number of keys read is compared with `num_entries` field in table
  // properties. A Corruption status is returned if they do not match.
  Status ReadSequential(bool print_kv, uint64_t read_num_limit, bool has_from,
                        const std::string& from_key, bool has_to,
                        const std::string& to_key,
                        bool use_from_as_prefix = false);

  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);
  uint64_t GetReadNumber() { return read_num_; }
  TableProperties* GetInitTableProperties() { return table_properties_.get(); }

  // Properties of the table file last built by ShowCompressionSize (i.e. the
  // last recompressed output). Returns nullptr until a recompression has been
  // measured. Unlike properties read back from an on-disk file, these are
  // freshly built, so build-time-only fields (e.g. uncompressed_data_size) are
  // populated.
  const TableProperties* GetRecompressOutputProperties() const {
    return have_recompress_output_props_ ? &recompress_output_props_ : nullptr;
  }

  Status VerifyChecksum();
  Status DumpTable(const std::string& out_filename);
  Status getStatus() { return init_result_; }

  // One recompression measurement, or (when is_input) the input file itself.
  // All sizes are block payload, excluding block trailers.
  struct RecompressionMeasurement {
    CompressionType compression_type = kNoCompression;
    std::string compression_name;
    // Level, strategy, parallel_threads, etc. are in compression_opts below.
    uint64_t block_size = 0;
    uint64_t compressed_data_payload = 0;
    uint64_t uncompressed_data_payload = 0;
    // Total uncompressed index content (all partitions + top level).
    uint64_t uncompressed_index_payload = 0;
    // Total on-disk index content; equals uncompressed_index_payload when the
    // index is not stored compressed.
    uint64_t compressed_index_payload = 0;
    uint64_t num_data_blocks = 0;
    uint64_t blocks_compressed = 0;
    // Compression was attempted but the result was rejected and the block
    // stored uncompressed, because it didn't beat max_compressed_bytes_per_kb
    // or failed verify_compression.
    uint64_t blocks_compression_rejected = 0;
    // Compression was not attempted (e.g. AutoSkip decided it wasn't paying
    // off, or kNoCompression), so the block is stored uncompressed.
    uint64_t blocks_compression_bypassed = 0;
    int64_t write_usec = 0;
    int64_t read_usec = 0;
    // Process CPU time (summed across all threads, so it includes parallel
    // compression workers) for the write/read benchmarks. -1 when not
    // applicable (e.g. write time for the input baseline) or unsupported by the
    // platform.
    int64_t write_cpu_usec = 0;
    int64_t read_cpu_usec = 0;
    // Full CompressionOptions used for this measurement (not meaningful for the
    // input baseline).
    CompressionOptions compression_opts;
    // Compression manager id, or empty for the built-in manager.
    std::string compression_manager;
    // True for the synthetic entry describing the input file as stored. For it,
    // fields that don't apply (write time, block counts, compression_opts) are
    // -1 / 0 / default and should be rendered as N/A.
    bool is_input = false;
  };

  // Measures the recompressed size for each combination of compression type,
  // strategy, and level, collecting them for retrieval via
  // GetRecompressMeasurements(). Whenever the compression type changes, a quiet
  // throw-away priming build is run first to warm up the compressor so the
  // timed measurements are not skewed by first-use overhead. If
  // `per_measurement` is set, it is invoked with each measurement as soon as it
  // is collected, so callers can stream output incrementally (measurements can
  // be slow). Does not print anything itself. An empty `compression_strategies`
  // means "use the strategy already in Options::compression_opts".
  Status ShowAllCompressionSizes(
      const std::vector<CompressionType>& compression_types,
      int32_t compress_level_from, int32_t compress_level_to,
      const std::vector<int>& compression_strategies,
      const std::function<void(const RecompressionMeasurement&)>&
          per_measurement);

  // Measures one recompression, filling *measurement.
  Status ShowCompressionSize(CompressionType compress_type,
                             const CompressionOptions& compress_opt,
                             RecompressionMeasurement* measurement);

  const std::vector<RecompressionMeasurement>& GetRecompressMeasurements()
      const {
    return recompress_measurements_;
  }

  // Builds a benchmark measurement describing the input file as stored: its
  // compression type, on-disk (compressed) vs uncompressed data/index sizes,
  // and a benchmarked read time. The uncompressed data size is inferred from
  // decompression Statistics tickers during a single (cold) read pass, so it
  // works even for files that do not persist
  // TableProperties::uncompressed_data_size. Requires that the input reader was
  // opened with a Statistics object (Options::statistics) for the uncompressed
  // data size and read time; otherwise those are left unknown (-1 read time,
  // uncompressed data size from properties, which may be 0).
  //
  // Exact when the file's index blocks are stored uncompressed (the default);
  // if the file was written with enable_index_compression, the uncompressed
  // data size may be overstated by the index's decompression delta.
  Status GetInputBenchmarkMeasurement(RecompressionMeasurement* measurement);

  BlockContents& GetMetaIndexContents() { return meta_index_contents_; }

  // Size of a block's trailer for the input file (0 for formats without one).
  uint32_t GetBlockTrailerSize() const { return block_trailer_size_; }

 private:
  // Get the TableReader implementation for the sst file
  Status GetTableReader(const std::string& file_path);
  Status ReadTableProperties(uint64_t table_magic_number,
                             RandomAccessFileReader* file, uint64_t file_size,
                             FilePrefetchBuffer* prefetch_buffer);

  Status CalculateCompressedTableSize(const TableBuilderOptions& tb_options,
                                      TableProperties* props,
                                      std::chrono::microseconds* write_time,
                                      std::chrono::microseconds* read_time,
                                      std::chrono::microseconds* write_cpu_time,
                                      std::chrono::microseconds* read_cpu_time,
                                      uint64_t* on_disk_index_size);

  // Builds a small throw-away table (a prefix of the input, about 1.5x the
  // block size of key-value payload, so ~2 data blocks) to warm up the
  // compressor before timed measurements. Output is discarded.
  Status PrimeCompression(CompressionType compress_type,
                          const CompressionOptions& compress_opt,
                          uint64_t block_size);

  // Reads the whole input file once into memory (see input_kvs_) so the
  // write benchmark can feed the builder from memory (excluding input
  // read/decompression from the timing) and be independent of block-cache
  // state.
  Status MaterializeInputKVs();
  Status AppendInputKV(const Slice& key, const Slice& value);
  // Calls fn(key, value) for each materialized input entry in order, until it
  // returns false
  template <typename Fn>
  void ForEachInputKV(const Fn& fn) const;

  Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);

  // Helper function to call the factory with settings specific to the
  // factory implementation
  Status NewTableReader(const ImmutableOptions& ioptions,
                        const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* table_reader);

  std::string file_name_;
  uint64_t read_num_;
  Temperature file_temp_;
  bool output_hex_;
  bool decode_blob_index_;
  bool show_sequence_number_type_;
  EnvOptions soptions_;
  // less verbose in stdout/stderr
  bool silent_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;

  Status init_result_;
  std::unique_ptr<TableReader> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  ImmutableOptions ioptions_;
  const MutableCFOptions moptions_;
  ReadOptions read_options_;
  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableProperties> table_properties_;
  // Whole input file materialized in memory so write benchmarks feed the
  // builder without re-reading/decompressing the input. Each entry is its
  // (internal) key's and value's sizes as two 32-bit integers, then the key
  // and value, back to back in the first input_kvs_used_ bytes of one
  // anonymous memory mapping, which avoids heap overhead and fragmentation.
  // The mapping is sized from the table properties, so it never needs to grow,
  // and only the pages written become resident.
  TypedMemMapping<char> input_kvs_ = MemMapping::AllocateLazyZeroed(0);
  size_t input_kvs_used_ = 0;
  // Properties of the last table file built by ShowCompressionSize.
  TableProperties recompress_output_props_;
  bool have_recompress_output_props_ = false;
  // Measurements collected by the last ShowAllCompressionSizes call.
  std::vector<RecompressionMeasurement> recompress_measurements_;
  // Block trailer size of the input file (from its footer).
  uint32_t block_trailer_size_ = 0;
  BlockContents meta_index_contents_;
};

}  // namespace ROCKSDB_NAMESPACE
