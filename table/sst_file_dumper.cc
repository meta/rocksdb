//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/sst_file_dumper.h"

#include <chrono>
#include <cinttypes>
#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/memtable.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_fetcher.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

SstFileDumper::SstFileDumper(const Options& options,
                             const std::string& file_path,
                             Temperature file_temp, size_t readahead_size,
                             bool verify_checksum, bool output_hex,
                             bool decode_blob_index, const EnvOptions& soptions,
                             bool silent, bool show_sequence_number_type)
    : file_name_(file_path),
      read_num_(0),
      file_temp_(file_temp),
      output_hex_(output_hex),
      decode_blob_index_(decode_blob_index),
      show_sequence_number_type_(show_sequence_number_type),
      soptions_(soptions),
      silent_(silent),
      options_(options),
      ioptions_(options_),
      moptions_(ColumnFamilyOptions(options_)),
      // TODO: plumb Env::IOActivity, Env::IOPriority
      read_options_(verify_checksum, false),
      internal_comparator_(BytewiseComparator()) {
  read_options_.readahead_size = readahead_size;
  if (!silent_) {
    fprintf(stdout, "Process %s\n", file_path.c_str());
  }
  init_result_ = GetTableReader(file_name_);
}

constexpr char testFileName[] = "test_file_name";

Status SstFileDumper::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kNullTableMagicNumber;

  // read table magic number
  Footer footer;

  const auto& fs = options_.env->GetFileSystem();
  std::unique_ptr<FSRandomAccessFile> file;
  uint64_t file_size = 0;
  FileOptions fopts = soptions_;
  fopts.temperature = file_temp_;
  fopts.file_checksum_func_name = kNoFileChecksumFuncName;
  Status s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
  if (s.ok()) {
    // check empty file
    // if true, skip further processing of this file
    s = fs->GetFileSize(file_path, IOOptions(), &file_size, nullptr);
    if (s.ok()) {
      if (file_size == 0) {
        return Status::Aborted(file_path, "Empty file");
      }
    }
  }

  file_.reset(new RandomAccessFileReader(std::move(file), file_path));

  FilePrefetchBuffer prefetch_buffer(ReadaheadParams(),
                                     !fopts.use_mmap_reads /* enable */,
                                     false /* track_min_offset */);
  if (s.ok()) {
    const uint64_t kSstDumpTailPrefetchSize = 512 * 1024;
    uint64_t prefetch_size = (file_size > kSstDumpTailPrefetchSize)
                                 ? kSstDumpTailPrefetchSize
                                 : file_size;
    uint64_t prefetch_off = file_size - prefetch_size;
    IOOptions opts;
    s = prefetch_buffer.Prefetch(opts, file_.get(), prefetch_off,
                                 static_cast<size_t>(prefetch_size));

    s = ReadFooterFromFile(opts, file_.get(), *fs, &prefetch_buffer, file_size,
                           &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
    block_trailer_size_ = static_cast<uint32_t>(footer.GetBlockTrailerSize());
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber ||
        magic_number == kCuckooTableMagicNumber) {
      soptions_.use_mmap_reads = true;
      fopts.use_mmap_reads = soptions_.use_mmap_reads;

      if (magic_number == kCuckooTableMagicNumber) {
        fopts = soptions_;
        fopts.temperature = file_temp_;
        fopts.file_checksum_func_name = kNoFileChecksumFuncName;
      }

      s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
      if (!s.ok()) {
        return s;
      }
      file_.reset(new RandomAccessFileReader(std::move(file), file_path));
    }

    s = ReadTableProperties(magic_number, file_.get(), file_size,
                            (magic_number == kBlockBasedTableMagicNumber)
                                ? &prefetch_buffer
                                : nullptr);
    if (s.ok()) {
      s = SetTableOptionsByMagicNumber(magic_number);
      if (s.ok()) {
        if (table_properties_ && !table_properties_->comparator_name.empty()) {
          ConfigOptions config_options;
          const Comparator* user_comparator = nullptr;
          s = Comparator::CreateFromString(config_options,
                                           table_properties_->comparator_name,
                                           &user_comparator);
          if (s.ok()) {
            assert(user_comparator);
            internal_comparator_ = InternalKeyComparator(user_comparator);
          }
        }
      }
    }
    options_.comparator = internal_comparator_.user_comparator();

    {
      Status status = ReadMetaIndexBlockInFile(
          file_.get(), file_size, magic_number, ImmutableOptions(options_),
          ReadOptions(), &meta_index_contents_);
      // Ignore any errors since this is required for a specific CLI option
      status.PermitUncheckedError();
    }
  }

  if (s.ok()) {
    s = NewTableReader(ioptions_, soptions_, internal_comparator_, file_size,
                       &table_reader_);
  }
  return s;
}

Status SstFileDumper::NewTableReader(
    const ImmutableOptions& /*ioptions*/, const EnvOptions& /*soptions*/,
    const InternalKeyComparator& /*internal_comparator*/, uint64_t file_size,
    std::unique_ptr<TableReader>* /*table_reader*/) {
  auto t_opt = TableReaderOptions(
      ioptions_, moptions_.prefix_extractor,
      moptions_.compression_manager.get(), soptions_, internal_comparator_,
      0 /* block_protection_bytes_per_key */, false /* skip_filters */,
      false /* immortal */, true /* force_direct_prefetch */, -1 /* level */,
      nullptr /* block_cache_tracer */, 0 /* max_file_size_for_l0_meta_pin */,
      "" /* cur_db_session_id */, 0 /* cur_file_num */, {} /* unique_id */,
      0 /* largest_seqno */, 0 /* tail_size */,
      table_properties_ == nullptr
          ? true
          : static_cast<bool>(
                table_properties_->user_defined_timestamps_persisted));
  // Allow open file with global sequence number for backward compatibility.
  t_opt.largest_seqno = kMaxSequenceNumber;

  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (options_.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    return options_.table_factory->NewTableReader(t_opt, std::move(file_),
                                                  file_size, &table_reader_,
                                                  /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(t_opt, std::move(file_),
                                                file_size, &table_reader_);
}

Status SstFileDumper::VerifyChecksum() {
  assert(read_options_.verify_checksums);
  // We could pass specific readahead setting into read options if needed.
  return table_reader_->VerifyChecksum(read_options_,
                                       TableReaderCaller::kSSTDumpTool);
}

Status SstFileDumper::DumpTable(const std::string& out_filename) {
  std::unique_ptr<WritableFile> out_file;
  Env* env = options_.env;
  Status s = env->NewWritableFile(out_filename, &out_file, soptions_);
  if (!s.ok()) {
    return s;
  }
  s = table_reader_->DumpTable(out_file.get(), show_sequence_number_type_);
  if (!s.ok()) {
    // close the file before return error, ignore the close error if there's any
    out_file->Close().PermitUncheckedError();
    return s;
  }
  return out_file->Close();
}

namespace {
// Process-wide CPU time (summed across all threads) in microseconds, or -1 if
// unavailable on this platform. Unlike Env::NowCPUNanos(), which measures only
// the calling thread (CLOCK_THREAD_CPUTIME_ID), this captures the parallel
// compression worker threads used when compression_opts.parallel_threads > 1.
// Used only for benchmarking, so degrading to -1 (reported as N/A) is fine.
int64_t ProcessCpuMicros() {
#if !defined(OS_WIN) && defined(CLOCK_PROCESS_CPUTIME_ID)
  struct timespec ts;
  if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) == 0) {
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
  }
#endif
  return -1;
}

// Returns the elapsed process CPU microseconds since `start_micros`, or -1 if
// either endpoint is unavailable.
int64_t ProcessCpuMicrosSince(int64_t start_micros) {
  int64_t now = ProcessCpuMicros();
  return (start_micros >= 0 && now >= 0) ? now - start_micros : -1;
}

// Computes the total on-disk (as stored, possibly compressed) size in bytes of
// all index blocks of a block-based table: the single/top-level index block
// plus, for a two-level (partitioned) index, every partition index block.
// Sizes exclude block trailers (BlockHandle::size()), matching the trailer-free
// convention used elsewhere for recompression sizes.
//
// `reader` must have been produced by `factory`. Returns NotSupported (rather
// than downcasting) unless `factory` is a block-based table factory -- i.e.
// `reader` is a BlockBasedTable. This guards the static_cast_with_check below,
// which would otherwise abort in debug builds (and be undefined behavior in
// release) when handed a plain- or cuckoo-table input reader.
Status GetOnDiskIndexSize(TableFactory* factory, TableReader* reader,
                          uint64_t* size) {
  assert(size);
  *size = 0;
  if (reader == nullptr) {
    return Status::NotSupported("No table reader");
  }
  if (factory == nullptr ||
      !factory->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
    return Status::NotSupported("Not a block-based table");
  }
  BlockBasedTable* bbt = static_cast_with_check<BlockBasedTable>(reader);
  const BlockBasedTable::Rep* rep = bbt->get_rep();
  if (rep == nullptr) {
    return Status::NotSupported("No block-based table rep");
  }
  uint64_t total = rep->index_handle.size();
  if (rep->index_type == BlockBasedTableOptions::kTwoLevelIndexSearch) {
    // The block at rep->index_handle is the top-level index; read it and sum
    // the on-disk sizes of the partition index blocks it points to.
    BlockContents contents;
    ReadOptions ro;
    BlockFetcher fetcher(
        rep->file.get(), /*prefetch_buffer=*/nullptr, rep->footer, ro,
        rep->index_handle, &contents, rep->ioptions, /*do_uncompress=*/true,
        /*maybe_compressed=*/rep->decompressor != nullptr, BlockType::kIndex,
        rep->decompressor.get(), rep->persistent_cache_options);
    Status s = fetcher.ReadBlockContents();
    if (!s.ok()) {
      return s;
    }
    Block top_level_index(std::move(contents));
    IndexBlockIter biter;
    top_level_index.NewIndexIterator(
        rep->internal_comparator.user_comparator(),
        rep->get_global_seqno(BlockType::kIndex), &biter, /*stats=*/nullptr,
        /*total_order_seek=*/true, rep->index_has_first_key,
        rep->index_key_includes_seq, rep->index_value_is_full,
        /*block_contents_pinned=*/false, rep->user_defined_timestamps_persisted,
        /*prefix_index=*/nullptr, BlockBasedTableOptions::kBinary,
        FormatVersionUsesValueDeltaEscape(rep->footer.format_version()));
    for (biter.SeekToFirst(); biter.Valid(); biter.Next()) {
      total += biter.value().handle.size();
    }
    s = biter.status();
    if (!s.ok()) {
      return s;
    }
  }
  *size = total;
  return Status::OK();
}
}  // namespace

Status SstFileDumper::CalculateCompressedTableSize(
    const TableBuilderOptions& tb_options, TableProperties* props,
    std::chrono::microseconds* write_time, std::chrono::microseconds* read_time,
    std::chrono::microseconds* write_cpu_time,
    std::chrono::microseconds* read_cpu_time, uint64_t* on_disk_index_size) {
  std::unique_ptr<Env> env(NewMemEnv(options_.env));
  std::unique_ptr<WritableFileWriter> dest_writer;
  Status s =
      WritableFileWriter::Create(env->GetFileSystem(), testFileName,
                                 FileOptions(soptions_), &dest_writer, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::chrono::steady_clock::time_point start =
      std::chrono::steady_clock::now();
  int64_t cpu_start_micros = ProcessCpuMicros();
  std::unique_ptr<TableBuilder> table_builder{
      tb_options.moptions.table_factory->NewTableBuilder(tb_options,
                                                         dest_writer.get())};
  // Feed the builder from the pre-materialized input (see MaterializeInputKVs)
  // rather than iterating the input reader, so the write benchmark measures the
  // build + compression cost only -- not reading/decompressing the input -- and
  // is independent of block-cache warmth.
  for (const auto& kv : input_kvs_) {
    table_builder->Add(kv.first, kv.second);
  }
  s = table_builder->Finish();
  *write_time = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start);
  *write_cpu_time =
      std::chrono::microseconds(ProcessCpuMicrosSince(cpu_start_micros));
  if (!s.ok()) {
    return s;
  }
  s = dest_writer->Close({});
  if (!s.ok()) {
    return s;
  }
  dest_writer.reset();
  *props = table_builder->GetTableProperties();
  start = std::chrono::steady_clock::now();
  cpu_start_micros = ProcessCpuMicros();
  TableReaderOptions reader_options(ioptions_, moptions_.prefix_extractor,
                                    moptions_.compression_manager.get(),
                                    soptions_, internal_comparator_,
                                    0 /* block_protection_bytes_per_key */);
  std::unique_ptr<RandomAccessFileReader> file_reader;
  s = RandomAccessFileReader::Create(env->GetFileSystem(), testFileName,
                                     soptions_, &file_reader, /*dbg=*/nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<TableReader> table_reader;
  // Read the freshly built table back with the block cache disabled. Each
  // iteration builds a distinct table into a fresh in-memory file, but these
  // temporary tables all share the same (unknown) cache key identity
  // (empty db_session_id and file number 0, see
  // BlockBasedTable::SetupBaseCacheKey). If the DB's block cache were used,
  // a later iteration could get a cache hit on a stale block written by an
  // earlier iteration's differently-compressed table at a colliding offset,
  // producing "Corruption: bad entry in block". Bypassing the cache also keeps
  // the measured read time consistent (always a cold read + decompress).
  std::shared_ptr<TableFactory> read_table_factory =
      tb_options.moptions.table_factory;
  if (read_table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName()) &&
      read_table_factory->GetOptions<BlockBasedTableOptions>()) {
    BlockBasedTableOptions read_bbto =
        *read_table_factory->GetOptions<BlockBasedTableOptions>();
    read_bbto.no_block_cache = true;
    read_bbto.block_cache = nullptr;
    read_table_factory = std::make_shared<BlockBasedTableFactory>(read_bbto);
  }
  s = read_table_factory->NewTableReader(reader_options, std::move(file_reader),
                                         table_builder->FileSize(),
                                         &table_reader);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<InternalIterator> read_iter(table_reader->NewIterator(
      read_options_, moptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTDumpTool));
  for (read_iter->SeekToFirst(); read_iter->Valid(); read_iter->Next()) {
  }
  s = read_iter->status();
  // Stop the read benchmark here, before the (untimed) on-disk index-size
  // accounting and teardown below, so read_time/read_cpu_time reflect only the
  // block reads + decompression.
  *read_time = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start);
  *read_cpu_time =
      std::chrono::microseconds(ProcessCpuMicrosSince(cpu_start_micros));
  read_iter.reset();
  if (!s.ok()) {
    return s;
  }
  // Total on-disk (compressed) index size of the freshly built table.
  if (on_disk_index_size != nullptr) {
    Status idx_s = GetOnDiskIndexSize(read_table_factory.get(),
                                      table_reader.get(), on_disk_index_size);
    if (!idx_s.ok()) {
      // Best effort: fall back to the uncompressed index content size so
      // callers won't misreport it as compressed.
      *on_disk_index_size =
          props->index_size > BlockBasedTable::kBlockTrailerSize
              ? props->index_size - BlockBasedTable::kBlockTrailerSize
              : props->index_size;
    }
  }
  table_reader.reset();
  file_reader.reset();
  return env->DeleteFile(testFileName);
}

Status SstFileDumper::PrimeCompression(CompressionType compress_type,
                                       const CompressionOptions& compress_opt,
                                       uint64_t block_size) {
  Options opts = options_;    // Use compression_manager etc.
  opts.statistics = nullptr;  // This warm-up run is not measured.
  if (!opts.table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
    opts.table_factory = std::make_shared<BlockBasedTableFactory>();
  }
  const ImmutableOptions imoptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);
  const ReadOptions read_options;
  const WriteOptions write_options;
  ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);
  InternalTblPropCollFactories coll_factories;
  std::string column_family_name;
  int unknown_level = -1;
  TableBuilderOptions tb_opts(
      imoptions, moptions, read_options, write_options, ikc, &coll_factories,
      compress_type, compress_opt,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      column_family_name, unknown_level, kUnknownNewestKeyTime);

  std::unique_ptr<Env> env(NewMemEnv(options_.env));
  std::unique_ptr<WritableFileWriter> dest_writer;
  Status s =
      WritableFileWriter::Create(env->GetFileSystem(), testFileName,
                                 FileOptions(soptions_), &dest_writer, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<TableBuilder> table_builder{
      tb_opts.moptions.table_factory->NewTableBuilder(tb_opts,
                                                      dest_writer.get())};
  // Feed a prefix of key-value payload roughly 1.5x the block size, so about
  // two data blocks are produced. This only warms up the compression code path
  // (allocations, library/context init, any JIT) before the timed runs; it is
  // intentionally too little to train a compression dictionary -- the full
  // timed build trains the dictionary when one is configured.
  const uint64_t payload_limit = block_size + block_size / 2;
  uint64_t payload = 0;
  for (const auto& kv : input_kvs_) {
    if (payload >= payload_limit) {
      break;
    }
    table_builder->Add(kv.first, kv.second);
    payload += kv.first.size() + kv.second.size();
  }
  s = table_builder->Finish();
  // Discard the warm-up output regardless of outcome.
  dest_writer->Close({}).PermitUncheckedError();
  env->DeleteFile(testFileName).PermitUncheckedError();
  return s;
}

Status SstFileDumper::MaterializeInputKVs() {
  input_kvs_.clear();
  std::unique_ptr<InternalIterator> iter(table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTDumpTool));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    input_kvs_.emplace_back(iter->key().ToString(), iter->value().ToString());
  }
  return iter->status();
}

Status SstFileDumper::ShowAllCompressionSizes(
    const std::vector<CompressionType>& compression_types,
    int32_t compress_level_from, int32_t compress_level_to,
    const std::vector<int>& compression_strategies,
    const std::function<void(const RecompressionMeasurement&)>&
        per_measurement) {
  recompress_measurements_.clear();
  // Materialize the input once so the timed write builds feed from memory.
  Status ms = MaterializeInputKVs();
  if (!ms.ok()) {
    return ms;
  }
  BlockBasedTableOptions bbto;
  if (options_.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    bbto = *(static_cast_with_check<BlockBasedTableFactory>(
                 options_.table_factory.get()))
                ->GetOptions<BlockBasedTableOptions>();
  }
  // Strategies to sweep; empty means "use whatever is in compression_opts".
  std::vector<int> strategies = compression_strategies;
  if (strategies.empty()) {
    strategies.push_back(options_.compression_opts.strategy);
  }

  for (CompressionType ctype : compression_types) {
    std::string cname;
    if (!GetStringFromCompressionType(&cname, ctype).ok()) {
      // Can produce names like "Reserved4F" for unrecognized values
      cname = CompressionTypeToString(ctype);
    }
    if (options_.compression_manager
            ? options_.compression_manager->SupportsCompressionType(ctype)
            : CompressionTypeSupported(ctype)) {
      CompressionOptions compress_opt = options_.compression_opts;
      // Warm up the compressor once per compression type before timed
      // measurements, so first-use overhead doesn't skew the results.
      compress_opt.level = compress_level_from;
      compress_opt.strategy = strategies.front();
      Status ps = PrimeCompression(ctype, compress_opt, bbto.block_size);
      if (!ps.ok()) {
        return ps;
      }
      for (int strategy : strategies) {
        compress_opt.strategy = strategy;
        for (int32_t j = compress_level_from; j <= compress_level_to; j++) {
          compress_opt.level = j;
          // Measure each combination independently: clear any AutoSkip
          // inter-file estimate left by the previous iteration on this thread.
          BlockBasedTableBuilder::ResetThreadLocalAutoSkipCarryover();
          RecompressionMeasurement m;
          m.compression_name = cname;
          m.block_size = bbto.block_size;
          Status s = ShowCompressionSize(ctype, compress_opt, &m);
          if (!s.ok()) {
            return s;
          }
          if (per_measurement) {
            per_measurement(m);
          }
          recompress_measurements_.push_back(std::move(m));
        }
      }
    } else {
      // Diagnose (to stderr, so it doesn't corrupt JSON on stdout) rather than
      // silently skipping a requested type that the build / compression manager
      // doesn't support.
      fprintf(stderr, "Unsupported compression type: %s.\n", cname.c_str());
    }
  }
  return Status::OK();
}

Status SstFileDumper::ShowCompressionSize(
    CompressionType compress_type, const CompressionOptions& compress_opt,
    RecompressionMeasurement* measurement) {
  assert(measurement);
  Options opts = options_;  // Use compression_manager etc.
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.statistics->set_stats_level(StatsLevel::kAll);
  if (!opts.table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
    // Currently need block-based table for compression
    opts.table_factory = std::make_shared<BlockBasedTableFactory>();
  }

  // Create internal Options types
  const ImmutableOptions imoptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;
  ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);
  InternalTblPropCollFactories block_based_table_factories;

  std::string column_family_name;
  int unknown_level = -1;

  TableBuilderOptions tb_opts(
      imoptions, moptions, read_options, write_options, ikc,
      &block_based_table_factories, compress_type, compress_opt,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      column_family_name, unknown_level, kUnknownNewestKeyTime);
  TableProperties props;
  std::chrono::microseconds write_time;
  std::chrono::microseconds read_time;
  std::chrono::microseconds write_cpu_time;
  std::chrono::microseconds read_cpu_time;
  uint64_t on_disk_index_size = 0;
  Status s = CalculateCompressedTableSize(tb_opts, &props, &write_time,
                                          &read_time, &write_cpu_time,
                                          &read_cpu_time, &on_disk_index_size);
  if (!s.ok()) {
    return s;
  }

  // Remember the properties of this (last) recompressed output so callers can
  // report them alongside the input file's properties.
  recompress_output_props_ = props;
  have_recompress_output_props_ = true;

  uint64_t num_data_blocks = props.num_data_blocks;

  // Report data-block sizes as payload, excluding block trailers, so the
  // compressed size, uncompressed size, and their ratio are all measured on the
  // same (trailer-free) basis. props.data_size is the on-disk data section
  // including one trailer per data block; props.uncompressed_data_size is
  // already trailer-free block payload.
  const uint64_t data_trailers =
      props.num_data_blocks * BlockBasedTable::kBlockTrailerSize;
  const uint64_t compressed_payload = props.data_size > data_trailers
                                          ? props.data_size - data_trailers
                                          : props.data_size;

  const uint64_t compressed_blocks =
      opts.statistics->getAndResetTickerCount(NUMBER_BLOCK_COMPRESSED);
  // Compression attempted but rejected (didn't beat max_compressed_bytes_per_kb
  // or failed verify_compression).
  const uint64_t rejected_blocks = opts.statistics->getAndResetTickerCount(
      NUMBER_BLOCK_COMPRESSION_REJECTED);
  // Compression not attempted (e.g. AutoSkip, or kNoCompression).
  const uint64_t bypassed_blocks = opts.statistics->getAndResetTickerCount(
      NUMBER_BLOCK_COMPRESSION_BYPASSED);
  // These tickers also count index block(s) when enable_index_compression is
  // true, so the total can exceed the (data-only) block count; report the
  // larger total so the categories add up.
  const uint64_t total_blocks =
      compressed_blocks + rejected_blocks + bypassed_blocks;
  if (total_blocks > num_data_blocks) {
    num_data_blocks = total_blocks;
  }

  measurement->compression_type = compress_type;
  measurement->compression_opts = compress_opt;
  measurement->compression_manager =
      options_.compression_manager ? options_.compression_manager->GetId() : "";
  measurement->compressed_data_payload = compressed_payload;
  measurement->uncompressed_data_payload = props.uncompressed_data_size;
  // props.index_size is uncompressed index content plus exactly one block
  // trailer, regardless of how many index partitions exist: for a partitioned
  // index, IndexBuilder::IndexSize() sums the (trailer-free) contents of every
  // partition index block plus the top-level index block, and the builder then
  // adds a single kBlockTrailerSize (see block_based_table_builder.cc). The
  // individual partition-block trailers are therefore not counted here (NOTE),
  // so subtracting one trailer yields the trailer-free index content size.
  measurement->uncompressed_index_payload =
      props.index_size > BlockBasedTable::kBlockTrailerSize
          ? props.index_size - BlockBasedTable::kBlockTrailerSize
          : props.index_size;
  measurement->compressed_index_payload = on_disk_index_size;
  measurement->num_data_blocks = num_data_blocks;
  measurement->blocks_compressed = compressed_blocks;
  measurement->blocks_compression_rejected = rejected_blocks;
  measurement->blocks_compression_bypassed = bypassed_blocks;
  measurement->write_usec = write_time.count();
  measurement->read_usec = read_time.count();
  measurement->write_cpu_usec = write_cpu_time.count();
  measurement->read_cpu_usec = read_cpu_time.count();
  return Status::OK();
}

Status SstFileDumper::GetInputBenchmarkMeasurement(
    RecompressionMeasurement* measurement) {
  assert(measurement);
  *measurement = RecompressionMeasurement();
  measurement->is_input = true;
  measurement->write_usec = -1;
  measurement->read_usec = -1;
  measurement->write_cpu_usec = -1;
  measurement->read_cpu_usec = -1;
  if (table_properties_ == nullptr) {
    return Status::NotSupported("Input table properties unavailable");
  }
  const TableProperties& tp = *table_properties_;
  measurement->compression_name = tp.compression_name;
  measurement->num_data_blocks = tp.num_data_blocks;

  // On-disk data-block payload (excluding per-block trailers). tp.data_size is
  // the data section size including one trailer per data block.
  const uint64_t data_trailers = tp.num_data_blocks * block_trailer_size_;
  const uint64_t data_on_disk_payload = tp.data_size > data_trailers
                                            ? tp.data_size - data_trailers
                                            : tp.data_size;
  measurement->compressed_data_payload = data_on_disk_payload;
  // tp.index_size (persisted) follows the same convention as the builder
  // output: uncompressed index content plus exactly one block trailer,
  // regardless of how many index partitions exist (NOTE the per-partition block
  // trailers are not counted; see the fuller explanation in ShowCompressionSize
  // and block_based_table_builder.cc). So subtract a single trailer to get the
  // trailer-free index content size.
  measurement->uncompressed_index_payload =
      tp.index_size > block_trailer_size_ ? tp.index_size - block_trailer_size_
                                          : tp.index_size;

  // On-disk (compressed) index size. Only block-based inputs have a
  // BlockBasedTable reader (and an index in this sense); for a plain- or
  // cuckoo-table input, GetOnDiskIndexSize returns NotSupported and we fall
  // back to the uncompressed index size. options_.table_factory is the factory
  // that produced table_reader_ (see SetTableOptionsByMagicNumber), so it
  // correctly identifies the reader's type.
  uint64_t on_disk_index = 0;
  if (GetOnDiskIndexSize(options_.table_factory.get(), table_reader_.get(),
                         &on_disk_index)
          .ok()) {
    measurement->compressed_index_payload = on_disk_index;
  } else {
    measurement->compressed_index_payload =
        measurement->uncompressed_index_payload;
  }

  // Uncompressed data size and a benchmarked read time from a single (cold)
  // read pass over the file. Read-side decompression tickers are recorded only
  // for blocks stored compressed; blocks stored uncompressed are recovered via
  // data_on_disk_payload - decompressed_from.
  Statistics* stats = ioptions_.stats;
  if (stats == nullptr) {
    // Statistics not enabled: fall back to the persisted value (may be 0) and
    // leave read time unknown.
    measurement->uncompressed_data_payload = tp.uncompressed_data_size;
    return Status::OK();
  }
  const uint64_t from_before = stats->getTickerCount(BYTES_DECOMPRESSED_FROM);
  const uint64_t to_before = stats->getTickerCount(BYTES_DECOMPRESSED_TO);
  const std::chrono::steady_clock::time_point start =
      std::chrono::steady_clock::now();
  const int64_t cpu_start_micros = ProcessCpuMicros();
  std::unique_ptr<InternalIterator> iter(table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTDumpTool));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
  }
  Status s = iter->status();
  const std::chrono::microseconds read_time =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start);
  const int64_t read_cpu_usec = ProcessCpuMicrosSince(cpu_start_micros);
  if (!s.ok()) {
    return s;
  }
  measurement->read_usec = read_time.count();
  measurement->read_cpu_usec = read_cpu_usec;
  const uint64_t decompressed_from =
      stats->getTickerCount(BYTES_DECOMPRESSED_FROM) - from_before;
  const uint64_t decompressed_to =
      stats->getTickerCount(BYTES_DECOMPRESSED_TO) - to_before;
  // Recover the uncompressed data-block payload as: the uncompressed payload of
  // blocks stored compressed (decompressed_to), plus the on-disk payload of
  // blocks stored uncompressed (data_on_disk_payload - decompressed_from).
  //
  // CAVEAT: the read-side decompression tickers also count index blocks. This
  // is exact when the input's index blocks are stored uncompressed (the
  // default), but if the file was written with enable_index_compression,
  // decompressing the index while iterating adds to decompressed_to (its
  // uncompressed content) more than to decompressed_from (its on-disk content),
  // overstating the reported uncompressed data size by that difference. The
  // clean fix is a persisted uncompressed_data_size table property (see the
  // follow-up noted in the commit that introduced this), which would remove the
  // need for this inference on all but pre-existing files.
  //
  // decompressed_to >= decompressed_from (uncompressed >= on-disk for the same
  // blocks), so (decompressed_to + data_on_disk_payload) >= decompressed_from
  // and the subtraction below does not underflow; the guard is belt-and-braces
  // in case of any ticker inconsistency, and the uncompressed payload is at
  // least the on-disk payload.
  const uint64_t sum = decompressed_to + data_on_disk_payload;
  measurement->uncompressed_data_payload =
      sum >= decompressed_from ? sum - decompressed_from : data_on_disk_payload;
  return Status::OK();
}

// Reads TableProperties prior to opening table reader in order to set up
// options.
Status SstFileDumper::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size,
                                          FilePrefetchBuffer* prefetch_buffer) {
  Status s = ROCKSDB_NAMESPACE::ReadTableProperties(
      file, file_size, table_magic_number, ioptions_, read_options_,
      &table_properties_,
      /* memory_allocator= */ nullptr, prefetch_buffer);
  if (!s.ok()) {
    if (!silent_) {
      fprintf(stdout, "Not able to read table properties\n");
    }
  }
  return s;
}

Status SstFileDumper::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kBlockBasedTableMagicNumber) {
    // Preserve BlockBasedTableOptions on options_ when possible
    if (!options_.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      options_.table_factory = std::make_shared<BlockBasedTableFactory>();
    }

    BlockBasedTableFactory* bbtf =
        static_cast_with_check<BlockBasedTableFactory>(
            options_.table_factory.get());
    // To force tail prefetching, we fake reporting two useful reads of 512KB
    // from the tail.
    // It needs at least two data points to warm up the stats.
    bbtf->tail_prefetch_stats()->RecordEffectiveSize(512 * 1024);
    bbtf->tail_prefetch_stats()->RecordEffectiveSize(512 * 1024);

    if (!silent_) {
      fprintf(stdout, "Sst file format: block-based\n");
    }

    auto& props = table_properties_->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      auto index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(pos->second.c_str()));
      if (index_type_on_file ==
          BlockBasedTableOptions::IndexType::kHashSearch) {
        options_.prefix_extractor.reset(NewNoopTransform());
      }
    }
  } else if (table_magic_number == kPlainTableMagicNumber ||
             table_magic_number == kLegacyPlainTableMagicNumber) {
    options_.allow_mmap_reads = true;

    PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = kPlainTableVariableLength;
    plain_table_options.bloom_bits_per_key = 0;
    plain_table_options.hash_table_ratio = 0;
    plain_table_options.index_sparseness = 1;
    plain_table_options.huge_page_tlb_size = 0;
    plain_table_options.encoding_type = kPlain;
    plain_table_options.full_scan_mode = true;

    options_.table_factory.reset(NewPlainTableFactory(plain_table_options));
    if (!silent_) {
      fprintf(stdout, "Sst file format: plain table\n");
    }
  } else if (table_magic_number == kCuckooTableMagicNumber) {
    ioptions_.allow_mmap_reads = true;

    options_.table_factory.reset(NewCuckooTableFactory());
    if (!silent_) {
      fprintf(stdout, "Sst file format: cuckoo table\n");
    }
  } else {
    char error_msg_buffer[80];
    snprintf(error_msg_buffer, sizeof(error_msg_buffer) - 1,
             "Unsupported table magic number --- %lx",
             (long)table_magic_number);
    return Status::InvalidArgument(error_msg_buffer);
  }

  return Status::OK();
}

Status SstFileDumper::ReadSequential(bool print_kv, uint64_t read_num_limit,
                                     bool has_from, const std::string& from_key,
                                     bool has_to, const std::string& to_key,
                                     bool use_from_as_prefix) {
  if (!table_reader_) {
    return init_result_;
  }

  InternalIterator* iter = table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(),
      /*arena=*/nullptr, /*skip_filters=*/false,
      TableReaderCaller::kSSTDumpTool);

  const Comparator* ucmp = internal_comparator_.user_comparator();
  size_t ts_sz = ucmp->timestamp_size();

  OptSlice from_opt = has_from ? from_key : OptSlice{};
  OptSlice to_opt = has_to ? to_key : OptSlice{};
  std::string from_key_buf;
  std::string to_key_buf;
  auto [from, to] = MaybeAddTimestampsToRange(from_opt, to_opt, ts_sz,
                                              &from_key_buf, &to_key_buf);
  uint64_t i = 0;
  if (from.has_value()) {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(from.value());
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }
  for (; iter->Valid(); iter->Next()) {
    if (read_num_limit > 0 && i >= read_num_limit) {
      break;
    }
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;

    ParsedInternalKey ikey;
    Status pik_status = ParseInternalKey(key, &ikey, true /* log_err_key */);
    if (!pik_status.ok()) {
      std::cerr << pik_status.getState() << "\n";
      continue;
    }

    // the key returned is not prefixed with out 'from' key
    if (use_from_as_prefix && !ikey.user_key.starts_with(from_key)) {
      break;
    }

    // If end marker was specified, we stop before it
    if (to.has_value() && ucmp->Compare(ikey.user_key, to.value()) >= 0) {
      break;
    }

    if (print_kv) {
      if (!decode_blob_index_ || ikey.type != kTypeBlobIndex) {
        if (ikey.type == kTypeWideColumnEntity) {
          std::ostringstream oss;
          const Status s = WideColumnsHelper::DumpSliceAsWideColumns(
              iter->value(), oss, output_hex_);
          if (!s.ok()) {
            fprintf(stderr, "%s => error deserializing wide columns\n",
                    ikey.DebugString(true, output_hex_, ucmp).c_str());
            continue;
          }
          fprintf(stdout, "%s => %s\n",
                  ikey.DebugString(true, output_hex_, ucmp).c_str(),
                  oss.str().c_str());
        } else if (ikey.type == kTypeValuePreferredSeqno) {
          auto [unpacked_value, preferred_seqno] =
              ParsePackedValueWithSeqno(value);
          fprintf(stdout, "%s => %s, %llu\n",
                  ikey.DebugString(true, output_hex_, ucmp).c_str(),
                  unpacked_value.ToString(output_hex_).c_str(),
                  static_cast<unsigned long long>(preferred_seqno));
        } else {
          fprintf(stdout, "%s => %s\n",
                  ikey.DebugString(true, output_hex_, ucmp).c_str(),
                  value.ToString(output_hex_).c_str());
        }
      } else {
        BlobIndex blob_index;

        const Status s = blob_index.DecodeFrom(value);
        if (!s.ok()) {
          fprintf(stderr, "%s => error decoding blob index\n",
                  ikey.DebugString(true, output_hex_, ucmp).c_str());
          continue;
        }

        fprintf(stdout, "%s => %s\n",
                ikey.DebugString(true, output_hex_, ucmp).c_str(),
                blob_index.DebugString(output_hex_).c_str());
      }
    }
  }

  read_num_ += i;

  Status ret = iter->status();

  bool verify_num_entries =
      (read_num_limit == 0 ||
       read_num_limit == std::numeric_limits<uint64_t>::max()) &&
      !has_from && !has_to;
  if (verify_num_entries && ret.ok()) {
    // Compare the number of entries
    if (!table_properties_) {
      fprintf(stderr, "Table properties not available.");
    } else {
      // TODO: verify num_range_deletions
      if (i != table_properties_->num_entries -
                   table_properties_->num_range_deletions) {
        std::ostringstream oss;
        oss << "Table property expects "
            << table_properties_->num_entries -
                   table_properties_->num_range_deletions
            << " entries when excluding range deletions,"
            << " but scanning the table returned " << std::to_string(i)
            << " entries";
        ret = Status::Corruption(oss.str());
      }
    }
  }

  delete iter;
  return ret;
}

// Provides TableProperties to API user
Status SstFileDumper::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}
}  // namespace ROCKSDB_NAMESPACE
