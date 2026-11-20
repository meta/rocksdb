// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.

enum CompressionType : unsigned char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x00,
  kSnappyCompression = 0x01,
  kZlibCompression = 0x02,
  kBZip2Compression = 0x03,
  // NOTE: LZ4 and LZ4HC should be considered variants of the same compression
  // type. See CompressionOptions::level
  kLZ4Compression = 0x04,
  kLZ4HCCompression = 0x05,
  kXpressCompression = 0x06,
  kZSTD = 0x07,
  kLastBuiltinCompression = kZSTD,

  // Reserved for future use: up to 0x7F

  // For use by user custom CompressionManagers
  kCustomCompression80 = 0x80,
  kFirstCustomCompression = kCustomCompression80,
  kCustomCompression81 = 0x81,
  kCustomCompression82 = 0x82,
  kCustomCompression83 = 0x83,
  kCustomCompression84 = 0x84,
  kCustomCompression85 = 0x85,
  kCustomCompression86 = 0x86,
  kCustomCompression87 = 0x87,
  kCustomCompression88 = 0x88,
  kCustomCompression89 = 0x89,
  kCustomCompression8A = 0x8A,
  kCustomCompression8B = 0x8B,
  kCustomCompression8C = 0x8C,
  kCustomCompression8D = 0x8D,
  kCustomCompression8E = 0x8E,
  kCustomCompression8F = 0x8F,
  kCustomCompression90 = 0x90,
  kCustomCompression91 = 0x91,
  kCustomCompression92 = 0x92,
  kCustomCompression93 = 0x93,
  kCustomCompression94 = 0x94,
  kCustomCompression95 = 0x95,
  kCustomCompression96 = 0x96,
  kCustomCompression97 = 0x97,
  kCustomCompression98 = 0x98,
  kCustomCompression99 = 0x99,
  kCustomCompression9A = 0x9A,
  kCustomCompression9B = 0x9B,
  kCustomCompression9C = 0x9C,
  kCustomCompression9D = 0x9D,
  kCustomCompression9E = 0x9E,
  kCustomCompression9F = 0x9F,
  kCustomCompressionA0 = 0xA0,
  kCustomCompressionA1 = 0xA1,
  kCustomCompressionA2 = 0xA2,
  kCustomCompressionA3 = 0xA3,
  kCustomCompressionA4 = 0xA4,
  kCustomCompressionA5 = 0xA5,
  kCustomCompressionA6 = 0xA6,
  kCustomCompressionA7 = 0xA7,
  kCustomCompressionA8 = 0xA8,
  kCustomCompressionA9 = 0xA9,
  kCustomCompressionAA = 0xAA,
  kCustomCompressionAB = 0xAB,
  kCustomCompressionAC = 0xAC,
  kCustomCompressionAD = 0xAD,
  kCustomCompressionAE = 0xAE,
  kCustomCompressionAF = 0xAF,
  kCustomCompressionB0 = 0xB0,
  kCustomCompressionB1 = 0xB1,
  kCustomCompressionB2 = 0xB2,
  kCustomCompressionB3 = 0xB3,
  kCustomCompressionB4 = 0xB4,
  kCustomCompressionB5 = 0xB5,
  kCustomCompressionB6 = 0xB6,
  kCustomCompressionB7 = 0xB7,
  kCustomCompressionB8 = 0xB8,
  kCustomCompressionB9 = 0xB9,
  kCustomCompressionBA = 0xBA,
  kCustomCompressionBB = 0xBB,
  kCustomCompressionBC = 0xBC,
  kCustomCompressionBD = 0xBD,
  kCustomCompressionBE = 0xBE,
  kCustomCompressionBF = 0xBF,
  kCustomCompressionC0 = 0xC0,
  kCustomCompressionC1 = 0xC1,
  kCustomCompressionC2 = 0xC2,
  kCustomCompressionC3 = 0xC3,
  kCustomCompressionC4 = 0xC4,
  kCustomCompressionC5 = 0xC5,
  kCustomCompressionC6 = 0xC6,
  kCustomCompressionC7 = 0xC7,
  kCustomCompressionC8 = 0xC8,
  kCustomCompressionC9 = 0xC9,
  kCustomCompressionCA = 0xCA,
  kCustomCompressionCB = 0xCB,
  kCustomCompressionCC = 0xCC,
  kCustomCompressionCD = 0xCD,
  kCustomCompressionCE = 0xCE,
  kCustomCompressionCF = 0xCF,
  kCustomCompressionD0 = 0xD0,
  kCustomCompressionD1 = 0xD1,
  kCustomCompressionD2 = 0xD2,
  kCustomCompressionD3 = 0xD3,
  kCustomCompressionD4 = 0xD4,
  kCustomCompressionD5 = 0xD5,
  kCustomCompressionD6 = 0xD6,
  kCustomCompressionD7 = 0xD7,
  kCustomCompressionD8 = 0xD8,
  kCustomCompressionD9 = 0xD9,
  kCustomCompressionDA = 0xDA,
  kCustomCompressionDB = 0xDB,
  kCustomCompressionDC = 0xDC,
  kCustomCompressionDD = 0xDD,
  kCustomCompressionDE = 0xDE,
  kCustomCompressionDF = 0xDF,
  kCustomCompressionE0 = 0xE0,
  kCustomCompressionE1 = 0xE1,
  kCustomCompressionE2 = 0xE2,
  kCustomCompressionE3 = 0xE3,
  kCustomCompressionE4 = 0xE4,
  kCustomCompressionE5 = 0xE5,
  kCustomCompressionE6 = 0xE6,
  kCustomCompressionE7 = 0xE7,
  kCustomCompressionE8 = 0xE8,
  kCustomCompressionE9 = 0xE9,
  kCustomCompressionEA = 0xEA,
  kCustomCompressionEB = 0xEB,
  kCustomCompressionEC = 0xEC,
  kCustomCompressionED = 0xED,
  kCustomCompressionEE = 0xEE,
  kCustomCompressionEF = 0xEF,
  kCustomCompressionF0 = 0xF0,
  kCustomCompressionF1 = 0xF1,
  kCustomCompressionF2 = 0xF2,
  kCustomCompressionF3 = 0xF3,
  kCustomCompressionF4 = 0xF4,
  kCustomCompressionF5 = 0xF5,
  kCustomCompressionF6 = 0xF6,
  kCustomCompressionF7 = 0xF7,
  kCustomCompressionF8 = 0xF8,
  kCustomCompressionF9 = 0xF9,
  kCustomCompressionFA = 0xFA,
  kCustomCompressionFB = 0xFB,
  kCustomCompressionFC = 0xFC,
  kCustomCompressionFD = 0xFD,
  kCustomCompressionFE = 0xFE,
  kLastCustomCompression = kCustomCompressionFE,

  // kDisableCompressionOption is used to disable some compression options.
  kDisableCompressionOption = 0xff,
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  // ==> BEGIN options that can be set by deprecated configuration syntax, <==
  // ==> e.g. compression_opts=5:6:7:8:9:10:true:11:false                  <==
  // ==> Please use compression_opts={level=6;strategy=7;} form instead.   <==

  // RocksDB's generic default compression level. Internally it'll be translated
  // to the default compression level specific to the library being used (see
  // comment above `ColumnFamilyOptions::compression`).
  //
  // The default value is the max 16-bit int as it'll be written out in OPTIONS
  // file, which should be portable.
  static constexpr int kDefaultCompressionLevel = 32767;

  // zlib only: windowBits parameter. See https://www.zlib.net/manual.html
  int window_bits = -14;

  // Compression "level" applicable to zstd, zlib, LZ4, and LZ4HC. Except for
  // kDefaultCompressionLevel (see above), the meaning of each value depends
  // on the compression algorithm. Decreasing across non-
  // `kDefaultCompressionLevel` values will either favor speed over
  // compression ratio or have no effect.
  //
  // LZ4 and LZ4HC share one on-disk format, so they expose a single monotonic
  // `level` axis: `level <= 0` selects LZ4 fast with `acceleration = -level`
  // (e.g. -10 means acceleration 10, 0 is clamped to minimum acceleration 1),
  // and `level >= 1` selects LZ4HC at that level (1..12). The configured type
  // (kLZ4Compression vs kLZ4HCCompression) only sets the default `level` and
  // the compression type byte recorded per block. Defaults are acceleration 1
  // (level -1) for LZ4 and level 9 for LZ4HC. Levels outside the
  // algorithm-recognized ranges are clamped to the nearest effective value:
  // below -65537 acts like -65537 (max acceleration) and above 12 like 12 (max
  // compression effort).
  //
  // For ZSTD (the other generally recommended compression alongside LZ4),
  // `level` follows zstd's own scale, where higher values trade more CPU for a
  // better compression ratio. The standard range is 1 (fastest) through 19,
  // with 20..22 being "ultra" levels that require substantially more memory to
  // compress (and decompress). zstd also supports negative levels (-1 and
  // below) that favor speed over ratio, analogous to LZ4 acceleration.
  // RocksDB's default (kDefaultCompressionLevel) maps to zstd's default of 3.
  // (zstd itself treats a `level` of 0 as "use the default", but prefer
  // kDefaultCompressionLevel to request that.)
  int level = kDefaultCompressionLevel;

  // zlib only: strategy parameter. See https://www.zlib.net/manual.html
  int strategy = 0;

  // Maximum size of dictionaries used to prime the compression library.
  // Enabling dictionary can improve compression ratios when there are
  // repetitions across data blocks.
  //
  // The dictionary is created by sampling the SST file data. If
  // `zstd_max_train_bytes` is nonzero, the samples are passed through zstd's
  // dictionary generator (see comments for option `use_zstd_dict_trainer` for
  // detail on dictionary generator). If `zstd_max_train_bytes` is zero, the
  // random samples are used directly as the dictionary.
  //
  // When compression dictionary is disabled, we compress and write each block
  // before buffering data for the next one. When compression dictionary is
  // enabled, we buffer SST file data in-memory so we can sample it, as data
  // can only be compressed and written after the dictionary has been finalized.
  //
  // The amount of data buffered can be limited by `max_dict_buffer_bytes`. This
  // buffered memory is charged to the block cache when there is a block cache.
  // If block cache insertion fails with `Status::MemoryLimit` (i.e., it is
  // full), we finalize the dictionary with whatever data we have and then stop
  // buffering.
  uint32_t max_dict_bytes = 0;

  // Maximum size of training data passed to zstd's dictionary trainer. Using
  // zstd's dictionary trainer can achieve even better compression ratio
  // improvements than using `max_dict_bytes` alone.
  //
  // The training data will be used to generate a dictionary of max_dict_bytes.
  uint32_t zstd_max_train_bytes = 0;

  // Number of threads for parallel compression for each running flush or
  // compaction job. Parallel compression is enabled only if threads > 1. Not
  // recommended for lightweight compression algorithms such as Snappy, LZ4, and
  // obviously kNoCompression because there is unlikely to be a throughput gain.
  //
  // This option is valid only when BlockBasedTable is used, and is sanitized
  // to 1 when partition_filters == true && decouple_partitioned_filters ==
  // false. With a user_defined_index_factory, an SST is compressed in parallel
  // only if every index builder supports it (see
  // IndexFactoryBuilder::SupportsParallelAddEntry()); otherwise it is
  // compressed single-threaded and a warning is logged.
  //
  // When parallel compression is enabled, SST size file sizes might be
  // more inflated compared to the target size, because more data of unknown
  // compressed size is in flight when compression is parallelized. To be
  // reasonably accurate, this inflation is also estimated by using historical
  // compression ratio and current bytes inflight.
  uint32_t parallel_threads = 1;

  // When the compression options are set by the user, it will be set to "true".
  // For bottommost_compression_opts, to enable it, user must set enabled=true.
  // Otherwise, bottommost compression will use compression_opts as default
  // compression options.
  //
  // For compression_opts, if compression_opts.enabled=false, it is still
  // used as compression options for compression process.
  bool enabled = false;

  // Limit on data buffering when gathering samples to build a dictionary. Zero
  // means no limit. When dictionary is disabled (`max_dict_bytes == 0`),
  // enabling this limit (`max_dict_buffer_bytes != 0`) has no effect.
  //
  // In compaction, the buffering is limited to the target file size (see
  // `target_file_size_base` and `target_file_size_multiplier`) even if this
  // setting permits more buffering. Since we cannot determine where the file
  // should be cut until data blocks are compressed with dictionary, buffering
  // more than the target file size could lead to selecting samples that belong
  // to a later output SST.
  //
  // Limiting too strictly may harm dictionary effectiveness since it forces
  // RocksDB to pick samples from the initial portion of the output SST, which
  // may not be representative of the whole file. Configuring this limit below
  // `zstd_max_train_bytes` (when enabled) can restrict how many samples we can
  // pass to the dictionary trainer. Configuring it below `max_dict_bytes` can
  // restrict the size of the final dictionary.
  uint64_t max_dict_buffer_bytes = 0;

  // Use zstd trainer to generate dictionaries. When this option is set to true,
  // zstd_max_train_bytes of training data sampled from max_dict_buffer_bytes
  // buffered data will be passed to zstd dictionary trainer to generate a
  // dictionary of size max_dict_bytes.
  //
  // When this option is false, zstd's API ZDICT_finalizeDictionary() will be
  // called to generate dictionaries. zstd_max_train_bytes of training sampled
  // data will be passed to this API. Using this API should save CPU time on
  // dictionary training, but the compression ratio may not be as good as using
  // a dictionary trainer.
  bool use_zstd_dict_trainer = true;

  // ===> END options that can be set by deprecated configuration syntax <===
  // ===> Use compression_opts={level=6;strategy=7;} form for below opts <===

  // Essentially specifies a minimum acceptable compression ratio. A block is
  // stored uncompressed if the compressed block does not achieve this ratio,
  // because the downstream cost of decompression is not considered worth such
  // a small savings (if any).
  // However, the ratio is specified in a way that is efficient for checking.
  // An integer from 1 to 1024 indicates the maximum allowable compressed bytes
  // per 1KB of input, so the minimum acceptable ratio is 1024.0 / this value.
  // For example, for a minimum ratio of 1.5:1, set to 683. See SetMinRatio().
  // Default: abandon use of compression for a specific block or entry if
  // compressed by less than 12.5% (minimum ratio of 1.143:1).
  int max_compressed_bytes_per_kb = 1024 * 7 / 8;

  // EXPERIMENTAL: "auto-skip" for compression. When true, the block-based table
  // builder continuously estimates the compression ratio it is actually
  // achieving on data blocks and, once that running estimate is worse than
  // max_compressed_bytes_per_kb (i.e. attempting compression is not paying
  // off), stops attempting compression on most data blocks to save CPU. It
  // still (a) always attempts the first data block of each file and (b) samples
  // a small fraction of blocks (see auto_skip_min_sample_every) so it can
  // resume compressing if the data becomes compressible again. Estimator state
  // is carried across the files a compaction/flush thread emits, so a sustained
  // incompressible region does not re-learn on every file. Applies to data
  // blocks only (index/meta blocks are unaffected). Default false (disabled).
  //
  // The decision reuses max_compressed_bytes_per_kb -- the same "minimum
  // compression worth keeping" bar already applied per block -- as the bar for
  // whether attempting compression on the stream is worthwhile. The estimate is
  // the aggregate stored bytes per KB the builder would achieve by attempting
  // (a block that compresses is counted at its achieved ratio; one that is
  // rejected and stored raw is counted at 1024/1024). This is inherently
  // payoff-weighted: a stream of blocks that each save only a little, or only a
  // few blocks that compress well, will not clear the bar and is skipped, while
  // a stream that compresses substantially is attempted. Because the bar is
  // shared, an amount of compression given up per KB is bounded by
  // (1024 - max_compressed_bytes_per_kb) -- i.e. never more than the minimum
  // savings you already declared worth pursuing.
  //
  // This lives on CompressionOptions (rather than a block-based-table-specific
  // option) because auto-skip is a property of how compression is applied, not
  // of the block-based format per se; the mechanism could reasonably generalize
  // to other table formats or compression consumers in the future. For now only
  // the block-based table builder implements it.
  //
  // Caveat: auto-skip is a payoff heuristic, not a CPU-cost model. It assumes
  // the CPU spent attempting compression is roughly proportional to the
  // uncompressed size (empirically true within a small factor for real data),
  // and it decides purely on achieved ratio. It will therefore still attempt
  // data that clears the bar even in the rare case that such data is unusually
  // expensive to compress.
  //
  // For now disabled by default, but we expect this to become enabled by
  // default in a future release.
  bool auto_skip = false;

  // EXPERIMENTAL: only meaningful when auto_skip is true.
  // While in the skip regime, auto-skip still attempts compression on roughly
  // one out of every this many skipped data blocks (randomized by +/- 25% to
  // avoid clustering and predictable patterns) so it can detect a return to
  // compressible data. Smaller values detect such transitions sooner at the
  // cost of more sampling CPU; larger values save more CPU but react more
  // slowly. 0 selects an internal default.
  //
  // In short, this parameter limits the local impact around a worst-case
  // transition from all-incompressible to all-compressible, while the
  // max_compressed_bytes_per_kb threshold governs the long term behavior with
  // mixed compressibility.
  int auto_skip_min_sample_every = 0;

  // ZSTD only.
  // Enable compression algorithm's checksum feature.
  // (https://github.com/facebook/zstd/blob/d857369028d997c92ff1f1861a4d7f679a125464/lib/zstd.h#L428)
  // Each compressed frame will have a 32-bit checksum attached. The checksum
  // computed from the uncompressed data and can be verified during
  // decompression.
  bool checksum = false;

  // A convenience function for setting max_compressed_bytes_per_kb based on a
  // minimum acceptable compression ratio (uncompressed size over compressed
  // size).
  void SetMinRatio(double min_ratio) {
    max_compressed_bytes_per_kb = static_cast<int>(1024.0 / min_ratio + 0.5);
  }

  bool operator==(const CompressionOptions& rhs) const = default;
};

// See advanced_compression.h
class CompressionManager;

}  // namespace ROCKSDB_NAMESPACE
