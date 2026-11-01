//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

// DataBlockFooter represents the footer of a data block, containing metadata
// about the block's structure and features.
//
// Current encoding (may expand in future format versions):
// - A single uint32_t where:
//   - The low 28 bits store the number of restart points (num_restarts)
//   - The high 4 bits are reserved for metadata/features:
//     - Bit 31: Hash index present (kDataBlockBinaryAndHash)
//     - Bit 30: RESERVED "extended metadata present" escape (format_version >=
//       8). No feature defines it yet, so it must be 0; a set bit is rejected
//       as unsupported/corrupt. It lets a future extension add block metadata
//       by setting this bit and writing an additional section -- handled in the
//       one shared DataBlockFooter decoder -- without another format_version
//       bump or plumbing format_version into every kind of block parser.
//     - Bit 29: Uniform keys flag (for kAuto index block search)
//     - Bit 28: Separated KV storage (keys and values stored in separate
//       sections within the block)
//
// The format_version 8 common user-key prefix feature does NOT use a footer
// bit: the prefix occupies the leading bytes [0, restarts[0]) of the block, so
// a non-zero restarts[0] self-signals it. restarts[0] is always 0 in every
// other block (all versions, all block types), so the reader detects the prefix
// without a bit and without knowing format_version. Older readers would
// misinterpret it, which is why it is gated on format_version 8 at the file
// level (older readers reject the whole file).
//
// Note on forward compatibility: Bits 28-29 can be read by older versions of
// RocksDB and interpreted as an extremely large num_restarts, which will be
// caught as a corruption. Bit 30 is special because num_restarts is multipled
// by 4, causing overflow and the corruption check to be silently ignored
// (https://github.com/meta/rocksdb/blob/10.11.fb/table/block_based/block.cc#L1070-L1103)
// Now format_version >= 8 officially unlocks use of that bit because we know
// the reader isn't going to ignore it.
//
// When separated KV is enabled, an additional uint32_t is prepended before the
// packed footer, storing the offset to the values section within the block.
//
// When any unrecognized reserved bit is set, DecodeFrom() returns an error,
// allowing older versions to fail gracefully on newer formats.
//
// The encoding size is not fixed - future format versions may expand it.
// Use kMaxEncodedLength for buffer sizing.
struct DataBlockFooter {
  // Maximum number of restarts that can be stored (2^28 - 1 = 268,435,455).
  // This reserves the top 4 bits for metadata (bit 31 for hash index, bits
  // 28-30 for future features). For historical compatibility purposes, the
  // limit is adequate because a 4GiB block (maximum due to 32-bit block size)
  // with restart_interval=1 and minimum entries (12 bytes: 3 varint bytes +
  // 9-byte internal key + empty value) plus 4-byte restart offsets = 16 bytes
  // per restart, fits at most (2^32 - 4) / 16 ~= 268 million restarts.
  static constexpr uint32_t kMaxNumRestarts = (1u << 28) - 1;

  // Maximum encoded length of a DataBlockFooter (for buffer sizing).
  // 8 bytes when separated KV is enabled (values_section_offset + packed),
  // 4 bytes otherwise.
  static constexpr uint32_t kMaxEncodedLength = 2 * sizeof(uint32_t);

  // Minimum encoded length (for current format version)
  static constexpr uint32_t kMinEncodedLength = sizeof(uint32_t);

  BlockBasedTableOptions::DataBlockIndexType index_type =
      BlockBasedTableOptions::kDataBlockBinarySearch;

  // Whether the block uses separated KV storage (keys and values in separate
  // sections). When true, values_section_offset indicates where the values
  // section begins within the block data.
  bool separated_kv = false;
  uint32_t values_section_offset = 0;

  uint32_t num_restarts = 0;
  bool is_uniform = false;

  DataBlockFooter() = default;
  DataBlockFooter(BlockBasedTableOptions::DataBlockIndexType _index_type,
                  uint32_t _num_restarts)
      : index_type(_index_type), num_restarts(_num_restarts) {}

  // Appends the encoded footer to dst.
  void EncodeTo(std::string* dst) const;

  // Decodes a footer from the end of input (consumes bytes from the end).
  // Returns an error if reserved/unrecognized feature bits are set.
  // On success, advances input to exclude the consumed footer bytes.
  Status DecodeFrom(Slice* input);
};

}  // namespace ROCKSDB_NAMESPACE
