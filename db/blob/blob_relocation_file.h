// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

// Blob relocation file
// ====================
//
// Background: indirect blob GC removes garbage from a blob file without
// rewriting any SST. It copies the live values of blob files that have a lot of
// garbage into one new file, the relocation file. A route (old file number ->
// relocation file) then sends reads of the old file number to the relocation
// file. The old file number is the generation number that the SST's BlobIndex
// stores. The reader finds the value in the relocation file by its stable
// identity (origin, original offset), which never changes when the value moves.
// This file defines the relocation file format, its directory, and its key
// encoding.
//
// Terms: generation number ("Generation" in db/blob/blob_index.h), origin,
// original offset, stable identity, logical CRC and stored CRC are defined in
// db/blob/blob_index.h. Its Generation paragraph also explains routes. This
// file adds:
//   origin partition  All values in this file that share one origin file.
//                     Each has one directory entry (BlobRelocationOrigin).
//   record            One stored value in the SimpleGen2Blob format
//                     (db/blob/blob_gen2_format.h). Normal SSTs use the
//                     same format for embedded (same-file) blobs, so
//                     "embedded blob", "same-file blob" and "SimpleGen2
//                     record" name the same record format.
//   record bytes      Stored value size + kSimpleGen2BlobTrailerSize
//                     (5 bytes: 1 compression byte and a 4-byte checksum).
//
// Layout
// ------
// A relocation file is a block-based table with an 8-byte prefix in front.
// The table builder writes each value itself, as a record in the file, before
// the data block that holds the value's location entry.
//
//   offset 0
//   +---------------------------+
//   | "RDBRLC01"                |  prefix (kBlobRelocationFilePrefix)
//   +---------------------------+
//   | record                    |  each record: stored bytes |
//   |                           |    compression (1) | checksum (4)
//   | record                    |
//   | ...                       |
//   | data block                |  location entries of the records above
//   | record ...                |
//   | data block                |
//   +---------------------------+  <- tail start
//   | index, properties,        |  properties hold the directory
//   | metaindex, footer         |
//   +---------------------------+
//
// - Offsets: every block handle and record offset is an absolute file offset.
//   The prefix only moves the first record to offset 8.
// - Prefix: relocation files use the normal blob file name (<number>.blob).
//   The prefix tells them apart from blob log files, which start with a
//   BlobLogHeader, for a tool that scans the DB directory without the
//   MANIFEST. A reader that is told the file is a relocation file does not
//   read the prefix. It checks the table footer, file number, tail size and
//   directory digest (see "Directory" below) instead
//   (BlobRelocationFileReader::Open).
// - Record: the stored bytes are copied verbatim (never recompressed). The
//   compression byte (the record's compression marker) tells how the stored
//   bytes are compressed. The checksum uses the table's checksum type. It
//   covers the stored bytes and the compression byte, and it is modified by
//   the record's file offset, so a record copied to another offset fails the
//   check.
// - Location entry: one key-value pair in a data block. It locates a group: up
//   to kBlobRelocationGroupSize (32) records of one origin, in original offset
//   order. The table builder starts a new group when the origin changes or the
//   group is full. A group shares one key and one entry header among up to
//   32 values, so it takes far fewer bytes per value than one entry per value.
// - Key: 16 bytes (kBlobRelocationKeySize): the big-endian origin file number,
//   then the big-endian original offset of the LAST value in the group.
//   Big-endian makes bytewise order equal to numeric (origin, offset) order.
//   So the first entry at or after (origin, offset) is the only group that
//   can hold that value. The internal key has sequence 0 and type kTypeValue:
//   the value is a location entry, not a BlobIndex, so code that decodes
//   BlobIndex values (for example a plain table read) does not try to decode
//   it. The comparator is always bytewise, without user timestamps.
// - Value (EncodeBlobRelocationGroup()), all numbers as varints:
//     count (varint32, 1..32) | first record offset (varint64) |
//     count x { offset gap (varint64) | stored size (varint64) }
//   The offset gap of a value is the number of bytes between the end of the
//   value before it and its own start: original offset - (previous original
//   offset + previous stored size). For the first value of an entry, the end
//   before it is 0, so its gap is its original offset. For the other values,
//   a record header and the user key lie between two values in a normal blob
//   file, so a gap is small and takes one or two bytes. Values of one origin
//   never overlap, so a gap is never negative, and it may be 0.
//   The records of one group are written back to back, so the record offset
//   of a value is the first record offset plus the record bytes of the values
//   before it. Each stored size is 1..kMaxSimpleGen2BlobPayloadSize (an empty
//   value is never written to a blob file). The entry has no compression
//   type: a reader expects the compression of the indirect BlobIndex, and the
//   record read checks it against the record's compression marker.
// - Table options (MakeBlobRelocationTableOptions): binary search index, no
//   filter, no UDI. Every lookup comes from a live BlobIndex, so the value
//   always exists and a filter cannot help. A lookup key usually lies between
//   two entry keys, so data blocks have no hash index (it finds only exact
//   keys), and the index uses no key shortening (kNoShortening): the index key
//   of a data block is its last key, so the block that the index finds always
//   holds the first entry at or after the lookup key. The batched lookup
//   (BlockBasedTable::MultiGetRawEntries) does not step to the next block. The
//   block size is 4 KB. This is an estimate, not a measured best value. The
//   column family's restart interval is kept. Block alignment is off, because
//   padding only wastes space. The table builder also turns off compression
//   sampling (BlockBasedTableBuilder::StartBlobRelocationFile()), because
//   sampling reports stats for user data and these data blocks hold only
//   location entries.
//
// Directory
// ---------
// The directory is stored in the table property
// kBlobRelocationDirectoryPropertyName. It starts with a 48-byte header:
//   magic (4) | format version (4) | origin count (8) | served file count (8) |
//   total entries (8) | total record bytes (8) | directory digest (4) |
//   header CRC (4)
// The header CRC covers the first 44 bytes. The decoder first checks that the
// counts match the directory size, then checks the header CRC, and only then
// reads any entry. So a damaged header fails before any entry is read. The
// digest covers the counts, the totals and the body, but not the magic and the
// version, so it identifies the file contents only. After the header, the
// directory has one entry per origin partition, sorted by origin:
//   origin file number | first and last original offset | entry count |
//   record bytes
// The entries are followed by the served files. These are the old file
// numbers that may be routed to this file. The writer adds them with
// AddBlobRelocationServedFile(). The reader checks at open that every served
// number is older than the file. The table builder computes the entries from
// the records it writes.
//
// The table property num_entries is the number of groups. With the group rule
// above it equals, for each origin partition, its entry count divided by 32
// and rounded up, summed over the partitions; BlobRelocationFileReader::Open()
// checks this.
//
// The directory digest (directory_digest()) is the CRC32C of the counts, the
// totals and the directory body. It is the file's identity: the code that
// installs the file keeps the digest, and BlobRelocationFileReader::Open()
// rejects a file whose directory digest does not match the expected one.
//
// Reads
// -----
// A read seeks the key (origin, original offset). It checks that the entry
// found has the same origin, decodes its group, finds the exact original
// offset in it (Corruption if it is not there), and reads the record. The
// table below says which checksums a read checks. The record checksum is
// checked by BlockBasedTable::MultiGetSameFileBlob(). The logical CRC is
// checked by BlobSource::FinishIndirectBlobRead(), with the same rule for
// indirect reads of normal blob files. As in base RocksDB, a range of a
// compressed value is not supported: a read of one returns Corruption.
//
//   Read kind                 Record checksum       Logical CRC
//   ------------------------  --------------------  -------------------------
//   whole value               when verify_checksums when verify_checksums is
//                             is set (the verify    set or the value goes into
//                             policy)               the stable blob cache
//   range of an uncompressed  never: only the       never
//   value (callers send only  range bytes are
//   strict parts)             read, not the record
//                             trailer

namespace ROCKSDB_NAMESPACE {

// For how the MANIFEST adds relocation files and routes reads to them, see the
// overview in db/blob/blob_relocation_manifest.h.

// Table property that holds the encoded BlobRelocationDirectory.
inline constexpr char kBlobRelocationDirectoryPropertyName[] =
    "rocksdb.blob.relocation-directory";

// The first 8 bytes of every relocation file. They tell a relocation file
// apart from a blob log file with the same <number>.blob name. Block handles
// and record offsets stay absolute file offsets. The prefix only moves the
// first record to offset 8.
inline constexpr char kBlobRelocationFilePrefix[] = "RDBRLC01";
inline constexpr size_t kBlobRelocationFilePrefixSize =
    sizeof(kBlobRelocationFilePrefix) - 1;

// Directory entry of one origin partition.
struct BlobRelocationOrigin {
  // Blob file where the values were first written.
  uint64_t origin_file_number = 0;
  // Smallest and largest original offset of the partition's values.
  uint64_t first_original_offset = 0;
  uint64_t last_original_offset = 0;
  // Number of values (records) in the partition. Each location entry holds up
  // to kBlobRelocationGroupSize (32) values, so the partition has entry_count
  // / 32 location entries, rounded up. For example, 70 values use 3 location
  // entries.
  uint64_t entry_count = 0;
  // Sum of stored value size + kSimpleGen2BlobTrailerSize over the entries.
  uint64_t record_bytes = 0;
};

// The directory of a relocation file (see "Directory" at the top of this
// file). It holds one checked entry per origin partition, sorted by origin,
// the list of served files, the totals and the digest that identifies the
// file. The table builder and BlobRelocationFileReader::Open check that every
// origin and served file is older (has a smaller number) than the relocation
// file itself. The directory is immutable after DecodeFrom() or
// BlobRelocationDirectoryBuilder::Finish(), so several threads can read it.
class BlobRelocationDirectory {
 public:
  // Version of the directory encoding. Readers reject any other version.
  static constexpr uint32_t kFormatVersion = 1;

  // Encodes the directory (header, entries, served files, digest, header CRC)
  // into `*output`, replacing its contents.
  void EncodeTo(std::string* output) const;

  // Decodes and checks an encoded directory. Returns Corruption on any
  // failure. `*directory` is changed only on success. It checks, in order:
  //   - the magic and the version;
  //   - at least one entry and one served file, and a size of exactly the
  //     header plus the stated entries and served files (this bounds the
  //     counts, so there is no fixed count limit);
  //   - the header CRC and the digest;
  //   - each entry and the entry order;
  //   - served files in strictly increasing order;
  //   - the totals.
  static Status DecodeFrom(const Slice& input,
                           BlobRelocationDirectory* directory);

  // Rejects a directory that cannot fit in the physical relocation file: its
  // encoded size must fit in `file_size`, and its record bytes must fit in
  // `file_size` minus the prefix. This is a loose bound: a file that passes
  // may still be too small.
  Status ValidateForFileSize(uint64_t file_size) const;

  // Entry of `origin_file_number`, or nullptr. Binary search.
  const BlobRelocationOrigin* Find(uint64_t origin_file_number) const;

  const std::vector<BlobRelocationOrigin>& origins() const { return origins_; }

  // The old file numbers that may be routed to this file, increasing.
  const std::vector<uint64_t>& served_file_numbers() const {
    return served_file_numbers_;
  }
  uint64_t total_entries() const { return total_entries_; }
  uint64_t total_record_bytes() const { return total_record_bytes_; }
  // The directory digest (see "Directory" at the top of this file).
  uint32_t directory_digest() const { return directory_digest_; }
  // Size of the encoding.
  uint64_t encoded_size() const { return encoded_size_; }

 private:
  friend class BlobRelocationDirectoryBuilder;

  std::vector<BlobRelocationOrigin> origins_;
  std::vector<uint64_t> served_file_numbers_;
  uint64_t total_entries_ = 0;
  uint64_t total_record_bytes_ = 0;
  uint32_t directory_digest_ = 0;
  // Size of the encoding this directory was decoded from or encodes to.
  uint64_t encoded_size_ = 0;
};

// Builds the directory while the table builder appends records in key order
// (origin, original offset). It keeps one entry per origin partition, not one
// object per value. It is not thread-safe. One table builder uses it.
class BlobRelocationDirectoryBuilder {
 public:
  // Records that this file serves the old file number `file_number`, so a
  // route from `file_number` to this file may exist. Call it at any time
  // before Finish(). Duplicate numbers are allowed: a repeat of the last one is
  // skipped, and Finish() removes the other duplicates. Returns InvalidArgument
  // for file number 0 or after Finish().
  Status AddServedFile(uint64_t file_number);

  // Adds one record. Calls must be in strictly increasing (origin, original
  // offset) order, and a value must not start before the end (original offset
  // + stored size) of the value before it of the same origin; otherwise
  // InvalidArgument.
  Status Add(uint64_t origin_file_number, uint64_t original_value_offset,
             uint64_t stored_value_size, CompressionType compression);
  // Produces the checked directory and, when `encoded` is non-null, its
  // encoding. InvalidArgument when there is no record or no served file. Can
  // be called once.
  Status Finish(BlobRelocationDirectory* directory,
                std::string* encoded = nullptr);

 private:
  // Appends the origin partition being built to the directory.
  Status FinishCurrentOrigin();

  BlobRelocationDirectory directory_;
  // Origin partition being built; valid when has_current_.
  BlobRelocationOrigin current_;
  // The end (original offset + stored size) of the last value of current_.
  uint64_t current_value_end_ = 0;
  // From AddServedFile; sorted and deduplicated in Finish().
  std::vector<uint64_t> served_file_numbers_;
  bool has_current_ = false;
  bool finished_ = false;
};

// Metadata of a finished relocation file: its encoded directory (the same
// bytes as the table property) and the size of its tail (index, properties,
// metaindex, footer). The caller that installs the file takes the totals and
// the digest from the directory.
struct BlobRelocationFileMetadata {
  std::string encoded_directory;
  uint64_t tail_size = 0;
};

// Encodes the relocation key (see "Key" at the top of this file). Both parts
// are fixed-width and big-endian, so the bytewise comparator orders keys in
// numeric (origin, original offset) order.
inline constexpr size_t kBlobRelocationKeySize = 16;
std::string EncodeBlobRelocationKey(uint64_t origin_file_number,
                                    uint64_t original_value_offset);
// Writes the same 16-byte key into `out` (kBlobRelocationKeySize bytes),
// without a heap allocation.
void EncodeBlobRelocationKeyTo(uint64_t origin_file_number,
                               uint64_t original_value_offset, char* out);
// Decodes a relocation key. Returns Corruption for a key that is not 16 bytes
// or has origin 0.
Status DecodeBlobRelocationKey(const Slice& key, uint64_t* origin_file_number,
                               uint64_t* original_value_offset);

// Returns the table options of a relocation file. It starts from the column
// family's options, so the block cache and the restart interval are kept. It
// removes the features that serve user keys (filter, UDI) and the data block
// hash index. It sets the binary search index, turns off index key shortening
// and sets the 4 KB block size. See "Table options" at the top of this file
// for the reasons. It forces format_version >= 6, so record checksums depend
// on the file offset.
BlockBasedTableOptions MakeBlobRelocationTableOptions(
    const BlockBasedTableOptions& source);

// Most values in one location entry (see "Location entry" at the top of this
// file). 32 is a design choice. Larger groups would save only a little more
// space, because the shared key and header are already small per value. But a
// lookup decodes every value of its entry, so a cached lookup gets slower.
inline constexpr size_t kBlobRelocationGroupSize = 32;

// One value of a location entry.
struct BlobRelocationGroupValue {
  uint64_t original_value_offset = 0;
  uint64_t stored_value_size = 0;
};

// Encodes the value of a location entry (see "Value" at the top of this file)
// into `*output`, replacing its contents. `values` must hold 1..32 values in
// original offset order that do not overlap (each value starts at or after
// the end of the value before it), each with a stored size of
// 1..kMaxSimpleGen2BlobPayloadSize.
void EncodeBlobRelocationGroup(uint64_t first_record_offset,
                               const BlobRelocationGroupValue* values,
                               size_t count, std::string* output);

// A decoded location entry. Readers decode an entry once and look up several
// values in it.
struct BlobRelocationGroup {
  struct Value {
    uint64_t original_value_offset = 0;
    uint64_t record_offset = 0;
    uint64_t stored_value_size = 0;
  };
  size_t count = 0;
  std::array<Value, kBlobRelocationGroupSize> values;

  // The value at `original_value_offset`, or nullptr. Binary search.
  const Value* Find(uint64_t original_value_offset) const;
};

// Decodes and checks the value of a location entry whose key holds
// `last_original_value_offset`. Returns Corruption when the value ends early,
// the count is not 1..32, a stored size is 0 or above
// kMaxSimpleGen2BlobPayloadSize, an offset or a value end overflows, bytes are
// left over, or the last original offset is not `last_original_value_offset`.
// `*group` is changed only on success.
Status DecodeBlobRelocationGroup(const Slice& value,
                                 uint64_t last_original_value_offset,
                                 BlobRelocationGroup* group);

}  // namespace ROCKSDB_NAMESPACE
