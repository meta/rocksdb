// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Exact blob reference metadata
//
// When an SST is built, it saves a sorted list of every blob reference it keeps
// that points to a blob file (inlined and same-file blob indexes are not
// included). This list is the "exact reference metadata". Its users are the
// census (live bytes and garbage per blob file) and indirect blob GC: they
// learn from it which blob values an SST keeps without reading its key-value
// data. See "Readers" below. It is called "exact" because it lists every such
// reference, not an estimate, so a reader can tell exactly which values the
// SST keeps.
//
// The caller of the table builder decides which SSTs get it
// (TableBuilderOptions::collect_blob_reference_metadata). When
// enable_blob_indirection is on, or the DB has used it before:
//   - a compaction output at L1 or deeper gets exact metadata;
//   - a compaction output at L0 gets it only when an input has it, or when
//     the compaction is bottommost (see
//     Compaction::CollectExactBlobReferences());
//   - flush output gets only a coarse classification (see "Coarse SST" and
//     "Coarse classification property" below).
// SstFileWriter output gets neither. It can hold only same-file (embedded)
// blob references (SstFileWriter::OpenWithEmbeddedBlobs), never a reference to
// a blob file, so it has nothing to list. It stays in the legacy form.
//
// "Legacy" means "written the way base RocksDB writes it, without blob
// indirection". For example, a legacy reference is a kBlob or kBlobTTL index,
// and a legacy SST has no exact reference metadata.
//
// Reference cap and epoch runs: the collector keeps all references of an SST
// in memory until the SST is finished. So compaction starts a new output once
// an exact output holds blob_indirection_max_references_per_sst references
// (the reference cap; the code calls it the blob reference limit). The option
// comment in include/rocksdb/advanced_options.h gives the memory numbers.
// A compaction to L0 is a special case. It happens only in a few cases:
// universal compaction with num_levels = 1 (every compaction writes to L0);
// universal compaction when the sorted run right after the picked runs is an
// L0 file; leveled intra-L0 compaction; and CompactFiles() with output level
// 0. In base RocksDB such an output is always one L0 file. With blob
// indirection, the cap can cut it into several exact L0 files that share one
// epoch number (an epoch run). Every compaction picker takes all files of an
// epoch run or none of them (see VersionStorageInfo::ExpandExactL0Inputs() for
// why).
//
// Terms (generation number, origin, original offset, relocation file, old file
// and route are defined in blob_index.h):
//   - encoding class (class): the kind of reference, stored as one byte.
//       kDirect (0): a legacy kBlob or kBlobTTL index (file number and offset
//         only).
//       kIndirectUnflattened (1, "unflattened"): a kIndirectBlob index with
//         generation == origin.
//       kIndirectFlattened (2, "flattened"): a kIndirectFlattenedBlob index,
//         whose generation is a relocation file.
//   - stored size: the size of the value's stored (maybe compressed) bytes.
//   - record bytes: the bytes the value's record takes in the file named by
//     its generation:
//       unflattened, kDirect: stored size + BlobLogRecord::kHeaderSize
//         (32 bytes) + user key size, as in a normal blob file.
//       flattened: stored size + kSimpleGen2BlobTrailerSize (5 bytes). This
//         sum is the size of one record in a relocation file.
//     The census and GC need record bytes, not just stored sizes, because a
//     file's size includes every record's header and key (or trailer).
//   - reference group (group): all references of one SST with the same
//     (encoding class, generation number, origin).
//   - run: the part of one group inside one data block. A group that fits in
//     one block is one run; a larger group continues at the start of the
//     next block with a new run. A run never crosses a block.
//   - reference index block (index block): the block that lists the data
//     blocks and the groups. It is not the SST's own index block.
//   - relocation-state fingerprint: a 128-bit hash of the routes and
//     relocation files of the Version that the SST was built from (see
//     BlobReferenceMetadataDescriptor).
//
// Sort order: (encoding class, generation, origin, original offset). So each
// group is contiguous, and inside a group the references are in offset order.
//
// Layout. All blocks are written back to back through the table's meta-block
// path, each with the normal block trailer (compression type and checksum).
// They are written before the SST tail (tail_start_offset), so a normal
// table open does not prefetch them.
//
//   +---------------------+
//   | data block 0        |  one or more runs per block
//   | ...                 |
//   | data block N-1      |
//   +---------------------+
//   | index block         |  block table, then one entry per group
//   +---------------------+
//   | descriptor property |  magic, format version, index handle, totals,
//   +---------------------+  CF ID, relocation-state fingerprint, SST
//                            file number, CRC32C (72 bytes)
//
//   An SST with no references still has an index block (block count 0,
//   group count 0) and the descriptor; DecodeFrom() requires a non-null
//   index handle.
//
//   The next diagram shows an example SST with four groups, A, B, C and D, in
//   three data blocks. The group names and the offsets x, y and z are example
//   values.
//
//   index block                         data blocks
//   +-------------------------+
//   | block table             |         block 0
//   |   first offset, sizes --+-------->+-------------------------------+
//   +-------------------------+         | run A | run B | run C1 ...    |
//   | group table             |         +-------------------------------+
//   |   A: block 0, offset 0  |         block 1
//   |   B: block 0, offset x  |         +-------------------------------+
//   |   C: block 0, offset y -+-------->| run C2 (all of block 1)       |
//   |   D: block 2, offset z  |         +-------------------------------+
//   +-------------------------+         block 2
//                                       +-------------------------------+
//                                       | run C3 | run D                |
//                                       +-------------------------------+
//
//   Group C is large. Its first run, C1, starts at offset y in block 0. C2
//   fills block 1, and C3 starts at offset 0 of block 2. Group D starts in
//   block 2 right after C3.
//
// Data block contents (after decompression; all varints are canonical).
// A block holds one or more runs, back to back. A run is a run header
// followed by `count` entries, one entry per reference:
//
//   +------------+---------+---------+-----+---------+------------+-----
//   | run header | entry 1 | entry 2 | ... | entry n | run header | ...
//   +------------+---------+---------+-----+---------+------------+-----
//   |<------------------- one run ------------------>|<-- next run
//
//   Run header:
//     count                varint; number of entries in this run
//     first offset         varint; original offset of entry 1
//     default compression  1 byte; the compression of the first
//                          reference that the collector received for
//                          this group (not always entry 1 of the run)
//
//   Entry:
//     gap                  varint; entries 2..n only
//     size and flag        varint; (stored size << 1) | differs (stored
//                          size < 2^63)
//     compression          1 byte; only when differs = 1
//     user key size        varint; unflattened and kDirect only (records in
//                          a normal blob file)
//     logical CRC          fixed 4 bytes; unflattened and flattened only
//     stored CRC           fixed 4 bytes; unflattened and flattened only,
//                          and only when compression != kNoCompression
//
//   A run header does not repeat the group key. A reader always reaches a
//   run through its index entry, which gives the class, generation and
//   origin.
//
//   What the fields mean:
//   - compression: the CompressionType the blob value itself was written
//     with in its blob file (the same field as in the BlobIndex). It is not
//     the compression of this metadata block, which is in the block trailer.
//   - differs: a 1-bit flag. 1 means this entry's compression is not the
//     run's default compression, and a compression byte follows. The flag is
//     kept in the lowest bit of the stored size varint, so the many entries
//     that use the default pay no extra byte for it.
//   - user key size: the size of the user key in the value's record in its
//     normal blob file. With BlobLogRecord::kHeaderSize it gives the record
//     bytes and the gap.
//   - gap: stores an offset as a small number. Offsets are sorted inside a
//     run, so each entry stores only how far it is from where it would be if
//     the records were back to back:
//
//       blob file:  [hdr][key 1][value 1][hdr][key 2][value 2]
//                               ^                    ^
//                               offset 1             offset 2
//                               |<size 1>|<--------->|
//                                         kHeaderSize + key size 2
//
//       unflattened, kDirect:
//         offset 2 = offset 1 + stored size 1 + BlobLogRecord::kHeaderSize
//                    + user key size 2 + gap
//       flattened (the user key size in the origin file is not known):
//         offset 2 = offset 1 + stored size 1 + gap
//
//     So the gap is 0 for back-to-back records. When it is not 0, it is the
//     bytes of the records in between that this SST does not reference.
//   - logical CRC, stored CRC: copies of the two checksums in the SST's
//     BlobIndex (see blob_index.h). GC reads the live set from this
//     metadata, not from the SST's entries, so they must be here.
//       stored CRC: GC checks every payload it copies against it, without
//         decompressing.
//       logical CRC: GC does not check it (it never decompresses), but
//         needs it to rebuild the stored CRC for kNoCompression (which is
//         not written) and to build a full BlobIndex for the reader APIs.
//
// Index block contents (never compressed; all varints are canonical):
//
//   Block table:
//     block count          varint
//     first block offset   varint; file offset of data block 0 (only when
//                          block count > 0)
//     size                 varint, per data block in file order; the block's
//                          size on disk without its trailer (compressed size
//                          if compressed), as in a BlockHandle
//
//   Group table:
//     group count          varint
//     then, per group in sort order:
//       class              1 byte
//       generation         varint; a delta from the previous entry's
//                          generation when the class is the same, else the
//                          full value
//       origin             varint; flattened only. A delta from the previous
//                          entry's origin when the class and generation are
//                          the same, else the full value
//       start block delta  varint; the number of the block holding the
//                          group's first run, minus the previous group's.
//                          0 means the same block as the previous group.
//       run offset         varint; where the group's first run starts inside
//                          that block (after decompression)
//       count              varint; references in the group
//       stored bytes       varint; sum of the stored sizes
//       user key bytes     varint; sum of the user key sizes; unflattened and
//                          kDirect only
//
//   What the fields are for:
//   - Block table: data blocks are back to back, so the reader rebuilds every
//     offset in memory: offset[0] = first block offset, offset[i+1] =
//     offset[i] + size[i] + block trailer size. The end of the last data
//     block must equal the index block's offset. Knowing every block's exact
//     size before reading any data lets a reader plan, merge and batch its
//     block reads.
//   - Deltas (generation, origin, start block): the sorted order makes
//     neighbors close, so most deltas fit in one byte.
//   - start block delta + run offset: let GC jump straight to a group's
//     first run. Groups that start in the same block have increasing run
//     offsets. The group's later runs, if any, start at offset 0 of the
//     following blocks.
//   - count, stored bytes, user key bytes: the group totals. The group's
//     record bytes are stored bytes + count * BlobLogRecord::kHeaderSize +
//     user key bytes (unflattened, kDirect), or stored bytes + count *
//     kSimpleGen2BlobTrailerSize (flattened). The census reads only these,
//     so it never reads a data block.
//   - kDirect groups are in the group table too. The census skips them.
//
// Writer:
//   The table builder (BlockBasedTableBuilder) owns one
//   BlobReferenceMetadataCollector per SST, only in the cases listed at the
//   top. It calls Add() for each BlobIndex that the SST keeps, in plain blob
//   values and in wide-column entities. Before it writes the SST tail, it
//   calls Finish(), which writes the data blocks and the index block. The
//   builder then adds the index block to the meta-index
//   (kBlobReferenceIndexBlockName) and stores the descriptor as a table
//   property (kBlobReferenceDescriptorPropertyName). When the table has a
//   block cache and charging for kBlobReferenceMetadataBuildingBuffer is not
//   disabled, the collector's memory is charged to the block cache. Details
//   are in BlobReferenceMetadataCollector.
//
// Readers: the readers below are the intended users of this format. The
// decoders in this file are the only code that reads it. A reader first
// decodes the descriptor (BlobReferenceMetadataDescriptor::DecodeFrom()), then
// the index (DecodeBlobReferenceIndex()), and then the runs of a group
// (BlobReferenceGroupDecoder, or the ForEachBlobReference* helpers that use
// it).
//   - The census reads only the index.
//   - GC sorts the groups it needs from one SST, finds them in the index,
//     reads only the blocks that hold their runs (merged and batched), and
//     decodes only those runs.
//   - Remote compaction verifies an SST's metadata by walking every group in
//     index order, which visits every run and every block.
//   A reader always reads, checks and decompresses a whole data block, then
//   decodes only the runs it needs. Every run is checked against its group:
//   entries must decode with the fields of the group's class, offsets must
//   increase and gaps cannot be negative, a run must end exactly at the next
//   run or the block end, and each group's count, stored bytes and user key
//   bytes must equal its index totals.
//
// Compression and block size:
//   - Data blocks are compressed with the compressor that the SST uses for
//     its own index blocks (the basic compressor of the output level, no
//     dictionary), so a reader needs no dictionary block. A block that does
//     not compress well is stored raw. The trailer records the type.
//   - The index block is never compressed. It is small: it has one entry per
//     reference group, so its size grows with the number of blob files that
//     the SST points to. Compression could save some bytes, but the census
//     reads this block at every open, and a raw block needs no decompressor
//     and no extra buffer.
//   - The data block target is 16 KB of encoded bytes, before compression
//     (kTargetDataBlockSize). The writer never makes a larger block. GC
//     usually needs only one small group from a block but must read, check
//     and decompress the whole block, so smaller blocks waste less I/O and
//     CPU. There is no point lookup and no block cache here, so larger blocks
//     bring no benefit.
//
// Format notes:
//   - The record bytes of unflattened and kDirect references assume the
//     current blob record header size (BlobLogRecord::kHeaderSize). If the
//     blob log record format ever changes it, this metadata format version
//     must change too.
//   - Record bytes (see Terms) are the size of the record in the file named
//     by the generation number. When that file is an old file whose values a
//     route sends to a relocation file, the value of an unflattened reference
//     takes the flattened record bytes (stored size +
//     kSimpleGen2BlobTrailerSize) there. A reader that
//     counts bytes per blob file or relocation file must use that size.
//
// Integrity: every block, including the index, is protected by the table's
// block checksum. The descriptor has its own CRC, and the index totals must
// match it. With BlockBasedTableOptions::checksum = kNoChecksum, corrupt
// metadata is detected only when it breaks the format or order checks.
//
// Coarse SST
//
// Background: exact reference metadata needs collector memory while the SST is
// built (see "Writer" above). To save that memory, a table builder (the
// intended writer) can skip the collector. A "coarse SST" is an SST built
// without exact reference metadata. Instead it records one file number, its
// frontier. An SST that has the full list is an "exact SST".
//
// Frontier: the oldest indirect origin that the coarse SST references
// (FileMetaData::oldest_coarse_indirect_origin_file_number). For example,
// frontier 12 (an example file number) means that the SST may reference blob
// file 12 or any newer indirect origin, but indirect blob GC cannot know which
// ones.
//   - kInvalidBlobFileNumber (0): the SST is not a coarse SST. A coarse SST
//     always has a frontier above 0; VersionEdit decoding rejects a coarse
//     frontier of 0.
//   - Any other value: the SST pins every indirect-origin file number at or
//     above the frontier.
// "Pin" here means that GC must treat the file as used and must not collect
// it. It is not the base RocksDB meaning (a Version, block or iterator that
// stays alive while something references it).
//
// A coarse SST never holds a flattened reference: its writer must not write
// one, and the code in this file does not check it. So each indirect reference
// of a coarse SST has generation number == origin, and one number (the oldest
// origin) is enough.
//
// The Version keeps the lowest frontier of all its SSTs
// (VersionStorageInfo::OldestCoarseIndirectOriginFileNumber()). An
// indirect-origin blob file at or above it is "coarse pinned"
// (VersionStorageInfo::IsCoarseIndirectOriginBlobPinned()), and the census
// keeps it out of the GC candidate heap. IsCoarseIndirectOriginBlobPinned()
// compares only the file number with the frontier, so it also returns true
// for a relocation file at or above the frontier. The census heap applies the
// pin only to indirect-origin blob files.
//
// Coarse classification property
//
// "Coarse SST" above defines a coarse SST and its frontier: the one file number
// it records instead of a reference list, the oldest indirect origin that it
// may reference. This section says where the frontier is stored and which
// builders write coarse SSTs.
//
// Property: a coarse SST stores its frontier in the table property
// kBlobReferenceClassificationPropertyName ("coarse classification", 16 bytes;
// see EncodeBlobReferenceClassification()). Compaction and flush read it back
// into FileMetaData::oldest_coarse_indirect_origin_file_number. The builder
// writes the property for every SST that it builds without exact metadata
// when track_blob_reference_classification is set, also when the SST has no
// indirect reference. So the property alone does not make an SST coarse.
// With frontier 0 (no indirect reference), the SST is stored as a legacy SST
// (see ExtractBlobReferenceClassificationFromTableProperties() in
// db/builder.cc). With any other frontier, it is a coarse SST.
//
// Builders: the table builder writes only this property, and no exact
// metadata, in three cases:
//   - flush output;
//   - WAL recovery flush output;
//   - a compaction output at L0 when no input has exact metadata and the
//     compaction is not bottommost (see
//     Compaction::CollectExactBlobReferences()).

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_constants.h"
#include "db/blob/blob_index.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/format.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// Table property that holds the encoded BlobReferenceMetadataDescriptor.
extern const char kBlobReferenceDescriptorPropertyName[];
// Meta-index name of the index block.
extern const char kBlobReferenceIndexBlockName[];
// Table property that holds the coarse classification.
extern const char kBlobReferenceClassificationPropertyName[];

// Encodes the coarse classification (see "Coarse classification property"
// above) as 16 bytes: format version (Fixed32, value 1), frontier (Fixed64),
// and a CRC32C of the first 12 bytes (Fixed32).
void EncodeBlobReferenceClassification(uint64_t frontier, std::string* output);
// Returns Corruption if the size, version or CRC is wrong, or if the frontier
// is above kFileNumberMask. Returns InvalidArgument for a null `frontier`.
Status DecodeBlobReferenceClassification(const Slice& input,
                                         uint64_t* frontier);

// Valid range of blob_indirection_max_references_per_sst.
//   - Upper bound: the metadata of one SST has at most 2^20 data blocks
//     (kMaxDataBlockCount in the .cc file) of about 16 KB, so it holds about
//     16 GB of entries. On disk, one entry uses about 8 to 12 bytes
//     (estimate), so that is about 1.4G to 2G references. The upper bound 2^28
//     (256M) stays well under that, so an output at the cap plus one key's
//     history still fits the format. In memory, the collector uses about the
//     on-disk entry size plus a small chunk overhead, about 8 to 14 bytes per
//     reference (an estimate; see "Memory usage" below), so 256M references
//     use about 2 to 3.5 GB of collector memory.
//   - Lower bound: 4096, a fixed choice (not measured).
inline constexpr uint64_t kMinBlobIndirectionMaxReferencesPerSst = 4096;
inline constexpr uint64_t kMaxBlobIndirectionMaxReferencesPerSst = 1ULL << 28;

// How a reference is encoded in its BlobIndex. It is stored on disk and is
// the first part of the group key and sort order.
enum class BlobReferenceEncodingClass : uint8_t {
  // Legacy kBlob or kBlobTTL index; generation == origin == its file number.
  kDirect = 0,
  // kIndirectBlob index with generation == origin (unflattened reference).
  kIndirectUnflattened = 1,
  // kIndirectFlattenedBlob index (generation > origin). Only flatten writes it:
  // compaction rewrote the generation to the file that held the value at
  // flatten time (a relocation file).
  kIndirectFlattened = 2,
};

// The key of a group: (encoding class, generation, origin). Groups are ordered
// and looked up by it. kDirect and unflattened groups have origin ==
// generation; flattened groups have generation > origin.
struct BlobReferenceGroupKey {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kDirect;
  uint64_t generation_file_number = 0;
  uint64_t origin_file_number = 0;

  auto Tie() const {
    return std::tie(encoding_class, generation_file_number, origin_file_number);
  }
  bool operator==(const BlobReferenceGroupKey& other) const {
    return Tie() == other.Tie();
  }
  bool operator<(const BlobReferenceGroupKey& other) const {
    return Tie() < other.Tie();
  }

  // True if the class is known, the file numbers are not 0, and the relation
  // between the generation and the origin fits the class (see above).
  bool IsValid() const;

  struct Hash {
    size_t operator()(const BlobReferenceGroupKey& key) const {
      size_t hash = static_cast<size_t>(key.encoding_class);
      hash = hash * 31 + std::hash<uint64_t>()(key.generation_file_number);
      return hash * 31 + std::hash<uint64_t>()(key.origin_file_number);
    }
  };
};

// One blob reference kept by an SST that points to a blob file (inlined and
// same-file indexes are not included). Terms follow blob_index.h.
struct BlobReference {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kDirect;
  // Generation. For kDirect, the blob file number.
  uint64_t generation_file_number = 0;
  // Origin. For kDirect, the same as the generation.
  uint64_t origin_file_number = 0;
  // Original offset in the origin file. For kDirect, the offset.
  uint64_t original_value_offset = 0;
  // Size of the stored (maybe compressed) value bytes.
  uint64_t stored_value_size = 0;
  // Record bytes: the bytes this record takes in the file named by its
  // generation. Stored size + BlobLogRecord::kHeaderSize + user key size for
  // unflattened and kDirect, or stored size + kSimpleGen2BlobTrailerSize for
  // flattened. The census and indirect blob GC need record bytes because a
  // file's size includes each record's header and key (or trailer). For
  // unflattened and kDirect references, FromBlobIndex() does not know the user
  // key size, so it sets this field to the stored size only, and Add() adds the
  // record header and the user key. For flattened references, FromBlobIndex()
  // sets the full value.
  uint64_t record_bytes = 0;
  // Compression of the stored value bytes.
  CompressionType compression = kNoCompression;
  // stable_checksum: the logical CRC (the name follows BlobIndex). 0 for
  // kDirect.
  uint32_t stable_checksum = 0;
  // stored_checksum: the stored CRC (the name follows BlobIndex). For
  // kNoCompression it equals the logical CRC extended with the compression
  // byte. 0 for kDirect.
  uint32_t stored_checksum = 0;

  // Converts `blob_index` into `*reference`. For an index that does not point
  // to a blob file (kInlinedTTL, or a same-file index), returns OK and sets
  // `*included` to false. A kBlob or kBlobTTL index becomes kDirect. Returns
  // Corruption for an invalid index and InvalidArgument for null outputs.
  // Except for kIndirectFlattened, the result's record_bytes is only the
  // stored size; the collector adds the record header and user key.
  static Status FromBlobIndex(const BlobIndex& blob_index,
                              BlobReference* reference, bool* included);

  // True for indirect references (kIndirectBlob or kIndirectFlattenedBlob),
  // which have a stable identity (origin, original offset) and checksums.
  bool HasStableIdentity() const {
    return encoding_class != BlobReferenceEncodingClass::kDirect;
  }

  BlobReferenceGroupKey group_key() const {
    return {encoding_class, generation_file_number, origin_file_number};
  }
};

// Memory limit (128 MB) for one vector that a reader decodes from this
// metadata. The index decoder's count limits keep its vectors under it. The
// data blocks may hold more references than fit, so a caller that decodes
// references into one vector must check this limit and return an error,
// instead of letting bad metadata use unbounded memory.
inline constexpr size_t kMaxDecodedBlobReferenceBytes = 128 * 1024 * 1024;

// Totals of one group: all references with the same (encoding class,
// generation, origin). Summaries keep only indirect (non-kDirect) groups.
// Readers such as the census and indirect blob GC use these totals (mainly
// record_bytes) for live-byte and garbage math without reading each reference.
// For a kDirect group, origin_file_number equals generation_file_number.
struct BlobReferenceGroup {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kDirect;
  uint64_t generation_file_number = 0;
  uint64_t origin_file_number = 0;
  uint64_t reference_count = 0;
  // Sum of stored_value_size: value bytes only.
  uint64_t stored_value_bytes = 0;
  // Sum of record_bytes: the bytes these references keep in use in the
  // generation file. From the index totals it is stored bytes + count *
  // BlobLogRecord::kHeaderSize + user key bytes (unflattened, kDirect), or
  // stored bytes + count * kSimpleGen2BlobTrailerSize (flattened).
  uint64_t record_bytes = 0;

  BlobReferenceGroupKey group_key() const {
    return {encoding_class, generation_file_number, origin_file_number};
  }
};

// One index entry: a group, the data block where it starts, and the offset
// of its first run in that block's uncompressed contents.
struct BlobReferenceIndexEntry {
  BlobReferenceGroup group;
  uint32_t start_block = 0;
  uint32_t run_offset = 0;
};

// The decoded index block.
struct BlobReferenceIndex {
  // Data blocks in file order.
  std::vector<BlockHandle> blocks;
  // Every group, kDirect ones included, in key order.
  std::vector<BlobReferenceIndexEntry> groups;
};

// Header of one run: the references of one group in one data block. Only
// the count, first offset and default compression are stored. The key fields
// (encoding class, generation, origin) are not on disk; the decoder copies
// them from the run's index group.
struct BlobReferenceRunHeader {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kDirect;
  uint64_t generation_file_number = 0;
  // Equals the generation for unflattened and kDirect runs.
  uint64_t origin_file_number = 0;
  uint64_t reference_count = 0;
  uint64_t first_offset = 0;
  CompressionType default_compression = kNoCompression;
};

// Top-level record of the exact reference metadata, stored as a table
// property. It points to the index block and holds the totals, the column
// family ID, the SST file number and the relocation-state fingerprint.
// Encoding (EncodeTo()), 72 bytes:
//   magic (fixed32) | format version (fixed32) | index offset (fixed64) |
//   index size (fixed64) | reference count (fixed64) | stored bytes
//   (fixed64) | CF ID (fixed32) | fingerprint high (fixed64) | fingerprint
//   low (fixed64) | SST file number (fixed64) | CRC32C of the rest (fixed32)
struct BlobReferenceMetadataDescriptor {
  // The only readable format.
  static constexpr uint32_t kFormatVersion = 1;
  // Size of the encoded descriptor, including its trailing CRC32C.
  static constexpr size_t kEncodedSize = 72;

  // Location of the index block.
  BlockHandle index_handle;
  // Number of references in the SST, kDirect ones included.
  uint64_t reference_count = 0;
  // Sum of stored_value_size over all references, kDirect ones included.
  uint64_t stored_value_bytes = 0;
  // Column family of the SST. The default,
  // TablePropertiesCollectorFactory::Context::kUnknownColumnFamily (the value
  // the table builder gets for an unknown column family), means unknown;
  // DecodeFrom() rejects it.
  uint32_t column_family_id =
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  // High and low halves of a 128-bit fingerprint of the relocation state
  // (relocation files and routes) that the SST was built with. A compaction
  // output stores the fingerprint of the compaction's input Version (see
  // VersionStorageInfo::ComputeBlobRelocationStateFingerprint()). 0 means not
  // set. DecodeFrom() does not check it.
  uint64_t route_snapshot_fingerprint_high = 0;
  uint64_t route_snapshot_fingerprint_low = 0;
  // The file number the SST had when it was built. It can differ from the
  // current file name: CompactionService output is renamed when it is
  // installed, but its contents, this number included, do not change.
  uint64_t sst_file_number = 0;

  // True for the metadata of an SST that keeps no blob reference to a blob
  // file.
  bool IsEmpty() const {
    return reference_count == 0 && stored_value_bytes == 0;
  }

  // True if the SST identity fields are set: a known column family and a
  // nonzero SST file number.
  bool HasSstIdentity() const {
    return column_family_id !=
               TablePropertiesCollectorFactory::Context::kUnknownColumnFamily &&
           sst_file_number != 0;
  }

  // Writes exactly kEncodedSize bytes, ending with a CRC32C of the rest.
  void EncodeTo(std::string* output) const;

  // Checks the size, CRC, magic, format version and HasSstIdentity(). Also
  // checks that the index handle is not null, that its size is more than 0
  // and at most 64 MB, that its end does not overflow, and that a descriptor
  // with no references has 0 stored bytes. Returns Corruption on any check
  // failure, and InvalidArgument when `descriptor` is null. On failure,
  // `*descriptor` is unchanged.
  static Status DecodeFrom(const Slice& input,
                           BlobReferenceMetadataDescriptor* descriptor);
};

// The group totals of one finished SST, kept in memory. Finish() returns it
// when the caller asks for it. It holds the indirect groups but not the
// references, which stay on disk. It is immutable and shared by pointer, so
// the cache charge belongs to this one object, not to each owner.
struct BlobReferenceSstSummary {
  // Same as descriptor.sst_file_number: the build file number, which may
  // differ from the current file name.
  uint64_t file_number = 0;
  BlobReferenceMetadataDescriptor descriptor;
  // Cache charge for MemoryUsage(), moved from the collector. Both are null
  // when charging is disabled.
  std::shared_ptr<CacheReservationManager> reservation_manager;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      reservation_handle;
  // Indirect groups in key order. Declared after the reservation members, so
  // this buffer is freed before its charge is released.
  std::vector<BlobReferenceGroup> groups;

  // Bytes charged to the cache for this summary.
  size_t MemoryUsage() const {
    return sizeof(*this) + groups.capacity() * sizeof(BlobReferenceGroup);
  }
};

// The identity of the SST that a BlobReferenceMetadataCollector builds; it is
// written into the descriptor. Callers set the fields by name, so the SST file
// number and the two fingerprint halves (all uint64_t) cannot be swapped by
// mistake. The column family ID and the SST file number are required: their
// defaults are the unknown values that
// BlobReferenceMetadataDescriptor::HasSstIdentity() rejects, and the collector
// fails every Add() and Finish() call when either one is not set.
struct BlobReferenceSstIdentity {
  uint32_t column_family_id =
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  uint64_t sst_file_number = 0;
  // The relocation-state fingerprint (two 64-bit halves); 0 when not set.
  uint64_t route_snapshot_fingerprint_high = 0;
  uint64_t route_snapshot_fingerprint_low = 0;
};

// Turns a failed block cache charge of blob reference buffers into OK. A failed
// charge means that the block cache is full under
// LRUCacheOptions::strict_capacity_limit. An SST build must not fail for this:
// it must still record every blob reference, and a failed flush or compaction
// would stop writes. So, as for readahead buffers, the memory above the cache
// capacity stays uncharged and the work goes on. The cache reservation keeps
// the part that it could charge, and the next resize tries to charge the rest
// again. The extra memory is at most the reference buffers of the SSTs that
// are being built and, when the caller keeps it, the BlobReferenceSstSummary of
// each finished SST. Each SST's buffers and summary grow with its number of
// blob references. This also covers the size overflow check of
// CacheReservationHandle::Resize(), which cannot happen for real buffer
// sizes. Other statuses are returned as they are.
Status AllowUnchargedWhenCacheIsFull(Status status);

// Builds the exact reference metadata of one SST (see the top of this file).
//
// Background: the census and indirect blob GC must know which blob values an
// SST keeps without reading its key-value data. The table builder therefore
// records each blob reference while it writes the SST, and stores the sorted
// list in metadata blocks before the SST tail. The table builder owns one
// collector per SST and calls it during flush and compaction.
//
// Write path:
//   1. Add() receives each BlobIndex that the SST keeps. It puts the
//      reference into its group, (encoding class, generation, origin), and
//      encodes it once, in the on-disk entry encoding.
//   2. Finish() sorts the groups by group key, packs their entries into data
//      blocks of about 16 KB, writes the blocks, and then writes the index
//      block.
//
// Chunks: each group keeps its encoded entries in a chain of 512-byte chunks.
// A chunk holds only whole entries. The group's first chunk starts with an
// entry. Every later chunk starts with the gap that joins its first entry to
// the previous entry of the group (the "join gap"). Chunks come from 64 KB
// slabs (kSlabSize, 128 chunks). 64 KB was chosen by judgment, not by
// measurement. It is large enough that slab allocations are rare (one per 128
// chunks), and small enough that a small SST wastes little memory.
//
// In one group, references normally arrive in rising offset order: one job
// writes each blob file in key order, and the SST is also built in key order.
// The collector then needs no sort. Finish() copies the chunks whole into
// data blocks. Consecutive chunks of one group in one block form one run.
// When a chunk starts a new run, Finish() drops its join gap.
//
// Why 512 bytes: the chunk size balances three costs.
//   - Chunk header: each chunk has a 16-byte header in memory, which is 3%
//     of 512 bytes. Smaller chunks need more headers (6% at 256 bytes).
//   - Partly filled chunks: the last chunk of each group is on average half
//     empty. This waste grows with the chunk size and the group count (256
//     bytes per group at 512, 2 KB per group at 4 KB).
//   - Block fill: a run takes only whole chunks, so a 16 KB data block can
//     end with up to one chunk of unused space. At 512 bytes this is about
//     3% of a block.
//   One entry, with its gap, is at most kMaxEntrySize (39 bytes), so a
//   512-byte chunk holds at least 13 entries, and about 50 typical small
//   entries (estimate).
//   For an SST with few groups, the chunk size changes memory very little. For
//   a small SST with many groups, the partly filled chunks dominate, so larger
//   chunks cost much more memory. Chunks smaller than 512 bytes use a little
//   more memory and make Finish() slower when an SST has many groups.
//
// Unsorted fallback: a group takes the fallback when it receives an offset
// lower than its last one. That reference and all later references of the
// group go into a second chunk chain. Each entry there stores its absolute
// offset instead of a gap. In Finish(), the collector sorts the group with
// one 16-byte key (offset and entry position) per reference, checks it for
// repeated and overlapping references, and encodes it again from the encoded
// entries. It decodes one entry at a time and never builds a vector of
// BlobReference.
//
// Memory usage: the collector buffers use about the size of the encoded
// entries, plus the chunk headers and the unused end of each group's last
// chunk. A compressed value needs more bytes because its entry also stores
// the CRC of the stored bytes. Many groups in a small SST cost more, because
// each group has a partly filled last chunk. A fallback group needs up to 16
// more bytes per reference for its sort keys, plus a second copy of its
// entries while they are encoded again. The collector charges all of this
// memory to the block cache when the cache has room (see "Memory charge"
// below).
//
// Memory charge: when charging is on (see "Writer" at the top of this file),
// the collector charges its buffers to the block cache through its
// reservation handle. The rule is: charge first, then allocate, then set the
// charge to the real usage. A charge that fails because the block cache is
// full does not fail the collector: the memory stays uncharged and the next
// resize tries again (see AllowUnchargedWhenCacheIsFull()).
// uncharged_charge_failures() counts these failures, and the table builder
// logs a WARN line for each SST that had one.
//   - ReserveCapacity() raises the charge to the current usage plus the new
//     buffer, because the old buffer is still alive while reserve() runs.
//     Then it allocates, and UpdateReservation() sets the charge to
//     ReservedMemory().
//   - NewChunk() charges each 64 KB slab before it allocates the slab.
//   - ReservedMemory() is the one list of charged buffers. A new buffer must
//     be added there. The group map is counted as an estimate per node
//     (kGroupMapNodeBytes) plus its bucket array (one pointer per bucket). The
//     temporary buffers of Finish() (the sort keys of a fallback group and the
//     block buffer) are counted through sort_scratch_bytes_ and
//     block_buffer_bytes_ while they exist.
//
// Lifecycle:
//   1. Construct the collector with the SST's column family, file number and
//      relocation-state fingerprint.
//   2. Call Add() for each BlobIndex in each entry that the SST keeps.
//   3. Call Finish() at most once.
//   4. Destroy the collector. The destructor frees every buffer and then
//      releases the cache charge, also when the build is abandoned.
//
// Errors: the first error is sticky, so later Add() and Finish() calls return
// it. After Finish(), later Add() and Finish() calls return InvalidArgument.
class BlobReferenceMetadataCollector {
 public:
  // Writes one metadata block (uncompressed contents) into the SST and
  // returns its handle, which must not be null on success. The writer may
  // compress a data block; it must write the index block (`is_index_block`)
  // raw.
  using BlockWriter = std::function<Status(
      const Slice& contents, bool is_index_block, BlockHandle* block_handle)>;

  BlobReferenceMetadataCollector(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager,
      const BlobReferenceSstIdentity& identity);
  ~BlobReferenceMetadataCollector();
  BlobReferenceMetadataCollector(const BlobReferenceMetadataCollector&) =
      delete;
  BlobReferenceMetadataCollector& operator=(
      const BlobReferenceMetadataCollector&) = delete;
  BlobReferenceMetadataCollector(BlobReferenceMetadataCollector&&) = delete;
  BlobReferenceMetadataCollector& operator=(BlobReferenceMetadataCollector&&) =
      delete;

  // `user_key_size` is the size stored in a normal blob-log record. It is
  // ignored for flattened references. An index that does not point to a blob
  // file is skipped (see FromBlobIndex()).
  Status Add(const BlobIndex& blob_index, uint64_t user_key_size);

  // Writes the data blocks and then the index block through `write_block`,
  // and fills `*descriptor`, index handle included. When `summary` is not
  // null, it also returns the SST summary, which takes over the group buffer
  // and the cache charge; the other buffers are freed first. The summary lives
  // only in memory; it is never written to the SST or the MANIFEST.
  Status Finish(const BlockWriter& write_block,
                BlobReferenceMetadataDescriptor* descriptor,
                std::shared_ptr<const BlobReferenceSstSummary>* summary);

  // A conservative O(1) upper bound on the uncompressed metadata size, used
  // by output-file size planning before Finish(): the encoded chunk bytes
  // (which already include the gaps at chunk joins) plus bounds for the run
  // headers, the index, the block-table entries and the block trailers.
  uint64_t EstimatedEncodedSize() const;

  // References added so far.
  uint64_t reference_count() const { return reference_count_; }

  size_t TEST_ReservedMemory() const { return ReservedMemory(); }

  // The number of block cache charges that failed because the cache was full
  // (see AllowUnchargedWhenCacheIsFull()). That memory is not charged.
  uint64_t uncharged_charge_failures() const {
    return uncharged_charge_failures_;
  }

 private:
  // Grows `container` to at least `target_capacity`. Charges the cache for
  // the new buffer first, then sets the charge to the real usage.
  template <typename Container>
  Status ReserveCapacity(Container* container, size_t target_capacity) {
    if (target_capacity <= container->capacity()) {
      return Status::OK();
    }
    const size_t current_memory = ReservedMemory();
    if (target_capacity >
        (std::numeric_limits<size_t>::max() - current_memory) /
            sizeof(typename Container::value_type)) {
      return Status::MemoryLimit("Blob reference metadata capacity overflow");
    }
    // The old allocation remains alive while reserve() allocates the new one.
    const Status status = ResizeReservation(
        current_memory +
        target_capacity * sizeof(typename Container::value_type));
    if (!status.ok()) {
      return status;
    }
    container->reserve(target_capacity);
    return UpdateReservation();
  }
  // Doubles the capacity of a full `container` (to `initial_capacity` when
  // it has none) through ReserveCapacity().
  template <typename Container>
  Status GrowIfFull(Container* container, size_t initial_capacity) {
    if (container->size() < container->capacity()) {
      return Status::OK();
    }
    return ReserveCapacity(container, container->capacity() == 0
                                          ? initial_capacity
                                          : 2 * container->capacity());
  }

  // Frees the collection buffers. Used when the summary takes the charge.
  void ReleaseBuffers();

  // The work of Finish(), which then makes the collector finished.
  Status FinishOnce(const BlockWriter& write_block,
                    BlobReferenceMetadataDescriptor* descriptor,
                    std::shared_ptr<const BlobReferenceSstSummary>* summary);

  // Sets the cache charge to ReservedMemory().
  Status UpdateReservation();

  // Sets the cache charge to `memory_used`, making the handle on first use.
  // Does nothing without a reservation manager.
  Status ResizeReservation(size_t memory_used);

  // Counts a charge that failed because the cache is full, and returns
  // AllowUnchargedWhenCacheIsFull(status).
  Status CountUnchargedCharge(Status status);

  // Heap memory of all collector buffers.
  size_t ReservedMemory() const;

  // Packs the group chunks into data blocks of about 16 KB and writes them.
  // Fills index_, groups_, and the descriptor totals.
  Status BuildAndWriteDataBlocks(const BlockWriter& write_block);

  // Encodes index_ into index_block_.
  Status EncodeIndex();

  // Bytes of one chunk and of one slab of chunks.
  static constexpr size_t kChunkSize = 512;
  static constexpr size_t kSlabSize = 64 * 1024;
  static constexpr uint32_t kNoChunk = std::numeric_limits<uint32_t>::max();
  static constexpr size_t kChunksPerSlab = kSlabSize / kChunkSize;
  // Header of one chunk, kept in memory only; the chunk bytes are at
  // ChunkData(). A chunk of a group's sorted chain holds whole entries in the
  // on-disk encoding. The group's first chunk starts with an entry (no gap).
  // Every later chunk starts with the gap that joins its first entry to the
  // last entry of the previous chunk. So Finish() copies a chunk as is when it
  // continues a run, and skips the gap when the chunk starts a new run.
  // `first_offset` is the original offset of the chunk's first entry.
  struct Chunk {
    uint64_t first_offset = 0;
    // Next chunk of the same chain, or kNoChunk.
    uint32_t next = kNoChunk;
    uint16_t count = 0;
    uint16_t used = 0;
  };
  static_assert(sizeof(Chunk) == 16, "Chunk headers are 3% of chunk memory");
  static_assert(kChunkSize <= std::numeric_limits<uint16_t>::max(),
                "Chunk counts and sizes fit uint16_t");
  char* ChunkData(uint32_t chunk) const {
    return slabs_[chunk / kChunksPerSlab].get() +
           (chunk % kChunksPerSlab) * kChunkSize;
  }
  // One group: its key and totals, and its chunk chain.
  struct GroupRecord {
    BlobReferenceGroup group;
    // The compression of the first reference that Add() received for this
    // group; the default of its runs.
    CompressionType default_compression = kNoCompression;
    uint64_t last_offset = 0;
    uint64_t last_stored_size = 0;
    uint32_t first_chunk = kNoChunk;
    uint32_t tail_chunk = kNoChunk;
    // Set when an offset arrived lower than the group's last offset. Such
    // references, and all later ones of the group, wait in the unsorted
    // chunk chain until Finish(). Each unsorted entry is its absolute offset
    // and then the entry without its gap.
    bool unsorted = false;
    uint32_t unsorted_first_chunk = kNoChunk;
    uint32_t unsorted_tail_chunk = kNoChunk;
  };
  // One reference of a fallback group while it is sorted: its offset and
  // where its entry (without gap or offset) starts in the chunks.
  struct SortKey {
    uint64_t offset = 0;
    uint32_t chunk = 0;
    uint32_t position = 0;
  };
  static_assert(sizeof(SortKey) == 16, "SortKey is the fallback's memory");
  // Charged heap bytes of one node of group_map_: an estimate of the key, the
  // value and two pointers.
  static constexpr size_t kGroupMapNodeBytes =
      sizeof(BlobReferenceGroupKey) + sizeof(uint32_t) + 2 * sizeof(void*);

  // Returns the record of the reference's group, making it on first use.
  Status FindOrAddGroup(const BlobReference& reference, uint32_t* group);

  // Encodes the entry of `reference` without its gap, in the on-disk
  // encoding, into `body` (kMaxEntrySize bytes). Sets the user key size for
  // classes that store it.
  static Status EncodeEntryBody(const BlobReference& reference,
                                CompressionType default_compression, char* body,
                                size_t* body_size, uint64_t* key_size);
  // Encodes a reference whose offset is above the group's last offset, with
  // its gap, into the group's tail chunk. Starts a new chunk when the gap and
  // the entry do not fit.
  Status AppendSorted(uint32_t group, const BlobReference& reference);

  // Encodes an out-of-order reference into the group's unsorted chain.
  Status AppendUnsorted(uint32_t group, const BlobReference& reference);

  // Gets one empty chunk, allocating (and charging) a slab when needed.
  Status NewChunk(uint32_t* chunk);

  // Sorts all references of a fallback group, sorted and unsorted chains,
  // through one 16-byte key each, checks for repeats and overlaps, and
  // re-encodes the group from its encoded entries.
  Status SortUnsortedGroup(uint32_t group);

  // The body of SortUnsortedGroup(), with the sort keys in `keys`, whose
  // charge the caller holds.
  Status SortAndEncodeGroup(uint32_t group, std::vector<SortKey>* keys);

  // Null when cache charging is disabled.
  std::shared_ptr<CacheReservationManager> cache_reservation_manager_;
  // Charges that failed because the block cache was full (see
  // AllowUnchargedWhenCacheIsFull()).
  uint64_t uncharged_charge_failures_ = 0;
  // Charge for ReservedMemory().
  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      reservation_handle_;
  // Slabs of chunk memory, and the chunk headers.
  std::vector<std::unique_ptr<char[]>> slabs_;
  size_t slab_used_ = kSlabSize;
  std::vector<Chunk> chunks_;
  // Group records in Add() order, and the map from group key to record.
  std::vector<GroupRecord> group_records_;
  UnorderedMapH<BlobReferenceGroupKey, uint32_t, BlobReferenceGroupKey::Hash>
      group_map_;
  // The group of the last Add(), for a fast path.
  uint32_t last_group_ = kNoChunk;
  uint64_t reference_count_ = 0;
  // Bytes of entries in chunks, for EstimatedEncodedSize().
  uint64_t chunk_bytes_ = 0;
  // Bytes of the sort keys while one group takes the fallback.
  size_t sort_scratch_bytes_ = 0;
  // Groups that are not kDirect, for the summary.
  size_t indirect_group_count_ = 0;
  // The index made by Finish().
  BlobReferenceIndex index_;
  // Indirect groups for the summary.
  std::vector<BlobReferenceGroup> groups_;
  // Charge for the block buffers while Finish() runs.
  size_t block_buffer_bytes_ = 0;
  std::string index_block_;
  BlobReferenceMetadataDescriptor descriptor_;
  // The first error; sticky.
  Status status_;
};

// Decodes the index block. Checks canonical varints, that the data blocks fit
// back to back before the index block and end exactly at its offset, that
// groups are valid and strictly increasing, that start blocks and run offsets
// do not go backward, and that the group totals add up to the descriptor
// totals. Returns Corruption on any failure; `*index` changes only on success.
// The caller reads the block with checksum verification.
Status DecodeBlobReferenceIndex(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    BlobReferenceIndex* index);

// Sets `*groups` to the groups of a decoded index that are not kDirect, in key
// order. These are the groups that a BlobReferenceSstSummary holds and the
// census reads. DecodeBlobReferenceIndex() has already validated the index.
// When set, `before_allocation` gets the size in bytes of the group buffer
// before the buffer is allocated. If it returns non-OK, this function returns
// that status and leaves `*groups` unchanged.
Status CopyIndirectBlobReferenceGroups(
    const BlobReferenceIndex& index,
    const std::function<Status(size_t buffer_bytes)>& before_allocation,
    std::vector<BlobReferenceGroup>* groups);

// Decodes the runs of one index group, one data block at a time. This is the
// only decoder of reference runs, so every check of a group walk is here:
// - each run's header and entries are valid for the group's class;
// - offsets strictly increase and records do not overlap, inside a run and
//   from the last reference of one run to the first of the next;
// - a run continued in the next block ends its block;
// - the run counts never pass the group count, and the counts and bytes add
//   up to the group totals;
// - the last run ends where the next group starts (the last group: at the end
//   of the last block).
// The caller reads block next_block() and passes its uncompressed contents.
class BlobReferenceGroupDecoder {
 public:
  // `index` must outlive the decoder. Requires group < index.groups.size().
  BlobReferenceGroupDecoder(const BlobReferenceIndex& index, size_t group);

  // True once the group's last run is decoded.
  bool done() const { return remaining_ == 0; }
  // The data block that holds the next run.
  size_t next_block() const { return block_; }

  // Decodes the next run in `contents`, the contents of block next_block(),
  // and calls `visit` for each reference in offset order. Returns Corruption
  // when `contents` (after decompression) are larger than the 64 KB block
  // limit.
  Status DecodeRun(const Slice& contents,
                   const std::function<Status(const BlobReference&)>& visit);

 private:
  // Decodes the header of the next run in `contents` and returns its reference
  // count, which is at most the references left in the group. `contents` must
  // stay valid until DecodeRunEntries() returns.
  Status DecodeRunHeader(const Slice& contents, uint64_t* reference_count);
  // Decodes the entries of the run whose header was just decoded and calls
  // `visit` for each reference in offset order.
  Status DecodeRunEntries(
      const std::function<Status(const BlobReference&)>& visit);

  const BlobReferenceIndex& index_;
  const size_t group_;
  size_t block_;
  size_t offset_;
  uint64_t remaining_;
  uint64_t stored_value_bytes_ = 0;
  uint64_t record_bytes_ = 0;
  Slice contents_;
  BlobReferenceRunHeader header_;
  bool has_previous_ = false;
  // The last decoded reference, for the order check across runs.
  BlobReference previous_;
};

// Returns the uncompressed contents of data block `block_ordinal`. The
// contents stay valid until the next call.
using BlobReferenceBlockReader =
    std::function<Status(size_t block_ordinal, Slice* contents)>;

// Visits the references of `index.groups[group]` in offset order through
// BlobReferenceGroupDecoder.
Status ForEachBlobReferenceInGroup(
    const BlobReferenceIndex& index, size_t group,
    const BlobReferenceBlockReader& read_block,
    const std::function<Status(const BlobReference&)>& visit);

// Visits every reference in key order by walking the index groups in order
// with BlobReferenceGroupDecoder, which reads every block once.
Status ForEachBlobReference(
    const BlobReferenceIndex& index, const BlobReferenceBlockReader& read_block,
    const std::function<Status(const BlobReference&)>& visit);

}  // namespace ROCKSDB_NAMESPACE
