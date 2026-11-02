//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>

#include "util/cast_util.h"
#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

namespace {

constexpr uint32_t kMagic = 0x54524945;  // "TRIE"
constexpr uint32_t kVersion = 2;
constexpr uint64_t kExplicitSizes = ~uint64_t{0};

// Columns.
constexpr uint32_t kGroupBits = 6;
constexpr uint64_t kGroupSize = uint64_t{1} << kGroupBits;
constexpr size_t kColumnHeaderSize = 8;
constexpr size_t kGroupHeaderSize = 16;
constexpr size_t kColumnPadding = 8;
// A residual is extracted with one unaligned 8-byte load and a shift of up to
// 7 bits.
constexpr uint32_t kMaxColumnWidth = 56;
// A group's layout field holds its residual width in the low kWidthBits bits
// and the position of its residuals, in 8-byte words, above them.
constexpr uint32_t kWidthBits = 6;
constexpr uint32_t kWidthMask = (1u << kWidthBits) - 1;
constexpr uint64_t kMaxResidualWords = uint64_t{1} << (32 - kWidthBits);
// A linear fit keeps base and slope arithmetic within int64_t for values
// below this; larger values are stored with a zero slope.
constexpr uint64_t kMaxLinearValue = uint64_t{1} << 62;

// Side tables.
constexpr size_t kSideTableEntrySize = 16;

// Trie node header.
constexpr uint8_t kKindMask = 3;
constexpr uint8_t kKindLeaf = 0;
constexpr uint8_t kKindSparse = 1;
constexpr uint8_t kKindBitmap = 2;
constexpr uint8_t kTerminalBit = 4;
constexpr int kPointerWidthShift = 3;
constexpr uint8_t kReservedNodeBits = 0xE0;
constexpr uint8_t kLongPathMarker = 255;
constexpr size_t kBitmapBytes = 32;
// Nodes with at least this many children use a bitmap instead of a label
// list.
constexpr size_t kMinBitmapChildren = 32;

// Bucket entry header.
constexpr uint8_t kLongEntryMarker = 0xFF;
constexpr uint32_t kShortEntryLimit = 15;

// An iterator reserves room for keys up to this long when it is created.
// Longer keys grow the buffer when reached, so one huge separator does not
// make every iterator over the index allocate its length.
constexpr size_t kInitialKeyCapacity = 256;

// Header field positions.
constexpr size_t kMagicPos = 0;
constexpr size_t kVersionPos = 4;
constexpr size_t kFlagsPos = 8;
constexpr size_t kBucketSizePos = 12;
constexpr size_t kNumBlocksPos = 16;
constexpr size_t kNumKeysPos = 24;
constexpr size_t kPrefixLenPos = 32;
constexpr size_t kMaxKeyLenPos = 36;
constexpr size_t kIdWidthPos = 40;
constexpr size_t kTrieRootPos = 44;
constexpr size_t kSizeGapPos = 48;
constexpr size_t kSectionsPos = 56;
// Indexes of the section offsets in the header.
constexpr size_t kTrieSection = 0;
constexpr size_t kBucketTableSection = 1;
constexpr size_t kBucketDataSection = 2;
constexpr size_t kOffsetTableSection = 3;
constexpr size_t kSizeTableSection = 4;
constexpr size_t kRunTableSection = 5;
constexpr size_t kTagTableSection = 6;
constexpr size_t kEnd = 7;
constexpr size_t kNumSectionOffsets = 8;
static_assert(kSectionsPos + kNumSectionOffsets * 8 == kTrieHeaderSize);

Status Corrupt(const char* what) {
  return Status::Corruption("Trie index", what);
}

// Number of bytes, 1 to 4, needed to store v, which must fit 32 bits.
uint32_t BytesFor(uint64_t v) {
  if (v <= 0xFF) {
    return 1;
  } else if (v <= 0xFFFF) {
    return 2;
  } else if (v <= 0xFFFFFF) {
    return 3;
  }
  return 4;
}

uint32_t BitsFor(uint64_t v) {
  return v == 0 ? 0 : lossless_cast<uint32_t>(FloorLog2(v) + 1);
}

uint8_t Byte(const char* p, size_t i) { return lossless_cast<uint8_t>(p[i]); }

// Label of bit `bit` of word `word` of a bitmap node.
uint8_t BitmapLabel(int word, int bit) {
  assert(word >= 0 && word < 4 && bit >= 0 && bit < 64);
  return static_cast<uint8_t>(64 * word + bit);
}

uint32_t LoadLE(const char* p, uint32_t width) {
  switch (width) {
    case 1:
      return Byte(p, 0);
    case 2:
      return DecodeFixed16(p);
    case 3:
      return DecodeFixed16(p) | (uint32_t{Byte(p, 2)} << 16);
    default:
      return DecodeFixed32(p);
  }
}

void PutLE(std::string* dst, uint64_t v, uint32_t width) {
  for (uint32_t i = 0; i < width; i++) {
    dst->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
}

// Length of the common prefix of a and b, looking at most n bytes.
size_t CommonPrefix(const char* a, const char* b, size_t n) {
  size_t i = 0;
  while (i + 8 <= n) {
    uint64_t diff = DecodeFixed64(a + i) ^ DecodeFixed64(b + i);
    if (diff != 0) {
      return i + lossless_cast<size_t>(CountTrailingZeroBits(diff)) / 8;
    }
    i += 8;
  }
  while (i < n && a[i] == b[i]) {
    i++;
  }
  return i;
}

// Appends values as a column.
Status AppendColumn(const std::vector<uint64_t>& values, std::string* out) {
  const uint64_t count = values.size();
  const uint64_t num_groups = (count + kGroupSize - 1) >> kGroupBits;
  PutFixed64(out, count);
  size_t group_pos = out->size();
  const size_t residuals_pos = group_pos + num_groups * kGroupHeaderSize;
  out->resize(residuals_pos + kColumnPadding);
  uint64_t words = 0;
  for (uint64_t g = 0; g < num_groups; g++) {
    const uint64_t lo = g << kGroupBits;
    const uint64_t n = std::min(count - lo, kGroupSize);
    const uint64_t* v = values.data() + lo;
    bool linear = n > 1 && v[n - 1] < kMaxLinearValue;
    for (uint64_t j = 1; linear && j < n; j++) {
      linear = v[j] >= v[j - 1];
    }
    uint64_t slope = linear ? (v[n - 1] - v[0]) / (n - 1) : 0;
    uint64_t base = v[0];
    if (linear && slope <= std::numeric_limits<uint32_t>::max()) {
      // v[j] - slope * j can be negative, but stays within int64_t.
      int64_t min_offset = std::numeric_limits<int64_t>::max();
      for (uint64_t j = 0; j < n; j++) {
        min_offset =
            std::min(min_offset, lossless_cast<int64_t>(v[j]) -
                                     lossless_cast<int64_t>(slope * j));
      }
      base = lossless_cast<uint64_t>(min_offset);
    } else {
      slope = 0;
      for (uint64_t j = 0; j < n; j++) {
        base = std::min(base, v[j]);
      }
    }
    // Unsigned wraparound gives the exact non-negative residuals.
    uint64_t max_residual = 0;
    for (uint64_t j = 0; j < n; j++) {
      max_residual = std::max(max_residual, v[j] - slope * j - base);
    }
    const uint32_t width = BitsFor(max_residual);
    if (width > kMaxColumnWidth) {
      return Status::NotSupported("Trie index: column value range too large");
    }
    if (words >= kMaxResidualWords) {
      return Status::NotSupported("Trie index: column too large");
    }
    char* group = &(*out)[group_pos];
    EncodeFixed64(group, base);
    // Both fit 32 bits, as checked above.
    EncodeFixed32(group + 8, static_cast<uint32_t>(slope));
    EncodeFixed32(group + 12,
                  static_cast<uint32_t>(words << kWidthBits | width));
    group_pos += kGroupHeaderSize;
    // The zero padding after the last group keeps the 8-byte stores in
    // bounds.
    const size_t start = residuals_pos + words * 8;
    words += (n * width + 63) / 64;
    out->resize(residuals_pos + words * 8 + kColumnPadding);
    for (uint64_t j = 0; j < n && width > 0; j++) {
      const uint64_t bit = j * width;
      char* p = &(*out)[start + bit / 8];
      EncodeFixed64(
          p, DecodeFixed64(p) | ((v[j] - slope * j - base) << (bit % 8)));
    }
  }
  return Status::OK();
}

// Writes a path-compressed trie over sorted, distinct heads, children before
// parents, without recursion so that deep tries cannot exhaust the stack.
class TrieWriter {
 public:
  TrieWriter(const std::vector<Slice>& heads, uint32_t id_width)
      : heads_(heads), id_width_(id_width) {}

  Status Write(std::string* out, uint32_t* root) {
    std::vector<Frame> stack;
    stack.push_back(MakeFrame(0, heads_.size(), 0));
    uint64_t last_pos = 0;
    while (!stack.empty()) {
      Frame& top = stack.back();
      if (top.next < top.end) {
        const size_t d = top.path_end;
        const uint8_t label = Byte(heads_[top.next].data(), d);
        uint64_t j = top.next + 1;
        while (j < top.end && Byte(heads_[j].data(), d) == label) {
          j++;
        }
        top.labels.push_back(label);
        const uint64_t child_begin = top.next;
        top.next = j;
        // Invalidates top.
        stack.push_back(MakeFrame(child_begin, j, d + 1));
        continue;
      }
      Status s = Emit(top, out, &last_pos);
      if (!s.ok()) {
        return s;
      }
      stack.pop_back();
      if (!stack.empty()) {
        stack.back().children.push_back(last_pos);
      }
    }
    if (out->size() > std::numeric_limits<uint32_t>::max()) {
      return Status::NotSupported("Trie index: trie too large");
    }
    *root = static_cast<uint32_t>(last_pos);
    return Status::OK();
  }

 private:
  // Heads [begin, end), which share their first `depth` bytes.
  struct Frame {
    uint64_t begin = 0;
    uint64_t end = 0;
    size_t depth = 0;
    size_t path_end = 0;
    bool terminal = false;
    uint64_t next = 0;
    std::vector<uint8_t> labels;
    std::vector<uint64_t> children;
  };

  Frame MakeFrame(uint64_t begin, uint64_t end, size_t depth) const {
    const Slice& first = heads_[begin];
    const Slice& last = heads_[end - 1];
    Frame f;
    f.begin = begin;
    f.end = end;
    f.depth = depth;
    f.path_end =
        depth + CommonPrefix(first.data() + depth, last.data() + depth,
                             std::min(first.size(), last.size()) - depth);
    // Only the first head of a sorted range can end at the shared path.
    f.terminal = first.size() == f.path_end;
    f.next = f.terminal ? begin + 1 : begin;
    return f;
  }

  Status Emit(const Frame& f, std::string* out, uint64_t* pos) const {
    *pos = out->size();
    uint64_t max_distance = 0;
    for (uint64_t child : f.children) {
      max_distance = std::max(max_distance, *pos - child);
    }
    if (max_distance > std::numeric_limits<uint32_t>::max()) {
      return Status::NotSupported("Trie index: trie too large");
    }
    const uint32_t pointer_width = BytesFor(max_distance);
    uint8_t kind = kKindLeaf;
    if (f.labels.size() >= kMinBitmapChildren) {
      kind = kKindBitmap;
    } else if (!f.labels.empty()) {
      kind = kKindSparse;
    }
    out->push_back(
        static_cast<char>(kind | (f.terminal ? kTerminalBit : 0) |
                          ((pointer_width - 1) << kPointerWidthShift)));
    const size_t path_len = f.path_end - f.depth;
    if (path_len < kLongPathMarker) {
      out->push_back(static_cast<char>(path_len));
    } else {
      out->push_back(lossless_cast<char>(kLongPathMarker));
      PutVarint32(out, static_cast<uint32_t>(path_len));
    }
    out->append(heads_[f.begin].data() + f.depth, path_len);
    if (f.terminal) {
      PutLE(out, f.begin, id_width_);
    }
    if (kind == kKindSparse) {
      out->push_back(static_cast<char>(f.labels.size() - 1));
      for (uint8_t label : f.labels) {
        out->push_back(lossless_cast<char>(label));
      }
    } else if (kind == kKindBitmap) {
      uint64_t words[4] = {0, 0, 0, 0};
      for (uint8_t label : f.labels) {
        words[label / 64] |= uint64_t{1} << (label % 64);
      }
      for (uint64_t w : words) {
        PutFixed64(out, w);
      }
    }
    for (uint64_t child : f.children) {
      PutLE(out, *pos - child, pointer_width);
    }
    return Status::OK();
  }

  const std::vector<Slice>& heads_;
  const uint32_t id_width_;
};

}  // namespace

// ============================================================================
// TrieBuilder
// ============================================================================

TrieBuilder::TrieBuilder(uint32_t bucket_size) : bucket_size_(bucket_size) {}

Slice TrieBuilder::LastSeparator() const {
  const size_t num_keys = key_ends_.size();
  if (num_keys == 0) {
    return Slice();
  }
  const size_t begin = num_keys > 1 ? key_ends_[num_keys - 2] : 0;
  return Slice(key_bytes_.data() + begin, key_ends_.back() - begin);
}

void TrieBuilder::AddBlock(const Slice& separator,
                           const TrieBlockHandle& handle, uint64_t tag) {
  const uint64_t num_keys = key_ends_.size();
  const int cmp = num_keys > 0 ? separator.compare(LastSeparator()) : 1;
  if (cmp > 0) {
    key_bytes_.append(separator.data(), separator.size());
    key_ends_.push_back(key_bytes_.size());
  } else {
    out_of_order_ |= cmp < 0;
    if (!runs_.empty() && runs_.back().key_index == num_keys - 1) {
      runs_.back().extra_blocks++;
    } else {
      runs_.push_back({num_keys - 1, 1});
    }
  }
  if (tag != 0) {
    tags_.push_back({handles_.size(), tag});
  }
  handles_.push_back(handle);
}

Status TrieBuilder::Finish(std::string* output) {
  if (out_of_order_) {
    return Status::InvalidArgument("Trie index: separators out of order");
  }
  if (bucket_size_ == 0) {
    return Status::InvalidArgument("Trie index: bucket size must be > 0");
  }
  const uint64_t num_blocks = handles_.size();
  const uint64_t num_keys = key_ends_.size();
  const uint64_t num_buckets = (num_keys + bucket_size_ - 1) / bucket_size_;
  if (num_buckets > uint64_t{std::numeric_limits<uint32_t>::max()} + 1) {
    return Status::NotSupported("Trie index: too many buckets");
  }
  auto key = [&](uint64_t i) {
    const size_t begin = i > 0 ? key_ends_[i - 1] : 0;
    return Slice(key_bytes_.data() + begin, key_ends_[i] - begin);
  };

  size_t prefix_len = 0;
  size_t max_key_len = 0;
  if (num_keys > 0) {
    const Slice first = key(0);
    const Slice last = key(num_keys - 1);
    prefix_len = CommonPrefix(first.data(), last.data(),
                              std::min(first.size(), last.size()));
    for (uint64_t i = 0; i < num_keys; i++) {
      max_key_len = std::max(max_key_len, key(i).size());
    }
  }
  if (max_key_len > std::numeric_limits<uint32_t>::max()) {
    return Status::NotSupported("Trie index: separator too long");
  }

  // Trie over the bucket heads, without the shared prefix.
  const uint32_t id_width = BytesFor(num_buckets > 0 ? num_buckets - 1 : 0);
  std::string trie;
  uint32_t trie_root = 0;
  if (num_keys > 0) {
    std::vector<Slice> heads;
    heads.reserve(num_buckets);
    for (uint64_t b = 0; b < num_buckets; b++) {
      Slice head = key(b * bucket_size_);
      head.remove_prefix(prefix_len);
      heads.push_back(head);
    }
    Status s = TrieWriter(heads, id_width).Write(&trie, &trie_root);
    if (!s.ok()) {
      return s;
    }
  }

  // Front-coded buckets.
  std::string bucket_data;
  std::vector<uint64_t> bucket_positions;
  bucket_positions.reserve(num_buckets + 1);
  for (uint64_t b = 0; b < num_buckets; b++) {
    bucket_positions.push_back(bucket_data.size());
    const uint64_t begin = b * bucket_size_;
    const uint64_t end = std::min(num_keys, begin + bucket_size_);
    for (uint64_t i = begin + 1; i < end; i++) {
      const Slice prev = key(i - 1);
      const Slice cur = key(i);
      const size_t shared = CommonPrefix(prev.data(), cur.data(),
                                         std::min(prev.size(), cur.size()));
      const size_t drop = prev.size() - shared;
      const size_t add = cur.size() - shared;
      if (drop < kShortEntryLimit && add < kShortEntryLimit) {
        bucket_data.push_back(static_cast<char>((drop << 4) | add));
      } else {
        bucket_data.push_back(lossless_cast<char>(kLongEntryMarker));
        PutVarint32(&bucket_data, static_cast<uint32_t>(drop));
        PutVarint32(&bucket_data, static_cast<uint32_t>(add));
      }
      bucket_data.append(cur.data() + shared, add);
    }
  }
  bucket_positions.push_back(bucket_data.size());

  // Block offsets, with sizes derived when every gap between blocks is the
  // same.
  std::vector<uint64_t> offsets(num_blocks + 1, 0);
  bool derived = true;
  uint64_t size_gap = 0;
  for (uint64_t i = 0; i < num_blocks; i++) {
    const TrieBlockHandle& h = handles_[i];
    if (i > 0 && h.offset < offsets[i - 1]) {
      return Status::InvalidArgument("Trie index: block offsets decrease");
    }
    offsets[i] = h.offset;
    if (h.size > std::numeric_limits<uint64_t>::max() - h.offset) {
      derived = false;
      continue;
    }
    const uint64_t block_end = h.offset + h.size;
    if (i + 1 < num_blocks) {
      const uint64_t next = handles_[i + 1].offset;
      if (next < block_end) {
        derived = false;
      } else if (i == 0) {
        size_gap = next - block_end;
      } else if (next - block_end != size_gap) {
        derived = false;
      }
    } else if (derived &&
               size_gap <= std::numeric_limits<uint64_t>::max() - block_end) {
      offsets[num_blocks] = block_end + size_gap;
    } else {
      derived = false;
    }
  }
  if (!derived) {
    // Sizes are stored, so the last offset only has to keep the column
    // non-decreasing.
    offsets[num_blocks] = offsets[num_blocks - 1];
  }

  std::string out;
  out.resize(kTrieHeaderSize);
  uint64_t sections[kNumSectionOffsets] = {};
  if (num_keys > 0) {
    out.append(key(0).data(), prefix_len);
  }
  sections[kTrieSection] = out.size();
  out.append(trie);
  sections[kBucketTableSection] = out.size();
  Status s = AppendColumn(bucket_positions, &out);
  if (!s.ok()) {
    return s;
  }
  sections[kBucketDataSection] = out.size();
  out.append(bucket_data);
  sections[kOffsetTableSection] = out.size();
  s = AppendColumn(offsets, &out);
  if (!s.ok()) {
    return s;
  }
  if (!derived) {
    sections[kSizeTableSection] = out.size();
    std::vector<uint64_t> sizes(num_blocks);
    for (uint64_t i = 0; i < num_blocks; i++) {
      sizes[i] = handles_[i].size;
    }
    s = AppendColumn(sizes, &out);
    if (!s.ok()) {
      return s;
    }
  }
  sections[kRunTableSection] = out.size();
  PutFixed64(&out, runs_.size());
  uint64_t extra_blocks = 0;
  for (const Run& run : runs_) {
    extra_blocks += run.extra_blocks;
    PutFixed64(&out, run.key_index);
    PutFixed64(&out, extra_blocks);
  }
  sections[kTagTableSection] = out.size();
  PutFixed64(&out, tags_.size());
  for (const Tag& tag : tags_) {
    PutFixed64(&out, tag.block_index);
    PutFixed64(&out, tag.tag);
  }
  sections[kEnd] = out.size();

  char* h = out.data();
  EncodeFixed32(h + kMagicPos, kMagic);
  EncodeFixed32(h + kVersionPos, kVersion);
  EncodeFixed32(h + kFlagsPos, 0);
  EncodeFixed32(h + kBucketSizePos, bucket_size_);
  EncodeFixed64(h + kNumBlocksPos, num_blocks);
  EncodeFixed64(h + kNumKeysPos, num_keys);
  EncodeFixed32(h + kPrefixLenPos, static_cast<uint32_t>(prefix_len));
  EncodeFixed32(h + kMaxKeyLenPos, static_cast<uint32_t>(max_key_len));
  EncodeFixed32(h + kIdWidthPos, id_width);
  EncodeFixed32(h + kTrieRootPos, trie_root);
  EncodeFixed64(h + kSizeGapPos, derived ? size_gap : kExplicitSizes);
  for (size_t i = 0; i < kNumSectionOffsets; i++) {
    EncodeFixed64(h + kSectionsPos + 8 * i, sections[i]);
  }
  *output = std::move(out);
  return Status::OK();
}

uint64_t TrieBuilder::EntrySize(uint64_t drop, uint64_t add) {
  return add + (drop < kShortEntryLimit && add < kShortEntryLimit
                    ? 1
                    : 1 + VarintLength(drop) + VarintLength(add));
}

uint64_t TrieBuilder::SizeBound(const SizeStats& stats, uint32_t bucket_size) {
  assert(bucket_size > 0);
  // A group's residuals never exceed the range of its values.
  auto column = [](uint64_t count, uint64_t max_range) {
    const uint64_t num_groups = (count + kGroupSize - 1) >> kGroupBits;
    return kColumnHeaderSize + num_groups * kGroupHeaderSize +
           num_groups * 8 * BitsFor(max_range) + kColumnPadding;
  };
  const uint64_t n = stats.num_blocks;
  const uint64_t num_buckets = (n + bucket_size - 1) / bucket_size;
  // Path and label bytes never exceed the suffix bytes. A trie with b
  // terminal nodes has fewer than 2b nodes, each costing at most 17 bytes
  // besides its path: header, path length, bucket id, child count, the
  // pointer to it, and its share of a bitmap.
  const uint64_t trie = stats.suffix_bytes + 17 * 2 * num_buckets;
  // Offsets come in block order, so a group of 64 offsets spans at most 64
  // steps (the last one to the sentinel), and a size is at most the largest.
  // Finish() fails for residuals wider than kMaxColumnWidth bits, so clamping
  // each term there keeps the product from wrapping and still bounds them.
  constexpr uint64_t kMaxRange = uint64_t{1} << kMaxColumnWidth;
  const uint64_t max_step = std::min(stats.max_block_step, kMaxRange) +
                            std::min(stats.max_block_size, kMaxRange);
  uint64_t size = kTrieHeaderSize + stats.max_key_len + trie +
                  column(num_buckets + 1, stats.entry_bytes) +
                  stats.entry_bytes + column(n + 1, kGroupSize * max_step) +
                  2 * 8 +
                  kSideTableEntrySize * (stats.duplicates + stats.tagged);
  if (stats.irregular_gaps) {
    size += column(n, stats.max_block_size);
  }
  return size;
}

// ============================================================================
// Trie
// ============================================================================

Status Trie::Column::Init(const char* begin, const char* end,
                          uint64_t expected_count) {
  const uint64_t avail = lossless_cast<uint64_t>(end - begin);
  if (avail < kColumnHeaderSize + kColumnPadding) {
    return Corrupt("truncated column");
  }
  count = DecodeFixed64(begin);
  if (count != expected_count) {
    return Corrupt("bad column header");
  }
  const uint64_t num_groups = (count + kGroupSize - 1) >> kGroupBits;
  if (num_groups >
      (avail - kColumnHeaderSize - kColumnPadding) / kGroupHeaderSize) {
    return Corrupt("bad column size");
  }
  groups = begin + kColumnHeaderSize;
  residuals = groups + num_groups * kGroupHeaderSize;
  // The groups' residuals must follow each other with no gaps.
  uint64_t words = 0;
  for (uint64_t g = 0; g < num_groups; g++) {
    const uint32_t layout = DecodeFixed32(groups + g * kGroupHeaderSize + 12);
    const uint32_t width = layout & kWidthMask;
    if (width > kMaxColumnWidth || layout >> kWidthBits != words) {
      return Corrupt("bad column group");
    }
    words +=
        (std::min(count - (g << kGroupBits), kGroupSize) * width + 63) / 64;
  }
  if (lossless_cast<uint64_t>(end - residuals) != words * 8 + kColumnPadding) {
    return Corrupt("bad column size");
  }
  return Status::OK();
}

uint64_t Trie::Column::Get(uint64_t i) const {
  const char* group = groups + (i >> kGroupBits) * kGroupHeaderSize;
  const uint64_t j = i & (kGroupSize - 1);
  const uint32_t layout = DecodeFixed32(group + 12);
  const uint32_t width = layout & kWidthMask;
  // With width 0 this reads bytes that are masked away.
  const uint64_t bit = uint64_t{layout >> kWidthBits} * 64 + j * width;
  const uint64_t residual = (DecodeFixed64(residuals + bit / 8) >> (bit % 8)) &
                            ((uint64_t{1} << width) - 1);
  return DecodeFixed64(group) + uint64_t{DecodeFixed32(group + 8)} * j +
         residual;
}

// Parsed trie node.
struct TrieIterator::Node {
  const char* path = nullptr;
  const uint8_t* labels = nullptr;
  const char* bitmap = nullptr;
  const char* pointers = nullptr;
  uint32_t pos = 0;
  uint32_t path_len = 0;
  uint32_t bucket_id = 0;
  uint32_t child_count = 0;
  uint32_t pointer_width = 0;
  uint8_t kind = 0;
  bool terminal = false;

  // Parses the node at pos, which Init() has validated.
  void Parse(const char* trie, uint32_t node_pos, uint32_t id_width) {
    const char* p = trie + node_pos;
    const uint8_t header = Byte(p++, 0);
    pos = node_pos;
    kind = header & kKindMask;
    terminal = (header & kTerminalBit) != 0;
    pointer_width = ((header >> kPointerWidthShift) & 3) + 1;
    path_len = Byte(p++, 0);
    if (path_len == kLongPathMarker) {
      p = GetVarint32Ptr(p, p + 5, &path_len);
    }
    path = p;
    p += path_len;
    if (terminal) {
      bucket_id = LoadLE(p, id_width);
      p += id_width;
    }
    if (kind == kKindSparse) {
      child_count = Byte(p, 0) + 1u;
      labels = lossless_cast<const uint8_t*>(p + 1);
      p += 1 + child_count;
    } else if (kind == kKindBitmap) {
      bitmap = p;
      child_count = 0;
      for (int i = 0; i < 4; i++) {
        child_count += BitsSetToOne(DecodeFixed64(p + 8 * i));
      }
      p += kBitmapBytes;
    } else {
      child_count = 0;
    }
    pointers = p;
  }

  uint32_t Child(uint32_t index) const {
    return pos - LoadLE(pointers + index * pointer_width, pointer_width);
  }

  // Labels of the first and last child.
  uint8_t FirstLabel() const {
    if (kind == kKindSparse) {
      return labels[0];
    }
    for (int i = 0; i < 4; i++) {
      const uint64_t w = DecodeFixed64(bitmap + 8 * i);
      if (w != 0) {
        return BitmapLabel(i, CountTrailingZeroBits(w));
      }
    }
    return 0;
  }
  uint8_t LastLabel() const {
    if (kind == kKindSparse) {
      return labels[child_count - 1];
    }
    for (int i = 3; i >= 0; i--) {
      const uint64_t w = DecodeFixed64(bitmap + 8 * i);
      if (w != 0) {
        return BitmapLabel(i, FloorLog2(w));
      }
    }
    return 0;
  }
};

Status Trie::Init(const Slice& data) {
  Status s = Parse(data);
  if (!s.ok()) {
    *this = Trie();
  }
  return s;
}

Status Trie::Parse(const Slice& data) {
  if (data.size() < kTrieHeaderSize) {
    return Corrupt("too short");
  }
  const char* p = data.data();
  if (DecodeFixed32(p + kMagicPos) != kMagic) {
    return Corrupt("bad magic number");
  }
  if (DecodeFixed32(p + kVersionPos) != kVersion) {
    return Corrupt("unsupported format version");
  }
  if (DecodeFixed32(p + kFlagsPos) != 0) {
    return Corrupt("unknown flags");
  }
  bucket_size_ = DecodeFixed32(p + kBucketSizePos);
  num_blocks_ = DecodeFixed64(p + kNumBlocksPos);
  num_keys_ = DecodeFixed64(p + kNumKeysPos);
  prefix_len_ = DecodeFixed32(p + kPrefixLenPos);
  max_key_len_ = DecodeFixed32(p + kMaxKeyLenPos);
  id_width_ = DecodeFixed32(p + kIdWidthPos);
  trie_root_ = DecodeFixed32(p + kTrieRootPos);
  size_gap_ = DecodeFixed64(p + kSizeGapPos);
  explicit_sizes_ = size_gap_ == kExplicitSizes;
  // Every block takes at least a quarter byte of the offset column, which
  // also keeps the arithmetic below from overflowing.
  if (bucket_size_ == 0 || num_keys_ > num_blocks_ ||
      (num_blocks_ == 0) != (num_keys_ == 0) || num_blocks_ / 4 > data.size() ||
      prefix_len_ > max_key_len_ || id_width_ < 1 || id_width_ > 4) {
    return Corrupt("bad header");
  }
  num_buckets_ = num_keys_ == 0 ? 0 : (num_keys_ - 1) / bucket_size_ + 1;
  if (num_buckets_ > 0 && num_buckets_ - 1 >= uint64_t{1} << (8 * id_width_)) {
    return Corrupt("bucket id width too small");
  }

  uint64_t sections[kNumSectionOffsets];
  for (size_t i = 0; i < kNumSectionOffsets; i++) {
    sections[i] = DecodeFixed64(p + kSectionsPos + 8 * i);
  }
  if (sections[kTrieSection] != kTrieHeaderSize + uint64_t{prefix_len_} ||
      (sections[kSizeTableSection] != 0) != explicit_sizes_ ||
      sections[kEnd] != data.size()) {
    return Corrupt("bad section offsets");
  }
  uint64_t prev = sections[kTrieSection];
  for (size_t i = kTrieSection + 1; i < kNumSectionOffsets; i++) {
    if (i == kSizeTableSection && !explicit_sizes_) {
      continue;
    }
    if (sections[i] < prev) {
      return Corrupt("bad section offsets");
    }
    prev = sections[i];
  }

  prefix_ = p + kTrieHeaderSize;
  trie_ = p + sections[kTrieSection];
  bucket_data_ = p + sections[kBucketDataSection];
  runs_ = p + sections[kRunTableSection];
  tags_ = p + sections[kTagTableSection];

  Status s =
      bucket_table_.Init(p + sections[kBucketTableSection],
                         p + sections[kBucketDataSection], num_buckets_ + 1);
  if (!s.ok()) {
    return s;
  }
  const uint64_t trie_size =
      sections[kBucketTableSection] - sections[kTrieSection];
  const uint64_t bucket_data_size =
      sections[kOffsetTableSection] - sections[kBucketDataSection];
  // A separator is the shared prefix, a bucket head spelled by the trie and
  // suffixes from its bucket, so those bytes bound its length. A larger
  // max_key_len is corrupt, and would loosen the length checks below and in
  // the iterator.
  if (max_key_len_ - prefix_len_ > trie_size + bucket_data_size) {
    return Corrupt("max key length exceeds index contents");
  }
  uint64_t prev_pos = 0;
  for (uint64_t b = 0; b <= num_buckets_; b++) {
    const uint64_t pos = bucket_table_.Get(b);
    if ((b == 0 && pos != 0) || pos < prev_pos ||
        (b == num_buckets_ && pos != bucket_data_size)) {
      return Corrupt("bad bucket table");
    }
    prev_pos = pos;
  }
  const uint64_t offset_table_end = explicit_sizes_
                                        ? sections[kSizeTableSection]
                                        : sections[kRunTableSection];
  s = offset_table_.Init(p + sections[kOffsetTableSection],
                         p + offset_table_end, num_blocks_ + 1);
  if (!s.ok()) {
    return s;
  }
  // Offsets never decrease and, when sizes are derived, leave room for the
  // gap after each block, so a block size cannot wrap around.
  const uint64_t min_step = explicit_sizes_ ? 0 : size_gap_;
  uint64_t prev_offset = offset_table_.Get(0);
  for (uint64_t i = 1; i <= num_blocks_; i++) {
    const uint64_t offset = offset_table_.Get(i);
    if (offset < prev_offset || offset - prev_offset < min_step) {
      return Corrupt("bad offset table");
    }
    prev_offset = offset;
  }
  if (explicit_sizes_) {
    s = size_table_.Init(p + sections[kSizeTableSection],
                         p + sections[kRunTableSection], num_blocks_);
    if (!s.ok()) {
      return s;
    }
  }
  s = ValidateSideTables(
      sections[kTagTableSection] - sections[kRunTableSection],
      sections[kEnd] - sections[kTagTableSection]);
  if (!s.ok()) {
    return s;
  }
  return ValidateTrie(trie_size);
}

Status Trie::ValidateSideTables(uint64_t run_table_size,
                                uint64_t tag_table_size) {
  if (run_table_size < 8 || tag_table_size < 8) {
    return Corrupt("truncated side table");
  }
  num_runs_ = DecodeFixed64(runs_);
  num_tags_ = DecodeFixed64(tags_);
  if (num_runs_ > run_table_size / kSideTableEntrySize ||
      run_table_size != 8 + num_runs_ * kSideTableEntrySize ||
      num_tags_ > tag_table_size / kSideTableEntrySize ||
      tag_table_size != 8 + num_tags_ * kSideTableEntrySize) {
    return Corrupt("bad side table size");
  }
  uint64_t prev_key = 0;
  uint64_t prev_extra = 0;
  for (uint64_t r = 0; r < num_runs_; r++) {
    const char* e = runs_ + 8 + r * kSideTableEntrySize;
    const uint64_t key_index = DecodeFixed64(e);
    const uint64_t extra = DecodeFixed64(e + 8);
    if (key_index >= num_keys_ || (r > 0 && key_index <= prev_key) ||
        extra <= prev_extra) {
      return Corrupt("bad run table");
    }
    prev_key = key_index;
    prev_extra = extra;
  }
  if (prev_extra != num_blocks_ - num_keys_) {
    return Corrupt("run table does not match block count");
  }
  uint64_t prev_block = 0;
  for (uint64_t t = 0; t < num_tags_; t++) {
    const char* e = tags_ + 8 + t * kSideTableEntrySize;
    const uint64_t block = DecodeFixed64(e);
    if (block >= num_blocks_ || (t > 0 && block <= prev_block) ||
        DecodeFixed64(e + 8) == 0) {
      return Corrupt("bad tag table");
    }
    prev_block = block;
  }
  return Status::OK();
}

uint64_t Trie::SideTableLowerBound(const char* table, uint64_t count,
                                   uint64_t value) {
  uint64_t lo = 0;
  uint64_t hi = count;
  while (lo < hi) {
    const uint64_t mid = lo + (hi - lo) / 2;
    if (DecodeFixed64(table + 8 + mid * kSideTableEntrySize) < value) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

uint64_t Trie::RunKey(uint64_t r) const {
  return DecodeFixed64(runs_ + 8 + r * kSideTableEntrySize);
}

uint64_t Trie::RunExtraThrough(uint64_t r) const {
  return DecodeFixed64(runs_ + 16 + r * kSideTableEntrySize);
}

uint64_t Trie::TagBlock(uint64_t t) const {
  return DecodeFixed64(tags_ + 8 + t * kSideTableEntrySize);
}

uint64_t Trie::TagValue(uint64_t t) const {
  return DecodeFixed64(tags_ + 16 + t * kSideTableEntrySize);
}

Status Trie::ValidateTrie(uint64_t trie_size) {
  if (num_keys_ == 0) {
    return trie_size == 0 && trie_root_ == 0 ? Status::OK()
                                             : Corrupt("unexpected trie");
  }
  if (trie_root_ >= trie_size) {
    return Corrupt("bad trie root");
  }
  const uint64_t max_head_len = max_key_len_ - prefix_len_;
  // Pre-order walk. A valid trie spells the bucket heads in bucket order.
  struct Visit {
    uint32_t pos;
    uint64_t head_len;
  };
  std::vector<Visit> stack;
  stack.push_back({trie_root_, 0});
  uint64_t next_bucket = 0;
  uint64_t visited = 0;
  while (!stack.empty()) {
    const Visit v = stack.back();
    stack.pop_back();
    // Every node takes at least two bytes.
    if (++visited > trie_size / 2) {
      return Corrupt("trie has too many nodes");
    }
    const char* p = trie_ + v.pos;
    const char* end = trie_ + trie_size;
    const uint8_t header = Byte(p++, 0);
    const uint8_t kind = header & kKindMask;
    const bool terminal = (header & kTerminalBit) != 0;
    const uint32_t pointer_width = ((header >> kPointerWidthShift) & 3) + 1;
    if ((header & kReservedNodeBits) != 0 || kind > kKindBitmap ||
        (kind == kKindLeaf && !terminal) || p >= end) {
      return Corrupt("bad trie node");
    }
    uint32_t path_len = Byte(p++, 0);
    if (path_len == kLongPathMarker) {
      p = GetVarint32Ptr(p, end, &path_len);
      if (p == nullptr) {
        return Corrupt("bad trie node");
      }
    }
    if (path_len > lossless_cast<uint64_t>(end - p) ||
        path_len > max_head_len - v.head_len) {
      return Corrupt("bad trie node path");
    }
    p += path_len;
    const uint64_t head_len = v.head_len + path_len;
    if (terminal) {
      if (lossless_cast<uint64_t>(end - p) < id_width_ ||
          LoadLE(p, id_width_) != next_bucket) {
        return Corrupt("bad trie bucket id");
      }
      next_bucket++;
      p += id_width_;
    }
    uint32_t child_count = 0;
    if (kind == kKindSparse) {
      if (p >= end) {
        return Corrupt("bad trie node");
      }
      child_count = Byte(p, 0) + 1u;
      if (child_count > lossless_cast<uint64_t>(end - p - 1)) {
        return Corrupt("bad trie node");
      }
      for (uint32_t i = 1; i < child_count; i++) {
        if (Byte(p + 1, i) <= Byte(p + 1, i - 1)) {
          return Corrupt("trie labels out of order");
        }
      }
      p += 1 + child_count;
    } else if (kind == kKindBitmap) {
      if (lossless_cast<uint64_t>(end - p) < kBitmapBytes) {
        return Corrupt("bad trie node");
      }
      for (int i = 0; i < 4; i++) {
        child_count += BitsSetToOne(DecodeFixed64(p + 8 * i));
      }
      if (child_count == 0) {
        return Corrupt("empty trie bitmap");
      }
      p += kBitmapBytes;
    }
    if (child_count > 0 && head_len >= max_head_len) {
      return Corrupt("bad trie node path");
    }
    if (uint64_t{child_count} * pointer_width >
        lossless_cast<uint64_t>(end - p)) {
      return Corrupt("bad trie node");
    }
    // Push children last first, so the first child is visited next.
    for (uint32_t i = child_count; i-- > 0;) {
      const uint32_t pointer = LoadLE(p + i * pointer_width, pointer_width);
      if (pointer == 0 || pointer > v.pos) {
        return Corrupt("bad trie pointer");
      }
      stack.push_back({v.pos - pointer, head_len + 1});
    }
  }
  if (next_bucket != num_buckets_) {
    return Corrupt("trie does not match bucket count");
  }
  return Status::OK();
}

// ============================================================================
// TrieIterator
// ============================================================================

TrieIterator::TrieIterator(const Trie* index)
    : index_(index), block_(index->num_blocks_) {
  key_.reserve(std::max<size_t>(
      index_->prefix_len_,
      std::min<size_t>(index_->max_key_len_, kInitialKeyCapacity)));
  key_.assign(index_->prefix_, index_->prefix_len_);
}

void TrieIterator::Invalidate() { block_ = index_->num_blocks_; }

TrieBlockHandle TrieIterator::value() const {
  const uint64_t offset = index_->offset_table_.Get(block_);
  const uint64_t size =
      index_->explicit_sizes_
          ? index_->size_table_.Get(block_)
          : index_->offset_table_.Get(block_ + 1) - offset - index_->size_gap_;
  return TrieBlockHandle{offset, size};
}

void TrieIterator::Descend(const char* target, size_t target_len,
                           Candidate* floor, Candidate* ceiling) const {
  floor->spell = Spell::kNone;
  ceiling->spell = Spell::kNone;
  uint32_t pos = index_->trie_root_;
  size_t depth = 0;
  Node node;
  for (;;) {
    node.Parse(index_->trie_, pos, index_->id_width_);
    const size_t n = std::min<size_t>(node.path_len, target_len - depth);
    const size_t matched = CommonPrefix(target + depth, node.path, n);
    if (matched < n) {
      // Every head below this node is on one side of target.
      if (Byte(target, depth + matched) < Byte(node.path, matched)) {
        *ceiling = Candidate{depth, pos, Spell::kSmallest};
      } else {
        *floor = Candidate{depth, pos, Spell::kLargest};
      }
      return;
    }
    if (n < node.path_len) {
      // Target ends inside the path: it precedes every head below.
      *ceiling = Candidate{depth, pos, Spell::kSmallest};
      return;
    }
    const size_t label_depth = depth + node.path_len;
    if (node.terminal) {
      if (label_depth == target_len) {
        *ceiling = Candidate{label_depth, pos, Spell::kTerminal};
        return;
      }
      *floor = Candidate{label_depth, pos, Spell::kTerminal};
    } else if (label_depth == target_len) {
      *ceiling = Candidate{depth, pos, Spell::kSmallest};
      return;
    }
    if (node.child_count == 0) {
      return;
    }
    const uint8_t c = Byte(target, label_depth);
    uint32_t rank;
    bool present;
    bool has_below = false;
    bool has_above = false;
    uint8_t below = 0;
    uint8_t above = 0;
    if (node.kind == kKindSparse) {
      rank = 0;
      while (rank < node.child_count && node.labels[rank] < c) {
        rank++;
      }
      present = rank < node.child_count && node.labels[rank] == c;
      if (rank > 0) {
        has_below = true;
        below = node.labels[rank - 1];
      }
      const uint32_t next = present ? rank + 1 : rank;
      if (next < node.child_count) {
        has_above = true;
        above = node.labels[next];
      }
    } else {
      uint64_t words[4];
      for (int i = 0; i < 4; i++) {
        words[i] = DecodeFixed64(node.bitmap + 8 * i);
      }
      const int wi = c / 64;
      const int bi = c % 64;
      rank = 0;
      for (int i = 0; i < wi; i++) {
        rank += BitsSetToOne(words[i]);
      }
      const uint64_t lower = words[wi] & ((uint64_t{1} << bi) - 1);
      rank += BitsSetToOne(lower);
      present = ((words[wi] >> bi) & 1) != 0;
      if (lower != 0) {
        has_below = true;
        below = BitmapLabel(wi, FloorLog2(lower));
      } else {
        for (int i = wi - 1; i >= 0; i--) {
          if (words[i] != 0) {
            has_below = true;
            below = BitmapLabel(i, FloorLog2(words[i]));
            break;
          }
        }
      }
      const uint64_t higher =
          bi == 63 ? 0 : (words[wi] & ~((uint64_t{2} << bi) - 1));
      if (higher != 0) {
        has_above = true;
        above = BitmapLabel(wi, CountTrailingZeroBits(higher));
      } else {
        for (int i = wi + 1; i < 4; i++) {
          if (words[i] != 0) {
            has_above = true;
            above = BitmapLabel(i, CountTrailingZeroBits(words[i]));
            break;
          }
        }
      }
    }
    if (has_below) {
      *floor = Candidate{label_depth, node.Child(rank - 1), Spell::kLargest,
                         true, below};
    }
    if (has_above) {
      *ceiling = Candidate{label_depth, node.Child(present ? rank + 1 : rank),
                           Spell::kSmallest, true, above};
    }
    if (!present) {
      return;
    }
    pos = node.Child(rank);
    depth = label_depth + 1;
  }
}

uint64_t TrieIterator::SpellHead(const Candidate& candidate,
                                 const char* target) {
  key_.resize(index_->prefix_len_);
  key_.append(target, candidate.depth);
  Node node;
  if (candidate.spell == Spell::kTerminal) {
    node.Parse(index_->trie_, candidate.node, index_->id_width_);
    return node.bucket_id;
  }
  if (candidate.has_label) {
    key_.push_back(lossless_cast<char>(candidate.label));
  }
  uint32_t pos = candidate.node;
  for (;;) {
    node.Parse(index_->trie_, pos, index_->id_width_);
    key_.append(node.path, node.path_len);
    if (candidate.spell == Spell::kSmallest) {
      if (node.terminal) {
        return node.bucket_id;
      }
      key_.push_back(lossless_cast<char>(node.FirstLabel()));
      pos = node.Child(0);
    } else {
      if (node.child_count == 0) {
        return node.bucket_id;
      }
      key_.push_back(lossless_cast<char>(node.LastLabel()));
      pos = node.Child(node.child_count - 1);
    }
  }
}

void TrieIterator::EnterBucket(uint64_t bucket) {
  bucket_ = bucket;
  key_index_ = bucket * index_->bucket_size_;
  bucket_pos_ = index_->bucket_data_ + index_->bucket_table_.Get(bucket);
  bucket_end_ = index_->bucket_data_ + index_->bucket_table_.Get(bucket + 1);
}

bool TrieIterator::EnterBucketAt(const Candidate& candidate, const char* target,
                                 uint64_t bucket) {
  if (candidate.spell == Spell::kNone ||
      SpellHead(candidate, target) != bucket) {
    Invalidate();
    return false;
  }
  EnterBucket(bucket);
  return true;
}

void TrieIterator::EnterKey() {
  uint64_t extra_before = 0;
  uint64_t extra = 0;
  const uint64_t num_runs = index_->num_runs_;
  if (num_runs > 0) {
    // First run at or after the current key.
    const uint64_t lo =
        Trie::SideTableLowerBound(index_->runs_, num_runs, key_index_);
    if (lo > 0) {
      extra_before = index_->RunExtraThrough(lo - 1);
    }
    if (lo < num_runs && index_->RunKey(lo) == key_index_) {
      extra = index_->RunExtraThrough(lo) - extra_before;
    }
  }
  run_begin_ = key_index_ + extra_before;
  run_end_ = run_begin_ + 1 + extra;
  block_ = run_begin_;
}

Status TrieIterator::ApplyTag(uint64_t tag) {
  const uint64_t num_tags = index_->num_tags_;
  if (num_tags == 0) {
    return Status::OK();
  }
  // First tagged block at or after the current one.
  uint64_t lo = Trie::SideTableLowerBound(index_->tags_, num_tags, run_begin_);
  // A block without a tag sorts after every tag of its key.
  if (lo == num_tags || index_->TagBlock(lo) != run_begin_) {
    return Status::OK();
  }
  for (uint64_t b = run_begin_; b < run_end_; b++) {
    uint64_t block_tag = 0;
    if (lo < num_tags && index_->TagBlock(lo) == b) {
      block_tag = index_->TagValue(lo);
      lo++;
    }
    if (block_tag == 0 || tag >= block_tag) {
      block_ = b;
      return Status::OK();
    }
  }
  // Every block of this key precedes the target.
  return NextKey();
}

Status TrieIterator::DecodeNext(size_t* shared, const char** suffix,
                                size_t* suffix_len) {
  if (bucket_pos_ >= bucket_end_) {
    return Corrupt("truncated bucket");
  }
  const uint8_t header = Byte(bucket_pos_++, 0);
  uint32_t drop = header >> 4;
  uint32_t add = header & 0xF;
  if (header == kLongEntryMarker) {
    const char* p = GetVarint32Ptr(bucket_pos_, bucket_end_, &drop);
    if (p != nullptr) {
      p = GetVarint32Ptr(p, bucket_end_, &add);
    }
    if (p == nullptr) {
      return Corrupt("bad bucket entry");
    }
    bucket_pos_ = p;
  }
  const size_t prefix_len = index_->prefix_len_;
  const size_t len = key_.size() - prefix_len;
  if (drop > len || add > lossless_cast<size_t>(bucket_end_ - bucket_pos_) ||
      len - drop + add > index_->max_key_len_ - prefix_len) {
    return Corrupt("bad bucket entry");
  }
  *shared = len - drop;
  key_.resize(prefix_len + *shared);
  key_.append(bucket_pos_, add);
  *suffix = bucket_pos_;
  *suffix_len = add;
  bucket_pos_ += add;
  key_index_++;
  return Status::OK();
}

Status TrieIterator::DecodeNext() {
  size_t shared;
  const char* suffix;
  size_t suffix_len;
  return DecodeNext(&shared, &suffix, &suffix_len);
}

Status TrieIterator::ScanBucket(const char* target, size_t target_len,
                                bool* found, bool* equal) {
  const size_t prefix_len = index_->prefix_len_;
  const uint64_t end =
      std::min(index_->num_keys_, key_index_ + index_->bucket_size_);
  // Length of the prefix the current separator shares with target. The
  // current separator is always below target.
  size_t matched = CommonPrefix(key_.data() + prefix_len, target,
                                std::min(key_.size() - prefix_len, target_len));
  while (key_index_ + 1 < end) {
    size_t shared;
    const char* suffix;
    size_t suffix_len;
    Status s = DecodeNext(&shared, &suffix, &suffix_len);
    if (!s.ok()) {
      return s;
    }
    if (shared > matched) {
      // Same byte as the previous separator where it fell below target.
      continue;
    }
    if (shared < matched) {
      // Larger than the previous separator where it matched target.
      *found = true;
      *equal = false;
      return Status::OK();
    }
    const size_t rest = target_len - matched;
    const size_t n = std::min(suffix_len, rest);
    const size_t common = CommonPrefix(suffix, target + matched, n);
    if (common < n) {
      if (Byte(suffix, common) > Byte(target, matched + common)) {
        *found = true;
        *equal = false;
        return Status::OK();
      }
      matched += common;
    } else if (rest <= suffix_len) {
      // Target is a prefix of this separator, or equal to it.
      *found = true;
      *equal = rest == suffix_len;
      return Status::OK();
    } else {
      matched += suffix_len;
    }
  }
  *found = false;
  return Status::OK();
}

Status TrieIterator::SeekToFirst() {
  if (index_->num_keys_ == 0) {
    Invalidate();
    return Status::OK();
  }
  const Candidate first{0, index_->trie_root_, Spell::kSmallest};
  if (SpellHead(first, nullptr) != 0) {
    Invalidate();
    return Corrupt("first bucket head not found");
  }
  EnterBucket(0);
  EnterKey();
  return Status::OK();
}

Status TrieIterator::Seek(const Slice& key, uint64_t tag) {
  if (index_->num_keys_ == 0) {
    Invalidate();
    return Status::OK();
  }
  const char* target = key.data();
  size_t target_len = key.size();
  const size_t prefix_len = index_->prefix_len_;
  if (prefix_len > 0) {
    const int cmp =
        memcmp(target, index_->prefix_, std::min(prefix_len, target_len));
    if (cmp > 0) {
      Invalidate();
      return Status::OK();
    }
    if (cmp < 0 || target_len < prefix_len) {
      return SeekToFirst();
    }
    target += prefix_len;
    target_len -= prefix_len;
  }
  Candidate floor;
  Candidate ceiling;
  Descend(target, target_len, &floor, &ceiling);
  if (ceiling.spell == Spell::kTerminal) {
    // Target is a bucket head, so the floor's bucket holds only smaller keys.
    EnterBucket(SpellHead(ceiling, target));
    EnterKey();
    return ApplyTag(tag);
  }
  if (floor.spell == Spell::kNone) {
    // Every head is greater than target, so tags do not matter.
    return SeekToFirst();
  }
  const uint64_t bucket = SpellHead(floor, target);
  EnterBucket(bucket);
  bool found;
  bool equal = false;
  Status s = ScanBucket(target, target_len, &found, &equal);
  if (!s.ok()) {
    Invalidate();
    return s;
  }
  if (!found) {
    if (bucket + 1 == index_->num_buckets_) {
      Invalidate();
      return Status::OK();
    }
    // The next head is greater than target, so tags do not matter.
    if (!EnterBucketAt(ceiling, target, bucket + 1)) {
      return Corrupt("bucket head out of order");
    }
  }
  EnterKey();
  return equal ? ApplyTag(tag) : Status::OK();
}

Status TrieIterator::NextKey() {
  if (key_index_ + 1 >= index_->num_keys_) {
    Invalidate();
    return Status::OK();
  }
  const uint64_t bucket_end =
      std::min(index_->num_keys_, (bucket_ + 1) * index_->bucket_size_);
  if (key_index_ + 1 < bucket_end) {
    Status s = DecodeNext();
    if (!s.ok()) {
      Invalidate();
      return s;
    }
  } else {
    // The next bucket's head is the smallest head after the current key.
    const size_t prefix_len = index_->prefix_len_;
    scratch_.assign(key_.data() + prefix_len, key_.size() - prefix_len);
    scratch_.push_back('\0');
    Candidate floor;
    Candidate ceiling;
    Descend(scratch_.data(), scratch_.size(), &floor, &ceiling);
    if (!EnterBucketAt(ceiling, scratch_.data(), bucket_ + 1)) {
      return Corrupt("bucket head out of order");
    }
  }
  EnterKey();
  return Status::OK();
}

Status TrieIterator::AdvanceTo(uint64_t key_index) {
  while (key_index_ < key_index) {
    Status s = DecodeNext();
    if (!s.ok()) {
      Invalidate();
      return s;
    }
  }
  return Status::OK();
}

void TrieIterator::EnterKeyFromEnd() {
  EnterKey();
  block_ = run_end_ - 1;
}

Status TrieIterator::SeekToLast() {
  if (index_->num_keys_ == 0) {
    Invalidate();
    return Status::OK();
  }
  const uint64_t last_bucket = index_->num_buckets_ - 1;
  const Candidate last{0, index_->trie_root_, Spell::kLargest};
  if (SpellHead(last, nullptr) != last_bucket) {
    Invalidate();
    return Corrupt("last bucket head not found");
  }
  EnterBucket(last_bucket);
  Status s = AdvanceTo(index_->num_keys_ - 1);
  if (s.ok()) {
    EnterKeyFromEnd();
  }
  return s;
}

Status TrieIterator::Prev() {
  if (!Valid()) {
    return Status::OK();
  }
  if (block_ > run_begin_) {
    block_--;
    return Status::OK();
  }
  if (key_index_ == 0) {
    Invalidate();
    return Status::OK();
  }
  // The previous key's bucket starts with the largest head below the current
  // key. Front coding only decodes forward, so decode from that head.
  const uint64_t prev_key = key_index_ - 1;
  const size_t prefix_len = index_->prefix_len_;
  scratch_.assign(key_.data() + prefix_len, key_.size() - prefix_len);
  Candidate floor;
  Candidate ceiling;
  Descend(scratch_.data(), scratch_.size(), &floor, &ceiling);
  if (!EnterBucketAt(floor, scratch_.data(), prev_key / index_->bucket_size_)) {
    return Corrupt("bucket head out of order");
  }
  Status s = AdvanceTo(prev_key);
  if (s.ok()) {
    EnterKeyFromEnd();
  }
  return s;
}

Status TrieIterator::Next() {
  if (!Valid()) {
    return Status::OK();
  }
  if (block_ + 1 < run_end_) {
    block_++;
    return Status::OK();
  }
  return NextKey();
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
