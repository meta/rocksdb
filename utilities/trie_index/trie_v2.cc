//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie_v2.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

namespace {

constexpr uint32_t kMagic = 0x54524945;  // "TRIE", shared with format 1
constexpr uint64_t kExplicitSizes = ~uint64_t{0};
constexpr size_t kTailPadding = 16;

// Columns.
constexpr uint32_t kGroupBits = 6;
constexpr uint64_t kGroupSize = uint64_t{1} << kGroupBits;
constexpr size_t kColumnHeaderSize = 16;
constexpr size_t kGroupHeaderSize = 16;
constexpr size_t kColumnPadding = 8;
// A residual is extracted with one unaligned 8-byte load and a shift of up to
// 7 bits.
constexpr uint32_t kMaxColumnWidth = 56;
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
enum Section : size_t {
  kTrieSection,
  kBucketTableSection,
  kBucketDataSection,
  kOffsetTableSection,
  kSizeTableSection,
  kRunTableSection,
  kTagTableSection,
  kEnd,
  kNumSectionOffsets,
};
static_assert(kSectionsPos + kNumSectionOffsets * 8 == kTrieV2HeaderSize);

Status Corrupt(const char* what) {
  return Status::Corruption("Trie index v2: ", what);
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

uint32_t LoadLE(const char* p, uint32_t width) {
  switch (width) {
    case 1:
      return static_cast<uint8_t>(p[0]);
    case 2:
      return DecodeFixed16(p);
    case 3:
      return DecodeFixed16(p) | (uint32_t{static_cast<uint8_t>(p[2])} << 16);
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

uint8_t Byte(const char* p, size_t i) { return static_cast<uint8_t>(p[i]); }

// Appends values as a column.
Status AppendColumn(const std::vector<uint64_t>& values, std::string* out) {
  const uint64_t count = values.size();
  const uint64_t num_groups = (count + kGroupSize - 1) >> kGroupBits;
  std::vector<uint64_t> residuals(count);
  std::string groups;
  uint64_t max_residual = 0;
  for (uint64_t g = 0; g < num_groups; g++) {
    const uint64_t lo = g << kGroupBits;
    const uint64_t n = std::min(count - lo, kGroupSize);
    const uint64_t* v = values.data() + lo;
    bool linear = v[n - 1] < kMaxLinearValue;
    for (uint64_t j = 1; linear && j < n; j++) {
      linear = v[j] >= v[j - 1];
    }
    uint64_t slope = 0;
    uint64_t base = v[0];
    if (linear && n > 1) {
      slope = (v[n - 1] - v[0]) / (n - 1);
      // v[j] - slope * j can be negative, but stays within int64_t.
      int64_t min_offset = std::numeric_limits<int64_t>::max();
      for (uint64_t j = 0; j < n; j++) {
        min_offset =
            std::min(min_offset, lossless_cast<int64_t>(v[j]) -
                                     lossless_cast<int64_t>(slope * j));
      }
      base = lossless_cast<uint64_t>(min_offset);
    } else {
      for (uint64_t j = 0; j < n; j++) {
        base = std::min(base, v[j]);
      }
    }
    for (uint64_t j = 0; j < n; j++) {
      // Unsigned wraparound gives the exact non-negative residual.
      residuals[lo + j] = v[j] - slope * j - base;
      max_residual = std::max(max_residual, residuals[lo + j]);
    }
    PutFixed64(&groups, base);
    PutFixed64(&groups, slope);
  }
  const uint32_t width = BitsFor(max_residual);
  if (width > kMaxColumnWidth) {
    return Status::NotSupported("Trie index v2: column value range too large");
  }
  PutFixed64(out, count);
  PutFixed32(out, width);
  PutFixed32(out, 0);
  out->append(groups);
  const size_t start = out->size();
  out->append((count * width + 7) / 8 + kColumnPadding, '\0');
  for (uint64_t i = 0; i < count && width > 0; i++) {
    const uint64_t bit = i * width;
    char* p = &(*out)[start + bit / 8];
    EncodeFixed64(p, DecodeFixed64(p) | (residuals[i] << (bit % 8)));
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
      return Status::NotSupported("Trie index v2: trie too large");
    }
    *root = static_cast<uint32_t>(last_pos);
    return Status::OK();
  }

 private:
  // Heads [begin, end), which share their first `depth` bytes.
  struct Frame {
    uint64_t begin;
    uint64_t end;
    size_t depth;
    size_t path_end;
    bool terminal;
    uint64_t next;
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
      return Status::NotSupported("Trie index v2: trie too large");
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
      out->push_back(static_cast<char>(kLongPathMarker));
      PutVarint32(out, static_cast<uint32_t>(path_len));
    }
    out->append(heads_[f.begin].data() + f.depth, path_len);
    if (f.terminal) {
      PutLE(out, f.begin, id_width_);
    }
    if (kind == kKindSparse) {
      out->push_back(static_cast<char>(f.labels.size() - 1));
      for (uint8_t label : f.labels) {
        out->push_back(static_cast<char>(label));
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
// TrieV2Builder
// ============================================================================

TrieV2Builder::TrieV2Builder(uint32_t bucket_size)
    : bucket_size_(bucket_size) {}

void TrieV2Builder::AddBlock(const Slice& separator,
                             const TrieBlockHandle& handle, uint64_t tag) {
  const uint64_t num_keys = key_ends_.size();
  int cmp = 1;
  if (num_keys > 0) {
    const size_t begin = num_keys > 1 ? key_ends_[num_keys - 2] : 0;
    cmp = separator.compare(
        Slice(key_bytes_.data() + begin, key_ends_.back() - begin));
  }
  if (cmp > 0) {
    key_bytes_.append(separator.data(), separator.size());
    key_ends_.push_back(key_bytes_.size());
  } else {
    out_of_order_ |= cmp < 0;
    if (!runs_.empty() && runs_.back().first == num_keys - 1) {
      runs_.back().second++;
    } else {
      runs_.emplace_back(num_keys - 1, 1);
    }
  }
  if (tag != 0) {
    tags_.emplace_back(handles_.size(), tag);
  }
  handles_.push_back(handle);
}

Status TrieV2Builder::Finish(std::string* output) {
  if (out_of_order_) {
    return Status::InvalidArgument("Trie index v2: separators out of order");
  }
  if (bucket_size_ == 0) {
    return Status::InvalidArgument("Trie index v2: bucket size must be > 0");
  }
  const uint64_t num_blocks = handles_.size();
  const uint64_t num_keys = key_ends_.size();
  const uint64_t num_buckets = (num_keys + bucket_size_ - 1) / bucket_size_;
  if (num_buckets > uint64_t{std::numeric_limits<uint32_t>::max()} + 1) {
    return Status::NotSupported("Trie index v2: too many buckets");
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
    return Status::NotSupported("Trie index v2: separator too long");
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
        bucket_data.push_back(static_cast<char>(kLongEntryMarker));
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
    offsets[i] = h.offset;
    uint64_t block_end = 0;
    if (h.size > std::numeric_limits<uint64_t>::max() - h.offset) {
      derived = false;
      continue;
    }
    block_end = h.offset + h.size;
    if (i + 1 == num_blocks) {
      if (derived &&
          size_gap <= std::numeric_limits<uint64_t>::max() - block_end) {
        offsets[num_blocks] = block_end + size_gap;
      } else {
        derived = false;
        offsets[num_blocks] = h.offset;
      }
    } else {
      const uint64_t next = handles_[i + 1].offset;
      if (next < block_end) {
        derived = false;
      } else if (i == 0) {
        size_gap = next - block_end;
      } else if (next - block_end != size_gap) {
        derived = false;
      }
    }
  }

  std::string out;
  out.resize(kTrieV2HeaderSize);
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
  for (const auto& run : runs_) {
    extra_blocks += run.second;
    PutFixed64(&out, run.first);
    PutFixed64(&out, extra_blocks);
  }
  sections[kTagTableSection] = out.size();
  PutFixed64(&out, tags_.size());
  for (const auto& tag : tags_) {
    PutFixed64(&out, tag.first);
    PutFixed64(&out, tag.second);
  }
  sections[kEnd] = out.size();
  out.append(kTailPadding, '\0');

  char* h = out.data();
  EncodeFixed32(h + kMagicPos, kMagic);
  EncodeFixed32(h + kVersionPos, kTrieV2FormatVersion);
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

// ============================================================================
// TrieV2Index
// ============================================================================

Status TrieV2Index::Column::Init(const char* begin, const char* end,
                                 uint64_t expected_count) {
  const uint64_t avail = lossless_cast<uint64_t>(end - begin);
  if (avail < kColumnHeaderSize) {
    return Corrupt("truncated column");
  }
  count = DecodeFixed64(begin);
  width = DecodeFixed32(begin + 8);
  if (count != expected_count || width > kMaxColumnWidth ||
      DecodeFixed32(begin + 12) != 0 || count / 4 > avail) {
    return Corrupt("bad column header");
  }
  const uint64_t num_groups = (count + kGroupSize - 1) >> kGroupBits;
  const uint64_t expected_size = kColumnHeaderSize +
                                 num_groups * kGroupHeaderSize +
                                 (count * width + 7) / 8 + kColumnPadding;
  if (expected_size != avail) {
    return Corrupt("bad column size");
  }
  groups = begin + kColumnHeaderSize;
  residuals = groups + num_groups * kGroupHeaderSize;
  mask = (uint64_t{1} << width) - 1;
  return Status::OK();
}

uint64_t TrieV2Index::Column::Get(uint64_t i) const {
  const char* group = groups + (i >> kGroupBits) * kGroupHeaderSize;
  // With width 0 this reads the zero padding and masks it away.
  const uint64_t bit = i * width;
  const uint64_t residual =
      (DecodeFixed64(residuals + bit / 8) >> (bit % 8)) & mask;
  return DecodeFixed64(group) +
         DecodeFixed64(group + 8) * (i & (kGroupSize - 1)) + residual;
}

// Parsed trie node.
struct TrieV2Iterator::Node {
  const char* path;
  const uint8_t* labels;
  const char* bitmap;
  const char* pointers;
  uint32_t pos;
  uint32_t path_len;
  uint32_t bucket_id;
  uint32_t child_count;
  uint32_t pointer_width;
  uint8_t kind;
  bool terminal;

  // Parses the node at pos, which Init() has validated.
  void Parse(const char* trie, uint32_t node_pos, uint32_t id_width) {
    const char* p = trie + node_pos;
    const uint8_t header = static_cast<uint8_t>(*p++);
    pos = node_pos;
    kind = header & kKindMask;
    terminal = (header & kTerminalBit) != 0;
    pointer_width = ((header >> kPointerWidthShift) & 3) + 1;
    path_len = static_cast<uint8_t>(*p++);
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
      child_count = static_cast<uint8_t>(*p) + 1u;
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

  // Label of child `index` when it is the first or last child.
  uint8_t FirstLabel() const {
    if (kind == kKindSparse) {
      return labels[0];
    }
    for (int i = 0; i < 4; i++) {
      const uint64_t w = DecodeFixed64(bitmap + 8 * i);
      if (w != 0) {
        return static_cast<uint8_t>(64 * i + CountTrailingZeroBits(w));
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
        return static_cast<uint8_t>(64 * i + FloorLog2(w));
      }
    }
    return 0;
  }
};

Status TrieV2Index::Init(const Slice& data) {
  if (data.size() < kTrieV2HeaderSize + kTailPadding) {
    return Corrupt("too short");
  }
  const char* p = data.data();
  if (DecodeFixed32(p + kMagicPos) != kMagic) {
    return Corrupt("bad magic number");
  }
  if (DecodeFixed32(p + kVersionPos) != kTrieV2FormatVersion) {
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
  if (num_buckets_ > 0 && id_width_ < 4 &&
      num_buckets_ - 1 >= uint64_t{1} << (8 * id_width_)) {
    return Corrupt("bucket id width too small");
  }

  uint64_t sections[kNumSectionOffsets];
  for (size_t i = 0; i < kNumSectionOffsets; i++) {
    sections[i] = DecodeFixed64(p + kSectionsPos + 8 * i);
  }
  if (sections[kTrieSection] != kTrieV2HeaderSize + uint64_t{prefix_len_} ||
      (sections[kSizeTableSection] != 0) != explicit_sizes_ ||
      sections[kEnd] > data.size() - kTailPadding) {
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

  data_ = p;
  prefix_ = p + kTrieV2HeaderSize;
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
  const uint64_t bucket_data_size =
      sections[kOffsetTableSection] - sections[kBucketDataSection];
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
  return ValidateTrie(sections[kBucketTableSection] - sections[kTrieSection]);
}

Status TrieV2Index::ValidateSideTables(uint64_t run_table_size,
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

uint64_t TrieV2Index::RunKey(uint64_t r) const {
  return DecodeFixed64(runs_ + 8 + r * kSideTableEntrySize);
}

uint64_t TrieV2Index::RunExtraThrough(uint64_t r) const {
  return DecodeFixed64(runs_ + 16 + r * kSideTableEntrySize);
}

uint64_t TrieV2Index::TagBlock(uint64_t t) const {
  return DecodeFixed64(tags_ + 8 + t * kSideTableEntrySize);
}

uint64_t TrieV2Index::TagValue(uint64_t t) const {
  return DecodeFixed64(tags_ + 16 + t * kSideTableEntrySize);
}

Status TrieV2Index::ValidateTrie(uint64_t trie_size) {
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
    const uint8_t header = static_cast<uint8_t>(*p++);
    const uint8_t kind = header & kKindMask;
    const bool terminal = (header & kTerminalBit) != 0;
    const uint32_t pointer_width = ((header >> kPointerWidthShift) & 3) + 1;
    if ((header & kReservedNodeBits) != 0 || kind > kKindBitmap ||
        (kind == kKindLeaf && !terminal) || p >= end) {
      return Corrupt("bad trie node");
    }
    uint32_t path_len = static_cast<uint8_t>(*p++);
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
      child_count = static_cast<uint8_t>(*p) + 1u;
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
// TrieV2Iterator
// ============================================================================

TrieV2Iterator::TrieV2Iterator(const TrieV2Index* index)
    : index_(index), block_(index->num_blocks_) {
  key_.reserve(index_->max_key_len_);
  key_.assign(index_->prefix_, index_->prefix_len_);
}

void TrieV2Iterator::Invalidate() { block_ = index_->num_blocks_; }

TrieBlockHandle TrieV2Iterator::value() const {
  const uint64_t offset = index_->offset_table_.Get(block_);
  const uint64_t size =
      index_->explicit_sizes_
          ? index_->size_table_.Get(block_)
          : index_->offset_table_.Get(block_ + 1) - offset - index_->size_gap_;
  return TrieBlockHandle{offset, size};
}

void TrieV2Iterator::Descend(const char* target, size_t target_len,
                             Candidate* floor, Candidate* ceiling) const {
  floor->spell = Spell::kNone;
  ceiling->spell = Spell::kNone;
  uint32_t pos = index_->trie_root_;
  size_t depth = 0;
  Node node;
  for (;;) {
    node.Parse(index_->trie_, pos, index_->id_width_);
    const uint32_t d32 = static_cast<uint32_t>(depth);
    const size_t n = std::min<size_t>(node.path_len, target_len - depth);
    const size_t matched = CommonPrefix(target + depth, node.path, n);
    if (matched < n) {
      // Every head below this node is on one side of target.
      if (Byte(target, depth + matched) < Byte(node.path, matched)) {
        *ceiling = Candidate{pos, d32, -1, Spell::kSmallest};
      } else {
        *floor = Candidate{pos, d32, -1, Spell::kLargest};
      }
      return;
    }
    if (n < node.path_len) {
      // Target ends inside the path: it precedes every head below.
      *ceiling = Candidate{pos, d32, -1, Spell::kSmallest};
      return;
    }
    const size_t label_depth = depth + node.path_len;
    const uint32_t ld32 = static_cast<uint32_t>(label_depth);
    if (node.terminal) {
      if (label_depth == target_len) {
        *ceiling = Candidate{pos, ld32, -1, Spell::kTerminal};
        return;
      }
      *floor = Candidate{pos, ld32, -1, Spell::kTerminal};
    } else if (label_depth == target_len) {
      *ceiling = Candidate{pos, d32, -1, Spell::kSmallest};
      return;
    }
    if (node.child_count == 0) {
      return;
    }
    const uint8_t c = Byte(target, label_depth);
    uint32_t rank;
    bool present;
    int below = -1;
    int above = -1;
    if (node.kind == kKindSparse) {
      rank = 0;
      while (rank < node.child_count && node.labels[rank] < c) {
        rank++;
      }
      present = rank < node.child_count && node.labels[rank] == c;
      if (rank > 0) {
        below = node.labels[rank - 1];
      }
      const uint32_t next = present ? rank + 1 : rank;
      if (next < node.child_count) {
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
        below = 64 * wi + FloorLog2(lower);
      } else {
        for (int i = wi - 1; i >= 0; i--) {
          if (words[i] != 0) {
            below = 64 * i + FloorLog2(words[i]);
            break;
          }
        }
      }
      const uint64_t higher =
          bi == 63 ? 0 : (words[wi] & ~((uint64_t{2} << bi) - 1));
      if (higher != 0) {
        above = 64 * wi + CountTrailingZeroBits(higher);
      } else {
        for (int i = wi + 1; i < 4; i++) {
          if (words[i] != 0) {
            above = 64 * i + CountTrailingZeroBits(words[i]);
            break;
          }
        }
      }
    }
    if (below >= 0) {
      *floor = Candidate{node.Child(rank - 1), ld32, below, Spell::kLargest};
    }
    if (above >= 0) {
      *ceiling = Candidate{node.Child(present ? rank + 1 : rank), ld32, above,
                           Spell::kSmallest};
    }
    if (!present) {
      return;
    }
    pos = node.Child(rank);
    depth = label_depth + 1;
  }
}

uint64_t TrieV2Iterator::SpellHead(const Candidate& candidate,
                                   const char* target) {
  key_.resize(index_->prefix_len_);
  key_.append(target, candidate.depth);
  Node node;
  if (candidate.spell == Spell::kTerminal) {
    node.Parse(index_->trie_, candidate.node, index_->id_width_);
    return node.bucket_id;
  }
  if (candidate.label >= 0) {
    key_.push_back(static_cast<char>(candidate.label));
  }
  uint32_t pos = candidate.node;
  for (;;) {
    node.Parse(index_->trie_, pos, index_->id_width_);
    key_.append(node.path, node.path_len);
    if (candidate.spell == Spell::kSmallest) {
      if (node.terminal) {
        return node.bucket_id;
      }
      key_.push_back(static_cast<char>(node.FirstLabel()));
      pos = node.Child(0);
    } else {
      if (node.child_count == 0) {
        return node.bucket_id;
      }
      key_.push_back(static_cast<char>(node.LastLabel()));
      pos = node.Child(node.child_count - 1);
    }
  }
}

void TrieV2Iterator::EnterBucket(uint64_t bucket) {
  bucket_ = bucket;
  key_index_ = bucket * index_->bucket_size_;
  bucket_pos_ = index_->bucket_data_ + index_->bucket_table_.Get(bucket);
  bucket_end_ = index_->bucket_data_ + index_->bucket_table_.Get(bucket + 1);
}

void TrieV2Iterator::EnterKey() {
  uint64_t extra_before = 0;
  uint64_t extra = 0;
  const uint64_t num_runs = index_->num_runs_;
  if (num_runs > 0) {
    // First run at or after the current key.
    uint64_t lo = 0;
    uint64_t hi = num_runs;
    while (lo < hi) {
      const uint64_t mid = lo + (hi - lo) / 2;
      if (index_->RunKey(mid) < key_index_) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
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

Status TrieV2Iterator::ApplyTag(const Slice& target, uint64_t tag) {
  const uint64_t num_tags = index_->num_tags_;
  if (num_tags == 0) {
    return Status::OK();
  }
  // First tagged block at or after the current one.
  uint64_t lo = 0;
  uint64_t hi = num_tags;
  while (lo < hi) {
    const uint64_t mid = lo + (hi - lo) / 2;
    if (index_->TagBlock(mid) < run_begin_) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  // A block without a tag sorts after every tag of its key, as does any
  // separator greater than target.
  if (lo == num_tags || index_->TagBlock(lo) != run_begin_ || key() != target) {
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

Status TrieV2Iterator::DecodeNext(size_t* shared, const char** suffix,
                                  size_t* suffix_len) {
  if (bucket_pos_ >= bucket_end_) {
    return Corrupt("truncated bucket");
  }
  const uint8_t header = static_cast<uint8_t>(*bucket_pos_++);
  uint32_t drop;
  uint32_t add;
  if (header != kLongEntryMarker) {
    drop = header >> 4;
    add = header & 0xF;
  } else {
    bucket_pos_ = GetVarint32Ptr(bucket_pos_, bucket_end_, &drop);
    if (bucket_pos_ != nullptr) {
      bucket_pos_ = GetVarint32Ptr(bucket_pos_, bucket_end_, &add);
    }
    if (bucket_pos_ == nullptr) {
      return Corrupt("bad bucket entry");
    }
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

Status TrieV2Iterator::ScanBucket(const char* target, size_t target_len,
                                  bool* found) {
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
      return Status::OK();
    }
    const size_t rest = target_len - matched;
    const size_t n = std::min(suffix_len, rest);
    const size_t q = CommonPrefix(suffix, target + matched, n);
    if (q < n) {
      if (Byte(suffix, q) > Byte(target, matched + q)) {
        *found = true;
        return Status::OK();
      }
      matched += q;
    } else if (rest <= suffix_len) {
      // Target is a prefix of this separator, or equal to it.
      *found = true;
      return Status::OK();
    } else {
      matched += suffix_len;
    }
  }
  *found = false;
  return Status::OK();
}

Status TrieV2Iterator::SeekToFirst() {
  if (index_->num_keys_ == 0) {
    Invalidate();
    return Status::OK();
  }
  const Candidate first{index_->trie_root_, 0, -1, Spell::kSmallest};
  if (SpellHead(first, nullptr) != 0) {
    Invalidate();
    return Corrupt("first bucket head not found");
  }
  EnterBucket(0);
  EnterKey();
  return Status::OK();
}

Status TrieV2Iterator::Seek(const Slice& key, uint64_t tag) {
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
  if (floor.spell == Spell::kNone) {
    Status s = SeekToFirst();
    return s.ok() ? ApplyTag(key, tag) : s;
  }
  const uint64_t bucket = SpellHead(floor, target);
  EnterBucket(bucket);
  bool found;
  Status s = ScanBucket(target, target_len, &found);
  if (!s.ok()) {
    Invalidate();
    return s;
  }
  if (!found) {
    if (bucket + 1 == index_->num_buckets_) {
      Invalidate();
      return Status::OK();
    }
    if (ceiling.spell == Spell::kNone ||
        SpellHead(ceiling, target) != bucket + 1) {
      Invalidate();
      return Corrupt("bucket head out of order");
    }
    EnterBucket(bucket + 1);
  }
  EnterKey();
  return ApplyTag(key, tag);
}

Status TrieV2Iterator::NextKey() {
  if (key_index_ + 1 >= index_->num_keys_) {
    Invalidate();
    return Status::OK();
  }
  const uint64_t bucket_end =
      std::min(index_->num_keys_, (bucket_ + 1) * index_->bucket_size_);
  if (key_index_ + 1 < bucket_end) {
    size_t shared;
    const char* suffix;
    size_t suffix_len;
    Status s = DecodeNext(&shared, &suffix, &suffix_len);
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
    if (ceiling.spell == Spell::kNone ||
        SpellHead(ceiling, scratch_.data()) != bucket_ + 1) {
      Invalidate();
      return Corrupt("bucket head out of order");
    }
    EnterBucket(bucket_ + 1);
  }
  EnterKey();
  return Status::OK();
}

Status TrieV2Iterator::AdvanceTo(uint64_t key_index) {
  while (key_index_ < key_index) {
    size_t shared;
    const char* suffix;
    size_t suffix_len;
    Status s = DecodeNext(&shared, &suffix, &suffix_len);
    if (!s.ok()) {
      Invalidate();
      return s;
    }
  }
  return Status::OK();
}

void TrieV2Iterator::EnterKeyFromEnd() {
  EnterKey();
  block_ = run_end_ - 1;
}

Status TrieV2Iterator::SeekToLast() {
  if (index_->num_keys_ == 0) {
    Invalidate();
    return Status::OK();
  }
  const uint64_t last_bucket = index_->num_buckets_ - 1;
  const Candidate last{index_->trie_root_, 0, -1, Spell::kLargest};
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

Status TrieV2Iterator::Prev() {
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
  const uint64_t bucket = prev_key / index_->bucket_size_;
  if (floor.spell == Spell::kNone ||
      SpellHead(floor, scratch_.data()) != bucket) {
    Invalidate();
    return Corrupt("bucket head out of order");
  }
  EnterBucket(bucket);
  Status s = AdvanceTo(prev_key);
  if (s.ok()) {
    EnterKeyFromEnd();
  }
  return s;
}

Status TrieV2Iterator::Next() {
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
