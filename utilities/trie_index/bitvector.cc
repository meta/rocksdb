//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/bitvector.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "port/lang.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Bitvector serialization format:
//
//   [num_bits      : uint64_t (fixed 8 bytes)]
//   [num_ones      : uint64_t (fixed 8 bytes)]
//   [words         : num_words * 8 bytes, where num_words = ceil(num_bits/64)]
//   [rank_lut      : num_rank_samples * 4 bytes (uint32_t), padded to 8]
//   [select1_hints : num_select1_hints * 4 bytes (uint32_t), padded to 8]
//   [select0_hints : num_select0_hints * 4 bytes (uint32_t), padded to 8]
//
// num_rank_samples = num_bits / kBitsPerRankSample + 1
//   (The +1 is for the sentinel entry at the end that stores total popcount.)
//
// num_select1_hints = ceil(num_ones / kOnesPerSelectHint) if num_ones > 0,
//   else 0. Analogously for num_select0_hints with (num_bits - num_ones).
//
// Using uint32_t for rank LUT entries halves the LUT memory overhead compared
// to uint64_t. See bitvector.h for why this is safe for trie index bitvectors.
// ============================================================================

size_t Bitvector::SerializedSize() const {
  // Header: 2 uint64_t (num_bits, num_ones).
  // Words: num_words * 8 bytes.
  // Rank LUT: num_rank_samples * 4 bytes, padded to 8.
  // FindNthOneBit hints: num_select1_hints * 4 bytes, padded to 8.
  // FindNthZeroBit hints: num_select0_hints * 4 bytes, padded to 8.
  size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
  size_t rank_padded = (rank_bytes + 7) & ~size_t(7);
  size_t s1_bytes = num_select1_hints_ * sizeof(uint32_t);
  size_t s1_padded = (s1_bytes + 7) & ~size_t(7);
  size_t s0_bytes = num_select0_hints_ * sizeof(uint32_t);
  size_t s0_padded = (s0_bytes + 7) & ~size_t(7);
  return 2 * sizeof(uint64_t) + num_words_ * sizeof(uint64_t) + rank_padded +
         s1_padded + s0_padded;
}

void Bitvector::EncodeTo(std::string* output) const {
  size_t old_size = output->size();
  size_t needed = SerializedSize();
  output->resize(old_size + needed);
  char* dst = &(*output)[old_size];

  // Write header: num_bits, num_ones
  memcpy(dst, &num_bits_, sizeof(uint64_t));
  dst += sizeof(uint64_t);
  memcpy(dst, &num_ones_, sizeof(uint64_t));
  dst += sizeof(uint64_t);

  // Write words
  if (num_words_ > 0) {
    memcpy(dst, words_, num_words_ * sizeof(uint64_t));
    dst += num_words_ * sizeof(uint64_t);
  }

  // Helper lambda to write a uint32_t array with 8-byte aligned padding.
  auto write_u32_array = [&](const uint32_t* arr, uint64_t count) {
    if (count > 0) {
      size_t bytes = count * sizeof(uint32_t);
      memcpy(dst, arr, bytes);
      size_t padded = (bytes + 7) & ~size_t(7);
      if (padded > bytes) {
        memset(dst + bytes, 0, padded - bytes);
      }
      dst += padded;
    }
  };

  // Write rank LUT, select1 hints, select0 hints.
  write_u32_array(rank_lut_, num_rank_samples_);
  write_u32_array(select1_hints_, num_select1_hints_);
  write_u32_array(select0_hints_, num_select0_hints_);
}

template <typename Fn>
void Bitvector::ForEachSelectHint(Fn fn) const {
  uint64_t sample_idx = 0;
  for (uint64_t k = 0; k < num_select1_hints_; k++) {
    uint64_t target = k * kOnesPerSelectHint;
    // Advance sample_idx until rank_lut_[sample_idx+1] > target.
    while (sample_idx + 1 < num_rank_samples_ &&
           rank_lut_[sample_idx + 1] <= target) {
      sample_idx++;
    }
    fn(true, k, static_cast<uint32_t>(sample_idx));
  }

  // Select0 hints analogously use zero counts.
  sample_idx = 0;
  for (uint64_t k = 0; k < num_select0_hints_; k++) {
    uint64_t target = k * kOnesPerSelectHint;
    while (sample_idx + 1 < num_rank_samples_ &&
           ((sample_idx + 1) * kBitsPerRankSample -
            rank_lut_[sample_idx + 1]) <= target) {
      sample_idx++;
    }
    fn(false, k, static_cast<uint32_t>(sample_idx));
  }
}

Status Bitvector::InitFromData(const char* data, size_t data_size,
                               size_t* bytes_consumed) {
  const char* start = data;

  // Read header
  if (data_size < 2 * sizeof(uint64_t)) {
    return Status::Corruption("Bitvector: insufficient data for header");
  }
  memcpy(&num_bits_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  memcpy(&num_ones_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  data_size -= 2 * sizeof(uint64_t);

  // Validate header fields from untrusted data.
  if (num_ones_ > num_bits_) {
    return Status::Corruption("Bitvector: num_ones exceeds num_bits");
  }
  // The rank LUT uses uint32_t entries, so num_bits must fit in uint32_t.
  // This also prevents integer overflow in derived size computations below.
  if (num_bits_ > UINT32_MAX) {
    return Status::Corruption(
        "Bitvector: num_bits exceeds uint32_t range for rank LUT");
  }

  // Compute derived sizes
  num_words_ = (num_bits_ + 63) / 64;
  num_rank_samples_ = num_bits_ / kBitsPerRankSample + 1;
  // Select hints: ceil(count / kOnesPerSelectHint) entries for ones/zeros.
  // If there are 0 ones (or 0 zeros), there are 0 hints.
  num_select1_hints_ =
      (num_ones_ > 0) ? (num_ones_ - 1) / kOnesPerSelectHint + 1 : 0;
  uint64_t num_zeros = num_bits_ - num_ones_;
  num_select0_hints_ =
      (num_zeros > 0) ? (num_zeros - 1) / kOnesPerSelectHint + 1 : 0;

  // Read words
  size_t words_bytes = num_words_ * sizeof(uint64_t);
  if (data_size < words_bytes) {
    return Status::Corruption("Bitvector: insufficient data for words");
  }
  // The data pointer must be 8-byte aligned for safe uint64_t access.
  // This is guaranteed by the serialization format (header is 16 bytes,
  // and the overall buffer comes from aligned block cache allocations).
  if (reinterpret_cast<uintptr_t>(data) % alignof(uint64_t) != 0) {
    return Status::Corruption("Bitvector: words data not 8-byte aligned");
  }
  words_ = reinterpret_cast<const uint64_t*>(data);
  data += words_bytes;
  data_size -= words_bytes;

  // Helper lambda: read a padded uint32_t array from the stream.
  auto read_u32_array = [&](const uint32_t** out, uint64_t count,
                            const char* name) -> Status {
    if (count == 0) {
      *out = nullptr;
      return Status::OK();
    }
    size_t bytes = count * sizeof(uint32_t);
    size_t padded = (bytes + 7) & ~size_t(7);
    if (data_size < padded) {
      return Status::Corruption(
          std::string("Bitvector: insufficient data for ") + name);
    }
    if (reinterpret_cast<uintptr_t>(data) % alignof(uint32_t) != 0) {
      return Status::Corruption(std::string("Bitvector: ") + name +
                                " not 4-byte aligned");
    }
    *out = reinterpret_cast<const uint32_t*>(data);
    data += padded;
    data_size -= padded;
    return Status::OK();
  };

  Status s;
  s = read_u32_array(&rank_lut_, num_rank_samples_, "rank LUT");
  if (!s.ok()) {
    return s;
  }
  s = read_u32_array(&select1_hints_, num_select1_hints_, "select1 hints");
  if (!s.ok()) {
    return s;
  }
  s = read_u32_array(&select0_hints_, num_select0_hints_, "select0 hints");
  if (!s.ok()) {
    return s;
  }

  // Validate select hint values. Each hint is a rank LUT index used to
  // narrow the FindNthOneBit/FindNthZeroBit search range. An out-of-bounds
  // hint would cause rank_lut_[hint] to read past the LUT array.
  for (uint64_t i = 0; i < num_select1_hints_; i++) {
    if (select1_hints_[i] >= num_rank_samples_) {
      return Status::Corruption(
          "Bitvector: select1 hint value exceeds rank LUT size");
    }
  }
  for (uint64_t i = 0; i < num_select0_hints_; i++) {
    if (select0_hints_[i] >= num_rank_samples_) {
      return Status::Corruption(
          "Bitvector: select0 hint value exceeds rank LUT size");
    }
  }

  s = ValidateDerivedData();
  if (!s.ok()) {
    return s;
  }

  *bytes_consumed = static_cast<size_t>(data - start);

  return Status::OK();
}

Status Bitvector::ValidateDerivedData() const {
  if (num_bits_ % 64 != 0 &&
      (words_[num_words_ - 1] >> (num_bits_ % 64)) != 0) {
    return Status::Corruption("Bitvector: bits set past num_bits");
  }
  uint64_t cumulative = 0;
  for (uint64_t s = 0; s < num_rank_samples_; s++) {
    if (rank_lut_[s] != cumulative) {
      return Status::Corruption("Bitvector: rank LUT inconsistent with bits");
    }
    const uint64_t word_start = s * kWordsPerRankSample;
    const uint64_t word_end =
        std::min(word_start + kWordsPerRankSample, num_words_);
    for (uint64_t w = word_start; w < word_end; w++) {
      cumulative += Popcount(words_[w]);
    }
  }
  if (cumulative != num_ones_) {
    return Status::Corruption("Bitvector: num_ones inconsistent with bits");
  }
  bool hints_match = true;
  ForEachSelectHint([&](bool ones, uint64_t k, uint32_t sample) {
    hints_match &= (ones ? select1_hints_ : select0_hints_)[k] == sample;
  });
  if (!hints_match) {
    return Status::Corruption("Bitvector: select hints inconsistent with bits");
  }
  return Status::OK();
}

void Bitvector::BuildFrom(const BitvectorBuilder& builder) {
  num_bits_ = builder.NumBits();
  num_words_ = (num_bits_ + 63) / 64;
  num_rank_samples_ = num_bits_ / kBitsPerRankSample + 1;

  // We need to build rank LUT first to know num_ones_, then compute hint
  // counts. Two-pass: first allocate for words + rank LUT, build rank LUT
  // to get num_ones_, then resize to include select hints.
  size_t words_bytes = num_words_ * sizeof(uint64_t);
  size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);

  // Phase 1: allocate words + rank LUT.
  owned_data_.resize(words_bytes + rank_bytes, '\0');
  if (num_words_ > 0) {
    memcpy(&owned_data_[0], builder.Words().data(), words_bytes);
  }
  words_ = reinterpret_cast<const uint64_t*>(owned_data_.data());
  rank_lut_ =
      reinterpret_cast<const uint32_t*>(owned_data_.data() + words_bytes);
  assert(reinterpret_cast<uintptr_t>(rank_lut_) % alignof(uint32_t) == 0);

  BuildRankLUT();

  // Phase 2: compute hint counts and reallocate to include them.
  num_select1_hints_ =
      (num_ones_ > 0) ? (num_ones_ - 1) / kOnesPerSelectHint + 1 : 0;
  uint64_t num_zeros = num_bits_ - num_ones_;
  num_select0_hints_ =
      (num_zeros > 0) ? (num_zeros - 1) / kOnesPerSelectHint + 1 : 0;

  size_t s1_bytes = num_select1_hints_ * sizeof(uint32_t);
  size_t s0_bytes = num_select0_hints_ * sizeof(uint32_t);
  owned_data_.resize(words_bytes + rank_bytes + s1_bytes + s0_bytes, '\0');

  // Recompute all pointers after resize (string may have reallocated).
  RecomputePointers();
  assert(reinterpret_cast<uintptr_t>(rank_lut_) % alignof(uint32_t) == 0);

  BuildSelectHints();
}

void Bitvector::BuildRankLUT() {
  // The rank LUT is mutable during construction, so we const_cast here.
  // After BuildRankLUT() completes, rank_lut_ is treated as read-only.
  uint32_t* lut = const_cast<uint32_t*>(rank_lut_);

  // Guard: uint32_t can hold values up to ~4 billion. The maximum cumulative
  // rank is num_ones_ (at most num_bits_), so asserting num_bits_ fits in
  // uint32_t guarantees all rank LUT entries fit.
  assert(num_bits_ <= UINT32_MAX);

  uint64_t cumulative = 0;
  for (uint64_t s = 0; s < num_rank_samples_; s++) {
    lut[s] = static_cast<uint32_t>(cumulative);
    // Sum popcount for the kWordsPerRankSample words in this sample interval.
    uint64_t word_start = s * kWordsPerRankSample;
    uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);
    for (uint64_t w = word_start; w < word_end; w++) {
      cumulative += Popcount(words_[w]);
    }
  }
  // Total number of 1-bits is the cumulative count after all words.
  num_ones_ = cumulative;
}

void Bitvector::BuildSelectHints() {
  ForEachSelectHint([this](bool ones, uint64_t k, uint32_t sample) {
    const_cast<uint32_t*>(ones ? select1_hints_ : select0_hints_)[k] = sample;
  });
}

// FindNthOneBit is now inline in bitvector.h for hot-path performance.

uint64_t Bitvector::FindNthZeroBit(uint64_t i) const {
  uint64_t num_zeros = num_bits_ - num_ones_;
  if (i >= num_zeros) {
    return num_bits_;
  }

  // Use select0 hints to narrow the search range.
  uint64_t lo;
  uint64_t hi;
  if (num_select0_hints_ > 0) {
    uint64_t hint_idx = i / kOnesPerSelectHint;
    lo = select0_hints_[hint_idx];
    hi = (hint_idx + 1 < num_select0_hints_) ? select0_hints_[hint_idx + 1]
                                             : num_rank_samples_ - 1;
  } else {
    lo = 0;
    hi = num_rank_samples_ - 1;
  }

  // Linear scan within the narrowed range.
  while (lo < hi) {
    uint64_t zeros_next = (lo + 1) * kBitsPerRankSample - rank_lut_[lo + 1];
    if (zeros_next <= i) {
      lo++;
    } else {
      break;
    }
  }

  uint64_t zeros_at_lo = lo * kBitsPerRankSample - rank_lut_[lo];
  uint64_t remaining = i - zeros_at_lo;
  uint64_t word_start = lo * kWordsPerRankSample;
  uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);

  for (uint64_t w = word_start; w < word_end; w++) {
    // For the last word, we may have fewer than 64 valid bits. We need to
    // mask off the padding bits so they don't count as zeros.
    uint64_t valid_bits = 64;
    if (w == num_words_ - 1 && num_bits_ % 64 != 0) {
      valid_bits = num_bits_ % 64;
    }
    uint64_t zeros_in_word = valid_bits - Popcount(words_[w]);
    if (remaining < zeros_in_word) {
      // The target 0-bit is in this word. Invert to get 1-bits at zero
      // positions, mask off padding, then find the n-th set bit in the word.
      uint64_t word = ~words_[w];
      if (valid_bits < 64) {
        word &= (uint64_t(1) << valid_bits) - 1;
      }
      return w * 64 + FindNthSetBitInWord(word, remaining);
    }
    remaining -= zeros_in_word;
  }

  assert(false);
  return num_bits_;
}

uint64_t Bitvector::DistanceToNextSetBit(uint64_t pos) const {
  assert(pos < num_bits_);
  assert(GetBit(pos));  // pos must be a set bit.

  // Find the next set bit after pos (exclusive).
  uint64_t next = NextSetBit(pos + 1);
  return next - pos;
}

// ============================================================================
// EliasFano implementation
// ============================================================================

void EliasFano::BuildFrom(const uint64_t* values, uint64_t count,
                          uint64_t universe) {
  count_ = count;
  universe_ = universe;

  if (count == 0) {
    low_bits_ = 0;
    low_mask_ = 0;
    low_words_ = nullptr;
    num_low_words_ = 0;
    // Build an empty high bitvector so it serializes/deserializes correctly.
    // Without this, high_bv_ stays default-constructed (num_rank_samples_=0),
    // but InitFromData computes num_rank_samples_=1 for 0-bit bitvectors,
    // causing a size mismatch on deserialization.
    BitvectorBuilder empty_builder;
    high_bv_.BuildFrom(empty_builder);
    return;
  }

  // Compute low_bits = floor(log2(universe / count)).
  // When universe <= count, low_bits = 0 (all bits are high).
  // FloorLog2 returns floor(log2(x)) for x >= 1.
  if (universe > count) {
    low_bits_ = static_cast<uint64_t>(FloorLog2(universe / count));
  } else {
    low_bits_ = 0;
  }
  low_mask_ =
      (low_bits_ < 64) ? ((uint64_t(1) << low_bits_) - 1) : ~uint64_t(0);

  // Build high-bits bitvector.
  // The high part of each value is (value >> low_bits_). The bitvector
  // has a 1-bit at position high[i] + i for each element i, with 0-bits
  // filling the gaps. Total length = max_high + count.
  uint64_t max_high =
      (count > 0 && low_bits_ < 64) ? (values[count - 1] >> low_bits_) : 0;
  uint64_t high_len = max_high + count;

  BitvectorBuilder high_builder;
  high_builder.Reserve(high_len + 1);

  uint64_t prev_high = 0;
  for (uint64_t i = 0; i < count; i++) {
    assert(i == 0 || values[i] >= values[i - 1]);  // Must be monotone.
    uint64_t high = (low_bits_ < 64) ? (values[i] >> low_bits_) : 0;
    // Append (high - prev_high) zeros followed by a 1.
    assert(high >= prev_high);
    high_builder.AppendMultiple(false, high - prev_high);
    high_builder.Append(true);
    prev_high = high;
  }

  high_bv_.BuildFrom(high_builder);

  // Build packed low-bits array.
  uint64_t total_low_bits = count * low_bits_;
  num_low_words_ = (total_low_bits + 63) / 64;
  owned_low_data_.resize(num_low_words_ * sizeof(uint64_t), '\0');
  uint64_t* low_buf = reinterpret_cast<uint64_t*>(&owned_low_data_[0]);
  low_words_ = low_buf;

  // Pack low bits: value i's low bits go at bit position i * low_bits_.
  for (uint64_t i = 0; i < count; i++) {
    uint64_t low = values[i] & low_mask_;
    uint64_t bit_pos = i * low_bits_;
    uint64_t word_idx = bit_pos / 64;
    uint64_t bit_idx = bit_pos % 64;
    low_buf[word_idx] |= low << bit_idx;
    // Handle word boundary crossing.
    if (bit_idx > 0 && bit_idx + low_bits_ > 64) {
      low_buf[word_idx + 1] |= low >> (64 - bit_idx);
    }
  }
}

size_t EliasFano::SerializedSize() const {
  // 3 uint64_t header + high bitvector + low words (8-byte aligned).
  size_t low_bytes = num_low_words_ * sizeof(uint64_t);
  return 3 * sizeof(uint64_t) + high_bv_.SerializedSize() + low_bytes;
}

void EliasFano::EncodeTo(std::string* output) const {
  size_t old_size = output->size();
  // Header: count, universe, low_bits
  PutFixed64(output, count_);
  PutFixed64(output, universe_);
  PutFixed64(output, low_bits_);

  // High bitvector
  high_bv_.EncodeTo(output);

  // Low words
  size_t low_bytes = num_low_words_ * sizeof(uint64_t);
  if (low_bytes > 0) {
    size_t cur = output->size();
    output->resize(cur + low_bytes);
    memcpy(&(*output)[cur], low_words_, low_bytes);
  }
  (void)old_size;
}

Status EliasFano::InitFromData(const char* data, size_t data_size,
                               size_t* bytes_consumed) {
  const char* start = data;

  // Read header: count, universe, low_bits.
  if (data_size < 3 * sizeof(uint64_t)) {
    return Status::Corruption("EliasFano: insufficient data for header");
  }
  memcpy(&count_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  memcpy(&universe_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  memcpy(&low_bits_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  data_size -= 3 * sizeof(uint64_t);

  // Validate header fields from untrusted data.
  if (low_bits_ > 63) {
    return Status::Corruption("EliasFano: low_bits exceeds 63");
  }
  // Upper-bound on count_: a sequence with > 2^30 elements would require
  // tens of GB of data. This prevents integer overflow in subsequent
  // count_ * low_bits_ and count_ * sizeof(...) arithmetic. Block checksums
  // are the first line of defense; this is defense-in-depth for the
  // deserialization path.
  static constexpr uint64_t kMaxReasonableCount = uint64_t(1) << 30;
  if (count_ > kMaxReasonableCount) {
    return Status::Corruption("EliasFano: count exceeds reasonable limit");
  }
  low_mask_ =
      (low_bits_ < 64) ? ((uint64_t(1) << low_bits_) - 1) : ~uint64_t(0);

  // Read high bitvector.
  size_t consumed = 0;
  Status s = high_bv_.InitFromData(data, data_size, &consumed);
  if (!s.ok()) {
    return s;
  }
  data += consumed;
  data_size -= consumed;

  // Read low words. The multiplication is safe from overflow because
  // count_ <= 2^30 and low_bits_ <= 63, so count_ * low_bits_ <= 2^36.
  uint64_t total_low_bits = count_ * low_bits_;
  num_low_words_ = (total_low_bits + 63) / 64;
  size_t low_bytes = num_low_words_ * sizeof(uint64_t);
  if (data_size < low_bytes) {
    return Status::Corruption("EliasFano: insufficient data for low words");
  }
  if (low_bytes > 0) {
    if (reinterpret_cast<uintptr_t>(data) % alignof(uint64_t) != 0) {
      return Status::Corruption("EliasFano: low words not 8-byte aligned");
    }
    low_words_ = reinterpret_cast<const uint64_t*>(data);
  } else {
    low_words_ = nullptr;
  }
  data += low_bytes;

  *bytes_consumed = static_cast<size_t>(data - start);
  return Status::OK();
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
