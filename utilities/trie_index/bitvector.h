//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Bit-vector with O(1) rank and select operations.
//
// This is the foundational data structure for the LOUDS-based succinct trie.
// It stores a sequence of bits and supports:
//   - rank1(pos): Count of 1-bits in positions [0, pos)
//   - select1(i): Position of the i-th 1-bit (0-indexed)
//   - rank0(pos): Count of 0-bits in positions [0, pos)
//   - select0(i): Position of the i-th 0-bit (0-indexed)
//
// Implementation uses a two-level lookup table for rank (sampling every
// kBitsPerRankSample bits) and select hints + linear scan for select. Both
// rank and select operations are O(1). Select uses precomputed hint arrays
// that narrow the search to O(1) rank samples, then a word-level popcount
// scan within that interval.
//
// Memory layout (serialized):
//   [num_bits: uint64_t]
//   [num_ones: uint64_t]
//   [raw bits: ceil(num_bits/64) uint64_t words, 64-bit aligned]
//   [rank LUT: (num_bits/kBitsPerRankSample + 1) uint32_t entries, padded to 8]
//   [select1 hints: uint32_t entries, padded to 8]
//   [select0 hints: uint32_t entries, padded to 8]
//
// The rank LUT uses uint32_t entries (not uint64_t), which halves the LUT
// memory overhead. This is safe because the maximum cumulative popcount
// equals num_bits, and uint32_t can hold values up to ~4 billion. This
// limits individual bitvectors to ~4 billion bits, which is far beyond
// any realistic trie index: the largest bitvector (d_labels_) uses 256
// bits per dense trie node, so the limit corresponds to ~16 million dense
// nodes. A typical SST file has 16K-64K data blocks, producing a trie
// with at most a few hundred thousand nodes. An assertion in BuildRankLUT
// guards against overflow.
//
// The rank LUT stores cumulative popcount at every kBitsPerRankSample-bit
// boundary. For positions between boundaries, we compute the remaining
// popcount using hardware popcount on the intermediate words.
// ============================================================================

// Number of bits per rank lookup table entry. Must be a power of 2 and a
// multiple of 64. 256 gives a good balance between LUT size overhead (~12.5%
// of bitvector size with uint32_t entries) and the number of popcounts needed
// per rank query (at most 4 words). Benchmarking showed 256 is ~20% faster
// than 512 for trie Seek due to fewer popcount iterations per Rank1 call.
inline constexpr uint64_t kBitsPerRankSample = 256;
inline constexpr uint64_t kWordsPerRankSample = kBitsPerRankSample / 64;

// Number of 1-bits (or 0-bits) between select hint entries. Each hint stores
// the rank LUT index where the k-th group of ones/zeros begins, narrowing the
// Select search to a linear scan of 1-2 rank samples, making it O(1).
// 256 matches kBitsPerRankSample so hints map directly to rank LUT entries.
inline constexpr uint64_t kOnesPerSelectHint = 256;

// Portable popcount using RocksDB's BitsSetToOne, which handles MSVC,
// GCC, and Clang with hardware POPCNT when available.
inline uint64_t Popcount(uint64_t x) {
  return static_cast<uint64_t>(BitsSetToOne(x));
}

// Count trailing zeros. Returns 64 if x == 0. Uses RocksDB's
// CountTrailingZeroBits for portability (MSVC + GCC/Clang).
inline uint64_t Ctz(uint64_t x) {
  return x == 0 ? 64 : static_cast<uint64_t>(CountTrailingZeroBits(x));
}

// Count trailing zeros for a known-nonzero value. Avoids the zero-check
// branch when the caller has already verified x != 0.
inline uint64_t CtzNonZero(uint64_t x) {
  assert(x != 0);
  return static_cast<uint64_t>(CountTrailingZeroBits(x));
}

// Select the i-th set bit (0-indexed) within a 64-bit word.
// Precondition: i < Popcount(word).
#if defined(__BMI2__) && defined(__x86_64__)
// Hardware-accelerated using BMI2 PDEP: deposits bit i among set bits of word,
// then counts trailing zeros to find its position. Single PDEP + CTZ.
inline uint64_t FindNthSetBitInWord(uint64_t word, uint64_t i) {
  return __builtin_ctzll(_pdep_u64(1ULL << i, word));
}
#else
// Popcount-based binary search: O(log 64) = 6 steps (5 popcounts + 1 bit test).
// Matches the SuRF reference implementation's select64_popcount_search().
inline uint64_t FindNthSetBitInWord(uint64_t word, uint64_t i) {
  // Binary search: narrow down which 32-bit half, then 16, 8, 4, 2, 1.
  uint64_t pos = 0;
  uint64_t pc;

  pc = Popcount(word & 0xFFFFFFFFULL);
  if (i >= pc) {
    word >>= 32;
    pos += 32;
    i -= pc;
  }
  pc = Popcount(word & 0xFFFFULL);
  if (i >= pc) {
    word >>= 16;
    pos += 16;
    i -= pc;
  }
  pc = Popcount(word & 0xFFULL);
  if (i >= pc) {
    word >>= 8;
    pos += 8;
    i -= pc;
  }
  pc = Popcount(word & 0xFULL);
  if (i >= pc) {
    word >>= 4;
    pos += 4;
    i -= pc;
  }
  pc = Popcount(word & 0x3ULL);
  if (i >= pc) {
    word >>= 2;
    pos += 2;
    i -= pc;
  }
  pc = word & 1;
  if (i >= pc) {
    pos += 1;
  }
  return pos;
}
#endif

// ============================================================================
// BitvectorBuilder: Builds a bitvector incrementally, bit by bit.
// ============================================================================
class BitvectorBuilder {
 public:
  BitvectorBuilder() : num_bits_(0) {}

  // Append a single bit (0 or 1).
  void Append(bool bit) {
    if (num_bits_ % 64 == 0) {
      words_.push_back(0);
    }
    if (bit) {
      words_.back() |= (uint64_t(1) << (num_bits_ % 64));
    }
    num_bits_++;
  }

  // Append a full 64-bit word of `nbits` bits (1..64). The bits are taken
  // from the low `nbits` positions of `word` in LSB-first order. This is
  // used by the LOUDS-Dense builder to emit 256-bit label bitmaps as 4
  // word-level operations instead of 256 individual Append() calls.
  // Precondition: num_bits_ must be 64-bit aligned (i.e., num_bits_ % 64 == 0).
  void AppendWord(uint64_t word, uint64_t nbits) {
    assert(nbits > 0 && nbits <= 64);
    assert(num_bits_ % 64 == 0);  // Must be word-aligned.
    words_.push_back(word);
    num_bits_ += nbits;
  }

  // Append `count` copies of `bit`. Optimized to operate at word granularity
  // when possible, which is significantly faster than the bit-by-bit loop for
  // large counts (e.g., appending 256 zeros for an empty dense node).
  void AppendMultiple(bool bit, uint64_t count) {
    if (count == 0) {
      return;
    }

    // Fill partial word at the end of the current buffer.
    uint64_t partial = num_bits_ % 64;
    if (partial > 0) {
      uint64_t fill = std::min(count, 64 - partial);
      if (bit) {
        // Set bits [partial, partial + fill) in the last word.
        uint64_t mask =
            ((fill == 64) ? ~uint64_t(0) : ((uint64_t(1) << fill) - 1))
            << partial;
        words_.back() |= mask;
      }
      // For bit==0, no action needed (words are zero-initialized).
      num_bits_ += fill;
      count -= fill;
    }

    // Append full 64-bit words.
    uint64_t full_word = bit ? ~uint64_t(0) : uint64_t(0);
    while (count >= 64) {
      words_.push_back(full_word);
      num_bits_ += 64;
      count -= 64;
    }

    // Append remaining bits (< 64).
    if (count > 0) {
      if (bit) {
        words_.push_back((uint64_t(1) << count) - 1);
      } else {
        words_.push_back(0);
      }
      num_bits_ += count;
    }
  }

  uint64_t NumBits() const { return num_bits_; }

  // Pre-allocate capacity for at least `num_bits` bits. Avoids repeated
  // reallocations when the final size is known or can be estimated.
  void Reserve(uint64_t num_bits) { words_.reserve((num_bits + 63) / 64); }

  // Access a specific bit. For testing/debugging only.
  bool GetBit(uint64_t pos) const {
    assert(pos < num_bits_);
    return (words_[pos / 64] >> (pos % 64)) & 1;
  }

  // Return the underlying word array.
  const std::vector<uint64_t>& Words() const { return words_; }

 private:
  std::vector<uint64_t> words_;
  uint64_t num_bits_;
};

// ============================================================================
// Bitvector: Immutable bitvector with O(1) rank and O(1) select.
//
// Created from serialized data (e.g., read from an SST file meta-block) or
// from a BitvectorBuilder. The bitvector does NOT own the underlying memory
// when created from a Slice -- the caller must ensure the data outlives this
// object.
//
// Select acceleration: select hints store the rank LUT index where every
// kOnesPerSelectHint-th 1-bit (or 0-bit) lives. This narrows the search
// in FindNthOneBit/FindNthZeroBit to a linear scan of O(1) rank samples,
// making Select O(1). The hint arrays add ~12.5% space overhead per bitvector.
// ============================================================================
class Bitvector {
 public:
  Bitvector()
      : words_(nullptr),
        rank_lut_(nullptr),
        select1_hints_(nullptr),
        select0_hints_(nullptr),
        num_bits_(0),
        num_ones_(0),
        num_words_(0),
        num_rank_samples_(0),
        num_select1_hints_(0),
        num_select0_hints_(0) {}

  // Bitvector contains raw pointers (words_, rank_lut_, select hints) that
  // may point into owned_data_ or into external memory (InitFromData).
  // Copying would create dangling pointers, so copy is deleted. Move is safe
  // only when the pointers point into owned_data_ (BuildFrom case) because
  // std::string's move constructor preserves the buffer address for
  // SSO-exceeding strings. For the InitFromData case, the pointers reference
  // external memory and are unaffected by moving owned_data_ (which is empty).
  ~Bitvector() = default;

  Bitvector(const Bitvector&) = delete;
  Bitvector& operator=(const Bitvector&) = delete;
  // Move constructor delegates to default ctor + move assignment to avoid
  // duplicating the field-copy + recompute + zero-other logic.
  Bitvector(Bitvector&& other) noexcept : Bitvector() {
    *this = std::move(other);
  }
  Bitvector& operator=(Bitvector&& other) noexcept {
    if (this != &other) {
      words_ = other.words_;
      rank_lut_ = other.rank_lut_;
      select1_hints_ = other.select1_hints_;
      select0_hints_ = other.select0_hints_;
      num_bits_ = other.num_bits_;
      num_ones_ = other.num_ones_;
      num_words_ = other.num_words_;
      num_rank_samples_ = other.num_rank_samples_;
      num_select1_hints_ = other.num_select1_hints_;
      num_select0_hints_ = other.num_select0_hints_;
      owned_data_ = std::move(other.owned_data_);
      // If this bitvector owns its data, the pointers must be re-seated into
      // our owned_data_ buffer. std::string move may or may not preserve the
      // buffer address (SSO optimization), so always re-seat.
      if (!owned_data_.empty()) {
        RecomputePointers();
      }
      other.words_ = nullptr;
      other.rank_lut_ = nullptr;
      other.select1_hints_ = nullptr;
      other.select0_hints_ = nullptr;
      other.num_bits_ = 0;
      other.num_ones_ = 0;
      other.num_words_ = 0;
      other.num_rank_samples_ = 0;
      other.num_select1_hints_ = 0;
      other.num_select0_hints_ = 0;
    }
    return *this;
  }

  // Initialize from serialized data. The data pointer must remain valid for
  // the lifetime of this object. On success, sets `*bytes_consumed` to the
  // number of bytes read from the input. Returns Status::OK() on success,
  // or Status::Corruption() if the data is malformed.
  Status InitFromData(const char* data, size_t data_size,
                      size_t* bytes_consumed);

  // Append serialized representation to `output`.
  void EncodeTo(std::string* output) const;

  // Build from a BitvectorBuilder. This allocates owned memory.
  void BuildFrom(const BitvectorBuilder& builder);

  // ---- Core Operations ----

  // Get the bit at position `pos`.
  inline bool GetBit(uint64_t pos) const {
    assert(pos < num_bits_);
    return (words_[pos / 64] >> (pos % 64)) & 1;
  }

  // rank1(pos): Count of 1-bits in positions [0, pos).
  // pos can be in [0, num_bits_].
  inline uint64_t Rank1(uint64_t pos) const {
    assert(pos <= num_bits_);
    // Which rank sample does `pos` fall into?
    uint64_t sample_idx = pos / kBitsPerRankSample;
    uint64_t sample_rank = rank_lut_[sample_idx];
    // Count remaining 1-bits in words between the sample boundary and `pos`.
    // Unrolled: kWordsPerRankSample is 4, so count is 0-3. The switch
    // eliminates loop control overhead and guarantees no branch mispredictions
    // from the loop counter.
    uint64_t word_start = sample_idx * kWordsPerRankSample;
    uint64_t word_end = pos / 64;
    switch (word_end - word_start) {
      case 3:
        sample_rank += Popcount(words_[word_start + 2]);
        [[fallthrough]];
      case 2:
        sample_rank += Popcount(words_[word_start + 1]);
        [[fallthrough]];
      case 1:
        sample_rank += Popcount(words_[word_start]);
        [[fallthrough]];
      case 0:
        break;
    }
    // Count bits within the final partial word [0, pos % 64).
    uint64_t remaining = pos % 64;
    if (remaining > 0) {
      uint64_t mask = (uint64_t(1) << remaining) - 1;
      sample_rank += Popcount(words_[word_end] & mask);
    }
    return sample_rank;
  }

  // rank0(pos): Count of 0-bits in positions [0, pos).
  inline uint64_t Rank0(uint64_t pos) const { return pos - Rank1(pos); }

  // select1(i): Position of the i-th 1-bit (0-indexed).
  // Returns num_bits_ if there are fewer than (i+1) 1-bits.
  // Inlined for hot-path performance -- the hint lookup + linear scan is
  // branch-predictor friendly and avoids function call overhead.
  inline uint64_t FindNthOneBit(uint64_t i) const {
    if (i >= num_ones_) {
      return num_bits_;
    }
    // Use select hints to narrow the search range.
    uint64_t lo;
    uint64_t hi;
    if (num_select1_hints_ > 0) {
      uint64_t hint_idx = i / kOnesPerSelectHint;
      lo = select1_hints_[hint_idx];
      hi = (hint_idx + 1 < num_select1_hints_) ? select1_hints_[hint_idx + 1]
                                               : num_rank_samples_ - 1;
    } else {
      lo = 0;
      hi = num_rank_samples_ - 1;
    }
    // Linear scan within the narrowed range.
    while (lo < hi && rank_lut_[lo + 1] <= i) {
      lo++;
    }
    // Scan words in the sample interval.
    uint64_t remaining = i - rank_lut_[lo];
    uint64_t word_start = lo * kWordsPerRankSample;
    uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);
    for (uint64_t w = word_start; w < word_end; w++) {
      uint64_t pc = Popcount(words_[w]);
      if (remaining < pc) {
        return w * 64 + FindNthSetBitInWord(words_[w], remaining);
      }
      remaining -= pc;
    }
    assert(false);
    return num_bits_;
  }

  // select0(i): Position of the i-th 0-bit (0-indexed).
  // Returns num_bits_ if there are fewer than (i+1) 0-bits.
  uint64_t FindNthZeroBit(uint64_t i) const;

  // Find the next set bit at or after position `pos`.
  // Returns num_bits_ if no set bit is found.
  // Inlined for hot-path performance -- called on every dense Seek, Advance,
  // and DescendToLeftmostLeaf.
  inline uint64_t NextSetBit(uint64_t pos) const {
    if (pos >= num_bits_) {
      return num_bits_;
    }
    uint64_t word_idx = pos / 64;
    uint64_t bit_idx = pos % 64;
    uint64_t word = words_[word_idx] >> bit_idx;
    if (word != 0) {
      uint64_t result = pos + CtzNonZero(word);
      return (result < num_bits_) ? result : num_bits_;
    }
    for (uint64_t w = word_idx + 1; w < num_words_; w++) {
      if (words_[w] != 0) {
        uint64_t result = w * 64 + CtzNonZero(words_[w]);
        return (result < num_bits_) ? result : num_bits_;
      }
    }
    return num_bits_;
  }

  // Find the last set bit strictly before position `pos`.
  // Returns num_bits_ (sentinel) if no set bit exists before pos.
  // Inlined for hot-path performance during reverse iteration.
  inline uint64_t PrevSetBit(uint64_t pos) const {
    if (pos == 0 || num_bits_ == 0) {
      return num_bits_;
    }
    if (pos > num_bits_) {
      pos = num_bits_;
    }
    pos--;
    uint64_t word_idx = pos / 64;
    uint64_t bit_idx = pos % 64;
    uint64_t mask =
        (bit_idx == 63) ? ~uint64_t(0) : ((uint64_t(1) << (bit_idx + 1)) - 1);
    uint64_t word = words_[word_idx] & mask;
    if (word != 0) {
      return word_idx * 64 + static_cast<uint64_t>(FloorLog2(word));
    }
    for (int64_t w = static_cast<int64_t>(word_idx) - 1; w >= 0; w--) {
      if (words_[w] != 0) {
        return static_cast<uint64_t>(w) * 64 +
               static_cast<uint64_t>(FloorLog2(words_[w]));
      }
    }
    return num_bits_;
  }

  // Find the distance from `pos` to the next set bit (exclusive).
  // Returns the distance in bits. Used by sparse level to compute node size.
  // pos must point to a set bit.
  uint64_t DistanceToNextSetBit(uint64_t pos) const;

  // ---- Accessors ----
  uint64_t NumBits() const { return num_bits_; }
  uint64_t NumOnes() const { return num_ones_; }
  uint64_t NumZeros() const { return num_bits_ - num_ones_; }

  // Size in bytes of the serialized representation.
  size_t SerializedSize() const;

 private:
  // Build the rank LUT and select hints from current words.
  // Used by BuildFrom().
  void BuildRankLUT();

  // Build select hint arrays from the rank LUT. Called after BuildRankLUT().
  void BuildSelectHints();

  // Calls fn(ones, k, sample) for every select hint: sample is the rank LUT
  // index of the interval holding the (k * kOnesPerSelectHint)-th one bit
  // (ones) or zero bit (!ones). Requires a correct rank LUT.
  template <typename Fn>
  void ForEachSelectHint(Fn fn) const;

  // Checks that the rank LUT, num_ones_ and select hints read by
  // InitFromData() match the bits, and that no bit past num_bits_ is set.
  Status ValidateDerivedData() const;

  // Re-compute pointers into owned_data_ after a move operation.
  // Re-seats pointers into owned_data_ buffer.
  // Layout: [words][rank_lut][select1_hints][select0_hints]
  void RecomputePointers() {
    const char* base = owned_data_.data();
    size_t words_bytes = num_words_ * sizeof(uint64_t);
    size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
    size_t select1_bytes = num_select1_hints_ * sizeof(uint32_t);

    words_ = reinterpret_cast<const uint64_t*>(base);
    rank_lut_ = reinterpret_cast<const uint32_t*>(base + words_bytes);
    select1_hints_ =
        reinterpret_cast<const uint32_t*>(base + words_bytes + rank_bytes);
    select0_hints_ = reinterpret_cast<const uint32_t*>(
        base + words_bytes + rank_bytes + select1_bytes);
  }

  // Pointer to the raw bit words. May point into external data (InitFromData)
  // or into owned_data_ (BuildFrom).
  const uint64_t* words_;

  // Pointer to the rank lookup table (uint32_t entries to halve LUT size).
  // Same ownership semantics as words_.
  const uint32_t* rank_lut_;

  // Select hint arrays: select1_hints_[k] is the rank LUT index where the
  // (k * kOnesPerSelectHint)-th 1-bit lives. select0_hints_ is analogous
  // for 0-bits. These narrow Select to a linear scan of 1-2 rank samples.
  const uint32_t* select1_hints_;
  const uint32_t* select0_hints_;

  uint64_t num_bits_;
  uint64_t num_ones_;
  uint64_t num_words_;
  uint64_t num_rank_samples_;
  uint64_t num_select1_hints_;
  uint64_t num_select0_hints_;

  // Owned storage when built from BitvectorBuilder (not from serialized data).
  std::string owned_data_;
};

// ============================================================================
// EliasFano: Compressed representation of a monotonically non-decreasing
// sequence of uint64_t values. Uses the Elias-Fano encoding which achieves
// near-optimal space (within 2 bits per element of the information-theoretic
// minimum) while supporting O(1) random access.
//
// Encoding:
//   Given n values in [0, U], each value v is split into:
//     - high part: v >> low_bits  (unary-coded in a bitvector)
//     - low part:  v & ((1 << low_bits) - 1)  (packed in a bit array)
//   where low_bits = floor(log2(U / n)) when n > 0 and U > n, or 0 when
//   n == 0 or U <= n.
//
//   The high-bits bitvector has ones at position high[i] + i for each
//   element i. The i-th value is recovered as:
//     value = (FindNthOneBit(high_bv, i) - i) << low_bits | packed_low[i]
//
// Memory layout (serialized):
//   [count     : uint64_t]  Number of elements
//   [universe  : uint64_t]  Maximum value + 1
//   [low_bits  : uint64_t]  Number of low bits per element
//   [high_bv   : Bitvector] Unary-coded high parts (with rank/select)
//   [low_words : ceil(count * low_bits / 64) * 8 bytes] Packed low parts
//
// Space for 32K offsets with U = 4GB: low_bits = 17, total ~76 KB vs
// 256 KB for raw uint64_t array (3.4x compression).
// ============================================================================
class EliasFano {
 public:
  EliasFano()
      : count_(0),
        universe_(0),
        low_bits_(0),
        low_mask_(0),
        low_words_(nullptr),
        num_low_words_(0) {}

  ~EliasFano() = default;

  EliasFano(const EliasFano&) = delete;
  EliasFano& operator=(const EliasFano&) = delete;

  // Custom move operations to re-seat low_words_ after moving owned_low_data_.
  // The default move would leave low_words_ pointing into the moved-from
  // object's owned_low_data_ buffer. For SSO-sized strings (owned_low_data_
  // with <= ~22 bytes, e.g. count_=1 low_bits_=8 -> 8 bytes), std::string
  // move copies the SSO buffer to a new address rather than transferring a
  // heap pointer, leaving low_words_ dangling. For heap-allocated strings
  // move transfers the pointer (address preserved), but we re-seat
  // unconditionally for correctness since SSO thresholds are implementation-
  // defined. This mirrors Bitvector's move pattern (RecomputePointers).
  EliasFano(EliasFano&& other) noexcept : EliasFano() {
    *this = std::move(other);
  }
  EliasFano& operator=(EliasFano&& other) noexcept {
    if (this != &other) {
      count_ = other.count_;
      universe_ = other.universe_;
      low_bits_ = other.low_bits_;
      low_mask_ = other.low_mask_;
      num_low_words_ = other.num_low_words_;
      high_bv_ = std::move(other.high_bv_);
      owned_low_data_ = std::move(other.owned_low_data_);
      // Re-seat low_words_ into our owned buffer. When owned_low_data_ is
      // non-empty, low_words_ must point into it (BuildFrom path). When
      // empty, low_words_ points to external memory (InitFromData path)
      // or is nullptr (empty sequence).
      if (!owned_low_data_.empty()) {
        low_words_ = reinterpret_cast<const uint64_t*>(owned_low_data_.data());
      } else {
        low_words_ = other.low_words_;
      }
      other.count_ = 0;
      other.universe_ = 0;
      other.low_bits_ = 0;
      other.low_mask_ = 0;
      other.low_words_ = nullptr;
      other.num_low_words_ = 0;
    }
    return *this;
  }

  // Build from a sorted sequence of uint64_t values.
  // Values must be monotonically non-decreasing.
  void BuildFrom(const uint64_t* values, uint64_t count, uint64_t universe);

  // Initialize from serialized data. Returns Status::OK() on success.
  Status InitFromData(const char* data, size_t data_size,
                      size_t* bytes_consumed);

  // Append serialized representation to `output`.
  void EncodeTo(std::string* output) const;

  // Access the i-th value (0-indexed). i must be < count_.
  inline uint64_t Access(uint64_t i) const {
    assert(i < count_);
    uint64_t high = high_bv_.FindNthOneBit(i) - i;
    uint64_t low = 0;
    if (low_bits_ > 0) {
      // Extract low_bits_ bits starting at bit position i * low_bits_.
      uint64_t bit_pos = i * low_bits_;
      uint64_t word_idx = bit_pos / 64;
      uint64_t bit_idx = bit_pos % 64;
      low = (low_words_[word_idx] >> bit_idx) & low_mask_;
      // Handle the case where the low bits span two words.
      // Guard bit_idx > 0 to prevent UB: when bit_idx == 0 the shift
      // (64 - bit_idx) would be 64 which overflows uint64_t.  This is
      // unreachable in practice (low_bits_ <= 63), but the guard makes
      // it explicit for static analyzers.
      if (bit_idx > 0 && bit_idx + low_bits_ > 64) {
        uint64_t remaining = bit_idx + low_bits_ - 64;
        low |= (low_words_[word_idx + 1] & ((uint64_t(1) << remaining) - 1))
               << (64 - bit_idx);
      }
    }
    return (high << low_bits_) | low;
  }

  uint64_t Count() const { return count_; }
  uint64_t Universe() const { return universe_; }

  // Size in bytes of the serialized representation.
  size_t SerializedSize() const;

 private:
  uint64_t count_;     // Number of elements.
  uint64_t universe_;  // Upper bound (max value + 1).
  uint64_t low_bits_;  // Number of low bits per element.
  uint64_t low_mask_;  // (1 << low_bits) - 1, precomputed for Access.

  // High bits: unary-coded bitvector with rank/select support.
  Bitvector high_bv_;

  // Low bits: packed array of low_bits_-bit values.
  // Points into external data (InitFromData) or into owned_low_data_.
  const uint64_t* low_words_;
  uint64_t num_low_words_;

  // Owned storage for low bits (when built from BuildFrom).
  std::string owned_low_data_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
