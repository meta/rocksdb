//  Copyright (c) 2019-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Implementation details of various Bloom filter implementations used in
// RocksDB. (DynamicBloom is in a separate file for now because it
// supports concurrent write.)

#pragma once
#include <stddef.h>
#include <stdint.h>

#include <cmath>

#include "port/port.h"  // for PREFETCH
#include "rocksdb/slice.h"
#include "util/hash.h"

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace ROCKSDB_NAMESPACE {

class BloomMath {
 public:
  // False positive rate of a standard Bloom filter, for given ratio of
  // filter memory bits to added keys, and number of probes per operation.
  // (The false positive rate is effectively independent of scale, assuming
  // the implementation scales OK.)
  static double StandardFpRate(double bits_per_key, int num_probes) {
    // Standard very-good-estimate formula. See
    // https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
    return std::pow(1.0 - std::exp(-num_probes / bits_per_key), num_probes);
  }

  // False positive rate of a "blocked"/"shareded"/"cache-local" Bloom filter,
  // for given ratio of filter memory bits to added keys, number of probes per
  // operation (all within the given block or cache line size), and block or
  // cache line size.
  static double CacheLocalFpRate(double bits_per_key, int num_probes,
                                 int cache_line_bits) {
    if (bits_per_key <= 0.0) {
      // Fix a discontinuity
      return 1.0;
    }
    double keys_per_cache_line = cache_line_bits / bits_per_key;
    // A reasonable estimate is the average of the FP rates for one standard
    // deviation above and below the mean bucket occupancy. See
    // https://github.com/meta/rocksdb/wiki/RocksDB-Bloom-Filter#the-math
    double keys_stddev = std::sqrt(keys_per_cache_line);
    double crowded_fp = StandardFpRate(
        cache_line_bits / (keys_per_cache_line + keys_stddev), num_probes);
    double uncrowded_fp = StandardFpRate(
        cache_line_bits / (keys_per_cache_line - keys_stddev), num_probes);
    return (crowded_fp + uncrowded_fp) / 2;
  }

  // False positive rate of querying a new item against `num_keys` items, all
  // hashed to `fingerprint_bits` bits. (This assumes the fingerprint hashes
  // themselves are stored losslessly. See Section 4 of
  // http://www.ccs.neu.edu/home/pete/pub/bloom-filters-verification.pdf)
  static double FingerprintFpRate(size_t num_keys, int fingerprint_bits) {
    double inv_fingerprint_space = std::pow(0.5, fingerprint_bits);
    // Base estimate assumes each key maps to a unique fingerprint.
    // Could be > 1 in extreme cases.
    double base_estimate = num_keys * inv_fingerprint_space;
    // To account for potential overlap, we choose between two formulas
    if (base_estimate > 0.0001) {
      // A very good formula assuming we don't construct a floating point
      // number extremely close to 1. Always produces a probability < 1.
      return 1.0 - std::exp(-base_estimate);
    } else {
      // A very good formula when base_estimate is far below 1. (Subtract
      // away the integral-approximated sum that some key has same hash as
      // one coming before it in a list.)
      return base_estimate - (base_estimate * base_estimate * 0.5);
    }
  }

  // Returns the probably of either of two independent(-ish) events
  // happening, given their probabilities. (This is useful for combining
  // results from StandardFpRate or CacheLocalFpRate with FingerprintFpRate
  // for a hash-efficient Bloom filter's FP rate. See Section 4 of
  // http://www.ccs.neu.edu/home/pete/pub/bloom-filters-verification.pdf)
  static double IndependentProbabilitySum(double rate1, double rate2) {
    // Use formula that avoids floating point extremely close to 1 if
    // rates are extremely small.
    return rate1 + rate2 - (rate1 * rate2);
  }
};

// A fast, flexible, and accurate cache-local Bloom implementation with
// SIMD-optimized query performance (currently using AVX2 on Intel). Write
// performance and non-SIMD read are very good, benefiting from FastRange32
// used in place of % and single-cycle multiplication on recent processors.
//
// Most other SIMD Bloom implementations sacrifice flexibility and/or
// accuracy by requiring num_probes to be a power of two and restricting
// where each probe can occur in a cache line. This implementation sacrifices
// SIMD-optimization for add (might still be possible, especially with AVX512)
// in favor of allowing any num_probes, not crossing cache line boundary,
// and accuracy close to theoretical best accuracy for a cache-local Bloom.
// E.g. theoretical best for 10 bits/key, num_probes=6, and 512-bit bucket
// (Intel cache line size) is 0.9535% FP rate. This implementation yields
// about 0.957%. (Compare to LegacyLocalityBloomImpl<false> at 1.138%, or
// about 0.951% for 1024-bit buckets, cache line size for some ARM CPUs.)
//
// This implementation can use a 32-bit hash (let h2 be h1 * 0x9e3779b9) or
// a 64-bit hash (split into two uint32s). With many millions of keys, the
// false positive rate associated with using a 32-bit hash can dominate the
// false positive rate of the underlying filter. At 10 bits/key setting, the
// inflection point is about 40 million keys, so 32-bit hash is a bad idea
// with 10s of millions of keys or more.
//
// Despite accepting a 64-bit hash, this implementation uses 32-bit fastrange
// to pick a cache line, which can be faster than 64-bit in some cases.
// This only hurts accuracy as you get into 10s of GB for a single filter,
// and accuracy abruptly breaks down at 256GB (2^32 cache lines). Switch to
// 64-bit fastrange if you need filters so big. ;)
//
// Using only a 32-bit input hash within each cache line has negligible
// impact for any reasonable cache line / bucket size, for arbitrary filter
// size, and potentially saves intermediate data size in some cases vs.
// tracking full 64 bits. (Even in an implementation using 64-bit arithmetic
// to generate indices, I might do the same, as a single multiplication
// suffices to generate a sufficiently mixed 64 bits from 32 bits.)
//
// This implementation is currently tied to Intel cache line size, 64 bytes ==
// 512 bits. If there's sufficient demand for other cache line sizes, this is
// a pretty good implementation to extend, but slight performance enhancements
// are possible with an alternate implementation (probably not very compatible
// with SIMD):
// (1) Use rotation in addition to multiplication for remixing
// (like murmur hash). (Using multiplication alone *slightly* hurts accuracy
// because lower bits never depend on original upper bits.)
// (2) Extract more than one bit index from each re-mix. (Only if rotation
// or similar is part of remix, because otherwise you're making the
// multiplication-only problem worse.)
// (3) Re-mix full 64 bit hash, to get maximum number of bit indices per
// re-mix.
//
class FastLocalBloomImpl {
 public:
  // NOTE: this has only been validated to enough accuracy for producing
  // reasonable warnings / user feedback, not for making functional decisions.
  static double EstimatedFpRate(size_t keys, size_t bytes, int num_probes,
                                int hash_bits) {
    return BloomMath::IndependentProbabilitySum(
        BloomMath::CacheLocalFpRate(8.0 * bytes / keys, num_probes,
                                    /*cache line bits*/ 512),
        BloomMath::FingerprintFpRate(keys, hash_bits));
  }

  static inline int ChooseNumProbes(int millibits_per_key) {
    // Since this implementation can (with AVX2) make up to 8 probes
    // for the same cost, we pick the most accurate num_probes, based
    // on actual tests of the implementation. Note that for higher
    // bits/key, the best choice for cache-local Bloom can be notably
    // smaller than standard bloom, e.g. 9 instead of 11 @ 16 b/k.
    if (millibits_per_key <= 2080) {
      return 1;
    } else if (millibits_per_key <= 3580) {
      return 2;
    } else if (millibits_per_key <= 5100) {
      return 3;
    } else if (millibits_per_key <= 6640) {
      return 4;
    } else if (millibits_per_key <= 8300) {
      return 5;
    } else if (millibits_per_key <= 10070) {
      return 6;
    } else if (millibits_per_key <= 11720) {
      return 7;
    } else if (millibits_per_key <= 14001) {
      // Would be something like <= 13800 but sacrificing *slightly* for
      // more settings using <= 8 probes.
      return 8;
    } else if (millibits_per_key <= 16050) {
      return 9;
    } else if (millibits_per_key <= 18300) {
      return 10;
    } else if (millibits_per_key <= 22001) {
      return 11;
    } else if (millibits_per_key <= 25501) {
      return 12;
    } else if (millibits_per_key > 50000) {
      // Top out at 24 probes (three sets of 8)
      return 24;
    } else {
      // Roughly optimal choices for remaining range
      // e.g.
      // 28000 -> 12, 28001 -> 13
      // 50000 -> 23, 50001 -> 24
      return (millibits_per_key - 1) / 2000 - 1;
    }
  }

  static inline void AddHash(uint32_t h1, uint32_t h2, uint32_t len_bytes,
                             int num_probes, char* data) {
    uint32_t bytes_to_cache_line = FastRange32(h1, len_bytes >> 6) << 6;
    AddHashPrepared(h2, num_probes, data + bytes_to_cache_line);
  }

  static inline void AddHashPrepared(uint32_t h2, int num_probes,
                                     char* data_at_cache_line) {
    uint32_t h = h2;
    for (int i = 0; i < num_probes; ++i, h *= uint32_t{0x9e3779b9}) {
      // 9-bit address within 512 bit cache line
      int bitpos = h >> (32 - 9);
      data_at_cache_line[bitpos >> 3] |= (uint8_t{1} << (bitpos & 7));
    }
  }

  static inline void PrepareHash(uint32_t h1, uint32_t len_bytes,
                                 const char* data,
                                 uint32_t /*out*/* byte_offset) {
    uint32_t bytes_to_cache_line = FastRange32(h1, len_bytes >> 6) << 6;
    PREFETCH(data + bytes_to_cache_line, 0 /* rw */, 1 /* locality */);
    PREFETCH(data + bytes_to_cache_line + 63, 0 /* rw */, 1 /* locality */);
    *byte_offset = bytes_to_cache_line;
  }

  static inline bool HashMayMatch(uint32_t h1, uint32_t h2, uint32_t len_bytes,
                                  int num_probes, const char* data) {
    uint32_t bytes_to_cache_line = FastRange32(h1, len_bytes >> 6) << 6;
    return HashMayMatchPrepared(h2, num_probes, data + bytes_to_cache_line);
  }

  static inline bool HashMayMatchPrepared(uint32_t h2, int num_probes,
                                          const char* data_at_cache_line) {
    uint32_t h = h2;
#ifdef __AVX2__
    int rem_probes = num_probes;

    // NOTE: For better performance for num_probes in {1, 2, 9, 10, 17, 18,
    // etc.} one can insert specialized code for rem_probes <= 2, bypassing
    // the SIMD code in those cases. There is a detectable but minor overhead
    // applied to other values of num_probes (when not statically determined),
    // but smoother performance curve vs. num_probes. But for now, when
    // in doubt, don't add unnecessary code.

    // Powers of 32-bit golden ratio, mod 2**32.
    const __m256i multipliers =
        _mm256_setr_epi32(0x00000001, 0x9e3779b9, 0xe35e67b1, 0x734297e9,
                          0x35fbe861, 0xdeb7c719, 0x448b211, 0x3459b749);

    for (;;) {
      // Eight copies of hash
      __m256i hash_vector = _mm256_set1_epi32(h);

      // Same effect as repeated multiplication by 0x9e3779b9 thanks to
      // associativity of multiplication.
      hash_vector = _mm256_mullo_epi32(hash_vector, multipliers);

      // Now the top 9 bits of each of the eight 32-bit values in
      // hash_vector are bit addresses for probes within the cache line.
      // While the platform-independent code uses byte addressing (6 bits
      // to pick a byte + 3 bits to pick a bit within a byte), here we work
      // with 32-bit words (4 bits to pick a word + 5 bits to pick a bit
      // within a word) because that works well with AVX2 and is equivalent
      // under little-endian.

      // Shift each right by 28 bits to get 4-bit word addresses.
      const __m256i word_addresses = _mm256_srli_epi32(hash_vector, 28);

      // Gather 32-bit values spread over 512 bits by 4-bit address. In
      // essence, we are dereferencing eight pointers within the cache
      // line.
      //
      // Option 1: AVX2 gather (seems to be a little slow - understandable)
      // const __m256i value_vector =
      //     _mm256_i32gather_epi32(static_cast<const int
      //     *>(data_at_cache_line),
      //                            word_addresses,
      //                            /*bytes / i32*/ 4);
      // END Option 1
      // Potentially unaligned as we're not *always* cache-aligned -> loadu
      const __m256i* mm_data =
          reinterpret_cast<const __m256i*>(data_at_cache_line);
      __m256i lower = _mm256_loadu_si256(mm_data);
      __m256i upper = _mm256_loadu_si256(mm_data + 1);
      // Option 2: AVX512VL permute hack
      // Only negligibly faster than Option 3, so not yet worth supporting
      // const __m256i value_vector =
      //    _mm256_permutex2var_epi32(lower, word_addresses, upper);
      // END Option 2
      // Option 3: AVX2 permute+blend hack
      // Use lowest three bits to order probing values, as if all from same
      // 256 bit piece.
      lower = _mm256_permutevar8x32_epi32(lower, word_addresses);
      upper = _mm256_permutevar8x32_epi32(upper, word_addresses);
      // Just top 1 bit of address, to select between lower and upper.
      const __m256i upper_lower_selector = _mm256_srai_epi32(hash_vector, 31);
      // Finally: the next 8 probed 32-bit values, in probing sequence order.
      const __m256i value_vector =
          _mm256_blendv_epi8(lower, upper, upper_lower_selector);
      // END Option 3

      // We might not need to probe all 8, so build a mask for selecting only
      // what we need. (The k_selector(s) could be pre-computed but that
      // doesn't seem to make a noticeable performance difference.)
      const __m256i zero_to_seven = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
      // Subtract rem_probes from each of those constants
      __m256i k_selector =
          _mm256_sub_epi32(zero_to_seven, _mm256_set1_epi32(rem_probes));
      // Negative after subtract -> use/select
      // Keep only high bit (logical shift right each by 31).
      k_selector = _mm256_srli_epi32(k_selector, 31);

      // Strip off the 4 bit word address (shift left)
      __m256i bit_addresses = _mm256_slli_epi32(hash_vector, 4);
      // And keep only 5-bit (32 - 27) bit-within-32-bit-word addresses.
      bit_addresses = _mm256_srli_epi32(bit_addresses, 27);
      // Build a bit mask
      const __m256i bit_mask = _mm256_sllv_epi32(k_selector, bit_addresses);

      // Like ((~value_vector) & bit_mask) == 0)
      bool match = _mm256_testc_si256(value_vector, bit_mask) != 0;

      // This check first so that it's easy for branch predictor to optimize
      // num_probes <= 8 case, making it free of unpredictable branches.
      if (rem_probes <= 8) {
        return match;
      } else if (!match) {
        return false;
      }
      // otherwise
      // Need another iteration. 0xab25f4c1 == golden ratio to the 8th power
      h *= 0xab25f4c1;
      rem_probes -= 8;
    }
#else
    for (int i = 0; i < num_probes; ++i, h *= uint32_t{0x9e3779b9}) {
      // 9-bit address within 512 bit cache line
      int bitpos = h >> (32 - 9);
      if ((data_at_cache_line[bitpos >> 3] & (char(1) << (bitpos & 7))) == 0) {
        return false;
      }
    }
    return true;
#endif
  }
};

// A legacy Bloom filter implementation with no locality of probes (slow).
// It uses double hashing to generate a sequence of hash values.
// Asymptotic analysis is in [Kirsch,Mitzenmacher 2006], but known to have
// subtle accuracy flaws for practical sizes [Dillinger,Manolios 2004].
//
// DO NOT REUSE
//
class LegacyNoLocalityBloomImpl {
 public:
  static inline int ChooseNumProbes(int bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    int num_probes = static_cast<int>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (num_probes < 1) {
      num_probes = 1;
    }
    if (num_probes > 30) {
      num_probes = 30;
    }
    return num_probes;
  }

  static inline void AddHash(uint32_t h, uint32_t total_bits, int num_probes,
                             char* data) {
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int i = 0; i < num_probes; i++) {
      const uint32_t bitpos = h % total_bits;
      data[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  }

  static inline bool HashMayMatch(uint32_t h, uint32_t total_bits,
                                  int num_probes, const char* data) {
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int i = 0; i < num_probes; i++) {
      const uint32_t bitpos = h % total_bits;
      if ((data[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
    return true;
  }
};

// A legacy Bloom filter implementation with probes local to a single
// cache line (fast). Because SST files might be transported between
// platforms, the cache line size is a parameter rather than hard coded.
// (But if specified as a constant parameter, an optimizing compiler
// should take advantage of that.)
//
// When ExtraRotates is false, this implementation is notably deficient in
// accuracy. Specifically, it uses double hashing with a 1/512 chance of the
// increment being zero (when cache line size is 512 bits). Thus, there's a
// 1/512 chance of probing only one index, which we'd expect to incur about
// a 1/2 * 1/512 or absolute 0.1% FP rate penalty. More detail at
// https://github.com/meta/rocksdb/issues/4120
//
// DO NOT REUSE
//
template <bool ExtraRotates>
class LegacyLocalityBloomImpl {
 private:
  static inline uint32_t GetLine(uint32_t h, uint32_t num_lines) {
    uint32_t offset_h = ExtraRotates ? (h >> 11) | (h << 21) : h;
    return offset_h % num_lines;
  }

 public:
  // NOTE: this has only been validated to enough accuracy for producing
  // reasonable warnings / user feedback, not for making functional decisions.
  static double EstimatedFpRate(size_t keys, size_t bytes, int num_probes) {
    double bits_per_key = 8.0 * bytes / keys;
    double filter_rate = BloomMath::CacheLocalFpRate(bits_per_key, num_probes,
                                                     /*cache line bits*/ 512);
    if (!ExtraRotates) {
      // Good estimate of impact of flaw in index computation.
      // Adds roughly 0.002 around 50 bits/key and 0.001 around 100 bits/key.
      // The + 22 shifts it nicely to fit for lower bits/key.
      filter_rate += 0.1 / (bits_per_key * 0.75 + 22);
    } else {
      // Not yet validated
      assert(false);
    }
    // Always uses 32-bit hash
    double fingerprint_rate = BloomMath::FingerprintFpRate(keys, 32);
    return BloomMath::IndependentProbabilitySum(filter_rate, fingerprint_rate);
  }

  static inline void AddHash(uint32_t h, uint32_t num_lines, int num_probes,
                             char* data, int log2_cache_line_bytes) {
    const int log2_cache_line_bits = log2_cache_line_bytes + 3;

    char* data_at_offset =
        data + (GetLine(h, num_lines) << log2_cache_line_bytes);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < num_probes; ++i) {
      // Mask to bit-within-cache-line address
      const uint32_t bitpos = h & ((1 << log2_cache_line_bits) - 1);
      data_at_offset[bitpos / 8] |= (1 << (bitpos % 8));
      if (ExtraRotates) {
        h = (h >> log2_cache_line_bits) | (h << (32 - log2_cache_line_bits));
      }
      h += delta;
    }
  }

  static inline void PrepareHashMayMatch(uint32_t h, uint32_t num_lines,
                                         const char* data,
                                         uint32_t /*out*/* byte_offset,
                                         int log2_cache_line_bytes) {
    uint32_t b = GetLine(h, num_lines) << log2_cache_line_bytes;
    PREFETCH(data + b, 0 /* rw */, 1 /* locality */);
    PREFETCH(data + b + ((1 << log2_cache_line_bytes) - 1), 0 /* rw */,
             1 /* locality */);
    *byte_offset = b;
  }

  static inline bool HashMayMatch(uint32_t h, uint32_t num_lines,
                                  int num_probes, const char* data,
                                  int log2_cache_line_bytes) {
    uint32_t b = GetLine(h, num_lines) << log2_cache_line_bytes;
    return HashMayMatchPrepared(h, num_probes, data + b, log2_cache_line_bytes);
  }

  static inline bool HashMayMatchPrepared(uint32_t h, int num_probes,
                                          const char* data_at_offset,
                                          int log2_cache_line_bytes) {
    const int log2_cache_line_bits = log2_cache_line_bytes + 3;

    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < num_probes; ++i) {
      // Mask to bit-within-cache-line address
      const uint32_t bitpos = h & ((1 << log2_cache_line_bits) - 1);
      if (((data_at_offset[bitpos / 8]) & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      if (ExtraRotates) {
        h = (h >> log2_cache_line_bits) | (h << (32 - log2_cache_line_bits));
      }
      h += delta;
    }
    return true;
  }
};

}  // namespace ROCKSDB_NAMESPACE
