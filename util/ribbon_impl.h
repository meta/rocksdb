//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cmath>

#include "port/port.h"  // for PREFETCH
#include "util/fastrange.h"
#include "util/ribbon_alg.h"

namespace ROCKSDB_NAMESPACE {

namespace ribbon {

// RIBBON PHSF & RIBBON Filter (Rapid Incremental Boolean Banding ON-the-fly)
//
// ribbon_impl.h: templated (parameterized) standard implementations
//
// Ribbon is a Perfect Hash Static Function construction useful as a compact
// static Bloom filter alternative. See ribbon_alg.h for core algorithms
// and core design details.
//
// TODO: more details on trade-offs and practical issues.
//
// APIs for configuring Ribbon are in ribbon_config.h

// Ribbon implementations in this file take these parameters, which must be
// provided in a class/struct type with members expressed in this concept:

// concept TypesAndSettings {
//   // See RibbonTypes and *Hasher in ribbon_alg.h, except here we have
//   // the added constraint that Hash be equivalent to either uint32_t or
//   // uint64_t.
//   typename Hash;
//   typename CoeffRow;
//   typename ResultRow;
//   typename Index;
//   typename Key;
//   static constexpr bool kFirstCoeffAlwaysOne;
//
//   // An unsigned integer type for identifying a hash seed, typically
//   // uint32_t or uint64_t. Importantly, this is the amount of data
//   // stored in memory for identifying a raw seed. See StandardHasher.
//   typename Seed;
//
//   // When true, the PHSF implements a static filter, expecting just
//   // keys as inputs for construction. When false, implements a general
//   // PHSF and expects std::pair<Key, ResultRow> as inputs for
//   // construction.
//   static constexpr bool kIsFilter;
//
//   // When true, enables a special "homogeneous" filter implementation that
//   // is slightly faster to construct, and never fails to construct though
//   // FP rate can quickly explode in cases where corresponding
//   // non-homogeneous filter would fail (or nearly fail?) to construct.
//   // For smaller filters, you can configure with ConstructionFailureChance
//   // smaller than desired FP rate to largely counteract this effect.
//   // TODO: configuring Homogeneous Ribbon for arbitrarily large filters
//   // based on data from OptimizeHomogAtScale
//   static constexpr bool kHomogeneous;
//
//   // When true, adds a tiny bit more hashing logic on queries and
//   // construction to improve utilization at the beginning and end of
//   // the structure.  Recommended when CoeffRow is only 64 bits (or
//   // less), so typical num_starts < 10k. Although this is compatible
//   // with kHomogeneous, the competing space vs. time priorities might
//   // not be useful.
//   static constexpr bool kUseSmash;
//
//   // When true, allows number of "starts" to be zero, for best support
//   // of the "no keys to add" case by always returning false for filter
//   // queries. (This is distinct from the "keys added but no space for
//   // any data" case, in which a filter always returns true.) The cost
//   // supporting this is a conditional branch (probably predictable) in
//   // queries.
//   static constexpr bool kAllowZeroStarts;
//
//   // A seedable stock hash function on Keys. All bits of Hash must
//   // be reasonably high quality. XXH functions recommended, but
//   // Murmur, City, Farm, etc. also work.
//   static Hash HashFn(const Key &, Seed raw_seed);
// };

// A bit of a hack to automatically construct the type for
// AddInput based on a constexpr bool.
template <typename Key, typename ResultRow, bool IsFilter>
struct AddInputSelector {
  // For general PHSF, not filter
  using T = std::pair<Key, ResultRow>;
};

template <typename Key, typename ResultRow>
struct AddInputSelector<Key, ResultRow, true /*IsFilter*/> {
  // For Filter
  using T = Key;
};

// To avoid writing 'typename' everywhere that we use types like 'Index'
#define IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings)                   \
  using CoeffRow = typename TypesAndSettings::CoeffRow;                      \
  using ResultRow = typename TypesAndSettings::ResultRow;                    \
  using Index = typename TypesAndSettings::Index;                            \
  using Hash = typename TypesAndSettings::Hash;                              \
  using Key = typename TypesAndSettings::Key;                                \
  using Seed = typename TypesAndSettings::Seed;                              \
                                                                             \
  /* Some more additions */                                                  \
  using QueryInput = Key;                                                    \
  using AddInput = typename ROCKSDB_NAMESPACE::ribbon::AddInputSelector<     \
      Key, ResultRow, TypesAndSettings::kIsFilter>::T;                       \
  static constexpr auto kCoeffBits =                                         \
      static_cast<Index>(sizeof(CoeffRow) * 8U);                             \
                                                                             \
  /* Export to algorithm */                                                  \
  static constexpr bool kFirstCoeffAlwaysOne =                               \
      TypesAndSettings::kFirstCoeffAlwaysOne;                                \
                                                                             \
  static_assert(sizeof(CoeffRow) + sizeof(ResultRow) + sizeof(Index) +       \
                        sizeof(Hash) + sizeof(Key) + sizeof(Seed) +          \
                        sizeof(QueryInput) + sizeof(AddInput) + kCoeffBits + \
                        kFirstCoeffAlwaysOne >                               \
                    0,                                                       \
                "avoid unused warnings, semicolon expected after macro call")

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4309)  // cast truncating constant
#pragma warning(disable : 4307)  // arithmetic constant overflow
#endif

// StandardHasher: A standard implementation of concepts RibbonTypes,
// PhsfQueryHasher, FilterQueryHasher, and BandingHasher from ribbon_alg.h.
//
// This implementation should be suitable for most all practical purposes
// as it "behaves" across a wide range of settings, with little room left
// for improvement. The key functionality in this hasher is generating
// CoeffRows, starts, and (for filters) ResultRows, which could be ~150
// bits of data or more, from a modest hash of 64 or even just 32 bits, with
// enough uniformity and bitwise independence to be close to "the best you
// can do" with available hash information in terms of FP rate and
// compactness. (64 bits recommended and sufficient for PHSF practical
// purposes.)
//
// Another feature of this hasher is a minimal "premixing" of seeds before
// they are provided to TypesAndSettings::HashFn in case that function does
// not provide sufficiently independent hashes when iterating merely
// sequentially on seeds. (This for example works around a problem with the
// preview version 0.7.2 of XXH3 used in RocksDB, a.k.a. XXPH3 or Hash64, and
// MurmurHash1 used in RocksDB, a.k.a. Hash.) We say this pre-mixing step
// translates "ordinal seeds," which we iterate sequentially to find a
// solution, into "raw seeds," with many more bits changing for each
// iteration. The translation is an easily reversible lightweight mixing,
// not suitable for hashing on its own. An advantage of this approach is that
// StandardHasher can store just the raw seed (e.g. 64 bits) for fast query
// times, while from the application perspective, we can limit to a small
// number of ordinal keys (e.g. 64 in 6 bits) for saving in metadata.
//
// The default constructor initializes the seed to ordinal seed zero, which
// is equal to raw seed zero.
//
template <class TypesAndSettings>
class StandardHasher {
 public:
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings);

  inline Hash GetHash(const Key& key) const {
    return TypesAndSettings::HashFn(key, raw_seed_);
  }
  // For when AddInput == pair<Key, ResultRow> (kIsFilter == false)
  inline Hash GetHash(const std::pair<Key, ResultRow>& bi) const {
    return GetHash(bi.first);
  }
  inline Index GetStart(Hash h, Index num_starts) const {
    // This is "critical path" code because it's required before memory
    // lookup.
    //
    // FastRange gives us a fast and effective mapping from h to the
    // appropriate range. This depends most, sometimes exclusively, on
    // upper bits of h.
    //
    if (TypesAndSettings::kUseSmash) {
      // Extra logic to "smash" entries at beginning and end, for
      // better utilization. For example, without smash and with
      // kFirstCoeffAlwaysOne, there's about a 30% chance that the
      // first slot in the banding will be unused, and worse without
      // kFirstCoeffAlwaysOne. The ending slots are even less utilized
      // without smash.
      //
      // But since this only affects roughly kCoeffBits of the slots,
      // it's usually small enough to be ignorable (less computation in
      // this function) when number of slots is roughly 10k or larger.
      //
      // The best values for these smash weights might depend on how
      // densely you're packing entries, and also kCoeffBits, but this
      // seems to work well for roughly 95% success probability.
      //
      constexpr Index kFrontSmash = kCoeffBits / 4;
      constexpr Index kBackSmash = kCoeffBits / 4;
      Index start = FastRangeGeneric(h, num_starts + kFrontSmash + kBackSmash);
      start = std::max(start, kFrontSmash);
      start -= kFrontSmash;
      start = std::min(start, num_starts - 1);
      return start;
    } else {
      // For query speed, we allow small number of initial and final
      // entries to be under-utilized.
      // NOTE: This call statically enforces that Hash is equivalent to
      // either uint32_t or uint64_t.
      return FastRangeGeneric(h, num_starts);
    }
  }
  inline CoeffRow GetCoeffRow(Hash h) const {
    // This is not so much "critical path" code because it can be done in
    // parallel (instruction level) with memory lookup.
    //
    // When we might have many entries squeezed into a single start,
    // we need reasonably good remixing for CoeffRow.
    if (TypesAndSettings::kUseSmash) {
      // Reasonably good, reasonably fast, reasonably general.
      // Probably not 1:1 but probably close enough.
      Unsigned128 a = Multiply64to128(h, kAltCoeffFactor1);
      Unsigned128 b = Multiply64to128(h, kAltCoeffFactor2);
      auto cr = static_cast<CoeffRow>(b ^ (a << 64) ^ (a >> 64));

      // Now ensure the value is non-zero
      if (kFirstCoeffAlwaysOne) {
        cr |= 1;
      } else {
        // Still have to ensure some bit is non-zero
        cr |= (cr == 0) ? 1 : 0;
      }
      return cr;
    }
    // If not kUseSmash, we ensure we're not squeezing many entries into a
    // single start, in part by ensuring num_starts > num_slots / 2. Thus,
    // here we do not need good remixing for CoeffRow, but just enough that
    // (a) every bit is reasonably independent from Start.
    // (b) every Hash-length bit subsequence of the CoeffRow has full or
    // nearly full entropy from h.
    // (c) if nontrivial bit subsequences within are correlated, it needs to
    // be more complicated than exact copy or bitwise not (at least without
    // kFirstCoeffAlwaysOne), or else there seems to be a kind of
    // correlated clustering effect.
    // (d) the CoeffRow is not zero, so that no one input on its own can
    // doom construction success. (Preferably a mix of 1's and 0's if
    // satisfying above.)

    // First, establish sufficient bitwise independence from Start, with
    // multiplication by a large random prime.
    // Note that we cast to Hash because if we use product bits beyond
    // original input size, that's going to correlate with Start (FastRange)
    // even with a (likely) different multiplier here.
    Hash a = h * kCoeffAndResultFactor;

    static_assert(
        sizeof(Hash) == sizeof(uint64_t) || sizeof(Hash) == sizeof(uint32_t),
        "Supported sizes");
    // If that's big enough, we're done. If not, we have to expand it,
    // maybe up to 4x size.
    uint64_t b;
    if (sizeof(Hash) < sizeof(uint64_t)) {
      // Almost-trivial hash expansion (OK - see above), favoring roughly
      // equal number of 1's and 0's in result
      b = (uint64_t{a} << 32) ^ (a ^ kCoeffXor32);
    } else {
      b = a;
    }
    static_assert(sizeof(CoeffRow) <= sizeof(Unsigned128), "Supported sizes");
    Unsigned128 c;
    if (sizeof(uint64_t) < sizeof(CoeffRow)) {
      // Almost-trivial hash expansion (OK - see above), favoring roughly
      // equal number of 1's and 0's in result
      c = (Unsigned128{b} << 64) ^ (b ^ kCoeffXor64);
    } else {
      c = b;
    }
    auto cr = static_cast<CoeffRow>(c);

    // Now ensure the value is non-zero
    if (kFirstCoeffAlwaysOne) {
      cr |= 1;
    } else if (sizeof(CoeffRow) == sizeof(Hash)) {
      // Still have to ensure some bit is non-zero
      cr |= (cr == 0) ? 1 : 0;
    } else {
      // (We did trivial expansion with constant xor, which ensures some
      // bits are non-zero.)
    }
    return cr;
  }
  inline ResultRow GetResultRowMask() const {
    // TODO: will be used with InterleavedSolutionStorage?
    // For now, all bits set (note: might be a small type so might need to
    // narrow after promotion)
    return static_cast<ResultRow>(~ResultRow{0});
  }
  inline ResultRow GetResultRowFromHash(Hash h) const {
    if (TypesAndSettings::kIsFilter && !TypesAndSettings::kHomogeneous) {
      // This is not so much "critical path" code because it can be done in
      // parallel (instruction level) with memory lookup.
      //
      // ResultRow bits only needs to be independent from CoeffRow bits if
      // many entries might have the same start location, where "many" is
      // comparable to number of hash bits or kCoeffBits. If !kUseSmash
      // and num_starts > kCoeffBits, it is safe and efficient to draw from
      // the same bits computed for CoeffRow, which are reasonably
      // independent from Start. (Inlining and common subexpression
      // elimination with GetCoeffRow should make this
      // a single shared multiplication in generated code when !kUseSmash.)
      Hash a = h * kCoeffAndResultFactor;

      // The bits here that are *most* independent of Start are the highest
      // order bits (as in Knuth multiplicative hash). To make those the
      // most preferred for use in the result row, we do a bswap here.
      auto rr = static_cast<ResultRow>(EndianSwapValue(a));
      return rr & GetResultRowMask();
    } else {
      // Must be zero
      return 0;
    }
  }
  // For when AddInput == Key (kIsFilter == true)
  inline ResultRow GetResultRowFromInput(const Key&) const {
    // Must be zero
    return 0;
  }
  // For when AddInput == pair<Key, ResultRow> (kIsFilter == false)
  inline ResultRow GetResultRowFromInput(
      const std::pair<Key, ResultRow>& bi) const {
    // Simple extraction
    return bi.second;
  }

  // Seed tracking APIs - see class comment
  void SetRawSeed(Seed seed) { raw_seed_ = seed; }
  Seed GetRawSeed() { return raw_seed_; }
  void SetOrdinalSeed(Seed count) {
    // A simple, reversible mixing of any size (whole bytes) up to 64 bits.
    // This allows casting the raw seed to any smaller size we use for
    // ordinal seeds without risk of duplicate raw seeds for unique ordinal
    // seeds.

    // Seed type might be smaller than numerical promotion size, but Hash
    // should be at least that size, so we use Hash as intermediate type.
    static_assert(sizeof(Seed) <= sizeof(Hash),
                  "Hash must be at least size of Seed");

    // Multiply by a large random prime (one-to-one for any prefix of bits)
    Hash tmp = count * kToRawSeedFactor;
    // Within-byte one-to-one mixing
    static_assert((kSeedMixMask & (kSeedMixMask >> kSeedMixShift)) == 0,
                  "Illegal mask+shift");
    tmp ^= (tmp & kSeedMixMask) >> kSeedMixShift;
    raw_seed_ = static_cast<Seed>(tmp);
    // dynamic verification
    assert(GetOrdinalSeed() == count);
  }
  Seed GetOrdinalSeed() {
    Hash tmp = raw_seed_;
    // Within-byte one-to-one mixing (its own inverse)
    tmp ^= (tmp & kSeedMixMask) >> kSeedMixShift;
    // Multiply by 64-bit multiplicative inverse
    static_assert(kToRawSeedFactor * kFromRawSeedFactor == Hash{1},
                  "Must be inverses");
    return static_cast<Seed>(tmp * kFromRawSeedFactor);
  }

 protected:
  // For expanding hash:
  // large random prime
  static constexpr Hash kCoeffAndResultFactor =
      static_cast<Hash>(0xc28f82822b650bedULL);
  static constexpr uint64_t kAltCoeffFactor1 = 0x876f170be4f1fcb9U;
  static constexpr uint64_t kAltCoeffFactor2 = 0xf0433a4aecda4c5fU;
  // random-ish data
  static constexpr uint32_t kCoeffXor32 = 0xa6293635U;
  static constexpr uint64_t kCoeffXor64 = 0xc367844a6e52731dU;

  // For pre-mixing seeds
  static constexpr Hash kSeedMixMask = static_cast<Hash>(0xf0f0f0f0f0f0f0f0ULL);
  static constexpr unsigned kSeedMixShift = 4U;
  static constexpr Hash kToRawSeedFactor =
      static_cast<Hash>(0xc78219a23eeadd03ULL);
  static constexpr Hash kFromRawSeedFactor =
      static_cast<Hash>(0xfe1a137d14b475abULL);

  // See class description
  Seed raw_seed_ = 0;
};

// StandardRehasher (and StandardRehasherAdapter): A variant of
// StandardHasher that uses the same type for keys as for hashes.
// This is primarily intended for building a Ribbon filter
// from existing hashes without going back to original inputs in
// order to apply a different seed. This hasher seeds a 1-to-1 mixing
// transformation to apply a seed to an existing hash. (Untested for
// hash-sized keys that are not already uniformly distributed.) This
// transformation builds on the seed pre-mixing done in StandardHasher.
//
// Testing suggests essentially no degradation of solution success rate
// vs. going back to original inputs when changing hash seeds. For example:
// Average re-seeds for solution with r=128, 1.02x overhead, and ~100k keys
// is about 1.10 for both StandardHasher and StandardRehasher.
//
// StandardRehasher is not really recommended for general PHSFs (not
// filters) because a collision in the original hash could prevent
// construction despite re-seeding the Rehasher. (Such collisions
// do not interfere with filter construction.)
//
// concept RehasherTypesAndSettings: like TypesAndSettings but
// does not require Key or HashFn.
template <class RehasherTypesAndSettings>
class StandardRehasherAdapter : public RehasherTypesAndSettings {
 public:
  using Hash = typename RehasherTypesAndSettings::Hash;
  using Key = Hash;
  using Seed = typename RehasherTypesAndSettings::Seed;

  static Hash HashFn(const Hash& input, Seed raw_seed) {
    // Note: raw_seed is already lightly pre-mixed, and this multiplication
    // by a large prime is sufficient mixing (low-to-high bits) on top of
    // that for good FastRange results, which depends primarily on highest
    // bits. (The hashed CoeffRow and ResultRow are less sensitive to
    // mixing than Start.)
    // Also note: did consider adding ^ (input >> some) before the
    // multiplication, but doesn't appear to be necessary.
    return (input ^ raw_seed) * kRehashFactor;
  }

 private:
  static constexpr Hash kRehashFactor =
      static_cast<Hash>(0x6193d459236a3a0dULL);
};

// See comment on StandardRehasherAdapter
template <class RehasherTypesAndSettings>
using StandardRehasher =
    StandardHasher<StandardRehasherAdapter<RehasherTypesAndSettings>>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

// Especially with smaller hashes (e.g. 32 bit), there can be noticeable
// false positives due to collisions in the Hash returned by GetHash.
// This function returns the expected FP rate due to those collisions,
// which can be added to the expected FP rate from the underlying data
// structure. (Note: technically, a + b is only a good approximation of
// 1-(1-a)(1-b) == a + b - a*b, if a and b are much closer to 0 than to 1.)
// The number of entries added can be a double here in case it's an
// average.
template <class Hasher, typename Numerical>
double ExpectedCollisionFpRate(const Hasher& hasher, Numerical added) {
  // Standardize on the 'double' specialization
  return ExpectedCollisionFpRate(hasher, 1.0 * added);
}
template <class Hasher>
double ExpectedCollisionFpRate(const Hasher& /*hasher*/, double added) {
  // Technically, there could be overlap among the added, but ignoring that
  // is typically close enough.
  return added / std::pow(256.0, sizeof(typename Hasher::Hash));
}

// StandardBanding: a canonical implementation of BandingStorage and
// BacktrackStorage, with convenience API for banding (solving with on-the-fly
// Gaussian elimination) with and without backtracking.
template <class TypesAndSettings>
class StandardBanding : public StandardHasher<TypesAndSettings> {
 public:
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings);

  explicit StandardBanding(Index num_slots = 0, Index backtrack_size = 0) {
    Reset(num_slots, backtrack_size);
  }

  void Reset(Index num_slots, Index backtrack_size = 0) {
    if (num_slots == 0) {
      // Unusual (TypesAndSettings::kAllowZeroStarts) or "uninitialized"
      num_starts_ = 0;
    } else {
      // Normal
      assert(num_slots >= kCoeffBits);
      if (num_slots > num_slots_allocated_) {
        coeff_rows_.reset(new CoeffRow[num_slots]());
        if (!TypesAndSettings::kHomogeneous) {
          // Note: don't strictly have to zero-init result_rows,
          // except possible information leakage, etc ;)
          result_rows_.reset(new ResultRow[num_slots]());
        }
        num_slots_allocated_ = num_slots;
      } else {
        for (Index i = 0; i < num_slots; ++i) {
          coeff_rows_[i] = 0;
          if (!TypesAndSettings::kHomogeneous) {
            // Note: don't strictly have to zero-init result_rows,
            // except possible information leakage, etc ;)
            result_rows_[i] = 0;
          }
        }
      }
      num_starts_ = num_slots - kCoeffBits + 1;
    }
    EnsureBacktrackSize(backtrack_size);
  }

  void EnsureBacktrackSize(Index backtrack_size) {
    if (backtrack_size > backtrack_size_) {
      backtrack_.reset(new Index[backtrack_size]);
      backtrack_size_ = backtrack_size;
    }
  }

  // ********************************************************************
  // From concept BandingStorage

  inline bool UsePrefetch() const {
    // A rough guesstimate of when prefetching during construction pays off.
    // TODO: verify/validate
    return num_starts_ > 1500;
  }
  inline void Prefetch(Index i) const {
    PREFETCH(&coeff_rows_[i], 1 /* rw */, 1 /* locality */);
    if (!TypesAndSettings::kHomogeneous) {
      PREFETCH(&result_rows_[i], 1 /* rw */, 1 /* locality */);
    }
  }
  inline void LoadRow(Index i, CoeffRow* cr, ResultRow* rr,
                      bool for_back_subst) const {
    *cr = coeff_rows_[i];
    if (TypesAndSettings::kHomogeneous) {
      if (for_back_subst && *cr == 0) {
        // Cheap pseudorandom data to fill unconstrained solution rows
        *rr = static_cast<ResultRow>(i * 0x9E3779B185EBCA87ULL);
      } else {
        *rr = 0;
      }
    } else {
      *rr = result_rows_[i];
    }
  }
  inline void StoreRow(Index i, CoeffRow cr, ResultRow rr) {
    coeff_rows_[i] = cr;
    if (TypesAndSettings::kHomogeneous) {
      assert(rr == 0);
    } else {
      result_rows_[i] = rr;
    }
  }
  inline Index GetNumStarts() const { return num_starts_; }

  // from concept BacktrackStorage, for when backtracking is used
  inline bool UseBacktrack() const { return true; }
  inline void BacktrackPut(Index i, Index to_save) { backtrack_[i] = to_save; }
  inline Index BacktrackGet(Index i) const { return backtrack_[i]; }

  // ********************************************************************
  // Some useful API, still somewhat low level. Here an input is
  // a Key for filters, or std::pair<Key, ResultRow> for general PHSF.

  // Adds a range of inputs to the banding, returning true if successful.
  // False means none or some may have been successfully added, so it's
  // best to Reset this banding before any further use.
  //
  // Adding can fail even before all the "slots" are completely "full".
  //
  template <typename InputIterator>
  bool AddRange(InputIterator begin, InputIterator end) {
    assert(num_starts_ > 0 || TypesAndSettings::kAllowZeroStarts);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual. Can't add any in this case.
      return begin == end;
    }
    // Normal
    return BandingAddRange(this, *this, begin, end);
  }

  // Adds a range of inputs to the banding, returning true if successful,
  // or if unsuccessful, rolls back to state before this call and returns
  // false. Caller guarantees that the number of inputs in this batch
  // does not exceed `backtrack_size` provided to Reset.
  //
  // Adding can fail even before all the "slots" are completely "full".
  //
  template <typename InputIterator>
  bool AddRangeOrRollBack(InputIterator begin, InputIterator end) {
    assert(num_starts_ > 0 || TypesAndSettings::kAllowZeroStarts);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual. Can't add any in this case.
      return begin == end;
    }
    // else Normal
    return BandingAddRange(this, this, *this, begin, end);
  }

  // Adds a single input to the banding, returning true if successful.
  // If unsuccessful, returns false and banding state is unchanged.
  //
  // Adding can fail even before all the "slots" are completely "full".
  //
  bool Add(const AddInput& input) {
    // Pointer can act as iterator
    return AddRange(&input, &input + 1);
  }

  // Return the number of "occupied" rows (with non-zero coefficients stored).
  Index GetOccupiedCount() const {
    Index count = 0;
    if (num_starts_ > 0) {
      const Index num_slots = num_starts_ + kCoeffBits - 1;
      for (Index i = 0; i < num_slots; ++i) {
        if (coeff_rows_[i] != 0) {
          ++count;
        }
      }
    }
    return count;
  }

  // Returns whether a row is "occupied" in the banding (non-zero
  // coefficients stored). (Only recommended for debug/test)
  bool IsOccupied(Index i) { return coeff_rows_[i] != 0; }

  // ********************************************************************
  // High-level API

  // Iteratively (a) resets the structure for `num_slots`, (b) attempts
  // to add the range of inputs, and (c) if unsuccessful, chooses next
  // hash seed, until either successful or unsuccessful with all the
  // allowed seeds. Returns true if successful. In that case, use
  // GetOrdinalSeed() or GetRawSeed() to get the successful seed.
  //
  // The allowed sequence of hash seeds is determined by
  // `starting_ordinal_seed,` the first ordinal seed to be attempted
  // (see StandardHasher), and `ordinal_seed_mask,` a bit mask (power of
  // two minus one) for the range of ordinal seeds to consider. The
  // max number of seeds considered will be ordinal_seed_mask + 1.
  // For filters we suggest `starting_ordinal_seed` be chosen randomly
  // or round-robin, to minimize false positive correlations between keys.
  //
  // If unsuccessful, how best to continue is going to be application
  // specific. It should be possible to choose parameters such that
  // failure is extremely unlikely, using max_seed around 32 to 64.
  // (TODO: APIs to help choose parameters) One option for fallback in
  // constructing a filter is to construct a Bloom filter instead.
  // Increasing num_slots is an option, but should not be used often
  // unless construction maximum latency is a concern (rather than
  // average running time of construction). Instead, choose parameters
  // appropriately and trust that seeds are independent. (Also,
  // increasing num_slots without changing hash seed would have a
  // significant correlation in success, rather than independence.)
  template <typename InputIterator>
  bool ResetAndFindSeedToSolve(Index num_slots, InputIterator begin,
                               InputIterator end,
                               Seed starting_ordinal_seed = 0U,
                               Seed ordinal_seed_mask = 63U) {
    // power of 2 minus 1
    assert((ordinal_seed_mask & (ordinal_seed_mask + 1)) == 0);
    // starting seed is within mask
    assert((starting_ordinal_seed & ordinal_seed_mask) ==
           starting_ordinal_seed);
    starting_ordinal_seed &= ordinal_seed_mask;  // if not debug

    Seed cur_ordinal_seed = starting_ordinal_seed;
    do {
      StandardHasher<TypesAndSettings>::SetOrdinalSeed(cur_ordinal_seed);
      Reset(num_slots);
      bool success = AddRange(begin, end);
      if (success) {
        return true;
      }
      cur_ordinal_seed = (cur_ordinal_seed + 1) & ordinal_seed_mask;
    } while (cur_ordinal_seed != starting_ordinal_seed);
    // Reached limit by circling around
    return false;
  }

  static std::size_t EstimateMemoryUsage(uint32_t num_slots) {
    std::size_t bytes_coeff_rows = num_slots * sizeof(CoeffRow);
    std::size_t bytes_result_rows = num_slots * sizeof(ResultRow);
    std::size_t bytes_backtrack = 0;
    std::size_t bytes_banding =
        bytes_coeff_rows + bytes_result_rows + bytes_backtrack;

    return bytes_banding;
  }

 protected:
  // TODO: explore combining in a struct
  std::unique_ptr<CoeffRow[]> coeff_rows_;
  std::unique_ptr<ResultRow[]> result_rows_;
  // We generally store "starts" instead of slots for speed of GetStart(),
  // as in StandardHasher.
  Index num_starts_ = 0;
  Index num_slots_allocated_ = 0;
  std::unique_ptr<Index[]> backtrack_;
  Index backtrack_size_ = 0;
};

// Implements concept SimpleSolutionStorage, mostly for demonstration
// purposes. This is "in memory" only because it does not handle byte
// ordering issues for serialization.
template <class TypesAndSettings>
class InMemSimpleSolution {
 public:
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings);

  void PrepareForNumStarts(Index num_starts) {
    if (TypesAndSettings::kAllowZeroStarts && num_starts == 0) {
      // Unusual
      num_starts_ = 0;
    } else {
      // Normal
      const Index num_slots = num_starts + kCoeffBits - 1;
      assert(num_slots >= kCoeffBits);
      if (num_slots > num_slots_allocated_) {
        // Do not need to init the memory
        solution_rows_.reset(new ResultRow[num_slots]);
        num_slots_allocated_ = num_slots;
      }
      num_starts_ = num_starts;
    }
  }

  Index GetNumStarts() const { return num_starts_; }

  ResultRow Load(Index slot_num) const { return solution_rows_[slot_num]; }

  void Store(Index slot_num, ResultRow solution_row) {
    solution_rows_[slot_num] = solution_row;
  }

  // ********************************************************************
  // High-level API

  template <typename BandingStorage>
  void BackSubstFrom(const BandingStorage& bs) {
    if (TypesAndSettings::kAllowZeroStarts && bs.GetNumStarts() == 0) {
      // Unusual
      PrepareForNumStarts(0);
    } else {
      // Normal
      SimpleBackSubst(this, bs);
    }
  }

  template <typename PhsfQueryHasher>
  ResultRow PhsfQuery(const Key& input, const PhsfQueryHasher& hasher) const {
    // assert(!TypesAndSettings::kIsFilter);  Can be useful in testing
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual
      return 0;
    } else {
      // Normal
      return SimplePhsfQuery(input, hasher, *this);
    }
  }

  template <typename FilterQueryHasher>
  bool FilterQuery(const Key& input, const FilterQueryHasher& hasher) const {
    assert(TypesAndSettings::kIsFilter);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual. Zero starts presumes no keys added -> always false
      return false;
    } else {
      // Normal, or upper_num_columns_ == 0 means "no space for data" and
      // thus will always return true.
      return SimpleFilterQuery(input, hasher, *this);
    }
  }

  double ExpectedFpRate() const {
    assert(TypesAndSettings::kIsFilter);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual, but we don't have FPs if we always return false.
      return 0.0;
    }
    // else Normal

    // Each result (solution) bit (column) cuts FP rate in half
    return std::pow(0.5, 8U * sizeof(ResultRow));
  }

  // ********************************************************************
  // Static high-level API

  // Round up to a number of slots supported by this structure. Note that
  // this needs to be must be taken into account for the banding if this
  // solution layout/storage is to be used.
  static Index RoundUpNumSlots(Index num_slots) {
    // Must be at least kCoeffBits for at least one start
    // Or if not smash, even more because hashing not equipped
    // for stacking up so many entries on a single start location
    auto min_slots = kCoeffBits * (TypesAndSettings::kUseSmash ? 1 : 2);
    return std::max(num_slots, static_cast<Index>(min_slots));
  }

 protected:
  // We generally store "starts" instead of slots for speed of GetStart(),
  // as in StandardHasher.
  Index num_starts_ = 0;
  Index num_slots_allocated_ = 0;
  std::unique_ptr<ResultRow[]> solution_rows_;
};

// Implements concept InterleavedSolutionStorage always using little-endian
// byte order, so easy for serialization/deserialization. This implementation
// fully supports fractional bits per key, where any number of segments
// (number of bytes multiple of sizeof(CoeffRow)) can be used with any number
// of slots that is a multiple of kCoeffBits.
//
// The structure is passed an externally allocated/de-allocated byte buffer
// that is optionally pre-populated (from storage) for answering queries,
// or can be populated by BackSubstFrom.
//
template <class TypesAndSettings>
class SerializableInterleavedSolution {
 public:
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings);

  // Does not take ownership of `data` but uses it (up to `data_len` bytes)
  // throughout lifetime
  SerializableInterleavedSolution(char* data, size_t data_len)
      : data_(data), data_len_(data_len) {}

  void PrepareForNumStarts(Index num_starts) {
    assert(num_starts == 0 || (num_starts % kCoeffBits == 1));
    num_starts_ = num_starts;

    InternalConfigure();
  }

  Index GetNumStarts() const { return num_starts_; }

  Index GetNumBlocks() const {
    const Index num_slots = num_starts_ + kCoeffBits - 1;
    return num_slots / kCoeffBits;
  }

  Index GetUpperNumColumns() const { return upper_num_columns_; }

  Index GetUpperStartBlock() const { return upper_start_block_; }

  Index GetNumSegments() const {
    return static_cast<Index>(data_len_ / sizeof(CoeffRow));
  }

  CoeffRow LoadSegment(Index segment_num) const {
    assert(data_ != nullptr);  // suppress clang analyzer report
    return DecodeFixedGeneric<CoeffRow>(data_ + segment_num * sizeof(CoeffRow));
  }
  void StoreSegment(Index segment_num, CoeffRow val) {
    assert(data_ != nullptr);  // suppress clang analyzer report
    EncodeFixedGeneric(data_ + segment_num * sizeof(CoeffRow), val);
  }
  void PrefetchSegmentRange(Index begin_segment_num,
                            Index end_segment_num) const {
    if (end_segment_num == begin_segment_num) {
      // Nothing to do
      return;
    }
    char* cur = data_ + begin_segment_num * sizeof(CoeffRow);
    char* last = data_ + (end_segment_num - 1) * sizeof(CoeffRow);
    while (cur < last) {
      PREFETCH(cur, 0 /* rw */, 1 /* locality */);
      cur += CACHE_LINE_SIZE;
    }
    PREFETCH(last, 0 /* rw */, 1 /* locality */);
  }

  // ********************************************************************
  // High-level API

  void ConfigureForNumBlocks(Index num_blocks) {
    if (num_blocks == 0) {
      PrepareForNumStarts(0);
    } else {
      PrepareForNumStarts(num_blocks * kCoeffBits - kCoeffBits + 1);
    }
  }

  void ConfigureForNumSlots(Index num_slots) {
    assert(num_slots % kCoeffBits == 0);
    ConfigureForNumBlocks(num_slots / kCoeffBits);
  }

  template <typename BandingStorage>
  void BackSubstFrom(const BandingStorage& bs) {
    if (TypesAndSettings::kAllowZeroStarts && bs.GetNumStarts() == 0) {
      // Unusual
      PrepareForNumStarts(0);
    } else {
      // Normal
      InterleavedBackSubst(this, bs);
    }
  }

  template <typename PhsfQueryHasher>
  ResultRow PhsfQuery(const Key& input, const PhsfQueryHasher& hasher) const {
    // assert(!TypesAndSettings::kIsFilter);  Can be useful in testing
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual
      return 0;
    } else {
      // Normal
      // NOTE: not using a struct to encourage compiler optimization
      Hash hash;
      Index segment_num;
      Index num_columns;
      Index start_bit;
      InterleavedPrepareQuery(input, hasher, *this, &hash, &segment_num,
                              &num_columns, &start_bit);
      return InterleavedPhsfQuery(hash, segment_num, num_columns, start_bit,
                                  hasher, *this);
    }
  }

  template <typename FilterQueryHasher>
  bool FilterQuery(const Key& input, const FilterQueryHasher& hasher) const {
    assert(TypesAndSettings::kIsFilter);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual. Zero starts presumes no keys added -> always false
      return false;
    } else {
      // Normal, or upper_num_columns_ == 0 means "no space for data" and
      // thus will always return true.
      // NOTE: not using a struct to encourage compiler optimization
      Hash hash;
      Index segment_num;
      Index num_columns;
      Index start_bit;
      InterleavedPrepareQuery(input, hasher, *this, &hash, &segment_num,
                              &num_columns, &start_bit);
      return InterleavedFilterQuery(hash, segment_num, num_columns, start_bit,
                                    hasher, *this);
    }
  }

  double ExpectedFpRate() const {
    assert(TypesAndSettings::kIsFilter);
    if (TypesAndSettings::kAllowZeroStarts && num_starts_ == 0) {
      // Unusual. Zero starts presumes no keys added -> always false
      return 0.0;
    }
    // else Normal

    // Note: Ignoring smash setting; still close enough in that case
    double lower_portion =
        (upper_start_block_ * 1.0 * kCoeffBits) / num_starts_;

    // Each result (solution) bit (column) cuts FP rate in half. Weight that
    // for upper and lower number of bits (columns).
    return lower_portion * std::pow(0.5, upper_num_columns_ - 1) +
           (1.0 - lower_portion) * std::pow(0.5, upper_num_columns_);
  }

  // ********************************************************************
  // Static high-level API

  // Round up to a number of slots supported by this structure. Note that
  // this needs to be must be taken into account for the banding if this
  // solution layout/storage is to be used.
  static Index RoundUpNumSlots(Index num_slots) {
    // Must be multiple of kCoeffBits
    Index corrected = (num_slots + kCoeffBits - 1) / kCoeffBits * kCoeffBits;

    // Do not use num_starts==1 unless kUseSmash, because the hashing
    // might not be equipped for stacking up so many entries on a
    // single start location.
    if (!TypesAndSettings::kUseSmash && corrected == kCoeffBits) {
      corrected += kCoeffBits;
    }
    return corrected;
  }

  // Round down to a number of slots supported by this structure. Note that
  // this needs to be must be taken into account for the banding if this
  // solution layout/storage is to be used.
  static Index RoundDownNumSlots(Index num_slots) {
    // Must be multiple of kCoeffBits
    Index corrected = num_slots / kCoeffBits * kCoeffBits;

    // Do not use num_starts==1 unless kUseSmash, because the hashing
    // might not be equipped for stacking up so many entries on a
    // single start location.
    if (!TypesAndSettings::kUseSmash && corrected == kCoeffBits) {
      corrected = 0;
    }
    return corrected;
  }

  // Compute the number of bytes for a given number of slots and desired
  // FP rate. Since desired FP rate might not be exactly achievable,
  // rounding_bias32==0 means to always round toward lower FP rate
  // than desired (more bytes); rounding_bias32==max uint32_t means always
  // round toward higher FP rate than desired (fewer bytes); other values
  // act as a proportional threshold or bias between the two.
  static size_t GetBytesForFpRate(Index num_slots, double desired_fp_rate,
                                  uint32_t rounding_bias32) {
    return InternalGetBytesForFpRate(num_slots, desired_fp_rate,
                                     1.0 / desired_fp_rate, rounding_bias32);
  }

  // The same, but specifying desired accuracy as 1.0 / FP rate, or
  // one_in_fp_rate. E.g. desired_one_in_fp_rate=100 means 1% FP rate.
  static size_t GetBytesForOneInFpRate(Index num_slots,
                                       double desired_one_in_fp_rate,
                                       uint32_t rounding_bias32) {
    return InternalGetBytesForFpRate(num_slots, 1.0 / desired_one_in_fp_rate,
                                     desired_one_in_fp_rate, rounding_bias32);
  }

 protected:
  static size_t InternalGetBytesForFpRate(Index num_slots,
                                          double desired_fp_rate,
                                          double desired_one_in_fp_rate,
                                          uint32_t rounding_bias32) {
    assert(TypesAndSettings::kIsFilter);
    if (TypesAndSettings::kAllowZeroStarts) {
      if (num_slots == 0) {
        // Unusual. Zero starts presumes no keys added -> always false (no FPs)
        return 0U;
      }
    } else {
      assert(num_slots > 0);
    }
    // Must be rounded up already.
    assert(RoundUpNumSlots(num_slots) == num_slots);

    if (desired_one_in_fp_rate > 1.0 && desired_fp_rate < 1.0) {
      // Typical: less than 100% FP rate
      if (desired_one_in_fp_rate <= static_cast<ResultRow>(-1)) {
        // Typical: Less than maximum result row entropy
        ResultRow rounded = static_cast<ResultRow>(desired_one_in_fp_rate);
        int lower_columns = FloorLog2(rounded);
        double lower_columns_fp_rate = std::pow(2.0, -lower_columns);
        double upper_columns_fp_rate = std::pow(2.0, -(lower_columns + 1));
        // Floating point don't let me down!
        assert(lower_columns_fp_rate >= desired_fp_rate);
        assert(upper_columns_fp_rate <= desired_fp_rate);

        double lower_portion = (desired_fp_rate - upper_columns_fp_rate) /
                               (lower_columns_fp_rate - upper_columns_fp_rate);
        // Floating point don't let me down!
        assert(lower_portion >= 0.0);
        assert(lower_portion <= 1.0);

        double rounding_bias = (rounding_bias32 + 0.5) / double{0x100000000};
        assert(rounding_bias > 0.0);
        assert(rounding_bias < 1.0);

        // Note: Ignoring smash setting; still close enough in that case
        Index num_starts = num_slots - kCoeffBits + 1;
        // Lower upper_start_block means lower FP rate (higher accuracy)
        Index upper_start_block = static_cast<Index>(
            (lower_portion * num_starts + rounding_bias) / kCoeffBits);
        Index num_blocks = num_slots / kCoeffBits;
        assert(upper_start_block < num_blocks);

        // Start by assuming all blocks use lower number of columns
        Index num_segments = num_blocks * static_cast<Index>(lower_columns);
        // Correct by 1 each for blocks using upper number of columns
        num_segments += (num_blocks - upper_start_block);
        // Total bytes
        return num_segments * sizeof(CoeffRow);
      } else {
        // one_in_fp_rate too big, thus requested FP rate is smaller than
        // supported. Use max number of columns for minimum supported FP rate.
        return num_slots * sizeof(ResultRow);
      }
    } else {
      // Effectively asking for 100% FP rate, or NaN etc.
      if (TypesAndSettings::kAllowZeroStarts) {
        // Zero segments
        return 0U;
      } else {
        // One segment (minimum size, maximizing FP rate)
        return sizeof(CoeffRow);
      }
    }
  }

  void InternalConfigure() {
    const Index num_blocks = GetNumBlocks();
    Index num_segments = GetNumSegments();

    if (num_blocks == 0) {
      // Exceptional
      upper_num_columns_ = 0;
      upper_start_block_ = 0;
    } else {
      // Normal
      upper_num_columns_ =
          (num_segments + /*round up*/ num_blocks - 1) / num_blocks;
      upper_start_block_ = upper_num_columns_ * num_blocks - num_segments;
      // Unless that's more columns than supported by ResultRow data type
      if (upper_num_columns_ > 8U * sizeof(ResultRow)) {
        // Use maximum columns (there will be space unused)
        upper_num_columns_ = static_cast<Index>(8U * sizeof(ResultRow));
        upper_start_block_ = 0;
        num_segments = num_blocks * upper_num_columns_;
      }
    }
    // Update data_len_ for correct rounding and/or unused space
    // NOTE: unused space stays gone if we PrepareForNumStarts again.
    // We are prioritizing minimizing the number of fields over making
    // the "unusued space" feature work well.
    data_len_ = num_segments * sizeof(CoeffRow);
  }

  char* const data_;
  size_t data_len_;
  Index num_starts_ = 0;
  Index upper_num_columns_ = 0;
  Index upper_start_block_ = 0;
};

}  // namespace ribbon

}  // namespace ROCKSDB_NAMESPACE

// For convenience working with templates
#define IMPORT_RIBBON_IMPL_TYPES(TypesAndSettings)                            \
  using Hasher = ROCKSDB_NAMESPACE::ribbon::StandardHasher<TypesAndSettings>; \
  using Banding =                                                             \
      ROCKSDB_NAMESPACE::ribbon::StandardBanding<TypesAndSettings>;           \
  using SimpleSoln =                                                          \
      ROCKSDB_NAMESPACE::ribbon::InMemSimpleSolution<TypesAndSettings>;       \
  using InterleavedSoln =                                                     \
      ROCKSDB_NAMESPACE::ribbon::SerializableInterleavedSolution<             \
          TypesAndSettings>;                                                  \
  static_assert(sizeof(Hasher) + sizeof(Banding) + sizeof(SimpleSoln) +       \
                        sizeof(InterleavedSoln) >                             \
                    0,                                                        \
                "avoid unused warnings, semicolon expected after macro call")
