//  Copyright (c) Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <assert.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <variant>

#include "rocksdb/comparator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

namespace detail {
int CountTrailingZeroBitsForSmallEnumSet(uint64_t);
int BitsSetToOneForSmallEnumSet(uint64_t);
}  // namespace detail

// Represents a set of values of some enum type with a small number of possible
// enumerators. Assumes that any combination of enumerators with values 0
// through MAX_ENUMERATOR (inclusive) might be part of the set. NOTE: would like
// to use std::bitset, but it doesn't support constexpr (in C++17) operations
// and doesn't support efficient iteration over sparse "set to true" entries.
template <typename ENUM_TYPE, ENUM_TYPE MAX_ENUMERATOR>
class SmallEnumSet {
 private:
  static constexpr int kMaxValue = static_cast<int>(MAX_ENUMERATOR);
  static_assert(kMaxValue >= 0);
  static_assert(kMaxValue < 1024, "MAX_ENUMERATOR is suspiciously large");
  using PieceT = uint64_t;
  static constexpr int kPieceBits = 64;
  static constexpr int kPieceMask = 63;
  static constexpr int kPieceShift = 6;
  static constexpr int kPieceCount = kMaxValue / kPieceBits + 1;
  using StateT = std::array<PieceT, kPieceCount>;
  static constexpr int kStateBits = kPieceBits * kPieceCount;
  static_assert(kStateBits == sizeof(StateT) * 8);
  static_assert(kMaxValue <= kStateBits - 1);

 public:
  // construct / create empty set
  SmallEnumSet() : state_{} {}

  template <class... TRest>
  // NOLINTNEXTLINE(google-explicit-constructor)
  /*implicit*/ constexpr SmallEnumSet(const ENUM_TYPE e, TRest... rest) {
    *this = SmallEnumSet(rest...).With(e);
  }

  // Return the set that includes all valid values, assuming the enum
  // is "dense" (includes all values converting to 0 through kMaxValue)
  static constexpr SmallEnumSet All() {
    StateT tmp;
    for (int i = 0; i < kPieceCount - 1; ++i) {
      tmp[i] = ~PieceT{0};
    }
    if constexpr (((kMaxValue + 1) & kPieceMask) != 0) {
      tmp[kPieceCount - 1] = (PieceT{1} << ((kMaxValue + 1) & kPieceMask)) - 1;
    } else {
      tmp[kPieceCount - 1] = ~PieceT{0};
    }
    return SmallEnumSet(RawStateMarker(), tmp);
  }

  // equality
  bool operator==(const SmallEnumSet& that) const {
    return this->state_ == that.state_;
  }
  bool operator!=(const SmallEnumSet& that) const { return !(*this == that); }

  // query

  // Return true if the input enum is contained in the "Set".
  bool Contains(const ENUM_TYPE e) const {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    return GetPiece(value) & (PieceT{1} << (value & kPieceMask));
  }

  bool empty() const {
    for (int i = 0; i < kPieceCount; ++i) {
      if (state_[i] != 0) {
        return false;
      }
    }
    return true;
  }

  // iterator
  class const_iterator {
   public:
    // copy
    const_iterator(const const_iterator& that) = default;
    const_iterator& operator=(const const_iterator& that) = default;

    // move
    const_iterator(const_iterator&& that) noexcept = default;
    const_iterator& operator=(const_iterator&& that) noexcept = default;

    ~const_iterator() = default;

    // equality
    bool operator==(const const_iterator& that) const {
      assert(set_ == that.set_);
      return this->pos_ == that.pos_;
    }

    bool operator!=(const const_iterator& that) const {
      return !(*this == that);
    }

    // ++iterator
    const_iterator& operator++() {
      if (pos_ < kMaxValue) {
        pos_ = set_->SkipUnset(pos_ + 1);
      } else {
        pos_ = kMaxValue + 1;
      }
      return *this;
    }

    // iterator++
    const_iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }

    ENUM_TYPE operator*() const {
      assert(pos_ <= kMaxValue);
      return static_cast<ENUM_TYPE>(pos_);
    }

   private:
    friend class SmallEnumSet;
    const_iterator(const SmallEnumSet* set, int pos) : set_(set), pos_(pos) {}
    const SmallEnumSet* set_;
    int pos_;
  };

  const_iterator begin() const { return const_iterator(this, SkipUnset(0)); }

  const_iterator end() const { return const_iterator(this, kMaxValue + 1); }

  size_t count() const {
    size_t rv = 0;
    for (int i = 0; i < kPieceCount; ++i) {
      rv += static_cast<size_t>(detail::BitsSetToOneForSmallEnumSet(state_[i]));
    }
    return rv;
  }

  // mutable ops

  // Modifies the set (if needed) to include the given value. Returns true
  // iff the set was modified.
  bool Add(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    PieceT& piece_ref = RefPiece(value);
    PieceT old_piece = piece_ref;
    piece_ref |= (PieceT{1} << (value & kPieceMask));
    return old_piece != piece_ref;
  }

  // Modifies the set (if needed) not to include the given value. Returns true
  // iff the set was modified.
  bool Remove(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    PieceT& piece_ref = RefPiece(value);
    PieceT old_piece = piece_ref;
    piece_ref &= ~(PieceT{1} << (value & kPieceMask));
    return old_piece != piece_ref;
  }

  // applicative ops

  // Return a new set based on this one with the additional value(s) inserted
  constexpr SmallEnumSet With(const ENUM_TYPE e) const {
    assert(static_cast<int>(e) >= 0 && static_cast<int>(e) <= kMaxValue);
    SmallEnumSet rv(*this);
    rv.Add(e);
    return rv;
  }
  template <class... TRest>
  constexpr SmallEnumSet With(const ENUM_TYPE e1, const ENUM_TYPE e2,
                              TRest... rest) const {
    return With(e1).With(e2, rest...);
  }

  // Return a new set based on this one excluding the given value(s)
  constexpr SmallEnumSet Without(const ENUM_TYPE e) const {
    assert(static_cast<int>(e) >= 0 && static_cast<int>(e) <= kMaxValue);
    SmallEnumSet rv(*this);
    rv.Remove(e);
    return rv;
  }
  template <class... TRest>
  constexpr SmallEnumSet Without(const ENUM_TYPE e1, const ENUM_TYPE e2,
                                 TRest... rest) const {
    return Without(e1).Without(e2, rest...);
  }

 private:
  int SkipUnset(int pos) const {
    while (pos <= kMaxValue) {
      PieceT remainder = GetPiece(pos) >> (pos & kPieceMask);
      if (remainder != 0) {
        return pos + detail::CountTrailingZeroBitsForSmallEnumSet(remainder);
      }
      pos = (pos + kPieceBits) & ~kPieceMask;
    }
    return kMaxValue + 1;
  }
  struct RawStateMarker {};
  explicit SmallEnumSet(RawStateMarker, StateT state) : state_(state) {}
  PieceT GetPiece(int pos) const {
    if constexpr (kPieceCount == 1) {
      return state_[0];
    } else {
      return state_[pos >> kPieceShift];
    }
  }
  PieceT& RefPiece(int pos) {
    if constexpr (kPieceCount == 1) {
      return state_[0];
    } else {
      return state_[pos >> kPieceShift];
    }
  }

  StateT state_;
};

// A smart pointer that tracks an object and an owner, using a statically
// determined function on those to reclaim the object, if both object and owner
// are non-null
template <typename T, class Owner, auto Fn>
class ManagedPtr {
 public:
  ManagedPtr() = default;
  ManagedPtr(T* ptr, Owner* owner) : ptr_(ptr), owner_(owner) {}
  ~ManagedPtr() { Free(); }
  // No copies
  ManagedPtr(const ManagedPtr&) = delete;
  ManagedPtr& operator=(const ManagedPtr&) = delete;
  // Moves
  ManagedPtr(ManagedPtr&& other) noexcept {
    ptr_ = other.ptr_;
    owner_ = other.owner_;
    other.ptr_ = nullptr;
    other.owner_ = nullptr;
  }
  ManagedPtr& operator=(ManagedPtr&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Free();
    ptr_ = other.ptr_;
    owner_ = other.owner_;
    other.ptr_ = nullptr;
    other.owner_ = nullptr;
    return *this;
  }

  T* get() const { return ptr_; }
  T* operator->() const { return ptr_; }
  T& operator*() const { return *ptr_; }
  operator bool() const { return ptr_ != nullptr; }

  Owner* owner() const { return owner_; }

 private:
  T* ptr_ = nullptr;
  Owner* owner_ = nullptr;

  void Free() {
    if (ptr_ && owner_) {
      if constexpr (std::is_member_function_pointer_v<decltype(Fn)>) {
        (owner_->*Fn)(ptr_);
      } else {
        Fn(owner_, ptr_);
      }
    }
  }
};

template <typename T, typename comp>
class Interval;

// The Interval Class is a generic class for holding a range, for example [2,
// 4]. It can be used within the IntervalSet class, which is able to keep an
// ordered, non-intersecting set of intervals within it.  Intervals can have
// open-ended end points, (i.e., to infinity) for example [2,).
template <typename T, typename comp = std::less<T>>
class Interval {
 public:
  enum class End { INF };
  struct CompareVariant {
    comp comparator;
    bool operator()(const std::variant<T, End>& a,
                    const std::variant<T, End>& b) const {
      if (std::holds_alternative<T>(a) && std::holds_alternative<T>(b)) {
        return comparator(std::get<T>(a), std::get<T>(b));
      }
      if (std::holds_alternative<End>(a) && std::holds_alternative<End>(b)) {
        return false;
      }
      if (std::holds_alternative<T>(a) && std::holds_alternative<End>(b)) {
        return false;
      }
      return true;  // std::holds_alternative<End>(a) &&
                    // std::holds_alternative<T>(b)
    }
  };

  /* implicit */ Interval(const T& start, const T& end)
      : start_(start), end_(end) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  /* implicit */ Interval(const T& start) : start_(start), end_(End::INF) {}

  // Add constructor that takes a pair
  // NOLINTNEXTLINE(google-explicit-constructor)
  /* implicit */ Interval(const std::pair<T, T>& p)
      : start_(p.first), end_(p.second) {}

  T& start() { return start_; }

  const T& start() const { return start_; }

  bool has_end() const { return std::holds_alternative<T>(end_); }

  T& end() { return std::get<T>(end_); }

  const T& end() const { return std::get<T>(end_); }

  // Support comparison with std::pair
  bool operator==(const std::pair<T, T>& p) const {
    return start_ == p.first && has_end() && end() == p.second;
  }

  // Support comparison with another Interval
  bool operator==(const Interval& other) const {
    if (start_ != other.start_) {
      return false;
    }

    // Both have infinite end
    if (!has_end() && !other.has_end()) {
      return true;
    }

    // One has infinite end, the other doesn't
    if (has_end() != other.has_end()) {
      return false;
    }

    // Both have finite end
    return end() == other.end();
  }

  // Support comparison with another Interval
  bool operator<(const Interval& other) const {
    return comparator(start_, other.start_);
  }

  bool Compare(const Interval& other) const {
    return comparator(start_, other.start_);
  }

 private:
  T start_;
  std::variant<T, End> end_;
  comp comparator;
};

// Specialized version of Interval for Slice
template <>
class Interval<Slice, Comparator> {
 public:
  enum class End { INF };

  // Constructors that take a Comparator
  /* implicit */ Interval(const Comparator* c, const Slice& start,
                          const Slice& end)
      : start_(start), end_(end), comparator_(c) {}

  /* implicit */ Interval(const Comparator* c, const Slice& start)
      : start_(start), end_(End::INF), comparator_(c) {}

  // Constructor that takes a pair
  /* implicit */ Interval(const Comparator* c, const std::pair<Slice, Slice>& p)
      : start_(p.first), end_(p.second), comparator_(c) {}

  Slice& start() { return start_; }

  const Slice& start() const { return start_; }

  bool has_end() const { return std::holds_alternative<Slice>(end_); }

  Slice& end() { return std::get<Slice>(end_); }

  const Slice& end() const { return std::get<Slice>(end_); }

  // Support comparison with std::pair
  bool operator==(const std::pair<Slice, Slice>& p) const {
    return start_ == p.first && has_end() && end() == p.second;
  }

  // Support comparison with another Interval
  bool operator==(const Interval& other) const {
    if (comparator_->Compare(start_, other.start_) != 0) {
      return false;
    }

    // Both have infinite end
    if (!has_end() && !other.has_end()) {
      return true;
    }

    // One has infinite end, the other doesn't
    if (has_end() != other.has_end()) {
      return false;
    }

    // Both have finite end
    return comparator_->Compare(end(), other.end()) == 0;
  }

  // Support comparison with another Interval
  bool operator<(const Interval& other) const {
    return comparator_->Compare(start_, other.start_) < 0;
  }

  bool Compare(const Interval& other) const {
    return comparator_->Compare(start_, other.start_) < 0;
  }

  const Comparator* GetComparator() const { return comparator_; }

 private:
  Slice start_;
  std::variant<Slice, End> end_;
  const Comparator* comparator_;

  std::unordered_map<std::string, std::string> property_bag;
};

template <typename T, typename Compare = std::less<T>>
struct CompareInterval {
  bool operator()(const Interval<T, Compare>& a,
                  const Interval<T, Compare>& b) const {
    return a.Compare(b);
  }
};

// IntervalSet will be used to represent a set of intervals (including unbounded
// ones). The intervals are unique and disjoint. Intervals that are inserted
// will merge with any range they intersect with.
template <typename T, typename Compare = typename Interval<T>::CompareVariant>
class IntervalSet {
 public:
  IntervalSet(Compare c = Compare()) : comp_(c) {}

  void insert(Interval<T>&& i) { insertImpl(i); }

  void insert(const T& start, const T& end) {
    insertImpl(Interval<T>(start, end));
  }

  void insert(const T& start) { insertImpl(Interval<T>(start)); }

  bool empty() const { return intervals_.empty(); }
  void clear() { intervals_.clear(); }

  auto begin() { return intervals_.begin(); }
  auto end() { return intervals_.end(); }

  auto cbegin() const { return intervals_.cbegin(); }
  auto cend() const { return intervals_.cend(); }

  size_t size() const { return intervals_.size(); }

 private:
  void insertImpl(const Interval<T>& i) {
    // Skip empty intervals
    if (i.has_end() && !comp_(i.start(), i.end()) &&
        !comp_(i.end(), i.start())) {
      return;
    }

    // First, check if there's any infinite interval that would contain this one
    for (auto it = intervals_.begin(); it != intervals_.end(); ++it) {
      if (!it->has_end() && !comp_(i.start(), it->start())) {
        // This interval starts at or after an infinite interval
        return;
      }
    }

    // Find the position where the interval should be inserted
    auto it = intervals_.begin();
    while (it != intervals_.end() && comp_(it->start(), i.start())) {
      ++it;
    }

    // Check if we need to consider the previous interval
    if (it != intervals_.begin()) {
      --it;
      if (it->has_end() && comp_(it->end(), i.start())) {
        ++it;
      }
    }

    T new_start = i.start();
    T new_end;
    bool inf_end = false;
    if (i.has_end()) {
      new_end = i.end();
    } else {
      // For infinite end intervals, we need to merge all intervals that start
      // after new_start
      std::vector<decltype(it)> to_erase;
      while (it != intervals_.end()) {
        new_start = comp_(it->start(), new_start) ? it->start() : new_start;
        to_erase.push_back(it++);
      }

      for (auto& eit : to_erase) {
        intervals_.erase(eit);
      }

      // Insert the new interval with infinite end
      intervals_.insert(Interval<T>(new_start));
      return;
    }

    // For finite end intervals, proceed as before
    std::vector<decltype(it)> to_erase;
    while (it != intervals_.end() && !comp_(new_end, it->start())) {
      if (it->has_end() && comp_(it->end(), new_start)) {
        ++it;
        continue;
      }
      new_start = comp_(it->start(), new_start) ? it->start() : new_start;
      if (it->has_end()) {
        new_end = comp_(new_end, it->end()) ? it->end() : new_end;
      } else {
        // If we encounter an interval with infinite end, our new interval also
        // becomes infinite
        inf_end = true;
        break;
      }
      to_erase.push_back(it++);
    }

    // Check for any infinite intervals that start after this one
    auto check_it = it;
    while (check_it != intervals_.end()) {
      if (!check_it->has_end()) {
        inf_end = true;
        to_erase.push_back(check_it);
      }
      ++check_it;
    }

    for (auto& eit : to_erase) {
      intervals_.erase(eit);
    }

    if (inf_end) {
      intervals_.insert(Interval<T>(new_start));
    } else {
      intervals_.insert(Interval<T>(new_start, new_end));
    }
  }

  std::set<Interval<T>, CompareInterval<T>> intervals_;
  Compare comp_;
};

// Specialization of IntervalSet for Slices.
// Slice based intervals can have properties attached to them. This is used to
// push down properties in the MultiScan API.  We accept two modes with
// IntervalSet, fail_on_intersect, which imposes a restriction that inserted
// ranges will be disjoint, this is needed when using properties. Insert will
// fail if a range is found to not be disjoint. When fail_on_instersect is
// false, the ranges will be merged.
template <>
class IntervalSet<Slice, Comparator> {
 public:
  explicit IntervalSet(const Comparator* c, bool fail_on_intersect = false)
      : comp_(c), prop_(fail_on_intersect) {}

  // Insert returns true if the interval was inserted. False indicates that the
  // interval was not inserted, this could be do to an empty range OR that the
  // IntervalSet is in with_properties mode and the interval overlaps with an
  // existing interval.
  bool insert(const Slice& start, const Slice& end) {
    return insertImpl(Interval<Slice, Comparator>(comp_, start, end));
  }

  // Insert returns true if the interval was inserted. False indicates that the
  // interval was not inserted, this could be do to an empty range OR that the
  // IntervalSet is in with_properties mode and the interval overlaps with an
  // existing interval.
  bool insert(const Slice& start) {
    // Create an interval with infinite end
    Interval<Slice, Comparator> interval(comp_, start);
    return insertImpl(interval);
  }

  bool insert(Interval<Slice, Comparator>&& i) { return insertImpl(i); }

  bool empty() const { return intervals_.empty(); }
  void clear() { intervals_.clear(); }

  auto begin() { return intervals_.begin(); }
  auto end() { return intervals_.end(); }

  auto cbegin() const { return intervals_.cbegin(); }
  auto cend() const { return intervals_.cend(); }

  size_t size() const { return intervals_.size(); }

 private:
  // Custom comparator for finding intervals in the vector
  struct IntervalComparator {
    explicit IntervalComparator(const Comparator* comp) : comp_(comp) {}

    bool operator()(const Interval<Slice, Comparator>& a,
                    const Interval<Slice, Comparator>& b) const {
      return comp_->Compare(a.start(), b.start()) < 0;
    }

    const Comparator* comp_;
  };

  typename std::vector<Interval<Slice, Comparator>>::iterator findPosition(
      const Interval<Slice, Comparator>& interval) {
    // Find the position where the new interval should be inserted
    for (auto it = intervals_.begin(); it != intervals_.end(); ++it) {
      if (comp_->Compare(it->start(), interval.start()) >= 0) {
        return it;
      }
    }
    return intervals_.end();
  }

  bool insertImpl(const Interval<Slice, Comparator>& i) {
    // Skip empty intervals
    if (i.has_end() && comp_->Compare(i.start(), i.end()) >= 0) {
      return false;
    }

    // Find the position where this interval would be inserted
    // This also checks if the interval is completely contained within an
    // existing one
    auto it = findPosition(i);

    // Check if we need to merge with previous interval
    if (it != intervals_.begin()) {
      auto prev = it - 1;
      if (prev->has_end() && comp_->Compare(prev->end(), i.start()) < 0) {
        // No overlap with previous interval
      } else {
        // There is overlap, adjust iterator to include previous interval
        if (prop_) {
          return false;
        }
        it = prev;
      }
    }

    Slice new_start = i.start();
    Slice new_end;
    bool inf_end = false;

    if (i.has_end()) {
      new_end = i.end();
    } else {
      // For infinite end intervals, we need to merge all intervals that start
      // after new_start
      auto erase_start = it;
      while (it != intervals_.end()) {
        if (comp_->Compare(it->start(), new_start) < 0) {
          if (prop_) {
            return false;
          }
          new_start = it->start();
        }
        ++it;
      }

      // Erase all intervals from erase_start to end
      if (erase_start != intervals_.end()) {
        if (prop_) {
          return false;
        }
        intervals_.erase(erase_start, intervals_.end());
      }

      // Insert the new interval with infinite end
      Interval<Slice, Comparator> new_interval(comp_, new_start);
      auto pos = findPosition(new_interval);
      intervals_.insert(pos, new_interval);
      return true;
    }

    // For finite end intervals, find all overlapping intervals
    auto erase_start = it;
    auto erase_end = it;

    while (it != intervals_.end() &&
           comp_->Compare(new_end, it->start()) >= 0) {
      if (it->has_end() && comp_->Compare(it->end(), new_start) < 0) {
        // No overlap
        ++it;
        erase_end = it;
        continue;
      }

      if (comp_->Compare(it->start(), new_start) < 0) {
        new_start = it->start();
      }

      if (it->has_end()) {
        if (comp_->Compare(new_end, it->end()) < 0) {
          new_end = it->end();
        }
      } else {
        // If we encounter an interval with infinite end, our new interval also
        // becomes infinite
        inf_end = true;
        erase_end = intervals_.end();
        break;
      }

      ++it;
      erase_end = it;
    }

    // Check for any infinite intervals that start after this one
    while (it != intervals_.end()) {
      if (!it->has_end()) {
        inf_end = true;
        erase_end = intervals_.end();
        break;
      }
      ++it;
    }

    // Erase all merged intervals
    if (erase_start != erase_end) {
      intervals_.erase(erase_start, erase_end);
    }

    // Insert the new merged interval
    Interval<Slice, Comparator> new_interval =
        inf_end ? Interval<Slice, Comparator>(comp_, new_start)
                : Interval<Slice, Comparator>(comp_, new_start, new_end);

    auto pos = findPosition(new_interval);
    intervals_.insert(pos, new_interval);
    return true;
  }

  const Comparator* comp_;
  std::vector<Interval<Slice, Comparator>> intervals_;
  bool prop_;
};

}  // namespace ROCKSDB_NAMESPACE
