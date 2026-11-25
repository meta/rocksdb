//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// `ExpectedValue` represents the expected value of a key used in db stress,
// which provides APIs to obtain various information e.g, value base, existence,
// pending operation status and APIs to edit expected value.
//
// This class is not thread-safe.
class ExpectedValue {
 public:
  static uint32_t GetValueBaseMask() { return VALUE_BASE_MASK; }
  static uint32_t GetValueBaseDelta() { return VALUE_BASE_DELTA; }
  static uint32_t GetDelCounterDelta() { return DEL_COUNTER_DELTA; }
  static uint32_t GetDelMask() { return DEL_MASK; }
  static bool IsValueBaseValid(uint32_t value_base) {
    return IsValuePartValid(value_base, VALUE_BASE_MASK);
  }

  ExpectedValue() : expected_value_(DEL_MASK) {}

  explicit ExpectedValue(uint32_t expected_value)
      : expected_value_(expected_value) {}

  bool Exists() const {
    assert(!PendingWrite() && !PendingDelete());
    return !IsDeleted();
  }

  uint32_t Read() const { return expected_value_; }

  void Put(bool pending);

  bool Delete(bool pending);

  void SyncPut(uint32_t value_base);

  void SyncPendingPut();

  void SyncDelete();

  uint32_t GetValueBase() const { return GetValuePart(VALUE_BASE_MASK); }

  uint32_t NextValueBase() const {
    return GetIncrementedValuePart(VALUE_BASE_MASK, VALUE_BASE_DELTA);
  }

  void SetValueBase(uint32_t new_value_base) {
    SetValuePart(VALUE_BASE_MASK, new_value_base);
  }

  bool PendingWrite() const {
    const uint32_t pending_write = GetValuePart(PENDING_WRITE_MASK);
    return pending_write != 0;
  }

  void SetPendingWrite() {
    SetValuePart(PENDING_WRITE_MASK, PENDING_WRITE_MASK);
  }

  void ClearPendingWrite() { ClearValuePart(PENDING_WRITE_MASK); }

  uint32_t GetDelCounter() const { return GetValuePart(DEL_COUNTER_MASK); }

  uint32_t NextDelCounter() const {
    return GetIncrementedValuePart(DEL_COUNTER_MASK, DEL_COUNTER_DELTA);
  }

  void SetDelCounter(uint32_t new_del_counter) {
    SetValuePart(DEL_COUNTER_MASK, new_del_counter);
  }

  bool PendingDelete() const {
    const uint32_t pending_del = GetValuePart(PENDING_DEL_MASK);
    return pending_del != 0;
  }

  void SetPendingDel() { SetValuePart(PENDING_DEL_MASK, PENDING_DEL_MASK); }

  void ClearPendingDel() { ClearValuePart(PENDING_DEL_MASK); }

  bool IsDeleted() const {
    const uint32_t deleted = GetValuePart(DEL_MASK);
    return deleted != 0;
  }

  void SetDeleted() { SetValuePart(DEL_MASK, DEL_MASK); }

  void ClearDeleted() { ClearValuePart(DEL_MASK); }

  uint32_t GetFinalValueBase() const;

  uint32_t GetFinalDelCounter() const;

 private:
  static bool IsValuePartValid(uint32_t value_part, uint32_t value_part_mask) {
    return (value_part & (~value_part_mask)) == 0;
  }

  // The 32-bit expected_value_ is divided into following parts:
  // Bit 0 - 14: value base
  static constexpr uint32_t VALUE_BASE_MASK = 0x7fff;
  static constexpr uint32_t VALUE_BASE_DELTA = 1;
  // Bit 15: whether write to this value base is pending (0 equals `false`)
  static constexpr uint32_t PENDING_WRITE_MASK = (uint32_t)1 << 15;
  // Bit 16 - 29: deletion counter (i.e, number of times this value base has
  // been deleted)
  static constexpr uint32_t DEL_COUNTER_MASK = 0x3fff0000;
  static constexpr uint32_t DEL_COUNTER_DELTA = (uint32_t)1 << 16;
  // Bit 30: whether deletion of this value base is pending (0 equals `false`)
  static constexpr uint32_t PENDING_DEL_MASK = (uint32_t)1 << 30;
  // Bit 31: whether this value base is deleted (0 equals `false`)
  static constexpr uint32_t DEL_MASK = (uint32_t)1 << 31;

  uint32_t GetValuePart(uint32_t value_part_mask) const {
    return expected_value_ & value_part_mask;
  }

  uint32_t GetIncrementedValuePart(uint32_t value_part_mask,
                                   uint32_t value_part_delta) const {
    uint32_t current_value_part = GetValuePart(value_part_mask);
    ExpectedValue temp_expected_value(current_value_part + value_part_delta);
    return temp_expected_value.GetValuePart(value_part_mask);
  }

  void SetValuePart(uint32_t value_part_mask, uint32_t new_value_part) {
    assert(IsValuePartValid(new_value_part, value_part_mask));
    ClearValuePart(value_part_mask);
    expected_value_ |= new_value_part;
  }

  void ClearValuePart(uint32_t value_part_mask) {
    expected_value_ &= (~value_part_mask);
  }

  uint32_t expected_value_;
};

// `PendingExpectedValue` represents the expected value of a key undergoing a
// pending operation in db stress.
//
// After a `PendingExpectedValue` object is created, either `Rollback` or
// `Commit` should be called to close its pending state before it's destructed.
// In case no pending state was introduced while creating this
// `PendingExpectedValue` and user want to ignore the unclosed pending state,
// `PermitUnclosedPendingState` should be called explicitly.
// This class is not thread-safe.
class PendingExpectedValue {
 public:
  explicit PendingExpectedValue(std::atomic<uint32_t>* value_ptr,
                                ExpectedValue orig_value,
                                ExpectedValue final_value)
      : value_ptr_(value_ptr),
        orig_value_(orig_value),
        final_value_(final_value),
        pending_state_closed_(false) {}

  PendingExpectedValue(const PendingExpectedValue& other)
      : value_ptr_(other.value_ptr_),
        orig_value_(other.orig_value_),
        final_value_(other.final_value_),
        pending_state_closed_(false) {
    other.ClosePendingState();
  }

  PendingExpectedValue(PendingExpectedValue&& other) noexcept
      : value_ptr_(std::move(other.value_ptr_)),
        orig_value_(std::move(other.orig_value_)),
        final_value_(std::move(other.final_value_)),
        pending_state_closed_(false) {
    other.ClosePendingState();
  }

  PendingExpectedValue& operator=(const PendingExpectedValue& other) {
    if (this != &other) {
      other.ClosePendingState();
      value_ptr_ = other.value_ptr_;
      orig_value_ = other.orig_value_;
      final_value_ = other.final_value_;
      pending_state_closed_ = false;
    }
    return *this;
  }

  PendingExpectedValue& operator=(PendingExpectedValue&& other) {
    if (this != &other) {
      other.ClosePendingState();
      value_ptr_ = std::move(other.value_ptr_);
      orig_value_ = std::move(other.orig_value_);
      final_value_ = std::move(other.final_value_);
      pending_state_closed_ = false;
    }
    return *this;
  }

  ~PendingExpectedValue() { assert(pending_state_closed_); }

  void Commit() {
    assert(!pending_state_closed_);
    ClosePendingState();
    // To prevent low-level instruction reordering that results
    // in setting expected value happens before db write
    std::atomic_thread_fence(std::memory_order_release);
    value_ptr_->store(final_value_.Read());
  }

  // Rollbacks the key to its original state.
  // This rollbacks the pending state created in `ExpectedState::Precommit`,
  // such as pending delete, pending put. If `ExpectedState::Precommit()` is not
  // called before creating this `PendingExpectedValue`, this is a no-op.
  void Rollback() {
    assert(!pending_state_closed_);
    ClosePendingState();
    // To prevent low-level instruction reordering that results
    // in setting expected value happens before db write
    std::atomic_thread_fence(std::memory_order_release);
    value_ptr_->store(orig_value_.Read());
  }

  void PermitUnclosedPendingState() const {
    assert(!pending_state_closed_);
    ClosePendingState();
  }

  uint32_t GetFinalValueBase() { return final_value_.GetValueBase(); }

 private:
  inline void ClosePendingState() const { pending_state_closed_ = true; }

  std::atomic<uint32_t>* value_ptr_;
  ExpectedValue orig_value_;
  ExpectedValue final_value_;
  mutable bool pending_state_closed_;
};

// `ExpectedValueHelper` provides utils to parse `ExpectedValue` to obtain
// useful info about it in db stress
class ExpectedValueHelper {
 public:
  // Return whether the key associated with `pre_read_expected_value` and
  // `post_read_expected_value` is expected not to exist from beginning till the
  // end of the read
  //
  // The negation of `MustHaveNotExisted()` is "may have not existed".
  // To assert some key must have existed, please use `MustHaveExisted()`
  static bool MustHaveNotExisted(ExpectedValue pre_read_expected_value,
                                 ExpectedValue post_read_expected_value);

  // Return whether the key associated with `pre_read_expected_value` and
  // `post_read_expected_value` is expected to exist from beginning till the end
  // of the read.
  //
  // The negation of `MustHaveExisted()` is "may have existed".
  // To assert some key must have not existed, please use
  // `MustHaveNotExisted()`
  static bool MustHaveExisted(ExpectedValue pre_read_expected_value,
                              ExpectedValue post_read_expected_value);

  // Return whether the `value_base` falls within the expected value base
  static bool InExpectedValueBaseRange(uint32_t value_base,
                                       ExpectedValue pre_read_expected_value,
                                       ExpectedValue post_read_expected_value);
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
