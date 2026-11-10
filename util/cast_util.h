//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <initializer_list>
#include <memory>
#include <type_traits>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// The helper function to assert the move from dynamic_cast<> to
// static_cast<> is correct. This function is to deal with legacy code.
// It is not recommended to add new code to issue class casting. The preferred
// solution is to implement the functionality without a need of casting.
template <class DestClass, class SrcClass>
inline DestClass* static_cast_with_check(SrcClass* x) {
  DestClass* ret = static_cast<DestClass*>(x);
#ifdef ROCKSDB_USE_RTTI
  assert(ret == dynamic_cast<DestClass*>(x));
#endif
  return ret;
}

template <class DestClass, class SrcClass>
inline std::shared_ptr<DestClass> static_cast_with_check(
    std::shared_ptr<SrcClass>&& x) {
#if defined(ROCKSDB_USE_RTTI) && !defined(NDEBUG)
  auto orig_raw = x.get();
#endif
  auto ret = std::static_pointer_cast<DestClass>(std::move(x));
#if defined(ROCKSDB_USE_RTTI) && !defined(NDEBUG)
  assert(ret.get() == dynamic_cast<DestClass*>(orig_raw));
#endif
  return ret;
}

// A safe, compile-time-checked upcast of a reference to a base class. Unlike a
// raw static_cast, this cannot silently perform a downcast or an unrelated
// conversion, and it reads as an explicit "treat this as its base" (e.g. to
// copy or swap just the base subobject). `Base` must be a public base of
// `Derived`; a free function cannot access a private/protected base.
template <class Base, class Derived>
inline Base& up_cast(Derived& ref) {
  static_assert(std::is_base_of_v<Base, Derived>,
                "up_cast requires Base to be a base class of Derived");
  return ref;
}

template <class Base, class Derived>
inline const Base& up_cast(const Derived& ref) {
  static_assert(std::is_base_of_v<Base, Derived>,
                "up_cast requires Base to be a base class of Derived");
  return ref;
}

// A wrapper around static_cast for lossless conversion between integral
// types, including enum types, and pointers to such types. For example, this
// can be used for converting between signed/unsigned or enum type and
// underlying type without fear of stripping away data, now or in the future.
template <typename To, typename From>
inline To lossless_cast(From x) {
  using FromValue = typename std::remove_reference_t<From>;
  if constexpr (std::is_pointer_v<FromValue>) {
    static_assert(std::is_pointer_v<To>);
    using FromDeref = typename std::remove_pointer_t<FromValue>;
    using ToDeref = typename std::remove_pointer_t<To>;
    static_assert(std::is_integral_v<FromDeref> || std::is_enum_v<FromDeref>,
                  "Only works on integral types");
    static_assert(std::is_integral_v<ToDeref> || std::is_enum_v<ToDeref>,
                  "Only works on integral types");
    static_assert(sizeof(ToDeref) == sizeof(FromDeref), "Must be lossless");
    return reinterpret_cast<To>(x);
  } else {
    static_assert(std::is_integral_v<FromValue> || std::is_enum_v<FromValue>,
                  "Only works on integral types");
    static_assert(std::is_integral_v<To> || std::is_enum_v<To>,
                  "Only works on integral types");
    static_assert(sizeof(To) >= sizeof(FromValue), "Must be lossless");
    return static_cast<To>(x);
  }
}

// For disambiguating a potentially heterogeneous aggregate as a homogeneous
// initializer list. E.g. might be able to write List({x, y}) in some cases
// instead of std::vector<const Widget&>({x, y}).
template <typename T>
inline const std::initializer_list<T>& List(
    const std::initializer_list<T>& list) {
  return list;
}

// UnownedPtr<T> is useful as an efficient "optional reference" that can't
// be accidentally converted to std::shared_ptr<T> nor std::unique_ptr<T>.
template <typename T>
class UnownedPtr {
 public:
  UnownedPtr() = default;
  UnownedPtr(std::nullptr_t) {}      // NOLINT(google-explicit-constructor)
  UnownedPtr(T* ptr) : ptr_(ptr) {}  // NOLINT(google-explicit-constructor)
  UnownedPtr(const UnownedPtr&) = default;
  UnownedPtr(UnownedPtr&&) = default;
  UnownedPtr& operator=(const UnownedPtr&) = default;
  UnownedPtr& operator=(UnownedPtr&&) = default;
  ~UnownedPtr() = default;

  T* get() const { return ptr_; }
  T* operator->() const { return ptr_; }
  T& operator*() const { return *ptr_; }
  explicit operator bool() const { return ptr_ != nullptr; }

 private:
  T* ptr_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
