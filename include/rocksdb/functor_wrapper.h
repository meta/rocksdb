//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

namespace detail {
template <std::size_t...>
struct IndexSequence {};

template <std::size_t N, std::size_t... Next>
struct IndexSequenceHelper
    : public IndexSequenceHelper<N - 1U, N - 1U, Next...> {};

template <std::size_t... Next>
struct IndexSequenceHelper<0U, Next...> {
  using type = IndexSequence<Next...>;
};

template <std::size_t N>
using make_index_sequence = typename IndexSequenceHelper<N>::type;

template <typename Function, typename Tuple, size_t... I>
void call(Function f, Tuple t, IndexSequence<I...>) {
  f(std::get<I>(t)...);
}

template <typename Function, typename Tuple>
void call(Function f, Tuple t) {
  static constexpr auto size = std::tuple_size<Tuple>::value;
  call(f, t, make_index_sequence<size>{});
}
}  // namespace detail

template <typename... Args>
class FunctorWrapper {
 public:
  explicit FunctorWrapper(std::function<void(Args...)> functor, Args&&... args)
      : functor_(std::move(functor)), args_(std::forward<Args>(args)...) {}

  void invoke() { detail::call(functor_, args_); }

 private:
  std::function<void(Args...)> functor_;
  std::tuple<Args...> args_;
};
}  // namespace ROCKSDB_NAMESPACE
