//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  DEPRECATED: backward-compatibility shim. New code should
//  #include "rocksdb/index_factory.h" directly. The type aliases below
//  preserve common source usage of the old type names by value or shared_ptr.
//  This experimental API does not provide binary compatibility: plugins and
//  dependents must be rebuilt. Code that forward-declared the old classes or
//  relied on implicit conversion from the old unscoped ValueType enum may
//  require source changes. Existing subclasses that override the previous
//  no-arg NewBuilder() / single-Slice NewReader(Slice&) methods continue
//  to compile through adapter methods on IndexFactory. New subclasses should
//  override the option-taking signatures on IndexFactory directly. On-disk
//  format is unchanged: the meta block key prefix is
//  "rocksdb.user_defined_index." (kIndexFactoryMetaPrefix).

#pragma once

#include "rocksdb/index_factory.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

using UserDefinedIndexBuilder = IndexFactoryBuilder;
using UserDefinedIndexIterator = IndexFactoryIterator;
using UserDefinedIndexReader = IndexFactoryReader;
using UserDefinedIndexOption = IndexFactoryOptions;
using UserDefinedIndexFactory = IndexFactory;

inline constexpr const char* kUserDefinedIndexPrefix = kIndexFactoryMetaPrefix;

}  // namespace ROCKSDB_NAMESPACE
