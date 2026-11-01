// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Cleanable {
 public:
  Cleanable();
  // No copy constructor and copy assignment allowed.
  Cleanable(Cleanable&) = delete;
  Cleanable& operator=(Cleanable&) = delete;

  // Executes all the registered cleanups
  ~Cleanable();

  // Move constructor and move assignment is allowed.
  Cleanable(Cleanable&&) noexcept;
  Cleanable& operator=(Cleanable&&) noexcept;

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  using CleanupFunction = void (*)(void* arg1, void* arg2);

  // Add another Cleanup to the list
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

  // Move the cleanups owned by this Cleanable to another Cleanable, adding to
  // any existing cleanups it has
  void DelegateCleanupsTo(Cleanable* other);

  // DoCleanup and also resets the pointers for reuse
  inline void Reset() {
    DoCleanup();
    cleanup_.function = nullptr;
    cleanup_.next = nullptr;
  }

  inline bool HasCleanups() const { return cleanup_.function != nullptr; }

 protected:
  struct Cleanup {
    CleanupFunction function;
    void* arg1;
    void* arg2;
    Cleanup* next;
  };
  Cleanup cleanup_;
  // It also becomes the owner of c
  void RegisterCleanup(Cleanup* c);

 private:
  // Performs all the cleanups. It does not reset the pointers. Making it
  // private
  // to prevent misuse
  inline void DoCleanup() {
    if (cleanup_.function != nullptr) {
      (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
      for (Cleanup* c = cleanup_.next; c != nullptr;) {
        (*c->function)(c->arg1, c->arg2);
        Cleanup* next = c->next;
        delete c;
        c = next;
      }
    }
  }
};

// A copyable, reference-counted pointer to a simple Cleanable that only
// performs registered cleanups after all copies are destroy. This is like
// shared_ptr<Cleanable> but works more efficiently with wrapping the pointer
// in an outer Cleanable (see RegisterCopyWith() and MoveAsCleanupTo()).
// WARNING: if you create a reference cycle, for example:
//   SharedCleanablePtr scp;
//   scp.Allocate();
//   scp.RegisterCopyWith(&*scp);
// It will prevent cleanups from ever happening!
class SharedCleanablePtr {
 public:
  // Empy/null pointer
  SharedCleanablePtr() {}
  // Copy and move constructors and assignment
  SharedCleanablePtr(const SharedCleanablePtr& from);
  SharedCleanablePtr(SharedCleanablePtr&& from) noexcept;
  SharedCleanablePtr& operator=(const SharedCleanablePtr& from);
  SharedCleanablePtr& operator=(SharedCleanablePtr&& from) noexcept;
  // Destructor (decrement refcount if non-null)
  ~SharedCleanablePtr();
  // Create a new simple Cleanable and make this assign this pointer to it.
  // (Reset()s first if necessary.)
  void Allocate();
  // Reset to empty/null (decrement refcount if previously non-null)
  void Reset();
  // Dereference to pointed-to Cleanable
  Cleanable& operator*();
  Cleanable* operator->();
  // Get as raw pointer to Cleanable
  Cleanable* get();
  const Cleanable* get() const;

  // Creates a (virtual) copy of this SharedCleanablePtr and registers its
  // destruction with target, so that the cleanups registered with the
  // Cleanable pointed to by this can only happen after the cleanups in the
  // target Cleanable are run.
  // No-op if this is empty (nullptr).
  void RegisterCopyWith(Cleanable* target);

  // Moves (virtually) this shared pointer to a new cleanup in the target.
  // This is essentilly a move semantics version of RegisterCopyWith(), for
  // performance optimization. No-op if this is empty (nullptr).
  void MoveAsCleanupTo(Cleanable* target);

 private:
  struct Impl;
  Impl* ptr_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
