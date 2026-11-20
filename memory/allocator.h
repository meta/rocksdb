//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Abstract interface for allocating memory in blocks. This memory is freed
// when the allocator object is destroyed. See the Arena class for more info.

#pragma once
#include <atomic>
#include <cerrno>
#include <cstddef>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class FlushInitiator;
class Logger;
class WriteBufferManager;

class Allocator {
 public:
  virtual ~Allocator() = default;

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                Logger* logger = nullptr) = 0;

  virtual size_t BlockSize() const = 0;
};

class AllocTracker {
 public:
  explicit AllocTracker(WriteBufferManager* write_buffer_manager,
                        FlushInitiator* flush_initiator);
  // No copying allowed
  AllocTracker(const AllocTracker&) = delete;
  void operator=(const AllocTracker&) = delete;
  AllocTracker(AllocTracker&&) = delete;
  void operator=(AllocTracker&&) = delete;

  ~AllocTracker();
  void Allocate(size_t bytes);

  // Starts publishing this memtable's allocation to its DB-level counters.
  // Safe to call repeatedly and concurrently after successful inserts.
  void ActivateFlushInitiator();

  // Stops publishing this memtable as mutable without changing the WBM's
  // global allocation accounting.
  void DeactivateFlushInitiator();

  // Publishes any allocation not yet reflected in the DB-level counters.
  void RefreshFlushInitiator();

  // Call when we're finished allocating memory so we can free it from
  // the write buffer's limit.
  void DoneAllocating();

  void FreeMem();

  bool is_freed() const { return write_buffer_manager_ == nullptr || freed_; }

  size_t tracked_bytes() const {
    return bytes_reported_.load(std::memory_order_relaxed);
  }

  size_t allocated_bytes() const {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }

  bool IsFlushInitiatorActive() const {
    return flush_initiator_active_.load(std::memory_order_acquire);
  }

 private:
  void ReportAllocations(size_t bytes_allocated);

  WriteBufferManager* write_buffer_manager_;
  FlushInitiator* flush_initiator_;
  std::atomic<size_t> bytes_allocated_;
  std::atomic<size_t> bytes_reported_;
  std::atomic<bool> flush_initiator_active_;
  bool done_allocating_;
  bool freed_;
};

}  // namespace ROCKSDB_NAMESPACE
