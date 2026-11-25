//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {

// This creates MemTableReps backed by an adaptive radix trie over an
// order-preserving byte encoding of internal keys. Common key prefixes are
// stored once, and lookups cost O(key length) instead of O(key length * log
// number of entries).
//
// The trie orders entries by raw bytes, so it requires the default bytewise
// user comparator without user timestamps. When the comparator is anything
// else, CreateMemTableRep logs a warning and falls back to a SkipListRep
// rather than risk misordering entries.
//
// Inserts are single-writer: DB::Open requires
// allow_concurrent_memtable_write=false, so the write group leader inserts
// each group's batches. Reads and iterators never block and may run
// concurrently with the writer.
//
// EXPERIMENTAL options for TrieRep.
struct TrieRepOptions {
  // Keep each entry's key bytes once: only the bytes below the entry's place
  // in the trie are stored with the value, since the trie path above it
  // already spells out the rest, saving memory roughly in proportion to how
  // long a prefix keys share.
  //
  // This is a behavior change, not just a speed/memory tradeoff, which is why
  // it is an option rather than always on: iterator keys are rebuilt on each
  // move and are not pinned (Iterator::IsKeyPinned() is false), and memtables
  // with inplace_update_support, paranoid_memory_checks,
  // memtable_verify_per_key_checksum_on_seek or MemPurge fall back to a skip
  // list. Off by default.
  bool store_keys_once = false;
};

class TrieRepFactory : public MemTableRepFactory {
 public:
  TrieRepFactory() = default;
  explicit TrieRepFactory(const TrieRepOptions& options) : options_(options) {}

  // Methods for Configurable/Customizable class overrides
  static const char* kClassName() { return "TrieRepFactory"; }
  static const char* kNickName() { return "trie"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  // Methods for MemTableRepFactory class overrides
  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&, Allocator*,
                                 const SliceTransform*,
                                 Logger* logger) override;

  bool IsInsertConcurrentlySupported() const override { return false; }

  bool CanHandleDuplicatedKey() const override { return true; }

 private:
  TrieRepOptions options_;
};

MemTableRepFactory* NewTrieRepFactory();
MemTableRepFactory* NewTrieRepFactory(const TrieRepOptions& options);

}  // namespace ROCKSDB_NAMESPACE
