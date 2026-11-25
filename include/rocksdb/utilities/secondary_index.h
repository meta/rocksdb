//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "rocksdb/iterator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

// EXPERIMENTAL
//
// A secondary index is an additional data structure built over a set of primary
// key-values that enables efficiently querying key-values by value instead of
// key. Both plain and wide-column key-values can be indexed, the latter on a
// per-column basis. The secondary index then maintains a mapping from (column)
// value to the list of primary keys that have the corresponding value (in the
// given column).
//
// The primary and secondary key-value pairs can be stored in either the same
// column family or different ones. It is the application's responsibility to
// avoid conflicts and ambiguities (for example, by using prefixes to create
// separate key spaces or using a dedicated column family for each secondary
// index). Also, note that applications are not expected to manipulate secondary
// index entries directly.
//
// In the general case where there are concurrent writers, maintaining a
// secondary index requires transactional semantics and concurrency control.
// Because of this, secondary indices are only supported via the transaction
// layer. With secondary indices, whenever a (primary) key-value is inserted,
// updated, or deleted via a transaction (regardless of whether it is an
// explicit or implicit one), RocksDB will invoke any applicable SecondaryIndex
// objects based on primary column family and column name, and it will
// automatically add or remove any secondary index entries as needed (using
// the same transaction).
//
// Note: the methods of SecondaryIndex implementations are expected to be
// thread-safe with the exception of Set{Primary,Secondary}ColumnFamily (which
// are not expected to be called after initialization).

class SecondaryIndex {
 public:
  virtual ~SecondaryIndex() = default;

  virtual void SetPrimaryColumnFamily(ColumnFamilyHandle* column_family) = 0;
  virtual void SetSecondaryColumnFamily(ColumnFamilyHandle* column_family) = 0;

  virtual ColumnFamilyHandle* GetPrimaryColumnFamily() const = 0;
  virtual ColumnFamilyHandle* GetSecondaryColumnFamily() const = 0;

  // The name of the primary column to index. Plain key-values can be indexed by
  // specifying kDefaultWideColumnName.
  virtual Slice GetPrimaryColumnName() const = 0;

  // Optionally update the primary column value during an insert or update of a
  // primary key-value. Called by the transaction layer before the primary
  // key-value write is added to the transaction. Returning a non-OK status
  // rolls back all operations in the transaction related to this primary
  // key-value.
  virtual Status UpdatePrimaryColumnValue(
      const Slice& primary_key, const Slice& primary_column_value,
      std::optional<std::variant<Slice, std::string>>* updated_column_value)
      const = 0;

  // Get the secondary key prefix for a given primary key-value. This method is
  // called by the transaction layer when adding or removing secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>) and should be deterministic. The output parameter
  // secondary_key_prefix is expected to be based on primary_key and/or
  // primary_column_value. Returning a non-OK status rolls back all operations
  // in the transaction related to this primary key-value.
  virtual Status GetSecondaryKeyPrefix(
      const Slice& primary_key, const Slice& primary_column_value,
      std::variant<Slice, std::string>* secondary_key_prefix) const = 0;

  // Finalize the secondary key prefix, for instance by adding some metadata to
  // prevent ambiguities (e.g. index id or length indicator). This method is
  // called by the transaction layer when adding or removing secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>) and also when querying the index (in which case it is
  // called with the search target). The method should be deterministic.
  // Returning a non-OK status rolls back all operations in the transaction
  // related to this primary key-value.
  virtual Status FinalizeSecondaryKeyPrefix(
      std::variant<Slice, std::string>* secondary_key_prefix) const = 0;

  // Get the optional secondary value for a given primary key-value. This method
  // is called by the transaction layer when adding secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>). previous_column_value contains the previous value of
  // the primary column in case it was changed by UpdatePrimaryColumnValue.
  // Returning a non-OK status rolls back all operations in the transaction
  // related to this primary key-value.
  virtual Status GetSecondaryValue(
      const Slice& primary_key, const Slice& primary_column_value,
      const Slice& previous_column_value,
      std::optional<std::variant<Slice, std::string>>* secondary_value)
      const = 0;
};

// SecondaryIndexIterator can be used to find the primary keys for a given
// search target. It can be used as-is or as a building block. Its interface
// mirrors most of the Iterator API, with the exception of SeekToFirst,
// SeekToLast, and SeekForPrev, which are not applicable to secondary indices
// and thus not present. Querying the index can be performed by calling the
// returned iterator's Seek API with a search target, and then using Next (and
// potentially Prev) to iterate through the matching index entries. The iterator
// exposes primary keys, that is, the secondary key prefix is stripped from the
// index entries.

class SecondaryIndexIterator {
 public:
  // Constructs a SecondaryIndexIterator. The SecondaryIndexIterator takes
  // ownership of the underlying iterator.
  // PRE: index is not nullptr
  // PRE: underlying_it is not nullptr and points to an iterator over the
  // index's secondary column family
  SecondaryIndexIterator(const SecondaryIndex* index,
                         std::unique_ptr<Iterator>&& underlying_it);

  // Returns whether the iterator is valid, i.e. whether it is positioned on a
  // secondary index entry matching the search target.
  bool Valid() const;

  // Returns the status of the iterator, which is guaranteed to be OK if the
  // iterator is valid. Otherwise, it might be non-OK, which indicates an error,
  // or OK, which means that the iterator has reached the end of the applicable
  // secondary index entries.
  Status status() const;

  // Query the index with the given search target.
  void Seek(const Slice& target);

  // Move the iterator to the next entry.
  // PRE: Valid()
  void Next();

  // Move the iterator back to the previous entry.
  // PRE: Valid()
  void Prev();

  // Prepare the value of the current entry. Should be called before calling
  // value() or columns() if the underlying iterator was constructed with the
  // read option allow_unprepared_value set to true. Returns true upon success.
  // Returns false and sets the status to non-OK upon failure.
  // PRE: Valid()
  bool PrepareValue();

  // Returns the primary key from the current secondary index entry.
  // PRE: Valid()
  Slice key() const;

  // Returns the value of the current secondary index entry.
  // PRE: Valid()
  Slice value() const;

  // Returns the value of the current secondary index entry as a wide-column
  // structure.
  // PRE: Valid()
  const WideColumns& columns() const;

  // Returns the timestamp of the current secondary index entry.
  // PRE: Valid()
  Slice timestamp() const;

  // Queries the given property of the underlying iterator. Returns OK on
  // success, non-OK on failure.
  // PRE: Valid()
  Status GetProperty(std::string prop_name, std::string* prop) const;

 private:
  const SecondaryIndex* index_;
  std::unique_ptr<Iterator> underlying_it_;
  Status status_;
  std::string prefix_;
};

}  // namespace ROCKSDB_NAMESPACE
