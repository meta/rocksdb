//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <forward_list>
#include <iterator>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "rocksdb/cleanable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class PinnableWideColumnsHelper;

// Class representing a non-owning view of a wide column, which is defined as a
// pair of column name and column value.
//
// WideColumn is a non-owning view. Both the column name and column value are
// stored as Slices that reference caller-managed memory. The backing storage
// must remain valid for as long as the WideColumn is used. In particular,
// passing temporary std::string objects is unsafe. When passing WideColumn or
// WideColumns to APIs like PutEntity(), keep the backing storage alive until
// that method returns.
class WideColumn {
 public:
  WideColumn() = default;

  // Initializes a WideColumn object by forwarding the non-owning name and value
  // arguments to the corresponding member Slices without copying the bytes.
  // The resulting WideColumn does not own the referenced data. Construction is
  // decoupled from the eventual PutEntity() call, but lifetime is not: any
  // buffers referenced here must remain valid until the PutEntity() call that
  // consumes this WideColumn or WideColumns collection returns. This makes it
  // possible to construct a WideColumn using combinations of const char*,
  // const std::string&, const Slice& etc., for example:
  //
  // std::string column_name = "attr";
  // std::string column_value = "value";
  // WideColumn column(column_name, column_value);
  // WideColumns columns{column};
  // ASSERT_OK(db->PutEntity(write_options, column_family, key, columns));
  // // column_name and column_value must stay alive until PutEntity() returns.
  //
  // // Unsafe: the temporary std::string storage is destroyed before
  // PutEntity(). WideColumns bad_columns{
  //     {std::string("attr"), std::string("value")},
  // };
  template <typename N, typename V>
  WideColumn(N&& name, V&& value)
      : name_(std::forward<N>(name)), value_(std::forward<V>(value)) {}

  // Initializes a WideColumn object by forwarding the elements of name_tuple
  // and value_tuple to the constructors of the corresponding member Slices
  // without copying the bytes. As above, the caller retains ownership of the
  // referenced bytes, so any buffers used here must stay alive for as long as
  // the WideColumn is used and until any consuming PutEntity() call returns.
  // This makes it possible to initialize the Slices using the Slice
  // constructors that take more than one argument, for example:
  //
  // constexpr char foo_name[] = "foo_name";
  // constexpr char bar_value[] = "bar_value";
  // WideColumn column(std::piecewise_construct,
  //                   std::forward_as_tuple(foo_name, 3),
  //                   std::forward_as_tuple(bar_value, 3));
  template <typename NTuple, typename VTuple>
  WideColumn(std::piecewise_construct_t, NTuple&& name_tuple,
             VTuple&& value_tuple)
      : name_(std::make_from_tuple<Slice>(std::forward<NTuple>(name_tuple))),
        value_(std::make_from_tuple<Slice>(std::forward<VTuple>(value_tuple))) {
  }

  const Slice& name() const { return name_; }
  const Slice& value() const { return value_; }

  Slice& name() { return name_; }
  Slice& value() { return value_; }

 private:
  Slice name_;
  Slice value_;
};

// Note: column names and values are compared bytewise.
inline bool operator==(const WideColumn& lhs, const WideColumn& rhs) {
  return lhs.name() == rhs.name() && lhs.value() == rhs.value();
}

inline bool operator!=(const WideColumn& lhs, const WideColumn& rhs) {
  return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const WideColumn& column) {
  const bool hex =
      (os.flags() & std::ios_base::basefield) == std::ios_base::hex;
  if (!column.name().empty()) {
    if (hex) {
      os << "0x";
    }
    os << column.name().ToString(hex);
  }
  os << ':';
  if (!column.value().empty()) {
    if (hex) {
      os << "0x";
    }
    os << column.value().ToString(hex);
  }
  return os;
}

// A collection of wide columns. Like WideColumn, this is a *non-owning view*
// of names and values that must be kept alive elsewhere. See also
// PinnableWideColumns.
using WideColumns = std::vector<WideColumn>;

// The anonymous default wide column (an empty Slice).
extern const Slice kDefaultWideColumnName;

// An empty set of wide columns.
extern const WideColumns kNoWideColumns;

// A self-contained result of a wide-column query (e.g. GetEntity(),
// MultiGetEntity(), or the columns view of an iterator). It owns the memory
// backing its columns: columns() returns the column names and values as
// zero-copy Slices that point into one or more stable backing buffers held
// or tracked by this object, so the result remains valid without the caller
// retaining any other state. Like PinnableSlice, it is move-only and cheap to
// move.
//
// A result may be backed by a single buffer (e.g. a plain value or a serialized
// entity) or by several buffers (e.g. after blob-referenced columns have been
// resolved, where each resolved value keeps its own buffer); either way,
// columns() presents a single unified, in-order column set.
class PinnableWideColumns {
 public:
  PinnableWideColumns() = default;

  PinnableWideColumns(const PinnableWideColumns&) = delete;
  PinnableWideColumns& operator=(const PinnableWideColumns&) = delete;

  PinnableWideColumns(PinnableWideColumns&&) = default;
  PinnableWideColumns& operator=(PinnableWideColumns&&) = default;

  ~PinnableWideColumns() = default;

  const WideColumns& columns() const { return columns_; }

  // Total size of the column names and values, computed as
  // sum(name.size() + value.size()) over all columns. This is the measure used
  // for read/threshold accounting; it is well-defined regardless of how the
  // backing bytes are laid out (single buffer, multiple buffers, or zero-copy
  // resolved columns). For a plain value (a single anonymous default column)
  // this equals the raw value size.
  size_t payload_size() const {
    size_t total = 0;
    for (const auto& column : columns_) {
      total += column.name().size() + column.value().size();
    }
    return total;
  }

  // DEPRECATED: use payload_size(). Returns the number of bytes this entity
  // would occupy if serialized in the version 1 wide-column format. Kept for
  // external API compatibility; it has no internal callers.
  size_t serialized_size() const;

  void SetPlainValue(const Slice& value);
  void SetPlainValue(const Slice& value, Cleanable* cleanable);
  void SetPlainValue(PinnableSlice&& value);
  void SetPlainValue(std::string&& value);

  Status SetWideColumnValue(const Slice& value);
  Status SetWideColumnValue(const Slice& value, Cleanable* cleanable);
  Status SetWideColumnValue(PinnableSlice&& value);
  Status SetWideColumnValue(std::string&& value);

  void Reset();

 private:
  void CopyValue(const Slice& value);
  void PinOrCopyValue(const Slice& value, Cleanable* cleanable);
  void MoveValue(PinnableSlice&& value);
  void MoveValue(std::string&& value);

  void BuildColumnsForPlainValue();
  Status BuildColumnsForEntity();

  friend class PinnableWideColumnsHelper;

  // One or more stable backing buffers. The name()/value() Slices in `columns_`
  // point into these nodes. std::forward_list nodes are individually
  // heap-allocated and never relocate on move, move-assignment, or splice, so
  // those Slices stay valid across all of those operations -- even for a
  // self-pinned (SSO) PinnableSlice, because the node holding the string never
  // moves.
  std::forward_list<PinnableSlice> backing_;
  WideColumns columns_;
  // Internal-only metadata for V2 entities whose blob columns still point to
  // serialized BlobIndex payloads in `backing_`.
  std::vector<size_t> unresolved_blob_column_indices_;
};

inline void PinnableWideColumns::Reset() {
  backing_.clear();
  columns_.clear();
  unresolved_blob_column_indices_.clear();
}

inline void PinnableWideColumns::CopyValue(const Slice& value) {
  // Reuse an existing single, self-owned backing buffer when we have one;
  // otherwise reset to a fresh one. Assigning into the existing buffer rather
  // than freeing it first keeps the copy safe even when `value` aliases our own
  // storage (e.g. a Slice returned by columns()): PinSelf() ->
  // std::string::assign() handles self-overlapping input. It also avoids
  // node/string churn when a PinnableWideColumns is reused across plain-value
  // reads. A pinned node (external Cleanable storage) can't be reused because
  // PinSelf() requires an unpinned target, so fall back to a fresh buffer.
  if (backing_.empty() || std::next(backing_.begin()) != backing_.end() ||
      backing_.front().IsPinned()) {
    backing_.clear();
    backing_.emplace_front();
  }
  backing_.front().PinSelf(value);
}

inline void PinnableWideColumns::PinOrCopyValue(const Slice& value,
                                                Cleanable* cleanable) {
  if (!cleanable) {
    CopyValue(value);
    return;
  }

  backing_.clear();
  backing_.emplace_front();
  backing_.front().PinSlice(value, cleanable);
}

inline void PinnableWideColumns::MoveValue(PinnableSlice&& value) {
  backing_.clear();
  backing_.emplace_front(std::move(value));
}

inline void PinnableWideColumns::MoveValue(std::string&& value) {
  backing_.clear();
  backing_.emplace_front();

  std::string* const buf = backing_.front().GetSelf();
  assert(buf);

  *buf = std::move(value);
  backing_.front().PinSelf();
}

inline void PinnableWideColumns::BuildColumnsForPlainValue() {
  unresolved_blob_column_indices_.clear();
  columns_ = WideColumns{{kDefaultWideColumnName, backing_.front()}};
}

inline void PinnableWideColumns::SetPlainValue(const Slice& value) {
  CopyValue(value);
  BuildColumnsForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(const Slice& value,
                                               Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);
  BuildColumnsForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(PinnableSlice&& value) {
  MoveValue(std::move(value));
  BuildColumnsForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(std::string&& value) {
  MoveValue(std::move(value));
  BuildColumnsForPlainValue();
}

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value) {
  CopyValue(value);

  const Status s = BuildColumnsForEntity();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value,
                                                      Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);

  const Status s = BuildColumnsForEntity();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

inline Status PinnableWideColumns::SetWideColumnValue(PinnableSlice&& value) {
  MoveValue(std::move(value));

  const Status s = BuildColumnsForEntity();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

inline Status PinnableWideColumns::SetWideColumnValue(std::string&& value) {
  MoveValue(std::move(value));

  const Status s = BuildColumnsForEntity();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

inline bool operator==(const PinnableWideColumns& lhs,
                       const PinnableWideColumns& rhs) {
  return lhs.columns() == rhs.columns();
}

inline bool operator!=(const PinnableWideColumns& lhs,
                       const PinnableWideColumns& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE
