//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator_base.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

// Class representing attribute group. Attribute group is a logical grouping of
// wide-column entities by leveraging Column Families.
// Used in Write Path
class AttributeGroup {
 public:
  explicit AttributeGroup(ColumnFamilyHandle* column_family,
                          const WideColumns& columns)
      : column_family_(column_family), columns_(columns) {}

  ColumnFamilyHandle* column_family() const { return column_family_; }
  const WideColumns& columns() const { return columns_; }
  WideColumns& columns() { return columns_; }

 private:
  ColumnFamilyHandle* column_family_;
  WideColumns columns_;
};

inline bool operator==(const AttributeGroup& lhs, const AttributeGroup& rhs) {
  return lhs.column_family() == rhs.column_family() &&
         lhs.columns() == rhs.columns();
}

inline bool operator!=(const AttributeGroup& lhs, const AttributeGroup& rhs) {
  return !(lhs == rhs);
}

// A collection of Attribute Groups.
using AttributeGroups = std::vector<AttributeGroup>;

// An empty set of Attribute Groups.
extern const AttributeGroups kNoAttributeGroups;

// Used in Read Path. Wide-columns returned from the query are pinnable.
class PinnableAttributeGroup {
 public:
  explicit PinnableAttributeGroup(ColumnFamilyHandle* column_family)
      : column_family_(column_family), status_(Status::OK()) {}

  ColumnFamilyHandle* column_family() const { return column_family_; }
  const Status& status() const { return status_; }
  const WideColumns& columns() const { return columns_.columns(); }

  void SetStatus(const Status& status);
  void SetColumns(PinnableWideColumns&& columns);

  void Reset();

 private:
  ColumnFamilyHandle* column_family_;
  Status status_;
  PinnableWideColumns columns_;
};

inline void PinnableAttributeGroup::SetStatus(const Status& status) {
  status_ = status;
}
inline void PinnableAttributeGroup::SetColumns(PinnableWideColumns&& columns) {
  columns_ = std::move(columns);
}

inline void PinnableAttributeGroup::Reset() {
  SetStatus(Status::OK());
  columns_.Reset();
}

// A collection of Pinnable Attribute Groups.
using PinnableAttributeGroups = std::vector<PinnableAttributeGroup>;

// Used in Iterator Path. Uses pointers to the columns to avoid having to copy
// all WideColumns objs during iteration.
class IteratorAttributeGroup {
 public:
  explicit IteratorAttributeGroup(ColumnFamilyHandle* column_family,
                                  const WideColumns* columns)
      : column_family_(column_family), columns_(columns) {}

  explicit IteratorAttributeGroup(const AttributeGroup& attribute_group)
      : IteratorAttributeGroup(attribute_group.column_family(),
                               &attribute_group.columns()) {}

  ColumnFamilyHandle* column_family() const { return column_family_; }
  const WideColumns& columns() const { return *columns_; }

 private:
  ColumnFamilyHandle* column_family_;
  const WideColumns* columns_;
};

inline bool operator==(const IteratorAttributeGroup& lhs,
                       const IteratorAttributeGroup& rhs) {
  return lhs.column_family() == rhs.column_family() &&
         lhs.columns() == rhs.columns();
}

inline bool operator!=(const IteratorAttributeGroup& lhs,
                       const IteratorAttributeGroup& rhs) {
  return !(lhs == rhs);
}

using IteratorAttributeGroups = std::vector<IteratorAttributeGroup>;

extern const IteratorAttributeGroups kNoIteratorAttributeGroups;

// A cross-column-family iterator that collects and returns attribute groups for
// each key in order provided by comparator
class AttributeGroupIterator : public IteratorBase {
 public:
  AttributeGroupIterator() {}
  ~AttributeGroupIterator() override {}

  // No copy allowed
  AttributeGroupIterator(const AttributeGroupIterator&) = delete;
  AttributeGroupIterator& operator=(const AttributeGroupIterator&) = delete;
  AttributeGroupIterator(AttributeGroupIterator&&) = delete;
  AttributeGroupIterator& operator=(AttributeGroupIterator&&) = delete;

  virtual const IteratorAttributeGroups& attribute_groups() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
