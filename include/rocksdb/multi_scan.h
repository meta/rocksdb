//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <exception>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// An iterator that returns results from multiple scan ranges. The ranges are
// expected to be in increasing sorted order.
// The results are returned in nested container objects that can be iterated
// using an std::input_iterator.
//
// MultiScan
//     |
//     ---
//       |
//  MultiScanIterator  <-- std::input_iterator (returns a Scan object for each
//         |                                    scan range)
//         ---
//           |
//          Scan
//            |
//            ---
//              |
//          ScanIterator <-- std::input_iterator (returns the KVs of a single
//                                                scan range)
//
// The application on top of RocksDB
// would use this as follows -
//
//  std::vector<ScanOptions> scans{{.start = Slice("bar")},
//                              {.start = Slice("foo")}};
//  std::unique_ptr<MultiScan> iter.reset(
//                                      db->NewMultiScan());
//  try {
//    for (auto scan : *iter) {
//      for (auto it : scan) {
//        // Do something with key - it.first
//        // Do something with value - it.second
//      }
//    }
//  } catch (MultiScanException& ex) {
//    // Check ex.status()
//  } catch (std::logic_error& ex) {
//    // Check ex.what()
//  }

class MultiScanException : public std::runtime_error {
 public:
  explicit MultiScanException(Status& s)
      : std::runtime_error(s.ToString()), s_(s) {}

  Status& status() { return s_; }

 private:
  Status s_;
};

// A container object encapsulating a single scan range. It supports an
// std::input_iterator for a single pass iteration of the KVs in the range.
// A Status exception is thrown if there is an error in scanning the range.
class Scan {
 public:
  class ScanIterator;

  explicit Scan(Iterator* db_iter, bool reverse = false)
      : db_iter_(db_iter), reverse_(reverse) {}

  void Reset(Iterator* db_iter, bool reverse = false) {
    db_iter_ = db_iter;
    reverse_ = reverse;
  }

  ScanIterator begin() { return ScanIterator(db_iter_, reverse_); }

  std::nullptr_t end() { return nullptr; }

  class ScanIterator {
   public:
    using self_type = ScanIterator;
    using value_type = std::pair<Slice, Slice>;
    using reference = std::pair<Slice, Slice>&;
    using pointer = std::pair<Slice, Slice>*;
    using difference_type = int;
    using iterator_category = std::input_iterator_tag;

    explicit ScanIterator(Iterator* db_iter, bool reverse = false)
        : db_iter_(db_iter), reverse_(reverse) {
      valid_ = db_iter_->Valid();
      if (valid_) {
        result_ = value_type(db_iter_->key(), db_iter_->value());
      }
    }

    ScanIterator() : db_iter_(nullptr), valid_(false) {}

    ScanIterator(const ScanIterator&) = default;
    ScanIterator& operator=(const ScanIterator&) = default;
    ScanIterator(ScanIterator&&) = default;
    ScanIterator& operator=(ScanIterator&&) = default;

    ~ScanIterator() {
      // operator++ reports errors by throwing MultiScanException after setting
      // status_. When that exception unwinds through this destructor, status_
      // is non-ok by design, so only assert when no exception is in flight.
      if (!status_.ok() && std::uncaught_exceptions() == 0) {
        fprintf(stderr, "ScanIterator status: %s\n",
                status_.ToString().c_str());
        assert(false);
      }
    }

    ScanIterator& operator++() {
      if (!valid_) {
        throw std::logic_error("Trying to advance invalid iterator");
      } else {
        if (reverse_) {
          db_iter_->Prev();
        } else {
          db_iter_->Next();
        }
        status_ = db_iter_->status();
        if (!status_.ok()) {
          throw MultiScanException(status_);
        } else {
          valid_ = db_iter_->Valid();
          if (valid_) {
            result_ = value_type(db_iter_->key(), db_iter_->value());
          }
        }
      }
      return *this;
    }

    bool operator==(std::nullptr_t /*other*/) const { return !valid_; }

    bool operator!=(std::nullptr_t /*other*/) const { return valid_; }

    reference operator*() {
      if (!valid_) {
        throw std::logic_error("Trying to deref invalid iterator");
      }
      return result_;
    }
    reference operator->() {
      if (!valid_) {
        throw std::logic_error("Trying to deref invalid iterator");
      }
      return result_;
    }

   private:
    Iterator* db_iter_;
    bool reverse_{};
    bool valid_;
    Status status_;
    value_type result_;
  };

 private:
  Iterator* db_iter_;
  bool reverse_;
};

// A container object encapsulating the scan ranges for a multi scan.
// It supports an std::input_iterator for a single pass iteration of the
// ScanOptions in scan_opts, which can be dereferenced to get the container
// (Scan) for a single range.
// A Status exception is thrown if there is an error.
class MultiScan {
 public:
  MultiScan(const ReadOptions& read_options, const MultiScanArgs& scan_opts,
            DB* db, ColumnFamilyHandle* cfh);

  explicit MultiScan(const Comparator* comp,
                     std::unique_ptr<Iterator>&& db_iter)
      : scan_opts_(comp), db_iter_(std::move(db_iter)) {}

  class MultiScanIterator {
   public:
    MultiScanIterator(const MultiScanIterator&) = delete;
    MultiScanIterator operator=(MultiScanIterator&) = delete;
    MultiScanIterator(MultiScanIterator&&) = delete;
    MultiScanIterator& operator=(MultiScanIterator&&) = delete;

    using self_type = MultiScanIterator;
    using value_type = Scan;
    using reference = Scan&;
    using pointer = Scan*;
    using difference_type = int;
    using iterator_category = std::input_iterator_tag;

    MultiScanIterator(const std::vector<ScanOptions>& scan_opts, DB* db,
                      ColumnFamilyHandle* cfh, ReadOptions& read_options,
                      Slice* lower_bound, Slice* upper_bound,
                      std::unique_ptr<Iterator>& db_iter, bool reverse)
        : scan_opts_(scan_opts),
          db_(db),
          cfh_(cfh),
          read_options_(read_options),
          lower_bound_(lower_bound),
          upper_bound_(upper_bound),
          idx_(0),
          reverse_(reverse),
          valid_(true),
          db_iter_(db_iter),
          scan_(db_iter_.get(), reverse_) {
      if (scan_opts_.empty()) {
        throw std::logic_error("Zero scans in multi-scan");
      }
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw MultiScanException(status_);
      }
      SeekCurrentRange();
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw MultiScanException(status_);
      }
    }

    ~MultiScanIterator() {
      // See ScanIterator::~ScanIterator: a non-ok status_ is expected while a
      // MultiScanException thrown by operator++ is unwinding.
      assert(status_.ok() || std::uncaught_exceptions() > 0);
    }

    MultiScanIterator& operator++();

    bool operator==(std::nullptr_t /*other*/) const { return !valid_; }

    bool operator!=(std::nullptr_t /*other*/) const { return valid_; }

    reference operator*() { return scan_; }
    reference operator->() { return scan_; }

   private:
    const std::vector<ScanOptions>& scan_opts_;
    DB* db_;
    ColumnFamilyHandle* cfh_;
    ReadOptions& read_options_;
    Slice* lower_bound_;
    Slice* upper_bound_;
    size_t idx_;
    bool reverse_;
    bool valid_;
    std::unique_ptr<Iterator>& db_iter_;
    Status status_;
    Scan scan_;

    void SeekCurrentRange();
  };

  MultiScanIterator begin() {
    return MultiScanIterator(scan_opts_.GetScanRanges(), db_, cfh_,
                             read_options_, &lower_bound_, &upper_bound_,
                             db_iter_, scan_opts_.reverse);
  }

  std::nullptr_t end() { return nullptr; }

 private:
  ReadOptions read_options_;
  const MultiScanArgs scan_opts_;
  DB* db_;
  ColumnFamilyHandle* cfh_{};
  Slice lower_bound_;
  Slice upper_bound_;
  std::unique_ptr<Iterator> db_iter_;
};

}  // namespace ROCKSDB_NAMESPACE
