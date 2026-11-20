//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include <sstream>

#include "db_stress_tool/db_stress_common.h"

namespace ROCKSDB_NAMESPACE {
class BatchedOpsStressTest : public StressTest {
 public:
  BatchedOpsStressTest(int db_index, const std::string& db_path,
                       const std::string& ev_path, const std::string& sec_path)
      : StressTest(db_index, db_path, ev_path, sec_path) {}
  ~BatchedOpsStressTest() override = default;

  bool IsStateTracked() const override { return false; }

  // Given a key K and value V, this puts ("0"+K, V+"0"), ("1"+K, V+"1"), ...,
  // ("9"+K, V+"9") in DB atomically i.e in a single batch.
  // Also refer BatchedOpsStressTest::TestGet
  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& /* read_opts */,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys,
                 char (&value)[100]) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    const std::string key_body = Key(rand_keys[0]);

    const uint32_t value_base = thread->rand.Next();
    const size_t sz = GenerateValue(value_base, value, sizeof(value));
    const std::string value_body = Slice(value, sz).ToString();

    WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                     FLAGS_batch_protection_bytes_per_key,
                     FLAGS_user_timestamp_size);

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    Status status;
    for (int i = 9; i >= 0; --i) {
      const std::string num = std::to_string(i);

      // Note: the digit in num is prepended to the key; however, it is appended
      // to the value because we want the "value base" to be encoded uniformly
      // at the beginning of the value for all types of stress tests (e.g.
      // batched, non-batched, CF consistency).
      const std::string k = num + key_body;
      const std::string v = value_body + num;
      if (FLAGS_use_put_entity_one_in > 0 &&
          (value_base % FLAGS_use_put_entity_one_in) == 0) {
        if (FLAGS_use_attribute_group) {
          status =
              batch.PutEntity(k, GenerateAttributeGroups({cfh}, value_base, v));
        } else {
          status = batch.PutEntity(cfh, k, GenerateWideColumns(value_base, v));
        }
      } else if (FLAGS_use_timed_put_one_in > 0 &&
                 ((value_base + kLargePrimeForCommonFactorSkew) %
                  FLAGS_use_timed_put_one_in) == 0) {
        uint64_t write_unix_time = GetWriteUnixTime(thread);
        status = batch.TimedPut(cfh, k, v, write_unix_time);
      } else if (FLAGS_use_merge) {
        status = batch.Merge(cfh, k, v);
      } else {
        status = batch.Put(cfh, k, v);
      }
      if (!status.ok()) {
        break;
      }
    }

    if (status.ok()) {
      status = db_->Write(write_opts, &batch);
    }

    if (!status.ok()) {
      fprintf(stderr, "multiput error: %s\n", status.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      // we did 10 writes each of size sz + 1
      thread->stats.AddBytesForWrites(10, (sz + 1) * 10);
    }

    return status;
  }

  // Given a key K, this deletes ("0"+K), ("1"+K), ..., ("9"+K)
  // in DB atomically i.e in a single batch. Also refer MultiGet.
  Status TestDelete(ThreadState* thread, WriteOptions& writeoptions,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys) override {
    std::string keys[10] = {"9", "7", "5", "3", "1", "8", "6", "4", "2", "0"};

    WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                     FLAGS_batch_protection_bytes_per_key,
                     FLAGS_user_timestamp_size);
    Status s;
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    for (int i = 0; i < 10; i++) {
      keys[i] += key_str;
      batch.Delete(cfh, keys[i]);
    }

    s = db_->Write(writeoptions, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multidelete error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      thread->stats.AddDeletes(10);
    }

    return s;
  }

  Status TestDeleteRange(ThreadState* /* thread */,
                         WriteOptions& /* write_opts */,
                         const std::vector<int>& /* rand_column_families */,
                         const std::vector<int64_t>& /* rand_keys */) override {
    assert(false);
    return Status::NotSupported(
        "BatchedOpsStressTest does not support "
        "TestDeleteRange");
  }

  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */) override {
    assert(false);
    fprintf(stderr,
            "BatchedOpsStressTest does not support "
            "TestIngestExternalFile\n");
    std::terminate();
  }

  // Given a key K, this gets values for "0"+K, "1"+K, ..., "9"+K
  // in the same snapshot, and verifies that all the values are of the form
  // V+"0", V+"1", ..., V+"9".
  // ASSUMES that BatchedOpsStressTest::TestPut was used to put (K, V) into
  // the DB.
  Status TestGet(ThreadState* thread, const ReadOptions& readoptions,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    std::string keys[10] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
    Slice key_slices[10];
    std::string values[10];
    ReadOptions readoptionscopy = readoptions;
    readoptionscopy.snapshot = db_->GetSnapshot();
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    auto cfh = column_families_[rand_column_families[0]];
    std::string from_db;
    Status s;
    for (int i = 0; i < 10; i++) {
      keys[i] += key.ToString();
      key_slices[i] = keys[i];
      if (i == 0) {
        OutputMetadata output_metadata;
        output_metadata.WantNewerVersionPresent();
        s = db_->GetWithMetadata(readoptionscopy, cfh, key_slices[i], &from_db,
                                 &output_metadata);
        if (s.IsNotSupported()) {
          // Transaction DBs that require custom visibility callbacks cannot
          // provide newer-version metadata. Preserve the ordinary batched
          // stress coverage for those configurations.
          s = DbStressGet(db_, readoptionscopy, cfh, key_slices[i], &from_db);
        }
      } else {
        s = DbStressGet(db_, readoptionscopy, cfh, key_slices[i], &from_db);
      }
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "get error: %s\n", s.ToString().c_str());
        values[i] = "";
        thread->stats.AddErrors(1);
        // we continue after error rather than exiting so that we can
        // find more errors if any
      } else if (s.IsNotFound()) {
        values[i] = "";
        thread->stats.AddGets(1, 0);
      } else {
        values[i] = from_db;

        assert(!keys[i].empty());
        assert(!values[i].empty());

        const char expected = keys[i].front();
        const char actual = values[i].back();

        if (expected != actual) {
          fprintf(stderr, "get error expected = %c actual = %c\n", expected,
                  actual);
        }

        values[i].pop_back();  // get rid of the differing character

        thread->stats.AddGets(1, 1);
      }
    }
    db_->ReleaseSnapshot(readoptionscopy.snapshot);

    // Now that we retrieved all values, check that they all match
    for (int i = 1; i < 10; i++) {
      if (values[i] != values[0]) {
        fprintf(stderr, "get error: inconsistent values for key %s: %s, %s\n",
                key.ToString(true).c_str(), StringToHex(values[0]).c_str(),
                StringToHex(values[i]).c_str());
        // we continue after error rather than exiting so that we can
        // find more errors if any
      }
    }

    return s;
  }

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& readoptions,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    size_t num_keys = rand_keys.size();
    std::vector<Status> ret_status(num_keys);
    std::array<std::string, 10> keys = {
        {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}};
    size_t num_prefixes = keys.size();
    for (size_t rand_key = 0; rand_key < num_keys; ++rand_key) {
      std::vector<Slice> key_slices;
      std::vector<PinnableSlice> values(num_prefixes);
      std::vector<Status> statuses(num_prefixes);
      ReadOptions readoptionscopy = readoptions;
      readoptionscopy.snapshot = db_->GetSnapshot();
      readoptionscopy.rate_limiter_priority =
          FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
      std::vector<std::string> key_str;
      key_str.reserve(num_prefixes);
      key_slices.reserve(num_prefixes);
      std::string from_db;
      ColumnFamilyHandle* cfh = column_families_[rand_column_families[0]];

      for (size_t key = 0; key < num_prefixes; ++key) {
        key_str.emplace_back(keys[key] + Key(rand_keys[rand_key]));
        key_slices.emplace_back(key_str.back());
      }
      MultiGetOutputMetadata output_metadata;
      output_metadata.WantNewerVersionPresent();
      db_->MultiGetWithMetadata(readoptionscopy, cfh, num_prefixes,
                                key_slices.data(), values.data(),
                                statuses.data(), &output_metadata);
      const bool metadata_not_supported = std::all_of(
          statuses.begin(), statuses.end(),
          [](const Status& status) { return status.IsNotSupported(); });
      if (metadata_not_supported) {
        // Preserve compatibility with transaction DBs whose custom visibility
        // callbacks make newer-version tracking unsupported.
        DbStressMultiGet(db_, readoptionscopy, cfh, num_prefixes,
                         key_slices.data(), values.data(), statuses.data());
      }
      for (size_t i = 0; i < num_prefixes; i++) {
        const Status& s = statuses[i];
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "multiget error: %s\n", s.ToString().c_str());
          thread->stats.AddErrors(1);
          ret_status[rand_key] = s;
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (s.IsNotFound()) {
          thread->stats.AddGets(1, 0);
          ret_status[rand_key] = s;
        } else {
          assert(!keys[i].empty());
          assert(!values[i].empty());

          const char expected = keys[i][0];
          const char actual = values[i][values[i].size() - 1];

          if (expected != actual) {
            fprintf(stderr, "multiget error expected = %c actual = %c\n",
                    expected, actual);
          }

          values[i].remove_suffix(1);  // get rid of the differing character

          thread->stats.AddGets(1, 1);
        }
      }
      db_->ReleaseSnapshot(readoptionscopy.snapshot);

      // Now that we retrieved all values, check that they all match
      for (size_t i = 1; i < num_prefixes; i++) {
        if (values[i] != values[0]) {
          fprintf(stderr,
                  "multiget error: inconsistent values for key %s: %s, %s\n",
                  StringToHex(key_str[i]).c_str(),
                  StringToHex(values[0].ToString()).c_str(),
                  StringToHex(values[i].ToString()).c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        }
      }
    }

    return ret_status;
  }

  void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override {
    assert(thread);

    ManagedSnapshot snapshot_guard(db_);

    ReadOptions read_opts_copy(read_opts);
    read_opts_copy.snapshot = snapshot_guard.snapshot();

    assert(!rand_keys.empty());

    const std::string key_suffix = Key(rand_keys[0]);

    assert(!rand_column_families.empty());
    assert(rand_column_families[0] >= 0);
    assert(rand_column_families[0] < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    constexpr size_t num_keys = 10;

    std::array<PinnableWideColumns, num_keys> column_results;
    std::array<PinnableAttributeGroups, num_keys> attribute_group_results;

    std::string error_msg_header = FLAGS_use_attribute_group
                                       ? "GetEntity (AttributeGroup) error"
                                       : "GetEntity error";

    for (size_t i = 0; i < num_keys; ++i) {
      const std::string key = std::to_string(i) + key_suffix;

      Status s;
      if (FLAGS_use_attribute_group) {
        attribute_group_results[i].emplace_back(cfh);
        s = db_->GetEntity(read_opts_copy, key, &attribute_group_results[i]);
        if (s.ok()) {
          s = attribute_group_results[i].back().status();
        }
      } else {
        s = db_->GetEntity(read_opts_copy, cfh, key, &column_results[i]);
      }

      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "%s: %s\n", error_msg_header.c_str(),
                s.ToString().c_str());
        thread->stats.AddErrors(1);
      } else if (s.IsNotFound()) {
        thread->stats.AddGets(1, 0);
      } else {
        thread->stats.AddGets(1, 1);
      }
    }

    const WideColumns& columns_to_compare =
        FLAGS_use_attribute_group ? attribute_group_results[0].front().columns()
                                  : column_results[0].columns();

    for (size_t i = 1; i < num_keys; ++i) {
      const WideColumns& columns =
          FLAGS_use_attribute_group
              ? attribute_group_results[i].front().columns()
              : column_results[i].columns();

      if (!CompareColumns(columns_to_compare, columns)) {
        fprintf(stderr, "%s: inconsistent entities for key %s: %s, %s\n",
                error_msg_header.c_str(), StringToHex(key_suffix).c_str(),
                WideColumnsToHex(columns_to_compare).c_str(),
                WideColumnsToHex(columns).c_str());
      }

      if (!columns.empty()) {
        // The last character of each column value should be 'i' as a decimal
        // digit
        const char expected = static_cast<char>('0' + i);

        for (const auto& column : columns) {
          const Slice& value = column.value();

          if (value.empty() || value[value.size() - 1] != expected) {
            fprintf(stderr,
                    "%s: incorrect column value for key "
                    "%s, entity %s, column value %s, expected %c\n",
                    error_msg_header.c_str(), StringToHex(key_suffix).c_str(),
                    WideColumnsToHex(columns).c_str(),
                    value.ToString(/* hex */ true).c_str(), expected);
          }
        }

        if (!VerifyWideColumns(columns)) {
          fprintf(stderr, "%s: inconsistent columns for key %s, entity %s\n",
                  error_msg_header.c_str(), StringToHex(key_suffix).c_str(),
                  WideColumnsToHex(columns).c_str());
        }
      }
    }

    // Reuse one of the sibling entities we just read (under read_opts_copy's
    // snapshot, and cross-checked above) as the lazy read's reference, rather
    // than issuing another eager read.
    const size_t lazy_idx = thread->rand.Uniform(static_cast<int>(num_keys));
    const std::string lazy_key = std::to_string(lazy_idx) + key_suffix;
    const WideColumns& eager_ref_columns =
        FLAGS_use_attribute_group
            ? attribute_group_results[lazy_idx].front().columns()
            : column_results[lazy_idx].columns();
    const WideColumns* const eager_ref =
        eager_ref_columns.empty() ? nullptr : &eager_ref_columns;
    MaybeTestGetEntityLazy(thread, read_opts_copy, cfh, lazy_key, eager_ref);
  }

  void TestMultiGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                          const std::vector<int>& rand_column_families,
                          const std::vector<int64_t>& rand_keys) override {
    assert(thread);

    assert(!rand_column_families.empty());
    assert(rand_column_families[0] >= 0);
    assert(rand_column_families[0] < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    assert(!rand_keys.empty());

    ManagedSnapshot snapshot_guard(db_);

    ReadOptions read_opts_copy(read_opts);
    read_opts_copy.snapshot = snapshot_guard.snapshot();

    const size_t num_keys = rand_keys.size();

    for (size_t i = 0; i < num_keys; ++i) {
      const std::string key_suffix = Key(rand_keys[i]);

      constexpr size_t num_prefixes = 10;

      std::array<std::string, num_prefixes> keys;
      std::array<Slice, num_prefixes> key_slices;

      for (size_t j = 0; j < num_prefixes; ++j) {
        keys[j] = std::to_string(j) + key_suffix;
        key_slices[j] = keys[j];
      }

      if (FLAGS_use_attribute_group) {
        // AttributeGroup MultiGetEntity verification

        std::vector<PinnableAttributeGroups> results;
        results.reserve(num_prefixes);
        for (size_t j = 0; j < num_prefixes; ++j) {
          PinnableAttributeGroups attribute_groups;
          attribute_groups.emplace_back(cfh);
          results.emplace_back(std::move(attribute_groups));
        }
        db_->MultiGetEntity(read_opts_copy, num_prefixes, key_slices.data(),
                            results.data());

        const WideColumns& cmp_columns = results[0][0].columns();

        for (size_t j = 0; j < num_prefixes; ++j) {
          const auto& attribute_groups = results[j];
          assert(attribute_groups.size() == 1);
          const Status& s = attribute_groups[0].status();
          if (!s.ok() && !s.IsNotFound()) {
            fprintf(stderr, "MultiGetEntity (AttributeGroup) error: %s\n",
                    s.ToString().c_str());
            thread->stats.AddErrors(1);
          } else if (s.IsNotFound()) {
            thread->stats.AddGets(1, 0);
          } else {
            thread->stats.AddGets(1, 1);
          }

          const WideColumns& columns = results[j][0].columns();
          if (!CompareColumns(cmp_columns, columns)) {
            fprintf(stderr,
                    "MultiGetEntity (AttributeGroup) error: inconsistent "
                    "entities for key %s: %s, "
                    "%s\n",
                    StringToHex(key_suffix).c_str(),
                    WideColumnsToHex(cmp_columns).c_str(),
                    WideColumnsToHex(columns).c_str());
          }
          if (!columns.empty()) {
            // The last character of each column value should be 'j' as a
            // decimal digit
            const char expected = static_cast<char>('0' + j);

            for (const auto& column : columns) {
              const Slice& value = column.value();

              if (value.empty() || value[value.size() - 1] != expected) {
                fprintf(stderr,
                        "MultiGetEntity (AttributeGroup) error: incorrect "
                        "column value for key "
                        "%s, entity %s, column value %s, expected %c\n",
                        StringToHex(key_suffix).c_str(),
                        WideColumnsToHex(columns).c_str(),
                        value.ToString(/* hex */ true).c_str(), expected);
              }
            }

            if (!VerifyWideColumns(columns)) {
              fprintf(stderr,
                      "MultiGetEntity (AttributeGroup) error: inconsistent "
                      "columns for key %s, "
                      "entity %s\n",
                      StringToHex(key_suffix).c_str(),
                      WideColumnsToHex(columns).c_str());
            }
          }
        }

        std::vector<EagerEntityRef> eager_refs(num_prefixes);
        for (size_t j = 0; j < num_prefixes; ++j) {
          eager_refs[j].status = results[j][0].status();
          if (eager_refs[j].status.ok()) {
            eager_refs[j].columns = &results[j][0].columns();
          }
        }
        MaybeTestMultiGetEntityLazy(thread, read_opts_copy, cfh, num_prefixes,
                                    key_slices.data(), &eager_refs);
      } else {
        // Non-AttributeGroup MultiGetEntity verification

        std::array<PinnableWideColumns, num_prefixes> results;
        std::array<Status, num_prefixes> statuses;

        db_->MultiGetEntity(read_opts_copy, cfh, num_prefixes,
                            key_slices.data(), results.data(), statuses.data());

        const WideColumns& cmp_columns = results[0].columns();

        for (size_t j = 0; j < num_prefixes; ++j) {
          const Status& s = statuses[j];

          if (!s.ok() && !s.IsNotFound()) {
            fprintf(stderr, "MultiGetEntity error: %s\n", s.ToString().c_str());
            thread->stats.AddErrors(1);
          } else if (s.IsNotFound()) {
            thread->stats.AddGets(1, 0);
          } else {
            thread->stats.AddGets(1, 1);
          }
          const WideColumns& columns = results[j].columns();

          if (!CompareColumns(cmp_columns, columns)) {
            fprintf(
                stderr,
                "MultiGetEntity error: inconsistent entities for key %s: %s, "
                "%s\n",
                StringToHex(key_suffix).c_str(),
                WideColumnsToHex(cmp_columns).c_str(),
                WideColumnsToHex(columns).c_str());
          }

          if (!columns.empty()) {
            // The last character of each column value should be 'j' as a
            // decimal digit
            const char expected = static_cast<char>('0' + j);

            for (const auto& column : columns) {
              const Slice& value = column.value();

              if (value.empty() || value[value.size() - 1] != expected) {
                fprintf(stderr,
                        "MultiGetEntity error: incorrect column value for key "
                        "%s, entity %s, column value %s, expected %c\n",
                        StringToHex(key_suffix).c_str(),
                        WideColumnsToHex(columns).c_str(),
                        value.ToString(/* hex */ true).c_str(), expected);
              }
            }

            if (!VerifyWideColumns(columns)) {
              fprintf(stderr,
                      "MultiGetEntity error: inconsistent columns for key %s, "
                      "entity %s\n",
                      StringToHex(key_suffix).c_str(),
                      WideColumnsToHex(columns).c_str());
            }
          }
        }

        std::vector<EagerEntityRef> eager_refs(num_prefixes);
        for (size_t j = 0; j < num_prefixes; ++j) {
          eager_refs[j].status = statuses[j];
          if (statuses[j].ok()) {
            eager_refs[j].columns = &results[j].columns();
          }
        }
        MaybeTestMultiGetEntityLazy(thread, read_opts_copy, cfh, num_prefixes,
                                    key_slices.data(), &eager_refs);
      }
    }
  }

  // Given a key, this does prefix scans for "0"+P, "1"+P, ..., "9"+P
  // in the same snapshot where P is the first FLAGS_prefix_size - 1 bytes
  // of the key. Each of these 10 scans returns a series of values;
  // each series should be the same length, and it is verified for each
  // index i that all the i'th values are of the form V+"0", V+"1", ..., V+"9".
  // ASSUMES that MultiPut was used to put (K, V)
  Status TestPrefixScan(ThreadState* thread, const ReadOptions& readoptions,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    const std::string key = Key(rand_keys[0]);

    assert(FLAGS_prefix_size > 0);
    const size_t prefix_to_use = static_cast<size_t>(FLAGS_prefix_size);

    constexpr size_t num_prefixes = 10;

    std::array<std::string, num_prefixes> prefixes;
    std::array<Slice, num_prefixes> prefix_slices;
    std::array<ReadOptions, num_prefixes> ro_copies;
    std::array<std::string, num_prefixes> upper_bounds;
    std::array<Slice, num_prefixes> ub_slices;
    std::array<std::function<bool(const TableProperties&)>, num_prefixes>
        table_filters;
    std::array<std::unique_ptr<Iterator>, num_prefixes> iters;

    const Snapshot* const snapshot = db_->GetSnapshot();

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    ScanWitnessLog witness(thread, "prefix_scan_batched");
    if (witness.Enabled()) {
      std::ostringstream start_oss;
      start_oss << "action=start cf=" << rand_column_families[0] << " key={"
                << FormatDiagnosticSlice(Slice(key))
                << "} prefix_count=" << num_prefixes;
      witness.Add(start_oss.str());
    }

    for (size_t i = 0; i < num_prefixes; ++i) {
      prefixes[i] = std::to_string(i) + key;
      prefix_slices[i] = Slice(prefixes[i].data(), prefix_to_use);

      ro_copies[i] = readoptions;
      ro_copies[i].snapshot = snapshot;
      // This verifier compares 10 prefix scans entry-by-entry, so each
      // iterator must be constrained to its seek prefix before we assert
      // lockstep progress across them. Either set an upper bound or
      // prefix_same_as_start so no iterator overshoots its prefix.
      if (GetNextPrefix(prefix_slices[i], &(upper_bounds[i])) &&
          thread->rand.OneIn(2)) {
        // Half the time, bound by upper key
        ub_slices[i] = upper_bounds[i];
        ro_copies[i].iterate_upper_bound = &(ub_slices[i]);
        if (FLAGS_use_sqfc_for_range_queries) {
          table_filters[i] = sqfc_factory_->GetTableFilterForRangeQuery(
              prefix_slices[i], ub_slices[i]);
          ro_copies[i].table_filter = &table_filters[i];
        }
      } else {
        // Otherwise, bound by prefix
        ro_copies[i].prefix_same_as_start = true;
      }

      iters[i].reset(db_->NewIterator(ro_copies[i], cfh));
      iters[i]->Seek(prefix_slices[i]);
      if (witness.Enabled()) {
        std::ostringstream seek_oss;
        seek_oss << "action=seek_setup iterator=" << i << " prefix={"
                 << FormatDiagnosticSlice(prefix_slices[i]) << "} upper_bound={"
                 << (upper_bounds[i].empty()
                         ? "none"
                         : FormatDiagnosticSlice(Slice(upper_bounds[i])))
                 << "} read_options={"
                 << FormatReadOptionsForDiagnostics(ro_copies[i]) << "}";
        witness.Add(seek_oss.str());
      }
      witness.AddIteratorState("Seek", i, *iters[i],
                               !ro_copies[i].allow_unprepared_value);
    }

    uint64_t count = 0;

    while (iters[0]->Valid() && iters[0]->key().starts_with(prefix_slices[0])) {
      ++count;

      std::array<std::string, num_prefixes> values;

      // get list of all values for this iteration
      for (size_t i = 0; i < num_prefixes; ++i) {
        // no iterator should finish before the first one
        if (!(iters[i]->Valid() &&
              iters[i]->key().starts_with(prefix_slices[i]))) {
          witness.Add("failure=prefix_iterator_finished_early iterator=" +
                      std::to_string(i) + " prefix={" +
                      FormatDiagnosticSlice(prefix_slices[i]) + "}");
          witness.AddIteratorState("IteratorFinishedEarly", i, *iters[i],
                                   !ro_copies[i].allow_unprepared_value);
          witness.Flush("prefix_scan_batched_iterator_finished_early");
        }
        assert(iters[i]->Valid() &&
               iters[i]->key().starts_with(prefix_slices[i]));

        if (ro_copies[i].allow_unprepared_value) {
          // Save key in case PrepareValue fails and invalidates the iterator
          const std::string prepare_value_key =
              iters[i]->key().ToString(/* hex */ true);
          witness.AddIteratorState("PrepareValueBefore", i, *iters[i], false);

          if (!iters[i]->PrepareValue()) {
            witness.AddIteratorState("PrepareValueFailed", i, *iters[i], false);
            witness.Flush("prefix_scan_batched_prepare_value");
            fprintf(stderr,
                    "prefix scan error: PrepareValue failed for key %s: %s\n",
                    prepare_value_key.c_str(),
                    iters[i]->status().ToString().c_str());
            continue;
          }
          witness.AddIteratorState("PrepareValue", i, *iters[i], true);
        }

        values[i] = iters[i]->value().ToString();

        // make sure the last character of the value is the expected digit
        assert(!prefixes[i].empty());
        assert(!values[i].empty());

        const char expected = prefixes[i].front();
        const char actual = values[i].back();

        if (expected != actual) {
          witness.Add("failure=prefix_value_suffix_mismatch iterator=" +
                      std::to_string(i) +
                      " expected=" + std::string(1, expected) +
                      " actual=" + std::string(1, actual));
          witness.AddIteratorState("ValueSuffixMismatch", i, *iters[i], true);
          witness.Flush("prefix_scan_batched_value_suffix");
          fprintf(stderr, "prefix scan error expected = %c actual = %c\n",
                  expected, actual);
        }

        values[i].pop_back();  // get rid of the differing character

        // make sure all values are equivalent
        if (values[i] != values[0]) {
          witness.Add("failure=prefix_values_inconsistent iterator=" +
                      std::to_string(i) + " prefix={" +
                      FormatDiagnosticSlice(prefix_slices[i]) + "}");
          witness.AddIteratorState("ValueMismatch", i, *iters[i], true);
          witness.Flush("prefix_scan_batched_value_mismatch");
          fprintf(stderr,
                  "prefix scan error : %" ROCKSDB_PRIszt
                  ", inconsistent values for prefix %s: %s, %s\n",
                  i, prefix_slices[i].ToString(/* hex */ true).c_str(),
                  StringToHex(values[0]).c_str(),
                  StringToHex(values[i]).c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        }

        // make sure value() and columns() are consistent
        if (!VerifyWideColumns(iters[i]->value(), iters[i]->columns())) {
          witness.Add("failure=prefix_wide_columns_inconsistent iterator=" +
                      std::to_string(i) + " prefix={" +
                      FormatDiagnosticSlice(prefix_slices[i]) + "}");
          witness.AddIteratorState("WideColumnsMismatch", i, *iters[i], true);
          witness.Flush("prefix_scan_batched_wide_columns");
          fprintf(stderr,
                  "prefix scan error : %" ROCKSDB_PRIszt
                  ", value and columns inconsistent for prefix %s: value: %s, "
                  "columns: %s\n",
                  i, prefix_slices[i].ToString(/* hex */ true).c_str(),
                  iters[i]->value().ToString(/* hex */ true).c_str(),
                  WideColumnsToHex(iters[i]->columns()).c_str());
        }

        iters[i]->Next();
        witness.AddIteratorState("Next", i, *iters[i],
                                 !ro_copies[i].allow_unprepared_value);
      }
    }

    // cleanup iterators and snapshot
    for (size_t i = 0; i < num_prefixes; ++i) {
      // if the first iterator finished, they should have all finished
      if (!(!iters[i]->Valid() ||
            !iters[i]->key().starts_with(prefix_slices[i]))) {
        witness.Add("failure=prefix_iterator_not_exhausted iterator=" +
                    std::to_string(i) + " prefix={" +
                    FormatDiagnosticSlice(prefix_slices[i]) + "}");
        witness.AddIteratorState("IteratorNotExhausted", i, *iters[i],
                                 !ro_copies[i].allow_unprepared_value);
        witness.Flush("prefix_scan_batched_iterator_not_exhausted");
      }
      assert(!iters[i]->Valid() ||
             !iters[i]->key().starts_with(prefix_slices[i]));
      if (!iters[i]->status().ok()) {
        witness.Add(
            "failure=prefix_iterator_status iterator=" + std::to_string(i) +
            " status=" + iters[i]->status().ToString());
        witness.AddIteratorState("IteratorStatus", i, *iters[i],
                                 !ro_copies[i].allow_unprepared_value);
        witness.Flush("prefix_scan_batched_status");
      }
      DB_STRESS_ASSERT_OK(iters[i]->status());
    }

    db_->ReleaseSnapshot(snapshot);

    thread->stats.AddPrefixes(1, count);

    return Status::OK();
  }

  void VerifyDb(ThreadState* /* thread */) const override {}

  void ContinuouslyVerifyDb(ThreadState* /* thread */) const override {}

  // Compare columns ignoring the last character of column values
  bool CompareColumns(const WideColumns& lhs, const WideColumns& rhs) {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    for (size_t i = 0; i < lhs.size(); ++i) {
      if (lhs[i].name() != rhs[i].name()) {
        return false;
      }

      if (lhs[i].value().size() != rhs[i].value().size()) {
        return false;
      }

      if (lhs[i].value().difference_offset(rhs[i].value()) <
          lhs[i].value().size() - 1) {
        return false;
      }
    }

    return true;
  }
};

StressTest* CreateBatchedOpsStressTest(int db_index, const std::string& db_path,
                                       const std::string& ev_path,
                                       const std::string& sec_path) {
  return new BatchedOpsStressTest(db_index, db_path, ev_path, sec_path);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
