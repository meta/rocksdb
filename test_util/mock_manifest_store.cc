//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/mock_manifest_store.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "util/cast_util.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::tuple<uint8_t, uint32_t, uint64_t> ToRowKey(const ManifestRowKey& key) {
  return {lossless_cast<uint8_t>(key.type), key.cf_id, key.number};
}

// A checksum of a set of rows: a sum of row hashes, so a real store can
// compute it while it writes chunks in any order.
uint64_t RowsChecksum(const std::map<std::tuple<uint8_t, uint32_t, uint64_t>,
                                     std::string>& rows) {
  uint64_t sum = 0;
  for (const auto& [key, fields] : rows) {
    std::string bytes;
    bytes.push_back(static_cast<char>(std::get<0>(key)));
    bytes.append(std::to_string(std::get<1>(key)) + "/" +
                 std::to_string(std::get<2>(key)) + "/");
    bytes.append(fields);
    sum += Hash64(bytes.data(), bytes.size(), /*seed=*/0);
  }
  return sum;
}

// Sets each field of `updates` in `fields`: replaces the field with the same
// ID, or adds the field if `fields` has none. An empty update value removes
// the field.
IOStatus MergeFields(const std::string& updates, std::string* fields) {
  std::vector<ManifestRowField> old_fields;
  std::vector<ManifestRowField> new_fields;
  Status s = ParseManifestRowFields(*fields, &old_fields);
  if (s.ok()) {
    s = ParseManifestRowFields(updates, &new_fields);
  }
  if (!s.ok()) {
    return status_to_io_status(std::move(s));
  }
  std::map<uint32_t, Slice> merged;
  for (const ManifestRowField& f : old_fields) {
    merged[f.id] = f.value;
  }
  for (const ManifestRowField& f : new_fields) {
    if (f.value.empty()) {
      merged.erase(f.id);
    } else {
      merged[f.id] = f.value;
    }
  }
  std::vector<ManifestRowField> out;
  for (const auto& [id, value] : merged) {
    out.push_back({id, value});
  }
  *fields = EncodeManifestRowFields(out);
  return IOStatus::OK();
}

}  // namespace

void MockManifestStoreData::SetNextFault(Fault fault) {
  MutexLock l(&mu_);
  next_fault_ = fault;
}

void MockManifestStoreData::SetAfterChunkCallback(
    std::function<void()> callback) {
  MutexLock l(&mu_);
  after_chunk_callback_ = std::move(callback);
}

uint64_t MockManifestStoreData::StoredEpoch() const {
  MutexLock l(&mu_);
  return stored_epoch_;
}

std::optional<Status::Code> MockManifestStoreData::HiddenApplyCode() const {
  MutexLock l(&mu_);
  return hidden_apply_code_;
}

MockManifestStore::MockManifestStore(
    std::shared_ptr<MockManifestStoreData> data, std::optional<uint64_t> epoch)
    : data_(std::move(data)), epoch_(epoch) {}

void MockManifestStore::RunLateApply() {
  if (data_->late_apply_) {
    std::function<void(MockManifestStoreData*)> apply =
        std::move(data_->late_apply_);
    data_->late_apply_ = nullptr;
    apply(data_.get());
  }
}

MockManifestStore::Fault MockManifestStore::TakeFault() {
  const Fault fault = data_->next_fault_;
  data_->next_fault_ = Fault::kNone;
  return fault;
}

IOStatus MockManifestStore::CheckEpoch(const MockManifestStoreData& data,
                                       const std::optional<uint64_t>& epoch) {
  if (epoch.has_value() && *epoch < data.stored_epoch_) {
    return IOStatus::IOFenced("A newer epoch claimed the DB");
  }
  return IOStatus::OK();
}

IOStatus MockManifestStore::CheckWriter(const MockManifestStoreData& data,
                                        const std::optional<uint64_t>& epoch) {
  if (!epoch.has_value()) {
    return IOStatus::NotSupported("A store object without an epoch only reads");
  }
  return CheckEpoch(data, epoch);
}

IOStatus MockManifestStore::CheckExpected(const MockManifestStoreData& data,
                                          uint64_t expected) {
  const uint64_t stored = data.has_db_row_ ? data.state_.commit_sequence : 0;
  if (expected != stored) {
    return IOStatus::Busy("The commit sequence does not match");
  }
  return IOStatus::OK();
}

IOStatus MockManifestStore::CheckPrecondition(
    const MockManifestStoreData& data, const std::optional<uint64_t>& expected,
    const std::string& expected_id) {
  if (expected.has_value()) {
    IOStatus s = CheckExpected(data, *expected);
    if (!s.ok()) {
      return s;
    }
  }
  if (!expected_id.empty() &&
      (!data.has_db_row_ || data.state_.last_commit_id != expected_id)) {
    return IOStatus::Busy("The last commit id does not match");
  }
  return IOStatus::OK();
}

void MockManifestStore::ApplyDbRow(MockManifestStoreData* data,
                                   const std::string& commit_id,
                                   const ManifestRow& db_row) {
  data->has_db_row_ = true;
  data->state_.commit_sequence++;
  data->state_.last_commit_id = commit_id;
  data->db_row_fields_ = db_row.fields;
}

void MockManifestStore::FillResult(ManifestDbRowResult* result) const {
  result->state = data_->state_;
  result->db_row = ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                               data_->db_row_fields_};
}

IOStatus MockManifestStore::ApplyOpen(MockManifestStoreData* data,
                                      const std::optional<uint64_t>& epoch,
                                      const ManifestStoreOpenOptions& opts) {
  if (!data->has_db_row_) {
    return IOStatus::NotFound("No DB row");
  }
  IOStatus s = CheckWriter(*data, epoch);
  if (s.ok()) {
    s = CheckPrecondition(*data, opts.expected_commit_sequence,
                          opts.expected_last_commit_id);
  }
  std::string fields = data->db_row_fields_;
  if (s.ok()) {
    s = MergeFields(opts.db_field_updates, &fields);
  }
  if (s.ok()) {
    data->stored_epoch_ = *epoch;
    ApplyDbRow(data, opts.commit_id,
               ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                           std::move(fields)});
  }
  return s;
}

IOStatus MockManifestStore::ApplyCommit(MockManifestStoreData* data,
                                        const std::optional<uint64_t>& epoch,
                                        const ManifestCommit& commit) {
  IOStatus s = CheckWriter(*data, epoch);
  if (s.ok()) {
    s = CheckExpected(*data, commit.expected_commit_sequence);
  }
  if (!s.ok()) {
    return s;
  }
  if (!data->has_db_row_) {
    // The creating commit is a claim: it stores the epoch.
    data->stored_epoch_ = *epoch;
  }
  for (const ManifestRow& row : commit.upserts) {
    data->rows_[ToRowKey(row.key)] = row.fields;
  }
  for (const ManifestRowKey& key : commit.deletes) {
    data->rows_.erase(ToRowKey(key));
  }
  ApplyDbRow(data, commit.commit_id, commit.db_row);
  return IOStatus::OK();
}

IOStatus MockManifestStore::ApplyWithFault(const ApplyFunction& apply) {
  switch (TakeFault()) {
    case Fault::kTryAgain:
      return status_to_io_status(Status::TryAgain());
    case Fault::kTimedOutNotApplied:
      return IOStatus::TimedOut();
    case Fault::kTimedOutApplied:
      // The caller sees TimedOut. A test reads the real result with
      // HiddenApplyCode().
      data_->hidden_apply_code_ = apply(data_.get()).code();
      return IOStatus::TimedOut();
    case Fault::kTimedOutLateApply:
      // The pending call gets the data as an argument when it runs, so it
      // keeps no pointer to this object and no reference to the data.
      data_->hidden_apply_code_.reset();
      data_->late_apply_ = [apply](MockManifestStoreData* data) {
        data->hidden_apply_code_ = apply(data).code();
      };
      return IOStatus::TimedOut();
    case Fault::kNone:
    case Fault::kLoseGenerationRow:
    case Fault::kCorruptGenerationRow:
      break;
  }
  return apply(data_.get());
}

IOStatus MockManifestStore::Open(const ManifestStoreOpenOptions& opts,
                                 ManifestDbRowResult* result) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s =
      ApplyWithFault([epoch = epoch_, opts](MockManifestStoreData* data) {
        return ApplyOpen(data, epoch, opts);
      });
  if (s.ok()) {
    FillResult(result);
  }
  return s;
}

IOStatus MockManifestStore::Commit(const ManifestCommit& commit) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  return ApplyWithFault([epoch = epoch_, commit](MockManifestStoreData* data) {
    return ApplyCommit(data, epoch, commit);
  });
}

IOStatus MockManifestStore::ReadDbRow(ManifestDbRowResult* result) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s = CheckEpoch(*data_, epoch_);
  if (s.ok() && !data_->has_db_row_) {
    s = IOStatus::NotFound("No DB row");
  }
  if (s.ok()) {
    FillResult(result);
  }
  return s;
}

IOStatus MockManifestStore::Scan(const ManifestScanOptions& /*opts*/,
                                 ManifestRowVisitor* visitor) {
  ManifestDbRowResult db_row;
  std::vector<ManifestRow> rows;
  {
    MutexLock l(&data_->mu_);
    RunLateApply();
    IOStatus s = CheckEpoch(*data_, epoch_);
    if (s.ok() && !data_->has_db_row_) {
      s = IOStatus::NotFound("No DB row");
    }
    if (!s.ok()) {
      return s;
    }
    FillResult(&db_row);
    for (const auto& [key, fields] : data_->rows_) {
      rows.push_back(ManifestRow{
          ManifestRowKey{static_cast<ManifestRowType>(std::get<0>(key)),
                         std::get<1>(key), std::get<2>(key)},
          fields});
    }
  }
  // The visitor runs without the lock, so it may call the store.
  Status s = visitor->OnDbRow(db_row);
  for (size_t i = 0; s.ok() && i < rows.size(); ++i) {
    s = visitor->OnRow(rows[i]);
  }
  return status_to_io_status(std::move(s));
}

IOStatus MockManifestStore::WriteGeneration(const ManifestGeneration& request,
                                            uint64_t* generation) {
  uint64_t new_generation = 0;
  Fault fault = Fault::kNone;
  std::function<void()> after_chunk;
  {
    MutexLock l(&data_->mu_);
    RunLateApply();
    IOStatus s = CheckWriter(*data_, epoch_);
    if (!s.ok()) {
      return s;
    }
    // WriteGeneration() does not use ApplyWithFault(): it applies the switch
    // itself and treats kTimedOutLateApply as kTimedOutApplied (see Fault in
    // the header).
    fault = TakeFault();
    if (fault == Fault::kTryAgain) {
      return status_to_io_status(Status::TryAgain());
    }
    if (fault == Fault::kTimedOutNotApplied) {
      return IOStatus::TimedOut();
    }
    new_generation = ++data_->generation_counter_;
    data_->pending_generations_[new_generation];
    after_chunk = data_->after_chunk_callback_;
  }
  // Write the rows in chunks. Each chunk checks the epoch, so a writer whose
  // epoch is no longer the newest cannot add rows.
  for (size_t start = 0; start < request.rows.size();
       start += kGenerationChunkRows) {
    {
      MutexLock l(&data_->mu_);
      IOStatus s = CheckEpoch(*data_, epoch_);
      if (!s.ok()) {
        data_->pending_generations_.erase(new_generation);
        return s;
      }
      const size_t end =
          std::min(request.rows.size(), start + kGenerationChunkRows);
      for (size_t i = start; i < end; ++i) {
        data_->pending_generations_[new_generation]
                                   [ToRowKey(request.rows[i].key)] =
            request.rows[i].fields;
      }
    }
    if (after_chunk) {
      after_chunk();
    }
  }

  MutexLock l(&data_->mu_);
  MockManifestStoreData::RowMap written =
      std::move(data_->pending_generations_[new_generation]);
  data_->pending_generations_.erase(new_generation);
  if (!written.empty()) {
    if (fault == Fault::kLoseGenerationRow) {
      written.erase(std::prev(written.end()));
    } else if (fault == Fault::kCorruptGenerationRow) {
      std::prev(written.end())->second.append("x");
    }
  }
  IOStatus s = CheckEpoch(*data_, epoch_);
  if (s.ok()) {
    s = CheckExpected(*data_, request.expected_commit_sequence);
  }
  if (!s.ok()) {
    return s;
  }
  // The switch checks the row count and the checksum of the rows, so that a
  // generation with lost or changed rows never becomes current. Such a switch
  // is not applied, never will be, and is safe to retry.
  MockManifestStoreData::RowMap given;
  for (const ManifestRow& row : request.rows) {
    given[ToRowKey(row.key)] = row.fields;
  }
  if (written.size() != given.size() ||
      RowsChecksum(written) != RowsChecksum(given)) {
    return status_to_io_status(
        Status::TryAgain("Generation rows are lost or changed"));
  }
  data_->rows_ = std::move(written);
  data_->state_.generation = new_generation;
  ApplyDbRow(data_.get(), request.commit_id, request.db_row);
  *generation = new_generation;
  if (fault == Fault::kTimedOutApplied || fault == Fault::kTimedOutLateApply) {
    return IOStatus::TimedOut();
  }
  return IOStatus::OK();
}

IOStatus MockManifestStore::Destroy(const ManifestStoreDestroyOptions& opts) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s = CheckWriter(*data_, epoch_);
  if (s.ok()) {
    s = CheckPrecondition(*data_, opts.expected_commit_sequence,
                          opts.expected_last_commit_id);
  }
  if (s.ok()) {
    data_->has_db_row_ = false;
    data_->state_ = ManifestStoreDbState();
    data_->db_row_fields_.clear();
    data_->rows_.clear();
    data_->pending_generations_.clear();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
