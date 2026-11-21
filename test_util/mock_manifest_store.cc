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

void MockManifestStoreData::SetReadDbRowFailures(int count) {
  MutexLock l(&mu_);
  read_db_row_failures_ = count;
}

void MockManifestStoreData::SetAfterChunkCallback(
    std::function<void()> callback) {
  MutexLock l(&mu_);
  after_chunk_callback_ = std::move(callback);
}

MockManifestStoreData::Fault MockManifestStoreData::PendingFault() const {
  MutexLock lock(&mu_);
  return next_fault_;
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
    std::function<void()> apply = std::move(data_->late_apply_);
    data_->late_apply_ = nullptr;
    apply();
  }
}

MockManifestStore::Fault MockManifestStore::TakeFault() {
  const Fault fault = data_->next_fault_;
  data_->next_fault_ = Fault::kNone;
  return fault;
}

IOStatus MockManifestStore::CheckEpoch() const {
  if (epoch_.has_value() && *epoch_ < data_->stored_epoch_) {
    return IOStatus::IOFenced("A newer epoch claimed the DB");
  }
  return IOStatus::OK();
}

IOStatus MockManifestStore::CheckWriter() const {
  if (!epoch_.has_value()) {
    return IOStatus::NotSupported("A store object without an epoch only reads");
  }
  return CheckEpoch();
}

IOStatus MockManifestStore::CheckExpected(uint64_t expected) const {
  const uint64_t stored =
      data_->has_db_row_ ? data_->state_.commit_sequence : 0;
  if (expected != stored) {
    return IOStatus::Busy("The commit sequence does not match");
  }
  return IOStatus::OK();
}

void MockManifestStore::ApplyDbRow(const std::string& commit_id,
                                   const ManifestRow& db_row) {
  data_->has_db_row_ = true;
  data_->state_.commit_sequence++;
  data_->state_.last_commit_id = commit_id;
  data_->db_row_fields_ = db_row.fields;
}

void MockManifestStore::FillResult(ManifestDbRowResult* result) const {
  result->state = data_->state_;
  result->db_row = ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                               data_->db_row_fields_};
}

IOStatus MockManifestStore::ApplyOpen(const ManifestStoreOpenOptions& opts) {
  if (!data_->has_db_row_) {
    return IOStatus::NotFound("No DB row");
  }
  IOStatus s = CheckWriter();
  std::string fields = data_->db_row_fields_;
  if (s.ok()) {
    s = MergeFields(opts.db_field_updates, &fields);
  }
  if (s.ok()) {
    data_->stored_epoch_ = *epoch_;
    ApplyDbRow(opts.commit_id,
               ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                           std::move(fields)});
  }
  return s;
}

IOStatus MockManifestStore::ApplyCommit(const ManifestCommit& commit) {
  IOStatus s = CheckWriter();
  if (s.ok()) {
    s = CheckExpected(commit.expected_commit_sequence);
  }
  if (!s.ok()) {
    return s;
  }
  if (!data_->has_db_row_) {
    // The creating commit is a claim: it stores the epoch.
    data_->stored_epoch_ = *epoch_;
  }
  for (const ManifestRow& row : commit.upserts) {
    data_->rows_[ToRowKey(row.key)] = row.fields;
  }
  for (const ManifestRowKey& key : commit.deletes) {
    data_->rows_.erase(ToRowKey(key));
  }
  ApplyDbRow(commit.commit_id, commit.db_row);
  return IOStatus::OK();
}

IOStatus MockManifestStore::ApplyWithFault(
    const std::function<IOStatus()>& apply) {
  switch (TakeFault()) {
    case Fault::kTryAgain:
      return status_to_io_status(Status::TryAgain());
    case Fault::kTimedOutNotApplied:
      return IOStatus::TimedOut();
    case Fault::kTimedOutApplied:
      // The caller sees TimedOut. A test reads the real result with
      // HiddenApplyCode().
      data_->hidden_apply_code_ = apply().code();
      return IOStatus::TimedOut();
    case Fault::kTimedOutLateApply: {
      MockManifestStoreData* data = data_.get();
      data_->hidden_apply_code_.reset();
      data_->late_apply_ = [data, apply]() {
        data->hidden_apply_code_ = apply().code();
      };
      return IOStatus::TimedOut();
    }
    case Fault::kNone:
    case Fault::kLoseGenerationRow:
    case Fault::kCorruptGenerationRow:
      break;
  }
  return apply();
}

IOStatus MockManifestStore::Open(const ManifestStoreOpenOptions& opts,
                                 ManifestDbRowResult* result) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s = ApplyWithFault([this, opts]() { return ApplyOpen(opts); });
  if (s.ok()) {
    FillResult(result);
  }
  return s;
}

IOStatus MockManifestStore::Commit(const ManifestCommit& commit) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  return ApplyWithFault([this, commit]() { return ApplyCommit(commit); });
}

IOStatus MockManifestStore::ReadDbRow(ManifestDbRowResult* result) {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s = CheckEpoch();
  if (s.ok() && data_->read_db_row_failures_ > 0) {
    --data_->read_db_row_failures_;
    s = IOStatus::IOError("Injected ReadDbRow() failure");
  }
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
    IOStatus s = CheckEpoch();
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
    IOStatus s = CheckWriter();
    if (!s.ok()) {
      return s;
    }
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
      IOStatus s = CheckEpoch();
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
  IOStatus s = CheckEpoch();
  if (s.ok()) {
    s = CheckExpected(request.expected_commit_sequence);
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
  ApplyDbRow(request.commit_id, request.db_row);
  *generation = new_generation;
  if (fault == Fault::kTimedOutApplied || fault == Fault::kTimedOutLateApply) {
    return IOStatus::TimedOut();
  }
  return IOStatus::OK();
}

IOStatus MockManifestStore::Destroy() {
  MutexLock l(&data_->mu_);
  RunLateApply();
  IOStatus s = CheckWriter();
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
