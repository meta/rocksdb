//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_store_shadow.h"

#include <algorithm>
#include <cinttypes>
#include <deque>
#include <initializer_list>

#include "db/version_set.h"
#include "logging/logging.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// The retry wait doubles at most this many times: 2^6 = 64 times the base
// wait, about a minute with the default bgerror_resume_retry_interval of one
// second.
constexpr uint32_t kMaxRetryWaitShift = 6;

// The job slots of all shadow DBs of the process, with the queue of the DBs
// that wait for a slot (see "Job slots" at ManifestStoreShadow). Release()
// hands the slot straight to the first waiting DB, so the queue is empty
// whenever a slot is free.
//
// States of one shadow DB:
// - Idle: no job, not in the queue.
// - Queued: in `queue_`. It needed a job while no slot was free.
// - In delivery: in `delivering_`. It holds a slot that a job gave it, but has
//   not scheduled its job yet.
// - Holding: `job_scheduled_` of the DB is true. Its job is queued in the Env
//   or running.
//
// Hand-off from DB A, whose job ends, to DB B, the first waiting DB:
//
//   thread of A's job                       ShadowJobSlots
//   ----------------------------------      ---------------------------------
//   RunJob() ends, holds A's mutex_
//   ReleaseJobSlotLocked() -> Release()     B: queued -> in delivery
//   unlocks A's mutex_
//   GiveSlotTo(B) -> B->TakeGivenSlot()
//     locks B's mutex_
//     schedules B's job                     B: in delivery -> holding
//     (or, when B no longer needs a job,
//      Release() for the next DB)
//     FinishDelivery(B)                     B leaves `delivering_`
//     unlocks B's mutex_
//
// Release() runs with A's mutex_ held, so it only records the delivery. The
// slot is delivered without any ManifestStoreShadow::mutex_ held, because B
// must take its own mutex_ to schedule its job, and a DB never takes another
// DB's mutex_ while it holds its own. B ends the delivery while it still holds
// its mutex_, so its job cannot end before the delivery does. Remove() waits
// until a delivery to its DB has finished, so that Close() never returns while
// another thread is about to use the DB.
class ShadowJobSlots {
 public:
  // Never destroyed, so that a job that ends during static destruction can
  // still give back its slot.
  static ShadowJobSlots& Get() {
    static ShadowJobSlots* const slots = new ShadowJobSlots();
    return *slots;
  }

  // Takes a slot for `shadow` when one is free. Otherwise adds `shadow` to the
  // back of the queue, unless it is in the queue or a slot is in delivery to
  // it, and returns false.
  bool TryAcquire(ManifestStoreShadow* shadow) {
    MutexLock lock(&mutex_);
    if (used_ < ManifestStoreShadow::kMaxJobsPerProcess) {
      assert(queue_.empty());
      ++used_;
      return true;
    }
    if (!IsWaitingLocked(shadow)) {
      queue_.push_back(shadow);
    }
    return false;
  }

  // Gives back one slot. Returns the first waiting DB, which now holds the
  // slot, or null when no DB waits.
  ManifestStoreShadow* Release() {
    MutexLock lock(&mutex_);
    assert(used_ > 0);
    if (queue_.empty()) {
      --used_;
      return nullptr;
    }
    ManifestStoreShadow* const next = queue_.front();
    queue_.pop_front();
    delivering_.insert(next);
    return next;
  }

  // Called by `shadow` when it has taken the slot that was delivered to it.
  void FinishDelivery(ManifestStoreShadow* shadow) {
    MutexLock lock(&mutex_);
    delivering_.erase(shadow);
    cv_.SignalAll();
  }

  // Removes `shadow` from the queue, and waits until no slot is in delivery to
  // it. REQUIRES: the mutex of `shadow` is not held.
  void Remove(ManifestStoreShadow* shadow) {
    MutexLock lock(&mutex_);
    queue_.erase(std::remove(queue_.begin(), queue_.end(), shadow),
                 queue_.end());
    while (delivering_.count(shadow) > 0) {
      cv_.Wait();
    }
  }

  // True while `shadow` is in the queue or a slot is in delivery to it.
  bool IsWaiting(ManifestStoreShadow* shadow) {
    MutexLock lock(&mutex_);
    return IsWaitingLocked(shadow);
  }

  // Waits until IsWaiting(`shadow`) is false. For tests.
  void WaitUntilNotWaiting(ManifestStoreShadow* shadow) {
    MutexLock lock(&mutex_);
    while (IsWaitingLocked(shadow)) {
      cv_.Wait();
    }
  }

 private:
  // The queue has one entry for each waiting shadow DB of the process, so a
  // linear search is cheap.
  bool IsWaitingLocked(ManifestStoreShadow* shadow) const {
    mutex_.AssertHeld();
    return std::find(queue_.begin(), queue_.end(), shadow) != queue_.end() ||
           delivering_.count(shadow) > 0;
  }

  mutable port::Mutex mutex_;
  port::CondVar cv_{&mutex_};
  int used_ = 0;
  // The waiting DBs, first in first out.
  std::deque<ManifestStoreShadow*> queue_;
  UnorderedSet<ManifestStoreShadow*> delivering_;
};

// Returns why shadow mode must not claim or write a store whose DB row is
// `db_row`, or an empty string when it may. `db_id` is the id of this DB.
// Shadow mode must not write to a store whose DB row CheckManifestDbRowOwner()
// rejects. This happens in two cases. First, the row does not decode, for
// example because a newer binary wrote an authority value that this release
// does not know. Then RocksDB cannot tell who owns the row. Second, the row has
// another DB id, because another DB uses the same store key. Shadow mode also
// must not write to a store that holds a DB in store mode (authority kStore).
std::string ReasonNotToWrite(const ManifestRow& db_row,
                             const std::string& db_id) {
  ManifestDbRowExtraFields extra;
  const Status s =
      CheckManifestDbRowOwner(db_row, db_id, /*rows=*/nullptr, &extra);
  if (!s.ok()) {
    return "the DB row of the store cannot be used: " + s.ToString();
  }
  if (extra.authority == ManifestAuthority::kStore) {
    return "the store holds the DB in store mode";
  }
  return std::string();
}

// Returns true if the encoded column family row has the dropped flag.
bool IsDroppedCfRow(const std::string& cf_row) {
  std::vector<ManifestRowField> fields;
  if (!ParseManifestRowFields(cf_row, &fields).ok()) {
    return false;
  }
  for (const ManifestRowField& field : fields) {
    if (field.id == lossless_cast<uint32_t>(ManifestCfField::kDropped)) {
      return field.value == Slice("\x01", 1);
    }
  }
  return false;
}

// The fields that may differ between the rows of the old MANIFEST file and
// the snapshot of a new one (see ManifestStoreShadow::ReplaceWithSnapshot()).
std::vector<uint32_t> KnownDifferentFields(ManifestRowType type) {
  switch (type) {
    case ManifestRowType::kColumnFamily:
      return {lossless_cast<uint32_t>(ManifestCfField::kLogNumber)};
    case ManifestRowType::kFile:
      return {lossless_cast<uint32_t>(ManifestFileField::kMarkedForCompaction)};
    case ManifestRowType::kDb:
    case ManifestRowType::kBlobFile:
    case ManifestRowType::kWal:
      break;
  }
  return {};
}

}  // namespace

ManifestStoreShadow::ManifestStoreShadow(std::shared_ptr<ManifestStore> store,
                                         Env* env, SystemClock* clock,
                                         std::shared_ptr<Logger> info_log,
                                         uint32_t feature_version,
                                         uint64_t retry_interval_us,
                                         const std::string& db_session_id)
    : store_(std::move(store)),
      env_(env),
      clock_(clock),
      info_log_(std::move(info_log)),
      feature_version_(feature_version),
      retry_interval_us_(retry_interval_us),
      commit_ids_(db_session_id),
      cv_(&mutex_),
      rows_(std::make_shared<RowMap>()) {
  extra_fields_.authority = ManifestAuthority::kShadow;
}

ManifestStoreShadow::~ManifestStoreShadow() {
  MutexLock lock(&mutex_);
  assert(!job_scheduled_);
}

void ManifestStoreShadow::OwnRowsAndLock() {
  mutex_.Lock();
  if (!writer_owns_) {
    const std::shared_ptr<RowMap> shared = rows_;
    mutex_.Unlock();
    // Only this thread replaces rows_ or sets `writer_owns_`, so `shared` is
    // still current when the copy is installed. The copy holds the same row
    // pointers.
    std::shared_ptr<RowMap> copy = std::make_shared<RowMap>(*shared);
    mutex_.Lock();
    rows_ = std::move(copy);
    writer_owns_ = true;
  }
}

void ManifestStoreShadow::MarkChangedLocked(const ManifestRowKey& key,
                                            bool had_row, bool has_row) {
  mutex_.AssertHeld();
  const auto it = changed_.find(key);
  if (it == changed_.end()) {
    if (had_row || has_row) {
      changed_.emplace(key, /*store_has_row=*/had_row);
    }
  } else if (!it->second && !has_row) {
    changed_.erase(it);
  }
}

void ManifestStoreShadow::ReplaceWithSnapshot(std::vector<ManifestRow>&& rows) {
  assert(!rows.empty() && rows[0].key.type == ManifestRowType::kDb);
  std::shared_ptr<RowMap> old_rows;
  bool had_rows = false;
  {
    MutexLock lock(&mutex_);
    old_rows = rows_;
    had_rows = has_rows_;
  }
  // Build the new map outside the mutex. Rows that did not change keep their
  // old pointer, so they use no new memory.
  std::shared_ptr<RowMap> new_rows = std::make_shared<RowMap>();
  std::vector<ManifestRowKey> changed;
  for (size_t i = 1; i < rows.size(); ++i) {
    const ManifestRowKey& key = rows[i].key;
    const RowMap::const_iterator it = old_rows->find(key);
    if (it != old_rows->end() && *it->second == rows[i].fields) {
      new_rows->emplace(key, it->second);
    } else {
      new_rows->emplace(
          key, std::make_shared<const std::string>(std::move(rows[i].fields)));
      changed.push_back(key);
    }
  }
  for (const auto& [key, row] : *old_rows) {
    if (new_rows->count(key) == 0) {
      changed.push_back(key);
    }
  }
  if (had_rows) {
    LogUnexpectedDifferences(*old_rows, *new_rows);
  }

  MutexLock lock(&mutex_);
  for (const ManifestRowKey& key : changed) {
    MarkChangedLocked(key, old_rows->count(key) > 0, new_rows->count(key) > 0);
  }
  rows_ = std::move(new_rows);
  writer_owns_ = true;
}

void ManifestStoreShadow::LogUnexpectedDifferences(const RowMap& old_rows,
                                                   const RowMap& new_rows) {
  UnorderedSet<uint32_t> dropped_cfs;
  for (const auto& [key, row] : old_rows) {
    if (key.type == ManifestRowType::kColumnFamily && IsDroppedCfRow(*row)) {
      dropped_cfs.insert(key.cf_id);
    }
  }
  auto report = [&](const ManifestRowKey& key, const std::string* old_row,
                    const std::string* new_row) {
    std::string text = DescribeManifestRowDifference(
        key, old_row, new_row, "old MANIFEST file", "new MANIFEST file",
        KnownDifferentFields(key.type));
    if (text.empty()) {
      return;
    }
    ROCKS_LOG_WARN(info_log_,
                   "Shadow mode: the rows of the new MANIFEST file differ "
                   "from the rows of the old one: %s",
                   text.c_str());
    TEST_SYNC_POINT_CALLBACK("ManifestStoreShadow::UnexpectedDifference",
                             &text);
  };
  for (const auto& [key, row] : old_rows) {
    const RowMap::const_iterator it = new_rows.find(key);
    if (it == new_rows.end()) {
      // A new MANIFEST file leaves out a dropped column family and the WALs
      // below the min log number to keep.
      if (key.type != ManifestRowType::kWal &&
          dropped_cfs.count(key.cf_id) == 0) {
        report(key, row.get(), nullptr);
      }
    } else {
      report(key, row.get(), it->second.get());
    }
  }
  for (const auto& [key, row] : new_rows) {
    if (old_rows.count(key) == 0) {
      report(key, nullptr, row.get());
    }
  }
}

void ManifestStoreShadow::Apply(ManifestCommit&& changes) {
  // Allocate the rows outside the mutex.
  std::vector<std::pair<ManifestRowKey, EncodedRow>> upserts;
  upserts.reserve(changes.upserts.size());
  for (ManifestRow& row : changes.upserts) {
    upserts.emplace_back(
        row.key, std::make_shared<const std::string>(std::move(row.fields)));
  }
  OwnRowsAndLock();
  for (std::pair<ManifestRowKey, EncodedRow>& upsert : upserts) {
    MarkChangedLocked(upsert.first, rows_->count(upsert.first) > 0,
                      /*has_row=*/true);
    (*rows_)[upsert.first] = std::move(upsert.second);
  }
  for (const ManifestRowKey& key : changes.deletes) {
    MarkChangedLocked(key, rows_->erase(key) > 0, /*has_row=*/false);
  }
  db_row_ = std::move(changes.db_row.fields);
  db_row_changed_ = true;
  // The first Apply() follows the first ReplaceWithSnapshot(), so the rows and
  // the DB row are complete from here on.
  has_rows_ = true;
  MaybeScheduleLocked();
  mutex_.Unlock();
}

void ManifestStoreShadow::Start(const std::string& db_id) {
  MutexLock lock(&mutex_);
  db_id_ = db_id;
  started_ = true;
  MaybeScheduleLocked();
}

bool ManifestStoreShadow::NeedsJobLocked() const {
  mutex_.AssertHeld();
  if (!started_ || closing_ || Stopped() || job_scheduled_ || !has_rows_) {
    return false;
  }
  if (!need_full_copy_ && changed_.empty() && !db_row_changed_) {
    return false;
  }
  return !need_full_copy_ || failures_ == 0 ||
         clock_->NowMicros() >= retry_after_us_;
}

void ManifestStoreShadow::MaybeScheduleLocked() {
  mutex_.AssertHeld();
  if (!NeedsJobLocked() || !ShadowJobSlots::Get().TryAcquire(this)) {
    // A DB that found the slot taken is in the queue now; the rows keep
    // collecting the changes until the slot is given to it.
    return;
  }
  job_scheduled_ = true;
  env_->Schedule(&ManifestStoreShadow::BGWork, this, Env::Priority::LOW, this);
}

ManifestStoreShadow* ManifestStoreShadow::TakeGivenSlot() {
  MutexLock lock(&mutex_);
  ShadowJobSlots& slots = ShadowJobSlots::Get();
  ManifestStoreShadow* next = nullptr;
  if (NeedsJobLocked()) {
    job_scheduled_ = true;
    env_->Schedule(&ManifestStoreShadow::BGWork, this, Env::Priority::LOW,
                   this);
  } else {
    // The DB is closing. A DB in the queue needed a job when it joined, and
    // only its own job or Close() changes that.
    next = slots.Release();
  }
  slots.FinishDelivery(this);
  return next;
}

void ManifestStoreShadow::GiveSlotTo(ManifestStoreShadow* shadow) {
  while (shadow != nullptr) {
    // After TakeGivenSlot() `shadow` may be destroyed, so it is not used
    // again.
    shadow = shadow->TakeGivenSlot();
  }
}

ManifestStoreShadow* ManifestStoreShadow::ReleaseJobSlotLocked() {
  mutex_.AssertHeld();
  assert(job_scheduled_);
  job_scheduled_ = false;
  return ShadowJobSlots::Get().Release();
}

void ManifestStoreShadow::BGWork(void* arg) {
  static_cast<ManifestStoreShadow*>(arg)->RunJob();
}

bool ManifestStoreShadow::IsClosing() {
  MutexLock lock(&mutex_);
  return closing_;
}

void ManifestStoreShadow::RecordFailureLocked(const char* step,
                                              const IOStatus& s) {
  mutex_.AssertHeld();
  need_full_copy_ = true;
  ++failures_;
  const uint64_t wait = retry_interval_us_
                        << std::min(failures_ - 1, kMaxRetryWaitShift);
  retry_after_us_ = clock_->NowMicros() + wait;
  ROCKS_LOG_ERROR(info_log_,
                  "Shadow mode: %s failed (%" PRIu32
                  " failures in a row), next full copy to the store in about "
                  "%" PRIu64 " ms: %s",
                  step, failures_, wait / 1000, s.ToString().c_str());
}

std::vector<ManifestStoreShadow::ChangedRow>
ManifestStoreShadow::TakeChangedRowsLocked() {
  mutex_.AssertHeld();
  std::vector<ChangedRow> changed;
  changed.reserve(changed_.size());
  for (const std::pair<const ManifestRowKey, bool>& entry : changed_) {
    const RowMap::const_iterator it = rows_->find(entry.first);
    changed.push_back(ChangedRow{entry.first,
                                 it == rows_->end() ? nullptr : it->second,
                                 /*store_has_row=*/entry.second});
  }
  changed_.clear();
  return changed;
}

ManifestCommit ManifestStoreShadow::MakeCommit(
    uint64_t expected_commit_sequence, std::string commit_id,
    const std::string& db_row, const std::vector<ChangedRow>& changed) const {
  ManifestCommit commit;
  commit.expected_commit_sequence = expected_commit_sequence;
  commit.commit_id = std::move(commit_id);
  commit.db_row = ManifestRow{ManifestRowKey{}, db_row};
  for (const ChangedRow& c : changed) {
    if (c.row != nullptr) {
      commit.upserts.push_back(ManifestRow{c.key, *c.row});
    } else if (c.store_has_row) {
      commit.deletes.push_back(c.key);
    }
  }
  return commit;
}

IOStatus ManifestStoreShadow::FullCopy(const RowMap& rows,
                                       const std::string& db_row,
                                       uint64_t* commit_sequence,
                                       std::string* stop_reason) {
  stop_reason->clear();
  // Step 1: never claim a DB that store mode, a newer binary or another DB
  // owns.
  ManifestDbRowResult current;
  IOStatus s = store_->ReadDbRow(&current);
  uint64_t claimed_sequence = 0;
  if (s.IsNotFound()) {
    // Step 2, no DB row: create it. A DB row that another instance creates
    // first makes this commit fail with Busy.
    ManifestCommit create;
    create.commit_id = commit_ids_.Next();
    create.db_row = ManifestRow{ManifestRowKey{}, db_row};
    s = store_->Commit(create);
    // A commit with expected commit sequence 0 creates the DB row with commit
    // sequence 1.
    claimed_sequence = 1;
  } else if (s.ok()) {
    *stop_reason = ReasonNotToWrite(current.db_row, db_id_);
    if (!stop_reason->empty()) {
      return IOStatus::OK();
    }
    if (IsClosing()) {
      return IOStatus::Aborted("closing");
    }
    // Step 2: claim. No DB row field changes: the generation writes the whole
    // DB row.
    ManifestStoreOpenOptions open_options;
    open_options.commit_id = commit_ids_.Next();
    ManifestDbRowResult claimed;
    s = store_->Open(open_options, &claimed);
    // Step 3.
    if (s.ok()) {
      *stop_reason = ReasonNotToWrite(claimed.db_row, db_id_);
      if (!stop_reason->empty()) {
        return IOStatus::OK();
      }
    }
    claimed_sequence = claimed.state.commit_sequence;
  }
  if (!s.ok()) {
    return s;
  }
  if (IsClosing()) {
    return IOStatus::Aborted("closing");
  }
  // Step 4.
  ManifestGeneration generation;
  generation.expected_commit_sequence = claimed_sequence;
  generation.commit_id = commit_ids_.Next();
  generation.db_row = ManifestRow{ManifestRowKey{}, db_row};
  generation.rows.reserve(rows.size());
  for (const std::pair<const ManifestRowKey, EncodedRow>& entry : rows) {
    generation.rows.push_back(ManifestRow{entry.first, *entry.second});
  }
  uint64_t generation_number = 0;
  s = store_->WriteGeneration(generation, &generation_number);
  if (s.ok()) {
    // The switch to the new generation is a commit, which raises the commit
    // sequence by 1.
    *commit_sequence = claimed_sequence + 1;
    ROCKS_LOG_INFO(info_log_,
                   "Shadow mode: wrote %" ROCKSDB_PRIszt
                   " rows to the store as generation %" PRIu64,
                   generation.rows.size(), generation_number);
  }
  return s;
}

void ManifestStoreShadow::RunJob() {
  TEST_SYNC_POINT("ManifestStoreShadow::RunJob:Start");
  mutex_.Lock();
  if (closing_ || Stopped()) {
    ManifestStoreShadow* const next = ReleaseJobSlotLocked();
    cv_.SignalAll();
    mutex_.Unlock();
    // `this` may be destroyed from here on.
    GiveSlotTo(next);
    return;
  }
  const bool full_copy = need_full_copy_;
  const std::string db_row = db_row_;
  db_row_changed_ = false;
  std::shared_ptr<const RowMap> rows_ref;
  std::vector<ChangedRow> changed;
  if (full_copy) {
    // The generation holds all current rows, so the changed keys up to here
    // are in it.
    rows_ref = rows_;
    writer_owns_ = false;
    changed_.clear();
  } else {
    changed = TakeChangedRowsLocked();
  }
  const uint64_t expected_sequence = commit_sequence_;
  mutex_.Unlock();
  // Two sync points are needed. A test waits at TookRows until the job has
  // taken its rows (SlowStoreDoesNotBlockWrites). Then the test makes the job
  // wait at BeforeStoreCall until it releases the job
  // (SlowStoreDoesNotBlockWrites, CloseWithSlowStore). With one point for
  // both, the job would wait for the test while the test waits for the job.
  TEST_SYNC_POINT("ManifestStoreShadow::RunJob:TookRows");
  TEST_SYNC_POINT("ManifestStoreShadow::RunJob:BeforeStoreCall");

  IOStatus s;
  std::string stop_reason;
  uint64_t new_sequence = 0;
  const char* step = full_copy ? "full copy" : "commit";
  if (full_copy) {
    s = FullCopy(*rows_ref, db_row, &new_sequence, &stop_reason);
    rows_ref.reset();
  } else {
    s = store_->Commit(
        MakeCommit(expected_sequence, commit_ids_.Next(), db_row, changed));
    new_sequence = expected_sequence + 1;
  }

  mutex_.Lock();
  // FullCopy() returns Aborted when Close() started. That is not a store
  // error, so it is not counted and not logged. The store may hold a partial
  // state, so a full copy is still needed.
  const bool aborted_by_close = s.IsAborted() && closing_;
  if (!stop_reason.empty()) {
    stopped_.store(true, std::memory_order_release);
    ROCKS_LOG_ERROR(info_log_,
                    "Shadow mode stopped: %s. Shadow mode writes no rows to "
                    "this store. For a DB in store mode, roll it back to its "
                    "MANIFEST file before shadow mode is used again; for "
                    "another DB, give this DB its own store key.",
                    stop_reason.c_str());
  } else if (s.IsIOFenced()) {
    // The MANIFEST file stays the authority, so the DB goes on without the
    // store.
    stopped_.store(true, std::memory_order_release);
    ROCKS_LOG_ERROR(info_log_,
                    "Shadow mode stopped: another DB instance claimed the "
                    "store with a newer epoch, for example a copy of this DB "
                    "opened with the same store key. Shadow mode writes no "
                    "rows to this store: %s",
                    s.ToString().c_str());
  } else if (s.ok()) {
    commit_sequence_ = new_sequence;
    need_full_copy_ = false;
    failures_ = 0;
  } else if (aborted_by_close) {
    need_full_copy_ = true;
  } else {
    RecordFailureLocked(step, s);
  }
  ManifestStoreShadow* const next = ReleaseJobSlotLocked();
  if (s.ok()) {
    // After a failure, the next Apply() schedules the full copy once the wait
    // has passed, so a store that keeps failing never makes a busy loop. With
    // work left, this DB joins the back of the queue when another DB waits.
    MaybeScheduleLocked();
  }
  cv_.SignalAll();
  mutex_.Unlock();
  // `this` may be destroyed from here on.
  GiveSlotTo(next);
}

void ManifestStoreShadow::Close() {
  TEST_SYNC_POINT("ManifestStoreShadow::Close:Start");
  {
    MutexLock lock(&mutex_);
    closing_ = true;
  }
  // With `closing_` set, this DB never joins the queue again.
  ShadowJobSlots::Get().Remove(this);
  mutex_.Lock();
  ManifestStoreShadow* next = nullptr;
  if (job_scheduled_ && env_->UnSchedule(this, Env::Priority::LOW) > 0) {
    next = ReleaseJobSlotLocked();
  }
  while (job_scheduled_) {
    cv_.Wait();
  }
  const bool in_sync = started_ && has_rows_ && !Stopped() && !need_full_copy_;
  const std::vector<ChangedRow> changed = TakeChangedRowsLocked();
  const std::string db_row = db_row_;
  const uint64_t expected_sequence = commit_sequence_;
  const bool started = started_;
  mutex_.Unlock();
  GiveSlotTo(next);
  if (!started || Stopped()) {
    return;
  }
  if (!in_sync) {
    ROCKS_LOG_WARN(info_log_,
                   "Shadow mode: the store is not in sync with the MANIFEST "
                   "at close");
    return;
  }
  const IOStatus s = store_->Commit(MakeCommit(
      expected_sequence, commit_ids_.NextCloseId(), db_row, changed));
  if (s.ok()) {
    ROCKS_LOG_INFO(info_log_,
                   "Shadow mode: the store is in sync with the MANIFEST at "
                   "close");
  } else {
    ROCKS_LOG_WARN(info_log_,
                   "Shadow mode: the store is not in sync with the MANIFEST "
                   "at close, the last commit failed: %s",
                   s.ToString().c_str());
  }
}

// ---------------------------------------------------------------------------
// VersionSet code of shadow mode. It is a VersionSet member, defined here so
// that all shadow mode code is in one file. ProcessManifestWrites() (in
// db/version_set.cc) calls it after a successful MANIFEST write.
void VersionSet::HandOffToManifestStoreShadow(
    const autovector<VersionEdit*>& batch_edits,
    const autovector<Version*>& versions,
    const std::vector<std::unique_ptr<BaseReferencedVersionBuilder>>&
        builder_guards,
    ColumnFamilyData* first_cfd, const ColumnFamilyOptions* new_cf_options,
    ManifestRows* snapshot_rows) {
  ManifestStoreShadow* const shadow = manifest_store_shadow_;
  assert(shadow != nullptr);
  if (shadow->Stopped()) {
    shadow_base_rows_.reset();
    return;
  }
  // The rollover decision makes sure that a write without base rows starts a
  // new MANIFEST file.
  const ManifestRows* base =
      snapshot_rows != nullptr ? snapshot_rows : shadow_base_rows_.get();
  assert(base != nullptr);
  ManifestRowChanges changes;
  std::vector<ManifestRow> snapshot_public;
  ManifestCommit commit;
  Status s =
      BuildManifestBatchRowChanges(batch_edits, versions, builder_guards,
                                   first_cfd, new_cf_options, *base, &changes);
  if (s.ok() && snapshot_rows != nullptr) {
    s = ManifestRowsToPublicRows(*snapshot_rows, shadow->extra_fields(),
                                 shadow->feature_version(), &snapshot_public);
  }
  if (s.ok()) {
    s = ManifestRowChangesToCommit(changes, shadow->extra_fields(),
                                   shadow->feature_version(),
                                   /*expected_commit_sequence=*/0,
                                   /*commit_id=*/"", &commit);
  }
  if (!s.ok()) {
    // The build fails only for a WAL edit that the install of this write
    // also rejects, so the write fails anyway. Shadow mode drops the base
    // rows, so the next MANIFEST write starts a new MANIFEST file, whose
    // snapshot gives the full state again.
    ROCKS_LOG_WARN(db_options_->info_log,
                   "Shadow mode: cannot build the row changes of a MANIFEST "
                   "write: %s",
                   s.ToString().c_str());
    shadow_base_rows_.reset();
    return;
  }
  if (snapshot_rows != nullptr) {
    shadow->ReplaceWithSnapshot(std::move(snapshot_public));
    shadow_base_rows_ =
        std::make_unique<ManifestRows>(std::move(*snapshot_rows));
  }
  ApplyManifestRowChanges(changes, shadow_base_rows_.get());
  // The base needs only the DB and column family rows (see
  // VersionSet::shadow_base_rows_).
  for (std::pair<const uint32_t, ManifestCfRows>& cf : shadow_base_rows_->cfs) {
    cf.second.files.clear();
    cf.second.blob_files.clear();
  }
  shadow_base_rows_->wals.clear();
  shadow->Apply(std::move(commit));
}

void ManifestStoreShadow::TEST_WaitForIdle() {
  ShadowJobSlots& slots = ShadowJobSlots::Get();
  while (true) {
    // A delivery finishes after it scheduled the job, so the wait below sees
    // that job.
    slots.WaitUntilNotWaiting(this);
    MutexLock lock(&mutex_);
    while (job_scheduled_) {
      cv_.Wait();
    }
    // A job that ends with work left may have joined the queue again.
    if (!slots.IsWaiting(this)) {
      return;
    }
  }
}

size_t ManifestStoreShadow::TEST_NumChangedKeys() {
  MutexLock lock(&mutex_);
  return changed_.size();
}

bool ManifestStoreShadow::TEST_InSync() {
  MutexLock lock(&mutex_);
  return !need_full_copy_ && !Stopped();
}

}  // namespace ROCKSDB_NAMESPACE
