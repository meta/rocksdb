//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db_stress_tool/db_stress_manifest_store.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <limits>

#include "file/file_util.h"
#include "port/port.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// The shared state of one store dir. `mu` serializes all calls on the store
// dir, so the state file always matches the in-memory state after each call.
struct FileManifestStoreState {
  FileManifestStoreState(Env* _env, std::string _dir, uint32_t seed)
      : env(_env), dir(std::move(_dir)), rnd(seed) {}

  Env* const env;
  const std::string dir;
  port::Mutex mu;
  std::shared_ptr<MockManifestStoreData> data =
      std::make_shared<MockManifestStoreData>();
  // The content of the state file.
  std::string persisted;
  // The epoch of the next writer object.
  uint64_t next_epoch = 1;
  Random rnd;
};

namespace {

using Fault = MockManifestStoreData::Fault;

// True when `commit` may come from a WAL edit: it changes a WAL row, or it
// changes only the DB row (a WAL edit whose WAL rows already hold the synced
// state). See "Commits without faults" at FileManifestStore.
bool MayComeFromWalEdit(const ManifestCommit& commit) {
  bool other_rows = false;
  for (const ManifestRow& row : commit.upserts) {
    if (row.key.type == ManifestRowType::kWal) {
      return true;
    }
    other_rows = true;
  }
  for (const ManifestRowKey& key : commit.deletes) {
    if (key.type == ManifestRowType::kWal) {
      return true;
    }
    other_rows = true;
  }
  return !other_rows;
}

port::Mutex registry_mu;
// The states of the store dirs that objects of this process use, by store dir.
// The pointers stay valid after the entry is removed, until the last object
// that holds one is destroyed.
UnorderedMap<std::string, std::shared_ptr<FileManifestStoreState>>& Registry() {
  static auto* registry =
      new UnorderedMap<std::string, std::shared_ptr<FileManifestStoreState>>();
  return *registry;
}

std::string StateFilePath(const std::string& dir) {
  return dir + "/" + FileManifestStore::kStateFileName;
}

[[noreturn]] void AbortWithStatus(const char* what, const Status& s) {
  fprintf(stderr, "FileManifestStore: %s: %s\n", what, s.ToString().c_str());
  fflush(stderr);
  std::abort();
}

// Writes the state file through a temporary file, so a process kill keeps the
// old or the new content.
Status WriteStateFile(Env* env, const std::string& dir,
                      const std::string& content) {
  const std::string path = StateFilePath(dir);
  const std::string tmp = path + ".tmp";
  Status s = WriteStringToFile(env, content, tmp, /*should_sync=*/true);
  if (s.ok()) {
    s = env->RenameFile(tmp, path);
  }
  std::unique_ptr<Directory> dir_obj;
  if (s.ok()) {
    s = env->NewDirectory(dir, &dir_obj);
  }
  if (s.ok()) {
    s = dir_obj->Fsync();
  }
  if (s.ok()) {
    s = dir_obj->Close();
  }
  return s;
}

// Loads the state of `dir` from its state file, or keeps the empty state when
// there is no state file.
void LoadState(FileManifestStoreState* state) {
  Status s = state->env->CreateDirIfMissing(state->dir);
  if (!s.ok()) {
    AbortWithStatus("cannot create the store dir", s);
  }
  const std::string path = StateFilePath(state->dir);
  s = state->env->FileExists(path);
  if (s.IsNotFound()) {
    return;
  }
  if (s.ok()) {
    s = ReadFileToString(state->env, path, &state->persisted);
  }
  if (s.ok()) {
    s = state->data->Deserialize(state->persisted);
  }
  if (!s.ok()) {
    AbortWithStatus("cannot read the state file", s);
  }
}

std::shared_ptr<FileManifestStoreState> GetState(Env* env,
                                                 const std::string& dir,
                                                 uint32_t seed) {
  MutexLock l(&registry_mu);
  auto& state = Registry()[dir];
  if (!state) {
    state = std::make_shared<FileManifestStoreState>(env, dir, seed);
    LoadState(state.get());
  }
  return state;
}

// Picks one fault evenly. The first faults of the list apply to every call. The
// last kGenerationOnlyFaults (2) lose or change a row of a generation, so only
// a WriteGeneration() call (`is_write_generation`) picks them.
Fault PickFault(Random* rnd, bool is_write_generation) {
  static constexpr Fault kFaults[] = {
      Fault::kTryAgain,          Fault::kTimedOutNotApplied,
      Fault::kTimedOutApplied,   Fault::kTimedOutLateApply,
      Fault::kLoseGenerationRow, Fault::kCorruptGenerationRow,
  };
  static constexpr uint32_t kGenerationOnlyFaults = 2;
  const uint32_t count = static_cast<uint32_t>(std::size(kFaults)) -
                         (is_write_generation ? 0 : kGenerationOnlyFaults);
  return kFaults[rnd->Uniform(static_cast<int>(count))];
}

// Adds the "injected" text to the status of an injected fault, so that
// db_stress accepts the errors that it causes.
IOStatus MarkInjected(const IOStatus& s) {
  const std::string msg =
      FaultInjectionTestFS::kInjected + " store fault: " + s.ToString();
  if (s.IsTryAgain()) {
    return status_to_io_status(Status::TryAgain(msg));
  }
  if (s.IsTimedOut()) {
    return IOStatus::TimedOut(msg);
  }
  return s;
}

}  // namespace

FileManifestStore::FileManifestStore(Env* env, const std::string& dir,
                                     bool writer, uint64_t fault_one_in,
                                     uint32_t seed)
    : state_(GetState(env, dir, seed)), fault_one_in_(fault_one_in) {
  MutexLock l(&state_->mu);
  if (writer) {
    state_->next_epoch =
        std::max(state_->next_epoch, state_->data->StoredEpoch() + 1);
    epoch_ = state_->next_epoch++;
  }
  inner_ = std::make_unique<MockManifestStore>(state_->data, epoch_);
}

FileManifestStore::~FileManifestStore() {
  // The shared data keeps at most one late apply. It holds a pointer to the
  // inner store that made the call, so it must not outlive that store. Drop it
  // here, also when another object of the store dir made the call. Dropping
  // equals a store that never finished the request, which is a valid outcome of
  // a TimedOut call.
  MutexLock l(&state_->mu);
  state_->data->DropLateApply();
}

IOStatus FileManifestStore::Run(bool may_fault, bool is_write_generation,
                                const std::function<IOStatus()>& call) {
  MutexLock l(&state_->mu);
  bool fault_set = false;
  if (may_fault && forced_fault_.has_value()) {
    state_->data->SetNextFault(*forced_fault_);
    forced_fault_.reset();
    fault_set = true;
  } else if (may_fault && fault_one_in_ > 0 && faults_enabled_.load() &&
             state_->rnd.OneIn(static_cast<int>(std::min<uint64_t>(
                 fault_one_in_, std::numeric_limits<int>::max())))) {
    state_->data->SetNextFault(PickFault(&state_->rnd, is_write_generation));
    fault_set = true;
  }
  IOStatus s = call();
  bool fault_used = false;
  if (fault_set) {
    if (state_->data->PendingFault() == Fault::kNone) {
      fault_used = true;
    } else {
      // The inner call returned before it used the fault: WriteGeneration()
      // returns at once for a reader object or a fenced object. Clear the
      // fault, so that it does not hit a later call.
      state_->data->SetNextFault(Fault::kNone);
    }
  }
  std::string content = state_->data->Serialize();
  if (content != state_->persisted) {
    Status ws = WriteStateFile(state_->env, state_->dir, content);
    if (!ws.ok()) {
      AbortWithStatus("cannot write the state file", ws);
    }
    state_->persisted = std::move(content);
  }
  return fault_used ? MarkInjected(s) : s;
}

IOStatus FileManifestStore::Open(const ManifestStoreOpenOptions& opts,
                                 ManifestDbRowResult* db_row) {
  return Run(/*may_fault=*/true, /*is_write_generation=*/false,
             [&]() { return inner_->Open(opts, db_row); });
}

IOStatus FileManifestStore::Commit(const ManifestCommit& commit) {
  return Run(/*may_fault=*/!MayComeFromWalEdit(commit),
             /*is_write_generation=*/false,
             [&]() { return inner_->Commit(commit); });
}

IOStatus FileManifestStore::ReadDbRow(ManifestDbRowResult* db_row) {
  return Run(/*may_fault=*/false, /*is_write_generation=*/false,
             [&]() { return inner_->ReadDbRow(db_row); });
}

IOStatus FileManifestStore::Scan(const ManifestScanOptions& opts,
                                 ManifestRowVisitor* visitor) {
  // The visitor runs with the lock of the store dir. The visitors of RocksDB
  // never call the store.
  return Run(/*may_fault=*/false, /*is_write_generation=*/false,
             [&]() { return inner_->Scan(opts, visitor); });
}

IOStatus FileManifestStore::WriteGeneration(const ManifestGeneration& request,
                                            uint64_t* generation) {
  return Run(/*may_fault=*/true, /*is_write_generation=*/true,
             [&]() { return inner_->WriteGeneration(request, generation); });
}

IOStatus FileManifestStore::Destroy() {
  return Run(/*may_fault=*/false, /*is_write_generation=*/false,
             [&]() { return inner_->Destroy(); });
}

uint64_t FileManifestStore::StoredEpoch() const {
  return state_->data->StoredEpoch();
}

Status FileManifestStore::DestroyStore(Env* env, const std::string& dir) {
  ForgetState(dir);
  return DestroyDir(env, dir);
}

void FileManifestStore::ForgetState(const std::string& dir) {
  MutexLock l(&registry_mu);
  Registry().erase(dir);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
