//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/lazy_wide_columns.h"

#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/version_set.h"
#include "db/wide/blob_column_resolver_util.h"
#include "db/wide/lazy_wide_columns_helper.h"
#include "db/wide/read_path_blob_resolver.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/options.h"
#include "rocksdb/wide_columns.h"
#include "util/autovector.h"

// Current implementation of the lazy blob resolution API. Enumeration and
// whole-column resolution are functional. Byte-range reads of an uncompressed
// blob -- whether it lives in a separate blob file or is embedded (same-file)
// in the SST -- are served by reading only the requested bytes from storage
// (skipping checksum verification and cache-fill); other cases (compressed,
// whole-column, already-cached, or force_verify) resolve the whole column and
// slice it. LazyWideColumnsBatch::MultiResolve coalesces reads across keys:
// classifies each read, then groups the storage reads per (Version, blob file)
// for separate-file references and per SST for embedded references, issuing one
// coalesced MultiRead per group (whole and byte-range). Async execution is
// future work.

namespace ROCKSDB_NAMESPACE {

namespace {
// Scope guard used while resolving a lazy result: sets the thread's
// ThreadStatus operation to OP_LAZY_RESOLVE for the duration, then returns it
// to OP_UNKNOWN on exit. A resolve is a self-contained top-level operation, so
// it starts and ends with no active op rather than restoring a prior one --
// thread operations do not stack, and any op lingering from the originating
// GetEntity/MultiGetEntity call is stale. (This mirrors the SetThreadOperation
// + ResetThreadStatus pattern other top-level ops such as compaction/DBOpen
// use.)
//
// Setting the parallel operation type keeps the io_activity and thread
// operation consistent while the deferred reads (which carry
// Env::IOActivity::kLazyResolve) run: db_stress asserts every file read's
// io_activity matches the one implied by the current thread operation (see
// db_stress_env_wrapper.h / TEST_GetExpectedIOActivity), and ThreadStatus
// reporting then attributes these reads to the lazy-resolve operation.
//
// TODO: unified RAII wrappers for thread status updates
class LazyResolveThreadOpScope {
 public:
  LazyResolveThreadOpScope() {
    ThreadStatusUtil::SetThreadOperation(
        ThreadStatus::OperationType::OP_LAZY_RESOLVE);
  }
  ~LazyResolveThreadOpScope() {
    ThreadStatusUtil::SetThreadOperation(
        ThreadStatus::OperationType::OP_UNKNOWN);
  }
  LazyResolveThreadOpScope(const LazyResolveThreadOpScope&) = delete;
  LazyResolveThreadOpScope& operator=(const LazyResolveThreadOpScope&) = delete;
  LazyResolveThreadOpScope(LazyResolveThreadOpScope&&) = delete;
  LazyResolveThreadOpScope& operator=(LazyResolveThreadOpScope&&) = delete;
};

// Identity of a physical blob record, shared across the batch: two reads that
// resolve to the same on-disk record and blob-cache key (e.g. duplicate keys
// in the batch, whose distinct entities reference the same record) map to the
// same id. The batch planner uses this to keep every read of a force-verified
// record on the individual path, so a non-verifying coalesced fetch cannot
// populate the shared blob cache ahead of the force_verify read.
using PhysicalBlobId = std::tuple<const void*, uint64_t, uint64_t>;
PhysicalBlobId MakePhysicalBlobId(const ReadPathBlobResolver& resolver,
                                  const BlobIndex& blob_index) {
  if (blob_index.IsSameFile()) {
    // Embedded record: keyed by the owning SST reader + in-file offset.
    return PhysicalBlobId(resolver.same_file_reader(), 0, blob_index.offset());
  }
  // Separate blob file: keyed by Version + blob file number + offset.
  return PhysicalBlobId(resolver.version(), blob_index.file_number(),
                        blob_index.offset());
}
}  // namespace

// Internal representation. Owns the serialized-entity backing buffer + inline
// columns (via `entity_`), the decoded blob references, the per-column views,
// the on-demand blob resolver, and the SuperVersion pin that keeps all of it
// (blob files, immortal table readers) valid after the producing DB call
// returns. The Rep is heap-allocated and never relocated (it lives behind a
// unique_ptr that only ever transfers ownership), so pointers to it and to its
// `columns_` elements stay stable across LazyWideColumns/Batch moves -- which
// is what lets a LazyColumnReadRequest identify its column (and owning result)
// by pointer without any move-time fix-ups.
class LazyWideColumns::Rep {
 public:
  // Holds the serialized entity as a single backing buffer and, after
  // SetWideColumnValue (without resolution), the full sorted column set --
  // inline columns' value() are zero-copy Slices into that buffer; blob
  // columns' value() are the raw serialized BlobIndex bytes (the resolver reads
  // decoded indices from blob_columns_, not from here).
  //
  // Lifetime invariant: `entity_` is populated once (by the point lookup, via
  // EntityBuffer(), before Finalize) and is never mutated afterwards, and the
  // Rep is heap-allocated and never relocated. So this backing buffer is stable
  // for the result's whole lifetime -- which is what keeps valid every Slice
  // that points into it: the inline column values, the decoded
  // inlined-BlobIndex values stored in blob_columns_, the per-column
  // inline_data_ in columns_, and the resolver's cached bytes.
  PinnableWideColumns entity_;

  // Decoded blob references (column_index -> BlobIndex), for the columns whose
  // value is a blob reference. Empty for a fully inline entity.
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_;

  // Per-column view returned by LazyWideColumns::operator[]. Derives from the
  // public LazyWideColumn (its ctor sets the protected base fields) and adds a
  // back-pointer to this Rep, used to validate that a read's column belongs to
  // this result (and, via owning_batch_rep_, to this batch). Nested (rather
  // than in an anonymous namespace) so it shares the enclosing class's linkage
  // -- an internal-linkage subobject type in this external-linkage class trips
  // -Wsubobject-linkage in unity builds.
  struct ColumnImpl : public LazyWideColumn {
    ColumnImpl(Rep* parent, size_t index, Slice name, const char* inline_data,
               std::optional<uint64_t> size)
        : parent_rep_(parent) {
      name_ = name;
      inline_data_ = inline_data;
      size_ = size;
      index_ = index;
    }
    Rep* parent_rep_ = nullptr;
  };

  // One entry per column in `entity_.columns()`, in sorted order.
  std::vector<ColumnImpl> columns_;

  // Identity of the owning batch's Rep (stable across batch move), or null for
  // a standalone result. Used to validate that a batch read targets an entity
  // of that batch. Stored as void* to avoid naming LazyWideColumnsBatch::Rep
  // here (it is only an identity token, never dereferenced).
  const void* owning_batch_rep_ = nullptr;

  // Stable copy of the user key, referenced by the resolver for blob
  // verification (the originating key argument does not outlive the call).
  std::string user_key_;

  // Pins the SuperVersion for this result's lifetime. Declared before
  // `resolver_` so the resolver (which references the Version) is destroyed
  // first.
  Cleanable pin_;

  // On-demand blob resolver, bound to entity_.columns() + blob_columns_.
  // Emplaced at finalize time (needs the Version). Absent for an unpopulated
  // (e.g. NotFound) result.
  std::optional<ReadPathBlobResolver> resolver_;

  Rep() = default;
  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  Rep(Rep&&) = delete;
  Rep& operator=(Rep&&) = delete;

  ~Rep() {
    // Releasing the SuperVersion pin (pin_) can trigger obsolete-file cleanup
    // I/O -- e.g. FindObsoleteFiles closing an obsolete WAL -- when this result
    // held the last reference to it. Mirror DBIter::~DBIter and run that
    // teardown with the thread operation reset to OP_UNKNOWN, so the incidental
    // I/O is not attributed to whatever read operation happens to be active on
    // the destroying thread (which otherwise misattributes I/O stats and trips
    // db_stress's io_activity invariant).
    const ThreadStatus::OperationType saved_op =
        ThreadStatusUtil::GetThreadOperation();
    ThreadStatusUtil::SetThreadOperation(
        ThreadStatus::OperationType::OP_UNKNOWN);
    resolver_.reset();  // borrows the pinned Version; drop before the pin
    pin_.Reset();       // runs CleanupSuperVersionHandle if this was last ref
    ThreadStatusUtil::SetThreadOperation(saved_op);
  }

  // Resolve one read against column `index` of this result (index < size()),
  // writing the per-read outcome to read.status / read.result. Shared by the
  // single-entity and batch MultiResolve paths.
  void ResolveOneRead(size_t index, LazyColumnReadRequest& read) {
    assert(resolver_);

    // Delegate to the resolver, which owns the partial-vs-whole decision: for a
    // strict sub-range of an uncompressed blob reference (separate-file or
    // embedded, and not force_verify) it reads only the requested bytes,
    // skipping checksum and cache-fill; every other case resolves the whole
    // column (caching it) and slices. Repeated whole-column reads therefore
    // read the blob at most once; partial reads own their own bytes and are not
    // cached. A null read.result still resolves (to surface I/O/integrity
    // errors on read.status and honor force_verify), just without producing
    // output -- ResolveColumnRange handles that case.
    const Status s = resolver_->ResolveColumnRange(
        index, read.offset, read.length, read.force_verify, read.result);

    assert(read.status);  // callers reject a null status up front
    *read.status = s;
  }
};

LazyWideColumns::LazyWideColumns() = default;
LazyWideColumns::~LazyWideColumns() = default;
LazyWideColumns::LazyWideColumns(LazyWideColumns&&) noexcept = default;
LazyWideColumns& LazyWideColumns::operator=(LazyWideColumns&&) noexcept =
    default;

size_t LazyWideColumns::size() const {
  return rep_ ? rep_->columns_.size() : 0;
}

const LazyWideColumn& LazyWideColumns::operator[](size_t i) const {
  assert(rep_);
  assert(i < rep_->columns_.size());
  return rep_->columns_[i];
}

Status LazyWideColumns::MultiResolve(size_t num_reads,
                                     LazyColumnReadRequest* reads) {
  LazyResolveThreadOpScope thread_op_scope;
  // Each read must supply a status out-param; a null one is a caller error with
  // nowhere to report that read's outcome, so fail the whole call (see
  // LazyColumnReadRequest::status).
  for (size_t i = 0; i < num_reads; ++i) {
    if (reads[i].status == nullptr) {
      return Status::InvalidArgument(
          "LazyColumnReadRequest::status must not be null");
    }
  }
  for (size_t i = 0; i < num_reads; ++i) {
    LazyColumnReadRequest& read = reads[i];
    *read.status = Status::OK();
    if (read.result) {
      read.result->Reset();  // failure paths below leave an empty result
    }
    // The column must belong to this result (it carries a back-pointer to the
    // Rep that owns it).
    if (read.column == nullptr ||
        static_cast<const Rep::ColumnImpl*>(read.column)->parent_rep_ !=
            rep_.get()) {
      *read.status = Status::InvalidArgument(
          "Column does not belong to this LazyWideColumns");
      continue;
    }
    rep_->ResolveOneRead(read.column->index(), read);
  }
  // Every read was dispatched synchronously; per-read outcomes are in
  // read.status.
  return Status::OK();
}

Status LazyWideColumns::ResolveColumnRange(const LazyWideColumn& column,
                                           uint64_t offset, size_t length,
                                           PinnableSlice* result) {
  // Sugar over a one-entry batch, so the batch path is the single real,
  // optimizable (coalescing/async) code path.
  Status status;
  LazyColumnReadRequest read;
  read.column = &column;
  read.offset = offset;
  read.length = length;
  read.result = result;
  read.status = &status;

  const Status batch_status = MultiResolve(/*num_reads=*/1, &read);
  return batch_status.ok() ? status : batch_status;
}

Status LazyWideColumns::ResolveColumn(const LazyWideColumn& column,
                                      PinnableSlice* result) {
  return ResolveColumnRange(column, /*offset=*/0, kLazyWholeColumn, result);
}

void LazyWideColumns::Reset() { rep_.reset(); }

// ---- LazyWideColumnsBatch ----

// Internal representation. Owns the shared SuperVersion pin(s) and the per-key
// LazyWideColumns. Each entity's resolver is bound to the batch's shared
// Version but takes no per-entity pin; the batch holds one shared pin per
// column family (`cf_pins`, a single entry for the common single-CF call),
// transferred in by the batched MultiGetEntityLazy.
class LazyWideColumnsBatch::Rep {
 public:
  // Shared SuperVersion pin per column family. Declared before `entities` so
  // the entities (whose resolvers reference the pinned Version) are destroyed
  // before these pins are released.
  std::map<uint32_t /* column_family_id */, Cleanable> cf_pins;

  // One result per key of the MultiGetEntityLazy call, in key order.
  std::vector<LazyWideColumns> entities;

  Rep() = default;
  // Not copyable or movable: the entities' resolvers hold pointers back into
  // this Rep, and the custom destructor below has teardown-ordering semantics.
  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  Rep(Rep&&) = delete;
  Rep& operator=(Rep&&) = delete;

  ~Rep() {
    // Releasing a shared SuperVersion pin can trigger obsolete-file cleanup I/O
    // (e.g. FindObsoleteFiles) when the batch held the last reference. Mirror
    // LazyWideColumns::Rep::~Rep / DBIter::~DBIter and run teardown with the
    // thread operation reset to OP_UNKNOWN so that incidental I/O is not
    // misattributed to whatever read op is active on the destroying thread.
    // Destroy the entities (their resolvers reference the pinned Version)
    // before releasing the pins.
    const ThreadStatus::OperationType saved_op =
        ThreadStatusUtil::GetThreadOperation();
    ThreadStatusUtil::SetThreadOperation(
        ThreadStatus::OperationType::OP_UNKNOWN);
    entities.clear();
    cf_pins.clear();
    ThreadStatusUtil::SetThreadOperation(saved_op);
  }
};

LazyWideColumnsBatch::LazyWideColumnsBatch() = default;
LazyWideColumnsBatch::~LazyWideColumnsBatch() = default;
LazyWideColumnsBatch::LazyWideColumnsBatch(LazyWideColumnsBatch&&) noexcept =
    default;
LazyWideColumnsBatch& LazyWideColumnsBatch::operator=(
    LazyWideColumnsBatch&&) noexcept = default;

size_t LazyWideColumnsBatch::size() const {
  return rep_ ? rep_->entities.size() : 0;
}

const LazyWideColumns& LazyWideColumnsBatch::operator[](size_t i) const {
  assert(rep_);
  assert(i < rep_->entities.size());
  return rep_->entities[i];
}

LazyWideColumns& LazyWideColumnsBatch::operator[](size_t i) {
  assert(rep_);
  assert(i < rep_->entities.size());
  return rep_->entities[i];
}

Status LazyWideColumnsBatch::MultiResolve(size_t num_reads,
                                          LazyColumnReadRequest* reads) {
  LazyResolveThreadOpScope thread_op_scope;
  // Each read must supply a status out-param; a null one is a caller error with
  // nowhere to report that read's outcome, so fail the whole call (see
  // LazyColumnReadRequest::status).
  for (size_t i = 0; i < num_reads; ++i) {
    if (reads[i].status == nullptr) {
      return Status::InvalidArgument(
          "LazyColumnReadRequest::status must not be null");
    }
  }
  const void* this_rep = rep_.get();

  using EntityRep = LazyWideColumns::Rep;
  using Plan = ReadPathBlobResolver::LazyColumnReadPlan;

  // A whole-value fetch, deduplicated per owning (entity, column): fetched into
  // `temp`, then adopted into the entity's resolver cache and sliced per read.
  struct WholeFetch {
    EntityRep* entity_rep = nullptr;
    size_t column_index = 0;
    bool same_file = false;
    const BlobIndex* blob_index = nullptr;
    PinnableSlice temp;
    Status status;
  };
  // A sub-range fetch: read straight into the user's output slice/status.
  struct RangeFetch {
    EntityRep* entity_rep = nullptr;
    bool same_file = false;
    const BlobIndex* blob_index = nullptr;
    uint64_t range_offset = 0;
    size_t range_length = 0;
    PinnableSlice* result = nullptr;
    Status* status = nullptr;  // the caller's status (never null; validated)
  };
  // Binds an original whole read to the whole fetch that satisfies it.
  struct WholeBinding {
    size_t fetch_idx = 0;
    LazyColumnReadRequest* read = nullptr;
  };

  std::vector<WholeFetch> whole_fetches;
  std::vector<RangeFetch> range_fetches;
  std::vector<WholeBinding> whole_bindings;
  std::vector<LazyColumnReadRequest*> serve_individually;
  std::map<std::pair<EntityRep*, size_t>, size_t> whole_index;
  // Each read contributes at most one entry to one of these plans; reserve up
  // front to avoid repeated reallocation on the batch path.
  whole_fetches.reserve(num_reads);
  range_fetches.reserve(num_reads);
  whole_bindings.reserve(num_reads);
  serve_individually.reserve(num_reads);

  // Any column with a force_verify read must be resolved entirely on the
  // individual (sequential) path, never coalesced: otherwise a non-verified
  // coalesced whole fetch for that column could adopt its bytes into the
  // resolver cache before the force_verify read runs, so that read would hit
  // the cache and skip checksum verification. Collect those columns first (a
  // read requesting verification may follow a non-verifying read of the same
  // column in the batch) so classification below can route all their reads to
  // the individual path, preserving the "verify on the first actual read"
  // semantics of resolving key-by-key.
  std::set<std::pair<EntityRep*, size_t>> force_verify_columns;
  // Physical blob records force-verified anywhere in this batch. Complements
  // force_verify_columns to also cover reads from a *different* entity that
  // resolve to the same on-disk record and shared blob-cache key -- e.g. a
  // duplicate key in the batch. Without this, such a read's non-verifying
  // coalesced whole fetch could populate the shared blob cache before the
  // force_verify read runs, and that read would then hit the cache and skip
  // checksum verification.
  std::set<PhysicalBlobId> force_verify_blobs;
  for (size_t i = 0; i < num_reads; ++i) {
    const LazyColumnReadRequest& read = reads[i];
    if (read.force_verify && read.column != nullptr) {
      EntityRep* entity_rep =
          static_cast<const EntityRep::ColumnImpl*>(read.column)->parent_rep_;
      if (entity_rep != nullptr) {
        const size_t column_index = read.column->index();
        force_verify_columns.emplace(entity_rep, column_index);
        if (entity_rep->resolver_) {
          const BlobIndex* blob_index = blob_resolver_util::FindBlobColumn(
              &entity_rep->blob_columns_, column_index);
          if (blob_index != nullptr && !blob_index->IsInlined()) {
            force_verify_blobs.insert(
                MakePhysicalBlobId(*entity_rep->resolver_, *blob_index));
          }
        }
      }
    }
  }

  // The (single-CF, shared) lazy ReadOptions used for every coalesced dispatch;
  // captured from the first classified entity. A future cross-CF batch would
  // need per-group ReadOptions instead.
  const ReadOptions* resolve_ro = nullptr;

  // Pass 1: reset outputs, validate ownership, classify.
  for (size_t i = 0; i < num_reads; ++i) {
    LazyColumnReadRequest& read = reads[i];
    *read.status = Status::OK();
    if (read.result) {
      read.result->Reset();  // failure paths below leave an empty result
    }
    if (read.column == nullptr) {
      *read.status = Status::InvalidArgument("Null column in batch read");
      continue;
    }
    // Route the read to the entity that owns its column, and require that
    // entity to belong to this batch. The owning_batch_rep_ == nullptr check is
    // essential: a standalone column (from GetEntityLazy) has
    // owning_batch_rep_ == nullptr, and an empty/default-constructed batch has
    // this_rep == nullptr, so without it a foreign standalone column would slip
    // through the nullptr == nullptr comparison instead of being rejected.
    EntityRep* entity_rep =
        static_cast<const EntityRep::ColumnImpl*>(read.column)->parent_rep_;
    if (entity_rep == nullptr || entity_rep->owning_batch_rep_ == nullptr ||
        entity_rep->owning_batch_rep_ != this_rep) {
      *read.status =
          Status::InvalidArgument("Column does not belong to this batch");
      continue;
    }
    // A read with no output buffer only wants to surface an I/O/integrity
    // error; resolve it individually (ResolveColumnRange handles the
    // null-result case), and defensively serve entities with no resolver
    // individually too.
    if (read.result == nullptr || !entity_rep->resolver_) {
      serve_individually.push_back(&read);
      continue;
    }

    const size_t column_index = read.column->index();
    // A column with any force_verify read in this batch is resolved entirely on
    // the individual path (see force_verify_columns above) so verification is
    // never skipped by a coalesced adoption.
    if (force_verify_columns.count({entity_rep, column_index}) != 0) {
      serve_individually.push_back(&read);
      continue;
    }
    const ReadPathBlobResolver::LazyColumnReadClassification cls =
        entity_rep->resolver_->ClassifyColumnRange(
            column_index, read.offset, read.length, read.force_verify);
    // If some force_verify read in this batch targets the same physical blob
    // (e.g. this read is for a duplicate key referencing that record), keep
    // this read on the individual path too, so its non-verifying coalesced
    // fetch cannot fill the shared blob cache ahead of the force_verify read
    // (which would then hit the cache and skip verification).
    if (cls.blob_index != nullptr &&
        force_verify_blobs.count(
            MakePhysicalBlobId(*entity_rep->resolver_, *cls.blob_index)) != 0) {
      serve_individually.push_back(&read);
      continue;
    }
    if (resolve_ro == nullptr) {
      resolve_ro = &entity_rep->resolver_->read_options();
    }
    switch (cls.plan) {
      case Plan::kServeIndividually:
        serve_individually.push_back(&read);
        break;
      case Plan::kFetchWholeSeparateFile:
      case Plan::kFetchWholeSameFile: {
        const std::pair<EntityRep*, size_t> key{entity_rep, column_index};
        auto it = whole_index.find(key);
        size_t idx;
        if (it == whole_index.end()) {
          idx = whole_fetches.size();
          whole_index.emplace(key, idx);
          whole_fetches.emplace_back();
          WholeFetch& wf = whole_fetches.back();
          wf.entity_rep = entity_rep;
          wf.column_index = column_index;
          wf.same_file = (cls.plan == Plan::kFetchWholeSameFile);
          wf.blob_index = cls.blob_index;
        } else {
          idx = it->second;
        }
        whole_bindings.push_back(WholeBinding{idx, &read});
        break;
      }
      case Plan::kFetchRangeSeparateFile:
      case Plan::kFetchRangeSameFile: {
        RangeFetch rf;
        rf.entity_rep = entity_rep;
        rf.same_file = (cls.plan == Plan::kFetchRangeSameFile);
        rf.blob_index = cls.blob_index;
        rf.range_offset = cls.range_offset;
        rf.range_length = cls.range_length;
        rf.result = read.result;
        rf.status = read.status;
        range_fetches.push_back(std::move(rf));
        break;
      }
    }
  }

  // Pass 2: build one dispatch list per group and issue a coalesced read.
  // Separate-file reads (whole + range) go through Version::MultiGetBlobLazy
  // (grouped by Version, coalesced per blob file inside); same-file reads
  // through SameFileBlobReader::MultiGetSameFileBlob (grouped by SST, coalesced
  // there).
  if (resolve_ro != nullptr) {
    std::map<const Version*, autovector<Version::LazyBlobReadRequest>>
        separate_groups;
    std::map<const SameFileBlobReader*, std::vector<SameFileBlobReadRequest>>
        same_groups;
    // Whole reads are never force-verify here (those are served individually),
    // so the verify policy is just today's global verify_checksums.
    const BlobVerifyPolicy whole_policy =
        resolve_ro->verify_checksums
            ? BlobVerifyPolicy::kVerifyIfNoAmplification
            : BlobVerifyPolicy::kSkip;

    for (WholeFetch& wf : whole_fetches) {
      ReadPathBlobResolver& resolver = *wf.entity_rep->resolver_;
      if (wf.same_file) {
        SameFileBlobReadRequest req;
        req.blob_index = wf.blob_index;
        req.range_offset = 0;
        req.range_length = kWholeBlobLength;
        req.verify_policy = whole_policy;
        req.result = &wf.temp;
        req.status = &wf.status;
        same_groups[resolver.same_file_reader()].push_back(req);
      } else {
        Version::LazyBlobReadRequest req;
        req.user_key = &resolver.user_key();
        req.blob_index = wf.blob_index;
        req.range_offset = 0;
        req.range_length = kWholeBlobLength;
        req.result = &wf.temp;
        req.status = &wf.status;
        separate_groups[resolver.version()].emplace_back(req);
      }
    }
    for (RangeFetch& rf : range_fetches) {
      ReadPathBlobResolver& resolver = *rf.entity_rep->resolver_;
      if (rf.same_file) {
        SameFileBlobReadRequest req;
        req.blob_index = rf.blob_index;
        req.range_offset = rf.range_offset;
        req.range_length = rf.range_length;
        req.verify_policy =
            whole_policy;  // range never verifies; policy unused
        req.result = rf.result;
        req.status = rf.status;
        same_groups[resolver.same_file_reader()].push_back(req);
      } else {
        Version::LazyBlobReadRequest req;
        req.user_key = &resolver.user_key();
        req.blob_index = rf.blob_index;
        req.range_offset = rf.range_offset;
        req.range_length = rf.range_length;
        req.result = rf.result;
        req.status = rf.status;
        separate_groups[resolver.version()].emplace_back(req);
      }
    }

    for (auto& [version, reqs] : separate_groups) {
      version->MultiGetBlobLazy(*resolve_ro, reqs);
    }
    for (auto& [reader, reqs] : same_groups) {
      reader->MultiGetSameFileBlob(*resolve_ro, reqs.size(), reqs.data());
    }
  }

  // Pass 3: adopt the fetched whole values into their resolvers' caches so the
  // slice below (and any later read of the same column) does no further I/O.
  for (WholeFetch& wf : whole_fetches) {
    if (wf.status.ok()) {
      wf.entity_rep->resolver_->AdoptResolvedWholeColumn(wf.column_index,
                                                         std::move(wf.temp));
    }
  }

  // Pass 4: finalize each whole read by slicing the requested range out of the
  // (now cached) whole value; a failed fetch leaves an empty result.
  for (WholeBinding& b : whole_bindings) {
    WholeFetch& wf = whole_fetches[b.fetch_idx];
    if (!wf.status.ok()) {
      *b.read->status = wf.status;
      continue;
    }
    const Status s = wf.entity_rep->resolver_->ResolveColumnRange(
        b.read->column->index(), b.read->offset, b.read->length,
        b.read->force_verify, b.read->result);
    *b.read->status = s;
  }

  // Pass 5: serve the individual (non-coalesced) reads.
  for (LazyColumnReadRequest* read : serve_individually) {
    EntityRep* entity_rep =
        static_cast<const EntityRep::ColumnImpl*>(read->column)->parent_rep_;
    entity_rep->ResolveOneRead(read->column->index(), *read);
  }

  return Status::OK();
}

void LazyWideColumnsBatch::Reset() { rep_.reset(); }

// ---- LazyWideColumnsHelper (DB-side construction) ----

PinnableWideColumns* LazyWideColumnsHelper::EntityBuffer(
    LazyWideColumns* result) {
  assert(result);
  if (!result->rep_) {
    result->rep_ = std::make_unique<LazyWideColumns::Rep>();
  }
  return &result->rep_->entity_;
}

Status LazyWideColumnsHelper::Finalize(
    LazyWideColumns* result, const Slice& user_key, const Version* version,
    const ReadOptions& read_options, BlobFileCache* blob_file_cache,
    bool allow_write_path_fallback, const SameFileBlobReader* same_file_reader,
    Cleanable&& pin) {
  assert(result);
  assert(result->rep_);
  // Take ownership of the per-result SuperVersion pin, then set up the resolver
  // exactly as the batched path does.
  result->rep_->pin_ = std::move(pin);
  return FinalizeInBatch(result, user_key, version, read_options,
                         blob_file_cache, allow_write_path_fallback,
                         same_file_reader);
}

Status LazyWideColumnsHelper::FinalizeInBatch(
    LazyWideColumns* result, const Slice& user_key, const Version* version,
    const ReadOptions& read_options, BlobFileCache* blob_file_cache,
    bool allow_write_path_fallback,
    const SameFileBlobReader* same_file_reader) {
  assert(result);
  assert(result->rep_);
  LazyWideColumns::Rep& rep = *result->rep_;

  // Decode the blob references (if the entity still has unresolved ones -- it
  // does not for a fully inline entity, or one resolved eagerly on the memtable
  // path). When unresolved indices are present the entity occupies a single
  // backing buffer, so GetSerializedEntity() is valid.
  if (!PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(rep.entity_)
           .empty()) {
    WideColumns unused_columns;
    const Status s = WideColumnSerialization::Deserialize(
        PinnableWideColumnsHelper::GetSerializedEntity(rep.entity_),
        unused_columns, &rep.blob_columns_);
    if (!s.ok()) {
      return s;
    }
  }

  // Build the per-column views from the (sorted) column set + blob references.
  const WideColumns& columns = rep.entity_.columns();
  rep.columns_.clear();
  rep.columns_.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    const BlobIndex* blob_index =
        blob_resolver_util::FindBlobColumn(&rep.blob_columns_, i);
    const char* inline_data = nullptr;
    std::optional<uint64_t> size;
    if (blob_index == nullptr) {
      // Inline column: bytes available with no I/O. Slice guarantees a non-null
      // data(), so a non-null inline_data marks this as inline (not a
      // reference) even for an empty value.
      inline_data = columns[i].value().data();
      size = columns[i].value().size();
    } else if (blob_index->IsInlined()) {
      // TTL-inlined blob: an unresolved reference whose logical size is known
      // (the value lives in the index).
      size = blob_index->value().size();
    } else if (blob_index->compression() == kNoCompression) {
      // Uncompressed reference: on-disk size equals logical size.
      size = blob_index->size();
    }
    // else: compressed reference -- logical size unknown until resolved.
    rep.columns_.emplace_back(&rep, i, columns[i].name(), inline_data, size);
  }

  // Stand up the resolver bound to the (address-stable, since Rep is
  // heap-allocated) entity columns + blob references. The resolver's deferred
  // blob-byte reads are attributed to Env::IOActivity::kLazyResolve (distinct
  // from the kGetEntity/kMultiGetEntity of the initial entity read that already
  // completed via GetImpl). A standalone result took ownership of its pin in
  // Finalize; a batched entity relies on its enclosing batch's shared pin.
  rep.user_key_.assign(user_key.data(), user_key.size());
  ReadOptions resolve_read_options(read_options);
  resolve_read_options.io_activity = Env::IOActivity::kLazyResolve;
  rep.resolver_.emplace(version, resolve_read_options, blob_file_cache,
                        allow_write_path_fallback);
  rep.resolver_->Reset(Slice(rep.user_key_), &rep.entity_.columns(),
                       &rep.blob_columns_, same_file_reader);
  return Status::OK();
}

void LazyWideColumnsHelper::InitBatch(LazyWideColumnsBatch* batch,
                                      size_t num_entities) {
  assert(batch);
  if (!batch->rep_) {
    batch->rep_ = std::make_unique<LazyWideColumnsBatch::Rep>();
  }
  batch->rep_->entities.clear();
  batch->rep_->entities.resize(num_entities);
}

Cleanable* LazyWideColumnsHelper::BatchCfPin(LazyWideColumnsBatch* batch,
                                             uint32_t cf_id) {
  assert(batch);
  assert(batch->rep_);
  return &batch->rep_->cf_pins[cf_id];
}

void LazyWideColumnsHelper::FinalizeBatch(LazyWideColumnsBatch* batch) {
  assert(batch);
  if (!batch->rep_) {
    return;
  }
  // Link each populated entity to this batch (by the batch Rep's stable
  // identity) so batch reads can validate that a column belongs to this batch.
  const void* batch_rep = batch->rep_.get();
  for (LazyWideColumns& entity : batch->rep_->entities) {
    if (entity.rep_) {
      entity.rep_->owning_batch_rep_ = batch_rep;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
