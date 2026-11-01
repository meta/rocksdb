// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_manifest.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Version;
struct BlobReferenceCensusDelta;

// Scalar physical-file accounting carried by the same installed delta as its
// routes. A zero total retires the candidate; ordinary identity roots and
// relocation files share one per-CF ratio heap.
struct BlobGcPhysicalFileSummary {
  uint64_t file_number = 0;
  uint64_t total_record_bytes = 0;
  bool is_identity_root = false;
  bool is_marked_missing = false;
};

// Indexed max heap updated by the single census worker, never under DB mutex.
// Live bytes are a conservative occurrence sum, not a deletion proof: the
// selected exact scan deduplicates stable IDs before authorizing relocation.
class BlobGcCandidateHeap {
 public:
  static constexpr size_t kMaxSelectedFiles = kBlobGcMaxSelectedFiles;
  using Selection = std::array<uint64_t, kMaxSelectedFiles>;

  void SetPhysicalFile(const BlobGcPhysicalFileSummary& file);
  void SetCoarseIdentityFrontier(uint64_t frontier);
  Status AdjustLiveBytes(uint64_t file_number, uint64_t bytes, bool add);
  void DeletePhysicalFile(uint64_t file_number);
  bool IsIdentityRoot(uint64_t file_number) const;
  bool IsCoarseIdentityPinned(const BlobRelocationRouteSource& source) const {
    return source.generation_file_number == source.origin_file_number &&
           coarse_identity_frontier_ != 0 &&
           source.origin_file_number >= coarse_identity_frontier_;
  }
  Selection Select() const;
  Selection SelectEmpty() const;
  uint64_t Revision(uint64_t file_number) const;
  size_t size() const { return candidates_.size(); }
  uint64_t RelocationGarbageBytes() const { return relocation_garbage_bytes_; }
  size_t MemoryUsage(size_t additional_candidates) const;

  // Off-mutex suppression persists across unrelated Version changes. A
  // relevant byte/membership change immediately resets it. Transient failures
  // have a retry time; corrupt candidates wait for such a relevant change or
  // a reopen/repair rebuild, not an unrelated compaction.
  void Suppress(uint64_t file_number, uint64_t retry_micros);
  void SuppressTransient(uint64_t file_number, uint64_t now_micros);
  uint64_t RetryDue(uint64_t now_micros);

  static int CompareRatios(uint64_t lhs_garbage, uint64_t lhs_total,
                           uint64_t rhs_garbage, uint64_t rhs_total);

 private:
  static constexpr size_t kNotInHeap = std::numeric_limits<size_t>::max();
  struct Candidate {
    uint64_t file_number = 0;
    uint64_t total_bytes = 0;
    uint64_t live_bytes = 0;
    uint64_t retry_micros = 0;
    uint64_t revision = 0;
    size_t position = kNotInHeap;
    bool is_identity_root = false;
    bool is_marked_missing = false;
    bool suppressed = false;
    bool coarse_pinned = false;
    uint8_t failures = 0;
  };
  static bool Before(const Candidate* lhs, const Candidate* rhs);
  void Swap(size_t lhs, size_t rhs);
  void Fix(size_t position);
  void Activate(uint64_t file_number, Candidate* candidate);
  void Remove(Candidate* candidate);
  static uint64_t RelocationGarbage(const Candidate& candidate);
  void AdjustRelocationGarbage(uint64_t before, uint64_t after);
  std::map<uint64_t, Candidate> candidates_;
  // std::map node addresses are stable. Direct pointers keep each heap
  // comparison O(1), rather than hiding another map lookup at every level.
  std::vector<Candidate*> heap_;
  // Indexed retry schedule avoids scanning every blob file on a timer tick.
  std::set<std::pair<uint64_t, uint64_t>> retries_;
  uint64_t next_revision_ = 1;
  uint64_t coarse_identity_frontier_ = 0;
  uint64_t relocation_garbage_bytes_ = 0;
  bool relocation_garbage_overflow_ = false;
};

// Captured while the GC listener freezes its starting worker epoch. File
// revisions, not unrelated Version numbers, guard later failure reports.
struct BlobGcCandidateStamp {
  uint64_t worker_token = 0;
  BlobGcCandidateHeap::Selection files{};
  std::array<uint64_t, BlobGcCandidateHeap::kMaxSelectedFiles> revisions{};
};

// Process-local bridge protection for one unfinished SST publication. Sources
// name only physical generations/origins the job could emit, not every route
// in its input Version. The sorted buffer and its charge are prepared unlocked.
struct BlobRelocationPublicationDependencies {
  std::vector<BlobRelocationRouteSource> sources;
  std::shared_ptr<CacheReservationManager> reservation_manager;
};

struct BlobRelocationPublicationReservation {
  std::shared_ptr<const BlobRelocationPublicationDependencies> dependencies;
  bool registered = false;
  BlobRelocationPublicationReservation* previous = nullptr;
  BlobRelocationPublicationReservation* next = nullptr;
};

struct BlobRelocationPublicationSnapshot {
  static constexpr size_t kMaxPublications = 64;
  uint64_t revision = 0;
  std::array<std::shared_ptr<const BlobRelocationPublicationDependencies>,
             kMaxPublications>
      dependencies{};
  bool Contains(const BlobRelocationRouteSource& source) const;
};

// Single background worker's bounded initial-scan position. The Version and
// CFD pins are released by the DB-provided shared_ptr deleter outside its
// mutex; cancellation only detaches the context handle.
struct BlobReferenceCensusBuildContext {
  std::shared_ptr<CacheReservationManager> reservation_manager;
  std::shared_ptr<Version> starting_version;
  uint64_t token = 0;
  int level = 0;
  size_t next_file = 0;
  std::set<uint64_t> deleted_starting_files;
  std::list<std::shared_ptr<const BlobReferenceCensusDelta>> pending_deltas;
  bool scan_complete = false;
  uint64_t candidate_retry_micros = 0;

  Status ReserveInitialFiles(size_t file_count);
};

// Immutable selected-route membership snapshot for one GC job's Version.
class BlobReferenceCensusState {
 public:
  explicit BlobReferenceCensusState(uint64_t version_number)
      : version_number_(version_number) {}
  BlobReferenceCensusState(const BlobReferenceCensusState&) = default;

  uint64_t version_number() const { return version_number_; }
  size_t route_source_count() const { return route_source_count_; }
  size_t sst_summary_count() const { return sst_summary_count_; }

  const std::vector<uint64_t>* FindSstFiles(
      const BlobRelocationRouteSource& source) const;
  std::shared_ptr<const BlobReferenceSstSummary> FindSstSummary(
      uint64_t file_number) const;
  size_t GetSstCount(const BlobRelocationRouteSource& source) const;

 private:
  friend class BlobReferenceCensus;

  static constexpr size_t kShardCount = 64;
  using SstFilesByRoute =
      std::map<BlobRelocationRouteSource, std::vector<uint64_t>>;
  using SstSummaries =
      std::map<uint64_t, std::shared_ptr<const BlobReferenceSstSummary>>;

  static size_t RouteShard(const BlobRelocationRouteSource& source);
  static size_t SstShard(uint64_t file_number);

  uint64_t version_number_;
  std::array<std::shared_ptr<SstFilesByRoute>, kShardCount> route_shards_;
  std::array<std::shared_ptr<SstSummaries>, kShardCount> sst_shards_;
  size_t route_source_count_ = 0;
  size_t sst_summary_count_ = 0;
  // A GC job copies only memberships for its selected physical files. Later
  // listener deltas apply to this same selection, never expand to a DB-wide
  // census, and still advance the installed Version epoch.
  std::shared_ptr<const std::set<BlobRelocationRouteSource>> selected_sources_;
  // Each selected snapshot owns an independent reservation. COW listener
  // updates share a monotone peak reservation until the GC job releases every
  // snapshot, so retaining the old snapshot never leaves its buffers uncharged.
  std::shared_ptr<CacheReservationManager> reservation_manager_;
  size_t snapshot_memory_bound_ = 0;
};

struct BlobReferenceCensusDelta {
  uint64_t base_version_number = 0;
  uint64_t version_number = 0;
  std::vector<uint64_t> removed_files;
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> added_files;
  // The same installed epoch's immutable forward state and physical changes.
  // Reverse memberships must never be interpreted through a different route
  // epoch while the asynchronous worker catches up.
  std::shared_ptr<const BlobRelocationState> forward_state;
  std::vector<BlobRelocationRouteSource> changed_routes;
  std::vector<uint64_t> changed_physical_files;
  std::vector<BlobGcPhysicalFileSummary> physical_files;
  size_t retained_forward_state_bytes = 0;
  uint64_t coarse_identity_frontier = 0;

  // Conservative retained-byte bound, including summary buffers and changed
  // forward-state shards, not arbitrary "weights". Detached but unapplied
  // work remains charged until acknowledgement.
  size_t QueueWeight() const;
};

using BlobReferenceCensusDeltaQueue =
    std::list<std::shared_ptr<const BlobReferenceCensusDelta>>;

struct BlobReferenceChangeLogEntry {
  explicit BlobReferenceChangeLogEntry(
      std::shared_ptr<const BlobReferenceCensusDelta> value,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle> charge)
      : delta(std::move(value)),
        retained_bytes(delta->QueueWeight()),
        reservation(std::move(charge)) {}
  ~BlobReferenceChangeLogEntry();
  const std::shared_ptr<const BlobReferenceCensusDelta> delta;
  std::shared_ptr<BlobReferenceChangeLogEntry> next;
  size_t retained_bytes;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle> reservation;
};

struct BlobReferenceCensusPreparedInstall {
  std::shared_ptr<const BlobReferenceCensusDelta> delta;
  // One preallocated log entry is shared by every consumer. Installation
  // links it once; it never allocates a list node for each listener.
  std::shared_ptr<BlobReferenceChangeLogEntry> log_entry;
  bool invalidate_census = false;
  uint64_t reservation_id = 0;
  size_t delta_queue_weight = 0;
};

// Objects returned here must be destroyed outside the DB mutex.
struct BlobReferenceCensusGarbage {
  // Type-erased ownership of a canceled worker; exchanged without acquiring
  // worker_mutex_. The in-flight slice retains its own handle until return.
  std::shared_ptr<void> worker_state;
  std::shared_ptr<BlobReferenceCensusBuildContext> build_context;
  std::shared_ptr<const BlobReferenceCensusState> state;
  BlobReferenceCensusDeltaQueue deltas;
  // At most three cursor handles per consumer. The handles, including any
  // last-owned log prefix, are released by the caller outside the DB mutex.
  std::array<std::shared_ptr<BlobReferenceChangeLogEntry>, 7> log_entries;
};

// Per-CF listener and ready-state owner. Its small mutex is never held while
// acquiring the DB mutex or while parsing, allocating, copying, or destroying
// census-sized state.
class BlobReferenceCensus {
 public:
  static constexpr size_t kMaxQueuedWeight = 64 * 1024 * 1024;
  static constexpr size_t kMaxDeltasPerRead = 64;

  enum class PublishResult : uint8_t {
    kPublished,
    kQueueNotEmpty,
    kInstallPending,
    kStale,
  };

  enum class GcListenerCloseResult : uint8_t {
    kClosed,
    kQueueNotEmpty,
    kInstallPending,
    kStale,
  };

  BlobReferenceCensus() = default;
  BlobReferenceCensus(const BlobReferenceCensus&) = delete;
  BlobReferenceCensus& operator=(const BlobReferenceCensus&) = delete;

  // DB mutex held: link/unlink one preallocated intrusive node in O(1).
  // Unregister retains the handle in the job for unlocked destruction.
  Status RegisterPublication(BlobRelocationPublicationReservation* reservation);
  void UnregisterPublication(BlobRelocationPublicationReservation* reservation);
  // Snapshot only lightweight shared handles outside DB mutex. If more than
  // 64 publications overlap, GC backs off; ordinary compaction is not capped.
  Status SnapshotPublications(
      BlobRelocationPublicationSnapshot* snapshot) const;
  bool PublicationsMatch(uint64_t revision) const;

  // REQUIRES: DB mutex held. One metadata-only writer intent closes admission
  // across unlocked Version preparation and MANIFEST sync. Lease snapshots
  // are prepared before this scalar check. Existing leases remain registered.
  Status BeginMetadataCommit(uint64_t publication_revision);
  void EndMetadataCommit(bool authority_settled);
  // REQUIRES: DB mutex held, after successful replacement MANIFEST/CURRENT
  // publication and Version installation settle the previous I/O ambiguity.
  // An arbitrary successful append is not authority to release this fence.
  void ResolveMetadataCommitAmbiguity();
  // Never wait under DB mutex. An unresolved ambiguity returns Busy; compaction
  // admission must translate it into its dependency-retry path, not report it
  // as a new background error. Cancellation cannot resurrect admission.
  Status WaitForMetadataCommit();

  // REQUIRES: DB mutex held. This O(1) reservation closes the gap between
  // unlocked delta preparation and Version publication.
  uint64_t BeginInstallPreparation();
  void CancelInstallPreparation(uint64_t reservation_id);

  Status PrepareInstall(std::shared_ptr<const BlobReferenceCensusDelta> delta,
                        uint64_t reservation_id,
                        BlobReferenceCensusPreparedInstall* prepared) const;
  Status PrepareInstallWithCache(
      std::shared_ptr<const BlobReferenceCensusDelta> delta,
      uint64_t reservation_id, std::shared_ptr<Cache> block_cache,
      BlobReferenceCensusPreparedInstall* prepared) const;

  // Prepares an allocation-free fail-closed action after optional census work
  // fails. Normal Version installation may proceed, but no stale or partial
  // census remains visible.
  Status PrepareInvalidation(
      uint64_t reservation_id,
      BlobReferenceCensusPreparedInstall* prepared) const;

  // REQUIRES: DB mutex held. Returns replaced or invalidated state for unlocked
  // destruction. The prepared queue node is spliced without allocation.
  BlobReferenceCensusGarbage Install(
      BlobReferenceCensusPreparedInstall& prepared);

  // REQUIRES: DB mutex held. Registers before the caller exposes a later
  // Version. The returned token uniquely identifies this build attempt.
  uint64_t BeginBuild();

  // Detaches queued handles for processing outside both mutexes.
  Status TakeBuildDeltas(uint64_t token, BlobReferenceCensusDeltaQueue* deltas,
                         bool* overflowed);

  // Releases the listener budget for handles the builder has finished
  // applying. Detached-but-unapplied handles remain charged so a slow initial
  // scan cannot accumulate an unbounded private backlog.
  Status AcknowledgeBuildDeltas(uint64_t token, size_t weight);

  // REQUIRES: DB mutex held. Failure leaves GC-facing state unavailable.
  BlobReferenceCensusGarbage FailBuild(uint64_t token);
  // REQUIRES: DB mutex held (or exclusive use in standalone tests). This also
  // serializes wakeup callback teardown with Register/Unregister/End/Resolve.
  BlobReferenceCensusGarbage Cancel();

  bool IsReady(uint64_t version_number) const;
  bool IsSnapshotPending() const;

  // REQUIRES: DB mutex held. At most one standalone GC listener is active for
  // a CF. The token closes the gap between the pinned starting Version and
  // later SST installations.
  Status BeginGcListener(uint64_t version_number, uint64_t* token);

  // Detaches immutable installed-file delta handles for processing outside
  // both the DB mutex and the census mutex.
  Status TakeGcDeltas(uint64_t token, BlobReferenceCensusDeltaQueue* deltas,
                      bool* overflowed);
  Status AcknowledgeGcDeltas(uint64_t token, size_t weight);
  Status WaitForGcChange(uint64_t token);

  // REQUIRES: DB mutex held and the caller is at the MANIFEST queue head.
  // Closing is allocation-free and succeeds only after all earlier installs
  // have been observed and acknowledged.
  GcListenerCloseResult TryCloseGcListener(uint64_t token,
                                           uint64_t applied_version_number,
                                           uint64_t current_version_number);

  // REQUIRES: DB mutex held. Returned handles must be destroyed unlocked.
  BlobReferenceCensusGarbage CancelGcListener(uint64_t token);

  static Status BuildState(
      uint64_t version_number,
      const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& files,
      std::shared_ptr<const BlobReferenceCensusState>* state);
  static Status ApplyDelta(
      const BlobReferenceCensusState& base,
      const BlobReferenceCensusDelta& delta,
      std::shared_ptr<const BlobReferenceCensusState>* state);

  // Single-worker mutation APIs. Call outside the DB mutex. The worker index
  // is derived state: it is not copied into Versions or on each install.
  Status ResetWorkerState(
      uint64_t token, uint64_t version_number,
      std::shared_ptr<const BlobRelocationState> forward_state,
      std::shared_ptr<Cache> block_cache);
  Status AddStartingFile(uint64_t token,
                         std::shared_ptr<const BlobReferenceSstSummary> file);
  Status SetWorkerPhysicalFiles(
      uint64_t token, const std::vector<BlobGcPhysicalFileSummary>& files);
  Status SetWorkerCoarseIdentityFrontier(uint64_t token, uint64_t frontier);
  BlobGcCandidateHeap::Selection SelectWorkerCandidates(uint64_t token);
  BlobGcCandidateHeap::Selection SelectWorkerEmptyCandidates(uint64_t token);
  using RetirementSelection = std::array<BlobRelocationRouteSource, 64>;
  RetirementSelection SelectWorkerRetirementSources(uint64_t token);
  uint64_t WorkerRelocationGarbageBytes(uint64_t token) const;
  uint64_t RetryWorkerCandidates(uint64_t token, uint64_t now_micros);
  bool NeedsWorkerRun(uint64_t version_number) const;
  // REQUIRES: DB mutex held. The DB/CFD-owned callback only enqueues work;
  // it neither blocks a LOW worker nor retains either owner. Install/cancel
  // invokes it after releasing the census mutex, still under the DB mutex.
  using BuildWakeup = void (*)(void*, void*);
  void SetBuildWakeup(BuildWakeup wakeup, void* db, void* cfd);
  Status CaptureCandidateStamp(uint64_t version_number,
                               const std::vector<uint64_t>& files,
                               BlobGcCandidateStamp* stamp) const;
  // Outside DB mutex. Reports from a replaced worker or changed source are
  // ignored. A retry deadline is used only for transient resource pressure.
  void SuppressWorkerCandidates(const BlobGcCandidateStamp& stamp,
                                const std::vector<uint64_t>& files,
                                const std::set<uint64_t>& transient_files,
                                uint64_t now_micros);
  Status ApplyWorkerDelta(uint64_t token,
                          const BlobReferenceCensusDelta& delta);
  uint64_t WorkerVersionNumber() const;
  PublishResult TryPublishWorker(uint64_t token,
                                 uint64_t applied_version_number,
                                 uint64_t current_version_number);
  std::shared_ptr<BlobReferenceCensusBuildContext> GetBuildContext() const;
  // Attach only while the context's token remains live. Invalidation may run
  // during unlocked preparation; rejected ownership is released unlocked.
  // An overflowed live token still attaches for ordinary failure cleanup.
  bool TrySetBuildContext(
      std::shared_ptr<BlobReferenceCensusBuildContext> context);
  // Call outside the DB mutex after BeginGcListener has frozen the matching
  // ready epoch. Membership copying never blocks an install.
  Status SnapshotSelected(
      uint64_t token, uint64_t version_number,
      const std::set<BlobRelocationRouteSource>& sources,
      std::shared_ptr<const BlobReferenceCensusState>* state);
  Status ReadSelectedSnapshot(
      uint64_t version_number,
      const std::set<BlobRelocationRouteSource>& sources,
      std::shared_ptr<const BlobReferenceCensusState>* state) const;
  // Test-only full materialization; call outside the DB mutex.
  std::shared_ptr<const BlobReferenceCensusState> TEST_GetReadyState(
      uint64_t version_number) const;

 private:
  struct WorkerState;
  static bool IsCensusGroup(const BlobReferenceGroup& group);
  static Status AddFile(
      const std::shared_ptr<const BlobReferenceSstSummary>& file,
      BlobReferenceCensusState* state);
  static Status RemoveFile(uint64_t file_number,
                           BlobReferenceCensusState* state);
  Status CopyWorkerSnapshot(
      const WorkerState& worker, uint64_t version_number,
      const std::set<BlobRelocationRouteSource>& sources,
      std::shared_ptr<const BlobReferenceCensusState>* state) const;
  static size_t SnapshotMemoryBound(const BlobReferenceCensusState& state);

  struct ChangeLogCursor {
    uint64_t token = 0;
    std::shared_ptr<BlobReferenceChangeLogEntry> first;
    std::shared_ptr<BlobReferenceChangeLogEntry> acknowledged;
    std::shared_ptr<BlobReferenceChangeLogEntry> delivered;
    size_t outstanding_bytes = 0;
    bool overflowed = false;
  };
  static bool HasPending(const ChangeLogCursor& cursor);
  static void DetachCursor(ChangeLogCursor* cursor,
                           BlobReferenceCensusGarbage* garbage,
                           size_t first_slot);
  void AppendChange(const BlobReferenceCensusPreparedInstall& prepared,
                    BlobReferenceCensusGarbage* garbage);
  Status TakeDeltas(uint64_t token, ChangeLogCursor* cursor,
                    BlobReferenceCensusDeltaQueue* deltas, bool* overflowed);
  Status AcknowledgeDeltas(uint64_t token, ChangeLogCursor* cursor,
                           size_t retained_bytes);
  void ResetCursorAfterDrain(ChangeLogCursor* cursor);

  struct WorkerRoute {
    std::set<uint64_t> sst_files;
    uint64_t reference_count = 0;
    uint64_t stored_value_bytes = 0;
    uint64_t direct_record_bytes = 0;
    uint64_t physical_file_number = 0;
  };
  struct WorkerState {
    uint64_t token = 0;
    uint64_t version_number = 0;
    std::map<BlobRelocationRouteSource, WorkerRoute> routes;
    std::set<BlobRelocationRouteSource> retirement_sources;
    BlobRelocationRouteSource retirement_cursor{};
    std::map<uint64_t, std::shared_ptr<const BlobReferenceSstSummary>> files;
    std::shared_ptr<const BlobRelocationState> forward_state;
    std::shared_ptr<Cache> block_cache;
    std::shared_ptr<CacheReservationManager> reservation_manager;
    size_t membership_count = 0;
    BlobGcCandidateHeap candidates;

    size_t MemoryUsage(size_t additional_memberships,
                       size_t additional_candidates) const;
    Status Reserve(size_t additional_memberships);
    Status Reserve(size_t additional_memberships, size_t additional_candidates);
    Status AddFile(std::shared_ptr<const BlobReferenceSstSummary> file);
    Status RemoveFile(uint64_t file_number);
    uint64_t ResolvePhysical(const BlobRelocationRouteSource& source) const;
    Status AdjustRouteBytes(const WorkerRoute& route, bool add);
  };
  // Never acquire this mutex while holding the DB mutex. Worker map mutation
  // and selected-only GC copying use it; install/readiness use scalar state
  // and the separate small log mutex instead.
  mutable std::mutex worker_mutex_;
  mutable std::mutex publications_mutex_;
  std::condition_variable publications_cv_;
  bool metadata_commit_pending_ = false;
  bool metadata_commit_ambiguous_ = false;
  bool publications_canceled_ = false;
  BlobRelocationPublicationReservation* publications_ = nullptr;
  size_t publication_count_ = 0;
  uint64_t publication_revision_ = 1;
  // Use portable shared_ptr atomic functions, also on pre-C++20 toolchains.
  std::shared_ptr<WorkerState> worker_state_;
  std::atomic<uint64_t> worker_active_token_{0};
  std::atomic<uint64_t> worker_ready_version_{0};
  std::atomic<bool> candidate_refresh_requested_{false};
  bool snapshot_pending_ = false;           // protected by the small log mutex
  bool build_waiting_for_install_ = false;  // protected by the log mutex
  // Protected by the DB mutex; cleared before owner teardown.
  BuildWakeup build_wakeup_ = nullptr;
  void* build_wakeup_db_ = nullptr;
  void* build_wakeup_cfd_ = nullptr;

  mutable std::mutex mutex_;
  // Shared concurrent reservation uses one cache granularity bucket across
  // all small log entries. RAII handles may be released by either consumer,
  // always outside the DB mutex, and keep the manager alive through teardown.
  mutable std::shared_ptr<CacheReservationManager> log_reservation_manager_;
  mutable std::shared_ptr<Cache> log_block_cache_;
  std::shared_ptr<BlobReferenceCensusBuildContext> build_context_;
  // A strong pin makes the only mutable link non-reclaimable. Detach it when
  // both cursors drain/cancel, and release it outside the DB/log mutexes.
  // Interior links are immutable, so unlocked iterative reclamation cannot
  // race an append resurrecting a weakly reachable interior node.
  std::shared_ptr<BlobReferenceChangeLogEntry> log_tail_;
  ChangeLogCursor build_cursor_;
  ChangeLogCursor gc_cursor_;
  std::condition_variable build_cv_;
  uint64_t next_build_token_ = 1;
  uint64_t next_reservation_id_ = 1;
  std::atomic<uint64_t> active_install_reservation_{0};
  uint64_t next_gc_token_ = 1;
  bool canceled_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
