// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_reference_census.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "db/blob/blob_gen2_format.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool BlobRelocationPublicationSnapshot::Contains(
    const BlobRelocationRouteSource& source) const {
  for (const auto& dependency : dependencies) {
    if (dependency == nullptr) {
      break;
    }
    if (std::binary_search(dependency->sources.begin(),
                           dependency->sources.end(), source)) {
      return true;
    }
  }
  return false;
}

Status BlobReferenceCensus::RegisterPublication(
    BlobRelocationPublicationReservation* reservation) {
  assert(reservation != nullptr && !reservation->registered &&
         reservation->dependencies != nullptr);
  std::lock_guard<std::mutex> lock(publications_mutex_);
  if (publications_canceled_) {
    return Status::ColumnFamilyDropped();
  }
  if (metadata_commit_ambiguous_) {
    return Status::Busy("Blob metadata publication requires recovery");
  }
  if (metadata_commit_pending_) {
    return Status::TryAgain("Blob metadata publication admission is pending");
  }
  reservation->next = publications_;
  if (publications_ != nullptr) {
    publications_->previous = reservation;
  }
  publications_ = reservation;
  reservation->registered = true;
  ++publication_count_;
  // Exhaustion disables GC until reopen; it must not reuse an old token.
  if (publication_revision_ != 0) {
    ++publication_revision_;
  }
  return Status::OK();
}

void BlobReferenceCensus::UnregisterPublication(
    BlobRelocationPublicationReservation* reservation) {
  assert(reservation != nullptr);
  {
    std::lock_guard<std::mutex> lock(publications_mutex_);
    if (!reservation->registered) {
      return;
    }
    if (reservation->previous != nullptr) {
      reservation->previous->next = reservation->next;
    } else {
      publications_ = reservation->next;
    }
    if (reservation->next != nullptr) {
      reservation->next->previous = reservation->previous;
    }
    reservation->previous = nullptr;
    reservation->next = nullptr;
    reservation->registered = false;
    assert(publication_count_ != 0);
    --publication_count_;
    if (publication_revision_ != 0) {
      ++publication_revision_;
    }
    candidate_refresh_requested_.store(true, std::memory_order_release);
  }
  // The caller holds DB mutex. Invoke only the bounded enqueue callback,
  // after releasing the registry mutex; idle cleanup needs no unrelated write.
  if (build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
}

Status BlobReferenceCensus::SnapshotPublications(
    BlobRelocationPublicationSnapshot* snapshot) const {
  if (snapshot == nullptr) {
    return Status::InvalidArgument("Null blob publication snapshot");
  }
  // Old shared handles are released before taking the small registry mutex.
  *snapshot = {};
  std::lock_guard<std::mutex> lock(publications_mutex_);
  if (publication_revision_ == 0 ||
      publication_count_ > snapshot->dependencies.size()) {
    return Status::TryAgain("Blob publication snapshot needs GC backoff");
  }
  snapshot->revision = publication_revision_;
  size_t index = 0;
  for (const BlobRelocationPublicationReservation* reservation = publications_;
       reservation != nullptr; reservation = reservation->next) {
    snapshot->dependencies[index++] = reservation->dependencies;
  }
  return Status::OK();
}

bool BlobReferenceCensus::PublicationsMatch(uint64_t revision) const {
  std::lock_guard<std::mutex> lock(publications_mutex_);
  return revision != 0 && revision == publication_revision_;
}

Status BlobReferenceCensus::BeginMetadataCommit(uint64_t revision) {
  std::lock_guard<std::mutex> lock(publications_mutex_);
  if (publications_canceled_) {
    return Status::ColumnFamilyDropped();
  }
  if (metadata_commit_pending_ || metadata_commit_ambiguous_ || revision == 0 ||
      revision != publication_revision_) {
    return Status::TryAgain("Blob metadata publication proof changed");
  }
  metadata_commit_pending_ = true;
  return Status::OK();
}

void BlobReferenceCensus::EndMetadataCommit(bool authority_settled) {
  {
    std::lock_guard<std::mutex> lock(publications_mutex_);
    metadata_commit_pending_ = false;
    metadata_commit_ambiguous_ |= !authority_settled;
    candidate_refresh_requested_.store(true, std::memory_order_release);
    publications_cv_.notify_all();
  }
  if (authority_settled && build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
}

Status BlobReferenceCensus::WaitForMetadataCommit() {
  std::unique_lock<std::mutex> lock(publications_mutex_);
  publications_cv_.wait(lock, [&] {
    return !metadata_commit_pending_ || publications_canceled_;
  });
  if (publications_canceled_) {
    return Status::ColumnFamilyDropped();
  }
  return metadata_commit_ambiguous_
             ? Status::Busy("Blob metadata publication requires recovery")
             : Status::OK();
}

void BlobReferenceCensus::ResolveMetadataCommitAmbiguity() {
  {
    std::lock_guard<std::mutex> lock(publications_mutex_);
    if (!metadata_commit_ambiguous_ || metadata_commit_pending_ ||
        publications_canceled_) {
      return;
    }
    metadata_commit_ambiguous_ = false;
    if (publication_revision_ != 0) {
      ++publication_revision_;
    }
    candidate_refresh_requested_.store(true, std::memory_order_release);
    publications_cv_.notify_all();
  }
  if (build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
}

int BlobGcCandidateHeap::CompareRatios(uint64_t lhs_garbage, uint64_t lhs_total,
                                       uint64_t rhs_garbage,
                                       uint64_t rhs_total) {
  assert(lhs_total != 0 && rhs_total != 0);
  if ((lhs_garbage | lhs_total | rhs_garbage | rhs_total) <=
      std::numeric_limits<uint32_t>::max()) {
    const uint64_t lhs = lhs_garbage * rhs_total;
    const uint64_t rhs = rhs_garbage * lhs_total;
    return lhs == rhs ? 0 : (lhs < rhs ? -1 : 1);
  }
  // Compare continued fractions exactly. Taking reciprocals reverses order;
  // Euclid's algorithm terminates in O(log(max denominator)) without floating
  // point rounding, overflowing products, or a non-portable 128-bit integer.
  bool reversed = false;
  for (;;) {
    const uint64_t lhs_integer = lhs_garbage / lhs_total;
    const uint64_t rhs_integer = rhs_garbage / rhs_total;
    if (lhs_integer != rhs_integer) {
      const int result = lhs_integer < rhs_integer ? -1 : 1;
      return reversed ? -result : result;
    }
    lhs_garbage %= lhs_total;
    rhs_garbage %= rhs_total;
    if (lhs_garbage == 0 || rhs_garbage == 0) {
      const int result =
          lhs_garbage == rhs_garbage ? 0 : (lhs_garbage == 0 ? -1 : 1);
      return reversed ? -result : result;
    }
    std::swap(lhs_garbage, lhs_total);
    std::swap(rhs_garbage, rhs_total);
    reversed = !reversed;
  }
}

bool BlobGcCandidateHeap::Before(const Candidate* lhs, const Candidate* rhs) {
  const Candidate& left = *lhs;
  const Candidate& right = *rhs;
  const int order = CompareRatios(
      left.total_bytes - std::min(left.live_bytes, left.total_bytes),
      left.total_bytes,
      right.total_bytes - std::min(right.live_bytes, right.total_bytes),
      right.total_bytes);
  return order != 0 ? order > 0 : left.file_number < right.file_number;
}

void BlobGcCandidateHeap::Swap(size_t lhs, size_t rhs) {
  std::swap(heap_[lhs], heap_[rhs]);
  heap_[lhs]->position = lhs;
  heap_[rhs]->position = rhs;
}

void BlobGcCandidateHeap::Fix(size_t position) {
  while (position != 0) {
    const size_t parent = (position - 1) / 2;
    if (!Before(heap_[position], heap_[parent])) {
      break;
    }
    Swap(position, parent);
    position = parent;
  }
  for (;;) {
    const size_t left = position * 2 + 1;
    if (left >= heap_.size()) {
      break;
    }
    size_t child = left;
    if (left + 1 < heap_.size() && Before(heap_[left + 1], heap_[left])) {
      child = left + 1;
    }
    if (!Before(heap_[child], heap_[position])) {
      break;
    }
    Swap(position, child);
    position = child;
  }
}

void BlobGcCandidateHeap::Activate(uint64_t file_number, Candidate* candidate) {
  if (candidate->retry_micros != 0) {
    retries_.erase({candidate->retry_micros, file_number});
  }
  candidate->retry_micros = 0;
  candidate->suppressed = false;
  // A loss target cannot supply live bytes, but after its SST references are
  // gone it still participates in exact, listener-protected metadata cleanup.
  if (candidate->coarse_pinned ||
      (candidate->is_marked_missing && candidate->live_bytes != 0)) {
    Remove(candidate);
    return;
  }
  if (candidate->position == kNotInHeap) {
    candidate->position = heap_.size();
    heap_.push_back(candidate);
  }
  Fix(candidate->position);
}

void BlobGcCandidateHeap::Remove(Candidate* candidate) {
  if (candidate->position == kNotInHeap) {
    return;
  }
  const size_t position = candidate->position;
  Swap(position, heap_.size() - 1);
  heap_.pop_back();
  candidate->position = kNotInHeap;
  if (position < heap_.size()) {
    Fix(position);
  }
}

uint64_t BlobGcCandidateHeap::RelocationGarbage(const Candidate& candidate) {
  return candidate.is_identity_root
             ? 0
             : candidate.total_bytes -
                   std::min(candidate.live_bytes, candidate.total_bytes);
}

void BlobGcCandidateHeap::AdjustRelocationGarbage(uint64_t before,
                                                  uint64_t after) {
  if (relocation_garbage_overflow_) {
    return;
  }
  if (before > relocation_garbage_bytes_ ||
      after > std::numeric_limits<uint64_t>::max() -
                  (relocation_garbage_bytes_ - before)) {
    // Even an unrepresentable transient aggregate must remain conservative.
    // Zero known garbage counts the entire file as live until a rebuild.
    relocation_garbage_overflow_ = true;
    relocation_garbage_bytes_ = 0;
    return;
  }
  relocation_garbage_bytes_ = relocation_garbage_bytes_ - before + after;
}

void BlobGcCandidateHeap::SetPhysicalFile(
    const BlobGcPhysicalFileSummary& file) {
  assert(file.file_number != 0);
  if (file.total_record_bytes == 0) {
    DeletePhysicalFile(file.file_number);
    return;
  }
  Candidate& candidate = candidates_[file.file_number];
  candidate.file_number = file.file_number;
  if (candidate.total_bytes == file.total_record_bytes &&
      candidate.is_identity_root == file.is_identity_root &&
      candidate.is_marked_missing == file.is_marked_missing) {
    return;
  }
  const uint64_t previous_garbage = RelocationGarbage(candidate);
  candidate.total_bytes = file.total_record_bytes;
  candidate.is_identity_root = file.is_identity_root;
  candidate.is_marked_missing = file.is_marked_missing;
  AdjustRelocationGarbage(previous_garbage, RelocationGarbage(candidate));
  candidate.coarse_pinned = file.is_identity_root &&
                            coarse_identity_frontier_ != 0 &&
                            file.file_number >= coarse_identity_frontier_;
  candidate.failures = 0;
  candidate.revision = next_revision_++;
  if (next_revision_ == 0) {
    ++next_revision_;
  }
  Activate(file.file_number, &candidate);
}

void BlobGcCandidateHeap::SetCoarseIdentityFrontier(uint64_t frontier) {
  if (frontier == coarse_identity_frontier_) {
    return;
  }
  const uint64_t old_boundary = coarse_identity_frontier_ == 0
                                    ? std::numeric_limits<uint64_t>::max()
                                    : coarse_identity_frontier_;
  const uint64_t new_boundary =
      frontier == 0 ? std::numeric_limits<uint64_t>::max() : frontier;
  coarse_identity_frontier_ = frontier;
  // Only the file-number interval whose pin state changed is touched. This
  // is background work, not an all-file scan per heap selection or install.
  for (std::map<uint64_t, Candidate>::iterator entry =
           candidates_.lower_bound(std::min(old_boundary, new_boundary));
       entry != candidates_.end() &&
       entry->first < std::max(old_boundary, new_boundary);
       ++entry) {
    Candidate& candidate = entry->second;
    if (!candidate.is_identity_root) {
      continue;
    }
    candidate.coarse_pinned = frontier != 0 && entry->first >= frontier;
    candidate.revision = next_revision_++;
    if (next_revision_ == 0) {
      ++next_revision_;
    }
    Activate(entry->first, &candidate);
  }
}

Status BlobGcCandidateHeap::AdjustLiveBytes(uint64_t file_number,
                                            uint64_t bytes, bool add) {
  const std::map<uint64_t, Candidate>::iterator entry =
      candidates_.find(file_number);
  if (entry == candidates_.end() || bytes == 0) {
    return Status::OK();
  }
  Candidate& candidate = entry->second;
  if ((add &&
       bytes > std::numeric_limits<uint64_t>::max() - candidate.live_bytes) ||
      (!add && bytes > candidate.live_bytes)) {
    return Status::Corruption("Blob GC candidate live-byte overflow");
  }
  const uint64_t previous_garbage = RelocationGarbage(candidate);
  if (add) {
    candidate.live_bytes += bytes;
  } else {
    candidate.live_bytes -= bytes;
  }
  AdjustRelocationGarbage(previous_garbage, RelocationGarbage(candidate));
  candidate.failures = 0;
  candidate.revision = next_revision_++;
  if (next_revision_ == 0) {
    ++next_revision_;
  }
  Activate(file_number, &candidate);
  return Status::OK();
}

void BlobGcCandidateHeap::DeletePhysicalFile(uint64_t file_number) {
  const std::map<uint64_t, Candidate>::iterator entry =
      candidates_.find(file_number);
  if (entry == candidates_.end()) {
    return;
  }
  Candidate& candidate = entry->second;
  if (candidate.retry_micros != 0) {
    retries_.erase({candidate.retry_micros, file_number});
  }
  Remove(&candidate);
  AdjustRelocationGarbage(RelocationGarbage(candidate), 0);
  candidates_.erase(entry);
}

bool BlobGcCandidateHeap::IsIdentityRoot(uint64_t file_number) const {
  const std::map<uint64_t, Candidate>::const_iterator entry =
      candidates_.find(file_number);
  return entry != candidates_.end() && entry->second.is_identity_root;
}

uint64_t BlobGcCandidateHeap::Revision(uint64_t file_number) const {
  const std::map<uint64_t, Candidate>::const_iterator entry =
      candidates_.find(file_number);
  return entry != candidates_.end() ? entry->second.revision : 0;
}

BlobGcCandidateHeap::Selection BlobGcCandidateHeap::Select() const {
  Selection selected{};
  if (heap_.empty()) {
    return selected;
  }
  // Expanding a max heap's best frontier finds eight candidates without
  // sorting/scanning B files. Fixed storage also avoids selection allocations.
  std::array<size_t, 2 * kMaxSelectedFiles + 1> frontier{};
  size_t frontier_size = 1;
  for (size_t output = 0; output < selected.size() && frontier_size != 0;
       ++output) {
    size_t best = 0;
    for (size_t i = 1; i < frontier_size; ++i) {
      if (Before(heap_[frontier[i]], heap_[frontier[best]])) {
        best = i;
      }
    }
    const size_t position = frontier[best];
    frontier[best] = frontier[--frontier_size];
    const Candidate& candidate = *heap_[position];
    if (candidate.live_bytes > candidate.total_bytes / 2) {
      break;
    }
    selected[output] = candidate.file_number;
    for (size_t child = position * 2 + 1;
         child < heap_.size() && child <= position * 2 + 2; ++child) {
      assert(frontier_size < frontier.size());
      frontier[frontier_size++] = child;
    }
  }
  return selected;
}

void BlobGcCandidateHeap::Suppress(uint64_t file_number,
                                   uint64_t retry_micros) {
  const std::map<uint64_t, Candidate>::iterator entry =
      candidates_.find(file_number);
  if (entry == candidates_.end()) {
    return;
  }
  Candidate& candidate = entry->second;
  if (candidate.retry_micros != 0) {
    retries_.erase({candidate.retry_micros, file_number});
  }
  Remove(&candidate);
  candidate.suppressed = true;
  candidate.retry_micros = retry_micros;
  if (retry_micros != 0) {
    retries_.emplace(retry_micros, file_number);
  }
}

BlobGcCandidateHeap::Selection BlobGcCandidateHeap::SelectEmpty() const {
  Selection result = Select();
  // Empty entries rank first. This inspects only the fixed best prefix, not
  // every physical file. Occurrence sums may delay cleanup, never prove it:
  // the selected census scan and publication listener remain authoritative.
  bool found_live = false;
  for (uint64_t& file_number : result) {
    if (file_number != 0 && !found_live) {
      found_live = candidates_.at(file_number).live_bytes != 0;
    }
    if (found_live) {
      file_number = 0;
    }
  }
  return result;
}

uint64_t BlobGcCandidateHeap::RetryDue(uint64_t now_micros) {
  constexpr size_t kMaxRetriesPerSlice = 32;
  size_t count = 0;
  while (!retries_.empty() && retries_.begin()->first <= now_micros &&
         count++ < kMaxRetriesPerSlice) {
    const uint64_t file_number = retries_.begin()->second;
    const std::map<uint64_t, Candidate>::iterator entry =
        candidates_.find(file_number);
    assert(entry != candidates_.end());
    Activate(file_number, &entry->second);
  }
  return retries_.empty() ? 0 : retries_.begin()->first;
}

void BlobGcCandidateHeap::SuppressTransient(uint64_t file_number,
                                            uint64_t now_micros) {
  const std::map<uint64_t, Candidate>::iterator entry =
      candidates_.find(file_number);
  if (entry == candidates_.end()) {
    return;
  }
  Candidate& candidate = entry->second;
  candidate.failures = std::min<uint8_t>(candidate.failures + 1, 6);
  const uint64_t delay = uint64_t{1000000} << (candidate.failures - 1);
  Suppress(file_number,
           now_micros + std::min(delay, std::numeric_limits<uint64_t>::max() -
                                            now_micros));
}

size_t BlobGcCandidateHeap::MemoryUsage(size_t additional_candidates) const {
  constexpr size_t kCandidateBytes =
      sizeof(std::map<uint64_t, Candidate>::value_type) + 4 * sizeof(void*) +
      2 * sizeof(Candidate*) + sizeof(std::pair<uint64_t, uint64_t>) +
      4 * sizeof(void*);
  const size_t max_count = std::numeric_limits<size_t>::max() / kCandidateBytes;
  if (candidates_.size() > max_count ||
      additional_candidates > max_count - candidates_.size()) {
    return std::numeric_limits<size_t>::max();
  }
  return (candidates_.size() + additional_candidates) * kCandidateBytes;
}

Status BlobReferenceCensusBuildContext::ReserveInitialFiles(size_t file_count) {
  // Only deletions from the pinned initial Version are remembered. Their
  // number cannot exceed that Version's file count. Delivered-but-unapplied
  // deltas are capped separately by the change log; charge their list nodes.
  constexpr size_t kDeletedFileBytes = sizeof(uint64_t) + 4 * sizeof(void*);
  constexpr size_t kPendingNodesBytes =
      BlobReferenceCensus::kMaxDeltasPerRead *
      (sizeof(std::shared_ptr<const BlobReferenceCensusDelta>) +
       3 * sizeof(void*));
  constexpr size_t kBaseBytes =
      sizeof(BlobReferenceCensusBuildContext) + kPendingNodesBytes;
  if (file_count >
      (std::numeric_limits<size_t>::max() - kBaseBytes) / kDeletedFileBytes) {
    return Status::MemoryLimit("Blob census initial file count overflow");
  }
  return reservation_manager != nullptr
             ? reservation_manager->UpdateCacheReservation(
                   kBaseBytes + file_count * kDeletedFileBytes)
             : Status::OK();
}

size_t BlobReferenceCensus::WorkerState::MemoryUsage(
    size_t additional_memberships, size_t additional_candidates) const {
  // Conservative node bounds cover std::map/set links and allocator rounding.
  // Summary payloads own a separate shared reservation and are not charged
  // again for each census or log handle.
  constexpr size_t kMembershipBytes =
      sizeof(uint64_t) + 4 * sizeof(void*) +
      sizeof(std::map<BlobRelocationRouteSource, WorkerRoute>::value_type) +
      4 * sizeof(void*);
  constexpr size_t kFileBytes =
      sizeof(
          std::map<uint64_t, std::shared_ptr<const BlobReferenceSstSummary>>::
              value_type) +
      4 * sizeof(void*);
  size_t bytes = sizeof(*this);
  const auto add = [&bytes](size_t count, size_t item_size) {
    if (count == 0 || item_size == 0) {
      return;
    }
    if (count > (std::numeric_limits<size_t>::max() - bytes) / item_size) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * item_size;
    }
  };
  add(membership_count, kMembershipBytes);
  add(additional_memberships, kMembershipBytes);
  add(files.size(), kFileBytes);
  // Map entries, geometric heap pointer capacity, and a worst-case timed
  // suppression node. Candidate updates happen only in this worker.
  add(1, candidates.MemoryUsage(additional_candidates));
  // A new complete-empty SST still needs a file-map entry.
  add(1, kFileBytes);
  return bytes;
}

Status BlobReferenceCensus::WorkerState::Reserve(
    size_t additional_memberships) {
  return Reserve(additional_memberships, /*additional_candidates=*/0);
}

Status BlobReferenceCensus::WorkerState::Reserve(size_t additional_memberships,
                                                 size_t additional_candidates) {
  const size_t bytes =
      MemoryUsage(additional_memberships, additional_candidates);
  if (bytes == std::numeric_limits<size_t>::max()) {
    return Status::MemoryLimit("Blob census index size overflow");
  }
  return reservation_manager != nullptr
             ? reservation_manager->UpdateCacheReservation(bytes)
             : Status::OK();
}

Status BlobReferenceCensus::WorkerState::AddFile(
    std::shared_ptr<const BlobReferenceSstSummary> file) {
  if (file == nullptr || file->file_number == 0 ||
      files.find(file->file_number) != files.end()) {
    return Status::Corruption("Invalid or duplicate census SST summary");
  }
  Status status = Reserve(file->groups.size());
  if (!status.ok()) {
    return status;
  }
  for (const BlobReferenceGroup& group : file->groups) {
    if (!IsCensusGroup(group)) {
      continue;
    }
    const BlobRelocationRouteSource source{group.generation_file_number,
                                           group.origin_file_number};
    if (source.generation_file_number == 0 || source.origin_file_number == 0) {
      return Status::Corruption("Invalid census route group");
    }
    WorkerRoute& route = routes[source];
    if (route.sst_files.empty()) {
      route.physical_file_number = ResolvePhysical(source);
    }
    status = AdjustRouteBytes(route, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
    if (group.reference_count >
            std::numeric_limits<uint64_t>::max() - route.reference_count ||
        group.stored_value_bytes >
            std::numeric_limits<uint64_t>::max() - route.stored_value_bytes ||
        group.record_bytes >
            std::numeric_limits<uint64_t>::max() - route.direct_record_bytes) {
      return Status::Corruption("Blob census group accounting overflow");
    }
    if (!route.sst_files.insert(file->file_number).second) {
      return Status::Corruption("Duplicate census SST route membership");
    }
    ++membership_count;
    route.reference_count += group.reference_count;
    route.stored_value_bytes += group.stored_value_bytes;
    route.direct_record_bytes += group.record_bytes;
    status = AdjustRouteBytes(route, /*add=*/true);
    if (!status.ok()) {
      return status;
    }
  }
  const uint64_t file_number = file->file_number;
  files.emplace(file_number, std::move(file));
  return Status::OK();
}

Status BlobReferenceCensus::WorkerState::RemoveFile(uint64_t file_number) {
  const std::map<
      uint64_t, std::shared_ptr<const BlobReferenceSstSummary>>::iterator file =
      files.find(file_number);
  if (file == files.end()) {
    // An initial scan may not have reached this starting SST yet.
    return Status::OK();
  }
  for (const BlobReferenceGroup& group : file->second->groups) {
    if (!IsCensusGroup(group)) {
      continue;
    }
    const BlobRelocationRouteSource source{group.generation_file_number,
                                           group.origin_file_number};
    std::map<BlobRelocationRouteSource, WorkerRoute>::iterator route =
        routes.find(source);
    if (route == routes.end() ||
        route->second.reference_count < group.reference_count ||
        route->second.stored_value_bytes < group.stored_value_bytes ||
        route->second.direct_record_bytes < group.record_bytes ||
        route->second.sst_files.erase(file_number) != 1) {
      return Status::Corruption("Missing census SST route accounting");
    }
    Status status = AdjustRouteBytes(route->second, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
    --membership_count;
    route->second.reference_count -= group.reference_count;
    route->second.stored_value_bytes -= group.stored_value_bytes;
    route->second.direct_record_bytes -= group.record_bytes;
    status = AdjustRouteBytes(route->second, /*add=*/true);
    if (!status.ok()) {
      return status;
    }
    if (route->second.sst_files.empty()) {
      routes.erase(route);
    }
  }
  files.erase(file);
  return Status::OK();
}

uint64_t BlobReferenceCensus::WorkerState::ResolvePhysical(
    const BlobRelocationRouteSource& source) const {
  const BlobRelocationRoute* const route =
      forward_state != nullptr ? forward_state->FindRoute(source) : nullptr;
  return route != nullptr ? route->GetPhysicalFileNumber()
                          : source.generation_file_number;
}

Status BlobReferenceCensus::WorkerState::AdjustRouteBytes(
    const WorkerRoute& route, bool add) {
  uint64_t bytes = route.direct_record_bytes;
  if (!candidates.IsIdentityRoot(route.physical_file_number)) {
    if (route.reference_count >
        (std::numeric_limits<uint64_t>::max() - route.stored_value_bytes) /
            kSimpleGen2BlobTrailerSize) {
      return Status::Corruption("Blob GC candidate route-byte overflow");
    }
    bytes = route.stored_value_bytes +
            route.reference_count * kSimpleGen2BlobTrailerSize;
  }
  return candidates.AdjustLiveBytes(route.physical_file_number, bytes, add);
}

Status BlobReferenceCensus::ResetWorkerState(
    uint64_t token, uint64_t version_number,
    std::shared_ptr<const BlobRelocationState> forward_state,
    std::shared_ptr<Cache> block_cache) {
  if (token == 0 || version_number == 0) {
    return Status::InvalidArgument("Invalid census worker initialization");
  }
  std::shared_ptr<WorkerState> next = std::make_shared<WorkerState>();
  next->token = token;
  next->version_number = version_number;
  next->forward_state = std::move(forward_state);
  next->block_cache = std::move(block_cache);
  if (next->block_cache != nullptr) {
    next->reservation_manager = std::make_shared<CacheReservationManagerImpl<
        CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
        next->block_cache);
  }
  Status status = next->Reserve(0);
  if (!status.ok()) {
    return status;
  }
  if (next->forward_state != nullptr) {
    status = next->Reserve(0, next->forward_state->files().size());
    if (!status.ok()) {
      return status;
    }
    for (const std::shared_ptr<BlobRelocationFileMetaData>& file :
         next->forward_state->files()) {
      next->candidates.SetPhysicalFile(
          {file->GetFileNumber(), file->GetDirectory().total_record_bytes(),
           /*is_identity_root=*/false, file->IsMarkedMissing()});
    }
    status = next->Reserve(0);
    if (!status.ok()) {
      return status;
    }
  }
  {
    std::lock_guard<std::mutex> worker_lock(worker_mutex_);
    std::lock_guard<std::mutex> log_lock(mutex_);
    if (canceled_ || token != build_cursor_.token) {
      return Status::Aborted("Census worker initialization was canceled");
    }
    worker_ready_version_.store(0, std::memory_order_release);
    next = std::atomic_exchange(&worker_state_, std::move(next));
    worker_active_token_.store(token, std::memory_order_release);
  }
  // Replaced maps and reservations are destroyed outside both mutexes.
  return Status::OK();
}

Status BlobReferenceCensus::AddStartingFile(
    uint64_t token, std::shared_ptr<const BlobReferenceSstSummary> file) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  TEST_SYNC_POINT("BlobReferenceCensus::AddStartingFile:CapturedWorker");
  TEST_SYNC_POINT("BlobReferenceCensus::AddStartingFile:BeforeWorkerLock");
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != token ||
      token != worker_active_token_.load(std::memory_order_acquire)) {
    return Status::Aborted("Census worker was replaced");
  }
  // Starting files are added before READY publication. Do not let a canceled
  // slice with a retained worker handle invalidate a replacement's readiness.
  return worker->AddFile(std::move(file));
}

Status BlobReferenceCensus::SetWorkerPhysicalFiles(
    uint64_t token, const std::vector<BlobGcPhysicalFileSummary>& files) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != token ||
      token != worker_active_token_.load(std::memory_order_acquire)) {
    return Status::Aborted("Census worker was replaced");
  }
  Status status = worker->Reserve(0, files.size());
  if (!status.ok()) {
    return status;
  }
  for (const BlobGcPhysicalFileSummary& file : files) {
    if (file.file_number == 0) {
      return Status::Corruption("Invalid blob GC physical file number");
    }
    worker->candidates.SetPhysicalFile(file);
  }
  return worker->Reserve(0);
}

BlobGcCandidateHeap::Selection BlobReferenceCensus::SelectWorkerCandidates(
    uint64_t token) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != token ||
      token != worker_active_token_.load(std::memory_order_acquire)) {
    return {};
  }
  // Reports racing the subsequent publication set this again; the next LOW
  // slice then refreshes the bounded prefix even without a Version change.
  candidate_refresh_requested_.store(false, std::memory_order_release);
  return worker != nullptr && worker->token == token
             ? worker->candidates.Select()
             : BlobGcCandidateHeap::Selection{};
}

uint64_t BlobReferenceCensus::RetryWorkerCandidates(uint64_t token,
                                                    uint64_t now_micros) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  return worker != nullptr && worker->token == token
             ? worker->candidates.RetryDue(now_micros)
             : 0;
}

BlobGcCandidateHeap::Selection BlobReferenceCensus::SelectWorkerEmptyCandidates(
    uint64_t token) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  return worker != nullptr && worker->token == token
             ? worker->candidates.SelectEmpty()
             : BlobGcCandidateHeap::Selection{};
}

bool BlobReferenceCensus::NeedsWorkerRun(uint64_t version_number) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return !build_waiting_for_install_ &&
         (version_number == 0 ||
          worker_ready_version_.load(std::memory_order_acquire) !=
              version_number ||
          candidate_refresh_requested_.load(std::memory_order_acquire));
}

void BlobReferenceCensus::SetBuildWakeup(BuildWakeup wakeup, void* db,
                                         void* cfd) {
  build_wakeup_ = wakeup;
  build_wakeup_db_ = db;
  build_wakeup_cfd_ = cfd;
}

uint64_t BlobReferenceCensus::WorkerRelocationGarbageBytes(
    uint64_t token) const {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  return worker != nullptr && worker->token == token
             ? worker->candidates.RelocationGarbageBytes()
             : 0;
}

Status BlobReferenceCensus::CaptureCandidateStamp(
    uint64_t version_number, const std::vector<uint64_t>& files,
    BlobGcCandidateStamp* stamp) const {
  if (stamp == nullptr || files.size() > stamp->files.size()) {
    return Status::InvalidArgument("Invalid GC candidate stamp");
  }
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->version_number != version_number) {
    return Status::TryAgain("GC candidate worker epoch changed");
  }
  *stamp = {};
  stamp->worker_token = worker->token;
  for (size_t index = 0; index < files.size(); ++index) {
    stamp->files[index] = files[index];
    stamp->revisions[index] = worker->candidates.Revision(files[index]);
  }
  return Status::OK();
}

void BlobReferenceCensus::SuppressWorkerCandidates(
    const BlobGcCandidateStamp& stamp, const std::vector<uint64_t>& files,
    const std::set<uint64_t>& transient_files, uint64_t now_micros) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != stamp.worker_token ||
      worker->token != worker_active_token_.load(std::memory_order_acquire)) {
    return;
  }
  if (!files.empty()) {
    // Even an ignored stale report may have removed an ID from the Version's
    // bounded prefix. Republish that prefix from current worker state.
    candidate_refresh_requested_.store(true, std::memory_order_release);
  }
  for (uint64_t file_number : files) {
    for (size_t index = 0; index < stamp.files.size(); ++index) {
      if (stamp.files[index] != file_number || stamp.revisions[index] == 0 ||
          worker->candidates.Revision(file_number) != stamp.revisions[index]) {
        continue;
      }
      if (transient_files.count(file_number) != 0) {
        worker->candidates.SuppressTransient(file_number, now_micros);
      } else {
        worker->candidates.Suppress(file_number, /*retry_micros=*/0);
      }
      candidate_refresh_requested_.store(true, std::memory_order_release);
      break;
    }
  }
}

Status BlobReferenceCensus::SetWorkerCoarseIdentityFrontier(uint64_t token,
                                                            uint64_t frontier) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != token ||
      token != worker_active_token_.load(std::memory_order_acquire)) {
    return Status::Aborted("Census worker was replaced");
  }
  worker->candidates.SetCoarseIdentityFrontier(frontier);
  return Status::OK();
}

Status BlobReferenceCensus::ApplyWorkerDelta(
    uint64_t token, const BlobReferenceCensusDelta& delta) {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker == nullptr || worker->token != token) {
    return Status::Aborted("Census worker was replaced");
  }
  {
    std::lock_guard<std::mutex> log_lock(mutex_);
    if (token != build_cursor_.token ||
        token != worker_active_token_.load(std::memory_order_acquire)) {
      return Status::Aborted("Census worker was replaced");
    }
    if (snapshot_pending_) {
      return Status::TryAgain("Blob GC is copying its selected memberships");
    }
    worker_ready_version_.store(0, std::memory_order_release);
  }
  if (worker->version_number != delta.base_version_number ||
      delta.version_number <= delta.base_version_number) {
    return Status::Corruption("Noncontiguous census worker delta");
  }
  size_t additional_memberships = 0;
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
       delta.added_files) {
    if (file == nullptr ||
        file->groups.size() >
            std::numeric_limits<size_t>::max() - additional_memberships) {
      return Status::Corruption("Invalid census worker delta summary");
    }
    additional_memberships += file->groups.size();
  }
  Status status =
      worker->Reserve(additional_memberships, delta.physical_files.size());
  if (!status.ok()) {
    return status;
  }
  // Add new physical files before applying memberships. Retired candidates
  // are removed only after transferring affected routes to their new targets.
  for (const BlobGcPhysicalFileSummary& file : delta.physical_files) {
    if (file.file_number == 0) {
      return Status::Corruption("Invalid blob GC physical file delta");
    }
    if (file.total_record_bytes != 0) {
      worker->candidates.SetPhysicalFile(file);
    }
  }
  worker->candidates.SetCoarseIdentityFrontier(delta.coarse_identity_frontier);
  // Removed and added SSTs are one net transition. No ready epoch is published
  // between them, including when a compaction has no output SST at all.
  for (uint64_t file_number : delta.removed_files) {
    status = worker->RemoveFile(file_number);
    if (!status.ok()) {
      return status;
    }
  }
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
       delta.added_files) {
    status = worker->AddFile(file);
    if (!status.ok()) {
      return status;
    }
  }
  worker->forward_state = delta.forward_state;
  for (const BlobRelocationRouteSource& source : delta.changed_routes) {
    const std::map<BlobRelocationRouteSource, WorkerRoute>::iterator route =
        worker->routes.find(source);
    if (route == worker->routes.end()) {
      continue;
    }
    const uint64_t target = worker->ResolvePhysical(source);
    if (target == route->second.physical_file_number) {
      continue;
    }
    status = worker->AdjustRouteBytes(route->second, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
    route->second.physical_file_number = target;
    status = worker->AdjustRouteBytes(route->second, /*add=*/true);
    if (!status.ok()) {
      return status;
    }
  }
  for (const BlobGcPhysicalFileSummary& file : delta.physical_files) {
    if (file.total_record_bytes == 0) {
      worker->candidates.DeletePhysicalFile(file.file_number);
    }
  }
  worker->version_number = delta.version_number;
  return worker->Reserve(0);
}

uint64_t BlobReferenceCensus::WorkerVersionNumber() const {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> lock(worker_mutex_);
  return worker != nullptr ? worker->version_number : 0;
}

BlobReferenceCensus::PublishResult BlobReferenceCensus::TryPublishWorker(
    uint64_t token, uint64_t applied_version_number,
    uint64_t current_version_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (token == 0 || token != build_cursor_.token || canceled_ ||
      build_cursor_.overflowed) {
    return PublishResult::kStale;
  }
  if (HasPending(build_cursor_) || build_cursor_.outstanding_bytes != 0) {
    return PublishResult::kQueueNotEmpty;
  }
  if (active_install_reservation_.load(std::memory_order_acquire) != 0) {
    // Return this LOW worker instead of repeatedly redispatching an unready
    // census. Completing/canceling the reservation explicitly wakes the CF.
    build_waiting_for_install_ = true;
    return PublishResult::kInstallPending;
  }
  if (applied_version_number != current_version_number) {
    return PublishResult::kStale;
  }
  worker_ready_version_.store(current_version_number,
                              std::memory_order_release);
  return PublishResult::kPublished;
}

std::shared_ptr<BlobReferenceCensusBuildContext>
BlobReferenceCensus::GetBuildContext() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return build_context_;
}

bool BlobReferenceCensus::TrySetBuildContext(
    std::shared_ptr<BlobReferenceCensusBuildContext> context) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (context == nullptr || context->token == 0 ||
        context->token != build_cursor_.token || canceled_ ||
        build_context_ != nullptr) {
      return false;
    }
    build_context_ = std::move(context);
  }
  return true;
}

Status BlobReferenceCensus::SnapshotSelected(
    uint64_t token, uint64_t version_number,
    const std::set<BlobRelocationRouteSource>& sources,
    std::shared_ptr<const BlobReferenceCensusState>* state) {
  if (state == nullptr) {
    return Status::InvalidArgument("Null selected census snapshot");
  }
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  {
    std::lock_guard<std::mutex> log_lock(mutex_);
    if (token == 0 || token != gc_cursor_.token || canceled_ ||
        gc_cursor_.overflowed || !snapshot_pending_) {
      return Status::Aborted("Selected census snapshot is no longer active");
    }
    if (worker == nullptr ||
        worker->token != worker_active_token_.load(std::memory_order_acquire) ||
        worker->version_number != version_number ||
        worker_ready_version_.load(std::memory_order_acquire) !=
            version_number) {
      snapshot_pending_ = false;
      return Status::TryAgain("Selected census snapshot epoch changed");
    }
  }
  Status status = CopyWorkerSnapshot(*worker, version_number, sources, state);
  {
    std::lock_guard<std::mutex> log_lock(mutex_);
    snapshot_pending_ = false;
  }
  return status;
}

Status BlobReferenceCensus::ReadSelectedSnapshot(
    uint64_t version_number, const std::set<BlobRelocationRouteSource>& sources,
    std::shared_ptr<const BlobReferenceCensusState>* state) const {
  if (state == nullptr) {
    return Status::InvalidArgument("Null selected census snapshot");
  }
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  if (worker == nullptr ||
      worker->token != worker_active_token_.load(std::memory_order_acquire) ||
      worker->version_number != version_number ||
      worker_ready_version_.load(std::memory_order_acquire) != version_number) {
    return Status::TryAgain("Selected census snapshot epoch changed");
  }
  return CopyWorkerSnapshot(*worker, version_number, sources, state);
}

std::shared_ptr<const BlobReferenceCensusState>
BlobReferenceCensus::TEST_GetReadyState(uint64_t version_number) const {
  std::shared_ptr<WorkerState> worker = std::atomic_load(&worker_state_);
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  if (worker == nullptr || worker->version_number != version_number ||
      worker_ready_version_.load(std::memory_order_acquire) != version_number) {
    return nullptr;
  }
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> files;
  files.reserve(worker->files.size());
  for (const std::pair<const uint64_t,
                       std::shared_ptr<const BlobReferenceSstSummary>>& file :
       worker->files) {
    files.push_back(file.second);
  }
  std::shared_ptr<const BlobReferenceCensusState> result;
  const Status status = BuildState(version_number, files, &result);
  assert(status.ok());
  status.PermitUncheckedError();
  return result;
}

Status BlobReferenceCensus::CopyWorkerSnapshot(
    const WorkerState& worker, uint64_t version_number,
    const std::set<BlobRelocationRouteSource>& sources,
    std::shared_ptr<const BlobReferenceCensusState>* state) const {
  // Count memberships, not all DB files. The bound includes a worst-case
  // route-map node, SST-map node and vector element for each membership, all
  // shard containers, selected-source nodes, and shared_ptr control blocks.
  constexpr size_t kMemberBytes =
      sizeof(BlobReferenceCensusState::SstFilesByRoute::value_type) +
      sizeof(BlobReferenceCensusState::SstSummaries::value_type) +
      sizeof(uint64_t) + 12 * sizeof(void*);
  constexpr size_t kSourceBytes =
      sizeof(BlobRelocationRouteSource) + 4 * sizeof(void*);
  size_t bytes = sizeof(BlobReferenceCensusState) +
                 128 * (sizeof(BlobReferenceCensusState::SstFilesByRoute) +
                        4 * sizeof(void*));
  if (sources.size() >
      (std::numeric_limits<size_t>::max() - bytes) / kSourceBytes) {
    return Status::MemoryLimit("Selected blob census size overflow");
  }
  bytes += sources.size() * kSourceBytes;
  for (const BlobRelocationRouteSource& source : sources) {
    const std::map<BlobRelocationRouteSource, WorkerRoute>::const_iterator
        route = worker.routes.find(source);
    if (route == worker.routes.end()) {
      continue;
    }
    if (route->second.sst_files.size() >
        (std::numeric_limits<size_t>::max() - bytes) / kMemberBytes) {
      return Status::MemoryLimit("Selected blob census size overflow");
    }
    bytes += route->second.sst_files.size() * kMemberBytes;
  }
  std::shared_ptr<CacheReservationManager> reservation;
  if (worker.block_cache != nullptr) {
    reservation = std::make_shared<CacheReservationManagerImpl<
        CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
        worker.block_cache);
    const Status status = reservation->UpdateCacheReservation(bytes);
    if (!status.ok()) {
      return status;
    }
  }
  std::shared_ptr<BlobReferenceCensusState> result =
      std::make_shared<BlobReferenceCensusState>(version_number);
  result->reservation_manager_ = std::move(reservation);
  result->snapshot_memory_bound_ = bytes;
  result->selected_sources_ =
      std::make_shared<const std::set<BlobRelocationRouteSource>>(sources);
  for (const BlobRelocationRouteSource& source : sources) {
    const std::map<BlobRelocationRouteSource, WorkerRoute>::const_iterator
        route = worker.routes.find(source);
    if (route == worker.routes.end()) {
      continue;
    }
    std::shared_ptr<BlobReferenceCensusState::SstFilesByRoute>& shard =
        result->route_shards_[BlobReferenceCensusState::RouteShard(source)];
    if (shard == nullptr) {
      shard = std::make_shared<BlobReferenceCensusState::SstFilesByRoute>();
    }
    (*shard)[source].assign(route->second.sst_files.begin(),
                            route->second.sst_files.end());
    ++result->route_source_count_;
    for (uint64_t file_number : route->second.sst_files) {
      std::shared_ptr<BlobReferenceCensusState::SstSummaries>& sst_shard =
          result->sst_shards_[BlobReferenceCensusState::SstShard(file_number)];
      if (sst_shard == nullptr) {
        sst_shard = std::make_shared<BlobReferenceCensusState::SstSummaries>();
      }
      if (sst_shard->emplace(file_number, worker.files.at(file_number))
              .second) {
        ++result->sst_summary_count_;
      }
    }
  }
  result->snapshot_memory_bound_ = SnapshotMemoryBound(*result);
  *state = std::move(result);
  return Status::OK();
}

size_t BlobReferenceCensus::SnapshotMemoryBound(
    const BlobReferenceCensusState& state) {
  size_t bytes =
      sizeof(state) + 128 * (sizeof(BlobReferenceCensusState::SstFilesByRoute) +
                             4 * sizeof(void*));
  const auto add = [&bytes](size_t count, size_t item_bytes) {
    if (count > (std::numeric_limits<size_t>::max() - bytes) / item_bytes) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * item_bytes;
    }
  };
  if (state.selected_sources_ != nullptr) {
    add(state.selected_sources_->size(),
        sizeof(BlobRelocationRouteSource) + 4 * sizeof(void*));
  }
  for (const std::shared_ptr<BlobReferenceCensusState::SstFilesByRoute>& shard :
       state.route_shards_) {
    if (shard == nullptr) {
      continue;
    }
    add(shard->size(),
        sizeof(BlobReferenceCensusState::SstFilesByRoute::value_type) +
            4 * sizeof(void*));
    for (const BlobReferenceCensusState::SstFilesByRoute::value_type& route :
         *shard) {
      add(route.second.capacity(), sizeof(uint64_t));
    }
  }
  for (const std::shared_ptr<BlobReferenceCensusState::SstSummaries>& shard :
       state.sst_shards_) {
    if (shard != nullptr) {
      add(shard->size(),
          sizeof(BlobReferenceCensusState::SstSummaries::value_type) +
              4 * sizeof(void*));
    }
  }
  return bytes;
}

const std::vector<uint64_t>* BlobReferenceCensusState::FindSstFiles(
    const BlobRelocationRouteSource& source) const {
  const auto& shard = route_shards_[RouteShard(source)];
  if (shard == nullptr) {
    return nullptr;
  }
  const auto iterator = shard->find(source);
  return iterator == shard->end() ? nullptr : &iterator->second;
}

std::shared_ptr<const BlobReferenceSstSummary>
BlobReferenceCensusState::FindSstSummary(uint64_t file_number) const {
  const auto& shard = sst_shards_[SstShard(file_number)];
  if (shard == nullptr) {
    return nullptr;
  }
  const auto iterator = shard->find(file_number);
  return iterator == shard->end() ? nullptr : iterator->second;
}

size_t BlobReferenceCensusState::GetSstCount(
    const BlobRelocationRouteSource& source) const {
  const auto* files = FindSstFiles(source);
  return files == nullptr ? 0 : files->size();
}

size_t BlobReferenceCensusState::RouteShard(
    const BlobRelocationRouteSource& source) {
  const uint64_t mixed =
      source.generation_file_number ^
      (source.origin_file_number + uint64_t{0x9e3779b97f4a7c15} +
       (source.generation_file_number << 6) +
       (source.generation_file_number >> 2));
  return mixed % kShardCount;
}

size_t BlobReferenceCensusState::SstShard(uint64_t file_number) {
  return file_number % kShardCount;
}

size_t BlobReferenceCensusDelta::QueueWeight() const {
  size_t bytes = sizeof(*this) + sizeof(BlobReferenceChangeLogEntry);
  const auto add_bytes = [&bytes](size_t count, size_t element_size) {
    if (count == 0 || element_size == 0) {
      return;
    }
    if (count > (std::numeric_limits<size_t>::max() - bytes) / element_size) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * element_size;
    }
  };
  add_bytes(removed_files.capacity(), sizeof(uint64_t));
  add_bytes(added_files.capacity(),
            sizeof(std::shared_ptr<const BlobReferenceSstSummary>));
  add_bytes(changed_routes.capacity(), sizeof(BlobRelocationRouteSource));
  add_bytes(changed_physical_files.capacity(), sizeof(uint64_t));
  add_bytes(physical_files.capacity(), sizeof(BlobGcPhysicalFileSummary));
  add_bytes(1, retained_forward_state_bytes);
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
       added_files) {
    if (file != nullptr) {
      add_bytes(1, file->MemoryUsage());
    }
  }
  return bytes;
}

BlobReferenceChangeLogEntry::~BlobReferenceChangeLogEntry() {
  // An acknowledged prefix may contain many tiny edits. Release a uniquely
  // owned chain iteratively, not with recursive shared_ptr destruction.
  std::shared_ptr<BlobReferenceChangeLogEntry> pending = std::move(next);
  while (pending != nullptr && pending.use_count() == 1) {
    std::shared_ptr<BlobReferenceChangeLogEntry> following =
        std::move(pending->next);
    pending.reset();
    pending = std::move(following);
  }
}

bool BlobReferenceCensus::IsCensusGroup(const BlobReferenceGroup& group) {
  return group.encoding_class == BlobReferenceEncodingClass::kFeatureIdentity ||
         group.encoding_class == BlobReferenceEncodingClass::kRelocated;
}

Status BlobReferenceCensus::AddFile(
    const std::shared_ptr<const BlobReferenceSstSummary>& file,
    BlobReferenceCensusState* state) {
  if (file == nullptr || state == nullptr || file->file_number == 0) {
    return Status::Corruption("Invalid blob reference SST summary");
  }
  if (state->selected_sources_ != nullptr &&
      std::none_of(file->groups.begin(), file->groups.end(),
                   [state](const BlobReferenceGroup& group) {
                     return IsCensusGroup(group) &&
                            state->selected_sources_->count(
                                {group.generation_file_number,
                                 group.origin_file_number}) != 0;
                   })) {
    return Status::OK();
  }
  auto& sst_shard =
      state->sst_shards_[BlobReferenceCensusState::SstShard(file->file_number)];
  if (sst_shard == nullptr) {
    sst_shard = std::make_shared<BlobReferenceCensusState::SstSummaries>();
  } else if (sst_shard.use_count() != 1) {
    sst_shard =
        std::make_shared<BlobReferenceCensusState::SstSummaries>(*sst_shard);
  }
  if (!sst_shard->emplace(file->file_number, file).second) {
    return Status::Corruption("Duplicate SST in blob reference census");
  }
  ++state->sst_summary_count_;

  for (const BlobReferenceGroup& group : file->groups) {
    if (!IsCensusGroup(group)) {
      continue;
    }
    const BlobRelocationRouteSource source{group.generation_file_number,
                                           group.origin_file_number};
    if (state->selected_sources_ != nullptr &&
        state->selected_sources_->count(source) == 0) {
      continue;
    }
    auto& route_shard =
        state->route_shards_[BlobReferenceCensusState::RouteShard(source)];
    if (route_shard == nullptr) {
      route_shard =
          std::make_shared<BlobReferenceCensusState::SstFilesByRoute>();
    } else if (route_shard.use_count() != 1) {
      route_shard = std::make_shared<BlobReferenceCensusState::SstFilesByRoute>(
          *route_shard);
    }
    auto insertion = route_shard->try_emplace(source);
    if (insertion.second) {
      ++state->route_source_count_;
    }
    auto& files = insertion.first->second;
    const auto position =
        std::lower_bound(files.begin(), files.end(), file->file_number);
    if (position != files.end() && *position == file->file_number) {
      return Status::Corruption("Duplicate SST route membership");
    }
    files.insert(position, file->file_number);
  }
  return Status::OK();
}

Status BlobReferenceCensus::RemoveFile(uint64_t file_number,
                                       BlobReferenceCensusState* state) {
  if (state == nullptr || file_number == 0) {
    return Status::Corruption("Invalid blob reference SST deletion");
  }
  const std::shared_ptr<const BlobReferenceSstSummary> summary =
      state->FindSstSummary(file_number);
  if (summary == nullptr) {
    // Legacy and complete-empty SSTs do not contribute route membership.
    return Status::OK();
  }
  for (const BlobReferenceGroup& group : summary->groups) {
    if (!IsCensusGroup(group)) {
      continue;
    }
    const BlobRelocationRouteSource source{group.generation_file_number,
                                           group.origin_file_number};
    if (state->selected_sources_ != nullptr &&
        state->selected_sources_->count(source) == 0) {
      continue;
    }
    auto& route_shard =
        state->route_shards_[BlobReferenceCensusState::RouteShard(source)];
    if (route_shard == nullptr) {
      return Status::Corruption("Missing blob reference route membership");
    }
    if (route_shard.use_count() != 1) {
      route_shard = std::make_shared<BlobReferenceCensusState::SstFilesByRoute>(
          *route_shard);
    }
    const auto route = route_shard->find(source);
    if (route == route_shard->end()) {
      return Status::Corruption("Missing blob reference route membership");
    }
    auto& files = route->second;
    const auto position =
        std::lower_bound(files.begin(), files.end(), file_number);
    if (position == files.end() || *position != file_number) {
      return Status::Corruption("Missing SST route membership");
    }
    files.erase(position);
    if (files.empty()) {
      route_shard->erase(route);
      assert(state->route_source_count_ > 0);
      --state->route_source_count_;
    }
  }
  auto& sst_shard =
      state->sst_shards_[BlobReferenceCensusState::SstShard(file_number)];
  assert(sst_shard != nullptr);
  if (sst_shard.use_count() != 1) {
    sst_shard =
        std::make_shared<BlobReferenceCensusState::SstSummaries>(*sst_shard);
  }
  if (sst_shard->erase(file_number) != 1) {
    return Status::Corruption("Missing blob reference SST summary");
  }
  assert(state->sst_summary_count_ > 0);
  --state->sst_summary_count_;
  return Status::OK();
}

Status BlobReferenceCensus::BuildState(
    uint64_t version_number,
    const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& files,
    std::shared_ptr<const BlobReferenceCensusState>* state) {
  if (state == nullptr) {
    return Status::InvalidArgument("Null blob reference census output");
  }
  auto result = std::make_shared<BlobReferenceCensusState>(version_number);
  for (const auto& file : files) {
    Status status = AddFile(file, result.get());
    if (!status.ok()) {
      return status;
    }
  }
  *state = std::move(result);
  return Status::OK();
}

Status BlobReferenceCensus::ApplyDelta(
    const BlobReferenceCensusState& base, const BlobReferenceCensusDelta& delta,
    std::shared_ptr<const BlobReferenceCensusState>* state) {
  if (state == nullptr || delta.base_version_number != base.version_number() ||
      delta.version_number <= delta.base_version_number) {
    return Status::Corruption("Invalid blob reference census epoch delta");
  }
  if (!std::is_sorted(delta.removed_files.begin(), delta.removed_files.end()) ||
      std::adjacent_find(delta.removed_files.begin(),
                         delta.removed_files.end()) !=
          delta.removed_files.end()) {
    return Status::Corruption(
        "Blob reference census deletions are not sorted and unique");
  }
  size_t next_bound = base.snapshot_memory_bound_;
  if (base.reservation_manager_ != nullptr) {
    constexpr size_t kAddedGroupBytes =
        sizeof(BlobReferenceCensusState::SstFilesByRoute::value_type) +
        sizeof(BlobReferenceCensusState::SstSummaries::value_type) +
        2 * sizeof(uint64_t) + 12 * sizeof(void*);
    for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
         delta.added_files) {
      if (file == nullptr ||
          file->groups.size() >=
              (std::numeric_limits<size_t>::max() - next_bound) /
                  kAddedGroupBytes) {
        return Status::MemoryLimit("Selected blob census delta size overflow");
      }
      size_t selected_groups = 0;
      for (const BlobReferenceGroup& group : file->groups) {
        if (IsCensusGroup(group) &&
            (base.selected_sources_ == nullptr ||
             base.selected_sources_->count({group.generation_file_number,
                                            group.origin_file_number}) != 0)) {
          ++selected_groups;
        }
      }
      if (selected_groups != 0) {
        next_bound += (selected_groups + 1) * kAddedGroupBytes;
      }
    }
    const size_t added_bound = next_bound - base.snapshot_memory_bound_;
    if (base.snapshot_memory_bound_ > std::numeric_limits<size_t>::max() / 4 ||
        added_bound > (std::numeric_limits<size_t>::max() -
                       4 * base.snapshot_memory_bound_) /
                          2) {
      return Status::MemoryLimit("Selected blob census COW size overflow");
    }
    // Covers the old snapshot, cloned shard/vector storage and geometric
    // vector reallocation. Recompute retained capacity after the transition;
    // charging every historical addition forever would eventually park an
    // otherwise constant-size selected census under a strict cache budget.
    const size_t peak = 4 * base.snapshot_memory_bound_ + 2 * added_bound;
    if (peak > base.reservation_manager_->GetTotalMemoryUsed()) {
      const Status status =
          base.reservation_manager_->UpdateCacheReservation(peak);
      if (!status.ok()) {
        return status;
      }
    }
  }
  auto result = std::make_shared<BlobReferenceCensusState>(base);
  result->version_number_ = delta.version_number;
  for (uint64_t file_number : delta.removed_files) {
    Status status = RemoveFile(file_number, result.get());
    if (!status.ok()) {
      return status;
    }
  }
  for (const auto& file : delta.added_files) {
    Status status = AddFile(file, result.get());
    if (!status.ok()) {
      return status;
    }
  }
  result->snapshot_memory_bound_ = SnapshotMemoryBound(*result);
  *state = std::move(result);
  return Status::OK();
}

Status BlobReferenceCensus::PrepareInstall(
    std::shared_ptr<const BlobReferenceCensusDelta> delta,
    uint64_t reservation_id,
    BlobReferenceCensusPreparedInstall* prepared) const {
  return PrepareInstallWithCache(std::move(delta), reservation_id,
                                 /*block_cache=*/nullptr, prepared);
}

Status BlobReferenceCensus::PrepareInstallWithCache(
    std::shared_ptr<const BlobReferenceCensusDelta> delta,
    uint64_t reservation_id, std::shared_ptr<Cache> block_cache,
    BlobReferenceCensusPreparedInstall* prepared) const {
  if (delta == nullptr || prepared == nullptr) {
    return Status::InvalidArgument("Null blob reference census install");
  }
  if (!std::is_sorted(delta->removed_files.begin(),
                      delta->removed_files.end()) ||
      std::adjacent_find(delta->removed_files.begin(),
                         delta->removed_files.end()) !=
          delta->removed_files.end()) {
    return Status::Corruption(
        "Blob reference census deletions are not sorted and unique");
  }
  std::unique_ptr<CacheReservationManager::CacheReservationHandle> charge;
  if (block_cache != nullptr) {
    std::shared_ptr<CacheReservationManager> manager;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (log_block_cache_ == block_cache) {
        manager = log_reservation_manager_;
      }
    }
    if (manager == nullptr) {
      manager = std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
              block_cache));
      // The MANIFEST queue serializes preparations for this CF. Construct
      // cache objects unlocked; swap only handles under the small log mutex.
      std::shared_ptr<CacheReservationManager> previous_manager;
      std::shared_ptr<Cache> previous_cache;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        previous_manager = std::move(log_reservation_manager_);
        previous_cache = std::move(log_block_cache_);
        log_reservation_manager_ = manager;
        log_block_cache_ = std::move(block_cache);
      }
    }
    size_t bytes = delta->QueueWeight();
    if (bytes == std::numeric_limits<size_t>::max()) {
      return Status::MemoryLimit("Blob change log retention size overflow");
    }
    for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
         delta->added_files) {
      if (file != nullptr && file->reservation_manager != nullptr) {
        // Builder-owned immutable summaries are already charged once.
        assert(bytes >= file->MemoryUsage());
        bytes -= file->MemoryUsage();
      }
    }
    const Status status = manager->MakeCacheReservation(bytes, &charge);
    if (!status.ok()) {
      return status;
    }
  }
  prepared->delta = std::move(delta);
  prepared->log_entry = std::make_shared<BlobReferenceChangeLogEntry>(
      prepared->delta, std::move(charge));
  prepared->invalidate_census = false;
  prepared->reservation_id = reservation_id;
  prepared->delta_queue_weight = prepared->delta->QueueWeight();

  // File metadata is validated/prepared by the builder. Reverse memberships
  // are updated by the permanent background cursor, not copied at queue head.
  return Status::OK();
}

Status BlobReferenceCensus::PrepareInvalidation(
    uint64_t reservation_id,
    BlobReferenceCensusPreparedInstall* prepared) const {
  if (reservation_id == 0 || prepared == nullptr) {
    return Status::InvalidArgument(
        "Invalid blob reference census invalidation");
  }
  prepared->delta.reset();
  prepared->log_entry.reset();
  prepared->invalidate_census = true;
  prepared->reservation_id = reservation_id;
  prepared->delta_queue_weight = 0;
  return Status::OK();
}

uint64_t BlobReferenceCensus::BeginInstallPreparation() {
  uint64_t reservation_id = next_reservation_id_++;
  if (reservation_id == 0) {
    reservation_id = next_reservation_id_++;
  }
  assert(active_install_reservation_.load(std::memory_order_relaxed) == 0);
  active_install_reservation_.store(reservation_id, std::memory_order_release);
  return reservation_id;
}

void BlobReferenceCensus::CancelInstallPreparation(uint64_t reservation_id) {
  if (reservation_id == 0) {
    return;
  }
  bool wake_build = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t expected = reservation_id;
    if (active_install_reservation_.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      wake_build = std::exchange(build_waiting_for_install_, false);
    }
  }
  if (wake_build && build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
  build_cv_.notify_all();
}

bool BlobReferenceCensus::HasPending(const ChangeLogCursor& cursor) {
  return cursor.delivered != nullptr ? cursor.delivered->next != nullptr
                                     : cursor.first != nullptr;
}

void BlobReferenceCensus::DetachCursor(ChangeLogCursor* cursor,
                                       BlobReferenceCensusGarbage* garbage,
                                       size_t first_slot) {
  if (cursor->first != nullptr) {
    assert(garbage->log_entries[first_slot] == nullptr);
    garbage->log_entries[first_slot] = std::move(cursor->first);
  }
  if (cursor->acknowledged != nullptr) {
    assert(garbage->log_entries[first_slot + 1] == nullptr);
    garbage->log_entries[first_slot + 1] = std::move(cursor->acknowledged);
  }
  if (cursor->delivered != nullptr) {
    assert(garbage->log_entries[first_slot + 2] == nullptr);
    garbage->log_entries[first_slot + 2] = std::move(cursor->delivered);
  }
  cursor->outstanding_bytes = 0;
}

void BlobReferenceCensus::ResetCursorAfterDrain(ChangeLogCursor* cursor) {
  assert(cursor->outstanding_bytes == 0 && !HasPending(*cursor));
  // Acknowledgement releases every handle when the outstanding bytes reach
  // zero, outside the DB mutex. Closing a drained cursor frees no log prefix.
  assert(cursor->first == nullptr && cursor->acknowledged == nullptr &&
         cursor->delivered == nullptr);
  *cursor = {};
}

void BlobReferenceCensus::AppendChange(
    const BlobReferenceCensusPreparedInstall& prepared,
    BlobReferenceCensusGarbage* garbage) {
  const size_t bytes = prepared.delta_queue_weight;
  bool retain = false;
  std::array<ChangeLogCursor*, 2> cursors{{&build_cursor_, &gc_cursor_}};
  for (size_t index = 0; index < cursors.size(); ++index) {
    ChangeLogCursor& cursor = *cursors[index];
    if (cursor.token == 0 || cursor.overflowed) {
      continue;
    }
    // One valid large delta is accepted into an empty cursor. It must not
    // permanently prevent a large but otherwise supported compaction.
    if (cursor.outstanding_bytes != 0 &&
        bytes > kMaxQueuedWeight -
                    std::min(cursor.outstanding_bytes, kMaxQueuedWeight)) {
      cursor.overflowed = true;
      DetachCursor(&cursor, garbage, 3 * index);
      continue;
    }
    retain = true;
    cursor.outstanding_bytes += bytes;
    if (cursor.delivered == nullptr && cursor.first == nullptr) {
      cursor.first = prepared.log_entry;
    }
  }
  if (!retain) {
    garbage->log_entries[6] = std::move(log_tail_);
    return;
  }
  assert(prepared.log_entry != nullptr && prepared.log_entry->next == nullptr);
  std::shared_ptr<BlobReferenceChangeLogEntry> previous = std::move(log_tail_);
  if (previous != nullptr) {
    assert(previous->next == nullptr);
    previous->next = prepared.log_entry;
    // A detached reader might concurrently release the only other owner.
    // Never let the temporary tail pin release a large prefix under DB mutex.
    garbage->log_entries[6] = std::move(previous);
  }
  log_tail_ = prepared.log_entry;
}

BlobReferenceCensusGarbage BlobReferenceCensus::Install(
    BlobReferenceCensusPreparedInstall& prepared) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto finish_install = [&]() {
    const bool wake_build = std::exchange(build_waiting_for_install_, false);
    lock.unlock();
    // The caller still owns DB mutex. The callback takes only the short log
    // mutex while checking readiness, so it must run after this unlock.
    if (wake_build && build_wakeup_ != nullptr) {
      build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
    }
    build_cv_.notify_all();
  };
  BlobReferenceCensusGarbage garbage;
  if ((!prepared.invalidate_census && prepared.delta == nullptr) || canceled_ ||
      prepared.reservation_id == 0 ||
      active_install_reservation_.load(std::memory_order_acquire) !=
          prepared.reservation_id) {
    worker_ready_version_.store(0, std::memory_order_release);
    garbage.build_context = std::move(build_context_);
    DetachCursor(&build_cursor_, &garbage, 0);
    DetachCursor(&gc_cursor_, &garbage, 3);
    garbage.worker_state =
        std::atomic_exchange(&worker_state_, std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    build_cursor_.token = 0;
    gc_cursor_.token = 0;
    garbage.log_entries[6] = std::move(log_tail_);
    active_install_reservation_.store(0, std::memory_order_release);
    finish_install();
    return garbage;
  }
  active_install_reservation_.store(0, std::memory_order_release);
  if (prepared.invalidate_census) {
    worker_ready_version_.store(0, std::memory_order_release);
    // A later retry must capture a new Version/token, not reuse this canceled
    // context. The active worker keeps its own pin until its slice returns.
    garbage.build_context = std::move(build_context_);
    garbage.worker_state =
        std::atomic_exchange(&worker_state_, std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    DetachCursor(&build_cursor_, &garbage, 0);
    build_cursor_.token = 0;
    if (gc_cursor_.token != 0) {
      DetachCursor(&gc_cursor_, &garbage, 3);
      gc_cursor_.overflowed = true;
    }
    garbage.log_entries[6] = std::move(log_tail_);
    finish_install();
    return garbage;
  }

  AppendChange(prepared, &garbage);
  finish_install();
  return garbage;
}

uint64_t BlobReferenceCensus::BeginBuild() {
  std::lock_guard<std::mutex> lock(mutex_);
  assert(build_cursor_.token == 0);
  canceled_ = false;
  build_cursor_.token = next_build_token_++;
  if (build_cursor_.token == 0) {
    build_cursor_.token = next_build_token_++;
  }
  build_cursor_.overflowed = false;
  return build_cursor_.token;
}

Status BlobReferenceCensus::TakeDeltas(uint64_t token, ChangeLogCursor* cursor,
                                       BlobReferenceCensusDeltaQueue* deltas,
                                       bool* overflowed) {
  if (deltas == nullptr || overflowed == nullptr || !deltas->empty()) {
    return Status::InvalidArgument("Invalid blob change log output");
  }
  std::array<std::shared_ptr<BlobReferenceChangeLogEntry>, kMaxDeltasPerRead>
      entries;
  size_t count = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (token == 0 || token != cursor->token || canceled_) {
      return Status::Aborted("Blob change log cursor is no longer active");
    }
    *overflowed = cursor->overflowed;
    if (cursor->overflowed) {
      return Status::OK();
    }
    std::shared_ptr<BlobReferenceChangeLogEntry> entry =
        cursor->delivered != nullptr ? cursor->delivered->next : cursor->first;
    while (entry != nullptr && count < entries.size()) {
      entries[count++] = entry;
      entry = entry->next;
    }
    if (count != 0) {
      cursor->delivered = entries[count - 1];
    }
  }
  // List allocation and any old output destruction happen outside the log
  // mutex. The entry handles above protect the immutable deltas while copying.
  for (size_t index = 0; index < count; ++index) {
    assert(entries[index]->delta != nullptr);
    deltas->emplace_back(entries[index]->delta);
  }
  return Status::OK();
}

Status BlobReferenceCensus::TakeBuildDeltas(
    uint64_t token, BlobReferenceCensusDeltaQueue* deltas, bool* overflowed) {
  return TakeDeltas(token, &build_cursor_, deltas, overflowed);
}

Status BlobReferenceCensus::TakeGcDeltas(uint64_t token,
                                         BlobReferenceCensusDeltaQueue* deltas,
                                         bool* overflowed) {
  return TakeDeltas(token, &gc_cursor_, deltas, overflowed);
}

Status BlobReferenceCensus::AcknowledgeDeltas(uint64_t token,
                                              ChangeLogCursor* cursor,
                                              size_t retained_bytes) {
  std::shared_ptr<BlobReferenceChangeLogEntry> old_acknowledged;
  std::shared_ptr<BlobReferenceChangeLogEntry> old_first;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_acknowledged;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_delivered;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_tail;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (token == 0 || token != cursor->token || canceled_ ||
        cursor->overflowed) {
      return Status::Aborted("Blob change log cursor is no longer active");
    }
    if (retained_bytes > cursor->outstanding_bytes) {
      return Status::Corruption("Blob change log acknowledged excess bytes");
    }
    if (retained_bytes == 0) {
      return Status::OK();
    }
    if (cursor->delivered == nullptr) {
      return Status::Corruption(
          "Blob change log acknowledged undelivered bytes");
    }
    std::shared_ptr<BlobReferenceChangeLogEntry> entry =
        cursor->acknowledged != nullptr ? cursor->acknowledged->next
                                        : cursor->first;
    std::shared_ptr<BlobReferenceChangeLogEntry> acknowledged;
    size_t remaining = retained_bytes;
    for (size_t count = 0; count < kMaxDeltasPerRead && entry != nullptr;
         ++count) {
      if (entry->retained_bytes > remaining) {
        break;
      }
      remaining -= entry->retained_bytes;
      acknowledged = entry;
      if (remaining == 0 || entry == cursor->delivered) {
        break;
      }
      entry = entry->next;
    }
    if (remaining != 0 || acknowledged == nullptr) {
      return Status::Corruption(
          "Blob change log acknowledged undelivered bytes");
    }
    old_acknowledged = std::move(cursor->acknowledged);
    old_first = std::move(cursor->first);
    cursor->acknowledged = std::move(acknowledged);
    cursor->outstanding_bytes -= retained_bytes;
    if (cursor->outstanding_bytes == 0) {
      drained_acknowledged = std::move(cursor->acknowledged);
      drained_delivered = std::move(cursor->delivered);
    }
    if (build_cursor_.outstanding_bytes == 0 &&
        gc_cursor_.outstanding_bytes == 0) {
      drained_tail = std::move(log_tail_);
    }
  }
  return Status::OK();
}

Status BlobReferenceCensus::AcknowledgeBuildDeltas(uint64_t token,
                                                   size_t retained_bytes) {
  return AcknowledgeDeltas(token, &build_cursor_, retained_bytes);
}

Status BlobReferenceCensus::AcknowledgeGcDeltas(uint64_t token,
                                                size_t retained_bytes) {
  return AcknowledgeDeltas(token, &gc_cursor_, retained_bytes);
}

Status BlobReferenceCensus::WaitForGcChange(uint64_t token) {
  std::unique_lock<std::mutex> lock(mutex_);
  build_cv_.wait(lock, [&] {
    return token == 0 || token != gc_cursor_.token || canceled_ ||
           gc_cursor_.overflowed || HasPending(gc_cursor_) ||
           active_install_reservation_.load(std::memory_order_acquire) == 0;
  });
  if (token == 0 || token != gc_cursor_.token || canceled_) {
    return Status::Aborted("Blob GC listener is no longer active");
  }
  return Status::OK();
}

BlobReferenceCensusGarbage BlobReferenceCensus::FailBuild(uint64_t token) {
  std::lock_guard<std::mutex> lock(mutex_);
  BlobReferenceCensusGarbage garbage;
  if (token != 0 && token == build_cursor_.token) {
    worker_ready_version_.store(0, std::memory_order_release);
    garbage.worker_state =
        std::atomic_exchange(&worker_state_, std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    garbage.build_context = std::move(build_context_);
    DetachCursor(&build_cursor_, &garbage, 0);
    build_cursor_.token = 0;
    build_cursor_.overflowed = false;
    if (gc_cursor_.token == 0 || gc_cursor_.overflowed) {
      garbage.log_entries[6] = std::move(log_tail_);
    }
  }
  build_cv_.notify_all();
  return garbage;
}

BlobReferenceCensusGarbage BlobReferenceCensus::Cancel() {
  std::lock_guard<std::mutex> lock(mutex_);
  {
    std::lock_guard<std::mutex> publications_lock(publications_mutex_);
    publications_canceled_ = true;
    publications_cv_.notify_all();
  }
  canceled_ = true;
  build_waiting_for_install_ = false;
  build_wakeup_ = nullptr;
  build_wakeup_db_ = nullptr;
  build_wakeup_cfd_ = nullptr;
  snapshot_pending_ = false;
  worker_ready_version_.store(0, std::memory_order_release);
  BlobReferenceCensusGarbage garbage;
  garbage.worker_state =
      std::atomic_exchange(&worker_state_, std::shared_ptr<WorkerState>());
  worker_active_token_.store(0, std::memory_order_release);
  garbage.build_context = std::move(build_context_);
  DetachCursor(&build_cursor_, &garbage, 0);
  DetachCursor(&gc_cursor_, &garbage, 3);
  build_cursor_.token = 0;
  gc_cursor_.token = 0;
  garbage.log_entries[6] = std::move(log_tail_);
  active_install_reservation_.store(0, std::memory_order_release);
  build_cv_.notify_all();
  return garbage;
}

bool BlobReferenceCensus::IsReady(uint64_t version_number) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return version_number != 0 &&
         worker_ready_version_.load(std::memory_order_acquire) ==
             version_number;
}

bool BlobReferenceCensus::IsSnapshotPending() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshot_pending_;
}

Status BlobReferenceCensus::BeginGcListener(uint64_t version_number,
                                            uint64_t* token) {
  if (token == nullptr || version_number == 0) {
    return Status::InvalidArgument("Invalid blob GC listener start");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (canceled_) {
    return Status::Aborted("Blob reference census is canceled");
  }
  const bool worker_ready =
      worker_ready_version_.load(std::memory_order_acquire) == version_number;
  if (!worker_ready) {
    return Status::TryAgain("Blob reference census is not ready");
  }
  if (gc_cursor_.token != 0) {
    return Status::Busy("Blob GC listener is already active");
  }
  gc_cursor_.token = next_gc_token_++;
  if (gc_cursor_.token == 0) {
    gc_cursor_.token = next_gc_token_++;
  }
  gc_cursor_.overflowed = false;
  snapshot_pending_ = worker_ready;
  *token = gc_cursor_.token;
  return Status::OK();
}

BlobReferenceCensus::GcListenerCloseResult
BlobReferenceCensus::TryCloseGcListener(uint64_t token,
                                        uint64_t applied_version_number,
                                        uint64_t current_version_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (token == 0 || token != gc_cursor_.token || canceled_ ||
      gc_cursor_.overflowed) {
    return GcListenerCloseResult::kStale;
  }
  if (HasPending(gc_cursor_) || gc_cursor_.outstanding_bytes != 0) {
    return GcListenerCloseResult::kQueueNotEmpty;
  }
  if (active_install_reservation_.load(std::memory_order_acquire) != 0) {
    return GcListenerCloseResult::kInstallPending;
  }
  if (applied_version_number != current_version_number) {
    return GcListenerCloseResult::kStale;
  }
  ResetCursorAfterDrain(&gc_cursor_);
  snapshot_pending_ = false;
  build_cv_.notify_all();
  return GcListenerCloseResult::kClosed;
}

BlobReferenceCensusGarbage BlobReferenceCensus::CancelGcListener(
    uint64_t token) {
  std::lock_guard<std::mutex> lock(mutex_);
  BlobReferenceCensusGarbage garbage;
  if (token != 0 && token == gc_cursor_.token) {
    snapshot_pending_ = false;
    DetachCursor(&gc_cursor_, &garbage, 3);
    gc_cursor_.token = 0;
    gc_cursor_.overflowed = false;
    if (build_cursor_.token == 0 || build_cursor_.overflowed) {
      garbage.log_entries[6] = std::move(log_tail_);
    }
  }
  build_cv_.notify_all();
  return garbage;
}

}  // namespace ROCKSDB_NAMESPACE
