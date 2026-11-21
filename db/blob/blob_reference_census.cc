// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_reference_census.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>

#include "db/blob/blob_gen2_format.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

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
  // Large values: compare the two fractions exactly as continued fractions.
  // Each round compares the integer parts, then takes the reciprocals of the
  // remainders, which reverses the order. Like Euclid's algorithm, this ends in
  // O(log(max denominator)) rounds. It needs no floating point, which can
  // round. It needs no products, which can overflow. It needs no 128-bit
  // integer, which is not portable.
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
  // A file pinned by the lowest coarse frontier of the Version stays out of the
  // heap. The coarse frontier of an SST is the oldest indirect-origin file
  // number a coarse SST may reference (see "Coarse SST" in
  // db/blob/blob_reference_metadata.h). A lost relocation file has no readable
  // values, so indirect blob GC cannot copy from it. It stays out of the heap
  // while references to it remain (live bytes > 0). When none remain, it enters
  // the heap so that the empty-file cleanup can pick it (see SelectEmpty()).
  if (candidate->coarse_pinned ||
      (candidate->is_lost && candidate->live_bytes != 0)) {
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
  return candidate.is_indirect_origin
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
    // The sum would overflow or go below zero. Stop tracking it and report 0
    // garbage bytes until the census is rebuilt. Reporting less garbage is
    // safe: the space estimate then counts these bytes as live.
    relocation_garbage_overflow_ = true;
    relocation_garbage_bytes_ = 0;
    return;
  }
  relocation_garbage_bytes_ = relocation_garbage_bytes_ - before + after;
}

void BlobGcCandidateHeap::AccountTotals(const Candidate& candidate, bool add) {
  const uint64_t garbage =
      candidate.total_bytes -
      std::min(candidate.live_bytes, candidate.total_bytes);
  const auto apply = [add](uint64_t* total, uint64_t value) {
    *total = add ? *total + value : *total - std::min(*total, value);
  };
  if (candidate.suppressed) {
    apply(&totals_.suppressed_garbage_bytes, garbage);
  } else if (candidate.coarse_pinned) {
    apply(&totals_.coarse_pinned_files, 1);
    apply(&totals_.coarse_pinned_bytes, candidate.total_bytes);
    apply(&totals_.coarse_pinned_garbage_bytes, garbage);
  } else {
    apply(&totals_.collectable_garbage_bytes, garbage);
  }
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
      candidate.is_indirect_origin == file.is_indirect_origin &&
      candidate.is_lost == file.is_lost) {
    return;
  }
  const uint64_t previous_garbage = RelocationGarbage(candidate);
  AccountTotals(candidate, /*add=*/false);
  candidate.total_bytes = file.total_record_bytes;
  candidate.is_indirect_origin = file.is_indirect_origin;
  candidate.is_lost = file.is_lost;
  AdjustRelocationGarbage(previous_garbage, RelocationGarbage(candidate));
  candidate.coarse_pinned =
      file.is_indirect_origin && coarse_indirect_origin_frontier_ != 0 &&
      file.file_number >= coarse_indirect_origin_frontier_;
  candidate.failures = 0;
  candidate.revision = next_revision_++;
  if (next_revision_ == 0) {
    ++next_revision_;
  }
  Activate(file.file_number, &candidate);
  AccountTotals(candidate, /*add=*/true);
}

void BlobGcCandidateHeap::SetCoarseIndirectOriginFrontier(uint64_t frontier) {
  if (frontier == coarse_indirect_origin_frontier_) {
    return;
  }
  const uint64_t old_boundary = coarse_indirect_origin_frontier_ == 0
                                    ? std::numeric_limits<uint64_t>::max()
                                    : coarse_indirect_origin_frontier_;
  const uint64_t new_boundary =
      frontier == 0 ? std::numeric_limits<uint64_t>::max() : frontier;
  coarse_indirect_origin_frontier_ = frontier;
  // Visit only the file numbers between the old and the new frontier, whose pin
  // state can change. This runs in the background worker, not on each selection
  // or install.
  for (std::map<uint64_t, Candidate>::iterator entry =
           candidates_.lower_bound(std::min(old_boundary, new_boundary));
       entry != candidates_.end() &&
       entry->first < std::max(old_boundary, new_boundary);
       ++entry) {
    Candidate& candidate = entry->second;
    if (!candidate.is_indirect_origin) {
      continue;
    }
    AccountTotals(candidate, /*add=*/false);
    candidate.coarse_pinned = frontier != 0 && entry->first >= frontier;
    candidate.revision = next_revision_++;
    if (next_revision_ == 0) {
      ++next_revision_;
    }
    Activate(entry->first, &candidate);
    AccountTotals(candidate, /*add=*/true);
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
  AccountTotals(candidate, /*add=*/false);
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
  AccountTotals(candidate, /*add=*/true);
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
  AccountTotals(candidate, /*add=*/false);
  AdjustRelocationGarbage(RelocationGarbage(candidate), 0);
  candidates_.erase(entry);
}

bool BlobGcCandidateHeap::IsIndirectOrigin(uint64_t file_number) const {
  const std::map<uint64_t, Candidate>::const_iterator entry =
      candidates_.find(file_number);
  return entry != candidates_.end() && entry->second.is_indirect_origin;
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
  // Pick the best kMaxSelectedFiles (8) files without sorting or scanning every
  // file. `frontier` holds the heap positions that can be the next best file:
  // start with the root, and after each pick add the children of the picked
  // position. `frontier` is a fixed array, so selection does not allocate. Stop
  // at the first file whose live bytes are more than half of its total bytes
  // (garbage ratio below 50%). For such a file, indirect blob GC would copy
  // more live bytes than the garbage bytes it removes.
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
  AccountTotals(candidate, /*add=*/false);
  candidate.suppressed = true;
  candidate.retry_micros = retry_micros;
  if (retry_micros != 0) {
    retries_.emplace(retry_micros, file_number);
  }
  AccountTotals(candidate, /*add=*/true);
}

BlobGcCandidateHeap::Selection BlobGcCandidateHeap::SelectEmpty() const {
  Selection result = Select();
  // Keep only the leading files with 0 live bytes. They rank first, because
  // their garbage ratio is 100%. This checks only the selected files, not every
  // physical file. Live bytes can be too high (see the class comment), so they
  // can delay a cleanup but never prove that a file is empty. Indirect blob GC
  // checks the exact holders before it acts.
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
  // At most kMaxRetriesPerSlice (32, an estimate) files per call, to bound the
  // work of one call. Files still due wait for the next call.
  constexpr size_t kMaxRetriesPerSlice = 32;
  size_t count = 0;
  while (!retries_.empty() && retries_.begin()->first <= now_micros &&
         count++ < kMaxRetriesPerSlice) {
    const uint64_t file_number = retries_.begin()->second;
    const std::map<uint64_t, Candidate>::iterator entry =
        candidates_.find(file_number);
    assert(entry != candidates_.end());
    AccountTotals(entry->second, /*add=*/false);
    Activate(file_number, &entry->second);
    AccountTotals(entry->second, /*add=*/true);
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
  // Retry delay: 1 s after the first failure, doubled after each failure, at
  // most 32 s (from the sixth failure on). So a short failure is retried soon,
  // and a lasting failure costs at most one attempt every 32 s. These are fixed
  // choices, not measured.
  constexpr uint64_t kFirstFailureRetryMicros = 1000 * 1000;
  constexpr uint8_t kMaxCountedFailures = 6;
  candidate.failures =
      std::min<uint8_t>(candidate.failures + 1, kMaxCountedFailures);
  const uint64_t delay = kFirstFailureRetryMicros << (candidate.failures - 1);
  Suppress(file_number,
           now_micros + std::min(delay, std::numeric_limits<uint64_t>::max() -
                                            now_micros));
}

size_t BlobGcCandidateHeap::MemoryUsage(size_t additional_candidates) const {
  // Bytes per candidate (estimate): a map node with its tree pointers, up to
  // two heap pointers (vector growth) and one retry set node.
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

Status BlobCensusSlotTable::Assign(uint64_t file_number, uint32_t* slot) {
  assert(slot != nullptr);
  if (file_number == 0 || slots_.count(file_number) != 0) {
    return Status::Corruption("Invalid or duplicate census SST slot");
  }
  uint32_t result = static_cast<uint32_t>(files_.size());
  for (size_t word = first_free_word_; word < free_words_.size(); ++word) {
    if (free_words_[word] != 0) {
      const uint32_t bit =
          static_cast<uint32_t>(CountTrailingZeroBits(free_words_[word]));
      result = static_cast<uint32_t>(word * 64 + bit);
      free_words_[word] &= ~(uint64_t{1} << bit);
      first_free_word_ = word;
      break;
    }
    first_free_word_ = word + 1;
  }
  if (result == files_.size()) {
    if (files_.size() == kNoSlot) {
      return Status::MemoryLimit("Too many census SST slots");
    }
    files_.push_back(file_number);
    free_words_.resize((files_.size() + 63) / 64);
  } else {
    files_[result] = file_number;
  }
  slots_.emplace(file_number, result);
  *slot = result;
  return Status::OK();
}

Status BlobCensusSlotTable::Release(uint64_t file_number, uint32_t* slot) {
  assert(slot != nullptr);
  const auto found = slots_.find(file_number);
  if (found == slots_.end()) {
    return Status::Corruption("Missing census SST slot");
  }
  *slot = found->second;
  slots_.erase(found);
  files_[*slot] = 0;
  free_words_[*slot / 64] |= uint64_t{1} << (*slot % 64);
  first_free_word_ = std::min<size_t>(first_free_word_, *slot / 64);
  // Keep U (the slot count) tight: drop the free slots at the end.
  while (!files_.empty() && files_.back() == 0) {
    const size_t last = files_.size() - 1;
    free_words_[last / 64] &= ~(uint64_t{1} << (last % 64));
    files_.pop_back();
  }
  free_words_.resize((files_.size() + 63) / 64);
  first_free_word_ = std::min(first_free_word_, free_words_.size());
  return Status::OK();
}

uint32_t BlobCensusSlotTable::Find(uint64_t file_number) const {
  const auto found = slots_.find(file_number);
  return found == slots_.end() ? kNoSlot : found->second;
}

size_t BlobCensusSlotTable::MemoryUsage() const {
  // An estimate for both hash map types behind UnorderedMap: per entry the key,
  // the value and three pointers (links, metadata and allocator rounding), plus
  // one pointer per bucket.
  return files_.capacity() * sizeof(uint64_t) +
         free_words_.capacity() * sizeof(uint64_t) +
         slots_.bucket_count() * sizeof(void*) +
         slots_.size() *
             (sizeof(uint64_t) + sizeof(uint32_t) + 3 * sizeof(void*));
}

bool BlobCensusSlotSet::Contains(uint32_t slot) const {
  if (bitmap_) {
    return slot / 64 < words_.size() &&
           (words_[slot / 64] >> (slot % 64) & 1) != 0;
  }
  Slice input(list_);
  uint32_t value = 0;
  for (uint32_t i = 0; i < count_; ++i) {
    uint32_t delta = 0;
    if (!GetVarint32(&input, &delta)) {
      assert(false);
      return false;
    }
    value = i == 0 ? delta : value + delta;
    if (value >= slot) {
      return value == slot;
    }
  }
  return false;
}

void BlobCensusSlotSet::AppendTo(std::vector<uint32_t>* slots) const {
  assert(slots != nullptr);
  slots->reserve(slots->size() + count_);
  if (bitmap_) {
    for (size_t word = 0; word < words_.size(); ++word) {
      uint64_t bits = words_[word];
      while (bits != 0) {
        const int bit = CountTrailingZeroBits(bits);
        slots->push_back(static_cast<uint32_t>(word * 64 + bit));
        bits &= bits - 1;
      }
    }
    return;
  }
  Slice input(list_);
  uint32_t value = 0;
  for (uint32_t i = 0; i < count_; ++i) {
    uint32_t delta = 0;
    const bool ok = GetVarint32(&input, &delta);
    assert(ok);
    (void)ok;
    value = i == 0 ? delta : value + delta;
    slots->push_back(value);
  }
}

Status BlobCensusSlotSet::Update(const std::vector<uint32_t>& removed,
                                 const std::vector<uint32_t>& added,
                                 uint32_t slot_count,
                                 std::vector<uint32_t>* scratch) {
  assert(scratch != nullptr);
  if (bitmap_) {
    return UpdateBitmap(removed, added, slot_count, scratch);
  }
  std::vector<uint32_t> current;
  AppendTo(&current);
  scratch->clear();
  scratch->reserve(current.size() + added.size());
  size_t r = 0;
  size_t a = 0;
  for (size_t c = 0; c <= current.size(); ++c) {
    const uint32_t value =
        c < current.size() ? current[c] : std::numeric_limits<uint32_t>::max();
    // Copy the additions that are below the next kept slot.
    while (a < added.size() && added[a] < value) {
      if (!scratch->empty() && scratch->back() >= added[a]) {
        return Status::Corruption("Unsorted census slot addition");
      }
      scratch->push_back(added[a++]);
    }
    if (c == current.size()) {
      break;
    }
    if (r < removed.size() && removed[r] < value) {
      return Status::Corruption("Census slot removal does not match");
    }
    if (r < removed.size() && removed[r] == value) {
      ++r;
      // A slot freed and reused in the same change is added back here.
      if (a < added.size() && added[a] == value) {
        scratch->push_back(added[a++]);
      }
      continue;
    }
    if (a < added.size() && added[a] == value) {
      return Status::Corruption("Census slot added twice");
    }
    scratch->push_back(value);
  }
  if (r != removed.size() || a != added.size()) {
    return Status::Corruption("Census slot removal does not match");
  }
  Encode(*scratch, slot_count);
  return Status::OK();
}

Status BlobCensusSlotSet::UpdateBitmap(const std::vector<uint32_t>& removed,
                                       const std::vector<uint32_t>& added,
                                       uint32_t slot_count,
                                       std::vector<uint32_t>* scratch) {
  assert(bitmap_);
  const auto is_set = [this](uint32_t slot) {
    return slot / 64 < words_.size() &&
           (words_[slot / 64] >> (slot % 64) & 1) != 0;
  };
  // Check everything first, so a bad change leaves the set as it was.
  for (size_t i = 0; i < removed.size(); ++i) {
    if ((i > 0 && removed[i - 1] >= removed[i]) || !is_set(removed[i])) {
      return Status::Corruption("Census slot removal does not match");
    }
  }
  for (size_t i = 0; i < added.size(); ++i) {
    if (i > 0 && added[i - 1] >= added[i]) {
      return Status::Corruption("Unsorted census slot addition");
    }
    // A slot freed and reused in the same change is added back.
    if (is_set(added[i]) &&
        !std::binary_search(removed.begin(), removed.end(), added[i])) {
      return Status::Corruption("Census slot added twice");
    }
  }
  // Set and clear bits in place; no decode and re-encode of the set.
  for (uint32_t slot : removed) {
    words_[slot / 64] &= ~(uint64_t{1} << (slot % 64));
  }
  if (!added.empty() && added.back() / 64 >= words_.size()) {
    words_.resize(added.back() / 64 + 1, 0);
  }
  for (uint32_t slot : added) {
    words_[slot / 64] |= uint64_t{1} << (slot % 64);
  }
  count_ = count_ - static_cast<uint32_t>(removed.size()) +
           static_cast<uint32_t>(added.size());
  // Drop the zero words after the highest set bit.
  while (!words_.empty() && words_.back() == 0) {
    words_.pop_back();
  }
  // Encode again only when the set may cross the switch point: a list takes at
  // least one byte per slot, so with count_ >= U/16 it stays a bitmap.
  const size_t bitmap_bytes = (size_t{slot_count} + 7) / 8;
  if (count_ == 0 || count_ < bitmap_bytes / 2) {
    scratch->clear();
    AppendTo(scratch);
    Encode(*scratch, slot_count);
  } else if (words_.capacity() > 2 * words_.size() + 8) {
    words_.shrink_to_fit();
  }
  return Status::OK();
}

void BlobCensusSlotSet::Assign(const std::vector<uint32_t>& slots,
                               uint32_t slot_count) {
  assert(std::adjacent_find(slots.begin(), slots.end(),
                            std::greater_equal<uint32_t>()) == slots.end());
  Encode(slots, slot_count);
}

void BlobCensusSlotSet::Encode(const std::vector<uint32_t>& slots,
                               uint32_t slot_count) {
  size_t list_bytes = 0;
  for (size_t i = 0; i < slots.size(); ++i) {
    list_bytes += VarintLength(i == 0 ? slots[i] : slots[i] - slots[i - 1]);
  }
  const size_t bitmap_bytes = (size_t{slot_count} + 7) / 8;
  const bool use_bitmap =
      bitmap_ ? list_bytes >= bitmap_bytes / 2 : list_bytes > bitmap_bytes;
  count_ = static_cast<uint32_t>(slots.size());
  bitmap_ = use_bitmap && !slots.empty();
  if (bitmap_) {
    list_.clear();
    words_.assign(slots.back() / 64 + 1, 0);
    for (uint32_t slot : slots) {
      words_[slot / 64] |= uint64_t{1} << (slot % 64);
    }
  } else {
    words_.clear();
    // Exact size, so no capacity slack is charged.
    std::string list;
    list.reserve(list_bytes);
    for (size_t i = 0; i < slots.size(); ++i) {
      PutVarint32(&list, i == 0 ? slots[i] : slots[i] - slots[i - 1]);
    }
    list_.swap(list);
    list_.shrink_to_fit();
  }
  if (words_.capacity() > words_.size()) {
    words_.shrink_to_fit();
  }
}

Status BlobReferenceCensusBuildContext::ReserveInitialFiles(size_t file_count) {
  // The context remembers only removed SSTs of the pinned starting Version, so
  // their number cannot exceed that Version's file count. The change log limits
  // the bytes of deltas returned but not yet applied. Here we charge only their
  // list nodes: at most kMaxDeltasPerRead.
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
    size_t additional_slots, size_t additional_candidates) const {
  // A node of the `generations` map holds the WorkerGeneration record. Its
  // holder set and pending vectors are counted by capacity in slot_set_bytes.
  // The worker keeps no SST summaries: each delta carries the groups of its
  // removed SSTs.
  constexpr size_t kGenerationBytes =
      sizeof(std::map<uint64_t, WorkerGeneration>::value_type) +
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
  add(generations.size(), kGenerationBytes);
  add(1, slot_set_bytes);
  add(1, slots.MemoryUsage());
  add(scratch.capacity(), sizeof(uint32_t));
  // Room for the slot changes of the next operation: one slot per group in a
  // pending vector or the merge scratch, plus a new slot-table entry.
  add(additional_slots, 2 * sizeof(uint32_t) + sizeof(uint64_t));
  // Candidate map nodes, heap pointer capacity and retry set nodes (see
  // BlobGcCandidateHeap::MemoryUsage()). Only this worker changes the heap.
  add(1, candidates.MemoryUsage(additional_candidates));
  return bytes;
}

Status BlobReferenceCensus::WorkerState::Reserve(size_t additional_slots) {
  return Reserve(additional_slots, /*additional_candidates=*/0);
}

Status BlobReferenceCensus::WorkerState::Reserve(size_t additional_slots,
                                                 size_t additional_candidates) {
  const size_t bytes = MemoryUsage(additional_slots, additional_candidates);
  if (bytes == std::numeric_limits<size_t>::max()) {
    return Status::MemoryLimit("Blob census index size overflow");
  }
  reserved_bytes.store(bytes, std::memory_order_relaxed);
  return reservation_manager != nullptr
             ? reservation_manager->UpdateCacheReservation(bytes)
             : Status::OK();
}

size_t BlobReferenceCensus::WorkerState::GenerationBytes(
    const WorkerGeneration& entry) {
  return entry.holders.MemoryUsage() +
         (entry.pending_added.capacity() + entry.pending_removed.capacity()) *
             sizeof(uint32_t);
}

Status BlobReferenceCensus::WorkerState::ApplyFiles(
    const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& removed,
    const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& added) {
  // First collect the slot changes per generation number, so that each touched
  // generation number gets one merge pass. Removals free their slots before
  // additions take the lowest free slots.
  struct GenerationChange {
    std::vector<uint32_t> removed;
    std::vector<uint32_t> added;
    uint64_t removed_count = 0;
    uint64_t removed_stored = 0;
    uint64_t removed_record = 0;
    uint64_t added_count = 0;
    uint64_t added_stored = 0;
    uint64_t added_record = 0;
  };
  std::map<uint64_t, GenerationChange> changes;
  const auto has_census_group = [](const BlobReferenceSstSummary& file) {
    return std::any_of(file.groups.begin(), file.groups.end(), IsCensusGroup);
  };
  const auto add_totals = [](uint64_t* total, uint64_t value) {
    if (value > std::numeric_limits<uint64_t>::max() - *total) {
      return false;
    }
    *total += value;
    return true;
  };
  // Adds the census groups of `file`, whose holder slot is `slot`, to
  // `changes`: as a removal, or with `add` as an addition. Only an added group
  // is checked for a zero generation number or origin; a removed group was
  // checked when it was added.
  const auto collect_groups = [&changes, &add_totals](
                                  const BlobReferenceSstSummary& file,
                                  uint32_t slot, bool add) -> Status {
    for (const BlobReferenceGroup& group : file.groups) {
      if (!IsCensusGroup(group)) {
        continue;
      }
      if (add && (group.generation_file_number == 0 ||
                  group.origin_file_number == 0)) {
        return Status::Corruption("Invalid census generation group");
      }
      GenerationChange& change = changes[group.generation_file_number];
      std::vector<uint32_t>& change_slots = add ? change.added : change.removed;
      // Several groups of one generation number in this SST add only one holder
      // slot.
      if (change_slots.empty() || change_slots.back() != slot) {
        change_slots.push_back(slot);
      }
      if (!add_totals(add ? &change.added_count : &change.removed_count,
                      group.reference_count) ||
          !add_totals(add ? &change.added_stored : &change.removed_stored,
                      group.stored_value_bytes) ||
          !add_totals(add ? &change.added_record : &change.removed_record,
                      group.record_bytes)) {
        return Status::Corruption("Blob census group accounting overflow");
      }
    }
    return Status::OK();
  };
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file : removed) {
    if (file == nullptr || file->file_number == 0) {
      return Status::Corruption("Invalid census SST removal");
    }
    if (!has_census_group(*file)) {
      continue;
    }
    uint32_t slot = BlobCensusSlotTable::kNoSlot;
    if (slots.Find(file->file_number) == BlobCensusSlotTable::kNoSlot) {
      if (finalized) {
        return Status::Corruption("Removed SST has no census slot");
      }
      // The initial scan has not reached this starting SST yet.
      continue;
    }
    Status status = slots.Release(file->file_number, &slot);
    if (!status.ok()) {
      return status;
    }
    status = collect_groups(*file, slot, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
  }
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file : added) {
    if (file == nullptr || file->file_number == 0) {
      return Status::Corruption("Invalid or duplicate census SST summary");
    }
    if (!has_census_group(*file)) {
      continue;
    }
    uint32_t slot = 0;
    Status status = slots.Assign(file->file_number, &slot);
    if (!status.ok()) {
      return status;
    }
    status = collect_groups(*file, slot, /*add=*/true);
    if (!status.ok()) {
      return status;
    }
  }
  for (auto& [generation, change] : changes) {
    auto found = generations.find(generation);
    if (found == generations.end()) {
      if (!change.removed.empty()) {
        return Status::Corruption("Missing census SST generation accounting");
      }
      found = generations.emplace(generation, WorkerGeneration()).first;
      found->second.physical_file_number = ResolvePhysical(generation);
      slot_set_bytes += GenerationBytes(found->second);
    }
    WorkerGeneration& entry = found->second;
    if (entry.reference_count < change.removed_count ||
        entry.stored_value_bytes < change.removed_stored ||
        entry.record_bytes < change.removed_record) {
      return Status::Corruption("Missing census SST generation accounting");
    }
    Status status = AdjustGenerationBytes(entry, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
    entry.reference_count -= change.removed_count;
    entry.stored_value_bytes -= change.removed_stored;
    entry.record_bytes -= change.removed_record;
    if (!add_totals(&entry.reference_count, change.added_count) ||
        !add_totals(&entry.stored_value_bytes, change.added_stored) ||
        !add_totals(&entry.record_bytes, change.added_record)) {
      return Status::Corruption("Blob census group accounting overflow");
    }
    const size_t bytes_before = GenerationBytes(entry);
    if (finalized) {
      std::sort(change.removed.begin(), change.removed.end());
      std::sort(change.added.begin(), change.added.end());
      status = entry.holders.Update(change.removed, change.added,
                                    slots.slot_count(), &scratch);
      if (!status.ok()) {
        return status;
      }
    } else {
      entry.pending_removed.insert(entry.pending_removed.end(),
                                   change.removed.begin(),
                                   change.removed.end());
      entry.pending_added.insert(entry.pending_added.end(),
                                 change.added.begin(), change.added.end());
    }
    slot_set_bytes = slot_set_bytes - bytes_before + GenerationBytes(entry);
    status = AdjustGenerationBytes(entry, /*add=*/true);
    if (!status.ok()) {
      return status;
    }
    if (finalized && entry.holders.empty()) {
      if (entry.reference_count != 0 || entry.stored_value_bytes != 0 ||
          entry.record_bytes != 0) {
        return Status::Corruption("Census totals remain without holders");
      }
      slot_set_bytes -= GenerationBytes(entry);
      generations.erase(found);
    }
  }
  return Status::OK();
}

Status BlobReferenceCensus::WorkerState::Finalize() {
  if (finalized) {
    return Status::OK();
  }
  // Build each holder set once: collect, sort, encode. During the build a slot
  // can be added, removed, and then reused by another SST that holds the same
  // generation number. So a slot is in the set if its additions are one more
  // than its removals.
  std::vector<uint32_t> slots_held;
  for (auto found = generations.begin(); found != generations.end();) {
    WorkerGeneration& record = found->second;
    std::sort(record.pending_added.begin(), record.pending_added.end());
    std::sort(record.pending_removed.begin(), record.pending_removed.end());
    slots_held.clear();
    size_t r = 0;
    for (size_t a = 0; a < record.pending_added.size();) {
      const uint32_t slot = record.pending_added[a];
      size_t adds = 0;
      while (a < record.pending_added.size() &&
             record.pending_added[a] == slot) {
        ++adds;
        ++a;
      }
      while (r < record.pending_removed.size() &&
             record.pending_removed[r] < slot) {
        return Status::Corruption("Census slot removal does not match");
      }
      size_t removes = 0;
      while (r < record.pending_removed.size() &&
             record.pending_removed[r] == slot) {
        ++removes;
        ++r;
      }
      if (adds < removes || adds - removes > 1) {
        return Status::Corruption("Census slot removal does not match");
      }
      if (adds - removes == 1) {
        slots_held.push_back(slot);
      }
    }
    if (r != record.pending_removed.size()) {
      return Status::Corruption("Census slot removal does not match");
    }
    slot_set_bytes -= GenerationBytes(record);
    std::vector<uint32_t>().swap(record.pending_added);
    std::vector<uint32_t>().swap(record.pending_removed);
    record.holders.Assign(slots_held, slots.slot_count());
    slot_set_bytes += GenerationBytes(record);
    if (record.holders.empty()) {
      if (record.reference_count != 0 || record.stored_value_bytes != 0 ||
          record.record_bytes != 0) {
        return Status::Corruption("Census totals remain without holders");
      }
      slot_set_bytes -= GenerationBytes(record);
      found = generations.erase(found);
    } else {
      ++found;
    }
  }
  finalized = true;
  return Status::OK();
}

uint64_t BlobReferenceCensus::WorkerState::ResolvePhysical(
    uint64_t generation) const {
  const uint64_t target = forward_state != nullptr
                              ? forward_state->FindRouteTarget(generation)
                              : kInvalidBlobFileNumber;
  return target != kInvalidBlobFileNumber ? target : generation;
}

// Adds (`add` true) or subtracts the live bytes of one generation number on its
// physical file in the candidate heap. ApplyFiles() calls it when holders
// change, and ApplyWorkerDelta() calls it when a route moves the generation
// number to a new physical file. Live bytes must use the same unit as the total
// bytes of the physical file. An indirect-origin blob file counts whole blob
// records (header, key and value), so it uses the record bytes of the
// generation number. A relocation file stores each value with a trailer of
// kSimpleGen2BlobTrailerSize (5 bytes, see db/blob/blob_gen2_format.h), so its
// live bytes are the stored value bytes plus 5 bytes per reference.
Status BlobReferenceCensus::WorkerState::AdjustGenerationBytes(
    const WorkerGeneration& entry, bool add) {
  uint64_t bytes = entry.record_bytes;
  if (!candidates.IsIndirectOrigin(entry.physical_file_number)) {
    if (entry.reference_count >
        (std::numeric_limits<uint64_t>::max() - entry.stored_value_bytes) /
            kSimpleGen2BlobTrailerSize) {
      return Status::Corruption("Blob GC candidate generation-byte overflow");
    }
    bytes = entry.stored_value_bytes +
            entry.reference_count * kSimpleGen2BlobTrailerSize;
  }
  return candidates.AdjustLiveBytes(entry.physical_file_number, bytes, add);
}

std::shared_ptr<BlobReferenceCensus::WorkerState>
BlobReferenceCensus::LoadWorkerState() const {
  std::lock_guard<std::mutex> lock(worker_state_mutex_);
  return worker_state_;
}

std::shared_ptr<BlobReferenceCensus::WorkerState>
BlobReferenceCensus::ExchangeWorkerState(std::shared_ptr<WorkerState> next) {
  std::lock_guard<std::mutex> lock(worker_state_mutex_);
  worker_state_.swap(next);
  // The caller frees the old WorkerState after this lock is released.
  return next;
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
    next->reservation_manager = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kBlobReferenceCensus>>(
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
          {file->GetFileNumber(), file->GetTotalRecordBytes(),
           /*is_indirect_origin=*/false, file->IsMarkedLost()});
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
    next = ExchangeWorkerState(std::move(next));
    worker_active_token_.store(token, std::memory_order_release);
  }
  // Replaced maps and reservations are destroyed outside both mutexes.
  return Status::OK();
}

bool BlobReferenceCensus::IsActiveWorker(const WorkerState* worker,
                                         uint64_t token) const {
  return worker != nullptr && worker->token == token &&
         token == worker_active_token_.load(std::memory_order_acquire);
}

Status BlobReferenceCensus::AddStartingFile(
    uint64_t token, std::shared_ptr<const BlobReferenceSstSummary> file) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  TEST_SYNC_POINT("BlobReferenceCensus::AddStartingFile:CapturedWorker");
  TEST_SYNC_POINT("BlobReferenceCensus::AddStartingFile:BeforeWorkerLock");
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), token)) {
    return Status::Aborted("Census worker was replaced");
  }
  // The token checks above stop a run of a canceled build from adding its SST
  // to the worker of a newer build. Starting SSTs are added before the census
  // is first marked ready, so such an add would make the new census wrong when
  // it becomes ready.
  if (file == nullptr) {
    return Status::Corruption("Invalid or duplicate census SST summary");
  }
  Status status = worker->Reserve(file->groups.size());
  if (!status.ok()) {
    return status;
  }
  status = worker->ApplyFiles({}, {std::move(file)});
  if (!status.ok()) {
    return status;
  }
  return worker->Reserve(0);
}

Status BlobReferenceCensus::FinalizeWorkerBuild(uint64_t token) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), token)) {
    return Status::Aborted("Census worker was replaced");
  }
  Status status = worker->Finalize();
  if (!status.ok()) {
    return status;
  }
  return worker->Reserve(0);
}

Status BlobReferenceCensus::SetWorkerPhysicalFiles(
    uint64_t token, const std::vector<BlobGcPhysicalFileSummary>& files) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), token)) {
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
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), token)) {
    return {};
  }
  // A failure report or the end of a metadata-only commit that comes before
  // this selection is published sets the flag again. Then the next job run
  // publishes a fresh selection, even without a Version change.
  candidate_refresh_requested_.store(false, std::memory_order_release);
  return worker->candidates.Select();
}

uint64_t BlobReferenceCensus::RetryWorkerCandidates(uint64_t token,
                                                    uint64_t now_micros) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  // Checks only the worker's token, not IsActiveWorker(): it reads or changes
  // only the candidate heap of this worker object (RetryDue()), and a replaced
  // worker is dropped with its heap. The caller publishes the result only when
  // TryPublishWorker() accepts the same token.
  return worker != nullptr && worker->token == token
             ? worker->candidates.RetryDue(now_micros)
             : 0;
}

BlobGcCandidateHeap::Selection BlobReferenceCensus::SelectWorkerEmptyCandidates(
    uint64_t token) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  // Checks only the worker's token, not IsActiveWorker(): it reads or changes
  // only the candidate heap of this worker object (SelectEmpty()), and a
  // replaced worker is dropped with its heap. The caller publishes the result
  // only when TryPublishWorker() accepts the same token.
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

BlobGcCandidateHeap::Totals BlobReferenceCensus::WorkerCandidateTotals(
    uint64_t token) const {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  // Same token rule as WorkerRelocationGarbageBytes().
  return worker != nullptr && worker->token == token
             ? worker->candidates.GetTotals()
             : BlobGcCandidateHeap::Totals{};
}

uint64_t BlobReferenceCensus::WorkerRelocationGarbageBytes(
    uint64_t token) const {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  // Checks only the worker's token, not IsActiveWorker(): it only reads the
  // candidate heap of this worker object. The caller publishes the result only
  // when TryPublishWorker() accepts the same token.
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
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
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
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), stamp.worker_token)) {
    return;
  }
  if (!files.empty()) {
    // The caller may have removed these files from the candidate list in the
    // Version, even when the report is stale and ignored below. Ask the worker
    // to publish the list again from its current heap.
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

Status BlobReferenceCensus::SetWorkerCoarseIndirectOriginFrontier(
    uint64_t token, uint64_t frontier) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (!IsActiveWorker(worker.get(), token)) {
    return Status::Aborted("Census worker was replaced");
  }
  worker->candidates.SetCoarseIndirectOriginFrontier(frontier);
  return Status::OK();
}

Status BlobReferenceCensus::ApplyWorkerDelta(
    uint64_t token, const BlobReferenceCensusDelta& delta) {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
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
  if (delta.rebuild) {
    // A rebuild delta never reaches the worker: Install() invalidates the
    // census instead of linking the delta into the change log.
    return Status::Corruption("Census rebuild delta reached the worker");
  }
  size_t additional_slots = 0;
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
       delta.added_files) {
    if (file == nullptr ||
        file->groups.size() >
            std::numeric_limits<size_t>::max() - additional_slots) {
      return Status::Corruption("Invalid census worker delta summary");
    }
    additional_slots += file->groups.size();
  }
  for (const std::shared_ptr<const BlobReferenceSstSummary>& file :
       delta.removed_summaries) {
    if (file == nullptr ||
        file->groups.size() >
            std::numeric_limits<size_t>::max() - additional_slots) {
      return Status::Corruption("Invalid census worker delta summary");
    }
    additional_slots += file->groups.size();
  }
  Status status =
      worker->Reserve(additional_slots, delta.physical_files.size());
  if (!status.ok()) {
    return status;
  }
  // Add new physical files before applying the SST changes. Remove the gone
  // files (total 0) only after the changed routes moved their generation
  // numbers to the new targets.
  for (const BlobGcPhysicalFileSummary& file : delta.physical_files) {
    if (file.file_number == 0) {
      return Status::Corruption("Invalid blob GC physical file delta");
    }
    if (file.total_record_bytes != 0) {
      worker->candidates.SetPhysicalFile(file);
    }
  }
  worker->candidates.SetCoarseIndirectOriginFrontier(
      delta.coarse_indirect_origin_frontier);
  // Apply the removed and added SSTs as one change. Nothing is published in
  // between, even when a compaction has no output SST.
  status = worker->ApplyFiles(delta.removed_summaries, delta.added_files);
  if (!status.ok()) {
    return status;
  }
  worker->forward_state = delta.forward_state;
  for (uint64_t generation : delta.changed_routes) {
    const std::map<uint64_t, WorkerGeneration>::iterator found =
        worker->generations.find(generation);
    if (found == worker->generations.end()) {
      continue;
    }
    const uint64_t target = worker->ResolvePhysical(generation);
    if (target == found->second.physical_file_number) {
      continue;
    }
    status = worker->AdjustGenerationBytes(found->second, /*add=*/false);
    if (!status.ok()) {
      return status;
    }
    found->second.physical_file_number = target;
    status = worker->AdjustGenerationBytes(found->second, /*add=*/true);
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
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
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
    // Return the LOW pool thread instead of scheduling the unready census again
    // and again. Install() or CancelInstallPreparation() wakes the census job
    // of this column family.
    build_waiting_for_install_ = true;
    return PublishResult::kInstallPending;
  }
  if (applied_version_number != current_version_number) {
    return PublishResult::kStale;
  }
  worker_ready_version_.store(current_version_number,
                              std::memory_order_release);
  overflow_restart_used_ = false;
  // The restart limit ends here (see UseOverflowRestart()). Later deltas have
  // the normal limit again.
  build_cursor_.restart_build = false;
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
    const std::set<uint64_t>& generation_numbers,
    std::shared_ptr<BlobReferenceCensusState>* state) {
  if (state == nullptr) {
    return Status::InvalidArgument("Null selected census snapshot");
  }
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  {
    std::lock_guard<std::mutex> log_lock(mutex_);
    if (token == 0 || token != gc_job_token_ || canceled_ ||
        !snapshot_pending_) {
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
  Status status =
      CopyWorkerSnapshot(*worker, version_number, generation_numbers, state);
  {
    std::lock_guard<std::mutex> log_lock(mutex_);
    snapshot_pending_ = false;
  }
  return status;
}

Status BlobReferenceCensus::ReadSelectedSnapshot(
    uint64_t version_number, const std::set<uint64_t>& generation_numbers,
    std::shared_ptr<BlobReferenceCensusState>* state) const {
  if (state == nullptr) {
    return Status::InvalidArgument("Null selected census snapshot");
  }
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  if (worker == nullptr ||
      worker->token != worker_active_token_.load(std::memory_order_acquire) ||
      worker->version_number != version_number ||
      worker_ready_version_.load(std::memory_order_acquire) != version_number) {
    return Status::TryAgain("Selected census snapshot epoch changed");
  }
  std::shared_ptr<BlobReferenceCensusState> copy;
  Status status =
      CopyWorkerSnapshot(*worker, version_number, generation_numbers, &copy);
  if (status.ok()) {
    *state = std::move(copy);
  }
  return status;
}

std::shared_ptr<const BlobReferenceCensusState>
BlobReferenceCensus::TEST_GetReadyState(uint64_t version_number) const {
  std::shared_ptr<WorkerState> worker = LoadWorkerState();
  std::lock_guard<std::mutex> worker_lock(worker_mutex_);
  if (worker == nullptr || worker->version_number != version_number ||
      worker_ready_version_.load(std::memory_order_acquire) != version_number) {
    return nullptr;
  }
  std::set<uint64_t> generation_numbers;
  for (const auto& entry : worker->generations) {
    generation_numbers.insert(entry.first);
  }
  std::shared_ptr<BlobReferenceCensusState> result;
  const Status status =
      CopyWorkerSnapshot(*worker, version_number, generation_numbers, &result);
  assert(status.ok());
  status.PermitUncheckedError();
  return result;
}

Status BlobReferenceCensus::CopyWorkerSnapshot(
    const WorkerState& worker, uint64_t version_number,
    const std::set<uint64_t>& generation_numbers,
    std::shared_ptr<BlobReferenceCensusState>* state) const {
  if (!worker.finalized) {
    return Status::TryAgain("Census build has not finished");
  }
  // Count only the copied data, not all DB files: one map node per selected
  // generation number and one SST number per holder.
  constexpr size_t kSelectedGenerationBytes =
      sizeof(uint64_t) + 4 * sizeof(void*) +
      sizeof(BlobReferenceCensusState::SstFilesByGeneration::value_type) +
      4 * sizeof(void*);
  size_t bytes = sizeof(BlobReferenceCensusState);
  if (generation_numbers.size() >
      (std::numeric_limits<size_t>::max() - bytes) / kSelectedGenerationBytes) {
    return Status::MemoryLimit("Selected blob census size overflow");
  }
  bytes += generation_numbers.size() * kSelectedGenerationBytes;
  for (uint64_t generation : generation_numbers) {
    const auto found = worker.generations.find(generation);
    if (found == worker.generations.end()) {
      continue;
    }
    if (found->second.holders.count() >
        (std::numeric_limits<size_t>::max() - bytes) / sizeof(uint64_t)) {
      return Status::MemoryLimit("Selected blob census size overflow");
    }
    bytes += found->second.holders.count() * sizeof(uint64_t);
  }
  std::shared_ptr<CacheReservationManager> reservation_manager;
  if (worker.block_cache != nullptr) {
    reservation_manager = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kBlobReferenceCensus>>(
        worker.block_cache);
    const Status status = reservation_manager->UpdateCacheReservation(bytes);
    if (!status.ok()) {
      return status;
    }
  }
  std::shared_ptr<BlobReferenceCensusState> result =
      std::make_shared<BlobReferenceCensusState>(version_number);
  result->reservation_manager_ = std::move(reservation_manager);
  result->snapshot_memory_bound_ = bytes;
  result->selected_generations_ =
      std::make_shared<const std::set<uint64_t>>(generation_numbers);
  std::vector<uint32_t> slots;
  for (uint64_t generation : generation_numbers) {
    const auto found = worker.generations.find(generation);
    if (found == worker.generations.end()) {
      continue;
    }
    BlobReferenceCensusState::GenerationEntry& entry =
        result->generations_[generation];
    slots.clear();
    found->second.holders.AppendTo(&slots);
    entry.sst_files.reserve(slots.size());
    for (uint32_t slot : slots) {
      const uint64_t file_number = worker.slots.FileNumber(slot);
      if (file_number == 0) {
        return Status::Corruption("Census holder slot has no SST");
      }
      entry.sst_files.push_back(file_number);
    }
    std::sort(entry.sst_files.begin(), entry.sst_files.end());
    entry.totals.reference_count = found->second.reference_count;
    entry.totals.stored_value_bytes = found->second.stored_value_bytes;
    entry.totals.record_bytes = found->second.record_bytes;
  }
  // The estimate above is an upper bound; charge the real size.
  result->snapshot_memory_bound_ = SnapshotMemoryBound(*result);
  if (result->reservation_manager_ != nullptr) {
    Status status = result->reservation_manager_->UpdateCacheReservation(
        result->snapshot_memory_bound_);
    if (!status.ok()) {
      return status;
    }
  }
  *state = std::move(result);
  return Status::OK();
}

size_t BlobReferenceCensus::SnapshotMemoryBound(
    const BlobReferenceCensusState& state) {
  // The real size: the state, the selected generation set, one map node per
  // generation number, and 8 bytes per holder SST (vector capacity). A tree
  // node costs its value plus about 4 pointers (an estimate of the node
  // header).
  size_t bytes = sizeof(state);
  const auto add = [&bytes](size_t count, size_t item_bytes) {
    if (count > (std::numeric_limits<size_t>::max() - bytes) / item_bytes) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * item_bytes;
    }
  };
  if (state.selected_generations_ != nullptr) {
    add(state.selected_generations_->size(),
        sizeof(uint64_t) + 4 * sizeof(void*));
  }
  add(state.generations_.size(),
      sizeof(BlobReferenceCensusState::SstFilesByGeneration::value_type) +
          4 * sizeof(void*));
  for (const BlobReferenceCensusState::SstFilesByGeneration::value_type&
           generation : state.generations_) {
    add(generation.second.sst_files.capacity(), sizeof(uint64_t));
  }
  return bytes;
}

const std::vector<uint64_t>* BlobReferenceCensusState::FindSstFiles(
    uint64_t generation) const {
  const auto iterator = generations_.find(generation);
  return iterator == generations_.end() ? nullptr : &iterator->second.sst_files;
}

const BlobCensusGenerationTotals* BlobReferenceCensusState::FindTotals(
    uint64_t generation) const {
  const auto iterator = generations_.find(generation);
  return iterator == generations_.end() ? nullptr : &iterator->second.totals;
}

std::vector<uint64_t> BlobReferenceCensusState::SstFileNumbers() const {
  std::set<uint64_t> files;
  for (const auto& generation : generations_) {
    files.insert(generation.second.sst_files.begin(),
                 generation.second.sst_files.end());
  }
  return std::vector<uint64_t>(files.begin(), files.end());
}

bool BlobReferenceCensusState::SameMemberships(
    const BlobReferenceCensusState& other, std::string* difference) const {
  assert(difference != nullptr);
  if (generations_.size() != other.generations_.size()) {
    *difference = "generation count " + std::to_string(generations_.size()) +
                  " vs " + std::to_string(other.generations_.size());
    return false;
  }
  for (const auto& [generation, entry] : generations_) {
    const std::vector<uint64_t>* const files = other.FindSstFiles(generation);
    const BlobCensusGenerationTotals* const totals =
        other.FindTotals(generation);
    if (files == nullptr || *files != entry.sst_files ||
        totals->reference_count != entry.totals.reference_count ||
        totals->stored_value_bytes != entry.totals.stored_value_bytes ||
        totals->record_bytes != entry.totals.record_bytes) {
      *difference = "generation " + std::to_string(generation);
      return false;
    }
  }
  return true;
}

size_t BlobReferenceCensusState::GetSstCount(uint64_t generation) const {
  const auto* files = FindSstFiles(generation);
  return files == nullptr ? 0 : files->size();
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
  add_bytes(removed_summaries.capacity(),
            sizeof(std::shared_ptr<const BlobReferenceSstSummary>));
  add_bytes(added_files.capacity(),
            sizeof(std::shared_ptr<const BlobReferenceSstSummary>));
  add_bytes(changed_routes.capacity(), sizeof(uint64_t));
  add_bytes(changed_physical_files.capacity(), sizeof(uint64_t));
  add_bytes(physical_files.capacity(), sizeof(BlobGcPhysicalFileSummary));
  add_bytes(1, retained_forward_state_bytes);
  for (const auto* files : {&added_files, &removed_summaries}) {
    for (const std::shared_ptr<const BlobReferenceSstSummary>& file : *files) {
      if (file != nullptr) {
        add_bytes(1, file->MemoryUsage());
      }
    }
  }
  return bytes;
}

size_t BlobReferenceCensusDelta::ChargeBytes() const {
  size_t bytes = QueueWeight();
  if (bytes == std::numeric_limits<size_t>::max()) {
    return bytes;
  }
  for (const auto* files : {&added_files, &removed_summaries}) {
    for (const std::shared_ptr<const BlobReferenceSstSummary>& file : *files) {
      // A summary with its own cache charge is charged by the code that created
      // it (see the comment at ChargeBytes() in the header).
      if (file != nullptr && file->reservation_handle != nullptr) {
        bytes -= std::min(bytes, file->MemoryUsage());
      }
    }
  }
  return bytes;
}

BlobReferenceChangeLogEntry::~BlobReferenceChangeLogEntry() {
  if (log_bytes != nullptr) {
    log_bytes->fetch_sub(retained_bytes, std::memory_order_relaxed);
  }
  // An acknowledged prefix can hold many small entries. Free the entries that
  // only this chain owns in a loop, so that shared_ptr destruction does not
  // recurse once per entry.
  std::shared_ptr<BlobReferenceChangeLogEntry> pending = std::move(next);
  while (pending != nullptr && pending.use_count() == 1) {
    std::shared_ptr<BlobReferenceChangeLogEntry> following =
        std::move(pending->next);
    pending.reset();
    pending = std::move(following);
  }
}

bool BlobReferenceCensus::IsCensusGroup(const BlobReferenceGroup& group) {
  return group.encoding_class ==
             BlobReferenceEncodingClass::kIndirectUnflattened ||
         group.encoding_class == BlobReferenceEncodingClass::kIndirectFlattened;
}

Status BlobReferenceCensus::SumGroupsByGeneration(
    const BlobReferenceSstSummary& file,
    const std::set<uint64_t>* selected_names,
    std::map<uint64_t, BlobCensusGenerationTotals>* totals) {
  // An SST can hold several groups of one generation number (one per origin, or
  // one per encoding class). The census counts the SST once per generation
  // number.
  for (const BlobReferenceGroup& group : file.groups) {
    if (!IsCensusGroup(group) ||
        (selected_names != nullptr &&
         selected_names->count(group.generation_file_number) == 0)) {
      continue;
    }
    BlobCensusGenerationTotals& sum = (*totals)[group.generation_file_number];
    if (group.reference_count >
            std::numeric_limits<uint64_t>::max() - sum.reference_count ||
        group.stored_value_bytes >
            std::numeric_limits<uint64_t>::max() - sum.stored_value_bytes ||
        group.record_bytes >
            std::numeric_limits<uint64_t>::max() - sum.record_bytes) {
      return Status::Corruption("Blob census group accounting overflow");
    }
    sum.reference_count += group.reference_count;
    sum.stored_value_bytes += group.stored_value_bytes;
    sum.record_bytes += group.record_bytes;
  }
  return Status::OK();
}

Status BlobReferenceCensus::AddFile(const BlobReferenceSstSummary& file,
                                    BlobReferenceCensusState* state) {
  if (state == nullptr || file.file_number == 0) {
    return Status::Corruption("Invalid blob reference SST summary");
  }
  std::map<uint64_t, BlobCensusGenerationTotals> sums;
  Status status =
      SumGroupsByGeneration(file, state->selected_generations_.get(), &sums);
  if (!status.ok()) {
    return status;
  }
  for (const auto& [generation, sum] : sums) {
    BlobReferenceCensusState::GenerationEntry& entry =
        state->generations_[generation];
    const auto position = std::lower_bound(
        entry.sst_files.begin(), entry.sst_files.end(), file.file_number);
    if (position != entry.sst_files.end() && *position == file.file_number) {
      return Status::Corruption("Duplicate SST generation membership");
    }
    BlobCensusGenerationTotals& totals = entry.totals;
    if (sum.reference_count >
            std::numeric_limits<uint64_t>::max() - totals.reference_count ||
        sum.stored_value_bytes >
            std::numeric_limits<uint64_t>::max() - totals.stored_value_bytes ||
        sum.record_bytes >
            std::numeric_limits<uint64_t>::max() - totals.record_bytes) {
      return Status::Corruption("Blob census group accounting overflow");
    }
    entry.sst_files.insert(position, file.file_number);
    totals.reference_count += sum.reference_count;
    totals.stored_value_bytes += sum.stored_value_bytes;
    totals.record_bytes += sum.record_bytes;
  }
  return Status::OK();
}

Status BlobReferenceCensus::BuildState(
    uint64_t version_number,
    const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& files,
    std::shared_ptr<BlobReferenceCensusState>* state) {
  if (state == nullptr) {
    return Status::InvalidArgument("Null blob reference census output");
  }
  auto result = std::make_shared<BlobReferenceCensusState>(version_number);
  for (const auto& file : files) {
    if (file == nullptr) {
      return Status::Corruption("Invalid blob reference SST summary");
    }
    Status status = AddFile(*file, result.get());
    if (!status.ok()) {
      return status;
    }
  }
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
  if (delta->rebuild) {
    // The census is rebuilt on the new Version. The added summaries are already
    // charged by their builders, and they become seeds of that build.
    prepared->delta = std::move(delta);
    prepared->log_entry.reset();
    prepared->invalidate_census = true;
    prepared->reservation_id = reservation_id;
    prepared->delta_queue_weight = 0;
    return Status::OK();
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
              CacheEntryRole::kBlobReferenceCensus>>(block_cache));
      // The MANIFEST queue runs one preparation at a time for this column
      // family. Construct the cache objects without a lock, and swap only the
      // handles under the log mutex.
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
    // ChargeBytes() leaves out the summaries that have their own charge, so
    // they are not charged twice.
    const size_t bytes = delta->ChargeBytes();
    if (bytes == std::numeric_limits<size_t>::max()) {
      return Status::MemoryLimit("Blob change log retention size overflow");
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

  // The table builder has already checked and prepared the SST metadata. The
  // census worker applies the holder changes later through the build cursor;
  // nothing is copied at the MANIFEST queue head.
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

void BlobReferenceCensus::AppendChange(
    const BlobReferenceCensusPreparedInstall& prepared,
    BlobReferenceCensusGarbage* garbage) {
  const size_t bytes = prepared.delta_queue_weight;
  size_t limit = kMaxLogMemory;
  // Tests lower the limit through this sync point. The restart limit keeps the
  // same factor (kMaxRestartLogMemory / kMaxLogMemory) to the limit.
  TEST_SYNC_POINT_CALLBACK("BlobReferenceCensus::AppendChange:Limit", &limit);
  constexpr size_t kRestartFactor = kMaxRestartLogMemory / kMaxLogMemory;
  const size_t restart_limit =
      limit > std::numeric_limits<size_t>::max() / kRestartFactor
          ? std::numeric_limits<size_t>::max()
          : limit * kRestartFactor;
  bool retain = false;
  ChangeLogCursor& cursor = build_cursor_;
  // The limit is checked before the append: the cursor overflows only when its
  // outstanding weight has already reached the limit. So it holds at most the
  // limit plus one delta. One large but valid delta (for example, a compaction
  // that rewrites a whole level) then does not overflow a cursor that holds
  // only small deltas, such as flushes.
  if (cursor.token != 0 && !cursor.overflowed) {
    if (cursor.outstanding_bytes >=
        (cursor.restart_build ? restart_limit : limit)) {
      cursor.overflowed = true;
      DetachCursor(&cursor, garbage,
                   BlobReferenceCensusGarbage::kBuildCursorSlot);
    } else {
      retain = true;
      cursor.outstanding_bytes += bytes;
      if (cursor.delivered == nullptr && cursor.first == nullptr) {
        cursor.first = prepared.log_entry;
      }
    }
  }
  if (!retain) {
    garbage->log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
        std::move(log_tail_);
    return;
  }
  assert(prepared.log_entry != nullptr && prepared.log_entry->next == nullptr);
  prepared.log_entry->log_bytes = log_bytes_;
  log_bytes_->fetch_add(prepared.log_entry->retained_bytes,
                        std::memory_order_relaxed);
  std::shared_ptr<BlobReferenceChangeLogEntry> previous = std::move(log_tail_);
  if (previous != nullptr) {
    assert(previous->next == nullptr);
    previous->next = prepared.log_entry;
    // A detached reader can release the only other owner at the same time. Hand
    // the old tail to the caller, so that it never frees a large prefix under
    // the DB mutex.
    garbage->log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
        std::move(previous);
  }
  log_tail_ = prepared.log_entry;
}

BlobReferenceCensusGarbage BlobReferenceCensus::Install(
    BlobReferenceCensusPreparedInstall& prepared) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto finish_install = [&]() {
    const bool wake_build = std::exchange(build_waiting_for_install_, false);
    lock.unlock();
    // The caller still holds the DB mutex. The callback takes the log mutex to
    // check readiness, so it must run after this unlock.
    if (wake_build && build_wakeup_ != nullptr) {
      build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
    }
  };
  BlobReferenceCensusGarbage garbage;
  // Guard: this prepared install cannot be trusted. This happens when the
  // census was canceled (for example, the column family was dropped), when the
  // install has no reservation or no prepared result, or when the active
  // reservation is not the one of this install. The census drops all of its
  // state (worker, build context, build cursor and log tail). If the census was
  // not canceled, the next census job builds it again. Unlike the invalidate
  // branch below, it adds no rebuild seeds and does not request a rebuild. A
  // running build fails its token check and waits for the normal retry delay.
  if ((!prepared.invalidate_census && prepared.delta == nullptr) || canceled_ ||
      prepared.reservation_id == 0 ||
      active_install_reservation_.load(std::memory_order_acquire) !=
          prepared.reservation_id) {
    worker_ready_version_.store(0, std::memory_order_release);
    garbage.build_context = std::move(build_context_);
    DetachCursor(&build_cursor_, &garbage,
                 BlobReferenceCensusGarbage::kBuildCursorSlot);
    garbage.worker_state = ExchangeWorkerState(std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    build_cursor_.token = 0;
    garbage.log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
        std::move(log_tail_);
    active_install_reservation_.store(0, std::memory_order_release);
    finish_install();
    return garbage;
  }
  active_install_reservation_.store(0, std::memory_order_release);
  if (prepared.invalidate_census) {
    worker_ready_version_.store(0, std::memory_order_release);
    if (prepared.delta != nullptr && prepared.delta->rebuild) {
      // The old seeds go to the garbage, which is released without the mutexes.
      garbage.summaries =
          std::exchange(rebuild_seeds_, prepared.delta->added_files);
      rebuild_requested_ = true;
      TEST_SYNC_POINT("BlobReferenceCensus::Install:Rebuild");
    }
    // A later retry must pin a new Version and get a new token, not reuse this
    // canceled context. An active worker run keeps its own pin until it
    // returns.
    garbage.build_context = std::move(build_context_);
    garbage.worker_state = ExchangeWorkerState(std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    DetachCursor(&build_cursor_, &garbage,
                 BlobReferenceCensusGarbage::kBuildCursorSlot);
    build_cursor_.token = 0;
    garbage.log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
        std::move(log_tail_);
    finish_install();
    return garbage;
  }

  AppendChange(prepared, &garbage);
  finish_install();
  return garbage;
}

uint64_t BlobReferenceCensus::StayingMemoryBytes() const {
  const std::shared_ptr<WorkerState> worker = LoadWorkerState();
  return (worker != nullptr
              ? worker->reserved_bytes.load(std::memory_order_relaxed)
              : 0) +
         log_bytes_->load(std::memory_order_relaxed);
}

uint64_t BlobReferenceCensus::BeginBuild() {
  build_count_.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(mutex_);
  assert(build_cursor_.token == 0);
  canceled_ = false;
  build_cursor_.token = next_build_token_++;
  if (build_cursor_.token == 0) {
    build_cursor_.token = next_build_token_++;
  }
  build_cursor_.overflowed = false;
  build_cursor_.restart_build = overflow_restart_used_;
  rebuild_requested_ = false;
  return build_cursor_.token;
}

bool BlobReferenceCensus::RebuildRequested() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return rebuild_requested_;
}

std::vector<std::shared_ptr<const BlobReferenceSstSummary>>
BlobReferenceCensus::TakeRebuildSeeds() {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::exchange(rebuild_seeds_, {});
}

bool BlobReferenceCensus::UseOverflowRestart() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (overflow_restart_used_) {
    return false;
  }
  overflow_restart_used_ = true;
  overflow_restart_count_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

Status BlobReferenceCensus::TakeBuildDeltas(
    uint64_t token, BlobReferenceCensusDeltaQueue* deltas, bool* overflowed) {
  if (deltas == nullptr || overflowed == nullptr || !deltas->empty()) {
    return Status::InvalidArgument("Invalid blob change log output");
  }
  std::array<std::shared_ptr<BlobReferenceChangeLogEntry>, kMaxDeltasPerRead>
      entries;
  size_t count = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (token == 0 || token != build_cursor_.token || canceled_) {
      return Status::Aborted("Blob change log cursor is no longer active");
    }
    *overflowed = build_cursor_.overflowed;
    if (build_cursor_.overflowed) {
      return Status::OK();
    }
    std::shared_ptr<BlobReferenceChangeLogEntry> entry =
        build_cursor_.delivered != nullptr ? build_cursor_.delivered->next
                                           : build_cursor_.first;
    while (entry != nullptr && count < entries.size()) {
      entries[count++] = entry;
      entry = entry->next;
    }
    if (count != 0) {
      build_cursor_.delivered = entries[count - 1];
    }
  }
  // Allocate the list nodes without the log mutex. The entry handles above keep
  // the immutable deltas alive while they are copied.
  for (size_t index = 0; index < count; ++index) {
    assert(entries[index]->delta != nullptr);
    deltas->emplace_back(entries[index]->delta);
  }
  return Status::OK();
}

Status BlobReferenceCensus::AcknowledgeBuildDeltas(uint64_t token,
                                                   size_t retained_bytes) {
  std::shared_ptr<BlobReferenceChangeLogEntry> old_acknowledged;
  std::shared_ptr<BlobReferenceChangeLogEntry> old_first;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_acknowledged;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_delivered;
  std::shared_ptr<BlobReferenceChangeLogEntry> drained_tail;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (token == 0 || token != build_cursor_.token || canceled_ ||
        build_cursor_.overflowed) {
      return Status::Aborted("Blob change log cursor is no longer active");
    }
    if (retained_bytes > build_cursor_.outstanding_bytes) {
      return Status::Corruption("Blob change log acknowledged excess bytes");
    }
    if (retained_bytes == 0) {
      return Status::OK();
    }
    if (build_cursor_.delivered == nullptr) {
      return Status::Corruption(
          "Blob change log acknowledged undelivered bytes");
    }
    std::shared_ptr<BlobReferenceChangeLogEntry> entry =
        build_cursor_.acknowledged != nullptr ? build_cursor_.acknowledged->next
                                              : build_cursor_.first;
    std::shared_ptr<BlobReferenceChangeLogEntry> acknowledged;
    size_t remaining = retained_bytes;
    for (size_t count = 0; count < kMaxDeltasPerRead && entry != nullptr;
         ++count) {
      if (entry->retained_bytes > remaining) {
        break;
      }
      remaining -= entry->retained_bytes;
      acknowledged = entry;
      if (remaining == 0 || entry == build_cursor_.delivered) {
        break;
      }
      entry = entry->next;
    }
    if (remaining != 0 || acknowledged == nullptr) {
      return Status::Corruption(
          "Blob change log acknowledged undelivered bytes");
    }
    old_acknowledged = std::move(build_cursor_.acknowledged);
    old_first = std::move(build_cursor_.first);
    build_cursor_.acknowledged = std::move(acknowledged);
    build_cursor_.outstanding_bytes -= retained_bytes;
    // Appends and acknowledgments both use QueueWeight(), so a cursor that has
    // acknowledged every entry appended to it has no outstanding weight.
    assert(build_cursor_.acknowledged->next != nullptr ||
           build_cursor_.outstanding_bytes == 0);
    if (build_cursor_.outstanding_bytes == 0) {
      drained_acknowledged = std::move(build_cursor_.acknowledged);
      drained_delivered = std::move(build_cursor_.delivered);
      drained_tail = std::move(log_tail_);
    }
  }
  return Status::OK();
}

bool BlobReferenceCensus::BuildCursorOverflowed(uint64_t token) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return token != 0 && token == build_cursor_.token && build_cursor_.overflowed;
}

BlobReferenceCensusGarbage BlobReferenceCensus::FailBuild(uint64_t token) {
  std::lock_guard<std::mutex> lock(mutex_);
  BlobReferenceCensusGarbage garbage;
  if (token != 0 && token == build_cursor_.token) {
    worker_ready_version_.store(0, std::memory_order_release);
    garbage.worker_state = ExchangeWorkerState(std::shared_ptr<WorkerState>());
    worker_active_token_.store(0, std::memory_order_release);
    garbage.build_context = std::move(build_context_);
    DetachCursor(&build_cursor_, &garbage,
                 BlobReferenceCensusGarbage::kBuildCursorSlot);
    build_cursor_.token = 0;
    build_cursor_.overflowed = false;
    garbage.log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
        std::move(log_tail_);
  }
  return garbage;
}

BlobReferenceCensusGarbage BlobReferenceCensus::Cancel() {
  std::lock_guard<std::mutex> lock(mutex_);
  {
    std::lock_guard<std::mutex> commit_lock(metadata_commit_mutex_);
    metadata_commit_canceled_ = true;
  }
  canceled_ = true;
  build_waiting_for_install_ = false;
  build_wakeup_ = nullptr;
  build_wakeup_db_ = nullptr;
  build_wakeup_cfd_ = nullptr;
  snapshot_pending_ = false;
  worker_ready_version_.store(0, std::memory_order_release);
  BlobReferenceCensusGarbage garbage;
  garbage.worker_state = ExchangeWorkerState(std::shared_ptr<WorkerState>());
  worker_active_token_.store(0, std::memory_order_release);
  garbage.build_context = std::move(build_context_);
  DetachCursor(&build_cursor_, &garbage,
               BlobReferenceCensusGarbage::kBuildCursorSlot);
  build_cursor_.token = 0;
  gc_job_token_ = 0;
  garbage.log_entries[BlobReferenceCensusGarbage::kLogTailSlot] =
      std::move(log_tail_);
  // The seeds are charged summaries: hand them to the caller, so they are freed
  // without the DB mutex.
  garbage.summaries = std::exchange(rebuild_seeds_, {});
  active_install_reservation_.store(0, std::memory_order_release);
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

Status BlobReferenceCensus::BeginGcJob(uint64_t version_number,
                                       uint64_t* token) {
  if (token == nullptr || version_number == 0) {
    return Status::InvalidArgument("Invalid blob GC job start");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (canceled_) {
    return Status::Aborted("Blob reference census is canceled");
  }
  {
    // A metadata-only commit (see the top of blob_reference_census.h) in
    // progress can remove a route that this indirect blob GC job would
    // re-point, so start after it. The lock order is the same as in Cancel().
    std::lock_guard<std::mutex> commit_lock(metadata_commit_mutex_);
    if (metadata_commit_ambiguous_) {
      return Status::Busy("Blob metadata commit result is unknown");
    }
    if (metadata_commit_pending_) {
      return Status::TryAgain("A blob metadata commit is in progress");
    }
  }
  const bool worker_ready =
      worker_ready_version_.load(std::memory_order_acquire) == version_number;
  if (!worker_ready) {
    return Status::TryAgain("Blob reference census is not ready");
  }
  if (gc_job_token_ != 0) {
    return Status::Busy("A blob GC job is already registered");
  }
  gc_job_token_ = next_gc_token_++;
  if (gc_job_token_ == 0) {
    gc_job_token_ = next_gc_token_++;
  }
  snapshot_pending_ = worker_ready;
  *token = gc_job_token_;
  return Status::OK();
}

bool BlobReferenceCensus::IsGcJobActive(uint64_t token) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return token != 0 && token == gc_job_token_ && !canceled_;
}

void BlobReferenceCensus::EndGcJob(uint64_t token) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (token != 0 && token == gc_job_token_) {
    gc_job_token_ = 0;
    snapshot_pending_ = false;
  }
}

// Groundwork for indirect blob GC jobs (see "Groundwork term" in the top
// comment of blob_reference_census.h): the metadata-only commit.

Status BlobReferenceCensus::BeginMetadataCommit() {
  std::lock_guard<std::mutex> lock(metadata_commit_mutex_);
  if (metadata_commit_canceled_) {
    return Status::ColumnFamilyDropped();
  }
  if (metadata_commit_pending_ || metadata_commit_ambiguous_) {
    return Status::TryAgain("A blob metadata commit is in progress");
  }
  metadata_commit_pending_ = true;
  return Status::OK();
}

void BlobReferenceCensus::EndMetadataCommit(bool result_known) {
  {
    std::lock_guard<std::mutex> lock(metadata_commit_mutex_);
    metadata_commit_pending_ = false;
    metadata_commit_ambiguous_ |= !result_known;
    candidate_refresh_requested_.store(true, std::memory_order_release);
  }
  if (result_known && build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
}

void BlobReferenceCensus::ResolveMetadataCommitAmbiguity() {
  {
    std::lock_guard<std::mutex> lock(metadata_commit_mutex_);
    if (!metadata_commit_ambiguous_ || metadata_commit_pending_ ||
        metadata_commit_canceled_) {
      return;
    }
    metadata_commit_ambiguous_ = false;
    candidate_refresh_requested_.store(true, std::memory_order_release);
  }
  if (build_wakeup_ != nullptr) {
    build_wakeup_(build_wakeup_db_, build_wakeup_cfd_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
