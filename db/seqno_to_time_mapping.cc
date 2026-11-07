//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/seqno_to_time_mapping.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <deque>
#include <functional>
#include <queue>
#include <vector>

#include "db/version_edit.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterTime(
    uint64_t time) const {
  assert(enforced_);
  return std::upper_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{0, time}, SeqnoTimePair::TimeLess);
}

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterEqSeqno(
    SequenceNumber seqno) const {
  assert(enforced_);
  return std::lower_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{seqno, 0}, SeqnoTimePair::SeqnoLess);
}

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterSeqno(
    SequenceNumber seqno) const {
  assert(enforced_);
  return std::upper_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{seqno, 0}, SeqnoTimePair::SeqnoLess);
}

uint64_t SeqnoToTimeMapping::GetProximalTimeBeforeSeqno(
    SequenceNumber seqno) const {
  assert(enforced_);
  // Find the last entry with a seqno strictly less than the given seqno.
  // First, find the first entry >= the given seqno (or end)
  auto it = FindGreaterEqSeqno(seqno);
  if (it == pairs_.cbegin()) {
    return kUnknownTimeBeforeAll;
  }
  // Then return data from previous.
  it--;
  return it->time;
}

SequenceNumber SeqnoToTimeMapping::GetProximalSeqnoBeforeTime(
    uint64_t time) const {
  assert(enforced_);

  // Find the last entry with a time <= the given time.
  // First, find the first entry > the given time (or end).
  auto it = FindGreaterTime(time);
  if (it == pairs_.cbegin()) {
    return kUnknownSeqnoBeforeAll;
  }
  // Then return data from previous.
  --it;
  return it->seqno;
}

void SeqnoToTimeMapping::GetCurrentTieringCutoffSeqnos(
    uint64_t current_time, uint64_t preserve_internal_time_seconds,
    uint64_t preclude_last_level_data_seconds,
    SequenceNumber* preserve_time_min_seqno,
    SequenceNumber* preclude_last_level_min_seqno) const {
  uint64_t preserve_time_duration = std::max(preserve_internal_time_seconds,
                                             preclude_last_level_data_seconds);
  if (preserve_time_duration <= 0) {
    return;
  }
  uint64_t preserve_time = current_time > preserve_time_duration
                               ? current_time - preserve_time_duration
                               : 0;
  // GetProximalSeqnoBeforeTime tells us the last seqno known to have been
  // written at or before the given time. + 1 to get the minimum we should
  // preserve without excluding anything that might have been written on or
  // after the given time.
  if (preserve_time_min_seqno) {
    *preserve_time_min_seqno = GetProximalSeqnoBeforeTime(preserve_time) + 1;
  }
  if (preclude_last_level_data_seconds > 0 && preclude_last_level_min_seqno) {
    uint64_t preclude_last_level_time =
        current_time > preclude_last_level_data_seconds
            ? current_time - preclude_last_level_data_seconds
            : 0;
    *preclude_last_level_min_seqno =
        GetProximalSeqnoBeforeTime(preclude_last_level_time) + 1;
  }
}

void SeqnoToTimeMapping::EnforceMaxTimeSpan(uint64_t now) {
  assert(enforced_);  // at least sorted
  uint64_t cutoff_time;
  if (pairs_.size() <= 1) {
    return;
  }
  if (now > 0) {
    if (now < max_time_span_) {
      // Nothing eligible to prune / avoid underflow
      return;
    }
    cutoff_time = now - max_time_span_;
  } else {
    const auto& last = pairs_.back();
    if (last.time < max_time_span_) {
      // Nothing eligible to prune / avoid underflow
      return;
    }
    cutoff_time = last.time - max_time_span_;
  }
  // Keep one entry <= cutoff_time
  while (pairs_.size() >= 2 && pairs_[0].time <= cutoff_time &&
         pairs_[1].time <= cutoff_time) {
    pairs_.pop_front();
  }
}

void SeqnoToTimeMapping::EnforceCapacity(bool strict) {
  assert(enforced_);  // at least sorted
  uint64_t strict_cap = capacity_;
  if (strict_cap == 0) {
    pairs_.clear();
    return;
  }
  // Treat cap of 1 as 2 to work with the below algorithm (etc.)
  if (strict_cap == 1) {
    strict_cap = 2;
  }
  // When !strict, allow being over nominal capacity by a modest fraction.
  uint64_t effective_cap = strict_cap + (strict ? 0 : strict_cap / 8);
  if (effective_cap < strict_cap) {
    // Correct overflow
    effective_cap = UINT64_MAX;
  }
  if (pairs_.size() <= effective_cap) {
    return;
  }
  // The below algorithm expects at least one removal candidate between first
  // and last.
  assert(pairs_.size() >= 3);
  size_t to_remove_count = pairs_.size() - strict_cap;

  struct RemovalCandidate {
    uint64_t new_time_gap;
    std::deque<SeqnoTimePair>::iterator it;
    RemovalCandidate(uint64_t _new_time_gap,
                     const std::deque<SeqnoTimePair>::iterator& _it)
        : new_time_gap(_new_time_gap), it(_it) {}
    bool operator>(const RemovalCandidate& other) const {
      if (new_time_gap == other.new_time_gap) {
        // If same gap, treat the newer entry as less attractive
        // for removal (like larger gap)
        return it->seqno > other.it->seqno;
      }
      return new_time_gap > other.new_time_gap;
    }
  };

  // A priority queue of best removal candidates (smallest time gap remaining
  // after removal)
  using RC = RemovalCandidate;
  using PQ = std::priority_queue<RC, std::vector<RC>, std::greater<RC>>;
  PQ pq;

  // Add all the candidates (not including first and last)
  {
    auto it = pairs_.begin();
    assert(it->time != kUnknownTimeBeforeAll);
    uint64_t prev_prev_time = it->time;
    ++it;
    assert(it->time != kUnknownTimeBeforeAll);
    auto prev_it = it;
    ++it;
    while (it != pairs_.end()) {
      assert(it->time != kUnknownTimeBeforeAll);
      uint64_t gap = it->time - prev_prev_time;
      pq.emplace(gap, prev_it);
      prev_prev_time = prev_it->time;
      prev_it = it;
      ++it;
    }
  }

  // Greedily remove the best candidate, iteratively
  while (to_remove_count > 0) {
    assert(!pq.empty());
    // Remove the candidate with smallest gap
    auto rc = pq.top();
    pq.pop();

    // NOTE: priority_queue does not support updating an existing element,
    // but we can work around that because the gap tracked in pq is only
    // going to be better than actuality, and we can detect and adjust
    // when a better-than-actual gap is found.

    // Determine actual time gap if this entry is removed (zero entries are
    // marked for deletion)
    auto it = rc.it + 1;
    uint64_t after_time = it->time;
    while (after_time == kUnknownTimeBeforeAll) {
      assert(it != pairs_.end());
      ++it;
      after_time = it->time;
    }
    it = rc.it - 1;
    uint64_t before_time = it->time;
    while (before_time == kUnknownTimeBeforeAll) {
      assert(it != pairs_.begin());
      --it;
      before_time = it->time;
    }
    // Check whether the gap is still valid (or needs to be recomputed)
    if (rc.new_time_gap == after_time - before_time) {
      // Mark the entry as removed
      rc.it->time = kUnknownTimeBeforeAll;
      --to_remove_count;
    } else {
      // Insert a replacement up-to-date removal candidate
      pq.emplace(after_time - before_time, rc.it);
    }
  }

  // Collapse away entries marked for deletion
  auto from_it = pairs_.begin();
  auto to_it = from_it;

  for (; from_it != pairs_.end(); ++from_it) {
    if (from_it->time != kUnknownTimeBeforeAll) {
      if (from_it != to_it) {
        *to_it = *from_it;
      }
      ++to_it;
    }
  }

  // Erase slots freed up
  pairs_.erase(to_it, pairs_.end());
  assert(pairs_.size() == strict_cap);
}

bool SeqnoToTimeMapping::SeqnoTimePair::Merge(const SeqnoTimePair& other) {
  assert(seqno <= other.seqno);
  if (seqno == other.seqno) {
    // Favoring GetProximalSeqnoBeforeTime over GetProximalTimeBeforeSeqno
    // by keeping the older time. For example, consider nothing has been
    // written to the DB in some time.
    time = std::min(time, other.time);
    return true;
  } else if (time == other.time) {
    // Favoring GetProximalSeqnoBeforeTime over GetProximalTimeBeforeSeqno
    // by keeping the newer seqno. For example, when a burst of writes ages
    // out, we want the cutoff to be the newest seqno from that burst.
    seqno = std::max(seqno, other.seqno);
    return true;
  } else if (time > other.time) {
    assert(seqno < other.seqno);
    // Need to resolve an inconsistency (clock drift? very rough time?).
    // Given the direction that entries are supposed to err, trust the earlier
    // time entry as more reliable, and this choice ensures we don't
    // accidentally throw out an entry within our time span.
    *this = other;
    return true;
  } else {
    // Not merged
    return false;
  }
}

void SeqnoToTimeMapping::SortAndMerge() {
  assert(!enforced_);
  if (!pairs_.empty()) {
    std::sort(pairs_.begin(), pairs_.end());

    auto from_it = pairs_.begin();
    auto to_it = from_it;
    for (++from_it; from_it != pairs_.end(); ++from_it) {
      if (to_it->Merge(*from_it)) {
        // Merged with last entry
      } else {
        // Copy into next entry
        *++to_it = *from_it;
      }
    }
    // Erase slots freed up from merging
    pairs_.erase(to_it + 1, pairs_.end());
  }
  // Mark as "at least sorted"
  enforced_ = true;
}

SeqnoToTimeMapping& SeqnoToTimeMapping::SetMaxTimeSpan(uint64_t max_time_span) {
  max_time_span_ = max_time_span;
  if (enforced_) {
    EnforceMaxTimeSpan();
  }
  return *this;
}

SeqnoToTimeMapping& SeqnoToTimeMapping::SetCapacity(uint64_t capacity) {
  capacity_ = capacity;
  if (enforced_) {
    EnforceCapacity(/*strict=*/true);
  }
  return *this;
}

SeqnoToTimeMapping& SeqnoToTimeMapping::Enforce(uint64_t now) {
  if (!enforced_) {
    SortAndMerge();
    assert(enforced_);
    EnforceMaxTimeSpan(now);
  } else if (now > 0) {
    EnforceMaxTimeSpan(now);
  }
  EnforceCapacity(/*strict=*/true);
  return *this;
}

void SeqnoToTimeMapping::AddUnenforced(SequenceNumber seqno, uint64_t time) {
  if (seqno == 0) {
    return;
  }
  enforced_ = false;
  pairs_.emplace_back(seqno, time);
}

// The encoded format is:
//  [num_of_entries][[seqno][time],[seqno][time],...]
//      ^                                 ^
//    var_int                      delta_encoded (var_int)
// Except empty string is used for empty mapping. This means the encoding
// doesn't fully form a prefix code, but that is OK for applications like
// TableProperties.
void SeqnoToTimeMapping::EncodeTo(std::string& dest) const {
  assert(enforced_);
  // Can use empty string for empty mapping
  if (pairs_.empty()) {
    return;
  }
  // Encode number of entries
  PutVarint64(&dest, pairs_.size());
  SeqnoTimePair base;
  for (auto& cur : pairs_) {
    assert(base < cur);
    // Delta encode each entry
    SeqnoTimePair val = cur.ComputeDelta(base);
    base = cur;
    val.Encode(dest);
  }
}

namespace {
Status DecodeImpl(Slice& input,
                  std::deque<SeqnoToTimeMapping::SeqnoTimePair>& pairs) {
  if (input.empty()) {
    return Status::OK();
  }
  uint64_t count;
  if (!GetVarint64(&input, &count)) {
    return Status::Corruption("Invalid sequence number time size");
  }

  SeqnoToTimeMapping::SeqnoTimePair base;
  for (uint64_t i = 0; i < count; i++) {
    SeqnoToTimeMapping::SeqnoTimePair val;
    Status s = val.Decode(input);
    if (!s.ok()) {
      return s;
    }
    val.ApplyDelta(base);
    pairs.emplace_back(val);
    base = val;
  }

  if (!input.empty()) {
    return Status::Corruption(
        "Extra bytes at end of sequence number time mapping");
  }
  return Status::OK();
}
}  // namespace

Status SeqnoToTimeMapping::DecodeFrom(const std::string& pairs_str) {
  size_t orig_size = pairs_.size();

  Slice input(pairs_str);
  Status s = DecodeImpl(input, pairs_);
  if (!s.ok()) {
    // Roll back in case of corrupted data
    pairs_.resize(orig_size);
  } else if (orig_size > 0 || max_time_span_ < UINT64_MAX ||
             capacity_ < UINT64_MAX) {
    enforced_ = false;
  }
  return s;
}

void SeqnoToTimeMapping::SeqnoTimePair::Encode(std::string& dest) const {
  PutVarint64Varint64(&dest, seqno, time);
}

Status SeqnoToTimeMapping::SeqnoTimePair::Decode(Slice& input) {
  if (!GetVarint64(&input, &seqno)) {
    return Status::Corruption("Invalid sequence number");
  }
  if (!GetVarint64(&input, &time)) {
    return Status::Corruption("Invalid time");
  }
  return Status::OK();
}

void SeqnoToTimeMapping::CopyFromSeqnoRange(const SeqnoToTimeMapping& src,
                                            SequenceNumber from_seqno,
                                            SequenceNumber to_seqno) {
  bool orig_empty = Empty();
  auto src_it = src.FindGreaterEqSeqno(from_seqno);
  // Allow nonsensical ranges like [1000, 0] which might show up e.g. for
  // an SST file with no entries.
  auto src_it_end =
      to_seqno < from_seqno ? src_it : src.FindGreaterSeqno(to_seqno);
  // To best answer GetProximalTimeBeforeSeqno(from_seqno) we need an entry
  // with a seqno before that (if available)
  if (src_it != src.pairs_.begin()) {
    --src_it;
  }
  assert(src_it <= src_it_end);
  std::copy(src_it, src_it_end, std::back_inserter(pairs_));

  if (!orig_empty || max_time_span_ < UINT64_MAX || capacity_ < UINT64_MAX) {
    enforced_ = false;
  }
}

bool SeqnoToTimeMapping::Append(SequenceNumber seqno, uint64_t time) {
  if (capacity_ == 0) {
    return false;
  }
  bool added = false;
  if (seqno == 0) {
    // skip seq number 0, which may have special meaning, like zeroed out data
    // TODO: consider changing?
  } else if (pairs_.empty()) {
    enforced_ = true;
    pairs_.emplace_back(seqno, time);
    // skip normal enforced check below
    return true;
  } else {
    auto& last = pairs_.back();
    // We can attempt to merge with the last entry if the new entry sorts with
    // it.
    if (last.seqno <= seqno) {
      bool merged = last.Merge({seqno, time});
      if (!merged) {
        if (enforced_ && (seqno <= last.seqno || time <= last.time)) {
          // Out of order append should not happen, except in case of clock
          // reset
          assert(false);
        } else {
          pairs_.emplace_back(seqno, time);
          added = true;
        }
      }
    } else if (!enforced_) {
      // Treat like AddUnenforced and fix up below
      pairs_.emplace_back(seqno, time);
      added = true;
    } else {
      // Out of order append attempted
      assert(false);
    }
  }
  // Similar to Enforce() but not quite
  if (!enforced_) {
    SortAndMerge();
    assert(enforced_);
  }
  EnforceMaxTimeSpan();
  EnforceCapacity(/*strict=*/false);
  return added;
}

void SeqnoToTimeMapping::PrePopulate(SequenceNumber from_seqno,
                                     SequenceNumber to_seqno,
                                     uint64_t from_time, uint64_t to_time) {
  assert(Empty());
  assert(from_seqno > 0);
  assert(to_seqno > from_seqno);
  assert(from_time > kUnknownTimeBeforeAll);
  assert(to_time >= from_time);

  // TODO: smartly limit this to max_capacity_ representative samples
  for (auto i = from_seqno; i <= to_seqno; i++) {
    uint64_t t = from_time + (to_time - from_time) * (i - from_seqno) /
                                 (to_seqno - from_seqno);
    pairs_.emplace_back(i, t);
  }
}

std::string SeqnoToTimeMapping::ToHumanString() const {
  std::string ret;
  for (const auto& seq_time : pairs_) {
    AppendNumberTo(&ret, seq_time.seqno);
    ret.append("->");
    AppendNumberTo(&ret, seq_time.time);
    ret.append(",");
  }
  return ret;
}

Slice PackValueAndWriteTime(const Slice& value, uint64_t unix_write_time,
                            std::string* buf) {
  buf->assign(value.data(), value.size());
  PutFixed64(buf, unix_write_time);
  return Slice(*buf);
}

Slice PackValueAndSeqno(const Slice& value, SequenceNumber seqno,
                        std::string* buf) {
  buf->assign(value.data(), value.size());
  PutFixed64(buf, seqno);
  return Slice(*buf);
}

uint64_t ParsePackedValueForWriteTime(const Slice& value) {
  assert(value.size() >= sizeof(uint64_t));
  Slice write_time_slice(value.data() + value.size() - sizeof(uint64_t),
                         sizeof(uint64_t));
  uint64_t write_time;
  [[maybe_unused]] auto res = GetFixed64(&write_time_slice, &write_time);
  assert(res);
  return write_time;
}

std::tuple<Slice, uint64_t> ParsePackedValueWithWriteTime(const Slice& value) {
  return std::make_tuple(Slice(value.data(), value.size() - sizeof(uint64_t)),
                         ParsePackedValueForWriteTime(value));
}

SequenceNumber ParsePackedValueForSeqno(const Slice& value) {
  assert(value.size() >= sizeof(SequenceNumber));
  Slice seqno_slice(value.data() + value.size() - sizeof(uint64_t),
                    sizeof(uint64_t));
  SequenceNumber seqno;
  [[maybe_unused]] auto res = GetFixed64(&seqno_slice, &seqno);
  assert(res);
  return seqno;
}

std::tuple<Slice, SequenceNumber> ParsePackedValueWithSeqno(
    const Slice& value) {
  return std::make_tuple(
      Slice(value.data(), value.size() - sizeof(SequenceNumber)),
      ParsePackedValueForSeqno(value));
}

Slice ParsePackedValueForValue(const Slice& value) {
  assert(value.size() >= sizeof(uint64_t));
  return Slice(value.data(), value.size() - sizeof(uint64_t));
}
}  // namespace ROCKSDB_NAMESPACE
