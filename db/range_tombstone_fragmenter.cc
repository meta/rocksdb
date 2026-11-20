//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <functional>
#include <set>

#include "db/read_callback.h"
#include "util/autovector.h"
#include "util/kv_map.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

FragmentedRangeTombstoneList::FragmentedRangeTombstoneList(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool for_compaction,
    const std::vector<SequenceNumber>& snapshots,
    const bool tombstone_end_include_ts) {
  if (unfragmented_tombstones == nullptr) {
    return;
  }
  bool is_sorted = true;
  InternalKey pinned_last_start_key;
  Slice last_start_key;
  num_unfragmented_tombstones_ = 0;
  total_tombstone_payload_bytes_ = 0;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next(), num_unfragmented_tombstones_++) {
    total_tombstone_payload_bytes_ += unfragmented_tombstones->key().size() +
                                      unfragmented_tombstones->value().size();
    if (num_unfragmented_tombstones_ > 0 &&
        icmp.Compare(last_start_key, unfragmented_tombstones->key()) > 0) {
      is_sorted = false;
      break;
    }
    if (unfragmented_tombstones->IsKeyPinned()) {
      last_start_key = unfragmented_tombstones->key();
    } else {
      pinned_last_start_key.DecodeFrom(unfragmented_tombstones->key());
      last_start_key = pinned_last_start_key.Encode();
    }
  }

  auto ucmp = icmp.user_comparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  bool pad_min_ts_for_end = ts_sz > 0 && !tombstone_end_include_ts;
  if (is_sorted && !pad_min_ts_for_end) {
    FragmentTombstones(std::move(unfragmented_tombstones), icmp, for_compaction,
                       snapshots);
    return;
  }

  // Sort the tombstones before fragmenting them.
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(num_unfragmented_tombstones_);
  values.reserve(num_unfragmented_tombstones_);
  // Reset the counter to zero for the next iteration over keys.
  total_tombstone_payload_bytes_ = 0;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    total_tombstone_payload_bytes_ += unfragmented_tombstones->key().size() +
                                      unfragmented_tombstones->value().size();
    keys.emplace_back(unfragmented_tombstones->key().data(),
                      unfragmented_tombstones->key().size());
    Slice value = unfragmented_tombstones->value();
    if (pad_min_ts_for_end) {
      AppendKeyWithMinTimestamp(&values.emplace_back(), value, ts_sz);
    } else {
      values.emplace_back(value.data(), value.size());
    }
  }
  num_unfragmented_tombstones_ = keys.size();
  if (pad_min_ts_for_end) {
    total_tombstone_payload_bytes_ += num_unfragmented_tombstones_ * ts_sz;
  }
  // VectorIterator implicitly sorts by key during construction.
  auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                               std::move(values), &icmp);
  FragmentTombstones(std::move(iter), icmp, for_compaction, snapshots);
}

void FragmentedRangeTombstoneList::FragmentTombstones(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool for_compaction,
    const std::vector<SequenceNumber>& snapshots) {
  Slice cur_start_key(nullptr, 0);
  auto cmp = ParsedInternalKeyComparator(&icmp);

  // Stores the end keys and sequence numbers of range tombstones with a start
  // key less than or equal to cur_start_key. Provides an ordering by end key
  // for use in flush_current_tombstones.
  std::set<ParsedInternalKey, ParsedInternalKeyComparator> cur_end_keys(cmp);

  size_t ts_sz = icmp.user_comparator()->timestamp_size();
  // Given the next start key in unfragmented_tombstones,
  // flush_current_tombstones writes every tombstone fragment that starts
  // and ends with a key before next_start_key, and starts with a key greater
  // than or equal to cur_start_key.
  auto flush_current_tombstones = [&](const Slice& next_start_key) {
    auto it = cur_end_keys.begin();
    bool reached_next_start_key = false;
    for (; it != cur_end_keys.end() && !reached_next_start_key; ++it) {
      Slice cur_end_key = it->user_key;
      if (icmp.user_comparator()->CompareWithoutTimestamp(cur_start_key,
                                                          cur_end_key) == 0) {
        // Empty tombstone.
        continue;
      }
      if (icmp.user_comparator()->CompareWithoutTimestamp(next_start_key,
                                                          cur_end_key) <= 0) {
        // All the end keys in [it, cur_end_keys.end()) are after
        // next_start_key, so the tombstones they represent can be used in
        // fragments that start with keys greater than or equal to
        // next_start_key. However, the end keys we already passed will not be
        // used in any more tombstone fragments.
        //
        // Remove the fully fragmented tombstones and stop iteration after a
        // final round of flushing to preserve the tombstones we can create more
        // fragments from.
        reached_next_start_key = true;
        cur_end_keys.erase(cur_end_keys.begin(), it);
        cur_end_key = next_start_key;
      }

      // Flush a range tombstone fragment [cur_start_key, cur_end_key), which
      // should not overlap with the last-flushed tombstone fragment.
      assert(tombstones_.empty() ||
             icmp.user_comparator()->CompareWithoutTimestamp(
                 tombstones_.back().end_key, cur_start_key) <= 0);

      // Sort the sequence numbers of the tombstones being fragmented in
      // descending order, and then flush them in that order.
      autovector<SequenceNumber> seqnums_to_flush;
      autovector<Slice> timestamps_to_flush;
      for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
        seqnums_to_flush.push_back(flush_it->sequence);
        if (ts_sz) {
          timestamps_to_flush.push_back(
              ExtractTimestampFromUserKey(flush_it->user_key, ts_sz));
        }
      }
      // TODO: bind the two sorting together to be more efficient
      std::sort(seqnums_to_flush.begin(), seqnums_to_flush.end(),
                std::greater<SequenceNumber>());
      if (ts_sz) {
        std::sort(timestamps_to_flush.begin(), timestamps_to_flush.end(),
                  [icmp](const Slice& ts1, const Slice& ts2) {
                    return icmp.user_comparator()->CompareTimestamp(ts1, ts2) >
                           0;
                  });
      }

      size_t start_idx = tombstone_seqs_.size();
      size_t end_idx = start_idx + seqnums_to_flush.size();

      // If user-defined timestamp is enabled, we should not drop tombstones
      // from any snapshot stripe. Garbage collection of range tombstones
      // happens in CompactionOutputs::AddRangeDels().
      if (for_compaction && ts_sz == 0) {
        // Drop all tombstone seqnums that are not preserved by a snapshot.
        SequenceNumber next_snapshot = kMaxSequenceNumber;
        for (auto seq : seqnums_to_flush) {
          if (seq <= next_snapshot) {
            // This seqnum is visible by a lower snapshot.
            tombstone_seqs_.push_back(seq);
            auto upper_bound_it =
                std::lower_bound(snapshots.begin(), snapshots.end(), seq);
            if (upper_bound_it == snapshots.begin()) {
              // This seqnum is the topmost one visible by the earliest
              // snapshot. None of the seqnums below it will be visible, so we
              // can skip them.
              break;
            }
            next_snapshot = *std::prev(upper_bound_it);
          }
        }
        end_idx = tombstone_seqs_.size();
      } else {
        // The fragmentation is being done for reads, so preserve all seqnums.
        tombstone_seqs_.insert(tombstone_seqs_.end(), seqnums_to_flush.begin(),
                               seqnums_to_flush.end());
        if (ts_sz) {
          tombstone_timestamps_.insert(tombstone_timestamps_.end(),
                                       timestamps_to_flush.begin(),
                                       timestamps_to_flush.end());
        }
      }

      assert(start_idx < end_idx);
      if (ts_sz) {
        std::string start_key_with_max_ts;
        AppendUserKeyWithMaxTimestamp(&start_key_with_max_ts, cur_start_key,
                                      ts_sz);
        pinned_slices_.emplace_back(std::move(start_key_with_max_ts));
        Slice start_key = pinned_slices_.back();

        std::string end_key_with_max_ts;
        AppendUserKeyWithMaxTimestamp(&end_key_with_max_ts, cur_end_key, ts_sz);
        pinned_slices_.emplace_back(std::move(end_key_with_max_ts));
        Slice end_key = pinned_slices_.back();

        // RangeTombstoneStack expects start_key and end_key to have max
        // timestamp.
        tombstones_.emplace_back(start_key, end_key, start_idx, end_idx);
      } else {
        tombstones_.emplace_back(cur_start_key, cur_end_key, start_idx,
                                 end_idx);
      }

      cur_start_key = cur_end_key;
    }
    if (!reached_next_start_key) {
      // There is a gap between the last flushed tombstone fragment and
      // the next tombstone's start key. Remove all the end keys in
      // the working set, since we have fully fragmented their corresponding
      // tombstones.
      cur_end_keys.clear();
    }
    cur_start_key = next_start_key;
  };

  pinned_iters_mgr_.StartPinning();

  bool no_tombstones = true;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    const Slice& ikey = unfragmented_tombstones->key();
    Slice tombstone_start_key = ExtractUserKey(ikey);
    SequenceNumber tombstone_seq = GetInternalKeySeqno(ikey);
    if (!unfragmented_tombstones->IsKeyPinned()) {
      pinned_slices_.emplace_back(tombstone_start_key.data(),
                                  tombstone_start_key.size());
      tombstone_start_key = pinned_slices_.back();
    }
    no_tombstones = false;

    Slice tombstone_end_key = unfragmented_tombstones->value();
    if (!unfragmented_tombstones->IsValuePinned()) {
      pinned_slices_.emplace_back(tombstone_end_key.data(),
                                  tombstone_end_key.size());
      tombstone_end_key = pinned_slices_.back();
    }
    if (!cur_end_keys.empty() &&
        icmp.user_comparator()->CompareWithoutTimestamp(
            cur_start_key, tombstone_start_key) != 0) {
      // The start key has changed. Flush all tombstones that start before
      // this new start key.
      flush_current_tombstones(tombstone_start_key);
    }
    cur_start_key = tombstone_start_key;

    cur_end_keys.emplace(tombstone_end_key, tombstone_seq, kTypeRangeDeletion);
  }
  if (!cur_end_keys.empty()) {
    ParsedInternalKey last_end_key = *std::prev(cur_end_keys.end());
    flush_current_tombstones(last_end_key.user_key);
  }

  if (!no_tombstones) {
    pinned_iters_mgr_.PinIterator(unfragmented_tombstones.release(),
                                  false /* arena */);
  }
}

bool FragmentedRangeTombstoneList::ContainsRange(SequenceNumber lower,
                                                 SequenceNumber upper) {
  std::call_once(seq_set_init_once_flag_, [this]() {
    for (auto s : tombstone_seqs_) {
      seq_set_.insert(s);
    }
  });
  auto seq_it = seq_set_.lower_bound(lower);
  return seq_it != seq_set_.end() && *seq_it <= upper;
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    FragmentedRangeTombstoneList* tombstones, const InternalKeyComparator& icmp,
    SequenceNumber _upper_bound, const Slice* ts_upper_bound,
    SequenceNumber _lower_bound)
    : tombstone_start_cmp_(icmp.user_comparator()),
      tombstone_end_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_(tombstones),
      upper_bound_(_upper_bound),
      lower_bound_(_lower_bound),
      ts_upper_bound_(ts_upper_bound) {
  assert(tombstones_ != nullptr);
  Invalidate();
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const std::shared_ptr<FragmentedRangeTombstoneList>& tombstones,
    const InternalKeyComparator& icmp, SequenceNumber _upper_bound,
    const Slice* ts_upper_bound, SequenceNumber _lower_bound)
    : tombstone_start_cmp_(icmp.user_comparator()),
      tombstone_end_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_ref_(tombstones),
      tombstones_(tombstones_ref_.get()),
      upper_bound_(_upper_bound),
      lower_bound_(_lower_bound),
      ts_upper_bound_(ts_upper_bound) {
  assert(tombstones_ != nullptr);
  Invalidate();
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const std::shared_ptr<FragmentedRangeTombstoneListCache>& tombstones_cache,
    const InternalKeyComparator& icmp, SequenceNumber _upper_bound,
    const Slice* ts_upper_bound, SequenceNumber _lower_bound)
    : tombstone_start_cmp_(icmp.user_comparator()),
      tombstone_end_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_cache_ref_(tombstones_cache),
      tombstones_(tombstones_cache_ref_->tombstones.get()),
      upper_bound_(_upper_bound),
      lower_bound_(_lower_bound) {
  assert(tombstones_ != nullptr);
  if (!ts_upper_bound || ts_upper_bound->empty()) {
    ts_upper_bound_ = nullptr;
  } else {
    ts_upper_bound_ = ts_upper_bound;
  }
  Invalidate();
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_->begin();
  seq_pos_ = tombstones_->seq_begin();
}

void FragmentedRangeTombstoneIterator::SeekToTopFirst() {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = tombstones_->begin();
  SetMaxVisibleSeqAndTimestamp();
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekToLast() {
  pos_ = std::prev(tombstones_->end());
  seq_pos_ = std::prev(tombstones_->seq_end());
}

void FragmentedRangeTombstoneIterator::SeekToTopLast() {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = std::prev(tombstones_->end());
  SetMaxVisibleSeqAndTimestamp();
  ScanBackwardToVisibleTombstone();
}

// @param `target` is a user key, with timestamp if user-defined timestamp is
// enabled.
void FragmentedRangeTombstoneIterator::Seek(const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  SeekToCoveringTombstone(target);
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekForPrev(const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  SeekForPrevToCoveringTombstone(target);
  ScanBackwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekToCoveringTombstone(
    const Slice& target) {
  pos_ = std::upper_bound(tombstones_->begin(), tombstones_->end(), target,
                          tombstone_end_cmp_);
  if (pos_ == tombstones_->end()) {
    // All tombstones end before target.
    seq_pos_ = tombstones_->seq_end();
    return;
  }
  SetMaxVisibleSeqAndTimestamp();
}

void FragmentedRangeTombstoneIterator::SeekForPrevToCoveringTombstone(
    const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = std::upper_bound(tombstones_->begin(), tombstones_->end(), target,
                          tombstone_start_cmp_);
  if (pos_ == tombstones_->begin()) {
    // All tombstones start after target.
    Invalidate();
    return;
  }
  --pos_;
  SetMaxVisibleSeqAndTimestamp();
}

void FragmentedRangeTombstoneIterator::ScanForwardToVisibleTombstone() {
  while (pos_ != tombstones_->end() &&
         (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx) ||
          *seq_pos_ < lower_bound_)) {
    ++pos_;
    if (pos_ == tombstones_->end()) {
      Invalidate();
      return;
    }
    SetMaxVisibleSeqAndTimestamp();
  }
}

void FragmentedRangeTombstoneIterator::ScanBackwardToVisibleTombstone() {
  while (pos_ != tombstones_->end() &&
         (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx) ||
          *seq_pos_ < lower_bound_)) {
    if (pos_ == tombstones_->begin()) {
      Invalidate();
      return;
    }
    --pos_;
    SetMaxVisibleSeqAndTimestamp();
  }
}

void FragmentedRangeTombstoneIterator::Next() {
  ++seq_pos_;
  if (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx)) {
    ++pos_;
  }
}

void FragmentedRangeTombstoneIterator::TopNext() {
  ++pos_;
  if (pos_ == tombstones_->end()) {
    return;
  }
  SetMaxVisibleSeqAndTimestamp();
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::Prev() {
  if (seq_pos_ == tombstones_->seq_begin()) {
    Invalidate();
    return;
  }
  --seq_pos_;
  if (pos_ == tombstones_->end() ||
      seq_pos_ == tombstones_->seq_iter(pos_->seq_start_idx - 1)) {
    --pos_;
  }
}

void FragmentedRangeTombstoneIterator::TopPrev() {
  if (pos_ == tombstones_->begin()) {
    Invalidate();
    return;
  }
  --pos_;
  SetMaxVisibleSeqAndTimestamp();
  ScanBackwardToVisibleTombstone();
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return tombstones_ != nullptr && pos_ != tombstones_->end();
}

SequenceNumber FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum(
    const Slice& target_user_key) {
  SeekToCoveringTombstone(target_user_key);
  return ValidPos() && ucmp_->CompareWithoutTimestamp(start_key(),
                                                      target_user_key) <= 0
             ? seq()
             : 0;
}

SequenceNumber FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum(
    const Slice& target_user_key, ReadCallback* callback) {
  assert(callback != nullptr);
  SeekToCoveringTombstone(target_user_key);
  if (!ValidPos() ||
      ucmp_->CompareWithoutTimestamp(start_key(), target_user_key) > 0) {
    return 0;
  }

  const auto seq_end = tombstones_->seq_iter(pos_->seq_end_idx);
  // Sequence numbers are ordered newest to oldest and seq_pos_ is already
  // bounded by upper_bound_. Return the first tombstone the metadata callback
  // considers newer than the read snapshot.
  while (seq_pos_ != seq_end && *seq_pos_ >= lower_bound_) {
    const SequenceNumber candidate_seq = *seq_pos_;
    if (callback->IsNewerVisibleForMetadataRead(candidate_seq)) {
      return candidate_seq;
    }
    ++seq_pos_;
  }
  return 0;
}

std::map<SequenceNumber, std::unique_ptr<FragmentedRangeTombstoneIterator>>
FragmentedRangeTombstoneIterator::SplitBySnapshot(
    const std::vector<SequenceNumber>& snapshots) {
  std::map<SequenceNumber, std::unique_ptr<FragmentedRangeTombstoneIterator>>
      splits;
  SequenceNumber lower = 0;
  SequenceNumber upper;
  // Safe to dereference icmp_ below: SplitBySnapshot is construction-time work
  // (called synchronously from the range-del aggregator while the reader that
  // owns *icmp_ is alive). See the icmp_ note in the header.
  for (size_t i = 0; i <= snapshots.size(); i++) {
    if (i >= snapshots.size()) {
      upper = kMaxSequenceNumber;
    } else {
      upper = snapshots[i];
    }
    if (tombstones_->ContainsRange(lower, upper)) {
      splits.emplace(upper,
                     std::make_unique<FragmentedRangeTombstoneIterator>(
                         tombstones_, *icmp_, upper, ts_upper_bound_, lower));
    }
    lower = upper + 1;
  }
  return splits;
}

}  // namespace ROCKSDB_NAMESPACE
