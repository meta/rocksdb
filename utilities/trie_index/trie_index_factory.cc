//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie_index_factory.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

namespace {
// Separators per bucket in format 2. Larger buckets make the index smaller
// and Seek slower; 8 was the best tradeoff on the key sets measured. The
// value is stored in each index, so it can change without a format change.
constexpr uint32_t kTrieV2BucketSize = 8;

std::unordered_map<std::string, OptionTypeInfo> trie_index_factory_type_info = {
    {"format_version",
     {0, OptionType::kUInt32T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};
}  // namespace

int RegisterBuiltinTrieIndexFactory(ObjectLibrary& library,
                                    const std::string& /*arg*/) {
  library.AddFactory<IndexFactory>(
      TrieIndexFactory::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<IndexFactory>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new TrieIndexFactory());
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

// ============================================================================
// TrieIndexBuilder
// ============================================================================

TrieIndexBuilder::TrieIndexBuilder(const Comparator* comparator,
                                   uint32_t format_version)
    : comparator_(comparator),
      format_version_(format_version),
      finished_(false) {}

Slice TrieIndexBuilder::AddIndexEntry(const Slice& last_key_in_current_block,
                                      const Slice* first_key_in_next_block,
                                      const BlockHandle& block_handle,
                                      std::string* separator_scratch,
                                      const IndexEntryContext& context) {
  uint64_t last_key_tag = context.last_key_tag;

  // Compute a short separator between the two user keys using the
  // comparator. FindShortestSeparator takes `*start` as both input and output:
  //   input:  *start == last_key_in_current_block
  //   output: *start modified to shortest string in [start, limit)
  // If first_key_in_next_block is nullptr, this is the last block -- use a
  // short successor of the last key.
  Slice separator;
  // True when last_key and first_key_in_next_block are the same user key
  // (same-user-key block boundary). Computed once and reused below for
  // both the sticky flag and the per-entry seqno decision.
  bool same_user_key = false;
  if (first_key_in_next_block != nullptr) {
    same_user_key = comparator_->Compare(last_key_in_current_block,
                                         *first_key_in_next_block) == 0;

    *separator_scratch = last_key_in_current_block.ToString();
    comparator_->FindShortestSeparator(separator_scratch,
                                       *first_key_in_next_block);
    separator = Slice(*separator_scratch);

    // Detect same-user-key block boundary: if the two user keys are
    // identical, FindShortestSeparator returns the same key for both sides,
    // making it impossible to distinguish the two blocks. Such an entry
    // carries a real tag rather than the 0 sentinel.

    // Edge case: FindShortestSeparator may fail to shorten the key even when
    // the user keys are different. Example: FindShortestSeparator("abc","abd")
    // returns "abc" unchanged because incrementing 'c' would yield "abd" which
    // is not < limit. When the resulting separator matches the previous entry's
    // separator, the blocks will be grouped into the same run in Finish().
    // We must mark this as a same-user-key boundary so it gets a real seqno
    // rather than kMaxSequenceNumber (which would trigger the overflow block
    // assertion in Finish()).
    if (!same_user_key && !buffered_entries_.empty() &&
        buffered_entries_.back().separator_key == *separator_scratch) {
      same_user_key = true;
    }
  } else {
    // Last block: use the last key itself as the separator, NOT a shortened
    // successor. This matches the standard ShortenedIndexBuilder behavior
    // (see index_builder.h GetSeparatorWithSeq lines 278-286): it only calls
    // FindShortInternalKeySuccessor when shortening_mode is
    // kShortenSeparatorsAndSuccessor, which is not the default. With the
    // default kShortenSeparators, the last block's separator is simply
    // last_key_in_current_block.
    //
    // Why this matters: FindShortSuccessor can widen the key range. For
    // example, if the actual last key is "9\xff\xff", FindShortSuccessor
    // produces ":" (0x3A). The trie would then claim to cover keys up to
    // ":", but the data block only contains keys up to "9\xff\xff". A seek
    // targeting a key in that gap (e.g., "9\xff\xff\x01") would find a
    // block via the trie that contains no matching data, causing iterator
    // desynchronization -- the trie index returns a valid block while the
    // standard index correctly reports no match.
    separator = last_key_in_current_block;

    // Edge case: if this last block's separator matches the previous entry's
    // separator, they share the same user key (same-user-key run boundary).
    if (!buffered_entries_.empty() &&
        comparator_->Compare(buffered_entries_.back().separator_key,
                             separator) == 0) {
      same_user_key = true;
    }
  }

  // Buffer the entry for deferred trie construction in Finish().
  // We buffer rather than adding to the trie immediately because the
  // all-or-nothing seqno encoding decision is made at Finish() time.
  TrieBlockHandle handle;
  handle.offset = block_handle.offset;
  handle.size = block_handle.size;

  BufferedEntry entry;
  entry.separator_key = separator.ToString();
  if (same_user_key) {
    // Same-user-key boundary: store the real tag for correct block
    // selection within the overflow run.
    entry.tag = last_key_tag;
  } else if (first_key_in_next_block == nullptr) {
    // Last block: store the real tag. The standard index stores the full
    // internal key (user key + seqno) as the last block's separator. The
    // real tag ensures the post-seek correction correctly handles seeks
    // where the target user key matches but the seqno differs.
    entry.tag = last_key_tag;
  } else {
    // Non-boundary separator between blocks with different user keys.
    // Store 0 (sentinel meaning "no seqno correction needed"). When the
    // standard index has index_key_is_user_key=true, it compares user keys
    // only and always stays on equal user keys. The trie matches this by
    // ensuring target_tag < 0 is always false.
    entry.tag = 0;
  }

  entry.handle = handle;
  has_unsupported_handle_ |=
      handle.offset > UINT32_MAX || handle.size > UINT32_MAX;
  TrackHandle(handle);
  const Slice previous_separator =
      buffered_entries_.empty() ? Slice()
                                : Slice(buffered_entries_.back().separator_key);
  UpdateSizeEstimate(entry.separator_key, previous_separator, entry.tag != 0);
  buffered_entries_.push_back(std::move(entry));

  return separator;
}

void TrieIndexBuilder::OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                                  const Slice& /*value*/) {
  // No-op: the trie is built from separator keys in AddIndexEntry(), not
  // from individual key-value pairs.
}

Status TrieIndexBuilder::Finish(Slice* index_contents) {
  if (finished_) {
    return Status::InvalidArgument("TrieIndexBuilder::Finish called twice");
  }
  finished_ = true;
  if (has_unprepared_entry_) {
    return Status::InvalidArgument(
        "TrieIndexBuilder::FinishAddEntry called without a matching "
        "PrepareAddEntry");
  }
  if (format_version_ == kTrieV2FormatVersion) {
    return FinishV2(index_contents);
  }
  if (has_unsupported_handle_) {
    return Status::NotSupported(
        "Trie index block offset or size exceeds the 32-bit encoding limit");
  }

  // Every staged entry needs the seqno side-table so the post-seek
  // correction can disambiguate the last block and any same-user-key run,
  // so this is on whenever the trie is non-empty. It costs 8 bytes per leaf.
  bool use_seqno = !buffered_entries_.empty();
  trie_builder_.SetHasSeqnoEncoding(use_seqno);

  if (use_seqno) {
    // Feed de-duplicated separators to the trie with seqno side-table metadata.
    // Consecutive identical separators form a "run" -- only the first
    // occurrence goes into the trie (as the primary block). The remaining
    // blocks in the run are stored as overflow blocks in the side-table.
    //
    // For non-boundary separators (different user keys), the tag is 0
    // (sentinel meaning "no seqno correction needed"), matching the standard
    // index's user-key-only comparison mode. For the last block, the real
    // tag is stored to match the standard index's full internal key behavior.
    size_t i = 0;
    while (i < buffered_entries_.size()) {
      const auto& entry = buffered_entries_[i];

      // Count how many consecutive entries share this separator key.
      size_t run_start = i;
      size_t run_end = i + 1;
      while (run_end < buffered_entries_.size() &&
             buffered_entries_[run_end].separator_key == entry.separator_key) {
        run_end++;
      }
      uint32_t block_count = static_cast<uint32_t>(run_end - run_start);

      // Non-boundary entries have tag=0 (sentinel meaning "no seqno
      // correction needed"). Same-user-key boundary and last-block entries
      // have real tags. The trie builder stores these directly in the seqno
      // side-table.
      //
      // For boundary separators (same user key), store the actual packed
      // tag for correct seqno-based block selection.
      uint64_t tag = entry.tag;

      // Add the primary (first) block for this separator.
      trie_builder_.AddKeyWithSeqno(Slice(entry.separator_key), entry.handle,
                                    tag, block_count);

      // Add overflow blocks (2nd, 3rd, ... in the run).
      // Overflow blocks only exist within same-key runs, so their packed
      // tags come from last_key_tag in AddIndexEntry.
      // The tag may be 0 when bottommost compaction zeroes all sequence
      // numbers -- this is valid; see AddOverflowBlock comment.
      for (size_t j = run_start + 1; j < run_end; j++) {
        assert(buffered_entries_[j].tag !=
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
        trie_builder_.AddOverflowBlock(buffered_entries_[j].handle,
                                       buffered_entries_[j].tag);
      }

      i = run_end;
    }
  }

  // Release buffered entries -- no longer needed after feeding to the trie.
  buffered_entries_.clear();
  buffered_entries_.shrink_to_fit();

  // Always finish the trie builder, even with 0 keys -- this produces a valid
  // serialized trie that can be parsed by NewReader. Without this, an empty
  // Slice would be returned, causing InitFromData to fail with "data too short
  // for header".
  trie_builder_.Finish();
  *index_contents = trie_builder_.GetSerializedData();
  return Status::OK();
}

Status TrieIndexBuilder::FinishV2(Slice* index_contents) {
  // The tags have the meaning TrieV2Builder::AddBlock() expects: 0 for a
  // block found by user key alone, otherwise the tag of the block's last key.
  TrieV2Builder builder(kTrieV2BucketSize);
  for (const BufferedEntry& entry : buffered_entries_) {
    builder.AddBlock(entry.separator_key, entry.handle, entry.tag);
  }
  buffered_entries_.clear();
  buffered_entries_.shrink_to_fit();
  Status s = builder.Finish(&v2_data_);
  if (s.ok()) {
    *index_contents = v2_data_;
  }
  return s;
}

// ============================================================================
// TrieIndexIterator
// ============================================================================

uint64_t TrieIndexBuilder::EstimatedSize() const {
  if (format_version_ == kTrieV2FormatVersion) {
    return finished_ ? v2_data_.size() : EstimatedV2Size();
  }
  if (finished_) {
    return trie_builder_.GetSerializedData().size();
  }
  // Dense/sparse labels, bitvectors, child positions, and capped chain metadata
  // use at most 12 bytes per distinct trie edge. Each handle and seqno record
  // needs at most 20 bytes. Reserve 1 KiB for headers and alignment. Shared
  // prefixes contribute only once, just as in LoudsTrieBuilder::Finish().
  return 1024 + estimated_trie_edges_ * 12 + estimated_num_entries_ * 20;
}

size_t TrieIndexBuilder::UpdateSizeEstimate(const Slice& separator,
                                            const Slice& previous_separator,
                                            bool may_have_tag) {
  const size_t shared_prefix = previous_separator.difference_offset(separator);
  const size_t drop = previous_separator.size() - shared_prefix;
  const size_t add = separator.size() - shared_prefix;
  estimated_trie_edges_ += add;
  if (format_version_ == kTrieV2FormatVersion) {
    // Matches the entry encoding in trie_v2.h. The first separator and the
    // bucket heads are charged too, which only overestimates.
    estimated_v2_bucket_bytes_ +=
        add + (drop < 15 && add < 15
                   ? 1
                   : 1 + VarintLength(drop) + VarintLength(add));
    estimated_v2_max_key_len_ =
        std::max<uint64_t>(estimated_v2_max_key_len_, separator.size());
    if (estimated_num_entries_ > 0 && drop == 0 && add == 0) {
      ++estimated_v2_duplicates_;
      may_have_tag = true;
    }
    if (may_have_tag) {
      ++estimated_v2_tagged_;
    }
  }
  ++estimated_num_entries_;
  return shared_prefix;
}

void TrieIndexBuilder::TrackHandle(const TrieBlockHandle& handle) {
  if (format_version_ != kTrieV2FormatVersion) {
    return;
  }
  max_block_size_.StoreRelaxed(
      std::max(max_block_size_.LoadRelaxed(), handle.size));
  if (!buffered_entries_.empty()) {
    const uint64_t step = handle.offset - previous_handle_.offset;
    max_block_step_.StoreRelaxed(std::max(max_block_step_.LoadRelaxed(), step));
    // Mirrors the derived-size rule in TrieV2Builder::Finish(). Wrapping
    // arithmetic only matters for blocks that already make sizes explicit.
    const uint64_t previous_end =
        previous_handle_.offset + previous_handle_.size;
    const uint64_t gap = handle.offset - previous_end;
    if (handle.offset < previous_end ||
        previous_end < previous_handle_.offset ||
        (buffered_entries_.size() > 1 && gap != block_gap_)) {
      irregular_block_gaps_.StoreRelaxed(true);
    }
    block_gap_ = gap;
  }
  previous_handle_ = handle;
}

uint64_t TrieIndexBuilder::EstimatedV2Size() const {
  // Upper bound on every section of the format described in trie_v2.h.
  auto column = [](uint64_t count, uint64_t max_range) {
    // The residuals of a group never exceed the range of its values.
    const uint64_t width = max_range == 0 ? 0 : FloorLog2(max_range) + 1;
    return 16 + 16 * ((count + 63) / 64) + (count * width + 7) / 8 + 8;
  };
  const uint64_t n = estimated_num_entries_;
  const uint64_t num_buckets = (n + kTrieV2BucketSize - 1) / kTrieV2BucketSize;
  // Path and label bytes never exceed the front-coded suffix bytes. A trie
  // with b terminal nodes has fewer than 2b nodes, each costing at most 17
  // bytes besides its path: header, path length, bucket id, child count,
  // the pointer to it, and its share of a bitmap.
  const uint64_t trie = estimated_trie_edges_ + 17 * 2 * num_buckets;
  // Offsets and sizes come in block order, so a group of 64 offsets spans at
  // most 64 steps (the last one to the sentinel), and a size column value is
  // at most the largest size.
  const uint64_t max_step = max_block_step_.LoadRelaxed();
  const uint64_t max_size = max_block_size_.LoadRelaxed();
  uint64_t size =
      kTrieV2HeaderSize + estimated_v2_max_key_len_ + trie +
      column(num_buckets + 1, estimated_v2_bucket_bytes_) +
      estimated_v2_bucket_bytes_ + column(n + 1, 64 * (max_step + max_size)) +
      8 + 16 * estimated_v2_duplicates_ + 8 + 16 * estimated_v2_tagged_ + 16;
  if (irregular_block_gaps_.LoadRelaxed()) {
    size += column(n, max_size);
  }
  return size;
}

// ---------------------------------------------------------------------------
// Parallel compression protocol
// ---------------------------------------------------------------------------

std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry>
TrieIndexBuilder::CreatePreparedAddEntry() {
  return std::make_unique<PreparedTrieEntry>();
}

void TrieIndexBuilder::PrepareAddEntry(const Slice& last_key_in_current_block,
                                       const Slice* first_key_in_next_block,
                                       const IndexEntryContext& context,
                                       PreparedAddEntry* out) {
  auto* p = static_cast_with_check<PreparedTrieEntry>(out);
  p->valid = false;
  p->same_user_key_initial = false;
  p->has_next_block = (first_key_in_next_block != nullptr);
  p->last_key_tag = context.last_key_tag;

  if (first_key_in_next_block != nullptr) {
    // Same-user-key boundary detection from inputs alone. The buffer-
    // state-dependent half of the check runs in FinishAddEntry, where
    // back() is the immediately preceding committed entry.
    p->same_user_key_initial =
        comparator_->Compare(last_key_in_current_block,
                             *first_key_in_next_block) == 0;

    p->separator_key = last_key_in_current_block.ToString();
    comparator_->FindShortestSeparator(&p->separator_key,
                                       *first_key_in_next_block);
  } else {
    // Last block: store the last key itself (no shortening).
    p->separator_key = last_key_in_current_block.ToString();
  }

  p->valid = true;
  // A separator equal to the previous one gets a tag in FinishAddEntry, which
  // UpdateSizeEstimate detects.
  const size_t shared_prefix =
      UpdateSizeEstimate(p->separator_key, previous_prepared_separator_,
                         p->same_user_key_initial || !p->has_next_block);
  // The writer can move the prepared key as soon as this callback returns.
  // Keep one emit-owned copy, reusing the prefix bytes already stored here.
  previous_prepared_separator_.replace(shared_prefix, std::string::npos,
                                       p->separator_key.data() + shared_prefix,
                                       p->separator_key.size() - shared_prefix);
}

void TrieIndexBuilder::FinishAddEntry(const BlockHandle& block_handle,
                                      PreparedAddEntry* entry,
                                      std::string* /*separator_scratch*/,
                                      bool /*skip_delta_encoding*/) {
  auto* p = static_cast_with_check<PreparedTrieEntry>(entry);
  if (!p->valid) {
    // Nothing was prepared into this entry, or it was already finished, so
    // there is no separator for this data block. Skipping the block would make
    // its keys unreachable through the index, so fail the build in Finish().
    has_unprepared_entry_ = true;
    return;
  }

  // Recheck the buffer-state-dependent branch of the same-user-key
  // detection. Now that we run on the BG writer thread in commit order,
  // buffered_entries_.back() is the immediately preceding block's entry.
  //
  // The branch structure mirrors the serial AddIndexEntry exactly, including
  // which comparison primitive each branch uses, so the two paths cannot
  // diverge on a comparator where byte equality and Compare() == 0 disagree.
  bool same_user_key = p->same_user_key_initial;
  if (p->has_next_block) {
    // Non-last block: a separator FindShortestSeparator could not shorten can
    // still collide with the previous entry's separator.
    if (!same_user_key && !buffered_entries_.empty() &&
        buffered_entries_.back().separator_key == p->separator_key) {
      same_user_key = true;
    }
  } else if (!buffered_entries_.empty() &&
             comparator_->Compare(buffered_entries_.back().separator_key,
                                  p->separator_key) == 0) {
    // Last block: its separator matching the previous entry's means they
    // share a user key.
    same_user_key = true;
  }

  BufferedEntry be;
  be.separator_key = std::move(p->separator_key);
  if (same_user_key) {
    // Same-user-key run: real tag distinguishes blocks within the run.
    be.tag = p->last_key_tag;
  } else if (!p->has_next_block) {
    // Last block: real tag covers any post-seek correction by seqno.
    be.tag = p->last_key_tag;
  } else {
    // Distinct-user-key separator: 0 sentinel -- no seqno correction
    // needed because the separator already disambiguates blocks.
    be.tag = 0;
  }
  be.handle.offset = block_handle.offset;
  be.handle.size = block_handle.size;
  has_unsupported_handle_ |=
      be.handle.offset > UINT32_MAX || be.handle.size > UINT32_MAX;
  TrackHandle(be.handle);

  buffered_entries_.push_back(std::move(be));

  // Reset valid so the same prepared slot can be reused (the table
  // builder rotates BlockReps in a ring buffer).
  p->valid = false;
}

TrieIndexIterator::TrieIndexIterator(const LoudsTrie* trie,
                                     const Comparator* comparator,
                                     bool has_seqno_encoding)
    : comparator_(comparator),
      iter_(trie),
      trie_(trie),
      has_seqno_encoding_(has_seqno_encoding),
      overflow_run_index_(0),
      overflow_run_size_(1),
      overflow_base_idx_(0) {}

void TrieIndexIterator::Prepare(const ScanOptions scan_opts[],
                                size_t num_opts) {
  bounds_.Prepare(scan_opts, num_opts);
}

Status TrieIndexIterator::SeekToFirstAndGetResult(IterateResult* result) {
  ResetOverflowState();

  if (!iter_.SeekToFirst()) {
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  CopyTrieKeyToResult(result);
  SetupOverflowForCurrentLeaf(/*position_at_last=*/false);
  result->bound_check_result = IterBoundCheck::kInbound;
  return Status::OK();
}

Status TrieIndexIterator::SeekToLastAndGetResult(IterateResult* result) {
  ResetOverflowState();

  if (!iter_.SeekToLast()) {
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  CopyTrieKeyToResult(result);
  SetupOverflowForCurrentLeaf(/*position_at_last=*/true);
  result->bound_check_result = IterBoundCheck::kInbound;
  return Status::OK();
}

Status TrieIndexIterator::PrevAndGetResult(IterateResult* result) {
  // Upper-bound checks are based on forward movement. While moving backward,
  // DBIter's lower-bound handling decides when to stop.
  // Overflow fast path: key doesn't change within the same run.
  if (overflow_run_index_ > 0) {
    overflow_run_index_--;
    result->key = Slice(current_key_scratch_);
    result->bound_check_result = IterBoundCheck::kInbound;
    return Status::OK();
  }

  // Non-overflow: current_key_scratch_ is about to be overwritten, so swap
  // into prev_key_scratch_ in O(1) instead of copying.
  std::swap(prev_key_scratch_, current_key_scratch_);

  // Move to the previous trie leaf.
  ResetOverflowState();

  if (!iter_.Prev()) {
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  CopyTrieKeyToResult(result);
  SetupOverflowForCurrentLeaf(/*position_at_last=*/true);
  result->bound_check_result = IterBoundCheck::kInbound;
  return Status::OK();
}

Status TrieIndexIterator::SeekAndGetResult(const Slice& target,
                                           IterateResult* result,
                                           const SeekContext& context) {
  uint64_t target_packed = context.target_tag;

  bounds_.SkipScansBefore(target, comparator_);

  ResetOverflowState();

  // Always seek with user key only -- the trie stores user-key separators.
  // When seqno encoding is active, post-seek correction handles the seqno.
  if (!iter_.Seek(target)) {
    // No leaf has a key >= target: the target is past all blocks in this SST.
    // Return kUnknown (not kOutOfBound) because exhausting this SST's trie
    // says nothing about the upper bound -- the next SST on the level may
    // still contain in-bound keys. kOutOfBound would cause LevelIterator to
    // stop scanning the level prematurely.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  // Set the result key (always a user key, no suffix stripping needed).
  // Reuse current_key_scratch_ capacity -- avoids heap allocation after warmup.
  {
    Slice trie_key = iter_.Key();
    current_key_scratch_.assign(trie_key.data(), trie_key.size());
    result->key = Slice(current_key_scratch_);
  }

  // ---- Post-seek correction for seqno side-table ----
  //
  // When has_seqno_encoding_ is true, the leaf we landed on has a seqno
  // side-table entry. We use it to determine if this is the right block
  // for the given (target, target_packed).
  //
  // For same-user-key boundaries: the tag determines which block
  // within a run of same-key blocks is correct. If target_packed < leaf_packed,
  // advance through overflow blocks.
  //
  // For non-boundary separators: leaf_seqno is 0. The comparison
  // target_packed < 0 is always false, so no advancement occurs. This matches
  // the standard index's index_key_is_user_key=true mode where equal user
  // keys always match without seqno comparison.
  //
  // For the last block: leaf_seqno stores the real tag of the last key.
  // This matches the standard index which stores the full internal key
  // as the last block's separator.
  if (has_seqno_encoding_ && iter_.Valid()) {
    uint64_t leaf_idx = iter_.LeafIndex();
    uint64_t leaf_seqno = trie_->GetLeafSeqno(leaf_idx);

    if (target_packed < leaf_seqno &&
        comparator_->Compare(Slice(current_key_scratch_), target) == 0) {
      // Target's internal key is AFTER the separator (lower tag =
      // later in internal key order for same user key). Advance through
      // overflow blocks.
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      uint32_t base = trie_->GetOverflowBase(leaf_idx);

      bool found = false;
      for (uint32_t oi = 0; oi < block_count - 1; oi++) {
        uint64_t ov_seqno = trie_->GetOverflowSeqno(base + oi);
        if (ov_seqno == 0 || target_packed >= ov_seqno) {
          // This overflow block is the right one.
          overflow_run_index_ = oi + 1;  // 1-based (0 = primary)
          overflow_run_size_ = block_count;
          overflow_base_idx_ = base;
          found = true;
          break;
        }
      }

      if (!found) {
        // target_packed is below all tags in this run. Advance to
        // the next trie leaf (the block after the run).
        if (!iter_.Next()) {
          // Exhausted all blocks: target is past the end of this SST.
          // Return kUnknown -- see comment in Seek path above.
          result->bound_check_result = IterBoundCheck::kUnknown;
          result->key = Slice();
          return Status::OK();
        }
        // Update key and overflow state for the new leaf.
        {
          Slice trie_key = iter_.Key();
          current_key_scratch_.assign(trie_key.data(), trie_key.size());
          result->key = Slice(current_key_scratch_);
        }
        overflow_run_index_ = 0;
        overflow_run_size_ = 1;
        overflow_base_idx_ = 0;
        // Check if the new leaf also has overflow (unlikely but possible
        // with adjacent same-key runs for different user keys).
        // iter_.Valid() is guaranteed here -- Next() returned true above.
        if (has_seqno_encoding_) {
          uint64_t new_leaf = iter_.LeafIndex();
          overflow_run_size_ = trie_->GetLeafBlockCount(new_leaf);
          overflow_base_idx_ = trie_->GetOverflowBase(new_leaf);
        }
      }
    } else {
      // Right block (common path). Set overflow state in case this leaf
      // has a run (for subsequent Next() calls).
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      overflow_run_index_ = 0;
      overflow_run_size_ = block_count;
      overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
    }
  }

  result->bound_check_result = CheckBounds(target);
  return Status::OK();
}

Status TrieIndexIterator::NextAndGetResult(IterateResult* result) {
  // Overflow fast path: key doesn't change within the same run, so
  // current_key_scratch_ can be passed directly to CheckBounds (no copy).
  if (overflow_run_index_ + 1 < overflow_run_size_) {
    overflow_run_index_++;
    result->key = Slice(current_key_scratch_);
    result->bound_check_result = CheckBounds(Slice(current_key_scratch_));
    return Status::OK();
  }

  // Non-overflow: current_key_scratch_ is about to be overwritten, so swap
  // into prev_key_scratch_ in O(1) instead of copying.
  std::swap(prev_key_scratch_, current_key_scratch_);

  // Advance to the next trie leaf.
  ResetOverflowState();

  if (!iter_.Next()) {
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  CopyTrieKeyToResult(result);
  SetupOverflowForCurrentLeaf(/*position_at_last=*/false);
  result->bound_check_result = CheckBounds(Slice(prev_key_scratch_));
  return Status::OK();
}

IndexFactoryBuilder::BlockHandle TrieIndexIterator::value() {
  if (overflow_run_index_ == 0) {
    // Primary block -- use the trie leaf's handle.
    auto handle = iter_.Value();
    return IndexFactoryBuilder::BlockHandle{handle.offset, handle.size};
  }
  // Overflow block -- use the side-table handle.
  // overflow_run_index_ is 1-based, overflow array is 0-based.
  uint32_t overflow_idx = overflow_base_idx_ + overflow_run_index_ - 1;
  auto handle = trie_->GetOverflowHandle(overflow_idx);
  return IndexFactoryBuilder::BlockHandle{handle.offset, handle.size};
}

// ============================================================================
// TrieScanBounds
// ============================================================================

void TrieScanBounds::Prepare(const ScanOptions scan_opts[], size_t num_opts) {
  scan_opts_.assign(scan_opts, scan_opts + num_opts);
  current_scan_idx_ = 0;
  prepared_ = true;
}

void TrieScanBounds::SkipScansBefore(const Slice& target,
                                     const Comparator* comparator) {
  // This handles the multi-scan case where the caller seeks into a later
  // scan range after the previous scan returned kOutOfBound.
  if (!prepared_) {
    return;
  }
  while (current_scan_idx_ < scan_opts_.size()) {
    const auto& opts = scan_opts_[current_scan_idx_];
    if (opts.range.limit.has_value() &&
        comparator->Compare(target, opts.range.limit.value()) >= 0) {
      current_scan_idx_++;
    } else {
      break;
    }
  }
}

IterBoundCheck TrieScanBounds::Check(const Slice& reference_key,
                                     const Comparator* comparator) const {
  if (!Active()) {
    return IterBoundCheck::kInbound;
  }
  if (current_scan_idx_ >= scan_opts_.size()) {
    return IterBoundCheck::kOutOfBound;
  }
  const auto& opts = scan_opts_[current_scan_idx_];

  // For Seek: reference_key = seek target. If target < limit, the found
  //   block may contain keys within bounds.
  // For Next: reference_key = previous separator. If prev_sep < limit,
  //   the current block may contain keys within bounds.
  //
  // This is conservative: it may return kInbound for a block that is fully
  // out of bounds. The data-level iterator handles per-key filtering.
  if (opts.range.limit.has_value() &&
      comparator->Compare(reference_key, opts.range.limit.value()) >= 0) {
    return IterBoundCheck::kOutOfBound;
  }
  return IterBoundCheck::kInbound;
}

// ============================================================================
// TrieV2IndexIterator
// ============================================================================

TrieV2IndexIterator::TrieV2IndexIterator(const TrieV2Index* index,
                                         const Comparator* comparator)
    : comparator_(comparator), iter_(index) {}

void TrieV2IndexIterator::Prepare(const ScanOptions scan_opts[],
                                  size_t num_opts) {
  bounds_.Prepare(scan_opts, num_opts);
}

Status TrieV2IndexIterator::SetResult(const Status& s, IterBoundCheck bounds,
                                      IterateResult* result) const {
  if (s.ok() && iter_.Valid()) {
    result->key = iter_.key();
    result->bound_check_result = bounds;
  } else {
    // Running off the end of this SST says nothing about the upper bound:
    // the next SST on the level may still hold in-bound keys, and
    // kOutOfBound would make LevelIterator stop scanning the level.
    result->key = Slice();
    result->bound_check_result = IterBoundCheck::kUnknown;
  }
  return s;
}

Status TrieV2IndexIterator::SeekAndGetResult(const Slice& target,
                                             IterateResult* result,
                                             const SeekContext& context) {
  bounds_.SkipScansBefore(target, comparator_);
  Status s = iter_.Seek(target, context.target_tag);
  return SetResult(s, bounds_.Check(target, comparator_), result);
}

Status TrieV2IndexIterator::NextAndGetResult(IterateResult* result) {
  const bool check_bounds = bounds_.Active();
  if (check_bounds) {
    const Slice key = iter_.key();
    prev_key_.assign(key.data(), key.size());
  }
  Status s = iter_.Next();
  return SetResult(s,
                   check_bounds ? bounds_.Check(prev_key_, comparator_)
                                : IterBoundCheck::kInbound,
                   result);
}

Status TrieV2IndexIterator::SeekToFirstAndGetResult(IterateResult* result) {
  return SetResult(iter_.SeekToFirst(), IterBoundCheck::kInbound, result);
}

Status TrieV2IndexIterator::SeekToLastAndGetResult(IterateResult* result) {
  return SetResult(iter_.SeekToLast(), IterBoundCheck::kInbound, result);
}

Status TrieV2IndexIterator::PrevAndGetResult(IterateResult* result) {
  // Upper-bound checks are based on forward movement. While moving backward,
  // DBIter's lower-bound handling decides when to stop.
  return SetResult(iter_.Prev(), IterBoundCheck::kInbound, result);
}

IndexFactoryBuilder::BlockHandle TrieV2IndexIterator::value() {
  const TrieBlockHandle handle = iter_.value();
  return IndexFactoryBuilder::BlockHandle{handle.offset, handle.size};
}

// ============================================================================
// TrieV2IndexReader
// ============================================================================

TrieV2IndexReader::TrieV2IndexReader(const Comparator* comparator)
    : comparator_(comparator) {}

Status TrieV2IndexReader::InitFromSlice(const Slice& data) {
  return index_.Init(data);
}

std::unique_ptr<IndexFactoryIterator> TrieV2IndexReader::NewIterator(
    const ReadOptions& /*read_options*/) {
  return std::make_unique<TrieV2IndexIterator>(&index_, comparator_);
}

size_t TrieV2IndexReader::ApproximateMemoryUsage() const {
  return sizeof(*this);
}

// ============================================================================
// TrieIndexReader
// ============================================================================

TrieIndexReader::TrieIndexReader(const Comparator* comparator)
    : comparator_(comparator) {}

Status TrieIndexReader::InitFromSlice(const Slice& data) {
  return trie_.InitFromData(data);
}

std::unique_ptr<IndexFactoryIterator> TrieIndexReader::NewIterator(
    const ReadOptions& /*read_options*/) {
  return std::make_unique<TrieIndexIterator>(&trie_, comparator_,
                                             trie_.HasSeqnoEncoding());
}

size_t TrieIndexReader::ApproximateMemoryUsage() const {
  // The serialized data is owned by the table reader or block cache. Only
  // report the child-position lookup tables allocated by InitFromData().
  return trie_.ApproximateAuxMemoryUsage();
}

// ============================================================================
// TrieIndexFactory
// ============================================================================

TrieIndexFactory::TrieIndexFactory() : TrieIndexFactory(1) {}

TrieIndexFactory::TrieIndexFactory(uint32_t format_version)
    : format_version_(format_version) {
  RegisterOptions("TrieIndexFactoryOptions", &format_version_,
                  &trie_index_factory_type_info);
}

namespace {
// Format 1 stores arrays in host byte order and reads them in place, with a
// little-endian header, so only little-endian hosts can read it.
Status CheckFormatOneSupported() {
  if (!port::kLittleEndian) {
    return Status::NotSupported(
        "Trie index format 1 requires a little-endian host; use "
        "format_version=2");
  }
  return Status::OK();
}
}  // namespace

Status TrieIndexFactory::ValidateFormatVersion() const {
  if (format_version_ == 1) {
    return CheckFormatOneSupported();
  }
  if (format_version_ != kTrieV2FormatVersion) {
    return Status::InvalidArgument(
        "TrieIndexFactory format_version must be 1 or 2; got " +
        std::to_string(format_version_));
  }
  return Status::OK();
}

Status TrieIndexFactory::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  Status s = ValidateFormatVersion();
  if (!s.ok()) {
    return s;
  }
  // NewBuilder() and NewReader() reject other comparators too; failing here
  // stops DB::Open instead of every flush and compaction.
  if (cf_opts.comparator != nullptr &&
      cf_opts.comparator != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ",
        cf_opts.comparator->Name());
  }
  return IndexFactory::ValidateOptions(db_opts, cf_opts);
}

Status TrieIndexFactory::NewBuilder(
    const IndexFactoryOptions& option,
    std::unique_ptr<IndexFactoryBuilder>& builder) const {
  // The trie traverses keys byte-by-byte in lexicographic order, so it
  // requires a bytewise comparator. Non-bytewise comparators (e.g.,
  // ReverseBytewiseComparator or custom comparators) would produce separator
  // keys in a different order than the trie's byte-level traversal, causing
  // incorrect Seek results.
  if (option.comparator != nullptr &&
      option.comparator != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ",
        option.comparator->Name());
  }
  // Default to BytewiseComparator when null. The trie requires a bytewise
  // comparator for separator key ordering; null would cause a dereference
  // crash in AddIndexEntry when comparing keys.
  Status s = ValidateFormatVersion();
  if (!s.ok()) {
    return s;
  }
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
  builder = std::make_unique<TrieIndexBuilder>(cmp, format_version_);
  return Status::OK();
}

Status TrieIndexFactory::NewReader(
    const IndexFactoryOptions& option, Slice& index_block,
    std::unique_ptr<IndexFactoryReader>& reader) const {
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
  if (cmp != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ", cmp->Name());
  }
  // Both formats start with the magic and a 32-bit version. Format 1 reports
  // its own errors for anything that is not format 2, including truncation.
  if (index_block.size() >= 8 &&
      DecodeFixed32(index_block.data() + 4) == kTrieV2FormatVersion) {
    std::unique_ptr<TrieV2IndexReader> v2_reader =
        std::make_unique<TrieV2IndexReader>(cmp);
    Status s = v2_reader->InitFromSlice(index_block);
    if (!s.ok()) {
      return s;
    }
    reader = std::move(v2_reader);
    return Status::OK();
  }
  Status s = CheckFormatOneSupported();
  if (!s.ok()) {
    return s;
  }
  auto trie_reader = std::make_unique<TrieIndexReader>(cmp);
  s = trie_reader->InitFromSlice(index_block);
  if (!s.ok()) {
    return s;
  }
  reader = std::move(trie_reader);
  return Status::OK();
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
