//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie_index_factory.h"

#include <algorithm>
#include <limits>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

namespace {
// Separators per bucket. Larger buckets make the index smaller and Seek
// slower; 8 was the best tradeoff on the key sets measured. The value is
// stored in each index, so it can change without a format change.
constexpr uint32_t kTrieBucketSize = 8;

// The trie orders separators byte by byte, so it requires a bytewise
// comparator. Null means the default, BytewiseComparator.
Status RequireBytewiseComparator(const Comparator* comparator) {
  if (comparator != nullptr && comparator != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ",
        comparator->Name());
  }
  return Status::OK();
}

// A block's tag is 0 when the block can be found by user key alone, and the
// real tag of its last key otherwise:
// - when it shares its separator with the previous block, the tags tell the
//   blocks of that user key apart;
// - for the last block, whose separator is the last user key itself, the tag
//   lets a Seek for that user key with a smaller sequence number move past it,
//   as the standard index does with its full internal key.
// A separator between different user keys needs no tag: the standard index
// with index_key_is_user_key also compares user keys only there.
uint64_t BlockTag(bool shares_previous_separator, bool has_next_block,
                  uint64_t last_key_tag) {
  return shares_previous_separator || !has_next_block ? last_key_tag : 0;
}
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

TrieIndexBuilder::TrieIndexBuilder(const Comparator* comparator)
    : comparator_(comparator),
      finished_(false),
      trie_builder_(kTrieBucketSize) {}

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
  // True when last_key and first_key_in_next_block are the same user key: the
  // separator is then that user key for both blocks, so this block needs its
  // real tag.
  bool same_user_key = false;
  if (first_key_in_next_block != nullptr) {
    same_user_key = comparator_->Compare(last_key_in_current_block,
                                         *first_key_in_next_block) == 0;

    separator_scratch->assign(last_key_in_current_block.data(),
                              last_key_in_current_block.size());
    comparator_->FindShortestSeparator(separator_scratch,
                                       *first_key_in_next_block);
    separator = Slice(*separator_scratch);
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
  }

  const uint64_t tag =
      BlockTag(same_user_key || SharesPreviousSeparator(separator),
               first_key_in_next_block != nullptr, last_key_tag);
  const TrieBlockHandle handle{block_handle.offset, block_handle.size};
  TrackHandle(handle);
  // Before AddBlock(), which may move the bytes LastSeparator() points to.
  UpdateSizeEstimate(separator, trie_builder_.LastSeparator(), tag != 0);
  trie_builder_.AddBlock(separator, handle, tag);
  return separator;
}

bool TrieIndexBuilder::SharesPreviousSeparator(const Slice& separator) const {
  // FindShortestSeparator() can leave a separator unshortened, e.g. "abc"
  // before "abd", so it can equal the previous block's separator even when
  // this block's last key and the next block's first key differ. The last
  // block's separator is its last user key, which equals the previous
  // separator when one user key spans both blocks.
  return trie_builder_.NumBlocks() > 0 &&
         trie_builder_.LastSeparator() == separator;
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
  // The tags have the meaning TrieBuilder::AddBlock() expects: 0 for a block
  // found by user key alone, otherwise the tag of the block's last key.
  Status s = trie_builder_.Finish(&data_);
  if (s.ok()) {
    *index_contents = data_;
  }
  return s;
}

uint64_t TrieIndexBuilder::EstimatedSize() const {
  if (finished_) {
    return data_.size();
  }
  TrieBuilder::SizeStats stats = estimate_;
  stats.max_block_step = max_block_step_.LoadRelaxed();
  stats.max_block_size = max_block_size_.LoadRelaxed();
  stats.irregular_gaps = irregular_block_gaps_.LoadRelaxed();
  return TrieBuilder::SizeBound(stats, kTrieBucketSize);
}

size_t TrieIndexBuilder::UpdateSizeEstimate(const Slice& separator,
                                            const Slice& previous_separator,
                                            bool may_have_tag) {
  const size_t shared_prefix = previous_separator.difference_offset(separator);
  const size_t drop = previous_separator.size() - shared_prefix;
  const size_t add = separator.size() - shared_prefix;
  // Matches the entry encoding in trie.h. The first separator and the bucket
  // heads are charged too, which only overestimates.
  estimate_.suffix_bytes += add;
  estimate_.entry_bytes += TrieBuilder::EntrySize(drop, add);
  estimate_.max_key_len =
      std::max<uint64_t>(estimate_.max_key_len, separator.size());
  if (estimate_.num_blocks > 0 && drop == 0 && add == 0) {
    ++estimate_.duplicates;
    may_have_tag = true;
  }
  if (may_have_tag) {
    ++estimate_.tagged;
  }
  ++estimate_.num_blocks;
  return shared_prefix;
}

void TrieIndexBuilder::TrackHandle(const TrieBlockHandle& handle) {
  max_block_size_.StoreRelaxed(
      std::max(max_block_size_.LoadRelaxed(), handle.size));
  // Mirrors the derived-size rule in TrieBuilder::Finish(): no block may end
  // past 2^64 - 1, the gaps between blocks must be equal, and the sentinel
  // after the last block, its end plus the gap, must fit too. Wrapping
  // arithmetic only matters once sizes are already explicit.
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  bool irregular = handle.size > max - handle.offset;
  if (trie_builder_.NumBlocks() > 0) {
    const uint64_t step = handle.offset - previous_handle_.offset;
    max_block_step_.StoreRelaxed(std::max(max_block_step_.LoadRelaxed(), step));
    const uint64_t previous_end =
        previous_handle_.offset + previous_handle_.size;
    const uint64_t gap = handle.offset - previous_end;
    irregular = irregular || handle.offset < previous_end ||
                (trie_builder_.NumBlocks() > 1 && gap != block_gap_);
    block_gap_ = gap;
  }
  // Any block may be the last. If this one is not, a sentinel that does not
  // fit means the next block's gap differs, so flagging it now is exact.
  if (irregular || block_gap_ > max - handle.offset - handle.size) {
    irregular_block_gaps_.StoreRelaxed(true);
  }
  previous_handle_ = handle;
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

    p->separator_key.assign(last_key_in_current_block.data(),
                            last_key_in_current_block.size());
    comparator_->FindShortestSeparator(&p->separator_key,
                                       *first_key_in_next_block);
  } else {
    // Last block: store the last key itself (no shortening).
    p->separator_key.assign(last_key_in_current_block.data(),
                            last_key_in_current_block.size());
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

  // The half of the tag decision that depends on the previous block runs here:
  // on the BG writer thread, in commit order, trie_builder_ holds every block
  // before this one. It is the same code as the serial AddIndexEntry().
  const Slice separator(p->separator_key);
  const uint64_t tag =
      BlockTag(p->same_user_key_initial || SharesPreviousSeparator(separator),
               p->has_next_block, p->last_key_tag);
  const TrieBlockHandle handle{block_handle.offset, block_handle.size};
  TrackHandle(handle);
  trie_builder_.AddBlock(separator, handle, tag);

  // Reset valid so the same prepared slot can be reused (the table
  // builder rotates BlockReps in a ring buffer).
  p->valid = false;
}

// ============================================================================
// TrieIndexIterator
// ============================================================================

TrieIndexIterator::TrieIndexIterator(const Trie* trie,
                                     const Comparator* comparator)
    : comparator_(comparator), iter_(trie) {}

void TrieIndexIterator::Prepare(const ScanOptions scan_opts[],
                                size_t num_opts) {
  scan_opts_.clear();
  scan_opts_.reserve(num_opts);
  for (size_t i = 0; i < num_opts; i++) {
    scan_opts_.push_back(scan_opts[i]);
  }
  current_scan_idx_ = 0;
  prepared_ = true;
}

Status TrieIndexIterator::SetResult(const Status& s, IterBoundCheck bounds,
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

Status TrieIndexIterator::SeekAndGetResult(const Slice& target,
                                           IterateResult* result,
                                           const SeekContext& context) {
  // Advance current_scan_idx_ past any scans whose limit <= target.
  // This handles the multi-scan case where the caller seeks into a later
  // scan range after the previous scan returned kOutOfBound.
  if (prepared_) {
    while (current_scan_idx_ < scan_opts_.size()) {
      const auto& opts = scan_opts_[current_scan_idx_];
      if (opts.range.limit.has_value() &&
          comparator_->Compare(target, opts.range.limit.value()) >= 0) {
        current_scan_idx_++;
      } else {
        break;
      }
    }
  }
  Status s = iter_.Seek(target, context.target_tag);
  return SetResult(s, CheckBounds(target), result);
}

Status TrieIndexIterator::NextAndGetResult(IterateResult* result) {
  // The bound check uses the separator before the step, so run it first.
  const IterBoundCheck bounds = CheckBounds(iter_.key());
  return SetResult(iter_.Next(), bounds, result);
}

Status TrieIndexIterator::SeekToFirstAndGetResult(IterateResult* result) {
  return SetResult(iter_.SeekToFirst(), IterBoundCheck::kInbound, result);
}

Status TrieIndexIterator::SeekToLastAndGetResult(IterateResult* result) {
  return SetResult(iter_.SeekToLast(), IterBoundCheck::kInbound, result);
}

Status TrieIndexIterator::PrevAndGetResult(IterateResult* result) {
  // Upper-bound checks are based on forward movement. While moving backward,
  // DBIter's lower-bound handling decides when to stop.
  return SetResult(iter_.Prev(), IterBoundCheck::kInbound, result);
}

IndexFactoryBuilder::BlockHandle TrieIndexIterator::value() {
  const TrieBlockHandle handle = iter_.value();
  return IndexFactoryBuilder::BlockHandle{handle.offset, handle.size};
}

IterBoundCheck TrieIndexIterator::CheckBounds(
    const Slice& reference_key) const {
  if (!HasBounds()) {
    // No bounds to check -- always in-bound.
    return IterBoundCheck::kInbound;
  }

  if (current_scan_idx_ >= scan_opts_.size()) {
    return IterBoundCheck::kOutOfBound;
  }

  const auto& opts = scan_opts_[current_scan_idx_];

  // Check upper bound (limit) against the reference key, NOT the current
  // separator. The trie stores separator keys (upper bounds on block
  // contents), so comparing the separator against the limit would
  // prematurely reject blocks that contain keys < limit.
  //
  // For Seek: reference_key = seek target. If target < limit, the found
  //   block may contain keys within bounds.
  // For Next: reference_key = previous separator. If prev_sep < limit,
  //   the current block may contain keys within bounds.
  //
  // This is conservative: it may return kInbound for a block that is fully
  // out of bounds. The data-level iterator handles per-key filtering.
  if (opts.range.limit.has_value()) {
    const Slice& limit = opts.range.limit.value();
    if (comparator_->Compare(reference_key, limit) >= 0) {
      return IterBoundCheck::kOutOfBound;
    }
  }

  return IterBoundCheck::kInbound;
}

// ============================================================================
// TrieIndexReader
// ============================================================================

TrieIndexReader::TrieIndexReader(const Comparator* comparator)
    : comparator_(comparator) {}

Status TrieIndexReader::InitFromSlice(const Slice& data) {
  return trie_.Init(data);
}

std::unique_ptr<IndexFactoryIterator> TrieIndexReader::NewIterator(
    const ReadOptions& /*read_options*/) {
  return std::make_unique<TrieIndexIterator>(&trie_, comparator_);
}

size_t TrieIndexReader::ApproximateMemoryUsage() const { return sizeof(*this); }

// ============================================================================
// TrieIndexFactory
// ============================================================================

Status TrieIndexFactory::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  // NewBuilder() and NewReader() reject other comparators too; failing here
  // stops DB::Open instead of every flush and compaction.
  Status s = RequireBytewiseComparator(cf_opts.comparator);
  if (!s.ok()) {
    return s;
  }
  return IndexFactory::ValidateOptions(db_opts, cf_opts);
}

Status TrieIndexFactory::NewBuilder(
    const IndexFactoryOptions& option,
    std::unique_ptr<IndexFactoryBuilder>& builder) const {
  Status s = RequireBytewiseComparator(option.comparator);
  if (!s.ok()) {
    return s;
  }
  // AddIndexEntry() compares keys, so a null comparator becomes the default.
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
  builder = std::make_unique<TrieIndexBuilder>(cmp);
  return Status::OK();
}

Status TrieIndexFactory::NewReader(
    const IndexFactoryOptions& option, Slice& index_block,
    std::unique_ptr<IndexFactoryReader>& reader) const {
  Status s = RequireBytewiseComparator(option.comparator);
  if (!s.ok()) {
    return s;
  }
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
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
