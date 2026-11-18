//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <sstream>
#include <utility>

#include "monitoring/perf_context_imp.h"

namespace ROCKSDB_NAMESPACE {

/*
 * Please add new metrics to this macro and appropriate fields will be copied,
 * and/or emitted when converted to string.
 * When people need to add new metrics please add the metric to the macro below
 * and enclose the name of the specific metric within defCmd().
 * The position of the field will be dictated by the
 * order in which the macros are enumerated and the offsets of the fields will
 * be matched against ''PerfContextByLevelBase'' declared in perf_context.h.
 */
// clang-format off
#define DEF_PERF_CONTEXT_LEVEL_METRICS(defCmd) \
  defCmd(bloom_filter_useful)                  \
  defCmd(bloom_filter_full_positive)           \
  defCmd(bloom_filter_full_true_positive)      \
  defCmd(user_key_return_count)                \
  defCmd(get_from_table_nanos)                 \
  defCmd(block_cache_hit_count)                \
  defCmd(block_cache_miss_count)
// clang-format on

// Break down performance counters by level and store per-level perf context in
// PerfContextByLevel
struct PerfContextByLevelInt {
#define EMIT_FIELDS(x) uint64_t x = 0;
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
};

/*
 * Please add new metrics to this macro and appropriate fields will be copied,
 * and/or emitted when converted to string.
 * When people need to add new metrics please enclose the name of the specific
 * metric within defCmd(). The position of the field will be dictated by the
 * order in which the macros are enumerated and the offsets of the fields will
 * be matched against ''PerfContextBase'' declared in perf_context.h.
 */

// clang-format off
#define DEF_PERF_CONTEXT_METRICS(defCmd)           \
  defCmd(user_key_comparison_count)                \
  defCmd(block_cache_hit_count)                    \
  defCmd(block_read_count)                         \
  defCmd(block_read_byte)                          \
  defCmd(block_read_time)                          \
  defCmd(block_read_cpu_time)                      \
  defCmd(block_cache_index_hit_count)              \
  defCmd(block_cache_standalone_handle_count)      \
  defCmd(block_cache_real_handle_count)            \
  defCmd(index_block_read_count)                   \
  defCmd(block_cache_filter_hit_count)             \
  defCmd(filter_block_read_count)                  \
  defCmd(compression_dict_block_read_count)        \
  defCmd(block_cache_index_read_byte)              \
  defCmd(block_cache_filter_read_byte)             \
  defCmd(block_cache_compression_dict_read_byte)   \
  defCmd(block_cache_read_byte)                    \
  defCmd(secondary_cache_hit_count)                \
  defCmd(compressed_sec_cache_insert_real_count)   \
  defCmd(compressed_sec_cache_insert_dummy_count)  \
  defCmd(compressed_sec_cache_uncompressed_bytes)  \
  defCmd(compressed_sec_cache_compressed_bytes)    \
  defCmd(block_checksum_time)                      \
  defCmd(block_decompress_time)                    \
  defCmd(block_decompress_count)                   \
  defCmd(get_read_bytes)                           \
  defCmd(multiget_read_bytes)                      \
  defCmd(iter_read_bytes)                          \
  defCmd(blob_cache_hit_count)                     \
  defCmd(blob_cache_read_byte)                     \
  defCmd(blob_read_count)                          \
  defCmd(blob_read_byte)                           \
  defCmd(blob_read_time)                           \
  defCmd(blob_checksum_time)                       \
  defCmd(blob_decompress_time)                     \
  defCmd(internal_key_skipped_count)               \
  defCmd(internal_delete_skipped_count)            \
  defCmd(internal_recent_skipped_count)            \
  defCmd(internal_merge_count)                     \
  defCmd(internal_merge_point_lookup_count)        \
  defCmd(internal_range_del_reseek_count)          \
  defCmd(get_snapshot_time)                        \
  defCmd(get_from_memtable_time)                   \
  defCmd(get_from_memtable_count)                  \
  defCmd(get_post_process_time)                    \
  defCmd(get_from_output_files_time)               \
  defCmd(seek_on_memtable_time)                    \
  defCmd(seek_on_memtable_count)                   \
  defCmd(next_on_memtable_count)                   \
  defCmd(prev_on_memtable_count)                   \
  defCmd(seek_child_seek_time)                     \
  defCmd(seek_child_seek_count)                    \
  defCmd(seek_min_heap_time)                       \
  defCmd(seek_max_heap_time)                       \
  defCmd(seek_internal_seek_time)                  \
  defCmd(find_next_user_entry_time)                \
  defCmd(write_wal_time)                           \
  defCmd(write_memtable_time)                      \
  defCmd(write_delay_time)                         \
  defCmd(write_scheduling_flushes_compactions_time)\
  defCmd(write_pre_and_post_process_time)          \
  defCmd(write_thread_wait_nanos)                  \
  defCmd(db_mutex_lock_nanos)                      \
  defCmd(db_condition_wait_nanos)                  \
  defCmd(merge_operator_time_nanos)                \
  defCmd(read_index_block_nanos)                   \
  defCmd(read_filter_block_nanos)                  \
  defCmd(new_table_block_iter_nanos)               \
  defCmd(new_table_iterator_nanos)                 \
  defCmd(block_seek_nanos)                         \
  defCmd(find_table_nanos)                         \
  defCmd(bloom_memtable_hit_count)                 \
  defCmd(bloom_memtable_miss_count)                \
  defCmd(bloom_sst_hit_count)                      \
  defCmd(bloom_sst_miss_count)                     \
  defCmd(key_lock_wait_time)                       \
  defCmd(key_lock_wait_count)                      \
  defCmd(env_new_sequential_file_nanos)            \
  defCmd(env_new_random_access_file_nanos)         \
  defCmd(env_new_writable_file_nanos)              \
  defCmd(env_reuse_writable_file_nanos)            \
  defCmd(env_new_random_rw_file_nanos)             \
  defCmd(env_new_directory_nanos)                  \
  defCmd(env_file_exists_nanos)                    \
  defCmd(env_get_children_nanos)                   \
  defCmd(env_get_children_file_attributes_nanos)   \
  defCmd(env_delete_file_nanos)                    \
  defCmd(env_create_dir_nanos)                     \
  defCmd(env_create_dir_if_missing_nanos)          \
  defCmd(env_delete_dir_nanos)                     \
  defCmd(env_get_file_size_nanos)                  \
  defCmd(env_get_file_modification_time_nanos)     \
  defCmd(env_rename_file_nanos)                    \
  defCmd(env_link_file_nanos)                      \
  defCmd(env_lock_file_nanos)                      \
  defCmd(env_unlock_file_nanos)                    \
  defCmd(env_new_logger_nanos)                     \
  defCmd(get_cpu_nanos)                            \
  defCmd(iter_next_cpu_nanos)                      \
  defCmd(iter_prev_cpu_nanos)                      \
  defCmd(iter_seek_cpu_nanos)                      \
  defCmd(iter_next_count)                          \
  defCmd(iter_prev_count)                          \
  defCmd(iter_seek_count)                          \
  defCmd(encrypt_data_nanos)                       \
  defCmd(decrypt_data_nanos)                       \
  defCmd(number_async_seek)                        \
  defCmd(file_ingestion_nanos)                     \
  defCmd(file_ingestion_blocking_live_writes_nanos)\
  defCmd(data_block_read_byte)                     \
  defCmd(index_block_read_byte)                    \
  defCmd(filter_block_read_byte)                   \
  defCmd(compression_dict_block_read_byte)         \
  defCmd(metadata_block_read_byte)                 \
  defCmd(multiscan_prepare_count)                  \
  defCmd(multiscan_blocks_prefetched)             \
  defCmd(multiscan_blocks_from_cache)             \
  defCmd(multiscan_prefetch_bytes)                \
  defCmd(multiscan_io_requests)                   \
  defCmd(multiscan_io_coalesced_nonadjacent)
// clang-format on

struct PerfContextInt {
#define EMIT_FIELDS(x) uint64_t x;
  DEF_PERF_CONTEXT_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
};

#if defined(NPERF_CONTEXT)
// Should not be used because the counters are not thread-safe.
// Put here just to make get_perf_context() simple without ifdef.
PerfContext perf_context;
#else
thread_local PerfContext perf_context;
thread_local constinit PerfContext* perf_context_fast_ptr = nullptr;

PerfContext* InitPerfContextFastPtr() {
  perf_context_fast_ptr = &perf_context;
  return perf_context_fast_ptr;
}
#endif

PerfContext* get_perf_context() {
  static_assert(sizeof(PerfContextBase) == sizeof(PerfContextInt));
  static_assert(sizeof(PerfContextByLevelBase) ==
                sizeof(PerfContextByLevelInt));
  /*
   * Validate that we have the same fields and offsets between the external user
   * facing
   * ''PerfContextBase'' and ''PerfContextByLevelBase' structures with the
   * internal structures that we generate from the DEF_* macros above. This way
   * if people add metrics to the user-facing header file, they will have a
   * build failure and need to add similar fields to the macros in this file.
   * These are compile-time validations and don't impose any run-time penalties.
   */
#define EMIT_OFFSET_ASSERTION(x) \
  static_assert(offsetof(PerfContextBase, x) == offsetof(PerfContextInt, x));
  DEF_PERF_CONTEXT_METRICS(EMIT_OFFSET_ASSERTION)
#undef EMIT_OFFSET_ASSERTION
#define EMIT_OFFSET_ASSERTION(x)                       \
  static_assert(offsetof(PerfContextByLevelBase, x) == \
                offsetof(PerfContextByLevelInt, x));
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_OFFSET_ASSERTION)
#undef EMIT_OFFSET_ASSERTION
  return &perf_context;
}

PerfContext::~PerfContext() {
#if !defined(NPERF_CONTEXT)
  ClearPerLevelPerfContext();
#endif
}

PerfContext::PerfContext(const PerfContext& other) : PerfContextBase() {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  copyMetrics(&other);
#endif
}

PerfContext::PerfContext(PerfContext&& other) noexcept : PerfContext() {
  *this = std::move(other);
}

PerfContext& PerfContext::operator=(const PerfContext& other) {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  copyMetrics(&other);
#endif
  return *this;
}

PerfContext& PerfContext::operator=(PerfContext&& other) noexcept {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  if (this != &other) {
    ClearPerLevelPerfContext();
#define EMIT_MOVE_FIELDS(x) x = other.x;
    DEF_PERF_CONTEXT_METRICS(EMIT_MOVE_FIELDS)
#undef EMIT_MOVE_FIELDS
    level_to_perf_context = other.level_to_perf_context;
    per_level_perf_context_enabled = other.per_level_perf_context_enabled;
    other.level_to_perf_context = nullptr;
    other.per_level_perf_context_enabled = false;
  }
#endif
  return *this;
}

void PerfContext::copyMetrics(const PerfContext* other) noexcept {
#ifdef NPERF_CONTEXT
  (void)other;
#else
#define EMIT_COPY_FIELDS(x) x = other->x;
  DEF_PERF_CONTEXT_METRICS(EMIT_COPY_FIELDS)
#undef EMIT_COPY_FIELDS
  if (per_level_perf_context_enabled && level_to_perf_context != nullptr) {
    ClearPerLevelPerfContext();
  }
  if (other->level_to_perf_context != nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
    *level_to_perf_context = *other->level_to_perf_context;
  }
  per_level_perf_context_enabled = other->per_level_perf_context_enabled;
#endif
}

void PerfContext::Reset() {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x = 0;
  DEF_PERF_CONTEXT_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
  if (per_level_perf_context_enabled && level_to_perf_context) {
    for (auto& kv : *level_to_perf_context) {
      kv.second.Reset();
    }
  }
#endif
}

void PerfContext::Merge(const PerfContext& other) {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x += other.x;
  DEF_PERF_CONTEXT_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
  if (other.level_to_perf_context != nullptr) {
    if (level_to_perf_context == nullptr) {
      level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
    }
    for (const auto& kv : *other.level_to_perf_context) {
      (*level_to_perf_context)[kv.first].Merge(kv.second);
    }
  }
  per_level_perf_context_enabled =
      per_level_perf_context_enabled || other.per_level_perf_context_enabled;
#else
  (void)other;
#endif
}

void PerfContextByLevel::Reset() {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x = 0;
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
#endif
}

void PerfContextByLevel::Merge(const PerfContextByLevel& other) {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x += other.x;
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
#else
  (void)other;
#endif
}

std::string PerfContext::ToString(bool exclude_zero_counters) const {
#ifdef NPERF_CONTEXT
  (void)exclude_zero_counters;
  return "";
#else
  std::ostringstream ss;
#define PERF_CONTEXT_OUTPUT(counter)             \
  if (!exclude_zero_counters || (counter > 0)) { \
    ss << #counter << " = " << counter << ", ";  \
  }
  DEF_PERF_CONTEXT_METRICS(PERF_CONTEXT_OUTPUT)
#undef PERF_CONTEXT_OUTPUT
  if (per_level_perf_context_enabled && level_to_perf_context) {
#define PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(counter)      \
  ss << #counter << " = ";                                     \
  for (auto& kv : *level_to_perf_context) {                    \
    if (!exclude_zero_counters || (kv.second.counter > 0)) {   \
      ss << kv.second.counter << "@level" << kv.first << ", "; \
    }                                                          \
  }
    DEF_PERF_CONTEXT_LEVEL_METRICS(PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER)
#undef PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER
  }
  std::string str = ss.str();
  str.erase(str.find_last_not_of(", ") + 1);
  return str;
#endif
}

void PerfContext::EnablePerLevelPerfContext() {
  if (level_to_perf_context == nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
  }
  per_level_perf_context_enabled = true;
}

void PerfContext::DisablePerLevelPerfContext() {
  per_level_perf_context_enabled = false;
}

void PerfContext::ClearPerLevelPerfContext() {
  if (level_to_perf_context != nullptr) {
    level_to_perf_context->clear();
    delete level_to_perf_context;
    level_to_perf_context = nullptr;
  }
  per_level_perf_context_enabled = false;
}

}  // namespace ROCKSDB_NAMESPACE
