//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// SOURCE TEMPLATE -- DO NOT INCLUDE DIRECTLY.
// Edit this file to modify the C API, then run:
//   python3 tools/c_api_gen/regen_all.py
// The public header for consumers is include/rocksdb/c.h
//
/*
  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . capturing post-write-snapshot
  . custom iter, db, env, cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
  On success, a leveldb routine leaves *errptr unchanged.
  On failure, leveldb frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type unsigned char (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#pragma once

#ifdef _WIN32
#ifdef ROCKSDB_DLL
#ifdef ROCKSDB_LIBRARY_EXPORTS
#define ROCKSDB_LIBRARY_API __declspec(dllexport)
#else
#define ROCKSDB_LIBRARY_API __declspec(dllimport)
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Exported types */

typedef struct rocksdb_t rocksdb_t;
typedef struct rocksdb_status_ptr_t rocksdb_status_ptr_t;
typedef struct rocksdb_backup_engine_t rocksdb_backup_engine_t;
typedef struct rocksdb_backup_engine_info_t rocksdb_backup_engine_info_t;
typedef struct rocksdb_backup_engine_options_t rocksdb_backup_engine_options_t;
typedef struct rocksdb_create_backup_options_t rocksdb_create_backup_options_t;
typedef struct rocksdb_restore_options_t rocksdb_restore_options_t;
typedef struct rocksdb_memory_allocator_t rocksdb_memory_allocator_t;
typedef struct rocksdb_lru_cache_options_t rocksdb_lru_cache_options_t;
typedef struct rocksdb_hyper_clock_cache_options_t
    rocksdb_hyper_clock_cache_options_t;
typedef struct rocksdb_cache_t rocksdb_cache_t;
typedef struct rocksdb_write_buffer_manager_t rocksdb_write_buffer_manager_t;
typedef struct rocksdb_sst_file_manager_t rocksdb_sst_file_manager_t;
typedef struct rocksdb_compactionfilter_t rocksdb_compactionfilter_t;
typedef struct rocksdb_compactionfiltercontext_t
    rocksdb_compactionfiltercontext_t;
typedef struct rocksdb_compactionfilterfactory_t
    rocksdb_compactionfilterfactory_t;
typedef struct rocksdb_walfilter_t rocksdb_walfilter_t;
typedef struct rocksdb_file_checksum_gen_factory_t
    rocksdb_file_checksum_gen_factory_t;
typedef struct rocksdb_sst_partitioner_factory_t
    rocksdb_sst_partitioner_factory_t;
typedef struct rocksdb_table_properties_collector_factory_t
    rocksdb_table_properties_collector_factory_t;
typedef struct rocksdb_comparator_t rocksdb_comparator_t;
typedef struct rocksdb_dbpath_t rocksdb_dbpath_t;
typedef struct rocksdb_env_t rocksdb_env_t;
typedef struct rocksdb_fifo_compaction_options_t
    rocksdb_fifo_compaction_options_t;
typedef struct rocksdb_filelock_t rocksdb_filelock_t;
typedef struct rocksdb_filterpolicy_t rocksdb_filterpolicy_t;
typedef struct rocksdb_flushoptions_t rocksdb_flushoptions_t;
typedef struct rocksdb_flushwaloptions_t rocksdb_flushwaloptions_t;
typedef struct rocksdb_iterator_t rocksdb_iterator_t;
typedef struct rocksdb_logger_t rocksdb_logger_t;
typedef struct rocksdb_mergeoperator_t rocksdb_mergeoperator_t;
typedef struct rocksdb_options_t rocksdb_options_t;
typedef struct rocksdb_compaction_options_t rocksdb_compaction_options_t;
typedef struct rocksdb_compactoptions_t rocksdb_compactoptions_t;
typedef struct rocksdb_block_based_table_options_t
    rocksdb_block_based_table_options_t;
typedef struct rocksdb_block_cache_trace_options_t
    rocksdb_block_cache_trace_options_t;
typedef struct rocksdb_block_cache_trace_writer_options_t
    rocksdb_block_cache_trace_writer_options_t;
typedef struct rocksdb_cuckoo_table_options_t rocksdb_cuckoo_table_options_t;
typedef struct rocksdb_randomfile_t rocksdb_randomfile_t;
typedef struct rocksdb_readoptions_t rocksdb_readoptions_t;
typedef struct rocksdb_seqfile_t rocksdb_seqfile_t;
typedef struct rocksdb_slicetransform_t rocksdb_slicetransform_t;
typedef struct rocksdb_snapshot_t rocksdb_snapshot_t;
typedef struct rocksdb_writablefile_t rocksdb_writablefile_t;
typedef struct rocksdb_writebatch_t rocksdb_writebatch_t;
typedef struct rocksdb_writebatch_wi_t rocksdb_writebatch_wi_t;
typedef struct rocksdb_writeoptions_t rocksdb_writeoptions_t;
typedef struct rocksdb_universal_compaction_options_t
    rocksdb_universal_compaction_options_t;
typedef struct rocksdb_livefile_t rocksdb_livefile_t;
typedef struct rocksdb_livefiles_t rocksdb_livefiles_t;
typedef struct rocksdb_column_family_handle_t rocksdb_column_family_handle_t;
typedef struct rocksdb_column_family_metadata_t
    rocksdb_column_family_metadata_t;
typedef struct rocksdb_import_column_family_options_t
    rocksdb_import_column_family_options_t;
typedef struct rocksdb_export_import_files_metadata_t
    rocksdb_export_import_files_metadata_t;
typedef struct rocksdb_level_metadata_t rocksdb_level_metadata_t;
typedef struct rocksdb_sst_file_metadata_t rocksdb_sst_file_metadata_t;
typedef struct rocksdb_column_family_metadata_options_t
    rocksdb_column_family_metadata_options_t;
typedef struct rocksdb_envoptions_t rocksdb_envoptions_t;
typedef struct rocksdb_ingestexternalfileoptions_t
    rocksdb_ingestexternalfileoptions_t;
typedef struct rocksdb_trace_options_t rocksdb_trace_options_t;
typedef struct rocksdb_trace_reader_t rocksdb_trace_reader_t;
typedef struct rocksdb_replay_options_t rocksdb_replay_options_t;
typedef struct rocksdb_replayer_t rocksdb_replayer_t;
typedef struct rocksdb_size_approximation_options_t
    rocksdb_size_approximation_options_t;
typedef struct rocksdb_livefiles_storage_info_options_t
    rocksdb_livefiles_storage_info_options_t;
typedef struct rocksdb_livefiles_storage_info_t
    rocksdb_livefiles_storage_info_t;
typedef struct rocksdb_sstfilewriter_t rocksdb_sstfilewriter_t;
typedef struct rocksdb_ratelimiter_t rocksdb_ratelimiter_t;
typedef struct rocksdb_perfcontext_t rocksdb_perfcontext_t;
typedef struct rocksdb_pinnableslice_t rocksdb_pinnableslice_t;
typedef struct rocksdb_pinnable_multi_get_t rocksdb_pinnable_multi_get_t;
typedef struct rocksdb_transactiondb_options_t rocksdb_transactiondb_options_t;
typedef struct rocksdb_transactiondb_t rocksdb_transactiondb_t;
typedef struct rocksdb_transaction_options_t rocksdb_transaction_options_t;
typedef struct rocksdb_optimistictransactiondb_t
    rocksdb_optimistictransactiondb_t;
typedef struct rocksdb_optimistictransactiondb_options_t
    rocksdb_optimistictransactiondb_options_t;
typedef struct rocksdb_optimistictransactiondb_lock_buckets_t
    rocksdb_optimistictransactiondb_lock_buckets_t;
typedef struct rocksdb_optimistictransaction_options_t
    rocksdb_optimistictransaction_options_t;
typedef struct rocksdb_transaction_t rocksdb_transaction_t;
typedef struct rocksdb_checkpoint_t rocksdb_checkpoint_t;
typedef struct rocksdb_wal_iterator_t rocksdb_wal_iterator_t;
typedef struct rocksdb_wal_readoptions_t rocksdb_wal_readoptions_t;
typedef struct rocksdb_wal_file_t rocksdb_wal_file_t;
typedef struct rocksdb_wal_files_t rocksdb_wal_files_t;
typedef struct rocksdb_memory_consumers_t rocksdb_memory_consumers_t;
typedef struct rocksdb_memory_usage_t rocksdb_memory_usage_t;
typedef struct rocksdb_statistics_histogram_data_t
    rocksdb_statistics_histogram_data_t;
typedef struct rocksdb_wait_for_compact_options_t
    rocksdb_wait_for_compact_options_t;

/* rocksdb_slice_t: Optimized slice type for high-performance C API operations
 * This struct is ABI-compatible with rocksdb::Slice for zero-copy interop.
 * Used by slice iterator functions and batched operations. */
typedef struct rocksdb_slice_t {
  const char* data;
  size_t size;
} rocksdb_slice_t;
typedef struct rocksdb_flushjobinfo_t rocksdb_flushjobinfo_t;
typedef struct rocksdb_compactionjobinfo_t rocksdb_compactionjobinfo_t;
typedef struct rocksdb_subcompactionjobinfo_t rocksdb_subcompactionjobinfo_t;
typedef struct rocksdb_externalfileingestioninfo_t
    rocksdb_externalfileingestioninfo_t;
typedef struct rocksdb_table_properties_t rocksdb_table_properties_t;
typedef struct rocksdb_compaction_job_stats_t rocksdb_compaction_job_stats_t;
typedef struct rocksdb_compaction_file_info_t rocksdb_compaction_file_info_t;
typedef struct rocksdb_blob_file_addition_info_t
    rocksdb_blob_file_addition_info_t;
typedef struct rocksdb_blob_file_garbage_info_t
    rocksdb_blob_file_garbage_info_t;
typedef struct rocksdb_eventlistener_t rocksdb_eventlistener_t;
typedef struct rocksdb_writestallinfo_t rocksdb_writestallinfo_t;
typedef struct rocksdb_writestallcondition_t rocksdb_writestallcondition_t;
typedef struct rocksdb_memtableinfo_t rocksdb_memtableinfo_t;

// Remote Compaction typedef
typedef struct rocksdb_compactionservice_scheduleresponse_t
    rocksdb_compactionservice_scheduleresponse_t;
typedef struct rocksdb_compactionservice_jobinfo_t
    rocksdb_compactionservice_jobinfo_t;
typedef struct rocksdb_compactionservice_t rocksdb_compactionservice_t;
typedef struct rocksdb_compaction_service_options_override_t
    rocksdb_compaction_service_options_override_t;
typedef struct rocksdb_open_and_compact_options_t
    rocksdb_open_and_compact_options_t;
typedef rocksdb_compactionservice_scheduleresponse_t* (
    *rocksdb_compaction_service_schedule_cb)(
    void* state, const rocksdb_compactionservice_jobinfo_t* info,
    const char* compaction_service_input, size_t input_len);

typedef int (*rocksdb_compaction_service_wait_cb)(void* state,
                                                  const char* scheduled_job_id,
                                                  char** result,
                                                  size_t* result_len);

typedef void (*rocksdb_compaction_service_cancel_awaiting_jobs_cb)(void* state);

typedef void (*rocksdb_compaction_service_on_installation_cb)(
    void* state, const char* scheduled_job_id, int status);

typedef void (*rocksdb_create_backup_options_progress_cb)(void* state);
typedef unsigned char (*rocksdb_create_backup_options_exclude_files_cb)(
    void* state, const char* relative_file, size_t relative_file_len);
typedef unsigned char (*rocksdb_readoptions_table_filter_cb)(
    void* state, const rocksdb_table_properties_t* table_properties);
typedef void (*rocksdb_walfilter_column_family_log_number_map_cb)(
    void* state, const uint32_t* column_family_ids, const uint64_t* log_numbers,
    size_t column_family_log_number_count,
    const char* const* column_family_names,
    const size_t* column_family_name_lengths,
    const uint32_t* column_family_name_ids, size_t column_family_name_count);
typedef int (*rocksdb_walfilter_log_record_found_cb)(
    void* state, unsigned long long log_number, const char* log_file_name,
    size_t log_file_name_len, const rocksdb_writebatch_t* batch,
    rocksdb_writebatch_t* new_batch, unsigned char* batch_changed);

/* DB operations */

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open(
    const rocksdb_options_t* options, const char* name, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_with_ttl(
    const rocksdb_options_t* options, const char* name, int ttl, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_for_read_only(
    const rocksdb_options_t* options, const char* name,
    unsigned char error_if_wal_file_exists, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_as_secondary(
    const rocksdb_options_t* options, const char* name,
    const char* secondary_path, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_backup_engine_t* rocksdb_backup_engine_open(
    const rocksdb_options_t* options, const char* path, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_backup_engine_t*
rocksdb_backup_engine_open_opts(const rocksdb_backup_engine_options_t* options,
                                rocksdb_env_t* env, char** errptr);

extern ROCKSDB_LIBRARY_API void
rocksdb_create_backup_options_set_progress_callback(
    rocksdb_create_backup_options_t* options, void* state,
    rocksdb_create_backup_options_progress_cb callback);

extern ROCKSDB_LIBRARY_API void
rocksdb_create_backup_options_set_exclude_files_callback(
    rocksdb_create_backup_options_t* options, void* state,
    rocksdb_create_backup_options_exclude_files_cb callback);

extern ROCKSDB_LIBRARY_API void
rocksdb_backup_engine_create_new_backup_with_options(
    rocksdb_backup_engine_t* be, rocksdb_t* db,
    const rocksdb_create_backup_options_t* options, uint32_t* backup_id,
    char** errptr);

extern ROCKSDB_LIBRARY_API void
rocksdb_backup_engine_create_new_backup_with_metadata(
    rocksdb_backup_engine_t* be, rocksdb_t* db,
    const rocksdb_create_backup_options_t* options, const char* app_metadata,
    size_t app_metadata_len, uint32_t* backup_id, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_backup_engine_restore_db_from_backup(
    rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const rocksdb_restore_options_t* restore_options, const uint32_t backup_id,
    char** errptr);

extern ROCKSDB_LIBRARY_API const rocksdb_backup_engine_info_t*
rocksdb_backup_engine_get_backup_info(rocksdb_backup_engine_t* be);

extern ROCKSDB_LIBRARY_API int rocksdb_backup_engine_info_count(
    const rocksdb_backup_engine_info_t* info);

extern ROCKSDB_LIBRARY_API int64_t rocksdb_backup_engine_info_timestamp(
    const rocksdb_backup_engine_info_t* info, int index);

extern ROCKSDB_LIBRARY_API uint32_t rocksdb_backup_engine_info_backup_id(
    const rocksdb_backup_engine_info_t* info, int index);

extern ROCKSDB_LIBRARY_API uint64_t rocksdb_backup_engine_info_size(
    const rocksdb_backup_engine_info_t* info, int index);

extern ROCKSDB_LIBRARY_API uint32_t rocksdb_backup_engine_info_number_files(
    const rocksdb_backup_engine_info_t* info, int index);

extern ROCKSDB_LIBRARY_API const char* rocksdb_backup_engine_info_app_metadata(
    const rocksdb_backup_engine_info_t* info, int index, size_t* len);

extern ROCKSDB_LIBRARY_API void rocksdb_backup_engine_close(
    rocksdb_backup_engine_t* be);

#include "c_api_gen/c_generated_db_simple_subset.h.inc"
extern ROCKSDB_LIBRARY_API void rocksdb_verify_checksum_with_options(
    rocksdb_t* db, const rocksdb_readoptions_t* options, char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_verify_file_checksums_with_options(
    rocksdb_t* db, const rocksdb_readoptions_t* options, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_increase_full_history_ts_low(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* ts_low, size_t ts_lowlen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_full_history_ts_low(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    size_t* ts_lowlen, char** errptr);

/* BackupEngineOptions */

extern ROCKSDB_LIBRARY_API rocksdb_backup_engine_options_t*
rocksdb_backup_engine_options_create(const char* backup_dir);

extern ROCKSDB_LIBRARY_API void rocksdb_backup_engine_options_set_env(
    rocksdb_backup_engine_options_t* options, rocksdb_env_t* env);

extern ROCKSDB_LIBRARY_API void
rocksdb_backup_engine_options_set_backup_rate_limiter(
    rocksdb_backup_engine_options_t* options, rocksdb_ratelimiter_t* limiter);

extern ROCKSDB_LIBRARY_API void
rocksdb_backup_engine_options_set_restore_rate_limiter(
    rocksdb_backup_engine_options_t* options, rocksdb_ratelimiter_t* limiter);

/* Checkpoint */

extern ROCKSDB_LIBRARY_API rocksdb_checkpoint_t*
rocksdb_checkpoint_object_create(rocksdb_t* db, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_export_import_files_metadata_t*
rocksdb_checkpoint_export_column_family(
    rocksdb_checkpoint_t* checkpoint,
    rocksdb_column_family_handle_t* column_family, const char* export_dir,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_and_trim_history(
    const rocksdb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char* trim_ts,
    size_t trim_tslen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_column_families(
    const rocksdb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_column_families_with_ttl(
    const rocksdb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, const int* ttls,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t*
rocksdb_open_for_read_only_column_families(
    const rocksdb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    unsigned char error_if_wal_file_exists, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_open_as_secondary_column_families(
    const rocksdb_options_t* options, const char* name,
    const char* secondary_path, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr);

extern ROCKSDB_LIBRARY_API char** rocksdb_list_column_families(
    const rocksdb_options_t* options, const char* name, size_t* lencf,
    char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_list_column_families_destroy(
    char** list, size_t len);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t*
rocksdb_create_column_family(rocksdb_t* db,
                             const rocksdb_options_t* column_family_options,
                             const char* column_family_name, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t**
rocksdb_create_column_families(rocksdb_t* db,
                               const rocksdb_options_t* column_family_options,
                               int num_column_families,
                               const char* const* column_family_names,
                               size_t* lencfs, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_create_column_families_destroy(
    rocksdb_column_family_handle_t** list);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t*
rocksdb_create_column_family_with_import(
    rocksdb_t* db, rocksdb_options_t* column_family_options,
    const char* column_family_name,
    rocksdb_import_column_family_options_t* import_options,
    rocksdb_export_import_files_metadata_t* metadata, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t*
rocksdb_create_column_family_with_ttl(
    rocksdb_t* db, const rocksdb_options_t* column_family_options,
    const char* column_family_name, int ttl, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t*
rocksdb_get_default_column_family_handle(rocksdb_t* db);

extern ROCKSDB_LIBRARY_API void rocksdb_column_family_handle_destroy(
    rocksdb_column_family_handle_t*);

extern ROCKSDB_LIBRARY_API void rocksdb_close(rocksdb_t* db);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
extern ROCKSDB_LIBRARY_API char* rocksdb_get(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_with_metadata(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, size_t* vallen, char** timestamp, size_t* timestamp_len,
    unsigned char* newer_version_present, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_with_ts(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, size_t* vallen, char** ts, size_t* tslen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_cf_with_metadata(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** timestamp, size_t* timestamp_len,
    unsigned char* newer_version_present, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_get_cf_with_ts(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** ts, size_t* tslen, char** errptr);

/**
 * Returns a malloc() buffer with the DB identity, assigning the length to
 * *id_len. Returns NULL if an error occurred.
 */
extern ROCKSDB_LIBRARY_API char* rocksdb_get_db_identity(rocksdb_t* db,
                                                         size_t* id_len);

// if values_list[i] == NULL and errs[i] == NULL,
// then we got status.IsNotFound(), which we will not return.
// all errors except status status.ok() and status.IsNotFound() are returned.
//
// errs, values_list and values_list_sizes must be num_keys in length,
// allocated by the caller.
// errs is a list of strings as opposed to the conventional one error,
// where errs[i] is the status for retrieval of keys_list[i].
// each non-NULL errs entry is a malloc()ed, null terminated string.
// each non-NULL values_list entry is a malloc()ed array, with
// the length for each stored in values_list_sizes[i].
extern ROCKSDB_LIBRARY_API void rocksdb_multi_get(
    rocksdb_t* db, const rocksdb_readoptions_t* options, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_multi_get_with_metadata(
    rocksdb_t* db, const rocksdb_readoptions_t* options, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes, char** timestamp_list,
    size_t* timestamp_list_sizes, unsigned char* newer_version_present,
    char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_multi_get_with_ts(
    rocksdb_t* db, const rocksdb_readoptions_t* options, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes, char** timestamp_list,
    size_t* timestamp_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_multi_get_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_multi_get_cf_with_metadata(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** timestamp_list,
    size_t* timestamp_list_sizes, unsigned char* newer_version_present,
    char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_multi_get_cf_with_ts(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** timestamps_list,
    size_t* timestamps_list_sizes, char** errs);

// The MultiGet API that improves performance by batching operations
// in the read path for greater efficiency. Currently, only the block based
// table format with full filters are supported. Other table formats such
// as plain table, block based table with block based filters and
// partitioned indexes will still work, but will not get any performance
// benefits.
//
// Note that all the keys passed to this API are restricted to a single
// column family.
//
// Parameters -
// db - the RocksDB instance.
// options - ReadOptions
// column_family - ColumnFamilyHandle* that the keys belong to. All the keys
//                 passed to the API are restricted to a single column family
// num_keys - Number of keys to lookup
// keys_list - Pointer to C style array of keys with num_keys elements
// keys_list_sizes - Pointer to C style array of the size of corresponding key
//   in key_list with num_keys elements.
// values - Pointer to C style array of PinnableSlices with num_keys elements
// statuses - Pointer to C style array of Status with num_keys elements
// sorted_input - If true, it means the input keys are already sorted by key
//                order, so the MultiGet() API doesn't have to sort them
//                again. If false, the keys will be copied and sorted
//                internally by the API - the input array will not be
//                modified
extern ROCKSDB_LIBRARY_API void rocksdb_batched_multi_get_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    rocksdb_pinnableslice_t** values, char** errs, const bool sorted_input);

/* Batched MultiGet with slice array: Takes rocksdb_slice_t array directly,
 * avoiding key conversion. faster than rocksdb_batched_multi_get_cf for
 * operations with many keys. Eliminates overhead of converting keys from
 * separate pointer+size arrays to Slice objects. */
extern ROCKSDB_LIBRARY_API void rocksdb_batched_multi_get_cf_slice(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, size_t num_keys,
    const rocksdb_slice_t* keys_list, rocksdb_pinnableslice_t** values,
    char** errs, const bool sorted_input);

/*
 * Result states returned by rocksdb_pinnable_multi_get_result().
 */
enum {
  rocksdb_pinnable_multi_get_not_found = 0,
  rocksdb_pinnable_multi_get_found = 1,
  rocksdb_pinnable_multi_get_error = 2,
  rocksdb_pinnable_multi_get_out_of_bounds = 3,
};

/*
 * Runs a batched MultiGet for one column family and returns one owner for all
 * pinned values and error messages. Result indexes match the input key order,
 * including duplicate keys.
 *
 * If sorted_input is nonzero, keys must be sorted using the column family's
 * comparator.
 */
extern ROCKSDB_LIBRARY_API rocksdb_pinnable_multi_get_t*
rocksdb_batched_multi_get_pinned_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, size_t num_keys,
    const rocksdb_slice_t* keys, unsigned char sorted_input);

extern ROCKSDB_LIBRARY_API size_t
rocksdb_pinnable_multi_get_count(const rocksdb_pinnable_multi_get_t* multi_get);

/*
 * Returns the result state at index. For a found value, value and value_size
 * describe borrowed value bytes. For an error, error and error_size describe a
 * borrowed null-terminated error message. The borrowed data remains valid until
 * rocksdb_pinnable_multi_get_destroy() is called. The batch must be destroyed
 * before closing the DB used to create it.
 *
 * Unused outputs are set to NULL and zero. An index greater than or equal to
 * rocksdb_pinnable_multi_get_count() returns
 * rocksdb_pinnable_multi_get_out_of_bounds.
 */
extern ROCKSDB_LIBRARY_API int rocksdb_pinnable_multi_get_result(
    const rocksdb_pinnable_multi_get_t* multi_get, size_t index,
    const char** value, size_t* value_size, const char** error,
    size_t* error_size);

extern ROCKSDB_LIBRARY_API void rocksdb_pinnable_multi_get_destroy(
    rocksdb_pinnable_multi_get_t* multi_get);

// The value is only allocated (using malloc) and returned if it is found and
// value_found isn't NULL. In that case the user is responsible for freeing it.
extern ROCKSDB_LIBRARY_API unsigned char rocksdb_key_may_exist(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t key_len, char** value, size_t* val_len, const char* timestamp,
    size_t timestamp_len, unsigned char* value_found);

// The value is only allocated (using malloc) and returned if it is found and
// value_found isn't NULL. In that case the user is responsible for freeing it.
extern ROCKSDB_LIBRARY_API unsigned char rocksdb_key_may_exist_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t key_len, char** value, size_t* val_len, const char* timestamp,
    size_t timestamp_len, unsigned char* value_found);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t* rocksdb_create_iterator(
    rocksdb_t* db, const rocksdb_readoptions_t* options);

extern ROCKSDB_LIBRARY_API rocksdb_wal_iterator_t* rocksdb_get_updates_since(
    rocksdb_t* db, uint64_t seq_number,
    const rocksdb_wal_readoptions_t* options, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_wal_files_t* rocksdb_get_sorted_wal_files(
    rocksdb_t* db, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_wal_file_t* rocksdb_get_current_wal_file(
    rocksdb_t* db, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t* rocksdb_create_iterator_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family);

extern ROCKSDB_LIBRARY_API void rocksdb_create_iterators(
    rocksdb_t* db, rocksdb_readoptions_t* opts,
    rocksdb_column_family_handle_t** column_families,
    rocksdb_iterator_t** iterators, size_t size, char** errptr);

extern ROCKSDB_LIBRARY_API const rocksdb_snapshot_t* rocksdb_create_snapshot(
    rocksdb_t* db);

extern ROCKSDB_LIBRARY_API void rocksdb_release_snapshot(
    rocksdb_t* db, const rocksdb_snapshot_t* snapshot);

/* Returns NULL if property name is unknown.
   Else returns a pointer to a malloc()-ed null-terminated value. */
extern ROCKSDB_LIBRARY_API char* rocksdb_property_value(rocksdb_t* db,
                                                        const char* propname);
/* returns 0 on success, -1 otherwise */
extern ROCKSDB_LIBRARY_API int rocksdb_property_int(rocksdb_t* db,
                                                    const char* propname,
                                                    uint64_t* out_val);

/* returns 0 on success, -1 otherwise */
extern ROCKSDB_LIBRARY_API int rocksdb_property_int_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* propname, uint64_t* out_val);

extern ROCKSDB_LIBRARY_API char* rocksdb_property_value_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* propname);

extern ROCKSDB_LIBRARY_API void rocksdb_approximate_sizes(
    rocksdb_t* db, int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_approximate_sizes_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes, char** errptr);

enum {
  rocksdb_size_approximation_flags_none = 0,
  rocksdb_size_approximation_flags_include_memtable = 1 << 0,
  rocksdb_size_approximation_flags_include_files = 1 << 1,
  rocksdb_size_approximation_flags_include_blob_files = 1 << 2,
};

extern ROCKSDB_LIBRARY_API void rocksdb_approximate_sizes_cf_with_flags(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint8_t include_flags, uint64_t* sizes,
    char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_approximate_sizes_with_options(
    rocksdb_t* db, const rocksdb_size_approximation_options_t* options,
    int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_approximate_sizes_cf_with_options(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const rocksdb_size_approximation_options_t* options, int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_range(rocksdb_t* db,
                                                      const char* start_key,
                                                      size_t start_key_len,
                                                      const char* limit_key,
                                                      size_t limit_key_len);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_range_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len);

extern ROCKSDB_LIBRARY_API void rocksdb_suggest_compact_range(
    rocksdb_t* db, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_suggest_compact_range_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_range_opt(
    rocksdb_t* db, rocksdb_compactoptions_t* opt, const char* start_key,
    size_t start_key_len, const char* limit_key, size_t limit_key_len);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_range_cf_opt(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    rocksdb_compactoptions_t* opt, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_files(
    rocksdb_t* db, const rocksdb_compaction_options_t* options,
    const char* const* input_file_names, size_t num_input_file_names,
    int output_level, int output_path_id, char*** output_file_names,
    size_t* num_output_file_names,
    rocksdb_compactionjobinfo_t* compaction_job_info, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_files_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const rocksdb_compaction_options_t* options,
    const char* const* input_file_names, size_t num_input_file_names,
    int output_level, int output_path_id, char*** output_file_names,
    size_t* num_output_file_names,
    rocksdb_compactionjobinfo_t* compaction_job_info, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_compact_files_output_file_names_destroy(
    char** output_file_names, size_t num_output_file_names);

extern ROCKSDB_LIBRARY_API const rocksdb_livefiles_t* rocksdb_livefiles(
    rocksdb_t* db);

extern ROCKSDB_LIBRARY_API void rocksdb_flush_cfs(
    rocksdb_t* db, const rocksdb_flushoptions_t* options,
    rocksdb_column_family_handle_t** column_family, int num_column_families,
    char** errptr);

/* Management operations */

/* Iterator */

extern ROCKSDB_LIBRARY_API rocksdb_writebatch_t* rocksdb_wal_iter_get_batch(
    const rocksdb_wal_iterator_t* iter, uint64_t* seq);
enum {
  rocksdb_wal_file_type_archived_log = 0,
  rocksdb_wal_file_type_alive_log = 1,
};
extern ROCKSDB_LIBRARY_API void rocksdb_wal_readoptions_set_verify_checksums(
    rocksdb_wal_readoptions_t* options, unsigned char verify_checksums);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_wal_readoptions_get_verify_checksums(
    rocksdb_wal_readoptions_t* options);
extern ROCKSDB_LIBRARY_API const char* rocksdb_wal_file_path_name(
    const rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_wal_file_log_number(const rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API int rocksdb_wal_file_type(
    const rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_wal_file_start_sequence(const rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_wal_file_size_file_bytes(const rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API void rocksdb_wal_file_destroy(
    rocksdb_wal_file_t* file);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_wal_files_count(const rocksdb_wal_files_t* files);
extern ROCKSDB_LIBRARY_API const rocksdb_wal_file_t*
rocksdb_wal_files_get_wal_file(const rocksdb_wal_files_t* files, size_t index);

/* Write batch */

extern ROCKSDB_LIBRARY_API rocksdb_writebatch_t* rocksdb_writebatch_create_from(
    const char* rep, size_t size);
extern ROCKSDB_LIBRARY_API rocksdb_writebatch_t*
rocksdb_writebatch_create_with_params(size_t reserved_bytes, size_t max_bytes,
                                      size_t protection_bytes_per_key,
                                      size_t default_cf_ts_sz);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_putv(
    rocksdb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_putv_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_mergev(
    rocksdb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_mergev_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_deletev(
    rocksdb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_deletev_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_delete_rangev(
    rocksdb_writebatch_t* b, int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_delete_rangev_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_iterate(
    rocksdb_writebatch_t*, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_iterate_ld(
    rocksdb_writebatch_t*, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen),
    void (*log_data)(void*, const char* blob, size_t blob_len));
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_iterate_cf(
    rocksdb_writebatch_t*, void* state,
    void (*put_cf)(void*, uint32_t cfid, const char* k, size_t klen,
                   const char* v, size_t vlen),
    void (*deleted_cf)(void*, uint32_t cfid, const char* k, size_t klen),
    void (*merge_cf)(void*, uint32_t cfid, const char* k, size_t klen,
                     const char* v, size_t vlen));
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_iterate_cf_ld(
    rocksdb_writebatch_t*, void* state,
    void (*put_cf)(void*, uint32_t cfid, const char* k, size_t klen,
                   const char* v, size_t vlen),
    void (*deleted_cf)(void*, uint32_t cfid, const char* k, size_t klen),
    void (*merge_cf)(void*, uint32_t cfid, const char* k, size_t klen,
                     const char* v, size_t vlen),
    void (*log_data)(void*, const char* blob, size_t blob_len));
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_update_timestamps(
    rocksdb_writebatch_t* wb, const char* ts, size_t tslen, void* state,
    size_t (*get_ts_size)(void*, uint32_t), char** errptr);

/* Write batch with index */

extern ROCKSDB_LIBRARY_API rocksdb_writebatch_wi_t*
rocksdb_writebatch_wi_create(size_t reserved_bytes,
                             unsigned char overwrite_keys);
extern ROCKSDB_LIBRARY_API rocksdb_writebatch_wi_t*
rocksdb_writebatch_wi_create_with_params(
    rocksdb_comparator_t* backup_index_comparator, size_t reserved_bytes,
    unsigned char overwrite_key, size_t max_bytes,
    size_t protection_bytes_per_key);
extern ROCKSDB_LIBRARY_API int rocksdb_writebatch_wi_count(
    rocksdb_writebatch_wi_t* b);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_putv(
    rocksdb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_putv_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_mergev(
    rocksdb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_mergev_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_deletev(
    rocksdb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_deletev_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes);
// DO NOT USE - rocksdb_writebatch_wi_delete_rangev is not yet supported
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_delete_rangev(
    rocksdb_writebatch_wi_t* b, int num_keys,
    const char* const* start_keys_list, const size_t* start_keys_list_sizes,
    const char* const* end_keys_list, const size_t* end_keys_list_sizes);
// DO NOT USE - rocksdb_writebatch_wi_delete_rangev_cf is not yet supported
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_delete_rangev_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_iterate(
    rocksdb_writebatch_wi_t* b, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
extern ROCKSDB_LIBRARY_API const char* rocksdb_writebatch_wi_data(
    rocksdb_writebatch_wi_t* b, size_t* size);
extern ROCKSDB_LIBRARY_API char* rocksdb_writebatch_wi_get_from_batch(
    rocksdb_writebatch_wi_t* wbwi, const rocksdb_options_t* options,
    const char* key, size_t keylen, size_t* vallen, char** errptr);
extern ROCKSDB_LIBRARY_API char* rocksdb_writebatch_wi_get_from_batch_cf(
    rocksdb_writebatch_wi_t* wbwi, const rocksdb_options_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);
extern ROCKSDB_LIBRARY_API char* rocksdb_writebatch_wi_get_from_batch_and_db(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_t* db,
    const rocksdb_readoptions_t* options, const char* key, size_t keylen,
    size_t* vallen, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_writebatch_wi_get_pinned_from_batch_and_db(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_t* db,
    const rocksdb_readoptions_t* options, const char* key, size_t keylen,
    char** errptr);
extern ROCKSDB_LIBRARY_API char* rocksdb_writebatch_wi_get_from_batch_and_db_cf(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_writebatch_wi_get_pinned_from_batch_and_db_cf(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_write_writebatch_wi(
    rocksdb_t* db, const rocksdb_writeoptions_t* options,
    rocksdb_writebatch_wi_t* wbwi, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_writebatch_wi_create_iterator_with_base(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_iterator_t* base_iterator);
extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_writebatch_wi_create_iterator_with_base_readopts(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_iterator_t* base_iterator,
    const rocksdb_readoptions_t* options);
extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_writebatch_wi_create_iterator_with_base_cf(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_iterator_t* base_iterator,
    rocksdb_column_family_handle_t* cf);
extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_writebatch_wi_create_iterator_with_base_cf_readopts(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_iterator_t* base_iterator,
    rocksdb_column_family_handle_t* cf, const rocksdb_readoptions_t* options);
extern ROCKSDB_LIBRARY_API void rocksdb_writebatch_wi_update_timestamps(
    rocksdb_writebatch_wi_t* wbwi, const char* ts, size_t tslen, void* state,
    size_t (*get_ts_size)(void*, uint32_t), char** errptr);
/* Options utils */

// Load the latest rocksdb options from the specified db_path.
//
// On success, num_column_families will be updated with a non-zero
// number indicating the number of column families.
// The returned db_options, column_family_names, and column_family_options
// should be released via rocksdb_load_latest_options_destroy().
//
// On error, a non-null errptr that includes the error message will be
// returned.  db_options, column_family_names, and column_family_options
// will be set to NULL.
extern ROCKSDB_LIBRARY_API void rocksdb_load_latest_options(
    const char* db_path, rocksdb_env_t* env, bool ignore_unknown_options,
    rocksdb_cache_t* cache, rocksdb_options_t** db_options,
    size_t* num_column_families, char*** column_family_names,
    rocksdb_options_t*** column_family_options, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_load_latest_options_destroy(
    rocksdb_options_t* db_options, char** list_column_family_names,
    rocksdb_options_t** list_column_family_options, size_t len);

/* Block based table options */

#include "c_api_gen/c_generated_block_based_options_subset.h.inc"
extern ROCKSDB_LIBRARY_API void rocksdb_block_based_options_set_filter_policy(
    rocksdb_block_based_table_options_t* options,
    rocksdb_filterpolicy_t* filter_policy);
extern ROCKSDB_LIBRARY_API void rocksdb_block_based_options_set_block_cache(
    rocksdb_block_based_table_options_t* options, rocksdb_cache_t* block_cache);
extern ROCKSDB_LIBRARY_API void
rocksdb_block_based_options_set_user_defined_index_factory_from_string(
    rocksdb_block_based_table_options_t* options, const char* value,
    size_t value_len, char** errptr);
extern ROCKSDB_LIBRARY_API void
rocksdb_block_based_options_clear_user_defined_index_factory(
    rocksdb_block_based_table_options_t* options);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_block_based_options_get_user_defined_index_factory_name(
    const rocksdb_block_based_table_options_t* options, size_t* name_len);
extern ROCKSDB_LIBRARY_API void
rocksdb_block_based_options_set_use_udi_as_primary_index(
    rocksdb_block_based_table_options_t* opt, unsigned char v);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_block_based_options_get_use_udi_as_primary_index(
    rocksdb_block_based_table_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_block_based_options_set_fail_if_no_udi_on_open(
    rocksdb_block_based_table_options_t* opt, unsigned char v);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_block_based_options_get_fail_if_no_udi_on_open(
    rocksdb_block_based_table_options_t* opt);
/* EXPERIMENTAL: selects which index tiers an SST is written with and which one
 * reads use by default. Takes a rocksdb_block_based_table_index_mode_* value.
 * An explicit index_mode wins over the deprecated
 * use_udi_as_primary_index / fail_if_no_udi_on_open setters regardless of the
 * order they are called in. */
extern ROCKSDB_LIBRARY_API void rocksdb_block_based_options_set_index_mode(
    rocksdb_block_based_table_options_t* opt, int v);
enum {
  rocksdb_block_based_table_index_mode_standard_only = 0,
  rocksdb_block_based_table_index_mode_standard_default = 1,
  rocksdb_block_based_table_index_mode_custom_default = 2,
  rocksdb_block_based_table_index_mode_custom_only = 3,
  rocksdb_block_based_table_index_mode_standard_required = 4,
};
enum {
  rocksdb_block_based_table_index_type_binary_search = 0,
  rocksdb_block_based_table_index_type_hash_search = 1,
  rocksdb_block_based_table_index_type_two_level_index_search = 2,
};
enum {
  rocksdb_block_based_table_data_block_index_type_binary_search = 0,
  rocksdb_block_based_table_data_block_index_type_binary_search_and_hash = 1,
};
enum {
  rocksdb_block_based_table_index_block_search_type_binary = 0,
  rocksdb_block_based_table_index_block_search_type_interpolation = 1,
  rocksdb_block_based_table_index_block_search_type_auto = 2,
};
// rocksdb_block_based_options_set_hash_index_allow_collision()
// is removed since BlockBasedTableOptions.hash_index_allow_collision()
// is removed
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_block_based_table_factory(
    rocksdb_options_t* opt, rocksdb_block_based_table_options_t* table_options);
enum {
  rocksdb_block_based_k_fallback_pinning_tier = 0,
  rocksdb_block_based_k_none_pinning_tier = 1,
  rocksdb_block_based_k_flush_and_similar_pinning_tier = 2,
  rocksdb_block_based_k_all_pinning_tier = 3,
};
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_write_buffer_manager(
    rocksdb_options_t* opt, rocksdb_write_buffer_manager_t* wbm);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_sst_file_manager(
    rocksdb_options_t* opt, rocksdb_sst_file_manager_t* sfm);

/* Flush job info */
#include "c_api_gen/c_generated_jobinfo_auto.h.inc"
#include "c_api_gen/c_generated_jobinfo_metadata_subset.h.inc"
#include "c_api_gen/c_generated_metadata_structs_auto.h.inc"
/* Compatibility aliases for the captured compaction pool selection. */
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionjobinfo_use_separate_remote_compaction_pool(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_subcompactionjobinfo_use_separate_remote_compaction_pool(
    const rocksdb_subcompactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API void rocksdb_reset_status(
    rocksdb_status_ptr_t* status_ptr);
extern ROCKSDB_LIBRARY_API void rocksdb_status_ptr_get_error(
    rocksdb_status_ptr_t* status, char** errptr);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_table_properties_has_referenced_blob_bytes(
    const rocksdb_table_properties_t* props);
/* Returns zero when the property is unknown; use the has accessor to
 * distinguish unknown from a known zero value. */
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_referenced_blob_bytes(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_table_properties_user_collected_properties_count(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_user_collected_properties_key_at(
    const rocksdb_table_properties_t* props, size_t pos, size_t* key_len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_user_collected_properties_value_at(
    const rocksdb_table_properties_t* props, size_t pos, size_t* value_len);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_table_properties_readable_properties_count(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_readable_properties_key_at(
    const rocksdb_table_properties_t* props, size_t pos, size_t* key_len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_readable_properties_value_at(
    const rocksdb_table_properties_t* props, size_t pos, size_t* value_len);
extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_flushjobinfo_table_properties(const rocksdb_flushjobinfo_t* info);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_flushjobinfo_blob_file_addition_infos_count(
    const rocksdb_flushjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_blob_file_addition_info_t*
rocksdb_flushjobinfo_blob_file_addition_info_at(
    const rocksdb_flushjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API const char* rocksdb_compactionjobinfo_input_file_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos, size_t* size);
extern ROCKSDB_LIBRARY_API const char* rocksdb_compactionjobinfo_output_file_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos, size_t* size);
extern ROCKSDB_LIBRARY_API const rocksdb_compaction_job_stats_t*
rocksdb_compactionjobinfo_stats(const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_compactionjobinfo_input_file_infos_count(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_compaction_file_info_t*
rocksdb_compactionjobinfo_input_file_info_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_compactionjobinfo_output_file_infos_count(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_compaction_file_info_t*
rocksdb_compactionjobinfo_output_file_info_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_compactionjobinfo_table_properties_count(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionjobinfo_table_properties_key_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos, size_t* key_len);
extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_compactionjobinfo_table_properties_value_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_compactionjobinfo_table_properties_for_file(
    const rocksdb_compactionjobinfo_t* info, const char* file_name,
    size_t file_name_len);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_compactionjobinfo_blob_file_addition_infos_count(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_blob_file_addition_info_t*
rocksdb_compactionjobinfo_blob_file_addition_info_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_compactionjobinfo_blob_file_garbage_infos_count(
    const rocksdb_compactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_blob_file_garbage_info_t*
rocksdb_compactionjobinfo_blob_file_garbage_info_at(
    const rocksdb_compactionjobinfo_t* info, size_t pos);
extern ROCKSDB_LIBRARY_API const rocksdb_compaction_job_stats_t*
rocksdb_subcompactionjobinfo_stats(const rocksdb_subcompactionjobinfo_t* info);
extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_externalfileingestioninfo_table_properties(
    const rocksdb_externalfileingestioninfo_t* info);

/* Event listener */

typedef void (*on_flush_begin_cb)(void*, rocksdb_t*,
                                  const rocksdb_flushjobinfo_t*);
typedef void (*on_flush_completed_cb)(void*, rocksdb_t*,
                                      const rocksdb_flushjobinfo_t*);
typedef void (*on_compaction_begin_cb)(void*, rocksdb_t*,
                                       const rocksdb_compactionjobinfo_t*);
typedef void (*on_compaction_completed_cb)(void*, rocksdb_t*,
                                           const rocksdb_compactionjobinfo_t*);
typedef void (*on_subcompaction_begin_cb)(
    void*, const rocksdb_subcompactionjobinfo_t*);
typedef void (*on_subcompaction_completed_cb)(
    void*, const rocksdb_subcompactionjobinfo_t*);
typedef void (*on_external_file_ingested_cb)(
    void*, rocksdb_t*, const rocksdb_externalfileingestioninfo_t*);
typedef void (*on_background_error_cb)(void*, uint32_t, rocksdb_status_ptr_t*);
typedef void (*on_stall_conditions_changed_cb)(void*,
                                               const rocksdb_writestallinfo_t*);
typedef void (*rocksdb_logger_logv_cb)(void*, uint32_t log_level, const char*);
typedef void (*on_memtable_sealed_cb)(void*, const rocksdb_memtableinfo_t*);
extern ROCKSDB_LIBRARY_API rocksdb_eventlistener_t*
rocksdb_eventlistener_create(
    void* state_, void (*destructor_)(void*), on_flush_begin_cb on_flush_begin,
    on_flush_completed_cb on_flush_completed,
    on_compaction_begin_cb on_compaction_begin,
    on_compaction_completed_cb on_compaction_completed,
    on_subcompaction_begin_cb on_subcompaction_begin,
    on_subcompaction_completed_cb on_subcompaction_completed,
    on_external_file_ingested_cb on_external_file_ingested,
    on_background_error_cb on_background_error,
    on_stall_conditions_changed_cb on_stall_conditions_changed,
    on_memtable_sealed_cb on_memtable_sealed);
extern ROCKSDB_LIBRARY_API void rocksdb_eventlistener_destroy(
    rocksdb_eventlistener_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_add_eventlistener(
    rocksdb_options_t*, rocksdb_eventlistener_t*);

/* Cuckoo table options */

#include "c_api_gen/c_generated_cuckoo_options_subset.h.inc"
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_cuckoo_table_factory(
    rocksdb_options_t* opt, rocksdb_cuckoo_table_options_t* table_options);

/* Options */
extern ROCKSDB_LIBRARY_API void rocksdb_set_options(rocksdb_t* db, int count,
                                                    const char* const keys[],
                                                    const char* const values[],
                                                    char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_set_db_options(
    rocksdb_t* db, int count, const char* const keys[],
    const char* const values[], char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_set_options_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* handle, int count,
    const char* const keys[], const char* const values[], char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_options_t* rocksdb_options_create_copy(
    rocksdb_options_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_compaction_filter(
    rocksdb_options_t*, rocksdb_compactionfilter_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_compaction_filter_factory(
    rocksdb_options_t*, rocksdb_compactionfilterfactory_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_comparator(
    rocksdb_options_t*, rocksdb_comparator_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_merge_operator(
    rocksdb_options_t*, rocksdb_mergeoperator_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_wal_filter(
    rocksdb_options_t*, rocksdb_walfilter_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_clear_wal_filter(
    rocksdb_options_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_uint64add_merge_operator(
    rocksdb_options_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_compression_per_level(
    rocksdb_options_t* opt, const int* level_values, size_t num_levels);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_db_paths(
    rocksdb_options_t*, const rocksdb_dbpath_t** path_values, size_t num_paths);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_cf_paths(
    rocksdb_options_t*, const rocksdb_dbpath_t** path_values, size_t num_paths);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_env(rocksdb_options_t*,
                                                        rocksdb_env_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_info_log(rocksdb_options_t*,
                                                             rocksdb_logger_t*);
extern ROCKSDB_LIBRARY_API rocksdb_logger_t* rocksdb_options_get_info_log(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API rocksdb_logger_t*
rocksdb_logger_create_stderr_logger(int log_level, const char* prefix);
extern ROCKSDB_LIBRARY_API rocksdb_logger_t*
rocksdb_logger_create_callback_logger(int log_level,
                                      void (*)(void* priv, unsigned lev,
                                               char* msg, size_t len),
                                      void* priv);

/* File Checksum Gen Factory */
extern ROCKSDB_LIBRARY_API rocksdb_file_checksum_gen_factory_t*
rocksdb_file_checksum_gen_crc32c_factory_create(void);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_file_checksum_gen_factory(
    rocksdb_options_t*, rocksdb_file_checksum_gen_factory_t*);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_checksum_handoff_file_types_clear(rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void rocksdb_options_checksum_handoff_file_types_add(
    rocksdb_options_t* opt, int file_type);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_checksum_handoff_file_types_remove(rocksdb_options_t* opt,
                                                   int file_type);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_options_checksum_handoff_file_types_contains(rocksdb_options_t* opt,
                                                     int file_type);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_options_checksum_handoff_file_types_count(rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_calculate_sst_write_lifetime_hint_set_clear(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_calculate_sst_write_lifetime_hint_set_add(
    rocksdb_options_t* opt, int compaction_style);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_calculate_sst_write_lifetime_hint_set_remove(
    rocksdb_options_t* opt, int compaction_style);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(
    rocksdb_options_t* opt, int compaction_style);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_options_calculate_sst_write_lifetime_hint_set_count(
    rocksdb_options_t* opt);

/* SST Partitioner Factory */
extern ROCKSDB_LIBRARY_API rocksdb_sst_partitioner_factory_t*
rocksdb_sst_partitioner_fixed_prefix_factory_create(size_t prefix_len);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_sst_partitioner_factory(
    rocksdb_options_t*, rocksdb_sst_partitioner_factory_t*);

/* Table Properties Collector Factory */
extern ROCKSDB_LIBRARY_API void
rocksdb_options_add_table_properties_collector_factory(
    rocksdb_options_t*, rocksdb_table_properties_collector_factory_t*);

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_compression_options(
    rocksdb_options_t*, int, int, int, int);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_compression_options_zstd_max_train_bytes(rocksdb_options_t*,
                                                             int);
extern ROCKSDB_LIBRARY_API int
rocksdb_options_get_compression_options_zstd_max_train_bytes(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_compression_options_use_zstd_dict_trainer(
    rocksdb_options_t*, unsigned char);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_options_get_compression_options_use_zstd_dict_trainer(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_compression_options_parallel_threads(rocksdb_options_t*,
                                                         int);
extern ROCKSDB_LIBRARY_API int
rocksdb_options_get_compression_options_parallel_threads(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_compression_options_max_dict_buffer_bytes(
    rocksdb_options_t*, uint64_t);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_options_get_compression_options_max_dict_buffer_bytes(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_bottommost_compression_options(rocksdb_options_t*, int, int,
                                                   int, int, unsigned char);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_bottommost_compression_options_zstd_max_train_bytes(
    rocksdb_options_t*, int, unsigned char);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_bottommost_compression_options_use_zstd_dict_trainer(
    rocksdb_options_t*, unsigned char, unsigned char);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_options_get_bottommost_compression_options_use_zstd_dict_trainer(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_bottommost_compression_options_max_dict_buffer_bytes(
    rocksdb_options_t*, uint64_t, unsigned char);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_prefix_extractor(
    rocksdb_options_t*, rocksdb_slicetransform_t*);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_max_bytes_for_level_multiplier_additional(
    rocksdb_options_t*, int* level_values, size_t num_levels);
extern ROCKSDB_LIBRARY_API void rocksdb_options_enable_statistics(
    rocksdb_options_t*);

enum {
  rocksdb_statistics_level_disable_all = 0,
  rocksdb_statistics_level_except_tickers =
      rocksdb_statistics_level_disable_all,
  rocksdb_statistics_level_except_histogram_or_timers = 1,
  rocksdb_statistics_level_except_timers = 2,
  rocksdb_statistics_level_except_detailed_timers = 3,
  rocksdb_statistics_level_except_time_for_mutex = 4,
  rocksdb_statistics_level_all = 5,
};

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_statistics_level(
    rocksdb_options_t*, int level);
extern ROCKSDB_LIBRARY_API int rocksdb_options_get_statistics_level(
    rocksdb_options_t*);
/* Blob Options Settings */

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_blob_cache(
    rocksdb_options_t* opt, rocksdb_cache_t* blob_cache);

enum {
  rocksdb_prepopulate_blob_disable = 0,
  rocksdb_prepopulate_blob_flush_only = 1
};

/* returns a pointer to a malloc()-ed, null terminated string */
extern ROCKSDB_LIBRARY_API char* rocksdb_options_statistics_get_string(
    rocksdb_options_t* opt);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_options_statistics_get_ticker_count(
    rocksdb_options_t* opt, uint32_t ticker_type);
extern ROCKSDB_LIBRARY_API void rocksdb_options_statistics_get_histogram_data(
    rocksdb_options_t* opt, uint32_t histogram_type,
    rocksdb_statistics_histogram_data_t* const data);

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_memtable_vector_rep(
    rocksdb_options_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_hash_skip_list_rep(
    rocksdb_options_t*, size_t, int32_t, int32_t);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_hash_link_list_rep(
    rocksdb_options_t*, size_t);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_plain_table_factory(
    rocksdb_options_t*, uint32_t, int, double, size_t, size_t, char,
    unsigned char, unsigned char);

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_min_level_to_compress(
    rocksdb_options_t* opt, int level);

enum {
  rocksdb_tolerate_corrupted_tail_records_recovery = 0,
  rocksdb_absolute_consistency_recovery = 1,
  rocksdb_point_in_time_recovery = 2,
  rocksdb_skip_any_corrupted_records_recovery = 3
};

enum {
  rocksdb_no_compression = 0,
  rocksdb_snappy_compression = 1,
  rocksdb_zlib_compression = 2,
  rocksdb_bz2_compression = 3,
  rocksdb_lz4_compression = 4,
  rocksdb_lz4hc_compression = 5,
  rocksdb_xpress_compression = 6,
  rocksdb_zstd_compression = 7
};

enum {
  rocksdb_level_compaction = 0,
  rocksdb_universal_compaction = 1,
  rocksdb_fifo_compaction = 2
};
extern ROCKSDB_LIBRARY_API void
rocksdb_options_set_universal_compaction_options(
    rocksdb_options_t*, rocksdb_universal_compaction_options_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_fifo_compaction_options(
    rocksdb_options_t* opt, rocksdb_fifo_compaction_options_t* fifo);
extern ROCKSDB_LIBRARY_API void rocksdb_options_set_ratelimiter(
    rocksdb_options_t* opt, rocksdb_ratelimiter_t* limiter);

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_row_cache(
    rocksdb_options_t* opt, rocksdb_cache_t* cache);

extern ROCKSDB_LIBRARY_API void
rocksdb_options_add_compact_on_deletion_collector_factory(
    rocksdb_options_t*, size_t window_size, size_t num_dels_trigger);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_add_compact_on_deletion_collector_factory_del_ratio(
    rocksdb_options_t*, size_t window_size, size_t num_dels_trigger,
    double deletion_ratio);
extern ROCKSDB_LIBRARY_API void
rocksdb_options_add_compact_on_deletion_collector_factory_min_file_size(
    rocksdb_options_t*, size_t window_size, size_t num_dels_trigger,
    double deletion_ratio, uint64_t min_file_size);

enum {
  rocksdb_k_by_compensated_size_compaction_pri = 0,
  rocksdb_k_oldest_largest_seq_first_compaction_pri = 1,
  rocksdb_k_oldest_smallest_seq_first_compaction_pri = 2,
  rocksdb_k_min_overlapping_ratio_compaction_pri = 3,
  rocksdb_k_round_robin_compaction_pri = 4
};

/* RateLimiter */
extern ROCKSDB_LIBRARY_API rocksdb_ratelimiter_t* rocksdb_ratelimiter_create(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness);
extern ROCKSDB_LIBRARY_API rocksdb_ratelimiter_t*
rocksdb_ratelimiter_create_auto_tuned(int64_t rate_bytes_per_sec,
                                      int64_t refill_period_us,
                                      int32_t fairness);
extern ROCKSDB_LIBRARY_API rocksdb_ratelimiter_t*
rocksdb_ratelimiter_create_with_mode(int64_t rate_bytes_per_sec,
                                     int64_t refill_period_us, int32_t fairness,
                                     int mode, bool auto_tuned);

/* PerfContext */
enum {
  rocksdb_uninitialized = 0,
  rocksdb_disable = 1,
  rocksdb_enable_count = 2,
  rocksdb_enable_time_except_for_mutex = 3,
  rocksdb_enable_time = 4,
  rocksdb_out_of_bounds = 5
};

enum {
  rocksdb_user_key_comparison_count = 0,
  rocksdb_block_cache_hit_count,
  rocksdb_block_read_count,
  rocksdb_block_read_byte,
  rocksdb_block_read_time,
  rocksdb_block_checksum_time,
  rocksdb_block_decompress_time,
  rocksdb_get_read_bytes,
  rocksdb_multiget_read_bytes,
  rocksdb_iter_read_bytes,
  rocksdb_internal_key_skipped_count,
  rocksdb_internal_delete_skipped_count,
  rocksdb_internal_recent_skipped_count,
  rocksdb_internal_merge_count,
  rocksdb_get_snapshot_time,
  rocksdb_get_from_memtable_time,
  rocksdb_get_from_memtable_count,
  rocksdb_get_post_process_time,
  rocksdb_get_from_output_files_time,
  rocksdb_seek_on_memtable_time,
  rocksdb_seek_on_memtable_count,
  rocksdb_next_on_memtable_count,
  rocksdb_prev_on_memtable_count,
  rocksdb_seek_child_seek_time,
  rocksdb_seek_child_seek_count,
  rocksdb_seek_min_heap_time,
  rocksdb_seek_max_heap_time,
  rocksdb_seek_internal_seek_time,
  rocksdb_find_next_user_entry_time,
  rocksdb_write_wal_time,
  rocksdb_write_memtable_time,
  rocksdb_write_delay_time,
  rocksdb_write_pre_and_post_process_time,
  rocksdb_db_mutex_lock_nanos,
  rocksdb_db_condition_wait_nanos,
  rocksdb_merge_operator_time_nanos,
  rocksdb_read_index_block_nanos,
  rocksdb_read_filter_block_nanos,
  rocksdb_new_table_block_iter_nanos,
  rocksdb_new_table_iterator_nanos,
  rocksdb_block_seek_nanos,
  rocksdb_find_table_nanos,
  rocksdb_bloom_memtable_hit_count,
  rocksdb_bloom_memtable_miss_count,
  rocksdb_bloom_sst_hit_count,
  rocksdb_bloom_sst_miss_count,
  rocksdb_key_lock_wait_time,
  rocksdb_key_lock_wait_count,
  rocksdb_env_new_sequential_file_nanos,
  rocksdb_env_new_random_access_file_nanos,
  rocksdb_env_new_writable_file_nanos,
  rocksdb_env_reuse_writable_file_nanos,
  rocksdb_env_new_random_rw_file_nanos,
  rocksdb_env_new_directory_nanos,
  rocksdb_env_file_exists_nanos,
  rocksdb_env_get_children_nanos,
  rocksdb_env_get_children_file_attributes_nanos,
  rocksdb_env_delete_file_nanos,
  rocksdb_env_create_dir_nanos,
  rocksdb_env_create_dir_if_missing_nanos,
  rocksdb_env_delete_dir_nanos,
  rocksdb_env_get_file_size_nanos,
  rocksdb_env_get_file_modification_time_nanos,
  rocksdb_env_rename_file_nanos,
  rocksdb_env_link_file_nanos,
  rocksdb_env_lock_file_nanos,
  rocksdb_env_unlock_file_nanos,
  rocksdb_env_new_logger_nanos,
  rocksdb_number_async_seek,
  rocksdb_blob_cache_hit_count,
  rocksdb_blob_read_count,
  rocksdb_blob_read_byte,
  rocksdb_blob_read_time,
  rocksdb_blob_checksum_time,
  rocksdb_blob_decompress_time,
  rocksdb_internal_range_del_reseek_count,
  rocksdb_block_read_cpu_time,
  rocksdb_internal_merge_point_lookup_count,
  rocksdb_data_block_read_byte,
  rocksdb_index_block_read_byte,
  rocksdb_filter_block_read_byte,
  rocksdb_compression_dict_block_read_byte,
  rocksdb_metadata_block_read_byte,
  rocksdb_blob_cache_read_byte,
  rocksdb_total_metric_count = 86
};

extern ROCKSDB_LIBRARY_API void rocksdb_set_perf_level(int);
extern ROCKSDB_LIBRARY_API rocksdb_perfcontext_t* rocksdb_perfcontext_create(
    void);
extern ROCKSDB_LIBRARY_API char* rocksdb_perfcontext_report(
    rocksdb_perfcontext_t* context, unsigned char exclude_zero_counters);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_perfcontext_metric(rocksdb_perfcontext_t* context, int metric);

/* Compaction Filter */

extern ROCKSDB_LIBRARY_API rocksdb_compactionfilter_t*
rocksdb_compactionfilter_create(
    void* state, void (*destructor)(void*),
    unsigned char (*filter)(void*, int level, const char* key,
                            size_t key_length, const char* existing_value,
                            size_t value_length, char** new_value,
                            size_t* new_value_length,
                            unsigned char* value_changed),
    const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API void rocksdb_compactionfilter_set_ignore_snapshots(
    rocksdb_compactionfilter_t*, unsigned char);
extern ROCKSDB_LIBRARY_API void rocksdb_compactionfilter_destroy(
    rocksdb_compactionfilter_t*);

/* WAL Filter */

enum {
  rocksdb_wal_filter_continue_processing = 0,
  rocksdb_wal_filter_ignore_current_record = 1,
  rocksdb_wal_filter_stop_replay = 2,
  rocksdb_wal_filter_corrupted_record = 3,
};

extern ROCKSDB_LIBRARY_API rocksdb_walfilter_t* rocksdb_walfilter_create(
    void* state, void (*destructor)(void*),
    rocksdb_walfilter_column_family_log_number_map_cb
        column_family_log_number_map,
    rocksdb_walfilter_log_record_found_cb log_record_found,
    const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API void rocksdb_walfilter_destroy(rocksdb_walfilter_t*);

/* Compaction Filter Context */

extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionfiltercontext_is_full_compaction(
    rocksdb_compactionfiltercontext_t* context);

extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionfiltercontext_is_manual_compaction(
    rocksdb_compactionfiltercontext_t* context);

/* Compaction Filter Factory */

extern ROCKSDB_LIBRARY_API rocksdb_compactionfilterfactory_t*
rocksdb_compactionfilterfactory_create(
    void* state, void (*destructor)(void*),
    rocksdb_compactionfilter_t* (*create_compaction_filter)(
        void*, rocksdb_compactionfiltercontext_t* context),
    const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API void rocksdb_compactionfilterfactory_destroy(
    rocksdb_compactionfilterfactory_t*);

/* Comparator */

extern ROCKSDB_LIBRARY_API rocksdb_comparator_t* rocksdb_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API void rocksdb_comparator_destroy(
    rocksdb_comparator_t*);

extern ROCKSDB_LIBRARY_API rocksdb_comparator_t*
rocksdb_comparator_with_ts_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    int (*compare_ts)(void*, const char* a_ts, size_t a_tslen, const char* b_ts,
                      size_t b_tslen),
    int (*compare_without_ts)(void*, const char* a, size_t alen,
                              unsigned char a_has_ts, const char* b,
                              size_t blen, unsigned char b_has_ts),
    const char* (*name)(void*), size_t timestamp_size);

/* Filter policy */

extern ROCKSDB_LIBRARY_API void rocksdb_filterpolicy_destroy(
    rocksdb_filterpolicy_t*);

extern ROCKSDB_LIBRARY_API rocksdb_filterpolicy_t*
rocksdb_filterpolicy_create_bloom(double bits_per_key);
extern ROCKSDB_LIBRARY_API rocksdb_filterpolicy_t*
rocksdb_filterpolicy_create_bloom_full(double bits_per_key);
extern ROCKSDB_LIBRARY_API rocksdb_filterpolicy_t*
rocksdb_filterpolicy_create_ribbon(double bloom_equivalent_bits_per_key);
extern ROCKSDB_LIBRARY_API rocksdb_filterpolicy_t*
rocksdb_filterpolicy_create_ribbon_hybrid(double bloom_equivalent_bits_per_key,
                                          int bloom_before_level);

/* Merge Operator */

extern ROCKSDB_LIBRARY_API rocksdb_mergeoperator_t*
rocksdb_mergeoperator_create(
    void* state, void (*destructor)(void*),
    char* (*full_merge)(void*, const char* key, size_t key_length,
                        const char* existing_value,
                        size_t existing_value_length,
                        const char* const* operands_list,
                        const size_t* operands_list_length, int num_operands,
                        unsigned char* success, size_t* new_value_length),
    char* (*partial_merge)(void*, const char* key, size_t key_length,
                           const char* const* operands_list,
                           const size_t* operands_list_length, int num_operands,
                           unsigned char* success, size_t* new_value_length),
    void (*delete_value)(void*, const char* value, size_t value_length),
    const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API void rocksdb_mergeoperator_destroy(
    rocksdb_mergeoperator_t*);

/* Read options */

#include "c_api_gen/c_generated_readoptions_auto.h.inc"
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_snapshot(
    rocksdb_readoptions_t*, const rocksdb_snapshot_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_iterate_upper_bound(
    rocksdb_readoptions_t*, const char* key, size_t keylen);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_iterate_lower_bound(
    rocksdb_readoptions_t*, const char* key, size_t keylen);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_timestamp(
    rocksdb_readoptions_t*, const char* ts, size_t tslen);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_iter_start_ts(
    rocksdb_readoptions_t*, const char* ts, size_t tslen);
extern ROCKSDB_LIBRARY_API void
rocksdb_readoptions_set_merge_operand_count_threshold(rocksdb_readoptions_t*,
                                                      size_t threshold);
extern ROCKSDB_LIBRARY_API void
rocksdb_readoptions_clear_merge_operand_count_threshold(rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_readoptions_has_merge_operand_count_threshold(rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_readoptions_get_merge_operand_count_threshold(rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_request_id(
    rocksdb_readoptions_t*, const char* request_id, size_t request_id_len);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_clear_request_id(
    rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API const char* rocksdb_readoptions_get_request_id(
    rocksdb_readoptions_t*, size_t* request_id_len);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_table_filter(
    rocksdb_readoptions_t*, void* state, void (*destructor)(void*),
    rocksdb_readoptions_table_filter_cb table_filter);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_clear_table_filter(
    rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API unsigned char rocksdb_readoptions_has_table_filter(
    const rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API void
rocksdb_readoptions_set_table_index_factory_from_string(rocksdb_readoptions_t*,
                                                        const char* value,
                                                        size_t value_len,
                                                        char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_clear_table_index_factory(
    rocksdb_readoptions_t*);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_readoptions_get_table_index_factory_name(const rocksdb_readoptions_t*,
                                                 size_t* name_len);
/* EXPERIMENTAL: overrides, for this read only, which index the SST is read
 * through. Takes a rocksdb_readoptions_read_index_* value.
 * rocksdb_readoptions_set_table_index_factory_from_string() also sets this to
 * prefer_custom as a side effect, so call this after it if you want a
 * different selection. */
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_read_index(
    rocksdb_readoptions_t*, int v);
extern ROCKSDB_LIBRARY_API int rocksdb_readoptions_get_read_index(
    const rocksdb_readoptions_t*);
enum {
  rocksdb_readoptions_read_index_default = 0,
  rocksdb_readoptions_read_index_builtin = 1,
  rocksdb_readoptions_read_index_prefer_custom = 2,
};

/* Compact range options */

extern ROCKSDB_LIBRARY_API unsigned char*
rocksdb_compaction_options_canceled_create(void);
extern ROCKSDB_LIBRARY_API void rocksdb_compaction_options_canceled_destroy(
    unsigned char* canceled);
extern ROCKSDB_LIBRARY_API void rocksdb_compaction_options_canceled_set(
    unsigned char* canceled, unsigned char value);
extern ROCKSDB_LIBRARY_API void rocksdb_compaction_options_set_canceled(
    rocksdb_compaction_options_t* options, unsigned char* canceled);

extern ROCKSDB_LIBRARY_API void rocksdb_compactoptions_set_full_history_ts_low(
    rocksdb_compactoptions_t*, char* ts, size_t tslen);

/* Flush options */

extern ROCKSDB_LIBRARY_API void rocksdb_flush_wal_with_options(
    rocksdb_t* db, const rocksdb_flushwaloptions_t* options, char** errptr);

/* Memory allocator */

extern ROCKSDB_LIBRARY_API rocksdb_memory_allocator_t*
rocksdb_jemalloc_nodump_allocator_create(char** errptr);

/* Cache */

extern ROCKSDB_LIBRARY_API void rocksdb_lru_cache_options_set_capacity(
    rocksdb_lru_cache_options_t*, size_t);
extern ROCKSDB_LIBRARY_API void rocksdb_lru_cache_options_set_num_shard_bits(
    rocksdb_lru_cache_options_t*, int);
extern ROCKSDB_LIBRARY_API void rocksdb_lru_cache_options_set_memory_allocator(
    rocksdb_lru_cache_options_t*, rocksdb_memory_allocator_t*);

extern ROCKSDB_LIBRARY_API rocksdb_cache_t* rocksdb_cache_create_lru(
    size_t capacity);
extern ROCKSDB_LIBRARY_API rocksdb_cache_t*
rocksdb_cache_create_lru_with_strict_capacity_limit(size_t capacity);
extern ROCKSDB_LIBRARY_API rocksdb_cache_t* rocksdb_cache_create_lru_opts(
    const rocksdb_lru_cache_options_t*);

/* WriteBufferManager */

enum {
  rocksdb_write_buffer_manager_flush_policy_oldest = 0,
  rocksdb_write_buffer_manager_flush_policy_largest = 1,
  rocksdb_write_buffer_manager_flush_policy_largest_across_dbs = 2
};

extern ROCKSDB_LIBRARY_API rocksdb_write_buffer_manager_t*
rocksdb_write_buffer_manager_create(size_t buffer_size, bool allow_stall);
extern ROCKSDB_LIBRARY_API rocksdb_write_buffer_manager_t*
rocksdb_write_buffer_manager_create_with_policy(size_t buffer_size,
                                                bool allow_stall,
                                                int flush_policy);
extern ROCKSDB_LIBRARY_API rocksdb_write_buffer_manager_t*
rocksdb_write_buffer_manager_create_with_cache(size_t buffer_size,
                                               const rocksdb_cache_t* cache,
                                               bool allow_stall);
extern ROCKSDB_LIBRARY_API rocksdb_write_buffer_manager_t*
rocksdb_write_buffer_manager_create_with_cache_and_policy(
    size_t buffer_size, const rocksdb_cache_t* cache, bool allow_stall,
    int flush_policy);

extern ROCKSDB_LIBRARY_API void rocksdb_write_buffer_manager_set_flush_policy(
    rocksdb_write_buffer_manager_t* wbm, int flush_policy);
/* SstFileManager */

extern ROCKSDB_LIBRARY_API rocksdb_sst_file_manager_t*
rocksdb_sst_file_manager_create(rocksdb_env_t* env);

/* HyperClockCache */

extern ROCKSDB_LIBRARY_API rocksdb_hyper_clock_cache_options_t*
rocksdb_hyper_clock_cache_options_create(size_t capacity,
                                         size_t estimated_entry_charge);
extern ROCKSDB_LIBRARY_API void rocksdb_hyper_clock_cache_options_set_capacity(
    rocksdb_hyper_clock_cache_options_t*, size_t);
extern ROCKSDB_LIBRARY_API void
rocksdb_hyper_clock_cache_options_set_estimated_entry_charge(
    rocksdb_hyper_clock_cache_options_t*, size_t);
extern ROCKSDB_LIBRARY_API void
rocksdb_hyper_clock_cache_options_set_num_shard_bits(
    rocksdb_hyper_clock_cache_options_t*, int);
extern ROCKSDB_LIBRARY_API void
rocksdb_hyper_clock_cache_options_set_memory_allocator(
    rocksdb_hyper_clock_cache_options_t*, rocksdb_memory_allocator_t*);

extern ROCKSDB_LIBRARY_API rocksdb_cache_t* rocksdb_cache_create_hyper_clock(
    size_t capacity, size_t estimated_entry_charge);
extern ROCKSDB_LIBRARY_API rocksdb_cache_t*
rocksdb_cache_create_hyper_clock_opts(
    const rocksdb_hyper_clock_cache_options_t*);

/* DBPath */

extern ROCKSDB_LIBRARY_API rocksdb_dbpath_t* rocksdb_dbpath_create(
    const char* path, uint64_t target_size);

/* Env */

extern ROCKSDB_LIBRARY_API rocksdb_env_t* rocksdb_create_default_env(void);
extern ROCKSDB_LIBRARY_API rocksdb_env_t* rocksdb_create_mem_env(void);

extern ROCKSDB_LIBRARY_API void rocksdb_env_destroy(rocksdb_env_t*);

extern ROCKSDB_LIBRARY_API void rocksdb_envoptions_set_rate_limiter(
    rocksdb_envoptions_t* opt, rocksdb_ratelimiter_t* rate_limiter);

/* Tracing */

extern ROCKSDB_LIBRARY_API rocksdb_trace_reader_t* rocksdb_trace_reader_create(
    rocksdb_env_t* env, const rocksdb_envoptions_t* env_options,
    const char* trace_path, char** errptr);
extern ROCKSDB_LIBRARY_API char* rocksdb_trace_reader_read(
    rocksdb_trace_reader_t* reader, size_t* size, char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_replay_options_set_num_threads(
    rocksdb_replay_options_t* options, uint32_t num_threads);
extern ROCKSDB_LIBRARY_API uint32_t
rocksdb_replay_options_get_num_threads(rocksdb_replay_options_t* options);
extern ROCKSDB_LIBRARY_API void rocksdb_replay_options_set_fast_forward(
    rocksdb_replay_options_t* options, double fast_forward);
extern ROCKSDB_LIBRARY_API double rocksdb_replay_options_get_fast_forward(
    rocksdb_replay_options_t* options);
extern ROCKSDB_LIBRARY_API rocksdb_replayer_t* rocksdb_new_default_replayer(
    rocksdb_t* db, rocksdb_column_family_handle_t** column_families,
    size_t num_column_families, rocksdb_env_t* env,
    const rocksdb_envoptions_t* env_options, const char* trace_path,
    char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_replayer_replay(
    rocksdb_replayer_t* replayer, const rocksdb_replay_options_t* options,
    char** errptr);
enum {
  rocksdb_trace_filter_none = 0x0,
  rocksdb_trace_filter_get = 0x1 << 0,
  rocksdb_trace_filter_write = 0x1 << 1,
  rocksdb_trace_filter_iterator_seek = 0x1 << 2,
  rocksdb_trace_filter_iterator_seek_for_prev = 0x1 << 3,
  rocksdb_trace_filter_multi_get = 0x1 << 4,
};
extern ROCKSDB_LIBRARY_API void rocksdb_start_trace(
    rocksdb_t* db, rocksdb_env_t* env, const rocksdb_envoptions_t* env_options,
    const rocksdb_trace_options_t* options, const char* trace_path,
    char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_start_io_trace(
    rocksdb_t* db, rocksdb_env_t* env, const rocksdb_envoptions_t* env_options,
    const rocksdb_trace_options_t* options, const char* trace_path,
    char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_start_block_cache_trace(
    rocksdb_t* db, rocksdb_env_t* env, const rocksdb_envoptions_t* env_options,
    const rocksdb_trace_options_t* options, const char* trace_path,
    char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_start_block_cache_trace_with_options(
    rocksdb_t* db, rocksdb_env_t* env, const rocksdb_envoptions_t* env_options,
    const rocksdb_block_cache_trace_options_t* options,
    const rocksdb_block_cache_trace_writer_options_t* writer_options,
    const char* trace_path, char** errptr);

/* SstFile */

extern ROCKSDB_LIBRARY_API rocksdb_sstfilewriter_t*
rocksdb_sstfilewriter_create(const rocksdb_envoptions_t* env,
                             const rocksdb_options_t* io_options);
extern ROCKSDB_LIBRARY_API rocksdb_sstfilewriter_t*
rocksdb_sstfilewriter_create_with_comparator(
    const rocksdb_envoptions_t* env, const rocksdb_options_t* io_options,
    const rocksdb_comparator_t* comparator);
extern ROCKSDB_LIBRARY_API void rocksdb_sstfilewriter_file_size(
    rocksdb_sstfilewriter_t* writer, uint64_t* file_size);

extern ROCKSDB_LIBRARY_API void rocksdb_ingest_external_file(
    rocksdb_t* db, const char* const* file_list, const size_t list_len,
    const rocksdb_ingestexternalfileoptions_t* opt, char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_ingest_external_file_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const rocksdb_ingestexternalfileoptions_t* opt, char** errptr);

/* SliceTransform */

extern ROCKSDB_LIBRARY_API rocksdb_slicetransform_t*
rocksdb_slicetransform_create(void* state, void (*destructor)(void*),
                              char* (*transform)(void*, const char* key,
                                                 size_t length,
                                                 size_t* dst_length),
                              unsigned char (*in_domain)(void*, const char* key,
                                                         size_t length),
                              const char* (*name)(void*));
extern ROCKSDB_LIBRARY_API rocksdb_slicetransform_t*
rocksdb_slicetransform_create_fixed_prefix(size_t);
extern ROCKSDB_LIBRARY_API rocksdb_slicetransform_t*
rocksdb_slicetransform_create_noop(void);
extern ROCKSDB_LIBRARY_API void rocksdb_slicetransform_destroy(
    rocksdb_slicetransform_t*);

/* Universal Compaction options */

enum {
  rocksdb_similar_size_compaction_stop_style = 0,
  rocksdb_total_size_compaction_stop_style = 1
};

extern ROCKSDB_LIBRARY_API rocksdb_universal_compaction_options_t*
rocksdb_universal_compaction_options_create(void);

extern ROCKSDB_LIBRARY_API rocksdb_fifo_compaction_options_t*
rocksdb_fifo_compaction_options_create(void);

extern ROCKSDB_LIBRARY_API int rocksdb_livefiles_count(
    const rocksdb_livefiles_t*);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_column_family_name(
    const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_name(
    const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_directory(
    const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API int rocksdb_livefiles_level(
    const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API size_t
rocksdb_livefiles_size(const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_smallestkey(
    const rocksdb_livefiles_t*, int index, size_t* size);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_largestkey(
    const rocksdb_livefiles_t*, int index, size_t* size);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_livefiles_smallest_seqno(const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_livefiles_largest_seqno(const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_livefiles_entries(const rocksdb_livefiles_t*, int index);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_livefiles_deletions(const rocksdb_livefiles_t*, int index);

extern ROCKSDB_LIBRARY_API rocksdb_livefiles_storage_info_t*
rocksdb_get_livefiles_storage_info(
    rocksdb_t* db, const rocksdb_livefiles_storage_info_options_t* options,
    char** errptr);
extern ROCKSDB_LIBRARY_API size_t rocksdb_livefiles_storage_info_count(
    const rocksdb_livefiles_storage_info_t* info);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_livefiles_storage_info_relative_filename(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API const char* rocksdb_livefiles_storage_info_directory(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_livefiles_storage_info_file_number(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API int rocksdb_livefiles_storage_info_file_type(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_livefiles_storage_info_size(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API int rocksdb_livefiles_storage_info_temperature(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_livefiles_storage_info_file_checksum(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_livefiles_storage_info_file_checksum_func_name(
    const rocksdb_livefiles_storage_info_t* info, size_t index);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_livefiles_storage_info_replacement_contents(
    const rocksdb_livefiles_storage_info_t* info, size_t index, size_t* size);
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_livefiles_storage_info_trim_to_size(
    const rocksdb_livefiles_storage_info_t* info, size_t index);

extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_column_family_name(
    rocksdb_livefile_t*, const char*);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_level(rocksdb_livefile_t*,
                                                           int);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_name(rocksdb_livefile_t*,
                                                          const char*);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_directory(
    rocksdb_livefile_t*, const char*);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_size(rocksdb_livefile_t*,
                                                          size_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_smallest_key(
    rocksdb_livefile_t*, const char*, size_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_largest_key(
    rocksdb_livefile_t*, const char*, size_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_smallest_seqno(
    rocksdb_livefile_t*, uint64_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_largest_seqno(
    rocksdb_livefile_t*, uint64_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_num_entries(
    rocksdb_livefile_t*, uint64_t);
extern ROCKSDB_LIBRARY_API void rocksdb_livefile_set_num_deletions(
    rocksdb_livefile_t*, uint64_t);

extern ROCKSDB_LIBRARY_API void rocksdb_livefiles_add(rocksdb_livefiles_t*,
                                                      rocksdb_livefile_t*);

/* Utility Helpers */

extern ROCKSDB_LIBRARY_API void rocksdb_get_options_from_string(
    const rocksdb_options_t* base_options, const char* opts_str,
    rocksdb_options_t* new_options, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_delete_file_in_range(
    rocksdb_t* db, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_delete_file_in_range_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, char** errptr);

/* MetaData */

extern ROCKSDB_LIBRARY_API rocksdb_column_family_metadata_t*
rocksdb_get_column_family_metadata(rocksdb_t* db);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_metadata_t*
rocksdb_get_column_family_metadata_with_options(
    rocksdb_t* db, const rocksdb_column_family_metadata_options_t* options);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_metadata_options_t*
rocksdb_column_family_metadata_options_create(void);
extern ROCKSDB_LIBRARY_API void
rocksdb_column_family_metadata_options_set_start_key(
    rocksdb_column_family_metadata_options_t* options, const char* key,
    size_t key_len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_column_family_metadata_options_get_start_key(
    const rocksdb_column_family_metadata_options_t* options, size_t* key_len);
extern ROCKSDB_LIBRARY_API void
rocksdb_column_family_metadata_options_set_end_key(
    rocksdb_column_family_metadata_options_t* options, const char* key,
    size_t key_len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_column_family_metadata_options_get_end_key(
    const rocksdb_column_family_metadata_options_t* options, size_t* key_len);

extern ROCKSDB_LIBRARY_API rocksdb_export_import_files_metadata_t*
rocksdb_export_import_files_metadata_create(void);

extern ROCKSDB_LIBRARY_API char*
rocksdb_export_import_files_metadata_get_db_comparator_name(
    rocksdb_export_import_files_metadata_t*);

extern ROCKSDB_LIBRARY_API void
rocksdb_export_import_files_metadata_set_db_comparator_name(
    rocksdb_export_import_files_metadata_t*, const char*);

extern ROCKSDB_LIBRARY_API rocksdb_livefiles_t*
rocksdb_export_import_files_metadata_get_files(
    rocksdb_export_import_files_metadata_t*);

extern ROCKSDB_LIBRARY_API void rocksdb_export_import_files_metadata_set_files(
    rocksdb_export_import_files_metadata_t*, rocksdb_livefiles_t*);

/**
 * Returns the rocksdb_column_family_metadata_t of the specified
 * column family.
 *
 * Note that the caller is responsible to release the returned memory
 * using rocksdb_column_family_metadata_destroy.
 */
extern ROCKSDB_LIBRARY_API rocksdb_column_family_metadata_t*
rocksdb_get_column_family_metadata_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family);

extern ROCKSDB_LIBRARY_API rocksdb_column_family_metadata_t*
rocksdb_get_column_family_metadata_cf_with_options(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const rocksdb_column_family_metadata_options_t* options);

extern ROCKSDB_LIBRARY_API uint64_t rocksdb_column_family_metadata_get_size(
    rocksdb_column_family_metadata_t* cf_meta);

extern ROCKSDB_LIBRARY_API size_t rocksdb_column_family_metadata_get_file_count(
    rocksdb_column_family_metadata_t* cf_meta);

extern ROCKSDB_LIBRARY_API char* rocksdb_column_family_metadata_get_name(
    rocksdb_column_family_metadata_t* cf_meta);

extern ROCKSDB_LIBRARY_API size_t
rocksdb_column_family_metadata_get_level_count(
    rocksdb_column_family_metadata_t* cf_meta);

/**
 * Returns the rocksdb_level_metadata_t of the ith level from the specified
 * column family metadata.
 *
 * If the specified i is greater than or equal to the number of levels
 * in the specified column family, then NULL will be returned.
 *
 * Note that the caller is responsible to release the returned memory
 * using rocksdb_level_metadata_destroy before releasing its parent
 * rocksdb_column_family_metadata_t.
 */
extern ROCKSDB_LIBRARY_API rocksdb_level_metadata_t*
rocksdb_column_family_metadata_get_level_metadata(
    rocksdb_column_family_metadata_t* cf_meta, size_t i);

/**
 * Releases the specified rocksdb_level_metadata_t.
 *
 * Note that the specified rocksdb_level_metadata_t must be released
 * before the release of its parent rocksdb_column_family_metadata_t.
 */
extern ROCKSDB_LIBRARY_API void rocksdb_level_metadata_destroy(
    rocksdb_level_metadata_t* level_meta);

extern ROCKSDB_LIBRARY_API int rocksdb_level_metadata_get_level(
    rocksdb_level_metadata_t* level_meta);

extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_level_metadata_get_size(rocksdb_level_metadata_t* level_meta);

extern ROCKSDB_LIBRARY_API size_t
rocksdb_level_metadata_get_file_count(rocksdb_level_metadata_t* level_meta);

/**
 * Returns the sst_file_metadata_t of the ith file from the specified level
 * metadata.
 *
 * If the specified i is greater than or equal to the number of files
 * in the specified level, then NULL will be returned.
 *
 * Note that the caller is responsible to release the returned memory
 * using rocksdb_sst_file_metadata_destroy before releasing its
 * parent rocksdb_level_metadata_t.
 */
extern ROCKSDB_LIBRARY_API rocksdb_sst_file_metadata_t*
rocksdb_level_metadata_get_sst_file_metadata(
    rocksdb_level_metadata_t* level_meta, size_t i);

/**
 * Releases the specified rocksdb_sst_file_metadata_t.
 *
 * Note that the specified rocksdb_sst_file_metadata_t must be released
 * before the release of its parent rocksdb_level_metadata_t.
 */
extern ROCKSDB_LIBRARY_API void rocksdb_sst_file_metadata_destroy(
    rocksdb_sst_file_metadata_t* file_meta);

extern ROCKSDB_LIBRARY_API char*
rocksdb_sst_file_metadata_get_relative_filename(
    rocksdb_sst_file_metadata_t* file_meta);

extern ROCKSDB_LIBRARY_API char* rocksdb_sst_file_metadata_get_directory(
    rocksdb_sst_file_metadata_t* file_meta);

extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_sst_file_metadata_get_size(rocksdb_sst_file_metadata_t* file_meta);

/**
 * Returns the smallest key of the specified sst file.
 * The caller is responsible for releasing the returned memory.
 *
 * @param file_meta the metadata of an SST file to obtain its smallest key.
 * @param len the out value which will contain the length of the returned key
 *     after the function call.
 */
extern ROCKSDB_LIBRARY_API char* rocksdb_sst_file_metadata_get_smallestkey(
    rocksdb_sst_file_metadata_t* file_meta, size_t* len);

/**
 * Returns the smallest key of the specified sst file.
 * The caller is responsible for releasing the returned memory.
 *
 * @param file_meta the metadata of an SST file to obtain its smallest key.
 * @param len the out value which will contain the length of the returned key
 *     after the function call.
 */
extern ROCKSDB_LIBRARY_API char* rocksdb_sst_file_metadata_get_largestkey(
    rocksdb_sst_file_metadata_t* file_meta, size_t* len);

/* Transactions */

extern ROCKSDB_LIBRARY_API rocksdb_column_family_handle_t*
rocksdb_transactiondb_create_column_family(
    rocksdb_transactiondb_t* txn_db,
    const rocksdb_options_t* column_family_options,
    const char* column_family_name, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_transactiondb_t* rocksdb_transactiondb_open(
    const rocksdb_options_t* options,
    const rocksdb_transactiondb_options_t* txn_db_options, const char* name,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_transactiondb_t*
rocksdb_transactiondb_open_column_families(
    const rocksdb_options_t* options,
    const rocksdb_transactiondb_options_t* txn_db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr);

extern ROCKSDB_LIBRARY_API const rocksdb_snapshot_t*
rocksdb_transactiondb_create_snapshot(rocksdb_transactiondb_t* txn_db);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_release_snapshot(
    rocksdb_transactiondb_t* txn_db, const rocksdb_snapshot_t* snapshot);

extern ROCKSDB_LIBRARY_API char* rocksdb_transactiondb_property_value(
    rocksdb_transactiondb_t* db, const char* propname);

extern ROCKSDB_LIBRARY_API int rocksdb_transactiondb_property_int(
    rocksdb_transactiondb_t* db, const char* propname, uint64_t* out_val);

extern ROCKSDB_LIBRARY_API rocksdb_t* rocksdb_transactiondb_get_base_db(
    rocksdb_transactiondb_t* txn_db);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_close_base_db(
    rocksdb_t* base_db);

extern ROCKSDB_LIBRARY_API rocksdb_transaction_t* rocksdb_transaction_begin(
    rocksdb_transactiondb_t* txn_db,
    const rocksdb_writeoptions_t* write_options,
    const rocksdb_transaction_options_t* txn_options,
    rocksdb_transaction_t* old_txn);

extern ROCKSDB_LIBRARY_API rocksdb_transaction_t**
rocksdb_transactiondb_get_prepared_transactions(rocksdb_transactiondb_t* txn_db,
                                                size_t* cnt);

extern ROCKSDB_LIBRARY_API void rocksdb_transaction_set_name(
    rocksdb_transaction_t* txn, const char* name, size_t name_len,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_writebatch_wi_t*
rocksdb_transaction_get_writebatch_wi(rocksdb_transaction_t* txn);

// This rocksdb_writebatch_wi_t should be freed with rocksdb_free
extern ROCKSDB_LIBRARY_API void rocksdb_transaction_rebuild_from_writebatch_wi(
    rocksdb_transaction_t* txn, rocksdb_writebatch_wi_t* wi, char** errptr);

// This snapshot should be freed using rocksdb_free
extern ROCKSDB_LIBRARY_API const rocksdb_snapshot_t*
rocksdb_transaction_get_snapshot(rocksdb_transaction_t* txn);

extern ROCKSDB_LIBRARY_API char* rocksdb_transaction_get(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transaction_get_pinned(rocksdb_transaction_t* txn,
                               const rocksdb_readoptions_t* options,
                               const char* key, size_t klen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_transaction_get_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key, size_t klen,
    size_t* vlen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transaction_get_pinned_cf(rocksdb_transaction_t* txn,
                                  const rocksdb_readoptions_t* options,
                                  rocksdb_column_family_handle_t* column_family,
                                  const char* key, size_t klen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_transaction_get_for_update(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, unsigned char exclusive,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transaction_get_pinned_for_update(rocksdb_transaction_t* txn,
                                          const rocksdb_readoptions_t* options,
                                          const char* key, size_t klen,
                                          unsigned char exclusive,
                                          char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_transaction_get_for_update_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key, size_t klen,
    size_t* vlen, unsigned char exclusive, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transaction_get_pinned_for_update_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key, size_t klen,
    unsigned char exclusive, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_transaction_multi_get(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_transaction_multi_get_for_update(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_transaction_multi_get_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_transaction_multi_get_for_update_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API char* rocksdb_transactiondb_get(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transactiondb_get_pinned(rocksdb_transactiondb_t* txn_db,
                                 const rocksdb_readoptions_t* options,
                                 const char* key, size_t klen, char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_transactiondb_get_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t*
rocksdb_transactiondb_get_pinned_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_multi_get(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_multi_get_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_batched_multi_get_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    rocksdb_pinnableslice_t** values, char** errs, const bool sorted_input);

extern ROCKSDB_LIBRARY_API void
rocksdb_transactiondb_batched_multi_get_cf_slice(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, size_t num_keys,
    const rocksdb_slice_t* keys_list, rocksdb_pinnableslice_t** values,
    char** errs, const bool sorted_input);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_transaction_create_iterator(rocksdb_transaction_t* txn,
                                    const rocksdb_readoptions_t* options);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_transaction_create_iterator_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_transactiondb_create_iterator(rocksdb_transactiondb_t* txn_db,
                                      const rocksdb_readoptions_t* options);

extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_transactiondb_create_iterator_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_close(
    rocksdb_transactiondb_t* txn_db);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_flush_cfs(
    rocksdb_transactiondb_t* txn_db, const rocksdb_flushoptions_t* options,
    rocksdb_column_family_handle_t** column_families, int num_column_families,
    char** errptr);

extern ROCKSDB_LIBRARY_API void rocksdb_transactiondb_flush_wal_with_options(
    rocksdb_transactiondb_t* txn_db, const rocksdb_flushwaloptions_t* options,
    char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_checkpoint_t*
rocksdb_transactiondb_checkpoint_object_create(rocksdb_transactiondb_t* txn_db,
                                               char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_optimistictransactiondb_t*
rocksdb_optimistictransactiondb_open(const rocksdb_options_t* options,
                                     const char* name, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_optimistictransactiondb_t*
rocksdb_optimistictransactiondb_open_with_otxn_db_options(
    const rocksdb_options_t* options,
    const rocksdb_optimistictransactiondb_options_t* otxn_db_options,
    const char* name, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_optimistictransactiondb_t*
rocksdb_optimistictransactiondb_open_column_families(
    const rocksdb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_optimistictransactiondb_t*
rocksdb_optimistictransactiondb_open_column_families_with_otxn_db_options(
    const rocksdb_options_t* options,
    const rocksdb_optimistictransactiondb_options_t* otxn_db_options,
    const char* name, int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_t*
rocksdb_optimistictransactiondb_get_base_db(
    rocksdb_optimistictransactiondb_t* otxn_db);

extern ROCKSDB_LIBRARY_API void rocksdb_optimistictransactiondb_close_base_db(
    rocksdb_t* base_db);

extern ROCKSDB_LIBRARY_API rocksdb_transaction_t*
rocksdb_optimistictransaction_begin(
    rocksdb_optimistictransactiondb_t* otxn_db,
    const rocksdb_writeoptions_t* write_options,
    const rocksdb_optimistictransaction_options_t* otxn_options,
    rocksdb_transaction_t* old_txn);

extern ROCKSDB_LIBRARY_API void rocksdb_optimistictransactiondb_close(
    rocksdb_optimistictransactiondb_t* otxn_db);

extern ROCKSDB_LIBRARY_API rocksdb_checkpoint_t*
rocksdb_optimistictransactiondb_checkpoint_object_create(
    rocksdb_optimistictransactiondb_t* otxn_db, char** errptr);

/* Transaction Options */

enum {
  rocksdb_txndb_write_policy_write_committed = 0,
  rocksdb_txndb_write_policy_write_prepared = 1,
  rocksdb_txndb_write_policy_write_unprepared = 2
};

extern ROCKSDB_LIBRARY_API rocksdb_optimistictransactiondb_lock_buckets_t*
rocksdb_optimistictransactiondb_lock_buckets_create(
    size_t bucket_count, unsigned char cache_aligned);

extern ROCKSDB_LIBRARY_API void
rocksdb_optimistictransactiondb_options_set_shared_lock_buckets(
    rocksdb_optimistictransactiondb_options_t* opt,
    rocksdb_optimistictransactiondb_lock_buckets_t* lock_buckets);

extern ROCKSDB_LIBRARY_API char* rocksdb_optimistictransactiondb_property_value(
    rocksdb_optimistictransactiondb_t* db, const char* propname);

extern ROCKSDB_LIBRARY_API int rocksdb_optimistictransactiondb_property_int(
    rocksdb_optimistictransactiondb_t* db, const char* propname,
    uint64_t* out_val);

// referring to convention (3), this should be used by client
// to free memory that was malloc()ed
extern ROCKSDB_LIBRARY_API void rocksdb_free(void* ptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t* rocksdb_get_pinned(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr);
extern ROCKSDB_LIBRARY_API rocksdb_pinnableslice_t* rocksdb_get_pinned_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);
extern ROCKSDB_LIBRARY_API const char* rocksdb_pinnableslice_value(
    const rocksdb_pinnableslice_t* t, size_t* vlen);

extern ROCKSDB_LIBRARY_API rocksdb_memory_consumers_t*
rocksdb_memory_consumers_create(void);
extern ROCKSDB_LIBRARY_API void rocksdb_memory_consumers_add_db(
    rocksdb_memory_consumers_t* consumers, rocksdb_t* db);
extern ROCKSDB_LIBRARY_API void rocksdb_memory_consumers_add_cache(
    rocksdb_memory_consumers_t* consumers, rocksdb_cache_t* cache);
extern ROCKSDB_LIBRARY_API void rocksdb_memory_consumers_destroy(
    rocksdb_memory_consumers_t* consumers);
extern ROCKSDB_LIBRARY_API rocksdb_memory_usage_t*
rocksdb_approximate_memory_usage_create(rocksdb_memory_consumers_t* consumers,
                                        char** errptr);
extern ROCKSDB_LIBRARY_API void rocksdb_approximate_memory_usage_destroy(
    rocksdb_memory_usage_t* usage);

extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_approximate_memory_usage_get_mem_table_total(
    rocksdb_memory_usage_t* memory_usage);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_approximate_memory_usage_get_mem_table_unflushed(
    rocksdb_memory_usage_t* memory_usage);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_approximate_memory_usage_get_mem_table_readers_total(
    rocksdb_memory_usage_t* memory_usage);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_approximate_memory_usage_get_cache_total(
    rocksdb_memory_usage_t* memory_usage);

extern ROCKSDB_LIBRARY_API void rocksdb_cancel_all_background_work(
    rocksdb_t* db, unsigned char wait);

extern ROCKSDB_LIBRARY_API rocksdb_statistics_histogram_data_t*
rocksdb_statistics_histogram_data_create(void);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_median(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_p95(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_p99(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_average(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_std_dev(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_max(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_statistics_histogram_data_get_count(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_statistics_histogram_data_get_sum(
    rocksdb_statistics_histogram_data_t* data);
extern ROCKSDB_LIBRARY_API double rocksdb_statistics_histogram_data_get_min(
    rocksdb_statistics_histogram_data_t* data);

extern ROCKSDB_LIBRARY_API void rocksdb_wait_for_compact(
    rocksdb_t* db, rocksdb_wait_for_compact_options_t* options, char** errptr);

/* High-performance zero-copy Get variants
   These functions avoid unnecessary memory allocations and copies.
   The returned buffer is valid until the handle is destroyed.
   Bindings should migrate to these for better performance. */

/* Zero-copy get that returns a handle to pinned data.
   The data remains valid until rocksdb_pinnable_handle_destroy is called.
   Returns NULL on error or not found. Check errptr to distinguish. */
typedef struct rocksdb_pinnable_handle_t rocksdb_pinnable_handle_t;

extern ROCKSDB_LIBRARY_API rocksdb_pinnable_handle_t* rocksdb_get_pinned_v2(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_pinnable_handle_t* rocksdb_get_pinned_cf_v2(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);

/* Get the data pointer and size from a pinnable handle.
   The data pointer is valid until the handle is destroyed. */
extern ROCKSDB_LIBRARY_API const char* rocksdb_pinnable_handle_get_value(
    const rocksdb_pinnable_handle_t* handle, size_t* vallen);

/* Direct get into caller-provided buffer.
   Returns 1 if value fits in buffer, 0 if buffer too small.
   Sets *vallen to actual value size.
   If buffer is too small, no data is copied but *vallen is set. */
extern ROCKSDB_LIBRARY_API unsigned char rocksdb_get_into_buffer(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, char* buffer, size_t buffer_size, size_t* vallen,
    unsigned char* found, char** errptr);

extern ROCKSDB_LIBRARY_API unsigned char rocksdb_get_into_buffer_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char* buffer, size_t buffer_size, size_t* vallen,
    unsigned char* found, char** errptr);

// Remote compaction
enum {
  rocksdb_compactionservice_jobstatus_success = 0,
  rocksdb_compactionservice_jobstatus_failure = 1,
  rocksdb_compactionservice_jobstatus_aborted = 2,
  rocksdb_compactionservice_jobstatus_use_local = 3,
};

extern ROCKSDB_LIBRARY_API rocksdb_compactionservice_scheduleresponse_t*
rocksdb_compactionservice_scheduleresponse_create(const char* scheduled_job_id,
                                                  int status, char** errptr);

extern ROCKSDB_LIBRARY_API rocksdb_compactionservice_scheduleresponse_t*
rocksdb_compactionservice_scheduleresponse_create_with_status(int status,
                                                              char** errptr);

extern ROCKSDB_LIBRARY_API int
rocksdb_compactionservice_scheduleresponse_getstatus(
    const rocksdb_compactionservice_scheduleresponse_t* response);

extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionservice_scheduleresponse_get_scheduled_job_id(
    const rocksdb_compactionservice_scheduleresponse_t* response, size_t* len);

extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionservice_jobinfo_t_get_db_name(
    const rocksdb_compactionservice_jobinfo_t* info, size_t* len);

extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionservice_jobinfo_t_get_db_id(
    const rocksdb_compactionservice_jobinfo_t* info, size_t* len);

extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionservice_jobinfo_t_get_db_session_id(
    const rocksdb_compactionservice_jobinfo_t* info, size_t* len);

extern ROCKSDB_LIBRARY_API const char*
rocksdb_compactionservice_jobinfo_t_get_cf_name(
    const rocksdb_compactionservice_jobinfo_t* info, size_t* len);

extern ROCKSDB_LIBRARY_API uint32_t
rocksdb_compactionservice_jobinfo_t_get_cf_id(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_compactionservice_jobinfo_t_get_job_id(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API int rocksdb_compactionservice_jobinfo_t_get_priority(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API int
rocksdb_compactionservice_jobinfo_t_get_compaction_reason(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API int
rocksdb_compactionservice_jobinfo_t_get_base_input_level(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API int
rocksdb_compactionservice_jobinfo_t_get_output_level(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionservice_jobinfo_t_is_full_compaction(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionservice_jobinfo_t_is_manual_compaction(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_compactionservice_jobinfo_t_is_bottommost_level(
    const rocksdb_compactionservice_jobinfo_t* info);

extern ROCKSDB_LIBRARY_API rocksdb_compactionservice_t*
rocksdb_compactionservice_create(
    void* state, void (*destructor)(void*),
    rocksdb_compaction_service_schedule_cb schedule, const char* name,
    rocksdb_compaction_service_wait_cb wait,
    rocksdb_compaction_service_cancel_awaiting_jobs_cb cancel_awaiting_jobs,
    rocksdb_compaction_service_on_installation_cb on_installation);

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_compaction_service(
    rocksdb_options_t* options, rocksdb_compactionservice_t* service);

// CompactionServiceOptionsOverride

extern ROCKSDB_LIBRARY_API rocksdb_compaction_service_options_override_t*
rocksdb_compaction_service_options_override_create_from_options(
    rocksdb_options_t* option);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_env(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_env_t* env);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_comparator(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_comparator_t* comparator);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_merge_operator(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_mergeoperator_t* merge_operator);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_compaction_filter(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_compactionfilter_t* compaction_filter);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_compaction_filter_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_compactionfilterfactory_t* compaction_filter_factory);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_prefix_extractor(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_slicetransform_t* prefix_extractor);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_block_based_table_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_block_based_table_options_t* table_options);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_cuckoo_table_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_cuckoo_table_options_t* table_options);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_add_event_listener(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_eventlistener_t* event_listener);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_statistics(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_options_t* options);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_info_log(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_logger_t* logger);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_option(
    rocksdb_compaction_service_options_override_t* override_options,
    const char* key, const char* value);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_file_checksum_gen_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_file_checksum_gen_factory_t* factory);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_set_sst_partitioner_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_sst_partitioner_factory_t* factory);

extern ROCKSDB_LIBRARY_API void
rocksdb_compaction_service_options_override_add_table_properties_collector_factory(
    rocksdb_compaction_service_options_override_t* override_options,
    rocksdb_table_properties_collector_factory_t* factory);

// Atomic bool management for cancellation
// Creates an atomic bool that can be used for cancellation.
// User must call rocksdb_open_and_compact_canceled_destroy() to free it.
extern ROCKSDB_LIBRARY_API unsigned char*
rocksdb_open_and_compact_canceled_create(void);

extern ROCKSDB_LIBRARY_API void rocksdb_open_and_compact_canceled_destroy(
    unsigned char* canceled);

extern ROCKSDB_LIBRARY_API void rocksdb_open_and_compact_canceled_set(
    unsigned char* canceled, unsigned char value);

// OpenAndCompactOptions

extern ROCKSDB_LIBRARY_API void rocksdb_open_and_compact_options_set_canceled(
    rocksdb_open_and_compact_options_t* options, unsigned char* canceled);

extern ROCKSDB_LIBRARY_API void
rocksdb_open_and_compact_options_set_allow_resumption(
    rocksdb_open_and_compact_options_t* options,
    unsigned char allow_resumption);

// OpenAndCompact - main functions
extern ROCKSDB_LIBRARY_API char* rocksdb_open_and_compact(
    const char* db_path, const char* output_directory, const char* input,
    size_t input_len, size_t* output_len,
    const rocksdb_compaction_service_options_override_t* override_options,
    char** errptr);

extern ROCKSDB_LIBRARY_API char* rocksdb_open_and_compact_with_options(
    const rocksdb_open_and_compact_options_t* options, const char* db_path,
    const char* output_directory, const char* input, size_t input_len,
    size_t* output_len,
    const rocksdb_compaction_service_options_override_t* override_options,
    char** errptr);

#include "c_api_gen/c_generated_handles.h.inc"
#include "c_api_gen/c_generated_option_structs_auto.h.inc"

#ifdef __cplusplus
} /* end extern "C" */
#endif
