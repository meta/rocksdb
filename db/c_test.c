/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors. */
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

#include "rocksdb/c.h"

#include <assert.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#ifndef OS_WIN
#include <unistd.h>
#endif

// Can not use port/port.h macros as this is a c file
#ifdef OS_WIN
#include <windows.h>

// Ok for uniqueness
int geteuid() {
  int result = 0;

  result = ((int)GetCurrentProcessId() << 16);
  result |= (int)GetCurrentThreadId();

  return result;
}

#endif

static uint64_t GetTestId(void) {
#ifdef OS_WIN
  return (uint64_t)geteuid();
#else
  return ((uint64_t)geteuid() << 32) ^ (uint64_t)getpid();
#endif
}

const char* phase = "";
static char dbname[200];
static char sstfilename[200];
static char dbbackupname[200];
static char dbcheckpointname[200];
static char dbpathname[200];
static char secondary_path[200];

static void StartPhase(const char* name) {
  fprintf(stderr, "=== Test %s\n", name);
  phase = name;
}
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4996)  // getenv security warning
#endif
static const char* GetTempDir(void) {
  const char* ret = getenv("TEST_TMPDIR");
  if (ret == NULL || ret[0] == '\0') {
#ifdef OS_WIN
    ret = getenv("TEMP");
#else
    ret = "/tmp";
#endif
  }
  return ret;
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#define CheckNoError(err)                                                 \
  if ((err) != NULL) {                                                    \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, (err)); \
    abort();                                                              \
  }

#define CheckCondition(cond)                                              \
  if (!(cond)) {                                                          \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, #cond); \
    abort();                                                              \
  }

static void CheckEqual(const char* expected, const char* v, size_t n) {
  if (expected == NULL && v == NULL) {
    // ok
  } else if (expected != NULL && v != NULL && n == strlen(expected) &&
             memcmp(expected, v, n) == 0) {
    // ok
    return;
  } else {
    fprintf(stderr, "%s: expected '%s', got '%s'\n", phase,
            (expected ? expected : "(null)"), (v ? v : "(null)"));
    abort();
  }
}

static void Free(char** ptr) {
  if (*ptr) {
    free(*ptr);
    *ptr = NULL;
  }
}

static long GetLocalFileSize(const char* path) {
  FILE* file = fopen(path, "rb");
  long size;
  if (file == NULL) {
    return -1;
  }
  CheckCondition(fseek(file, 0, SEEK_END) == 0);
  size = ftell(file);
  fclose(file);
  return size;
}

static void CheckValue(char* err, const char* expected, char** actual,
                       size_t actual_length) {
  CheckNoError(err);
  CheckEqual(expected, *actual, actual_length);
  Free(actual);
}

static void CheckPinnedValue(char* err, const char* expected,
                             const char** actual, size_t actual_length) {
  CheckNoError(err);
  CheckEqual(expected, *actual, actual_length);
}

static void CheckGet(rocksdb_t* db, const rocksdb_readoptions_t* options,
                     const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_get(db, options, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckGetCF(rocksdb_t* db, const rocksdb_readoptions_t* options,
                       rocksdb_column_family_handle_t* handle, const char* key,
                       const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_get_cf(db, options, handle, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckPinGet(rocksdb_t* db, const rocksdb_readoptions_t* options,
                        const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  const char* val;
  rocksdb_pinnableslice_t* p;
  p = rocksdb_get_pinned(db, options, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckPinGetCF(rocksdb_t* db, const rocksdb_readoptions_t* options,
                          rocksdb_column_family_handle_t* handle,
                          const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  const char* val;
  rocksdb_pinnableslice_t* p;
  p = rocksdb_get_pinned_cf(db, options, handle, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckMultiGetValues(size_t num_keys, char** values,
                                size_t* values_sizes, char** errs,
                                const char** expected) {
  for (size_t i = 0; i < num_keys; i++) {
    CheckNoError(errs[i]);
    CheckEqual(expected[i], values[i], values_sizes[i]);
    Free(&values[i]);
  }
}

static void CheckPinnableMultiGetFound(
    const rocksdb_pinnable_multi_get_t* multi_get, size_t index,
    const char* expected, size_t expected_size) {
  const char* value;
  const char* error;
  size_t value_size;
  size_t error_size;
  int result = rocksdb_pinnable_multi_get_result(
      multi_get, index, &value, &value_size, &error, &error_size);
  CheckCondition(result == rocksdb_pinnable_multi_get_found);
  CheckCondition(value_size == expected_size);
  CheckCondition(error == NULL);
  CheckCondition(error_size == 0);
  if (expected_size > 0) {
    CheckCondition(value != NULL);
    CheckCondition(memcmp(value, expected, expected_size) == 0);
  }
}

static void CheckPinnableMultiGetNotFound(
    const rocksdb_pinnable_multi_get_t* multi_get, size_t index) {
  const char* value;
  const char* error;
  size_t value_size;
  size_t error_size;
  int result = rocksdb_pinnable_multi_get_result(
      multi_get, index, &value, &value_size, &error, &error_size);
  CheckCondition(result == rocksdb_pinnable_multi_get_not_found);
  CheckCondition(value == NULL);
  CheckCondition(value_size == 0);
  CheckCondition(error == NULL);
  CheckCondition(error_size == 0);
}

static void CheckPinnableMultiGetError(
    const rocksdb_pinnable_multi_get_t* multi_get, size_t index,
    const char* expected) {
  const char* value;
  const char* error;
  size_t value_size;
  size_t error_size;
  int result = rocksdb_pinnable_multi_get_result(
      multi_get, index, &value, &value_size, &error, &error_size);
  CheckCondition(result == rocksdb_pinnable_multi_get_error);
  CheckCondition(value == NULL);
  CheckCondition(value_size == 0);
  CheckCondition(error != NULL);
  CheckCondition(error_size == strlen(error));
  CheckCondition(strstr(error, expected) != NULL);
}

static void CheckTxnDBBatchedMultiGet(rocksdb_transactiondb_t* db,
                                      const rocksdb_readoptions_t* options,
                                      rocksdb_column_family_handle_t* cf,
                                      const char* expected_a) {
  for (int sorted = 0; sorted <= 1; ++sorted) {
    const char* keys[4] = {"b", "a", "b", "missing"};
    const size_t sizes[4] = {1, 1, 1, 7};
    const char* expected[4] = {"", expected_a, "", NULL};
    if (sorted) {
      keys[0] = "a";
      keys[1] = "b";
      expected[0] = expected_a;
      expected[1] = "";
    }
    for (int slices = 0; slices <= 1; ++slices) {
      rocksdb_pinnableslice_t* values[4];
      char* errs[4];
      if (slices) {
        rocksdb_slice_t key_slices[4];
        for (size_t i = 0; i < 4; ++i) {
          key_slices[i].data = keys[i];
          key_slices[i].size = sizes[i];
        }
        rocksdb_transactiondb_batched_multi_get_cf_slice(
            db, options, cf, 4, key_slices, values, errs, sorted);
      } else {
        rocksdb_transactiondb_batched_multi_get_cf(db, options, cf, 4, keys,
                                                   sizes, values, errs, sorted);
      }
      for (size_t i = 0; i < 4; ++i) {
        CheckNoError(errs[i]);
        if (expected[i] == NULL) {
          CheckCondition(values[i] == NULL);
        } else {
          size_t size;
          CheckCondition(values[i] != NULL);
          const char* value = rocksdb_pinnableslice_value(values[i], &size);
          CheckEqual(expected[i], value, size);
        }
        rocksdb_pinnableslice_destroy(values[i]);
      }
    }
  }
  rocksdb_transactiondb_batched_multi_get_cf(db, options, cf, 0, NULL, NULL,
                                             NULL, NULL, false);
  rocksdb_transactiondb_batched_multi_get_cf_slice(db, options, cf, 0, NULL,
                                                   NULL, NULL, true);
}

static void TestTxnDBBatchedMultiGet(void) {
  rocksdb_options_t* options = rocksdb_options_create();
  rocksdb_readoptions_t* ro = rocksdb_readoptions_create();
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  rocksdb_transaction_options_t* to = rocksdb_transaction_options_create();
  rocksdb_transactiondb_options_t* dbo = rocksdb_transactiondb_options_create();
  char path[200];
  char* err = NULL;
  snprintf(path, sizeof(path), "%s/rocksdb_c_test-batched-txn-%" PRIu64,
           GetTempDir(), GetTestId());
  rocksdb_options_set_create_if_missing(options, 1);
  rocksdb_transaction_options_set_skip_prepare(to, 0);
  for (int policy = rocksdb_txndb_write_policy_write_committed;
       policy <= rocksdb_txndb_write_policy_write_prepared; ++policy) {
    rocksdb_destroy_db(options, path, &err);
    CheckNoError(err);
    rocksdb_transactiondb_options_set_write_policy(dbo, policy);
    rocksdb_transactiondb_t* db =
        rocksdb_transactiondb_open(options, dbo, path, &err);
    CheckNoError(err);
    rocksdb_column_family_handle_t* cf =
        rocksdb_transactiondb_create_column_family(db, options, "batched",
                                                   &err);
    CheckNoError(err);
    rocksdb_transactiondb_put_cf(db, wo, cf, "a", 1, "old", 3, &err);
    CheckNoError(err);
    rocksdb_transactiondb_put_cf(db, wo, cf, "b", 1, "", 0, &err);
    CheckNoError(err);
    CheckTxnDBBatchedMultiGet(db, ro, cf, "old");

    rocksdb_transaction_t* txn = rocksdb_transaction_begin(db, wo, to, NULL);
    rocksdb_transaction_set_name(txn, "batched", 7, &err);
    CheckNoError(err);
    rocksdb_transaction_put_cf(txn, cf, "a", 1, "new", 3, &err);
    CheckNoError(err);
    rocksdb_transaction_prepare(txn, &err);
    CheckNoError(err);
    // WRITE_PREPARED puts the new value in the base DB before commit. Reads
    // must go through TransactionDB to keep that value invisible.
    CheckTxnDBBatchedMultiGet(db, ro, cf, "old");
    const rocksdb_snapshot_t* snapshot =
        rocksdb_transactiondb_create_snapshot(db);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    CheckTxnDBBatchedMultiGet(db, ro, cf, "new");
    rocksdb_readoptions_set_snapshot(ro, snapshot);
    CheckTxnDBBatchedMultiGet(db, ro, cf, "old");
    rocksdb_readoptions_set_snapshot(ro, NULL);
    rocksdb_transactiondb_release_snapshot(db, snapshot);
    // A timestamp on a non-timestamped column family must return an error,
    // rather than being reported as a missing key.
    rocksdb_readoptions_set_timestamp(ro, "timestamp", 9);
    for (int slices = 0; slices <= 1; ++slices) {
      const char* keys[1] = {"a"};
      const size_t sizes[1] = {1};
      const rocksdb_slice_t key_slices[1] = {{"a", 1}};
      rocksdb_pinnableslice_t* values[1];
      char* errs[1];
      if (slices) {
        rocksdb_transactiondb_batched_multi_get_cf_slice(
            db, ro, cf, 1, key_slices, values, errs, true);
      } else {
        rocksdb_transactiondb_batched_multi_get_cf(db, ro, cf, 1, keys, sizes,
                                                   values, errs, true);
      }
      CheckCondition(values[0] == NULL);
      CheckCondition(errs[0] != NULL);
      rocksdb_free(errs[0]);
    }
    rocksdb_readoptions_set_timestamp(ro, NULL, 0);
    rocksdb_transaction_destroy(txn);
    rocksdb_column_family_handle_destroy(cf);
    rocksdb_transactiondb_close(db);
    rocksdb_destroy_db(options, path, &err);
    CheckNoError(err);
  }
  rocksdb_transactiondb_options_destroy(dbo);
  rocksdb_transaction_options_destroy(to);
  rocksdb_writeoptions_destroy(wo);
  rocksdb_readoptions_destroy(ro);
  rocksdb_options_destroy(options);
}

static void CheckIter(rocksdb_iterator_t* iter, const char* key,
                      const char* val) {
  size_t len;
  const char* str;
  str = rocksdb_iter_key(iter, &len);
  CheckEqual(key, str, len);
  str = rocksdb_iter_value(iter, &len);
  CheckEqual(val, str, len);
}

// Callback from rocksdb_writebatch_iterate()
static void CheckPut(void* ptr, const char* k, size_t klen, const char* v,
                     size_t vlen) {
  int* state = (int*)ptr;
  CheckCondition(*state < 2);
  switch (*state) {
    case 0:
      CheckEqual("bar", k, klen);
      CheckEqual("b", v, vlen);
      break;
    case 1:
      CheckEqual("box", k, klen);
      CheckEqual("c", v, vlen);
      break;
  }
  (*state)++;
}

// Callback from rocksdb_writebatch_iterate()
static void CheckDel(void* ptr, const char* k, size_t klen) {
  int* state = (int*)ptr;
  CheckCondition(*state == 2);
  CheckEqual("bar", k, klen);
  (*state)++;
}

static void NopPut(void* ptr, const char* k, size_t klen, const char* v,
                   size_t vlen) {
  (void)ptr;
  (void)k;
  (void)klen;
  (void)v;
  (void)vlen;
}

static void NopDel(void* ptr, const char* k, size_t klen) {
  (void)ptr;
  (void)k;
  (void)klen;
}

static void CheckLogData(void* ptr, const char* blob, size_t blen) {
  CheckEqual("log_blob", blob, blen);
  int* found = (int*)ptr;
  *found = 1;
}

// Callback from rocksdb_writebatch_iterate_cf()
static void CheckPutCF(void* ptr, uint32_t cfid, const char* k, size_t klen,
                       const char* v, size_t vlen) {
  int* state = (int*)ptr;
  switch (*state) {
    case 0:
      CheckEqual("bar", k, klen);
      CheckEqual("b", v, vlen);
      CheckCondition(cfid == 1);
      break;
    case 1:
      CheckEqual("box", k, klen);
      CheckEqual("c", v, vlen);
      CheckCondition(cfid == 1);
      break;
    case 4:
      CheckEqual("foo", k, klen);
      CheckEqual("f", v, vlen);
      CheckCondition(cfid == 0);
      break;
    case 6:
      CheckEqual("baz", k, klen);
      CheckEqual("a", v, vlen);
      CheckCondition(cfid == 0);
      break;
    default:
      CheckCondition(false);
  }
  (*state)++;
}

// Callback from rocksdb_writebatch_iterate_cf()
static void CheckDelCF(void* ptr, uint32_t cfid, const char* k, size_t klen) {
  int* state = (int*)ptr;
  switch (*state) {
    case 2:
      CheckEqual("bar", k, klen);
      CheckCondition(cfid == 1);
      break;
    case 5:
      CheckEqual("foo", k, klen);
      CheckCondition(cfid == 0);
      break;
    default:
      CheckCondition(false);
  }
  (*state)++;
}

// Callback from rocksdb_writebatch_iterate_cf()
static void CheckMergeCF(void* ptr, uint32_t cfid, const char* k, size_t klen,
                         const char* v, size_t vlen) {
  int* state = (int*)ptr;
  switch (*state) {
    case 3:
      CheckEqual("box", k, klen);
      CheckEqual("cc", v, vlen);
      CheckCondition(cfid == 1);
      break;
    case 7:
      CheckEqual("baz", k, klen);
      CheckEqual("aa", v, vlen);
      CheckCondition(cfid == 0);
      break;
    default:
      CheckCondition(false);
  }
  (*state)++;
}

static void CmpDestroy(void* arg) { (void)arg; }

static int CmpCompare(void* arg, const char* a, size_t alen, const char* b,
                      size_t blen) {
  (void)arg;
  size_t n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) {
      r = -1;
    } else if (alen > blen) {
      r = +1;
    }
  }
  return r;
}

static const char* CmpName(void* arg) {
  (void)arg;
  return "foo";
}

// Custom compaction filter
static void CFilterDestroy(void* arg) { (void)arg; }
static const char* CFilterName(void* arg) {
  (void)arg;
  return "foo";
}
static unsigned char CFilterFilter(void* arg, int level, const char* key,
                                   size_t key_length,
                                   const char* existing_value,
                                   size_t value_length, char** new_value,
                                   size_t* new_value_length,
                                   unsigned char* value_changed) {
  (void)arg;
  (void)level;
  (void)existing_value;
  (void)value_length;
  if (key_length == 3) {
    if (memcmp(key, "bar", key_length) == 0) {
      return 1;
    } else if (memcmp(key, "baz", key_length) == 0) {
      *value_changed = 1;
      *new_value = "newbazvalue";
      *new_value_length = 11;
      return 0;
    }
  }
  return 0;
}

static void CFilterFactoryDestroy(void* arg) { (void)arg; }
static const char* CFilterFactoryName(void* arg) {
  (void)arg;
  return "foo";
}
static rocksdb_compactionfilter_t* CFilterCreate(
    void* arg, rocksdb_compactionfiltercontext_t* context) {
  (void)arg;
  (void)context;
  return rocksdb_compactionfilter_create(NULL, CFilterDestroy, CFilterFilter,
                                         CFilterName);
}

typedef struct TableFilterState {
  int calls;
  int saw_entries_1;
  int saw_entries_2;
  int saw_entries_3;
  uint64_t hidden_num_entries;
  int destructor_calls;
} TableFilterState;

static void TableFilterStateDestroy(void* arg) {
  TableFilterState* state = (TableFilterState*)arg;
  state->destructor_calls++;
}

static unsigned char TableFilterCallback(
    void* arg, const rocksdb_table_properties_t* props) {
  TableFilterState* state = (TableFilterState*)arg;
  uint64_t num_entries = rocksdb_table_properties_num_entries(props);
  size_t cf_name_len = 0;
  const char* cf_name =
      rocksdb_table_properties_column_family_name(props, &cf_name_len);
  size_t readable_count =
      rocksdb_table_properties_readable_properties_count(props);

  state->calls++;
  CheckEqual("default", cf_name, cf_name_len);
  CheckCondition(1 == rocksdb_table_properties_has_key_largest_seqno(props));
  CheckCondition(1 == rocksdb_table_properties_has_key_smallest_seqno(props));
  if (readable_count > 0) {
    size_t key_len = 0;
    size_t value_len = 0;
    CheckCondition(rocksdb_table_properties_readable_properties_key_at(
                       props, 0, &key_len) != NULL);
    CheckCondition(key_len > 0);
    CheckCondition(rocksdb_table_properties_readable_properties_value_at(
                       props, 0, &value_len) != NULL);
    CheckCondition(value_len > 0);
  }

  switch (num_entries) {
    case 1:
      state->saw_entries_1 = 1;
      break;
    case 2:
      state->saw_entries_2 = 1;
      break;
    case 3:
      state->saw_entries_3 = 1;
      break;
    default:
      CheckCondition(0);
  }

  return num_entries != state->hidden_num_entries;
}

typedef struct ListenerState {
  int flush_count;
  int saw_flush_entries_1;
  int saw_flush_entries_2;
  int saw_flush_entries_3;
  int compaction_count;
  int external_ingest_count;
  const char* expected_external_file_path;
} ListenerState;

static void ListenerNoopDestructor(void* arg) { (void)arg; }

static void ListenerOnFlushBegin(void* state, rocksdb_t* db,
                                 const rocksdb_flushjobinfo_t* info) {
  (void)state;
  (void)db;
  (void)info;
}

static void ListenerOnFlushCompleted(void* arg, rocksdb_t* db,
                                     const rocksdb_flushjobinfo_t* info) {
  ListenerState* state = (ListenerState*)arg;
  const rocksdb_table_properties_t* props;
  size_t cf_name_len = 0;
  const char* cf_name = rocksdb_flushjobinfo_cf_name(info, &cf_name_len);
  uint64_t num_entries;

  (void)db;
  CheckEqual("default", cf_name, cf_name_len);
  props = rocksdb_flushjobinfo_table_properties(info);
  num_entries = rocksdb_table_properties_num_entries(props);
  CheckCondition(1 == rocksdb_table_properties_has_key_largest_seqno(props));
  CheckCondition(1 == rocksdb_table_properties_has_key_smallest_seqno(props));
  CheckCondition(0 ==
                 rocksdb_flushjobinfo_blob_file_addition_infos_count(info));

  switch (num_entries) {
    case 1:
      state->saw_flush_entries_1 = 1;
      break;
    case 2:
      state->saw_flush_entries_2 = 1;
      break;
    case 3:
      state->saw_flush_entries_3 = 1;
      break;
    default:
      CheckCondition(0);
  }
  state->flush_count++;
}

static void ListenerOnCompactionBegin(void* state, rocksdb_t* db,
                                      const rocksdb_compactionjobinfo_t* info) {
  (void)state;
  (void)db;
  (void)info;
}

static void ListenerOnCompactionCompleted(
    void* arg, rocksdb_t* db, const rocksdb_compactionjobinfo_t* info) {
  ListenerState* state = (ListenerState*)arg;
  size_t input_count = rocksdb_compactionjobinfo_input_files_count(info);
  size_t output_count = rocksdb_compactionjobinfo_output_files_count(info);
  size_t file_name_len = 0;
  const char* file_name;
  const rocksdb_compaction_file_info_t* input_file_info;
  const rocksdb_compaction_file_info_t* output_file_info;
  const rocksdb_compaction_job_stats_t* stats;
  const rocksdb_table_properties_t* props;

  (void)db;
  CheckCondition(input_count > 0);
  CheckCondition(output_count > 0);

  file_name = rocksdb_compactionjobinfo_input_file_at(info, 0, &file_name_len);
  CheckCondition(file_name != NULL);
  CheckCondition(file_name_len > 0);

  input_file_info = rocksdb_compactionjobinfo_input_file_info_at(info, 0);
  CheckCondition(input_file_info != NULL);
  CheckCondition(rocksdb_compaction_file_info_file_number(input_file_info) > 0);

  output_file_info = rocksdb_compactionjobinfo_output_file_info_at(info, 0);
  CheckCondition(output_file_info != NULL);
  CheckCondition(rocksdb_compaction_file_info_file_number(output_file_info) >
                 0);

  stats = rocksdb_compactionjobinfo_stats(info);
  CheckCondition(stats != NULL);
  CheckCondition(rocksdb_compactionjobinfo_input_file_infos_count(info) ==
                 input_count);
  CheckCondition(rocksdb_compactionjobinfo_output_file_infos_count(info) ==
                 output_count);
  CheckCondition(rocksdb_compaction_job_stats_num_input_files(stats) >=
                 input_count);
  CheckCondition(rocksdb_compaction_job_stats_num_output_files(stats) <=
                 output_count);

  CheckCondition(rocksdb_compactionjobinfo_table_properties_count(info) > 0);
  props = rocksdb_compactionjobinfo_table_properties_for_file(info, file_name,
                                                              file_name_len);
  CheckCondition(props != NULL);
  CheckCondition(rocksdb_table_properties_num_entries(props) > 0);

  CheckCondition(
      0 == rocksdb_compactionjobinfo_blob_file_addition_infos_count(info));
  CheckCondition(0 ==
                 rocksdb_compactionjobinfo_blob_file_garbage_infos_count(info));
  state->compaction_count++;
}

static void ListenerOnSubcompactionBegin(
    void* state, const rocksdb_subcompactionjobinfo_t* info) {
  (void)state;
  (void)info;
}

static void ListenerOnSubcompactionCompleted(
    void* state, const rocksdb_subcompactionjobinfo_t* info) {
  (void)state;
  (void)info;
}

static void ListenerOnExternalFileIngested(
    void* arg, rocksdb_t* db, const rocksdb_externalfileingestioninfo_t* info) {
  ListenerState* state = (ListenerState*)arg;
  size_t path_len = 0;
  const char* external_path =
      rocksdb_externalfileingestioninfo_external_file_path(info, &path_len);
  const rocksdb_table_properties_t* props =
      rocksdb_externalfileingestioninfo_table_properties(info);

  (void)db;
  CheckEqual(state->expected_external_file_path, external_path, path_len);
  CheckCondition(rocksdb_table_properties_num_entries(props) == 2);
  state->external_ingest_count++;
}

static void ListenerOnBackgroundError(void* state, uint32_t reason,
                                      rocksdb_status_ptr_t* status) {
  (void)state;
  (void)reason;
  (void)status;
}

static void ListenerOnStallConditionsChanged(
    void* state, const rocksdb_writestallinfo_t* info) {
  (void)state;
  (void)info;
}

static void ListenerOnMemtableSealed(void* state,
                                     const rocksdb_memtableinfo_t* info) {
  (void)state;
  (void)info;
}

typedef struct WalFilterState {
  int column_family_map_calls;
  int log_record_calls;
  int destructor_calls;
} WalFilterState;

static void WalFilterDestroy(void* arg) {
  WalFilterState* state = (WalFilterState*)arg;
  state->destructor_calls++;
}

static void WalFilterColumnFamilyLogNumberMap(
    void* arg, const uint32_t* column_family_ids, const uint64_t* log_numbers,
    size_t column_family_log_number_count,
    const char* const* column_family_names,
    const size_t* column_family_name_lengths,
    const uint32_t* column_family_name_ids, size_t column_family_name_count) {
  WalFilterState* state = (WalFilterState*)arg;
  size_t i;

  if (column_family_log_number_count > 0) {
    CheckCondition(column_family_ids != NULL);
    CheckCondition(log_numbers != NULL);
  }
  if (column_family_name_count > 0) {
    CheckCondition(column_family_names != NULL);
    CheckCondition(column_family_name_lengths != NULL);
    CheckCondition(column_family_name_ids != NULL);
  }
  for (i = 0; i < column_family_name_count; ++i) {
    CheckCondition(column_family_names[i] != NULL);
    CheckCondition(column_family_name_lengths[i] > 0);
  }
  state->column_family_map_calls++;
}

static int WalFilterLogRecordFound(void* arg, unsigned long long log_number,
                                   const char* log_file_name,
                                   size_t log_file_name_len,
                                   const rocksdb_writebatch_t* batch,
                                   rocksdb_writebatch_t* new_batch,
                                   unsigned char* batch_changed) {
  WalFilterState* state = (WalFilterState*)arg;
  (void)log_number;
  (void)log_file_name;
  (void)log_file_name_len;
  (void)new_batch;

  CheckCondition(rocksdb_writebatch_count((rocksdb_writebatch_t*)batch) == 1);
  *batch_changed = 0;
  state->log_record_calls++;
  if (state->log_record_calls == 1) {
    return rocksdb_wal_filter_ignore_current_record;
  }
  return rocksdb_wal_filter_continue_processing;
}

static const char* WalFilterName(void* arg) {
  (void)arg;
  return "CTestWalFilter";
}

void CheckMetaData(rocksdb_column_family_metadata_t* cf_meta,
                   const char* expected_cf_name) {
  char* cf_name = rocksdb_column_family_metadata_get_name(cf_meta);
  assert(strcmp(cf_name, expected_cf_name) == 0);
  rocksdb_free(cf_name);

  size_t cf_size = rocksdb_column_family_metadata_get_size(cf_meta);
  assert(cf_size > 0);
  size_t cf_file_count = rocksdb_column_family_metadata_get_size(cf_meta);
  assert(cf_file_count > 0);

  uint64_t total_level_size = 0;
  size_t total_file_count = 0;
  size_t level_count = rocksdb_column_family_metadata_get_level_count(cf_meta);
  assert(level_count > 0);
  for (size_t l = 0; l < level_count; ++l) {
    rocksdb_level_metadata_t* level_meta =
        rocksdb_column_family_metadata_get_level_metadata(cf_meta, l);
    assert(level_meta);
    assert(rocksdb_level_metadata_get_level(level_meta) >= (int)l);
    uint64_t level_size = rocksdb_level_metadata_get_size(level_meta);
    uint64_t file_size_in_level = 0;

    size_t file_count = rocksdb_level_metadata_get_file_count(level_meta);
    total_file_count += file_count;
    for (size_t f = 0; f < file_count; ++f) {
      rocksdb_sst_file_metadata_t* file_meta =
          rocksdb_level_metadata_get_sst_file_metadata(level_meta, f);
      assert(file_meta);

      uint64_t file_size = rocksdb_sst_file_metadata_get_size(file_meta);
      assert(file_size > 0);
      file_size_in_level += file_size;

      char* file_name =
          rocksdb_sst_file_metadata_get_relative_filename(file_meta);
      assert(file_name);
      assert(strlen(file_name) > 0);
      rocksdb_free(file_name);

      size_t smallest_key_len;
      char* smallest_key = rocksdb_sst_file_metadata_get_smallestkey(
          file_meta, &smallest_key_len);
      assert(smallest_key);
      assert(smallest_key_len > 0);
      size_t largest_key_len;
      char* largest_key =
          rocksdb_sst_file_metadata_get_largestkey(file_meta, &largest_key_len);
      assert(largest_key);
      assert(largest_key_len > 0);
      rocksdb_free(smallest_key);
      rocksdb_free(largest_key);

      rocksdb_sst_file_metadata_destroy(file_meta);
    }
    assert(level_size == file_size_in_level);
    total_level_size += level_size;
    rocksdb_level_metadata_destroy(level_meta);
  }
  assert(total_file_count > 0);
  assert(cf_size == total_level_size);
}

void GetAndCheckMetaData(rocksdb_t* db) {
  rocksdb_column_family_metadata_t* cf_meta =
      rocksdb_get_column_family_metadata(db);

  CheckMetaData(cf_meta, "default");

  rocksdb_column_family_metadata_destroy(cf_meta);
}

void GetAndCheckMetaDataCf(rocksdb_t* db,
                           rocksdb_column_family_handle_t* handle,
                           const char* cf_name) {
  // Compact to make sure we have at least one sst file to obtain datadata.
  rocksdb_compact_range_cf(db, handle, NULL, 0, NULL, 0);

  rocksdb_column_family_metadata_t* cf_meta =
      rocksdb_get_column_family_metadata_cf(db, handle);

  CheckMetaData(cf_meta, cf_name);

  rocksdb_column_family_metadata_destroy(cf_meta);
}

static rocksdb_t* CheckCompaction(rocksdb_t* db, rocksdb_options_t* options,
                                  rocksdb_readoptions_t* roptions,
                                  rocksdb_writeoptions_t* woptions) {
  char* err = NULL;
  db = rocksdb_open(options, dbname, &err);
  CheckNoError(err);
  rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", "foovalue");
  rocksdb_put(db, woptions, "bar", 3, "barvalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "bar", "barvalue");
  rocksdb_put(db, woptions, "baz", 3, "bazvalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "baz", "bazvalue");

  // Disable compaction
  rocksdb_disable_manual_compaction(db);
  rocksdb_compact_range(db, NULL, 0, NULL, 0);
  // should not filter anything when disabled
  CheckGet(db, roptions, "foo", "foovalue");
  CheckGet(db, roptions, "bar", "barvalue");
  CheckGet(db, roptions, "baz", "bazvalue");
  // Reenable compaction
  rocksdb_enable_manual_compaction(db);

  // Force compaction
  rocksdb_compact_range(db, NULL, 0, NULL, 0);
  rocksdb_wait_for_compact_options_t* wco;
  wco = rocksdb_wait_for_compact_options_create();
  rocksdb_wait_for_compact(db, wco, &err);
  CheckNoError(err);
  rocksdb_wait_for_compact_options_destroy(wco);
  // should have filtered bar, but not foo
  CheckGet(db, roptions, "foo", "foovalue");
  CheckGet(db, roptions, "bar", NULL);
  CheckGet(db, roptions, "baz", "newbazvalue");

  rocksdb_suggest_compact_range(db, "bar", 3, "foo", 3, &err);
  GetAndCheckMetaData(db);
  CheckNoError(err);

  return db;
}

// Custom merge operator
static void MergeOperatorDestroy(void* arg) { (void)arg; }
static const char* MergeOperatorName(void* arg) {
  (void)arg;
  return "TestMergeOperator";
}
static char* MergeOperatorFullMerge(
    void* arg, const char* key, size_t key_length, const char* existing_value,
    size_t existing_value_length, const char* const* operands_list,
    const size_t* operands_list_length, int num_operands,
    unsigned char* success, size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)existing_value;
  (void)existing_value_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *new_value_length = 4;
  *success = 1;
  char* result = malloc(4);
  memcpy(result, "fake", 4);
  return result;
}
static char* MergeOperatorPartialMerge(void* arg, const char* key,
                                       size_t key_length,
                                       const char* const* operands_list,
                                       const size_t* operands_list_length,
                                       int num_operands, unsigned char* success,
                                       size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *new_value_length = 4;
  *success = 1;
  char* result = malloc(4);
  memcpy(result, "fake", 4);
  return result;
}

static char* FailingMergeOperatorFullMerge(
    void* arg, const char* key, size_t key_length, const char* existing_value,
    size_t existing_value_length, const char* const* operands_list,
    const size_t* operands_list_length, int num_operands,
    unsigned char* success, size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)existing_value;
  (void)existing_value_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *success = 0;
  *new_value_length = 0;
  return NULL;
}

static char* FailingMergeOperatorPartialMerge(
    void* arg, const char* key, size_t key_length,
    const char* const* operands_list, const size_t* operands_list_length,
    int num_operands, unsigned char* success, size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *success = 0;
  *new_value_length = 0;
  return NULL;
}

static void CheckTxnGet(rocksdb_transaction_t* txn,
                        const rocksdb_readoptions_t* options, const char* key,
                        const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transaction_get(txn, options, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnGetCF(rocksdb_transaction_t* txn,
                          const rocksdb_readoptions_t* options,
                          rocksdb_column_family_handle_t* column_family,
                          const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transaction_get_cf(txn, options, column_family, key,
                                   strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnPinGet(rocksdb_transaction_t* txn,
                           const rocksdb_readoptions_t* options,
                           const char* key, const char* expected) {
  rocksdb_pinnableslice_t* p = NULL;
  const char* val = NULL;
  char* err = NULL;
  size_t val_len;
  p = rocksdb_transaction_get_pinned(txn, options, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckTxnPinGetCF(rocksdb_transaction_t* txn,
                             const rocksdb_readoptions_t* options,
                             rocksdb_column_family_handle_t* column_family,
                             const char* key, const char* expected) {
  rocksdb_pinnableslice_t* p = NULL;
  const char* val = NULL;
  char* err = NULL;
  size_t val_len;
  p = rocksdb_transaction_get_pinned_cf(txn, options, column_family, key,
                                        strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckTxnGetForUpdate(rocksdb_transaction_t* txn,
                                 const rocksdb_readoptions_t* options,
                                 const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transaction_get_for_update(txn, options, key, strlen(key),
                                           &val_len, true, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnDBGet(rocksdb_transactiondb_t* txn_db,
                          const rocksdb_readoptions_t* options, const char* key,
                          const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transactiondb_get(txn_db, options, key, strlen(key), &val_len,
                                  &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnDBGetCF(rocksdb_transactiondb_t* txn_db,
                            const rocksdb_readoptions_t* options,
                            rocksdb_column_family_handle_t* column_family,
                            const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transactiondb_get_cf(txn_db, options, column_family, key,
                                     strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnGetForUpdateCF(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transaction_get_for_update_cf(
      txn, options, column_family, key, strlen(key), &val_len, true, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnDBPinGet(rocksdb_transactiondb_t* txn_db,
                             const rocksdb_readoptions_t* options,
                             const char* key, const char* expected) {
  rocksdb_pinnableslice_t* p = NULL;
  const char* val = NULL;
  char* err = NULL;
  size_t val_len;
  p = rocksdb_transactiondb_get_pinned(txn_db, options, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckTxnDBPinGetCF(rocksdb_transactiondb_t* txn_db,
                               const rocksdb_readoptions_t* options,
                               rocksdb_column_family_handle_t* column_family,
                               const char* key, const char* expected) {
  rocksdb_pinnableslice_t* p = NULL;
  const char* val = NULL;
  char* err = NULL;
  size_t val_len;
  p = rocksdb_transactiondb_get_pinned_cf(txn_db, options, column_family, key,
                                          strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void LoadAndCheckLatestOptions(const char* db_name, rocksdb_env_t* env,
                                      bool ignore_unknown_options,
                                      rocksdb_cache_t* cache,
                                      rocksdb_comparator_t* cmp,
                                      const size_t expected_num_column_families,
                                      const char** expected_cf_names,
                                      const char* expected_open_err) {
  rocksdb_options_t* db_options;
  size_t num_column_families;
  char** list_column_family_names;
  rocksdb_options_t** list_column_family_options;
  char* err = 0;

  // load the latest rocksdb option
  rocksdb_load_latest_options(db_name, env, ignore_unknown_options, cache,
                              &db_options, &num_column_families,
                              &list_column_family_names,
                              &list_column_family_options, &err);
  assert(num_column_families == expected_num_column_families);
  CheckNoError(err);

  // verify the loaded options by opening the db.
  rocksdb_options_set_error_if_exists(db_options, 0);

  char** list_const_cf_names =
      (char**)malloc(num_column_families * sizeof(char*));
  rocksdb_options_t** list_const_cf_options = (rocksdb_options_t**)malloc(
      num_column_families * sizeof(rocksdb_options_t*));
  for (size_t i = 0; i < num_column_families; ++i) {
    assert(strcmp(list_column_family_names[i], expected_cf_names[i]) == 0);
    list_const_cf_names[i] = list_column_family_names[i];
    if (cmp) {
      rocksdb_options_set_comparator(list_column_family_options[i], cmp);
    }
    list_const_cf_options[i] = list_column_family_options[i];
  }
  rocksdb_column_family_handle_t** handles =
      (rocksdb_column_family_handle_t**)malloc(
          num_column_families * sizeof(rocksdb_column_family_handle_t*));

  rocksdb_t* db = rocksdb_open_column_families(
      db_options, db_name, (int)num_column_families,
      (const char* const*)list_const_cf_names,
      (const rocksdb_options_t* const*)list_const_cf_options, handles, &err);
  if (expected_open_err == NULL) {
    CheckNoError(err);
    for (size_t i = 0; i < num_column_families; ++i) {
      rocksdb_column_family_handle_destroy(handles[i]);
    }
    free(handles);
    rocksdb_close(db);
  } else {
    assert(err != NULL);
    assert(strcmp(err, expected_open_err) == 0);
    free(handles);
    free(err);
  }

  free(list_const_cf_names);
  free(list_const_cf_options);
  rocksdb_load_latest_options_destroy(db_options, list_column_family_names,
                                      list_column_family_options,
                                      num_column_families);
}

// Global state for tracking remote compaction calls
typedef struct {
  int schedule_called;
  int wait_called;
  int cancel_called;
  char last_scheduled_job_id[256];
  char last_db_name[256];
} RemoteCompactionState;

typedef struct {
  int progress_calls;
  int exclude_calls;
  char last_relative_file[256];
} CreateBackupOptionsState;

// Schedule callback - gets called when compaction is scheduled
static rocksdb_compactionservice_scheduleresponse_t* RemoteCompactionSchedule(
    void* state, const rocksdb_compactionservice_jobinfo_t* info,
    const char* input, size_t input_len) {
  (void)input;
  (void)input_len;
  RemoteCompactionState* rcs = (RemoteCompactionState*)state;
  rcs->schedule_called++;

  // Extract job info
  size_t db_name_len;
  const char* db_name =
      rocksdb_compactionservice_jobinfo_t_get_db_name(info, &db_name_len);
  memcpy(rcs->last_db_name, db_name, db_name_len);
  rcs->last_db_name[db_name_len] = '\0';

  // Generate a job ID
  snprintf(rcs->last_scheduled_job_id, sizeof(rcs->last_scheduled_job_id),
           "job-%d", rcs->schedule_called);

  // Create response with success status
  char* err = NULL;
  rocksdb_compactionservice_scheduleresponse_t* response =
      rocksdb_compactionservice_scheduleresponse_create(
          rcs->last_scheduled_job_id,
          rocksdb_compactionservice_jobstatus_success, &err);
  if (err) {
    free(err);
  }
  return response;
}

// Wait callback - simulates waiting for remote compaction to complete
static int RemoteCompactionWait(void* state, const char* scheduled_job_id,
                                char** result, size_t* result_len) {
  RemoteCompactionState* rcs = (RemoteCompactionState*)state;
  rcs->wait_called++;

  if (strcmp(scheduled_job_id, rcs->last_scheduled_job_id) != 0) {
    return rocksdb_compactionservice_jobstatus_failure;
  }

  // For testing purposes, return kUseLocal to cause RocksDB to fall back to
  // local compaction. This tests the callback mechanism without needing a fully
  // serialized result. In a real scenario, this would communicate with a remote
  // worker that calls rocksdb_open_and_compact() and returns a properly
  // serialized CompactionServiceResult
  *result = NULL;
  *result_len = 0;

  return rocksdb_compactionservice_jobstatus_use_local;
}

// Cancel callback - cancels pending jobs
static void RemoteCompactionCancel(void* state) {
  RemoteCompactionState* rcs = (RemoteCompactionState*)state;
  rcs->cancel_called++;
}

// Destructor callback
static void RemoteCompactionDestroy(void* state) { (void)state; }

static void CreateBackupProgress(void* state) {
  CreateBackupOptionsState* backup_state = (CreateBackupOptionsState*)state;
  backup_state->progress_calls++;
}

static unsigned char CreateBackupExcludeFile(void* state,
                                             const char* relative_file,
                                             size_t relative_file_len) {
  CreateBackupOptionsState* backup_state = (CreateBackupOptionsState*)state;
  backup_state->exclude_calls++;
  if (relative_file_len >= sizeof(backup_state->last_relative_file)) {
    relative_file_len = sizeof(backup_state->last_relative_file) - 1;
  }
  memcpy(backup_state->last_relative_file, relative_file, relative_file_len);
  backup_state->last_relative_file[relative_file_len] = '\0';
  return 0;
}

// NULL schedule callback for testing failure handling
static rocksdb_compactionservice_scheduleresponse_t* NullSchedule(
    void* state, const rocksdb_compactionservice_jobinfo_t* info,
    const char* input, size_t input_len) {
  (void)state;
  (void)info;
  (void)input;
  (void)input_len;
  return NULL;  // Return NULL to simulate failure
}

// Auto-generated set->get->assert round-trip coverage for the generated option
// wrappers. Defines static rocksdb_roundtrip_test_* helpers and
// rocksdb_run_generated_roundtrip_tests(); regenerated by
// tools/c_api_gen/regen_all.py. Included here (after the CheckCondition macro)
// so it can use the test harness.
#include "c_api_gen/c_generated_roundtrip_tests.c.inc"

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;
  rocksdb_t* db;
  rocksdb_comparator_t* cmp;
  rocksdb_cache_t* cache;
  rocksdb_dbpath_t* dbpath;
  rocksdb_env_t* env;
  rocksdb_options_t* options;
  rocksdb_compactoptions_t* coptions;
  rocksdb_block_based_table_options_t* table_options;
  rocksdb_readoptions_t* roptions;
  rocksdb_writeoptions_t* woptions;
  rocksdb_ratelimiter_t* rate_limiter;
  rocksdb_transactiondb_t* txn_db;
  rocksdb_transactiondb_options_t* txn_db_options;
  rocksdb_transaction_t* txn;
  rocksdb_transaction_options_t* txn_options;
  rocksdb_optimistictransactiondb_t* otxn_db;
  rocksdb_optimistictransaction_options_t* otxn_options;
  char* err = NULL;
  int run = -1;

  snprintf(dbname, sizeof(dbname), "%s/rocksdb_c_test-%" PRIu64, GetTempDir(),
           GetTestId());

  snprintf(dbbackupname, sizeof(dbbackupname),
           "%s/rocksdb_c_test-%" PRIu64 "-backup", GetTempDir(), GetTestId());

  snprintf(dbcheckpointname, sizeof(dbcheckpointname),
           "%s/rocksdb_c_test-%" PRIu64 "-checkpoint", GetTempDir(),
           GetTestId());

  snprintf(sstfilename, sizeof(sstfilename),
           "%s/rocksdb_c_test-%" PRIu64 "-sst", GetTempDir(), GetTestId());

  snprintf(dbpathname, sizeof(dbpathname),
           "%s/rocksdb_c_test-%" PRIu64 "-dbpath", GetTempDir(), GetTestId());

  StartPhase("create_objects");
  cmp = rocksdb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);
  dbpath = rocksdb_dbpath_create(dbpathname, 1024 * 1024);
  env = rocksdb_create_default_env();

  rocksdb_create_dir_if_missing(env, GetTempDir(), &err);
  CheckNoError(err);

  cache = rocksdb_cache_create_lru(100000);

  options = rocksdb_options_create();
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_options_set_error_if_exists(options, 1);
  rocksdb_options_set_env(options, env);
  rocksdb_options_set_info_log(options, NULL);
  rocksdb_options_set_write_buffer_size(options, 100000);
  rocksdb_options_set_paranoid_checks(options, 1);
  rocksdb_options_set_max_open_files(options, 10);
  /* Compatibility with how test was written */
  rocksdb_options_set_write_dbid_to_manifest(options, 0);

  table_options = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(table_options, cache);
  rocksdb_block_based_options_set_data_block_index_type(table_options, 1);
  rocksdb_block_based_options_set_data_block_hash_ratio(table_options, 0.75);
  rocksdb_block_based_options_set_index_block_search_type(
      table_options, rocksdb_block_based_table_index_block_search_type_auto);
  rocksdb_block_based_options_set_uniform_cv_threshold(table_options, 0.2);
  rocksdb_block_based_options_set_top_level_index_pinning_tier(table_options,
                                                               1);
  rocksdb_block_based_options_set_partition_pinning_tier(table_options, 2);
  rocksdb_block_based_options_set_unpartitioned_pinning_tier(table_options, 3);
  rocksdb_options_set_block_based_table_factory(options, table_options);

  rocksdb_options_set_compression(options, rocksdb_no_compression);
  rocksdb_options_set_compression_options(options, -14, -1, 0, 0);
  int compression_levels[] = {rocksdb_no_compression, rocksdb_no_compression,
                              rocksdb_no_compression, rocksdb_no_compression};
  rocksdb_options_set_compression_per_level(options, compression_levels, 4);
  rate_limiter = rocksdb_ratelimiter_create(1000 * 1024 * 1024, 100 * 1000, 10);
  rocksdb_options_set_ratelimiter(options, rate_limiter);
  rocksdb_ratelimiter_destroy(rate_limiter);

  rate_limiter =
      rocksdb_ratelimiter_create_auto_tuned(1000 * 1024 * 1024, 100 * 1000, 10);
  rocksdb_options_set_ratelimiter(options, rate_limiter);
  rocksdb_ratelimiter_destroy(rate_limiter);

  rate_limiter = rocksdb_ratelimiter_create_with_mode(1000 * 1024 * 1024,
                                                      100 * 1000, 10, 0, true);
  rocksdb_options_set_ratelimiter(options, rate_limiter);
  rocksdb_ratelimiter_destroy(rate_limiter);

  roptions = rocksdb_readoptions_create();
  rocksdb_readoptions_set_verify_checksums(roptions, 1);
  rocksdb_readoptions_set_fill_cache(roptions, 1);

  woptions = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(woptions, 1);

  coptions = rocksdb_compactoptions_create();
  rocksdb_compactoptions_set_exclusive_manual_compaction(coptions, 1);

  rocksdb_options_add_compact_on_deletion_collector_factory(options, 10000,
                                                            10001);
  rocksdb_options_add_compact_on_deletion_collector_factory_del_ratio(
      options, 10000, 10001, 0.0);

  StartPhase("destroy");
  rocksdb_destroy_db(options, dbname, &err);
  Free(&err);

  StartPhase("open_error");
  rocksdb_open(options, dbname, &err);
  CheckCondition(err != NULL);
  Free(&err);

  StartPhase("open");
  rocksdb_options_set_create_if_missing(options, 1);
  db = rocksdb_open(options, dbname, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", NULL);

  StartPhase("put");
  rocksdb_put(db, woptions, "foo", 3, "hello", 5, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("set_db_options");
  {
    const char* keys[] = {"stats_dump_period_sec"};
    const char* values[] = {"10"};
    rocksdb_set_db_options(db, 1, keys, values, &err);
    CheckNoError(err);

    keys[0] = "not_a_db_option";
    rocksdb_set_db_options(db, 1, keys, values, &err);
    CheckCondition(err != NULL);
    Free(&err);
  }

  StartPhase("backup_and_restore");
  {
    rocksdb_destroy_db(options, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_backup_engine_t* be =
        rocksdb_backup_engine_open(options, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_backup_engine_create_new_backup(be, db, &err);
    CheckNoError(err);

    // need a change to trigger a new backup
    rocksdb_delete(db, woptions, "does-not-exist", 14, &err);
    CheckNoError(err);

    rocksdb_backup_engine_create_new_backup(be, db, &err);
    CheckNoError(err);

    const rocksdb_backup_engine_info_t* bei =
        rocksdb_backup_engine_get_backup_info(be);
    CheckCondition(rocksdb_backup_engine_info_count(bei) > 1);
    rocksdb_backup_engine_info_destroy(bei);

    rocksdb_backup_engine_purge_old_backups(be, 1, &err);
    CheckNoError(err);

    bei = rocksdb_backup_engine_get_backup_info(be);
    CheckCondition(rocksdb_backup_engine_info_count(bei) == 1);
    rocksdb_backup_engine_info_destroy(bei);

    rocksdb_delete(db, woptions, "foo", 3, &err);
    CheckNoError(err);

    // get the identity before the backup
    size_t before_db_id_len = 0;
    char* before_db_id = rocksdb_get_db_identity(db, &before_db_id_len);
    CheckCondition(before_db_id_len == 36);

    rocksdb_close(db);

    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_restore_options_t* restore_options =
        rocksdb_restore_options_create();
    rocksdb_restore_options_set_keep_log_files(restore_options, 0);
    rocksdb_backup_engine_restore_db_from_latest_backup(be, dbname, dbname,
                                                        restore_options, &err);
    CheckNoError(err);
    rocksdb_restore_options_destroy(restore_options);

    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_set_error_if_exists(options, 1);

    CheckGet(db, roptions, "foo", "hello");

    // the db_identity after the backup is different
    size_t after_db_id_len = 0;
    char* after_db_id = rocksdb_get_db_identity(db, &after_db_id_len);
    CheckCondition(after_db_id_len == 36);
    CheckCondition(memcmp(after_db_id, before_db_id, after_db_id_len) != 0);
    Free(&before_db_id);
    Free(&after_db_id);

    rocksdb_backup_engine_stop_backup(be);
    rocksdb_backup_engine_create_new_backup(be, db, &err);
    CheckCondition(err != NULL);
    free(err);
    err = NULL;

    rocksdb_backup_engine_close(be);
  }

  StartPhase("backup_create_options");
  {
    char backup_options_dbname[300];
    char backup_options_backup_dir[300];
    snprintf(backup_options_dbname, sizeof(backup_options_dbname),
             "%s/rocksdb_c_test-%d-backup-options-db", GetTempDir(),
             ((int)geteuid()));
    snprintf(backup_options_backup_dir, sizeof(backup_options_backup_dir),
             "%s/rocksdb_c_test-%d-backup-options-backup", GetTempDir(),
             ((int)geteuid()));

    rocksdb_options_t* backup_db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(backup_db_options, 1);
    rocksdb_destroy_db(backup_db_options, backup_options_dbname, &err);
    CheckNoError(err);

    rocksdb_t* backup_db =
        rocksdb_open(backup_db_options, backup_options_dbname, &err);
    CheckNoError(err);
    rocksdb_put(backup_db, woptions, "backup-key", 10, "backup-value", 12,
                &err);
    CheckNoError(err);

    rocksdb_backup_engine_options_t* backup_engine_options =
        rocksdb_backup_engine_options_create(backup_options_backup_dir);
    rocksdb_backup_engine_options_set_destroy_old_data(backup_engine_options,
                                                       1);
    rocksdb_backup_engine_options_set_callback_trigger_interval_size(
        backup_engine_options, 1);
    rocksdb_backup_engine_t* be =
        rocksdb_backup_engine_open_opts(backup_engine_options, env, &err);
    CheckNoError(err);
    rocksdb_backup_engine_options_destroy(backup_engine_options);

    rocksdb_create_backup_options_t* create_backup_options =
        rocksdb_create_backup_options_create();
    CreateBackupOptionsState backup_state = {0};
    uint32_t backup_id = 0;
    rocksdb_create_backup_options_set_flush_before_backup(create_backup_options,
                                                          1);
    CheckCondition(1 == rocksdb_create_backup_options_get_flush_before_backup(
                            create_backup_options));
    rocksdb_create_backup_options_set_decrease_background_thread_cpu_priority(
        create_backup_options, 1);
    CheckCondition(
        1 ==
        rocksdb_create_backup_options_get_decrease_background_thread_cpu_priority(
            create_backup_options));
    rocksdb_create_backup_options_set_background_thread_cpu_priority(
        create_backup_options, 1);
    CheckCondition(
        1 == rocksdb_create_backup_options_get_background_thread_cpu_priority(
                 create_backup_options));
    rocksdb_create_backup_options_set_atomic_flush(create_backup_options, 1);
    CheckCondition(1 == rocksdb_create_backup_options_get_atomic_flush(
                            create_backup_options));
    rocksdb_create_backup_options_set_atomic_flush(create_backup_options, 0);
    rocksdb_create_backup_options_set_progress_callback(
        create_backup_options, &backup_state, CreateBackupProgress);
    rocksdb_create_backup_options_set_exclude_files_callback(
        create_backup_options, &backup_state, CreateBackupExcludeFile);
    rocksdb_create_backup_options_set_exclude_files_callback(
        create_backup_options, NULL, NULL);

    rocksdb_backup_engine_create_new_backup_with_options(
        be, backup_db, create_backup_options, &backup_id, &err);
    CheckNoError(err);
    CheckCondition(backup_id > 0);
    CheckCondition(backup_state.progress_calls > 0);

    rocksdb_put(backup_db, woptions, "backup-key-2", 12, "backup-value-2", 14,
                &err);
    CheckNoError(err);

    rocksdb_backup_engine_create_new_backup_with_metadata(
        be, backup_db, create_backup_options, "meta-1", 6, &backup_id, &err);
    CheckNoError(err);
    CheckCondition(backup_id > 0);

    {
      const rocksdb_backup_engine_info_t* bei =
          rocksdb_backup_engine_get_backup_info(be);
      CheckCondition(rocksdb_backup_engine_info_count(bei) > 1);
      {
        int latest_backup = rocksdb_backup_engine_info_count(bei) - 1;
        size_t app_metadata_len = 0;
        const char* app_metadata = rocksdb_backup_engine_info_app_metadata(
            bei, latest_backup, &app_metadata_len);
        CheckEqual("meta-1", app_metadata, app_metadata_len);
      }
      rocksdb_backup_engine_info_destroy(bei);
    }

    rocksdb_create_backup_options_destroy(create_backup_options);
    rocksdb_backup_engine_close(be);
    rocksdb_close(backup_db);
    rocksdb_destroy_db(backup_db_options, backup_options_dbname, &err);
    CheckNoError(err);
    rocksdb_options_destroy(backup_db_options);
  }

  StartPhase("checkpoint");
  {
    rocksdb_destroy_db(options, dbcheckpointname, &err);
    CheckNoError(err);

    rocksdb_checkpoint_t* checkpoint =
        rocksdb_checkpoint_object_create(db, &err);
    CheckNoError(err);

    rocksdb_checkpoint_create(checkpoint, dbcheckpointname, 0, &err);
    CheckNoError(err);

    rocksdb_checkpoint_object_destroy(checkpoint);
    checkpoint = NULL;

    // get the identity before the checkpoint
    size_t before_db_id_len = 0;
    char* before_db_id = rocksdb_get_db_identity(db, &before_db_id_len);
    CheckCondition(before_db_id_len == 36);

    // start a new database from the checkpoint
    rocksdb_close(db);
    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbcheckpointname, &err);
    CheckNoError(err);

    CheckGet(db, roptions, "foo", "hello");

    // the db_identity after the checkpoint is different
    size_t after_db_id_len = 0;
    char* after_db_id = rocksdb_get_db_identity(db, &after_db_id_len);
    CheckCondition(after_db_id_len == 36);
    CheckCondition(memcmp(after_db_id, before_db_id, after_db_id_len) != 0);
    Free(&before_db_id);
    Free(&after_db_id);

    rocksdb_close(db);
    rocksdb_destroy_db(options, dbcheckpointname, &err);
    CheckNoError(err);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_set_error_if_exists(options, 1);
  }

  StartPhase("checkpoint_db_id_in_manifest");
  {
    // create new DB with set_write_dbid_to_manifest=true
    // db_identity is now the same across checkpoints
    rocksdb_close(db);
    db = NULL;

    rocksdb_options_t* options_dbid_in_manifest = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options_dbid_in_manifest, 1);

    rocksdb_options_set_write_dbid_to_manifest(options_dbid_in_manifest, false);
    unsigned char write_to_manifest =
        rocksdb_options_get_write_dbid_to_manifest(options_dbid_in_manifest);
    CheckCondition(!write_to_manifest);
    rocksdb_options_set_write_dbid_to_manifest(options_dbid_in_manifest, true);
    write_to_manifest =
        rocksdb_options_get_write_dbid_to_manifest(options_dbid_in_manifest);
    CheckCondition(write_to_manifest);

    rocksdb_options_set_write_identity_file(options_dbid_in_manifest, true);
    unsigned char write_identity_file =
        rocksdb_options_get_write_identity_file(options_dbid_in_manifest);
    CheckCondition(write_identity_file);
    rocksdb_options_set_write_identity_file(options_dbid_in_manifest, false);
    write_identity_file =
        rocksdb_options_get_write_identity_file(options_dbid_in_manifest);
    CheckCondition(!write_identity_file);

    db = rocksdb_open(options_dbid_in_manifest, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_checkpoint_t* checkpoint =
        rocksdb_checkpoint_object_create(db, &err);
    CheckNoError(err);

    rocksdb_checkpoint_create(checkpoint, dbcheckpointname, 0, &err);
    CheckNoError(err);

    rocksdb_checkpoint_object_destroy(checkpoint);
    checkpoint = NULL;

    // get the identity before the backup
    size_t before_db_id_len = 0;
    char* before_db_id = rocksdb_get_db_identity(db, &before_db_id_len);
    CheckCondition(before_db_id_len == 36);

    // open the checkpoint
    rocksdb_close(db);
    rocksdb_destroy_db(options_dbid_in_manifest, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_options_set_error_if_exists(options_dbid_in_manifest, 0);
    db = rocksdb_open(options_dbid_in_manifest, dbcheckpointname, &err);
    CheckNoError(err);

    // the db_identity after the checkpoint is the same
    size_t after_db_id_len = 0;
    char* after_db_id = rocksdb_get_db_identity(db, &after_db_id_len);
    CheckCondition(after_db_id_len == 36);
    CheckCondition(memcmp(after_db_id, before_db_id, after_db_id_len) == 0);
    Free(&before_db_id);
    Free(&after_db_id);

    rocksdb_close(db);
    rocksdb_destroy_db(options_dbid_in_manifest, dbcheckpointname, &err);
    CheckNoError(err);
    rocksdb_options_destroy(options_dbid_in_manifest);
    options_dbid_in_manifest = NULL;

    // re-open the default database
    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_set_error_if_exists(options, 1);
  }

  StartPhase("checkpoint_export_column_family");
  {
    static char cf_export_path[200];
    static char db_import_path[200];
    snprintf(cf_export_path, sizeof(cf_export_path),
             "%s/rocksdb_c_test-%" PRIu64 "-cf_export", GetTempDir(),
             GetTestId());
    snprintf(db_import_path, sizeof(db_import_path),
             "%s/rocksdb_c_test-%" PRIu64 "-db_import", GetTempDir(),
             GetTestId());

    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_column_family_handle_t* cf_export =
        rocksdb_create_column_family(db, db_options, "cf_export", &err);
    CheckNoError(err);

    rocksdb_put_cf(db, woptions, cf_export, "k1", 2, "v1", 2, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, cf_export, "k2", 2, "v2", 2, &err);
    CheckNoError(err);

    rocksdb_checkpoint_t* checkpoint =
        rocksdb_checkpoint_object_create(db, &err);
    CheckNoError(err);

    rocksdb_export_import_files_metadata_t* export_metadata =
        rocksdb_checkpoint_export_column_family(checkpoint, cf_export,
                                                cf_export_path, &err);
    CheckNoError(err);
    const char* comparator_name =
        rocksdb_export_import_files_metadata_get_db_comparator_name(
            export_metadata);
    CheckEqual("leveldb.BytewiseComparator", comparator_name, 26);
    rocksdb_free((void*)comparator_name);
    rocksdb_checkpoint_object_destroy(checkpoint);
    checkpoint = NULL;
    rocksdb_drop_column_family(db, cf_export, &err);
    CheckNoError(err);
    rocksdb_column_family_handle_destroy(cf_export);
    rocksdb_options_set_create_if_missing(db_options, 1);
    rocksdb_options_set_error_if_exists(db_options, 1);
    rocksdb_t* db_import = rocksdb_open(db_options, db_import_path, &err);
    CheckNoError(err);
    rocksdb_import_column_family_options_t* import_options =
        rocksdb_import_column_family_options_create();
    rocksdb_column_family_handle_t* cf_import =
        rocksdb_create_column_family_with_import(db_import, db_options,
                                                 "cf_import", import_options,
                                                 export_metadata, &err);
    CheckNoError(err);
    rocksdb_import_column_family_options_destroy(import_options);
    rocksdb_export_import_files_metadata_destroy(export_metadata);
    size_t val_len;
    char* val =
        rocksdb_get_cf(db_import, roptions, cf_import, "k1", 2, &val_len, &err);
    CheckNoError(err);
    CheckEqual("v1", val, val_len);
    free(val);

    val =
        rocksdb_get_cf(db_import, roptions, cf_import, "k2", 2, &val_len, &err);
    CheckNoError(err);
    CheckEqual("v2", val, val_len);
    free(val);

    rocksdb_column_family_handle_destroy(cf_import);
    cf_import = NULL;
    rocksdb_close(db_import);
    rocksdb_destroy_db(db_options, db_import_path, &err);
    CheckNoError(err);
    rocksdb_options_destroy(db_options);
    db_options = NULL;
  }

  StartPhase("compactall");
  rocksdb_compact_range(db, NULL, 0, NULL, 0);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactrange");
  rocksdb_compact_range(db, "a", 1, "z", 1);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactallopt");
  rocksdb_compact_range_opt(db, coptions, NULL, 0, NULL, 0);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactrangeopt");
  rocksdb_compact_range_opt(db, coptions, "a", 1, "z", 1);
  CheckGet(db, roptions, "foo", "hello");

  // Simple check cache usage
  StartPhase("cache_usage");
  {
    rocksdb_readoptions_set_pin_data(roptions, 1);
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek(iter, "foo", 3);

    size_t usage = rocksdb_cache_get_usage(cache);
    CheckCondition(usage > 0);

    size_t pin_usage = rocksdb_cache_get_pinned_usage(cache);
    CheckCondition(pin_usage > 0);

    rocksdb_iter_next(iter);
    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_set_pin_data(roptions, 0);
  }

  StartPhase("addfile");
  {
    rocksdb_envoptions_t* env_opt = rocksdb_envoptions_create();
    rocksdb_ratelimiter_t* env_rate_limiter =
        rocksdb_ratelimiter_create(32 * 1024 * 1024, 100 * 1000, 10);
    rocksdb_envoptions_set_use_mmap_reads(env_opt, 1);
    CheckCondition(1 == rocksdb_envoptions_get_use_mmap_reads(env_opt));
    rocksdb_envoptions_set_use_mmap_writes(env_opt, 0);
    CheckCondition(0 == rocksdb_envoptions_get_use_mmap_writes(env_opt));
    rocksdb_envoptions_set_use_direct_reads(env_opt, 1);
    CheckCondition(1 == rocksdb_envoptions_get_use_direct_reads(env_opt));
    rocksdb_envoptions_set_use_direct_writes(env_opt, 1);
    CheckCondition(1 == rocksdb_envoptions_get_use_direct_writes(env_opt));
    rocksdb_envoptions_set_allow_fallocate(env_opt, 0);
    CheckCondition(0 == rocksdb_envoptions_get_allow_fallocate(env_opt));
    rocksdb_envoptions_set_fd_cloexec(env_opt, 0);
    CheckCondition(0 == rocksdb_envoptions_get_fd_cloexec(env_opt));
    rocksdb_envoptions_set_bytes_per_sync(env_opt, 4096);
    CheckCondition(4096 == rocksdb_envoptions_get_bytes_per_sync(env_opt));
    rocksdb_envoptions_set_strict_bytes_per_sync(env_opt, 1);
    CheckCondition(1 == rocksdb_envoptions_get_strict_bytes_per_sync(env_opt));
    rocksdb_envoptions_set_fallocate_with_keep_size(env_opt, 0);
    CheckCondition(0 ==
                   rocksdb_envoptions_get_fallocate_with_keep_size(env_opt));
    rocksdb_envoptions_set_compaction_readahead_size(env_opt, 8192);
    CheckCondition(8192 ==
                   rocksdb_envoptions_get_compaction_readahead_size(env_opt));
    rocksdb_envoptions_set_writable_file_max_buffer_size(env_opt, 16384);
    CheckCondition(
        16384 == rocksdb_envoptions_get_writable_file_max_buffer_size(env_opt));
    rocksdb_envoptions_set_rate_limiter(env_opt, env_rate_limiter);
    rocksdb_ratelimiter_destroy(env_rate_limiter);
    rocksdb_sstfilewriter_t* writer =
        rocksdb_sstfilewriter_create(env_opt, options);

    remove(sstfilename);
    rocksdb_sstfilewriter_open(writer, sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk1", 5, "v1", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk2", 5, "v2", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk3", 5, "v3", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    rocksdb_ingestexternalfileoptions_t* ing_opt =
        rocksdb_ingestexternalfileoptions_create();
    const char* file_list[1] = {sstfilename};
    rocksdb_ingest_external_file(db, file_list, 1, ing_opt, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "sstk1", "v1");
    CheckGet(db, roptions, "sstk2", "v2");
    CheckGet(db, roptions, "sstk3", "v3");

    remove(sstfilename);
    rocksdb_sstfilewriter_open(writer, sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk2", 5, "v4", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk22", 6, "v5", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk3", 5, "v6", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    rocksdb_ingest_external_file(db, file_list, 1, ing_opt, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "sstk1", "v1");
    CheckGet(db, roptions, "sstk2", "v4");
    CheckGet(db, roptions, "sstk22", "v5");
    CheckGet(db, roptions, "sstk3", "v6");

    rocksdb_sstfilewriter_open(writer, sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "abc1", 4, "v7", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "abc2", 4, "v8", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "abc3", 4, "v9", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "abc4", 4, "v10", 3, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_delete_range(writer, "abc1", 4, "abc4", 4, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    rocksdb_ingestexternalfileoptions_destroy(ing_opt);
    rocksdb_sstfilewriter_destroy(writer);
    rocksdb_envoptions_destroy(env_opt);

    // Delete all keys we just ingested
    rocksdb_delete(db, woptions, "sstk1", 5, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk2", 5, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk22", 6, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk3", 5, &err);
    CheckNoError(err);
  }

  StartPhase("writebatch");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_t* protected_wb =
        rocksdb_writebatch_create_with_params(0, 0, 8, 0);
    rocksdb_writebatch_put(wb, "foo", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "box", 3, "c", 1);
    rocksdb_writebatch_delete(wb, "bar", 3);
    rocksdb_writebatch_put(protected_wb, "sum", 3, "value", 5);
    rocksdb_writebatch_verify_checksum(protected_wb, &err);
    CheckNoError(err);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "hello");
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    int pos = 0;
    rocksdb_writebatch_iterate(wb, &pos, CheckPut, CheckDel);
    CheckCondition(pos == 3);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "bay", 3, "d", 1);
    rocksdb_writebatch_delete_range(wb, "bar", 3, "bay", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "bay", "d");
    rocksdb_writebatch_clear(wb);
    const char* start_list[1] = {"bay"};
    const size_t start_sizes[1] = {3};
    const char* end_list[1] = {"baz"};
    const size_t end_sizes[1] = {3};
    rocksdb_writebatch_delete_rangev(wb, 1, start_list, start_sizes, end_list,
                                     end_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bay", NULL);
    rocksdb_writebatch_destroy(protected_wb);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_vectors_cf");
  {
    const char* cf_name = "wb_vectors_cf";
    rocksdb_column_family_handle_t* wb_cf =
        rocksdb_create_column_family(db, options, cf_name, &err);
    CheckNoError(err);

    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();

    // Test putv_cf: concatenates multiple slices into a single key/value
    const char* put_keys[2] = {"k", "ey"};
    const size_t put_key_sizes[2] = {1, 2};
    const char* put_vals[3] = {"v", "a", "l"};
    const size_t put_val_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_putv_cf(wb, wb_cf, 2, put_keys, put_key_sizes, 3,
                               put_vals, put_val_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    // putv_cf concatenates: key="k"+"ey"="key", value="v"+"a"+"l"="val"
    CheckGetCF(db, roptions, wb_cf, "key", "val");
    CheckGetCF(db, roptions, wb_cf, "k", NULL);
    CheckGetCF(db, roptions, wb_cf, "ey", NULL);

    // Test deletev_cf: concatenates multiple slices for key
    rocksdb_writebatch_clear(wb);
    const char* del_keys[2] = {"k", "ey"};
    const size_t del_key_sizes[2] = {1, 2};
    rocksdb_writebatch_deletev_cf(wb, wb_cf, 2, del_keys, del_key_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGetCF(db, roptions, wb_cf, "key", NULL);

    // Test delete_rangev_cf: concatenates slices for range deletion
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put_cf(wb, wb_cf, "a", 1, "1", 1);
    rocksdb_writebatch_put_cf(wb, wb_cf, "b", 1, "2", 1);
    rocksdb_writebatch_put_cf(wb, wb_cf, "c", 1, "3", 1);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGetCF(db, roptions, wb_cf, "a", "1");
    CheckGetCF(db, roptions, wb_cf, "b", "2");
    CheckGetCF(db, roptions, wb_cf, "c", "3");

    rocksdb_writebatch_clear(wb);
    const char* range_start[2] = {"a", ""};  // "a" + "" = "a"
    const size_t range_start_sizes[2] = {1, 0};
    const char* range_end[2] = {"c", ""};
    const size_t range_end_sizes[2] = {1, 0};
    rocksdb_writebatch_delete_rangev_cf(wb, wb_cf, 2, range_start,
                                        range_start_sizes, range_end,
                                        range_end_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    // Range [a, c) should delete "a" and "b", but not "c"
    CheckGetCF(db, roptions, wb_cf, "a", NULL);
    CheckGetCF(db, roptions, wb_cf, "b", NULL);
    CheckGetCF(db, roptions, wb_cf, "c", "3");

    rocksdb_writebatch_destroy(wb);
    rocksdb_drop_column_family(db, wb_cf, &err);
    CheckNoError(err);
    rocksdb_column_family_handle_destroy(wb_cf);
  }

  StartPhase("writebatch_vectors");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", "xyz");
    rocksdb_writebatch_delete(wb, "zap", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_savepoint");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_set_save_point(wb);
    rocksdb_writebatch_set_save_point(wb);
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_pop_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_writebatch_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_writebatch_rollback_to_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_rep");
  {
    rocksdb_writebatch_t* wb1 = rocksdb_writebatch_create();
    rocksdb_writebatch_put(wb1, "baz", 3, "d", 1);
    rocksdb_writebatch_put(wb1, "quux", 4, "e", 1);
    rocksdb_writebatch_delete(wb1, "quux", 4);
    size_t repsize1 = 0;
    const char* rep = rocksdb_writebatch_data(wb1, &repsize1);
    rocksdb_writebatch_t* wb2 = rocksdb_writebatch_create_from(rep, repsize1);
    CheckCondition(rocksdb_writebatch_count(wb1) ==
                   rocksdb_writebatch_count(wb2));
    size_t repsize2 = 0;
    CheckCondition(
        memcmp(rep, rocksdb_writebatch_data(wb2, &repsize2), repsize1) == 0);
    rocksdb_writebatch_destroy(wb1);
    rocksdb_writebatch_destroy(wb2);
  }

  StartPhase("writebatch_wi");
  {
    rocksdb_writebatch_wi_t* wbi = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_put(wbi, "foo", 3, "a", 1);
    rocksdb_writebatch_wi_clear(wbi);
    rocksdb_writebatch_wi_put(wbi, "bar", 3, "b", 1);
    rocksdb_writebatch_wi_put(wbi, "box", 3, "c", 1);
    rocksdb_writebatch_wi_delete(wbi, "bar", 3);
    int count = rocksdb_writebatch_wi_count(wbi);
    CheckCondition(count == 3);
    size_t size;
    char* value;
    const char* pinned_value;
    rocksdb_pinnableslice_t* p;
    value = rocksdb_writebatch_wi_get_from_batch(wbi, options, "box", 3, &size,
                                                 &err);
    CheckValue(err, "c", &value, size);
    value = rocksdb_writebatch_wi_get_from_batch(wbi, options, "bar", 3, &size,
                                                 &err);
    CheckValue(err, NULL, &value, size);
    value = rocksdb_writebatch_wi_get_from_batch_and_db(wbi, db, roptions,
                                                        "foo", 3, &size, &err);
    CheckValue(err, "hello", &value, size);
    p = rocksdb_writebatch_wi_get_pinned_from_batch_and_db(wbi, db, roptions,
                                                           "foo", 3, &err);
    pinned_value = rocksdb_pinnableslice_value(p, &size);
    CheckPinnedValue(err, "hello", &pinned_value, size);
    rocksdb_pinnableslice_destroy(p);
    value = rocksdb_writebatch_wi_get_from_batch_and_db(wbi, db, roptions,
                                                        "box", 3, &size, &err);
    CheckValue(err, "c", &value, size);
    p = rocksdb_writebatch_wi_get_pinned_from_batch_and_db(wbi, db, roptions,
                                                           "box", 3, &err);
    pinned_value = rocksdb_pinnableslice_value(p, &size);
    CheckPinnedValue(err, "c", &pinned_value, size);
    rocksdb_pinnableslice_destroy(p);
    rocksdb_write_writebatch_wi(db, woptions, wbi, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "hello");
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    int pos = 0;
    rocksdb_writebatch_wi_iterate(wbi, &pos, CheckPut, CheckDel);
    CheckCondition(pos == 3);
    rocksdb_writebatch_wi_clear(wbi);
    rocksdb_writebatch_wi_destroy(wbi);
  }

  StartPhase("writebatch_wi_vectors");
  {
    rocksdb_writebatch_wi_t* wb = rocksdb_writebatch_wi_create(0, 1);
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_wi_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", "xyz");
    rocksdb_writebatch_wi_delete(wb, "zap", 3);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_wi_destroy(wb);
  }

  StartPhase("writebatch_wi_savepoint");
  {
    rocksdb_writebatch_wi_t* wb = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_set_save_point(wb);
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_wi_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_writebatch_wi_rollback_to_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_wi_destroy(wb);
  }

  StartPhase("iter");
  {
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "box", "c");
    rocksdb_iter_next(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_prev(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_prev(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_last(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_seek(iter, "b", 1);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek_for_prev(iter, "g", 1);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_seek_for_prev(iter, "box", 3);
    CheckIter(iter, "box", "c");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
  }

  StartPhase("iter_slice");
  {
    // Test the new slice-based iterator API for better performance
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));

    // Test rocksdb_iter_key_slice
    rocksdb_slice_t key_slice = rocksdb_iter_key_slice(iter);
    CheckEqual("box", key_slice.data, key_slice.size);

    // Test rocksdb_iter_value_slice
    rocksdb_slice_t value_slice = rocksdb_iter_value_slice(iter);
    CheckEqual("c", value_slice.data, value_slice.size);

    // Move to next entry and test again
    rocksdb_iter_next(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    key_slice = rocksdb_iter_key_slice(iter);
    value_slice = rocksdb_iter_value_slice(iter);
    CheckEqual("foo", key_slice.data, key_slice.size);
    CheckEqual("hello", value_slice.data, value_slice.size);

    // Test seeking with slice API
    rocksdb_iter_seek(iter, "b", 1);
    CheckCondition(rocksdb_iter_valid(iter));
    key_slice = rocksdb_iter_key_slice(iter);
    value_slice = rocksdb_iter_value_slice(iter);
    CheckEqual("box", key_slice.data, key_slice.size);
    CheckEqual("c", value_slice.data, value_slice.size);

    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
  }

  StartPhase("wbwi_iter");
  {
    rocksdb_iterator_t* base_iter = rocksdb_create_iterator(db, roptions);
    rocksdb_writebatch_wi_t* wbi = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_put(wbi, "bar", 3, "b", 1);
    rocksdb_writebatch_wi_delete(wbi, "foo", 3);
    rocksdb_iterator_t* iter =
        rocksdb_writebatch_wi_create_iterator_with_base(wbi, base_iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar", "b");
    rocksdb_iter_next(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_prev(iter);
    CheckIter(iter, "bar", "b");
    rocksdb_iter_prev(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_last(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek(iter, "b", 1);
    CheckIter(iter, "bar", "b");
    rocksdb_iter_seek_for_prev(iter, "c", 1);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek_for_prev(iter, "box", 3);
    CheckIter(iter, "box", "c");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
    rocksdb_writebatch_wi_destroy(wbi);
  }

  StartPhase("wbwi_iter_readoptions");
  {
    rocksdb_readoptions_t* iter_roptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_iterate_lower_bound(iter_roptions, "boy", 3);
    rocksdb_readoptions_set_iterate_upper_bound(iter_roptions, "fool", 4);
    rocksdb_iterator_t* base_iter = rocksdb_create_iterator(db, iter_roptions);
    rocksdb_writebatch_wi_t* wbi = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_put(wbi, "bar", 3, "b",
                              1);  // should get filtered out
    rocksdb_writebatch_wi_put(wbi, "cat", 3, "miau", 4);
    rocksdb_writebatch_wi_put(wbi, "gnu", 3, "muh",
                              3);  // should get filtered out
    rocksdb_iterator_t* iter =
        rocksdb_writebatch_wi_create_iterator_with_base_readopts(wbi, base_iter,
                                                                 iter_roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "cat", "miau");
    rocksdb_iter_next(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_prev(iter);
    CheckIter(iter, "cat", "miau");
    rocksdb_iter_prev(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_last(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_seek(iter, "b", 1);
    CheckIter(iter, "cat", "miau");
    rocksdb_iter_seek_for_prev(iter, "d", 1);
    CheckIter(iter, "cat", "miau");
    rocksdb_iter_seek_for_prev(iter, "fool", 3);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
    rocksdb_writebatch_wi_destroy(wbi);
    rocksdb_readoptions_destroy(iter_roptions);
  }

  StartPhase("multiget");
  {
    const char* keys[3] = {"box", "foo", "notfound"};
    const size_t keys_sizes[3] = {3, 3, 8};
    char* vals[3];
    size_t vals_sizes[3];
    char* errs[3];
    const char* expected[3] = {"c", "hello", NULL};
    rocksdb_multi_get(db, roptions, 3, keys, keys_sizes, vals, vals_sizes,
                      errs);
    CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
  }

  StartPhase("zero_copy_get_pinned_v2");
  {
    // Test new zero-copy get functions

    // Test rocksdb_get_pinned_v2
    rocksdb_pinnable_handle_t* handle =
        rocksdb_get_pinned_v2(db, roptions, "foo", 3, &err);
    CheckNoError(err);
    CheckCondition(handle != NULL);
    size_t val_len;
    const char* val = rocksdb_pinnable_handle_get_value(handle, &val_len);
    CheckEqual("hello", val, val_len);
    rocksdb_pinnable_handle_destroy(handle);

    // Test with non-existent key
    handle = rocksdb_get_pinned_v2(db, roptions, "notfound", 8, &err);
    CheckNoError(err);
    CheckCondition(handle == NULL);

    // Test rocksdb_get_into_buffer
    char buffer[100];
    unsigned char found;
    unsigned char success = rocksdb_get_into_buffer(
        db, roptions, "foo", 3, buffer, sizeof(buffer), &val_len, &found, &err);
    CheckNoError(err);
    CheckCondition(success == 1);
    CheckCondition(found == 1);
    CheckCondition(val_len == 5);
    CheckCondition(memcmp(buffer, "hello", 5) == 0);

    // Test with buffer too small
    success = rocksdb_get_into_buffer(db, roptions, "foo", 3, buffer,
                                      2,  // Buffer too small
                                      &val_len, &found, &err);
    CheckNoError(err);
    CheckCondition(success == 0);  // Should fail due to small buffer
    CheckCondition(found == 1);
    CheckCondition(val_len == 5);  // Should still report actual size

    // Test with non-existent key
    success = rocksdb_get_into_buffer(db, roptions, "notfound", 8, buffer,
                                      sizeof(buffer), &val_len, &found, &err);
    CheckNoError(err);
    CheckCondition(success == 0);
    CheckCondition(found == 0);
  }

  StartPhase("pin_get");
  {
    CheckPinGet(db, roptions, "box", "c");
    CheckPinGet(db, roptions, "foo", "hello");
    CheckPinGet(db, roptions, "notfound", NULL);
  }

  StartPhase("approximate_sizes");
  {
    int i;
    int n = 20000;
    char keybuf[100];
    char valbuf[100];
    uint64_t sizes[2];
    const char* start[2] = {"a", "k00000000000000010000"};
    size_t start_len[2] = {1, 21};
    const char* limit[2] = {"k00000000000000010000", "z"};
    size_t limit_len[2] = {21, 1};
    rocksdb_writeoptions_set_sync(woptions, 0);
    for (i = 0; i < n; i++) {
      snprintf(keybuf, sizeof(keybuf), "k%020d", i);
      snprintf(valbuf, sizeof(valbuf), "v%020d", i);
      rocksdb_put(db, woptions, keybuf, strlen(keybuf), valbuf, strlen(valbuf),
                  &err);
      CheckNoError(err);
    }
    rocksdb_approximate_sizes(db, 2, start, start_len, limit, limit_len, sizes,
                              &err);
    CheckNoError(err);
    CheckCondition(sizes[0] > 0);
    CheckCondition(sizes[1] > 0);
  }

  StartPhase("size_approximation_options");
  {
    uint64_t sizes[2];
    const char* start[2] = {"a", "k00000000000000010000"};
    size_t start_len[2] = {1, 21};
    const char* limit[2] = {"k00000000000000010000", "z"};
    size_t limit_len[2] = {21, 1};
    rocksdb_size_approximation_options_t* size_options =
        rocksdb_size_approximation_options_create();
    rocksdb_size_approximation_options_set_include_memtables(size_options, 1);
    CheckCondition(
        1 ==
        rocksdb_size_approximation_options_get_include_memtables(size_options));
    rocksdb_size_approximation_options_set_include_files(size_options, 1);
    CheckCondition(1 == rocksdb_size_approximation_options_get_include_files(
                            size_options));
    rocksdb_size_approximation_options_set_include_blob_files(size_options, 1);
    CheckCondition(1 ==
                   rocksdb_size_approximation_options_get_include_blob_files(
                       size_options));
    rocksdb_size_approximation_options_set_files_size_error_margin(size_options,
                                                                   0.25);
    CheckCondition(
        0.25 == rocksdb_size_approximation_options_get_files_size_error_margin(
                    size_options));
    rocksdb_approximate_sizes_with_options(
        db, size_options, 2, start, start_len, limit, limit_len, sizes, &err);
    CheckNoError(err);
    CheckCondition(sizes[0] > 0);
    CheckCondition(sizes[1] > 0);
    rocksdb_size_approximation_options_destroy(size_options);
  }

  StartPhase("column_family_metadata_options");
  {
    size_t key_len = 0;
    rocksdb_column_family_metadata_options_t* metadata_options =
        rocksdb_column_family_metadata_options_create();
    CheckCondition(-1 == rocksdb_column_family_metadata_options_get_level(
                             metadata_options));
    rocksdb_column_family_metadata_options_set_level(metadata_options, 0);
    CheckCondition(0 == rocksdb_column_family_metadata_options_get_level(
                            metadata_options));
    rocksdb_column_family_metadata_options_set_level(metadata_options, -1);
    CheckCondition(-1 == rocksdb_column_family_metadata_options_get_level(
                             metadata_options));
    rocksdb_column_family_metadata_options_set_start_key(metadata_options, "zz",
                                                         2);
    rocksdb_column_family_metadata_options_set_end_key(metadata_options, "zzz",
                                                       3);
    CheckCondition(0 ==
                   memcmp(rocksdb_column_family_metadata_options_get_start_key(
                              metadata_options, &key_len),
                          "zz", key_len));
    CheckCondition(key_len == 2);
    CheckCondition(0 ==
                   memcmp(rocksdb_column_family_metadata_options_get_end_key(
                              metadata_options, &key_len),
                          "zzz", key_len));
    CheckCondition(key_len == 3);

    rocksdb_column_family_metadata_t* filtered_meta =
        rocksdb_get_column_family_metadata_with_options(db, metadata_options);
    CheckCondition(
        0 == rocksdb_column_family_metadata_get_file_count(filtered_meta));
    rocksdb_column_family_metadata_destroy(filtered_meta);

    rocksdb_column_family_metadata_options_set_start_key(metadata_options, NULL,
                                                         0);
    rocksdb_column_family_metadata_options_set_end_key(metadata_options, NULL,
                                                       0);
    CheckCondition(NULL == rocksdb_column_family_metadata_options_get_start_key(
                               metadata_options, &key_len));
    CheckCondition(key_len == 0);
    CheckCondition(NULL == rocksdb_column_family_metadata_options_get_end_key(
                               metadata_options, &key_len));
    CheckCondition(key_len == 0);

    filtered_meta =
        rocksdb_get_column_family_metadata_with_options(db, metadata_options);
    CheckCondition(
        rocksdb_column_family_metadata_get_file_count(filtered_meta) > 0);
    rocksdb_column_family_metadata_destroy(filtered_meta);
    rocksdb_column_family_metadata_options_destroy(metadata_options);
  }

  StartPhase("import_column_family_options");
  {
    rocksdb_import_column_family_options_t* import_options =
        rocksdb_import_column_family_options_create();
    rocksdb_import_column_family_options_set_move_files(import_options, 1);
    CheckCondition(1 == rocksdb_import_column_family_options_get_move_files(
                            import_options));
    rocksdb_import_column_family_options_destroy(import_options);
  }

  StartPhase("property");
  {
    char* prop = rocksdb_property_value(db, "nosuchprop");
    CheckCondition(prop == NULL);
    prop = rocksdb_property_value(db, "rocksdb.stats");
    CheckCondition(prop != NULL);
    Free(&prop);
  }

  StartPhase("snapshot");
  {
    const rocksdb_snapshot_t* snap;
    snap = rocksdb_create_snapshot(db);
    rocksdb_delete(db, woptions, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_readoptions_set_snapshot(roptions, snap);
    CheckGet(db, roptions, "foo", "hello");

    size_t metadata_value_len = 0;
    unsigned char newer_version_present = 0;
    char* metadata_value =
        rocksdb_get_with_metadata(db, roptions, "foo", 3, &metadata_value_len,
                                  NULL, NULL, &newer_version_present, &err);
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    CheckNoError(err);
    CheckEqual("hello", metadata_value, metadata_value_len);
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    CheckCondition(newer_version_present == 1);
    Free(&metadata_value);

    const char* metadata_keys[] = {"foo"};
    const size_t metadata_key_sizes[] = {3};
    char* metadata_values[1];
    size_t metadata_value_sizes[1];
    unsigned char newer_versions[1];
    char* metadata_errors[1];
    rocksdb_multi_get_with_metadata(
        db, roptions, 1, metadata_keys, metadata_key_sizes, metadata_values,
        metadata_value_sizes, NULL, NULL, newer_versions, metadata_errors);
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    CheckCondition(metadata_errors[0] == NULL);
    CheckEqual("hello", metadata_values[0], metadata_value_sizes[0]);
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    CheckCondition(newer_versions[0] == 1);
    Free(&metadata_values[0]);

    rocksdb_readoptions_set_snapshot(roptions, NULL);
    CheckGet(db, roptions, "foo", NULL);
    rocksdb_release_snapshot(db, snap);
  }
  StartPhase("snapshot_with_memtable_inplace_update");
  {
    rocksdb_close(db);
    const rocksdb_snapshot_t* snap = NULL;
    const char* s_key = "foo_snap";
    const char* value1 = "hello_s1";
    const char* value2 = "hello_s2";
    rocksdb_options_set_allow_concurrent_memtable_write(options, 0);
    rocksdb_options_set_inplace_update_support(options, 1);
    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, s_key, 8, value1, 8, &err);
    snap = rocksdb_create_snapshot(db);
    assert(snap != NULL);
    rocksdb_put(db, woptions, s_key, 8, value2, 8, &err);
    CheckNoError(err);
    rocksdb_readoptions_set_snapshot(roptions, snap);
    CheckGet(db, roptions, "foo", NULL);
    // snapshot syntax is invalid, because of inplace update supported is set
    CheckGet(db, roptions, s_key, value2);
    // restore the data and options
    rocksdb_delete(db, woptions, s_key, 8, &err);
    CheckGet(db, roptions, s_key, NULL);
    rocksdb_release_snapshot(db, snap);
    rocksdb_readoptions_set_snapshot(roptions, NULL);
    rocksdb_options_set_inplace_update_support(options, 0);
    rocksdb_options_set_allow_concurrent_memtable_write(options, 1);
    rocksdb_options_set_error_if_exists(options, 1);
  }
  StartPhase("repair");
  {
    // If we do not compact here, then the lazy deletion of
    // files (https://reviews.facebook.net/D6123) would leave
    // around deleted files and the repair process will find
    // those files and put them back into the database.
    rocksdb_compact_range(db, NULL, 0, NULL, 0);
    rocksdb_close(db);
    rocksdb_options_set_create_if_missing(options, 0);
    rocksdb_options_set_error_if_exists(options, 0);
    rocksdb_options_set_wal_recovery_mode(options, 2);
    rocksdb_repair_db(options, dbname, &err);
    CheckNoError(err);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", NULL);
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_set_error_if_exists(options, 1);
  }

  StartPhase("filter");
  for (run = 1; run <= 4; run++) {
    // run=0 uses custom filter (not currently supported)
    // run=1 uses old block-based bloom filter
    // run=2 run uses full bloom filter
    // run=3 uses Ribbon
    // run=4 uses Ribbon-Bloom hybrid configuration
    CheckNoError(err);
    rocksdb_filterpolicy_t* policy;
    if (run == 1) {
      policy = rocksdb_filterpolicy_create_bloom(8.0);
    } else if (run == 2) {
      policy = rocksdb_filterpolicy_create_bloom_full(8.0);
    } else if (run == 3) {
      policy = rocksdb_filterpolicy_create_ribbon(8.0);
    } else {
      policy = rocksdb_filterpolicy_create_ribbon_hybrid(8.0, 1);
    }
    rocksdb_block_based_options_set_filter_policy(table_options, policy);
    rocksdb_block_based_options_set_optimize_filters_for_memory(table_options,
                                                                0);

    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_set_block_based_table_factory(options, table_options);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar", 3, "barvalue", 8, &err);
    CheckNoError(err);

    {
      // Add enough keys to get just one reasonably populated Bloom filter
      const int keys_to_add = 1500;
      int i;
      char keybuf[100];
      for (i = 0; i < keys_to_add; i++) {
        snprintf(keybuf, sizeof(keybuf), "yes%020d", i);
        rocksdb_put(db, woptions, keybuf, strlen(keybuf), "val", 3, &err);
        CheckNoError(err);
      }
    }
    rocksdb_compact_range(db, NULL, 0, NULL, 0);

    CheckGet(db, roptions, "foo", "foovalue");
    CheckGet(db, roptions, "bar", "barvalue");

    {
      // Query some keys not added to identify Bloom filter implementation
      // from false positive queries, using perfcontext to detect Bloom
      // filter behavior
      rocksdb_perfcontext_t* perf = rocksdb_perfcontext_create();
      rocksdb_perfcontext_reset(perf);

      const int keys_to_query = 10000;
      int i;
      char keybuf[100];
      for (i = 0; i < keys_to_query; i++) {
        snprintf(keybuf, sizeof(keybuf), "no%020d", i);
        CheckGet(db, roptions, keybuf, NULL);
      }

      const int hits =
          (int)rocksdb_perfcontext_metric(perf, rocksdb_bloom_sst_hit_count);
      if (run == 0) {
        // Due to half true, half false with fake filter result
        CheckCondition(hits == keys_to_query / 2);
      } else if (run == 1 || run == 2 || run == 4) {
        // For run == 1, block-based Bloom is no longer available in public
        // API; attempting to enable it enables full Bloom instead.
        //
        // Essentially a fingerprint of full Bloom schema, format_version=5
        CheckCondition(hits == 188);
      } else {
        // Essentially a fingerprint of Ribbon schema
        CheckCondition(hits == 226);
      }
      CheckCondition(
          (keys_to_query - hits) ==
          (int)rocksdb_perfcontext_metric(perf, rocksdb_bloom_sst_miss_count));

      rocksdb_perfcontext_destroy(perf);
    }

    // Reset the policy
    rocksdb_block_based_options_set_filter_policy(table_options, NULL);
    rocksdb_options_set_block_based_table_factory(options, table_options);
  }

  StartPhase("compaction_filter");
  {
    rocksdb_options_t* options_with_filter = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options_with_filter, 1);
    rocksdb_compactionfilter_t* cfilter;
    cfilter = rocksdb_compactionfilter_create(NULL, CFilterDestroy,
                                              CFilterFilter, CFilterName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options_with_filter, dbname, &err);
    rocksdb_options_set_compaction_filter(options_with_filter, cfilter);
    db = CheckCompaction(db, options_with_filter, roptions, woptions);

    rocksdb_options_set_compaction_filter(options_with_filter, NULL);
    rocksdb_compactionfilter_destroy(cfilter);
    rocksdb_options_destroy(options_with_filter);
  }

  StartPhase("compaction_filter_factory");
  {
    rocksdb_options_t* options_with_filter_factory = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options_with_filter_factory, 1);
    rocksdb_compactionfilterfactory_t* factory;
    factory = rocksdb_compactionfilterfactory_create(
        NULL, CFilterFactoryDestroy, CFilterCreate, CFilterFactoryName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options_with_filter_factory, dbname, &err);
    rocksdb_options_set_compaction_filter_factory(options_with_filter_factory,
                                                  factory);
    db = CheckCompaction(db, options_with_filter_factory, roptions, woptions);

    rocksdb_options_set_compaction_filter_factory(options_with_filter_factory,
                                                  NULL);
    rocksdb_options_destroy(options_with_filter_factory);
  }

  StartPhase("merge_operator");
  {
    rocksdb_mergeoperator_t* merge_operator;
    merge_operator = rocksdb_mergeoperator_create(
        NULL, MergeOperatorDestroy, MergeOperatorFullMerge,
        MergeOperatorPartialMerge, NULL, MergeOperatorName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_set_merge_operator(options, merge_operator);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "foovalue");
    rocksdb_merge(db, woptions, "foo", 3, "barvalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "fake");

    // Merge of a non-existing value
    rocksdb_merge(db, woptions, "bar", 3, "barvalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bar", "fake");
  }

  StartPhase("columnfamilies");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(db_options, 1);
    db = rocksdb_open(db_options, dbname, &err);
    CheckNoError(err);
    rocksdb_close(db);
    {
      const char* expected_cf_names[1] = {"default"};
      LoadAndCheckLatestOptions(dbname, env, false, cache, NULL, 1,
                                expected_cf_names, NULL);
    }

    rocksdb_options_set_create_if_missing(db_options, 0);
    db = rocksdb_open(db_options, dbname, &err);
    rocksdb_column_family_handle_t* cfh;
    cfh = rocksdb_create_column_family(db, db_options, "cf1", &err);
    rocksdb_column_family_handle_destroy(cfh);
    CheckNoError(err);
    rocksdb_close(db);

    size_t cflen;
    char** column_fams =
        rocksdb_list_column_families(db_options, dbname, &cflen, &err);
    CheckNoError(err);
    CheckEqual("default", column_fams[0], 7);
    CheckEqual("cf1", column_fams[1], 3);
    CheckCondition(cflen == 2);
    rocksdb_list_column_families_destroy(column_fams, cflen);

    rocksdb_options_t* cf_options_1 = rocksdb_options_create();
    rocksdb_options_t* cf_options_2 = rocksdb_options_create();

    // use dbpathname2 as the cf_path for "cf1"
    rocksdb_dbpath_t* dbpath2;
    char dbpathname2[200];
    rocksdb_mergeoperator_t* failing_merge_operator;
    snprintf(dbpathname2, sizeof(dbpathname2),
             "%s/rocksdb_c_test-%" PRIu64 "-dbpath2", GetTempDir(),
             GetTestId());
    dbpath2 = rocksdb_dbpath_create(dbpathname2, 1024 * 1024);
    const rocksdb_dbpath_t* cf_paths[1] = {dbpath2};
    rocksdb_options_set_cf_paths(cf_options_2, cf_paths, 1);
    failing_merge_operator = rocksdb_mergeoperator_create(
        NULL, MergeOperatorDestroy, FailingMergeOperatorFullMerge,
        FailingMergeOperatorPartialMerge, NULL, MergeOperatorName);
    rocksdb_options_set_merge_operator(cf_options_2, failing_merge_operator);

    const char* cf_names[2] = {"default", "cf1"};
    const rocksdb_options_t* cf_opts[2] = {cf_options_1, cf_options_2};
    rocksdb_column_family_handle_t* handles[2];

    LoadAndCheckLatestOptions(dbname, env, false, cache, NULL, 2, cf_names,
                              NULL);

    db = rocksdb_open_column_families(db_options, dbname, 2, cf_names, cf_opts,
                                      handles, &err);
    CheckNoError(err);

    rocksdb_put_cf(db, woptions, handles[1], "foo", 3, "hello", 5, &err);
    CheckNoError(err);

    rocksdb_put_cf(db, woptions, handles[1], "foobar1", 7, "hello1", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar2", 7, "hello2", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar3", 7, "hello3", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar4", 7, "hello4", 6, &err);
    CheckNoError(err);
    rocksdb_suggest_compact_range_cf(db, handles[1], "foo", 3, "foobar9", 7,
                                     &err);
    CheckNoError(err);

    rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_options, 1);
    rocksdb_flush_cf(db, flush_options, handles[1], &err);

    // make sure all files in "cf1" are under the specified cf path
    {
      rocksdb_column_family_metadata_t* cf_meta =
          rocksdb_get_column_family_metadata_cf(db, handles[1]);
      size_t cf_file_count = rocksdb_column_family_metadata_get_size(cf_meta);
      assert(cf_file_count > 0);
      size_t level_count =
          rocksdb_column_family_metadata_get_level_count(cf_meta);
      assert(level_count > 0);
      for (size_t l = 0; l < level_count; ++l) {
        rocksdb_level_metadata_t* level_meta =
            rocksdb_column_family_metadata_get_level_metadata(cf_meta, l);
        assert(level_meta);

        size_t file_count = rocksdb_level_metadata_get_file_count(level_meta);
        for (size_t f = 0; f < file_count; ++f) {
          rocksdb_sst_file_metadata_t* file_meta =
              rocksdb_level_metadata_get_sst_file_metadata(level_meta, f);
          assert(file_meta);
          char* file_path = rocksdb_sst_file_metadata_get_directory(file_meta);
          assert(strcmp(file_path, dbpathname2) == 0);
          Free(&file_path);
          rocksdb_sst_file_metadata_destroy(file_meta);
        }
        rocksdb_level_metadata_destroy(level_meta);
      }

      rocksdb_column_family_metadata_destroy(cf_meta);
    }

    CheckNoError(err) rocksdb_flushoptions_destroy(flush_options);

    CheckGetCF(db, roptions, handles[1], "foo", "hello");
    CheckPinGetCF(db, roptions, handles[1], "foo", "hello");

    rocksdb_delete_cf(db, woptions, handles[1], "foo", 3, &err);
    CheckNoError(err);

    rocksdb_delete_range_cf(db, woptions, handles[1], "foobar2", 7, "foobar4",
                            7, &err);
    CheckNoError(err);

    CheckGetCF(db, roptions, handles[1], "foo", NULL);
    CheckPinGetCF(db, roptions, handles[1], "foo", NULL);

    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_put_cf(wb, handles[1], "baz", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put_cf(wb, handles[1], "bar", 3, "b", 1);
    rocksdb_writebatch_put_cf(wb, handles[1], "box", 3, "c", 1);
    rocksdb_writebatch_put_cf(wb, handles[1], "buff", 4, "rocksdb", 7);
    rocksdb_writebatch_delete_cf(wb, handles[1], "bar", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGetCF(db, roptions, handles[1], "baz", NULL);
    CheckGetCF(db, roptions, handles[1], "bar", NULL);
    CheckGetCF(db, roptions, handles[1], "box", "c");
    CheckGetCF(db, roptions, handles[1], "buff", "rocksdb");
    CheckPinGetCF(db, roptions, handles[1], "baz", NULL);
    CheckPinGetCF(db, roptions, handles[1], "bar", NULL);
    CheckPinGetCF(db, roptions, handles[1], "box", "c");
    CheckPinGetCF(db, roptions, handles[1], "buff", "rocksdb");
    rocksdb_writebatch_clear(wb);
    // Test WriteBatch iteration with Column Family
    int pos = 0;
    rocksdb_writebatch_put_cf(wb, handles[1], "bar", 3, "b", 1);
    rocksdb_writebatch_put_cf(wb, handles[1], "box", 3, "c", 1);
    rocksdb_writebatch_delete_cf(wb, handles[1], "bar", 3);
    rocksdb_writebatch_merge_cf(wb, handles[1], "box", 3, "cc", 2);
    rocksdb_writebatch_put(wb, "foo", 3, "f", 1);
    rocksdb_writebatch_delete(wb, "foo", 3);
    rocksdb_writebatch_put(wb, "baz", 3, "a", 1);
    rocksdb_writebatch_merge(wb, "baz", 3, "aa", 2);
    rocksdb_writebatch_iterate_cf(wb, &pos, CheckPutCF, CheckDelCF,
                                  CheckMergeCF);
    CheckCondition(pos == 8);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_destroy(wb);

    rocksdb_flush_wal(db, 1, &err);
    CheckNoError(err);
    {
      rocksdb_flushwaloptions_t* flush_wal_options =
          rocksdb_flushwaloptions_create();
      rocksdb_flushwaloptions_set_sync(flush_wal_options, 1);
      CheckCondition(1 == rocksdb_flushwaloptions_get_sync(flush_wal_options));
      rocksdb_flushwaloptions_set_rate_limiter_priority(flush_wal_options, 2);
      CheckCondition(2 == rocksdb_flushwaloptions_get_rate_limiter_priority(
                              flush_wal_options));
      rocksdb_flush_wal_with_options(db, flush_wal_options, &err);
      CheckNoError(err);
      rocksdb_flushwaloptions_destroy(flush_wal_options);
    }

    {
      rocksdb_wal_readoptions_t* wal_read_options =
          rocksdb_wal_readoptions_create();
      rocksdb_wal_iterator_t* wal_iterator;
      rocksdb_writebatch_t* wal_batch;
      uint64_t wal_seq = 0;

      CheckCondition(
          1 == rocksdb_wal_readoptions_get_verify_checksums(wal_read_options));
      rocksdb_wal_readoptions_set_verify_checksums(wal_read_options, 0);
      CheckCondition(
          0 == rocksdb_wal_readoptions_get_verify_checksums(wal_read_options));

      wal_iterator = rocksdb_get_updates_since(db, 0, wal_read_options, &err);
      CheckNoError(err);
      CheckCondition(wal_iterator != NULL);
      CheckCondition(1 == rocksdb_wal_iter_valid(wal_iterator));
      wal_batch = rocksdb_wal_iter_get_batch(wal_iterator, &wal_seq);
      CheckCondition(wal_seq > 0);
      CheckCondition(rocksdb_writebatch_count(wal_batch) > 0);

      rocksdb_writebatch_destroy(wal_batch);
      rocksdb_wal_iter_destroy(wal_iterator);
      rocksdb_wal_readoptions_destroy(wal_read_options);
    }

    {
      const char* start[1] = {"a"};
      size_t start_len[1] = {1};
      const char* limit[1] = {"z"};
      size_t limit_len[1] = {1};
      uint64_t sizes[1];
      rocksdb_size_approximation_options_t* size_options =
          rocksdb_size_approximation_options_create();
      rocksdb_size_approximation_options_set_include_files(size_options, 1);
      rocksdb_approximate_sizes_cf_with_options(db, handles[1], size_options, 1,
                                                start, start_len, limit,
                                                limit_len, sizes, &err);
      CheckNoError(err);
      CheckCondition(sizes[0] > 0);
      rocksdb_size_approximation_options_destroy(size_options);
    }

    {
      size_t key_len = 0;
      rocksdb_column_family_metadata_options_t* metadata_options =
          rocksdb_column_family_metadata_options_create();
      rocksdb_column_family_metadata_options_set_start_key(metadata_options,
                                                           "zzz", 3);
      rocksdb_column_family_metadata_options_set_end_key(metadata_options,
                                                         "zzzz", 4);
      CheckCondition(
          0 == memcmp(rocksdb_column_family_metadata_options_get_start_key(
                          metadata_options, &key_len),
                      "zzz", key_len));
      CheckCondition(key_len == 3);

      rocksdb_column_family_metadata_t* filtered_meta =
          rocksdb_get_column_family_metadata_cf_with_options(db, handles[1],
                                                             metadata_options);
      CheckCondition(
          0 == rocksdb_column_family_metadata_get_file_count(filtered_meta));
      rocksdb_column_family_metadata_destroy(filtered_meta);
      rocksdb_column_family_metadata_options_destroy(metadata_options);
    }

    {
      char* input_file_name = NULL;
      rocksdb_compaction_options_t* compaction_options =
          rocksdb_compaction_options_create();
      rocksdb_compactionjobinfo_t* compaction_info =
          rocksdb_compactionjobinfo_create();
      unsigned char* canceled = rocksdb_compaction_options_canceled_create();
      char** output_files = NULL;
      size_t num_output_files = 0;
      rocksdb_column_family_metadata_t* cf_meta =
          rocksdb_get_column_family_metadata_cf(db, handles[1]);
      size_t level_count =
          rocksdb_column_family_metadata_get_level_count(cf_meta);

      rocksdb_compaction_options_set_compression(compaction_options, 0);
      CheckCondition(
          0 == rocksdb_compaction_options_get_compression(compaction_options));
      rocksdb_compaction_options_set_output_file_size_limit(compaction_options,
                                                            1024 * 1024);
      CheckCondition(1024 * 1024 ==
                     rocksdb_compaction_options_get_output_file_size_limit(
                         compaction_options));
      rocksdb_compaction_options_set_max_subcompactions(compaction_options, 1);
      CheckCondition(1 == rocksdb_compaction_options_get_max_subcompactions(
                              compaction_options));
      rocksdb_compaction_options_set_output_temperature_override(
          compaction_options, 4);
      CheckCondition(4 ==
                     rocksdb_compaction_options_get_output_temperature_override(
                         compaction_options));
      rocksdb_compaction_options_set_allow_trivial_move(compaction_options, 1);
      CheckCondition(1 == rocksdb_compaction_options_get_allow_trivial_move(
                              compaction_options));
      rocksdb_compaction_options_set_allow_trivial_move(compaction_options, 0);
      CheckCondition(0 == rocksdb_compaction_options_get_allow_trivial_move(
                              compaction_options));
      rocksdb_compaction_options_canceled_set(canceled, 0);
      rocksdb_compaction_options_set_canceled(compaction_options, canceled);

      for (size_t l = 0; l < level_count && input_file_name == NULL; ++l) {
        rocksdb_level_metadata_t* level_meta =
            rocksdb_column_family_metadata_get_level_metadata(cf_meta, l);
        size_t file_count = rocksdb_level_metadata_get_file_count(level_meta);
        if (file_count > 0) {
          rocksdb_sst_file_metadata_t* file_meta =
              rocksdb_level_metadata_get_sst_file_metadata(level_meta, 0);
          input_file_name =
              rocksdb_sst_file_metadata_get_relative_filename(file_meta);
          rocksdb_sst_file_metadata_destroy(file_meta);
        }
        rocksdb_level_metadata_destroy(level_meta);
      }
      CheckCondition(input_file_name != NULL);

      {
        const char* input_files[1] = {input_file_name};
        rocksdb_compact_files_cf(db, handles[1], compaction_options,
                                 input_files, 1, 1, -1, &output_files,
                                 &num_output_files, compaction_info, &err);
      }
      CheckNoError(err);
      CheckCondition(output_files != NULL);
      CheckCondition(num_output_files > 0);
      CheckCondition(rocksdb_compactionjobinfo_job_id(compaction_info) > 0);

      rocksdb_compact_files_output_file_names_destroy(output_files,
                                                      num_output_files);
      rocksdb_compaction_options_canceled_destroy(canceled);
      rocksdb_compactionjobinfo_destroy(compaction_info);
      rocksdb_compaction_options_destroy(compaction_options);
      rocksdb_column_family_metadata_destroy(cf_meta);
      Free(&input_file_name);
    }

    {
      rocksdb_readoptions_t* verify_options = rocksdb_readoptions_create();
      rocksdb_wal_files_t* wal_files;
      rocksdb_wal_file_t* current_wal_file;
      const rocksdb_wal_file_t* last_wal_file;
      size_t wal_count;

      rocksdb_pause_background_work(db, &err);
      CheckNoError(err);
      rocksdb_continue_background_work(db, &err);
      CheckNoError(err);

      // Rewrite an existing key to guarantee WAL activity without changing
      // the number of visible keys in cf1.
      rocksdb_put_cf(db, woptions, handles[1], "buff", 4, "rocksdb", 7, &err);
      CheckNoError(err);

      rocksdb_disable_file_deletions(db, &err);
      CheckNoError(err);
      wal_files = rocksdb_get_sorted_wal_files(db, &err);
      CheckNoError(err);
      CheckCondition(wal_files != NULL);
      wal_count = rocksdb_wal_files_count(wal_files);
      CheckCondition(wal_count > 0);
      last_wal_file = rocksdb_wal_files_get_wal_file(wal_files, wal_count - 1);
      CheckCondition(last_wal_file != NULL);
      CheckCondition(
          strstr(rocksdb_wal_file_path_name(last_wal_file), ".log") != NULL);
      CheckCondition(rocksdb_wal_file_log_number(last_wal_file) > 0);
      CheckCondition(rocksdb_wal_file_size_file_bytes(last_wal_file) > 0);
      CheckCondition(rocksdb_wal_file_type(last_wal_file) ==
                         rocksdb_wal_file_type_alive_log ||
                     rocksdb_wal_file_type(last_wal_file) ==
                         rocksdb_wal_file_type_archived_log);
      rocksdb_wal_files_destroy(wal_files);
      rocksdb_enable_file_deletions(db, &err);
      CheckNoError(err);

      current_wal_file = rocksdb_get_current_wal_file(db, &err);
      CheckNoError(err);
      CheckCondition(current_wal_file != NULL);
      CheckCondition(
          strstr(rocksdb_wal_file_path_name(current_wal_file), ".log") != NULL);
      CheckCondition(rocksdb_wal_file_log_number(current_wal_file) > 0);
      CheckCondition(rocksdb_wal_file_type(current_wal_file) ==
                     rocksdb_wal_file_type_alive_log);
      rocksdb_wal_file_destroy(current_wal_file);

      rocksdb_verify_checksum(db, &err);
      CheckNoError(err);
      rocksdb_verify_checksum_with_options(db, verify_options, &err);
      CheckNoError(err);
      rocksdb_verify_file_checksums(db, &err);
      CheckCondition(err != NULL);
      CheckCondition(strstr(err, "file_checksum_gen_factory is null") != NULL);
      Free(&err);
      rocksdb_verify_file_checksums_with_options(db, verify_options, &err);
      CheckCondition(err != NULL);
      CheckCondition(strstr(err, "file_checksum_gen_factory is null") != NULL);
      Free(&err);

      rocksdb_readoptions_destroy(verify_options);
    }

    // Test column family handle get name
    {
      size_t name_len;
      char* cf_name =
          rocksdb_column_family_handle_get_name(handles[1], &name_len);
      CheckCondition(name_len == 3);
      CheckCondition(memcmp(cf_name, "cf1", 3) == 0);
      rocksdb_free(cf_name);
    }

    // Test zero-copy get with column families
    {
      rocksdb_pinnable_handle_t* handle =
          rocksdb_get_pinned_cf_v2(db, roptions, handles[1], "box", 3, &err);
      CheckNoError(err);
      CheckCondition(handle != NULL);
      size_t val_len;
      const char* val = rocksdb_pinnable_handle_get_value(handle, &val_len);
      CheckEqual("c", val, val_len);
      rocksdb_pinnable_handle_destroy(handle);

      // Test with non-existent key
      handle = rocksdb_get_pinned_cf_v2(db, roptions, handles[1], "notfound", 8,
                                        &err);
      CheckNoError(err);
      CheckCondition(handle == NULL);

      // Test rocksdb_get_into_buffer_cf
      char buffer[100];
      unsigned char found;
      unsigned char success = rocksdb_get_into_buffer_cf(
          db, roptions, handles[1], "buff", 4, buffer, sizeof(buffer), &val_len,
          &found, &err);
      CheckNoError(err);
      CheckCondition(success == 1);
      CheckCondition(found == 1);
      CheckCondition(val_len == 7);
      CheckCondition(memcmp(buffer, "rocksdb", 7) == 0);

      // Test with buffer too small
      success = rocksdb_get_into_buffer_cf(db, roptions, handles[1], "buff", 4,
                                           buffer, 3,  // Buffer too small
                                           &val_len, &found, &err);
      CheckNoError(err);
      CheckCondition(success == 0);  // Should fail due to small buffer
      CheckCondition(found == 1);
      CheckCondition(val_len == 7);  // Should still report actual size
    }

    // Test WriteBatchWithIndex iteration with Column Family
    rocksdb_writebatch_wi_t* wbwi = rocksdb_writebatch_wi_create(0, true);
    rocksdb_writebatch_wi_put_cf(wbwi, handles[1], "boat", 4, "row",
                                 3);  // should be filtered out
    rocksdb_writebatch_wi_put_cf(wbwi, handles[1], "buffy", 5, "charmed", 7);
    rocksdb_writebatch_wi_put_cf(wbwi, handles[1], "bus", 3, "yellow",
                                 6);  // should be filtered out
    rocksdb_readoptions_t* iter_roptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_iterate_lower_bound(iter_roptions, "bu", 2);
    rocksdb_readoptions_set_iterate_upper_bound(iter_roptions, "buffz", 5);
    rocksdb_iterator_t* base_iter =
        rocksdb_create_iterator_cf(db, iter_roptions, handles[1]);
    rocksdb_iterator_t* wbwi_iter =
        rocksdb_writebatch_wi_create_iterator_with_base_cf_readopts(
            wbwi, base_iter, handles[1], iter_roptions);

    CheckCondition(!rocksdb_iter_valid(wbwi_iter));
    rocksdb_iter_seek_to_first(wbwi_iter);
    CheckCondition(rocksdb_iter_valid(wbwi_iter));
    CheckIter(wbwi_iter, "buff", "rocksdb");
    rocksdb_iter_next(wbwi_iter);
    CheckIter(wbwi_iter, "buffy", "charmed");
    rocksdb_iter_next(wbwi_iter);
    CheckCondition(!rocksdb_iter_valid(wbwi_iter));

    rocksdb_iter_destroy(wbwi_iter);
    rocksdb_writebatch_wi_destroy(wbwi);
    rocksdb_readoptions_destroy(iter_roptions);

    const char* keys[3] = {"box", "box", "barfooxx"};
    const rocksdb_column_family_handle_t* get_handles[3] = {
        handles[0], handles[1], handles[1]};
    const size_t keys_sizes[3] = {3, 3, 8};
    char* vals[3];
    size_t vals_sizes[3];
    char* errs[3];
    rocksdb_multi_get_cf(db, roptions, get_handles, 3, keys, keys_sizes, vals,
                         vals_sizes, errs);

    int i;
    for (i = 0; i < 3; i++) {
      CheckEqual(NULL, errs[i], 0);
      switch (i) {
        case 0:
          CheckEqual(NULL, vals[i], vals_sizes[i]);  // wrong cf
          break;
        case 1:
          CheckEqual("c", vals[i], vals_sizes[i]);  // bingo
          break;
        case 2:
          CheckEqual(NULL, vals[i], vals_sizes[i]);  // normal not found
          break;
      }
      Free(&vals[i]);
    }

    {
      const char* batched_keys[4] = {"box", "buff", "barfooxx", "box"};
      const size_t batched_keys_sizes[4] = {3, 4, 8, 3};
      const char* expected_value[4] = {"c", "rocksdb", NULL, "c"};
      char* batched_errs[4];

      rocksdb_pinnableslice_t* pvals[4];
      rocksdb_batched_multi_get_cf(db, roptions, handles[1], 4, batched_keys,
                                   batched_keys_sizes, pvals, batched_errs,
                                   false);
      const char* val;
      size_t val_len;
      for (i = 0; i < 4; ++i) {
        val = rocksdb_pinnableslice_value(pvals[i], &val_len);
        CheckNoError(batched_errs[i]);
        CheckEqual(expected_value[i], val, val_len);
        rocksdb_pinnableslice_destroy(pvals[i]);
      }
    }

    {
      // Test rocksdb_batched_multi_get_cf_slice for better performance
      // Build rocksdb_slice_t array directly to avoid conversion overhead
      rocksdb_slice_t batched_key_slices[4];
      batched_key_slices[0].data = "box";
      batched_key_slices[0].size = 3;
      batched_key_slices[1].data = "buff";
      batched_key_slices[1].size = 4;
      batched_key_slices[2].data = "barfooxx";
      batched_key_slices[2].size = 8;
      batched_key_slices[3].data = "box";
      batched_key_slices[3].size = 3;

      const char* expected_value[4] = {"c", "rocksdb", NULL, "c"};
      char* batched_errs[4];
      rocksdb_pinnableslice_t* pvals[4];

      rocksdb_batched_multi_get_cf_slice(db, roptions, handles[1], 4,
                                         batched_key_slices, pvals,
                                         batched_errs, false);

      const char* val;
      size_t val_len;
      for (i = 0; i < 4; ++i) {
        CheckNoError(batched_errs[i]);
        if (pvals[i] != NULL) {
          val = rocksdb_pinnableslice_value(pvals[i], &val_len);
          CheckEqual(expected_value[i], val, val_len);
          rocksdb_pinnableslice_destroy(pvals[i]);
        } else {
          CheckEqual(expected_value[i], NULL, 0);
        }
      }
    }

    {
      // Test rocksdb_batched_multi_get_cf_slice with sorted_input=true
      // Keys must be in sorted order for this optimization
      rocksdb_slice_t sorted_key_slices[3];
      sorted_key_slices[0].data = "box";
      sorted_key_slices[0].size = 3;
      sorted_key_slices[1].data = "buff";
      sorted_key_slices[1].size = 4;
      sorted_key_slices[2].data = "notfound";
      sorted_key_slices[2].size = 8;

      const char* expected_value[3] = {"c", "rocksdb", NULL};
      char* batched_errs[3];
      rocksdb_pinnableslice_t* pvals[3];

      rocksdb_batched_multi_get_cf_slice(db, roptions, handles[1], 3,
                                         sorted_key_slices, pvals, batched_errs,
                                         true);

      const char* val;
      size_t val_len;
      for (i = 0; i < 3; ++i) {
        CheckNoError(batched_errs[i]);
        if (pvals[i] != NULL) {
          val = rocksdb_pinnableslice_value(pvals[i], &val_len);
          CheckEqual(expected_value[i], val, val_len);
          rocksdb_pinnableslice_destroy(pvals[i]);
        } else {
          CheckEqual(expected_value[i], NULL, 0);
        }
      }
    }

    {
      rocksdb_slice_t pinned_keys[6] = {{"box", 3},   {"missing", 7},
                                        {"empty", 5}, {"merge-error", 11},
                                        {"box", 3},   {"owned", 5}};
      rocksdb_pinnable_multi_get_t* multi_get;
      const char* value;
      const char* error;
      size_t value_size;
      size_t error_size;
      int result;

      rocksdb_put_cf(db, woptions, handles[1], "empty", 5, "", 0, &err);
      CheckNoError(err);
      rocksdb_put_cf(db, woptions, handles[1], "merge-error", 11, "base", 4,
                     &err);
      CheckNoError(err);
      rocksdb_merge_cf(db, woptions, handles[1], "merge-error", 11, "operand",
                       7, &err);
      CheckNoError(err);
      rocksdb_put_cf(db, woptions, handles[1], "owned", 5, "before", 6, &err);
      CheckNoError(err);

      multi_get = rocksdb_batched_multi_get_pinned_cf(db, roptions, handles[1],
                                                      6, pinned_keys, 0);
      CheckCondition(rocksdb_pinnable_multi_get_count(multi_get) == 6);
      CheckPinnableMultiGetFound(multi_get, 0, "c", 1);
      CheckPinnableMultiGetNotFound(multi_get, 1);
      CheckPinnableMultiGetFound(multi_get, 2, "", 0);
      CheckPinnableMultiGetError(multi_get, 3, "Merge operator failed");
      CheckPinnableMultiGetFound(multi_get, 4, "c", 1);
      CheckPinnableMultiGetFound(multi_get, 5, "before", 6);

      value = "unchanged";
      error = "unchanged";
      value_size = 9;
      error_size = 9;
      result = rocksdb_pinnable_multi_get_result(
          multi_get, 6, &value, &value_size, &error, &error_size);
      CheckCondition(result == rocksdb_pinnable_multi_get_out_of_bounds);
      CheckCondition(value == NULL);
      CheckCondition(value_size == 0);
      CheckCondition(error == NULL);
      CheckCondition(error_size == 0);

      result = rocksdb_pinnable_multi_get_result(
          multi_get, 5, &value, &value_size, &error, &error_size);
      CheckCondition(result == rocksdb_pinnable_multi_get_found);
      const char* borrowed_value = value;
      size_t borrowed_value_size = value_size;
      result = rocksdb_pinnable_multi_get_result(
          multi_get, 3, &value, &value_size, &error, &error_size);
      CheckCondition(result == rocksdb_pinnable_multi_get_error);
      const char* borrowed_error = error;
      size_t borrowed_error_size = error_size;
      rocksdb_put_cf(db, woptions, handles[1], "owned", 5, "after", 5, &err);
      CheckNoError(err);
      CheckCondition(borrowed_value_size == 6);
      CheckCondition(memcmp(borrowed_value, "before", 6) == 0);
      CheckCondition(borrowed_error_size == strlen(borrowed_error));
      CheckCondition(strstr(borrowed_error, "Merge operator failed") != NULL);
      rocksdb_pinnable_multi_get_destroy(multi_get);
    }

    {
      rocksdb_slice_t sorted_keys[4] = {
          {"box", 3}, {"box", 3}, {"empty", 5}, {"missing", 7}};
      rocksdb_pinnable_multi_get_t* multi_get =
          rocksdb_batched_multi_get_pinned_cf(db, roptions, handles[1], 4,
                                              sorted_keys, 1);
      CheckCondition(rocksdb_pinnable_multi_get_count(multi_get) == 4);
      CheckPinnableMultiGetFound(multi_get, 0, "c", 1);
      CheckPinnableMultiGetFound(multi_get, 1, "c", 1);
      CheckPinnableMultiGetFound(multi_get, 2, "", 0);
      CheckPinnableMultiGetNotFound(multi_get, 3);
      rocksdb_pinnable_multi_get_destroy(multi_get);

      multi_get = rocksdb_batched_multi_get_pinned_cf(db, roptions, handles[1],
                                                      0, sorted_keys, 1);
      CheckCondition(rocksdb_pinnable_multi_get_count(multi_get) == 0);
      rocksdb_pinnable_multi_get_destroy(multi_get);
    }

    rocksdb_delete_cf(db, woptions, handles[1], "empty", 5, &err);
    CheckNoError(err);
    rocksdb_delete_cf(db, woptions, handles[1], "merge-error", 11, &err);
    CheckNoError(err);
    rocksdb_delete_cf(db, woptions, handles[1], "owned", 5, &err);
    CheckNoError(err);

    {
      unsigned char value_found = 0;

      CheckCondition(!rocksdb_key_may_exist(db, roptions, "invalid_key", 11,
                                            NULL, NULL, NULL, 0, NULL));
      CheckCondition(!rocksdb_key_may_exist(db, roptions, "invalid_key", 11,
                                            &vals[0], &vals_sizes[0], NULL, 0,
                                            &value_found));
      if (value_found) {
        Free(&vals[0]);
      }

      CheckCondition(!rocksdb_key_may_exist_cf(db, roptions, handles[1],
                                               "invalid_key", 11, NULL, NULL,
                                               NULL, 0, NULL));
      CheckCondition(!rocksdb_key_may_exist_cf(db, roptions, handles[1],
                                               "invalid_key", 11, &vals[0],
                                               &vals_sizes[0], NULL, 0, NULL));
      if (value_found) {
        Free(&vals[0]);
      }
    }

    rocksdb_iterator_t* iter =
        rocksdb_create_iterator_cf(db, roptions, handles[1]);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));

    for (i = 0; rocksdb_iter_valid(iter) != 0; rocksdb_iter_next(iter)) {
      i++;
    }
    CheckCondition(i == 4);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_column_family_handle_t* iters_cf_handles[2] = {handles[0],
                                                           handles[1]};
    rocksdb_iterator_t* iters_handles[2];
    rocksdb_create_iterators(db, roptions, iters_cf_handles, iters_handles, 2,
                             &err);
    CheckNoError(err);

    iter = iters_handles[0];
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_destroy(iter);

    iter = iters_handles[1];
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));

    for (i = 0; rocksdb_iter_valid(iter) != 0; rocksdb_iter_next(iter)) {
      i++;
    }
    CheckCondition(i == 4);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    GetAndCheckMetaDataCf(db, handles[1], cf_names[1]);

    rocksdb_drop_column_family(db, handles[1], &err);
    CheckNoError(err);
    for (i = 0; i < 2; i++) {
      rocksdb_column_family_handle_destroy(handles[i]);
    }
    rocksdb_close(db);
    {
      // As column family has been dropped, we expect only one column family.
      const char* expected_cf_names[1] = {"default"};
      LoadAndCheckLatestOptions(dbname, env, false, cache, NULL, 1,
                                expected_cf_names, NULL);
    }
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_destroy(db_options);
    rocksdb_options_destroy(cf_options_1);
    rocksdb_options_destroy(cf_options_2);
    rocksdb_dbpath_destroy(dbpath2);
  }

  StartPhase("prefix");
  {
    // Create new database
    rocksdb_options_set_allow_mmap_reads(options, 1);
    rocksdb_options_set_prefix_extractor(
        options, rocksdb_slicetransform_create_fixed_prefix(3));
    rocksdb_options_set_hash_skip_list_rep(options, 5000, 4, 4);
    rocksdb_options_set_plain_table_factory(options, 4, 10, 0.75, 16, 0, 0, 0,
                                            0);
    rocksdb_options_set_allow_concurrent_memtable_write(options, 0);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_put(db, woptions, "foo1", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo2", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo3", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar1", 4, "bar", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar2", 4, "bar", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar3", 4, "bar", 3, &err);
    CheckNoError(err);

    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));

    rocksdb_iter_seek(iter, "bar", 3);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    CheckCondition(rocksdb_iter_valid(iter));

    CheckIter(iter, "bar1", "bar");
    rocksdb_iter_next(iter);
    CheckIter(iter, "bar2", "bar");
    rocksdb_iter_next(iter);
    CheckIter(iter, "bar3", "bar");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_readoptions_set_total_order_seek(roptions, 1);
    iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));

    rocksdb_iter_seek(iter, "ba", 2);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar1", "bar");

    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_set_total_order_seek(roptions, 0);

    rocksdb_close(db);

    {
      const char* expected_cf_names[1] = {"default"};
      LoadAndCheckLatestOptions(dbname, env, false, cache, NULL, 1,
                                expected_cf_names,
                                "Invalid argument: leveldb.BytewiseComparator: "
                                "does not match existing comparator foo");
      LoadAndCheckLatestOptions(dbname, env, false, cache, cmp, 1,
                                expected_cf_names, NULL);
    }
    rocksdb_destroy_db(options, dbname, &err);
  }

  // Check memory usage stats
  StartPhase("approximate_memory_usage");
  {
    // Create database
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_memory_consumers_t* consumers;
    consumers = rocksdb_memory_consumers_create();
    rocksdb_memory_consumers_add_db(consumers, db);
    rocksdb_memory_consumers_add_cache(consumers, cache);

    // take memory usage report before write-read operation
    rocksdb_memory_usage_t* mu1;
    mu1 = rocksdb_approximate_memory_usage_create(consumers, &err);
    CheckNoError(err);

    // Put data (this should affect memtables)
    rocksdb_put(db, woptions, "memory", 6, "test", 4, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "memory", "test");

    // take memory usage report after write-read operation
    rocksdb_memory_usage_t* mu2;
    mu2 = rocksdb_approximate_memory_usage_create(consumers, &err);
    CheckNoError(err);

    // amount of memory used within memtables should grow
    CheckCondition(rocksdb_approximate_memory_usage_get_mem_table_total(mu2) >=
                   rocksdb_approximate_memory_usage_get_mem_table_total(mu1));
    CheckCondition(
        rocksdb_approximate_memory_usage_get_mem_table_unflushed(mu2) >=
        rocksdb_approximate_memory_usage_get_mem_table_unflushed(mu1));

    rocksdb_memory_consumers_destroy(consumers);
    rocksdb_approximate_memory_usage_destroy(mu1);
    rocksdb_approximate_memory_usage_destroy(mu2);
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
  }

  StartPhase("cuckoo_options");
  {
    rocksdb_cuckoo_table_options_t* cuckoo_options;
    cuckoo_options = rocksdb_cuckoo_options_create();
    rocksdb_cuckoo_options_set_hash_ratio(cuckoo_options, 0.5);
    rocksdb_cuckoo_options_set_max_search_depth(cuckoo_options, 200);
    rocksdb_cuckoo_options_set_cuckoo_block_size(cuckoo_options, 10);
    rocksdb_cuckoo_options_set_identity_as_first_hash(cuckoo_options, 1);
    rocksdb_cuckoo_options_set_use_module_hash(cuckoo_options, 0);
    rocksdb_options_set_cuckoo_table_factory(options, cuckoo_options);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_cuckoo_options_destroy(cuckoo_options);
  }

  StartPhase("block_based_options_auto");
  {
    rocksdb_block_based_table_options_t* bbo;
    const char* factory_name;
    size_t factory_name_len;
    bbo = rocksdb_block_based_options_create();

    rocksdb_block_based_options_set_enable_index_compression(bbo, 1);
    CheckCondition(
        1 == rocksdb_block_based_options_get_enable_index_compression(bbo));

    rocksdb_block_based_options_set_max_auto_readahead_size(bbo, 4096);
    CheckCondition(
        4096 == rocksdb_block_based_options_get_max_auto_readahead_size(bbo));

    rocksdb_block_based_options_set_prepopulate_block_cache(bbo, 2);
    CheckCondition(
        2 == rocksdb_block_based_options_get_prepopulate_block_cache(bbo));

    rocksdb_block_based_options_set_initial_auto_readahead_size(bbo, 2048);
    CheckCondition(
        2048 ==
        rocksdb_block_based_options_get_initial_auto_readahead_size(bbo));

    rocksdb_block_based_options_set_num_file_reads_for_auto_readahead(bbo, 7);
    CheckCondition(
        7 ==
        rocksdb_block_based_options_get_num_file_reads_for_auto_readahead(bbo));

    rocksdb_block_based_options_set_user_defined_index_factory_from_string(
        bbo, "trie_index", 10, &err);
    CheckNoError(err);
    factory_name =
        rocksdb_block_based_options_get_user_defined_index_factory_name(
            bbo, &factory_name_len);
    CheckEqual("trie_index", factory_name, factory_name_len);
    rocksdb_block_based_options_clear_user_defined_index_factory(bbo);
    factory_name =
        rocksdb_block_based_options_get_user_defined_index_factory_name(
            bbo, &factory_name_len);
    CheckCondition(factory_name == NULL);
    CheckCondition(factory_name_len == 0);
    CheckCondition(
        rocksdb_block_based_options_get_use_udi_as_primary_index(bbo) == 0);
    rocksdb_block_based_options_set_use_udi_as_primary_index(bbo, 1);
    CheckCondition(
        rocksdb_block_based_options_get_use_udi_as_primary_index(bbo) == 1);
    rocksdb_block_based_options_set_use_udi_as_primary_index(bbo, 0);
    CheckCondition(
        rocksdb_block_based_options_get_use_udi_as_primary_index(bbo) == 0);
    CheckCondition(
        rocksdb_block_based_options_get_fail_if_no_udi_on_open(bbo) == 0);
    rocksdb_block_based_options_set_fail_if_no_udi_on_open(bbo, 1);
    CheckCondition(
        rocksdb_block_based_options_get_fail_if_no_udi_on_open(bbo) == 1);
    rocksdb_block_based_options_set_fail_if_no_udi_on_open(bbo, 0);
    CheckCondition(
        rocksdb_block_based_options_get_fail_if_no_udi_on_open(bbo) == 0);

    // index_mode defaults to standard_default and tracks the deprecated
    // bools until it is set explicitly; after that an explicit value wins no
    // matter what order the deprecated setters are called in.
    CheckCondition(rocksdb_block_based_table_index_mode_standard_default ==
                   rocksdb_block_based_options_get_index_mode(bbo));
    rocksdb_block_based_options_set_use_udi_as_primary_index(bbo, 1);
    CheckCondition(rocksdb_block_based_table_index_mode_custom_default ==
                   rocksdb_block_based_options_get_index_mode(bbo));
    rocksdb_block_based_options_set_index_mode(
        bbo, rocksdb_block_based_table_index_mode_custom_only);
    CheckCondition(rocksdb_block_based_table_index_mode_custom_only ==
                   rocksdb_block_based_options_get_index_mode(bbo));
    rocksdb_block_based_options_set_index_mode(
        bbo, rocksdb_block_based_table_index_mode_standard_only);
    rocksdb_block_based_options_set_fail_if_no_udi_on_open(bbo, 1);
    CheckCondition(rocksdb_block_based_table_index_mode_standard_only ==
                   rocksdb_block_based_options_get_index_mode(bbo));
    rocksdb_block_based_options_set_use_udi_as_primary_index(bbo, 0);
    rocksdb_block_based_options_set_fail_if_no_udi_on_open(bbo, 0);

    rocksdb_block_based_options_destroy(bbo);
  }

  StartPhase("options");
  {
    rocksdb_options_t* o;
    o = rocksdb_options_create();

    // Set and check options.
    rocksdb_options_set_allow_ingest_behind(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(o));

    rocksdb_options_compaction_readahead_size(o, 10);
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(o));

    rocksdb_options_set_create_if_missing(o, 1);
    CheckCondition(1 == rocksdb_options_get_create_if_missing(o));

    rocksdb_options_set_create_missing_column_families(o, 1);
    CheckCondition(1 == rocksdb_options_get_create_missing_column_families(o));

    rocksdb_options_set_error_if_exists(o, 1);
    CheckCondition(1 == rocksdb_options_get_error_if_exists(o));

    rocksdb_options_set_paranoid_checks(o, 1);
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(o));

    rocksdb_options_set_info_log_level(o, 3);
    CheckCondition(3 == rocksdb_options_get_info_log_level(o));

    rocksdb_options_set_write_buffer_size(o, 100);
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(o));

    rocksdb_options_set_db_write_buffer_size(o, 1000);
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(o));

    rocksdb_options_set_max_open_files(o, 21);
    CheckCondition(21 == rocksdb_options_get_max_open_files(o));

    rocksdb_options_set_max_file_opening_threads(o, 5);
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(o));

    rocksdb_options_set_max_total_wal_size(o, 400);
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(o));

    rocksdb_options_set_num_levels(o, 7);
    CheckCondition(7 == rocksdb_options_get_num_levels(o));

    rocksdb_options_set_level0_file_num_compaction_trigger(o, 4);
    CheckCondition(4 ==
                   rocksdb_options_get_level0_file_num_compaction_trigger(o));

    rocksdb_options_set_level0_slowdown_writes_trigger(o, 6);
    CheckCondition(6 == rocksdb_options_get_level0_slowdown_writes_trigger(o));

    rocksdb_options_set_level0_stop_writes_trigger(o, 8);
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(o));

    rocksdb_options_set_target_file_size_base(o, 256);
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(o));

    rocksdb_options_set_target_file_size_multiplier(o, 3);
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(o));

    rocksdb_options_set_max_bytes_for_level_base(o, 1024);
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(o));

    rocksdb_options_set_level_compaction_dynamic_level_bytes(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_level_compaction_dynamic_level_bytes(o));

    rocksdb_options_set_max_bytes_for_level_multiplier(o, 2.0);
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(o));

    rocksdb_options_set_periodic_compaction_seconds(o, 100000);
    CheckCondition(100000 ==
                   rocksdb_options_get_periodic_compaction_seconds(o));

    rocksdb_options_set_memtable_op_scan_flush_trigger(o, 100);
    CheckCondition(100 ==
                   rocksdb_options_get_memtable_op_scan_flush_trigger(o));

    rocksdb_options_set_memtable_avg_op_scan_flush_trigger(o, 150);
    CheckCondition(150 ==
                   rocksdb_options_get_memtable_avg_op_scan_flush_trigger(o));

    rocksdb_options_set_min_tombstones_for_range_conversion(o, 200);
    CheckCondition(200 ==
                   rocksdb_options_get_min_tombstones_for_range_conversion(o));

    rocksdb_options_set_ttl(o, 5000);
    CheckCondition(5000 == rocksdb_options_get_ttl(o));

    rocksdb_options_set_skip_stats_update_on_db_open(o, 1);
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(o));

    rocksdb_options_set_max_write_buffer_number(o, 97);
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(o));

    rocksdb_options_set_min_write_buffer_number_to_merge(o, 23);
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(o));

    rocksdb_options_set_max_write_buffer_size_to_maintain(o, 50000);
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(o));

    rocksdb_options_set_enable_pipelined_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(o));

    rocksdb_options_set_unordered_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_unordered_write(o));

    rocksdb_options_set_max_subcompactions(o, 123456);
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(o));

    rocksdb_options_set_max_background_jobs(o, 2);
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(o));

    rocksdb_options_set_max_background_compactions(o, 3);
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(o));

    rocksdb_options_set_max_background_flushes(o, 5);
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(o));

    rocksdb_options_set_max_log_file_size(o, 6);
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(o));

    rocksdb_options_set_log_file_time_to_roll(o, 7);
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(o));

    rocksdb_options_set_keep_log_file_num(o, 8);
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(o));

    rocksdb_options_set_recycle_log_file_num(o, 9);
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(o));

    rocksdb_options_set_soft_pending_compaction_bytes_limit(o, 10);
    CheckCondition(10 ==
                   rocksdb_options_get_soft_pending_compaction_bytes_limit(o));

    rocksdb_options_set_hard_pending_compaction_bytes_limit(o, 11);
    CheckCondition(11 ==
                   rocksdb_options_get_hard_pending_compaction_bytes_limit(o));

    rocksdb_options_set_max_manifest_file_size(o, 12);
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(o));

    rocksdb_options_set_table_cache_numshardbits(o, 13);
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(o));

    rocksdb_options_set_arena_block_size(o, 14);
    CheckCondition(14 == rocksdb_options_get_arena_block_size(o));

    rocksdb_options_set_use_fsync(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_fsync(o));

    rocksdb_options_set_WAL_ttl_seconds(o, 15);
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(o));

    rocksdb_options_set_WAL_size_limit_MB(o, 16);
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(o));

    rocksdb_options_set_manifest_preallocation_size(o, 17);
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(o));

    rocksdb_options_set_allow_mmap_reads(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(o));

    rocksdb_options_set_allow_mmap_writes(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(o));

    rocksdb_options_set_use_direct_reads(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(o));

    rocksdb_options_set_use_direct_io_for_flush_and_compaction(o, 1);
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(o));

    rocksdb_options_set_use_direct_io_for_compaction_reads(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_use_direct_io_for_compaction_reads(o));

    rocksdb_options_set_is_fd_close_on_exec(o, 1);
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(o));

    rocksdb_options_set_stats_dump_period_sec(o, 18);
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(o));

    rocksdb_options_set_stats_persist_period_sec(o, 5);
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(o));

    rocksdb_options_set_advise_random_on_open(o, 1);
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(o));

    rocksdb_options_set_use_adaptive_mutex(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(o));

    rocksdb_options_set_bytes_per_sync(o, 19);
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(o));

    rocksdb_options_set_wal_bytes_per_sync(o, 20);
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(o));

    rocksdb_options_set_writable_file_max_buffer_size(o, 21);
    CheckCondition(21 == rocksdb_options_get_writable_file_max_buffer_size(o));

    rocksdb_options_set_allow_concurrent_memtable_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_concurrent_memtable_write(o));

    rocksdb_options_set_enable_write_thread_adaptive_yield(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_enable_write_thread_adaptive_yield(o));

    rocksdb_options_set_max_sequential_skip_in_iterations(o, 22);
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(o));

    rocksdb_options_set_disable_auto_compactions(o, 1);
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(o));

    rocksdb_options_set_optimize_filters_for_hits(o, 1);
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(o));

    rocksdb_options_set_delete_obsolete_files_period_micros(o, 23);
    CheckCondition(23 ==
                   rocksdb_options_get_delete_obsolete_files_period_micros(o));

    rocksdb_options_set_memtable_prefix_bloom_size_ratio(o, 2.0);
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(o));

    rocksdb_options_set_max_compaction_bytes(o, 24);
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(o));

    rocksdb_options_set_memtable_huge_page_size(o, 25);
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(o));

    // memtable_batch_lookup_optimization defaults to 0; flip to 1 to verify
    // the setter is wired through to the underlying C++ ColumnFamilyOptions
    // field.
    CheckCondition(0 ==
                   rocksdb_options_get_memtable_batch_lookup_optimization(o));
    rocksdb_options_set_memtable_batch_lookup_optimization(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_memtable_batch_lookup_optimization(o));

    rocksdb_options_set_max_successive_merges(o, 26);
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(o));

    rocksdb_options_set_bloom_locality(o, 27);
    CheckCondition(27 == rocksdb_options_get_bloom_locality(o));

    rocksdb_options_set_inplace_update_support(o, 1);
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(o));

    rocksdb_options_set_inplace_update_num_locks(o, 28);
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(o));

    rocksdb_options_set_report_bg_io_stats(o, 1);
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(o));

    rocksdb_options_set_wal_recovery_mode(o, 2);
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(o));

    rocksdb_options_set_compression(o, 5);
    CheckCondition(5 == rocksdb_options_get_compression(o));

    rocksdb_options_set_bottommost_compression(o, 4);
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(o));

    rocksdb_options_set_compaction_style(o, 2);
    CheckCondition(2 == rocksdb_options_get_compaction_style(o));

    rocksdb_options_set_compaction_pri(o, 4);
    CheckCondition(4 == rocksdb_options_get_compaction_pri(o));

    rocksdb_options_set_atomic_flush(o, 1);
    CheckCondition(1 == rocksdb_options_get_atomic_flush(o));

    rocksdb_options_set_manual_wal_flush(o, 1);
    CheckCondition(1 == rocksdb_options_get_manual_wal_flush(o));

    rocksdb_options_set_wal_compression(o, 1);
    CheckCondition(1 == rocksdb_options_get_wal_compression(o));

    rocksdb_options_set_experimental_mempurge_threshold(o, 29.0);
    CheckCondition(29.0 ==
                   rocksdb_options_get_experimental_mempurge_threshold(o));

    CheckCondition(rocksdb_statistics_level_disable_all ==
                   rocksdb_options_get_statistics_level(o));
    rocksdb_options_enable_statistics(o);
    CheckCondition(rocksdb_statistics_level_disable_all !=
                   rocksdb_options_get_statistics_level(o));
    rocksdb_options_set_statistics_level(o, rocksdb_statistics_level_all);
    CheckCondition(rocksdb_statistics_level_all ==
                   rocksdb_options_get_statistics_level(o));

    CheckCondition(0 ==
                   rocksdb_options_get_track_and_verify_wals_in_manifest(o));
    rocksdb_options_set_track_and_verify_wals_in_manifest(o, 42);
    CheckCondition(1 ==
                   rocksdb_options_get_track_and_verify_wals_in_manifest(o));
    CheckCondition(0 == rocksdb_options_get_async_wal_precreate(o));
    rocksdb_options_set_async_wal_precreate(o, 1);
    CheckCondition(1 == rocksdb_options_get_async_wal_precreate(o));

    CheckCondition(0 == rocksdb_options_checksum_handoff_file_types_count(o));
    rocksdb_options_checksum_handoff_file_types_add(o, 0);
    rocksdb_options_checksum_handoff_file_types_add(o, 2);
    CheckCondition(2 == rocksdb_options_checksum_handoff_file_types_count(o));
    CheckCondition(1 ==
                   rocksdb_options_checksum_handoff_file_types_contains(o, 0));
    CheckCondition(1 ==
                   rocksdb_options_checksum_handoff_file_types_contains(o, 2));
    rocksdb_options_checksum_handoff_file_types_remove(o, 0);
    CheckCondition(0 ==
                   rocksdb_options_checksum_handoff_file_types_contains(o, 0));
    CheckCondition(1 == rocksdb_options_checksum_handoff_file_types_count(o));

    CheckCondition(
        1 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(o));
    CheckCondition(
        1 ==
        rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(o, 0));
    rocksdb_options_calculate_sst_write_lifetime_hint_set_clear(o);
    CheckCondition(
        0 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(o));
    rocksdb_options_calculate_sst_write_lifetime_hint_set_add(o, 1);
    rocksdb_options_calculate_sst_write_lifetime_hint_set_add(o, 2);
    CheckCondition(
        2 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(o));
    CheckCondition(
        1 ==
        rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(o, 1));
    CheckCondition(
        1 ==
        rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(o, 2));

    /* Blob Options */
    rocksdb_options_set_enable_blob_files(o, 1);
    CheckCondition(1 == rocksdb_options_get_enable_blob_files(o));

    rocksdb_options_set_min_blob_size(o, 29);
    CheckCondition(29 == rocksdb_options_get_min_blob_size(o));

    rocksdb_options_set_blob_file_size(o, 30);
    CheckCondition(30 == rocksdb_options_get_blob_file_size(o));

    rocksdb_options_set_blob_compression_type(o, 4);
    CheckCondition(4 == rocksdb_options_get_blob_compression_type(o));

    rocksdb_options_set_enable_blob_gc(o, 1);
    CheckCondition(1 == rocksdb_options_get_enable_blob_gc(o));

    rocksdb_options_set_blob_gc_age_cutoff(o, 0.5);
    CheckCondition(0.5 == rocksdb_options_get_blob_gc_age_cutoff(o));

    rocksdb_options_set_blob_gc_force_threshold(o, 0.75);
    CheckCondition(0.75 == rocksdb_options_get_blob_gc_force_threshold(o));

    rocksdb_options_set_read_triggered_compaction_threshold(o, 0.001);
    CheckCondition(0.001 ==
                   rocksdb_options_get_read_triggered_compaction_threshold(o));

    rocksdb_options_set_max_compaction_trigger_wakeup_seconds(o, 3600);
    CheckCondition(
        3600 == rocksdb_options_get_max_compaction_trigger_wakeup_seconds(o));

    rocksdb_options_set_blob_compaction_readahead_size(o, 262144);
    CheckCondition(262144 ==
                   rocksdb_options_get_blob_compaction_readahead_size(o));

    rocksdb_options_set_blob_file_starting_level(o, 5);
    CheckCondition(5 == rocksdb_options_get_blob_file_starting_level(o));

    rocksdb_options_set_prepopulate_blob_cache(o, 1 /* flush only */);
    CheckCondition(1 == rocksdb_options_get_prepopulate_blob_cache(o));

    // Create a copy that should be equal to the original.
    rocksdb_options_t* copy;
    copy = rocksdb_options_create_copy(o);

    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(copy));
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(copy));
    CheckCondition(1 == rocksdb_options_get_create_if_missing(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_create_missing_column_families(copy));
    CheckCondition(1 == rocksdb_options_get_error_if_exists(copy));
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(copy));
    CheckCondition(3 == rocksdb_options_get_info_log_level(copy));
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(copy));
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(copy));
    CheckCondition(21 == rocksdb_options_get_max_open_files(copy));
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(copy));
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(copy));
    CheckCondition(7 == rocksdb_options_get_num_levels(copy));
    CheckCondition(
        4 == rocksdb_options_get_level0_file_num_compaction_trigger(copy));
    CheckCondition(6 ==
                   rocksdb_options_get_level0_slowdown_writes_trigger(copy));
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(copy));
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(copy));
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(copy));
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(copy));
    CheckCondition(
        1 == rocksdb_options_get_level_compaction_dynamic_level_bytes(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(copy));
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(copy));
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(copy));
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(copy));
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(copy));
    CheckCondition(1 == rocksdb_options_get_unordered_write(copy));
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(copy));
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(copy));
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(copy));
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(copy));
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(copy));
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(copy));
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(copy));
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(copy));
    CheckCondition(
        10 == rocksdb_options_get_soft_pending_compaction_bytes_limit(copy));
    CheckCondition(
        11 == rocksdb_options_get_hard_pending_compaction_bytes_limit(copy));
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(copy));
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(copy));
    CheckCondition(14 == rocksdb_options_get_arena_block_size(copy));
    CheckCondition(1 == rocksdb_options_get_use_fsync(copy));
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(copy));
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(copy));
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(copy));
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(copy));
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(copy));
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_compaction_reads(copy));
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(copy));
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(copy));
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(copy));
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(copy));
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(copy));
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(copy));
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(copy));
    CheckCondition(21 ==
                   rocksdb_options_get_writable_file_max_buffer_size(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_allow_concurrent_memtable_write(copy));
    CheckCondition(
        1 == rocksdb_options_get_enable_write_thread_adaptive_yield(copy));
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(copy));
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(copy));
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(copy));
    CheckCondition(
        23 == rocksdb_options_get_delete_obsolete_files_period_micros(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(copy));
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(copy));
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(copy));
    CheckCondition(
        1 == rocksdb_options_get_memtable_batch_lookup_optimization(copy));
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(copy));
    CheckCondition(27 == rocksdb_options_get_bloom_locality(copy));
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(copy));
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(copy));
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(copy));
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(copy));
    CheckCondition(5 == rocksdb_options_get_compression(copy));
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(copy));
    CheckCondition(2 == rocksdb_options_get_compaction_style(copy));
    CheckCondition(1 == rocksdb_options_get_atomic_flush(copy));
    CheckCondition(29.0 ==
                   rocksdb_options_get_experimental_mempurge_threshold(copy));
    CheckCondition(1 ==
                   rocksdb_options_checksum_handoff_file_types_count(copy));
    CheckCondition(
        1 == rocksdb_options_checksum_handoff_file_types_contains(copy, 2));
    CheckCondition(
        2 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(copy));
    CheckCondition(
        1 == rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(
                 copy, 1));
    CheckCondition(
        1 == rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(
                 copy, 2));

    // Copies should be independent.
    rocksdb_options_set_allow_ingest_behind(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_ingest_behind(copy));
    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(o));

    rocksdb_options_compaction_readahead_size(copy, 20);
    CheckCondition(20 == rocksdb_options_get_compaction_readahead_size(copy));
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(o));

    rocksdb_options_set_create_if_missing(copy, 0);
    CheckCondition(0 == rocksdb_options_get_create_if_missing(copy));
    CheckCondition(1 == rocksdb_options_get_create_if_missing(o));

    rocksdb_options_set_create_missing_column_families(copy, 0);
    CheckCondition(0 ==
                   rocksdb_options_get_create_missing_column_families(copy));
    CheckCondition(1 == rocksdb_options_get_create_missing_column_families(o));

    rocksdb_options_set_error_if_exists(copy, 0);
    CheckCondition(0 == rocksdb_options_get_error_if_exists(copy));
    CheckCondition(1 == rocksdb_options_get_error_if_exists(o));

    rocksdb_options_set_paranoid_checks(copy, 0);
    CheckCondition(0 == rocksdb_options_get_paranoid_checks(copy));
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(o));

    rocksdb_options_set_info_log_level(copy, 2);
    CheckCondition(2 == rocksdb_options_get_info_log_level(copy));
    CheckCondition(3 == rocksdb_options_get_info_log_level(o));

    rocksdb_options_set_write_buffer_size(copy, 200);
    CheckCondition(200 == rocksdb_options_get_write_buffer_size(copy));
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(o));

    rocksdb_options_set_db_write_buffer_size(copy, 2000);
    CheckCondition(2000 == rocksdb_options_get_db_write_buffer_size(copy));
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(o));

    rocksdb_options_set_max_open_files(copy, 42);
    CheckCondition(42 == rocksdb_options_get_max_open_files(copy));
    CheckCondition(21 == rocksdb_options_get_max_open_files(o));

    rocksdb_options_set_max_file_opening_threads(copy, 3);
    CheckCondition(3 == rocksdb_options_get_max_file_opening_threads(copy));
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(o));

    rocksdb_options_set_max_total_wal_size(copy, 4000);
    CheckCondition(4000 == rocksdb_options_get_max_total_wal_size(copy));
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(o));

    rocksdb_options_set_num_levels(copy, 6);
    CheckCondition(6 == rocksdb_options_get_num_levels(copy));
    CheckCondition(7 == rocksdb_options_get_num_levels(o));

    rocksdb_options_set_level0_file_num_compaction_trigger(copy, 14);
    CheckCondition(
        14 == rocksdb_options_get_level0_file_num_compaction_trigger(copy));
    CheckCondition(4 ==
                   rocksdb_options_get_level0_file_num_compaction_trigger(o));

    rocksdb_options_set_level0_slowdown_writes_trigger(copy, 61);
    CheckCondition(61 ==
                   rocksdb_options_get_level0_slowdown_writes_trigger(copy));
    CheckCondition(6 == rocksdb_options_get_level0_slowdown_writes_trigger(o));

    rocksdb_options_set_level0_stop_writes_trigger(copy, 17);
    CheckCondition(17 == rocksdb_options_get_level0_stop_writes_trigger(copy));
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(o));

    rocksdb_options_set_target_file_size_base(copy, 128);
    CheckCondition(128 == rocksdb_options_get_target_file_size_base(copy));
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(o));

    rocksdb_options_set_target_file_size_multiplier(copy, 13);
    CheckCondition(13 == rocksdb_options_get_target_file_size_multiplier(copy));
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(o));

    rocksdb_options_set_max_bytes_for_level_base(copy, 900);
    CheckCondition(900 == rocksdb_options_get_max_bytes_for_level_base(copy));
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(o));

    rocksdb_options_set_level_compaction_dynamic_level_bytes(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_level_compaction_dynamic_level_bytes(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_level_compaction_dynamic_level_bytes(o));

    rocksdb_options_set_max_bytes_for_level_multiplier(copy, 8.0);
    CheckCondition(8.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(o));

    rocksdb_options_set_periodic_compaction_seconds(copy, 8000);
    CheckCondition(8000 ==
                   rocksdb_options_get_periodic_compaction_seconds(copy));
    CheckCondition(100000 ==
                   rocksdb_options_get_periodic_compaction_seconds(o));

    rocksdb_options_set_memtable_op_scan_flush_trigger(copy, 800);
    CheckCondition(800 ==
                   rocksdb_options_get_memtable_op_scan_flush_trigger(copy));
    CheckCondition(100 ==
                   rocksdb_options_get_memtable_op_scan_flush_trigger(o));

    rocksdb_options_set_memtable_avg_op_scan_flush_trigger(copy, 900);
    CheckCondition(
        900 == rocksdb_options_get_memtable_avg_op_scan_flush_trigger(copy));
    CheckCondition(150 ==
                   rocksdb_options_get_memtable_avg_op_scan_flush_trigger(o));

    rocksdb_options_set_min_tombstones_for_range_conversion(copy, 1000);
    CheckCondition(
        1000 == rocksdb_options_get_min_tombstones_for_range_conversion(copy));
    CheckCondition(200 ==
                   rocksdb_options_get_min_tombstones_for_range_conversion(o));

    rocksdb_options_set_ttl(copy, 8000);
    CheckCondition(8000 == rocksdb_options_get_ttl(copy));
    CheckCondition(5000 == rocksdb_options_get_ttl(o));

    rocksdb_options_set_skip_stats_update_on_db_open(copy, 0);
    CheckCondition(0 == rocksdb_options_get_skip_stats_update_on_db_open(copy));
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(o));

    rocksdb_options_set_max_write_buffer_number(copy, 2000);
    CheckCondition(2000 == rocksdb_options_get_max_write_buffer_number(copy));
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(o));

    rocksdb_options_set_min_write_buffer_number_to_merge(copy, 146);
    CheckCondition(146 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(o));

    rocksdb_options_set_max_write_buffer_size_to_maintain(copy, 9000);
    CheckCondition(9000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(copy));
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(o));

    rocksdb_options_set_enable_pipelined_write(copy, 0);
    CheckCondition(0 == rocksdb_options_get_enable_pipelined_write(copy));
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(o));

    rocksdb_options_set_unordered_write(copy, 0);
    CheckCondition(0 == rocksdb_options_get_unordered_write(copy));
    CheckCondition(1 == rocksdb_options_get_unordered_write(o));

    rocksdb_options_set_max_subcompactions(copy, 90001);
    CheckCondition(90001 == rocksdb_options_get_max_subcompactions(copy));
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(o));

    rocksdb_options_set_max_background_jobs(copy, 12);
    CheckCondition(12 == rocksdb_options_get_max_background_jobs(copy));
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(o));

    rocksdb_options_set_max_background_compactions(copy, 13);
    CheckCondition(13 == rocksdb_options_get_max_background_compactions(copy));
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(o));

    rocksdb_options_set_max_background_flushes(copy, 15);
    CheckCondition(15 == rocksdb_options_get_max_background_flushes(copy));
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(o));

    rocksdb_options_set_max_log_file_size(copy, 16);
    CheckCondition(16 == rocksdb_options_get_max_log_file_size(copy));
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(o));

    rocksdb_options_set_log_file_time_to_roll(copy, 17);
    CheckCondition(17 == rocksdb_options_get_log_file_time_to_roll(copy));
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(o));

    rocksdb_options_set_keep_log_file_num(copy, 18);
    CheckCondition(18 == rocksdb_options_get_keep_log_file_num(copy));
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(o));

    rocksdb_options_set_recycle_log_file_num(copy, 19);
    CheckCondition(19 == rocksdb_options_get_recycle_log_file_num(copy));
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(o));

    rocksdb_options_set_soft_pending_compaction_bytes_limit(copy, 110);
    CheckCondition(
        110 == rocksdb_options_get_soft_pending_compaction_bytes_limit(copy));
    CheckCondition(10 ==
                   rocksdb_options_get_soft_pending_compaction_bytes_limit(o));

    rocksdb_options_set_hard_pending_compaction_bytes_limit(copy, 111);
    CheckCondition(
        111 == rocksdb_options_get_hard_pending_compaction_bytes_limit(copy));
    CheckCondition(11 ==
                   rocksdb_options_get_hard_pending_compaction_bytes_limit(o));

    rocksdb_options_set_max_manifest_file_size(copy, 112);
    CheckCondition(112 == rocksdb_options_get_max_manifest_file_size(copy));
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(o));

    rocksdb_options_set_table_cache_numshardbits(copy, 113);
    CheckCondition(113 == rocksdb_options_get_table_cache_numshardbits(copy));
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(o));

    rocksdb_options_set_arena_block_size(copy, 114);
    CheckCondition(114 == rocksdb_options_get_arena_block_size(copy));
    CheckCondition(14 == rocksdb_options_get_arena_block_size(o));

    rocksdb_options_set_use_fsync(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_fsync(copy));
    CheckCondition(1 == rocksdb_options_get_use_fsync(o));

    rocksdb_options_set_WAL_ttl_seconds(copy, 115);
    CheckCondition(115 == rocksdb_options_get_WAL_ttl_seconds(copy));
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(o));

    rocksdb_options_set_WAL_size_limit_MB(copy, 116);
    CheckCondition(116 == rocksdb_options_get_WAL_size_limit_MB(copy));
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(o));

    rocksdb_options_set_manifest_preallocation_size(copy, 117);
    CheckCondition(117 ==
                   rocksdb_options_get_manifest_preallocation_size(copy));
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(o));

    rocksdb_options_set_allow_mmap_reads(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_mmap_reads(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(o));

    rocksdb_options_set_allow_mmap_writes(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_mmap_writes(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(o));

    rocksdb_options_set_use_direct_reads(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_direct_reads(copy));
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(o));

    rocksdb_options_set_use_direct_io_for_flush_and_compaction(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(copy));
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(o));

    rocksdb_options_set_use_direct_io_for_compaction_reads(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_use_direct_io_for_compaction_reads(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_use_direct_io_for_compaction_reads(o));

    rocksdb_options_set_is_fd_close_on_exec(copy, 0);
    CheckCondition(0 == rocksdb_options_get_is_fd_close_on_exec(copy));
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(o));

    rocksdb_options_set_stats_dump_period_sec(copy, 218);
    CheckCondition(218 == rocksdb_options_get_stats_dump_period_sec(copy));
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(o));

    rocksdb_options_set_stats_persist_period_sec(copy, 600);
    CheckCondition(600 == rocksdb_options_get_stats_persist_period_sec(copy));
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(o));

    rocksdb_options_set_advise_random_on_open(copy, 0);
    CheckCondition(0 == rocksdb_options_get_advise_random_on_open(copy));
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(o));

    rocksdb_options_set_use_adaptive_mutex(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_adaptive_mutex(copy));
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(o));

    rocksdb_options_set_bytes_per_sync(copy, 219);
    CheckCondition(219 == rocksdb_options_get_bytes_per_sync(copy));
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(o));

    rocksdb_options_set_wal_bytes_per_sync(copy, 120);
    CheckCondition(120 == rocksdb_options_get_wal_bytes_per_sync(copy));
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(o));

    rocksdb_options_set_writable_file_max_buffer_size(copy, 121);
    CheckCondition(121 ==
                   rocksdb_options_get_writable_file_max_buffer_size(copy));
    CheckCondition(21 == rocksdb_options_get_writable_file_max_buffer_size(o));

    rocksdb_options_set_allow_concurrent_memtable_write(copy, 0);
    CheckCondition(0 ==
                   rocksdb_options_get_allow_concurrent_memtable_write(copy));
    CheckCondition(1 == rocksdb_options_get_allow_concurrent_memtable_write(o));

    rocksdb_options_set_enable_write_thread_adaptive_yield(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_enable_write_thread_adaptive_yield(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_enable_write_thread_adaptive_yield(o));

    rocksdb_options_set_max_sequential_skip_in_iterations(copy, 122);
    CheckCondition(122 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(copy));
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(o));

    rocksdb_options_set_disable_auto_compactions(copy, 0);
    CheckCondition(0 == rocksdb_options_get_disable_auto_compactions(copy));
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(o));

    rocksdb_options_set_optimize_filters_for_hits(copy, 0);
    CheckCondition(0 == rocksdb_options_get_optimize_filters_for_hits(copy));
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(o));

    rocksdb_options_set_delete_obsolete_files_period_micros(copy, 123);
    CheckCondition(
        123 == rocksdb_options_get_delete_obsolete_files_period_micros(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_delete_obsolete_files_period_micros(o));

    rocksdb_options_set_memtable_prefix_bloom_size_ratio(copy, 4.0);
    CheckCondition(4.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(o));

    rocksdb_options_set_max_compaction_bytes(copy, 124);
    CheckCondition(124 == rocksdb_options_get_max_compaction_bytes(copy));
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(o));

    rocksdb_options_set_memtable_huge_page_size(copy, 125);
    CheckCondition(125 == rocksdb_options_get_memtable_huge_page_size(copy));
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(o));

    rocksdb_options_set_max_successive_merges(copy, 126);
    CheckCondition(126 == rocksdb_options_get_max_successive_merges(copy));
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(o));

    rocksdb_options_set_bloom_locality(copy, 127);
    CheckCondition(127 == rocksdb_options_get_bloom_locality(copy));
    CheckCondition(27 == rocksdb_options_get_bloom_locality(o));

    rocksdb_options_set_inplace_update_support(copy, 0);
    CheckCondition(0 == rocksdb_options_get_inplace_update_support(copy));
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(o));

    rocksdb_options_set_inplace_update_num_locks(copy, 128);
    CheckCondition(128 == rocksdb_options_get_inplace_update_num_locks(copy));
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(o));

    rocksdb_options_set_report_bg_io_stats(copy, 0);
    CheckCondition(0 == rocksdb_options_get_report_bg_io_stats(copy));
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(o));

    rocksdb_options_set_wal_recovery_mode(copy, 1);
    CheckCondition(1 == rocksdb_options_get_wal_recovery_mode(copy));
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(o));

    rocksdb_options_set_compression(copy, 4);
    CheckCondition(4 == rocksdb_options_get_compression(copy));
    CheckCondition(5 == rocksdb_options_get_compression(o));

    rocksdb_options_set_bottommost_compression(copy, 3);
    CheckCondition(3 == rocksdb_options_get_bottommost_compression(copy));
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(o));

    rocksdb_options_set_compaction_style(copy, 1);
    CheckCondition(1 == rocksdb_options_get_compaction_style(copy));
    CheckCondition(2 == rocksdb_options_get_compaction_style(o));

    rocksdb_options_set_compaction_pri(copy, 1);
    CheckCondition(1 == rocksdb_options_get_compaction_pri(copy));
    CheckCondition(4 == rocksdb_options_get_compaction_pri(o));

    rocksdb_options_set_atomic_flush(copy, 0);
    CheckCondition(0 == rocksdb_options_get_atomic_flush(copy));
    CheckCondition(1 == rocksdb_options_get_atomic_flush(o));

    rocksdb_options_set_experimental_mempurge_threshold(copy, 229.0);
    CheckCondition(229.0 ==
                   rocksdb_options_get_experimental_mempurge_threshold(copy));
    CheckCondition(29.0 ==
                   rocksdb_options_get_experimental_mempurge_threshold(o));

    rocksdb_options_checksum_handoff_file_types_add(copy, 3);
    CheckCondition(2 ==
                   rocksdb_options_checksum_handoff_file_types_count(copy));
    CheckCondition(1 == rocksdb_options_checksum_handoff_file_types_count(o));
    CheckCondition(
        1 == rocksdb_options_checksum_handoff_file_types_contains(copy, 3));
    CheckCondition(0 ==
                   rocksdb_options_checksum_handoff_file_types_contains(o, 3));

    rocksdb_options_calculate_sst_write_lifetime_hint_set_remove(copy, 2);
    CheckCondition(
        1 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(copy));
    CheckCondition(
        2 == rocksdb_options_calculate_sst_write_lifetime_hint_set_count(o));
    CheckCondition(
        0 == rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(
                 copy, 2));
    CheckCondition(
        1 ==
        rocksdb_options_calculate_sst_write_lifetime_hint_set_contains(o, 2));

    rocksdb_options_destroy(copy);
    rocksdb_options_destroy(o);
  }

  StartPhase("options_auto");
  {
    rocksdb_options_t* o;
    const char* str;
    size_t len;
    o = rocksdb_options_create();

    rocksdb_options_set_db_log_dir(o, "db-log-dir");
    str = rocksdb_options_get_db_log_dir(o, &len);
    CheckEqual("db-log-dir", str, len);

    rocksdb_options_set_wal_dir(o, "wal-dir");
    str = rocksdb_options_get_wal_dir(o, &len);
    CheckEqual("wal-dir", str, len);

    rocksdb_options_set_allow_fallocate(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_fallocate(o));

    rocksdb_options_set_db_host_id(o, "db-host");
    str = rocksdb_options_get_db_host_id(o, &len);
    CheckEqual("db-host", str, len);

    rocksdb_options_set_metadata_write_temperature(o, 2);
    CheckCondition(2 == rocksdb_options_get_metadata_write_temperature(o));

    rocksdb_options_set_cf_allow_ingest_behind(o, 1);
    CheckCondition(1 == rocksdb_options_get_cf_allow_ingest_behind(o));

    rocksdb_options_set_block_protection_bytes_per_key(o, 8);
    CheckCondition(8 == rocksdb_options_get_block_protection_bytes_per_key(o));

    rocksdb_options_destroy(o);
  }

  StartPhase("read_options");
  {
    rocksdb_readoptions_t* ro;
    const char* request_id;
    size_t request_id_len;
    const char* table_index_factory_name;
    size_t table_index_factory_name_len;

    ro = rocksdb_readoptions_create();

    rocksdb_readoptions_set_verify_checksums(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_verify_checksums(ro));

    rocksdb_readoptions_set_fill_cache(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_fill_cache(ro));

    rocksdb_readoptions_set_read_tier(ro, 2);
    CheckCondition(2 == rocksdb_readoptions_get_read_tier(ro));

    rocksdb_readoptions_set_rate_limiter_priority(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_rate_limiter_priority(ro));

    rocksdb_readoptions_set_value_size_soft_limit(ro, 123);
    CheckCondition(123 == rocksdb_readoptions_get_value_size_soft_limit(ro));

    CheckCondition(0 ==
                   rocksdb_readoptions_has_merge_operand_count_threshold(ro));
    rocksdb_readoptions_set_merge_operand_count_threshold(ro, 456);
    CheckCondition(1 ==
                   rocksdb_readoptions_has_merge_operand_count_threshold(ro));
    CheckCondition(456 ==
                   rocksdb_readoptions_get_merge_operand_count_threshold(ro));
    rocksdb_readoptions_clear_merge_operand_count_threshold(ro);
    CheckCondition(0 ==
                   rocksdb_readoptions_has_merge_operand_count_threshold(ro));

    rocksdb_readoptions_set_tailing(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_tailing(ro));

    rocksdb_readoptions_set_optimize_multiget_for_io(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_optimize_multiget_for_io(ro));

    rocksdb_readoptions_set_readahead_size(ro, 100);
    CheckCondition(100 == rocksdb_readoptions_get_readahead_size(ro));

    rocksdb_readoptions_set_auto_prefix_mode(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_auto_prefix_mode(ro));

    rocksdb_readoptions_set_prefix_same_as_start(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_prefix_same_as_start(ro));

    rocksdb_readoptions_set_pin_data(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_pin_data(ro));

    rocksdb_readoptions_set_adaptive_readahead(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_adaptive_readahead(ro));

    rocksdb_readoptions_set_total_order_seek(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_total_order_seek(ro));

    rocksdb_readoptions_set_max_skippable_internal_keys(ro, 200);
    CheckCondition(200 ==
                   rocksdb_readoptions_get_max_skippable_internal_keys(ro));

    rocksdb_readoptions_set_background_purge_on_iterator_cleanup(ro, 1);
    CheckCondition(
        1 == rocksdb_readoptions_get_background_purge_on_iterator_cleanup(ro));

    rocksdb_readoptions_set_ignore_range_deletions(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_ignore_range_deletions(ro));

    rocksdb_readoptions_set_deadline(ro, 300);
    CheckCondition(300 == rocksdb_readoptions_get_deadline(ro));

    rocksdb_readoptions_set_io_timeout(ro, 400);
    CheckCondition(400 == rocksdb_readoptions_get_io_timeout(ro));

    rocksdb_readoptions_set_async_io(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_async_io(ro));

    rocksdb_readoptions_set_auto_readahead_size(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_auto_readahead_size(ro));

    rocksdb_readoptions_set_allow_unprepared_value(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_allow_unprepared_value(ro));

    rocksdb_readoptions_set_auto_refresh_iterator_with_snapshot(ro, 1);
    CheckCondition(
        1 == rocksdb_readoptions_get_auto_refresh_iterator_with_snapshot(ro));

    rocksdb_readoptions_set_io_activity(ro, 3);
    CheckCondition(3 == rocksdb_readoptions_get_io_activity(ro));

    request_id = rocksdb_readoptions_get_request_id(ro, &request_id_len);
    CheckCondition(request_id == NULL);
    CheckCondition(request_id_len == 0);
    rocksdb_readoptions_set_request_id(ro, "request-123", 11);
    request_id = rocksdb_readoptions_get_request_id(ro, &request_id_len);
    CheckEqual("request-123", request_id, request_id_len);
    rocksdb_readoptions_clear_request_id(ro);
    request_id = rocksdb_readoptions_get_request_id(ro, &request_id_len);
    CheckCondition(request_id == NULL);
    CheckCondition(request_id_len == 0);

    CheckCondition(0 == rocksdb_readoptions_has_table_filter(ro));
    rocksdb_readoptions_set_table_index_factory_from_string(ro, "trie_index",
                                                            10, &err);
    CheckNoError(err);
    table_index_factory_name = rocksdb_readoptions_get_table_index_factory_name(
        ro, &table_index_factory_name_len);
    CheckEqual("trie_index", table_index_factory_name,
               table_index_factory_name_len);
    rocksdb_readoptions_clear_table_index_factory(ro);
    table_index_factory_name = rocksdb_readoptions_get_table_index_factory_name(
        ro, &table_index_factory_name_len);
    CheckCondition(table_index_factory_name == NULL);
    CheckCondition(table_index_factory_name_len == 0);

    // read_index: set_table_index_factory_from_string implies prefer_custom;
    // clearing it returns to default; the explicit setter reaches builtin,
    // which the legacy factory shim cannot express.
    CheckCondition(rocksdb_readoptions_read_index_default ==
                   rocksdb_readoptions_get_read_index(ro));
    rocksdb_readoptions_set_read_index(ro,
                                       rocksdb_readoptions_read_index_builtin);
    CheckCondition(rocksdb_readoptions_read_index_builtin ==
                   rocksdb_readoptions_get_read_index(ro));
    rocksdb_readoptions_set_table_index_factory_from_string(ro, "trie_index",
                                                            10, &err);
    CheckNoError(err);
    CheckCondition(rocksdb_readoptions_read_index_prefer_custom ==
                   rocksdb_readoptions_get_read_index(ro));
    rocksdb_readoptions_clear_table_index_factory(ro);
    CheckCondition(rocksdb_readoptions_read_index_default ==
                   rocksdb_readoptions_get_read_index(ro));

    // optimize_multiget_for_io defaults to true (1); flip to 0 to verify the
    // setter is wired through to the underlying C++ ReadOptions field.
    CheckCondition(1 == rocksdb_readoptions_get_optimize_multiget_for_io(ro));
    rocksdb_readoptions_set_optimize_multiget_for_io(ro, 0);
    CheckCondition(0 == rocksdb_readoptions_get_optimize_multiget_for_io(ro));

    rocksdb_readoptions_destroy(ro);
  }

  StartPhase("table_filter_listener_metadata");
  {
    char local_dbname[400];
    char local_sstfilename[400];
    const char* file_list[1];
    rocksdb_options_t* local_options = rocksdb_options_create();
    rocksdb_t* local_db;
    rocksdb_writeoptions_t* local_woptions = rocksdb_writeoptions_create();
    rocksdb_readoptions_t* local_roptions = rocksdb_readoptions_create();
    rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();
    rocksdb_wait_for_compact_options_t* wait_options =
        rocksdb_wait_for_compact_options_create();
    rocksdb_envoptions_t* env_options = rocksdb_envoptions_create();
    rocksdb_ingestexternalfileoptions_t* ingest_options =
        rocksdb_ingestexternalfileoptions_create();
    rocksdb_sstfilewriter_t* writer;
    rocksdb_iterator_t* iter;
    char* val;
    size_t val_len;
    char* iter_err = NULL;
    ListenerState listener_state = {0};
    TableFilterState filter_state = {0};

    snprintf(local_dbname, sizeof(local_dbname),
             "%s/rocksdb_c_test-%d-table-filter-listener", GetTempDir(),
             ((int)geteuid()));
    snprintf(local_sstfilename, sizeof(local_sstfilename),
             "%s/rocksdb_c_test-%d-table-filter-listener.sst", GetTempDir(),
             ((int)geteuid()));
    listener_state.expected_external_file_path = local_sstfilename;

    rocksdb_destroy_db(local_options, local_dbname, &err);
    Free(&err);
    remove(local_sstfilename);

    rocksdb_options_set_create_if_missing(local_options, 1);
    rocksdb_options_set_disable_auto_compactions(local_options, 1);
    rocksdb_options_set_compression(local_options, rocksdb_no_compression);
    rocksdb_options_add_eventlistener(
        local_options,
        rocksdb_eventlistener_create(
            &listener_state, ListenerNoopDestructor, ListenerOnFlushBegin,
            ListenerOnFlushCompleted, ListenerOnCompactionBegin,
            ListenerOnCompactionCompleted, ListenerOnSubcompactionBegin,
            ListenerOnSubcompactionCompleted, ListenerOnExternalFileIngested,
            ListenerOnBackgroundError, ListenerOnStallConditionsChanged,
            ListenerOnMemtableSealed));

    local_db = rocksdb_open(local_options, local_dbname, &err);
    CheckNoError(err);

    rocksdb_flushoptions_set_wait(flush_options, 1);
    // Ensure the OnFlushCompleted callbacks (which update listener_state) have
    // finished by the time rocksdb_flush() returns, so the flush_count /
    // saw_flush_entries_* assertions below are not racy.
    rocksdb_flushoptions_set_listener_wait(flush_options, 1);
    CheckCondition(1 == rocksdb_flushoptions_get_listener_wait(flush_options));

    rocksdb_put(local_db, local_woptions, "a", 1, "1", 1, &err);
    CheckNoError(err);
    rocksdb_flush(local_db, flush_options, &err);
    CheckNoError(err);

    rocksdb_put(local_db, local_woptions, "b", 1, "2", 1, &err);
    CheckNoError(err);
    rocksdb_put(local_db, local_woptions, "c", 1, "3", 1, &err);
    CheckNoError(err);
    rocksdb_flush(local_db, flush_options, &err);
    CheckNoError(err);

    rocksdb_put(local_db, local_woptions, "d", 1, "4", 1, &err);
    CheckNoError(err);
    rocksdb_put(local_db, local_woptions, "e", 1, "5", 1, &err);
    CheckNoError(err);
    rocksdb_put(local_db, local_woptions, "f", 1, "6", 1, &err);
    CheckNoError(err);
    rocksdb_flush(local_db, flush_options, &err);
    CheckNoError(err);

    CheckCondition(listener_state.flush_count == 3);
    CheckCondition(listener_state.saw_flush_entries_1);
    CheckCondition(listener_state.saw_flush_entries_2);
    CheckCondition(listener_state.saw_flush_entries_3);

    filter_state.hidden_num_entries = 2;
    CheckCondition(0 == rocksdb_readoptions_has_table_filter(local_roptions));
    rocksdb_readoptions_set_table_filter(local_roptions, &filter_state,
                                         TableFilterStateDestroy,
                                         TableFilterCallback);
    CheckCondition(1 == rocksdb_readoptions_has_table_filter(local_roptions));

    iter = rocksdb_create_iterator(local_db, local_roptions);
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "a", "1");
    rocksdb_iter_next(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "d", "4");
    rocksdb_iter_next(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "e", "5");
    rocksdb_iter_next(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "f", "6");
    rocksdb_iter_next(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_get_error(iter, &iter_err);
    CheckNoError(iter_err);
    rocksdb_iter_destroy(iter);

    CheckCondition(filter_state.calls == 3);
    CheckCondition(filter_state.saw_entries_1);
    CheckCondition(filter_state.saw_entries_2);
    CheckCondition(filter_state.saw_entries_3);

    rocksdb_readoptions_clear_table_filter(local_roptions);
    CheckCondition(0 == rocksdb_readoptions_has_table_filter(local_roptions));
    CheckCondition(filter_state.destructor_calls == 1);
    rocksdb_readoptions_set_table_filter(local_roptions, &filter_state,
                                         TableFilterStateDestroy,
                                         TableFilterCallback);

    rocksdb_compact_range(local_db, NULL, 0, NULL, 0);
    rocksdb_wait_for_compact(local_db, wait_options, &err);
    CheckNoError(err);
    CheckCondition(listener_state.compaction_count >= 1);

    writer = rocksdb_sstfilewriter_create(env_options, local_options);
    rocksdb_sstfilewriter_open(writer, local_sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "g", 1, "7", 1, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "h", 1, "8", 1, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    file_list[0] = local_sstfilename;
    rocksdb_ingest_external_file(local_db, file_list, 1, ingest_options, &err);
    CheckNoError(err);
    CheckCondition(listener_state.external_ingest_count == 1);

    val = rocksdb_get(local_db, local_roptions, "g", 1, &val_len, &err);
    CheckNoError(err);
    CheckEqual("7", val, val_len);
    Free(&val);

    rocksdb_sstfilewriter_destroy(writer);
    rocksdb_ingestexternalfileoptions_destroy(ingest_options);
    rocksdb_envoptions_destroy(env_options);
    rocksdb_wait_for_compact_options_destroy(wait_options);
    rocksdb_flushoptions_destroy(flush_options);
    rocksdb_writeoptions_destroy(local_woptions);
    rocksdb_readoptions_destroy(local_roptions);
    CheckCondition(filter_state.destructor_calls == 2);
    rocksdb_close(local_db);
    rocksdb_destroy_db(local_options, local_dbname, &err);
    CheckNoError(err);
    rocksdb_options_destroy(local_options);
    remove(local_sstfilename);
  }

  StartPhase("wal_filter");
  {
    char wal_dbname[400];
    rocksdb_options_t* wal_options = rocksdb_options_create();
    rocksdb_writeoptions_t* wal_woptions = rocksdb_writeoptions_create();
    rocksdb_readoptions_t* wal_roptions = rocksdb_readoptions_create();
    rocksdb_t* wal_db;
    rocksdb_walfilter_t* wal_filter;
    WalFilterState wal_filter_state = {0};
    char* val;
    size_t val_len;

    snprintf(wal_dbname, sizeof(wal_dbname), "%s/rocksdb_c_test-%d-wal-filter",
             GetTempDir(), ((int)geteuid()));
    rocksdb_destroy_db(wal_options, wal_dbname, &err);
    Free(&err);

    rocksdb_options_set_create_if_missing(wal_options, 1);
    rocksdb_options_set_compression(wal_options, rocksdb_no_compression);
    rocksdb_writeoptions_set_sync(wal_woptions, 0);

    wal_db = rocksdb_open(wal_options, wal_dbname, &err);
    CheckNoError(err);
    rocksdb_put(wal_db, wal_woptions, "k1", 2, "v1", 2, &err);
    CheckNoError(err);
    rocksdb_put(wal_db, wal_woptions, "k2", 2, "v2", 2, &err);
    CheckNoError(err);
    rocksdb_close(wal_db);

    wal_filter = rocksdb_walfilter_create(
        &wal_filter_state, WalFilterDestroy, WalFilterColumnFamilyLogNumberMap,
        WalFilterLogRecordFound, WalFilterName);
    rocksdb_options_set_wal_filter(wal_options, wal_filter);

    wal_db = rocksdb_open(wal_options, wal_dbname, &err);
    CheckNoError(err);
    CheckCondition(wal_filter_state.column_family_map_calls > 0);
    CheckCondition(wal_filter_state.log_record_calls == 2);

    val = rocksdb_get(wal_db, wal_roptions, "k1", 2, &val_len, &err);
    CheckNoError(err);
    CheckEqual(NULL, val, val_len);
    Free(&val);

    val = rocksdb_get(wal_db, wal_roptions, "k2", 2, &val_len, &err);
    CheckNoError(err);
    CheckEqual("v2", val, val_len);
    Free(&val);

    rocksdb_close(wal_db);
    rocksdb_options_clear_wal_filter(wal_options);
    rocksdb_walfilter_destroy(wal_filter);
    CheckCondition(wal_filter_state.destructor_calls == 1);
    rocksdb_readoptions_destroy(wal_roptions);
    rocksdb_writeoptions_destroy(wal_woptions);
    rocksdb_destroy_db(wal_options, wal_dbname, &err);
    CheckNoError(err);
    rocksdb_options_destroy(wal_options);
  }

  StartPhase("write_options");
  {
    rocksdb_writeoptions_t* wo;
    wo = rocksdb_writeoptions_create();

    rocksdb_writeoptions_set_sync(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_sync(wo));

    rocksdb_writeoptions_disable_WAL(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_disable_WAL(wo));

    rocksdb_writeoptions_set_ignore_missing_column_families(wo, 1);
    CheckCondition(1 ==
                   rocksdb_writeoptions_get_ignore_missing_column_families(wo));

    rocksdb_writeoptions_set_no_slowdown(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_no_slowdown(wo));

    rocksdb_writeoptions_set_low_pri(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_low_pri(wo));

    rocksdb_writeoptions_set_memtable_insert_hint_per_batch(wo, 1);
    CheckCondition(1 ==
                   rocksdb_writeoptions_get_memtable_insert_hint_per_batch(wo));

    rocksdb_writeoptions_set_rate_limiter_priority(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_rate_limiter_priority(wo));

    rocksdb_writeoptions_set_protection_bytes_per_key(wo, 8);
    CheckCondition(8 == rocksdb_writeoptions_get_protection_bytes_per_key(wo));

    rocksdb_writeoptions_set_io_activity(wo, 2);
    CheckCondition(2 == rocksdb_writeoptions_get_io_activity(wo));

    rocksdb_writeoptions_destroy(wo);
  }

  StartPhase("compact_options");
  {
    rocksdb_compactoptions_t* co;
    co = rocksdb_compactoptions_create();

    rocksdb_compactoptions_set_exclusive_manual_compaction(co, 1);
    CheckCondition(1 ==
                   rocksdb_compactoptions_get_exclusive_manual_compaction(co));

    rocksdb_compactoptions_set_bottommost_level_compaction(co, 1);
    CheckCondition(1 ==
                   rocksdb_compactoptions_get_bottommost_level_compaction(co));

    rocksdb_compactoptions_set_change_level(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_change_level(co));

    rocksdb_compactoptions_set_target_level(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_target_level(co));

    rocksdb_compactoptions_set_target_path_id(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_target_path_id(co));

    rocksdb_compactoptions_set_allow_write_stall(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_allow_write_stall(co));

    rocksdb_compactoptions_set_max_subcompactions(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_max_subcompactions(co));

    rocksdb_compactoptions_set_blob_garbage_collection_policy(co, 1);
    CheckCondition(
        1 == rocksdb_compactoptions_get_blob_garbage_collection_policy(co));

    rocksdb_compactoptions_set_blob_garbage_collection_age_cutoff(co, 0.75);
    CheckCondition(
        0.75 ==
        rocksdb_compactoptions_get_blob_garbage_collection_age_cutoff(co));

    rocksdb_compactoptions_destroy(co);
  }

  StartPhase("flush_options");
  {
    rocksdb_flushoptions_t* fo;
    fo = rocksdb_flushoptions_create();

    rocksdb_flushoptions_set_wait(fo, 1);
    CheckCondition(1 == rocksdb_flushoptions_get_wait(fo));

    rocksdb_flushoptions_set_allow_write_stall(fo, 1);
    CheckCondition(1 == rocksdb_flushoptions_get_allow_write_stall(fo));

    rocksdb_flushoptions_set_force_atomic_flush(fo, 1);
    CheckCondition(1 == rocksdb_flushoptions_get_force_atomic_flush(fo));

    rocksdb_flushoptions_destroy(fo);
  }

  StartPhase("ingest_external_file_options");
  {
    rocksdb_ingestexternalfileoptions_t* io;
    io = rocksdb_ingestexternalfileoptions_create();

    rocksdb_ingestexternalfileoptions_set_move_files(io, 1);
    CheckCondition(1 == rocksdb_ingestexternalfileoptions_get_move_files(io));

    rocksdb_ingestexternalfileoptions_set_link_files(io, 1);
    CheckCondition(1 == rocksdb_ingestexternalfileoptions_get_link_files(io));

    rocksdb_ingestexternalfileoptions_set_failed_move_fall_back_to_copy(io, 0);
    CheckCondition(
        0 ==
        rocksdb_ingestexternalfileoptions_get_failed_move_fall_back_to_copy(
            io));

    rocksdb_ingestexternalfileoptions_set_snapshot_consistency(io, 0);
    CheckCondition(
        0 == rocksdb_ingestexternalfileoptions_get_snapshot_consistency(io));

    rocksdb_ingestexternalfileoptions_set_verify_checksums_before_ingest(io, 1);
    CheckCondition(
        1 ==
        rocksdb_ingestexternalfileoptions_get_verify_checksums_before_ingest(
            io));

    rocksdb_ingestexternalfileoptions_set_verify_checksums_readahead_size(io,
                                                                          1024);
    CheckCondition(
        1024 ==
        rocksdb_ingestexternalfileoptions_get_verify_checksums_readahead_size(
            io));

    rocksdb_ingestexternalfileoptions_set_verify_file_checksum(io, 0);
    CheckCondition(
        0 == rocksdb_ingestexternalfileoptions_get_verify_file_checksum(io));

    rocksdb_ingestexternalfileoptions_set_allow_db_generated_files(io, 1);
    CheckCondition(
        1 ==
        rocksdb_ingestexternalfileoptions_get_allow_db_generated_files(io));

    rocksdb_ingestexternalfileoptions_set_fill_cache(io, 0);
    CheckCondition(0 == rocksdb_ingestexternalfileoptions_get_fill_cache(io));

    rocksdb_ingestexternalfileoptions_destroy(io);
  }

  StartPhase("cache_options");
  {
    rocksdb_cache_t* co;
    co = rocksdb_cache_create_lru(100);
    CheckCondition(100 == rocksdb_cache_get_capacity(co));

    rocksdb_cache_set_capacity(co, 200);
    CheckCondition(200 == rocksdb_cache_get_capacity(co));

    rocksdb_cache_destroy(co);
  }

  StartPhase("jemalloc_nodump_allocator");
  {
    rocksdb_memory_allocator_t* allocator;
    allocator = rocksdb_jemalloc_nodump_allocator_create(&err);
    if (err != NULL) {
      // not supported on all platforms, allow unsupported error
      const char* ni = "Not implemented: ";
      size_t ni_len = strlen(ni);
      size_t err_len = strlen(err);

      CheckCondition(err_len >= ni_len);
      CheckCondition(memcmp(ni, err, ni_len) == 0);
      Free(&err);
    } else {
      rocksdb_cache_t* co;
      rocksdb_lru_cache_options_t* copts;

      copts = rocksdb_lru_cache_options_create();

      rocksdb_lru_cache_options_set_capacity(copts, 100);
      rocksdb_lru_cache_options_set_memory_allocator(copts, allocator);

      co = rocksdb_cache_create_lru_opts(copts);
      CheckCondition(100 == rocksdb_cache_get_capacity(co));

      rocksdb_cache_destroy(co);
      rocksdb_lru_cache_options_destroy(copts);
    }
    rocksdb_memory_allocator_destroy(allocator);
  }

  StartPhase("stderr_logger");
  {
    rocksdb_options_t* o_no_prefix = rocksdb_options_create();
    rocksdb_logger_t* no_prefix_logger =
        rocksdb_logger_create_stderr_logger(3, NULL);
    rocksdb_options_set_info_log(o_no_prefix, no_prefix_logger);
    rocksdb_logger_t* no_prefix_info_log =
        rocksdb_options_get_info_log(o_no_prefix);
    CheckCondition(no_prefix_info_log != NULL);
    rocksdb_logger_destroy(no_prefix_logger);
    rocksdb_logger_destroy(no_prefix_info_log);
    rocksdb_options_destroy(o_no_prefix);

    rocksdb_options_t* o_prefix = rocksdb_options_create();
    rocksdb_logger_t* prefix_logger =
        rocksdb_logger_create_stderr_logger(3, "some prefix");
    rocksdb_options_set_info_log(o_prefix, prefix_logger);
    rocksdb_logger_t* prefix_info_log = rocksdb_options_get_info_log(o_prefix);
    CheckCondition(prefix_info_log != NULL);
    rocksdb_logger_destroy(prefix_logger);
    rocksdb_logger_destroy(prefix_info_log);
    rocksdb_options_destroy(o_prefix);
  }

  StartPhase("env");
  {
    rocksdb_env_t* e;
    e = rocksdb_create_default_env();

    rocksdb_env_set_background_threads(e, 10);
    CheckCondition(10 == rocksdb_env_get_background_threads(e));

    rocksdb_env_set_high_priority_background_threads(e, 20);
    CheckCondition(20 == rocksdb_env_get_high_priority_background_threads(e));

    rocksdb_env_set_low_priority_background_threads(e, 30);
    CheckCondition(30 == rocksdb_env_get_low_priority_background_threads(e));

    rocksdb_env_set_bottom_priority_background_threads(e, 40);
    CheckCondition(40 == rocksdb_env_get_bottom_priority_background_threads(e));

    rocksdb_env_destroy(e);
  }

  StartPhase("universal_compaction_options");
  {
    rocksdb_universal_compaction_options_t* uco;
    uco = rocksdb_universal_compaction_options_create();

    rocksdb_universal_compaction_options_set_size_ratio(uco, 5);
    CheckCondition(5 ==
                   rocksdb_universal_compaction_options_get_size_ratio(uco));

    rocksdb_universal_compaction_options_set_min_merge_width(uco, 15);
    CheckCondition(
        15 == rocksdb_universal_compaction_options_get_min_merge_width(uco));

    rocksdb_universal_compaction_options_set_max_merge_width(uco, 25);
    CheckCondition(
        25 == rocksdb_universal_compaction_options_get_max_merge_width(uco));

    rocksdb_universal_compaction_options_set_max_size_amplification_percent(uco,
                                                                            35);
    CheckCondition(
        35 ==
        rocksdb_universal_compaction_options_get_max_size_amplification_percent(
            uco));

    rocksdb_universal_compaction_options_set_compression_size_percent(uco, 45);
    CheckCondition(
        45 ==
        rocksdb_universal_compaction_options_get_compression_size_percent(uco));

    rocksdb_universal_compaction_options_set_stop_style(uco, 1);
    CheckCondition(1 ==
                   rocksdb_universal_compaction_options_get_stop_style(uco));

    rocksdb_universal_compaction_options_set_max_read_amp(uco, 2);
    CheckCondition(2 ==
                   rocksdb_universal_compaction_options_get_max_read_amp(uco));

    rocksdb_universal_compaction_options_set_allow_trivial_move(uco, 1);
    CheckCondition(
        1 == rocksdb_universal_compaction_options_get_allow_trivial_move(uco));

    rocksdb_universal_compaction_options_set_incremental(uco, 1);
    CheckCondition(1 ==
                   rocksdb_universal_compaction_options_get_incremental(uco));

    rocksdb_universal_compaction_options_set_reduce_file_locking(uco, 1);
    CheckCondition(
        1 == rocksdb_universal_compaction_options_get_reduce_file_locking(uco));

    rocksdb_universal_compaction_options_destroy(uco);
  }

  StartPhase("fifo_compaction_options");
  {
    rocksdb_fifo_compaction_options_t* fco;
    fco = rocksdb_fifo_compaction_options_create();

    rocksdb_fifo_compaction_options_set_max_table_files_size(fco, 100000);
    CheckCondition(
        100000 ==
        rocksdb_fifo_compaction_options_get_max_table_files_size(fco));

    rocksdb_fifo_compaction_options_set_max_data_files_size(fco, 200000);
    CheckCondition(
        200000 == rocksdb_fifo_compaction_options_get_max_data_files_size(fco));

    rocksdb_fifo_compaction_options_set_use_kv_ratio_compaction(fco, 1);
    CheckCondition(
        1 == rocksdb_fifo_compaction_options_get_use_kv_ratio_compaction(fco));

    rocksdb_fifo_compaction_options_set_age_for_warm(fco, 300);
    CheckCondition(300 ==
                   rocksdb_fifo_compaction_options_get_age_for_warm(fco));

    rocksdb_fifo_compaction_options_set_allow_trivial_copy_when_change_temperature(
        fco, 1);
    CheckCondition(
        1 ==
        rocksdb_fifo_compaction_options_get_allow_trivial_copy_when_change_temperature(
            fco));

    rocksdb_fifo_compaction_options_set_trivial_copy_buffer_size(fco, 8192);
    CheckCondition(
        8192 ==
        rocksdb_fifo_compaction_options_get_trivial_copy_buffer_size(fco));

    rocksdb_fifo_compaction_options_destroy(fco);
  }

  StartPhase("backup_engine_option");
  {
    rocksdb_backup_engine_options_t* bdo;
    const char* str;
    size_t len;
    bdo = rocksdb_backup_engine_options_create("path");

    str = rocksdb_backup_engine_options_get_backup_dir(bdo, &len);
    CheckEqual("path", str, len);

    rocksdb_backup_engine_options_set_share_table_files(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backup_engine_options_get_share_table_files(bdo));

    rocksdb_backup_engine_options_set_sync(bdo, 1);
    CheckCondition(1 == rocksdb_backup_engine_options_get_sync(bdo));

    rocksdb_backup_engine_options_set_destroy_old_data(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backup_engine_options_get_destroy_old_data(bdo));

    rocksdb_backup_engine_options_set_backup_log_files(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backup_engine_options_get_backup_log_files(bdo));

    rocksdb_backup_engine_options_set_backup_rate_limit(bdo, 123);
    CheckCondition(123 ==
                   rocksdb_backup_engine_options_get_backup_rate_limit(bdo));

    rocksdb_backup_engine_options_set_restore_rate_limit(bdo, 37);
    CheckCondition(37 ==
                   rocksdb_backup_engine_options_get_restore_rate_limit(bdo));

    {
      rocksdb_ratelimiter_t* limiter = rocksdb_ratelimiter_create_with_mode(
          1024 * 1024, 100 * 1000, 10, 2, 0);
      rocksdb_backup_engine_options_set_backup_rate_limiter(bdo, limiter);
      rocksdb_backup_engine_options_set_restore_rate_limiter(bdo, limiter);
      rocksdb_ratelimiter_destroy(limiter);

      rocksdb_backup_engine_options_t* rate_beo =
          rocksdb_backup_engine_options_create(dbbackupname);
      rocksdb_ratelimiter_t* backup_limiter =
          rocksdb_ratelimiter_create_with_mode(1024 * 1024, 100 * 1000, 10, 2,
                                               0);
      rocksdb_ratelimiter_t* restore_limiter =
          rocksdb_ratelimiter_create_with_mode(1024 * 1024, 100 * 1000, 10, 2,
                                               0);
      rocksdb_backup_engine_options_set_backup_rate_limiter(rate_beo,
                                                            backup_limiter);
      rocksdb_backup_engine_options_set_restore_rate_limiter(rate_beo,
                                                             restore_limiter);
      rocksdb_ratelimiter_destroy(backup_limiter);
      rocksdb_ratelimiter_destroy(restore_limiter);
      rocksdb_env_t* rate_benv = rocksdb_create_default_env();
      rocksdb_backup_engine_t* rate_be =
          rocksdb_backup_engine_open_opts(rate_beo, rate_benv, &err);
      rocksdb_backup_engine_options_destroy(rate_beo);
      rocksdb_env_destroy(rate_benv);
      CheckNoError(err);
      rocksdb_backup_engine_create_new_backup(rate_be, db, &err);
      CheckNoError(err);
      {
        char rate_restore_path[220];
        snprintf(rate_restore_path, sizeof(rate_restore_path),
                 "%s.rate_restore", dbname);
        rocksdb_restore_options_t* rate_restore_opts =
            rocksdb_restore_options_create();
        rocksdb_backup_engine_restore_db_from_latest_backup(
            rate_be, rate_restore_path, rate_restore_path, rate_restore_opts,
            &err);
        CheckNoError(err);
        rocksdb_restore_options_destroy(rate_restore_opts);
        rocksdb_destroy_db(options, rate_restore_path, &err);
        err = NULL;
      }
      rocksdb_backup_engine_close(rate_be);
    }

    rocksdb_backup_engine_options_set_max_background_operations(bdo, 20);
    CheckCondition(
        20 == rocksdb_backup_engine_options_get_max_background_operations(bdo));

    rocksdb_backup_engine_options_set_callback_trigger_interval_size(bdo, 9000);
    CheckCondition(
        9000 ==
        rocksdb_backup_engine_options_get_callback_trigger_interval_size(bdo));

    rocksdb_backup_engine_options_set_max_valid_backups_to_open(bdo, 40);
    CheckCondition(
        40 == rocksdb_backup_engine_options_get_max_valid_backups_to_open(bdo));

    rocksdb_backup_engine_options_set_share_files_with_checksum_naming(bdo, 2);
    CheckCondition(
        2 == rocksdb_backup_engine_options_get_share_files_with_checksum_naming(
                 bdo));

    rocksdb_backup_engine_options_set_io_buffer_size(bdo, 8192);
    CheckCondition(8192 ==
                   rocksdb_backup_engine_options_get_io_buffer_size(bdo));

    rocksdb_backup_engine_options_set_share_files_with_checksum(bdo, 1);
    CheckCondition(
        1 == rocksdb_backup_engine_options_get_share_files_with_checksum(bdo));

    rocksdb_backup_engine_options_set_schema_version(bdo, 2);
    CheckCondition(2 == rocksdb_backup_engine_options_get_schema_version(bdo));

    rocksdb_backup_engine_options_set_current_temperatures_override_manifest(
        bdo, 1);
    CheckCondition(
        1 ==
        rocksdb_backup_engine_options_get_current_temperatures_override_manifest(
            bdo));

    rocksdb_backup_engine_options_destroy(bdo);
  }

  StartPhase("restore_options");
  {
    rocksdb_restore_options_t* ro;
    ro = rocksdb_restore_options_create();

    rocksdb_restore_options_set_keep_log_files(ro, 1);
    CheckCondition(1 == rocksdb_restore_options_get_keep_log_files(ro));

    rocksdb_restore_options_set_mode(ro, 2);
    CheckCondition(2 == rocksdb_restore_options_get_mode(ro));

    rocksdb_restore_options_destroy(ro);
  }

  StartPhase("transaction_options_roundtrip");
  {
    rocksdb_transactiondb_options_t* tdb;
    rocksdb_transaction_options_t* to;
    rocksdb_optimistictransaction_options_t* oto;

    tdb = rocksdb_transactiondb_options_create();
    rocksdb_transactiondb_options_set_max_num_locks(tdb, 99);
    CheckCondition(99 == rocksdb_transactiondb_options_get_max_num_locks(tdb));
    rocksdb_transactiondb_options_set_num_stripes(tdb, 10);
    CheckCondition(10 == rocksdb_transactiondb_options_get_num_stripes(tdb));
    rocksdb_transactiondb_options_set_transaction_lock_timeout(tdb, 11);
    CheckCondition(
        11 == rocksdb_transactiondb_options_get_transaction_lock_timeout(tdb));
    rocksdb_transactiondb_options_set_default_lock_timeout(tdb, 12);
    CheckCondition(12 ==
                   rocksdb_transactiondb_options_get_default_lock_timeout(tdb));
    rocksdb_transactiondb_options_set_max_num_deadlocks(tdb, 13);
    CheckCondition(13 ==
                   rocksdb_transactiondb_options_get_max_num_deadlocks(tdb));
    rocksdb_transactiondb_options_set_write_policy(tdb, 2);
    CheckCondition(2 == rocksdb_transactiondb_options_get_write_policy(tdb));
    rocksdb_transactiondb_options_set_skip_concurrency_control(tdb, 1);
    CheckCondition(
        1 == rocksdb_transactiondb_options_get_skip_concurrency_control(tdb));
    rocksdb_transactiondb_options_set_default_write_batch_flush_threshold(tdb,
                                                                          14);
    CheckCondition(
        14 ==
        rocksdb_transactiondb_options_get_default_write_batch_flush_threshold(
            tdb));
    rocksdb_transactiondb_options_set_enable_udt_validation(tdb, 0);
    CheckCondition(
        0 == rocksdb_transactiondb_options_get_enable_udt_validation(tdb));
    rocksdb_transactiondb_options_set_txn_commit_bypass_memtable_threshold(tdb,
                                                                           15);
    CheckCondition(
        15 ==
        rocksdb_transactiondb_options_get_txn_commit_bypass_memtable_threshold(
            tdb));
    rocksdb_transactiondb_options_destroy(tdb);

    to = rocksdb_transaction_options_create();
    rocksdb_transaction_options_set_set_snapshot(to, 1);
    CheckCondition(1 == rocksdb_transaction_options_get_set_snapshot(to));
    rocksdb_transaction_options_set_deadlock_detect(to, 1);
    CheckCondition(1 == rocksdb_transaction_options_get_deadlock_detect(to));
    rocksdb_transaction_options_set_use_only_the_last_commit_time_batch_for_recovery(
        to, 1);
    CheckCondition(
        1 ==
        rocksdb_transaction_options_get_use_only_the_last_commit_time_batch_for_recovery(
            to));
    rocksdb_transaction_options_set_lock_timeout(to, 16);
    CheckCondition(16 == rocksdb_transaction_options_get_lock_timeout(to));
    rocksdb_transaction_options_set_deadlock_timeout_us(to, 17);
    CheckCondition(17 ==
                   rocksdb_transaction_options_get_deadlock_timeout_us(to));
    rocksdb_transaction_options_set_expiration(to, 18);
    CheckCondition(18 == rocksdb_transaction_options_get_expiration(to));
    rocksdb_transaction_options_set_deadlock_detect_depth(to, 19);
    CheckCondition(19 ==
                   rocksdb_transaction_options_get_deadlock_detect_depth(to));
    rocksdb_transaction_options_set_max_write_batch_size(to, 20);
    CheckCondition(20 ==
                   rocksdb_transaction_options_get_max_write_batch_size(to));
    rocksdb_transaction_options_set_skip_concurrency_control(to, 1);
    CheckCondition(
        1 == rocksdb_transaction_options_get_skip_concurrency_control(to));
    rocksdb_transaction_options_set_skip_prepare(to, 0);
    CheckCondition(0 == rocksdb_transaction_options_get_skip_prepare(to));
    rocksdb_transaction_options_set_write_batch_flush_threshold(to, 21);
    CheckCondition(
        21 == rocksdb_transaction_options_get_write_batch_flush_threshold(to));
    rocksdb_transaction_options_set_write_batch_track_timestamp_size(to, 1);
    CheckCondition(
        1 ==
        rocksdb_transaction_options_get_write_batch_track_timestamp_size(to));
    rocksdb_transaction_options_set_commit_bypass_memtable(to, 1);
    CheckCondition(1 ==
                   rocksdb_transaction_options_get_commit_bypass_memtable(to));
    rocksdb_transaction_options_set_large_txn_commit_optimize_threshold(to, 22);
    CheckCondition(
        22 ==
        rocksdb_transaction_options_get_large_txn_commit_optimize_threshold(
            to));
    rocksdb_transaction_options_set_large_txn_commit_optimize_byte_threshold(
        to, 23);
    CheckCondition(
        23 ==
        rocksdb_transaction_options_get_large_txn_commit_optimize_byte_threshold(
            to));
    rocksdb_transaction_options_destroy(to);

    oto = rocksdb_optimistictransaction_options_create();
    rocksdb_optimistictransaction_options_set_set_snapshot(oto, 1);
    CheckCondition(1 ==
                   rocksdb_optimistictransaction_options_get_set_snapshot(oto));
    rocksdb_optimistictransaction_options_destroy(oto);
  }

  StartPhase("compression_options");
  {
    rocksdb_options_t* co;
    co = rocksdb_options_create();

    rocksdb_options_set_compression_options_zstd_max_train_bytes(co, 100);
    CheckCondition(
        100 ==
        rocksdb_options_get_compression_options_zstd_max_train_bytes(co));

    rocksdb_options_set_compression_options_parallel_threads(co, 2);
    CheckCondition(
        2 == rocksdb_options_get_compression_options_parallel_threads(co));

    rocksdb_options_set_compression_options_max_dict_buffer_bytes(co, 200);
    CheckCondition(
        200 ==
        rocksdb_options_get_compression_options_max_dict_buffer_bytes(co));

    rocksdb_options_set_compression_options_use_zstd_dict_trainer(co, 0);
    CheckCondition(
        0 == rocksdb_options_get_compression_options_use_zstd_dict_trainer(co));
    rocksdb_options_destroy(co);
  }

  StartPhase("iterate_upper_bound");
  {
    // Create new empty database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_set_prefix_extractor(options, NULL);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_put(db, woptions, "a", 1, "0", 1, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo", 3, "bar", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo1", 4, "bar1", 4, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "g1", 2, "0", 1, &err);
    CheckNoError(err);

    // testing basic case with no iterate_upper_bound and no prefix_extractor
    {
      rocksdb_readoptions_set_iterate_upper_bound(roptions, NULL, 0);
      rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);

      rocksdb_iter_seek(iter, "foo", 3);
      CheckCondition(rocksdb_iter_valid(iter));
      CheckIter(iter, "foo", "bar");

      rocksdb_iter_next(iter);
      CheckCondition(rocksdb_iter_valid(iter));
      CheckIter(iter, "foo1", "bar1");

      rocksdb_iter_next(iter);
      CheckCondition(rocksdb_iter_valid(iter));
      CheckIter(iter, "g1", "0");

      rocksdb_iter_destroy(iter);
    }

    // testing iterate_upper_bound and forward iterator
    // to make sure it stops at bound
    {
      // iterate_upper_bound points beyond the last expected entry
      rocksdb_readoptions_set_iterate_upper_bound(roptions, "foo2", 4);

      rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);

      rocksdb_iter_seek(iter, "foo", 3);
      CheckCondition(rocksdb_iter_valid(iter));
      CheckIter(iter, "foo", "bar");

      rocksdb_iter_next(iter);
      CheckCondition(rocksdb_iter_valid(iter));
      CheckIter(iter, "foo1", "bar1");

      rocksdb_iter_next(iter);
      // should stop here...
      CheckCondition(!rocksdb_iter_valid(iter));

      rocksdb_iter_destroy(iter);
      rocksdb_readoptions_set_iterate_upper_bound(roptions, NULL, 0);
    }
  }

  StartPhase("transactiondb_set_write_policy_prepared");
  {
    char wp_dbname[200];
    rocksdb_transactiondb_t* wp_txn_db;
    rocksdb_transactiondb_options_t* wp_txn_opts;
    snprintf(wp_dbname, sizeof(wp_dbname), "%s/rocksdb_c_test-txnwp-%" PRIu64,
             GetTempDir(), GetTestId());
    rocksdb_destroy_db(options, wp_dbname, &err);
    Free(&err);
    wp_txn_opts = rocksdb_transactiondb_options_create();
    rocksdb_transactiondb_options_set_write_policy(
        wp_txn_opts, rocksdb_txndb_write_policy_write_prepared);
    rocksdb_options_set_create_if_missing(options, 1);
    // create new TransactionDB with write policy WRITE_PREPARED
    wp_txn_db =
        rocksdb_transactiondb_open(options, wp_txn_opts, wp_dbname, &err);
    CheckNoError(err);
    {
      rocksdb_transaction_options_t* wp_txn_options =
          rocksdb_transaction_options_create();
      rocksdb_transaction_t* wp_txn =
          rocksdb_transaction_begin(wp_txn_db, woptions, wp_txn_options, NULL);
      // write one record within a transaction
      rocksdb_transaction_put(wp_txn, "k", 1, "v", 1, &err);
      CheckNoError(err);
      rocksdb_transaction_commit(wp_txn, &err);
      CheckNoError(err);
      rocksdb_transaction_destroy(wp_txn);
      rocksdb_transaction_options_destroy(wp_txn_options);
    }
    CheckTxnDBGet(wp_txn_db, roptions, "k", "v");
    rocksdb_transactiondb_close(wp_txn_db);
    rocksdb_transactiondb_options_destroy(wp_txn_opts);
    rocksdb_destroy_db(options, wp_dbname, &err);
    CheckNoError(err);
  }

  StartPhase("transactiondb_batched_multi_get");
  TestTxnDBBatchedMultiGet();

  StartPhase("transactions");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    // open a TransactionDB
    txn_db_options = rocksdb_transactiondb_options_create();
    txn_options = rocksdb_transaction_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    txn_db = rocksdb_transactiondb_open(options, txn_db_options, dbname, &err);
    CheckNoError(err);

    // put outside a transaction
    rocksdb_transactiondb_put(txn_db, woptions, "foo", 3, "hello", 5, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo", "hello");
    CheckTxnDBPinGet(txn_db, roptions, "foo", "hello");

    // delete from outside transaction
    rocksdb_transactiondb_delete(txn_db, woptions, "foo", 3, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "foo", NULL);

    // write batch into TransactionDB
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_put(wb, "foo", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "box", 3, "c", 1);
    rocksdb_writebatch_delete(wb, "bar", 3);
    rocksdb_transactiondb_write(txn_db, woptions, wb, &err);
    rocksdb_writebatch_destroy(wb);
    CheckTxnDBGet(txn_db, roptions, "box", "c");
    CheckTxnDBPinGet(txn_db, roptions, "box", "c");
    CheckNoError(err);

    // multi get
    {
      const char* keys[3] = {"box", "foo", "notfound"};
      const size_t keys_sizes[3] = {3, 3, 8};
      char* vals[3];
      size_t vals_sizes[3];
      char* errs[3];
      const char* expected[3] = {"c", NULL, NULL};
      rocksdb_transactiondb_multi_get(txn_db, roptions, 3, keys, keys_sizes,
                                      vals, vals_sizes, errs);
      CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
    }

    // begin a transaction
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    // put
    rocksdb_transaction_put(txn, "foo", 3, "hello", 5, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo", "hello");
    CheckTxnPinGet(txn, roptions, "foo", "hello");
    {
      const char* keys[3] = {"box", "foo", "notfound"};
      const size_t keys_sizes[3] = {3, 3, 8};
      char* vals[3];
      size_t vals_sizes[3];
      char* errs[3];
      const char* expected[3] = {"c", "hello", NULL};
      rocksdb_transaction_multi_get(txn, roptions, 3, keys, keys_sizes, vals,
                                    vals_sizes, errs);
      CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
    }
    // delete
    rocksdb_transaction_delete(txn, "foo", 3, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo", NULL);
    CheckTxnPinGet(txn, roptions, "foo", NULL);

    rocksdb_transaction_put(txn, "foo", 3, "hello", 5, &err);
    CheckNoError(err);

    // test transaction get/set name (before commit)
    {
      rocksdb_transaction_set_name(txn, "test_txn", 8, &err);
      CheckNoError(err);
      size_t name_len;
      char* txn_name = rocksdb_transaction_get_name(txn, &name_len);
      CheckCondition(name_len == 8);
      CheckCondition(memcmp(txn_name, "test_txn", 8) == 0);
      rocksdb_free(txn_name);
    }

    // read from outside transaction, before commit
    CheckTxnDBGet(txn_db, roptions, "foo", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "foo", NULL);
    {
      const char* keys[3] = {"box", "foo", "notfound"};
      const size_t keys_sizes[3] = {3, 3, 8};
      char* vals[3];
      size_t vals_sizes[3];
      char* errs[3];
      const char* expected[3] = {"c", NULL, NULL};
      rocksdb_transactiondb_multi_get(txn_db, roptions, 3, keys, keys_sizes,
                                      vals, vals_sizes, errs);
      CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
    }

    rocksdb_transaction_put_log_data(txn, "log_blob", 8);
    // record sequence number before commit so we can scan the WAL after
    rocksdb_t* base_db_ld = rocksdb_transactiondb_get_base_db(txn_db);
    uint64_t seq_before = rocksdb_get_latest_sequence_number(base_db_ld);

    // commit
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);

    // verify log data was written to WAL by scanning batches since seq_before
    {
      rocksdb_wal_iterator_t* wal_iter =
          rocksdb_get_updates_since(base_db_ld, seq_before, NULL, &err);
      CheckNoError(err);
      int log_found = 0;
      for (; rocksdb_wal_iter_valid(wal_iter) && !log_found;
           rocksdb_wal_iter_next(wal_iter)) {
        uint64_t seq;
        rocksdb_writebatch_t* wal_wb =
            rocksdb_wal_iter_get_batch(wal_iter, &seq);
        rocksdb_writebatch_iterate_ld(wal_wb, &log_found, NopPut, NopDel,
                                      CheckLogData);
        rocksdb_writebatch_destroy(wal_wb);
      }
      CheckCondition(log_found == 1);
      rocksdb_wal_iter_destroy(wal_iter);
    }
    rocksdb_transactiondb_close_base_db(base_db_ld);

    // read from outside transaction, after commit
    CheckTxnDBGet(txn_db, roptions, "foo", "hello");
    CheckTxnDBPinGet(txn_db, roptions, "foo", "hello");
    {
      const char* keys[3] = {"box", "foo", "notfound"};
      const size_t keys_sizes[3] = {3, 3, 8};
      char* vals[3];
      size_t vals_sizes[3];
      char* errs[3];
      const char* expected[3] = {"c", "hello", NULL};
      rocksdb_transactiondb_multi_get(txn_db, roptions, 3, keys, keys_sizes,
                                      vals, vals_sizes, errs);
      CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
    }

    // reuse old transaction
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, txn);

    // snapshot
    const rocksdb_snapshot_t* snapshot;
    snapshot = rocksdb_transactiondb_create_snapshot(txn_db);
    rocksdb_readoptions_set_snapshot(roptions, snapshot);

    rocksdb_transactiondb_put(txn_db, woptions, "foo", 3, "hey", 3, &err);
    CheckNoError(err);

    CheckTxnDBGet(txn_db, roptions, "foo", "hello");
    CheckTxnDBPinGet(txn_db, roptions, "foo", "hello");
    rocksdb_readoptions_set_snapshot(roptions, NULL);
    rocksdb_transactiondb_release_snapshot(txn_db, snapshot);
    CheckTxnDBGet(txn_db, roptions, "foo", "hey");
    CheckTxnDBPinGet(txn_db, roptions, "foo", "hey");

    // iterate
    rocksdb_transaction_put(txn, "bar", 3, "hi", 2, &err);
    rocksdb_iterator_t* iter =
        rocksdb_transaction_create_iterator(txn, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar", "hi");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    // rollback
    rocksdb_transaction_rollback(txn, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "bar", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "bar", NULL);

    // save point
    rocksdb_transaction_put(txn, "foo1", 4, "hi1", 3, &err);
    rocksdb_transaction_set_savepoint(txn);
    CheckTxnGet(txn, roptions, "foo1", "hi1");
    CheckTxnPinGet(txn, roptions, "foo1", "hi1");
    rocksdb_transaction_put(txn, "foo2", 4, "hi2", 3, &err);
    CheckTxnGet(txn, roptions, "foo2", "hi2");
    CheckTxnPinGet(txn, roptions, "foo2", "hi2");

    // rollback to savepoint
    rocksdb_transaction_rollback_to_savepoint(txn, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo2", NULL);
    CheckTxnGet(txn, roptions, "foo1", "hi1");
    CheckTxnPinGet(txn, roptions, "foo2", NULL);
    CheckTxnPinGet(txn, roptions, "foo1", "hi1");
    CheckTxnDBGet(txn_db, roptions, "foo1", NULL);
    CheckTxnDBGet(txn_db, roptions, "foo2", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "foo1", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "foo2", NULL);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo1", "hi1");
    CheckTxnDBGet(txn_db, roptions, "foo2", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "foo1", "hi1");
    CheckTxnDBPinGet(txn_db, roptions, "foo2", NULL);

    // Column families.
    rocksdb_column_family_handle_t* cfh;
    cfh = rocksdb_transactiondb_create_column_family(txn_db, options,
                                                     "txn_db_cf", &err);
    CheckNoError(err);

    rocksdb_transactiondb_put_cf(txn_db, woptions, cfh, "cf_foo", 6, "cf_hello",
                                 8, &err);
    CheckNoError(err);
    CheckTxnDBGetCF(txn_db, roptions, cfh, "cf_foo", "cf_hello");
    CheckTxnDBPinGetCF(txn_db, roptions, cfh, "cf_foo", "cf_hello");
    {
      const rocksdb_column_family_handle_t* get_handles[2] = {cfh, cfh};
      const char* keys[2] = {"cf_foo", "notfound"};
      const size_t keys_sizes[2] = {6, 8};
      char* vals[2];
      size_t vals_sizes[2];
      char* errs[2];
      const char* expected[2] = {"cf_hello", NULL};
      rocksdb_transactiondb_multi_get_cf(txn_db, roptions, get_handles, 2, keys,
                                         keys_sizes, vals, vals_sizes, errs);
      CheckMultiGetValues(2, vals, vals_sizes, errs, expected);
    }

    rocksdb_transactiondb_delete_cf(txn_db, woptions, cfh, "cf_foo", 6, &err);
    CheckNoError(err);
    CheckTxnDBGetCF(txn_db, roptions, cfh, "cf_foo", NULL);
    CheckTxnDBPinGetCF(txn_db, roptions, cfh, "cf_foo", NULL);

    // memory usage
    rocksdb_t* base_db = rocksdb_transactiondb_get_base_db(txn_db);
    rocksdb_memory_consumers_t* consumers = rocksdb_memory_consumers_create();
    rocksdb_memory_consumers_add_db(consumers, base_db);
    rocksdb_memory_usage_t* usage =
        rocksdb_approximate_memory_usage_create(consumers, &err);
    CheckNoError(err);
    rocksdb_approximate_memory_usage_destroy(usage);
    rocksdb_memory_consumers_destroy(consumers);
    rocksdb_transactiondb_close_base_db(base_db);

    // flush
    rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();
    rocksdb_flushwaloptions_t* txn_flush_wal_options =
        rocksdb_flushwaloptions_create();
    rocksdb_flushoptions_set_wait(flush_options, 1);
    rocksdb_flushwaloptions_set_sync(txn_flush_wal_options, 1);
    CheckCondition(1 ==
                   rocksdb_flushwaloptions_get_sync(txn_flush_wal_options));
    rocksdb_flushwaloptions_set_rate_limiter_priority(txn_flush_wal_options, 1);
    CheckCondition(1 == rocksdb_flushwaloptions_get_rate_limiter_priority(
                            txn_flush_wal_options));
    rocksdb_transactiondb_flush_wal_with_options(txn_db, txn_flush_wal_options,
                                                 &err);
    CheckNoError(err);
    rocksdb_transactiondb_flush_cf(txn_db, flush_options, cfh, &err);
    CheckNoError(err);
    rocksdb_transactiondb_flush(txn_db, flush_options, &err);
    CheckNoError(err);
    rocksdb_flushwaloptions_destroy(txn_flush_wal_options);
    rocksdb_flushoptions_destroy(flush_options);

    // close and destroy
    rocksdb_column_family_handle_destroy(cfh);
    rocksdb_transaction_destroy(txn);
    rocksdb_transactiondb_close(txn_db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
    rocksdb_transaction_options_destroy(txn_options);
    rocksdb_transactiondb_options_destroy(txn_db_options);
  }

  StartPhase("two-phase commit");
  {
    // open a TransactionDB
    txn_db_options = rocksdb_transactiondb_options_create();
    txn_options = rocksdb_transaction_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    txn_db = rocksdb_transactiondb_open(options, txn_db_options, dbname, &err);
    CheckNoError(err);

    rocksdb_transaction_options_set_skip_prepare(txn_options, 0);
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    rocksdb_transaction_commit(txn, &err);
    CheckCondition(err != NULL);
    Free(&err);
    err = NULL;
    rocksdb_transaction_prepare(txn, &err);
    CheckCondition(err != NULL);
    Free(&err);
    err = NULL;
    rocksdb_transaction_set_name(txn, "txn1", 4, &err);
    CheckNoError(err);
    rocksdb_transaction_prepare(txn, &err);
    CheckNoError(err);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    rocksdb_transaction_destroy(txn);

    // prepare 2 transactions and close db.
    rocksdb_transaction_t* txn1 =
        rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    rocksdb_transaction_put(txn1, "bar1", 4, "1", 1, &err);
    CheckNoError(err);
    rocksdb_transaction_set_name(txn1, "txn1", 4, &err);
    CheckNoError(err);
    rocksdb_transaction_prepare(txn1, &err);
    CheckNoError(err);
    rocksdb_transaction_t* txn2 =
        rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    rocksdb_transaction_put(txn2, "bar2", 4, "2", 1, &err);
    CheckNoError(err);
    rocksdb_transaction_set_name(txn2, "txn2", 4, &err);
    CheckNoError(err);
    rocksdb_transaction_prepare(txn2, &err);
    CheckNoError(err);
    rocksdb_transaction_destroy(txn1);
    rocksdb_transaction_destroy(txn2);
    rocksdb_transactiondb_close(txn_db);
    rocksdb_transaction_options_destroy(txn_options);
    rocksdb_transactiondb_options_destroy(txn_db_options);

    // reopen db and get all prepared.
    txn_db_options = rocksdb_transactiondb_options_create();
    txn_options = rocksdb_transaction_options_create();
    rocksdb_options_set_error_if_exists(options, 0);
    txn_db = rocksdb_transactiondb_open(options, txn_db_options, dbname, &err);
    CheckNoError(err);
    CheckTxnDBPinGet(txn_db, roptions, "bar1", NULL);
    CheckTxnDBPinGet(txn_db, roptions, "bar2", NULL);
    size_t cnt;
    rocksdb_transaction_t** txns =
        rocksdb_transactiondb_get_prepared_transactions(txn_db, &cnt);
    CheckCondition(cnt == 2);
    size_t i;
    for (i = 0; i < cnt; i++) {
      txn = txns[i];
      size_t name_len = 0;
      char* name = rocksdb_transaction_get_name(txn, &name_len);
      CheckCondition(name_len == 4);
      if (strncmp(name, "txn1", name_len) == 0) {
        rocksdb_transaction_commit(txn, &err);
      } else if (strncmp(name, "txn2", name_len) == 0) {
        rocksdb_transaction_rollback(txn, &err);
      }
      rocksdb_free(name);
      CheckNoError(err);
      rocksdb_transaction_destroy(txn);
    }
    rocksdb_free(txns);
    CheckTxnDBGet(txn_db, roptions, "bar1", "1");
    CheckTxnDBGet(txn_db, roptions, "bar2", NULL);
    rocksdb_transactiondb_put(txn_db, woptions, "bar2", 4, "2", 1, &err);
    CheckNoError(err);

    // close and destroy
    rocksdb_transactiondb_close(txn_db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
    rocksdb_transaction_options_destroy(txn_options);
    rocksdb_transactiondb_options_destroy(txn_db_options);
  }

  StartPhase("transactions_multi_get_for_update");
  {
    // open a TransactionDB
    txn_db_options = rocksdb_transactiondb_options_create();
    rocksdb_transactiondb_options_set_transaction_lock_timeout(txn_db_options,
                                                               0);
    txn_options = rocksdb_transaction_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    txn_db = rocksdb_transactiondb_open(options, txn_db_options, dbname, &err);
    CheckNoError(err);

    rocksdb_transactiondb_put(txn_db, woptions, "foo", 3, "hey", 3, &err);
    CheckNoError(err);
    rocksdb_transactiondb_put(txn_db, woptions, "bar", 3, "hello", 5, &err);
    CheckNoError(err);

    // begin transactions
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    rocksdb_transaction_t* txn2 =
        rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);

    // multi get
    {
      const char* keys[2] = {"foo", "bar"};
      const size_t keys_sizes[2] = {3, 3};
      char* vals[2];
      size_t vals_sizes[2];
      char* errs[2];
      const char* expected[2] = {"hey", "hello"};
      rocksdb_transaction_multi_get_for_update(
          txn, roptions, 2, keys, keys_sizes, vals, vals_sizes, errs);
      CheckMultiGetValues(2, vals, vals_sizes, errs, expected);
    }

    char* conflict_err = NULL;
    size_t val_len;
    rocksdb_transaction_get_for_update(txn2, roptions, "foo", 3, &val_len, true,
                                       &conflict_err);
    // get-for-update conflict
    CheckCondition(conflict_err != NULL);
    Free(&conflict_err);

    // commit
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);

    // should work after first tx is commited
    CheckTxnGetForUpdate(txn2, roptions, "foo", "hey");

    // commit the second one
    rocksdb_transaction_commit(txn2, &err);
    CheckNoError(err);

    // destroy txns
    rocksdb_transaction_destroy(txn);
    rocksdb_transaction_destroy(txn2);

    // same for column families

    rocksdb_column_family_handle_t* cfh;
    cfh = rocksdb_transactiondb_create_column_family(txn_db, options,
                                                     "txn_db_cf", &err);
    CheckNoError(err);

    rocksdb_transactiondb_put_cf(txn_db, woptions, cfh, "cf_foo", 6, "cf_hello",
                                 8, &err);
    CheckNoError(err);
    rocksdb_transactiondb_put_cf(txn_db, woptions, cfh, "cf_bar", 6, "cf_hey",
                                 6, &err);
    CheckNoError(err);

    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    txn2 = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);

    {
      const rocksdb_column_family_handle_t* get_handles[2] = {cfh, cfh};
      const char* keys[2] = {"cf_foo", "cf_bar"};
      const size_t keys_sizes[2] = {6, 6};
      char* vals[2];
      size_t vals_sizes[2];
      char* errs[2];
      const char* expected[2] = {"cf_hello", "cf_hey"};
      rocksdb_transaction_multi_get_for_update_cf(txn, roptions, get_handles, 2,
                                                  keys, keys_sizes, vals,
                                                  vals_sizes, errs);
      CheckMultiGetValues(2, vals, vals_sizes, errs, expected);
    }

    char* conflict_err_cf = NULL;
    size_t val_len_cf;
    rocksdb_transaction_get_for_update_cf(txn2, roptions, cfh, "cf_foo", 6,
                                          &val_len_cf, true, &conflict_err_cf);
    CheckCondition(conflict_err_cf != NULL);
    Free(&conflict_err_cf);

    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);

    CheckTxnGetForUpdateCF(txn2, roptions, cfh, "cf_foo", "cf_hello");

    rocksdb_transaction_commit(txn2, &err);
    CheckNoError(err);

    // close and destroy
    rocksdb_column_family_handle_destroy(cfh);
    rocksdb_transaction_destroy(txn);
    rocksdb_transaction_destroy(txn2);
    rocksdb_transactiondb_close(txn_db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
    rocksdb_transaction_options_destroy(txn_options);
    rocksdb_transactiondb_options_destroy(txn_db_options);
  }

  StartPhase("optimistic_transactions");
  {
    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_optimistictransactiondb_options_t* otxn_db_open_options =
        rocksdb_optimistictransactiondb_options_create();
    rocksdb_optimistictransactiondb_lock_buckets_t* lock_buckets =
        rocksdb_optimistictransactiondb_lock_buckets_create(128, 1);
    rocksdb_options_set_create_if_missing(db_options, 1);
    rocksdb_options_set_allow_concurrent_memtable_write(db_options, 1);
    rocksdb_optimistictransactiondb_options_set_validate_policy(
        otxn_db_open_options, 0);
    CheckCondition(0 ==
                   rocksdb_optimistictransactiondb_options_get_validate_policy(
                       otxn_db_open_options));
    rocksdb_optimistictransactiondb_options_set_occ_lock_buckets(
        otxn_db_open_options, 1024);
    CheckCondition(1024 ==
                   rocksdb_optimistictransactiondb_options_get_occ_lock_buckets(
                       otxn_db_open_options));
    CheckCondition(
        rocksdb_optimistictransactiondb_lock_buckets_approximate_memory_usage(
            lock_buckets) > 0);
    rocksdb_optimistictransactiondb_options_set_shared_lock_buckets(
        otxn_db_open_options, lock_buckets);
    otxn_db = rocksdb_optimistictransactiondb_open_with_otxn_db_options(
        db_options, otxn_db_open_options, dbname, &err);
    otxn_options = rocksdb_optimistictransaction_options_create();
    rocksdb_transaction_t* txn1 = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    rocksdb_transaction_t* txn2 = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    rocksdb_transaction_put(txn1, "key", 3, "value", 5, &err);
    CheckNoError(err);
    rocksdb_transaction_put(txn2, "key1", 4, "value1", 6, &err);
    CheckNoError(err);
    CheckTxnGet(txn1, roptions, "key", "value");
    CheckTxnPinGet(txn1, roptions, "key", "value");
    rocksdb_transaction_commit(txn1, &err);
    CheckNoError(err);
    rocksdb_transaction_commit(txn2, &err);
    CheckNoError(err);
    rocksdb_transaction_destroy(txn1);
    rocksdb_transaction_destroy(txn2);

    // Check column family
    db = rocksdb_optimistictransactiondb_get_base_db(otxn_db);
    rocksdb_put(db, woptions, "key", 3, "value", 5, &err);
    CheckNoError(err);
    rocksdb_column_family_handle_t *cfh1, *cfh2;
    char** list_const_cf_names = (char**)malloc(2 * sizeof(char*));
    list_const_cf_names[0] = "txn_db_cf1";
    list_const_cf_names[1] = "txn_db_cf2";
    size_t cflen;
    rocksdb_column_family_handle_t** list_cfh = rocksdb_create_column_families(
        db, db_options, 2, (const char* const*)list_const_cf_names, &cflen,
        &err);
    free(list_const_cf_names);
    CheckNoError(err);
    assert(cflen == 2);
    cfh1 = list_cfh[0];
    cfh2 = list_cfh[1];
    rocksdb_create_column_families_destroy(list_cfh);
    txn = rocksdb_optimistictransaction_begin(otxn_db, woptions, otxn_options,
                                              NULL);
    rocksdb_transaction_put_cf(txn, cfh1, "key_cf1", 7, "val_cf1", 7, &err);
    CheckNoError(err);
    rocksdb_transaction_put_cf(txn, cfh2, "key_cf2", 7, "val_cf2", 7, &err);
    CheckNoError(err);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    txn = rocksdb_optimistictransaction_begin(otxn_db, woptions, otxn_options,
                                              txn);
    CheckGetCF(db, roptions, cfh1, "key_cf1", "val_cf1");
    CheckTxnGetCF(txn, roptions, cfh1, "key_cf1", "val_cf1");
    CheckTxnPinGetCF(txn, roptions, cfh1, "key_cf1", "val_cf1");
    {
      const rocksdb_column_family_handle_t* get_handles[3] = {cfh1, cfh2, cfh2};
      const char* keys[3] = {"key_cf1", "key_cf2", "notfound"};
      const size_t keys_sizes[3] = {7, 7, 8};
      char* vals[3];
      size_t vals_sizes[3];
      char* errs[3];
      const char* expected[3] = {"val_cf1", "val_cf2", NULL};
      rocksdb_transaction_multi_get_cf(txn, roptions, get_handles, 3, keys,
                                       keys_sizes, vals, vals_sizes, errs);
      CheckMultiGetValues(3, vals, vals_sizes, errs, expected);
    }

    // Check iterator with column family
    rocksdb_transaction_put_cf(txn, cfh1, "key1_cf", 7, "val1_cf", 7, &err);
    CheckNoError(err);
    rocksdb_iterator_t* iter =
        rocksdb_transaction_create_iterator_cf(txn, roptions, cfh1);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "key1_cf", "val1_cf");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_transaction_destroy(txn);
    rocksdb_column_family_handle_destroy(cfh1);
    rocksdb_column_family_handle_destroy(cfh2);
    rocksdb_optimistictransactiondb_close_base_db(db);
    rocksdb_optimistictransactiondb_close(otxn_db);

    // Check open optimistic transaction db with column families
    size_t cf_len;
    char** column_fams =
        rocksdb_list_column_families(db_options, dbname, &cf_len, &err);
    CheckNoError(err);
    CheckEqual("default", column_fams[0], 7);
    CheckEqual("txn_db_cf1", column_fams[1], 10);
    CheckEqual("txn_db_cf2", column_fams[2], 10);
    CheckCondition(cf_len == 3);
    rocksdb_list_column_families_destroy(column_fams, cf_len);

    const char* cf_names[3] = {"default", "txn_db_cf1", "txn_db_cf2"};
    rocksdb_options_t* cf_options = rocksdb_options_create();
    const rocksdb_options_t* cf_opts[3] = {cf_options, cf_options, cf_options};

    rocksdb_options_set_error_if_exists(cf_options, 0);
    rocksdb_column_family_handle_t* cf_handles[3];
    otxn_db =
        rocksdb_optimistictransactiondb_open_column_families_with_otxn_db_options(
            db_options, otxn_db_open_options, dbname, 3, cf_names, cf_opts,
            cf_handles, &err);
    CheckNoError(err);
    rocksdb_transaction_t* txn_cf = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    CheckTxnGetCF(txn_cf, roptions, cf_handles[0], "key", "value");
    CheckTxnGetCF(txn_cf, roptions, cf_handles[1], "key_cf1", "val_cf1");
    CheckTxnGetCF(txn_cf, roptions, cf_handles[2], "key_cf2", "val_cf2");
    CheckTxnPinGetCF(txn_cf, roptions, cf_handles[0], "key", "value");
    CheckTxnPinGetCF(txn_cf, roptions, cf_handles[1], "key_cf1", "val_cf1");
    CheckTxnPinGetCF(txn_cf, roptions, cf_handles[2], "key_cf2", "val_cf2");
    rocksdb_transaction_destroy(txn_cf);
    rocksdb_options_destroy(cf_options);
    rocksdb_column_family_handle_destroy(cf_handles[0]);
    rocksdb_column_family_handle_destroy(cf_handles[1]);
    rocksdb_column_family_handle_destroy(cf_handles[2]);
    rocksdb_optimistictransactiondb_close(otxn_db);
    rocksdb_destroy_db(db_options, dbname, &err);
    rocksdb_options_destroy(db_options);
    rocksdb_optimistictransactiondb_lock_buckets_destroy(lock_buckets);
    rocksdb_optimistictransactiondb_options_destroy(otxn_db_open_options);
    rocksdb_optimistictransaction_options_destroy(otxn_options);
    CheckNoError(err);
  }

  // Simple sanity check that setting memtable rep works.
  StartPhase("memtable_reps");
  {
    // Create database with vector memtable.
    rocksdb_options_set_memtable_vector_rep(options);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    // Create database with hash skiplist memtable.
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_set_hash_skip_list_rep(options, 5000, 4, 4);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
  }

  // Check that secondary instance works.
  StartPhase("open_as_secondary");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);

    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(db_options, 1);
    db = rocksdb_open(db_options, dbname, &err);
    CheckNoError(err);
    rocksdb_t* db1;
    rocksdb_options_t* opts = rocksdb_options_create();
    rocksdb_options_set_max_open_files(opts, -1);
    rocksdb_options_set_create_if_missing(opts, 1);
    snprintf(secondary_path, sizeof(secondary_path),
             "%s/rocksdb_c_test_secondary-%" PRIu64, GetTempDir(), GetTestId());
    db1 = rocksdb_open_as_secondary(opts, dbname, secondary_path, &err);
    CheckNoError(err);

    rocksdb_writeoptions_set_sync(woptions, 0);
    rocksdb_writeoptions_disable_WAL(woptions, 1);
    rocksdb_put(db, woptions, "key0", 4, "value0", 6, &err);
    CheckNoError(err);
    rocksdb_flushoptions_t* flush_opts = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_opts, 1);
    rocksdb_flush(db, flush_opts, &err);
    CheckNoError(err);
    rocksdb_try_catch_up_with_primary(db1, &err);
    CheckNoError(err);
    rocksdb_readoptions_t* ropts = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(ropts, 1);
    rocksdb_readoptions_set_snapshot(ropts, NULL);
    CheckGet(db, ropts, "key0", "value0");
    CheckGet(db1, ropts, "key0", "value0");

    rocksdb_writeoptions_disable_WAL(woptions, 0);
    rocksdb_put(db, woptions, "key1", 4, "value1", 6, &err);
    CheckNoError(err);
    rocksdb_try_catch_up_with_primary(db1, &err);
    CheckNoError(err);
    CheckGet(db1, ropts, "key0", "value0");
    CheckGet(db1, ropts, "key1", "value1");

    rocksdb_close(db1);
    rocksdb_destroy_db(opts, secondary_path, &err);
    CheckNoError(err);

    rocksdb_options_destroy(db_options);
    rocksdb_options_destroy(opts);
    rocksdb_readoptions_destroy(ropts);
    rocksdb_flushoptions_destroy(flush_opts);
  }

  // Simple sanity check that options setting db_paths work.
  StartPhase("open_db_paths");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);

    const rocksdb_dbpath_t* paths[1] = {dbpath};
    rocksdb_options_set_db_paths(options, paths, 1);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
  }

  StartPhase("filter_with_prefix_seek");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_set_prefix_extractor(
        options, rocksdb_slicetransform_create_fixed_prefix(1));
    rocksdb_filterpolicy_t* filter_policy =
        rocksdb_filterpolicy_create_bloom_full(8.0);
    rocksdb_block_based_options_set_filter_policy(table_options, filter_policy);
    rocksdb_options_set_block_based_table_factory(options, table_options);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    int i;
    for (i = 0; i < 10; ++i) {
      char key = '0' + (char)i;
      rocksdb_put(db, woptions, &key, 1, "", 1, &err);
      CheckNoError(err);
    }

    // Flush to generate an L0 so that filter will be used later.
    rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_options, 1);
    rocksdb_flush(db, flush_options, &err);
    rocksdb_flushoptions_destroy(flush_options);
    CheckNoError(err);

    rocksdb_readoptions_t* ropts = rocksdb_readoptions_create();
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, ropts);

    rocksdb_iter_seek(iter, "0", 1);
    int cnt = 0;
    while (rocksdb_iter_valid(iter)) {
      ++cnt;
      rocksdb_iter_next(iter);
    }
    CheckCondition(10 == cnt);

    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_destroy(ropts);
  }

  StartPhase("statistics");
  {
    const uint32_t BYTES_WRITTEN_TICKER = 61;
    const uint32_t DB_WRITE_HIST = 1;

    rocksdb_statistics_histogram_data_t* hist =
        rocksdb_statistics_histogram_data_create();
    {
      // zero by default
      CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_median(hist));
      CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_p95(hist));
      CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_p99(hist));
      CheckCondition(0.0 ==
                     rocksdb_statistics_histogram_data_get_average(hist));
      CheckCondition(0.0 ==
                     rocksdb_statistics_histogram_data_get_std_dev(hist));
      CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_max(hist));
      CheckCondition(0 == rocksdb_statistics_histogram_data_get_count(hist));
      CheckCondition(0 == rocksdb_statistics_histogram_data_get_sum(hist));
      CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_min(hist));
    }

    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_enable_statistics(options);
    rocksdb_options_set_statistics_level(options, rocksdb_statistics_level_all);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    CheckCondition(0 == rocksdb_options_statistics_get_ticker_count(
                            options, BYTES_WRITTEN_TICKER));
    rocksdb_options_statistics_get_histogram_data(options, DB_WRITE_HIST, hist);
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_median(hist));
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_p95(hist));
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_p99(hist));
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_average(hist));
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_std_dev(hist));
    CheckCondition(0.0 == rocksdb_statistics_histogram_data_get_max(hist));
    CheckCondition(0 == rocksdb_statistics_histogram_data_get_count(hist));
    CheckCondition(0 == rocksdb_statistics_histogram_data_get_sum(hist));

    int i;
    for (i = 0; i < 10; ++i) {
      char key = '0' + (char)i;
      rocksdb_put(db, woptions, &key, 1, "", 1, &err);
      CheckNoError(err);
    }
    CheckCondition(0 != rocksdb_options_statistics_get_ticker_count(
                            options, BYTES_WRITTEN_TICKER));
    rocksdb_options_statistics_get_histogram_data(options, DB_WRITE_HIST, hist);
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_median(hist));
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_p95(hist));
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_p99(hist));
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_average(hist));
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_std_dev(hist));
    CheckCondition(0.0 != rocksdb_statistics_histogram_data_get_max(hist));
    CheckCondition(0 != rocksdb_statistics_histogram_data_get_count(hist));
    CheckCondition(0 != rocksdb_statistics_histogram_data_get_sum(hist));

    rocksdb_statistics_histogram_data_destroy(hist);
  }

  StartPhase("wait_for_compact_options");
  {
    rocksdb_wait_for_compact_options_t* wco;
    wco = rocksdb_wait_for_compact_options_create();

    rocksdb_wait_for_compact_options_set_abort_on_pause(wco, 1);
    CheckCondition(1 ==
                   rocksdb_wait_for_compact_options_get_abort_on_pause(wco));

    rocksdb_wait_for_compact_options_set_flush(wco, 1);
    CheckCondition(1 == rocksdb_wait_for_compact_options_get_flush(wco));

    rocksdb_wait_for_compact_options_set_close_db(wco, 1);
    CheckCondition(1 == rocksdb_wait_for_compact_options_get_close_db(wco));

    rocksdb_wait_for_compact_options_set_wait_for_purge(wco, 1);
    CheckCondition(1 ==
                   rocksdb_wait_for_compact_options_get_wait_for_purge(wco));

    rocksdb_wait_for_compact_options_set_timeout(wco, 342);
    CheckCondition(342 == rocksdb_wait_for_compact_options_get_timeout(wco));

    rocksdb_wait_for_compact_options_destroy(wco);
  }
  StartPhase("wait_for_compact");
  {
    rocksdb_wait_for_compact_options_t* wco;
    wco = rocksdb_wait_for_compact_options_create();
    rocksdb_wait_for_compact_options_set_flush(wco, 1);

    rocksdb_wait_for_compact(db, wco, &err);
    CheckNoError(err);
    rocksdb_wait_for_compact_options_destroy(wco);
  }

  StartPhase("livefiles_storage_info");
  {
    int found_named_entry = 0;
    rocksdb_livefiles_storage_info_options_t* storage_options =
        rocksdb_livefiles_storage_info_options_create();
    rocksdb_livefiles_storage_info_t* storage_info;
    rocksdb_livefiles_storage_info_options_set_include_checksum_info(
        storage_options, 1);
    CheckCondition(
        1 == rocksdb_livefiles_storage_info_options_get_include_checksum_info(
                 storage_options));
    rocksdb_livefiles_storage_info_options_set_wal_size_for_flush(
        storage_options, UINT64_MAX);
    CheckCondition(
        UINT64_MAX ==
        rocksdb_livefiles_storage_info_options_get_wal_size_for_flush(
            storage_options));
    rocksdb_livefiles_storage_info_options_set_atomic_flush(storage_options, 1);
    CheckCondition(1 == rocksdb_livefiles_storage_info_options_get_atomic_flush(
                            storage_options));

    storage_info =
        rocksdb_get_livefiles_storage_info(db, storage_options, &err);
    CheckNoError(err);
    CheckCondition(storage_info != NULL);
    CheckCondition(rocksdb_livefiles_storage_info_count(storage_info) > 0);
    for (size_t i = 0; i < rocksdb_livefiles_storage_info_count(storage_info);
         ++i) {
      size_t replacement_contents_len = 0;
      const char* relative_filename =
          rocksdb_livefiles_storage_info_relative_filename(storage_info, i);
      const char* directory =
          rocksdb_livefiles_storage_info_directory(storage_info, i);
      CheckCondition(relative_filename != NULL);
      CheckCondition(directory != NULL);
      if (strlen(relative_filename) > 0) {
        found_named_entry = 1;
      }
      (void)rocksdb_livefiles_storage_info_file_number(storage_info, i);
      (void)rocksdb_livefiles_storage_info_file_type(storage_info, i);
      (void)rocksdb_livefiles_storage_info_size(storage_info, i);
      (void)rocksdb_livefiles_storage_info_temperature(storage_info, i);
      (void)rocksdb_livefiles_storage_info_file_checksum(storage_info, i);
      (void)rocksdb_livefiles_storage_info_file_checksum_func_name(storage_info,
                                                                   i);
      (void)rocksdb_livefiles_storage_info_replacement_contents(
          storage_info, i, &replacement_contents_len);
      (void)rocksdb_livefiles_storage_info_trim_to_size(storage_info, i);
    }
    CheckCondition(found_named_entry);
    rocksdb_livefiles_storage_info_destroy(storage_info);
    rocksdb_livefiles_storage_info_options_destroy(storage_options);
  }

  StartPhase("trace_options");
  {
    rocksdb_trace_options_t* trace_options = rocksdb_trace_options_create();
    rocksdb_block_cache_trace_options_t* block_trace_options =
        rocksdb_block_cache_trace_options_create();
    rocksdb_block_cache_trace_writer_options_t* writer_options =
        rocksdb_block_cache_trace_writer_options_create();
    rocksdb_trace_options_set_max_trace_file_size(trace_options, 1024 * 1024);
    CheckCondition(1024 * 1024 == rocksdb_trace_options_get_max_trace_file_size(
                                      trace_options));
    rocksdb_trace_options_set_sampling_frequency(trace_options, 3);
    CheckCondition(3 ==
                   rocksdb_trace_options_get_sampling_frequency(trace_options));
    rocksdb_trace_options_set_filter(trace_options,
                                     rocksdb_trace_filter_multi_get);
    CheckCondition(rocksdb_trace_filter_multi_get ==
                   rocksdb_trace_options_get_filter(trace_options));
    rocksdb_trace_options_set_preserve_write_order(trace_options, 1);
    CheckCondition(
        1 == rocksdb_trace_options_get_preserve_write_order(trace_options));
    rocksdb_block_cache_trace_options_set_sampling_frequency(
        block_trace_options, 7);
    CheckCondition(7 ==
                   rocksdb_block_cache_trace_options_get_sampling_frequency(
                       block_trace_options));
    rocksdb_block_cache_trace_writer_options_set_max_trace_file_size(
        writer_options, 2048);
    CheckCondition(
        2048 ==
        rocksdb_block_cache_trace_writer_options_get_max_trace_file_size(
            writer_options));
    rocksdb_block_cache_trace_writer_options_destroy(writer_options);
    rocksdb_block_cache_trace_options_destroy(block_trace_options);
    rocksdb_trace_options_destroy(trace_options);
  }

  StartPhase("trace");
  {
    char trace_filename[400];
    char io_trace_filename[400];
    char block_trace_filename[400];
    char block_trace_filename_2[400];
    char replay_dbname[400];
    size_t val_len;
    char* val;
    rocksdb_trace_options_t* trace_options = rocksdb_trace_options_create();
    rocksdb_envoptions_t* trace_env_options = rocksdb_envoptions_create();
    rocksdb_block_cache_trace_options_t* block_trace_options =
        rocksdb_block_cache_trace_options_create();
    rocksdb_block_cache_trace_writer_options_t* writer_options =
        rocksdb_block_cache_trace_writer_options_create();
    rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();

    snprintf(trace_filename, sizeof(trace_filename), "%s/rocksdb.trace",
             dbname);
    snprintf(io_trace_filename, sizeof(io_trace_filename),
             "%s/rocksdb_io.trace", dbname);
    snprintf(block_trace_filename, sizeof(block_trace_filename),
             "%s/rocksdb_block.trace", dbname);
    snprintf(block_trace_filename_2, sizeof(block_trace_filename_2),
             "%s/rocksdb_block2.trace", dbname);
    remove(trace_filename);
    remove(io_trace_filename);
    remove(block_trace_filename);
    remove(block_trace_filename_2);

    rocksdb_trace_options_set_max_trace_file_size(trace_options, 1024 * 1024);
    rocksdb_trace_options_set_sampling_frequency(trace_options, 1);
    rocksdb_trace_options_set_filter(trace_options, rocksdb_trace_filter_none);
    rocksdb_block_cache_trace_options_set_sampling_frequency(
        block_trace_options, 1);
    rocksdb_block_cache_trace_writer_options_set_max_trace_file_size(
        writer_options, 1024 * 1024);
    rocksdb_flushoptions_set_wait(flush_options, 1);

    rocksdb_start_trace(db, NULL, trace_env_options, trace_options,
                        trace_filename, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "tracek", 6, "tracev", 6, &err);
    CheckNoError(err);
    rocksdb_end_trace(db, &err);
    CheckNoError(err);
    CheckCondition(GetLocalFileSize(trace_filename) > 0);

    rocksdb_start_io_trace(db, NULL, trace_env_options, trace_options,
                           io_trace_filename, &err);
    CheckNoError(err);
    val = rocksdb_get(db, roptions, "tracek", 6, &val_len, &err);
    CheckValue(err, "tracev", &val, val_len);
    rocksdb_end_io_trace(db, &err);
    CheckNoError(err);
    CheckCondition(GetLocalFileSize(io_trace_filename) > 0);

    rocksdb_flush(db, flush_options, &err);
    CheckNoError(err);

    rocksdb_start_block_cache_trace(db, NULL, trace_env_options, trace_options,
                                    block_trace_filename, &err);
    CheckNoError(err);
    val = rocksdb_get(db, roptions, "tracek", 6, &val_len, &err);
    CheckValue(err, "tracev", &val, val_len);
    rocksdb_end_block_cache_trace(db, &err);
    CheckNoError(err);
    CheckCondition(GetLocalFileSize(block_trace_filename) > 0);

    rocksdb_start_block_cache_trace_with_options(
        db, NULL, trace_env_options, block_trace_options, writer_options,
        block_trace_filename_2, &err);
    CheckNoError(err);
    val = rocksdb_get(db, roptions, "tracek", 6, &val_len, &err);
    CheckValue(err, "tracev", &val, val_len);
    rocksdb_end_block_cache_trace(db, &err);
    CheckNoError(err);
    CheckCondition(GetLocalFileSize(block_trace_filename_2) > 0);

    {
      rocksdb_trace_reader_t* trace_reader;
      rocksdb_replay_options_t* replay_options =
          rocksdb_replay_options_create();
      rocksdb_options_t* replay_db_options = rocksdb_options_create();
      rocksdb_t* replay_db;
      rocksdb_replayer_t* replayer;
      char* encoded_trace = NULL;
      size_t encoded_trace_size = 0;

      trace_reader = rocksdb_trace_reader_create(env, trace_env_options,
                                                 trace_filename, &err);
      CheckNoError(err);
      CheckCondition(trace_reader != NULL);

      encoded_trace =
          rocksdb_trace_reader_read(trace_reader, &encoded_trace_size, &err);
      CheckNoError(err);
      CheckCondition(encoded_trace != NULL);
      CheckCondition(encoded_trace_size > 0);
      free(encoded_trace);

      rocksdb_trace_reader_reset(trace_reader, &err);
      CheckNoError(err);
      encoded_trace =
          rocksdb_trace_reader_read(trace_reader, &encoded_trace_size, &err);
      CheckNoError(err);
      CheckCondition(encoded_trace != NULL);
      free(encoded_trace);

      rocksdb_trace_reader_close(trace_reader, &err);
      CheckNoError(err);
      rocksdb_trace_reader_destroy(trace_reader);

      snprintf(replay_dbname, sizeof(replay_dbname),
               "%s/rocksdb_c_test-%d-replay", GetTempDir(), ((int)geteuid()));
      rocksdb_options_set_create_if_missing(replay_db_options, 1);
      rocksdb_options_set_env(replay_db_options, env);
      rocksdb_options_set_comparator(replay_db_options, cmp);
      replay_db = rocksdb_open(replay_db_options, replay_dbname, &err);
      CheckNoError(err);

      rocksdb_replay_options_set_num_threads(replay_options, 1);
      rocksdb_replay_options_set_fast_forward(replay_options, 1000000.0);
      CheckCondition(1 ==
                     rocksdb_replay_options_get_num_threads(replay_options));
      CheckCondition(1000000.0 ==
                     rocksdb_replay_options_get_fast_forward(replay_options));

      replayer = rocksdb_new_default_replayer(
          replay_db, NULL, 0, env, trace_env_options, trace_filename, &err);
      CheckNoError(err);
      CheckCondition(replayer != NULL);
      rocksdb_replayer_prepare(replayer, &err);
      CheckNoError(err);
      CheckCondition(rocksdb_replayer_get_header_timestamp(replayer) > 0);
      rocksdb_replayer_replay(replayer, replay_options, &err);
      CheckNoError(err);
      CheckGet(replay_db, roptions, "tracek", "tracev");

      rocksdb_replayer_destroy(replayer);
      rocksdb_close(replay_db);
      rocksdb_options_destroy(replay_db_options);
      rocksdb_replay_options_destroy(replay_options);
    }

    rocksdb_flushoptions_destroy(flush_options);
    rocksdb_block_cache_trace_writer_options_destroy(writer_options);
    rocksdb_block_cache_trace_options_destroy(block_trace_options);
    rocksdb_envoptions_destroy(trace_env_options);
    rocksdb_trace_options_destroy(trace_options);
  }

  StartPhase("write_buffer_manager");
  {
    rocksdb_cache_t* lru;
    lru = rocksdb_cache_create_lru(100);

    rocksdb_write_buffer_manager_t* write_buffer_manager;
    write_buffer_manager =
        rocksdb_write_buffer_manager_create_with_cache(200, lru, false);

    CheckCondition(true ==
                   rocksdb_write_buffer_manager_enabled(write_buffer_manager));
    CheckCondition(true == rocksdb_write_buffer_manager_cost_to_cache(
                               write_buffer_manager));
    CheckCondition(
        200 == rocksdb_write_buffer_manager_buffer_size(write_buffer_manager));

    rocksdb_write_buffer_manager_set_buffer_size(write_buffer_manager, 300);
    CheckCondition(
        300 == rocksdb_write_buffer_manager_buffer_size(write_buffer_manager));

    rocksdb_write_buffer_manager_destroy(write_buffer_manager);

    write_buffer_manager = rocksdb_write_buffer_manager_create_with_policy(
        200, true,
        rocksdb_write_buffer_manager_flush_policy_largest_across_dbs);
    CheckCondition(write_buffer_manager != NULL);
    CheckCondition(
        rocksdb_write_buffer_manager_flush_policy_largest_across_dbs ==
        rocksdb_write_buffer_manager_flush_policy(write_buffer_manager));
    rocksdb_write_buffer_manager_set_flush_policy(
        write_buffer_manager,
        rocksdb_write_buffer_manager_flush_policy_largest);
    CheckCondition(
        rocksdb_write_buffer_manager_flush_policy_largest ==
        rocksdb_write_buffer_manager_flush_policy(write_buffer_manager));
    rocksdb_write_buffer_manager_set_flush_policy(write_buffer_manager, 999);
    CheckCondition(
        rocksdb_write_buffer_manager_flush_policy_largest ==
        rocksdb_write_buffer_manager_flush_policy(write_buffer_manager));
    rocksdb_write_buffer_manager_destroy(write_buffer_manager);

    write_buffer_manager =
        rocksdb_write_buffer_manager_create_with_cache_and_policy(
            200, lru, true,
            rocksdb_write_buffer_manager_flush_policy_largest_across_dbs);
    CheckCondition(write_buffer_manager != NULL);
    rocksdb_write_buffer_manager_destroy(write_buffer_manager);

    write_buffer_manager =
        rocksdb_write_buffer_manager_create_with_policy(200, true, 999);
    CheckCondition(write_buffer_manager == NULL);
    rocksdb_cache_destroy(lru);
  }

  StartPhase("remote_compaction_service");
  {
    RemoteCompactionState remote_state = {0, 0, 0, "", ""};

    // Create compaction service
    rocksdb_compactionservice_t* service = rocksdb_compactionservice_create(
        &remote_state,             // state
        RemoteCompactionDestroy,   // destructor
        RemoteCompactionSchedule,  // schedule callback
        "TestRemoteCompaction",    // name
        RemoteCompactionWait,      // wait callback
        RemoteCompactionCancel,    // cancel_awaiting_jobs
        NULL);                     // on_installation

    // Create options with remote compaction
    rocksdb_options_t* remote_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(remote_options, 1);
    rocksdb_options_set_level0_file_num_compaction_trigger(remote_options, 2);
    rocksdb_options_set_write_buffer_size(remote_options,
                                          64 * 1024);  // 64KB buffer
    rocksdb_options_set_max_bytes_for_level_base(remote_options,
                                                 256 * 1024);  // 256KB
    rocksdb_options_set_target_file_size_base(
        remote_options, 64 * 1024);  // 64KB target file size
    // Disable automatic compactions to test manual compaction only
    rocksdb_options_set_disable_auto_compactions(remote_options, 1);
    rocksdb_options_set_compaction_service(remote_options, service);

    // Destroy old DB and create new one
    rocksdb_close(db);
    rocksdb_destroy_db(remote_options, dbname, &err);
    CheckNoError(err);

    db = rocksdb_open(remote_options, dbname, &err);
    CheckNoError(err);

    // Create multiple SST files to trigger compaction
    rocksdb_flushoptions_t* flush_opts = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_opts, 1);

    // Write and flush multiple times to create multiple L0 files
    // Write more data with larger values to ensure files are substantial
    for (int batch = 0; batch < 5; batch++) {
      for (int i = 0; i < 200; i++) {
        char key[20], val[1000];
        snprintf(key, sizeof(key), "key%d_%d", batch, i);
        // Fill value with repeated data to make it larger
        memset(val, 'a' + (batch % 26), sizeof(val) - 1);
        val[sizeof(val) - 1] = '\0';
        rocksdb_put(db, woptions, key, strlen(key), val, strlen(val), &err);
        CheckNoError(err);
      }
      rocksdb_flush(db, flush_opts, &err);
      CheckNoError(err);
    }
    rocksdb_flushoptions_destroy(flush_opts);

    // Trigger manual compaction to invoke remote compaction service
    rocksdb_compact_range(db, NULL, 0, NULL, 0);

    rocksdb_wait_for_compact_options_t* wco =
        rocksdb_wait_for_compact_options_create();
    rocksdb_wait_for_compact(db, wco, &err);
    CheckNoError(err);
    rocksdb_wait_for_compact_options_destroy(wco);

    // Verify that callbacks were actually called
    CheckCondition(remote_state.schedule_called > 0);
    CheckCondition(remote_state.wait_called > 0);
    CheckCondition(strlen(remote_state.last_db_name) > 0);
    CheckCondition(strstr(remote_state.last_db_name, "rocksdb_c_test") != NULL);

    // Verify data is still accessible after remote compaction
    // Just check a few keys to verify data integrity
    for (int batch = 0; batch < 5; batch++) {
      char key[20];
      snprintf(key, sizeof(key), "key%d_0", batch);
      size_t vallen;
      char* val = rocksdb_get(db, roptions, key, strlen(key), &vallen, &err);
      CheckNoError(err);
      CheckCondition(val != NULL);
      CheckCondition(vallen == 999);  // strlen of 1000-byte string
      free(val);
    }

    // Test cancellation API directly
    RemoteCompactionCancel(&remote_state);
    CheckCondition(remote_state.cancel_called > 0);

    // Cleanup
    rocksdb_close(db);
    rocksdb_destroy_db(remote_options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_destroy(remote_options);

    // Reopen DB with original options for subsequent tests
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
  }

  StartPhase("remote_compaction_scheduleresponse");
  {
    // Test scheduleresponse creation and getters
    rocksdb_compactionservice_scheduleresponse_t* response;

    // Test success response
    err = NULL;
    response = rocksdb_compactionservice_scheduleresponse_create(
        "test-job-123", rocksdb_compactionservice_jobstatus_success, &err);
    CheckNoError(err);
    CheckCondition(response != NULL);
    CheckCondition(
        rocksdb_compactionservice_scheduleresponse_getstatus(response) ==
        rocksdb_compactionservice_jobstatus_success);

    size_t job_id_len;
    const char* job_id =
        rocksdb_compactionservice_scheduleresponse_get_scheduled_job_id(
            response, &job_id_len);
    CheckCondition(job_id_len == strlen("test-job-123"));
    CheckCondition(memcmp(job_id, "test-job-123", job_id_len) == 0);
    rocksdb_compactionservice_scheduleresponse_t_destroy(response);

    // Test failure response
    response = rocksdb_compactionservice_scheduleresponse_create_with_status(
        rocksdb_compactionservice_jobstatus_failure, &err);
    CheckCondition(response != NULL);
    CheckCondition(
        rocksdb_compactionservice_scheduleresponse_getstatus(response) ==
        rocksdb_compactionservice_jobstatus_failure);
    rocksdb_compactionservice_scheduleresponse_t_destroy(response);

    response = rocksdb_compactionservice_scheduleresponse_create_with_status(
        999, &err);
    CheckCondition(response == NULL);  // Invalid status
    if (err) {
      Free(&err);
    }
  }

  StartPhase("remote_compaction_options_override");
  {
    // Test CompactionServiceOptionsOverride API
    rocksdb_compaction_service_options_override_t* override_opts =
        rocksdb_compaction_service_options_override_create();
    CheckCondition(override_opts != NULL);

    // Set up override options
    rocksdb_compaction_service_options_override_set_env(override_opts, env);
    rocksdb_compaction_service_options_override_set_comparator(override_opts,
                                                               cmp);

    // Test file checksum gen factory
    rocksdb_file_checksum_gen_factory_t* checksum_factory =
        rocksdb_file_checksum_gen_crc32c_factory_create();
    CheckCondition(checksum_factory != NULL);
    rocksdb_compaction_service_options_override_set_file_checksum_gen_factory(
        override_opts, checksum_factory);

    // Test SST partitioner factory
    rocksdb_sst_partitioner_factory_t* partitioner_factory =
        rocksdb_sst_partitioner_fixed_prefix_factory_create(4);
    CheckCondition(partitioner_factory != NULL);
    rocksdb_compaction_service_options_override_set_sst_partitioner_factory(
        override_opts, partitioner_factory);

    // Test merge operator
    rocksdb_compaction_service_options_override_set_merge_operator(
        override_opts, NULL);

    // Test compaction filter
    rocksdb_compaction_service_options_override_set_compaction_filter(
        override_opts, NULL);

    // Test prefix extractor
    rocksdb_compaction_service_options_override_set_prefix_extractor(
        override_opts, NULL);

    // Test table factory - block based
    rocksdb_block_based_table_options_t* table_opts =
        rocksdb_block_based_options_create();
    rocksdb_compaction_service_options_override_set_block_based_table_factory(
        override_opts, table_opts);
    rocksdb_block_based_options_destroy(table_opts);

    // Test statistics via options
    rocksdb_options_t* stats_opts = rocksdb_options_create();
    rocksdb_options_enable_statistics(stats_opts);
    rocksdb_compaction_service_options_override_set_statistics(override_opts,
                                                               stats_opts);
    rocksdb_options_destroy(stats_opts);

    // Test info log
    rocksdb_logger_t* logger =
        rocksdb_logger_create_stderr_logger(1, "test_prefix");
    rocksdb_compaction_service_options_override_set_info_log(override_opts,
                                                             logger);
    rocksdb_logger_destroy(logger);

    // Test options map
    rocksdb_compaction_service_options_override_set_option(
        override_opts, "max_bytes_for_level_base", "67108864");

    // Cleanup
    rocksdb_file_checksum_gen_factory_destroy(checksum_factory);
    rocksdb_sst_partitioner_factory_destroy(partitioner_factory);
    rocksdb_compaction_service_options_override_destroy(override_opts);
  }

  StartPhase("factory_options_on_regular_options");
  {
    // Test that the new factory types work with regular rocksdb_options_t
    rocksdb_options_t* test_opts = rocksdb_options_create();
    rocksdb_t* checksum_db;
    rocksdb_writeoptions_t* checksum_woptions = rocksdb_writeoptions_create();
    rocksdb_flushoptions_t* checksum_flushoptions =
        rocksdb_flushoptions_create();
    rocksdb_readoptions_t* checksum_roptions = rocksdb_readoptions_create();
    char checksum_dbname[400];

    // Test file checksum gen factory on regular options
    rocksdb_file_checksum_gen_factory_t* checksum_factory =
        rocksdb_file_checksum_gen_crc32c_factory_create();
    CheckCondition(checksum_factory != NULL);
    rocksdb_options_set_file_checksum_gen_factory(test_opts, checksum_factory);
    rocksdb_options_set_create_if_missing(test_opts, 1);
    rocksdb_options_set_env(test_opts, env);

    // Test SST partitioner factory on regular options
    rocksdb_sst_partitioner_factory_t* partitioner_factory =
        rocksdb_sst_partitioner_fixed_prefix_factory_create(8);
    CheckCondition(partitioner_factory != NULL);
    rocksdb_options_set_sst_partitioner_factory(test_opts, partitioner_factory);

    snprintf(checksum_dbname, sizeof(checksum_dbname),
             "%s/rocksdb_c_test-%d-checksum", GetTempDir(), ((int)geteuid()));
    checksum_db = rocksdb_open(test_opts, checksum_dbname, &err);
    CheckNoError(err);
    rocksdb_put(checksum_db, checksum_woptions, "checksum-key", 12,
                "checksum-value", 14, &err);
    CheckNoError(err);
    rocksdb_flush(checksum_db, checksum_flushoptions, &err);
    CheckNoError(err);
    rocksdb_verify_file_checksums(checksum_db, &err);
    CheckNoError(err);
    rocksdb_verify_file_checksums_with_options(checksum_db, checksum_roptions,
                                               &err);
    CheckNoError(err);
    rocksdb_close(checksum_db);
    rocksdb_readoptions_destroy(checksum_roptions);
    rocksdb_flushoptions_destroy(checksum_flushoptions);
    rocksdb_writeoptions_destroy(checksum_woptions);

    // Cleanup
    rocksdb_file_checksum_gen_factory_destroy(checksum_factory);
    rocksdb_sst_partitioner_factory_destroy(partitioner_factory);
    rocksdb_options_destroy(test_opts);
  }

  StartPhase("remote_compaction_null_callback_handling");
  {
    // Test that NULL callback returns are handled gracefully
    // This simulates a failure in the remote compaction service
    rocksdb_compactionservice_t* null_service =
        rocksdb_compactionservice_create(NULL, NULL, NullSchedule,
                                         "NullTestService", NULL, NULL, NULL);

    rocksdb_options_t* null_opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(null_opts, 1);
    rocksdb_options_set_compaction_service(null_opts, null_service);

    char null_db[200];
    int null_db_len = snprintf(null_db, sizeof(null_db),
                               "%s/rocksdb_c_test_null_service-%" PRIu64,
                               GetTempDir(), GetTestId());
    if (null_db_len <= 0 || (size_t)null_db_len >= sizeof(null_db)) {
      abort();
    }

    rocksdb_t* null_db_handle = rocksdb_open(null_opts, null_db, &err);
    CheckNoError(err);

    // Write data and trigger compaction
    for (int i = 0; i < 100; i++) {
      char key[20], val[50];
      snprintf(key, sizeof(key), "key%d", i);
      snprintf(val, sizeof(val), "val%d", i);
      rocksdb_put(null_db_handle, woptions, key, strlen(key), val, strlen(val),
                  &err);
      CheckNoError(err);
    }

    // This should fall back to local compaction (not crash)
    rocksdb_compact_range(null_db_handle, NULL, 0, NULL, 0);

    // Data should still be readable
    CheckGet(null_db_handle, roptions, "key50", "val50");

    rocksdb_close(null_db_handle);
    rocksdb_destroy_db(null_opts, null_db, &err);
    rocksdb_options_destroy(null_opts);
  }

  StartPhase("remote_compaction_canceled_flag");
  {
    // Test atomic cancellation flag API
    unsigned char* canceled = rocksdb_open_and_compact_canceled_create();
    CheckCondition(canceled != NULL);

    // Set cancellation
    rocksdb_open_and_compact_canceled_set(canceled, 1);

    // Use with OpenAndCompactOptions
    rocksdb_open_and_compact_options_t* oac_opts =
        rocksdb_open_and_compact_options_create();
    rocksdb_open_and_compact_options_set_canceled(oac_opts, canceled);
    rocksdb_open_and_compact_options_set_allow_resumption(oac_opts, 1);
    CheckCondition(
        1 == rocksdb_open_and_compact_options_get_allow_resumption(oac_opts));

    // Cleanup
    rocksdb_open_and_compact_options_destroy(oac_opts);
    rocksdb_open_and_compact_canceled_destroy(canceled);
  }

  StartPhase("sst_file_manager");
  {
    rocksdb_sst_file_manager_t* sst_file_manager;
    sst_file_manager = rocksdb_sst_file_manager_create(env);
    rocksdb_sst_file_manager_set_delete_rate_bytes_per_second(sst_file_manager,
                                                              1);
    rocksdb_sst_file_manager_set_max_trash_db_ratio(sst_file_manager, 0.75);

    CheckCondition(1 ==
                   rocksdb_sst_file_manager_get_delete_rate_bytes_per_second(
                       sst_file_manager));
    CheckCondition(0.75 == rocksdb_sst_file_manager_get_max_trash_db_ratio(
                               sst_file_manager));

    rocksdb_sst_file_manager_destroy(sst_file_manager);
  }

  StartPhase("create_column_family_error_returns_null");
  {
    // Creating a column family with a name that already exists should fail
    // and return NULL. Without the fix, the handle is leaked and a non-NULL
    // pointer with an indeterminate rep field is returned.
    char* cf_err = NULL;
    rocksdb_column_family_handle_t* cf_handle =
        rocksdb_create_column_family(db, options, "default", &cf_err);
    // Should have an error since "default" already exists
    CheckCondition(cf_err != NULL);
    // The handle should be NULL on error (this is the bug fix)
    CheckCondition(cf_handle == NULL);
    free(cf_err);
  }

  StartPhase("livefiles_checksum");
  {
    // Open a separate DB with file checksum generation enabled
    rocksdb_options_t* ck_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(ck_options, 1);
    rocksdb_file_checksum_gen_factory_t* ck_factory =
        rocksdb_file_checksum_gen_crc32c_factory_create();
    rocksdb_options_set_file_checksum_gen_factory(ck_options, ck_factory);

    char ck_dbname[200];
    snprintf(ck_dbname, sizeof(ck_dbname),
             "%s/rocksdb_c_test-%d-livefiles-checksum", GetTempDir(),
             ((int)geteuid()));

    rocksdb_t* ck_db = rocksdb_open(ck_options, ck_dbname, &err);
    CheckNoError(err);

    // Write some data and flush to create SST files
    rocksdb_writeoptions_t* ck_woptions = rocksdb_writeoptions_create();
    rocksdb_put(ck_db, ck_woptions, "key1", 4, "val1", 4, &err);
    CheckNoError(err);
    rocksdb_put(ck_db, ck_woptions, "key2", 4, "val2", 4, &err);
    CheckNoError(err);

    rocksdb_flushoptions_t* ck_flush = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(ck_flush, 1);
    rocksdb_flush(ck_db, ck_flush, &err);
    CheckNoError(err);

    // Verify getters return populated checksum fields from live files
    const rocksdb_livefiles_t* lf = rocksdb_livefiles(ck_db);
    CheckCondition(rocksdb_livefiles_count(lf) > 0);

    for (int i = 0; i < rocksdb_livefiles_count(lf); i++) {
      const char* checksum = rocksdb_livefiles_file_checksum(lf, i);
      const char* func_name =
          rocksdb_livefiles_file_checksum_func_name(lf, i);
      CheckCondition(checksum != NULL && strlen(checksum) > 0);
      CheckCondition(strcmp(func_name, "FileChecksumCrc32c") == 0);
    }

    rocksdb_livefiles_destroy(lf);
    rocksdb_flushoptions_destroy(ck_flush);
    rocksdb_writeoptions_destroy(ck_woptions);
    rocksdb_close(ck_db);
    rocksdb_destroy_db(ck_options, ck_dbname, &err);
    CheckNoError(err);
    rocksdb_file_checksum_gen_factory_destroy(ck_factory);
    rocksdb_options_destroy(ck_options);
  }

  StartPhase("cancel_all_background_work");
  rocksdb_cancel_all_background_work(db, 1);

  StartPhase("generated_option_roundtrips");
  rocksdb_run_generated_roundtrip_tests();

  StartPhase("cleanup");
  rocksdb_close(db);
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(table_options);
  rocksdb_readoptions_destroy(roptions);
  rocksdb_writeoptions_destroy(woptions);
  rocksdb_compactoptions_destroy(coptions);
  rocksdb_cache_destroy(cache);
  rocksdb_comparator_destroy(cmp);
  rocksdb_dbpath_destroy(dbpath);
  rocksdb_env_destroy(env);

  fprintf(stderr, "PASS\n");
  return 0;
}
