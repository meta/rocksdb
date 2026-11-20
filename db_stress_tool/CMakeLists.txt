add_executable(db_stress${ARTIFACT_SUFFIX}
  batched_ops_stress.cc
  cf_consistency_stress.cc
  db_stress.cc
  db_stress_compaction_service.cc
  db_stress_compression_manager.cc
  db_stress_common.cc
  db_stress_driver.cc
  db_stress_filters.cc
  db_stress_gflags.cc
  db_stress_listener.cc
  db_stress_shared_state.cc
  db_stress_structured_key_value.cc
  db_stress_test_base.cc
  db_stress_wide_merge_operator.cc
  db_stress_tool.cc
  expected_state.cc
  expected_value.cc
  multi_ops_txns_stress.cc
  no_batched_ops_stress.cc
  ../test_util/simple_external_table_factory.cc)
target_link_libraries(db_stress${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
list(APPEND tool_deps db_stress)
