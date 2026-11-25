# This file is used by Meta-internal infrastructure as well as by Makefile

# When included from Makefile, there are rules to build DB_STRESS_CMD. When
# used directly with `make -f crashtest.mk ...` there will be no rules to
# build DB_STRESS_CMD so it must exist prior.
DB_STRESS_CMD?=./db_stress

include common.mk

ifdef COMPILE_WITH_TSAN
	# Keep direct `make -f crash_test.mk COMPILE_WITH_TSAN=1 ...` runs
	# aligned with the main Makefile's TSAN runtime options.
	TSAN_OPTIONS?=suppressions=$(CURDIR)/tools/tsan_suppressions.txt
	export TSAN_OPTIONS
endif

CRASHTEST_MAKE=$(MAKE) -f crash_test.mk
CRASHTEST_PY=$(PYTHON) -u tools/db_crashtest.py --stress_cmd=$(DB_STRESS_CMD) --cleanup_cmd='$(DB_CLEANUP_CMD)' --destroy_db_initially=1
export CRASH_TEST_EXT_ARGS

.PHONY: crash_test crash_test_with_atomic_flush crash_test_with_txn \
	crash_test_with_wc_txn crash_test_with_wp_txn crash_test_with_wup_txn \
	crash_test_with_best_efforts_recovery crash_test_with_ts \
	crash_test_with_multiops_wc_txn \
	crash_test_with_multiops_wp_txn \
	crash_test_with_multiops_wup_txn \
	crash_test_with_optimistic_txn \
	crash_test_with_tiered_storage \
	crash_test_with_full_external_table \
	liveness_crash_test \
	simple_blackbox_crash_test std_blackbox_crash_test \
	simple_whitebox_crash_test std_whitebox_crash_test \
	blackbox_crash_test blackbox_crash_test_with_atomic_flush \
	blackbox_crash_test_with_wc_txn blackbox_crash_test_with_wp_txn \
	blackbox_crash_test_with_wup_txn \
	blackbox_crash_test_with_txn blackbox_crash_test_with_ts \
	blackbox_crash_test_with_best_efforts_recovery \
	blackbox_crash_test_with_multiops_wc_txn \
	blackbox_crash_test_with_multiops_wp_txn \
	blackbox_crash_test_with_multiops_wup_txn \
	blackbox_crash_test_with_optimistic_txn \
	blackbox_crash_test_with_tiered_storage \
	blackbox_crash_test_with_full_external_table \
	whitebox_crash_test whitebox_crash_test_with_atomic_flush \
	whitebox_crash_test_with_wc_txn whitebox_crash_test_with_wp_txn \
	whitebox_crash_test_with_wup_txn \
	whitebox_crash_test_with_txn whitebox_crash_test_with_ts \
	whitebox_crash_test_with_optimistic_txn \
	whitebox_crash_test_with_tiered_storage \
	crash_test_db_cleanup \

crash_test: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test
	$(CRASHTEST_MAKE) blackbox_crash_test

crash_test_with_atomic_flush: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_atomic_flush
	$(CRASHTEST_MAKE) blackbox_crash_test_with_atomic_flush

crash_test_with_wc_txn: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_wc_txn
	$(CRASHTEST_MAKE) blackbox_crash_test_with_wc_txn

crash_test_with_wp_txn: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_wp_txn
	$(CRASHTEST_MAKE) blackbox_crash_test_with_wp_txn

crash_test_with_wup_txn: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_wup_txn
	$(CRASHTEST_MAKE) blackbox_crash_test_with_wup_txn

crash_test_with_optimistic_txn: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_optimistic_txn
	$(CRASHTEST_MAKE) blackbox_crash_test_with_optimistic_txn

crash_test_with_best_efforts_recovery: blackbox_crash_test_with_best_efforts_recovery

crash_test_with_ts: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_ts
	$(CRASHTEST_MAKE) blackbox_crash_test_with_ts

crash_test_with_tiered_storage: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) whitebox_crash_test_with_tiered_storage
	$(CRASHTEST_MAKE) blackbox_crash_test_with_tiered_storage

crash_test_with_full_external_table: blackbox_crash_test_with_full_external_table

crash_test_with_multiops_wc_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_MAKE) blackbox_crash_test_with_multiops_wc_txn

crash_test_with_multiops_wp_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_MAKE) blackbox_crash_test_with_multiops_wp_txn

crash_test_with_multiops_wup_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_MAKE) blackbox_crash_test_with_multiops_wup_txn

liveness_crash_test: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) liveness $(CRASH_TEST_EXT_ARGS)

# Leaf targets: exactly one db_crashtest.py invocation each, so a CI system can
# schedule them as separate jobs. blackbox_crash_test remains the aggregate for
# open source CI and local runs.
simple_blackbox_crash_test: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --simple blackbox $(CRASH_TEST_EXT_ARGS)

std_blackbox_crash_test: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) simple_blackbox_crash_test
	$(CRASHTEST_MAKE) std_blackbox_crash_test

blackbox_crash_test_with_atomic_flush: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --cf_consistency blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_wc_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn blackbox --txn_write_policy 0 $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_wp_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn blackbox --txn_write_policy 1 $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_wup_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn blackbox --txn_write_policy 2 $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_best_efforts_recovery: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_best_efforts_recovery blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_ts: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --enable_ts blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_multiops_wc_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_multiops_txn --txn_write_policy 0 blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_multiops_wp_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_multiops_txn --txn_write_policy 1 blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_multiops_wup_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_multiops_txn --txn_write_policy 2 blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_tiered_storage: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_tiered_storage blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_full_external_table: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_full_external_table blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_optimistic_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --optimistic_txn blackbox $(CRASH_TEST_EXT_ARGS)

ifeq ($(CRASH_TEST_KILL_ODD),)
  CRASH_TEST_KILL_ODD=888887
endif

# Leaf targets: exactly one db_crashtest.py invocation each, so a CI system can
# schedule them as separate jobs. whitebox_crash_test remains the aggregate for
# open source CI and local runs.
simple_whitebox_crash_test: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --simple whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

std_whitebox_crash_test: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) whitebox  --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test: $(DB_STRESS_CMD)
# Do not parallelize
	$(CRASHTEST_MAKE) simple_whitebox_crash_test
	$(CRASHTEST_MAKE) std_whitebox_crash_test

whitebox_crash_test_with_atomic_flush: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --cf_consistency whitebox  --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_wc_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn whitebox --txn_write_policy 0 \
	  --random_kill_odd $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_wp_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn whitebox --txn_write_policy 1 \
      --random_kill_odd $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_wup_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --txn whitebox --txn_write_policy 2 \
      --random_kill_odd $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_ts: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --enable_ts whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_tiered_storage: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --test_tiered_storage whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_optimistic_txn: $(DB_STRESS_CMD)
	$(CRASHTEST_PY) --optimistic_txn whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

crash_test_db_cleanup: $(DB_STRESS_CMD)
	$(DB_STRESS_CMD) --delete_dir_and_exit=$(TEST_TMPDIR)

# Old names DEPRECATED
crash_test_with_txn: crash_test_with_wc_txn
whitebox_crash_test_with_txn: whitebox_crash_test_with_wc_txn
blackbox_crash_test_with_txn: blackbox_crash_test_with_wc_txn
