# Write Stall and Pressure Events

**Files:** include/rocksdb/listener.h, db/job_context.h, db/db_impl/db_impl_compaction_flush.cc

## OnStallConditionsChanged

Called whenever a superversion change triggers a change in write stall conditions. This callback is dispatched from JobContext::Clean() (see db/job_context.h), which runs after the DB mutex is released.

The dispatch mechanism uses deferred notification: stall condition changes are collected into JobContext::write_stall_notifications while holding db_mutex_, then dispatched during Clean() without holding any mutex.

The WriteStallInfo struct provides:

| Field | Description |
|-------|-------------|
| cf_name | Column family name |
| condition.cur | Current WriteStallCondition |
| condition.prev | Previous WriteStallCondition |

WriteStallCondition (see include/rocksdb/advanced_options.h) has three states:

| Value | Meaning |
|-------|---------|
| kNormal | No write stall |
| kDelayed | Writes are being slowed down |
| kStopped | Writes are completely stopped |

This callback fires on every transition, including recovery from stall (e.g., kStopped -> kNormal).

## OnBackgroundJobPressureChanged (EXPERIMENTAL)

Called after every flush or compaction background job completes, providing a snapshot of current background job scheduling pressure and write-stall proximity. Dispatched from DBImpl::NotifyOnBackgroundJobPressureChanged().

The dispatch flow:

Step 1: Assert db_mutex_ is held
Step 2: Call CaptureBackgroundJobPressure() to snapshot current state
Step 3: Increment bg_pressure_callback_in_progress_ counter
Step 4: Release db_mutex_
Step 5: Call OnBackgroundJobPressureChanged() on each listener
Step 6: Reacquire db_mutex_ and decrement counter

Important: This callback fires on **every** background job completion, even if pressure values have not changed from the previous call. Implementations should compare with previous values if they only want to act on changes.

Note: This callback does NOT check the shutting_down_ flag, unlike most flush/compaction notification methods. Pressure callbacks can fire during shutdown.

### BackgroundJobPressure Struct

The BackgroundJobPressure struct (see include/rocksdb/listener.h) provides:

| Field | Description |
|-------|-------------|
| compaction_scheduled | Total compactions scheduled (LOW + BOTTOM priority) |
| compaction_running | Total compactions currently running |
| compaction_low_scheduled | LOW priority compactions scheduled |
| compaction_low_running | LOW priority compactions running |
| compaction_bottom_scheduled | BOTTOM priority compactions scheduled |
| compaction_bottom_running | BOTTOM priority compactions running |
| flush_scheduled | Flushes scheduled |
| flush_running | Flushes currently running |
| write_stall_proximity_pct | How close to write stall (0=healthy, 100=at threshold, >100=stalling) |
| compaction_speedup_active | Whether compaction speedup is engaged due to write pressure |

The write_stall_proximity_pct is computed as the maximum across all column families based on two write-stall triggers: L0 file count proximity to the slowdown trigger (or stop trigger as fallback), and pending compaction bytes proximity to the soft limit (or hard limit as fallback). A value of 0 means healthy, 100 means at the stall threshold, and values above 100 indicate active stalling.

### Shutdown Safety

DBImpl tracks bg_pressure_callback_in_progress_ (incremented before callbacks, decremented after). The destructor waits for this counter to reach zero before proceeding, ensuring no listener callback is in flight when the DB object is destroyed. A slow OnBackgroundJobPressureChanged implementation can therefore delay DB::Close().

### Use Case

This callback enables external systems to dynamically adjust compaction thread pool sizes or resource allocation based on real-time pressure signals. For example, an auto-scaler could expand the compaction thread pool when write_stall_proximity_pct exceeds a threshold.
