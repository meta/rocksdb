# Registration and Configuration

**Files:** include/rocksdb/listener.h, include/rocksdb/options.h, db/event_helpers.cc

## Registering Listeners

Listeners are registered by adding std::shared_ptr<EventListener> instances to DBOptions::listeners before opening the database. The listeners vector is copied into ImmutableDBOptions at DB open time and cannot be modified afterward.

The listeners field is declared in DBOptions (see include/rocksdb/options.h). Multiple listeners can be registered; all receive every event.

## EventListener Base Class

EventListener extends Customizable (see include/rocksdb/customizable.h), which provides:

- **Name()**: Returns the listener name. Default is empty string; implementations should override for identification.
- **CreateFromString()**: Factory method enabling construction from option strings (see EventListener::CreateFromString() in db/event_helpers.cc). Uses LoadSharedObject<EventListener>() internally.

## Listener Lifecycle

| Phase | Behavior |
|-------|----------|
| Registration | Add to DBOptions::listeners before DB::Open() |
| Active | All callbacks invoked for every event on this DB instance |
| Shutdown | Some callbacks are skipped when shutting_down_ flag is set (flush, compaction, subcompaction, manual flush); others continue to fire (file I/O, error handling, pressure, external file ingestion, column family deletion) |
| Close | Listeners are released when DBImpl is destroyed |

Important: Listeners are shared pointers, so the same listener instance can be shared across multiple DB instances if desired.

## Selective Override Pattern

All callback methods have empty default implementations (no-ops). Implement only the callbacks you need:

| To observe... | Override... |
|---------------|-------------|
| Flush events | OnFlushBegin(), OnFlushCompleted() |
| Compaction events | OnCompactionBegin(), OnCompactionCompleted() |
| File I/O | ShouldBeNotifiedOnFileIO() + OnFileReadFinish(), etc. |
| Background errors | OnBackgroundError() |
| Write stalls | OnStallConditionsChanged() |
