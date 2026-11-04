//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <vector>

#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Tracks synchronous lifecycle callbacks for dependencies on active listener
// leases. Callback delivery remains synchronous and on the calling thread.
class ScopedEventListenerCallback {
 public:
  explicit ScopedEventListenerCallback(const EventListener* listener);
  ~ScopedEventListenerCallback();
  ScopedEventListenerCallback(const ScopedEventListenerCallback&) = delete;
  ScopedEventListenerCallback& operator=(const ScopedEventListenerCallback&) =
      delete;
  ScopedEventListenerCallback(ScopedEventListenerCallback&&) = delete;
  ScopedEventListenerCallback& operator=(ScopedEventListenerCallback&&) =
      delete;

  const EventListener* listener() const { return listener_; }
  const ScopedEventListenerCallback* previous() const { return previous_; }

 private:
  const EventListener* listener_;
  const ScopedEventListenerCallback* previous_;
};

// Construct and destroy on the same thread, outside DB locks. Existing
// I/O callbacks finish before the scope starts; subsequent callbacks for these
// listeners, including worker-thread callbacks, are delivered at scope exit.
// Scopes sharing listeners are exclusive through callback delivery. Admission
// rejects leases whose release depends on this thread or its active callbacks.
// Check the construction status before taking DB locks.
class ScopedDeferredFileIOListeners {
 public:
  struct State;
  using FileCallback = void (EventListener::*)(const FileOperationInfo&);

  ScopedDeferredFileIOListeners(
      bool defer, const std::vector<std::shared_ptr<EventListener>>& listeners,
      Status* status);
  ~ScopedDeferredFileIOListeners();

  ScopedDeferredFileIOListeners(const ScopedDeferredFileIOListeners&) = delete;
  ScopedDeferredFileIOListeners& operator=(
      const ScopedDeferredFileIOListeners&) = delete;
  ScopedDeferredFileIOListeners(ScopedDeferredFileIOListeners&&) = delete;
  ScopedDeferredFileIOListeners& operator=(ScopedDeferredFileIOListeners&&) =
      delete;

  static void Notify(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      FileCallback callback, const FileOperationInfo& info);
  static void NotifyError(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const IOErrorInfo& info);

  static bool HasReentrantLeaseDependency(
      const std::vector<std::shared_ptr<EventListener>>& listeners);

 private:
  std::shared_ptr<State> state_;
};

}  // namespace ROCKSDB_NAMESPACE
