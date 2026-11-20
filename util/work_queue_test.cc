//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

/*
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */
#include "util/work_queue.h"

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

// Unit test for work_queue.h.
//
// This file is an excerpt from Facebook's zstd repo at
// https://github.com/facebook/zstd/. The relevant file is
// contrib/pzstd/utils/test/WorkQueueTest.cpp.

struct Popper {
  WorkQueue<int>* queue;
  int* results;
  std::mutex* mutex;

  void operator()() {
    int result;
    while (queue->pop(result)) {
      std::lock_guard<std::mutex> lock(*mutex);
      results[result] = result;
    }
  }
};

TEST(WorkQueue, SingleThreaded) {
  WorkQueue<int> queue;
  int result;

  queue.push(5);
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(5, result);

  queue.push(1);
  queue.push(2);
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(1, result);
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(2, result);

  queue.push(1);
  queue.push(2);
  queue.finish();
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(1, result);
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(2, result);
  EXPECT_FALSE(queue.pop(result));

  queue.waitUntilFinished();
}

TEST(WorkQueue, SPSC) {
  WorkQueue<int> queue;
  const int max = 100;

  for (int i = 0; i < 10; ++i) {
    queue.push(i);
  }

  std::thread thread([&queue, max] {
    int result;
    for (int i = 0;; ++i) {
      if (!queue.pop(result)) {
        EXPECT_EQ(i, max);
        break;
      }
      EXPECT_EQ(i, result);
    }
  });

  std::this_thread::yield();
  for (int i = 10; i < max; ++i) {
    queue.push(i);
  }
  queue.finish();

  thread.join();
}

TEST(WorkQueue, SPMC) {
  WorkQueue<int> queue;
  std::vector<int> results(50, -1);
  std::mutex mutex;
  std::vector<std::thread> threads;
  threads.reserve(5);
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back(Popper{&queue, results.data(), &mutex});
  }

  for (int i = 0; i < 50; ++i) {
    queue.push(i);
  }
  queue.finish();

  for (auto& thread : threads) {
    thread.join();
  }

  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(i, results[i]);
  }
}

TEST(WorkQueue, MPMC) {
  WorkQueue<int> queue;
  std::vector<int> results(100, -1);
  std::mutex mutex;
  std::vector<std::thread> popperThreads;
  popperThreads.reserve(4);
  for (int i = 0; i < 4; ++i) {
    popperThreads.emplace_back(Popper{&queue, results.data(), &mutex});
  }

  std::vector<std::thread> pusherThreads;
  for (int i = 0; i < 2; ++i) {
    auto min = i * 50;
    auto max = (i + 1) * 50;
    pusherThreads.emplace_back([&queue, min, max] {
      for (int j = min; j < max; ++j) {
        queue.push(j);
      }
    });
  }

  for (auto& thread : pusherThreads) {
    thread.join();
  }
  queue.finish();

  for (auto& thread : popperThreads) {
    thread.join();
  }

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i, results[i]);
  }
}

TEST(WorkQueue, BoundedSizeWorks) {
  WorkQueue<int> queue(1);
  int result;
  queue.push(5);
  queue.pop(result);
  queue.push(5);
  queue.pop(result);
  queue.push(5);
  queue.finish();
  queue.pop(result);
  EXPECT_EQ(5, result);
}

TEST(WorkQueue, BoundedSizePushAfterFinish) {
  WorkQueue<int> queue(1);
  int result;
  queue.push(5);
  std::thread pusher([&queue] { queue.push(6); });
  // Dirtily try and make sure that pusher has run.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  queue.finish();
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(5, result);
  EXPECT_FALSE(queue.pop(result));

  pusher.join();
}

TEST(WorkQueue, SetMaxSize) {
  WorkQueue<int> queue(2);
  int result;
  queue.push(5);
  queue.push(6);
  queue.setMaxSize(1);
  std::thread pusher([&queue] { queue.push(7); });
  // Dirtily try and make sure that pusher has run.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  queue.finish();
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(5, result);
  EXPECT_TRUE(queue.pop(result));
  EXPECT_EQ(6, result);
  EXPECT_FALSE(queue.pop(result));

  pusher.join();
}

TEST(WorkQueue, BoundedSizeMPMC) {
  WorkQueue<int> queue(10);
  std::vector<int> results(200, -1);
  std::mutex mutex;
  std::cerr << "Creating popperThreads" << std::endl;
  std::vector<std::thread> popperThreads;
  popperThreads.reserve(4);
  for (int i = 0; i < 4; ++i) {
    popperThreads.emplace_back(Popper{&queue, results.data(), &mutex});
  }

  std::cerr << "Creating pusherThreads" << std::endl;
  std::vector<std::thread> pusherThreads;
  for (int i = 0; i < 2; ++i) {
    auto min = i * 100;
    auto max = (i + 1) * 100;
    pusherThreads.emplace_back([&queue, min, max] {
      for (int j = min; j < max; ++j) {
        queue.push(j);
      }
    });
  }

  std::cerr << "Joining pusherThreads" << std::endl;
  for (auto& thread : pusherThreads) {
    thread.join();
  }
  std::cerr << "Finishing queue" << std::endl;
  queue.finish();

  std::cerr << "Joining popperThreads" << std::endl;
  for (auto& thread : popperThreads) {
    thread.join();
  }

  std::cerr << "Inspecting results" << std::endl;
  for (int i = 0; i < 200; ++i) {
    EXPECT_EQ(i, results[i]);
  }
}

TEST(WorkQueue, FailedPush) {
  WorkQueue<int> queue;
  EXPECT_TRUE(queue.push(1));
  queue.finish();
  EXPECT_FALSE(queue.push(1));
}

TEST(WorkQueue, FailedPop) {
  WorkQueue<int> queue;
  int x = 5;
  EXPECT_TRUE(queue.push(x));
  queue.finish();
  x = 0;
  EXPECT_TRUE(queue.pop(x));
  EXPECT_EQ(5, x);
  EXPECT_FALSE(queue.pop(x));
  EXPECT_EQ(5, x);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
