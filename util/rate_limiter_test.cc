//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <limits>

#include "db/db_test_util.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

// TODO(yhchiang): the rate will not be accurate when we run test in parallel.
class RateLimiterTest : public testing::Test {
 public:
  ~RateLimiterTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

TEST_F(RateLimiterTest, OverflowRate) {
  GenericRateLimiter limiter(std::numeric_limits<int64_t>::max(), 1000, 10,
                             RateLimiter::Mode::kWritesOnly,
                             SystemClock::Default(), false /* auto_tuned */,
                             0 /* single_burst_bytes */);
  ASSERT_GT(limiter.GetSingleBurstBytes(), 1000000000ll);
}

TEST_F(RateLimiterTest, StartStop) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(100, 100, 10));
}

TEST_F(RateLimiterTest, GetTotalBytesThrough) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(
      200 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
      10 /* fairness */));
  for (int i = Env::IO_LOW; i <= Env::IO_TOTAL; ++i) {
    ASSERT_EQ(limiter->GetTotalBytesThrough(static_cast<Env::IOPriority>(i)),
              0);
  }

  std::int64_t request_byte = 200;
  std::int64_t request_byte_sum = 0;
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    limiter->Request(request_byte, static_cast<Env::IOPriority>(i),
                     nullptr /* stats */, RateLimiter::OpType::kWrite);
    request_byte_sum += request_byte;
  }

  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    EXPECT_EQ(limiter->GetTotalBytesThrough(static_cast<Env::IOPriority>(i)),
              request_byte)
        << "Failed to track total_bytes_through_ correctly when IOPriority = "
        << static_cast<Env::IOPriority>(i);
  }
  EXPECT_EQ(limiter->GetTotalBytesThrough(Env::IO_TOTAL), request_byte_sum)
      << "Failed to track total_bytes_through_ correctly when IOPriority = "
         "Env::IO_TOTAL";
}

TEST_F(RateLimiterTest, GetTotalRequests) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(
      200 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
      10 /* fairness */));
  for (int i = Env::IO_LOW; i <= Env::IO_TOTAL; ++i) {
    ASSERT_EQ(limiter->GetTotalRequests(static_cast<Env::IOPriority>(i)), 0);
  }

  std::int64_t total_requests_sum = 0;
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    limiter->Request(200, static_cast<Env::IOPriority>(i), nullptr /* stats */,
                     RateLimiter::OpType::kWrite);
    total_requests_sum += 1;
  }

  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    EXPECT_EQ(limiter->GetTotalRequests(static_cast<Env::IOPriority>(i)), 1)
        << "Failed to track total_requests_ correctly when IOPriority = "
        << static_cast<Env::IOPriority>(i);
  }
  EXPECT_EQ(limiter->GetTotalRequests(Env::IO_TOTAL), total_requests_sum)
      << "Failed to track total_requests_ correctly when IOPriority = "
         "Env::IO_TOTAL";
}

TEST_F(RateLimiterTest, GetTotalPendingRequests) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(
      200 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
      10 /* fairness */));
  int64_t total_pending_requests = 0;
  for (int i = Env::IO_LOW; i <= Env::IO_TOTAL; ++i) {
    ASSERT_OK(limiter->GetTotalPendingRequests(
        &total_pending_requests, static_cast<Env::IOPriority>(i)));
    ASSERT_EQ(total_pending_requests, 0);
  }
  // This is a variable for making sure the following callback is called
  // and the assertions in it are indeed excuted
  bool nonzero_pending_requests_verified = false;
  SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:PostEnqueueRequest", [&](void* arg) {
        port::Mutex* request_mutex = (port::Mutex*)arg;
        // We temporarily unlock the mutex so that the following
        // GetTotalPendingRequests() can acquire it
        request_mutex->Unlock();
        for (int i = Env::IO_LOW; i <= Env::IO_TOTAL; ++i) {
          EXPECT_OK(limiter->GetTotalPendingRequests(
              &total_pending_requests, static_cast<Env::IOPriority>(i)))
              << "Failed to return total pending requests for priority level = "
              << static_cast<Env::IOPriority>(i);
          if (i == Env::IO_USER || i == Env::IO_TOTAL) {
            EXPECT_EQ(total_pending_requests, 1)
                << "Failed to correctly return total pending requests for "
                   "priority level = "
                << static_cast<Env::IOPriority>(i);
          } else {
            EXPECT_EQ(total_pending_requests, 0)
                << "Failed to correctly return total pending requests for "
                   "priority level = "
                << static_cast<Env::IOPriority>(i);
          }
        }
        // We lock the mutex again so that the request thread can resume running
        // with the mutex locked
        request_mutex->Lock();
        nonzero_pending_requests_verified = true;
      });

  SyncPoint::GetInstance()->EnableProcessing();
  limiter->Request(200, Env::IO_USER, nullptr /* stats */,
                   RateLimiter::OpType::kWrite);
  ASSERT_EQ(nonzero_pending_requests_verified, true);
  for (int i = Env::IO_LOW; i <= Env::IO_TOTAL; ++i) {
    EXPECT_OK(limiter->GetTotalPendingRequests(&total_pending_requests,
                                               static_cast<Env::IOPriority>(i)))
        << "Failed to return total pending requests for priority level = "
        << static_cast<Env::IOPriority>(i);
    EXPECT_EQ(total_pending_requests, 0)
        << "Failed to correctly return total pending requests for priority "
           "level = "
        << static_cast<Env::IOPriority>(i);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "GenericRateLimiter::Request:PostEnqueueRequest");
}

TEST_F(RateLimiterTest, Modes) {
  for (auto mode : {RateLimiter::Mode::kWritesOnly,
                    RateLimiter::Mode::kReadsOnly, RateLimiter::Mode::kAllIo}) {
    GenericRateLimiter limiter(
        2000 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
        10 /* fairness */, mode, SystemClock::Default(), false /* auto_tuned */,
        0 /* single_burst_bytes */);
    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kRead);
    if (mode == RateLimiter::Mode::kWritesOnly) {
      ASSERT_EQ(0, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }

    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kWrite);
    if (mode == RateLimiter::Mode::kAllIo) {
      ASSERT_EQ(2000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }
  }
}

TEST_F(RateLimiterTest, Statistics) {
  constexpr int64_t kBytesPerSecond = 100;
  constexpr int64_t kMicrosPerSecond = 1000 * 1000;
  constexpr int64_t kBytesPerRefill = kBytesPerSecond;

  auto stats = CreateDBStatistics();
  auto mock_clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());
  GenericRateLimiter write_limiter(
      kBytesPerSecond, kMicrosPerSecond, 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, mock_clock, false /* auto_tuned */,
      0 /* single_burst_bytes */);

  write_limiter.Request(1 /* bytes */, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kRead);
  EXPECT_EQ(0, stats->getTickerCount(RATE_LIMITER_BYTES_READ));
  EXPECT_EQ(0, stats->getTickerCount(RATE_LIMITER_REQUESTS_READ));

  write_limiter.Request(kBytesPerRefill, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  EXPECT_EQ(kBytesPerRefill, stats->getTickerCount(RATE_LIMITER_BYTES_WRITE));
  EXPECT_EQ(1, stats->getTickerCount(RATE_LIMITER_REQUESTS_WRITE));
  EXPECT_EQ(0, stats->getTickerCount(RATE_LIMITER_DELAYED_REQUESTS_WRITE));
  EXPECT_EQ(0, stats->getTickerCount(RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE));

  HistogramData hist;
  stats->histogramData(RATE_LIMITER_WAIT_MICROS_WRITE, &hist);
  EXPECT_EQ(0, hist.count);

  write_limiter.Request(1 /* bytes */, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  EXPECT_EQ(kBytesPerRefill + 1,
            stats->getTickerCount(RATE_LIMITER_BYTES_WRITE));
  EXPECT_EQ(2, stats->getTickerCount(RATE_LIMITER_REQUESTS_WRITE));
  EXPECT_EQ(1, stats->getTickerCount(RATE_LIMITER_DELAYED_REQUESTS_WRITE));
  EXPECT_GT(stats->getTickerCount(NUMBER_RATE_LIMITER_DRAINS), 0);
  EXPECT_GT(stats->getTickerCount(RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE), 0);

  stats->histogramData(RATE_LIMITER_WAIT_MICROS_WRITE, &hist);
  EXPECT_EQ(1, hist.count);
  EXPECT_GT(hist.sum, 0);

  auto read_stats = CreateDBStatistics();
  auto read_mock_clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());
  GenericRateLimiter read_limiter(kBytesPerSecond, kMicrosPerSecond,
                                  10 /* fairness */, RateLimiter::Mode::kAllIo,
                                  read_mock_clock, false /* auto_tuned */,
                                  0 /* single_burst_bytes */);

  read_limiter.Request(10 /* bytes */, Env::IO_HIGH, read_stats.get(),
                       RateLimiter::OpType::kRead);
  EXPECT_EQ(10, read_stats->getTickerCount(RATE_LIMITER_BYTES_READ));
  EXPECT_EQ(1, read_stats->getTickerCount(RATE_LIMITER_REQUESTS_READ));
  EXPECT_EQ(0, read_stats->getTickerCount(RATE_LIMITER_BYTES_WRITE));
  EXPECT_EQ(0, read_stats->getTickerCount(RATE_LIMITER_REQUESTS_WRITE));
}

TEST_F(RateLimiterTest, GeneratePriorityIterationOrder) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(
      200 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
      10 /* fairness */));

  bool possible_random_one_in_fairness_results_for_high_mid_pri[4][2] = {
      {false, false}, {false, true}, {true, false}, {true, true}};
  std::vector<Env::IOPriority> possible_priority_iteration_orders[4] = {
      {Env::IO_USER, Env::IO_HIGH, Env::IO_MID, Env::IO_LOW},
      {Env::IO_USER, Env::IO_HIGH, Env::IO_LOW, Env::IO_MID},
      {Env::IO_USER, Env::IO_MID, Env::IO_LOW, Env::IO_HIGH},
      {Env::IO_USER, Env::IO_LOW, Env::IO_MID, Env::IO_HIGH}};

  for (int i = 0; i < 4; ++i) {
    // These are variables for making sure the following callbacks are called
    // and the assertion in the last callback is indeed excuted
    bool high_pri_iterated_after_mid_low_pri_set = false;
    bool mid_pri_itereated_after_low_pri_set = false;
    bool pri_iteration_order_verified = false;
    SyncPoint::GetInstance()->SetCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PostRandomOneInFairnessForHighPri",
        [&](void* arg) {
          bool* high_pri_iterated_after_mid_low_pri = (bool*)arg;
          *high_pri_iterated_after_mid_low_pri =
              possible_random_one_in_fairness_results_for_high_mid_pri[i][0];
          high_pri_iterated_after_mid_low_pri_set = true;
        });

    SyncPoint::GetInstance()->SetCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PostRandomOneInFairnessForMidPri",
        [&](void* arg) {
          bool* mid_pri_itereated_after_low_pri = (bool*)arg;
          *mid_pri_itereated_after_low_pri =
              possible_random_one_in_fairness_results_for_high_mid_pri[i][1];
          mid_pri_itereated_after_low_pri_set = true;
        });

    SyncPoint::GetInstance()->SetCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PreReturnPriIterationOrder",
        [&](void* arg) {
          std::vector<Env::IOPriority>* pri_iteration_order =
              (std::vector<Env::IOPriority>*)arg;
          EXPECT_EQ(*pri_iteration_order, possible_priority_iteration_orders[i])
              << "Failed to generate priority iteration order correctly when "
                 "high_pri_iterated_after_mid_low_pri = "
              << possible_random_one_in_fairness_results_for_high_mid_pri[i][0]
              << ", mid_pri_itereated_after_low_pri = "
              << possible_random_one_in_fairness_results_for_high_mid_pri[i][1]
              << std::endl;
          pri_iteration_order_verified = true;
        });

    SyncPoint::GetInstance()->EnableProcessing();
    limiter->Request(200 /* request max bytes to drain so that refill and order
                           generation will be triggered every time
                           GenericRateLimiter::Request() is called */
                     ,
                     Env::IO_USER, nullptr /* stats */,
                     RateLimiter::OpType::kWrite);
    ASSERT_EQ(high_pri_iterated_after_mid_low_pri_set, true);
    ASSERT_EQ(mid_pri_itereated_after_low_pri_set, true);
    ASSERT_EQ(pri_iteration_order_verified, true);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PreReturnPriIterationOrder");
    SyncPoint::GetInstance()->ClearCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PostRandomOneInFairnessForMidPri");
    SyncPoint::GetInstance()->ClearCallBack(
        "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
        "PostRandomOneInFairnessForHighPri");
  }
}

TEST_F(RateLimiterTest, Rate) {
  auto* env = Env::Default();
  struct Arg {
    Arg(int32_t _target_rate, int _burst)
        : limiter(NewGenericRateLimiter(_target_rate /* rate_bytes_per_sec */,
                                        100 * 1000 /* refill_period_us */,
                                        10 /* fairness */)),
          request_size(_target_rate /
                       10 /* refill period here is 1/10 second */),
          burst(_burst) {}
    std::unique_ptr<RateLimiter> limiter;
    int32_t request_size;
    int burst;
  };

  auto writer = [](void* p) {
    const auto& thread_clock = SystemClock::Default();
    auto* arg = static_cast<Arg*>(p);
    // Test for 2 seconds
    auto until = thread_clock->NowMicros() + 2 * 1000000;
    Random r((uint32_t)(thread_clock->NowNanos() %
                        std::numeric_limits<uint32_t>::max()));
    while (thread_clock->NowMicros() < until) {
      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst * 2) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_USER, nullptr /* stats */,
                              RateLimiter::OpType::kWrite);
      }

      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_HIGH, nullptr /* stats */,
                              RateLimiter::OpType::kWrite);
      }

      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst / 2 + 1) + 1);
           ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_MID,
                              nullptr /* stats */, RateLimiter::OpType::kWrite);
      }

      arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_LOW,
                            nullptr /* stats */, RateLimiter::OpType::kWrite);
    }
  };

  int samples = 0;
  int samples_at_minimum = 0;

  for (int i = 1; i <= 16; i *= 2) {
    int32_t target = i * 1024 * 10;
    Arg arg(target, i / 4 + 1);
    int64_t old_total_bytes_through = 0;
    for (int iter = 1; iter <= 2; ++iter) {
      // second iteration changes the target dynamically
      if (iter == 2) {
        target *= 2;
        arg.limiter->SetBytesPerSecond(target);
      }
      auto start = env->NowMicros();
      for (int t = 0; t < i; ++t) {
        env->StartThread(writer, &arg);
      }
      env->WaitForJoin();

      auto elapsed = env->NowMicros() - start;
      double rate =
          (arg.limiter->GetTotalBytesThrough() - old_total_bytes_through) *
          1000000.0 / elapsed;
      old_total_bytes_through = arg.limiter->GetTotalBytesThrough();
      fprintf(stderr,
              "request size [1 - %" PRIi32 "], limit %" PRIi32
              " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
              arg.request_size - 1, target / 1024, rate / 1024,
              elapsed / 1000000.0);

      ++samples;
      if (rate / target >= 0.80) {
        ++samples_at_minimum;
      }
      ASSERT_LE(rate / target, 1.25);
    }
  }

  // This can fail due to slow execution speed, like when using valgrind or in
  // heavily loaded CI environments
  bool skip_minimum_rate_check =
#if defined(ROCKSDB_VALGRIND_RUN)
      true;
#elif defined(OS_MACOSX)
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      getenv("CIRCLECI") || getenv("GITHUB_ACTIONS");
#elif defined(__aarch64__) || defined(__arm__)
      // Unlike x86_64, the small/loaded ARM CI runners can't reliably sustain
      // the target rate, so also exempt GitHub Actions here.
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      getenv("SANDCASTLE") || getenv("GITHUB_ACTIONS");
#else
      getenv("SANDCASTLE");  // NOLINT(concurrency-mt-unsafe)
#endif
  if (skip_minimum_rate_check) {
    fprintf(stderr, "Skipped minimum rate check (%d / %d passed)\n",
            samples_at_minimum, samples);
  } else {
    ASSERT_EQ(samples_at_minimum, samples);
  }
}

TEST_F(RateLimiterTest, LimitChangeTest) {
  // starvation test when limit changes to a smaller value
  int64_t refill_period = 1000 * 1000;
  auto* env = Env::Default();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  struct Arg {
    Arg(int32_t _request_size, Env::IOPriority _pri,
        std::shared_ptr<RateLimiter> _limiter)
        : request_size(_request_size), pri(_pri), limiter(_limiter) {}
    int32_t request_size;
    Env::IOPriority pri;
    std::shared_ptr<RateLimiter> limiter;
  };

  auto writer = [](void* p) {
    auto* arg = static_cast<Arg*>(p);
    arg->limiter->Request(arg->request_size, arg->pri, nullptr /* stats */,
                          RateLimiter::OpType::kWrite);
  };

  for (uint32_t i = 1; i <= 16; i <<= 1) {
    int32_t target = i * 1024 * 10;
    // refill per second
    for (int iter = 0; iter < 2; iter++) {
      std::shared_ptr<RateLimiter> limiter =
          std::make_shared<GenericRateLimiter>(
              target, refill_period, 10, RateLimiter::Mode::kWritesOnly,
              SystemClock::Default(), false /* auto_tuned */,
              0 /* single_burst_bytes */);
      // After "GenericRateLimiter::Request:1" the mutex is held until the bytes
      // are refilled. This test could be improved to change the limit when lock
      // is released in `TimedWait()`.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"GenericRateLimiter::Request",
            "RateLimiterTest::LimitChangeTest:changeLimitStart"},
           {"RateLimiterTest::LimitChangeTest:changeLimitEnd",
            "GenericRateLimiter::Request:1"}});
      Arg arg(target, Env::IO_HIGH, limiter);
      // The idea behind is to start a request first, then before it refills,
      // update limit to a different value (2X/0.5X). No starvation should
      // be guaranteed under any situation
      // TODO(lightmark): more test cases are welcome.
      env->StartThread(writer, &arg);
      int32_t new_limit = (target << 1) >> (iter << 1);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitStart");
      arg.limiter->SetBytesPerSecond(new_limit);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitEnd");
      env->WaitForJoin();
      fprintf(stderr,
              "[COMPLETE] request size %" PRIi32 " KB, new limit %" PRIi32
              "KB/sec, refill period %" PRIi64 " ms\n",
              target / 1024, new_limit / 1024, refill_period / 1000);
    }
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(RateLimiterTest, AvailableByteSizeExhaustTest) {
  SpecialEnv special_env(Env::Default(), /*time_elapse_only_sleep*/ true);
  const std::chrono::seconds kTimePerRefill(1);

  // This test makes sure available_bytes_ get exhausted first before queuing
  // any remaining bytes when requested_bytes > available_bytes
  const int64_t available_bytes_per_period = 500;

  std::shared_ptr<RateLimiter> limiter = std::make_shared<GenericRateLimiter>(
      available_bytes_per_period,
      std::chrono::microseconds(kTimePerRefill).count(), 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, special_env.GetSystemClock(),
      false /* auto_tuned */, 0 /* single_burst_bytes */);

  // Step 1. Request 100 and wait for the refill
  // so that the remaining available bytes are 400
  limiter->Request(100, Env::IO_USER, nullptr /* stats */,
                   RateLimiter::OpType::kWrite);
  special_env.SleepForMicroseconds(
      static_cast<int>(std::chrono::microseconds(kTimePerRefill).count()));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:PostEnqueueRequest", [&](void* arg) {
        port::Mutex* request_mutex = (port::Mutex*)arg;
        request_mutex->Unlock();
        // Step 3. Check GetTotalBytesThrough = available_bytes_per_period
        // to make sure that the first request (100) and the part of the second
        // request (400) made through when the remaining of the second request
        // got queued
        ASSERT_EQ(available_bytes_per_period,
                  limiter->GetTotalBytesThrough(Env::IO_USER));
        request_mutex->Lock();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Step 2. Request 500, which is greater than the remaining available bytes
  // (400)
  limiter->Request(500, Env::IO_USER, nullptr /* stats */,
                   RateLimiter::OpType::kWrite);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "GenericRateLimiter::Request:PostEnqueueRequest");
}

TEST_F(RateLimiterTest, AutoTuneIncreaseWhenFull) {
  const std::chrono::seconds kTimePerRefill(1);
  const int kRefillsPerTune = 100;  // needs to match util/rate_limiter.cc

  auto mock_clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());

  auto stats = CreateDBStatistics();
  std::unique_ptr<RateLimiter> rate_limiter(new GenericRateLimiter(
      1000 /* rate_bytes_per_sec */,
      std::chrono::microseconds(kTimePerRefill).count(), 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, mock_clock, true /* auto_tuned */,
      0 /* single_burst_bytes */));

  // verify rate limit increases after a sequence of periods where rate limiter
  // is always drained
  int64_t orig_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  rate_limiter->Request(orig_bytes_per_sec, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  while (std::chrono::microseconds(mock_clock->NowMicros()) <=
         kRefillsPerTune * kTimePerRefill) {
    rate_limiter->Request(orig_bytes_per_sec, Env::IO_HIGH, stats.get(),
                          RateLimiter::OpType::kWrite);
  }
  int64_t new_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  ASSERT_GT(new_bytes_per_sec, orig_bytes_per_sec);

  // decreases after a sequence of periods where rate limiter is not drained
  orig_bytes_per_sec = new_bytes_per_sec;
  mock_clock->SleepForMicroseconds(static_cast<int>(
      kRefillsPerTune * std::chrono::microseconds(kTimePerRefill).count()));
  // make a request so tuner can be triggered
  rate_limiter->Request(1 /* bytes */, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  new_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  ASSERT_LT(new_bytes_per_sec, orig_bytes_per_sec);
}

TEST_F(RateLimiterTest, WaitHangingBug) {
  // At t=0: Threads 0 and 1 request `kBytesPerRefill` bytes at low-pri. One
  // will be granted immediately and the other will enter `TimedWait()`.
  //
  // At t=`kMicrosPerRefill`: Thread 2 requests `kBytesPerRefill` bytes at
  // low-pri. Thread 2's request enters the queue. To expose the bug scenario,
  // `SyncPoint`s ensure this happens while the lock is temporarily released in
  // `TimedWait()`. Before the bug fix, Thread 2's request would then hang in
  // `Wait()` interminably.
  const int kBytesPerSecond = 100;
  const int kMicrosPerSecond = 1000 * 1000;
  const int kMicrosPerRefill = kMicrosPerSecond;
  const int kBytesPerRefill =
      kBytesPerSecond * kMicrosPerRefill / kMicrosPerSecond;

  auto mock_clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());
  std::unique_ptr<RateLimiter> limiter(new GenericRateLimiter(
      kBytesPerSecond, kMicrosPerRefill, 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, mock_clock, false /* auto_tuned */,
      0 /* single_burst_bytes */));
  std::array<std::thread, 3> request_threads;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RateLimiterTest::WaitHangingBug:InitialRequestsReady",
        "MockSystemClock::TimedWait:UnlockedPreSleep"},
       {"MockSystemClock::TimedWait:UnlockedPostSleep1",
        "RateLimiterTest::WaitHangingBug:TestThreadRequestBegin"},
       {"RateLimiterTest::WaitHangingBug:TestThreadRequestEnd",
        "MockSystemClock::TimedWait:UnlockedPostSleep2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 2; i++) {
    request_threads[i] = std::thread([&]() {
      limiter->Request(kBytesPerRefill /* bytes */, Env::IOPriority::IO_LOW,
                       nullptr /* stats */, RateLimiter::OpType::kWrite);
    });
  }
  while (limiter->GetTotalRequests() < 2) {
  }
  TEST_SYNC_POINT("RateLimiterTest::WaitHangingBug:InitialRequestsReady");

  TEST_SYNC_POINT("RateLimiterTest::WaitHangingBug:TestThreadRequestBegin");
  request_threads[2] = std::thread([&]() {
    limiter->Request(kBytesPerRefill /* bytes */, Env::IOPriority::IO_LOW,
                     nullptr /* stats */, RateLimiter::OpType::kWrite);
  });
  while (limiter->GetTotalRequests() < 3) {
  }
  TEST_SYNC_POINT("RateLimiterTest::WaitHangingBug:TestThreadRequestEnd");

  for (int i = 0; i < 3; i++) {
    request_threads[i].join();
  }
}

TEST_F(RateLimiterTest, RuntimeSingleBurstBytesChange) {
  constexpr int kMicrosecondsPerSecond = 1000000;

  const int64_t kRateBytesPerSec = 400;
  const int64_t kRefillBytes = 100;

  const int64_t kRefillPeriodMicros =
      kRefillBytes * kMicrosecondsPerSecond / kRateBytesPerSec;

  const int64_t kRefillsPerBurst = 17;
  const int64_t kBurstBytes = kRefillBytes * kRefillsPerBurst;

  auto mock_clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());

  // Zero as `single_burst_bytes` is a special value meaning the refill size
  std::unique_ptr<RateLimiter> limiter(new GenericRateLimiter(
      kRateBytesPerSec, kRefillPeriodMicros, 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, mock_clock, false /* auto_tuned */,
      0 /* single_burst_bytes */));
  ASSERT_EQ(kRefillBytes, limiter->GetSingleBurstBytes());

  // Dynamically setting to zero should change nothing
  ASSERT_OK(limiter->SetSingleBurstBytes(0));
  ASSERT_EQ(kRefillBytes, limiter->GetSingleBurstBytes());

  // Negative values are invalid and should change nothing
  ASSERT_TRUE(limiter->SetSingleBurstBytes(-1).IsInvalidArgument());
  ASSERT_EQ(kRefillBytes, limiter->GetSingleBurstBytes());

  // Positive values take effect as the new burst size
  ASSERT_OK(limiter->SetSingleBurstBytes(kBurstBytes));
  ASSERT_EQ(kBurstBytes, limiter->GetSingleBurstBytes());

  // Initially the supply is full so a request of size `kBurstBytes` needs
  // `kRefillsPerBurst - 1` refill periods to elapse.
  limiter->Request(limiter->GetSingleBurstBytes() /* bytes */,
                   Env::IOPriority::IO_USER, nullptr /* stats */,
                   RateLimiter::OpType::kWrite);
  ASSERT_EQ((kRefillsPerBurst - 1) * kRefillPeriodMicros,
            mock_clock->NowMicros());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
