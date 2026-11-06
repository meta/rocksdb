/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

// PORT2: #include "toku_config.h"

#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#if defined(__powerpc__)
#include <sys/platform/ppc.h>
#endif

#if 0
static inline float toku_tdiff (struct timeval *a, struct timeval *b) {
    return (float)((a->tv_sec - b->tv_sec) + 1e-6 * (a->tv_usec - b->tv_usec));
}
// PORT2: temporary:
#define HAVE_CLOCK_REALTIME
#if !defined(HAVE_CLOCK_REALTIME)
// OS X does not have clock_gettime, we fake clockid_t for the interface, and we'll implement it with clock_get_time.
typedef int clockid_t;
// just something bogus, it doesn't matter, we just want to make sure we're
// only supporting this mode because we're not sure we can support other modes
// without a real clock_gettime()
#define CLOCK_REALTIME 0x01867234
#endif
int toku_clock_gettime(clockid_t clk_id, struct timespec *ts) __attribute__((__visibility__("default")));
#endif

// *************** Performance timers ************************
// What do you really want from a performance timer:
//  (1) Can determine actual time of day from the performance time.
//  (2) Time goes forward, never backward.
//  (3) Same time on different processors (or even different machines).
//  (4) Time goes forward at a constant rate (doesn't get faster and slower)
//  (5) Portable.
//  (6) Getting the time is cheap.
// Unfortuately it seems tough to get Properties 1-5.  So we go for Property 6,,
// but we abstract it. We offer a type tokutime_t which can hold the time. This
// type can be subtracted to get a time difference. We can get the present time
// cheaply. We can convert this type to seconds (but that can be expensive). The
// implementation is to use RDTSC (hence we lose property 3: not portable).
// Recent machines have constant_tsc in which case we get property (4).
// Recent OSs on recent machines (that have RDTSCP) fix the per-processor clock
// skew, so we get property (3). We get property 2 with RDTSC (as long as
// there's not any skew). We don't even try to get propety 1, since we don't
// need it. The decision here is that these times are really accurate only on
// modern machines with modern OSs.
typedef uint64_t tokutime_t;  // Time type used in by tokutek timers.

#if 0
// The value of tokutime_t is not specified here. 
// It might be microseconds since 1/1/1970 (if gettimeofday() is
// used), or clock cycles since boot (if rdtsc is used).  Or something
// else.
// Two tokutime_t values can be subtracted to get a time difference.
// Use tokutime_to_seconds to that convert difference  to seconds.
// We want get_tokutime() to be fast, but don't care so much about tokutime_to_seconds();
//
// For accurate time calculations do the subtraction in the right order:
//   Right:  tokutime_to_seconds(t1-t2);
//   Wrong   tokutime_to_seconds(t1)-toku_time_to_seconds(t2);
// Doing it the wrong way is likely to result in loss of precision.
// A double can hold numbers up to about 53 bits.  RDTSC which uses about 33 bits every second, so that leaves
// 2^20 seconds from booting (about 2 weeks) before the RDTSC value cannot be represented accurately as a double.
//
double tokutime_to_seconds(tokutime_t)  __attribute__((__visibility__("default"))); // Convert tokutime to seconds.

#endif

// Get the value of tokutime for right now.  We want this to be fast, so we
// expose the implementation as RDTSC.
static inline tokutime_t toku_time_now(void) {
#if defined(__x86_64__) || defined(__i386__)
  uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return (uint64_t)hi << 32 | lo;
#elif defined(__aarch64__)
  uint64_t result;
  __asm __volatile__("mrs %[rt], cntvct_el0" : [rt] "=r"(result));
  return result;
#elif defined(__powerpc__)
  return __ppc_get_timebase();
#elif defined(__arm__)
  uint32_t lo, hi;
  __asm __volatile__("mrrc p15, 1, %[lo], %[hi], c14"
                     : [lo] "=r"(lo), [hi] "=r"(hi));
  return (uint64_t)hi << 32 | lo;
#elif defined(__s390x__)
  uint64_t result;
  asm volatile("stckf %0" : "=Q"(result) : : "cc");
  return result;
#elif defined(__riscv) && __riscv_xlen == 32
  uint32_t cycles_lo, cycles_hi0, cycles_hi1;
  // Implemented in assembly because Clang insisted on branching.
  asm volatile(
      "rdcycleh %0\n"
      "rdcycle %1\n"
      "rdcycleh %2\n"
      "sub %0, %0, %2\n"
      "seqz %0, %0\n"
      "sub %0, zero, %0\n"
      "and %1, %1, %0\n"
      : "=r"(cycles_hi0), "=r"(cycles_lo), "=r"(cycles_hi1));
  return (static_cast<uint64_t>(cycles_hi1) << 32) | cycles_lo;
#elif defined(__riscv) && __riscv_xlen == 64
  uint64_t cycles;
  asm volatile("rdcycle %0" : "=r"(cycles));
  return cycles;
#elif defined(__loongarch64)
  unsigned long result;
  asm volatile("rdtime.d\t%0,$r0" : "=r"(result));
  return result;
#else
#error No timer implementation for this platform
#endif
}

static inline uint64_t toku_current_time_microsec(void) {
  struct timeval t;
  gettimeofday(&t, nullptr);
  return t.tv_sec * (1UL * 1000 * 1000) + t.tv_usec;
}

#if 0
// sleep microseconds
static inline void toku_sleep_microsec(uint64_t ms) {
    struct timeval  t;

    t.tv_sec = ms / 1000000;
    t.tv_usec = ms % 1000000;

    select(0, NULL, NULL, NULL, &t);
}
#endif

/*
  PORT: Usage of this file:

  uint64_t toku_current_time_microsec()   // uses gettimeoday
      is used to track how much time various operations took (for example, lock
      escalation). (TODO: it is not clear why these operations are tracked with
      microsecond precision while others use nanoseconds)

  tokutime_t toku_time_now() // uses rdtsc
      seems to be used for a very similar purpose. This has greater precision

  RocksDB environment provides Env::Default()->NowMicros() and NowNanos() which
  should be adequate substitutes.
*/
