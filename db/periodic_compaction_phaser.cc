//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_compaction_phaser.h"

#include <algorithm>
#include <limits>

#include "util/fastrange.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

bool PeriodicCompactionPhaser::SetConfig(int recovery_percent) {
  const bool changed = recovery_percent != recovery_percent_;
  recovery_percent_ = recovery_percent;
  return changed;
}

PeriodicCompactionPhaseParams PeriodicCompactionPhaser::ParamsForCf(
    uint32_t cf_id) const {
  PeriodicCompactionPhaseParams params;
  if (recovery_percent_ <= 0) {
    // Phasing disabled; return default (disabled) params.
    return params;
  }
  // DB-level base phase (a fixed-point fraction * 2^64), hashed from the DB ID.
  // The DB ID is stable across re-open and physical cloning/migration but
  // distinct across DBs, so DBs configured alike do not herd. db_id_ is read
  // live because it is populated after construction (during DB::Open): versions
  // built before it is known hash an empty id and self-heal on later versions.
  const uint64_t db_base_phase = Hash64(db_id_.data(), db_id_.size());
  // Spread this CF quasi-uniformly around the DB base phase (golden ratio) so
  // CFs of one DB do not share a phase.
  params.seed_hash = CfPhaseSeedHash(db_base_phase, cf_id);
  params.anchor_time = anchor_time_;
  params.recovery_percent = std::min(recovery_percent_, 100);
  return params;
}

uint64_t PeriodicCompactionPhaser::TriggerTime(
    uint64_t file_modification_time, uint64_t periodic_compaction_seconds,
    const PeriodicCompactionPhaseParams& params) {
  const uint64_t n = periodic_compaction_seconds;
  // The classic deadline: fire once the file reaches age n. We treat this as
  // a best-effort upper bound, and phasing normally only pulls the trigger
  // earlier (exception: natural_trigger <= anchor case below).
  const uint64_t natural_trigger = file_modification_time + n;
  const int recovery_percent = params.recovery_percent;
  if (recovery_percent <= 0 || n == 0) {
    return natural_trigger;
  }
  if (n > kMaxPhasedPeriod) {
    // Exclude impractically long periods to keep the arithmetic behaving.
    return natural_trigger;
  }
  // This (DB, CF)'s preferred phase: a stable position in [0, n) that repeats
  // every period. FastRange64 spreads different seeds uniformly across [0, n),
  // so unrelated (DB, CF)s prefer different spots and their compactions do not
  // herd. (This equals floor(p * n) with the fraction p = seed_hash / 2^64.)
  const uint64_t target = FastRange64(params.seed_hash, n);
  // How far past the preferred phase the deadline falls -- equivalently, how
  // much earlier we would fire to land exactly on the phase. Always in [0, n);
  // the rules below pull the trigger back by some fraction of it.
  const uint64_t offset = (file_modification_time % n + n - target) % n;
  const uint64_t anchor = params.anchor_time;
  if (natural_trigger <= anchor) {
    // The file was already overdue the moment phasing took effect (the
    // anchor) -- e.g. the DB is recovering from a long outage, or
    // periodic_compaction_seconds was just lowered. Many files can be overdue
    // at once, so firing them all right at the anchor would itself be a herd.
    // Instead, spread the cohort over the first quarter-period after the
    // anchor: a bit of extra lateness (<= ~n/4) in exchange for de-herding.
    //
    // Each file lands on a stable per-(DB, CF) point of an absolute grid with
    // spacing n/4. The grid is tied to the epoch rather than to the anchor on
    // purpose: a DB that keeps restarting re-anchors each time, and an
    // anchor-relative target would keep sliding just out of reach, starving
    // the file. A fixed wall-clock grid point arrives no matter how often the
    // DB restarts.
    const uint64_t grid = n / 4;
    if (grid == 0) {
      return anchor;
    }
    const uint64_t phase = FastRange64(params.seed_hash, grid);
    // Smallest t >= anchor with t % grid == phase.
    return anchor + (phase + grid - anchor % grid) % grid;
  }
  // Steady state: fire earlier than the deadline by early_pull, a duration, so
  // trigger = natural_trigger - early_pull. The basic pull is a fraction q =
  // recovery_percent/100 of the offset:
  //
  //   early_pull = q * offset
  //
  // (offset and early_pull are durations in [0, n); q is a fraction in (0, 1].)
  // Each compaction re-stamps the file with a fresh modification time, leaving
  // a new offset of (1 - q) times the old one, so the phase error shrinks
  // geometrically and the file homes in on its preferred phase within a few
  // cycles. E.g. at q = 33% a file whose deadline is 9 days past its phase
  // fires ~3 days early this cycle, then ~2, then ~1.3, ... onto the phase.
  //
  // The pull is gradual rather than a snap straight to the phase on purpose: a
  // compaction actually runs some unpredictable delay after it comes due
  // (scheduling/queue latency), and snapping can instead pin part of the fleet
  // at that latency-induced time -- re-herding, the opposite of the goal. The
  // gradual pull has no such fixed point and still ends up well spread.
  //
  // When phasing is enabled well into a DB's life -- most often a config flip
  // that switches it on fleet-wide and re-anchors every DB at once -- a file
  // can have its preferred phase just before the anchor and its deadline just
  // after. A full q pull on such a file could land at or before the anchor
  // ("compact now"), and across the fleet these files bunch into a spike right
  // after the anchor.
  //
  // To bound that, near the anchor we replace q with a smaller fraction. With
  // r = natural_trigger - anchor (how long after the anchor the file is due, a
  // duration) and k = 1/2, so that k*r/n is a fraction that grows with r:
  //
  //   early_pull = offset * min(q, k * r / n)
  //
  // k*r/n reaches q at r = q*n/k, so the pull equals the full steady-state
  // q*offset once r >= q*n/k (0.66n at q=33%) and is gentler for files due
  // closer to the anchor. For the recommended q <= 1/2 the reduced pull hits
  // only files written before the anchor; later files get the full q pull.
  // The scaling buys three things:
  // * A file never fires before the anchor: early_pull <= offset*k*r/n < k*r =
  //   r/2 < r (offset < n), so trigger = natural_trigger - early_pull > anchor.
  //   Turning phasing on never back-dates a trigger.
  // * No post-anchor spike. A pull proportional to r keeps each file's trigger
  //   at least a fixed fraction (1-k = 1/2) of the way from the anchor to its
  //   own deadline, so nearby files stay spread instead of piling onto it. On
  //   a uniform-age fleet the peak density is only about -ln(1-k)/k = 1.39x
  //   the steady rate (any q). The rejected alternative -- spreading each file
  //   uniformly over [anchor, natural_trigger] -- diverges like ln(n/t) at
  //   time t after the anchor, since every such window starts at the anchor.
  //   (It grows unbounded as k -> 1, which is why k stays 1/2, not q.)
  // * No herd re-clumping. The pull stays proportional to offset, which differs
  //   by phase, so a herd sharing one deadline stays spread; capping at q*r
  //   would give every DB in the herd the same pull and collapse much of it
  //   onto one instant.
  //
  // k = 1/2 (not k = q) keeps that first de-herding cycle near full speed for
  // q <= 1/2: at q = 33% a lockstep herd due 0.62*n after the anchor still
  // spreads over 0.31*n (vs 0.33*n unscaled). For q > 1/2 it spreads like
  // q = 1/2.
  //
  // DB open is also an anchor and gets the same scaling: restarts can be
  // coordinated across a fleet (e.g. DC power recovery), and write-driven
  // workloads keep many files off their phase, so the same spike could follow.
  // The cost is slower convergence for a DB reopened much more often than once
  // per period; no trigger moves past the classic deadline.
  const uint64_t q_pct = static_cast<uint64_t>(std::min(recovery_percent, 100));
  constexpr uint64_t k_pct = 50;
  const uint64_t r = natural_trigger - anchor;
  uint64_t early_pull;
  // r >= 2*n already implies k_pct*r >= q_pct*n (k_pct == 50, q_pct <= 100),
  // and testing it first avoids overflowing k_pct*r when r is very large.
  if (r >= 2 * n || k_pct * r >= q_pct * n) {
    early_pull = offset * q_pct / 100;
  } else {
    // Here offset < n and r < n * 100 / k_pct, so with n <= kMaxPhasedPeriod
    // the product offset * r cannot overflow.
    static_assert(kMaxPhasedPeriod <= std::numeric_limits<uint64_t>::max() /
                                          (kMaxPhasedPeriod * 100 / k_pct));
    early_pull = offset * r / (n * 100 / k_pct);
  }
  return natural_trigger - early_pull;
}

uint64_t PeriodicCompactionPhaser::CfPhaseSeedHash(uint64_t db_base_phase,
                                                   uint32_t cf_id) {
  // Golden-ratio conjugate ((sqrt(5)-1)/2) as a fixed-point fraction * 2^64.
  // The additive recurrence db_base + cf_id * kGolden (wrapping mod 2^64, i.e.
  // mod 1) is a low-discrepancy Weyl/Kronecker sequence, so a DB's CFs are
  // spread quasi-uniformly around the base phase for any number of CFs.
  constexpr uint64_t kGoldenRatioU64 = 0x9E3779B97F4A7C15ULL;
  return db_base_phase + static_cast<uint64_t>(cf_id) * kGoldenRatioU64;
}

}  // namespace ROCKSDB_NAMESPACE
