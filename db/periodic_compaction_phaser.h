//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Parameters controlling randomized-but-stable phasing of periodic
// (time-based) compaction, derived per Version from
// DBOptions::periodic_compaction_phase_recovery_percent together with the DB ID
// and column-family id. recovery_percent == 0 disables phasing (exact legacy
// behavior).
struct PeriodicCompactionPhaseParams {
  // Stable per-(DB, CF) hash seeding the preferred phase.
  uint64_t seed_hash = 0;
  // Unix time (seconds) at which phasing took effect for this DB (DB::Open, or
  // the last relevant SetDBOptions change). Used to spread out the initial
  // catch-up burst.
  uint64_t anchor_time = 0;
  // Percent [0, 100] of the phase gap closed per trigger; 0 disables phasing.
  int recovery_percent = 0;
};

// De-herds periodic (time-based) compaction across a fleet: gives each (DB, CF)
// a stable "preferred phase" within periodic_compaction_seconds and steers
// compactions toward it, never past the deadline (enabled by
// DBOptions::periodic_compaction_phase_recovery_percent). The DB-level base
// phase is derived from the DB ID -- stable across re-open and physical
// cloning/migration but distinct across DBs -- and each column family is spread
// around it by cf id. This owns the DB-level recovery percent and the phasing
// anchor; the per-file trigger math and phase derivation are stateless static
// helpers. Lives on VersionSet, which caches the resulting per-CF params on
// each Version.
class PeriodicCompactionPhaser {
 public:
  // db_id is referenced live (it is populated after construction, during
  // DB::Open, and is read lazily) and must outlive this phaser -- it is owned
  // by the same VersionSet.
  explicit PeriodicCompactionPhaser(const std::string& db_id) : db_id_(db_id) {}

  // Apply the (mutable) recovery percent. Returns true if it changed -- in
  // which case the caller should Reanchor() and refresh cached params. Does not
  // itself move the anchor.
  //
  // A setting of 25 to 33 will set this phaser to "stun"
  bool SetConfig(int recovery_percent);

  // Move the phasing anchor to `now` (unix seconds).
  void Reanchor(uint64_t now) { anchor_time_ = now; }

  int recovery_percent() const { return recovery_percent_; }

  // Phasing params for a column family (by id): the DB base phase (hashed from
  // the DB ID) spread by cf_id (golden ratio), plus the current anchor and
  // recovery percent. Returns disabled params when phasing is off.
  PeriodicCompactionPhaseParams ParamsForCf(uint32_t cf_id) const;

  // --- Stateless helpers (static; unit-tested directly) ---

  // Wall-clock time (unix seconds) at which a file with the given modification
  // time becomes eligible for periodic compaction under `params`, NOT
  // accounting for the offpeak pull. When params.recovery_percent == 0 (phasing
  // disabled) this is file_modification_time + periodic_compaction_seconds
  // (classic behavior); otherwise it is steered earlier toward the preferred
  // phase. It is never before params.anchor_time and, unless the file was
  // already past due then, never later than the classic deadline. Catch-up
  // after anchor_time is spread out so that many DBs anchoring at once (e.g.
  // phasing enabled across a fleet) do not concentrate periodic compactions
  // shortly after it.
  static uint64_t TriggerTime(uint64_t file_modification_time,
                              uint64_t periodic_compaction_seconds,
                              const PeriodicCompactionPhaseParams& params);

  // Longest periodic_compaction_seconds (~68 years) to which phasing applies,
  // to keep the arithmetic simple and direct.
  static constexpr uint64_t kMaxPhasedPeriod = uint64_t{1} << 31;

  // Spreads a DB's column families quasi-uniformly around a DB-level base phase
  // via the golden-ratio additive (Weyl/Kronecker) recurrence: cf phase ==
  // db_base_phase + cf_id * (phi conjugate), all in fixed point (fraction *
  // 2^64) so the mod-1 wrap is the natural uint64 overflow. Low-discrepancy for
  // any number of CFs.
  static uint64_t CfPhaseSeedHash(uint64_t db_base_phase, uint32_t cf_id);

 private:
  const std::string& db_id_;
  int recovery_percent_ = 0;
  // Unix time (seconds) at which phasing took effect for this DB (construction
  // or last relevant SetDBOptions change); spreads the initial catch-up burst.
  uint64_t anchor_time_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
