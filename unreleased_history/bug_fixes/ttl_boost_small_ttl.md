Fixed a bug where, with leveled compaction, `compaction_pri = kMinOverlappingRatio`, and a `ttl` below 32 seconds, files older than half of `ttl` were not prioritized for compaction.
