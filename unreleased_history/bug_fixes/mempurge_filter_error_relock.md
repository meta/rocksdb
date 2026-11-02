Fixed a bug where MemPurge returned with the DB mutex unlocked when the compaction filter created for flush did not ignore snapshots.
