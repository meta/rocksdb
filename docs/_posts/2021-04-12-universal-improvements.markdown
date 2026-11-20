---
title: (Call For Contribution) Make Universal Compaction More Incremental
layout: post
author: siying
category: blog
---

### Motivation

Universal Compaction is an important compaction style, but few changes were made after we made the structure multi-leveled. Yet the major restriction of always compacting full sorted run is not relaxed. Compared to Leveled Compaction, where we usually only compile several SST files together, in universal compaction, we frequently compact GBs of data. Two issues with this gap: 1. it makes it harder to unify universal and leveled compaction; 2. periodically data is fully compacted, and in the mean time space is doubled. To ease the problem, we can break the restriction and do similar as leveled compaction, and bring it closer to unified compaction.

We call for help for making following improvements.


### How Universal Compaction Works

In universal, whole levels are compacted together to satisfy two conditions (See [wiki page](https://github.com/meta/rocksdb/wiki/Universal-Compaction) for more details):

1. total size / bottommost level size > a threshold, or
2. total number of sorted runs (non-0 levels + L0 files) is within a threshold

1 is to limit extra space overhead used for dead data and 2 is for read performance.

If 1 is triggered, likely a full compaction will be triggered. If 2 is triggered, RocksDB compact some sorted runs to bring the number down. It does it by using a simple heuristic so that less writes needed for that purpose over time: it starts from compacting smaller files, but if total size to compact is similar to or larger than size of the next level, it will take that level together, as soon on (whether it is the best heuristic is another question and we’ve never seriously looked at it).

### How We Can Improve?

Let’s start from condition 1. Here we do full compaction but is not necessary.  A simple optimization would be to compact so that just enough files are merged into the bottommost level (Lmax) to satisfy condition 1. It would work if we only need to pick some files from Lmax-1, or if it is cheaper over time, we can pick some files from other levels too.

Then condition 2. If we finish condition 1, there might be holes in some ranges in older levels. These holes might make it possible that only by compacting some sub ranges, we can fix the LSM-tree for condition 2. RocksDB can take single files into consideration and apply more sophisticated heuristic.

This new approach makes universal compaction closer to leveled compaction. The operation for 1 is closer to how Leveled compaction triggeres Lmax-1 to Lmax compaction. And 2 can potentially be implemented as something similar to level picking in Leveled Compaction. In fact, all those file picking can co-existing in one single compaction style and there isn’t fundamental conflicts to that.

### Limitation

There are two limitations:

* Periodic automatic full compaction is unpleasant but at the same time is pleasant in another way. Some users might uses it to reason that everything is periodically collapsed so dead data is gone and old data is rewritten. We need to make sure periodic compaction works to continue with that.
* L0 to the first non-L0 level compaction is the first time data is partitioned in LSM-tree so that incremental compaction by range is possible. We might need to do more of these compactions in order to make incremental possible, which will increase compaction slightly.
* Compacting subset of a level would introduce some extra overhead for unaligned files, just as in leveled compaction. More SST boundary cutting heuristic can reduce this overhead but it will be there.

But I believe the benefits would outweight the limitations. Reducing temporary space doubling and moving towards to unified compaction would be important achievements.

### Interested in Help?

Compaction is the core of LSM-tree, but its improvements are far overdue. If you are a user of universal compaction and would be able to benefit from those improvements, we will be happy to work with you on speeding up the project and bring them to RocksDB sooner. Feel free to communicate with us in [this issue](https://github.com/meta/rocksdb/issues/8181).
