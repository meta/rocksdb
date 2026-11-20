---
title: Online Validation
layout: post
author: siying
category: blog
---
To prevent or mitigate data corrution in RocksDB when some software or hardware issues happens, we keep adding online consistency checks and improving existing ones.

We improved ColumnFamilyOptions::force_consistency_checks and enabled it by default. The option does some basic consistency checks to LSM-tree, e.g., files in one level are not overlapping. The DB will be frozen from new writes if a violation is detected. Previously, the feature’s check was too limited and didn’t always freeze the DB in a timely manner. Last year, we made the checking stricter so that it can [catch much more corrupted LSM-tree structures](https://github.com/meta/rocksdb/pull/6901). We also fixed several issues where the checking failure was swallowed without freezing the DB. After making force_consistency_checks more reliable, we changed the default value to be on.

ColumnFamilyOptions::paranoid_file_checks does some more expensive extra checking when generating a new SST file. Last year, we advanced coverage to this feature: after every SST file is generated, the SST file is created, read back keys one by one and check two things: (1) the keys are in comparator order (also available and enabled by default during file write via ColumnFamilyOptions::check_flush_compaction_key_order); (2) the hash of all the KVs is the same as calculated when we add KVs into it. These checks detect certain corruptions so we can prevent the corrupt files from being applied to the DB. We suggest users turn it on at least in shadow environments, and consider to run it in production too if you can afford the overheads.

A recent feature is added to check the count of entries added into memtable while flushing it into an SST file. This feature is to have some online coverage to memtable corruption, caused by either software bug or hardware issue. This feature will be released in the coming release (6.21) and by default on. In the future, we will check more counters during memtables, e.g. number of puts or number of deletes.

We also improved the reporting of online validation errors to improve debuggability. For example, failure to parse a corrupt key now reports details about the corrupt key. Since we did not want to expose key data in logs, error messages, etc., by default, this reporting is opt-in via DBOptions::allow_data_in_errors.

More online checking features are planned and some are more sophisticated, including key/value checksums and sample based query validation.
