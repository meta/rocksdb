#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import functools
import gc
import heapq
import random
import sys
import time
from collections import deque, OrderedDict
from os import path

import numpy as np


kSampleSize = 64  # The sample size used when performing eviction.
kMicrosInSecond = 1000000
kSecondsInMinute = 60
kSecondsInHour = 3600


class TraceRecord:
    """
    A trace record represents a block access.
    It holds the same struct as BlockCacheTraceRecord in
    trace_replay/block_cache_tracer.h
    """

    def __init__(
        self,
        access_time,
        block_id,
        block_type,
        block_size,
        cf_id,
        cf_name,
        level,
        fd,
        caller,
        no_insert,
        get_id,
        key_id,
        kv_size,
        is_hit,
        referenced_key_exist_in_block,
        num_keys_in_block,
        table_id,
        seq_number,
        block_key_size,
        key_size,
        block_offset_in_file,
        next_access_seq_no,
    ):
        self.access_time = access_time
        self.block_id = block_id
        self.block_type = block_type
        self.block_size = block_size + block_key_size
        self.cf_id = cf_id
        self.cf_name = cf_name
        self.level = level
        self.fd = fd
        self.caller = caller
        if no_insert == 1:
            self.no_insert = True
        else:
            self.no_insert = False
        self.get_id = get_id
        self.key_id = key_id
        self.kv_size = kv_size
        if is_hit == 1:
            self.is_hit = True
        else:
            self.is_hit = False
        if referenced_key_exist_in_block == 1:
            self.referenced_key_exist_in_block = True
        else:
            self.referenced_key_exist_in_block = False
        self.num_keys_in_block = num_keys_in_block
        self.table_id = table_id
        self.seq_number = seq_number
        self.block_key_size = block_key_size
        self.key_size = key_size
        self.block_offset_in_file = block_offset_in_file
        self.next_access_seq_no = next_access_seq_no


class CacheEntry:
    """A cache entry stored in the cache."""

    def __init__(
        self,
        value_size,
        cf_id,
        level,
        block_type,
        table_id,
        access_number,
        time_s,
        num_hits=0,
    ):
        self.value_size = value_size
        self.last_access_number = access_number
        self.num_hits = num_hits
        self.cf_id = 0
        self.level = level
        self.block_type = block_type
        self.last_access_time = time_s
        self.insertion_time = time_s
        self.table_id = table_id

    def __repr__(self):
        """Debug string."""
        return "(s={},last={},hits={},cf={},l={},bt={})\n".format(
            self.value_size,
            self.last_access_number,
            self.num_hits,
            self.cf_id,
            self.level,
            self.block_type,
        )

    def cost_class(self, cost_class_label):
        if cost_class_label == "table_bt":
            return f"{self.table_id}-{self.block_type}"
        elif cost_class_label == "table":
            return f"{self.table_id}"
        elif cost_class_label == "bt":
            return f"{self.block_type}"
        elif cost_class_label == "cf":
            return f"{self.cf_id}"
        elif cost_class_label == "cf_bt":
            return f"{self.cf_id}-{self.block_type}"
        elif cost_class_label == "table_level_bt":
            return f"{self.table_id}-{self.level}-{self.block_type}"
        assert False, f"Unknown cost class label {cost_class_label}"
        return None


class HashEntry:
    """A hash entry stored in a hash table."""

    def __init__(self, key, hash, value):
        self.key = key
        self.hash = hash
        self.value = value

    def __repr__(self):
        return f"k={self.key},h={self.hash},v=[{self.value}]"


class HashTable:
    """
    A custom implementation of hash table to support fast random sampling.
    It is closed hashing and uses chaining to resolve hash conflicts.
    It grows/shrinks the hash table upon insertion/deletion to support
    fast lookups and random samplings.
    """

    def __init__(self):
        self.initial_size = 32
        self.table = [None] * self.initial_size
        self.elements = 0

    def random_sample(self, sample_size):
        """Randomly sample 'sample_size' hash entries from the table."""
        samples = []
        index = random.randint(0, len(self.table) - 1)
        pos = index
        # Starting from index, adding hash entries to the sample list until
        # sample_size is met or we ran out of entries.
        while True:
            if self.table[pos] is not None:
                for i in range(len(self.table[pos])):
                    if self.table[pos][i] is None:
                        continue
                    samples.append(self.table[pos][i])
                    if len(samples) == sample_size:
                        break
            pos += 1
            pos = pos % len(self.table)
            if pos == index or len(samples) == sample_size:
                break
        assert len(samples) <= sample_size
        return samples

    def __repr__(self):
        all_entries = []
        for i in range(len(self.table)):
            if self.table[i] is None:
                continue
            for j in range(len(self.table[i])):
                if self.table[i][j] is not None:
                    all_entries.append(self.table[i][j])
        return f"{all_entries}"

    def values(self):
        all_values = []
        for i in range(len(self.table)):
            if self.table[i] is None:
                continue
            for j in range(len(self.table[i])):
                if self.table[i][j] is not None:
                    all_values.append(self.table[i][j].value)
        return all_values

    def __len__(self):
        return self.elements

    def insert(self, key, hash, value):
        """
        Insert a hash entry in the table. Replace the old entry if it already
        exists.
        """
        self.grow()
        inserted = False
        index = hash % len(self.table)
        if self.table[index] is None:
            self.table[index] = []
        # Search for the entry first.
        for i in range(len(self.table[index])):
            if self.table[index][i] is None:
                continue
            if self.table[index][i].hash == hash and self.table[index][i].key == key:
                # The entry already exists in the table.
                self.table[index][i] = HashEntry(key, hash, value)
                return

        # Find an empty slot.
        for i in range(len(self.table[index])):
            if self.table[index][i] is None:
                self.table[index][i] = HashEntry(key, hash, value)
                inserted = True
                break
        if not inserted:
            self.table[index].append(HashEntry(key, hash, value))
        self.elements += 1

    def resize(self, new_size):
        if new_size == len(self.table):
            return
        if new_size < self.initial_size:
            return
        if self.elements < 100:
            return
        new_table = [None] * new_size
        # Copy 'self.table' to new_table.
        for i in range(len(self.table)):
            entries = self.table[i]
            if entries is None:
                continue
            for j in range(len(entries)):
                if entries[j] is None:
                    continue
                index = entries[j].hash % new_size
                if new_table[index] is None:
                    new_table[index] = []
                new_table[index].append(entries[j])
        self.table = new_table
        del new_table
        # Manually call python gc here to free the memory as 'self.table'
        # might be very large.
        gc.collect()

    def grow(self):
        if self.elements < 4 * len(self.table):
            return
        new_size = int(len(self.table) * 1.5)
        self.resize(new_size)

    def delete(self, key, hash):
        index = hash % len(self.table)
        deleted = False
        deleted_entry = None
        if self.table[index] is None:
            return
        for i in range(len(self.table[index])):
            if (
                self.table[index][i] is not None
                and self.table[index][i].hash == hash
                and self.table[index][i].key == key
            ):
                deleted_entry = self.table[index][i]
                self.table[index][i] = None
                self.elements -= 1
                deleted = True
                break
        if deleted:
            self.shrink()
        return deleted_entry

    def shrink(self):
        if self.elements * 2 >= len(self.table):
            return
        new_size = int(len(self.table) * 0.7)
        self.resize(new_size)

    def lookup(self, key, hash):
        index = hash % len(self.table)
        if self.table[index] is None:
            return None
        for i in range(len(self.table[index])):
            if (
                self.table[index][i] is not None
                and self.table[index][i].hash == hash
                and self.table[index][i].key == key
            ):
                return self.table[index][i].value
        return None


class MissRatioStats:
    def __init__(self, time_unit):
        self.num_misses = 0
        self.num_accesses = 0
        self.time_unit = time_unit
        self.time_misses = {}
        self.time_miss_bytes = {}
        self.time_accesses = {}

    def update_metrics(self, access_time, is_hit, miss_bytes):
        access_time /= kMicrosInSecond * self.time_unit
        self.num_accesses += 1
        if access_time not in self.time_accesses:
            self.time_accesses[access_time] = 0
        self.time_accesses[access_time] += 1
        if not is_hit:
            self.num_misses += 1
            if access_time not in self.time_misses:
                self.time_misses[access_time] = 0
                self.time_miss_bytes[access_time] = 0
            self.time_misses[access_time] += 1
            self.time_miss_bytes[access_time] += miss_bytes

    def reset_counter(self):
        self.num_misses = 0
        self.num_accesses = 0
        self.time_miss_bytes.clear()
        self.time_misses.clear()
        self.time_accesses.clear()

    def compute_miss_bytes(self):
        miss_bytes = []
        for at in self.time_miss_bytes:
            miss_bytes.append(self.time_miss_bytes[at])
        miss_bytes = sorted(miss_bytes)
        avg_miss_bytes = 0
        p95_miss_bytes = 0
        for i in range(len(miss_bytes)):
            avg_miss_bytes += float(miss_bytes[i]) / float(len(miss_bytes))

        p95_index = min(int(0.95 * float(len(miss_bytes))), len(miss_bytes) - 1)
        p95_miss_bytes = miss_bytes[p95_index]
        return avg_miss_bytes, p95_miss_bytes

    def miss_ratio(self):
        return float(self.num_misses) * 100.0 / float(self.num_accesses)

    def write_miss_timeline(
        self, cache_type, cache_size, target_cf_name, result_dir, start, end
    ):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-miss-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        if not path.exists(header_file_path):
            with open(header_file_path, "w+") as header_file:
                header = "time"
                for trace_time in range(start, end):
                    header += f",{trace_time}"
                header_file.write(header + "\n")
        file_path = "{}/data-ml-miss-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        with open(file_path, "w+") as file:
            row = f"{cache_type}"
            for trace_time in range(start, end):
                row += f",{self.time_misses.get(trace_time, 0)}"
            file.write(row + "\n")

    def write_miss_ratio_timeline(
        self, cache_type, cache_size, target_cf_name, result_dir, start, end
    ):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-miss-ratio-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        if not path.exists(header_file_path):
            with open(header_file_path, "w+") as header_file:
                header = "time"
                for trace_time in range(start, end):
                    header += f",{trace_time}"
                header_file.write(header + "\n")
        file_path = "{}/data-ml-miss-ratio-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        with open(file_path, "w+") as file:
            row = f"{cache_type}"
            for trace_time in range(start, end):
                naccesses = self.time_accesses.get(trace_time, 0)
                miss_ratio = 0
                if naccesses > 0:
                    miss_ratio = float(
                        self.time_misses.get(trace_time, 0) * 100.0
                    ) / float(naccesses)
                row += f",{miss_ratio:.2f}"
            file.write(row + "\n")


class PolicyStats:
    def __init__(self, time_unit, policies):
        self.time_selected_polices = {}
        self.time_accesses = {}
        self.policy_names = {}
        self.time_unit = time_unit
        for i in range(len(policies)):
            self.policy_names[i] = policies[i].policy_name()

    def update_metrics(self, access_time, selected_policy):
        access_time /= kMicrosInSecond * self.time_unit
        if access_time not in self.time_accesses:
            self.time_accesses[access_time] = 0
        self.time_accesses[access_time] += 1
        if access_time not in self.time_selected_polices:
            self.time_selected_polices[access_time] = {}
        policy_name = self.policy_names[selected_policy]
        if policy_name not in self.time_selected_polices[access_time]:
            self.time_selected_polices[access_time][policy_name] = 0
        self.time_selected_polices[access_time][policy_name] += 1

    def write_policy_timeline(
        self, cache_type, cache_size, target_cf_name, result_dir, start, end
    ):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-policy-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        if not path.exists(header_file_path):
            with open(header_file_path, "w+") as header_file:
                header = "time"
                for trace_time in range(start, end):
                    header += f",{trace_time}"
                header_file.write(header + "\n")
        file_path = "{}/data-ml-policy-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        with open(file_path, "w+") as file:
            for policy in self.policy_names:
                policy_name = self.policy_names[policy]
                row = f"{cache_type}-{policy_name}"
                for trace_time in range(start, end):
                    row += ",{}".format(
                        self.time_selected_polices.get(trace_time, {}).get(
                            policy_name, 0
                        )
                    )
                file.write(row + "\n")

    def write_policy_ratio_timeline(
        self, cache_type, cache_size, target_cf_name, file_path, start, end
    ):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-policy-ratio-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        if not path.exists(header_file_path):
            with open(header_file_path, "w+") as header_file:
                header = "time"
                for trace_time in range(start, end):
                    header += f",{trace_time}"
                header_file.write(header + "\n")
        file_path = "{}/data-ml-policy-ratio-timeline-{}-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size, target_cf_name
        )
        with open(file_path, "w+") as file:
            for policy in self.policy_names:
                policy_name = self.policy_names[policy]
                row = f"{cache_type}-{policy_name}"
                for trace_time in range(start, end):
                    naccesses = self.time_accesses.get(trace_time, 0)
                    ratio = 0
                    if naccesses > 0:
                        ratio = float(
                            self.time_selected_polices.get(trace_time, {}).get(
                                policy_name, 0
                            )
                            * 100.0
                        ) / float(naccesses)
                    row += f",{ratio:.2f}"
                file.write(row + "\n")


class Policy:
    """
    A policy maintains a set of evicted keys. It returns a reward of one to
    itself if it has not evicted a missing key. Otherwise, it gives itself 0
    reward.
    """

    def __init__(self):
        self.evicted_keys = {}

    def evict(self, key, max_size):
        self.evicted_keys[key] = 0

    def delete(self, key):
        self.evicted_keys.pop(key, None)

    def prioritize_samples(self, samples, auxilliary_info):
        raise NotImplementedError

    def policy_name(self):
        raise NotImplementedError

    def generate_reward(self, key):
        if key in self.evicted_keys:
            return 0
        return 1


class LRUPolicy(Policy):
    def prioritize_samples(self, samples, auxilliary_info):
        return sorted(
            samples,
            key=functools.cmp_to_key(
                lambda e1, e2: e1.value.last_access_number
                - e2.value.last_access_number
            ),
        )

    def policy_name(self):
        return "lru"


class MRUPolicy(Policy):
    def prioritize_samples(self, samples, auxilliary_info):
        return sorted(
            samples,
            key=functools.cmp_to_key(
                lambda e1, e2: e2.value.last_access_number
                - e1.value.last_access_number
            ),
        )

    def policy_name(self):
        return "mru"


class LFUPolicy(Policy):
    def prioritize_samples(self, samples, auxilliary_info):
        return sorted(
            samples,
            key=functools.cmp_to_key(
                lambda e1, e2: e1.value.num_hits - e2.value.num_hits
            ),
        )

    def policy_name(self):
        return "lfu"


class HyperbolicPolicy(Policy):
    """
    An implementation of Hyperbolic caching.

    Aaron Blankstein, Siddhartha Sen, and Michael J. Freedman. 2017.
    Hyperbolic caching: flexible caching for web applications. In Proceedings
    of the 2017 USENIX Conference on Usenix Annual Technical Conference
    (USENIX ATC '17). USENIX Association, Berkeley, CA, USA, 499-511.
    """

    def compare(self, e1, e2, now):
        e1_duration = max(0, (now - e1.value.insertion_time) / kMicrosInSecond) * float(
            e1.value.value_size
        )
        e2_duration = max(0, (now - e2.value.insertion_time) / kMicrosInSecond) * float(
            e2.value.value_size
        )
        if e1_duration == e2_duration:
            return e1.value.num_hits - e2.value.num_hits
        # Antisymmetric, as cmp_to_key needs: an entry with zero duration
        # (just inserted) orders after any other.
        if e1_duration == 0:
            return 1
        if e2_duration == 0:
            return -1
        diff = (float(e1.value.num_hits) / (float(e1_duration))) - (
            float(e2.value.num_hits) / float(e2_duration)
        )
        if diff == 0:
            return 0
        elif diff > 0:
            return 1
        else:
            return -1

    def prioritize_samples(self, samples, auxilliary_info):
        assert len(auxilliary_info) == 3
        now = auxilliary_info[0]
        return sorted(
            samples,
            key=functools.cmp_to_key(lambda e1, e2: self.compare(e1, e2, now)),
        )

    def policy_name(self):
        return "hb"


class CostClassPolicy(Policy):
    """
    We calculate the hit density of a cost class as
    number of hits / total size in cache * average duration in the cache.

    An entry has a higher priority if its class's hit density is higher.
    """

    def compare(self, e1, e2, now, cost_classes, cost_class_label):
        e1_class = e1.value.cost_class(cost_class_label)
        e2_class = e2.value.cost_class(cost_class_label)

        assert e1_class in cost_classes
        assert e2_class in cost_classes

        e1_entry = cost_classes[e1_class]
        e2_entry = cost_classes[e2_class]
        e1_density = e1_entry.density(now)
        e2_density = e2_entry.density(now)
        e1_hits = cost_classes[e1_class].hits
        e2_hits = cost_classes[e2_class].hits

        if e1_density == e2_density:
            return e1_hits - e2_hits

        if e1_entry.num_entries_in_cache == 0:
            return -1
        if e2_entry.num_entries_in_cache == 0:
            return 1

        if e1_density == 0:
            return 1
        if e2_density == 0:
            return -1
        diff = (float(e1_hits) / float(e1_density)) - (
            float(e2_hits) / float(e2_density)
        )
        if diff == 0:
            return 0
        elif diff > 0:
            return 1
        else:
            return -1

    def prioritize_samples(self, samples, auxilliary_info):
        assert len(auxilliary_info) == 3
        now = auxilliary_info[0]
        cost_classes = auxilliary_info[1]
        cost_class_label = auxilliary_info[2]
        return sorted(
            samples,
            key=functools.cmp_to_key(
                lambda e1, e2: self.compare(
                    e1, e2, now, cost_classes, cost_class_label
                )
            ),
        )

    def policy_name(self):
        return "cc"


class Cache:
    """
    This is the base class for the implementations of alternative cache
    replacement policies.
    """

    def __init__(self, cache_size, enable_cache_row_key):
        self.cache_size = cache_size
        self.used_size = 0
        self.per_second_miss_ratio_stats = MissRatioStats(1)
        self.miss_ratio_stats = MissRatioStats(kSecondsInMinute)
        self.per_hour_miss_ratio_stats = MissRatioStats(kSecondsInHour)
        # 0: disabled. 1: enabled. Insert both row and the refereneced data block.
        # 2: enabled. Insert only the row but NOT the referenced data block.
        self.enable_cache_row_key = enable_cache_row_key
        self.get_id_row_key_map = {}
        self.max_seen_get_id = 0
        self.retain_get_id_range = 100000

    def block_key(self, trace_record):
        return f"b{trace_record.block_id}"

    def row_key(self, trace_record):
        return f"g{trace_record.fd}-{trace_record.key_id}"

    def _lookup(self, trace_record, key, hash):
        """
        Look up the key in the cache.
        Returns true upon a cache hit, false otherwise.
        """
        raise NotImplementedError

    def _evict(self, trace_record, key, hash, value_size):
        """
        Evict entries in the cache until there is enough room to insert the new
        entry with 'value_size'.
        """
        raise NotImplementedError

    def _insert(self, trace_record, key, hash, value_size):
        """
        Insert the new entry into the cache.
        """
        raise NotImplementedError

    def _should_admit(self, trace_record, key, hash, value_size):
        """
        A custom admission policy to decide whether we should admit the new
        entry upon a cache miss.
        Returns true if the new entry should be admitted, false otherwise.
        """
        raise NotImplementedError

    def cache_name(self):
        """
        The name of the replacement policy.
        """
        raise NotImplementedError

    def is_ml_cache(self):
        return False

    def _update_stats(self, access_time, is_hit, miss_bytes):
        self.per_second_miss_ratio_stats.update_metrics(access_time, is_hit, miss_bytes)
        self.miss_ratio_stats.update_metrics(access_time, is_hit, miss_bytes)
        self.per_hour_miss_ratio_stats.update_metrics(access_time, is_hit, miss_bytes)

    def access(self, trace_record):
        """
        Access a trace record. The simulator calls this function to access a
        trace record.
        """
        assert self.used_size <= self.cache_size
        if (
            self.enable_cache_row_key > 0
            and trace_record.caller == 1
            and trace_record.key_id != 0
            and trace_record.get_id != 0
        ):
            # This is a get request.
            self._access_row(trace_record)
            return
        is_hit = self._access_kv(
            trace_record,
            self.block_key(trace_record),
            trace_record.block_id,
            trace_record.block_size,
            trace_record.no_insert,
        )
        self._update_stats(
            trace_record.access_time, is_hit=is_hit, miss_bytes=trace_record.block_size
        )

    def _access_row(self, trace_record):
        row_key = self.row_key(trace_record)
        self.max_seen_get_id = max(self.max_seen_get_id, trace_record.get_id)
        self.get_id_row_key_map.pop(
            self.max_seen_get_id - self.retain_get_id_range, None
        )
        if trace_record.get_id not in self.get_id_row_key_map:
            self.get_id_row_key_map[trace_record.get_id] = {}
            self.get_id_row_key_map[trace_record.get_id]["h"] = False
        if self.get_id_row_key_map[trace_record.get_id]["h"]:
            # We treat future accesses as hits since this get request
            # completes.
            # print("row hit 1")
            self._update_stats(trace_record.access_time, is_hit=True, miss_bytes=0)
            return
        if row_key not in self.get_id_row_key_map[trace_record.get_id]:
            # First time seen this key.
            is_hit = self._access_kv(
                trace_record,
                key=row_key,
                hash=trace_record.key_id,
                value_size=trace_record.kv_size,
                no_insert=False,
            )
            inserted = False
            if trace_record.kv_size > 0:
                inserted = True
            self.get_id_row_key_map[trace_record.get_id][row_key] = inserted
            self.get_id_row_key_map[trace_record.get_id]["h"] = is_hit
        if self.get_id_row_key_map[trace_record.get_id]["h"]:
            # We treat future accesses as hits since this get request
            # completes.
            # print("row hit 2")
            self._update_stats(trace_record.access_time, is_hit=True, miss_bytes=0)
            return
        # Access its blocks.
        no_insert = trace_record.no_insert
        if (
            self.enable_cache_row_key == 2
            and trace_record.kv_size > 0
            and trace_record.block_type == 9
        ):
            no_insert = True
        is_hit = self._access_kv(
            trace_record,
            key=self.block_key(trace_record),
            hash=trace_record.block_id,
            value_size=trace_record.block_size,
            no_insert=no_insert,
        )
        self._update_stats(
            trace_record.access_time, is_hit, miss_bytes=trace_record.block_size
        )
        if (
            trace_record.kv_size > 0
            and not self.get_id_row_key_map[trace_record.get_id][row_key]
        ):
            # Insert the row key-value pair.
            self._access_kv(
                trace_record,
                key=row_key,
                hash=trace_record.key_id,
                value_size=trace_record.kv_size,
                no_insert=False,
            )
            # Mark as inserted.
            self.get_id_row_key_map[trace_record.get_id][row_key] = True

    def _access_kv(self, trace_record, key, hash, value_size, no_insert):
        # Sanity checks.
        assert self.used_size <= self.cache_size
        if self._lookup(trace_record, key, hash):
            # A cache hit.
            return True
        if no_insert or value_size <= 0:
            return False
        # A cache miss.
        if value_size > self.cache_size:
            # The block is too large to fit into the cache.
            return False
        self._evict(trace_record, key, hash, value_size)
        if self._should_admit(trace_record, key, hash, value_size):
            self._insert(trace_record, key, hash, value_size)
            self.used_size += value_size
        return False


class CostClassEntry:
    """
    A cost class maintains aggregated statistics of cached entries in a class.
    For example, we may define block type as a class. Then, cached blocks of the
    same type will share one cost class entry.
    """

    def __init__(self):
        self.hits = 0
        self.num_entries_in_cache = 0
        self.size_in_cache = 0
        self.sum_insertion_times = 0
        self.sum_last_access_time = 0

    def insert(self, trace_record, key, value_size):
        self.size_in_cache += value_size
        self.num_entries_in_cache += 1
        self.sum_insertion_times += trace_record.access_time / kMicrosInSecond
        self.sum_last_access_time += trace_record.access_time / kMicrosInSecond

    def remove(self, insertion_time, last_access_time, key, value_size, num_hits):
        self.hits -= num_hits
        self.num_entries_in_cache -= 1
        self.sum_insertion_times -= insertion_time / kMicrosInSecond
        self.size_in_cache -= value_size
        self.sum_last_access_time -= last_access_time / kMicrosInSecond

    def update_on_hit(self, trace_record, last_access_time):
        self.hits += 1
        self.sum_last_access_time -= last_access_time / kMicrosInSecond
        self.sum_last_access_time += trace_record.access_time / kMicrosInSecond

    def avg_lifetime_in_cache(self, now):
        avg_insertion_time = self.sum_insertion_times / self.num_entries_in_cache
        return now / kMicrosInSecond - avg_insertion_time

    def avg_last_access_time(self):
        if self.num_entries_in_cache == 0:
            return 0
        return float(self.sum_last_access_time) / float(self.num_entries_in_cache)

    def avg_size(self):
        if self.num_entries_in_cache == 0:
            return 0
        return float(self.sum_last_access_time) / float(self.num_entries_in_cache)

    def density(self, now):
        avg_insertion_time = self.sum_insertion_times / self.num_entries_in_cache
        in_cache_duration = now / kMicrosInSecond - avg_insertion_time
        return self.size_in_cache * in_cache_duration


class MLCache(Cache):
    """
    MLCache is the base class for implementations of alternative replacement
    policies using reinforcement learning.
    """

    def __init__(self, cache_size, enable_cache_row_key, policies, cost_class_label):
        super().__init__(cache_size, enable_cache_row_key)
        self.table = HashTable()
        self.policy_stats = PolicyStats(kSecondsInMinute, policies)
        self.per_hour_policy_stats = PolicyStats(kSecondsInHour, policies)
        self.policies = policies
        self.cost_classes = {}
        self.cost_class_label = cost_class_label

    def is_ml_cache(self):
        return True

    def _lookup(self, trace_record, key, hash):
        value = self.table.lookup(key, hash)
        if value is not None:
            # Update the entry's cost class statistics.
            if self.cost_class_label is not None:
                cost_class = value.cost_class(self.cost_class_label)
                assert cost_class in self.cost_classes
                self.cost_classes[cost_class].update_on_hit(
                    trace_record, value.last_access_time
                )
            # Update the entry's last access time.
            self.table.insert(
                key,
                hash,
                CacheEntry(
                    value_size=value.value_size,
                    cf_id=value.cf_id,
                    level=value.level,
                    block_type=value.block_type,
                    table_id=value.table_id,
                    access_number=self.miss_ratio_stats.num_accesses,
                    time_s=trace_record.access_time,
                    num_hits=value.num_hits + 1,
                ),
            )
            return True
        return False

    def _evict(self, trace_record, key, hash, value_size):
        # Select a policy, random sample kSampleSize keys from the cache, then
        # evict keys in the sample set until we have enough room for the new
        # entry.
        policy_index = self._select_policy(trace_record, key)
        assert policy_index < len(self.policies) and policy_index >= 0
        self.policies[policy_index].delete(key)
        self.policy_stats.update_metrics(trace_record.access_time, policy_index)
        self.per_hour_policy_stats.update_metrics(
            trace_record.access_time, policy_index
        )
        while self.used_size + value_size > self.cache_size:
            # Randomly sample n entries.
            samples = self.table.random_sample(kSampleSize)
            samples = self.policies[policy_index].prioritize_samples(
                samples,
                [trace_record.access_time, self.cost_classes, self.cost_class_label],
            )
            for hash_entry in samples:
                assert self.table.delete(hash_entry.key, hash_entry.hash) is not None
                self.used_size -= hash_entry.value.value_size
                self.policies[policy_index].evict(
                    key=hash_entry.key, max_size=self.table.elements
                )
                # Update the entry's cost class statistics.
                if self.cost_class_label is not None:
                    cost_class = hash_entry.value.cost_class(self.cost_class_label)
                    assert cost_class in self.cost_classes
                    self.cost_classes[cost_class].remove(
                        hash_entry.value.insertion_time,
                        hash_entry.value.last_access_time,
                        key,
                        hash_entry.value.value_size,
                        hash_entry.value.num_hits,
                    )
                if self.used_size + value_size <= self.cache_size:
                    break

    def _insert(self, trace_record, key, hash, value_size):
        assert self.used_size + value_size <= self.cache_size
        entry = CacheEntry(
            value_size,
            trace_record.cf_id,
            trace_record.level,
            trace_record.block_type,
            trace_record.table_id,
            self.miss_ratio_stats.num_accesses,
            trace_record.access_time,
        )
        # Update the entry's cost class statistics.
        if self.cost_class_label is not None:
            cost_class = entry.cost_class(self.cost_class_label)
            if cost_class not in self.cost_classes:
                self.cost_classes[cost_class] = CostClassEntry()
            self.cost_classes[cost_class].insert(trace_record, key, value_size)
        self.table.insert(key, hash, entry)

    def _should_admit(self, trace_record, key, hash, value_size):
        return True

    def _select_policy(self, trace_record, key):
        raise NotImplementedError


class ThompsonSamplingCache(MLCache):
    """
    An implementation of Thompson Sampling for the Bernoulli Bandit.

    Daniel J. Russo, Benjamin Van Roy, Abbas Kazerouni, Ian Osband,
    and Zheng Wen. 2018. A Tutorial on Thompson Sampling. Found.
    Trends Mach. Learn. 11, 1 (July 2018), 1-96.
    DOI: https://doi.org/10.1561/2200000070
    """

    def __init__(
        self,
        cache_size,
        enable_cache_row_key,
        policies,
        cost_class_label,
        init_a=1,
        init_b=1,
    ):
        super().__init__(
            cache_size, enable_cache_row_key, policies, cost_class_label
        )
        self._as = {}
        self._bs = {}
        for _i in range(len(policies)):
            self._as = [init_a] * len(self.policies)
            self._bs = [init_b] * len(self.policies)

    def _select_policy(self, trace_record, key):
        if len(self.policies) == 1:
            return 0
        samples = [
            np.random.beta(self._as[x], self._bs[x]) for x in range(len(self.policies))
        ]
        selected_policy = max(range(len(self.policies)), key=lambda x: samples[x])
        reward = self.policies[selected_policy].generate_reward(key)
        assert reward <= 1 and reward >= 0
        self._as[selected_policy] += reward
        self._bs[selected_policy] += 1 - reward
        return selected_policy

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid ThompsonSampling with cost class {} (ts_hybrid)".format(
                self.cost_class_label
            )
        return f"ThompsonSampling with cost class {self.cost_class_label} (ts)"


class LinUCBCache(MLCache):
    """
    An implementation of LinUCB with disjoint linear models.

    Lihong Li, Wei Chu, John Langford, and Robert E. Schapire. 2010.
    A contextual-bandit approach to personalized news article recommendation.
    In Proceedings of the 19th international conference on World wide web
    (WWW '10). ACM, New York, NY, USA, 661-670.
    DOI=http://dx.doi.org/10.1145/1772690.1772758
    """

    def __init__(self, cache_size, enable_cache_row_key, policies, cost_class_label):
        super().__init__(
            cache_size, enable_cache_row_key, policies, cost_class_label
        )
        self.nfeatures = 4  # Block type, level, cf.
        self.th = np.zeros((len(self.policies), self.nfeatures))
        self.eps = 0.2
        self.b = np.zeros_like(self.th)
        self.A = np.zeros((len(self.policies), self.nfeatures, self.nfeatures))
        self.A_inv = np.zeros((len(self.policies), self.nfeatures, self.nfeatures))
        for i in range(len(self.policies)):
            self.A[i] = np.identity(self.nfeatures)
        self.th_hat = np.zeros_like(self.th)
        self.p = np.zeros(len(self.policies))
        self.alph = 0.2

    def _select_policy(self, trace_record, key):
        if len(self.policies) == 1:
            return 0
        x_i = np.zeros(self.nfeatures)  # The current context vector
        x_i[0] = trace_record.block_type
        x_i[1] = trace_record.level
        x_i[2] = trace_record.cf_id
        p = np.zeros(len(self.policies))
        for a in range(len(self.policies)):
            self.th_hat[a] = self.A_inv[a].dot(self.b[a])
            ta = x_i.dot(self.A_inv[a]).dot(x_i)
            a_upper_ci = self.alph * np.sqrt(ta)
            a_mean = self.th_hat[a].dot(x_i)
            p[a] = a_mean + a_upper_ci
        p = p + (np.random.random(len(p)) * 0.000001)
        selected_policy = p.argmax()
        reward = self.policies[selected_policy].generate_reward(key)
        assert reward <= 1 and reward >= 0
        self.A[selected_policy] += np.outer(x_i, x_i)
        self.b[selected_policy] += reward * x_i
        self.A_inv[selected_policy] = np.linalg.inv(self.A[selected_policy])
        del x_i
        return selected_policy

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid LinUCB with cost class {} (linucb_hybrid)".format(
                self.cost_class_label
            )
        return f"LinUCB with cost class {self.cost_class_label} (linucb)"


class OPTCacheEntry:
    """
    A cache entry for the OPT algorithm. The entries are sorted based on its
    next access sequence number in reverse order, i.e., the entry which next
    access is the furthest in the future is ordered before other entries.
    """

    def __init__(self, key, next_access_seq_no, value_size):
        self.key = key
        self.next_access_seq_no = next_access_seq_no
        self.value_size = value_size
        self.is_removed = False

    def __cmp__(self, other):
        if other.next_access_seq_no != self.next_access_seq_no:
            return other.next_access_seq_no - self.next_access_seq_no
        return self.value_size - other.value_size

    # heapq orders entries with <; Python 3 ignores __cmp__.
    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __repr__(self):
        return "({} {} {} {})".format(
            self.key, self.next_access_seq_no, self.value_size, self.is_removed
        )


class PQTable:
    """
    A hash table with a priority queue.
    """

    def __init__(self):
        # A list of entries arranged in a heap sorted based on the entry custom
        # implementation of __cmp__
        self.pq = []
        self.table = {}

    def pqinsert(self, entry):
        "Add a new key or update the priority of an existing key"
        # Remove the entry from the table first.
        removed_entry = self.table.pop(entry.key, None)
        if removed_entry:
            # Mark as removed since there is no 'remove' API in heappq.
            # Instead, an entry in pq is removed lazily when calling pop.
            removed_entry.is_removed = True
        self.table[entry.key] = entry
        heapq.heappush(self.pq, entry)
        return removed_entry

    def pqpop(self):
        while self.pq:
            entry = heapq.heappop(self.pq)
            if not entry.is_removed:
                del self.table[entry.key]
                return entry
        return None

    def pqpeek(self):
        while self.pq:
            entry = self.pq[0]
            if not entry.is_removed:
                return entry
            heapq.heappop(self.pq)
        return

    def __contains__(self, k):
        return k in self.table

    def __getitem__(self, k):
        return self.table[k]

    def __len__(self):
        return len(self.table)

    def values(self):
        return self.table.values()


class OPTCache(Cache):
    """
    An implementation of the Belady MIN algorithm. OPTCache evicts an entry
    in the cache whose next access occurs furthest in the future.

    Note that Belady MIN algorithm is optimal assuming all blocks having the
    same size and a missing entry will be inserted in the cache.
    These are NOT true for the block cache trace since blocks have different
    sizes and we may not insert a block into the cache upon a cache miss.
    However, it is still useful to serve as a "theoretical upper bound" on the
    lowest miss ratio we can achieve given a cache size.

    L. A. Belady. 1966. A Study of Replacement Algorithms for a
    Virtual-storage Computer. IBM Syst. J. 5, 2 (June 1966), 78-101.
    DOI=http://dx.doi.org/10.1147/sj.52.0078
    """

    def __init__(self, cache_size):
        super().__init__(cache_size, enable_cache_row_key=0)
        self.table = PQTable()

    def _lookup(self, trace_record, key, hash):
        if key not in self.table:
            return False
        # A cache hit. Update its next access time.
        assert (
            self.table.pqinsert(
                OPTCacheEntry(
                    key, trace_record.next_access_seq_no, self.table[key].value_size
                )
            )
            is not None
        )
        return True

    def _evict(self, trace_record, key, hash, value_size):
        while self.used_size + value_size > self.cache_size:
            evict_entry = self.table.pqpop()
            assert evict_entry is not None
            self.used_size -= evict_entry.value_size

    def _insert(self, trace_record, key, hash, value_size):
        assert (
            self.table.pqinsert(
                OPTCacheEntry(key, trace_record.next_access_seq_no, value_size)
            )
            is None
        )

    def _should_admit(self, trace_record, key, hash, value_size):
        return True

    def cache_name(self):
        return "Belady MIN (opt)"


class GDSizeEntry:
    """
    A cache entry for the greedy dual size replacement policy.
    """

    def __init__(self, key, value_size, priority):
        self.key = key
        self.value_size = value_size
        self.priority = priority
        self.is_removed = False

    def __cmp__(self, other):
        if other.priority != self.priority:
            return self.priority - other.priority
        return self.value_size - other.value_size

    # heapq orders entries with <; Python 3 ignores __cmp__.
    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __repr__(self):
        return "({} {} {} {})".format(
            self.key, self.next_access_seq_no, self.value_size, self.is_removed
        )


class GDSizeCache(Cache):
    """
    An implementation of the greedy dual size algorithm.
    We define cost as an entry's size.

    See https://www.usenix.org/legacy/publications/library/proceedings/usits97/full_papers/cao/cao_html/node8.html
    and N. Young. The k-server dual and loose competitiveness for paging.
    Algorithmica,June 1994, vol. 11,(no.6):525-41.
    Rewritten version of ''On-line caching as cache size varies'',
    in The 2nd Annual ACM-SIAM Symposium on Discrete Algorithms, 241-250, 1991.
    """

    def __init__(self, cache_size, enable_cache_row_key):
        super().__init__(cache_size, enable_cache_row_key)
        self.table = PQTable()
        self.L = 0.0

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid GreedyDualSize (gdsize_hybrid)"
        return "GreedyDualSize (gdsize)"

    def _lookup(self, trace_record, key, hash):
        if key not in self.table:
            return False
        # A cache hit. Update its priority.
        entry = self.table[key]
        assert (
            self.table.pqinsert(
                GDSizeEntry(key, entry.value_size, self.L + entry.value_size)
            )
            is not None
        )
        return True

    def _evict(self, trace_record, key, hash, value_size):
        while self.used_size + value_size > self.cache_size:
            evict_entry = self.table.pqpop()
            assert evict_entry is not None
            self.L = evict_entry.priority
            self.used_size -= evict_entry.value_size

    def _insert(self, trace_record, key, hash, value_size):
        assert (
            self.table.pqinsert(GDSizeEntry(key, value_size, self.L + value_size))
            is None
        )

    def _should_admit(self, trace_record, key, hash, value_size):
        return True


class Deque:
    """A Deque class facilitates the implementation of LRU and ARC."""

    def __init__(self):
        self.od = OrderedDict()

    def appendleft(self, k):
        if k in self.od:
            del self.od[k]
        self.od[k] = None

    def pop(self):
        item = self.od.popitem(last=False) if self.od else None
        if item is not None:
            return item[0]
        return None

    def remove(self, k):
        del self.od[k]

    def __len__(self):
        return len(self.od)

    def __contains__(self, k):
        return k in self.od

    def __iter__(self):
        return reversed(self.od)

    def __repr__(self):
        return "Deque({!r})".format(list(self))


class S3FIFOCache(Cache):
    """
    An implementation of S3-FIFO with a small probationary queue, a main
    queue, and a ghost queue of recently evicted probation entries.

    This follows the C++ FIFOCache (cache/fifo_cache.cc):
    - new entries enter the small queue unless their key is remembered in the
      ghost queue, in which case they go directly to the main queue;
    - hits increment a per-entry frequency counter, saturating at max_freq
      (3), but do not reorder entries;
    - small-queue evictions promote entries whose frequency reached the
      promotion threshold, otherwise they are evicted and remembered in ghost;
    - main-queue eviction is CLOCK-like: entries with non-zero frequency are
      demoted by one and rotated, otherwise they are evicted;
    - the small queue's budget is 12% of the cache by size, and the queue
      that pays for an insert is chosen from the small queue's size plus the
      incoming size, which is 0 for a key that the ghost readmits to main;
    - the ghost is a ring of victim slots, oldest dropped first while their
      total size exceeds the cache size or they outnumber the cached
      entries; readmitting (or re-recording) a key leaves its old slot
      behind as a stale slot that still counts toward both bounds until it
      ages out, as GhostTake and GhostRecord do in C++.
    """

    def __init__(
        self,
        cache_size,
        enable_cache_row_key,
        small_size_ratio=0.12,
        ghost_size_ratio=1.0,
        move_to_main_threshold=2,
        max_freq=3,
    ):
        super().__init__(cache_size, enable_cache_row_key)
        self.table = {}
        self.small = Deque()
        self.main = Deque()
        self.small_size_ratio = small_size_ratio
        self.ghost_size_ratio = ghost_size_ratio
        self.move_to_main_threshold = move_to_main_threshold
        self.max_freq = max_freq
        self.small_used_size = 0
        # Ghost ring, oldest first: one (key, size, seq) slot per probation
        # victim. self.ghost maps each remembered key to the seq of its live
        # slot, and self.ghost_sizes to that slot's size. ghost_used_size
        # sums every slot in the ring, stale ones included.
        self.ghost_ring = deque()
        self.ghost = {}
        self.ghost_sizes = {}
        self.ghost_used_size = 0
        self.ghost_next_seq = 0

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid S3-FIFO (s3fifo_hybrid)"
        return "S3-FIFO (s3fifo)"

    def _in_small(self, key):
        return key in self.small

    def _in_main(self, key):
        return key in self.main

    def _small_budget(self):
        return self.cache_size * self.small_size_ratio

    def _forget_ghost(self, key):
        # Readmission drops only the key's index entry; its slot stays in the
        # ring as a stale slot, as GhostTake leaves it in C++.
        del self.ghost[key]
        del self.ghost_sizes[key]

    def _remember_ghost(self, key, value_size):
        # Called while the victim is still in the table, as GhostRecord is in
        # C++, so a full cache of N entries keeps N ghost slots. A key that
        # is already remembered gets a new slot; the old one turns stale.
        seq = self.ghost_next_seq
        self.ghost_next_seq += 1
        self.ghost_ring.append((key, value_size, seq))
        self.ghost[key] = seq
        self.ghost_sizes[key] = value_size
        self.ghost_used_size += value_size
        while len(self.ghost_ring) > 0 and (
            self.ghost_used_size > self.cache_size * self.ghost_size_ratio
            or len(self.ghost_ring) > len(self.table)
        ):
            oldest_key, oldest_size, oldest_seq = self.ghost_ring.popleft()
            if self.ghost.get(oldest_key) == oldest_seq:
                del self.ghost[oldest_key]
                del self.ghost_sizes[oldest_key]
            self.ghost_used_size -= oldest_size

    def _lookup(self, trace_record, key, hash):
        if key not in self.table:
            return False
        entry = self.table[key]
        entry.num_hits = min(entry.num_hits + 1, self.max_freq)
        return True

    def _evict_from_small(self, value_size):
        while self.used_size + value_size > self.cache_size and len(self.small) > 0:
            victim = self.small.pop()
            entry = self.table[victim]
            if entry.num_hits >= self.move_to_main_threshold:
                entry.num_hits = 0
                self.main.appendleft(victim)
                self.small_used_size -= entry.value_size
                continue
            self._remember_ghost(victim, entry.value_size)
            self.small_used_size -= entry.value_size
            self.used_size -= entry.value_size
            del self.table[victim]

    def _evict_from_main(self, value_size):
        while self.used_size + value_size > self.cache_size and len(self.main) > 0:
            victim = self.main.pop()
            entry = self.table[victim]
            if entry.num_hits > 0:
                entry.num_hits -= 1
                self.main.appendleft(victim)
                continue
            self.used_size -= entry.value_size
            del self.table[victim]

    def _evict(self, trace_record, key, hash, value_size):
        # A key the ghost readmits lands in main, so it does not count
        # against the small queue's budget.
        incoming_small = 0 if key in self.ghost else value_size
        while self.used_size + value_size > self.cache_size:
            small_first = (
                self.small_used_size + incoming_small > self._small_budget()
                or len(self.main) == 0
            )
            before = self.used_size
            if small_first:
                self._evict_from_small(value_size)
                if self.used_size + value_size > self.cache_size:
                    self._evict_from_main(value_size)
            else:
                self._evict_from_main(value_size)
                if self.used_size + value_size > self.cache_size:
                    self._evict_from_small(value_size)
                    if self.used_size == before and self.used_size + value_size > self.cache_size:
                        self._evict_from_main(value_size)
            if self.used_size == before:
                break

    def _insert(self, trace_record, key, hash, value_size):
        entry = CacheEntry(
            value_size,
            trace_record.cf_id,
            trace_record.level,
            trace_record.block_type,
            trace_record.table_id,
            0,
            trace_record.access_time,
        )
        self.table[key] = entry
        if key in self.ghost:
            self._forget_ghost(key)
            self.main.appendleft(key)
        else:
            self.small.appendleft(key)
            self.small_used_size += value_size

    def _should_admit(self, trace_record, key, hash, value_size):
        return True


class ARCCache(Cache):
    """
    An implementation of ARC. ARC assumes that all blocks are having the
    same size. The size of index and filter blocks are variable. To accommodate
    this, we modified ARC as follows:
    1) We use 16 KB as the average block size and calculate the number of blocks
       (c) in the cache.
    2) When we insert an entry, the cache evicts entries in both t1 and t2
       queues until it has enough space for the new entry. This also requires
       modification of the algorithm to maintain a maximum of 2*c blocks.

    Nimrod Megiddo and Dharmendra S. Modha. 2003. ARC: A Self-Tuning, Low
    Overhead Replacement Cache. In Proceedings of the 2nd USENIX Conference on
    File and Storage Technologies (FAST '03). USENIX Association, Berkeley, CA,
    USA, 115-130.
    """

    def __init__(self, cache_size, enable_cache_row_key):
        super().__init__(cache_size, enable_cache_row_key)
        self.table = {}
        self.c = cache_size / 16 * 1024  # Number of elements in the cache.
        self.p = 0  # Target size for the list T1
        # L1: only once recently
        self.t1 = Deque()  # T1: recent cache entries
        self.b1 = Deque()  # B1: ghost entries recently evicted from the T1 cache
        # L2: at least twice recently
        self.t2 = Deque()  # T2: frequent entries
        self.b2 = Deque()  # B2: ghost entries recently evicted from the T2 cache

    def _replace(self, key, value_size):
        while self.used_size + value_size > self.cache_size:
            if self.t1 and ((key in self.b2) or (len(self.t1) > self.p)):
                old = self.t1.pop()
                self.b1.appendleft(old)
            else:
                if self.t2:
                    old = self.t2.pop()
                    self.b2.appendleft(old)
                else:
                    old = self.t1.pop()
                    self.b1.appendleft(old)
            self.used_size -= self.table[old].value_size
            del self.table[old]

    def _lookup(self, trace_record, key, hash):
        # Case I: key is in T1 or T2.
        #   Move key to MRU position in T2.
        if key in self.t1:
            self.t1.remove(key)
            self.t2.appendleft(key)
            return True

        if key in self.t2:
            self.t2.remove(key)
            self.t2.appendleft(key)
            return True
        return False

    def _evict(self, trace_record, key, hash, value_size):
        # Case II: key is in B1
        #   Move x from B1 to the MRU position in T2 (also fetch x to the cache).
        if key in self.b1:
            self.p = min(self.c, self.p + max(len(self.b2) / len(self.b1), 1))
            self._replace(key, value_size)
            self.b1.remove(key)
            self.t2.appendleft(key)
            return

        # Case III: key is in B2
        #   Move x from B2 to the MRU position in T2 (also fetch x to the cache).
        if key in self.b2:
            self.p = max(0, self.p - max(len(self.b1) / len(self.b2), 1))
            self._replace(key, value_size)
            self.b2.remove(key)
            self.t2.appendleft(key)
            return

        # Case IV: key is not in (T1 u B1 u T2 u B2)
        self._replace(key, value_size)
        while len(self.t1) + len(self.b1) >= self.c and self.b1:
            self.b1.pop()

        total = len(self.t1) + len(self.b1) + len(self.t2) + len(self.b2)
        while total >= (2 * self.c) and self.b2:
            self.b2.pop()
            total -= 1
        # Finally, move it to MRU position in T1.
        self.t1.appendleft(key)
        return

    def _insert(self, trace_record, key, hash, value_size):
        self.table[key] = CacheEntry(
            value_size,
            trace_record.cf_id,
            trace_record.level,
            trace_record.block_type,
            trace_record.table_id,
            0,
            trace_record.access_time,
        )

    def _should_admit(self, trace_record, key, hash, value_size):
        return True

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid Adaptive Replacement Cache (arc_hybrid)"
        return "Adaptive Replacement Cache (arc)"


class LRUCache(Cache):
    """
    A strict LRU queue.
    """

    def __init__(self, cache_size, enable_cache_row_key):
        super().__init__(cache_size, enable_cache_row_key)
        self.table = {}
        self.lru = Deque()

    def cache_name(self):
        if self.enable_cache_row_key:
            return "Hybrid LRU (lru_hybrid)"
        return "LRU (lru)"

    def _lookup(self, trace_record, key, hash):
        if key not in self.table:
            return False
        # A cache hit. Update LRU queue.
        self.lru.remove(key)
        self.lru.appendleft(key)
        return True

    def _evict(self, trace_record, key, hash, value_size):
        while self.used_size + value_size > self.cache_size:
            evict_key = self.lru.pop()
            self.used_size -= self.table[evict_key].value_size
            del self.table[evict_key]

    def _insert(self, trace_record, key, hash, value_size):
        self.table[key] = CacheEntry(
            value_size,
            trace_record.cf_id,
            trace_record.level,
            trace_record.block_type,
            trace_record.table_id,
            0,
            trace_record.access_time,
        )
        self.lru.appendleft(key)

    def _should_admit(self, trace_record, key, hash, value_size):
        return True


class TraceCache(Cache):
    """
    A trace cache. Lookup returns true if the trace observes a cache hit.
    It is used to maintain cache hits observed in the trace.
    """

    def __init__(self, cache_size):
        super().__init__(cache_size, enable_cache_row_key=0)

    def _lookup(self, trace_record, key, hash):
        return trace_record.is_hit

    def _evict(self, trace_record, key, hash, value_size):
        pass

    def _insert(self, trace_record, key, hash, value_size):
        pass

    def _should_admit(self, trace_record, key, hash, value_size):
        return False

    def cache_name(self):
        return "Trace"


def parse_cache_size(cs):
    cs = cs.replace("\n", "")
    if cs[-1] == "M":
        return int(cs[: len(cs) - 1]) * 1024 * 1024
    if cs[-1] == "G":
        return int(cs[: len(cs) - 1]) * 1024 * 1024 * 1024
    if cs[-1] == "T":
        return int(cs[: len(cs) - 1]) * 1024 * 1024 * 1024 * 1024
    return int(cs)


def create_cache(cache_type, cache_size, downsample_size):
    cache_size = cache_size / downsample_size
    enable_cache_row_key = 0
    if "hybridn" in cache_type:
        enable_cache_row_key = 2
        cache_type = cache_type[:-8]
    if "hybrid" in cache_type:
        enable_cache_row_key = 1
        cache_type = cache_type[:-7]
    if cache_type == "ts":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [LRUPolicy(), LFUPolicy(), HyperbolicPolicy()],
            cost_class_label=None,
        )
    elif cache_type == "linucb":
        return LinUCBCache(
            cache_size,
            enable_cache_row_key,
            [LRUPolicy(), LFUPolicy(), HyperbolicPolicy()],
            cost_class_label=None,
        )
    elif cache_type == "pylru":
        return ThompsonSamplingCache(
            cache_size, enable_cache_row_key, [LRUPolicy()], cost_class_label=None
        )
    elif cache_type == "pymru":
        return ThompsonSamplingCache(
            cache_size, enable_cache_row_key, [MRUPolicy()], cost_class_label=None
        )
    elif cache_type == "pylfu":
        return ThompsonSamplingCache(
            cache_size, enable_cache_row_key, [LFUPolicy()], cost_class_label=None
        )
    elif cache_type == "pyhb":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [HyperbolicPolicy()],
            cost_class_label=None,
        )
    elif cache_type == "pycctbbt":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [CostClassPolicy()],
            cost_class_label="table_bt",
        )
    elif cache_type == "pycccf":
        return ThompsonSamplingCache(
            cache_size, enable_cache_row_key, [CostClassPolicy()], cost_class_label="cf"
        )
    elif cache_type == "pycctblevelbt":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [CostClassPolicy()],
            cost_class_label="table_level_bt",
        )
    elif cache_type == "pycccfbt":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [CostClassPolicy()],
            cost_class_label="cf_bt",
        )
    elif cache_type == "pycctb":
        return ThompsonSamplingCache(
            cache_size,
            enable_cache_row_key,
            [CostClassPolicy()],
            cost_class_label="table",
        )
    elif cache_type == "pyccbt":
        return ThompsonSamplingCache(
            cache_size, enable_cache_row_key, [CostClassPolicy()], cost_class_label="bt"
        )
    elif cache_type == "opt":
        if enable_cache_row_key:
            print("opt does not support hybrid mode.")
            assert False
        return OPTCache(cache_size)
    elif cache_type == "trace":
        if enable_cache_row_key:
            print("trace does not support hybrid mode.")
            assert False
        return TraceCache(cache_size)
    elif cache_type == "lru":
        return LRUCache(cache_size, enable_cache_row_key)
    elif cache_type == "arc":
        return ARCCache(cache_size, enable_cache_row_key)
    elif cache_type == "s3fifo":
        return S3FIFOCache(cache_size, enable_cache_row_key)
    elif cache_type == "gdsize":
        return GDSizeCache(cache_size, enable_cache_row_key)
    else:
        print(f"Unknown cache type {cache_type}")
        assert False
    return None


class BlockAccessTimeline:
    """
    BlockAccessTimeline stores all accesses of a block.
    """

    def __init__(self):
        self.accesses = []
        self.current_access_index = 1

    def get_next_access(self):
        if self.current_access_index == len(self.accesses):
            return sys.maxsize
        next_access_seq_no = self.accesses[self.current_access_index]
        self.current_access_index += 1
        return next_access_seq_no


def percent(e1, e2):
    if e2 == 0:
        return -1
    return float(e1) * 100.0 / float(e2)


def is_target_cf(access_cf, target_cf_name):
    if target_cf_name == "all":
        return True
    return access_cf == target_cf_name


def run(
    trace_file_path,
    cache_type,
    cache,
    warmup_seconds,
    max_accesses_to_process,
    target_cf_name,
):
    warmup_complete = False
    trace_miss_ratio_stats = MissRatioStats(kSecondsInMinute)
    access_seq_no = 0
    time_interval = 1
    start_time = time.time()
    trace_start_time = 0
    trace_duration = 0
    is_opt_cache = False
    if cache.cache_name() == "Belady MIN (opt)":
        is_opt_cache = True

    block_access_timelines = {}
    num_no_inserts = 0
    num_blocks_with_no_size = 0
    num_inserts_block_with_no_size = 0

    if is_opt_cache:
        # Read all blocks in memory and stores their access times so that OPT
        # can use this information to evict the cached key which next access is
        # the furthest in the future.
        print("Preprocessing block traces.")
        with open(trace_file_path) as trace_file:
            for line in trace_file:
                if (
                    max_accesses_to_process != -1
                    and access_seq_no > max_accesses_to_process
                ):
                    break
                ts = line.split(",")
                timestamp = int(ts[0])
                cf_name = ts[5]
                if not is_target_cf(cf_name, target_cf_name):
                    continue
                if trace_start_time == 0:
                    trace_start_time = timestamp
                trace_duration = timestamp - trace_start_time
                block_id = int(ts[1])
                block_size = int(ts[3])
                no_insert = int(ts[9])
                if block_id not in block_access_timelines:
                    block_access_timelines[block_id] = BlockAccessTimeline()
                    if block_size == 0:
                        num_blocks_with_no_size += 1
                block_access_timelines[block_id].accesses.append(access_seq_no)
                access_seq_no += 1
                if no_insert == 1:
                    num_no_inserts += 1
                if no_insert == 0 and block_size == 0:
                    num_inserts_block_with_no_size += 1
                if access_seq_no % 100 != 0:
                    continue
                now = time.time()
                if now - start_time > time_interval * 10:
                    print(
                        "Take {} seconds to process {} trace records with trace "
                        "duration of {} seconds. Throughput: {} records/second.".format(
                            now - start_time,
                            access_seq_no,
                            trace_duration / 1000000,
                            access_seq_no / (now - start_time),
                        )
                    )
                    time_interval += 1
            print(
                "Trace contains {} blocks, {}({:.2f}%) blocks with no size."
                "{} accesses, {}({:.2f}%) accesses with no_insert,"
                "{}({:.2f}%) accesses that want to insert but block size is 0.".format(
                    len(block_access_timelines),
                    num_blocks_with_no_size,
                    percent(num_blocks_with_no_size, len(block_access_timelines)),
                    access_seq_no,
                    num_no_inserts,
                    percent(num_no_inserts, access_seq_no),
                    num_inserts_block_with_no_size,
                    percent(num_inserts_block_with_no_size, access_seq_no),
                )
            )

    access_seq_no = 0
    time_interval = 1
    start_time = time.time()
    trace_start_time = 0
    trace_duration = 0
    print(f"Running simulated {cache.cache_name()} cache on block traces.")
    with open(trace_file_path) as trace_file:
        for line in trace_file:
            if (
                max_accesses_to_process != -1
                and access_seq_no > max_accesses_to_process
            ):
                break
            if access_seq_no % 1000000 == 0:
                # Force a python gc periodically to reduce memory usage.
                gc.collect()
            ts = line.split(",")
            timestamp = int(ts[0])
            cf_name = ts[5]
            if not is_target_cf(cf_name, target_cf_name):
                continue
            if trace_start_time == 0:
                trace_start_time = timestamp
            trace_duration = timestamp - trace_start_time
            if (
                not warmup_complete
                and warmup_seconds > 0
                and trace_duration > warmup_seconds * 1000000
            ):
                cache.miss_ratio_stats.reset_counter()
                warmup_complete = True
            next_access_seq_no = 0
            block_id = int(ts[1])
            if is_opt_cache:
                next_access_seq_no = block_access_timelines[block_id].get_next_access()
            record = TraceRecord(
                access_time=int(ts[0]),
                block_id=int(ts[1]),
                block_type=int(ts[2]),
                block_size=int(ts[3]),
                cf_id=int(ts[4]),
                cf_name=ts[5],
                level=int(ts[6]),
                fd=int(ts[7]),
                caller=int(ts[8]),
                no_insert=int(ts[9]),
                get_id=int(ts[10]),
                key_id=int(ts[11]),
                kv_size=int(ts[12]),
                is_hit=int(ts[13]),
                referenced_key_exist_in_block=int(ts[14]),
                num_keys_in_block=int(ts[15]),
                table_id=int(ts[16]),
                seq_number=int(ts[17]),
                block_key_size=int(ts[18]),
                key_size=int(ts[19]),
                block_offset_in_file=int(ts[20]),
                next_access_seq_no=next_access_seq_no,
            )
            trace_miss_ratio_stats.update_metrics(
                record.access_time, is_hit=record.is_hit, miss_bytes=record.block_size
            )
            cache.access(record)
            access_seq_no += 1
            del record
            del ts
            if access_seq_no % 100 != 0:
                continue
            # Report progress every 10 seconds.
            now = time.time()
            if now - start_time > time_interval * 10:
                print(
                    "Take {} seconds to process {} trace records with trace "
                    "duration of {} seconds. Throughput: {} records/second. "
                    "Trace miss ratio {}".format(
                        now - start_time,
                        access_seq_no,
                        trace_duration / 1000000,
                        access_seq_no / (now - start_time),
                        trace_miss_ratio_stats.miss_ratio(),
                    )
                )
                time_interval += 1
                print(
                    "{},0,0,{},{},{}".format(
                        cache_type,
                        cache.cache_size,
                        cache.miss_ratio_stats.miss_ratio(),
                        cache.miss_ratio_stats.num_accesses,
                    )
                )
    now = time.time()
    print(
        "Take {} seconds to process {} trace records with trace duration of {} "
        "seconds. Throughput: {} records/second. Trace miss ratio {}".format(
            now - start_time,
            access_seq_no,
            trace_duration / 1000000,
            access_seq_no / (now - start_time),
            trace_miss_ratio_stats.miss_ratio(),
        )
    )
    print(
        "{},0,0,{},{},{}".format(
            cache_type,
            cache.cache_size,
            cache.miss_ratio_stats.miss_ratio(),
            cache.miss_ratio_stats.num_accesses,
        )
    )
    return trace_start_time, trace_duration


def report_stats(
    cache,
    cache_type,
    cache_size,
    target_cf_name,
    result_dir,
    trace_start_time,
    trace_end_time,
):
    cache_label = f"{cache_type}-{cache_size}-{target_cf_name}"
    with open(f"{result_dir}/data-ml-mrc-{cache_label}", "w+") as mrc_file:
        mrc_file.write(
            "{},0,0,{},{},{}\n".format(
                cache_type,
                cache_size,
                cache.miss_ratio_stats.miss_ratio(),
                cache.miss_ratio_stats.num_accesses,
            )
        )

    cache_stats = [
        cache.per_second_miss_ratio_stats,
        cache.miss_ratio_stats,
        cache.per_hour_miss_ratio_stats,
    ]
    for i in range(len(cache_stats)):
        avg_miss_bytes, p95_miss_bytes = cache_stats[i].compute_miss_bytes()

        with open(
            "{}/data-ml-avgmb-{}-{}".format(
                result_dir, cache_stats[i].time_unit, cache_label
            ),
            "w+",
        ) as mb_file:
            mb_file.write(
                f"{cache_type},0,0,{cache_size},{avg_miss_bytes}\n"
            )

        with open(
            "{}/data-ml-p95mb-{}-{}".format(
                result_dir, cache_stats[i].time_unit, cache_label
            ),
            "w+",
        ) as mb_file:
            mb_file.write(
                f"{cache_type},0,0,{cache_size},{p95_miss_bytes}\n"
            )

        cache_stats[i].write_miss_timeline(
            cache_type,
            cache_size,
            target_cf_name,
            result_dir,
            trace_start_time,
            trace_end_time,
        )
        cache_stats[i].write_miss_ratio_timeline(
            cache_type,
            cache_size,
            target_cf_name,
            result_dir,
            trace_start_time,
            trace_end_time,
        )

    if not cache.is_ml_cache():
        return

    policy_stats = [cache.policy_stats, cache.per_hour_policy_stats]
    for i in range(len(policy_stats)):
        policy_stats[i].write_policy_timeline(
            cache_type,
            cache_size,
            target_cf_name,
            result_dir,
            trace_start_time,
            trace_end_time,
        )
        policy_stats[i].write_policy_ratio_timeline(
            cache_type,
            cache_size,
            target_cf_name,
            result_dir,
            trace_start_time,
            trace_end_time,
        )


if __name__ == "__main__":
    if len(sys.argv) <= 8:
        print(
            "Must provide 8 arguments.\n"
            "1) Cache type (ts, linucb, arc, lru, opt, pylru, pymru, pylfu, "
            "pyhb, gdsize, trace). One may evaluate the hybrid row_block cache "
            "by appending '_hybrid' to a cache_type, e.g., ts_hybrid. "
            "Note that hybrid is not supported with opt and trace. \n"
            "2) Cache size (xM, xG, xT).\n"
            "3) The sampling frequency used to collect the trace. (The "
            "simulation scales down the cache size by the sampling frequency).\n"
            "4) Warmup seconds (The number of seconds used for warmup).\n"
            "5) Trace file path.\n"
            "6) Result directory (A directory that saves generated results)\n"
            "7) Max number of accesses to process\n"
            "8) The target column family. (The simulation will only run "
            "accesses on the target column family. If it is set to all, "
            "it will run against all accesses.)"
        )
        exit(1)
    print(f"Arguments: {sys.argv}")
    cache_type = sys.argv[1]
    cache_size = parse_cache_size(sys.argv[2])
    downsample_size = int(sys.argv[3])
    warmup_seconds = int(sys.argv[4])
    trace_file_path = sys.argv[5]
    result_dir = sys.argv[6]
    max_accesses_to_process = int(sys.argv[7])
    target_cf_name = sys.argv[8]
    cache = create_cache(cache_type, cache_size, downsample_size)
    trace_start_time, trace_duration = run(
        trace_file_path,
        cache_type,
        cache,
        warmup_seconds,
        max_accesses_to_process,
        target_cf_name,
    )
    trace_end_time = trace_start_time + trace_duration
    report_stats(
        cache,
        cache_type,
        cache_size,
        target_cf_name,
        result_dir,
        trace_start_time,
        trace_end_time,
    )
