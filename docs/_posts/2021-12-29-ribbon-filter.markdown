---
title: Ribbon Filter
layout: post
author: pdillinger
category: blog
---

## Summary
Since version 6.15 last year, RocksDB supports Ribbon filters, a new
alternative to Bloom filters that save space, especially memory, at
the cost of more CPU usage, mostly in constructing the filters in the
background. Most applications with long-lived data (many hours or
longer) will likely benefit from adopting a Ribbon+Bloom hybrid filter
policy. Here we explain why and how.

[Ribbon filter on RocksDB wiki](https://github.com/meta/rocksdb/wiki/RocksDB-Bloom-Filter#ribbon-filter)

[Ribbon filter paper](https://arxiv.org/abs/2103.02515)

## Problem & background
Bloom filters play a critical role in optimizing point queries and
some range queries in LSM-tree storage systems like RocksDB. Very
large DBs can use 10% or more of their RAM memory for (Bloom) filters,
so that (average case) read performance can be very good despite high
(worst case) read amplification, [which is useful for lowering write
and/or space
amplification](http://smalldatum.blogspot.com/2015/11/read-write-space-amplification-pick-2_23.html).
Although the `format_version=5` Bloom filter in RocksDB is extremely
fast, all Bloom filters use around 50% more space than is
theoretically possible for a hashed structure configured for the same
false positive (FP) rate and number of keys added. What would it take
to save that significant share of “wasted” filter memory, and when
does it make sense to use such a Bloom alternative?

A number of alternatives to Bloom filters were known, especially for
static filters (not modified after construction), but all the
previously known structures were unsatisfying for SSTs because of some
combination of
* Not enough space savings for CPU increase. For example, [Xor
  filters](https://arxiv.org/abs/1912.08258) use 3-4x more CPU than
  Bloom but only save 15-20% of
  space. [GOV](https://arxiv.org/pdf/1603.04330.pdf) can save around
  30% space but requires around 10x more CPU than Bloom.
* Inconsistent space savings. [Cuckoo
  filters](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)
  and Xor+ filters offer significant space savings for very low FP
  rates (high bits per key) but little or no savings for higher FP
  rates (low bits per key). ([Higher FP rates are considered best for
  largest levels of
  LSM.](https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf))
  [Spatially-coupled Xor
  filters](https://arxiv.org/pdf/2001.10500.pdf) require very large
  number of keys per filter for large space savings.
* Inflexible configuration. No published alternatives offered the same
  continuous configurability of Bloom filters, where any FP rate and
  any fractional bits per key could be chosen. This flexibility
  improves memory efficiency with the `optimize_filters_for_memory`
  option that minimizes internal fragmentation on filters.

## Ribbon filter development and implementation
The Ribbon filter came about when I developed a faster, simpler, and
more adaptable algorithm for constructing a little-known [Xor-based
structure from Dietzfelbinger and
Walzer](https://arxiv.org/pdf/1907.04750.pdf). It has very good space
usage for required CPU time (~30% space savings for 3-4x CPU) and,
with some engineering, Bloom-like configurability. The complications
were managable for use in RocksDB:
* Ribbon space efficiency does not naturally scale to very large
  number of keys in a single filter (whole SST file or partition), but
  with the current 128-bit Ribbon implementation in RocksDB, even 100
  million keys in one filter saves 27% space vs. Bloom rather than 30%
  for 100,000 keys in a filter.
* More temporary memory is required during construction, ~230 bits per
  key for 128-bit Ribbon vs. ~75 bits per key for Bloom filter. A
  quick calculation shows that if you are saving 3 bits per key on the
  generated filter, you only need about 50 generated filters in memory
  to offset this temporary memory usage. (Thousands of filters in
  memory is typical.) Starting in RocksDB version 6.27, this temporary
  memory can be accounted for under block cache using
  `BlockBasedTableOptions::reserve_table_builder_memory`.
* Ribbon filter queries use relatively more CPU for lower FP rates
  (but still O(1) relative to number of keys added to filter). This
  should be OK because lower FP rates are only appropriate when then
  cost of a false positive is very high (worth extra query time) or
  memory is not so constrained (can use Bloom instead).

Future: data in [the paper](https://arxiv.org/abs/2103.02515) suggests
that 32-bit Balanced Ribbon (new name: [Bump-Once
Ribbon](https://arxiv.org/pdf/2109.01892.pdf)) would improve all of
these issues and be better all around (except for code complexity).

## Ribbon vs. Bloom in RocksDB configuration
Different applications and hardware configurations have different
constraints, but we can use hardware costs to examine and better
understand the trade-off between Bloom and Ribbon.

### Same FP rate, RAM vs. CPU hardware cost
Under ideal conditions where we can adjust our hardware to suit the
application, in terms of dollars, how much does it cost to construct,
query, and keep in memory a Bloom filter vs. a Ribbon filter?  The
Ribbon filter costs more for CPU but less for RAM. Importantly, the
RAM cost directly depends on how long the filter is kept in memory,
which in RocksDB is essentially the lifetime of the filter.
(Temporary RAM during construction is so short-lived that it is
ignored.)  Using some consumer hardware and electricity prices and a
predicted balance between construction and queries, we can compute a
“break even” duration in memory. To minimize cost, filters with a
lifetime shorter than this should be Bloom and filters with a lifetime
longer than this should be Ribbon. (Python code)

```
# Commodity prices based roughly on consumer prices and rough guesses
# Upfront cost of a CPU per hardware thread
upfront_dollars_per_cpu_thread = 30.0

# CPU average power usage per hardware thread
watts_per_cpu_thread = 3.5

# Upfront cost of a GB of RAM
upfront_dollars_per_gb_ram = 8.0

# RAM average power usage per GB
# https://www.crucial.com/support/articles-faq-memory/how-much-power-does-memory-use
watts_per_gb_ram = 0.375

# Estimated price of power per kilowatt-hour, including overheads like conversion losses and cooling
dollars_per_kwh = 0.35

# Assume 3 year hardware lifetime
hours_per_lifetime = 3 * 365 * 24
seconds_per_lifetime = hours_per_lifetime * 60 * 60

# Number of filter queries per key added in filter construction is heavily dependent on workload.
# When replication is in layer above RocksDB, it will be low, likely < 1. When replication is in
# storage layer below RocksDB, it will likely be > 1. Using a rough and general guesstimate.
key_query_per_construct = 1.0

#==================================
# Bloom & Ribbon filter performance
typical_bloom_bits_per_key = 10.0
typical_ribbon_bits_per_key = 7.0

# Speeds here are sensitive to many variables, especially query speed because it
# is so dependent on memory latency. Using this benchmark here:
# for IMPL in 2 3; do
#   ./filter_bench -impl=$IMPL -quick -m_keys_total_max=200 -use_full_block_reader
# done
# and "Random filter" queries.
nanoseconds_per_construct_bloom_key = 32.0
nanoseconds_per_construct_ribbon_key = 140.0

nanoseconds_per_query_bloom_key = 500.0
nanoseconds_per_query_ribbon_key = 600.0

#==================================
# Some constants
kwh_per_watt_lifetime = hours_per_lifetime / 1000.0
bits_per_gb = 8 * 1024 * 1024 * 1024

#==================================
# Crunching the numbers
# on CPU for constructing filters
dollars_per_cpu_thread_lifetime = upfront_dollars_per_cpu_thread + watts_per_cpu_thread * kwh_per_watt_lifetime * dollars_per_kwh
dollars_per_cpu_thread_second = dollars_per_cpu_thread_lifetime / seconds_per_lifetime

dollars_per_construct_bloom_key = dollars_per_cpu_thread_second * nanoseconds_per_construct_bloom_key / 10**9
dollars_per_construct_ribbon_key = dollars_per_cpu_thread_second * nanoseconds_per_construct_ribbon_key / 10**9

dollars_per_query_bloom_key = dollars_per_cpu_thread_second * nanoseconds_per_query_bloom_key / 10**9
dollars_per_query_ribbon_key = dollars_per_cpu_thread_second * nanoseconds_per_query_ribbon_key / 10**9

dollars_per_bloom_key_cpu = dollars_per_construct_bloom_key + key_query_per_construct * dollars_per_query_bloom_key
dollars_per_ribbon_key_cpu = dollars_per_construct_ribbon_key + key_query_per_construct * dollars_per_query_ribbon_key

# on holding filters in RAM
dollars_per_gb_ram_lifetime = upfront_dollars_per_gb_ram + watts_per_gb_ram * kwh_per_watt_lifetime * dollars_per_kwh
dollars_per_gb_ram_second = dollars_per_gb_ram_lifetime / seconds_per_lifetime

dollars_per_bloom_key_in_ram_second = dollars_per_gb_ram_second / bits_per_gb * typical_bloom_bits_per_key
dollars_per_ribbon_key_in_ram_second = dollars_per_gb_ram_second / bits_per_gb * typical_ribbon_bits_per_key

#==================================
# How many seconds does it take for the added cost of constructing a ribbon filter instead
# of bloom to be offset by the added cost of holding the bloom filter in memory?
break_even_seconds = (dollars_per_ribbon_key_cpu - dollars_per_bloom_key_cpu) / (dollars_per_bloom_key_in_ram_second - dollars_per_ribbon_key_in_ram_second)
print(break_even_seconds)
# -> 3235.1647730256936
```

So roughly speaking, filters that live in memory for more than an hour
should be Ribbon, and filters that live less than an hour should be
Bloom. This is very interesting, but how long do filters live in
RocksDB?

First let's consider the average case. Write-heavy RocksDB loads are
often backed by flash storage, which has some specified write
endurance for its intended lifetime. This can be expressed as *device
writes per day* (DWPD), and supported DWPD is typically < 10.0 even
for high end devices (excluding NVRAM). Roughly speaking, the DB would
need to be writing at a rate of 20+ DWPD for data to have an average
lifetime of less than one hour. Thus, unless you are prematurely
burning out your flash or massively under-utilizing available storage,
using the Ribbon filter has the better cost profile *on average*.

### Predictable lifetime
But we can do even better than optimizing for the average case. LSM
levels give us very strong data lifetime hints.  Data in L0 might live
for minutes or a small number of hours. Data in Lmax might live for
days or weeks. So even if Ribbon filters weren't the best choice on
average for a workload, they almost certainly make sense for the
larger, longer-lived levels of the LSM. As of RocksDB 6.24, you can
specify a minimum LSM level for Ribbon filters with
`NewRibbonFilterPolicy`, and earlier levels will use Bloom filters.

### Resident filter memory
The above analysis assumes that nearly all filters for all live SST
files are resident in memory. This is true if using
`cache_index_and_filter_blocks=0` and `max_open_files=-1` (defaults),
but `cache_index_and_filter_blocks=1` is popular. In that case,
if you use `optimize_filters_for_hits=1` and non-partitioned filters
(a popular MyRocks configuration), it is also likely that nearly all
live filters are in memory. However, if you don't use
`optimize_filters_for_hits` and use partitioned filters, then
cold data (by age or by key range) can lead to only a portion of
filters being resident in memory. In that case, benefit from Ribbon
filter is not as clear, though because Ribbon filters are smaller,
they are more efficient to read into memory.

RocksDB version 6.21 and later include a rough feature to determine
block cache usage for data blocks, filter blocks, index blocks, etc.
Data like this is periodically dumped to LOG file
(`stats_dump_period_sec`):

```
Block cache entry stats(count,size,portion): DataBlock(441761,6.82 GB,75.765%) FilterBlock(3002,1.27 GB,14.1387%) IndexBlock(17777,887.75 MB,9.63267%) Misc(1,0.00 KB,0%)
Block cache LRUCache@0x7fdd08104290#7004432 capacity: 9.00 GB collections: 2573 last_copies: 10 last_secs: 0.143248 secs_since: 0
```

This indicates that at this moment in time, the block cache object
identified by `LRUCache@0x7fdd08104290#7004432` (potentially used
by multiple DBs) uses roughly 14% of its 9GB, about 1.27 GB, on filter
blocks. This same data is available through `DB::GetMapProperty` with
`DB::Properties::kBlockCacheEntryStats`, and (with some effort) can
be compared to total size of all filters (not necessarily in memory)
using `rocksdb.filter.size` from
`DB::Properties::kAggregatedTableProperties`.

### Sanity checking lifetime
Can we be sure that using filters even makes sense for such long-lived
data? We can apply [the current 5 minute rule for caching SSD data in
RAM](http://renata.borovica-gajic.com/data/adms2017_5minuterule.pdf). A
4KB filter page holds data for roughly 4K keys. If we assume at least
one negative (useful) filter query in its lifetime per added key, it
can satisfy the 5 minute rule with a lifetime of up to about two
weeks. Thus, the lifetime threshold for “no filter” is about 300x
higher than the lifetime threshold for Ribbon filter.

### What to do with saved memory
The default way to improve overall RocksDB performance with more
available memory is to use more space for caching, which improves
latency, CPU load, read IOs, etc.  With
`cache_index_and_filter_blocks=1`, savings in filters will
automatically make room for caching more data blocks in block
cache. With `cache_index_and_filter_blocks=0`, consider increasing
block cache size.

Using the space savings to lower filter FP rates is also an option,
but there is less evidence for this commonly improving existing
*optimized* configurations.

## Generic recommendation
If using `NewBloomFilterPolicy(bpk)` for a large persistent DB using
compression, try using `NewRibbonFilterPolicy(bpk)` instead, which
will generate Ribbon filters during compaction and Bloom filters
for flush, both with the same FP rate as the old setting. Once new SST
files are generated under the new policy, this should free up some
memory for more caching without much effect on burst or sustained
write speed. Both kinds of filters can be read under either policy, so
there's always an option to adjust settings or gracefully roll back to
using Bloom filter only (keeping in mind that SST files must be
replaced to see effect of that change).
