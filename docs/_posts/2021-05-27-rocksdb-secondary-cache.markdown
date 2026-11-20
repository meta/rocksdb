---
title: RocksDB Secondary Cache
layout: post
author: anand1976
category: blog
---
## Introduction

The RocksDB team is implementing support for a block cache on non-volatile media, such as a local flash device or NVM/SCM. It can be viewed as an extension of RocksDB’s current volatile block cache (LRUCache or ClockCache). The non-volatile block cache acts as a second tier cache that contains blocks evicted from the volatile cache. Those blocks are then promoted to the volatile cache as they become hotter due to access.

This feature is meant for cases where the DB is located on remote storage or cloud storage. The non-volatile cache is officially referred to in RocksDB as the SecondaryCache. By maintaining a SecondaryCache that’s an order of magnitude larger than DRAM, fewer reads would be required from remote storage, thus reducing read latency as well as network bandwidth consumption. 

From the user point of view, the local flash cache will support the following requirements -

1. Provide a pointer to a secondary cache when opening a DB
2. Be able to share the secondary cache across DBs in the same process
3. Have multiple secondary caches on a host
4. Support persisting the cache across process restarts and reboots by ensuring repeatability of the cache key

![Architecture](/static/images/rocksdb-secondary-cache/arch_diagram.png)
{: style="display: block; margin-left: auto; margin-right: auto; width: 80%"}

## Design

When designing the API for a SecondaryCache, we had a choice between making it visible to the RocksDB code (table reader) or hiding it behind the RocksDB block cache. There are several advantages of hiding it behind the block cache -

* Allows flexibility in insertion of blocks into the secondary cache. A block can be inserted on eviction from the RAM tier, or it could be eagerly inserted.
* It makes the rest of the RocksDB code less complex by providing a uniform interface regardless of whether a secondary cache is configured or not
* Makes parallel reads, peeking in the cache for prefetching, failure handling etc. easier
* Makes it easier to extend to compressed data if needed, and allows other persistent media, such as PM, to be added as an additional tier


We decided to make the secondary cache transparent to the rest of RocksDB code by hiding it behind the block cache. A key issue that we needed to address was the allocation and ownership of memory of the cached items - insertion into the secondary cache may require that memory be allocated by the same. This means that parts of the cached object that can be transferred to the secondary cache needs to be copied out (referred to as **unpacking**), and on a lookup the data stored in the secondary cache needs to be provided to the object constructor (referred to as **packing**). For RocksDB cached objects such as data blocks, index and filter blocks, and compression dictionaries, unpacking involves copying out the raw uncompressed BlockContents of the block, and packing involves constructing the corresponding block/index/filter/dictionary object using the raw uncompressed data.

Another alternative we considered was the existing PersistentCache interface. However, we decided to not pursue it and eventually deprecate it for the following reasons -
* It is exposed directly to the table reader code, which makes it more difficult to implement different policies such as inclusive/exclusive cache, as well as extending it to more sophisticated admission control policies
* The interface does not allow for custom memory allocation and object packing/unpacking, so new APIs would have to be defined anyway
* The current PersistentCache implementation is very simple and does not have any admission control policies

## API

The interface between RocksDB’s block cache and the secondary cache is designed to allow pluggable implementations. For FB internal usage, we plan to use Cachelib  with a wrapper to provide the plug-in implementation and use folly and other fbcode libraries, which cannot be used directly by RocksDB, to efficiently implement the cache operations. The following diagrams show the flow of insertion  and lookup of a block.

![Insert flow](/static/images/rocksdb-secondary-cache/insert_flow.png)
{: style="display: block; margin-left: auto; margin-right: auto; width: 80%"}

![Lookup flow](/static/images/rocksdb-secondary-cache/lookup_flow.png)
{: style="display: block; margin-left: auto; margin-right: auto; width: 80%"}

An item in the secondary cache is referenced by a SecondaryCacheHandle. The handle may not be immediately ready or have a valid  value. The caller can call IsReady() to determine if its ready, and can call Wait() in order to block until it becomes ready. The caller must call Value() after it becomes ready to determine if the item was successfully read. Value() must return nullptr on failure.

```
class SecondaryCacheHandle {
 public:
  virtual ~SecondaryCacheHandle() {}

  // Returns whether the handle is ready or not
  virtual bool IsReady() = 0;

  // Block until handle becomes ready
  virtual void Wait() = 0;

  // Return the value. If nullptr, it means the lookup was unsuccessful
  virtual void* Value() = 0;

  // Return the size of value
  virtual size_t Size() = 0;
};
```

The user of the secondary cache (for example, BlockBasedTableReader indirectly through LRUCache) must implement the callbacks defined in CacheItemHelper, in order to facilitate the unpacking/packing of objects for saving to and restoring from the secondary cache. The CreateCallback must be implemented to construct a cacheable object from the raw data in secondary cache.

```
  // The SizeCallback takes a void* pointer to the object and returns the size
  // of the persistable data. It can be used by the secondary cache to allocate
  // memory if needed.
  using SizeCallback = size_t (*)(void* obj);

  // The SaveToCallback takes a void* object pointer and saves the persistable
  // data into a buffer. The secondary cache may decide to not store it in a
  // contiguous buffer, in which case this callback will be called multiple
  // times with increasing offset
  using SaveToCallback = Status (*)(void* from_obj, size_t from_offset,
                                    size_t length, void* out);

  // A function pointer type for custom destruction of an entry's
  // value. The Cache is responsible for copying and reclaiming space
  // for the key, but values are managed by the caller.
  using DeleterFn = void (*)(const Slice& key, void* value);

  // A struct with pointers to helper functions for spilling items from the
  // cache into the secondary cache. May be extended in the future. An
  // instance of this struct is expected to outlive the cache.
  struct CacheItemHelper {
    SizeCallback size_cb;
    SaveToCallback saveto_cb;
    DeleterFn del_cb;

    CacheItemHelper() : size_cb(nullptr), saveto_cb(nullptr), del_cb(nullptr) {}
    CacheItemHelper(SizeCallback _size_cb, SaveToCallback _saveto_cb,
                    DeleterFn _del_cb)
        : size_cb(_size_cb), saveto_cb(_saveto_cb), del_cb(_del_cb) {}
  };

  // The CreateCallback is passed by the block cache user to Lookup(). It
  // takes in a buffer from the NVM cache and constructs an object using
  // it. The callback doesn't have ownership of the buffer and should
  // copy the contents into its own buffer.
  // typedef std::function<Status(void* buf, size_t size, void** out_obj,
  //                             size_t* charge)>
  //    CreateCallback;
  using CreateCallback = std::function<Status(void* buf, size_t size,
                                              void** out_obj, size_t* charge)>;
```

The secondary cache provider must provide a concrete implementation of the SecondaryCache abstract class.

```
// SecondaryCache
//
// Cache interface for caching blocks on a secondary tier (which can include
// non-volatile media, or alternate forms of caching such as compressed data)
class SecondaryCache {
 public:
  virtual ~SecondaryCache() {}

  virtual std::string Name() = 0;

  static const std::string Type() { return "SecondaryCache"; }

  // Insert the given value into this cache. The value is not written
  // directly. Rather, the SaveToCallback provided by helper_cb will be
  // used to extract the persistable data in value, which will be written
  // to this tier. The implementation may or may not write it to cache
  // depending on the admission control policy, even if the return status is
  // success.
  virtual Status Insert(const Slice& key, void* value,
                        const Cache::CacheItemHelper* helper) = 0;

  // Lookup the data for the given key in this cache. The create_cb
  // will be used to create the object. The handle returned may not be
  // ready yet, unless wait=true, in which case Lookup() will block until
  // the handle is ready
  virtual std::unique_ptr<SecondaryCacheHandle> Lookup(
      const Slice& key, const Cache::CreateCallback& create_cb, bool wait) = 0;

  // At the discretion of the implementation, erase the data associated
  // with key
  virtual void Erase(const Slice& key) = 0;

  // Wait for a collection of handles to become ready. This would be used
  // by MultiGet, for example, to read multitple data blocks in parallel
  virtual void WaitAll(std::vector<SecondaryCacheHandle*> handles) = 0;

  virtual std::string GetPrintableOptions() const = 0;
};
```

A SecondaryCache is configured by the user by providing a pointer to it in LRUCacheOptions -
```
struct LRUCacheOptions {
  ...
  // A SecondaryCache instance to use as an additional cache tier
  std::shared_ptr<SecondaryCache> secondary_cache;
  ...
};
```

## Current Status

The initial RocksDB support for the secondary cache has been merged into the main branch, and will be available in the 6.21 release. This includes providing a way for the user to configure a secondary cache when instantiating RocksDB’s LRU cache (volatile block cache), spilling blocks evicted from the LRU cache to the flash cache, promoting a block read from the SecondaryCache to the LRU cache, update tools such as cache_bench and db_bench to specify a flash cache. The relevant PRs are [#8271](https://github.com/meta/rocksdb/pull/8271), [#8191](https://github.com/meta/rocksdb/pull/8191), and [#8312](https://github.com/meta/rocksdb/pull/8312).

We prototyped an end-to-end solution, with the above PRs as well as a Cachelib based implementation of the SecondaryCache. We ran a mixgraph benchmark to simulate a realistic read/write workload. The results showed a 15% gain with the local flash cache over no local cache, and a ~25-30% reduction in network reads with a corresponding decrease in cache misses.

![Throughput](/static/images/rocksdb-secondary-cache/Mixgraph_throughput.png)
{: style="display: block; margin-left: auto; margin-right: auto; width: 80%"}

![Hit Rate](/static/images/rocksdb-secondary-cache/Mixgraph_hit_rate.png)
{: style="display: block; margin-left: auto; margin-right: auto; width: 80%"}

## Future Work

In the short term, we plan to do the following in order to fully integrate the SecondaryCache with RocksDB -

1. Use DB session ID as the cache key prefix to ensure uniqueness and repeatability
2. Optimize flash cache usage of MultiGet and iterator workloads
3. Stress testing
4. More benchmarking

Longer term, we plan to deploy this in production at Facebook.

## Call to Action

We are hoping for a community contribution of a secondary cache implementation, which would make this feature usable by the broader RocksDB userbase. If you are interested in contributing, please reach out to us in [this issue](https://github.com/meta/rocksdb/issues/8347).

