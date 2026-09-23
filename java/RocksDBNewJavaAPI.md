# Thoughts on the New RocksDB Java (FFM) API

## Resources

- [Adam's Presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future)
- [JNI Spec](https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html)
- [Prototype Blog Post](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html)
- [Prototype PR](https://github.com/facebook/rocksdb/pull/11095)
- [FFM API](https://docs.oracle.com/en/java/javase/25/core/foreign-function-and-memory-api.html)
- [FFM JEP 454](https://openjdk.org/jeps/454)

## Ground Rules

- We don't want a big bang change. That is to say, we will keep supporting Java 8, and the associated JNI-based
API, for the foreseeable future. In parallel, we will introduce an FFM-based API. This API will initially support
only core functions, and will be considered experimental; it will be grown incrementally so that at a point,
we will be able to deprecate and remove the JNI API.
- It seems clear to me that this is what @adamretter describes as the *Parallel* approach , in his [Future Presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future).

## Observations

- FFM has potential performance advantages. It seems more efficient at crossing the native boundary than JNI. This is in terms of cost per Native method invocation.

## Prototype

See [Prototype Blog Post](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html).

Implemented with the preview at Java 19, theory was to use the RocksDB `PinnableSlice`
concept to return a reference to a result buffer without adding copy operations,
when performing a `get()` operation.
This resulted in comparable performance to the existing JNI-based implementation, when the copy-out
was measured.

## Random thoughts

- Is the existing/new `C` API wrappable ?
    - See `rocksdb_batched_multi_get_pinned_cf()` in `c_base.cc`
    - The API does not seem to be AI-generated, but is auto-generated from configuration, by a script. This seems to be an eminently sensible way of doing it.
- How small can the new API be ? That's to say, can we implement a catch-all `multiGetCF()` which can be as efficient for the single value `get()` as implementing a `Get()` method ?
- Does `jextract` have a place ? Perhaps it allows us to wrap a bigger API surface easily, as an alternative to the catch-all method solution ?
- Do we implement a `MemorySegment`-based API, and if so, do we support heap segments ? If heap segments are used we need to test for heap vs native in order to access critical vs non-critical methods. 
- What are the FFM performance improvements in Java 24 ? [JDK 24 Performance Improvements](https://inside.java/2025/03/19/performance-improvements-in-jdk24/)
- Think about `Arena`s for allocation, specifically sliced arenas etc, see the FFM documentation.
- Look at existing clients, and how they use RocksJava. Kafka, and what else ?

## jextract

Having installed or built a recent version of `jextract`

```bash
$ cd $ROCKSDB_REPO_DIR
$ jextract --include-dir ./include/rocksdb --output ./java/src/main/java --target-package org.rocksdb.generated --library rocksdb ./include/rocksdb/c.h
```