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
## Critical APIs

We can use [`Linker.Option.critical`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Linker.Option.html#critical(boolean)) to flag a MethodHandle as *critical*, and promise to the
JVM that the called method will obey extra rules which allow it to apply special optimizations. This option may also take a `boolean allowHeapAccess` which allows the use of on-heap `MemorySegment`s as parameters to the called methods; by wrapping a Java `byte[]` in a `MemorySegment` it can be passed to native without first being copied to native memory.

## Adam's RocksDB Talk - My Input

I've just discussed the get() side of the data transfer benchmarks here, but the put() side shows similar results. Written up in https://github.com/evolvedbinary/jni-benchmarks/blob/ffm24proto/DataBenchmarks.md
Xeon and Mac M1 both have similar results, so I'm just showing you Xeon ones, but the M1 ones are there too
The benchmark simulates get()ting a value for a key, and returning it in the supplied value buffer. This used to be a byte[], ByteBuffer etc when we used JNI, now extended to be a MemorySegment using FFM. There are variants of native MemorySegments which are allocated as native memory using the allocation facilities (Arena) of the FFM, and heap-based MemorySegments which simply wrap byte[].
There are 2 variants of each benchmark, one of which does nothing (_none_) with the result, the other of which "copies it out" (_copyout_) into a byte[] - I wanted to check that some work wasn't being hidden/elided behind native segments. 
For small value sizes, it's clear that FFM is just more efficient at crossing the Java/Native boundary , so the lower graphs (less time is better) are all the MemorySegment benchmarks https://github.com/evolvedbinary/jni-benchmarks/blob/ffm24proto/analysisWithFFM/jmh_xeon_get_2026-09-22T09%3A18%3A38.145289/fig_1024_1_none_allsmall.png 
For large value sizes FFM is just as efficient as anything else at data copying (it all just boils down to efficient copying) https://github.com/evolvedbinary/jni-benchmarks/blob/ffm24proto/analysisWithFFM/jmh_xeon_get_2026-09-22T09%3A18%3A38.145289/fig_1024_1_none_allbig.png
You can, in the right circumstances, "save" a copy with wrapped MemorySegments, https://github.com/evolvedbinary/jni-benchmarks/blob/ffm24proto/analysisWithFFM/jmh_xeon_get_2026-09-22T09%3A18%3A38.145289/fig_1024_1_copyout_allbig.png but note that for wrapped segments you need to mark the API call as critical and risk the problems associated with a critical API
API overhead is no worse with non-critical versions of the API when using native arena-based MemorySegments 
There is a good case for ultimately having an API variant that uses, or can use, wrapped byte[] -based MemorySegments to save that copy. You could configure an API that is the same at the Java level, uses byte[], but either wraps the byte[] and calls a critical native method, or copies into / out of native MemorySegment s from a pool, and passes those to/from a non-critical API.
You would still want to expose a native MemorySegment API - you could imagine handing off MemorySegments to/from a streaming system without the contents ever touching Java heap memory.

My summary is that FFM vs JNI
Often has significantly better performance than JNI, e.g. for lots of small reads and writes
Has as good or better performance in the worst case
Is much easier and more flexible to framework
no stub methods to write
no headers to generate
just use (or write) whatever you want in native libraries, and extract what you care about. Using an existing C/native API becomes very simple.
OR write (or even generate) some fixed code to lookup symbols, link them and implement an API method that calls the linked MethodHandles.
There are potential ways to clean up the remaining boilerplate of calling the MethodHandles if you feel that needs to happen.

FFM lets us move forward with a Java/Native API at a faster pace.
FFM is the future of Java, and JNI will not be around forever.