# Thoughts on the New RocksDB Java (FFM) API

## Resources

- [Adam's Presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future)
- [Adam's Presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-past-present-and-future#18)
- [JNI Spec](https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html)
- [Prototype Blog Post](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html)
- [Prototype PR](https://github.com/facebook/rocksdb/pull/11095)
- [FFM API](https://docs.oracle.com/en/java/javase/25/core/foreign-function-and-memory-api.html)
- [FFM JEP 454](https://openjdk.org/jeps/454)
- [New Prototype jextract PR](https://github.com/facebook/rocksdb/pull/15369)

## Ground Rules

- We don't want a big bang change. That is to say, we will keep supporting Java 8, and the associated JNI-based
API, for the foreseeable future. In parallel, we will introduce an FFM-based API. This API will initially support
only core functions, and will be considered experimental; it will be grown incrementally so that at a point,
we will be able to deprecate and remove the JNI API.
- It seems clear to me that this is what @adamretter describes as the *Parallel* approach , in his [Future Presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future).

## Observations

- FFM has performance advantages. It is generally (a little, sometimes a lot) more efficient at crossing the native boundary than JNI. This is in terms of cost per Native method invocation.

## Earlier Prototype

See [Prototype Blog Post](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html).

Implemented with the preview at Java 19, theory was to use the RocksDB `PinnableSlice`
concept to return a reference to a result buffer without adding copy operations,
when performing a `get()` operation.
This resulted in comparable performance to the existing JNI-based implementation, when the copy-out
was measured.

## Existing `C` API

The existing ``C` API is now partially auto-generated from configuration, by a script. Base methods are provided
as source for the script, and together with some configuration.
This seems to be an eminently pragmatic way of building the API.

## jextract

Having installed or built a recent version of `jextract`
```bash
$ cd $ROCKSDB_REPO_DIR
$ jextract --include-dir ./include/rocksdb --output ./java/src/main/java --target-package org.rocksdb.generated --library rocksdb ./include/rocksdb/c.h
```
we have modified the RocksDB `./Makefile` and `./java/Makefile` to run `jextract` on the (already-generated) `C`-API,
generating a Java API, adding the generated code to the current (JNI) library. There is a top-level `jffm_sample` goal
which does all the work to build and run this.
```
EXTRA_CXXFLAGS="-Wno-error -Wno-nonnull" make -j12 jffm_sample
```
## Critical APIs

Some [JDK 24 Performance Improvements](https://inside.java/2025/03/19/performance-improvements-in-jdk24/) were
introduced in JDK 24.

We can use [`Linker.Option.critical`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Linker.Option.html#critical(boolean)) to flag a MethodHandle as *critical*, and promise to the
JVM that the called method will obey extra rules which allow it to apply special optimizations. This option may also take a `boolean allowHeapAccess` which allows the use of on-heap `MemorySegment`s as parameters to the called methods; by wrapping a Java `byte[]` in a `MemorySegment` it can be passed to native without first being copied to native memory.

## Performance Critical Java API Methods

We would like to ensure that a set of core, or a set of usable, methods can be provided that exploit the benefits
of FFM as well as possible, allowing clients to extract maximum performance from an FFM-based Java API.

See for example `rocksdb_batched_multi_get_pinned_cf()` in `c_base.cc`

We would like to be able to fetch the results of this method efficiently into Java.
Using `Linker.Option.critical` here opens up some possibilities as 

## Wrapping the generated API

Even with the API *generated* by `jextract`, we have to write quite a lot of boilerplate to turn the notionally
Java method call
```
MemorySegment value =
        rocksdb_c_api_2.rocksdb_get(db, read_options, keySegment, keySize, vallenSegment, errptr);
```
into something that is still quite verbose. Our first attempt at calling RocksDB via FFM,
specifically the basic *get* method `rocksdb_c_api_2.rocksdb_get()` has us writing this:
```java
  /**
   * Fetch the value for a supplied key
   * @param key to look up
   * @return a {@link java.lang.foreign.MemorySegment} containing the retrieved value, or {@code null} if no value exists for the key
   * @throws RocksDBException if an error occurs
   */
  public MemorySegment get(String key) throws RocksDBException {
    MemorySegment read_options = rocksdb_c_api.rocksdb_readoptions_create();
    MemorySegment keySegment = dbArena.allocateFrom(key, UTF_8);
    long keySize = keySegment.byteSize() - 1; // 0-terminated
    MemorySegment vallenSegment = dbArena.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment errptr = dbArena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
        rocksdb_c_api_2.rocksdb_get(db, read_options, keySegment, keySize, vallenSegment, errptr);
    MemorySegment err = errptr.get(ValueLayout.ADDRESS, 0L);
    if (err.address() == NULL.address()) {
      // success
      if (value.address() == NULL.address()) {
        // Status is NOT_FOUND
        return null;
      }
      long vallen = vallenSegment.get(ValueLayout.JAVA_LONG, 0);
      return value.asSlice(0L, vallen);
    } else {
      err = err.reinterpret(Long.MAX_VALUE);
      String message = err.getString(0, UTF_8);
      throw new RocksDBException("rocksdb_get() " + message);
    }
  }
```
We need to simplify that, and to address the fact that _everything_ is currently a `MemorySegment`.

Analogously to the existing `RocksObject` class in Java, we propose a `RocksFFMObject` which
usually (almost always ?) will wrap a `MemorySegment`, just as `RocksObject` wraps a 
`long _nativeHandle`. It also seems natural to give this the same
lifecycle-management as `RocksObject`.

So then we would have:
```java
class ReadOptions extends RocksFFMObject {
  // methods wrapping the generated `read_options_t` API
}
```
and we call
```java
public MemorySegment get(ReadOptions readOptions, String key)
```

## MultiGet

See `rocksdb_batched_multi_get_pinned_cf()` in `c_base.cc`.
- We want to look at how to offer this method (really a set of methods) efficiently.
- This is a test case because `multiGet()` methods were created for the sake of efficiency.

## Write Batch

The cost of the JNI native transition was always a major problem for efficient use of the `WriteBatch` API.
The combination of critical `MethodHandle`s and heap-based `MemorySegment`s may offer enough of a performance
win to make the difference when accessing the `WriteBatch` API. The idea is that adding a Java heap-based
value to the write batch becomes almost as cheap as adding a `malloc()`-ed memory buffer in `C`; certainly
for values of more than 2 or 3 bytes in size. This is based on the assumption that buffers are copied exactly
once, into a native/C++ write batch buffer, in both the Java and the `malloc()` case.

## Arenas and efficient boilerplate code

We need to think about how `Arena`s work for efficient allocation of short-lived and small `MemorySegment`s,
such as error pointers.
We might want some sliced `Arena`s for that. We also need to consider the scope of `Arena`s.
- Sometimes a `MemorySegment` can naturally come from a confined `Arena`, because its usage is restricted to the
lifetime of a single call.
- Sometimes an `Arena` should be provided to the API to allocate from,
because the caller best knows the scope of 

## API Variants

The existing Java API provides a broad range of methods, with permutations of the API
for each of the underlying RocksDB operations:
* different encapsulations of data (`byte[]`, `ByteBuffer`)
* Column family specified or not
* Options specified or not
* Transactional API vs standard API vs ...

We believe that the variants will be requested by users. There are 2 ways to go about this.
1. Implement all the method variants in the same way, with every native method wrapped by a Java `MethodHandle`
which ultimately exposes a method to the user-level Java API.
2. Implement the broadest method variant (largest number of parameters) wrapping that Java `MethodHandle`,
and then wrap the Java method with variants in pure Java; then some `C` methods do not get exposed as `MethodHandle`s.

The first option is more consistent with an automated API generation process, which we further discuss and argue for
below.

## Automation / Generation

The `C` API is a generated API. The generation is driven from a combination of base code and configuration.
This seems like a very pragmatic way to do it. But, it probably means that not all the `C` API methods pass
through the generation machinery as abstract objects (i.e. some of the work is just copying opaque text).

With the use of `jextract` on top we get to the stage where we have all of the `C` API exposed as low-level
Java methods; the example above shows how much boilerplate we still need to add, in order to turn that into
_natural_ Java.

If we could hook into `jextract` where it generates code, we could potentially access all the methods we want
to expose, in particular those that are passed through by the `C` API generation. This would be the perfect
place to enhance the generated code with our own opinionated Java API boilerplate.

We should also still think about the `C` API code generation, and whether we can hook into or adapt that to generate
Java (FFM) at the same time. The information configured in the `C` API configuration, and the information parsed
by `jextract` are probably complementary.

We can also control what we want
to generate from `jextract`, in the sense that we don't need to expose *everything*.
This is slightly incidental, and more about hygiene, and not creating reams of unused stuff.

## Portability

`jextract` has to map the types declared in `C` APIs to Java types. These are then exposed at the generated
Java API. How is this made portable ? In [JEP 454, Describing C types in Java code](https://openjdk.org/jeps/454#:~:text=Describing%20C%20types%20in%20Java%20code) we see that this is done via `Linker.nativeLinker().canonicalLayouts()`, so for example a `long` in `C` is always represented as a `C_LONG` in Java, but that value
layout is calculated dynamically through the current linker:
```java
public static final ValueLayout.OfLong C_LONG = (ValueLayout.OfLong) Linker.nativeLinker().canonicalLayouts().get("long");
```
Earlier versions of `jextract` generated hard constants for these values, based on the platform on which it was
being run. These were definitively non-portable.

Current, platform independent (JDK24+) jextract-ed code gets to be platform independent by generating code which references at runtime `Linker.nativeLinker().canonicalLayouts()` when defining the sizes/types of C_LONG, C_POINTER etc which are used as the parameter types for the method handles for extracted methods.
The NativeLinker is implemented only for common 64bit platforms (aarch64, x64, not much else).
The NativeLinker is also necessary to actually link and call native methods.

32bit builds of the JDK do not have a native linker (e.g. Liberica 23). You can build a 32bit JDK with a fallback linker (which uses libffm, and which is a confusing name, as it's not "native", but it implements the interface). You do this by explicitly configuring the JDK build (I did this with my JDK22 build and it works). BUT if there is a supported-ish 32bit distribution with the fallback linker, I don't know of it.

Slightly incidentally to this argument, it is perfectly reasonable to generate an API without using jextract, we don't need all the things jextract does, the boilerplate is pretty straightforward, and I think it's a good approach to take because we could drive it from the C API generation, we can make our API higher level (RocksFFM objects), and it lets us be flexible with choosing which methods to make critical or allow heap segments.

Nonetheless, we still need a linker, so to support 32bit we still need a custom 32bit JDK which is stuck at JDK23 forever, because 32bit support is deprecated in 24 and removed in 25. So I very strongly recommend that a new RocksFFM API can be 64bit only. I think it is reasonable to tell the 32bit world that they are dinosaurs, and their only option is the existing JNI API, which may be supported for some time, but never taken forward.

### 32 bit is deprecated - some references
- [No 32-bit JDK maintainer](https://mail.openjdk.org/pipermail/jdk-dev/2024-July/009223.html)
- [JEP - Deprecate 32-bit](https://openjdk.org/jeps/501)
- [Canonical Layouts](https://bugs.openjdk.org/browse/JDK-8312523)
- [Require Linker API](https://github.com/openjdk/panama-foreign/pull/850)
- [State of `jextract`](https://cr.openjdk.org/~mcimadamore/panama/jextract_changes.html)
- [Guide - `jextract`](https://github.com/openjdk/jextract/blob/master/doc/GUIDE.md)
- [Heap Segments via `MemorySegment`](https://bugs.openjdk.org/browse/JDK-8318645)

## Outstanding Items to think about

1. A fluent prototype; how does it look/feel ? Both [Auto-Complete](https://poutsma-principles.com/blog/2025/05/15/designing-for-short-completion-menus/) and [consistency](https://poutsma-principles.com/blog/2025/06/11/making-api-feel-like-home/) are driving factors. [Fluent APIs](https://dzone.com/articles/java-fluent-api)
2. Could we build an API as an additional back end of the `C` API generator ? 
The boilerplate generated by jextract is easy to duplicate if we have the API in data structures,
as we do in the generator.
3. We have more need of jextract to handle the hand-crafted portions of the C API
4. We want control to allow us to mark methods as critical/heap-access
5. Performance is a missing ingredient in this. Do we even have the `io_uring` methods exposed in `C` and
therefore wrappable in Java ? Do we need to add some methods to the `C` API to help it ?
6. Performance of write. Can we do something efficient with `WriteBatch` ? Again, probably it requires some additions
to the `C` API; we would like these to be an official part of the `C` API, from a support/maintenance point-of-view.
7. Performance again; returning *pinned* buffers to Java as `MemorySegment`s as in the early FFM Preview prototype.
8. Do we need or want any code generation tools ? Both [Spoon](https://spoon.gforge.inria.fr/) and [Java Poet](https://www.baeldung.com/java-poet) are familiar to Evolved Binary, and may have a role to play.
9. Memory ownership and lifecycle. Sometimes we get memory returned from an API (as a `MemorySegment`) which is
owned by `malloc()` on the native side. Sometimes we create memory using FFM with `Arena.allocate()`. We should rely on the arena lifecycle to handle the disposal of FFM memory; perhaps we can create shorter-lived arenas for things like
result message pointers.
10. Disposal of `malloc()`-ed error messages. This is yet another thing to remember to do when checking the error
status of an operation, we need to wrap it into the helpers that do this. The small `Arena`-allocated segments
which receive the pointer to error message can just go in an `Arena.ofAuto()`. 
11. General disposal of large `malloc()`-ed results. One idea is to have a `Lifetime implements AutoClosable` object to which these results are added. Then we just need to implement a small stub in the `C` API to do the disposal of
a list of pointers supplied to it.

There's a blended approach which is:
1. We will use a subset of the `C` API for our purposes; probably fetched using `jextract` and restricted to the methods that we need, but maybe generated.
2. We will need to enhance the `C` API in some ways, for some core, performance-critical operations
3. We will build a lot of the boilerplate API in a highly-automated fashion. When we are processing options,

### How to assess new API
- Does it flow nicely ? UX of the programmer.
- How much boilerplate is needed ?
- How automated is it ? Does new stuff that appears in the `C` API just appear ?
- Does it deliver the performance that is on offer ?

### Development/Maintenance cost

We would like to keep the Java API in sync with the `C` API at zero effort. This demands automation.

## Plans

### Alternative Approaches
* How does `jpassport` compare to `jextract` as a basis for an API ?
* Is there a possibility of rolling our own ?
  * Post-processing via JavaParser or [Spoon](https://tomassetti.me/analyze-generate-and-transform-java-code/)

#### JPassport
* JPassport appears to have been very minimally tested on MacOS
  * Lots of crashes when I try to do what it says to do
  * But perhaps that is only that I have been using it in a non-typical way ?
    * Trying `PassportFactory.link()` now
  * `PassportFactory.link()` may be the canonical way to approach it.
    * Generates FFM method calls to implement a Java interface.
    * We would need to specify each interface / method that we want to use, manually.

### Platform Stability
* Is the RocksDB Java API stable across important platforms ?
  * `i386` vs `x86-64`
  * 32-bit vs 64-bit ARM
  * ARM vs Intel