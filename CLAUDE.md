# RocksDB Code Generation and Review Guidance

This document provides guidance for generating and reviewing code in the RocksDB project, derived from analysis of code review feedback across hundreds of complex merged Pull Requests. Use this as a reference when writing code with AI assistants or conducting code reviews.

---

## General Best Practices

### Code Quality and Maintainability

**Clarity and Readability:** Write clear, self-documenting code. Use meaningful variable names, add comments for complex logic, and structure code to minimize cognitive load. Avoid clever tricks that sacrifice readability for marginal performance gains unless absolutely necessary.

**Consistent Style:** Follow existing code style conventions. RocksDB uses `.clang-format` for formatting, specific naming conventions, and structural patterns. Deviations from these patterns are frequently flagged in reviews.

**Error Handling:** Ensure robust error handling throughout the codebase. Use RocksDB's `Status` type consistently, propagate errors appropriately, and avoid silently ignoring failures. Reviewers pay close attention to edge cases and failure modes.

**Refactoring traps:** At least in production code, avoid constructs that could allow existing code to unexpectedly, quietly change meaning.
Specifically:
* Avoid new defaulted parameters. This is the #1 trap on refactoring! (Permitted exception: optional outputs that are generally safe to ignore.)
* Avoid static_cast, reinterpret_cast, and C-style casts; static_cast_with_check, up_cast, and lossless_cast from cast_util.h are preferred.
* Avoid declared type `auto` (`auto&`, `auto*` OK).

### Testing Philosophy

**Comprehensive Coverage:** Every change should include appropriate test coverage. This includes unit tests for isolated functionality, integration tests for component interactions, and stress tests for concurrency and performance validation. Reviewers will ask for additional tests if coverage is insufficient.

**Edge Cases and Failure Modes:** Tests should explicitly cover edge cases, boundary conditions, and potential failure scenarios. This is especially important for changes affecting core database operations, compaction, or recovery logic. For non-OK Statuses, prefer comparing code() rather than using ASSERT_TRUE
which is bad for debugging.

**Platform-Specific Testing:** RocksDB supports multiple platforms (Linux, Windows, macOS) and compilers (GCC, Clang, MSVC). Changes should be tested across relevant platforms, particularly when touching platform-specific code or using compiler-specific features.

### Performance Considerations

**⚠️ PERFORMANCE IS CRITICAL:** RocksDB is a high-performance storage engine where every CPU cycle and memory access matters. When writing code, always evaluate from a performance perspective. This is not optional—performance-aware coding is a fundamental requirement for all contributions.

**Benchmarking and Profiling:** Performance claims should be backed by empirical evidence. Use RocksDB's benchmarking tools (e.g., `db_bench`) to validate improvements. Reviewers will request benchmark results for changes that could impact performance.

**Memory Allocation:** Minimize dynamic memory allocations, especially in hot paths. Prefer stack allocation over heap allocation. Reuse buffers when possible. Consider using arena allocators or memory pools for frequent small allocations. Every `new`, `malloc`, or container resize has a cost.

**Memory Copy:** Avoid unnecessary memory copies. Use move semantics, `std::string_view`, `Slice`, and pass-by-reference where appropriate. Be aware of implicit copies in STL containers and function returns. Prefer in-place operations over copy-and-modify patterns.

**CPU Cache Efficiency:** Design data structures and access patterns to be cache-friendly. Keep frequently accessed data together (data locality). Prefer sequential memory access over random access. Be mindful of cache line sizes (CACHE_LINE_SIZE, typically 64 bytes) and avoid false sharing in concurrent code (see CacheAlignedWrapper). Especially when adding new data members, be mindful of ordering within structs and classes to reduce unnecessary padding (special care with bool members) and avoid larger-than-necessary types (e.g. prefer OptSlice to std::optional<Slice>).

**Loop Optimization:** Look for opportunities to collapse nested loops, reduce loop overhead, and minimize branch mispredictions. Hoist invariant computations out of loops. Consider loop unrolling for tight inner loops. Batch operations when possible to amortize per-operation overhead.

**SIMD and Vectorization:** Leverage SIMD instructions (SSE, AVX) for data-parallel operations when appropriate. Structure data to enable auto-vectorization by the compiler. Consider explicit SIMD intrinsics for critical hot paths like checksum computation, encoding/decoding, and bulk data processing.

**Branch Prediction:** Minimize unpredictable branches in hot paths. Use `LIKELY`/`UNLIKELY` macros to hint branch prediction, e.g. for error cases and other rare or otherwise costly cases, but NOT for predicting popular configurations. Consider branchless alternatives for simple conditionals. Order switch cases and if-else chains by frequency.

**Memory and Resource Management:** Be mindful of memory allocations, especially in hot paths. Use RAII patterns, smart pointers, and RocksDB's memory management utilities appropriately.

**Hot Path Analysis:** When deciding how aggressively to optimize code, consider whether it's on a hot path:
- **Hot path** (executed thousands+ times, e.g., data access, iteration, compaction loops): Performance is paramount. Apply all optimization techniques—loop collapsing, SIMD, cache optimization, pre-allocation, etc. The cost of each operation is multiplied by execution frequency.
- **Cold path** (executed rarely, e.g., DB open, configuration parsing, error handling): Maintainability and clarity are more important. Prefer readable code over micro-optimizations. Complex optimizations here add maintenance burden with negligible performance benefit.
- **Warm path** (moderate frequency): Balance both concerns. Use profiling data to guide optimization decisions.

**Avoid Premature Optimization:** Performance is known critical in places like inner loops of compaction, read path and write path. Elsewhere, try to find a "holy trinity" solution of *correct, simple, and reasonably efficient*. More complex solutions are only appropriate when profiling/benchmarking data shows a measurable payoff.

### API Design and Compatibility

**Backwards Compatibility:** RocksDB maintains strong backwards compatibility guarantees. Breaking changes are rare and require extensive justification. When deprecating features, follow the project's deprecation policy (typically spanning multiple releases).

**Behavioral Compatibility:** Behavior changes are human judgment calls that must be intentional, well-documented (including release notes), and well researched for potential downstream production impact. Agents and reviewers must call out potential gaps, ambiguity, or adverse impacts in intentionality, documentation, and production impact. For an existing accepted input—including an undocumented, discouraged, or historically ignored value—the analysis should compare old and new observable behavior, including performance and resource impact, identify known direct callers and stored configurations, and add or update regression coverage as appropriate.

**API Consistency:** New APIs should be consistent with existing patterns. Use similar naming conventions, parameter ordering, and return types. Reviewers will suggest changes to improve consistency with the broader codebase.

**Documentation:** Public APIs must be thoroughly documented, without unnecessary embelishment nor dwelling on implementation details nor project planning. When non-obvious, include usage examples, parameter descriptions, known bugs or limitations, notes on thread safety, performance characteristics, and compatibility considerations. Re-read comments for ambiguous terminology and phrasing, such as ambiguously re-purposed programming jargon.

---

## Component-Specific Guidance

### Database Core (`db`)

The database core handles write-ahead logging (WAL), memtables, compaction, and recovery. This component receives the most scrutiny in code reviews.

**Concurrency and Thread Safety:** Database operations are highly concurrent. Reviewers carefully examine locking strategies, atomic operations, and memory ordering. Document synchronization assumptions clearly. Use appropriate memory ordering semantics (`acquire`/`release` vs. `seq_cst`).

**Compaction Logic:** Changes to compaction are complex and high-risk. Ensure that compaction logic respects configured parameters, handles edge cases (empty databases, single-file compactions), and maintains correctness under concurrent operations.

**Error Propagation:** Database operations can fail in many ways (I/O errors, corruption, resource exhaustion). Ensure that errors are properly propagated, logged, and handled. Avoid assertions in production code paths.

**Testing:** Database core changes require extensive testing, including unit tests, integration tests, and stress tests. Test with various configurations, compaction styles, and concurrent workloads.

### Public Headers / "Public API" (`include/rocksdb/`)

Public headers define RocksDB's API surface. Changes here have the highest compatibility impact.

**API Design:** New APIs should be intuitive, consistent with existing patterns, and well-documented. Consider how the API will be used in practice and avoid adding unnecessary complexity.

**Backwards Compatibility:** Breaking changes to public APIs require extensive justification and a deprecation plan. Maintain ABI compatibility for bug fixes and patch releases.

**Documentation:** Every public API must be thoroughly documented with usage examples, parameter descriptions, and notes on thread safety and performance characteristics.

**Deprecation:** When deprecating APIs, follow the project's policy. Mark deprecated APIs clearly, provide migration guidance, and maintain support for at least one major release.

### Internal Utilities (`util`)

Internal utilities provide common functionality used throughout the codebase.

**Code Reuse:** Utilities should be general-purpose and reusable. Avoid duplicating functionality that already exists elsewhere in the codebase.

**Error Handling:** Utility functions should handle errors robustly and propagate them appropriately. Consider edge cases like overflow, underflow, and invalid inputs.

**Testing:** Utility functions should have comprehensive test coverage, including edge cases and failure modes. Consider adding death tests for assertions.

**Performance:** Utilities are often used in hot paths. Ensure that implementations are efficient and avoid unnecessary allocations or copies.

### Table Management (`table`)

Table management handles SST file format, block-based tables, and table readers/writers.

**Block Format and Checksums:** Changes to block format require extreme care. Ensure that checksums are computed and verified correctly. Test with various compression algorithms and block sizes.

**Iterator Correctness:** Table iterators are used throughout the codebase. Ensure that iterator semantics (Seek, Next, Prev) are correct, especially at boundaries and with deletions.

**Caching and Prefetching:** Table readers interact with the block cache and prefetching logic. Ensure that cache keys are unique and that prefetching respects configured limits.

**Performance:** Table operations are performance-critical. Benchmark changes that could impact read or write performance.

### Utilities (`utilities`)

Utilities include optional features like transactions, backup engine, and checkpoint.

**Feature Isolation:** Utilities should be self-contained and not introduce unnecessary dependencies on core database internals.

**Deprecation and Cleanup:** Legacy features are being phased out. When removing deprecated code, ensure that migration paths are documented and that users have sufficient warning.

**Cross-Platform Compatibility:** Utilities often interact with OS-specific APIs. Ensure that code works on all supported platforms.

### Options and Configuration (`options`)

Options define RocksDB's configuration system.

**Type Safety:** Use appropriate types for options (e.g., `uint32_t` for flags, scoped enums for enumerated values).

**Deprecation Policy:** When deprecating options, follow the project's policy. Document the deprecation, provide migration guidance, and maintain support for at least one major release.

**Dynamic Configuration:** Some options can be changed dynamically. Ensure that dynamic changes are thread-safe and take effect correctly.

**Validation:** Validate option values and provide clear error messages for invalid configurations.

### Cache (`cache`)

Cache management is critical for RocksDB's performance.

**Concurrency:** Cache operations are highly concurrent. Ensure that implementations are thread-safe and use appropriate synchronization primitives.

**Performance:** Cache operations are in the hot path. Optimize for low latency and high throughput. Benchmark changes carefully.

**Memory Management:** Cache implementations must manage memory carefully to avoid leaks and excessive allocations.

**Eviction Policies:** Changes to eviction policies should be well-tested and benchmarked to ensure they improve overall performance.

---

## Code Review Checklist

When reviewing RocksDB code (or preparing code for review), use this checklist:

### Contract Boundaries
- [ ] Is each behavior owned by the right layer? High-level policy (for example,
  "compaction wants this I/O mode") should live at the caller/policy layer, while
  lower layers should expose generic mechanisms (for example, "open a fresh
  reader", "skip shared cache insertion", or "use these FileOptions").
- [ ] Do comments and names describe local contracts rather than leaking a
  specific caller's rationale into reusable APIs? Generic code should not need to
  know about one current use case unless the API itself is intentionally
  use-case-specific.
- [ ] Does each flag or parameter control one coherent behavior? If one boolean
  starts implying ownership, cache policy, I/O mode, prefetching, and caller
  identity, split it into explicit flags or an options struct.
- [ ] Could a future caller use this lower-level API without accidentally
  inheriting assumptions from compaction, backup, user reads, or a particular
  table format? If not, tighten the contract with assertions, clearer names, or
  a narrower API.
- [ ] Are implementation details not being used as policy signals? Prefer an
  explicit contract over inferring behavior from incidental fields such as file
  options, cache handles, or current table-reader state.

### Correctness
- [ ] Does the change preserve database semantics (e.g., snapshot isolation, key ordering)?
- [ ] Are all error cases handled appropriately?
- [ ] Is the change thread-safe? Are synchronization primitives used correctly?
- [ ] Are there any potential data races or deadlocks?

### Testing
- [ ] Does the change include appropriate test coverage?
- [ ] Are edge cases and failure modes tested?
- [ ] Have the tests been run on all supported platforms?
- [ ] Are stress tests passing?

### Performance
- [ ] Are there benchmark results for performance-sensitive changes?
- [ ] Does the change avoid unnecessary allocations or copies?
- [ ] Are hot paths optimized appropriately?

### API and Compatibility
- [ ] Is the change backwards compatible?
- [ ] Are new APIs consistent with existing patterns?
- [ ] Is the public API documented?
- [ ] Are deprecated features handled according to policy?

### Code Quality
- [ ] Does the code follow RocksDB's style conventions?
- [ ] Is the code clear and maintainable?
- [ ] Are comments and documentation sufficient?
- [ ] Are there any code smells or anti-patterns?

---

## Common Review Feedback Patterns

The following patterns emerged as frequent sources of review feedback:

1. **Test Coverage:** Reviewers frequently request additional tests for edge cases, platform-specific behavior, and failure modes. Complex changes require comprehensive test coverage including unit tests, integration tests, and stress tests.

2. **Error Handling:** Ensure proper error propagation using RocksDB's `Status` type. Avoid silent failures and provide clear error messages that include context about what failed and why.

3. **API Design:** New APIs should be consistent with existing patterns. Use descriptive names that follow established conventions. Avoid breaking changes without strong justification and a clear deprecation plan.

4. **Documentation:** Public APIs must be documented with usage examples and notes on thread safety, performance characteristics, and compatibility considerations. Complex internal logic should also be well-commented.

5. **Performance:** Performance-sensitive changes require benchmark results to validate improvements. Use `db_bench` and other profiling tools to measure impact. Avoid premature optimization that adds complexity without measurable benefit.

6. **Concurrency:** Thread safety is critical in RocksDB. Document synchronization assumptions clearly. Use appropriate memory ordering semantics. Consider potential race conditions and deadlocks.

7. **Code Style:** Follow existing conventions for naming, formatting, and structure. Use `.clang-format` for consistent formatting. Prefer scoped enums (`enum class`) over unscoped enums.

8. **Backwards Compatibility:** RocksDB maintains strong compatibility guarantees. Breaking changes require extensive justification. When deprecating features, provide migration guidance and maintain support across multiple releases.

9. **Refactoring:** Reviewers appreciate refactoring that improves code readability and maintainability. Look for opportunities to deduplicate code and simplify complex logic.

10. **Platform Compatibility:** Ensure changes work correctly on all supported platforms (Linux, Windows, macOS) and with all supported compilers (GCC, Clang, MSVC).

11. **Contract Boundary Leaks:** When a change plumbs a new option or use-case
specific behavior through multiple subsystems, review the call chain for
contract leaks. Caller-specific rationale belongs at the call site or public API
documentation; reusable layers should expose precise, layer-local capabilities.
Watch especially for comments mentioning one caller in generic code, booleans
that silently bundle several behaviors, and downstream code inferring policy
from an implementation detail instead of an explicit option.

---

## Important tips

### Pull request and diff summaries
* Before writing or updating an external RocksDB pull request summary or its
  corresponding internal Phabricator diff summary, read and follow
  `claude_md/pull_request_summary_guideline.md`.

### Build system
* There are 3 build system. Make for git clones, BUCK (meta internal) for hg
  clones, and CMake for some special cases.
* When a new .cc file is added, update Makefile, CMakeLists.txt, src.mk, BUCK.
* Don't manually edit BUCK file, after updating src.mk, run
    /usr/local/bin/python3 buckifier/buckify_rocksdb.py to update it
* For -j in make command, use the number of CPU cores to decide it.
* When searching for references to something (a symbol, library, etc.), do not
  restrict or truncate your search based on presumed relevance or scope. It is
  important and time-saving to keep the repo reasonably consistent across
  different build systems, programming languages, and even between
  documentation and implementation.

### Avoiding mixed build modes with Make (use `AUTO_CLEAN=1`)

Object files are written to the same paths regardless of build flags, so
reusing objects from a prior build with different flags causes confusing
linker errors, etc. This problem is essentially avoidable by ALWAYS using
`AUTO_CLEAN=1 make -j<n> <something>` for manual make invocations. This
will automatically clean object files if the build parameters/flavor have
changed. The `build_tools/rockstest.sh` / `rocksptest.sh` helpers described
below set `AUTO_CLEAN=1` for you.

`AUTO_CLEAN=1` does not fix Make failures associated with stale .d files
referring to removed files. Resolve that manually or with `make clean`
without complaining to the user.

### Source checks
* Run `make check-sources` before committing. This catches non-ASCII
    characters in source files and other source-level issues that CI will
    reject. In particular, **do not use Unicode characters** (em dashes,
    smart quotes, etc.) in comments or strings -- use ASCII equivalents
    (`--` instead of em dash, `'` instead of smart quote, etc.).

### License headers
* Every new source file needs a license header. For a file that does **not**
    carry an outside/third-party copyright, use the standard Meta dual-licensed
    header (the dual-license designation is required -- a bare
    "All Rights Reserved" copyright is not an acceptable open-source header):
    ```
    //  Copyright (c) Meta Platforms, Inc. and affiliates.
    //  This source code is licensed under both the GPLv2 (found in the
    //  COPYING file in the root directory) and Apache 2.0 License
    //  (found in the LICENSE.Apache file in the root directory).
    ```
    Use a `#` comment prefix instead of `//` for shell, Python, and Makefile
    fragments.
* Files derived from an external source (e.g. LevelDB) keep their original
    upstream copyright line in addition to the header above.

### RTTI and dynamic_cast
* Production code and `db_stress` must build in **release mode
    (`-fno-rtti`)**. Do not use `dynamic_cast` anywhere except unit tests.
    Use `static_cast_with_check` from `util/cast_util.h` (validates with
    `dynamic_cast` in debug builds, plain `static_cast` in release).
* Unit tests (`*_test.cc`) are built in debug mode with RTTI enabled.

### Cross-platform / portability
Local `make` only exercises Linux with GCC/Clang, but CI
(`.github/workflows/pr-jobs.yml` and `nightly.yml`) gates on a much wider
matrix, so portability breaks are invisible locally until CI fails. Code must
build (and where noted, run tests) across:

| Axis | Must support |
|------|--------------|
| OS | Linux (x86_64 + ARM), macOS, Windows |
| Architecture | Little-endian and big-endian targets, including Linux on s390x |
| Compiler | GCC, Clang (libstdc++ **and** libc++), AppleClang, **MSVC (VS2022)**, MinGW (Linux cross-compile, build-only, no gflags) |
| Build system | Make, CMake, and BUCK (internal) -- keep all in sync (see "Build system" above) |
| Config | release (`-fno-rtti`), `ASSERT_STATUS_CHECKED`, ASAN/UBSAN/TSAN, folly, unity build, JNI/Java |

Treat these as constraints to satisfy and infer the specifics from them before
adding any system header, libc call, or compiler-specific construct. The most
common trap: anything that compiles under GCC/Clang on Linux but not under
**MSVC/MinGW** -- e.g. unguarded POSIX-only headers/functions (`<unistd.h>`,
`<sys/*.h>`, `getpid`, `_exit`, ...) or GCC/Clang extensions
(`__attribute__`, `__builtin_*`, VLAs, `alloca`). Prefer the `port::`/`Env`
abstractions; otherwise guard with `#ifdef OS_WIN` (POSIX `<unistd.h>` ->
Windows `<process.h>`). Because libc++ is also tested, include what you use
rather than relying on libstdc++ transitive includes.

Do not use `static_assert` to reject a supported architecture for an optional
feature. Check an existing `port::` capability such as `port::kLittleEndian`
and disable only the unsupported feature so the rest of RocksDB still builds.
Use a portable representation when the feature itself must work across
architectures.

### Unit Test
* After all of the unit tests are added, review them and try to extract common
    reusable utility functions to reduce code duplication due to copy past between
    unit tests. This should be done every time unit test is updated.
* Don't use sleep to wait for certain events to happen. This will cause test to
    be flaky. Instead, use sync point to synchronize thread progress.
* Cap unit test execution with 60 seconds timeout.
* To build and run unit tests locally, prefer these helper scripts:
    * `build_tools/rocksptest.sh <test_binary> [more_binaries...] [args...]`
        builds the binary(ies) with parallel make and `AUTO_CLEAN=1` and runs
        them under gtest-parallel, sharding the test cases across CPUs. Prefer
        this whenever running more than a couple of test cases, e.g.
        `build_tools/rocksptest.sh table_test` or
        `build_tools/rocksptest.sh db_test env_test --gtest_filter=*Foo*`.
    * `build_tools/rockstest.sh <test_binary> [args...]` builds with parallel
        make and `AUTO_CLEAN=1` and runs the binary directly (serially).
        Use it only for a very small number of test cases, e.g.
        `build_tools/rockstest.sh db_test --gtest_filter=*MixedSlowdown*`.
* After writing a test, stress-test for flakiness (AUTO_CLEAN handles the
    rebuild needed by the `COERCE_CONTEXT_SWITCH=1` flag change):
    ```bash
    COERCE_CONTEXT_SWITCH=1 build_tools/rockstest.sh {test_binary} -r100 \
        --gtest_filter="*YourTestName*"
    ```
* For CI-style flaky tests that do not reproduce with `gtest_parallel.py`,
    `--gtest_repeat`, or normal coerce-mode runs, inspect
    `tools/gtest_parallel_repro.py --help`.
* Each unit test file has overheads, so avoid creating new unit test files
  for random minor features. Consider adding to slice_test, db_etc3_test, or
  others.

### Unit test dedup guidelines
* Extract helper functions for repeated patterns such as object
    construction, round-trip (encode → decode → verify), and common
    assertion sequences.
* Use table-driven tests (struct array + loop) when multiple test cases
    share the same logic but differ only in input/expected data.
* Prefer randomized tests over exhaustive parameter permutations. Use
    `Random` from `util/random.h` (not `std::mt19937`). Use a time-based
    seed with `SCOPED_TRACE("seed=" + std::to_string(seed))` so failures
    are reproducible.
* Keep deterministic edge-case tests separate from randomized tests
    (error paths, boundary conditions, format verification).
* Methods only used in tests should be private with `friend class` +
    `TEST_F` fixture wrappers. In wrappers, always fully qualify the
    target method to avoid infinite recursion.

### Adding new public API
    Refer to claude_md/add_public_api.md

### Changing existing public API behavior
    Follow the Behavioral Compatibility requirements above.

### Adding new option
    Refer to claude_md/add_option.md

### Removing deprecated option
    Refer to claude_md/remove_option.md

### Metrics
* When adding a new feature, evaluate whether there is opportunity to add
    metrics. Try to avoid causing performance regression on hot path when adding
    metrics.

### Stress test
* When adding a new feature, make sure stress test covers the new option.

### Component docs
* For component-level design notes and implementation walkthroughs, start with
    `docs/components/index.md`.
* Documentation under `docs/components/` is organized by subsystem in
    `docs/components/<area>/`.
* Each subsystem directory should have an `index.md` entry point plus focused
    chapter files for deeper topics.

### DB bench update
* When adding a performance related feature, support it in db_bench

### Adding release note
* Release note should be kept short at high level for external user consumption.
    Release notes identify what users might care about most in a release. They
    are not exhaustive and are not a guide. PLEASE learn from past agents who
    tried to build elaborate release notes with implementation details and
    elsewhere-documented nuance. That wastes time. Fight the bias that
    "my change" is important so must be worthy of release note mention.
* If more than single markdown line, consider how their formatting will be
    integrated into HISTORY.md.

### Blog posts (docs/_posts)
* Blog post authors must be defined in `docs/_data/authors.yml` to be displayed

### Final verification of the change
* Execute `AUTO_CLEAN=1 make check` to build all of the changes and execute all
    of the tests. `AUTO_CLEAN=1` ensures a clean rebuild if your previous build
    used different parameters. Note that executing all of the tests could take
    multiple minutes.
* Run `AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make check` to verify all Status
    objects are properly checked. This catches missing error handling that can
    lead to silent data corruption. Merely building with ASSERT_STATUS_CHECKED=1
    accomplishes nothing; it enables a runtime check. New unit tests missing
    Status checks is a common failure point, even from agents. Avoid new uses
    of PermitUncheckedError(). That's the lazy solution.

### Monitoring make check progress
* Use `make check-progress` to get machine-parseable JSON progress while
    `make check` is running. This is useful for Claude Code to monitor long
    builds without timeout issues.
* Run `make check` in background, then poll progress:
    ```bash
    AUTO_CLEAN=1 make check &
    # Poll periodically:
    make check-progress
    ```
* The output shows current phase and progress:
    ```json
    {"status":"running","phase":"compiling","completed":300,"total":919,...}
    {"status":"running","phase":"testing","completed":1500,"total":29962,"failed":0,"percent":5,...}
    {"status":"completed","phase":"testing","completed":29962,"total":29962,"failed":0,"percent":100,...}
    ```
* Phases: `compiling` -> `linking` -> `generating` -> `testing` -> `completed`
* Key fields: `status`, `phase`, `completed`, `total`, `failed`, `percent`
* When tests fail, `failed_tests` array shows details (up to 10 failures):
    ```json
    {"status":"running",...,"failed":3,"failed_tests":[
      {"test":"cache_test-CacheTest.Usage","exit_code":1,"signal":0,"output":"...test log..."},
      {"test":"env_test-EnvTest.Open","exit_code":0,"signal":11,"output":"...Segmentation fault..."}
    ]}
    ```
* `exit_code`: non-zero means test assertion failed
* `signal`: non-zero means test was killed (e.g., 9=SIGKILL, 6=SIGABRT, 11=SIGSEGV)
* `output`: last 50 lines of test log including error messages and stack traces

### Executing benchmark using db_bench
* Since the goal is to measure performance, we need to build a release binary
    using `AUTO_CLEAN=1 DEBUG_LEVEL=0 make db_bench`. If there is an engine
    crash due to a bug, switch back to a debug build with
    `AUTO_CLEAN=1 make dbg`; `AUTO_CLEAN=1` handles the release<->debug rebuild
    automatically.

### Formatting code
* After making change, use `make format-auto` to auto-apply formatting without
    interactive prompts (Claude Code friendly).
