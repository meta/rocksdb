# RocksDB C API Code Generation Prototype

This directory contains a **prototype** for semi-automating parts of
RocksDB's C API based on the existing C++ public API and the current C API
conventions in `include/rocksdb/c.h` / `db/c.cc`.

The intent is **not** to automatically wrap arbitrary C++ APIs. Instead, the
goal is to reduce maintenance burden for the parts of the C API that are
already highly mechanical today.

## Why a semi-automated approach

RocksDB's C API follows stable conventions that downstream users rely on:

- opaque handle types like `rocksdb_t*`, `rocksdb_transaction_t*`
- `Status` translated to `char** errptr`
- `Slice` translated to `const char*` + `size_t`
- binary/string results copied into `malloc()`-owned buffers
- booleans represented as `unsigned char`
- `rocksdb_<object>_<verb>[_cf][_with_ts]` naming

These conventions are valuable and should remain the source of truth for the
generated code.

At the same time, the C++ public API includes features that are poor fits for
fully automatic wrapper generation:

- callback-backed polymorphic types (`Comparator`, `MergeOperator`, etc.)
- complex object construction/open flows
- ownership-transferring factory setters
- vector-heavy marshalling APIs like the `MultiGet` family
- virtual extension points and custom bridging logic

For those cases, hand-designed wrappers remain the right tool.

## Why the spec-driven generator still exists

The spec-driven generator is still needed for wrappers whose C shape cannot be
reliably inferred from the C++ declaration alone.

Yes, this means `spec.json` is manually maintained. That is intentional. The
goal is not to eliminate human decisions; the goal is to stop hand-writing the
same declaration/definition boilerplate once those decisions have been made.

For many wrappers, someone still has to decide:

- the C function name and parameter order
- whether a C++ `Status` becomes `char** errptr`
- whether default C++ objects should be synthesized, such as `ReadOptions()`
- how ownership and lifetime should work for pointers, factories, and callbacks
- how vectors, maps, and option collections should be flattened into C
- whether the C API should intentionally differ from the C++ API shape

Those are API design choices, not mechanical translations. The spec-driven
path keeps those choices explicit while still generating consistent boilerplate.

Backward compatibility with the existing hand-written C API is one input, but
it is not the primary reason this layer exists. Even for brand-new wrappers, we
still need an explicit policy layer for non-trivial C bindings.

## What is generated

Three generators share the work. Each owns the declaration in `c.h` and the
definition in `c.cc` of every function it generates.

1. **Public struct fields** (`auto_simple_bindings.py`): setters and getters for
   the scalar, enum, string and chrono fields of the managed option and
   metadata structs, discovered from the C++ headers. A new field gets its
   bindings at the next regeneration.
   - `rocksdb_options_set_create_if_missing`, `rocksdb_readoptions_set_fill_cache`
   - `rocksdb_flushjobinfo_cf_name`, `rocksdb_flushjobinfo_largest_seqno`

2. **Handle methods and lifecycle** (`handle_bindings.py` + `handles.json`):
   C functions that forward to one C++ member function of the object a handle
   wraps, plus trivial `create` (`return new H;`) and `destroy` (`delete h;`)
   functions and the definition of each handle struct that holds only `rep`.
   The C signature comes from the C++ declaration; `handles.json` records only
   what the declaration cannot decide (which overload, bound arguments,
   borrowed or copied string results, ABI pins).
   - `rocksdb_writebatch_put`, `rocksdb_iter_seek`, `rocksdb_iter_key`
   - `rocksdb_put_cf`, `rocksdb_transaction_commit`, `rocksdb_flush_wal`

3. **Explicit spec** (`generate_c_api.py` + `spec.json`): wrappers whose call
   expression is written out by hand, such as `rocksdb_destroy_db` and the
   nested `compression_opts` accessors.

Everything else in `c_base.h` / `c_base.cc` is hand-written: functions with
more than one statement of logic, NULL-tolerant arguments, ownership transfer,
callbacks, vectors, or a C shape that intentionally differs from the C++ one.

## What should remain manual

These families should stay hand-written unless a future design adds a much
richer generator:

- comparators / merge operators / compaction filters / event listeners
- compaction service wrappers
- openers and constructors that build vectors/maps of descriptors/options
- `MultiGet` / batched `MultiGet` families
- wrappers involving `std::shared_ptr` / `std::unique_ptr` ownership transfer
- C APIs whose behavior intentionally differs from the C++ API shape

## Design principles

### 1. Keep the current C API conventions

The code generator should not invent a new style. It should emit code that
looks like today's hand-written wrappers:

- `SaveError(errptr, ...)`
- `Slice(key, keylen)`
- direct field assignments for plain option setters
- same function names and signatures

### 2. Let auto-discovery handle the simple direct cases

For a set of mechanically mappable public structs, the generator discovers
the public fields directly from the C++ headers and owns their C bindings, both
the declaration in `c.h` and the definition in `c.cc`. A hand-written binding
for such a field in `c_base.h` / `c_base.cc` fails regeneration, so the
generated and hand-written layers never overlap. When an already-shipped
binding has a C type that differs from the natural mapping, pin it in
`abi_type_overrides.json` instead of hand-writing it.

Today this strict auto-discovery path is used for:

- simple `ReadOptions` scalar / enum fields
- simple scalar / enum fields across selected public option structs
- simple metadata fields on listener/job-info structs
- simple metadata view structs such as table-properties and compaction metadata

For those managed families, unsupported or intentionally deferred fields must
be explicitly recorded in `auto_simple_bindings_blocklist.json` rather than
being skipped silently. Anything outside those families remains hand-written.

### 3. Derive method wrappers from the C++ declaration

A C function that forwards to one C++ member function is listed in
`handles.json` under the handle it takes first. `handle_bindings.py` looks the
method up with clang (following C++ name lookup, so a derived declaration hides
base overloads unless re-exported with `using`) and maps each parameter and the
result with the conventions documented at the top of that file. Generated
calls pass every argument by reference or as a `Slice`; string results are
returned borrowed (`result: borrow` / `slice`, no copy) unless the shipped
signature hands the caller a `malloc()`ed buffer (`result: copy`).

In practice:

- auto-discovery owns simple direct field coverage for selected struct families
- `handles.json` owns single-call method forwards and trivial lifecycle
  functions
- `spec.json` owns other wrappers whose call expression is spelled out
- functions with logic of their own remain hand-written

### 4. Let C++ headers guide validation, not policy

Longer term, Clang/libTooling can help validate that a wrapper spec still
matches the underlying C++ method signature. But the C naming, ownership, and
error-handling policy should remain explicit in the spec.

## Files

Source templates (hand-written; NOT user-includable and NOT compiled directly):

- `c_base.h`
  - hand-written template for `include/rocksdb/c.h`; the generated `.h.inc`
    fragments are inlined into it to produce the self-contained public header
- `c_base.cc`
  - hand-written template for `db/c.cc`; the generated `.cc.inc` fragments are
    inlined into it to produce the compiled implementation

Generators and inputs:

- `spec.json`
  - declarative input for the spec-driven generator
- `generate_c_api.py`
  - emits header/source fragments from the spec
- `auto_simple_bindings.py`
  - auto-discovers simple public field bindings and generates checked-in
    `.inc` files for them
- `auto_simple_bindings_blocklist.json`
  - checked-in exceptions for auto-managed fields that are intentionally manual
    or temporarily deferred
- `abi_type_overrides.json`
  - per-function C type pins so already-shipped wrappers keep their historical
    C signature even when the C++ field type maps to a different C type
- `handle_bindings.py`
  - generates handle struct definitions, lifecycle functions and method
    forwards from `handles.json`
- `handles.json`
  - one entry per C handle type: the C++ type of `rep`, its create/destroy
    functions and its method bindings
- `regen_all.py`
  - orchestrates the generators and inlines the fragments into `c.h` / `c.cc`

Verification (run by `make check-c-api-gen` and in CI):

- `check_api_completeness.py`
  - asserts every declared public C function has exactly one definition
    (dependency-free; guards against link-time breakage of bindings)
- `check_api_compatibility.py` + `api_compatibility_allowlist.json`
  - signature-level backward-compatibility gate against a reference revision;
    the authoritative ABI/API-stability check
- `verify_generated_up_to_date.py`
  - regenerates in a temp dir and confirms the checked-in output is current
- `check_method_coverage.py` + `method_coverage.json`
  - fails when a public method of a covered C++ class is neither called from
    `db/c.cc` nor excluded with a reason, or when an exclusion is stale
- `c_api_gen_test.py`
  - unit tests for the generator rules (coverage policy, blocklist schema,
    ABI pins, access specifiers, handle method lookup and C mapping), the
    equivalence normalizations and the method coverage rules
- `validate_generated_equivalence.py` + `equivalence_allowlist.json`
  - best-effort migration aid: compares generated wrapper *bodies* against a
    PRE-migration hand-written revision. Body comparison is heuristic; for
    backward-compatibility rely on `check_api_compatibility.py`.

## Toolchain requirements

Regeneration needs two external tools:

- **`clang++`** (libclang) — used to dump the C++ struct ASTs that drive
  auto-discovery. Resolution order is `$CXX` (if it names a clang), then bare
  `clang++`, then versioned fallbacks (`clang++-21` … `clang++-13`).
- **`clang-format`** — used to canonicalize the generated `.inc` fragments (and
  therefore the inlined `c.h` / `c.cc`).

The checked-in generated output is byte-reproducible only for a **pinned
clang-format version**. CI uses **clang-format-21** (see
`.github/workflows/pr-jobs.yml`); pin the same version locally so your
regeneration matches CI:

```bash
python3 tools/c_api_gen/regen_all.py --clang-format clang-format-21
# or, when running the staleness check via make:
make check-c-api-gen CLANG_FORMAT_BINARY=clang-format-21
```

If `clang++` is not installed, `make check` skips the C API staleness and
method coverage checks with a message instead of failing, so it still works without the codegen
toolchain. CI is the authoritative gate.

The dedicated `build-linux-clang-21-no_test_run` CI job runs this target with
`CHECK_C_API_GEN_STRICT=1`, which turns every such "skip" (missing `clang++` or
clang-format, or an unresolvable compat baseline ref) into a hard error, so the
core CI coverage cannot silently regress to a no-op:

```bash
make check-c-api-gen CHECK_C_API_GEN_STRICT=1 CLANG_FORMAT_BINARY=clang-format-21
```

## Running the generators

From the repo root, to regenerate all checked-in fragments and the inlined
`c.h` / `c.cc`:

```bash
python3 tools/c_api_gen/regen_all.py
```

This regenerates:

- the spec-driven fragments from `spec.json`
- the auto-discovered fragments produced by `auto_simple_bindings.py`
- the handle fragments produced by `handle_bindings.py` from `handles.json`
- the round-trip tests produced by `gen_roundtrip_tests.py`

To preview only the spec-driven output for one section without touching the
checked-in fragments, run `generate_c_api.py` directly with `--header-out` /
`--source-out` pointing at a scratch path (e.g. under `/tmp`).

To verify the auto-discovered checked-in output is up to date:

```bash
python3 tools/c_api_gen/auto_simple_bindings.py --check
```

To verify the full checked-in generated set is stable without relying on Git
worktree metadata:

```bash
python3 tools/c_api_gen/verify_generated_up_to_date.py
```

## Maintaining Auto-Managed Families

When a new public field is added to an auto-managed family, use this decision
process:

1. If it is a simple scalar, enum, string, or supported chrono field and the
   desired C API shape matches the existing conventions, do not hand-edit
   `include/rocksdb/c.h` or `db/c.cc`. Run `python3 tools/c_api_gen/regen_all.py`
   and let `auto_simple_bindings.py` generate the wrapper.
2. If the field needs an explicit C API design because of ownership, callbacks,
   vectors, maps, nested structs, or intentionally different C naming/behavior,
   keep it out of auto-generation by adding an entry to
   `tools/c_api_gen/auto_simple_bindings_blocklist.json` with
   `"policy": "manual"`.
3. If the field belongs to an auto-managed family but you intentionally want to
   postpone C API coverage for a short time, add a blocklist entry with
   `"policy": "deferred"` and a concrete reason. Include `tracking_issue` when
   you have one.
4. If none of the above applies, the build should fail. Extend
   `auto_simple_bindings.py`, add a spec-driven wrapper, or add a hand-written
   wrapper and then update the blocklist policy accordingly.

The blocklist is intentionally strict:

- each entry must identify the managed family and field
- each entry must have a non-empty reason
- policy must be either `manual` or `deferred`
- stale entries fail regeneration if the field disappears or is renamed
- an entry may list `functions` to leave only those C functions of the field
  to another layer (for example a legacy setter with extra behavior); the
  generator still owns the field's other bindings, and each listed function
  must exist in `c_base.h` / `c_base.cc` or `handles.json`
- non-public members are never discovered, so they need no entry

Example deferred entry:

```json
{
  "name": "ReadOptions",
  "entries": [
    {
      "field": "future_slice_option",
      "policy": "deferred",
      "reason": "Needs an explicit C buffer ownership design before exposing it.",
      "tracking_issue": "T123456789"
    }
  ]
}
```

The blocklist only applies to the auto-managed families handled by
`auto_simple_bindings.py`. It is not a substitute for `spec.json`, which still
owns non-trivial method-style wrappers. It is also unrelated to
`equivalence_allowlist.json`, which only suppresses intentional diff mismatches
in the equivalence checker.

## Adding a handle method binding

To expose a C++ member function as `rocksdb_<object>_<verb>`, add an entry to
the handle's `methods` list in `handles.json` and regenerate:

```json
{"name": "rocksdb_writebatch_put", "method": "Put", "overload": ["const Slice &", "const Slice &"], "result": "discard"}
```

Only `name` and `method` are required (or `field`, `count` or `set`, described
below).
The optional keys are:

- `overload`: the C++ parameter types, when the method is overloaded
- `arity`: call with only the first N arguments and let the rest default
- `bind`: fixed C++ arguments, e.g. `{"pri": "Env::BOTTOM"}`
- `result`: `discard` to drop a result, `borrow` / `slice` / `copy` for a
  `Slice` or string result (`c_str` for a `std::string` returned through
  `c_str()`, and `strdup` for a `strdup()` of it, both without a length),
  `handle` to return a pointer or smart pointer result in a new handle
- `result_handle`: the handle to return, when several wrap the result's type
- `out_handle`: a `T*` out-parameter to fill straight into a new handle, which
  the C function returns instead of taking the parameter
- `consumes`: `T*` parameters the C++ call takes ownership of; their C handles
  are freed after the call (needs `result: handle`)
- `param_order`: the order of the C parameters, when it differs from the C++
  one (the handle still comes first, and `errptr` or `len` last)
- `nullable`: parameters that accept a NULL C pointer. A `const Slice *` takes
  `const char* x, size_t x_len` and passes `nullptr` when `x` is NULL; a `T*`
  or `shared_ptr<T>` taken from a handle passes `nullptr` when the handle is
  NULL; a `const T &` whose handle holds a `T` passes a default-constructed `T`
  when the handle is NULL
- `skip_if_null`: pointer parameters, or the handle by its `var`, for which
  NULL makes the function return without doing anything (only for a function
  with no result)
- `shared_count`: one C `int` parameter, of this name, that gives the length
  of every `const SliceParts &` parameter (each one otherwise takes its own
  `num_x`)
- `receiver_const`: override the `const` of the handle parameter, which
  otherwise follows the C++ method
- `c_types`: pin a parameter's (or `return`'s) C type for ABI compatibility;
  a handle parameter can only drop `const`, and for a new handle, `return` can
  only add `const`
- `param_handles`: the handle to use for a parameter when several wrap its
  C++ type
- `param_names`: a C name for a parameter the C++ declaration leaves unnamed,
  keyed by its zero-based position
- `doc`: comment lines emitted above the declaration in `c.h`

Keys naming a parameter use its C++ name (or position, when unnamed).

An entry can also name a public data member of `rep` instead of a method.
`"field": "x"` returns `rep.x`, `"count": "x"` returns `rep.x.size()` for a
container member, and `"set": "x"` assigns `rep.x` from one C parameter, which
maps like a by-value C++ parameter of the member's type (a class that a handle
holds is passed by reference, and `std::string` as `const char*`). The
parameter takes the member's name, which other keys use for it. Read entries
take only `result`, `receiver_const`, `c_types` and `doc`; `set` entries take
`param_names`, `param_handles`, `nullable`, `skip_if_null`, `c_types` and
`doc`:

```json
{"name": "rocksdb_options_set_row_cache", "set": "row_cache", "param_names": {"row_cache": "cache"}, "skip_if_null": ["row_cache"]}
```

A handle whose `rep` is a `std::vector<T>` reads the vector instead: an entry
with `"count": true` returns `rep.size()`, and an entry with `"field": "x"`
returns `rep[index].x`, where `index` is a C parameter of the type the handle
names in `index_type` (`int` or `size_t`):

```json
{"name": "rocksdb_livefiles_name", "field": "name", "result": "c_str"}
```

A function listed here must not also be hand-written; regeneration fails if it
is. If the C function needs more than the one call, write it by hand in
`c_base.h` / `c_base.cc` instead.

To refresh the full checked-in generated set and confirm nothing changes:

```bash
python3 tools/c_api_gen/verify_generated_up_to_date.py
```

To verify active generated wrappers are equivalent to the handwritten wrappers
from a reference revision:

```bash
python3 tools/c_api_gen/validate_generated_equivalence.py --ref HEAD
```

This checks the generated fragments currently included by
`include/rocksdb/c.h` and `db/c.cc` against `HEAD:include/rocksdb/c.h` and
`HEAD:db/c.cc`. Known historical inconsistencies can be documented in
`equivalence_allowlist.json` with a reason instead of being silently ignored.

## Method coverage

`check_method_coverage.py` keeps the list of what the C API leaves out. For
each class in `method_coverage.json` it takes every public method from the C++
headers and every call in `db/c.cc` from clang's AST, and fails if a method is
neither called nor listed in that class's `excluded` map. A call counts for the
static type of the object it is made on: `db->rep->Put(...)` covers `DB::Put`,
and a call through a `TransactionDB*` covers `TransactionDB`, not `DB`. Methods
a class inherits from another covered class (or from a subclass of one) are
checked on that class only, and the bases in `excluded_bases` are not checked
at all. Deprecated, deleted and `TEST_` methods are ignored. Coverage is per
name, so a new overload of a method that is already called does not trip it.

When it fails on a new method, bind the method (in `handles.json`, or by hand)
or add it to `excluded` with one of the keys in `reasons`. An exclusion also
fails once its method is removed or `db/c.cc` starts calling it, so the list
only ever holds methods that really have no binding. `--report` prints how
many methods of each class are bound and excluded:

```bash
python3 tools/c_api_gen/check_method_coverage.py --clang clang++ --report
```

## Suggested migration path

1. Start with generated preview fragments only.
2. Migrate a few low-risk existing wrappers to generated sections.
3. Keep manual and generated sections clearly separated.
4. Let auto-discovery pick up new simple public fields automatically.
5. Extend the spec or keep manual wrappers for complex/manual families.

This incremental approach minimizes downstream impact while still reducing
maintenance burden over time.
