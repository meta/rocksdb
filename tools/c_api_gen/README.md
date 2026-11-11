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

## What this prototype focuses on

The generator currently targets representative examples from the easiest and
most repetitive wrapper families:

1. **Simple scalar field setters/getters**
   - `rocksdb_options_set_create_if_missing`
   - `rocksdb_options_get_create_if_missing`
   - `rocksdb_readoptions_set_fill_cache`
   - `rocksdb_writeoptions_set_sync`
   - `rocksdb_block_based_options_set_block_size`
   - `rocksdb_cuckoo_options_set_hash_ratio`

2. **Simple forwarding wrappers with no `Status`**
   - `rocksdb_writebatch_clear`
   - `rocksdb_writebatch_put`
   - `rocksdb_writebatch_put_cf`
   - `rocksdb_writebatch_delete`
   - `rocksdb_writebatch_put_log_data`
   - `rocksdb_writebatch_set_save_point`
   - `rocksdb_transaction_set_savepoint`

3. **Simple forwarding wrappers with `Status` -> `char** errptr`**
   - `rocksdb_put`, `rocksdb_put_cf`
   - `rocksdb_delete`, `rocksdb_delete_cf`
   - `rocksdb_merge`, `rocksdb_merge_cf`
   - `rocksdb_write`
   - `rocksdb_writebatch_rollback_to_save_point`
   - `rocksdb_writebatch_pop_save_point`
   - `rocksdb_transaction_put`, `rocksdb_transaction_put_cf`
   - `rocksdb_transaction_merge`, `rocksdb_transaction_merge_cf`
   - `rocksdb_transaction_delete`, `rocksdb_transaction_delete_cf`
   - `rocksdb_transaction_commit`
   - `rocksdb_transaction_rollback`
   - `rocksdb_transaction_rollback_to_savepoint`

4. **Simple metadata accessors**
   - `rocksdb_flushjobinfo_cf_name`
   - `rocksdb_flushjobinfo_largest_seqno`

This is intentionally a constrained subset. The point is to demonstrate a
maintainable pattern and a spec format that can be extended gradually.

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

### 3. Keep explicit spec for non-trivial wrappers

The spec-driven generator remains the right tool for method-style wrappers and
other APIs whose C surface requires an explicit design decision. In practice:

- auto-discovery owns simple direct field coverage for selected struct families
- `spec.json` owns wrappers that are still manual in design but repetitive in
  implementation
- fully irregular ownership-heavy or callback-heavy APIs remain hand-written

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
- `regen_all.py`
  - orchestrates both generators and inlines the fragments into `c.h` / `c.cc`

Verification (run by `make check-c-api-gen` and in CI):

- `check_api_completeness.py`
  - asserts every declared public C function has exactly one definition
    (dependency-free; guards against link-time breakage of bindings)
- `check_api_compatibility.py` + `api_compatibility_allowlist.json`
  - signature-level backward-compatibility gate against a reference revision;
    the authoritative ABI/API-stability check
- `verify_generated_up_to_date.py`
  - regenerates in a temp dir and confirms the checked-in output is current
- `c_api_gen_test.py`
  - unit tests for the generator rules (coverage policy, blocklist schema,
    ABI pins, access specifiers) and the equivalence normalizations
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

If `clang++` is not installed, `make check` skips the C API staleness check
with a message instead of failing, so it still works without the codegen
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
- an entry may list `functions` to keep only those C functions of the field
  hand-written (for example a legacy setter with extra behavior); the
  generator still owns the field's other bindings, and each listed function
  must exist in `c_base.h` / `c_base.cc`
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

## Suggested migration path

1. Start with generated preview fragments only.
2. Migrate a few low-risk existing wrappers to generated sections.
3. Keep manual and generated sections clearly separated.
4. Let auto-discovery pick up new simple public fields automatically.
5. Extend the spec or keep manual wrappers for complex/manual families.

This incremental approach minimizes downstream impact while still reducing
maintenance burden over time.
