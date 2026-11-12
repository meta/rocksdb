#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Generate C API bindings for opaque handle types from handles.json.

Each C handle (`rocksdb_iterator_t`, `rocksdb_writebatch_t`, ...) wraps one
C++ object in a `rep` member. handles.json records, per handle:

- the C++ type of `rep`, and whether this generator defines the struct
- trivial lifecycle functions (`return new H;`, `delete h;`)
- methods: C functions that forward to one C++ member function

The C signature of a method is derived from the C++ declaration
(parsed with clang), using the C API conventions:

  C++ parameter                  C parameters             C++ argument
  -----------------------------  -----------------------  ------------------
  const Slice& / Slice           const char* x, size_t    Slice(x, x_len)
                                 x_len
  bool                           unsigned char            x
  arithmetic / typedef of one    same type                x
  enum                           int                      static_cast<E>(x)
  const std::string&             const char*              std::string(x)
  const SliceParts&              int num_x, const char*   SliceParts over a
                                 const* x_list, const     Slice array built
                                 size_t* x_list_sizes     from the C arrays
  handle rep (value, pointer,    H* / const H*            x->rep, &x->rep,
  reference, smart pointer)                               *x->rep, ...

  C++ result                     C result
  -----------------------------  ---------------------------------------
  Status / IOStatus              void, plus a trailing `char** errptr`
  bool                           unsigned char
  arithmetic / typedef of one    same type
  enum                           int (widened with lossless_cast)
  Slice / const std::string&     const char* (borrowed, `result: borrow`) or
                                 malloc()ed char* (`result: copy`), plus a
                                 trailing `size_t* len`; or a borrowed
                                 rocksdb_slice_t (`result: slice`); or, for a
                                 std::string only, its borrowed NUL-terminated
                                 c_str() (`result: c_str`)
  std::string by value           malloc()ed char* (`result: copy`) only
  T* / shared_ptr<T> /           a new handle holding it (`result: handle`)
  unique_ptr<T>
  anything                       void, with `result: discard`

A parameter listed in `nullable` accepts a NULL C pointer:

  C++ parameter                  NULL in C passes
  -----------------------------  ---------------------------------------
  const Slice *                  nullptr (C: const char* x, size_t x_len)
  const T & (value handle rep)   a default-constructed T

A handle whose rep is a std::vector<T> can also have entries that read the
vector instead of calling a method: `count` returns rep.size(), and `field`
returns one public data member of rep[index], taking the index as a C
parameter of the handle's `index_type`. A std::string member is returned like
a `const std::string&` result.

The SliceParts parameters of an entry with `shared_count` all take their
length from one C parameter of that name instead of one each.

A `T*` out-parameter named by `out_handle` is filled straight into a new
handle, which the C function returns. A `T*` parameter listed in `consumes`
is handed to the C++ call, which takes ownership; its C handle is freed after
the call.

Everything the conventions cannot decide is explicit in the entry: which
overload, parameters bound to fixed values, whether a string result is
borrowed or copied, and C types pinned for ABI compatibility. Generated code
passes every argument by reference or as a Slice (no copies) except where the
C++ API itself takes a std::string, and returns string results without copying
unless the C signature hands the caller a malloc()ed buffer (`result: copy`).

Like the auto-discovered field bindings, every function named here is owned by
this generator: a declaration or definition of the same name in c_base.h /
c_base.cc or another generated fragment fails generation.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from generate_c_api import format_signature, write_if_changed
from validate_generated_equivalence import extract_function_decls, extract_function_defs

ROOT = Path(__file__).resolve().parents[2]
HANDLES_PATH = ROOT / "tools/c_api_gen/handles.json"
TEMPLATE_ROOT = ROOT / "tools/c_api_gen"
GENERATED_ROOT = ROOT / "c_api_gen"
STRUCTS_OUT = GENERATED_ROOT / "c_generated_handle_structs.cc.inc"
HEADER_OUT = GENERATED_ROOT / "c_generated_handles.h.inc"
SOURCE_OUT = GENERATED_ROOT / "c_generated_handles.cc.inc"
OWN_OUTPUTS = {STRUCTS_OUT.name, HEADER_OUT.name, SOURCE_OUT.name}

SCHEMA_VERSION = 1
HANDLE_KEYS = {
    "c_type",
    "rep",
    "struct",
    "var",
    "create",
    "destroy",
    "index_type",
    "methods",
}
METHOD_KEYS = {
    "name",
    "method",
    "field",
    "count",
    "overload",
    "arity",
    "bind",
    "result",
    "receiver_const",
    "c_types",
    "param_handles",
    "param_names",
    "doc",
    "result_handle",
    "out_handle",
    "consumes",
    "param_order",
    "nullable",
    "shared_count",
}

# C types that a C++ parameter or result of the same spelling maps to as-is.
C_SCALARS = frozenset(
    {
        "char",
        "signed char",
        "unsigned char",
        "short",
        "unsigned short",
        "int",
        "unsigned int",
        "long",
        "unsigned long",
        "long long",
        "unsigned long long",
        "int8_t",
        "int16_t",
        "int32_t",
        "int64_t",
        "uint8_t",
        "uint16_t",
        "uint32_t",
        "uint64_t",
        "size_t",
        "float",
        "double",
    }
)
SLICE_TYPES = frozenset({"const Slice &", "Slice", "const Slice"})
STRING_PARAM_TYPES = frozenset({"const std::string &", "const std::string"})
SLICE_PARTS = "const SliceParts &"
STATUS_TYPES = frozenset({"Status", "IOStatus"})
STRING_RESULT_TYPES = {
    "Slice": "Slice",
    "const Slice": "Slice",
    "const std::string &": "const std::string&",
    "std::string": "std::string",
    "const std::string": "std::string",
}


class GenError(Exception):
    pass


def norm_type(t: str) -> str:
    t = t.replace("ROCKSDB_NAMESPACE::", "").replace("rocksdb::", "")
    return " ".join(t.split())


def c_type_spelling(t: str) -> str:
    """Canonical C spelling used for comparisons: `const char *` -> `const char*`."""
    t = " ".join(t.replace("*", " * ").split())
    return t.replace(" *", "*")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@dataclass
class Binding:
    handle: "Handle"
    entry: dict

    @property
    def name(self) -> str:
        return self.entry["name"]


@dataclass
class Handle:
    c_type: str
    rep: str
    struct: str
    var: str
    create: str | None
    destroy: dict | None
    index_type: str | None = None
    bindings: list[Binding] = field(default_factory=list)

    def element(self) -> str | None:
        """The element type of a std::vector rep."""
        m = re.fullmatch(r"std::vector<\s*([\w:]+)\s*>", norm_type(self.rep))
        return m and m.group(1)

    def rep_kind(self) -> tuple[str, str]:
        """Return (kind, C++ class) of rep: value, pointer, shared or unique."""
        rep = norm_type(self.rep)
        m = re.fullmatch(r"std::(shared|unique)_ptr<\s*(?:const\s+)?([\w:]+)\s*>", rep)
        if m:
            return m.group(1), m.group(2)
        m = re.fullmatch(r"(?:const\s+)?([\w:]+)\s*\*", rep)
        if m:
            return "pointer", m.group(1)
        return "value", rep.removeprefix("const ")

    def rep_is_const(self) -> bool:
        rep = norm_type(self.rep)
        return bool(re.match(r"(std::(shared|unique)_ptr<\s*)?const\b", rep))


def load_handles(path: Path) -> tuple[list[str], list[Handle]]:
    data = json.loads(path.read_text())
    if data.get("schema_version") != SCHEMA_VERSION:
        raise GenError(f"{path}: unsupported schema_version")
    unknown = set(data) - {"schema_version", "description", "headers", "handles"}
    if unknown:
        raise GenError(f"{path}: unknown top-level keys {sorted(unknown)}")
    handles: list[Handle] = []
    seen_types: set[str] = set()
    seen_names: set[str] = set()

    def claim(name: str) -> None:
        if not re.fullmatch(r"rocksdb_\w+", name):
            raise GenError(f"invalid C function name {name!r}")
        if name in seen_names:
            raise GenError(f"duplicate C function {name} in {path.name}")
        seen_names.add(name)

    for raw in data["handles"]:
        unknown = set(raw) - HANDLE_KEYS
        if unknown:
            raise GenError(f"{raw.get('c_type')}: unknown keys {sorted(unknown)}")
        c_type = raw["c_type"]
        if c_type in seen_types:
            raise GenError(f"duplicate handle {c_type}")
        seen_types.add(c_type)
        if raw.get("struct") not in ("generated", "manual"):
            raise GenError(f"{c_type}: struct must be 'generated' or 'manual'")
        destroy = raw.get("destroy")
        if destroy is not None:
            if (
                set(destroy) - {"name", "owns_rep", "receiver_const"}
                or not isinstance(destroy.get("owns_rep"), bool)
                or not isinstance(destroy.get("receiver_const", False), bool)
            ):
                raise GenError(
                    f"{c_type}: destroy takes name, owns_rep and optional receiver_const"
                )
            claim(destroy["name"])
        if raw.get("create") is not None:
            claim(raw["create"])
        handle = Handle(
            c_type=c_type,
            rep=raw["rep"],
            struct=raw["struct"],
            var=raw["var"],
            create=raw.get("create"),
            destroy=destroy,
            index_type=raw.get("index_type"),
        )
        handle.rep_kind()  # validate
        if handle.index_type is not None and (
            handle.index_type not in ("int", "size_t") or handle.element() is None
        ):
            raise GenError(
                f"{c_type}: index_type is int or size_t, for a std::vector rep"
            )
        if (
            destroy is not None
            and destroy["owns_rep"]
            and handle.rep_kind()[0] != "pointer"
        ):
            raise GenError(f"{c_type}: owns_rep only applies to a raw pointer rep")
        for entry in raw.get("methods", []):
            unknown = set(entry) - METHOD_KEYS
            if unknown:
                raise GenError(f"{entry.get('name')}: unknown keys {sorted(unknown)}")
            claim(entry["name"])
            handle.bindings.append(Binding(handle, entry))
        handles.append(handle)
    return data.get("headers", []), handles


# ---------------------------------------------------------------------------
# C++ declarations (clang JSON AST)
# ---------------------------------------------------------------------------


@dataclass
class Param:
    name: str
    type: str  # normalized qualType as written
    has_default: bool


@dataclass
class Function:
    owner: str  # declaring class
    name: str
    params: list[Param]
    result: str
    is_const: bool


def _iter_json_docs(text: str) -> Iterable[dict]:
    decoder = json.JSONDecoder()
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            return
        obj, index = decoder.raw_decode(text, index)
        yield obj


def _walk(node: dict, parents: tuple = ()) -> Iterable[tuple[dict, tuple]]:
    yield node, parents
    for child in node.get("inner", []) or []:
        yield from _walk(child, parents + (node,))


class CxxIndex:
    """Looks up C++ declarations in a translation unit of public headers."""

    def __init__(self, clang: str, headers: list[str]):
        self.clang = clang
        self._tu = tempfile.NamedTemporaryFile(
            "w", suffix=".cc", prefix="c_api_gen_handles_", delete=False
        )
        for header in headers:
            self._tu.write(f'#include "{header}"\n')
        self._tu.close()
        self._dumps: dict[str, list[tuple[dict, tuple]]] = {}

    def close(self) -> None:
        Path(self._tu.name).unlink(missing_ok=True)

    def prefetch(self, names: Iterable[str]) -> None:
        todo = sorted({n for n in names if n not in self._dumps})
        with ThreadPoolExecutor() as pool:
            for name, nodes in zip(todo, pool.map(self._dump, todo)):
                self._dumps[name] = nodes

    def _dump(self, name: str) -> list[tuple[dict, tuple]]:
        cmd = [
            self.clang,
            "-std=c++20",
            "-x",
            "c++",
            f"-I{ROOT / 'include'}",
            f"-I{ROOT}",
            "-Xclang",
            "-ast-dump=json",
            "-Xclang",
            # The filter is a substring match on the qualified name, so the
            # namespace prefix keeps nested namesakes (MemTableRep::Iterator)
            # out of a lookup of a namespace-scope class (Iterator).
            f"-ast-dump-filter=rocksdb::{name}",
            "-fsyntax-only",
            self._tu.name,
        ]
        proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return [item for doc in _iter_json_docs(proc.stdout) for item in _walk(doc)]

    def nodes(self, name: str) -> list[tuple[dict, tuple]]:
        if name not in self._dumps:
            self._dumps[name] = self._dump(name)
        return self._dumps[name]

    def _find(self, qualified: str, kinds: tuple[str, ...]) -> dict | None:
        # Each declaration matching the dump filter is a separate top-level
        # document, so only top-level nodes are candidates.
        last = qualified.split("::")[-1]
        for node, parents in self.nodes(qualified):
            if parents or node.get("kind") not in kinds or node.get("name") != last:
                continue
            if node.get("kind") == "CXXRecordDecl" and not node.get(
                "completeDefinition"
            ):
                continue
            return node
        return None

    def record(self, name: str) -> dict:
        node = self._find(name, ("CXXRecordDecl",))
        if node is None:
            raise GenError(f"C++ class {name} not found in the parsed headers")
        return node

    def is_enum(self, name: str) -> bool:
        return self._find(name, ("EnumDecl",)) is not None

    def typedef_target(self, name: str) -> str | None:
        node = self._find(name, ("TypedefDecl", "TypeAliasDecl"))
        return None if node is None else norm_type(node["type"]["qualType"])

    def field(self, cls: str, name: str) -> str | None:
        """The type of public data member `name` of `cls` or a public base."""
        node = self.record(cls)
        access = "private" if node.get("tagUsed") == "class" else "public"
        for child in node.get("inner", []) or []:
            if child.get("kind") == "AccessSpecDecl":
                access = child.get("access", access)
            elif child.get("kind") == "FieldDecl" and child.get("name") == name:
                if access != "public":
                    return None
                return norm_type(child["type"]["qualType"])
        for base in node.get("bases", []):
            if base.get("access") == "public":
                base_name = norm_type(base["type"]["qualType"]).split("<")[0]
                self.prefetch([base_name])
                found = self.field(base_name, name)
                if found is not None:
                    return found
        return None

    def methods(self, cls: str, name: str) -> list[Function]:
        """Public overloads of `name` that a call through `cls` can resolve to.

        Follows C++ name lookup: a declaration of `name` in a class hides every
        base-class overload unless the class re-exports them with a public
        `using Base::name;`.
        """
        found: list[Function] = []
        self._collect_methods(cls, name, found, set(), visible_only=True)
        if any(not p.name for f in found for p in f.params):
            # An override often leaves parameters unnamed; name them after the
            # declaration it overrides, even where name lookup hides that one.
            every: list[Function] = []
            self._collect_methods(cls, name, every, set(), visible_only=False)
            for f in found:
                fill_param_names(f, every)
        return found

    def _collect_methods(
        self,
        cls: str,
        name: str,
        out: list[Function],
        seen: set[str],
        visible_only: bool,
    ) -> None:
        if cls in seen:
            return
        seen.add(cls)
        node = self.record(cls)
        access = "private" if node.get("tagUsed") == "class" else "public"
        declared = False
        reexported = False
        for child in node.get("inner", []) or []:
            kind = child.get("kind")
            if kind == "AccessSpecDecl":
                access = child.get("access", access)
                continue
            if child.get("isImplicit"):
                continue
            if kind == "UsingDecl" and child.get("name", "").split("::")[-1] == name:
                declared = True
                reexported = reexported or access == "public"
            elif (
                kind in ("CXXMethodDecl", "FunctionTemplateDecl")
                and child.get("name") == name
            ):
                declared = True
                if kind == "CXXMethodDecl" and (access == "public" or not visible_only):
                    out.append(_function(cls, child))
        if visible_only and declared and not reexported:
            return
        for base in node.get("bases", []):
            if base.get("access") == "public" or not visible_only:
                base_name = norm_type(base["type"]["qualType"]).split("<")[0]
                self.prefetch([base_name])
                self._collect_methods(base_name, name, out, seen, visible_only)


def fill_param_names(target: Function, donors: list[Function]) -> None:
    """Name `target`'s unnamed parameters after a donor with the same types."""
    types = [p.type for p in target.params]
    for donor in donors:
        if [p.type for p in donor.params] == types:
            target.params = [
                p if p.name else Param(q.name, p.type, p.has_default)
                for p, q in zip(target.params, donor.params)
            ]


def _function(owner: str, node: dict) -> Function:
    qual = node["type"]["qualType"]
    params = [
        Param(p.get("name", ""), norm_type(p["type"]["qualType"]), "init" in p)
        for p in node.get("inner", []) or []
        if p.get("kind") == "ParmVarDecl"
    ]
    result = norm_type(qual[: qual.index("(")])
    is_const = bool(re.search(r"\)\s*const\b", qual))
    return Function(owner, node["name"], params, result, is_const)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


@dataclass
class CParam:
    c_type: str
    name: str


@dataclass
class Mapped:
    c_params: list[CParam]
    arg: str
    # Statements that must run before the call, and the locals they declare.
    setup: list[str] = field(default_factory=list)
    locals: list[str] = field(default_factory=list)


class TypeMapper:
    def __init__(self, index: CxxIndex, handles: list[Handle]):
        self.index = index
        self.handles = {h.c_type: h for h in handles}
        self.by_class: dict[str, list[Handle]] = {}
        for h in handles:
            self.by_class.setdefault(h.rep_kind()[1], []).append(h)

    def scalar(self, cxx: str, context: str, scope: str = "") -> tuple[str, str | None]:
        """Map an arithmetic/enum type. Returns (C type, enum C++ type or None).

        `scope` is the class that declares the function: a type written inside
        it (`IndexMode`) may name one of its members (`Owner::IndexMode`).
        """
        seen = set()
        t = cxx
        while True:
            if t in C_SCALARS:
                return t, None
            if t == "bool":
                return "unsigned char", None
            names = [f"{scope}::{t}", t] if scope and "::" not in t else [t]
            enum = next((n for n in names if self.index.is_enum(n)), None)
            if enum is not None:
                return "int", enum
            if t in seen:
                break
            seen.add(t)
            target = next(
                (r for r in map(self.index.typedef_target, names) if r is not None),
                None,
            )
            if target is None:
                break
            t = target
        raise GenError(f"{context}: no C mapping for C++ type {cxx!r}")

    def resolve_result(self, fn: Function) -> str:
        """The result type, with a typedef of Status or a string resolved."""
        known = STATUS_TYPES | STRING_RESULT_TYPES.keys() | {"void"}
        if fn.result in known:
            return fn.result
        const = "const " if fn.result.startswith("const ") else ""
        bare = fn.result.removeprefix("const ")
        for name in (f"{fn.owner}::{bare}", bare):
            target = self.index.typedef_target(name)
            if target is not None and f"{const}{target}" in known:
                return f"{const}{target}"
        return fn.result

    def handle_for(self, cls: str, context: str, preferred: str | None) -> Handle:
        if preferred is not None:
            handle = self.handles.get(preferred)
            if handle is None or handle.rep_kind()[1] != cls:
                raise GenError(
                    f"{context}: param_handles names {preferred}, which does not wrap {cls}"
                )
            return handle
        candidates = self.by_class.get(cls, [])
        if len(candidates) != 1:
            raise GenError(
                f"{context}: {len(candidates)} handles wrap {cls}; "
                "name one in param_handles"
            )
        return candidates[0]

    def result_handle(self, cxx: str, context: str, preferred: str | None) -> Handle:
        """The handle a returned pointer or smart pointer is stored in."""
        m = re.fullmatch(r"(const )?([\w:]+) \*", cxx)
        sp = re.fullmatch(r"std::(shared|unique)_ptr<(const )?([\w:]+)>", cxx)
        if m:
            kind, const, cls = "pointer", bool(m.group(1)), m.group(2)
        elif sp:
            kind, const, cls = sp.group(1), bool(sp.group(2)), sp.group(3)
        else:
            raise GenError(
                f"{context}: result: handle needs a pointer or smart pointer, "
                f"not {cxx!r}"
            )
        handle = self.handle_for(cls, context, preferred)
        rep_kind = handle.rep_kind()[0]
        if rep_kind != kind:
            raise GenError(
                f"{context}: {cxx} needs a {kind} rep, {handle.c_type} has {rep_kind}"
            )
        if const and not handle.rep_is_const():
            raise GenError(
                f"{context}: {cxx} does not fit the non-const rep of {handle.c_type}"
            )
        return handle

    def out_handle(self, cxx: str, context: str, preferred: str | None) -> Handle:
        """The handle whose rep an out-parameter (`T*`) fills."""
        m = re.fullmatch(r"(?!const )(.+) \*", cxx)
        if m is None:
            raise GenError(f"{context}: out_handle needs a non-const T*, not {cxx!r}")
        handle = self.handle_for(m.group(1), context, preferred)
        if handle.rep_kind()[0] != "value":
            raise GenError(f"{context}: out_handle needs a handle that holds a value")
        return handle

    def param(
        self,
        p: Param,
        context: str,
        preferred: str | None,
        scope: str = "",
        nullable: bool = False,
    ) -> Mapped:
        t = p.type
        n = p.name
        if not n:
            raise GenError(f"{context}: unnamed C++ parameter")
        if nullable:
            return self._nullable_param(t, n, context, preferred)
        if t in SLICE_TYPES:
            return Mapped(
                [CParam("const char*", n), CParam("size_t", f"{n}_len")],
                f"Slice({n}, {n}_len)",
            )
        if t in STRING_PARAM_TYPES:
            return Mapped([CParam("const char*", n)], f"std::string({n})")
        if t == SLICE_PARTS:
            return self.slice_parts([n], f"num_{n}")[0]
        m = re.fullmatch(r"(const )?([\w:]+) (\*|&)", t)
        sp = re.fullmatch(
            r"(?:const )?std::(shared_ptr|unique_ptr)<(?:const )?([\w:]+)> ?&?", t
        )
        if m or sp:
            cls = (sp or m).group(2)
            if cls in self.by_class or preferred is not None:
                return self._handle_param(t, cls, n, m, sp, context, preferred)
        c_type, enum = self.scalar(t.removeprefix("const "), context, scope)
        arg = f"static_cast<{enum}>({n})" if enum else n
        return Mapped([CParam(c_type, n)], arg)

    @staticmethod
    def slice_parts(names: list[str], count: str) -> list[Mapped]:
        """One SliceParts argument per name, over Slices built from C arrays
        of pointers and sizes.

        Every array has `count` elements, a C parameter that comes with the
        first name, and one loop fills them all. SliceParts only points at an
        array, which is a local of the C function and so outlives the call.
        """
        mapped = []
        fills = []
        for n in names:
            parts = f"{n}_slices"
            c_params = [
                CParam("const char* const*", f"{n}_list"),
                CParam("const size_t*", f"{n}_list_sizes"),
            ]
            if not mapped:
                c_params.insert(0, CParam("int", count))
            mapped.append(
                Mapped(
                    c_params,
                    f"SliceParts({parts}.get(), {count})",
                    [f"std::unique_ptr<Slice[]> {parts}(new Slice[{count}]);"],
                    [parts],
                )
            )
            fills.append(f"  {parts}[i] = Slice({n}_list[i], {n}_list_sizes[i]);")
        mapped[-1].setup += [f"for (int i = 0; i < {count}; i++) {{", *fills, "}"]
        mapped[-1].locals.append("i")
        return mapped

    def _nullable_param(
        self, t: str, n: str, context: str, preferred: str | None
    ) -> Mapped:
        if t == "const Slice *":
            local = f"{n}_slice"
            return Mapped(
                [CParam("const char*", n), CParam("size_t", f"{n}_len")],
                f"{n} ? &{local} : nullptr",
                [f"Slice {local}({n}, {n}_len);"],
                [local],
            )
        m = re.fullmatch(r"const ([\w:]+) &", t)
        handle = None
        if m and (m.group(1) in self.by_class or preferred is not None):
            handle = self.handle_for(m.group(1), context, preferred)
        if handle is None or handle.rep_kind()[0] != "value":
            raise GenError(
                f"{context}: nullable needs a const Slice * or a const T & whose "
                f"handle holds a value, not {t!r}"
            )
        cls = handle.rep_kind()[1]
        default, ref = f"{n}_default", f"{n}_ref"
        return Mapped(
            [CParam(f"const {handle.c_type}*", n)],
            ref,
            [
                f"{cls} {default};",
                f"const {cls}& {ref} = {n} != nullptr ? {n}->rep : {default};",
            ],
            [default, ref],
        )

    def _handle_param(self, t, cls, n, m, sp, context, preferred) -> Mapped:
        handle = self.handle_for(cls, context, preferred)
        kind, _ = handle.rep_kind()
        is_const = bool(m and m.group(1))
        if sp:
            if kind != sp.group(1).split("_")[0]:
                raise GenError(
                    f"{context}: {t} needs a {sp.group(1)} rep, {handle.c_type} has {kind}"
                )
            return Mapped([CParam(f"{handle.c_type}*", n)], f"{n}->rep")
        ref = m.group(3)
        c_type = f"{'const ' if is_const else ''}{handle.c_type}*"
        if kind == "value":
            arg = f"{n}->rep" if ref == "&" else f"&{n}->rep"
        elif kind == "pointer":
            arg = f"*{n}->rep" if ref == "&" else f"{n}->rep"
        else:  # shared / unique
            arg = f"*{n}->rep" if ref == "&" else f"{n}->rep.get()"
        return Mapped([CParam(c_type, n)], arg)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


@dataclass
class Rendered:
    name: str
    decl: list[str]
    impl: list[str]


def _doc_lines(entry: dict) -> list[str]:
    doc = entry.get("doc")
    if doc is None:
        return []
    if isinstance(doc, str):
        doc = [doc]
    return list(doc)


def _select(name: str, candidates: list[Function], entry: dict, label: str) -> Function:
    if not candidates:
        raise GenError(f"{name}: no public C++ {label}")
    # An override in a derived class repeats its base declaration. Keep the
    # first (most derived) declaration of each parameter list, since a call
    # uses its default arguments, but take a parameter name from a base
    # declaration when the override leaves it unnamed.
    unique: list[Function] = []
    for f in candidates:
        types = [p.type for p in f.params]
        if all([q.type for q in g.params] != types for g in unique):
            unique.append(
                Function(f.owner, f.name, list(f.params), f.result, f.is_const)
            )
    for f in unique:
        fill_param_names(f, candidates)
    overload = entry.get("overload")
    if overload is not None:
        wanted = [norm_type(t) for t in overload]
        matches = [f for f in unique if [p.type for p in f.params] == wanted]
        if len(matches) != 1:
            raise GenError(
                f"{name}: overload {wanted} matches none of: "
                + "; ".join(str([p.type for p in f.params]) for f in unique)
            )
        return matches[0]
    if len(unique) != 1:
        raise GenError(
            f"{name}: {label} is overloaded; pick one with `overload`: "
            + "; ".join(str([p.type for p in f.params]) for f in unique)
        )
    return unique[0]


# The keys an entry that reads a std::vector rep (`field` or `count`) takes.
ELEMENT_KEYS = {"name", "field", "count", "result", "c_types", "doc"}


def _element_access(entry: dict, handle: Handle, index: CxxIndex) -> Function:
    """A const accessor standing in for the C++ method a `field` or `count`
    entry would otherwise call."""
    name = entry["name"]
    access = [k for k in ("method", "field", "count") if k in entry]
    if len(access) != 1:
        raise GenError(f"{name}: an entry takes one of method, field or count")
    extra = set(entry) - ELEMENT_KEYS
    if extra:
        raise GenError(f"{name}: {sorted(extra)} do not apply to {access[0]}")
    element = handle.element()
    if element is None:
        raise GenError(f"{name}: {access[0]} needs a std::vector rep")
    if "count" in entry:
        if entry["count"] is not True:
            raise GenError(f"{name}: count must be true")
        return Function(element, "size", [], "size_t", True)
    if handle.index_type is None:
        raise GenError(f"{name}: field needs an index_type on {handle.c_type}")
    cxx = index.field(element, entry["field"])
    if cxx is None:
        raise GenError(f"{name}: {element} has no public field {entry['field']}")
    # The member is an lvalue that lives as long as the handle.
    result = "const std::string &" if cxx == "std::string" else cxx
    return Function(element, entry["field"], [], result, True)


def render_binding(binding: Binding, index: CxxIndex, mapper: TypeMapper) -> Rendered:
    entry = binding.entry
    name = entry["name"]
    handle = binding.handle
    kind, cls = handle.rep_kind()
    if "method" in entry and not {"field", "count"} & set(entry):
        fn = _select(
            name,
            index.methods(cls, entry["method"]),
            entry,
            f"method {cls}::{entry['method']}",
        )
    else:
        fn = _element_access(entry, handle, index)

    # Entry keys name a C++ parameter by its name, or by its zero-based
    # position when the declaration leaves it unnamed.
    param_names = dict(entry.get("param_names", {}))
    params = []
    for i, p in enumerate(fn.params):
        key = p.name or str(i)
        params.append(Param(key, p.type, p.has_default))
    renames = {p.name: param_names.pop(p.name) for p in params if p.name in param_names}
    if param_names:
        raise GenError(
            f"{name}: param_names names unknown parameters {sorted(param_names)}"
        )
    bind = dict(entry.get("bind", {}))
    arity = entry.get("arity", len(params))
    if not 0 <= arity <= len(params):
        raise GenError(f"{name}: arity {arity} out of range")
    for p in params[arity:]:
        if not p.has_default:
            raise GenError(
                f"{name}: C++ parameter {p.name} has no default; raise arity"
            )
        if p.name in bind:
            raise GenError(f"{name}: bound parameter {p.name} is past arity {arity}")
    c_types = dict(entry.get("c_types", {}))
    param_handles = entry.get("param_handles", {})

    c_params: list[CParam] = []
    args: list[str] = []
    receiver_const = entry.get("receiver_const", fn.is_const)
    c_params.append(
        CParam(f"{'const ' if receiver_const else ''}{handle.c_type}*", handle.var)
    )
    if "field" in entry:
        c_params.append(CParam(handle.index_type, "index"))
    # The handle a `result: handle` or `out_handle` call returns.
    new_handle: Handle | None = None
    out_name = entry.get("out_handle")
    consumes = list(entry.get("consumes", []))
    consumed: list[str] = []
    nullable = list(entry.get("nullable", []))
    shared: dict[str, Mapped] = {}
    shared_count = entry.get("shared_count")
    if shared_count is not None:
        if not re.fullmatch(r"[A-Za-z_]\w*", shared_count):
            raise GenError(f"{name}: shared_count must name a C parameter")
        sharing = [
            p.name
            for p in params[:arity]
            if p.type == SLICE_PARTS and p.name not in bind and p.name not in nullable
        ]
        if len(sharing) < 2:
            raise GenError(f"{name}: shared_count needs two SliceParts parameters")
        shared_names = [renames.get(n, n) for n in sharing]
        shared = dict(zip(sharing, mapper.slice_parts(shared_names, shared_count)))
    setup: list[str] = []
    local_names: list[str] = []
    by_param: dict[str, list[CParam]] = {}
    for p in params[:arity]:
        if p.name in bind:
            if p.name in nullable:
                raise GenError(f"{name}: nullable parameter {p.name} is bound")
            args.append(bind.pop(p.name))
            continue
        if p.name == out_name:
            new_handle = mapper.out_handle(
                p.type, f"{name}({p.name})", entry.get("result_handle")
            )
            args.append("&result->rep")
            continue
        c_name = renames.get(p.name, p.name)
        if not re.fullmatch(r"[A-Za-z_]\w*", c_name):
            raise GenError(
                f"{name}: C++ parameter {p.name} is unnamed; name it in param_names"
            )
        is_nullable = p.name in nullable
        if is_nullable:
            nullable.remove(p.name)
        if p.name in shared:
            mapped = shared[p.name]
        else:
            mapped = mapper.param(
                Param(c_name, p.type, p.has_default),
                f"{name}({p.name})",
                param_handles.get(p.name),
                fn.owner,
                is_nullable,
            )
        setup += mapped.setup
        local_names += mapped.locals
        if p.name in c_types:
            if len(mapped.c_params) != 1:
                raise GenError(
                    f"{name}: c_types cannot pin multi-value parameter {p.name}"
                )
            pinned = c_types.pop(p.name)
            derived = mapped.c_params[0].c_type
            bare = derived.removeprefix("const ")
            if bare.removesuffix("*") in mapper.handles and pinned not in (
                derived,
                bare,
            ):
                # A shipped function may take the handle without const; any
                # other type would reach the C++ call through the wrong rep.
                raise GenError(
                    f"{name}: c_types can only drop const from handle parameter "
                    f"{p.name} ({derived})"
                )
            mapped.c_params[0].c_type = pinned
            cxx_type = p.type.removeprefix("const ")
            if mapped.arg == c_name and pinned != cxx_type:
                mapped.arg = f"static_cast<{cxx_type}>({c_name})"
        if p.name in consumes:
            # The call takes ownership of the C++ object, so only the C
            # handle around it is left to free.
            consumes.remove(p.name)
            cls = re.fullmatch(r"([\w:]+) \*", p.type)
            owner = cls and mapper.handle_for(
                cls.group(1), f"{name}({p.name})", param_handles.get(p.name)
            )
            if not owner or owner.rep_kind()[0] != "pointer":
                raise GenError(
                    f"{name}: consumes needs a T* parameter whose handle holds a "
                    f"raw pointer, not {p.name} ({p.type})"
                )
            consumed.append(f"delete {c_name};")
        by_param[p.name] = mapped.c_params
        c_params.extend(mapped.c_params)
        args.append(mapped.arg)
    order = entry.get("param_order")
    if order is not None:
        # The C parameters follow this order instead of the C++ one. The
        # receiver stays first, and errptr or len still come last.
        if sorted(order) != sorted(by_param):
            raise GenError(
                f"{name}: param_order must list each C parameter once: "
                f"{sorted(by_param)}"
            )
        c_params = c_params[:1] + [c for key in order for c in by_param[key]]
    if bind:
        raise GenError(f"{name}: bind names unknown parameters {sorted(bind)}")
    if out_name is not None and new_handle is None:
        raise GenError(f"{name}: out_handle names unknown parameter {out_name}")
    if consumes:
        raise GenError(f"{name}: consumes names unknown parameters {consumes}")
    if nullable:
        raise GenError(f"{name}: nullable names unknown parameters {nullable}")
    if out_name is not None or entry.get("result") == "handle":
        local_names.append("result")

    access = "." if kind == "value" else "->"
    if "field" in entry:
        call = f"{handle.var}->rep[index].{fn.name}"
    else:
        call = f"{handle.var}->rep{access}{fn.name}({', '.join(args)})"

    result_policy = entry.get("result")
    body: list[str] = []
    ret = "void"
    result = mapper.resolve_result(fn)
    if consumed and result_policy != "handle":
        raise GenError(f"{name}: consumes needs `result: handle`")
    if "result_handle" in entry and new_handle is None and result_policy != "handle":
        raise GenError(f"{name}: result_handle needs `result: handle` or out_handle")
    if new_handle is not None or result_policy == "handle":
        if new_handle is not None:
            if result != "void" or result_policy is not None:
                raise GenError(f"{name}: out_handle needs a void C++ function")
            fill = [f"{call};"]
        else:
            new_handle = mapper.result_handle(
                fn.result, f"{name}(result)", entry.get("result_handle")
            )
            fill = [f"result->rep = {call};"]
        local = f"{new_handle.c_type}*"
        # A shipped function may hand the new handle back as const.
        ret = c_types.pop("return", local)
        if ret not in (local, f"const {local}"):
            raise GenError(f"{name}: c_types can only make the returned handle const")
        body += [f"{local} result = new {new_handle.c_type};", *setup, *fill]
        body += [*consumed, "return result;"]
        setup = []
    elif result_policy == "discard" or result == "void":
        if result_policy == "discard" and result == "void":
            raise GenError(f"{name}: result: discard on a void C++ function")
        body.append(f"{call};")
    elif result in STATUS_TYPES:
        c_params.append(CParam("char**", "errptr"))
        body.append(f"SaveError(errptr, {call});")
    elif result in STRING_RESULT_TYPES:
        if result_policy not in ("borrow", "slice", "copy", "c_str"):
            raise GenError(
                f"{name}: string result {fn.result!r} needs `result: borrow`, "
                "`result: slice`, `result: copy` or `result: c_str`"
            )
        local_type = STRING_RESULT_TYPES[result]
        if result_policy != "copy" and local_type == "std::string":
            raise GenError(f"{name}: cannot borrow a std::string returned by value")
        if result_policy == "c_str" and local_type != "const std::string&":
            raise GenError(f"{name}: `result: c_str` needs a std::string")
        if result_policy != "c_str":
            body.append(f"{local_type} s = {call};")
        if result_policy == "c_str":
            ret = "const char*"
            body.append(f"return {call}.c_str();")
        elif result_policy == "slice":
            ret = "rocksdb_slice_t"
            body += [
                "rocksdb_slice_t result;",
                "result.data = s.data();",
                "result.size = s.size();",
                "return result;",
            ]
        else:
            c_params.append(CParam("size_t*", "len"))
            body.append("*len = s.size();")
            if result_policy == "borrow":
                ret = "const char*"
                body.append("return s.data();")
            else:
                ret = "char*"
                body.append("return CopyString(s);")
    elif result_policy is not None:
        raise GenError(
            f"{name}: result policy {result_policy!r} does not apply to {fn.result!r}"
        )
    else:
        c_ret, enum = mapper.scalar(fn.result, f"{name}(result)", fn.owner)
        pinned = c_types.pop("return", None)
        if pinned is not None and pinned != fn.result:
            ret = pinned
            body.append(f"return static_cast<{ret}>({call});")
        elif pinned is not None:
            ret = pinned
            body.append(f"return {call};")
        elif enum is not None:
            # Widening an enum to its C integer is checked at compile time.
            ret = c_ret
            body.append(f"return ROCKSDB_NAMESPACE::lossless_cast<{ret}>({call});")
        else:
            ret = c_ret
            body.append(f"return {call};")
    if c_types:
        raise GenError(f"{name}: c_types names unknown parameters {sorted(c_types)}")
    clashes = sorted({p.name for p in c_params} & set(local_names))
    if clashes:
        raise GenError(f"{name}: C parameters {clashes} clash with generated locals")
    c_names = [p.name for p in c_params]
    repeated = sorted({n for n in c_names if c_names.count(n) > 1})
    if repeated:
        raise GenError(f"{name}: C parameters {repeated} appear twice")
    body = setup + body

    param_texts = [f"{p.c_type} {p.name}" for p in c_params] or ["void"]
    decl = _doc_lines(entry) + format_signature(
        f"extern ROCKSDB_LIBRARY_API {ret} {name}(", param_texts, ");"
    )
    impl = format_signature(
        f"{ret} {name}(", [t for t in param_texts if t != "void"], ") {"
    )
    impl += [f"  {line}" for line in body] + ["}"]
    return Rendered(name, decl, impl)


def render_lifecycle(handle: Handle) -> list[Rendered]:
    out = []
    if handle.create:
        decl = format_signature(
            f"extern ROCKSDB_LIBRARY_API {handle.c_type}* {handle.create}(",
            ["void"],
            ");",
        )
        impl = [
            f"{handle.c_type}* {handle.create}() {{",
            f"  return new {handle.c_type};",
            "}",
        ]
        out.append(Rendered(handle.create, decl, impl))
    if handle.destroy:
        name = handle.destroy["name"]
        const = "const " if handle.destroy.get("receiver_const", False) else ""
        param = f"{const}{handle.c_type}* {handle.var}"
        decl = format_signature(
            f"extern ROCKSDB_LIBRARY_API void {name}(", [param], ");"
        )
        body = []
        if handle.destroy["owns_rep"]:
            body.append(f"  delete {handle.var}->rep;")
        body.append(f"  delete {handle.var};")
        impl = format_signature(f"void {name}(", [param], ") {") + body + ["}"]
        out.append(Rendered(name, decl, impl))
    return out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

INCLUDE_RE = re.compile(r'^#include "(c_api_gen/[^"]+\.inc)"$', re.MULTILINE)


def existing_names() -> set[str]:
    """C functions declared or defined outside this generator's own outputs."""
    names: set[str] = set()
    for base in ("c_base.h", "c_base.cc"):
        texts = [(TEMPLATE_ROOT / base).read_text()]
        for rel in INCLUDE_RE.findall(texts[0]):
            path = ROOT / rel
            if Path(rel).name not in OWN_OUTPUTS and path.exists():
                texts.append(path.read_text())
        for text in texts:
            names |= set(extract_function_decls(text)) | set(
                extract_function_defs(text)
            )
    return names


def function_names(path: Path) -> set[str]:
    """The C functions generated for the handles in `path`."""
    _, handles = load_handles(path)
    names: set[str] = set()
    for handle in handles:
        if handle.create:
            names.add(handle.create)
        if handle.destroy:
            names.add(handle.destroy["name"])
        names.update(binding.name for binding in handle.bindings)
    return names


BANNER = [
    "//  Copyright (c) Meta Platforms, Inc. and affiliates.",
    "//  This source code is licensed under both the GPLv2 (found in the",
    "//  COPYING file in the root directory) and Apache 2.0 License",
    "//  (found in the LICENSE.Apache file in the root directory).",
    # "\x40" is "@" -- escaped so this generator source is not flagged as generated
    "// \x40generated",
    "// -----------------------------------------------------------------------------",
    "// Auto-generated by tools/c_api_gen/handle_bindings.py.",
    "// DO NOT EDIT THIS FILE DIRECTLY.",
    "// Source: tools/c_api_gen/handles.json",
    "// -----------------------------------------------------------------------------",
    "",
]


def generate(clang: str, path: Path = HANDLES_PATH) -> dict[Path, str]:
    headers, handles = load_handles(path)
    taken = existing_names()
    index = CxxIndex(clang, headers)
    try:
        index.prefetch(
            {
                h.element() if "field" in b.entry else h.rep_kind()[1]
                for h in handles
                for b in h.bindings
                if "count" not in b.entry
            }
        )
        mapper = TypeMapper(index, handles)
        structs = list(BANNER)
        header = list(BANNER)
        source = list(BANNER)
        errors: list[str] = []
        for handle in handles:
            if handle.struct == "generated":
                structs += [
                    f"struct {handle.c_type} {{",
                    f"  {handle.rep} rep;",
                    "};",
                    "",
                ]
            rendered: list[Rendered] = render_lifecycle(handle)
            for binding in handle.bindings:
                try:
                    rendered.append(render_binding(binding, index, mapper))
                except GenError as exc:
                    errors.append(str(exc))
            if not rendered:
                continue
            header += [f"/* {handle.c_type} */", ""]
            source += [f"/* {handle.c_type} */", ""]
            for r in rendered:
                if r.name in taken:
                    errors.append(
                        f"{r.name} is generated from {path.name} but also hand-written "
                        "or generated elsewhere; delete the other copy"
                    )
                header += r.decl + [""]
                source += r.impl + [""]
        if errors:
            raise GenError("\n".join(errors))
    finally:
        index.close()
    return {
        STRUCTS_OUT: "\n".join(structs).rstrip() + "\n",
        HEADER_OUT: "\n".join(header).rstrip() + "\n",
        SOURCE_OUT: "\n".join(source).rstrip() + "\n",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--clang", default="clang++", help="clang++ used to parse C++ headers"
    )
    parser.add_argument("--handles", type=Path, default=HANDLES_PATH)
    args = parser.parse_args()
    try:
        outputs = generate(args.clang, args.handles)
    except GenError as exc:
        for line in str(exc).splitlines():
            print(f"error: {line}", file=sys.stderr)
        return 1
    for out_path, content in outputs.items():
        write_if_changed(out_path, content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
