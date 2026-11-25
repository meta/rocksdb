#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Check that the C API keeps up with the C++ classes it wraps.

For every class listed in ``method_coverage.json``, each public method must
either be called somewhere in ``db/c.cc`` (generated or hand-written) or be
listed under that class's ``excluded`` map with a reason. A new public method
therefore fails the check until someone binds it or records why it has no C
binding, and an exclusion fails once it is stale (the method is gone or now
called), so the list of unbound methods stays accurate.

Coverage is per method name: once any overload of a name is called, adding
another overload does not trip the check.

A call counts for the static type of its receiver, so ``db->rep->Put(...)``
covers ``DB::Put`` and a call through a ``TransactionDB*`` covers
``TransactionDB``, not ``DB``. Methods a class inherits from another covered
class (or from a subclass of one) are checked on that class only, and the
bases in ``excluded_bases`` are not checked at all. Deprecated, deleted and
``TEST_`` methods are ignored.

Needs clang++, which parses both the public headers and db/c.cc.

Run:
    python3 tools/c_api_gen/check_method_coverage.py --clang clang++
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from handle_bindings import CxxIndex, GenError, ROOT, _iter_json_docs
from validate_generated_equivalence import strip_comments

MANIFEST = Path(__file__).resolve().parent / "method_coverage.json"
C_SOURCE = ROOT / "db" / "c.cc"

_QUALIFIERS_RE = re.compile(r"\b(?:const|volatile|struct|class)\b")
_NAMESPACE_RE = re.compile(r"^(?:::)?(?:rocksdb|ROCKSDB_NAMESPACE)::")
# The pointee may be followed by a deleter, which can itself be a template.
_SMART_POINTER_RE = re.compile(
    r"^std::(?:__\w+::)?(?:shared_ptr|unique_ptr)<\s*(?P<pointee>[^,<>]+?)\s*(?:,.*)?>$"
)
_TRANSPARENT_KINDS = {
    "ImplicitCastExpr",
    "ParenExpr",
    "MaterializeTemporaryExpr",
    "CXXBindTemporaryExpr",
    "ExprWithCleanups",
}


def class_name(qual_type: str) -> str:
    """`const rocksdb::DB *` -> `DB`."""
    t = _QUALIFIERS_RE.sub("", qual_type).replace("*", "").replace("&", "")
    t = " ".join(t.split())
    return _NAMESPACE_RE.sub("", t)


def _expr_type(node: dict) -> str:
    ty = node.get("type") or {}
    return ty.get("desugaredQualType") or ty.get("qualType", "")


def _referenced_name(node: dict) -> str:
    while node.get("kind") in _TRANSPARENT_KINDS and node.get("inner"):
        node = node["inner"][0]
    return (node.get("referencedDecl") or {}).get("name", "")


def _smart_pointee(node: dict) -> str | None:
    """Pointee class of `p->` or `p.get()` on a smart pointer `p`.

    clang types these calls as `element_type *` without desugaring it, so the
    class has to come from the smart pointer's own type. (`*p` is typed
    `element_type`, which clang does desugar.)
    """
    inner = node.get("inner") or []
    obj = None
    if node.get("kind") == "CXXOperatorCallExpr" and len(inner) == 2:
        if _referenced_name(inner[0]) == "operator->":
            obj = inner[1]
    elif node.get("kind") == "CXXMemberCallExpr" and inner:
        callee = inner[0]
        if callee.get("kind") == "MemberExpr" and callee.get("name") == "get":
            obj = (callee.get("inner") or [None])[0]
    if obj is None:
        return None
    # libstdc++ declares these in a base (`__shared_ptr_access`), so the
    # smart pointer itself sits under a derived-to-base cast.
    while True:
        m = _SMART_POINTER_RE.match(class_name(_expr_type(obj)))
        if m:
            return class_name(m.group("pointee"))
        if obj.get("kind") not in _TRANSPARENT_KINDS or not obj.get("inner"):
            return None
        obj = obj["inner"][0]


def receiver_class(base: dict) -> str:
    """Static class of a member call's object expression, as written.

    Implicit conversions are looked through, so a call to a base-class method
    counts for the class it was called on. A smart pointer's `->` and `get()`
    yield the pointee class; its own methods (`reset`) do not.
    """
    while base.get("kind") in _TRANSPARENT_KINDS and base.get("inner"):
        base = base["inner"][0]
    return _smart_pointee(base) or class_name(_expr_type(base))


_STATIC_CALL_RE = re.compile(
    r"\b(?:ROCKSDB_NAMESPACE::)?([A-Z]\w*)::([A-Za-z_]\w*)\s*\("
)


def called_methods(ast_docs: list[dict], source: str) -> dict[str, set[str]]:
    """Method names db/c.cc calls, keyed by receiver class.

    Member calls come from the AST. Static calls are spelled `Class::Name(`,
    so they are read from the source text.
    """
    called: dict[str, set[str]] = {}
    stack = list(ast_docs)
    while stack:
        node = stack.pop()
        if node.get("kind") == "MemberExpr" and node.get("inner"):
            cls = receiver_class(node["inner"][0])
            called.setdefault(cls, set()).add(node.get("name", ""))
        stack.extend(node.get("inner") or [])
    for cls, name in _STATIC_CALL_RE.findall(strip_comments(source)):
        called.setdefault(cls, set()).add(name)
    return called


def platform_defines() -> list[str]:
    defines = ["-DROCKSDB_PLATFORM_POSIX", "-DROCKSDB_LIB_IO_POSIX"]
    if sys.platform == "darwin":
        defines.append("-DOS_MACOSX")
    elif sys.platform.startswith("linux"):
        defines.append("-DOS_LINUX")
    return defines


_HEAD_END_RE = re.compile(r"[{;]")
_SCOPE_HEAD_RE = re.compile(r'^(?:extern\s+"C"|namespace\b|enum\b)')
_CLASS_HEAD_RE = re.compile(
    r"^(?:template\s*<[^{]*>\s*)?(?:class|struct|union)\s+(?P<name>[A-Za-z_]\w*)"
)
_FUNCTION_NAME_RE = re.compile(r"(?P<name>[A-Za-z_]\w*)\s*\(")
_VARIABLE_NAME_RE = re.compile(r"(?P<name>[A-Za-z_]\w*)\s*(?:\[[^\]]*\]\s*)?=")


def file_scope_names(source: str) -> list[str]:
    """Functions, classes and variables db/c.cc defines at file scope, other
    than the rocksdb_* ones.

    db/c.cc is clang-formatted, so each file-scope declaration starts in
    column 0 and runs to its first `{` or `;`. One that reaches `{` first is a
    definition, named by its class key, its parameter list or its initializer.
    A definition that cannot be named is an error rather than being skipped.
    """
    lines = strip_comments(source).splitlines()
    names = set()
    i = 0
    while i < len(lines):
        if not re.match(r"[A-Za-z_]", lines[i]):  # blank, indented, '#' or '}'
            i += 1
            continue
        head = lines[i]
        while not _HEAD_END_RE.search(head) and i + 1 < len(lines):
            i += 1
            head += " " + lines[i]
        i += 1
        end = _HEAD_END_RE.search(head)
        if end is None or end.group() == ";":
            continue
        head = " ".join(head[: end.start()].split())
        if _SCOPE_HEAD_RE.match(head):
            continue
        m = None if "(" in head else _CLASS_HEAD_RE.match(head)
        m = m or _FUNCTION_NAME_RE.search(head) or _VARIABLE_NAME_RE.search(head)
        if m is None:
            raise GenError(f"cannot name the file-scope definition {head!r}")
        names.add(m.group("name"))
    return sorted(n for n in names if not n.startswith("rocksdb_"))


def _dump(clang: str, dump_filter: str, path: Path) -> list[dict]:
    cmd = [
        clang,
        "-std=c++20",
        "-x",
        "c++",
        f"-I{ROOT}",
        f"-I{ROOT / 'include'}",
        *platform_defines(),
        "-w",
        "-fsyntax-only",
        "-Xclang",
        "-ast-dump=json",
        "-Xclang",
        f"-ast-dump-filter={dump_filter}",
        str(path),
    ]
    proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
    return list(_iter_json_docs(proc.stdout))


def source_ast(clang: str, path: Path, source: str) -> list[dict]:
    """File-scope declarations of `path` that can call C++ methods.

    Every C API function and handle struct is named rocksdb_*, and one dump
    finds them all. Each other file-scope definition gets a dump of its own.
    The filter is a substring match, so only declarations with exactly that
    name are kept, and a name clang does not know means the scan misread a
    definition.
    """
    others = file_scope_names(source)
    with ThreadPoolExecutor() as pool:
        dumps = list(pool.map(lambda f: _dump(clang, f, path), ["rocksdb_"] + others))
    docs = dumps[0]
    for name, dump in zip(others, dumps[1:]):
        found = [d for d in dump if d.get("name") == name]
        if not found:
            raise GenError(f"{path.name}: no declaration named {name} in the AST")
        docs += found
    return docs


@dataclass
class Method:
    name: str
    skip: bool  # deprecated, deleted or test-only


def _record_bases(node: dict) -> list[str]:
    return [
        class_name(b["type"]["qualType"])
        for b in node.get("bases", []) or []
        if b.get("access") == "public"
    ]


def is_a(
    index: CxxIndex, cls: str, targets: set[str], seen: set[str] | None = None
) -> bool:
    """Whether `cls` is one of `targets` or publicly derives from one."""
    if cls in targets:
        return True
    seen = seen if seen is not None else set()
    if cls in seen:
        return False
    seen.add(cls)
    try:
        node = index.record(cls)
    except GenError:
        return False
    return any(is_a(index, b, targets, seen) for b in _record_bases(node))


def _skipped(decl: dict) -> bool:
    name = decl.get("name", "")
    attrs = {i.get("kind") for i in decl.get("inner", []) or []}
    return (
        name.startswith("TEST_")
        or decl.get("explicitlyDeleted", False)
        or "DeprecatedAttr" in attrs
    )


def public_methods(
    index: CxxIndex, cls: str, covered: set[str], excluded_bases: set[str]
) -> dict[str, Method]:
    """Public methods to check on `cls`, including what it inherits from bases
    that are neither covered classes (or derived from one) nor excluded."""
    methods: dict[str, Method] = {}

    def visit(name: str, root: bool, seen: set[str]) -> None:
        if name in seen:
            return
        seen.add(name)
        if not root and (name in excluded_bases or is_a(index, name, covered)):
            return
        index.prefetch([name])
        node = index.record(name)
        access = "private" if node.get("tagUsed") == "class" else "public"
        for child in node.get("inner", []) or []:
            kind = child.get("kind")
            if kind == "AccessSpecDecl":
                access = child.get("access", access)
                continue
            if access != "public":
                continue
            decl = child
            if kind == "FunctionTemplateDecl":
                decl = next(
                    (
                        i
                        for i in child.get("inner", []) or []
                        if i.get("kind") == "CXXMethodDecl"
                    ),
                    None,
                )
                if decl is None:
                    continue
            elif kind != "CXXMethodDecl":
                continue
            method = decl.get("name", "")
            if not method or method.startswith("operator"):
                continue
            skip = _skipped(decl)
            prior = methods.get(method)
            # A name counts if any overload is checked.
            methods[method] = Method(method, skip and (prior is None or prior.skip))
        for base in _record_bases(node):
            visit(base, False, seen)

    visit(cls, True, set())
    return methods


def coverage_problems(
    cls: str,
    methods: dict[str, Method],
    called: set[str],
    excluded: dict[str, str],
    reasons: dict[str, str],
) -> list[str]:
    problems = []
    for name in sorted(methods):
        if methods[name].skip or name in called or name in excluded:
            continue
        problems.append(
            f"{cls}::{name} has no C binding. Bind it, or add it to "
            f'classes["{cls}"]["excluded"] in {MANIFEST.name} with a reason.'
        )
    for name, reason in sorted(excluded.items()):
        if reason not in reasons:
            problems.append(f'{cls}::{name}: unknown reason "{reason}".')
        if name not in methods or methods[name].skip:
            problems.append(
                f"{cls}::{name} is excluded but is not a checked public method; "
                "remove the exclusion."
            )
        elif name in called:
            problems.append(
                f"{cls}::{name} is excluded but db/c.cc now calls it; "
                "remove the exclusion."
            )
    return problems


def manifest_problems(manifest: dict) -> list[str]:
    """Reason keys that are unknown, or that no exclusion uses."""
    reasons = manifest["reasons"]
    used = set(manifest["excluded_bases"].values())
    problems = []
    for base, reason in sorted(manifest["excluded_bases"].items()):
        if reason not in reasons:
            problems.append(f'excluded_bases["{base}"]: unknown reason "{reason}".')
    for entry in manifest["classes"].values():
        used.update(entry.get("excluded", {}).values())
    for reason in sorted(set(reasons) - used):
        problems.append(f'reason "{reason}" is not used; remove it.')
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--clang", required=True, help="clang++ binary")
    parser.add_argument(
        "--report", action="store_true", help="print per-class coverage counts"
    )
    args = parser.parse_args()

    manifest = json.loads(MANIFEST.read_text())
    reasons: dict[str, str] = manifest["reasons"]
    excluded_bases = set(manifest["excluded_bases"])
    classes: dict[str, dict] = manifest["classes"]
    problems = manifest_problems(manifest)

    headers = sorted({c["header"] for c in classes.values()})
    index = CxxIndex(args.clang, headers)
    try:
        source = C_SOURCE.read_text()
        called = called_methods(source_ast(args.clang, C_SOURCE, source), source)
        index.prefetch(list(classes) + list(excluded_bases))
        covered = set(classes)
        for cls, entry in classes.items():
            methods = public_methods(index, cls, covered, excluded_bases)
            excluded = entry.get("excluded", {})
            problems += coverage_problems(
                cls, methods, called.get(cls, set()), excluded, reasons
            )
            if args.report:
                checked = [m for m in methods.values() if not m.skip]
                bound = [m for m in checked if m.name in called.get(cls, set())]
                print(
                    f"{cls}: {len(bound)} of {len(checked)} public methods bound, "
                    f"{len(excluded)} excluded"
                )
    finally:
        index.close()

    if problems:
        print("C API method coverage check failed:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print(f"C API method coverage: {len(classes)} classes checked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
