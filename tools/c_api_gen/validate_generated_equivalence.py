#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Validate generated C API wrappers against a reference handwritten revision.

This script compares the generated wrapper fragments currently included by
`include/rocksdb/c.h` and `db/c.cc` against the declarations/definitions in a
reference git revision, which defaults to `HEAD`.

The intent is to provide a guardrail when migrating handwritten wrappers to the
code generator:

- the generated declaration should match the old declaration shape
- the generated definition should match the old implementation shape
- known historical inconsistencies can be allowlisted with an explanation
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


@dataclass
class FunctionDecl:
    name: str
    text: str
    return_type: str
    param_types: list[str]


@dataclass
class FunctionDef:
    name: str
    signature: str
    return_type: str
    param_types: list[str]
    param_names: list[str]
    body: str


def strip_comments(text: str) -> str:
    text = re.sub(r"//.*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return text


def normalize_space(text: str) -> str:
    return " ".join(strip_comments(text).split())


TOKEN_RE = re.compile(
    r'"(?:\\.|[^"\\])*"'
    r"|'(?:\\.|[^'\\])*'"
    r"|->|::|==|!=|<=|>=|&&|\|\|"
    r"|[A-Za-z_][A-Za-z0-9_]*"
    r"|[0-9]+"
    r"|[{}()[\];,.*&<>!=+\-/%:?]"
)

TYPE_WORD_PREFIXES = {
    "const",
    "volatile",
    "unsigned",
    "signed",
    "short",
    "long",
}


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(strip_comments(text))


def map_identifiers(tokens: list[str], mapping: dict[str, str]) -> list[str]:
    # Rename parameters only. A token right after `.`, `->` or `::` is a member
    # or qualified name (e.g. the field in `opt->rep.move_files = move_files`),
    # never a parameter, so it must keep its spelling.
    return [
        tok if i > 0 and tokens[i - 1] in (".", "->", "::") else mapping.get(tok, tok)
        for i, tok in enumerate(tokens)
    ]


_DECLTYPE_ASSIGN_RE = re.compile(
    r"(?P<lhs>[A-Za-z_][\w.>\-]*)\s*=\s*static_cast\s*<\s*decltype\s*\(\s*"
    r"(?P=lhs)\s*\)\s*>\s*\((?P<rhs>[^;]*)\)\s*;"
)


def normalize_decltype_assignments(body: str) -> str:
    """Rewrite `x = static_cast<decltype(x)>(e);` as `x = e;`.

    Converting to the assignment target's own type is the conversion the
    assignment performs anyway, so the two spellings are equivalent whenever
    the plain assignment compiles.
    """
    return _DECLTYPE_ASSIGN_RE.sub(lambda m: f"{m.group('lhs')} = {m.group('rhs')};", body)


def normalize_return_casts(body: str, return_type: str) -> str:
    escaped_type = re.escape(normalize_space(return_type)).replace(r"\ ", r"\s+")
    pattern = re.compile(
        rf"return\s+static_cast\s*<\s*{escaped_type}\s*>\s*\((.*?)\)\s*;",
        re.S,
    )
    return pattern.sub(lambda m: f"return {m.group(1).strip()};", body)


def find_matching(text: str, start: int, open_ch: str, close_ch: str) -> int:
    assert text[start] == open_ch
    depth = 0
    i = start
    in_string = False
    string_quote = ""
    while i < len(text):
        ch = text[i]
        if in_string:
            if ch == "\\":
                i += 2
                continue
            if ch == string_quote:
                in_string = False
            i += 1
            continue
        if ch in ("'", '"'):
            in_string = True
            string_quote = ch
            i += 1
            continue
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError(f"Unmatched {open_ch} in text")


def find_signature_start(text: str, pos: int) -> int:
    start = text.rfind("\n", 0, pos)
    start = 0 if start < 0 else start + 1
    while start > 0:
        prev_end = start - 1
        prev_start = text.rfind("\n", 0, prev_end)
        prev_start = 0 if prev_start < 0 else prev_start + 1
        prev_line = text[prev_start:prev_end].strip()
        if not prev_line:
            break
        if prev_line.startswith("//"):
            break
        if prev_line.endswith(";") or prev_line.endswith("}") or prev_line.endswith("{"):
            break
        start = prev_start
    return start


def split_top_level(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    depth_paren = depth_angle = depth_bracket = 0
    start = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "<":
            depth_angle += 1
        elif ch == ">":
            depth_angle -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1
        elif (
            ch == delimiter
            and depth_paren == 0
            and depth_angle == 0
            and depth_bracket == 0
        ):
            parts.append(text[start:i].strip())
            start = i + 1
        i += 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def parse_param_decl(param: str) -> tuple[str, str]:
    param = normalize_space(param)
    if not param or param == "void":
        return "", ""
    if "(" in param and "*" in param:
        raise ValueError(f"Unsupported function pointer parameter: {param}")
    m = re.match(r"^(.*?)([A-Za-z_][A-Za-z0-9_]*)$", param)
    if not m:
        return param, ""
    type_part = m.group(1).strip()
    name = m.group(2)
    return normalize_space(type_part), name


def parse_param_type_only(param: str) -> str:
    param = normalize_space(param)
    if not param or param == "void":
        return ""
    if "(" in param and "*" in param:
        raise ValueError(f"Unsupported function pointer parameter: {param}")
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[&*\[\]]", param)
    if not tokens:
        return param
    if tokens[-1] in {"*", "&", "[", "]"}:
        return param
    if len(tokens) == 1:
        return param
    if len(tokens) == 2 and tokens[0] in TYPE_WORD_PREFIXES:
        return param
    # Treat the trailing identifier as the parameter name.
    m = re.match(r"^(.*?)([A-Za-z_][A-Za-z0-9_]*)$", param)
    if not m:
        return param
    return normalize_space(m.group(1).strip())


def parse_signature_shape(
    signature: str, name: str, require_param_names: bool
) -> tuple[str, list[str], list[str]]:
    signature = normalize_space(signature)
    signature = re.sub(r"\bextern\b", "", signature)
    signature = re.sub(r"\bROCKSDB_LIBRARY_API\b", "", signature)
    signature = normalize_space(signature)
    marker = f"{name}("
    pos = signature.find(marker)
    if pos < 0:
        raise ValueError(f"Could not find function name {name} in signature")
    return_type = normalize_space(signature[:pos].strip())
    params_start = pos + len(name)
    assert signature[params_start] == "("
    params_end = find_matching(signature, params_start, "(", ")")
    params_text = signature[params_start + 1 : params_end]
    param_types: list[str] = []
    param_names: list[str] = []
    if params_text.strip():
        for raw_param in split_top_level(params_text, ","):
            if require_param_names:
                ptype, pname = parse_param_decl(raw_param)
            else:
                ptype, pname = parse_param_type_only(raw_param), ""
            if ptype:
                param_types.append(ptype)
                param_names.append(pname)
    return return_type, param_types, param_names


def extract_function_decls(text: str) -> dict[str, FunctionDecl]:
    result: dict[str, FunctionDecl] = {}
    pos = 0
    while True:
        start = text.find("extern ROCKSDB_LIBRARY_API", pos)
        if start < 0:
            break
        semi = text.find(";", start)
        if semi < 0:
            break
        snippet = text[start : semi + 1]
        m = re.search(r"\b(rocksdb_[A-Za-z0-9_]+)\s*\(", snippet)
        if m:
            name = m.group(1)
            try:
                return_type, param_types, _ = parse_signature_shape(
                    snippet[:-1], name, require_param_names=False
                )
                result[name] = FunctionDecl(
                    name=name,
                    text=snippet,
                    return_type=return_type,
                    param_types=param_types,
                )
            except ValueError:
                pass
        pos = semi + 1
    return result


def extract_function_defs(text: str) -> dict[str, FunctionDef]:
    result: dict[str, FunctionDef] = {}
    pattern = re.compile(r"\b(rocksdb_[A-Za-z0-9_]+)\s*\(")
    pos = 0
    while True:
        m = pattern.search(text, pos)
        if not m:
            break
        name = m.group(1)
        paren_start = text.find("(", m.start(1))
        paren_end = find_matching(text, paren_start, "(", ")")
        body_start = paren_end + 1
        while body_start < len(text) and text[body_start].isspace():
            body_start += 1
        if body_start >= len(text) or text[body_start] != "{":
            pos = m.end(1)
            continue
        sig_start = find_signature_start(text, m.start(1))
        signature = text[sig_start:paren_end + 1]
        body_end = find_matching(text, body_start, "{", "}")
        body = text[body_start : body_end + 1]
        try:
            return_type, param_types, param_names = parse_signature_shape(
                signature, name, require_param_names=True
            )
            result[name] = FunctionDef(
                name=name,
                signature=signature,
                return_type=return_type,
                param_types=param_types,
                param_names=param_names,
                body=body,
            )
        except ValueError:
            pass
        pos = body_end + 1
    return result


def git_show(rev: str, path: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"{rev}:{path}"], cwd=ROOT, text=True
    )


def active_generated_paths(
    path: Path, include_prefix: str, root_prefix: str
) -> list[Path]:
    text = path.read_text()
    pattern = re.compile(rf'#include "{re.escape(include_prefix)}/(c_generated_[^"]+)"')
    return [ROOT / root_prefix / m.group(1) for m in pattern.finditer(text)]


def load_allowlist(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {entry["name"]: entry["reason"] for entry in data.get("allowlist", [])}


def compare_headers(
    generated_headers: list[Path],
    ref_header_text: str,
    allowlist: dict[str, str],
) -> list[str]:
    ref = extract_function_decls(ref_header_text)
    failures: list[str] = []
    for path in generated_headers:
        for name, generated in extract_function_decls(path.read_text()).items():
            if name in allowlist:
                continue
            ref_decl = ref.get(name)
            if ref_decl is None:
                failures.append(
                    f"[header missing] {name}: not declared in reference include/rocksdb/c.h"
                )
                continue
            if (
                generated.return_type != ref_decl.return_type
                or generated.param_types != ref_decl.param_types
            ):
                failures.append(
                    f"[header mismatch] {name}: generated "
                    f"{generated.return_type}({', '.join(generated.param_types)}) "
                    f"!= reference {ref_decl.return_type}"
                    f"({', '.join(ref_decl.param_types)})"
                )
    return failures


def compare_sources(
    generated_sources: list[Path],
    ref_source_text: str,
    allowlist: dict[str, str],
) -> list[str]:
    ref = extract_function_defs(ref_source_text)
    failures: list[str] = []
    for path in generated_sources:
        for name, generated in extract_function_defs(path.read_text()).items():
            if name in allowlist:
                continue
            ref_def = ref.get(name)
            if ref_def is None:
                failures.append(
                    f"[source missing] {name}: not defined in reference db/c.cc"
                )
                continue
            if (
                generated.return_type != ref_def.return_type
                or generated.param_types != ref_def.param_types
            ):
                failures.append(
                    f"[source signature mismatch] {name}: generated "
                    f"{generated.return_type}({', '.join(generated.param_types)}) "
                    f"!= reference {ref_def.return_type}"
                    f"({', '.join(ref_def.param_types)})"
                )
                continue
            mapping = {
                ref_name: gen_name
                for ref_name, gen_name in zip(ref_def.param_names, generated.param_names)
                if ref_name and gen_name
            }
            ref_body = normalize_decltype_assignments(
                normalize_return_casts(ref_def.body, generated.return_type)
            )
            gen_body = normalize_decltype_assignments(
                normalize_return_casts(generated.body, generated.return_type)
            )
            ref_tokens = map_identifiers(tokenize(ref_body), mapping)
            gen_tokens = tokenize(gen_body)
            if ref_tokens != gen_tokens:
                failures.append(f"[source body mismatch] {name}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ref",
        required=True,
        help=(
            "Reference git revision to compare the generated wrappers against. "
            "This must be a PRE-migration revision that still had the "
            "hand-written wrappers (e.g. a release tag from before codegen "
            "landed); comparing against a post-migration revision is a "
            "tautology because the reference is itself generated. This is a "
            "best-effort body-equivalence aid; the authoritative, ongoing "
            "backward-compatibility gate is check_api_compatibility.py."
        ),
    )
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=ROOT / "tools/c_api_gen/equivalence_allowlist.json",
        help="JSON allowlist for intentional historical differences",
    )
    parser.add_argument(
        "--generated-header",
        action="append",
        type=Path,
        default=[],
        help="Generated header fragment(s) to validate. Defaults to active includes.",
    )
    parser.add_argument(
        "--generated-source",
        action="append",
        type=Path,
        default=[],
        help="Generated source fragment(s) to validate. Defaults to active includes.",
    )
    args = parser.parse_args()

    generated_headers = (
        args.generated_header
        if args.generated_header
        else active_generated_paths(
            ROOT / "tools/c_api_gen/c_base.h", "c_api_gen", "c_api_gen"
        )
    )
    generated_sources = (
        args.generated_source
        if args.generated_source
        else active_generated_paths(
            ROOT / "tools/c_api_gen/c_base.cc", "c_api_gen", "c_api_gen"
        )
    )
    allowlist = load_allowlist(args.allowlist)

    ref_header_text = git_show(args.ref, "include/rocksdb/c.h")
    ref_source_text = git_show(args.ref, "db/c.cc")

    failures = []
    failures.extend(compare_headers(generated_headers, ref_header_text, allowlist))
    failures.extend(compare_sources(generated_sources, ref_source_text, allowlist))

    if failures:
        for name, reason in sorted(allowlist.items()):
            print(f"[allowlist] {name}: {reason}")
        for failure in failures:
            print(failure)
        return 1

    print(
        "Validated generated wrappers against "
        f"{args.ref}: {len(generated_headers)} header fragments, "
        f"{len(generated_sources)} source fragments."
    )
    if allowlist:
        print(f"Applied allowlist entries: {len(allowlist)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
