#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Generate round-trip (set -> get -> assert) C tests for the generated C API.

A large share of the auto-generated option getters/setters had no test
coverage. Rather than hand-writing hundreds of set/get assertions (and letting
coverage drift as the API grows), this tool derives the tests from the same
checked-in generated fragments that define the wrappers.

It parses the generated declaration fragments (c_api_gen/*.h.inc), pairs every
``rocksdb_<prefix>_set_<field>`` with its ``rocksdb_<prefix>_get_<field>``,
and emits, for each option object with a parameterless ``_create`` /
``_destroy``, a test that creates the object, sets a sentinel value, asserts
the getter returns it, and destroys the object.

Because the generated setters/getters are direct field assignments, a sentinel
round-trips reliably:
  - string getters (``const char*`` + ``size_t*``) -> set "test", compare bytes
  - malloc()ed string getters (``char*``) -> set "test", strcmp, free
  - scalars -> set 1 then 0 and assert the getter returns each.
Pairs of any other shape (a pointer or handle on either side) are left out.

1 and 0 are used (rather than a larger sentinel) because a bool field whose C
getter has the legacy ``int`` shape -- indistinguishable from a real scalar by
the C signature alone -- would collapse any value > 1 to 1. Using both 1 and 0
also exercises the setter in both directions.

No clang/libclang is required (it only reads the committed .inc files). The
output, c_api_gen/c_generated_roundtrip_tests.c.inc, is included by db/c_test.c.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GENERATED_ROOT = ROOT / "c_api_gen"
C_HEADER = ROOT / "include/rocksdb/c.h"
OUT = GENERATED_ROOT / "c_generated_roundtrip_tests.c.inc"

DECL_RE = re.compile(
    r"extern\s+ROCKSDB_LIBRARY_API\s+(.+?)\s*\b(rocksdb_[A-Za-z0-9_]+)\s*\(([^;]*?)\)\s*;",
    re.S,
)


class Pair:
    __slots__ = (
        "prefix",
        "field",
        "receiver",
        "getter",
        "setter",
        "ret",
        "value_type",
        "is_string",
    )

    def __init__(self, prefix: str, field: str, receiver: str) -> None:
        self.prefix = prefix
        self.field = field
        self.receiver = receiver
        self.getter: str | None = None
        self.setter: str | None = None
        self.ret: str | None = None
        self.value_type: str | None = None
        self.is_string = False

    def kind(self) -> str | None:
        """How the pair round-trips: "string", "strdup", "scalar", or None
        when a sentinel value cannot express it."""
        if self.is_string:
            return "string"
        if self.ret == "char*" and self.value_type == "const char*":
            return "strdup"
        if "*" in (self.ret or "") or "*" in (self.value_type or ""):
            return None
        return "scalar"


def _params(param_str: str) -> list[str]:
    return [p.strip() for p in param_str.split(",") if p.strip()]


def _first_param_type(param: str) -> str:
    # "rocksdb_options_t* opt" -> "rocksdb_options_t*"
    param = " ".join(param.split())
    return re.sub(r"\b[A-Za-z_]\w*$", "", param).strip()


def _split_role(name: str) -> tuple[str, str, str] | None:
    for role in ("_set_", "_get_"):
        idx = name.find(role)
        if idx != -1:
            return name[:idx], role.strip("_"), name[idx + len(role) :]
    return None


def collect_pairs() -> list[Pair]:
    return pairs_from_texts(
        inc.read_text() for inc in sorted(GENERATED_ROOT.glob("*.h.inc"))
    )


def pairs_from_texts(texts) -> list[Pair]:
    pairs: dict[tuple[str, str], Pair] = {}
    for text in texts:
        for m in DECL_RE.finditer(text):
            ret = " ".join(m.group(1).split())
            name = m.group(2)
            params = _params(m.group(3))
            split = _split_role(name)
            if split is None or not params:
                continue
            prefix, role, field = split
            receiver = _first_param_type(params[0])
            key = (prefix, field)
            pair = pairs.get(key)
            if pair is None:
                pair = Pair(prefix, field, receiver)
                pairs[key] = pair
            if role == "set":
                # Only simple scalar/string setters: (receiver, value).
                if len(params) == 2:
                    pair.setter = name
                    pair.value_type = _first_param_type(params[1])
            else:  # get
                if len(params) == 1 and ret != "void":
                    pair.getter = name
                    pair.ret = ret
                elif len(params) == 2 and ret == "const char*":
                    pair.getter = name
                    pair.ret = ret
                    pair.is_string = True
    return [p for p in pairs.values() if p.getter and p.setter and p.kind()]


def parameterless_creators(header_text: str) -> set[str]:
    """Prefixes whose <prefix>_create() takes no args and <prefix>_destroy exists."""
    creators: set[str] = set()
    for m in re.finditer(
        r"\b(rocksdb_[A-Za-z0-9_]+)_create\s*\(\s*(void)?\s*\)\s*;", header_text
    ):
        creators.add(m.group(1))
    destroyers = set(
        re.findall(r"\b(rocksdb_[A-Za-z0-9_]+)_destroy\s*\(", header_text)
    )
    return {p for p in creators if p in destroyers}


def emit() -> str:
    header_text = C_HEADER.read_text()
    declared = set(re.findall(r"\b(rocksdb_[A-Za-z0-9_]+)\s*\(", header_text))
    creators = parameterless_creators(header_text)

    by_prefix: dict[str, list[Pair]] = {}
    for pair in collect_pairs():
        if pair.prefix not in creators:
            continue
        if pair.getter not in declared or pair.setter not in declared:
            continue
        by_prefix.setdefault(pair.prefix, []).append(pair)

    lines: list[str] = []
    lines.append("//  Copyright (c) Meta Platforms, Inc. and affiliates.")
    lines.append("//  This source code is licensed under both the GPLv2 (found in the")
    lines.append("//  COPYING file in the root directory) and Apache 2.0 License")
    lines.append("//  (found in the LICENSE.Apache file in the root directory).")
    # "\x40" is "@" -- escaped so this generator source is not flagged as generated
    lines.append("// \x40generated")
    lines.append(
        "// -----------------------------------------------------------------------------"
    )
    lines.append("// Auto-generated by tools/c_api_gen/gen_roundtrip_tests.py.")
    lines.append("// DO NOT EDIT THIS FILE DIRECTLY.")
    lines.append(
        "// Round-trip (set -> get -> assert) coverage for generated option wrappers."
    )
    lines.append("// Rerun `make regen-c-api`")
    lines.append(
        "// -----------------------------------------------------------------------------"
    )
    lines.append("")

    test_fns: list[str] = []
    for prefix in sorted(by_prefix):
        fn = f"rocksdb_roundtrip_test_{prefix}"
        test_fns.append(fn)
        lines += emit_test(fn, prefix, by_prefix[prefix])

    lines.append("static void rocksdb_run_generated_roundtrip_tests(void) {")
    for fn in test_fns:
        lines.append(f"  {fn}();")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def emit_test(fn: str, prefix: str, pairs: list[Pair]) -> list[str]:
    pairs = sorted(pairs, key=lambda p: p.field)
    receiver = pairs[0].receiver
    lines = [f"static void {fn}(void) {{", f"  {receiver} obj = {prefix}_create();"]
    for pair in pairs:
        kind = pair.kind()
        if kind == "string":
            lines.append(f'  {pair.setter}(obj, "test");')
            lines.append("  {")
            lines.append("    size_t rt_len = 0;")
            lines.append(f"    const char* rt_v = {pair.getter}(obj, &rt_len);")
            lines.append(
                "    CheckCondition(rt_len == 4 && rt_v != NULL && "
                'memcmp(rt_v, "test", 4) == 0);'
            )
            lines.append("  }")
        elif kind == "strdup":
            lines.append(f'  {pair.setter}(obj, "test");')
            lines.append("  {")
            lines.append(f"    char* rt_v = {pair.getter}(obj);")
            lines.append(
                '    CheckCondition(rt_v != NULL && strcmp(rt_v, "test") == 0);'
            )
            lines.append("    rocksdb_free(rt_v);")
            lines.append("  }")
        else:
            # Use 1 and 0 (both directions). These round-trip through any
            # numeric/enum field AND through bool fields -- a larger value
            # would be collapsed to 1 by a bool field whose C getter is the
            # legacy `int` shape (e.g. rocksdb_options_get_use_fsync), so we
            # cannot tell bool from a plain scalar by the C signature alone.
            for value in ("1", "0"):
                lines.append(f"  {pair.setter}(obj, {value});")
                lines.append(f"  CheckCondition({pair.getter}(obj) == {value});")
    lines.append(f"  {prefix}_destroy(obj);")
    lines.append("}")
    lines.append("")
    return lines


def main() -> int:
    OUT.write_text(emit())
    print(f"Generated {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
