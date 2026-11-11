#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Unit tests for the C API generators in tools/c_api_gen/.

Run with `python3 tools/c_api_gen/c_api_gen_test.py`. Tests that parse C++
need clang++; set CLANG_CXX to enable them (they are skipped otherwise).
`make check-c-api-gen` runs this file with CLANG_CXX set.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


import auto_simple_bindings as asb  # noqa: E402
import validate_generated_equivalence as veq  # noqa: E402

CLANG = os.environ.get("CLANG_CXX") or shutil.which("clang++")


def _family(**overrides):
    fields = dict(
        struct_name="FakeOptions",
        header=asb.INCLUDE_ROOT / "options.h",
        c_receiver_type="rocksdb_fake_t*",
        c_receiver_name="opt",
        c_prefix="rocksdb_fake",
        mode="options",
        enum_types=("FakeEnum",),
    )
    fields.update(overrides)
    return asb.FamilyConfig(**fields)


class GenerateGroupsTest(unittest.TestCase):
    """Coverage policy: the generator owns every binding of a managed field."""

    def setUp(self):
        self._saved = {
            name: getattr(asb, name)
            for name in (
                "OUTPUT_GROUPS",
                "MANAGED_FAMILY_NAMES",
                "discover_fields",
                "collect_existing_bindings",
            )
        }
        self.tmp = tempfile.TemporaryDirectory()
        self.family = _family()
        group = asb.OutputGroup(
            name="Fake",
            header_out=Path(self.tmp.name) / "fake.h.inc",
            source_out=Path(self.tmp.name) / "fake.cc.inc",
            families=(self.family,),
        )
        asb.OUTPUT_GROUPS = (group,)
        asb.MANAGED_FAMILY_NAMES = frozenset({"FakeOptions"})
        asb.discover_fields = lambda family: [("level", "int"), ("on", "bool")]
        self.decls: set[str] = set()
        self.defs: set[str] = set()
        asb.collect_existing_bindings = lambda: (self.decls, self.defs)

    def tearDown(self):
        for name, value in self._saved.items():
            setattr(asb, name, value)
        self.tmp.cleanup()

    def _run(self, families=()):
        path = Path(self.tmp.name) / "blocklist.json"
        path.write_text(json.dumps({"schema_version": 1, "families": list(families)}))
        outputs, errors = asb.generate_groups(path)
        header = outputs[asb.OUTPUT_GROUPS[0].header_out]
        source = outputs[asb.OUTPUT_GROUPS[0].source_out]
        return header, source, errors

    def test_generates_missing_bindings(self):
        header, source, errors = self._run()
        self.assertEqual(errors, [])
        for name in (
            "rocksdb_fake_set_level",
            "rocksdb_fake_get_level",
            "rocksdb_fake_set_on",
            "rocksdb_fake_get_on",
        ):
            self.assertIn(name, header)
            self.assertIn(name, source)

    def test_hand_written_duplicate_is_an_error(self):
        self.decls |= {"rocksdb_fake_set_level"}
        self.defs |= {"rocksdb_fake_set_level"}
        _, source, errors = self._run()
        self.assertEqual(len(errors), 1)
        self.assertIn("hand-written binding for auto-managed field", errors[0])
        self.assertIn("rocksdb_fake_set_level", errors[0])
        self.assertNotIn("rocksdb_fake_set_level", source)

    def test_hand_written_declaration_alone_is_an_error(self):
        self.decls |= {"rocksdb_fake_get_level"}
        _, _, errors = self._run()
        self.assertEqual(len(errors), 1)
        self.assertIn("rocksdb_fake_get_level", errors[0])

    def test_function_scoped_manual_entry(self):
        self.decls |= {"rocksdb_fake_set_on"}
        self.defs |= {"rocksdb_fake_set_on"}
        entry = {
            "field": "on",
            "policy": "manual",
            "reason": "legacy NULL-tolerant setter",
            "functions": ["rocksdb_fake_set_on"],
        }
        header, _, errors = self._run([{"name": "FakeOptions", "entries": [entry]}])
        self.assertEqual(errors, [])
        self.assertNotIn("rocksdb_fake_set_on", header)
        self.assertIn("rocksdb_fake_get_on", header)

    def test_function_scoped_manual_entry_must_be_hand_written(self):
        entry = {
            "field": "on",
            "policy": "manual",
            "reason": "r",
            "functions": ["rocksdb_fake_set_on"],
        }
        _, _, errors = self._run([{"name": "FakeOptions", "entries": [entry]}])
        self.assertEqual(len(errors), 1)
        self.assertIn("not hand-written", errors[0])

    def test_stale_manual_function(self):
        entry = {
            "field": "on",
            "policy": "manual",
            "reason": "r",
            "functions": ["rocksdb_fake_set_typo"],
        }
        _, _, errors = self._run([{"name": "FakeOptions", "entries": [entry]}])
        self.assertTrue(any("stale blocklist function" in e for e in errors), errors)

    def test_whole_field_manual_entry_skips_field(self):
        self.decls |= {"rocksdb_fake_set_on", "rocksdb_fake_get_on"}
        self.defs |= {"rocksdb_fake_set_on", "rocksdb_fake_get_on"}
        entry = {"field": "on", "policy": "manual", "reason": "r"}
        header, _, errors = self._run([{"name": "FakeOptions", "entries": [entry]}])
        self.assertEqual(errors, [])
        self.assertNotIn("_on", header)


class AbiOverrideTest(unittest.TestCase):
    def setUp(self):
        self._saved = dict(asb.ABI_TYPE_OVERRIDES)

    def tearDown(self):
        asb.ABI_TYPE_OVERRIDES.clear()
        asb.ABI_TYPE_OVERRIDES.update(self._saved)

    def test_enum_getter_pin_casts_once(self):
        asb.ABI_TYPE_OVERRIDES["rocksdb_fake_get_mode"] = "unsigned char"
        specs = asb.build_specs(_family(), "mode", "FakeEnum")
        getter = next(s for s in specs if s["kind"] == "field_getter")
        self.assertEqual(getter["return_type"], "unsigned char")
        self.assertEqual(getter["expr"], "static_cast<unsigned char>(opt->rep.mode)")

    def test_setter_pin_converts_to_field_type(self):
        asb.ABI_TYPE_OVERRIDES["rocksdb_fake_set_size"] = "size_t"
        specs = asb.build_specs(_family(), "size", "uint64_t")
        setter = next(s for s in specs if s["kind"] == "field_setter")
        self.assertEqual(setter["value_type"], "size_t")
        self.assertEqual(
            setter["value_expr"], "static_cast<decltype(opt->rep.size)>(v)"
        )


@unittest.skipUnless(CLANG, "needs clang++ (set CLANG_CXX)")
class DiscoverFieldsTest(unittest.TestCase):
    def test_only_public_fields_are_discovered(self):
        source = """
            struct Base { int base_public; };
            struct S : public Base {
              int a;
             private:
              int hidden;
             public:
              bool b;
            };
            class C {
              int class_default_private;
             public:
              int visible;
            };
        """
        with tempfile.TemporaryDirectory(dir=asb.ROOT) as tmp:
            header = Path(tmp) / "fake.h"
            header.write_text(source)
            saved = asb._CLANG_BINARY
            asb._CLANG_BINARY = CLANG
            try:
                s_fields = asb.discover_fields(_family(struct_name="S", header=header))
                c_fields = asb.discover_fields(_family(struct_name="C", header=header))
            finally:
                asb._CLANG_BINARY = saved
        self.assertEqual(
            [name for name, _ in s_fields], ["base_public", "a", "b"]
        )
        self.assertEqual([name for name, _ in c_fields], ["visible"])


class EquivalenceNormalizationTest(unittest.TestCase):
    def test_member_names_are_not_renamed(self):
        tokens = veq.tokenize("opt->rep.move_files = move_files;")
        mapped = veq.map_identifiers(tokens, {"move_files": "v", "opt": "opt"})
        self.assertEqual(" ".join(mapped), "opt -> rep . move_files = v ;")

    def test_decltype_assignment_is_plain_assignment(self):
        body = "opt->rep.x = static_cast<decltype(opt->rep.x)>(v);"
        self.assertEqual(veq.normalize_decltype_assignments(body), "opt->rep.x = v;")

    def test_cast_to_other_target_is_kept(self):
        body = "opt->rep.x = static_cast<decltype(opt->rep.y)>(v);"
        self.assertEqual(veq.normalize_decltype_assignments(body), body)


if __name__ == "__main__":
    unittest.main()
