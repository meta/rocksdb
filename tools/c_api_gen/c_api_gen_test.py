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
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


import auto_simple_bindings as asb  # noqa: E402
import validate_generated_equivalence as veq  # noqa: E402
import handle_bindings as hb  # noqa: E402
import check_method_coverage as cmc  # noqa: E402

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

    def test_function_scoped_manual_entry_must_exist(self):
        entry = {
            "field": "on",
            "policy": "manual",
            "reason": "r",
            "functions": ["rocksdb_fake_set_on"],
        }
        _, _, errors = self._run([{"name": "FakeOptions", "entries": [entry]}])
        self.assertEqual(len(errors), 1)
        self.assertIn("in neither", errors[0])

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


class ExistingBindingsTest(unittest.TestCase):
    """auto_simple_bindings.py runs before handle_bindings.py, so the handle
    fragments on disk can be from the previous run."""

    def setUp(self):
        self._saved = (asb.ROOT, asb.TEMPLATE_ROOT, hb.HANDLES_PATH)
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        asb.ROOT = root
        asb.TEMPLATE_ROOT = root / "tools/c_api_gen"
        hb.HANDLES_PATH = asb.TEMPLATE_ROOT / "handles.json"
        asb.TEMPLATE_ROOT.mkdir(parents=True)
        (root / "c_api_gen").mkdir()
        for base, ext, body in (("c_base.h", "h", ";"), ("c_base.cc", "cc", " {}")):
            (asb.TEMPLATE_ROOT / base).write_text(
                f'#include "c_api_gen/c_generated_handles.{ext}.inc"\n'
                f"void rocksdb_base_fn(void){body}\n"
            )
            # Written before rocksdb_thing_moved was added to handles.json and
            # rocksdb_thing_dropped was taken out.
            (root / f"c_api_gen/c_generated_handles.{ext}.inc").write_text(
                f"void rocksdb_thing_dropped(void){body}\n"
            )
        thing = {
            "c_type": "rocksdb_thing_t",
            "rep": "Thing",
            "struct": "generated",
            "var": "t",
            "create": "rocksdb_thing_create",
            "destroy": {"name": "rocksdb_thing_destroy", "owns_rep": False},
            "methods": [{"name": "rocksdb_thing_moved", "method": "Moved"}],
        }
        hb.HANDLES_PATH.write_text(
            json.dumps({"schema_version": hb.SCHEMA_VERSION, "handles": [thing]})
        )

    def tearDown(self):
        asb.ROOT, asb.TEMPLATE_ROOT, hb.HANDLES_PATH = self._saved
        self.tmp.cleanup()

    def test_handle_functions_come_from_handles_json(self):
        decls, defs = asb.collect_existing_bindings()
        expected = {
            "rocksdb_base_fn",
            "rocksdb_thing_create",
            "rocksdb_thing_destroy",
            "rocksdb_thing_moved",
        }
        self.assertEqual(decls, expected)
        self.assertEqual(defs, expected)

    def test_function_names_match_the_generated_header(self):
        handles_path = self._saved[2]
        generated = veq.extract_function_decls(hb.HEADER_OUT.read_text())
        self.assertEqual(hb.function_names(handles_path), set(generated))


class FieldSpecTest(unittest.TestCase):
    def test_metadata_getter_name_override(self):
        family = _family(
            mode="metadata",
            c_prefix="rocksdb_fake_t_get",
            getter_name_overrides=(("bottommost", "rocksdb_fake_t_is_bottommost"),),
        )
        (spec,) = asb.build_specs(family, "bottommost", "bool")
        self.assertEqual(spec["name"], "rocksdb_fake_t_is_bottommost")
        (spec,) = asb.build_specs(family, "level", "int")
        self.assertEqual(spec["name"], "rocksdb_fake_t_get_level")

    def test_int32_field(self):
        setter, getter = asb.build_specs(_family(), "seed", "int32_t")
        self.assertEqual(setter["value_type"], "int32_t")
        self.assertEqual(getter["return_type"], "int32_t")


class AbiOverrideTest(unittest.TestCase):
    def setUp(self):
        self._saved = dict(asb.ABI_TYPE_OVERRIDES)

    def tearDown(self):
        asb.ABI_TYPE_OVERRIDES.clear()
        asb.ABI_TYPE_OVERRIDES.update(self._saved)

    def test_enum_getter_widens_with_lossless_cast(self):
        specs = asb.build_specs(_family(), "mode", "FakeEnum")
        getter = next(s for s in specs if s["kind"] == "field_getter")
        self.assertEqual(getter["return_type"], "int")
        self.assertEqual(
            getter["expr"], "ROCKSDB_NAMESPACE::lossless_cast<int>(opt->rep.mode)"
        )

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
    SOURCE = """
        namespace rocksdb {
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
        struct Outer {
          struct S { int nested_only; };
          struct Inner { int inner_field; };
        };
        // Matches the dump filter for `Inner` by prefix but only nests one.
        struct Inner2 {
          struct Inner { int wrong; };
        };
        }  // namespace rocksdb
    """

    def _fields(self, struct_name):
        with tempfile.TemporaryDirectory(dir=asb.ROOT) as tmp:
            header = Path(tmp) / "fake.h"
            header.write_text(self.SOURCE)
            saved = asb._CLANG_BINARY
            asb._CLANG_BINARY = CLANG
            try:
                family = _family(struct_name=struct_name, header=header)
                return [name for name, _ in asb.discover_fields(family)]
            finally:
                asb._CLANG_BINARY = saved

    def test_only_public_fields_are_discovered(self):
        self.assertEqual(self._fields("S"), ["base_public", "a", "b"])
        self.assertEqual(self._fields("C"), ["visible"])

    def test_nested_struct_family(self):
        self.assertEqual(self._fields("Outer::Inner"), ["inner_field"])

    def test_namespace_struct_is_not_a_nested_namesake(self):
        with self.assertRaisesRegex(
            RuntimeError, "Could not find AST definition for Inner"
        ):
            self._fields("Inner")


def _flat(text: str) -> str:
    return " ".join(text.split())


class FakeIndex:
    """Stands in for handle_bindings.CxxIndex without parsing C++."""

    def __init__(self, methods=None, enums=(), typedefs=None):
        self._methods = methods or {}
        self._enums = set(enums)
        self._typedefs = typedefs or {}

    def methods(self, cls, name):
        return list(self._methods.get((cls, name), []))

    def is_enum(self, name):
        return name in self._enums

    def typedef_target(self, name):
        return self._typedefs.get(name)

    def prefetch(self, names):
        pass

    def close(self):
        pass


def _fn(name, params=(), result="void", is_const=False, owner="Thing"):
    return hb.Function(
        owner,
        name,
        [hb.Param(*p) if len(p) == 3 else hb.Param(p[0], p[1], False) for p in params],
        result,
        is_const,
    )


THING = hb.Handle("rocksdb_thing_t", "Thing*", "generated", "t", None, None)
OTHER_HANDLES = [
    hb.Handle("rocksdb_val_t", "Val", "generated", "v", None, None),
    hb.Handle("rocksdb_ptr_t", "Ptr*", "generated", "p", None, None),
    hb.Handle("rocksdb_sh_t", "std::shared_ptr<Sh>", "generated", "s", None, None),
]


def _render(entry, *fns, enums=(), typedefs=None, handle=THING):
    index = FakeIndex({(handle.rep_kind()[1], fns[0].name): list(fns)}, enums, typedefs)
    mapper = hb.TypeMapper(index, [handle] + OTHER_HANDLES)
    full = {"name": "rocksdb_thing_x", "method": fns[0].name, **entry}
    rendered = hb.render_binding(hb.Binding(handle, full), index, mapper)
    return _flat("\n".join(rendered.decl)), _flat("\n".join(rendered.impl))


class HandleRenderTest(unittest.TestCase):
    """C conventions applied by handle_bindings.render_binding."""

    def test_status_slice_and_bool(self):
        fn = _fn("Put", [("key", "const Slice &"), ("sync", "bool")], "Status")
        decl, impl = _render({}, fn)
        self.assertIn(
            "void rocksdb_thing_x( rocksdb_thing_t* t, const char* key, "
            "size_t key_len, unsigned char sync, char** errptr) {",
            impl,
        )
        self.assertIn(
            "SaveError(errptr, t->rep->Put(Slice(key, key_len), sync));", impl
        )
        self.assertTrue(
            decl.startswith("extern ROCKSDB_LIBRARY_API void rocksdb_thing_x(")
        )

    def test_const_method_gives_const_receiver(self):
        decl, _ = _render({}, _fn("Size", [], "uint64_t", is_const=True))
        self.assertIn("(const rocksdb_thing_t* t)", decl)
        decl, _ = _render(
            {"receiver_const": False}, _fn("Size", [], "uint64_t", is_const=True)
        )
        self.assertIn("(rocksdb_thing_t* t)", decl)

    def test_string_results(self):
        fn = _fn("Name", [], "const std::string &", is_const=True)
        _, borrow = _render({"result": "borrow"}, fn)
        self.assertIn(
            "const char* rocksdb_thing_x(const rocksdb_thing_t* t, size_t* len) { "
            "const std::string& s = t->rep->Name(); *len = s.size(); return s.data(); }",
            borrow,
        )
        _, copy = _render({"result": "copy"}, fn)
        self.assertIn("char* rocksdb_thing_x(", copy)
        self.assertIn("return CopyString(s);", copy)
        _, sl = _render({"result": "slice"}, _fn("key", [], "Slice", is_const=True))
        self.assertIn(
            "rocksdb_slice_t rocksdb_thing_x(const rocksdb_thing_t* t) { Slice s = "
            "t->rep->key(); rocksdb_slice_t result; result.data = s.data(); "
            "result.size = s.size(); return result; }",
            sl,
        )
        with self.assertRaisesRegex(hb.GenError, "needs `result: borrow`"):
            _render({}, fn)
        with self.assertRaisesRegex(hb.GenError, "cannot borrow"):
            _render({"result": "borrow"}, _fn("Name", [], "std::string"))

    def test_typedef_of_string_result(self):
        fn = _fn("GetName", [], "TransactionName", is_const=True)
        _, impl = _render(
            {"result": "copy"}, fn, typedefs={"TransactionName": "std::string"}
        )
        self.assertIn("std::string s = t->rep->GetName();", impl)

    def test_discard(self):
        fn = _fn("Delete", [("key", "const Slice &")], "Status")
        _, impl = _render({"result": "discard"}, fn)
        self.assertIn("{ t->rep->Delete(Slice(key, key_len)); }", impl)
        self.assertNotIn("errptr", impl)
        with self.assertRaisesRegex(hb.GenError, "discard on a void"):
            _render({"result": "discard"}, _fn("Clear"))

    def test_enums(self):
        fn = _fn("SetMode", [("mode", "Mode")])
        _, impl = _render({}, fn, enums={"Mode"})
        self.assertIn("int mode", impl)
        self.assertIn("t->rep->SetMode(static_cast<Mode>(mode));", impl)
        _, impl = _render({}, _fn("GetMode", [], "Mode"), enums={"Mode"})
        self.assertIn(
            "return ROCKSDB_NAMESPACE::lossless_cast<int>(t->rep->GetMode());", impl
        )

    def test_nested_enum_resolves_in_owner_scope(self):
        fn = _fn("GetMode", [], "IndexMode", owner="Opts")
        _, impl = _render({}, fn, enums={"Opts::IndexMode"})
        self.assertIn("lossless_cast<int>", impl)
        fn = _fn("SetMode", [("m", "IndexMode")], owner="Opts")
        _, impl = _render({}, fn, enums={"Opts::IndexMode"})
        self.assertIn("static_cast<Opts::IndexMode>(m)", impl)

    def test_pins_cast_only_when_types_differ(self):
        fn = _fn("Set", [("on", "bool")], "bool")
        _, impl = _render({"c_types": {"on": "bool", "return": "bool"}}, fn)
        self.assertIn(
            "bool rocksdb_thing_x(rocksdb_thing_t* t, bool on) { return t->rep->Set(on); }",
            impl,
        )
        fn = _fn("Set", [("n", "uint64_t")], "uint32_t")
        _, impl = _render({"c_types": {"n": "int", "return": "int"}}, fn)
        self.assertIn(
            "return static_cast<int>(t->rep->Set(static_cast<uint64_t>(n)));", impl
        )

    def test_bind_and_arity(self):
        fn = _fn("SetThreads", [("n", "int", False), ("pri", "Priority", True)])
        _, impl = _render({"bind": {"pri": "Env::BOTTOM"}}, fn, enums={"Priority"})
        self.assertIn("(rocksdb_thing_t* t, int n)", impl)
        self.assertIn("t->rep->SetThreads(n, Env::BOTTOM);", impl)
        _, impl = _render({"arity": 1}, fn, enums={"Priority"})
        self.assertIn("t->rep->SetThreads(n);", impl)
        with self.assertRaisesRegex(hb.GenError, "past arity"):
            _render(
                {"arity": 1, "bind": {"pri": "Env::BOTTOM"}}, fn, enums={"Priority"}
            )
        with self.assertRaisesRegex(hb.GenError, "has no default"):
            _render({"arity": 0}, fn, enums={"Priority"})

    def test_unnamed_parameter_needs_param_names(self):
        fn = _fn("FlushWAL", [("", "bool")], "Status")
        with self.assertRaisesRegex(hb.GenError, "unnamed"):
            _render({}, fn)
        _, impl = _render({"param_names": {"0": "sync"}}, fn)
        self.assertIn("unsigned char sync", impl)
        self.assertIn("t->rep->FlushWAL(sync)", impl)
        with self.assertRaisesRegex(hb.GenError, "unknown parameters"):
            _render({"param_names": {"1": "x"}}, fn)

    def test_overload_selection_and_base_names(self):
        derived = _fn(
            "DeleteRange", [("", "const Slice &"), ("", "const Slice &")], "Status"
        )
        base = _fn(
            "DeleteRange",
            [("begin", "const Slice &"), ("end", "const Slice &")],
            "Status",
            owner="Base",
        )
        cf = _fn(
            "DeleteRange",
            [("cf", "Ptr *"), ("begin", "const Slice &"), ("end", "const Slice &")],
            "Status",
        )
        with self.assertRaisesRegex(hb.GenError, "overloaded"):
            _render({}, derived, cf, base)
        _, impl = _render(
            {"overload": ["const Slice &", "const Slice &"]}, derived, cf, base
        )
        self.assertIn(
            "t->rep->DeleteRange(Slice(begin, begin_len), Slice(end, end_len))", impl
        )
        _, impl = _render(
            {"overload": ["Ptr *", "const Slice &", "const Slice &"]}, derived, cf, base
        )
        self.assertIn("rocksdb_ptr_t* cf", impl)
        self.assertIn("t->rep->DeleteRange(cf->rep,", impl)

    def test_handle_parameter_kinds(self):
        cases = [
            ("const Val &", "const rocksdb_val_t* x", "x->rep"),
            ("Val *", "rocksdb_val_t* x", "&x->rep"),
            ("const Ptr &", "const rocksdb_ptr_t* x", "*x->rep"),
            ("Ptr *", "rocksdb_ptr_t* x", "x->rep"),
            ("const std::shared_ptr<Sh> &", "rocksdb_sh_t* x", "x->rep"),
            ("Sh *", "rocksdb_sh_t* x", "x->rep.get()"),
        ]
        for cxx, c_param, arg in cases:
            with self.subTest(cxx=cxx):
                _, impl = _render({}, _fn("Use", [("x", cxx)]))
                self.assertIn(c_param, impl)
                self.assertIn(f"t->rep->Use({arg});", impl)

    def test_value_rep_uses_member_access(self):
        handle = hb.Handle("rocksdb_val_t", "Val", "generated", "v", None, None)
        _, impl = _render({}, _fn("Count", [], "int", True, owner="Val"), handle=handle)
        self.assertIn("return v->rep.Count();", impl)


class HandleLifecycleTest(unittest.TestCase):
    def test_create_and_destroy(self):
        handle = hb.Handle(
            "rocksdb_thing_t",
            "Thing*",
            "generated",
            "t",
            "rocksdb_thing_create",
            {"name": "rocksdb_thing_destroy", "owns_rep": True},
        )
        impls = [_flat("\n".join(r.impl)) for r in hb.render_lifecycle(handle)]
        self.assertEqual(
            impls,
            [
                "rocksdb_thing_t* rocksdb_thing_create() { return new rocksdb_thing_t; }",
                "void rocksdb_thing_destroy(rocksdb_thing_t* t) { delete t->rep; delete t; }",
            ],
        )

    def test_const_destroy_without_owned_rep(self):
        handle = hb.Handle(
            "rocksdb_thing_t",
            "Thing",
            "generated",
            "t",
            None,
            {
                "name": "rocksdb_thing_destroy",
                "owns_rep": False,
                "receiver_const": True,
            },
        )
        (r,) = hb.render_lifecycle(handle)
        self.assertEqual(
            _flat("\n".join(r.impl)),
            "void rocksdb_thing_destroy(const rocksdb_thing_t* t) { delete t; }",
        )


class HandleSchemaTest(unittest.TestCase):
    def _load(self, handles):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "handles.json"
            path.write_text(
                json.dumps({"schema_version": 1, "headers": [], "handles": handles})
            )
            return hb.load_handles(path)

    def _handle(self, **kw):
        h = {
            "c_type": "rocksdb_thing_t",
            "rep": "Thing*",
            "struct": "generated",
            "var": "t",
        }
        h.update(kw)
        return h

    def test_valid(self):
        _, handles = self._load(
            [self._handle(methods=[{"name": "rocksdb_thing_x", "method": "X"}])]
        )
        self.assertEqual([b.name for b in handles[0].bindings], ["rocksdb_thing_x"])

    def test_errors(self):
        cases = [
            ([self._handle(bogus=1)], "unknown keys"),
            ([self._handle(struct="maybe")], "struct must be"),
            (
                [
                    self._handle(
                        methods=[{"name": "rocksdb_a", "method": "A", "extra": 1}]
                    )
                ],
                "unknown keys",
            ),
            (
                [self._handle(methods=[{"name": "rocksdb_a", "method": "A"}] * 2)],
                "duplicate C function",
            ),
            ([self._handle(), self._handle()], "duplicate handle"),
            (
                [
                    self._handle(
                        rep="Thing", destroy={"name": "rocksdb_d", "owns_rep": True}
                    )
                ],
                "owns_rep only",
            ),
            ([self._handle(destroy={"name": "rocksdb_d"})], "destroy takes"),
            (
                [
                    self._handle(
                        destroy={
                            "name": "rocksdb_d",
                            "owns_rep": False,
                            "receiver_const": 1,
                        }
                    )
                ],
                "destroy takes",
            ),
            ([self._handle(create="not_rocksdb")], "invalid C function name"),
        ]
        for handles, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(hb.GenError, message):
                    self._load(handles)


class HandleOwnershipTest(unittest.TestCase):
    """A generated name that also exists elsewhere fails generation."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=hb.ROOT)
        root = Path(self.tmp.name)
        (root / "c_base.h").write_text(
            "extern ROCKSDB_LIBRARY_API void rocksdb_declared(rocksdb_thing_t* t);\n"
        )
        (root / "c_base.cc").write_text(
            "void rocksdb_defined(rocksdb_thing_t* t) { rocksdb_only_called(t); }\n"
        )
        self.saved = hb.TEMPLATE_ROOT
        hb.TEMPLATE_ROOT = root

    def tearDown(self):
        hb.TEMPLATE_ROOT = self.saved
        self.tmp.cleanup()

    def test_calls_are_not_definitions(self):
        self.assertEqual(hb.existing_names(), {"rocksdb_declared", "rocksdb_defined"})

    def test_conflict_fails_generation(self):
        path = Path(self.tmp.name) / "handles.json"
        methods = [
            {"name": n, "method": "Clear"}
            for n in ("rocksdb_defined", "rocksdb_only_called")
        ]
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "headers": [],
                    "handles": [
                        {
                            "c_type": "rocksdb_thing_t",
                            "rep": "Thing*",
                            "struct": "generated",
                            "var": "t",
                            "methods": methods,
                        }
                    ],
                }
            )
        )
        saved = hb.CxxIndex
        hb.CxxIndex = lambda clang, headers: FakeIndex(
            {("Thing", "Clear"): [_fn("Clear")]}
        )
        try:
            with self.assertRaises(hb.GenError) as ctx:
                hb.generate("unused-clang", path)
        finally:
            hb.CxxIndex = saved
        self.assertIn(
            "rocksdb_defined is generated from handles.json but also",
            str(ctx.exception),
        )
        self.assertNotIn("rocksdb_only_called", str(ctx.exception))


@unittest.skipUnless(CLANG, "needs clang++ (set CLANG_CXX)")
class CxxIndexTest(unittest.TestCase):
    """C++ lookup rules handle_bindings relies on."""

    SOURCE = """
        #include <cstdint>
        namespace rocksdb {
        struct Slice {};
        class Base {
         public:
          virtual void Seek(const Slice& target) = 0;
          virtual int Status() const = 0;
          virtual void Put(const Slice& key, const Slice& value) = 0;
          void Reexported(int a);
        };
        class Hider : public Base {
         public:
          void Seek(const Slice& target) override;
          void Put(const Slice& /*key*/, const Slice& /*value*/) override;
          using Base::Reexported;
          void Reexported(int a, int b);
         private:
          void Status(int hidden);
        };
        struct Holder {
          class Hider {
           public:
            void Seek(const Slice& t, const char* memtable_key);
          };
          enum IndexMode : char { kA };
          IndexMode GetMode() const;
        };
        // Matches the dump filter for `Mode` by prefix but only nests a Mode.
        struct Mode2 {
          enum Mode : char { kB };
        };
        }  // namespace rocksdb
    """

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(dir=hb.ROOT)
        header = Path(cls.tmp.name) / "fake.h"
        header.write_text(cls.SOURCE)
        cls.index = hb.CxxIndex(CLANG, [str(header.relative_to(hb.ROOT))])

    @classmethod
    def tearDownClass(cls):
        cls.index.close()
        cls.tmp.cleanup()

    def _params(self, cls, name):
        return [[p.type for p in f.params] for f in self.index.methods(cls, name)]

    def test_namespace_class_not_nested_namesake(self):
        self.assertEqual(self._params("Hider", "Seek"), [["const Slice &"]])

    def test_derived_declaration_hides_base_overloads(self):
        # Hider declares a private Status(int): Base::Status() is hidden.
        self.assertEqual(self._params("Hider", "Status"), [])

    def test_using_reexports_base_overloads(self):
        self.assertEqual(self._params("Hider", "Reexported"), [["int", "int"], ["int"]])

    def test_override_takes_base_parameter_names(self):
        fn = hb._select("x", self.index.methods("Hider", "Put"), {}, "Put")
        self.assertEqual(fn.owner, "Hider")
        self.assertEqual([p.name for p in fn.params], ["key", "value"])

    def test_nested_enum(self):
        self.assertTrue(self.index.is_enum("Holder::IndexMode"))
        self.assertFalse(self.index.is_enum("IndexMode"))
        self.assertFalse(self.index.is_enum("Mode"))
        self.assertTrue(self.index.is_enum("Mode2::Mode"))
        mapper = hb.TypeMapper(self.index, [])
        self.assertEqual(
            mapper.scalar("IndexMode", "ctx", "Holder"), ("int", "Holder::IndexMode")
        )


class MethodCoverageProblemsTest(unittest.TestCase):
    REASONS = {"not-yet": "", "internal": ""}

    def _problems(self, called, excluded):
        methods = {
            "Bound": cmc.Method("Bound", False),
            "Unbound": cmc.Method("Unbound", False),
            "Excluded": cmc.Method("Excluded", False),
            "TEST_Hook": cmc.Method("TEST_Hook", True),
        }
        return cmc.coverage_problems("DB", methods, called, excluded, self.REASONS)

    def test_bound_excluded_and_skipped_methods_pass(self):
        problems = self._problems({"Bound", "Unbound"}, {"Excluded": "not-yet"})
        self.assertEqual(problems, [])

    def test_unbound_method_fails(self):
        problems = self._problems({"Bound"}, {"Excluded": "not-yet"})
        self.assertEqual(len(problems), 1)
        self.assertIn("DB::Unbound has no C binding", problems[0])

    def test_unknown_reason_fails(self):
        problems = self._problems({"Bound", "Unbound"}, {"Excluded": "later"})
        self.assertEqual(problems, ['DB::Excluded: unknown reason "later".'])

    def test_exclusion_of_missing_or_skipped_method_fails(self):
        for name in ("Gone", "TEST_Hook"):
            excluded = {"Excluded": "not-yet", name: "internal"}
            problems = self._problems({"Bound", "Unbound"}, excluded)
            self.assertEqual(len(problems), 1, name)
            self.assertIn(f"DB::{name} is excluded but is not a checked", problems[0])

    def test_exclusion_of_called_method_fails(self):
        problems = self._problems(
            {"Bound", "Unbound", "Excluded"}, {"Excluded": "not-yet"}
        )
        self.assertEqual(len(problems), 1)
        self.assertIn("DB::Excluded is excluded but db/c.cc now calls it", problems[0])

    def test_manifest_reasons(self):
        manifest = {
            "reasons": {"customizable": "", "not-yet": ""},
            "excluded_bases": {"Customizable": "customizable"},
            "classes": {"DB": {"header": "rocksdb/db.h", "excluded": {"X": "not-yet"}}},
        }
        self.assertEqual(cmc.manifest_problems(manifest), [])
        manifest["excluded_bases"]["Configurable"] = "plumbing"
        manifest["classes"]["DB"]["excluded"] = {}
        self.assertEqual(
            cmc.manifest_problems(manifest),
            [
                'excluded_bases["Configurable"]: unknown reason "plumbing".',
                'reason "not-yet" is not used; remove it.',
            ],
        )


class ReceiverClassTest(unittest.TestCase):
    def test_libstdcxx_arrow_goes_through_base_cast(self):
        # `x->s->Use()` as clang dumps it against libstdc++, where
        # operator-> is declared in __shared_ptr_access.
        base = {
            "kind": "CXXOperatorCallExpr",
            "type": {"qualType": "element_type *"},
            "inner": [
                {
                    "kind": "ImplicitCastExpr",
                    "inner": [
                        {
                            "kind": "DeclRefExpr",
                            "referencedDecl": {"name": "operator->"},
                        }
                    ],
                },
                {
                    "kind": "ImplicitCastExpr",
                    "type": {
                        "qualType": "const std::__shared_ptr_access<rocksdb::Cache, "
                        "__gnu_cxx::_S_atomic>"
                    },
                    "inner": [
                        {
                            "kind": "MemberExpr",
                            "name": "s",
                            "type": {"qualType": "std::shared_ptr<rocksdb::Cache>"},
                        }
                    ],
                },
            ],
        }
        self.assertEqual(cmc.receiver_class(base), "Cache")

    def test_unique_ptr_with_deleter(self):
        obj = {
            "kind": "MemberExpr",
            "type": {
                "qualType": "std::unique_ptr<rocksdb::Iterator>",
                "desugaredQualType": "std::unique_ptr<rocksdb::Iterator, "
                "std::default_delete<rocksdb::Iterator>>",
            },
        }
        get = {
            "kind": "CXXMemberCallExpr",
            "type": {"qualType": "pointer"},
            "inner": [{"kind": "MemberExpr", "name": "get", "inner": [obj]}],
        }
        self.assertEqual(cmc.receiver_class(get), "Iterator")


class FileScopeNamesTest(unittest.TestCase):
    def test_names_each_kind_of_definition(self):
        source = textwrap.dedent(
            """\
            #include <memory>
            using ROCKSDB_NAMESPACE::DB;
            struct rocksdb_t;
            struct Forward;
            static void Prototype(int a);
            template <typename MapType>
            // A comment between the template head and the function.
            static typename MapType::const_iterator GetAt(const MapType& map,
                                                          size_t pos) {
              if (pos >= map.size()) {
                return map.cend();
              }
            }
            static std::unique_ptr<Range[]> BuildRanges(int n) {
              return nullptr;
            }
            struct Thing* MakeThing() { return nullptr; }
            class H : public WriteBatch::Handler {
             public:
              void Put(const Slice& key, const Slice& value) override {}
            };
            static const int kTable[] = {1, 2};
            enum Local { kA };
            extern "C" {
            struct rocksdb_t {
              DB* rep;
            };
            void rocksdb_close(rocksdb_t* db) { delete db->rep; }
            }  // end extern "C"
            """
        )
        self.assertEqual(
            cmc.file_scope_names(source),
            ["BuildRanges", "GetAt", "H", "MakeThing", "kTable"],
        )

    def test_unnamed_definition_is_an_error(self):
        with self.assertRaisesRegex(hb.GenError, "cannot name"):
            cmc.file_scope_names("static auto [a, b] {\n}\n")


@unittest.skipUnless(CLANG, "needs clang++ (set CLANG_CXX)")
class CalledMethodsTest(unittest.TestCase):
    # Each call path calls a differently named method, so a test failure
    # names the path that broke.
    HEADER = textwrap.dedent(
        """\
        #include <memory>
        #include <vector>
        namespace rocksdb {
        class Base {
         public:
          void ViaBase();
        };
        class Cache : public Base {
         public:
          static Cache* Make();
          void ViaPointer();
          void ViaShared();
          void ViaDeref();
          void ViaGet();
          void ViaUnique();
          void ViaVectorOfPointers();
          void ViaVector();
          void ViaHelper();
          void ViaHandler();
          void reset();
          void ViaComment();
          void NotCalled();
          // Its name contains "Helper", so the dump for Helper includes it.
          void InlineHelper() { NotCalled(); }
        };
        }  // namespace rocksdb
        """
    )
    SOURCE = textwrap.dedent(
        """\
        struct rocksdb_x_t {
          rocksdb::Cache* p;
          std::shared_ptr<rocksdb::Cache> s;
          std::unique_ptr<rocksdb::Cache> u;
          std::vector<rocksdb::Cache*> v;
          std::vector<rocksdb::Cache> w;
        };
        static void Helper(rocksdb::Cache* c) { c->ViaHelper(); }
        class Handler {
         public:
          void On(rocksdb::Cache* c) { c->ViaHandler(); }
        };
        extern "C" void rocksdb_x_call(rocksdb_x_t* x) {
          x->p->ViaPointer();
          x->p->ViaBase();
          x->s->ViaShared();
          (*x->s).ViaDeref();
          x->s.get()->ViaGet();
          x->s.reset();
          x->u->ViaUnique();
          x->v[0]->ViaVectorOfPointers();
          x->w[0].ViaVector();
          Helper(rocksdb::Cache::Make());
          // rocksdb::Cache::ViaComment();
        }
        """
    )

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory(dir=cmc.ROOT) as tmp:
            header = Path(tmp) / "fake.h"
            header.write_text(cls.HEADER)
            include = header.relative_to(cmc.ROOT)
            source = f'#include "{include}"\n' + cls.SOURCE
            path = Path(tmp) / "fake_c.cc"
            path.write_text(source)
            docs = cmc.source_ast(CLANG, path, source)
        cls.called = cmc.called_methods(docs, source)

    def test_calls_count_for_the_receiver_class(self):
        self.assertEqual(
            self.called["Cache"],
            {
                "Make",
                "ViaPointer",
                "ViaBase",
                "ViaShared",
                "ViaDeref",
                "ViaGet",
                "ViaUnique",
                "ViaVectorOfPointers",
                "ViaVector",
                "ViaHelper",
                "ViaHandler",
            },
        )

    def test_misnamed_definition_is_an_error(self):
        # The scan takes the first name before a parenthesis, `__attribute__`,
        # which clang has no declaration for.
        source = "__attribute__((unused)) static void Quiet() {}\n"
        with tempfile.TemporaryDirectory(dir=cmc.ROOT) as tmp:
            path = Path(tmp) / "fake_c.cc"
            path.write_text(source)
            with self.assertRaisesRegex(
                hb.GenError, "no declaration named __attribute__"
            ):
                cmc.source_ast(CLANG, path, source)

    def test_comments_and_header_bodies_do_not_count(self):
        self.assertNotIn("ViaComment", self.called["Cache"])
        self.assertNotIn("NotCalled", self.called["Cache"])

    def test_base_method_counts_for_the_derived_receiver(self):
        self.assertNotIn("Base", self.called)

    def test_smart_pointer_methods_are_not_pointee_methods(self):
        self.assertNotIn("reset", self.called["Cache"])
        self.assertIn("reset", self.called["std::shared_ptr<rocksdb::Cache>"])


@unittest.skipUnless(CLANG, "needs clang++ (set CLANG_CXX)")
class PublicMethodsTest(unittest.TestCase):
    SOURCE = """
        namespace rocksdb {
        class Configurable {
         public:
          void Configure();
        };
        class Covered {
         public:
          void CoveredMethod();
        };
        class CoveredChild : public Covered {
         public:
          void ChildMethod();
        };
        class Mixin : public Configurable {
         public:
          void MixinMethod();
        };
        class Target : public CoveredChild, public Mixin {
         public:
          Target();
          ~Target();
          void Plain();
          void Overloaded(int a);
          [[deprecated]] void Overloaded(int a, int b);
          [[deprecated]] void OnlyDeprecated();
          void TEST_Hook();
          void Deleted() = delete;
          template <typename T>
          void Templated(T t);
          bool operator==(const Target& other) const;
         protected:
          void Protected();
         private:
          void Private();
        };
        class DefaultPrivate {
          void Hidden();
         public:
          void Shown();
        };
        }  // namespace rocksdb
    """

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(dir=hb.ROOT)
        header = Path(cls.tmp.name) / "fake.h"
        header.write_text(cls.SOURCE)
        cls.index = hb.CxxIndex(CLANG, [str(header.relative_to(hb.ROOT))])

    @classmethod
    def tearDownClass(cls):
        cls.index.close()
        cls.tmp.cleanup()

    def _methods(self, cls):
        methods = cmc.public_methods(
            self.index, cls, {"Target", "Covered", "DefaultPrivate"}, {"Configurable"}
        )
        return {name: m.skip for name, m in methods.items()}

    def test_public_methods_and_skips(self):
        self.assertEqual(
            self._methods("Target"),
            {
                "Plain": False,
                "Overloaded": False,
                "OnlyDeprecated": True,
                "TEST_Hook": True,
                "Deleted": True,
                "Templated": False,
                "MixinMethod": False,
            },
        )

    def test_class_members_default_to_private(self):
        self.assertEqual(self._methods("DefaultPrivate"), {"Shown": False})


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

    def test_lossless_cast_is_static_cast_for_values_only(self):
        body = "return ROCKSDB_NAMESPACE::lossless_cast<int>(x);"
        self.assertEqual(
            veq.normalize_lossless_casts(body), "return static_cast<int>(x);"
        )
        pointer = "return lossless_cast<int*>(p);"
        self.assertEqual(veq.normalize_lossless_casts(pointer), pointer)

    def test_void_sole_return(self):
        self.assertEqual(
            veq.normalize_void_returns("{ return f(x); }", "void"), "{ f(x); }"
        )
        self.assertEqual(
            veq.normalize_void_returns("{ return f(x); }", "int"), "{ return f(x); }"
        )
        lam = "{ g([] { return 1; }); }"
        self.assertEqual(veq.normalize_void_returns(lam, "void"), lam)

    def test_guarded_delete_is_plain_delete(self):
        for guarded in ("{ if (p) { delete p; } }", "{ if (p) delete p; }"):
            self.assertEqual(veq.normalize_guarded_deletes(guarded), "{ delete p; }")
        self.assertEqual(
            veq.normalize_guarded_deletes(
                "{ if (b->rep) { delete b->rep; } delete b; }"
            ),
            "{ delete b->rep; delete b; }",
        )
        kept = (
            "{ if (p) { delete q; } }",
            "{ if (p) { delete p; } else { f(); } }",
            "{ if (p) { delete p; f(); } }",
            "{ if (!p) { delete p; } }",
        )
        for body in kept:
            self.assertEqual(veq.normalize_guarded_deletes(body), body)

    def test_returned_local_is_direct_return(self):
        self.assertEqual(
            veq.normalize_returned_locals("{ T* opt = new T; return opt; }"),
            "{ return new T; }",
        )
        kept = (
            "{ T* opt = new T; return other; }",
            "{ T* opt = new T; opt->x = 1; return opt; }",
            "{ T opt = make(); return opt; }",
            "{ if (c) { T* opt = new T; return opt; } return nullptr; }",
        )
        for body in kept:
            self.assertEqual(veq.normalize_returned_locals(body), body)

    def test_bodies_equivalent(self):
        def make(names, body):
            return veq.FunctionDef("f", "", "void", ["int"], names, body)

        ref = make(["n"], "{ return o->rep->Set(n); }")
        gen = make(["v"], "{ o->rep->Set(v); }")
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        other = make(["v"], "{ o->rep->Set(v + 1); }")
        self.assertFalse(veq.bodies_equivalent(ref, other))


if __name__ == "__main__":
    unittest.main()
