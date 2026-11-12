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

    def __init__(self, methods=None, enums=(), typedefs=None, fields=None):
        self._methods = methods or {}
        self._enums = set(enums)
        self._typedefs = typedefs or {}
        self._fields = fields or {}

    def methods(self, cls, name):
        return list(self._methods.get((cls, name), []))

    def field(self, cls, name):
        return self._fields.get((cls, name))

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
    hb.Handle("rocksdb_cptr_t", "const CPtr*", "generated", "c", None, None),
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


class HandleResultTest(unittest.TestCase):
    """Results and out-parameters wrapped in a new handle."""

    def test_pointer_results(self):
        cases = [
            ("Ptr *", "rocksdb_ptr_t"),
            ("std::shared_ptr<Sh>", "rocksdb_sh_t"),
            ("const CPtr *", "rocksdb_cptr_t"),
            ("CPtr *", "rocksdb_cptr_t"),
        ]
        for cxx, c_type in cases:
            with self.subTest(cxx=cxx):
                decl, impl = _render({"result": "handle"}, _fn("Make", [], cxx))
                self.assertIn(
                    f"extern ROCKSDB_LIBRARY_API {c_type}* rocksdb_thing_x(", decl
                )
                self.assertIn(
                    f"{{ {c_type}* result = new {c_type}; "
                    "result->rep = t->rep->Make(); return result; }",
                    impl,
                )

    def test_result_errors(self):
        cases = [
            ("const Ptr *", {}, "does not fit the non-const rep"),
            ("std::shared_ptr<Ptr>", {}, "needs a shared rep"),
            ("int", {}, "needs a pointer or smart pointer"),
            ("Ptr *", {"c_types": {"return": "void*"}}, "only make the returned"),
        ]
        for cxx, extra, message in cases:
            with self.subTest(cxx=cxx):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render({"result": "handle", **extra}, _fn("Make", [], cxx))

    def test_returned_handle_can_be_pinned_const(self):
        entry = {"result": "handle", "c_types": {"return": "const rocksdb_ptr_t*"}}
        decl, impl = _render(entry, _fn("Make", [], "Ptr *"))
        self.assertIn("const rocksdb_ptr_t* rocksdb_thing_x(", decl)
        self.assertIn("{ rocksdb_ptr_t* result = new rocksdb_ptr_t;", impl)

    def test_out_handle(self):
        fn = _fn("Fill", [("out", "Val *"), ("n", "int")])
        decl, impl = _render({"out_handle": "out"}, fn)
        self.assertIn(
            "rocksdb_val_t* rocksdb_thing_x( rocksdb_thing_t* t, int n);", decl
        )
        self.assertIn(
            "{ rocksdb_val_t* result = new rocksdb_val_t; "
            "t->rep->Fill(&result->rep, n); return result; }",
            impl,
        )

    def test_out_handle_errors(self):
        cases = [
            ([("out", "Val *")], "Status", {"out_handle": "out"}, "needs a void"),
            ([("out", "Val *")], "void", {"out_handle": "o"}, "unknown parameter o"),
            ([("out", "Ptr *")], "void", {"out_handle": "out"}, "holds a value"),
            ([("out", "const Val *")], "void", {"out_handle": "out"}, "non-const"),
        ]
        for params, result, entry, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render(entry, _fn("Fill", params, result))

    def test_consumes_frees_the_handle_after_the_call(self):
        fn = _fn("Wrap", [("base", "Ptr *")], "Ptr *")
        _, impl = _render({"result": "handle", "consumes": ["base"]}, fn)
        self.assertIn(
            "result->rep = t->rep->Wrap(base->rep); delete base; return result; }",
            impl,
        )

    def test_consumes_errors(self):
        cases = [
            ("Ptr *", {"consumes": ["base"]}, "needs `result: handle`"),
            ("Ptr *", {"result": "handle", "consumes": ["other"]}, "unknown param"),
        ]
        for cxx, entry, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render(entry, _fn("Wrap", [("base", cxx)], "Ptr *"))
        with self.assertRaisesRegex(hb.GenError, "raw pointer"):
            _render(
                {"result": "handle", "consumes": ["base"]},
                _fn("Wrap", [("base", "Val *")], "Ptr *"),
            )

    def test_result_handle_needs_a_handle_result(self):
        with self.assertRaisesRegex(hb.GenError, "result_handle needs"):
            _render({"result_handle": "rocksdb_ptr_t"}, _fn("Make", [], "int"))

    def test_c_parameter_named_result(self):
        fn = _fn("Make", [("result", "int")], "Ptr *")
        with self.assertRaisesRegex(hb.GenError, r"\['result'\] clash"):
            _render({"result": "handle"}, fn)


class NullableTest(unittest.TestCase):
    """Parameters whose C pointer may be NULL."""

    def test_optional_slices(self):
        fn = _fn(
            "Compact", [("begin", "const Slice *"), ("end", "const Slice *")], "Status"
        )
        decl, impl = _render({"nullable": ["begin", "end"]}, fn)
        self.assertIn(
            "void rocksdb_thing_x( rocksdb_thing_t* t, const char* begin, "
            "size_t begin_len, const char* end, size_t end_len, char** errptr);",
            decl,
        )
        self.assertIn(
            "{ Slice begin_slice(begin, begin_len); Slice end_slice(end, end_len); "
            "SaveError(errptr, t->rep->Compact(begin ? &begin_slice : nullptr, "
            "end ? &end_slice : nullptr)); }",
            impl,
        )

    def test_null_options_mean_defaults(self):
        fn = _fn("Verify", [("opts", "const Val &")], "Status")
        decl, impl = _render({"nullable": ["opts"]}, fn)
        self.assertIn("rocksdb_thing_t* t, const rocksdb_val_t* opts, char**", decl)
        self.assertIn(
            "{ Val opts_default; "
            "const Val& opts_ref = opts != nullptr ? opts->rep : opts_default; "
            "SaveError(errptr, t->rep->Verify(opts_ref)); }",
            impl,
        )

    def test_null_handles_pass_nullptr(self):
        params = [
            ("a", "Val *"),
            ("b", "Ptr *"),
            ("c", "const CPtr *"),
            ("d", "std::shared_ptr<Sh>"),
            ("e", "Sh *"),
            ("f", "const std::shared_ptr<Sh> &"),
        ]
        decl, impl = _render({"nullable": [p for p, _ in params]}, _fn("Use", params))
        self.assertIn(
            "rocksdb_thing_t* t, rocksdb_val_t* a, rocksdb_ptr_t* b, "
            "const rocksdb_cptr_t* c, rocksdb_sh_t* d, rocksdb_sh_t* e, "
            "rocksdb_sh_t* f);",
            decl,
        )
        self.assertIn(
            "{ t->rep->Use(a != nullptr ? &a->rep : nullptr, "
            "b != nullptr ? b->rep : nullptr, c != nullptr ? c->rep : nullptr, "
            "d != nullptr ? d->rep : nullptr, "
            "e != nullptr ? e->rep.get() : nullptr, "
            "f != nullptr ? f->rep : nullptr); }",
            impl,
        )

    def test_setup_runs_after_the_new_handle(self):
        fn = _fn("Fill", [("opts", "const Val &"), ("out", "Val *")])
        _, impl = _render({"nullable": ["opts"], "out_handle": "out"}, fn)
        self.assertIn(
            "{ rocksdb_val_t* result = new rocksdb_val_t; Val opts_default; "
            "const Val& opts_ref = opts != nullptr ? opts->rep : opts_default; "
            "t->rep->Fill(opts_ref, &result->rep); return result; }",
            impl,
        )

    def test_pointer_parameters_need_nullable(self):
        with self.assertRaisesRegex(hb.GenError, "no C mapping"):
            _render({}, _fn("Compact", [("begin", "const Slice *")]))

    def test_errors(self):
        cases = [
            ([("k", "const Slice &")], {"nullable": ["k"]}, "nullable needs"),
            ([("k", "Slice *")], {"nullable": ["k"]}, "nullable needs"),
            ([("o", "const Ptr &")], {"nullable": ["o"]}, "nullable needs"),
            ([("o", "std::unique_ptr<Sh>")], {"nullable": ["o"]}, "nullable needs"),
            ([("o", "int *")], {"nullable": ["o"]}, "nullable needs"),
            ([("n", "int")], {"nullable": ["n"]}, "nullable needs"),
            ([("n", "int")], {"nullable": ["m"]}, "nullable names unknown"),
            (
                [("b", "const Slice *")],
                {"nullable": ["b"], "bind": {"b": "nullptr"}},
                "is bound",
            ),
            (
                [("b", "const Slice *"), ("b_slice", "int")],
                {"nullable": ["b"]},
                r"\['b_slice'\] clash",
            ),
            (
                [("o", "const Val &"), ("o_ref", "int")],
                {"nullable": ["o"]},
                r"\['o_ref'\] clash",
            ),
        ]
        for params, entry, message in cases:
            with self.subTest(message=message, params=params):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render(entry, _fn("Use", params))


class SlicePartsTest(unittest.TestCase):
    """const SliceParts & parameters, built from C arrays."""

    KV = [("key", "const SliceParts &"), ("value", "const SliceParts &")]
    RANGE = [("begin", "const SliceParts &"), ("end", "const SliceParts &")]

    def _render(self, entry, params):
        decl, impl = _render(
            {"result": "discard", **entry}, _fn("Put", params, "Status")
        )
        return decl.replace("( ", "("), impl.replace("( ", "(")

    def test_each_parameter_has_its_own_count(self):
        decl, impl = self._render({"param_names": {"key": "keys"}}, self.KV)
        self.assertIn(
            "void rocksdb_thing_x(rocksdb_thing_t* t, int num_keys, "
            "const char* const* keys_list, const size_t* keys_list_sizes, "
            "int num_value, const char* const* value_list, "
            "const size_t* value_list_sizes);",
            decl,
        )
        self.assertIn(
            "{ std::unique_ptr<Slice[]> keys_slices(new Slice[num_keys]); "
            "for (int i = 0; i < num_keys; i++) { "
            "keys_slices[i] = Slice(keys_list[i], keys_list_sizes[i]); } "
            "std::unique_ptr<Slice[]> value_slices(new Slice[num_value]); "
            "for (int i = 0; i < num_value; i++) { "
            "value_slices[i] = Slice(value_list[i], value_list_sizes[i]); } "
            "t->rep->Put(SliceParts(keys_slices.get(), num_keys), "
            "SliceParts(value_slices.get(), num_value)); }",
            impl,
        )

    def test_shared_count_comes_first_and_fills_in_one_loop(self):
        decl, impl = self._render(
            {"shared_count": "n", "param_names": {"begin": "start"}}, self.RANGE
        )
        self.assertIn(
            "(rocksdb_thing_t* t, int n, const char* const* start_list, "
            "const size_t* start_list_sizes, const char* const* end_list, "
            "const size_t* end_list_sizes);",
            decl,
        )
        self.assertIn(
            "{ std::unique_ptr<Slice[]> start_slices(new Slice[n]); "
            "std::unique_ptr<Slice[]> end_slices(new Slice[n]); "
            "for (int i = 0; i < n; i++) { "
            "start_slices[i] = Slice(start_list[i], start_list_sizes[i]); "
            "end_slices[i] = Slice(end_list[i], end_list_sizes[i]); } "
            "t->rep->Put(SliceParts(start_slices.get(), n), "
            "SliceParts(end_slices.get(), n)); }",
            impl,
        )

    def test_bound_parameter_does_not_share(self):
        params = [("fixed", "const SliceParts &"), *self.RANGE]
        decl, impl = self._render(
            {"shared_count": "n", "bind": {"fixed": "SliceParts()"}}, params
        )
        self.assertIn(
            "(rocksdb_thing_t* t, int n, const char* const* begin_list,", decl
        )
        self.assertIn(
            "} t->rep->Put(SliceParts(), SliceParts(begin_slices.get(), n), "
            "SliceParts(end_slices.get(), n)); }",
            impl,
        )

    def test_errors(self):
        one = self.KV[:1]
        cases = [
            (one, {"shared_count": "n"}, "needs two SliceParts"),
            (self.RANGE, {"shared_count": "1n"}, "must name a C parameter"),
            (one, {"nullable": ["key"]}, "nullable needs"),
            (
                [*self.RANGE, ("extra", "const SliceParts &")],
                {"shared_count": "n", "nullable": ["extra"]},
                "nullable needs",
            ),
            (self.RANGE, {"shared_count": "i"}, r"\['i'\] clash"),
            ([*one, ("i", "int")], {}, r"\['i'\] clash"),
            ([*one, ("key_slices", "int")], {}, r"\['key_slices'\] clash"),
            (self.KV, {"param_names": {"value": "key"}}, "appear twice"),
            (one, {"c_types": {"key": "int"}}, "multi-value"),
        ]
        for params, entry, message in cases:
            with self.subTest(message=message, entry=entry):
                with self.assertRaisesRegex(hb.GenError, message):
                    self._render(entry, params)


VEC = hb.Handle("rocksdb_vec_t", "std::vector<Elem>", "generated", "v", None, None, "int")
ELEM_FIELDS = {
    ("Elem", "level"): "int",
    ("Elem", "name"): "std::string",
    ("Elem", "key"): "Slice",
    ("Elem", "size"): "uint64_t",
    ("Elem", "seqno"): "SequenceNumber",
    ("Elem", "kind"): "Kind",
    ("Elem", "flag"): "bool",
}


def _render_element(entry, handle=VEC):
    index = FakeIndex({}, ("Kind",), {"SequenceNumber": "uint64_t"}, ELEM_FIELDS)
    mapper = hb.TypeMapper(index, [handle, THING] + OTHER_HANDLES)
    full = {"name": "rocksdb_vec_x", **entry}
    rendered = hb.render_binding(hb.Binding(handle, full), index, mapper)
    # The signature wraps after `(` only when it is long.
    return tuple(
        _flat("\n".join(lines)).replace("( ", "(")
        for lines in (rendered.decl, rendered.impl)
    )


class ElementAccessTest(unittest.TestCase):
    """`field` and `count` entries on a std::vector rep."""

    def test_fields(self):
        cases = [
            ({"field": "level"}, "int", "return v->rep[index].level;"),
            ({"field": "seqno"}, "uint64_t", "return v->rep[index].seqno;"),
            ({"field": "flag"}, "unsigned char", "return v->rep[index].flag;"),
            (
                {"field": "kind"},
                "int",
                "return ROCKSDB_NAMESPACE::lossless_cast<int>(v->rep[index].kind);",
            ),
            (
                {"field": "size", "c_types": {"return": "size_t"}},
                "size_t",
                "return static_cast<size_t>(v->rep[index].size);",
            ),
            (
                {"field": "name", "result": "c_str"},
                "const char*",
                "return v->rep[index].name.c_str();",
            ),
        ]
        for entry, ret, statement in cases:
            with self.subTest(entry=entry):
                decl, impl = _render_element(entry)
                self.assertIn(
                    f"{ret} rocksdb_vec_x(const rocksdb_vec_t* v, int index);", decl
                )
                self.assertIn(f"int index) {{ {statement} }}", impl)

    def test_string_field_with_length(self):
        _, impl = _render_element({"field": "name", "result": "borrow"})
        self.assertIn(
            "const char* rocksdb_vec_x(const rocksdb_vec_t* v, int index, "
            "size_t* len) { const std::string& s = v->rep[index].name; "
            "*len = s.size(); return s.data(); }",
            impl,
        )
        _, impl = _render_element({"field": "name", "result": "copy"})
        self.assertIn("*len = s.size(); return CopyString(s); }", impl)

    def test_index_type(self):
        handle = hb.Handle(
            "rocksdb_vec_t", "std::vector<Elem>", "generated", "v", None, None, "size_t"
        )
        decl, _ = _render_element({"field": "level"}, handle)
        self.assertIn("rocksdb_vec_x(const rocksdb_vec_t* v, size_t index);", decl)

    def test_count(self):
        handle = hb.Handle(
            "rocksdb_vec_t", "std::vector<Elem>", "generated", "v", None, None
        )
        _, impl = _render_element({"count": True}, handle)
        self.assertEqual(
            impl,
            "size_t rocksdb_vec_x(const rocksdb_vec_t* v) { return v->rep.size(); }",
        )
        _, impl = _render_element({"count": True, "c_types": {"return": "int"}})
        self.assertIn("{ return static_cast<int>(v->rep.size()); }", impl)

    def test_errors(self):
        no_index = hb.Handle(
            "rocksdb_vec_t", "std::vector<Elem>", "generated", "v", None, None
        )
        cases = [
            ({"field": "level"}, no_index, "needs an index_type"),
            ({"field": "missing"}, VEC, "no public field missing"),
            ({"field": "level", "method": "X"}, VEC, "one of method, field, count"),
            ({"field": "level", "count": True}, VEC, "one of method, field, count"),
            ({"field": "level", "set": "level"}, VEC, "one of method, field, count"),
            ({}, VEC, "one of method, field, count or set"),
            ({"field": "level", "overload": []}, VEC, "do not apply to field"),
            ({"field": "level", "param_order": []}, VEC, "do not apply to field"),
            ({"count": True, "result": "copy"}, VEC, "do not apply to count"),
            ({"count": 1}, VEC, "count on a std::vector rep must be true"),
            ({"count": "level"}, VEC, "count on a std::vector rep must be true"),
            ({"set": "level"}, VEC, "set does not apply to a std::vector rep"),
            ({"field": "name"}, VEC, "needs `result: borrow`"),
            ({"field": "key", "result": "c_str"}, VEC, "c_str` needs a std::string"),
            ({"field": "level", "result": "c_str"}, VEC, "does not apply"),
        ]
        for entry, handle, message in cases:
            with self.subTest(entry=entry):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render_element(entry, handle)

    def test_c_str_method_results(self):
        _, impl = _render({"result": "c_str"}, _fn("Name", [], "const std::string &"))
        self.assertIn("const char* rocksdb_thing_x(rocksdb_thing_t* t) {", impl)
        self.assertIn("{ return t->rep->Name().c_str(); }", impl)
        with self.assertRaisesRegex(hb.GenError, "cannot borrow"):
            _render({"result": "c_str"}, _fn("Name", [], "std::string"))


MEMBER_FIELDS = {
    ("Thing", "level"): "int",
    ("Thing", "name"): "std::string",
    ("Thing", "flag"): "bool",
    ("Thing", "kind"): "Kind",
    ("Thing", "levels"): "std::vector<Level>",
    ("Thing", "props"): "Props",
    ("Thing", "env"): "Ptr *",
    ("Thing", "snap"): "const CPtr *",
    ("Thing", "cache"): "std::shared_ptr<Sh>",
    ("Thing", "acache"): "std::shared_ptr<ShAlias>",
    ("Thing", "owned"): "std::unique_ptr<Sh>",
    ("Thing", "opts"): "Val",
    ("Val", "size"): "uint64_t",
    ("Val", "levels"): "std::vector<Level>",
    ("Val", "fixed"): "const int",
    ("Val", "files"): "const std::vector<Level>",
}
VAL = OTHER_HANDLES[0]


def _render_member(entry, handle=THING):
    typedefs = {"Props": "std::map<std::string, std::string>", "ShAlias": "Sh"}
    index = FakeIndex({}, ("Kind",), typedefs, MEMBER_FIELDS)
    mapper = hb.TypeMapper(index, [THING, VEC] + OTHER_HANDLES)
    full = {"name": "rocksdb_thing_x", **entry}
    rendered = hb.render_binding(hb.Binding(handle, full), index, mapper)
    return tuple(
        _flat("\n".join(lines)).replace("( ", "(")
        for lines in (rendered.decl, rendered.impl)
    )


class MemberAccessTest(unittest.TestCase):
    """`field`, `count` and `set` entries on a handle that is not a vector."""

    def test_field_reads(self):
        cases = [
            ({"field": "level"}, THING, "int", "return t->rep->level;"),
            ({"field": "flag"}, THING, "unsigned char", "return t->rep->flag;"),
            (
                {"field": "kind"},
                THING,
                "int",
                "return ROCKSDB_NAMESPACE::lossless_cast<int>(t->rep->kind);",
            ),
            (
                {"field": "name", "result": "c_str"},
                THING,
                "const char*",
                "return t->rep->name.c_str();",
            ),
            ({"field": "size"}, VAL, "uint64_t", "return v->rep.size;"),
            # A const member reads like its type.
            ({"field": "fixed"}, VAL, "int", "return v->rep.fixed;"),
        ]
        for entry, handle, ret, statement in cases:
            with self.subTest(entry=entry):
                decl, impl = _render_member(entry, handle)
                receiver = f"const {handle.c_type}* {handle.var}"
                self.assertIn(f"{ret} rocksdb_thing_x({receiver});", decl)
                self.assertIn(f"{receiver}) {{ {statement} }}", impl)
        decl, _ = _render_member({"field": "level", "receiver_const": False})
        self.assertIn("int rocksdb_thing_x(rocksdb_thing_t* t);", decl)

    def test_counts(self):
        cases = [
            ({"count": "levels"}, THING, "size_t", "return t->rep->levels.size();"),
            ({"count": "props"}, THING, "size_t", "return t->rep->props.size();"),
            ({"count": "levels"}, VAL, "size_t", "return v->rep.levels.size();"),
            ({"count": "files"}, VAL, "size_t", "return v->rep.files.size();"),
            (
                {"count": "levels", "c_types": {"return": "int"}},
                THING,
                "int",
                "return static_cast<int>(t->rep->levels.size());",
            ),
        ]
        for entry, handle, ret, statement in cases:
            with self.subTest(entry=entry):
                decl, impl = _render_member(entry, handle)
                receiver = f"const {handle.c_type}* {handle.var}"
                self.assertIn(f"{ret} rocksdb_thing_x({receiver});", decl)
                self.assertIn(f"{receiver}) {{ {statement} }}", impl)

    def test_sets(self):
        cases = [
            ({"set": "level"}, "int level", "t->rep->level = level;"),
            ({"set": "flag"}, "unsigned char flag", "t->rep->flag = flag;"),
            ({"set": "kind"}, "int kind", "t->rep->kind = static_cast<Kind>(kind);"),
            ({"set": "name"}, "const char* name", "t->rep->name = std::string(name);"),
            ({"set": "env"}, "rocksdb_ptr_t* env", "t->rep->env = env->rep;"),
            (
                {"set": "snap"},
                "const rocksdb_cptr_t* snap",
                "t->rep->snap = snap->rep;",
            ),
            ({"set": "cache"}, "rocksdb_sh_t* cache", "t->rep->cache = cache->rep;"),
            # An alias of a class a handle wraps maps to that handle.
            (
                {"set": "acache"},
                "rocksdb_sh_t* acache",
                "t->rep->acache = acache->rep;",
            ),
            ({"set": "opts"}, "const rocksdb_val_t* opts", "t->rep->opts = opts->rep;"),
            (
                {"set": "opts", "c_types": {"opts": "rocksdb_val_t*"}},
                "rocksdb_val_t* opts",
                "t->rep->opts = opts->rep;",
            ),
            (
                {"set": "env", "param_names": {"env": "e"}},
                "rocksdb_ptr_t* e",
                "t->rep->env = e->rep;",
            ),
            (
                {"set": "env", "nullable": ["env"]},
                "rocksdb_ptr_t* env",
                "t->rep->env = env != nullptr ? env->rep : nullptr;",
            ),
            (
                {"set": "cache", "nullable": ["cache"]},
                "rocksdb_sh_t* cache",
                "t->rep->cache = cache != nullptr ? cache->rep : nullptr;",
            ),
            (
                {"set": "acache", "nullable": ["acache"]},
                "rocksdb_sh_t* acache",
                "t->rep->acache = acache != nullptr ? acache->rep : nullptr;",
            ),
        ]
        for entry, param, statement in cases:
            with self.subTest(entry=entry):
                decl, impl = _render_member(entry)
                self.assertIn(
                    f"void rocksdb_thing_x(rocksdb_thing_t* t, {param});", decl
                )
                self.assertIn(f"{param}) {{ {statement} }}", impl)
        _, impl = _render_member({"set": "size"}, VAL)
        self.assertIn("{ v->rep.size = size; }", impl)

    def test_skip_if_null(self):
        _, impl = _render_member({"set": "cache", "skip_if_null": ["cache"]})
        self.assertIn("{ if (cache != nullptr) { t->rep->cache = cache->rep; } }", impl)
        # The checks follow the C parameter order.
        _, impl = _render_member({"set": "cache", "skip_if_null": ["cache", "t"]})
        self.assertIn("{ if (t != nullptr && cache != nullptr) {", impl)
        # Setup statements run inside the check.
        fn = _fn("Verify", [("opts", "const Val &")])
        _, impl = _render({"nullable": ["opts"], "skip_if_null": ["t"]}, fn)
        self.assertIn(
            "{ if (t != nullptr) { Val opts_default; const Val& opts_ref = "
            "opts != nullptr ? opts->rep : opts_default; "
            "t->rep->Verify(opts_ref); } }",
            impl,
        )
        _, impl = _render(
            {"skip_if_null": ["p"], "result": "discard"},
            _fn("Use", [("p", "Ptr *")], "Status"),
        )
        self.assertIn("{ if (p != nullptr) { t->rep->Use(p->rep); } }", impl)

    def test_errors(self):
        cases = [
            ({"set": "level", "result": "discard"}, THING, "do not apply to set"),
            ({"field": "level", "nullable": ["level"]}, THING, "do not apply to field"),
            ({"field": "level", "skip_if_null": ["t"]}, THING, "do not apply"),
            ({"count": "level"}, THING, "count needs a container member"),
            ({"count": True}, THING, "count must name a data member"),
            ({"field": True}, THING, "field must name a data member"),
            ({"set": "missing"}, THING, "no public field missing"),
            ({"field": "missing"}, VAL, "Val has no public field missing"),
            ({"set": "owned"}, THING, "cannot assign unique_ptr"),
            ({"set": "fixed"}, VAL, "cannot assign const member fixed"),
            ({"set": "levels"}, THING, "no C mapping"),
            ({"set": "cache", "skip_if_null": ["level"]}, THING, "neither the handle"),
            ({"set": "level", "skip_if_null": ["level"]}, THING, "neither the handle"),
            (
                {"set": "cache", "skip_if_null": ["cache"], "nullable": ["cache"]},
                THING,
                "both nullable and skip_if_null",
            ),
            (
                {"set": "cache", "skip_if_null": ["cache", "cache"]},
                THING,
                "lists a parameter twice",
            ),
            (
                {"set": "env", "c_types": {"env": "rocksdb_val_t*"}},
                THING,
                "can only drop const",
            ),
        ]
        for entry, handle, message in cases:
            with self.subTest(entry=entry):
                with self.assertRaisesRegex(hb.GenError, message):
                    _render_member(entry, handle)
        for result in ("Status", "int"):
            with self.subTest(result=result):
                with self.assertRaisesRegex(hb.GenError, "with no result"):
                    _render({"skip_if_null": ["t"]}, _fn("Run", [], result))


class HandleParamPinTest(unittest.TestCase):
    def test_pin_drops_const(self):
        fn = _fn("Use", [("o", "const Val &")])
        decl, impl = _render({"c_types": {"o": "rocksdb_val_t*"}}, fn)
        self.assertIn("rocksdb_thing_x( rocksdb_thing_t* t, rocksdb_val_t* o);", decl)
        self.assertIn("t->rep->Use(o->rep);", impl)

    def test_pin_cannot_change_the_handle(self):
        cases = [
            ("const Val &", "rocksdb_ptr_t*"),
            ("const Val &", "void*"),
            ("Val &", "const rocksdb_val_t*"),
        ]
        for cxx, pinned in cases:
            with self.subTest(cxx=cxx, pinned=pinned):
                with self.assertRaisesRegex(hb.GenError, "only drop const"):
                    _render({"c_types": {"o": pinned}}, _fn("Use", [("o", cxx)]))


class ParamOrderTest(unittest.TestCase):
    def test_c_order_differs_from_cxx_order(self):
        fn = _fn("Use", [("a", "Val *"), ("b", "const Slice &")])
        _, impl = _render({"param_order": ["b", "a"]}, fn)
        self.assertIn(
            "void rocksdb_thing_x( rocksdb_thing_t* t, const char* b, size_t b_len, "
            "rocksdb_val_t* a) { t->rep->Use(&a->rep, Slice(b, b_len)); }",
            impl,
        )

    def test_order_must_list_each_parameter(self):
        fn = _fn("Use", [("a", "Val *"), ("b", "int")])
        for order in (["b"], ["b", "a", "a"], ["b", "c"]):
            with self.subTest(order=order):
                with self.assertRaisesRegex(hb.GenError, "param_order must list"):
                    _render({"param_order": order}, fn)


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

    def test_member_keys_load(self):
        # load_handles rejects unknown keys before render_binding sees them.
        for kind, keys in hb.MEMBER_KEYS.items():
            with self.subTest(kind=kind):
                self.assertLessEqual(keys, hb.METHOD_KEYS)
                entry = {k: "x" for k in keys} | {"name": "rocksdb_thing_x"}
                _, handles = self._load([self._handle(methods=[entry])])
                self.assertEqual(handles[0].bindings[0].entry, entry)

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
            (
                [self._handle(rep="std::vector<Elem>", index_type="long")],
                "index_type is int or size_t",
            ),
            ([self._handle(index_type="int")], "index_type is int or size_t"),
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
        #include <string>
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
        struct FileBase {
          uint64_t size = 0;
         private:
          int base_secret;
        };
        class FileMeta : public FileBase {
         public:
          std::string name;
         private:
          int secret;
        };
        class HiddenBase : FileBase {};
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

    def test_fields(self):
        self.assertEqual(self.index.field("FileMeta", "name"), "std::string")
        self.assertEqual(self.index.field("FileMeta", "size"), "uint64_t")
        for cls, name in (
            ("FileMeta", "secret"),
            ("FileMeta", "base_secret"),
            ("FileMeta", "missing"),
            ("HiddenBase", "size"),
        ):
            with self.subTest(cls=cls, name=name):
                self.assertIsNone(self.index.field(cls, name))

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
        # A member that happens to share the new name is not a capture.
        ref = make(["n"], "{ o->rep.v = n; }")
        self.assertTrue(veq.bodies_equivalent(ref, make(["v"], "{ o->rep.v = v; }")))

    def test_statements_are_not_declarations(self):
        def make(body):
            return veq.FunctionDef("f", "", "int", [], [], body)

        pairs = [
            ("{ return f(x); }", "{ return g(x); }"),
            ("{ delete p; }", "{ delete q; }"),
            ("{ throw e; }", "{ throw d; }"),
            ("{ if (c) { x(); } else f(y); }", "{ if (c) { x(); } else g(y); }"),
        ]
        for ref, gen in pairs:
            with self.subTest(ref=ref):
                self.assertEqual(veq.local_renames(ref, gen), {})
                self.assertFalse(veq.bodies_equivalent(make(ref), make(gen)))

    def test_new_handle_locals_pair_up(self):
        def make(body):
            return veq.FunctionDef("f", "", "rocksdb_h_t*", [], [], body)

        gen = make(
            "{ rocksdb_h_t* result = new rocksdb_h_t; "
            "result->rep = d->rep->Make(); return result; }"
        )
        ref = make(
            "{ rocksdb_h_t* meta = new rocksdb_h_t; "
            "meta->rep = d->rep->Make(); return meta; }"
        )
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        # A different handle type still differs after the rename.
        ref = make(
            "{ rocksdb_g_t* meta = new rocksdb_g_t; "
            "meta->rep = d->rep->Make(); return meta; }"
        )
        self.assertFalse(veq.bodies_equivalent(ref, gen))
        # The reference returns some other `result`. Renaming `meta` into it
        # would make the bodies token-equal.
        ref = make(
            "{ rocksdb_h_t* meta = new rocksdb_h_t; "
            "meta->rep = d->rep->Make(); return result; }"
        )
        self.assertEqual(veq.local_renames(ref.body, gen.body), {"meta": "result"})
        self.assertFalse(veq.bodies_equivalent(ref, gen))

    def test_locals_pair_by_type_then_order(self):
        ref = (
            "{ Slice a; Slice b; T* t = new T; const O& o = x ? x->rep : d; "
            "for (size_t i = 0; i < n; i++) {} return f(a, b); }"
        )
        gen = "{ T* result = new T; Slice lo(p, n); Slice hi(q, m); const O& o_ref = y; }"
        self.assertEqual(
            veq.local_renames(ref, gen),
            {"a": "lo", "b": "hi", "t": "result", "o": "o_ref"},
        )

    def test_optional_slice_is_built_before_the_check(self):
        ref = "{ Slice a; f(x, (p ? (a = Slice(p, n), &a) : nullptr)); }"
        self.assertEqual(
            veq.normalize_optional_slices(ref),
            "{ Slice a(p, n); f(x, p ? &a : nullptr); }",
        )
        kept = (
            # a is read again after the call.
            "{ Slice a; f((p ? (a = Slice(p, n), &a) : nullptr)); g(a); }",
            # The Slice would capture p and n before they change.
            "{ Slice a; p = q; f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            "{ Slice a; n += 1; f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            "{ Slice a; ++n; f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            "{ Slice a; g(&n); f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            # Two declarations of a.
            "{ Slice a; { Slice a; } f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            # a starts out as some other Slice.
            "{ Slice a = s; f((p ? (a = Slice(p, n), &a) : nullptr)); }",
            # A Slice of another pointer.
            "{ Slice a; f((p ? (a = Slice(q, n), &a) : nullptr)); }",
        )
        for body in kept:
            with self.subTest(body=body):
                self.assertEqual(veq.normalize_optional_slices(body), body)
        # Comparisons and logical operators are not writes.
        reads = "{ Slice a; f(n == 1 && p != q, (p ? (a = Slice(p, n), &a) : nullptr)); }"
        self.assertNotEqual(veq.normalize_optional_slices(reads), reads)

    def test_optional_slice_bodies(self):
        """The shipped rocksdb_compact_range_cf against the generator's shape."""

        def make(names, body):
            return veq.FunctionDef("f", "", "void", [], names, body)

        ref = make(
            ["db", "column_family", "start_key", "start_key_len", "limit_key", "limit_key_len"],
            "{ Slice a; Slice b; db->rep->CompactRange( CompactRangeOptions(), "
            "column_family->rep, "
            "(start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr), "
            "(limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)); }",
        )
        names = ["db", "column_family", "begin", "begin_len", "end", "end_len"]
        gen = make(
            names,
            "{ Slice begin_slice(begin, begin_len); Slice end_slice(end, end_len); "
            "db->rep->CompactRange(CompactRangeOptions(), column_family->rep, "
            "begin ? &begin_slice : nullptr, end ? &end_slice : nullptr); }",
        )
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        swapped = make(
            names,
            "{ Slice begin_slice(begin, begin_len); Slice end_slice(end, end_len); "
            "db->rep->CompactRange(CompactRangeOptions(), column_family->rep, "
            "end ? &end_slice : nullptr, begin ? &begin_slice : nullptr); }",
        )
        self.assertFalse(veq.bodies_equivalent(ref, swapped))

    def test_null_options_bodies(self):
        """The shipped rocksdb_verify_checksum_with_options against the
        generator's shape, where the C parameter takes the C++ name that the
        reference used for its local."""

        def make(names, body):
            return veq.FunctionDef("f", "", "void", [], names, body)

        ref = make(
            ["db", "options", "errptr"],
            "{ ReadOptions default_options; const ReadOptions& read_options = "
            "options != nullptr ? options->rep : default_options; "
            "SaveError(errptr, db->rep->VerifyChecksum(read_options)); }",
        )
        gen = make(
            ["db", "read_options", "errptr"],
            "{ ReadOptions read_options_default; const ReadOptions& read_options_ref = "
            "read_options != nullptr ? read_options->rep : read_options_default; "
            "SaveError(errptr, db->rep->VerifyChecksum(read_options_ref)); }",
        )
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        # Passing the possibly-NULL handle's rep directly is not equivalent.
        unguarded = make(
            ["db", "read_options", "errptr"],
            "{ ReadOptions read_options_default; const ReadOptions& read_options_ref = "
            "read_options->rep; "
            "SaveError(errptr, db->rep->VerifyChecksum(read_options_ref)); }",
        )
        self.assertFalse(veq.bodies_equivalent(ref, unguarded))

    def test_pointer_conditions(self):
        pointers = frozenset({"p", "q"})

        def norm(text):
            return " ".join(
                veq.normalize_pointer_conditions(veq.tokenize(text), pointers)
            )

        rewritten = [
            ("if (p) {", "if ( p != nullptr ) {"),
            ("while (p) {", "while ( p != nullptr ) {"),
            ("if (p && q) {", "if ( p != nullptr && q != nullptr ) {"),
            ("if (n && p || q) {", "if ( n && p != nullptr || q != nullptr ) {"),
            ("x = p ? a : b;", "x = p != nullptr ? a : b ;"),
            ("return (p ? a : b);", "return ( p != nullptr ? a : b ) ;"),
            ("f(c, p && q);", "f ( c , p != nullptr && q != nullptr ) ;"),
            # Only the whole operand is rewritten.
            ("if (!p && q) {", "if ( ! p && q != nullptr ) {"),
            ("if (q && p == nullptr) {", "if ( q != nullptr && p == nullptr ) {"),
            ("if (t->p && q) {", "if ( t -> p && q != nullptr ) {"),
        ]
        kept = [
            "if (!p) {",
            "if (p != nullptr) {",
            "if (p == nullptr) {",
            "f(p);",
            "x = p;",
            "x = c ? p : q;",
            "if (n) {",
            "if (t->p) {",
            "if (g(p)) {",
            "if ((T*)p) {",
        ]
        for text, want in rewritten:
            with self.subTest(text=text):
                self.assertEqual(norm(text), want)
        for text in kept:
            with self.subTest(text=text):
                self.assertEqual(norm(text), " ".join(veq.tokenize(text)))

    def test_redundant_parens(self):
        def norm(text):
            return " ".join(veq.drop_redundant_parens(veq.tokenize(text)))

        cases = [
            ("x = (a + b);", "x = a + b ;"),
            ("x = ((a));", "x = a ;"),
            ("return (c ? a : b);", "return c ? a : b ;"),
            ("x = (f(a, b));", "x = f ( a , b ) ;"),
            ("x = (a, b);", "x = ( a , b ) ;"),
            ("x = (T)(v);", "x = ( T ) ( v ) ;"),
            ("x = (a) + b;", "x = ( a ) + b ;"),
            ("x == (a);", "x == ( a ) ;"),
            ("f((a));", "f ( ( a ) ) ;"),
        ]
        for text, want in cases:
            with self.subTest(text=text):
                self.assertEqual(norm(text), want)

    def test_null_handle_setter_bodies(self):
        """The shipped `(env ? env->rep : nullptr)` and `if (a && b)` setters
        against the generator's shapes."""

        def make(
            body, names=("opt", "env"), types=("rocksdb_options_t*", "rocksdb_env_t*")
        ):
            return veq.FunctionDef("f", "", "void", list(types), list(names), body)

        ref = make("{ opt->rep.env = (env ? env->rep : nullptr); }")
        gen = make("{ opt->rep.env = env != nullptr ? env->rep : nullptr; }")
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        self.assertFalse(
            veq.bodies_equivalent(ref, make("{ opt->rep.env = env->rep; }"))
        )
        ref = make("{ if (opt && env) { opt->rep.env = env->rep; } }")
        gen = make(
            "{ if (opt != nullptr && env != nullptr) { opt->rep.env = env->rep; } }"
        )
        self.assertTrue(veq.bodies_equivalent(ref, gen))
        self.assertFalse(
            veq.bodies_equivalent(
                ref, make("{ if (env != nullptr) { opt->rep.env = env->rep; } }")
            )
        )
        # A parameter that is not a pointer is not compared with nullptr.
        ints = ("opt", "n"), ("rocksdb_options_t*", "int")
        self.assertFalse(
            veq.bodies_equivalent(
                make("{ if (n) { f(); } }", *ints),
                make("{ if (n != nullptr) { f(); } }", *ints),
            )
        )


if __name__ == "__main__":
    unittest.main()
