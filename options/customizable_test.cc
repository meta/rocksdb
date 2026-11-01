//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/customizable.h"

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>
#include <unordered_set>

#include "db/db_test_util.h"
#include "memory/jemalloc_nodump_allocator.h"
#include "memory/memkind_kmem_allocator.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/flush_block_policy_impl.h"
#include "table/mock_table.h"
#include "test_util/mock_time_env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/file_checksum_helper.h"
#include "util/string_util.h"
#include "utilities/compaction_filters/remove_emptyvalue_compactionfilter.h"
#include "utilities/memory_allocators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {
namespace {
class TestCustomizable : public Customizable {
 public:
  TestCustomizable(const std::string& name) : name_(name) {}
  // Method to allow CheckedCast to work for this class
  static const char* kClassName() { return "TestCustomizable"; }

  const char* Name() const override { return name_.c_str(); }
  static const char* Type() { return "test.custom"; }
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::unique_ptr<TestCustomizable>* result);
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::shared_ptr<TestCustomizable>* result);
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 TestCustomizable** result);
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return Customizable::IsInstanceOf(name);
    }
  }

 protected:
  const std::string name_;
};

struct AOptions {
  static const char* kName() { return "A"; }
  int i = 0;
  bool b = false;
};

static std::unordered_map<std::string, OptionTypeInfo> a_option_info = {
    {"int",
     {offsetof(struct AOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
    {"bool",
     {offsetof(struct AOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

class ACustomizable : public TestCustomizable {
 public:
  explicit ACustomizable(const std::string& id)
      : TestCustomizable("A"), id_(id) {
    RegisterOptions(&opts_, &a_option_info);
  }
  std::string GetId() const override { return id_; }
  static const char* kClassName() { return "A"; }

 private:
  AOptions opts_;
  const std::string id_;
};

struct BOptions {
  std::string s;
  bool b = false;
};

static std::unordered_map<std::string, OptionTypeInfo> b_option_info = {
    {"string",
     {offsetof(struct BOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"bool",
     {offsetof(struct BOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

class BCustomizable : public TestCustomizable {
 private:
 public:
  explicit BCustomizable(const std::string& name) : TestCustomizable(name) {
    RegisterOptions(name, &opts_, &b_option_info);
  }
  static const char* kClassName() { return "B"; }

 private:
  BOptions opts_;
};

static int A_count = 0;
static int RegisterCustomTestObjects(ObjectLibrary& library,
                                     const std::string& /*arg*/) {
  library.AddFactory<TestCustomizable>(
      ObjectLibrary::PatternEntry("A", true).AddSeparator("_"),
      [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new ACustomizable(name));
        A_count++;
        return guard->get();
      });
  library.AddFactory<TestCustomizable>(
      "B", [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
              std::string* /* msg */) {
        guard->reset(new BCustomizable(name));
        return guard->get();
      });

  library.AddFactory<TestCustomizable>(
      "S", [](const std::string& name,
              std::unique_ptr<TestCustomizable>* /* guard */,
              std::string* /* msg */) { return new BCustomizable(name); });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

struct SimpleOptions {
  static const char* kName() { return "simple"; }
  bool b = true;
  std::unique_ptr<TestCustomizable> cu;
  std::shared_ptr<TestCustomizable> cs;
  TestCustomizable* cp = nullptr;
};

static std::unordered_map<std::string, OptionTypeInfo> simple_option_info = {
    {"bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"unique",
     OptionTypeInfo::AsCustomUniquePtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cu), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
    {"shared",
     OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cs), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
    {"pointer",
     OptionTypeInfo::AsCustomRawPtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cp), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
};

class SimpleConfigurable : public Configurable {
 private:
  SimpleOptions simple_;

 public:
  SimpleConfigurable() { RegisterOptions(&simple_, &simple_option_info); }
};

static void GetMapFromProperties(
    const std::string& props,
    std::unordered_map<std::string, std::string>* map) {
  std::istringstream iss(props);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  map->clear();
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    (*map)[name] = value;
  }
}
}  // namespace

Status TestCustomizable::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<TestCustomizable>* result) {
  return LoadSharedObject<TestCustomizable>(config_options, value, result);
}

Status TestCustomizable::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::unique_ptr<TestCustomizable>* result) {
  return LoadUniqueObject<TestCustomizable>(config_options, value, result);
}

Status TestCustomizable::CreateFromString(const ConfigOptions& config_options,
                                          const std::string& value,
                                          TestCustomizable** result) {
  return LoadStaticObject<TestCustomizable>(config_options, value, result);
}

class CustomizableTest : public testing::Test {
 public:
  CustomizableTest() {
    config_options_.invoke_prepare_options = false;
    config_options_.registry->AddLibrary("CustomizableTest",
                                         RegisterCustomTestObjects, "");
  }

  ConfigOptions config_options_;
};

// Tests that a Customizable can be created by:
//    - a simple name
//    - a XXX.id option
//    - a property with a name
TEST_F(CustomizableTest, CreateByNameTest) {
  ObjectLibrary::Default()->AddFactory<TestCustomizable>(
      ObjectLibrary::PatternEntry("TEST", false).AddSeparator("_"),
      [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new TestCustomizable(name));
        return guard->get();
      });
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique={id=TEST_1}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_1");
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique.id=TEST_2"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_2");
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique=TEST_3"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_3");
}

TEST_F(CustomizableTest, ToStringTest) {
  std::unique_ptr<TestCustomizable> custom(new TestCustomizable("test"));
  ASSERT_EQ(custom->ToString(config_options_), "test");
}

TEST_F(CustomizableTest, SimpleConfigureTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=B;string=s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(
      configurable->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(CustomizableTest, ConfigureFromPropsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  std::string mismatch;
  config_options_.delimiter = "\n";
  std::unordered_map<std::string, std::string> props;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  GetMapFromProperties(opt_str, &props);
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromMap(config_options_, props));
  ASSERT_TRUE(
      configurable->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(CustomizableTest, ConfigureFromShortTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
}

TEST_F(CustomizableTest, AreEquivalentOptionsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=A;int=1;bool=true"},
  };
  std::string mismatch;
  ConfigOptions config_options = config_options_;
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  std::unique_ptr<Configurable> c2(new SimpleConfigurable());
  ASSERT_OK(c1->ConfigureFromMap(config_options, opt_map));
  ASSERT_OK(c2->ConfigureFromMap(config_options, opt_map));
  ASSERT_TRUE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  SimpleOptions* simple = c1->GetOptions<SimpleOptions>();
  ASSERT_TRUE(
      simple->cu->AreEquivalent(config_options, simple->cs.get(), &mismatch));
  ASSERT_OK(simple->cu->ConfigureOption(config_options, "int", "2"));
  ASSERT_FALSE(
      simple->cu->AreEquivalent(config_options, simple->cs.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  ConfigOptions loosely = config_options;
  loosely.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_TRUE(simple->cu->AreEquivalent(loosely, simple->cs.get(), &mismatch));

  ASSERT_OK(c1->ConfigureOption(config_options, "shared", "id=B;string=3"));
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  ASSERT_FALSE(simple->cs->AreEquivalent(loosely, simple->cu.get(), &mismatch));
  simple->cs.reset();
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
}

// Tests that we can initialize a customizable from its options
TEST_F(CustomizableTest, ConfigureStandaloneCustomTest) {
  std::unique_ptr<TestCustomizable> base;
  std::unique_ptr<TestCustomizable> copy;
  const auto& registry = config_options_.registry;
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &base));
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &copy));
  ASSERT_OK(base->ConfigureFromString(config_options_, "int=33;bool=true"));
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
}

// Tests that we fail appropriately if the pattern is not registered
TEST_F(CustomizableTest, BadNameTest) {
  config_options_.ignore_unsupported_options = false;
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ASSERT_NOK(
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name"));
  config_options_.ignore_unsupported_options = true;
  ASSERT_OK(
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name"));
}

// Tests that we fail appropriately if a bad option is passed to the underlying
// configurable
TEST_F(CustomizableTest, BadOptionTest) {
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ConfigOptions ignore = config_options_;
  ignore.ignore_unknown_options = true;

  ASSERT_NOK(c1->ConfigureFromString(config_options_, "A.int=11"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "shared={id=B;int=1}"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared={id=A;string=s}"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "B.int=11"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "B.int=11"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "A.string=s"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "A.string=s"));
  // Test as detached
  ASSERT_NOK(
      c1->ConfigureFromString(config_options_, "shared.id=A;A.string=b}"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared.id=A;A.string=s}"));
}

TEST_F(CustomizableTest, FailingFactoryTest) {
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ConfigOptions ignore = config_options_;

  Status s;
  ignore.registry->AddLibrary("failing")->AddFactory<TestCustomizable>(
      "failing",
      [](const std::string& /*uri*/,
         std::unique_ptr<TestCustomizable>* /*guard */, std::string* errmsg) {
        *errmsg = "Bad Factory";
        return nullptr;
      });

  // If we are ignoring unknown and unsupported options, will see
  // different errors for failing versus missing
  ignore.ignore_unknown_options = false;
  ignore.ignore_unsupported_options = false;
  s = c1->ConfigureFromString(ignore, "shared.id=failing");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = c1->ConfigureFromString(ignore, "unique.id=failing");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = c1->ConfigureFromString(ignore, "shared.id=missing");
  ASSERT_TRUE(s.IsNotSupported());
  s = c1->ConfigureFromString(ignore, "unique.id=missing");
  ASSERT_TRUE(s.IsNotSupported());

  // If we are ignoring unsupported options, will see
  // errors for failing but not missing
  ignore.ignore_unknown_options = false;
  ignore.ignore_unsupported_options = true;
  s = c1->ConfigureFromString(ignore, "shared.id=failing");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = c1->ConfigureFromString(ignore, "unique.id=failing");
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(c1->ConfigureFromString(ignore, "shared.id=missing"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "unique.id=missing"));

  // If we are ignoring unknown options, will see no errors
  // for failing or missing
  ignore.ignore_unknown_options = true;
  ignore.ignore_unsupported_options = false;
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared.id=failing"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "unique.id=failing"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared.id=missing"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "unique.id=missing"));
}

// Tests that different IDs lead to different objects
TEST_F(CustomizableTest, UniqueIdTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=true}"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), std::string("A_1"));
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_2;int=1;bool=true}"));
  ASSERT_FALSE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_EQ(simple->cu->GetId(), std::string("A_2"));
}

TEST_F(CustomizableTest, IsInstanceOfTest) {
  std::shared_ptr<TestCustomizable> tc = std::make_shared<ACustomizable>("A_1");

  ASSERT_EQ(tc->GetId(), std::string("A_1"));
  ASSERT_TRUE(tc->IsInstanceOf("A"));
  ASSERT_TRUE(tc->IsInstanceOf("TestCustomizable"));
  ASSERT_FALSE(tc->IsInstanceOf("B"));
  ASSERT_FALSE(tc->IsInstanceOf("A_1"));
  ASSERT_EQ(tc->CheckedCast<ACustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<TestCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<BCustomizable>(), nullptr);

  tc.reset(new BCustomizable("B"));
  ASSERT_TRUE(tc->IsInstanceOf("B"));
  ASSERT_TRUE(tc->IsInstanceOf("TestCustomizable"));
  ASSERT_FALSE(tc->IsInstanceOf("A"));
  ASSERT_EQ(tc->CheckedCast<BCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<TestCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<ACustomizable>(), nullptr);
}

TEST_F(CustomizableTest, PrepareOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> p_option_info = {
      {"can_prepare",
       {0, OptionType::kBoolean, OptionVerificationType::kNormal,
        OptionTypeFlags::kNone}},
  };

  class PrepareCustomizable : public TestCustomizable {
   public:
    bool can_prepare_ = true;

    PrepareCustomizable() : TestCustomizable("P") {
      RegisterOptions("Prepare", &can_prepare_, &p_option_info);
    }

    Status PrepareOptions(const ConfigOptions& opts) override {
      if (!can_prepare_) {
        return Status::InvalidArgument("Cannot Prepare");
      } else {
        return TestCustomizable::PrepareOptions(opts);
      }
    }
  };

  ObjectLibrary::Default()->AddFactory<TestCustomizable>(
      "P",
      [](const std::string& /*name*/, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new PrepareCustomizable());
        return guard->get();
      });

  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ConfigOptions prepared(config_options_);
  prepared.invoke_prepare_options = true;

  ASSERT_OK(base->ConfigureFromString(
      prepared, "unique=A_1; shared={id=B;string=s}; pointer.id=S"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_NE(simple->cs, nullptr);
  ASSERT_NE(simple->cp, nullptr);
  delete simple->cp;
  base.reset(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString(
      config_options_, "unique=A_1; shared={id=B;string=s}; pointer.id=S"));

  simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_NE(simple->cs, nullptr);
  ASSERT_NE(simple->cp, nullptr);

  ASSERT_OK(base->PrepareOptions(config_options_));
  delete simple->cp;
  base.reset(new SimpleConfigurable());
  simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);

  ASSERT_NOK(
      base->ConfigureFromString(prepared, "unique={id=P; can_prepare=false}"));
  ASSERT_EQ(simple->cu, nullptr);

  ASSERT_OK(
      base->ConfigureFromString(prepared, "unique={id=P; can_prepare=true}"));
  ASSERT_NE(simple->cu, nullptr);

  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=P; can_prepare=true}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_OK(simple->cu->PrepareOptions(prepared));

  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=P; can_prepare=false}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_NOK(simple->cu->PrepareOptions(prepared));
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo> inner_option_info = {
    {"inner", OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
                  0, OptionVerificationType::kNormal,
                  OptionTypeFlags::kStringNameOnly)}};

struct InnerOptions {
  static const char* kName() { return "InnerOptions"; }
  std::shared_ptr<Customizable> inner;
};

class InnerCustomizable : public Customizable {
 public:
  explicit InnerCustomizable(const std::shared_ptr<Customizable>& w) {
    iopts_.inner = w;
    RegisterOptions(&iopts_, &inner_option_info);
  }
  static const char* kClassName() { return "Inner"; }
  const char* Name() const override { return kClassName(); }

  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return Customizable::IsInstanceOf(name);
    }
  }

 protected:
  const Customizable* Inner() const override { return iopts_.inner.get(); }

 private:
  InnerOptions iopts_;
};

struct WrappedOptions1 {
  static const char* kName() { return "WrappedOptions1"; }
  int i = 42;
};

class WrappedCustomizable1 : public InnerCustomizable {
 public:
  explicit WrappedCustomizable1(const std::shared_ptr<Customizable>& w)
      : InnerCustomizable(w) {
    RegisterOptions(&wopts_, nullptr);
  }
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Wrapped1"; }

 private:
  WrappedOptions1 wopts_;
};

struct WrappedOptions2 {
  static const char* kName() { return "WrappedOptions2"; }
  std::string s = "42";
};
class WrappedCustomizable2 : public InnerCustomizable {
 public:
  explicit WrappedCustomizable2(const std::shared_ptr<Customizable>& w)
      : InnerCustomizable(w) {}
  const void* GetOptionsPtr(const std::string& name) const override {
    if (name == WrappedOptions2::kName()) {
      return &wopts_;
    } else {
      return InnerCustomizable::GetOptionsPtr(name);
    }
  }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Wrapped2"; }

 private:
  WrappedOptions2 wopts_;
};
}  // namespace

TEST_F(CustomizableTest, WrappedInnerTest) {
  std::shared_ptr<TestCustomizable> ac =
      std::make_shared<TestCustomizable>("A");

  ASSERT_TRUE(ac->IsInstanceOf("A"));
  ASSERT_TRUE(ac->IsInstanceOf("TestCustomizable"));
  ASSERT_EQ(ac->CheckedCast<TestCustomizable>(), ac.get());
  ASSERT_EQ(ac->CheckedCast<InnerCustomizable>(), nullptr);
  ASSERT_EQ(ac->CheckedCast<WrappedCustomizable1>(), nullptr);
  ASSERT_EQ(ac->CheckedCast<WrappedCustomizable2>(), nullptr);
  std::shared_ptr<Customizable> wc1 =
      std::make_shared<WrappedCustomizable1>(ac);

  ASSERT_TRUE(wc1->IsInstanceOf(WrappedCustomizable1::kClassName()));
  ASSERT_EQ(wc1->CheckedCast<WrappedCustomizable1>(), wc1.get());
  ASSERT_EQ(wc1->CheckedCast<WrappedCustomizable2>(), nullptr);
  ASSERT_EQ(wc1->CheckedCast<InnerCustomizable>(), wc1.get());
  ASSERT_EQ(wc1->CheckedCast<TestCustomizable>(), ac.get());

  std::shared_ptr<Customizable> wc2 =
      std::make_shared<WrappedCustomizable2>(wc1);
  ASSERT_TRUE(wc2->IsInstanceOf(WrappedCustomizable2::kClassName()));
  ASSERT_EQ(wc2->CheckedCast<WrappedCustomizable2>(), wc2.get());
  ASSERT_EQ(wc2->CheckedCast<WrappedCustomizable1>(), wc1.get());
  ASSERT_EQ(wc2->CheckedCast<InnerCustomizable>(), wc2.get());
  ASSERT_EQ(wc2->CheckedCast<TestCustomizable>(), ac.get());
}

TEST_F(CustomizableTest, CustomizableInnerTest) {
  std::shared_ptr<Customizable> c =
      std::make_shared<InnerCustomizable>(std::make_shared<ACustomizable>("a"));
  std::shared_ptr<Customizable> wc1 = std::make_shared<WrappedCustomizable1>(c);
  std::shared_ptr<Customizable> wc2 = std::make_shared<WrappedCustomizable2>(c);
  auto inner = c->GetOptions<InnerOptions>();
  ASSERT_NE(inner, nullptr);

  auto aopts = c->GetOptions<AOptions>();
  ASSERT_NE(aopts, nullptr);
  ASSERT_EQ(aopts, wc1->GetOptions<AOptions>());
  ASSERT_EQ(aopts, wc2->GetOptions<AOptions>());
  auto w1opts = wc1->GetOptions<WrappedOptions1>();
  ASSERT_NE(w1opts, nullptr);
  ASSERT_EQ(c->GetOptions<WrappedOptions1>(), nullptr);
  ASSERT_EQ(wc2->GetOptions<WrappedOptions1>(), nullptr);

  auto w2opts = wc2->GetOptions<WrappedOptions2>();
  ASSERT_NE(w2opts, nullptr);
  ASSERT_EQ(c->GetOptions<WrappedOptions2>(), nullptr);
  ASSERT_EQ(wc1->GetOptions<WrappedOptions2>(), nullptr);
}

TEST_F(CustomizableTest, CopyObjectTest) {
  class CopyCustomizable : public Customizable {
   public:
    CopyCustomizable() : prepared_(0), validated_(0) {}
    const char* Name() const override { return "CopyCustomizable"; }

    Status PrepareOptions(const ConfigOptions& options) override {
      prepared_++;
      return Customizable::PrepareOptions(options);
    }
    Status ValidateOptions(const DBOptions& db_opts,
                           const ColumnFamilyOptions& cf_opts) const override {
      validated_++;
      return Customizable::ValidateOptions(db_opts, cf_opts);
    }
    int prepared_;
    mutable int validated_;
  };

  CopyCustomizable c1;
  ConfigOptions config_options;
  Options options;

  ASSERT_OK(c1.PrepareOptions(config_options));
  ASSERT_OK(c1.ValidateOptions(options, options));
  ASSERT_EQ(c1.prepared_, 1);
  ASSERT_EQ(c1.validated_, 1);
  CopyCustomizable c2 = c1;
  ASSERT_OK(c1.PrepareOptions(config_options));
  ASSERT_OK(c1.ValidateOptions(options, options));
  ASSERT_EQ(c2.prepared_, 1);
  ASSERT_EQ(c2.validated_, 1);
  ASSERT_EQ(c1.prepared_, 2);
  ASSERT_EQ(c1.validated_, 2);
}

TEST_F(CustomizableTest, TestStringDepth) {
  ConfigOptions shallow = config_options_;
  std::unique_ptr<Configurable> c(
      new InnerCustomizable(std::make_shared<ACustomizable>("a")));
  std::string opt_str;
  shallow.depth = ConfigOptions::Depth::kDepthShallow;
  ASSERT_OK(c->GetOptionString(shallow, &opt_str));
  ASSERT_EQ(opt_str, "inner=a;");
  shallow.depth = ConfigOptions::Depth::kDepthDetailed;
  ASSERT_OK(c->GetOptionString(shallow, &opt_str));
  ASSERT_NE(opt_str, "inner=a;");
}

// Tests that we only get a new customizable when it changes
TEST_F(CustomizableTest, NewUniqueCustomizableTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  A_count = 0;
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=true}"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(A_count, 1);  // Created one A
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=false}"));
  ASSERT_EQ(A_count, 2);  // Create another A_1
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique={id=}"));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 2);
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_2;int=1;bool=false}"));
  ASSERT_EQ(A_count, 3);  // Created another A
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique.id="));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique=nullptr"));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique.id=nullptr"));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 3);
}

TEST_F(CustomizableTest, NewEmptyUniqueTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_EQ(simple->cu, nullptr);
  simple->cu.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "unique={id=}"));
  ASSERT_EQ(simple->cu, nullptr);
  simple->cu.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "unique={id=nullptr}"));
  ASSERT_EQ(simple->cu, nullptr);
  simple->cu.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "unique.id="));
  ASSERT_EQ(simple->cu, nullptr);
  simple->cu.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "unique=nullptr"));
  ASSERT_EQ(simple->cu, nullptr);
  simple->cu.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "unique.id=nullptr"));
  ASSERT_EQ(simple->cu, nullptr);
}

TEST_F(CustomizableTest, NewEmptySharedTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());

  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->cs, nullptr);
  simple->cs.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "shared={id=}"));
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->cs, nullptr);
  simple->cs.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "shared={id=nullptr}"));
  ASSERT_EQ(simple->cs, nullptr);
  simple->cs.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "shared.id="));
  ASSERT_EQ(simple->cs, nullptr);
  simple->cs.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "shared.id=nullptr"));
  ASSERT_EQ(simple->cs, nullptr);
  simple->cs.reset(new BCustomizable("B"));

  ASSERT_OK(base->ConfigureFromString(config_options_, "shared=nullptr"));
  ASSERT_EQ(simple->cs, nullptr);
}

TEST_F(CustomizableTest, NewEmptyStaticTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer={id=}"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->cp, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer={id=nullptr}"));
  ASSERT_EQ(simple->cp, nullptr);

  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer="));
  ASSERT_EQ(simple->cp, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer=nullptr"));
  ASSERT_EQ(simple->cp, nullptr);

  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer.id="));
  ASSERT_EQ(simple->cp, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options_, "pointer.id=nullptr"));
  ASSERT_EQ(simple->cp, nullptr);
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo> vector_option_info = {
    {"vector",
     OptionTypeInfo::Vector<std::shared_ptr<TestCustomizable>>(
         0, OptionVerificationType::kNormal,

         OptionTypeFlags::kNone,

         OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
             0, OptionVerificationType::kNormal, OptionTypeFlags::kNone))},
};
class VectorConfigurable : public SimpleConfigurable {
 public:
  VectorConfigurable() { RegisterOptions("vector", &cv, &vector_option_info); }
  std::vector<std::shared_ptr<TestCustomizable>> cv;
};
}  // namespace

TEST_F(CustomizableTest, VectorConfigTest) {
  VectorConfigurable orig;
  VectorConfigurable copy;
  std::shared_ptr<TestCustomizable> c1;
  std::shared_ptr<TestCustomizable> c2;
  ASSERT_OK(TestCustomizable::CreateFromString(config_options_, "A", &c1));
  ASSERT_OK(TestCustomizable::CreateFromString(config_options_, "B", &c2));
  orig.cv.push_back(c1);
  orig.cv.push_back(c2);
  ASSERT_OK(orig.ConfigureFromString(config_options_, "unique=A2"));
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(orig.GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(orig.AreEquivalent(config_options_, &copy, &mismatch));
}

TEST_F(CustomizableTest, NoNameTest) {
  // If Customizables are created without names, they are not
  // part of the serialization (since they cannot be recreated)
  VectorConfigurable orig;
  VectorConfigurable copy;
  auto sopts = orig.GetOptions<SimpleOptions>();
  auto copts = copy.GetOptions<SimpleOptions>();
  sopts->cu.reset(new ACustomizable(""));
  orig.cv.push_back(std::make_shared<ACustomizable>(""));
  orig.cv.push_back(std::make_shared<ACustomizable>("A_1"));
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(orig.GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(config_options_, opt_str));
  ASSERT_EQ(copy.cv.size(), 1U);
  ASSERT_EQ(copy.cv[0]->GetId(), "A_1");
  ASSERT_EQ(copts->cu, nullptr);
}

TEST_F(CustomizableTest, IgnoreUnknownObjects) {
  ConfigOptions ignore = config_options_;
  std::shared_ptr<TestCustomizable> shared;
  std::unique_ptr<TestCustomizable> unique;
  TestCustomizable* pointer = nullptr;
  ignore.ignore_unsupported_options = false;
  ASSERT_NOK(LoadSharedObject<TestCustomizable>(ignore, "Unknown", &shared));
  ASSERT_NOK(LoadUniqueObject<TestCustomizable>(ignore, "Unknown", &unique));
  ASSERT_NOK(LoadStaticObject<TestCustomizable>(ignore, "Unknown", &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ignore.ignore_unsupported_options = true;
  ASSERT_OK(LoadSharedObject<TestCustomizable>(ignore, "Unknown", &shared));
  ASSERT_OK(LoadUniqueObject<TestCustomizable>(ignore, "Unknown", &unique));
  ASSERT_OK(LoadStaticObject<TestCustomizable>(ignore, "Unknown", &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_OK(LoadSharedObject<TestCustomizable>(ignore, "id=Unknown", &shared));
  ASSERT_OK(LoadUniqueObject<TestCustomizable>(ignore, "id=Unknown", &unique));
  ASSERT_OK(LoadStaticObject<TestCustomizable>(ignore, "id=Unknown", &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_OK(LoadSharedObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               &shared));
  ASSERT_OK(LoadUniqueObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               &unique));
  ASSERT_OK(LoadStaticObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
}

TEST_F(CustomizableTest, URLFactoryTest) {
  std::unique_ptr<TestCustomizable> unique;
  config_options_.registry->AddLibrary("URL")->AddFactory<TestCustomizable>(
      ObjectLibrary::PatternEntry("Z", false).AddSeparator(""),
      [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new TestCustomizable(name));
        return guard->get();
      });

  ConfigOptions ignore = config_options_;
  ignore.ignore_unsupported_options = false;
  ignore.ignore_unsupported_options = false;
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "Z=1;x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "Z=1;x=y");
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "Z;x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "Z;x=y");
  unique.reset();
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "Z=1?x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "Z=1?x=y");
}

TEST_F(CustomizableTest, MutableOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> mutable_option_info = {
      {"mutable",
       OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
           0, OptionVerificationType::kNormal, OptionTypeFlags::kMutable)}};
  static std::unordered_map<std::string, OptionTypeInfo> immutable_option_info =
      {{"immutable",
        OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
            0, OptionVerificationType::kNormal, OptionTypeFlags::kAllowNull)}};

  class MutableCustomizable : public Customizable {
   private:
    std::shared_ptr<TestCustomizable> mutable_;
    std::shared_ptr<TestCustomizable> immutable_;

   public:
    MutableCustomizable() {
      RegisterOptions("mutable", &mutable_, &mutable_option_info);
      RegisterOptions("immutable", &immutable_, &immutable_option_info);
    }
    const char* Name() const override { return "MutableCustomizable"; }
  };
  MutableCustomizable mc;
  MutableCustomizable mc2;
  std::string mismatch;
  std::string opt_str;

  ConfigOptions options = config_options_;
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{id=B;}"));
  options.mutable_options_only = true;
  ASSERT_OK(mc.GetOptionString(options, &opt_str));
  ASSERT_OK(mc2.ConfigureFromString(options, opt_str));
  ASSERT_TRUE(mc.AreEquivalent(options, &mc2, &mismatch));

  options.mutable_options_only = false;
  ASSERT_OK(mc.ConfigureOption(options, "immutable", "{id=A; int=10}"));
  auto* mm = mc.GetOptions<std::shared_ptr<TestCustomizable>>("mutable");
  auto* im = mc.GetOptions<std::shared_ptr<TestCustomizable>>("immutable");
  ASSERT_NE(mm, nullptr);
  ASSERT_NE(mm->get(), nullptr);
  ASSERT_NE(im, nullptr);
  ASSERT_NE(im->get(), nullptr);

  // Now only deal with mutable options
  options.mutable_options_only = true;

  // Setting nested immutable customizable options fails
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "{id=B;}"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable.id", "B"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable.bool", "true"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "bool=true"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "{int=11;bool=true}"));
  auto* im_a = im->get()->GetOptions<AOptions>("A");
  ASSERT_NE(im_a, nullptr);
  ASSERT_EQ(im_a->i, 10);
  ASSERT_EQ(im_a->b, false);

  // Setting nested mutable customizable options succeeds but the object did not
  // change
  ASSERT_OK(mc.ConfigureOption(options, "immutable.int", "11"));
  ASSERT_EQ(im_a->i, 11);
  ASSERT_EQ(im_a, im->get()->GetOptions<AOptions>("A"));

  // The mutable configurable itself can be changed
  ASSERT_OK(mc.ConfigureOption(options, "mutable.id", "A"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "A"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{id=A}"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{bool=true}"));

  // The Nested options in the mutable object can be changed
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{bool=true}"));
  auto* mm_a = mm->get()->GetOptions<AOptions>("A");
  ASSERT_EQ(mm_a->b, true);
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{int=22;bool=false}"));
  mm_a = mm->get()->GetOptions<AOptions>("A");
  ASSERT_EQ(mm_a->i, 22);
  ASSERT_EQ(mm_a->b, false);

  // Only the mutable options should get serialized
  options.mutable_options_only = false;
  ASSERT_OK(mc.GetOptionString(options, &opt_str));
  ASSERT_OK(mc.ConfigureOption(options, "immutable", "{id=B;}"));
  options.mutable_options_only = true;

  ASSERT_OK(mc.GetOptionString(options, &opt_str));
  ASSERT_OK(mc2.ConfigureFromString(options, opt_str));
  ASSERT_TRUE(mc.AreEquivalent(options, &mc2, &mismatch));
  options.mutable_options_only = false;
  ASSERT_FALSE(mc.AreEquivalent(options, &mc2, &mismatch));
  ASSERT_EQ(mismatch, "immutable");
}

TEST_F(CustomizableTest, CustomManagedObjects) {
  std::shared_ptr<TestCustomizable> object1;
  std::shared_ptr<TestCustomizable> object2;
  ASSERT_OK(LoadManagedObject<TestCustomizable>(
      config_options_, "id=A_1;int=1;bool=true", &object1));
  ASSERT_NE(object1, nullptr);
  ASSERT_OK(
      LoadManagedObject<TestCustomizable>(config_options_, "A_1", &object2));
  ASSERT_EQ(object1, object2);
  auto* opts = object2->GetOptions<AOptions>("A");
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->i, 1);
  ASSERT_EQ(opts->b, true);
  ASSERT_OK(
      LoadManagedObject<TestCustomizable>(config_options_, "A_2", &object2));
  ASSERT_NE(object1, object2);
  object1.reset();
  ASSERT_OK(LoadManagedObject<TestCustomizable>(
      config_options_, "id=A_1;int=2;bool=false", &object1));
  opts = object1->GetOptions<AOptions>("A");
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->i, 2);
  ASSERT_EQ(opts->b, false);
}

TEST_F(CustomizableTest, CreateManagedObjects) {
  class ManagedCustomizable : public Customizable {
   public:
    static const char* Type() { return "ManagedCustomizable"; }
    static const char* kClassName() { return "Managed"; }
    const char* Name() const override { return kClassName(); }
    std::string GetId() const override { return id_; }
    ManagedCustomizable() { id_ = GenerateIndividualId(); }
    static Status CreateFromString(
        const ConfigOptions& opts, const std::string& value,
        std::shared_ptr<ManagedCustomizable>* result) {
      return LoadManagedObject<ManagedCustomizable>(opts, value, result);
    }

   private:
    std::string id_;
  };

  config_options_.registry->AddLibrary("Managed")
      ->AddFactory<ManagedCustomizable>(
          ObjectLibrary::PatternEntry::AsIndividualId(
              ManagedCustomizable::kClassName()),
          [](const std::string& /*name*/,
             std::unique_ptr<ManagedCustomizable>* guard,
             std::string* /* msg */) {
            guard->reset(new ManagedCustomizable());
            return guard->get();
          });

  std::shared_ptr<ManagedCustomizable> mc1;
  std::shared_ptr<ManagedCustomizable> mc2;
  std::shared_ptr<ManagedCustomizable> mc3;
  std::shared_ptr<ManagedCustomizable> obj;
  // Create a "deadbeef" customizable
  std::string deadbeef =
      std::string(ManagedCustomizable::kClassName()) + "@0xdeadbeef#0001";
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, deadbeef, &mc1));
  // Create an object with the base/class name
  ASSERT_OK(ManagedCustomizable::CreateFromString(
      config_options_, ManagedCustomizable::kClassName(), &mc2));
  // Creating another with the base name returns a different object
  ASSERT_OK(ManagedCustomizable::CreateFromString(
      config_options_, ManagedCustomizable::kClassName(), &mc3));
  // At this point, there should be 4 managed objects (deadbeef, mc1, 2, and 3)
  std::vector<std::shared_ptr<ManagedCustomizable>> objects;
  ASSERT_OK(config_options_.registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 4U);
  objects.clear();
  // Three separate object, none of them equal
  ASSERT_NE(mc1, mc2);
  ASSERT_NE(mc1, mc3);
  ASSERT_NE(mc2, mc3);

  // Creating another object with "deadbeef" object
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, deadbeef, &obj));
  ASSERT_EQ(mc1, obj);
  // Create another with the IDs of the instances
  ASSERT_OK(ManagedCustomizable::CreateFromString(config_options_, mc1->GetId(),
                                                  &obj));
  ASSERT_EQ(mc1, obj);
  ASSERT_OK(ManagedCustomizable::CreateFromString(config_options_, mc2->GetId(),
                                                  &obj));
  ASSERT_EQ(mc2, obj);
  ASSERT_OK(ManagedCustomizable::CreateFromString(config_options_, mc3->GetId(),
                                                  &obj));
  ASSERT_EQ(mc3, obj);

  // Now get rid of deadbeef.  2 Objects left (m2+m3)
  mc1.reset();
  ASSERT_EQ(
      config_options_.registry->GetManagedObject<ManagedCustomizable>(deadbeef),
      nullptr);
  ASSERT_OK(config_options_.registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 2U);
  objects.clear();

  // Associate deadbeef with #2
  ASSERT_OK(config_options_.registry->SetManagedObject(deadbeef, mc2));
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, deadbeef, &obj));
  ASSERT_EQ(mc2, obj);
  obj.reset();

  // Get the ID of mc2 and then reset it.  1 Object left
  std::string mc2id = mc2->GetId();
  mc2.reset();
  ASSERT_EQ(
      config_options_.registry->GetManagedObject<ManagedCustomizable>(mc2id),
      nullptr);
  ASSERT_OK(config_options_.registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 1U);
  objects.clear();

  // Create another object with the old mc2id.
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, mc2id, &mc2));
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, mc2id, &obj));
  ASSERT_EQ(mc2, obj);

  // For good measure, create another deadbeef object
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, deadbeef, &mc1));
  ASSERT_OK(
      ManagedCustomizable::CreateFromString(config_options_, deadbeef, &obj));
  ASSERT_EQ(mc1, obj);
}

namespace {
class TestSecondaryCache : public SecondaryCache {
 public:
  static const char* kClassName() { return "Test"; }
  const char* Name() const override { return kClassName(); }
  Status Insert(const Slice& /*key*/, Cache::ObjectPtr /*value*/,
                const Cache::CacheItemHelper* /*helper*/,
                bool /*force_insert*/) override {
    return Status::NotSupported();
  }
  Status InsertSaved(const Slice& /*key*/, const Slice& /*saved*/,
                     CompressionType /*type*/, CacheTier /*source*/) override {
    return Status::OK();
  }
  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& /*key*/, const Cache::CacheItemHelper* /*helper*/,
      Cache::CreateContext* /*create_context*/, bool /*wait*/,
      bool /*advise_erase*/, Statistics* /*stats*/,
      bool& kept_in_sec_cache) override {
    kept_in_sec_cache = true;
    return nullptr;
  }

  bool SupportForceErase() const override { return false; }

  void Erase(const Slice& /*key*/) override {}

  // Wait for a collection of handles to become ready
  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}

  std::string GetPrintableOptions() const override { return ""; }
};

class TestStatistics : public StatisticsImpl {
 public:
  TestStatistics() : StatisticsImpl(nullptr) {}
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Test"; }
};

class TestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  TestFlushBlockPolicyFactory() = default;

  static const char* kClassName() { return "TestFlushBlockPolicyFactory"; }
  const char* Name() const override { return kClassName(); }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& /*data_block_builder*/) const override {
    return nullptr;
  }
};

class MockSliceTransform : public SliceTransform {
 public:
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Mock"; }

  Slice Transform(const Slice& /*key*/) const override { return Slice(); }

  bool InDomain(const Slice& /*key*/) const override { return false; }
};

class MockMemoryAllocator : public BaseMemoryAllocator {
 public:
  static const char* kClassName() { return "MockMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }
};

class MockEncryptionProvider : public EncryptionProvider {
 public:
  explicit MockEncryptionProvider(const std::string& id) : id_(id) {}
  static const char* kClassName() { return "Mock"; }
  const char* Name() const override { return kClassName(); }
  size_t GetPrefixLength() const override { return 0; }
  Status CreateNewPrefix(const std::string& /*fname*/, char* /*prefix*/,
                         size_t /*prefixLength*/) const override {
    return Status::NotSupported();
  }

  Status AddCipher(const std::string& /*descriptor*/, const char* /*cipher*/,
                   size_t /*len*/, bool /*for_write*/) override {
    return Status::NotSupported();
  }

  Status CreateCipherStream(
      const std::string& /*fname*/, const EnvOptions& /*options*/,
      Slice& /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream>* /*result*/) override {
    return Status::NotSupported();
  }
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    if (EndsWith(id_, "://test")) {
      return EncryptionProvider::ValidateOptions(db_opts, cf_opts);
    } else {
      return Status::InvalidArgument("MockProvider not initialized");
    }
  }

 private:
  std::string id_;
};

class MockCipher : public BlockCipher {
 public:
  const char* Name() const override { return "Mock"; }
  size_t BlockSize() override { return 0; }
  Status Encrypt(char* /*data*/) override { return Status::NotSupported(); }
  Status Decrypt(char* data) override { return Encrypt(data); }
};

class DummyFileSystem : public FileSystemWrapper {
 public:
  explicit DummyFileSystem(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  static const char* kClassName() { return "DummyFileSystem"; }
  const char* Name() const override { return kClassName(); }
};

class MockTablePropertiesCollectorFactory
    : public TablePropertiesCollectorFactory {
 private:
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return nullptr;
  }
  static const char* kClassName() { return "Mock"; }
  const char* Name() const override { return kClassName(); }
};

class MockSstPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() { return "Mock"; }
  const char* Name() const override { return kClassName(); }
  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return nullptr;
  }
};

class MockFileChecksumGenFactory : public FileChecksumGenFactory {
 public:
  static const char* kClassName() { return "Mock"; }
  const char* Name() const override { return kClassName(); }
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& /*context*/) override {
    return nullptr;
  }
};

class MockFilterPolicy : public FilterPolicy {
 public:
  static const char* kClassName() { return "MockFilterPolicy"; }
  const char* Name() const override { return kClassName(); }
  const char* CompatibilityName() const override { return Name(); }
  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const override {
    return nullptr;
  }
  FilterBitsReader* GetFilterBitsReader(
      const Slice& /*contents*/) const override {
    return nullptr;
  }
};

class MockCache : public CacheWrapper {
 public:
  static const char* kClassName() { return "MockCache"; }
  const char* Name() const override { return kClassName(); }

  MockCache()
      : CacheWrapper(NewLRUCache(LRUCacheOptions(100, 0, false, 0.0))) {}

  bool IsInstanceOf(const std::string& name) const override {
    return name.find(Name()) == 0;
  }
};

static int RegisterLocalObjects(ObjectLibrary& library,
                                const std::string& /*arg*/) {
  size_t num_types;
  library.AddFactory<TableFactory>(
      mock::MockTableFactory::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new mock::MockTableFactory());
        return guard->get();
      });
  library.AddFactory<EventListener>(
      OnFileDeletionListener::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<EventListener>* guard,
         std::string* /* errmsg */) {
        guard->reset(new OnFileDeletionListener());
        return guard->get();
      });
  library.AddFactory<EventListener>(
      FlushCounterListener::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<EventListener>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FlushCounterListener());
        return guard->get();
      });
  // Load any locally defined objects here
  library.AddFactory<const SliceTransform>(
      MockSliceTransform::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const SliceTransform>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockSliceTransform());
        return guard->get();
      });
  library.AddFactory<Statistics>(
      TestStatistics::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Statistics>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestStatistics());
        return guard->get();
      });

  library.AddFactory<EncryptionProvider>(
      ObjectLibrary::PatternEntry(MockEncryptionProvider::kClassName(), true)
          .AddSuffix("://test"),
      [](const std::string& uri, std::unique_ptr<EncryptionProvider>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockEncryptionProvider(uri));
        return guard->get();
      });
  library.AddFactory<BlockCipher>(
      "Mock",
      [](const std::string& /*uri*/, std::unique_ptr<BlockCipher>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockCipher());
        return guard->get();
      });
  library.AddFactory<MemoryAllocator>(
      MockMemoryAllocator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockMemoryAllocator());
        return guard->get();
      });
  library.AddFactory<FlushBlockPolicyFactory>(
      TestFlushBlockPolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestFlushBlockPolicyFactory());
        return guard->get();
      });

  library.AddFactory<SecondaryCache>(
      TestSecondaryCache::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<SecondaryCache>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestSecondaryCache());
        return guard->get();
      });

  library.AddFactory<FileSystem>(
      DummyFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /* errmsg */) {
        guard->reset(new DummyFileSystem(nullptr));
        return guard->get();
      });

  library.AddFactory<SstPartitionerFactory>(
      MockSstPartitionerFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<SstPartitionerFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockSstPartitionerFactory());
        return guard->get();
      });

  library.AddFactory<FileChecksumGenFactory>(
      MockFileChecksumGenFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FileChecksumGenFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockFileChecksumGenFactory());
        return guard->get();
      });

  library.AddFactory<TablePropertiesCollectorFactory>(
      MockTablePropertiesCollectorFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<TablePropertiesCollectorFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockTablePropertiesCollectorFactory());
        return guard->get();
      });

  library.AddFactory<const FilterPolicy>(
      MockFilterPolicy::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockFilterPolicy());
        return guard->get();
      });

  library.AddFactory<Cache>(
      ObjectLibrary::PatternEntry(MockCache::kClassName())
          .AddSeparator("://", /*at_least_one=*/false),
      [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockCache());
        return guard->get();
      });

  return static_cast<int>(library.GetFactoryCount(&num_types));
}
}  // namespace

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() {
    config_options_.ignore_unsupported_options = false;
    config_options_.invoke_prepare_options = false;
  }
  bool RegisterTests(const std::string& arg) {
    config_options_.registry->AddLibrary("custom-tests",
                                         test::RegisterTestObjects, arg);
    config_options_.registry->AddLibrary("local-tests", RegisterLocalObjects,
                                         arg);
    return true;
  }

  template <typename T, typename U>
  Status TestCreateStatic(const std::string& name, U** result,
                          bool delete_result = false) {
    Status s = T::CreateFromString(config_options_, name, result);
    if (s.ok()) {
      EXPECT_NE(*result, nullptr);
      EXPECT_TRUE(*result != nullptr && (*result)->IsInstanceOf(name));
    }
    if (delete_result) {
      delete *result;
      *result = nullptr;
    }
    return s;
  }

  template <typename T, typename U>
  std::shared_ptr<U> ExpectCreateShared(const std::string& name,
                                        std::shared_ptr<U>* object) {
    EXPECT_OK(T::CreateFromString(config_options_, name, object));
    EXPECT_NE(object->get(), nullptr);
    EXPECT_TRUE(object->get()->IsInstanceOf(name));
    return *object;
  }

  template <typename T>
  std::shared_ptr<T> ExpectCreateShared(const std::string& name) {
    std::shared_ptr<T> result;
    return ExpectCreateShared<T>(name, &result);
  }

  template <typename T, typename U>
  Status TestExpectedBuiltins(
      const std::string& mock, const std::unordered_set<std::string>& expected,
      std::shared_ptr<U>* object, std::vector<std::string>* failed,
      const std::function<std::vector<std::string>(const std::string&)>& alt =
          nullptr) {
    std::unordered_set<std::string> factories = expected;
    Status s = T::CreateFromString(config_options_, mock, object);
    EXPECT_NOK(s);
    std::vector<std::string> builtins;
    ObjectLibrary::Default()->GetFactoryNames(T::Type(), &builtins);
    factories.insert(builtins.begin(), builtins.end());
    Status result;
    int created = 0;
    for (const auto& name : factories) {
      created++;
      s = T::CreateFromString(config_options_, name, object);
      if (!s.ok() && alt != nullptr) {
        for (const auto& alt_name : alt(name)) {
          s = T::CreateFromString(config_options_, alt_name, object);
          if (s.ok()) {
            break;
          }
        }
      }
      if (!s.ok()) {
        result = s;
        failed->push_back(name);
      } else {
        EXPECT_NE(object->get(), nullptr);
        EXPECT_TRUE(object->get()->IsInstanceOf(name));
      }
    }
    std::vector<std::string> plugins;
    ObjectRegistry::Default()->GetFactoryNames(T::Type(), &plugins);
    if (plugins.size() > builtins.size()) {
      for (const auto& name : plugins) {
        if (factories.find(name) == factories.end()) {
          created++;
          s = T::CreateFromString(config_options_, name, object);
          if (!s.ok() && alt != nullptr) {
            for (const auto& alt_name : alt(name)) {
              s = T::CreateFromString(config_options_, alt_name, object);
              if (s.ok()) {
                break;
              }
            }
          }
          if (!s.ok()) {
            failed->push_back(name);
            if (result.ok()) {
              result = s;
            }
            printf("%s: Failed creating plugin[%s]: %s\n", T::Type(),
                   name.c_str(), s.ToString().c_str());
          } else if (object->get() == nullptr ||
                     !object->get()->IsInstanceOf(name)) {
            failed->push_back(name);
            printf("%s: Invalid plugin[%s]\n", T::Type(), name.c_str());
          }
        }
      }
    }
    printf("%s: Created %d (expected+builtins+plugins %d+%d+%d) %d Failed\n",
           T::Type(), created, (int)expected.size(),
           (int)(factories.size() - expected.size()),
           (int)(plugins.size() - builtins.size()), (int)failed->size());
    return result;
  }

  template <typename T>
  Status TestSharedBuiltins(const std::string& mock,
                            const std::string& expected,
                            std::vector<std::string>* failed = nullptr) {
    std::unordered_set<std::string> values;
    if (!expected.empty()) {
      values.insert(expected);
    }
    std::shared_ptr<T> object;
    if (failed != nullptr) {
      return TestExpectedBuiltins<T>(mock, values, &object, failed);
    } else {
      std::vector<std::string> failures;
      Status s = TestExpectedBuiltins<T>(mock, values, &object, &failures);
      EXPECT_EQ(0U, failures.size());
      return s;
    }
  }

  template <typename T, typename U>
  Status TestStaticBuiltins(const std::string& mock, U** object,
                            const std::unordered_set<std::string>& expected,
                            std::vector<std::string>* failed,
                            bool delete_objects = false) {
    std::unordered_set<std::string> factories = expected;
    Status s = TestCreateStatic<T>(mock, object, delete_objects);
    EXPECT_NOK(s);
    std::vector<std::string> builtins;
    ObjectLibrary::Default()->GetFactoryNames(T::Type(), &builtins);
    factories.insert(builtins.begin(), builtins.end());
    int created = 0;
    Status result;
    for (const auto& name : factories) {
      created++;
      s = TestCreateStatic<T>(name, object, delete_objects);
      if (!s.ok()) {
        result = s;
        failed->push_back(name);
      }
    }
    std::vector<std::string> plugins;
    ObjectRegistry::Default()->GetFactoryNames(T::Type(), &plugins);
    if (plugins.size() > builtins.size()) {
      for (const auto& name : plugins) {
        if (factories.find(name) == factories.end()) {
          created++;
          s = T::CreateFromString(config_options_, name, object);
          if (!s.ok() || *object == nullptr ||
              !((*object)->IsInstanceOf(name))) {
            failed->push_back(name);
            if (result.ok() && !s.ok()) {
              result = s;
            }
            printf("%s: Failed creating plugin[%s]: %s\n", T::Type(),
                   name.c_str(), s.ToString().c_str());
          }
          if (delete_objects) {
            delete *object;
            *object = nullptr;
          }
        }
      }
    }
    printf("%s: Created %d (expected+builtins+plugins %d+%d+%d) %d Failed\n",
           T::Type(), created, (int)expected.size(),
           (int)(factories.size() - expected.size()),
           (int)(plugins.size() - builtins.size()), (int)failed->size());
    return result;
  }

 protected:
  DBOptions db_opts_;
  ColumnFamilyOptions cf_opts_;
  ConfigOptions config_options_;
};

TEST_F(LoadCustomizableTest, LoadTableFactoryTest) {
  ASSERT_OK(
      TestSharedBuiltins<TableFactory>(mock::MockTableFactory::kClassName(),
                                       TableFactory::kBlockBasedTableName()));
  std::string opts_str = "table_factory=";
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      opts_str + TableFactory::kBlockBasedTableName(), &cf_opts_));
  ASSERT_NE(cf_opts_.table_factory.get(), nullptr);
  ASSERT_STREQ(cf_opts_.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
  if (RegisterTests("Test")) {
    ExpectCreateShared<TableFactory>(mock::MockTableFactory::kClassName());
    ASSERT_OK(GetColumnFamilyOptionsFromString(
        config_options_, cf_opts_,
        opts_str + mock::MockTableFactory::kClassName(), &cf_opts_));
    ASSERT_NE(cf_opts_.table_factory.get(), nullptr);
    ASSERT_STREQ(cf_opts_.table_factory->Name(),
                 mock::MockTableFactory::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadFileSystemTest) {
  ASSERT_OK(TestSharedBuiltins<FileSystem>(DummyFileSystem::kClassName(),
                                           FileSystem::kDefaultName()));
  if (RegisterTests("Test")) {
    auto fs = ExpectCreateShared<FileSystem>(DummyFileSystem::kClassName());
    ASSERT_FALSE(fs->IsInstanceOf(FileSystem::kDefaultName()));
  }
}

TEST_F(LoadCustomizableTest, LoadSecondaryCacheTest) {
  ASSERT_OK(
      TestSharedBuiltins<SecondaryCache>(TestSecondaryCache::kClassName(), ""));
  if (RegisterTests("Test")) {
    ExpectCreateShared<SecondaryCache>(TestSecondaryCache::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadSstPartitionerFactoryTest) {
  ASSERT_OK(TestSharedBuiltins<SstPartitionerFactory>(
      "Mock", SstPartitionerFixedPrefixFactory::kClassName()));
  if (RegisterTests("Test")) {
    ExpectCreateShared<SstPartitionerFactory>("Mock");
  }
}

TEST_F(LoadCustomizableTest, LoadChecksumGenFactoryTest) {
  ASSERT_OK(TestSharedBuiltins<FileChecksumGenFactory>("Mock", ""));
  if (RegisterTests("Test")) {
    ExpectCreateShared<FileChecksumGenFactory>("Mock");
  }
}

TEST_F(LoadCustomizableTest, LoadTablePropertiesCollectorFactoryTest) {
  ASSERT_OK(TestSharedBuiltins<TablePropertiesCollectorFactory>(
      MockTablePropertiesCollectorFactory::kClassName(), ""));
  if (RegisterTests("Test")) {
    ExpectCreateShared<TablePropertiesCollectorFactory>(
        MockTablePropertiesCollectorFactory::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadComparatorTest) {
  const Comparator* bytewise = BytewiseComparator();
  const Comparator* reverse = ReverseBytewiseComparator();
  const Comparator* result = nullptr;
  std::unordered_set<std::string> expected = {bytewise->Name(),
                                              reverse->Name()};
  std::vector<std::string> failures;
  ASSERT_OK(TestStaticBuiltins<Comparator>(
      test::SimpleSuffixReverseComparator::kClassName(), &result, expected,
      &failures));
  if (RegisterTests("Test")) {
    ASSERT_OK(TestCreateStatic<Comparator>(
        test::SimpleSuffixReverseComparator::kClassName(), &result));
  }
}

TEST_F(LoadCustomizableTest, LoadSliceTransformFactoryTest) {
  std::shared_ptr<const SliceTransform> result;
  std::vector<std::string> failures;
  std::unordered_set<std::string> expected = {"rocksdb.Noop", "fixed",
                                              "rocksdb.FixedPrefix", "capped",
                                              "rocksdb.CappedPrefix"};
  ASSERT_OK(TestExpectedBuiltins<SliceTransform>(
      "Mock", expected, &result, &failures, [](const std::string& name) {
        std::vector<std::string> names = {name + ":22", name + ".22"};
        return names;
      }));
  ASSERT_OK(SliceTransform::CreateFromString(
      config_options_, "rocksdb.FixedPrefix.22", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf("fixed"));
  ASSERT_OK(SliceTransform::CreateFromString(
      config_options_, "rocksdb.CappedPrefix.22", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf("capped"));
  if (RegisterTests("Test")) {
    ExpectCreateShared<SliceTransform>("Mock", &result);
  }
}

TEST_F(LoadCustomizableTest, LoadStatisticsTest) {
  ASSERT_OK(TestSharedBuiltins<Statistics>(TestStatistics::kClassName(),
                                           "BasicStatistics"));
  // Empty will create a default BasicStatistics
  ASSERT_OK(
      Statistics::CreateFromString(config_options_, "", &db_opts_.statistics));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_STREQ(db_opts_.statistics->Name(), "BasicStatistics");

  ASSERT_NOK(GetDBOptionsFromString(config_options_, db_opts_,
                                    "statistics=Test", &db_opts_));
  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_,
                                   "statistics=BasicStatistics", &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_STREQ(db_opts_.statistics->Name(), "BasicStatistics");

  if (RegisterTests("test")) {
    auto stats = ExpectCreateShared<Statistics>(TestStatistics::kClassName());

    ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_,
                                     "statistics=Test", &db_opts_));
    ASSERT_NE(db_opts_.statistics, nullptr);
    ASSERT_STREQ(db_opts_.statistics->Name(), TestStatistics::kClassName());

    ASSERT_OK(GetDBOptionsFromString(
        config_options_, db_opts_, "statistics={id=Test;inner=BasicStatistics}",
        &db_opts_));
    ASSERT_NE(db_opts_.statistics, nullptr);
    ASSERT_STREQ(db_opts_.statistics->Name(), TestStatistics::kClassName());
    auto* inner = db_opts_.statistics->GetOptions<std::shared_ptr<Statistics>>(
        "StatisticsOptions");
    ASSERT_NE(inner, nullptr);
    ASSERT_NE(inner->get(), nullptr);
    ASSERT_STREQ(inner->get()->Name(), "BasicStatistics");

    ASSERT_OK(Statistics::CreateFromString(
        config_options_, "id=BasicStatistics;inner=Test", &stats));
    ASSERT_NE(stats, nullptr);
    ASSERT_STREQ(stats->Name(), "BasicStatistics");
    inner = stats->GetOptions<std::shared_ptr<Statistics>>("StatisticsOptions");
    ASSERT_NE(inner, nullptr);
    ASSERT_NE(inner->get(), nullptr);
    ASSERT_STREQ(inner->get()->Name(), TestStatistics::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadMemTableRepFactoryTest) {
  std::unordered_set<std::string> expected = {
      SkipListFactory::kClassName(),
      SkipListFactory::kNickName(),
  };

  std::vector<std::string> failures;
  std::shared_ptr<MemTableRepFactory> factory;
  Status s = TestExpectedBuiltins<MemTableRepFactory>(
      "SpecialSkipListFactory", expected, &factory, &failures);
  // There is a "cuckoo" factory registered that we expect to fail.  Ignore the
  // error if this is the one
  if (s.ok() || failures.size() > 1 || failures[0] != "cuckoo") {
    ASSERT_OK(s);
  }
  if (RegisterTests("Test")) {
    ExpectCreateShared<MemTableRepFactory>("SpecialSkipListFactory");
  }
}

TEST_F(LoadCustomizableTest, LoadMergeOperatorTest) {
  std::shared_ptr<MergeOperator> result;
  std::vector<std::string> failed;
  std::unordered_set<std::string> expected = {
      "put", "put_v1",      "PutOperator", "uint64add", "UInt64AddOperator",
      "max", "MaxOperator",
  };
  expected.insert({
      StringAppendOperator::kClassName(),
      StringAppendOperator::kNickName(),
      StringAppendTESTOperator::kClassName(),
      StringAppendTESTOperator::kNickName(),
      SortList::kClassName(),
      SortList::kNickName(),
      BytesXOROperator::kClassName(),
      BytesXOROperator::kNickName(),
  });

  ASSERT_OK(TestExpectedBuiltins<MergeOperator>("Changling", expected, &result,
                                                &failed));
  if (RegisterTests("Test")) {
    ExpectCreateShared<MergeOperator>("Changling");
  }
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterFactoryTest) {
  ASSERT_OK(TestSharedBuiltins<CompactionFilterFactory>("Changling", ""));
  if (RegisterTests("Test")) {
    ExpectCreateShared<CompactionFilterFactory>("Changling");
  }
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterTest) {
  const CompactionFilter* result = nullptr;
  std::vector<std::string> failures;
  ASSERT_OK(TestStaticBuiltins<CompactionFilter>("Changling", &result, {},
                                                 &failures, true));
  if (RegisterTests("Test")) {
    ASSERT_OK(TestCreateStatic<CompactionFilter>("Changling", &result, true));
  }
}

TEST_F(LoadCustomizableTest, LoadEventListenerTest) {
  ASSERT_OK(TestSharedBuiltins<EventListener>(
      OnFileDeletionListener::kClassName(), ""));
  if (RegisterTests("Test")) {
    ExpectCreateShared<EventListener>(OnFileDeletionListener::kClassName());
    ExpectCreateShared<EventListener>(FlushCounterListener::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadEncryptionProviderTest) {
  std::vector<std::string> failures;
  std::shared_ptr<EncryptionProvider> result;
  ASSERT_OK(
      TestExpectedBuiltins<EncryptionProvider>("Mock", {}, &result, &failures));
  if (!failures.empty()) {
    ASSERT_EQ(failures[0], "1://test");
    ASSERT_EQ(failures.size(), 1U);
  }

  result = ExpectCreateShared<EncryptionProvider>("CTR");
  ASSERT_NOK(result->ValidateOptions(db_opts_, cf_opts_));
  ASSERT_OK(EncryptionProvider::CreateFromString(config_options_, "CTR://test",
                                                 &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), "CTR");
  ASSERT_OK(result->ValidateOptions(db_opts_, cf_opts_));

  if (RegisterTests("Test")) {
    ExpectCreateShared<EncryptionProvider>("Mock");
    ASSERT_OK(EncryptionProvider::CreateFromString(config_options_,
                                                   "Mock://test", &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), "Mock");
    ASSERT_OK(result->ValidateOptions(db_opts_, cf_opts_));
  }
}

TEST_F(LoadCustomizableTest, LoadEncryptionCipherTest) {
  ASSERT_OK(TestSharedBuiltins<BlockCipher>("Mock", "ROT13"));
  if (RegisterTests("Test")) {
    ExpectCreateShared<BlockCipher>("Mock");
  }
}

TEST_F(LoadCustomizableTest, LoadSystemClockTest) {
  ASSERT_OK(TestSharedBuiltins<SystemClock>(MockSystemClock::kClassName(),
                                            SystemClock::kDefaultName()));
  if (RegisterTests("Test")) {
    auto result =
        ExpectCreateShared<SystemClock>(MockSystemClock::kClassName());
    ASSERT_FALSE(result->IsInstanceOf(SystemClock::kDefaultName()));
  }
}

TEST_F(LoadCustomizableTest, LoadMemoryAllocatorTest) {
  std::vector<std::string> failures;
  Status s = TestSharedBuiltins<MemoryAllocator>(
      MockMemoryAllocator::kClassName(), DefaultMemoryAllocator::kClassName(),
      &failures);
  if (failures.empty()) {
    ASSERT_OK(s);
  } else {
    ASSERT_NOK(s);
    for (const auto& failure : failures) {
      if (failure == JemallocNodumpAllocator::kClassName()) {
        ASSERT_FALSE(JemallocNodumpAllocator::IsSupported());
      } else if (failure == MemkindKmemAllocator::kClassName()) {
        ASSERT_FALSE(MemkindKmemAllocator::IsSupported());
      } else {
        printf("BYPASSED: %s -- %s\n", failure.c_str(), s.ToString().c_str());
      }
    }
  }
  if (RegisterTests("Test")) {
    ExpectCreateShared<MemoryAllocator>(MockMemoryAllocator::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadFilterPolicyTest) {
  const std::string kAutoBloom = BloomFilterPolicy::kClassName();
  const std::string kAutoRibbon = RibbonFilterPolicy::kClassName();

  std::shared_ptr<const FilterPolicy> result;
  std::vector<std::string> failures;
  std::unordered_set<std::string> expected = {
      ReadOnlyBuiltinFilterPolicy::kClassName(),
  };

  expected.insert({
      kAutoBloom,
      BloomFilterPolicy::kNickName(),
      kAutoRibbon,
      RibbonFilterPolicy::kNickName(),
  });
  ASSERT_OK(TestExpectedBuiltins<const FilterPolicy>(
      "Mock", expected, &result, &failures, [](const std::string& name) {
        std::vector<std::string> names = {name + ":1.234"};
        return names;
      }));
  ASSERT_OK(FilterPolicy::CreateFromString(
      config_options_, kAutoBloom + ":1.234:false", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf(kAutoBloom));
  ASSERT_OK(FilterPolicy::CreateFromString(
      config_options_, kAutoBloom + ":1.234:false", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf(kAutoBloom));
  ASSERT_OK(FilterPolicy::CreateFromString(config_options_,
                                           kAutoRibbon + ":1.234:-1", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf(kAutoRibbon));
  ASSERT_OK(FilterPolicy::CreateFromString(config_options_,
                                           kAutoRibbon + ":1.234:56", &result));
  ASSERT_NE(result.get(), nullptr);
  ASSERT_TRUE(result->IsInstanceOf(kAutoRibbon));

  if (RegisterTests("Test")) {
    ExpectCreateShared<FilterPolicy>(MockFilterPolicy::kClassName(), &result);
  }

  std::shared_ptr<TableFactory> table;

  std::string table_opts = "id=BlockBasedTable; filter_policy=";
  ASSERT_OK(TableFactory::CreateFromString(config_options_,
                                           table_opts + "nullptr", &table));
  ASSERT_NE(table.get(), nullptr);
  auto bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->filter_policy.get(), nullptr);
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, table_opts + ReadOnlyBuiltinFilterPolicy::kClassName(),
      &table));
  bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->filter_policy.get(), nullptr);
  ASSERT_STREQ(bbto->filter_policy->Name(),
               ReadOnlyBuiltinFilterPolicy::kClassName());
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, table_opts + MockFilterPolicy::kClassName(), &table));
  bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->filter_policy.get(), nullptr);
  ASSERT_TRUE(
      bbto->filter_policy->IsInstanceOf(MockFilterPolicy::kClassName()));
}

TEST_F(LoadCustomizableTest, LoadFlushBlockPolicyFactoryTest) {
  std::shared_ptr<FlushBlockPolicyFactory> result;
  std::shared_ptr<TableFactory> table;
  std::vector<std::string> failed;
  std::unordered_set<std::string> expected = {
      FlushBlockBySizePolicyFactory::kClassName(),
      FlushBlockEveryKeyPolicyFactory::kClassName(),
  };

  ASSERT_OK(TestExpectedBuiltins<FlushBlockPolicyFactory>(
      TestFlushBlockPolicyFactory::kClassName(), expected, &result, &failed));

  // An empty policy name creates a BySize policy
  ASSERT_OK(
      FlushBlockPolicyFactory::CreateFromString(config_options_, "", &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), FlushBlockBySizePolicyFactory::kClassName());

  std::string table_opts = "id=BlockBasedTable; flush_block_policy_factory=";
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_,
      table_opts + FlushBlockEveryKeyPolicyFactory::kClassName(), &table));
  auto bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->flush_block_policy_factory.get(), nullptr);
  ASSERT_STREQ(bbto->flush_block_policy_factory->Name(),
               FlushBlockEveryKeyPolicyFactory::kClassName());
  if (RegisterTests("Test")) {
    ExpectCreateShared<FlushBlockPolicyFactory>(
        TestFlushBlockPolicyFactory::kClassName());
    ASSERT_OK(TableFactory::CreateFromString(
        config_options_, table_opts + TestFlushBlockPolicyFactory::kClassName(),
        &table));
    bbto = table->GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(bbto, nullptr);
    ASSERT_NE(bbto->flush_block_policy_factory.get(), nullptr);
    ASSERT_STREQ(bbto->flush_block_policy_factory->Name(),
                 TestFlushBlockPolicyFactory::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadCacheTest) {
  if (RegisterTests("Test")) {
    std::string uri(MockCache::kClassName());
    uri.append("://");
    auto cache = ExpectCreateShared<Cache>(uri);
    ASSERT_TRUE(cache->IsInstanceOf(MockCache::kClassName()));
  }
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
