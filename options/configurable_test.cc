//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "options/configurable_test.h"

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/configurable.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE::test {
class StringLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[1000];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    string_.append(buffer);
  }
  const std::string& str() const { return string_; }
  void clear() { string_.clear(); }

 private:
  std::string string_;
};
static const std::unordered_map<std::string, OptionTypeInfo>&
StructOptionInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> info = {
      {"struct", OptionTypeInfo::Struct("struct", &simple_option_info, 0,
                                        OptionVerificationType::kNormal,
                                        OptionTypeFlags::kMutable)},
  };
  return info;
}

class SimpleConfigurable : public TestConfigurable<Configurable> {
 public:
  static SimpleConfigurable* Create(
      const std::string& name = "simple",
      int mode = TestConfigMode::kDefaultMode,
      const std::unordered_map<std::string, OptionTypeInfo>* map =
          &simple_option_info) {
    return new SimpleConfigurable(name, mode, map);
  }

  SimpleConfigurable(const std::string& name, int mode,
                     const std::unordered_map<std::string, OptionTypeInfo>*
                         map = &simple_option_info)
      : TestConfigurable(name, mode, map) {
    if ((mode & TestConfigMode::kUniqueMode) != 0) {
      unique_.reset(SimpleConfigurable::Create("Unique" + name_));
      RegisterOptions(name_ + "Unique", &unique_, &unique_option_info);
    }
    if ((mode & TestConfigMode::kSharedMode) != 0) {
      shared_.reset(SimpleConfigurable::Create("Shared" + name_));
      RegisterOptions(name_ + "Shared", &shared_, &shared_option_info);
    }
    if ((mode & TestConfigMode::kRawPtrMode) != 0) {
      pointer_ = SimpleConfigurable::Create("Pointer" + name_);
      RegisterOptions(name_ + "Pointer", &pointer_, &pointer_option_info);
    }
  }

};  // End class SimpleConfigurable

using ConfigTestFactoryFunc = std::function<Configurable*()>;

class ConfigurableTest : public testing::Test {
 public:
  ConfigurableTest() { config_options_.invoke_prepare_options = false; }

  ConfigOptions config_options_;
};

TEST_F(ConfigurableTest, GetOptionsPtrTest) {
  std::string opt_str;
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  ASSERT_NE(configurable->GetOptions<TestOptions>("simple"), nullptr);
  ASSERT_EQ(configurable->GetOptions<TestOptions>("bad-opt"), nullptr);
}

TEST_F(ConfigurableTest, ConfigureFromMapTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, {}));
  ASSERT_NE(opts, nullptr);
  std::unordered_map<std::string, std::string> options_map = {
      {"int", "1"}, {"bool", "true"}, {"string", "string"}};
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, options_map));
  ASSERT_EQ(opts->i, 1);
  ASSERT_EQ(opts->b, true);
  ASSERT_EQ(opts->s, "string");
}

TEST_F(ConfigurableTest, ConfigureFromStringTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_OK(configurable->ConfigureFromString(config_options_, ""));
  ASSERT_NE(opts, nullptr);
  ASSERT_OK(configurable->ConfigureFromString(config_options_,
                                              "int=1;bool=true;string=s"));
  ASSERT_EQ(opts->i, 1);
  ASSERT_EQ(opts->b, true);
  ASSERT_EQ(opts->s, "s");
}

TEST_F(ConfigurableTest, ConfigureIgnoreTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  std::unordered_map<std::string, std::string> options_map = {{"unused", "u"}};
  ConfigOptions ignore = config_options_;
  ignore.ignore_unknown_options = true;
  ASSERT_NOK(configurable->ConfigureFromMap(config_options_, options_map));
  ASSERT_OK(configurable->ConfigureFromMap(ignore, options_map));
  ASSERT_NOK(configurable->ConfigureFromString(config_options_, "unused=u"));
  ASSERT_OK(configurable->ConfigureFromString(ignore, "unused=u"));
}

TEST_F(ConfigurableTest, ConfigureNestedOptionsTest) {
  std::unique_ptr<Configurable> base;
  std::unique_ptr<Configurable> copy;
  std::string opt_str;
  std::string mismatch;

  base.reset(SimpleConfigurable::Create("simple", TestConfigMode::kAllOptMode));
  copy.reset(SimpleConfigurable::Create("simple", TestConfigMode::kAllOptMode));
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "shared={int=10; string=10};"
                                      "unique={int=20; string=20};"
                                      "pointer={int=30; string=30};"));
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(ConfigurableTest, GetOptionsTest) {
  std::unique_ptr<Configurable> simple;

  simple.reset(
      SimpleConfigurable::Create("simple", TestConfigMode::kAllOptMode));
  int i = 11;
  for (auto opt : {"", "shared.", "unique.", "pointer."}) {
    std::string value;
    std::string expected = std::to_string(i);
    std::string opt_name = opt;
    ASSERT_OK(
        simple->ConfigureOption(config_options_, opt_name + "int", expected));
    ASSERT_OK(simple->GetOption(config_options_, opt_name + "int", &value));
    ASSERT_EQ(expected, value);
    ASSERT_OK(simple->ConfigureOption(config_options_, opt_name + "string",
                                      expected));
    ASSERT_OK(simple->GetOption(config_options_, opt_name + "string", &value));
    ASSERT_EQ(expected, value);

    ASSERT_NOK(
        simple->ConfigureOption(config_options_, opt_name + "bad", expected));
    ASSERT_NOK(simple->GetOption(config_options_, "bad option", &value));
    ASSERT_TRUE(value.empty());
    i += 11;
  }
}

TEST_F(ConfigurableTest, ConfigureBadOptionsTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_OK(configurable->ConfigureOption(config_options_, "int", "42"));
  ASSERT_EQ(opts->i, 42);
  ASSERT_NOK(configurable->ConfigureOption(config_options_, "int", "fred"));
  ASSERT_NOK(configurable->ConfigureOption(config_options_, "bool", "fred"));
  ASSERT_NOK(
      configurable->ConfigureFromString(config_options_, "int=33;unused=u"));
  ASSERT_EQ(opts->i, 42);
}

TEST_F(ConfigurableTest, InvalidOptionTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  std::unordered_map<std::string, std::string> options_map = {
      {"bad-option", "bad"}};
  ASSERT_NOK(configurable->ConfigureFromMap(config_options_, options_map));
  ASSERT_NOK(
      configurable->ConfigureFromString(config_options_, "bad-option=bad"));
  ASSERT_NOK(
      configurable->ConfigureOption(config_options_, "bad-option", "bad"));
}

class ValidatedConfigurable : public SimpleConfigurable {
 public:
  ValidatedConfigurable(const std::string& name, unsigned char mode,
                        bool dont_prepare = false)
      : SimpleConfigurable(name, TestConfigMode::kDefaultMode),
        validated(false),
        prepared(0) {
    static const std::unordered_map<std::string, OptionTypeInfo>
        validated_option_info = {
            {"validated",
             {0, OptionType::kBoolean, OptionVerificationType::kNormal,
              OptionTypeFlags::kNone}},
        };
    static const std::unordered_map<std::string, OptionTypeInfo>
        prepared_option_info = {
            {"prepared",
             {0, OptionType::kInt, OptionVerificationType::kNormal,
              OptionTypeFlags::kMutable}},
        };
    static const std::unordered_map<std::string, OptionTypeInfo>
        dont_prepare_option_info = {
            {"unique",
             {0, OptionType::kConfigurable, OptionVerificationType::kNormal,
              (OptionTypeFlags::kUnique | OptionTypeFlags::kDontPrepare)}},
        };
    RegisterOptions("Validated", &validated, &validated_option_info);
    RegisterOptions("Prepared", &prepared, &prepared_option_info);
    if ((mode & TestConfigMode::kUniqueMode) != 0) {
      unique_.reset(new ValidatedConfigurable(
          "Unique" + name_, TestConfigMode::kDefaultMode, false));
      if (dont_prepare) {
        RegisterOptions(name_ + "Unique", &unique_, &dont_prepare_option_info);
      } else {
        RegisterOptions(name_ + "Unique", &unique_, &unique_option_info);
      }
    }
  }

  Status PrepareOptions(const ConfigOptions& config_options) override {
    if (++prepared <= 0) {
      return Status::InvalidArgument("Cannot prepare option");
    } else {
      return SimpleConfigurable::PrepareOptions(config_options);
    }
  }

  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    if (!validated) {
      return Status::InvalidArgument("Not Validated");
    } else {
      return SimpleConfigurable::ValidateOptions(db_opts, cf_opts);
    }
  }

 private:
  bool validated;
  int prepared;
};

TEST_F(ConfigurableTest, ValidateOptionsTest) {
  std::unique_ptr<Configurable> configurable(
      new ValidatedConfigurable("validated", TestConfigMode::kDefaultMode));
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;
  ASSERT_OK(
      configurable->ConfigureOption(config_options_, "validated", "false"));
  ASSERT_NOK(configurable->ValidateOptions(db_opts, cf_opts));
  ASSERT_OK(
      configurable->ConfigureOption(config_options_, "validated", "true"));
  ASSERT_OK(configurable->ValidateOptions(db_opts, cf_opts));
}

TEST_F(ConfigurableTest, PrepareOptionsTest) {
  std::unique_ptr<Configurable> c(
      new ValidatedConfigurable("Simple", TestConfigMode::kUniqueMode, false));
  auto cp = c->GetOptions<int>("Prepared");
  auto u = c->GetOptions<std::unique_ptr<Configurable>>("SimpleUnique");
  auto up = u->get()->GetOptions<int>("Prepared");
  config_options_.invoke_prepare_options = false;

  ASSERT_NE(cp, nullptr);
  ASSERT_NE(up, nullptr);
  ASSERT_EQ(*cp, 0);
  ASSERT_EQ(*up, 0);
  ASSERT_OK(c->ConfigureFromMap(config_options_, {}));
  ASSERT_EQ(*cp, 0);
  ASSERT_EQ(*up, 0);
  config_options_.invoke_prepare_options = true;
  ASSERT_OK(c->ConfigureFromMap(config_options_, {}));
  ASSERT_EQ(*cp, 1);
  ASSERT_EQ(*up, 1);
  ASSERT_OK(c->ConfigureFromString(config_options_, "prepared=0"));
  ASSERT_EQ(*up, 2);
  ASSERT_EQ(*cp, 1);

  ASSERT_NOK(c->ConfigureFromString(config_options_, "prepared=-2"));

  c.reset(
      new ValidatedConfigurable("Simple", TestConfigMode::kUniqueMode, true));
  cp = c->GetOptions<int>("Prepared");
  u = c->GetOptions<std::unique_ptr<Configurable>>("SimpleUnique");
  up = u->get()->GetOptions<int>("Prepared");

  ASSERT_OK(c->ConfigureFromString(config_options_, "prepared=0"));
  ASSERT_EQ(*cp, 1);
  ASSERT_EQ(*up, 0);
}

TEST_F(ConfigurableTest, CopyObjectTest) {
  class CopyConfigurable : public Configurable {
   public:
    CopyConfigurable() : prepared_(0), validated_(0) {}
    Status PrepareOptions(const ConfigOptions& options) override {
      prepared_++;
      return Configurable::PrepareOptions(options);
    }
    Status ValidateOptions(const DBOptions& db_opts,
                           const ColumnFamilyOptions& cf_opts) const override {
      validated_++;
      return Configurable::ValidateOptions(db_opts, cf_opts);
    }
    int prepared_;
    mutable int validated_;
  };

  CopyConfigurable c1;
  ConfigOptions config_options;
  Options options;

  ASSERT_OK(c1.PrepareOptions(config_options));
  ASSERT_OK(c1.ValidateOptions(options, options));
  ASSERT_EQ(c1.prepared_, 1);
  ASSERT_EQ(c1.validated_, 1);
  CopyConfigurable c2 = c1;
  ASSERT_OK(c1.PrepareOptions(config_options));
  ASSERT_OK(c1.ValidateOptions(options, options));
  ASSERT_EQ(c2.prepared_, 1);
  ASSERT_EQ(c2.validated_, 1);
  ASSERT_EQ(c1.prepared_, 2);
  ASSERT_EQ(c1.validated_, 2);
}

TEST_F(ConfigurableTest, MutableOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> imm_option_info = {
      {"imm", OptionTypeInfo::Struct("imm", &simple_option_info, 0,
                                     OptionVerificationType::kNormal,
                                     OptionTypeFlags::kNone)},
  };

  class MutableConfigurable : public SimpleConfigurable {
   public:
    MutableConfigurable()
        : SimpleConfigurable("mutable", TestConfigMode::kDefaultMode |
                                            TestConfigMode::kUniqueMode |
                                            TestConfigMode::kSharedMode) {
      RegisterOptions("struct", &options_, &StructOptionInfo());
      RegisterOptions("imm", &options_, &imm_option_info);
    }
  };
  MutableConfigurable mc;
  ConfigOptions options = config_options_;

  ASSERT_OK(mc.ConfigureOption(options, "bool", "true"));
  ASSERT_OK(mc.ConfigureOption(options, "int", "42"));
  auto* opts = mc.GetOptions<TestOptions>("mutable");
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->i, 42);
  ASSERT_EQ(opts->b, true);
  ASSERT_OK(mc.ConfigureOption(options, "struct", "{bool=false;}"));
  ASSERT_OK(mc.ConfigureOption(options, "imm", "{int=55;}"));

  options.mutable_options_only = true;

  // Now only mutable options should be settable.
  ASSERT_NOK(mc.ConfigureOption(options, "bool", "true"));
  ASSERT_OK(mc.ConfigureOption(options, "int", "24"));
  ASSERT_EQ(opts->i, 24);
  ASSERT_EQ(opts->b, false);
  ASSERT_NOK(mc.ConfigureFromString(options, "bool=false;int=33;"));
  ASSERT_EQ(opts->i, 24);
  ASSERT_EQ(opts->b, false);

  // Setting options through an immutable struct fails
  ASSERT_NOK(mc.ConfigureOption(options, "imm", "{int=55;}"));
  ASSERT_NOK(mc.ConfigureOption(options, "imm.int", "55"));
  ASSERT_EQ(opts->i, 24);
  ASSERT_EQ(opts->b, false);

  // Setting options through an mutable struct succeeds
  ASSERT_OK(mc.ConfigureOption(options, "struct", "{int=44;}"));
  ASSERT_EQ(opts->i, 44);
  ASSERT_OK(mc.ConfigureOption(options, "struct.int", "55"));
  ASSERT_EQ(opts->i, 55);

  // Setting nested immutable configurable options fail
  ASSERT_NOK(mc.ConfigureOption(options, "shared", "{bool=true;}"));
  ASSERT_NOK(mc.ConfigureOption(options, "shared.bool", "true"));

  // Setting nested mutable configurable options succeeds
  ASSERT_OK(mc.ConfigureOption(options, "unique", "{bool=true}"));
  ASSERT_OK(mc.ConfigureOption(options, "unique.bool", "true"));
}

TEST_F(ConfigurableTest, DeprecatedOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo>
      deprecated_option_info = {
          {"deprecated",
           {offsetof(struct TestOptions, b), OptionType::kBoolean,
            OptionVerificationType::kDeprecated, OptionTypeFlags::kNone}}};
  std::unique_ptr<Configurable> orig;
  orig.reset(SimpleConfigurable::Create("simple", TestConfigMode::kDefaultMode,
                                        &deprecated_option_info));
  auto* opts = orig->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  opts->d = true;
  ASSERT_OK(orig->ConfigureOption(config_options_, "deprecated", "false"));
  ASSERT_TRUE(opts->d);
  ASSERT_OK(orig->ConfigureFromString(config_options_, "deprecated=false"));
  ASSERT_TRUE(opts->d);
}

TEST_F(ConfigurableTest, AliasOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> alias_option_info = {
      {"bool",
       {offsetof(struct TestOptions, b), OptionType::kBoolean,
        OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
      {"alias",
       {offsetof(struct TestOptions, b), OptionType::kBoolean,
        OptionVerificationType::kAlias, OptionTypeFlags::kNone, nullptr}}};
  std::unique_ptr<Configurable> orig;
  orig.reset(SimpleConfigurable::Create("simple", TestConfigMode::kDefaultMode,
                                        &alias_option_info));
  auto* opts = orig->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_OK(orig->ConfigureOption(config_options_, "bool", "false"));
  ASSERT_FALSE(opts->b);
  ASSERT_OK(orig->ConfigureOption(config_options_, "alias", "true"));
  ASSERT_TRUE(opts->b);
  std::string opts_str;
  ASSERT_OK(orig->GetOptionString(config_options_, &opts_str));
  ASSERT_EQ(opts_str.find("alias"), std::string::npos);

  ASSERT_OK(orig->ConfigureOption(config_options_, "bool", "false"));
  ASSERT_FALSE(opts->b);
  ASSERT_OK(orig->GetOption(config_options_, "alias", &opts_str));
  ASSERT_EQ(opts_str, "false");
}

TEST_F(ConfigurableTest, NestedUniqueConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(
      SimpleConfigurable::Create("Outer", TestConfigMode::kAllOptMode));
  const auto outer = simple->GetOptions<TestOptions>("Outer");
  const auto unique =
      simple->GetOptions<std::unique_ptr<Configurable>>("OuterUnique");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(unique, nullptr);
  ASSERT_OK(
      simple->ConfigureFromString(config_options_, "int=24;string=outer"));
  ASSERT_OK(simple->ConfigureFromString(config_options_,
                                        "unique={int=42;string=nested}"));
  const auto inner = unique->get()->GetOptions<TestOptions>("UniqueOuter");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedSharedConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(SimpleConfigurable::Create(
      "Outer", TestConfigMode::kDefaultMode | TestConfigMode::kSharedMode));
  ASSERT_OK(
      simple->ConfigureFromString(config_options_, "int=24;string=outer"));
  ASSERT_OK(simple->ConfigureFromString(config_options_,
                                        "shared={int=42;string=nested}"));
  const auto outer = simple->GetOptions<TestOptions>("Outer");
  const auto shared =
      simple->GetOptions<std::shared_ptr<Configurable>>("OuterShared");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(shared, nullptr);
  const auto inner = shared->get()->GetOptions<TestOptions>("SharedOuter");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedRawConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(SimpleConfigurable::Create(
      "Outer", TestConfigMode::kDefaultMode | TestConfigMode::kRawPtrMode));
  ASSERT_OK(
      simple->ConfigureFromString(config_options_, "int=24;string=outer"));
  ASSERT_OK(simple->ConfigureFromString(config_options_,
                                        "pointer={int=42;string=nested}"));
  const auto outer = simple->GetOptions<TestOptions>("Outer");
  const auto pointer = simple->GetOptions<Configurable*>("OuterPointer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(pointer, nullptr);
  const auto inner = (*pointer)->GetOptions<TestOptions>("PointerOuter");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, MatchesTest) {
  std::string mismatch;
  std::unique_ptr<Configurable> base;
  std::unique_ptr<Configurable> copy;
  base.reset(SimpleConfigurable::Create(
      "simple", TestConfigMode::kDefaultMode | TestConfigMode::kNestedMode));
  copy.reset(SimpleConfigurable::Create(
      "simple", TestConfigMode::kDefaultMode | TestConfigMode::kNestedMode));
  ASSERT_OK(base->ConfigureFromString(
      config_options_,
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}"));
  ASSERT_OK(copy->ConfigureFromString(
      config_options_,
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}"));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(base->ConfigureOption(config_options_, "shared", "int=44"));
  ASSERT_FALSE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_EQ(mismatch, "shared.int");
  std::string c1value;
  std::string c2value;
  ASSERT_OK(base->GetOption(config_options_, mismatch, &c1value));
  ASSERT_OK(copy->GetOption(config_options_, mismatch, &c2value));
  ASSERT_NE(c1value, c2value);
}

static Configurable* SimpleStructFactory() {
  return SimpleConfigurable::Create(
      "simple-struct", TestConfigMode::kDefaultMode, &StructOptionInfo());
}

TEST_F(ConfigurableTest, ConfigureStructTest) {
  std::unique_ptr<Configurable> base(SimpleStructFactory());
  std::unique_ptr<Configurable> copy(SimpleStructFactory());
  std::string opt_str;
  std::string value;
  std::string mismatch;
  std::unordered_set<std::string> names;

  ASSERT_OK(
      base->ConfigureFromString(config_options_, "struct={int=10; string=10}"));
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(base->GetOptionNames(config_options_, &names));
  ASSERT_EQ(names.size(), 1);
  ASSERT_EQ(*(names.begin()), "struct");
  ASSERT_OK(
      base->ConfigureFromString(config_options_, "struct={int=20; string=20}"));
  ASSERT_OK(base->GetOption(config_options_, "struct", &value));
  ASSERT_OK(copy->ConfigureOption(config_options_, "struct", value));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));

  ASSERT_NOK(base->ConfigureFromString(config_options_,
                                       "struct={int=10; string=10; bad=11}"));
  ASSERT_OK(base->ConfigureOption(config_options_, "struct.int", "42"));
  ASSERT_NOK(base->ConfigureOption(config_options_, "struct.bad", "42"));
  ASSERT_NOK(base->GetOption(config_options_, "struct.bad", &value));
  ASSERT_OK(base->GetOption(config_options_, "struct.int", &value));
  ASSERT_EQ(value, "42");
}

TEST_F(ConfigurableTest, ConfigurableEnumTest) {
  std::unique_ptr<Configurable> base;
  std::unique_ptr<Configurable> copy;
  base.reset(SimpleConfigurable::Create("e", TestConfigMode::kEnumMode));
  copy.reset(SimpleConfigurable::Create("e", TestConfigMode::kEnumMode));

  std::string opts_str;
  std::string mismatch;

  ASSERT_OK(base->ConfigureFromString(config_options_, "enum=B"));
  ASSERT_FALSE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(base->GetOptionString(config_options_, &opts_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opts_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_NOK(base->ConfigureOption(config_options_, "enum", "bad"));
  ASSERT_NOK(base->ConfigureOption(config_options_, "unknown", "bad"));
}

TEST_F(ConfigurableTest, TestNoSerialize) {
  const std::unordered_map<std::string, OptionTypeInfo>
      noserialize_option_info = {
          {"int",
           {offsetof(struct TestOptions, i), OptionType::kInt,
            OptionVerificationType::kNormal, OptionTypeFlags::kDontSerialize}},
      };
  std::unique_ptr<Configurable> base;
  base.reset(SimpleConfigurable::Create("c", TestConfigMode::kDefaultMode,
                                        &noserialize_option_info));
  std::string opts_str;
  std::string value;
  ASSERT_OK(base->ConfigureFromString(config_options_, "int=10"));
  ASSERT_OK(base->GetOptionString(config_options_, &opts_str));
  ASSERT_EQ(opts_str, "");
  ASSERT_NOK(base->GetOption(config_options_, "int", &value));
}

TEST_F(ConfigurableTest, TestNoCompare) {
  std::unordered_map<std::string, OptionTypeInfo> nocomp_option_info = {
      {"int",
       {offsetof(struct TestOptions, i), OptionType::kInt,
        OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
  };
  std::unordered_map<std::string, OptionTypeInfo> normal_option_info = {
      {"int",
       {offsetof(struct TestOptions, i), OptionType::kInt,
        OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
  };

  std::unique_ptr<Configurable> base;
  std::unique_ptr<Configurable> copy;
  base.reset(SimpleConfigurable::Create("c", TestConfigMode::kDefaultMode,
                                        &nocomp_option_info));
  copy.reset(SimpleConfigurable::Create("c", TestConfigMode::kDefaultMode,
                                        &normal_option_info));
  ASSERT_OK(base->ConfigureFromString(config_options_, "int=10"));
  ASSERT_OK(copy->ConfigureFromString(config_options_, "int=20"));
  std::string bvalue;
  std::string cvalue;
  std::string mismatch;
  ASSERT_OK(base->GetOption(config_options_, "int", &bvalue));
  ASSERT_OK(copy->GetOption(config_options_, "int", &cvalue));
  ASSERT_EQ(bvalue, "10");
  ASSERT_EQ(cvalue, "20");
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_FALSE(copy->AreEquivalent(config_options_, base.get(), &mismatch));
}

TEST_F(ConfigurableTest, NullOptionMapTest) {
  std::unique_ptr<Configurable> base;
  std::unordered_set<std::string> names;
  std::string str;

  base.reset(
      SimpleConfigurable::Create("c", TestConfigMode::kDefaultMode, nullptr));
  ASSERT_NOK(base->ConfigureFromString(config_options_, "int=10"));
  ASSERT_NOK(base->ConfigureFromString(config_options_, "int=20"));
  ASSERT_NOK(base->ConfigureOption(config_options_, "int", "20"));
  ASSERT_NOK(base->GetOption(config_options_, "int", &str));
  ASSERT_NE(base->GetOptions<TestOptions>("c"), nullptr);
  ASSERT_OK(base->GetOptionNames(config_options_, &names));
  ASSERT_EQ(names.size(), 0UL);
  ASSERT_OK(base->PrepareOptions(config_options_));
  ASSERT_OK(base->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  std::unique_ptr<Configurable> copy;
  copy.reset(
      SimpleConfigurable::Create("c", TestConfigMode::kDefaultMode, nullptr));
  ASSERT_OK(base->GetOptionString(config_options_, &str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &str));
}

static const std::unordered_map<std::string, ConfigTestFactoryFunc>&
TestFactories() {
  static const std::unordered_map<std::string, ConfigTestFactoryFunc>
      factories = {
          {"Simple", []() { return SimpleConfigurable::Create("simple"); }},
          {"Struct", []() { return SimpleStructFactory(); }},
          {"Unique",
           []() {
             return SimpleConfigurable::Create(
                 "simple",
                 TestConfigMode::kSimpleMode | TestConfigMode::kUniqueMode);
           }},
          {"Shared",
           []() {
             return SimpleConfigurable::Create(
                 "simple",
                 TestConfigMode::kSimpleMode | TestConfigMode::kSharedMode);
           }},
          {"Nested",
           []() {
             return SimpleConfigurable::Create(
                 "simple",
                 TestConfigMode::kSimpleMode | TestConfigMode::kNestedMode);
           }},
          {"Mutable",
           []() {
             return SimpleConfigurable::Create("simple",
                                               TestConfigMode::kMutableMode |
                                                   TestConfigMode::kSimpleMode |
                                                   TestConfigMode::kNestedMode);
           }},
          {"ThreeDeep",
           []() {
             Configurable* simple = SimpleConfigurable::Create(
                 "Simple",
                 TestConfigMode::kUniqueMode | TestConfigMode::kDefaultMode);
             auto* unique = simple->GetOptions<std::unique_ptr<Configurable>>(
                 "SimpleUnique");
             unique->reset(SimpleConfigurable::Create(
                 "Child",
                 TestConfigMode::kUniqueMode | TestConfigMode::kDefaultMode));
             unique = unique->get()->GetOptions<std::unique_ptr<Configurable>>(
                 "ChildUnique");
             unique->reset(SimpleConfigurable::Create(
                 "Child", TestConfigMode::kDefaultMode));
             return simple;
           }},
          {"DBOptions",
           []() {
             auto config = DBOptionsAsConfigurable(DBOptions());
             return config.release();
           }},
          {"CFOptions",
           []() {
             auto config = CFOptionsAsConfigurable(ColumnFamilyOptions());
             return config.release();
           }},
          {"BlockBased", []() { return NewBlockBasedTableFactory(); }},
      };
  return factories;
}

class ConfigurableParamTest : public ConfigurableTest,
                              virtual public ::testing::WithParamInterface<
                                  std::pair<std::string, std::string>> {
 public:
  ConfigurableParamTest() {
    type_ = GetParam().first;
    configuration_ = GetParam().second;
    assert(TestFactories().find(type_) != TestFactories().end());
    object_.reset(CreateConfigurable());
  }

  Configurable* CreateConfigurable() {
    const auto& iter = TestFactories().find(type_);
    return (iter->second)();
  }

  void TestConfigureOptions(const ConfigOptions& opts);
  std::string type_;
  std::string configuration_;
  std::unique_ptr<Configurable> object_;
};

void ConfigurableParamTest::TestConfigureOptions(
    const ConfigOptions& config_options) {
  std::unique_ptr<Configurable> base;
  std::unique_ptr<Configurable> copy;
  std::unordered_set<std::string> names;
  std::string opt_str;
  std::string mismatch;

  base.reset(CreateConfigurable());
  copy.reset(CreateConfigurable());

  ASSERT_OK(base->ConfigureFromString(config_options, configuration_));
  ASSERT_OK(base->GetOptionString(config_options, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options, opt_str));
  ASSERT_OK(copy->GetOptionString(config_options, &opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options, copy.get(), &mismatch));

  copy.reset(CreateConfigurable());
  ASSERT_OK(base->GetOptionNames(config_options, &names));
  std::unordered_map<std::string, std::string> unused;
  bool found_one = false;
  for (const auto& name : names) {
    std::string value;
    Status s = base->GetOption(config_options, name, &value);
    if (s.ok()) {
      s = copy->ConfigureOption(config_options, name, value);
      if (s.ok() || s.IsNotSupported()) {
        found_one = true;
      } else {
        unused[name] = value;
      }
    } else {
      ASSERT_TRUE(s.IsNotSupported());
    }
  }
  ASSERT_TRUE(found_one || names.empty());
  while (found_one && !unused.empty()) {
    found_one = false;
    for (auto iter = unused.begin(); iter != unused.end();) {
      if (copy->ConfigureOption(config_options, iter->first, iter->second)
              .ok()) {
        found_one = true;
        iter = unused.erase(iter);
      } else {
        ++iter;
      }
    }
  }
  ASSERT_EQ(0, unused.size());
  ASSERT_TRUE(base->AreEquivalent(config_options, copy.get(), &mismatch));
}

TEST_P(ConfigurableParamTest, GetDefaultOptionsTest) {
  TestConfigureOptions(config_options_);
}

TEST_P(ConfigurableParamTest, ConfigureFromPropsTest) {
  std::string opt_str;
  std::string mismatch;
  std::unordered_set<std::string> names;
  std::unique_ptr<Configurable> copy(CreateConfigurable());

  ASSERT_OK(object_->ConfigureFromString(config_options_, configuration_));
  config_options_.delimiter = "\n";
  ASSERT_OK(object_->GetOptionString(config_options_, &opt_str));
  std::istringstream iss(opt_str);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    copy_map[name] = value;
  }
  ASSERT_OK(copy->ConfigureFromMap(config_options_, copy_map));
  ASSERT_TRUE(object_->AreEquivalent(config_options_, copy.get(), &mismatch));
}

INSTANTIATE_TEST_CASE_P(
    ParamTest, ConfigurableParamTest,
    testing::Values(
        std::pair<std::string, std::string>("Simple",
                                            "int=42;bool=true;string=s"),
        std::pair<std::string, std::string>(
            "Mutable", "int=42;unique={int=33;string=unique}"),
        std::pair<std::string, std::string>(
            "Struct", "struct={int=33;bool=true;string=s;}"),
        std::pair<std::string, std::string>("Shared",
                                            "int=33;bool=true;string=outer;"
                                            "shared={int=42;string=shared}"),
        std::pair<std::string, std::string>("Unique",
                                            "int=33;bool=true;string=outer;"
                                            "unique={int=42;string=unique}"),
        std::pair<std::string, std::string>("Nested",
                                            "int=11;bool=true;string=outer;"
                                            "pointer={int=22;string=pointer};"
                                            "unique={int=33;string=unique};"
                                            "shared={int=44;string=shared}"),
        std::pair<std::string, std::string>("ThreeDeep",
                                            "int=11;bool=true;string=outer;"
                                            "unique={int=22;string=inner;"
                                            "unique={int=33;string=unique}};"),
        std::pair<std::string, std::string>("DBOptions",
                                            "max_background_jobs=100;"
                                            "max_open_files=200;"),
        std::pair<std::string, std::string>("CFOptions",
                                            "table_factory=BlockBasedTable;"
                                            "disable_auto_compactions=true;"),
        std::pair<std::string, std::string>("BlockBased",
                                            "block_size=1024;"
                                            "no_block_cache=true;")));

}  // namespace ROCKSDB_NAMESPACE::test
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
