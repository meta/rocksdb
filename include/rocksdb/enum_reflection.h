// created by leipeng at 2019-12-25
// clang-format off
#pragma once
#include "rocksdb/preproc.h"
#include "rocksdb/slice.h"
#include <type_traits>

namespace ROCKSDB_NAMESPACE {
  Slice var_symbol(const char* s);

template<class Enum>
class EnumValueInit {
    Enum val;
public:
    operator Enum() const { return val; }

    /// set val
    EnumValueInit& operator-(Enum v) { val = v; return *this; }

    /// absorb the IntRep param
    template<class IntRep>
    EnumValueInit& operator=(IntRep) { return *this; }
};

template<class Enum>
Slice enum_name(Enum v, const char* unknown = "") {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    if (v == values[i])
      return names.first[i];
  }
  return unknown;
}

template<class Enum>
std::string enum_stdstr(Enum v) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    if (v == values[i])
      return names.first[i].ToString();
  }
  return "unknown:" + (sizeof(Enum) <= sizeof(int)
                          ? std::to_string((int)v)
                          : std::to_string((long)v));
}

template<class Enum>
const char* enum_cstr(Enum v, const char* unknown = "") {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    if (v == values[i])
      return names.first[i].data();
  }
  return unknown;
}

template<class Enum>
bool enum_value(const ROCKSDB_NAMESPACE::Slice& name, Enum* result) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
      if (name == names.first[i]) {
          *result = values[i];
          return true;
      }
  }
  return false;
}

/// for convenient
template<class Enum>
Enum enum_value(const ROCKSDB_NAMESPACE::Slice& name, Enum Default) {
  enum_value(name, &Default);
  return Default;
}

template<class Enum, class Func>
void enum_for_each(Func fn) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    fn(names.first[i], values[i]);
  }
}

template<class Enum>
std::string enum_str_all_names() {
  auto names = enum_all_names((Enum*)0);
  std::string s;
  for (size_t i = 0; i < names.second; ++i) {
    ROCKSDB_NAMESPACE::Slice name = names.first[i];
    s.append(name.data(), name.size());
    s.append(", ");
  };
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}

template<class Enum>
std::string enum_str_all_namevalues() {
  typedef decltype(enum_rep_type((Enum*)0)) IntRep;
  auto names = enum_all_names((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  std::string s;
  for (size_t i = 0; i < names.second; ++i) {
    ROCKSDB_NAMESPACE::Slice name = names.first[i];
    const Enum v = values[i];
    char buf[32];
    s.append(name.data(), name.size());
    s.append(" = ");
    s.append(buf, snprintf(buf, sizeof(buf),
      std::is_signed<IntRep>::value ? "%zd" : "%zu",
      size_t(v)));
    s.append(", ");
  };
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}

// return number of ignored flags
template<class Enum>
size_t enum_flags(Slice str, Enum* flags) {
  *flags = Enum(0);
  size_t ignored = 0;
  const char* cur = str.data();
  const char* end = str.size() + cur;
  while (cur < end) {
    Slice sym = var_symbol(cur);
    if (!sym.empty()) {
      Enum one;
      if (enum_value(sym, &one)) {
        *flags = Enum(size_t(*flags) | size_t(one));
      } else {
        ignored++;
      }
    }
    cur += sym.size() + 1;
  }
  return ignored;
}
template<class Enum>
Enum enum_flags(Slice str) {
    Enum flags;
    enum_flags(str, &flags); // ignore return value
    return flags;
}

#define ROCKSDB_PP_SYMBOL(ctx, arg) ROCKSDB_NAMESPACE::var_symbol(#arg)

///@param Inline can be 'inline' or 'friend'
///@param ... enum values
#define ROCKSDB_ENUM_IMPL(Inline, Class, EnumType, IntRep, EnumScope, ...) \
  enum Class EnumType : IntRep { \
    __VA_ARGS__ \
  }; \
  Inline IntRep enum_rep_type(EnumType*) { return (IntRep)(0); } \
  Inline ROCKSDB_NAMESPACE::Slice enum_str_define(EnumType*) { \
    return ROCKSDB_PP_STR(enum Class EnumType : IntRep) \
      " { " #__VA_ARGS__ " }"; \
  } \
  Inline std::pair<const ROCKSDB_NAMESPACE::Slice*, size_t> \
  enum_all_names(const EnumType*) { \
    static const ROCKSDB_NAMESPACE::Slice s_names[] = { \
      ROCKSDB_PP_MAP(ROCKSDB_PP_SYMBOL, ~, __VA_ARGS__) }; \
    return std::make_pair(s_names, ROCKSDB_PP_EXTENT(s_names)); \
  } \
  Inline const EnumType* enum_all_values(const EnumType*) { \
    static const EnumType s_values[] = { \
      ROCKSDB_PP_MAP(ROCKSDB_PP_PREPEND, \
                    EnumValueInit<EnumType>() - EnumScope, \
                    __VA_ARGS__) }; \
      return s_values; \
   }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

///@param ... enum values
#define ROCKSDB_ENUM_PLAIN(EnumType, IntRep, ...) \
  ROCKSDB_ENUM_IMPL(inline,,EnumType,IntRep,,__VA_ARGS__)

#define ROCKSDB_ENUM_PLAIN_INCLASS(EnumType, IntRep, ...) \
  ROCKSDB_ENUM_IMPL(friend,,EnumType,IntRep,,__VA_ARGS__)

///@param ... enum values
#define ROCKSDB_ENUM_CLASS(EnumType, IntRep, ...) \
  ROCKSDB_ENUM_IMPL(inline,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

#define ROCKSDB_ENUM_CLASS_INCLASS(EnumType, IntRep, ...) \
  ROCKSDB_ENUM_IMPL(friend,class,EnumType,IntRep,EnumType::,__VA_ARGS__)


//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
/// max number of macro parameters in Visual C++ is 127, this makes
/// ROCKSDB_PP_MAP only support max 61 __VA_ARGS__
/// so we use:
///   ROCKSDB_BIG_ENUM_PLAIN
///   ROCKSDB_BIG_ENUM_CLASS
///   ROCKSDB_BIG_ENUM_PLAIN_INCLASS
///   ROCKSDB_BIG_ENUM_CLASS_INCLASS
/// arguments are grouped by parents, this enlarges max allowed enum values.
/// example:
///   ROCKSDB_BIG_ENUM_PLAIN(MyEnum, int, (v1, v2), (v3, v4), (v5,v6))
///@note
/// enum_str_define(EnumType) = enum MyEnum : int { v1, v2, v3, v4, v5, v6, };
/// ---------------------------------------- this is valid ---------------^
/// there is an extra ", " after value list, this is a valid enum definition.
/// it is too hard to remove the "," so let it be there.

///@param Inline can be 'inline' or 'friend'
///@param ... enum values
#define ROCKSDB_BIG_ENUM_IMPL(Inline, Class, EnumType, IntRep, EnumScope, ...) \
  enum Class EnumType : IntRep { \
    ROCKSDB_PP_FLATTEN(__VA_ARGS__) \
  }; \
  Inline IntRep enum_rep_type(EnumType*) { return (IntRep)(0); } \
  Inline ROCKSDB_NAMESPACE::Slice enum_str_define(EnumType*) { \
    return ROCKSDB_PP_STR(enum Class EnumType : IntRep) \
     " { " \
         ROCKSDB_PP_APPLY( \
           ROCKSDB_PP_CAT2(ROCKSDB_PP_JOIN_,ROCKSDB_PP_ARG_N(__VA_ARGS__)), \
           ROCKSDB_PP_APPLY( \
             ROCKSDB_PP_CAT2(ROCKSDB_PP_MAP_,ROCKSDB_PP_ARG_N(__VA_ARGS__)), \
             ROCKSDB_PP_APPEND, ", ", \
             ROCKSDB_PP_STR_FLATTEN(__VA_ARGS__))) "}"; \
  } \
  Inline std::pair<const ROCKSDB_NAMESPACE::Slice*, size_t> \
  enum_all_names(const EnumType*) { \
    static const ROCKSDB_NAMESPACE::Slice s_names[] = { \
      ROCKSDB_PP_BIG_MAP(ROCKSDB_PP_SYMBOL, ~, __VA_ARGS__) }; \
    return std::make_pair(s_names, ROCKSDB_PP_EXTENT(s_names)); \
  } \
  Inline const EnumType* enum_all_values(const EnumType*) { \
    static const EnumType s_values[] = { \
      ROCKSDB_PP_BIG_MAP(ROCKSDB_PP_PREPEND, \
                        EnumValueInit<EnumType>() - EnumScope, \
                        __VA_ARGS__) }; \
      return s_values; \
   }

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

///@param ... enum values
#define ROCKSDB_BIG_ENUM_PLAIN(EnumType, IntRep, ...) \
  ROCKSDB_BIG_ENUM_IMPL(inline,,EnumType,IntRep,,__VA_ARGS__)

#define ROCKSDB_BIG_ENUM_PLAIN_INCLASS(EnumType, IntRep, ...) \
  ROCKSDB_BIG_ENUM_IMPL(friend,,EnumType,IntRep,,__VA_ARGS__)

///@param ... enum values
#define ROCKSDB_BIG_ENUM_CLASS(EnumType, IntRep, ...) \
  ROCKSDB_BIG_ENUM_IMPL(inline,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

#define ROCKSDB_BIG_ENUM_CLASS_INCLASS(EnumType, IntRep, ...) \
  ROCKSDB_BIG_ENUM_IMPL(friend,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

} // ROCKSDB_NAMESPACE
// clang-format on
