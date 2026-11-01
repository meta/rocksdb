// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// The OptionTypeInfo and related classes provide a framework for
// configuring and validating RocksDB classes via the Options framework.
// This file is part of the public API to allow developers who wish to
// write their own extensions and plugins to take use the Options
// framework in their custom implementations.
//
// See https://github.com/meta/rocksdb/wiki/RocksDB-Configurable-Objects
// for more information on how to develop and use custom extensions

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class OptionTypeInfo;
struct ColumnFamilyOptions;
struct DBOptions;

// The underlying "class/type" of the option.
// This enum is used to determine how the option should
// be converted to/from strings and compared.
enum class OptionType {
  kBoolean,
  kInt,
  kInt32T,
  kInt64T,
  kUInt,
  kUInt8T,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kDouble,
  kAtomicInt,
  kString,
  kCompactionStyle,
  kCompactionPri,
  kCompressionType,
  kCompactionStopStyle,
  kChecksumType,
  kEncodingType,
  kEnv,
  kEnum,
  kStruct,
  kVector,
  kConfigurable,
  kCustomizable,
  kEncodedString,
  kTemperature,
  kArray,
  kStringMap,  // Map of <std::string, std::string>
  kUnknown,
};

enum class OptionVerificationType {
  kNormal,
  kByName,               // The option is pointer typed so we can only verify
                         // based on it's name.
  kByNameAllowNull,      // Same as kByName, but it also allows the case
                         // where one of them is a nullptr.
  kByNameAllowFromNull,  // Same as kByName, but it also allows the case
                         // where the old option is nullptr.
  kDeprecated,           // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
  kAlias,                // This option represents is a name/shortcut for
                         // another option and should not be written or verified
                         // independently
};

// A set of modifier flags used to alter how an option is evaluated or
// processed. These flags can be combined together (e.g. kMutable | kShared).
// The kCompare flags can be used to control if/when options are compared.
// If kCompareNever is set, two related options would never be compared (always
// equal) If kCompareExact is set, the options will only be compared if the
// sanity mode
//                  is exact
// kMutable       means the option can be changed after it is prepared
// kShared        means the option is contained in a std::shared_ptr
// kUnique        means the option is contained in a std::uniqued_ptr
// kRawPointer    means the option is a raw pointer value.
// kAllowNull     means that an option is allowed to be null for verification
//                purposes.
// kDontSerialize means this option should not be serialized and included in
//                the string representation.
// kDontPrepare   means do not call PrepareOptions for this pointer value.
enum class OptionTypeFlags : uint32_t {
  kNone = 0x00,  // No flags
  kCompareDefault = 0x0,
  kCompareNever = ConfigOptions::kSanityLevelNone,
  kCompareLoose = ConfigOptions::kSanityLevelLooselyCompatible,
  kCompareExact = ConfigOptions::kSanityLevelExactMatch,

  kMutable = 0x0100,         // Option is mutable
  kRawPointer = 0x0200,      // The option is stored as a raw pointer
  kShared = 0x0400,          // The option is stored as a shared_ptr
  kUnique = 0x0800,          // The option is stored as a unique_ptr
  kAllowNull = 0x1000,       // The option can be null
  kDontSerialize = 0x2000,   // Don't serialize the option
  kDontPrepare = 0x4000,     // Don't prepare or sanitize this option
  kStringNameOnly = 0x8000,  // The option serializes to a name only
};

inline OptionTypeFlags operator|(const OptionTypeFlags& a,
                                 const OptionTypeFlags& b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) |
                                      static_cast<uint32_t>(b));
}

inline OptionTypeFlags operator&(const OptionTypeFlags& a,
                                 const OptionTypeFlags& b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) &
                                      static_cast<uint32_t>(b));
}

// Converts an string into its enumerated value.
// @param type_map Mapping between strings and enum values
// @param type The string representation of the enum
// @param value Returns the enum value represented by the string
// @return true if the string was found in the enum map, false otherwise.
template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

// Converts an enum into its string representation.
// @param type_map Mapping between strings and enum values
// @param type The enum
// @param value Returned as the string representation of the enum
// @return true if the enum was found in the enum map, false otherwise.
template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

template <typename T, size_t kSize>
Status ParseArray(const ConfigOptions& config_options,
                  const OptionTypeInfo& elem_info, char separator,
                  const std::string& name, const std::string& value,
                  std::array<T, kSize>* result);

template <typename T, size_t kSize>
Status SerializeArray(const ConfigOptions& config_options,
                      const OptionTypeInfo& elem_info, char separator,
                      const std::string& name, const std::array<T, kSize>& vec,
                      std::string* value);

template <typename T, size_t kSize>
bool ArraysAreEqual(const ConfigOptions& config_options,
                    const OptionTypeInfo& elem_info, const std::string& name,
                    const std::array<T, kSize>& array1,
                    const std::array<T, kSize>& array2, std::string* mismatch);

template <typename T>
Status ParseVector(const ConfigOptions& config_options,
                   const OptionTypeInfo& elem_info, char separator,
                   const std::string& name, const std::string& value,
                   std::vector<T>* result);

template <typename T>
Status SerializeVector(const ConfigOptions& config_options,
                       const OptionTypeInfo& elem_info, char separator,
                       const std::string& name, const std::vector<T>& vec,
                       std::string* value);
template <typename T>
bool VectorsAreEqual(const ConfigOptions& config_options,
                     const OptionTypeInfo& elem_info, const std::string& name,
                     const std::vector<T>& vec1, const std::vector<T>& vec2,
                     std::string* mismatch);

// Function for converting a option string value into its underlying
// representation in "addr"
// On success, Status::OK is returned and addr is set to the parsed form
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the value is parsed
// @param name  The name of the options being parsed
// @param value The string representation of the option
// @param addr  Pointer to the object
using ParseFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const std::string& /*value*/, void* /*addr*/)>;

// Function for converting an option "addr" into its string representation.
// On success, Status::OK is returned and value is the serialized form.
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the values are serialized
// @param name  The name of the options being serialized
// @param addr  Pointer to the value being serialized
// @param value The result of the serialization.
using SerializeFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const void* /*addr*/, std::string* /*value*/)>;

// Function for comparing two option values
// If they are not equal, updates "mismatch" with the name of the bad option
// @param opts  The ConfigOptions controlling how the values are compared
// @param name  The name of the options being compared
// @param addr1 The first address to compare
// @param addr2 The address to compare to
// @param mismatch If the values are not equal, the name of the option that
// first differs
using EqualsFunc = std::function<bool(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const void* /*addr1*/, const void* /*addr2*/, std::string* mismatch)>;

// Function for preparing/initializing an option.
using PrepareFunc =
    std::function<Status(const ConfigOptions& /*opts*/,
                         const std::string& /*name*/, void* /*addr*/)>;

// Function for validating an option.
using ValidateFunc = std::function<Status(
    const DBOptions& /*db_opts*/, const ColumnFamilyOptions& /*cf_opts*/,
    const std::string& /*name*/, const void* /*addr*/)>;

// A struct for storing constant option information such as option name,
// option type, and offset.
class OptionTypeInfo {
 public:
  OptionTypeInfo(
      int offset, OptionType type,
      OptionVerificationType verification = OptionVerificationType::kNormal,
      OptionTypeFlags flags = OptionTypeFlags::kNone,
      const ParseFunc& parse_func = {},
      const SerializeFunc& serialize_func = {},
      const EqualsFunc& equals_func = {})
      : offset_(offset),
        parse_func_(parse_func),
        serialize_func_(serialize_func),
        equals_func_(equals_func),
        type_(type),
        verification_(verification),
        flags_(flags) {}

  // Creates an OptionTypeInfo for an enum type.  Enums use an additional
  // map to convert the enums to/from their string representation.
  // To create an OptionTypeInfo that is an Enum, one should:
  // - Create a static map of string values to the corresponding enum value
  // - Call this method passing the static map in as a parameter.
  // Note that it is not necessary to add a new OptionType or make any
  // other changes -- the returned object handles parsing, serialization, and
  // comparisons.
  //
  // @param offset The offset in the option object for this enum
  // @param map The string to enum mapping for this enum
  template <typename T>
  static OptionTypeInfo Enum(
      int offset, const std::unordered_map<std::string, T>* const map,
      OptionTypeFlags flags = OptionTypeFlags::kNone,
      OptionVerificationType verification = OptionVerificationType::kNormal) {
    OptionTypeInfo info(offset, OptionType::kEnum, verification, flags);
    info.SetParseFunc(
        // Uses the map argument to convert the input string into
        // its corresponding enum value.  If value is found in the map,
        // addr is updated to the corresponding map entry.
        // @return OK if the value is found in the map
        // @return InvalidArgument if the value is not found in the map
        [map](const ConfigOptions&, const std::string& name,
              const std::string& value, void* addr) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (ParseEnum<T>(*map, value, static_cast<T*>(addr))) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        });
    info.SetSerializeFunc(
        // Uses the map argument to convert the input enum into
        // its corresponding string value.  If enum value is found in the map,
        // value is updated to the corresponding string value in the map.
        // @return OK if the enum is found in the map
        // @return InvalidArgument if the enum is not found in the map
        [map](const ConfigOptions&, const std::string& name, const void* addr,
              std::string* value) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (SerializeEnum<T>(*map, (*static_cast<const T*>(addr)),
                                      value)) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        });
    info.SetEqualsFunc(
        // Casts addr1 and addr2 to the enum type and returns true if
        // they are equal, false otherwise.
        [](const ConfigOptions&, const std::string&, const void* addr1,
           const void* addr2, std::string*) {
          return (*static_cast<const T*>(addr1) ==
                  *static_cast<const T*>(addr2));
        });
    return info;
  }  // End OptionTypeInfo::Enum

  // Creates an OptionTypeInfo for a Struct type.  Structs have a
  // map of string-OptionTypeInfo associated with them that describes how
  // to process the object for parsing, serializing, and matching.
  // Structs also have a struct_name, which is the name of the object
  // as registered in the parent map.
  // When processing a struct, the option name can be specified as:
  //   - <struct_name>       Meaning to process the entire struct.
  //   - <struct_name.field> Meaning to process the single field
  //   - <field>             Process the single fields
  // The CompactionOptionsFIFO, CompactionOptionsUniversal, and LRUCacheOptions
  // are all examples of Struct options.
  //
  // To create an OptionTypeInfo that is a Struct, one should:
  // - Create a static map of string-OptionTypeInfo corresponding to the
  //   properties of the object that can be set via the options.
  // - Call this method passing the name and map in as parameters.
  // Note that it is not necessary to add a new OptionType or make any
  // other changes -- the returned object handles parsing, serialization, and
  // comparisons.
  //
  // @param offset The offset in the option object for this enum
  // @param map The string to enum mapping for this enum
  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int offset, OptionVerificationType verification, OptionTypeFlags flags) {
    OptionTypeInfo info(offset, OptionType::kStruct, verification, flags);
    info.SetParseFunc(
        // Parses the struct and updates the fields at addr
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name,
                                  const std::string& value, void* addr) {
          return ParseStruct(opts, struct_name, struct_map, name, value, addr);
        });
    info.SetSerializeFunc(
        // Serializes the struct options into value
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr,
                                  std::string* value) {
          return SerializeStruct(opts, struct_name, struct_map, name, addr,
                                 value);
        });
    info.SetEqualsFunc(
        // Compares the struct fields of addr1 and addr2 for equality
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr1,
                                  const void* addr2, std::string* mismatch) {
          return StructsAreEqual(opts, struct_name, struct_map, name, addr1,
                                 addr2, mismatch);
        });
    return info;
  }
  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int offset, OptionVerificationType verification, OptionTypeFlags flags,
      const ParseFunc& parse_func) {
    OptionTypeInfo info(
        Struct(struct_name, struct_map, offset, verification, flags));
    return info.SetParseFunc(parse_func);
  }

  template <typename T, size_t kSize>
  static OptionTypeInfo Array(int _offset, OptionVerificationType _verification,
                              OptionTypeFlags _flags,
                              const OptionTypeInfo& elem_info,
                              char separator = ':') {
    OptionTypeInfo info(_offset, OptionType::kArray, _verification, _flags);
    info.SetParseFunc([elem_info, separator](
                          const ConfigOptions& opts, const std::string& name,
                          const std::string& value, void* addr) {
      auto result = static_cast<std::array<T, kSize>*>(addr);
      return ParseArray<T, kSize>(opts, elem_info, separator, name, value,
                                  result);
    });
    info.SetSerializeFunc([elem_info, separator](const ConfigOptions& opts,
                                                 const std::string& name,
                                                 const void* addr,
                                                 std::string* value) {
      const auto& array = *(static_cast<const std::array<T, kSize>*>(addr));
      return SerializeArray<T, kSize>(opts, elem_info, separator, name, array,
                                      value);
    });
    info.SetEqualsFunc([elem_info](const ConfigOptions& opts,
                                   const std::string& name, const void* addr1,
                                   const void* addr2, std::string* mismatch) {
      const auto& array1 = *(static_cast<const std::array<T, kSize>*>(addr1));
      const auto& array2 = *(static_cast<const std::array<T, kSize>*>(addr2));
      return ArraysAreEqual<T, kSize>(opts, elem_info, name, array1, array2,
                                      mismatch);
    });
    return info;
  }

  template <typename T>
  static OptionTypeInfo Vector(int _offset,
                               OptionVerificationType _verification,
                               OptionTypeFlags _flags,
                               const OptionTypeInfo& elem_info,
                               char separator = ':') {
    OptionTypeInfo info(_offset, OptionType::kVector, _verification, _flags);
    info.SetParseFunc([elem_info, separator](
                          const ConfigOptions& opts, const std::string& name,
                          const std::string& value, void* addr) {
      auto result = static_cast<std::vector<T>*>(addr);
      return ParseVector<T>(opts, elem_info, separator, name, value, result);
    });
    info.SetSerializeFunc([elem_info, separator](const ConfigOptions& opts,
                                                 const std::string& name,
                                                 const void* addr,
                                                 std::string* value) {
      const auto& vec = *(static_cast<const std::vector<T>*>(addr));
      return SerializeVector<T>(opts, elem_info, separator, name, vec, value);
    });
    info.SetEqualsFunc([elem_info](const ConfigOptions& opts,
                                   const std::string& name, const void* addr1,
                                   const void* addr2, std::string* mismatch) {
      const auto& vec1 = *(static_cast<const std::vector<T>*>(addr1));
      const auto& vec2 = *(static_cast<const std::vector<T>*>(addr2));
      return VectorsAreEqual<T>(opts, elem_info, name, vec1, vec2, mismatch);
    });
    return info;
  }

  static OptionTypeInfo StringMap(int _offset,
                                  OptionVerificationType _verification,
                                  OptionTypeFlags _flags,
                                  char kv_separator = '=',
                                  char item_separator = ';') {
    OptionTypeInfo info(_offset, OptionType::kStringMap, _verification, _flags);
    info.SetParseFunc([kv_separator, item_separator](
                          const ConfigOptions&, const std::string&,
                          const std::string& value, void* addr) {
      std::map<std::string, std::string> map;
      Status s;
      // Permit `{k1=v1;k2=v2}` (as serialized) or bare `k1=v1;k2=v2`.
      std::string stripped = OptionTypeInfo::StripOuterBraces(value);
      for (size_t start = 0, end = 0;
           s.ok() && start < stripped.size() && end != std::string::npos;
           start = end + 1) {
        std::string token;
        s = OptionTypeInfo::NextToken(stripped, item_separator, start, &end,
                                      &token);
        if (s.ok() && !token.empty()) {
          size_t pos = token.find(kv_separator);
          if (pos == std::string::npos) {
            return Status::InvalidArgument("Invalid string map entry: ", token);
          }
          std::string k = token.substr(0, pos);
          std::string v = token.substr(pos + 1);
          std::string decoded_key;
          std::string decoded_value;
          if (!Slice(k).DecodeHex(&decoded_key) ||
              !Slice(v).DecodeHex(&decoded_value)) {
            return Status::InvalidArgument("Invalid hex in string map entry: ",
                                           token);
          }
          map.emplace(std::move(decoded_key), std::move(decoded_value));
        }
      }
      if (s.ok()) {
        *(static_cast<std::map<std::string, std::string>*>(addr)) = map;
      }
      return s;
    });
    info.SetSerializeFunc(
        [kv_separator, item_separator](const ConfigOptions&, const std::string&,
                                       const void* addr, std::string* value) {
          const auto map =
              static_cast<const std::map<std::string, std::string>*>(addr);
          value->append("{");
          for (const auto& entry : *map) {
            value->append(Slice(entry.first).ToString(true));
            *value += kv_separator;
            value->append(Slice(entry.second).ToString(true));
            *value += item_separator;
          }
          value->append("}");
          return Status::OK();
        });
    info.SetEqualsFunc([](const ConfigOptions&, const std::string&,
                          const void* addr1, const void* addr2, std::string*) {
      return (*static_cast<const std::map<std::string, std::string>*>(addr1) ==
              *static_cast<const std::map<std::string, std::string>*>(addr2));
    });
    return info;
  }

  // Create a new std::shared_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::shared_ptr<T> object.
  //
  // @param offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags Extra flags specifying the behavior of this option
  // @param serialize_func Optional function for serializing this option
  // @param equals_func Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomSharedPtr(
      int offset, OptionVerificationType ovt,
      OptionTypeFlags flags = OptionTypeFlags::kNone,
      const SerializeFunc& serialize_func = {},
      const EqualsFunc& equals_func = {}) {
    auto parse_func = [](const ConfigOptions& opts, const std::string& name,
                         const std::string& value, void* addr) {
      auto* shared = static_cast<std::shared_ptr<T>*>(addr);
      if (name == kIdPropName() && value.empty()) {
        shared->reset();
        return Status::OK();
      } else {
        return T::CreateFromString(opts, value, shared);
      }
    };
    return OptionTypeInfo(offset, OptionType::kCustomizable, ovt,
                          flags | OptionTypeFlags::kShared, parse_func,
                          serialize_func, equals_func);
  }

  // Create a new std::unique_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::unique_ptr<T> object.
  //
  // @param offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomUniquePtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags) {
    OptionTypeInfo info(offset, OptionType::kCustomizable, ovt,
                        flags | OptionTypeFlags::kUnique);
    return info.SetParseFunc([](const ConfigOptions& opts,
                                const std::string& name,
                                const std::string& value, void* addr) {
      auto* unique = static_cast<std::unique_ptr<T>*>(addr);
      if (name == kIdPropName() && value.empty()) {
        unique->reset();
        return Status::OK();
      } else {
        return T::CreateFromString(opts, value, unique);
      }
    });
  }

  template <typename T>
  static OptionTypeInfo AsCustomUniquePtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags,
                                          const SerializeFunc& serialize_func,
                                          const EqualsFunc& equals_func) {
    OptionTypeInfo info(AsCustomUniquePtr<T>(offset, ovt, flags));
    info.SetSerializeFunc(serialize_func);
    info.SetEqualsFunc(equals_func);
    return info;
  }

  // Create a new Customizable* OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // T object.
  //
  // @param _offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomRawPtr(int offset, OptionVerificationType ovt,
                                       OptionTypeFlags flags) {
    OptionTypeInfo info(offset, OptionType::kCustomizable, ovt,
                        flags | OptionTypeFlags::kRawPointer);
    return info.SetParseFunc([](const ConfigOptions& opts,
                                const std::string& name,
                                const std::string& value, void* addr) {
      auto** pointer = static_cast<T**>(addr);
      if (name == kIdPropName() && value.empty()) {
        *pointer = nullptr;
        return Status::OK();
      } else {
        return T::CreateFromString(opts, value, pointer);
      }
    });
  }

  template <typename T>
  static OptionTypeInfo AsCustomRawPtr(int offset, OptionVerificationType ovt,
                                       OptionTypeFlags flags,
                                       const SerializeFunc& serialize_func,
                                       const EqualsFunc& equals_func) {
    OptionTypeInfo info(AsCustomRawPtr<T>(offset, ovt, flags));
    info.SetSerializeFunc(serialize_func);
    info.SetEqualsFunc(equals_func);
    return info;
  }

  OptionTypeInfo& SetParseFunc(const ParseFunc& f) {
    parse_func_ = f;
    return *this;
  }

  OptionTypeInfo& SetSerializeFunc(const SerializeFunc& f) {
    serialize_func_ = f;
    return *this;
  }
  OptionTypeInfo& SetEqualsFunc(const EqualsFunc& f) {
    equals_func_ = f;
    return *this;
  }

  OptionTypeInfo& SetPrepareFunc(const PrepareFunc& f) {
    prepare_func_ = f;
    return *this;
  }

  OptionTypeInfo& SetValidateFunc(const ValidateFunc& f) {
    validate_func_ = f;
    return *this;
  }

  OptionTypeFlags GetFlags() const { return flags_; }
  void SetFlags(OptionTypeFlags flags) { flags_ = flags; }

  bool IsEnabled(OptionTypeFlags otf) const { return (flags_ & otf) == otf; }

  bool IsEditable(const ConfigOptions& opts) const {
    if (opts.mutable_options_only) {
      return IsMutable();
    } else {
      return true;
    }
  }
  bool IsMutable() const { return IsEnabled(OptionTypeFlags::kMutable); }

  bool IsDeprecated() const {
    return IsEnabled(OptionVerificationType::kDeprecated);
  }

  // Returns true if the option is marked as an Alias.
  // Aliases are valid options that are parsed but are not converted to strings
  // or compared.
  bool IsAlias() const { return IsEnabled(OptionVerificationType::kAlias); }

  bool IsEnabled(OptionVerificationType ovf) const {
    return verification_ == ovf;
  }

  // Returns the sanity level for comparing the option.
  // If the options should not be compared, returns None
  // If the option has a compare flag, returns it.
  // Otherwise, returns "exact"
  ConfigOptions::SanityLevel GetSanityLevel() const {
    if (IsDeprecated() || IsAlias()) {
      return ConfigOptions::SanityLevel::kSanityLevelNone;
    } else {
      auto match = (flags_ & OptionTypeFlags::kCompareExact);
      if (match == OptionTypeFlags::kCompareDefault) {
        return ConfigOptions::SanityLevel::kSanityLevelExactMatch;
      } else {
        return (ConfigOptions::SanityLevel)match;
      }
    }
  }

  // Returns true if the option should be serialized.
  // Options should be serialized if the are not deprecated, aliases,
  // or marked as "Don't Serialize".
  bool ShouldSerialize() const {
    if (IsDeprecated() || IsAlias()) {
      return false;
    } else if (IsEnabled(OptionTypeFlags::kDontSerialize)) {
      return false;
    } else {
      return true;
    }
  }

  bool ShouldPrepare() const {
    if (IsDeprecated() || IsAlias()) {
      return false;
    } else if (IsEnabled(OptionTypeFlags::kDontPrepare)) {
      return false;
    } else {
      return (prepare_func_ != nullptr || IsConfigurable());
    }
  }

  bool ShouldValidate() const {
    if (IsDeprecated() || IsAlias()) {
      return false;
    } else {
      return (validate_func_ != nullptr || IsConfigurable());
    }
  }

  // Returns true if the option is allowed to be null.
  // Options can be null if the verification type is allow from null
  // or if the flags specify allow null.
  bool CanBeNull() const {
    return (IsEnabled(OptionTypeFlags::kAllowNull) ||
            IsEnabled(OptionVerificationType::kByNameAllowNull) ||
            IsEnabled(OptionVerificationType::kByNameAllowFromNull));
  }

  bool IsSharedPtr() const { return IsEnabled(OptionTypeFlags::kShared); }

  bool IsUniquePtr() const { return IsEnabled(OptionTypeFlags::kUnique); }

  bool IsRawPtr() const { return IsEnabled(OptionTypeFlags::kRawPointer); }

  bool IsByName() const {
    return (verification_ == OptionVerificationType::kByName ||
            verification_ == OptionVerificationType::kByNameAllowNull ||
            verification_ == OptionVerificationType::kByNameAllowFromNull);
  }

  bool IsStruct() const { return (type_ == OptionType::kStruct); }

  bool IsConfigurable() const {
    return (type_ == OptionType::kConfigurable ||
            type_ == OptionType::kCustomizable);
  }

  bool IsCustomizable() const { return (type_ == OptionType::kCustomizable); }

  OptionType GetType() const { return type_; }

  inline const void* GetOffset(const void* base) const {
    return static_cast<const char*>(base) + offset_;
  }

  inline void* GetOffset(void* base) const {
    return static_cast<char*>(base) + offset_;
  }

  template <typename T>
  const T* GetOffsetAs(const void* base) const {
    const void* addr = GetOffset(base);
    return static_cast<const T*>(addr);
  }

  template <typename T>
  T* GetOffsetAs(void* base) const {
    void* addr = GetOffset(base);
    return static_cast<T*>(addr);
  }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  const T* AsRawPointer(const void* const base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    if (IsUniquePtr()) {
      const auto ptr = GetOffsetAs<std::unique_ptr<T>>(base_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      const auto ptr = GetOffsetAs<std::shared_ptr<T>>(base_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      const T* const* ptr = GetOffsetAs<T* const>(base_addr);
      return *ptr;
    } else {
      return GetOffsetAs<T>(base_addr);
    }
  }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  T* AsRawPointer(void* base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    if (IsUniquePtr()) {
      auto ptr = GetOffsetAs<std::unique_ptr<T>>(base_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      auto ptr = GetOffsetAs<std::shared_ptr<T>>(base_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      auto ptr = GetOffsetAs<T*>(base_addr);
      return *ptr;
    } else {
      return GetOffsetAs<T>(base_addr);
    }
  }

  // Parses the option in "opt_value" according to the rules of this class
  // and updates the value at "opt_ptr".
  // On success, Status::OK() is returned.  On failure:
  // NotFound means the opt_name is not valid for this option
  // NotSupported means we do not know how to parse the value for this option
  // InvalidArgument means the opt_value is not valid for this option.
  Status Parse(const ConfigOptions& config_options, const std::string& opt_name,
               const std::string& opt_value, void* const opt_ptr) const;

  // Serializes the option in "opt_addr" according to the rules of this class
  // into the value at "opt_value".
  Status Serialize(const ConfigOptions& config_options,
                   const std::string& opt_name, const void* const opt_ptr,
                   std::string* opt_value) const;

  // Compares the "addr1" and "addr2" values according to the rules of this
  // class and returns true if they match.  On a failed match, mismatch is the
  // name of the option that failed to match.
  bool AreEqual(const ConfigOptions& config_options,
                const std::string& opt_name, const void* const addr1,
                const void* const addr2, std::string* mismatch) const;

  // Used to override the match rules for "ByName" options.
  bool AreEqualByName(const ConfigOptions& config_options,
                      const std::string& opt_name, const void* const this_ptr,
                      const void* const that_ptr) const;
  bool AreEqualByName(const ConfigOptions& config_options,
                      const std::string& opt_name, const void* const this_ptr,
                      const std::string& that_value) const;

  Status Prepare(const ConfigOptions& config_options, const std::string& name,
                 void* opt_ptr) const;
  Status Validate(const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts,
                  const std::string& name, const void* opt_ptr) const;

  // Parses the input opts_map according to the type_map for the opt_addr
  // For each name-value pair in opts_map, find the corresponding name in
  // type_map If the name is found:
  //    - set the corresponding value in opt_addr, returning the status on
  //    failure;
  // If the name is not found:
  //    - If unused is specified, add the name-value to unused and continue
  //    - If ingore_unknown_options is false, return NotFound
  // Returns OK if all options were either:
  //    - Successfully set
  //    - options were not found and ignore_unknown_options=true
  //    - options were not found and unused was specified
  // Note that this method is much less sophisticated than the comparable
  // Configurable::Configure methods.  For example, on error, there is no
  // attempt to return opt_addr to the initial state.  Additionally, there
  // is no effort to initialize (Configurable::PrepareOptions) the object
  // on success.  This method should typically only be used for simpler,
  // standalone structures and not those that contain shared and embedded
  // objects.
  static Status ParseType(
      const ConfigOptions& config_options, const std::string& opts_str,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      void* opt_addr,
      std::unordered_map<std::string, std::string>* unused = nullptr);
  static Status ParseType(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      void* opt_addr,
      std::unordered_map<std::string, std::string>* unused = nullptr);

  // Parses the input value according to the map for the struct at opt_addr
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it, based on struct_name and
  // opt_name.
  static Status ParseStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const std::string& value, void* opt_addr);

  // Serializes the values from opt_addr using the rules in type_map.
  // Returns the serialized form in result.
  // Returns OK on success or non-OK if some option could not be serialized.
  static Status SerializeType(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      const void* opt_addr, std::string* value);

  // Serializes the input addr according to the map for the struct to value.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static Status SerializeStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const void* opt_addr, std::string* value);

  // Compares the values in this_addr and that_addr using the rules in type_map.
  // If the values are equal, returns true
  // If the values are not equal, returns false and sets mismatch to the name
  // of the first value that did not match.
  static bool TypesAreEqual(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, OptionTypeInfo>& map,
      const void* this_addr, const void* that_addr, std::string* mismatch);

  // Compares the input offsets according to the map for the struct and returns
  // true if they are equivalent, false otherwise.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static bool StructsAreEqual(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const void* this_offset,
      const void* that_offset, std::string* mismatch);

  // Finds the entry for the opt_name in the opt_map, returning
  // nullptr if not found.
  // If found, elem_name will be the name of option to find.
  // This may be opt_name, or a substring of opt_name.
  // For "simple" options, opt_name will be equal to elem_name.  Given the
  // opt_name "opt", elem_name will equal "opt".
  // For "embedded" options (like structs), elem_name may be opt_name
  // or a field within the opt_name.  For example, given the struct "struct",
  // and opt_name of "struct.field", elem_name will be "field"
  static const OptionTypeInfo* Find(
      const std::string& opt_name,
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      std::string* elem_name);

  // Returns the next token marked by the delimiter from "opts" after start in
  // token and updates end to point to where that token stops. Delimiters inside
  // of braces are ignored. Returns OK if a token is found and an error if the
  // input opts string is mis-formatted.
  // Given "a=AA;b=BB;" start=2 and delimiter=";", token is "AA" and end points
  // to "b" Given "{a=A;b=B}", the token would be "a=A;b=B"
  //
  // @param opts The string in which to find the next token
  // @param delimiter The delimiter between tokens
  // @param start     The position in opts to start looking for the token
  // @param ed        Returns the end position in opts of the token
  // @param token     Returns the token
  // @returns OK if a token was found
  // @return InvalidArgument if the braces mismatch
  //          (e.g. "{a={b=c;}" ) -- missing closing brace
  // @return InvalidArgument if an expected delimiter is not found
  //        e.g. "{a=b}c=d;" -- missing delimiter before "c"
  static Status NextToken(const std::string& opts, char delimiter, size_t start,
                          size_t* end, std::string* token);

  // Strips a single layer of outer "{" / "}" wrapping iff the leading '{'
  // pairs with the trailing '}'. This permissively unwraps values that
  // could legitimately be either braced or bare (e.g. `{a:b:c}` and
  // `a:b:c` are accepted equivalently by list-style parsers). The check
  // is done at brace depth: `{a}:{b}` is left as-is because the leading
  // '{' closes before the end of the string.
  static std::string StripOuterBraces(const std::string& value);

  constexpr static const char* kIdPropName() { return "id"; }
  constexpr static const char* kIdPropSuffix() { return ".id"; }

 private:
  int offset_;

  // The optional function to convert a string to its representation
  ParseFunc parse_func_;

  // The optional function to convert a value to its string representation
  SerializeFunc serialize_func_;

  // The optional function to match two option values
  EqualsFunc equals_func_;

  PrepareFunc prepare_func_;
  ValidateFunc validate_func_;
  OptionType type_;
  OptionVerificationType verification_;
  OptionTypeFlags flags_;
};

// Parses the input value into elements of the result array, which has fixed
// array size. For example, if the value=1:2:3 and elem_info parses integers,
// the result array will be {1,2,3}. Array size is defined in the OptionTypeInfo
// the input value has to match with that.
// @param config_options Controls how the option value is parsed.
// @param elem_info Controls how individual tokens in value are parsed
// @param separator Character separating tokens in values (':' in the above
// example)
// @param name      The name associated with this array option
// @param value     The input string to parse into tokens
// @param result    Returns the results of parsing value into its elements.
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or element number
//                          doesn't match array size defined in OptionTypeInfo
//                          or if the token could not be parsed
// @return NotFound         If the tokenized value contains unknown options for
// its type
template <typename T, size_t kSize>
Status ParseArray(const ConfigOptions& config_options,
                  const OptionTypeInfo& elem_info, char separator,
                  const std::string& name, const std::string& value,
                  std::array<T, kSize>* result) {
  Status status;

  ConfigOptions copy = config_options;
  copy.ignore_unsupported_options = false;
  // Permit the caller to pass either bare `item1:item2` or wrapped
  // `{item1:item2}`. Either form parses identically.
  std::string stripped = OptionTypeInfo::StripOuterBraces(value);
  size_t i = 0;
  size_t start = 0;
  size_t end = 0;
  for (; status.ok() && i < kSize && start < stripped.size() &&
         end != std::string::npos;
       i++, start = end + 1) {
    std::string token;
    status =
        OptionTypeInfo::NextToken(stripped, separator, start, &end, &token);
    if (status.ok()) {
      status = elem_info.Parse(copy, name, token, &((*result)[i]));
      if (config_options.ignore_unsupported_options &&
          status.IsNotSupported()) {
        // If we were ignoring unsupported options and this one should be
        // ignored, ignore it by setting the status to OK
        status = Status::OK();
      }
    }
  }
  if (!status.ok()) {
    return status;
  }
  // make sure the element number matches the array size
  if (i < kSize) {
    return Status::InvalidArgument(
        "Serialized value has less elements than array size", name);
  }
  if (start < value.size() && end != std::string::npos) {
    return Status::InvalidArgument(
        "Serialized value has more elements than array size", name);
  }
  return status;
}

// Serializes the fixed size input array into its output value.  Elements are
// separated by the separator character.  This element will convert all of the
// elements in array into their serialized form, using elem_info to perform the
// serialization.
// For example, if the array contains the integers 1,2,3 and elem_info
// serializes the output would be 1:2:3 for separator ":".
// @param config_options Controls how the option value is serialized.
// @param elem_info Controls how individual tokens in value are serialized
// @param separator Character separating tokens in value (':' in the above
// example)
// @param name      The name associated with this array option
// @param array     The input array to serialize
// @param value     The output string of serialized options
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or if the token
//                          could not be parsed
// @return NotFound        If the tokenized value contains unknown options for
//                          its type
template <typename T, size_t kSize>
Status SerializeArray(const ConfigOptions& config_options,
                      const OptionTypeInfo& elem_info, char separator,
                      const std::string& name,
                      const std::array<T, kSize>& array, std::string* value) {
  std::string result;
  ConfigOptions embedded = config_options;
  embedded.delimiter = ";";
  int printed = 0;
  for (const auto& elem : array) {
    std::string elem_str;
    Status s = elem_info.Serialize(embedded, name, &elem, &elem_str);
    if (!s.ok()) {
      return s;
    } else if (!elem_str.empty()) {
      if (printed++ > 0) {
        result += separator;
      }
      // If the element contains embedded separators, put it inside of brackets
      if (elem_str.find(separator) != std::string::npos) {
        result += "{" + elem_str + "}";
      } else {
        result += elem_str;
      }
    }
  }
  if (result.find("=") != std::string::npos) {
    *value = "{" + result + "}";
  } else if (printed > 1 && result.at(0) == '{') {
    *value = "{" + result + "}";
  } else {
    *value = result;
  }
  return Status::OK();
}

// Compares the input arrays array1 and array2 for equality
// Elements of the array are compared one by one using elem_info to perform the
// comparison.
//
// @param config_options Controls how the arrays are compared.
// @param elem_info  Controls how individual elements in the arrays are compared
// @param name          The name associated with this array option
// @param array1,array2 The arrays to compare.
// @param mismatch      If the arrays are not equivalent, mismatch will point to
//                       the first element of the comparison that did not match.
// @return true         If vec1 and vec2 are "equal", false otherwise
template <typename T, size_t kSize>
bool ArraysAreEqual(const ConfigOptions& config_options,
                    const OptionTypeInfo& elem_info, const std::string& name,
                    const std::array<T, kSize>& array1,
                    const std::array<T, kSize>& array2, std::string* mismatch) {
  assert(array1.size() == kSize);
  assert(array2.size() == kSize);
  for (size_t i = 0; i < kSize; ++i) {
    if (!elem_info.AreEqual(config_options, name, &array1[i], &array2[i],
                            mismatch)) {
      return false;
    }
  }
  return true;
}

// Parses the input value into elements of the result vector.  This method
// will break the input value into the individual tokens (based on the
// separator), where each of those tokens will be parsed based on the rules of
// elem_info. The result vector will be populated with elements based on the
// input tokens. For example, if the value=1:2:3:4:5 and elem_info parses
// integers, the result vector will contain the integers 1,2,3,4,5
// @param config_options Controls how the option value is parsed.
// @param elem_info Controls how individual tokens in value are parsed
// @param separator Character separating tokens in values (':' in the above
// example)
// @param name      The name associated with this vector option
// @param value     The input string to parse into tokens
// @param result    Returns the results of parsing value into its elements.
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or if the token
//                          could not be parsed
// @return NotFound         If the tokenized value contains unknown options for
// its type
template <typename T>
Status ParseVector(const ConfigOptions& config_options,
                   const OptionTypeInfo& elem_info, char separator,
                   const std::string& name, const std::string& value,
                   std::vector<T>* result) {
  result->clear();
  Status status;

  // Turn off ignore_unknown_objects so we can tell if the returned
  // object is valid or not.
  ConfigOptions copy = config_options;
  copy.ignore_unsupported_options = false;
  // Permit the caller to pass either bare `item1:item2` or wrapped
  // `{item1:item2}`. Either form parses identically.
  std::string stripped = OptionTypeInfo::StripOuterBraces(value);
  for (size_t start = 0, end = 0;
       status.ok() && start < stripped.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status =
        OptionTypeInfo::NextToken(stripped, separator, start, &end, &token);
    if (status.ok()) {
      T elem;
      status = elem_info.Parse(copy, name, token, &elem);
      if (status.ok()) {
        result->emplace_back(elem);
      } else if (config_options.ignore_unsupported_options &&
                 status.IsNotSupported()) {
        // If we were ignoring unsupported options and this one should be
        // ignored, ignore it by setting the status to OK
        status = Status::OK();
      }
    }
  }
  return status;
}

// Serializes the input vector into its output value.  Elements are
// separated by the separator character.  This element will convert all of the
// elements in vec into their serialized form, using elem_info to perform the
// serialization.
// For example, if the vec contains the integers 1,2,3,4,5 and elem_info
// serializes the output would be 1:2:3:4:5 for separator ":".
// @param config_options Controls how the option value is serialized.
// @param elem_info Controls how individual tokens in value are serialized
// @param separator Character separating tokens in value (':' in the above
// example)
// @param name      The name associated with this vector option
// @param vec       The input vector to serialize
// @param value     The output string of serialized options
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or if the token
//                          could not be parsed
// @return NotFound         If the tokenized value contains unknown options for
// its type
template <typename T>
Status SerializeVector(const ConfigOptions& config_options,
                       const OptionTypeInfo& elem_info, char separator,
                       const std::string& name, const std::vector<T>& vec,
                       std::string* value) {
  std::string result;
  ConfigOptions embedded = config_options;
  embedded.delimiter = ";";
  int printed = 0;
  for (const auto& elem : vec) {
    std::string elem_str;
    Status s = elem_info.Serialize(embedded, name, &elem, &elem_str);
    if (!s.ok()) {
      return s;
    } else if (!elem_str.empty()) {
      if (printed++ > 0) {
        result += separator;
      }
      // If the element contains embedded separators, put it inside of brackets
      if (elem_str.find(separator) != std::string::npos) {
        result += "{" + elem_str + "}";
      } else {
        result += elem_str;
      }
    }
  }
  if (result.find("=") != std::string::npos) {
    *value = "{" + result + "}";
  } else if (printed > 1 && result.at(0) == '{') {
    *value = "{" + result + "}";
  } else {
    *value = result;
  }
  return Status::OK();
}

// Compares the input vectors vec1 and vec2 for equality
// If the vectors are the same size, elements of the vectors are compared one by
// one using elem_info to perform the comparison.
//
// @param config_options Controls how the vectors are compared.
// @param elem_info Controls how individual elements in the vectors are compared
// @param name      The name associated with this vector option
// @param vec1,vec2 The vectors to compare.
// @param mismatch  If the vectors are not equivalent, mismatch will point to
// the first
//                  element of the comparison that did not match.
// @return true     If vec1 and vec2 are "equal", false otherwise
template <typename T>
bool VectorsAreEqual(const ConfigOptions& config_options,
                     const OptionTypeInfo& elem_info, const std::string& name,
                     const std::vector<T>& vec1, const std::vector<T>& vec2,
                     std::string* mismatch) {
  if (vec1.size() != vec2.size()) {
    *mismatch = name;
    return false;
  } else {
    for (size_t i = 0; i < vec1.size(); ++i) {
      if (!elem_info.AreEqual(
              config_options, name, reinterpret_cast<const char*>(&vec1[i]),
              reinterpret_cast<const char*>(&vec2[i]), mismatch)) {
        return false;
      }
    }
    return true;
  }
}
}  // namespace ROCKSDB_NAMESPACE
