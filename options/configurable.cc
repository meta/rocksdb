// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"

#include "logging/logging.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
intptr_t GetOffset(const Configurable* holder, void* field) {
  return reinterpret_cast<intptr_t>(field) -
         reinterpret_cast<intptr_t>(static_cast<const void*>(holder));
}

void* ApplyOffset(const Configurable* holder, intptr_t offset) {
  // offset can point outside *holder, into the derived object.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<void*>(
      reinterpret_cast<intptr_t>(static_cast<const void*>(holder)) + offset);
}
}  // namespace

void Configurable::RegisterOptions(
    const std::string& name, void* opt_ptr,
    const std::unordered_map<std::string, OptionTypeInfo>* type_map) {
  RegisteredOptions opts;
  opts.name = name;
  opts.type_map = type_map;
  opts.opt_offset = GetOffset(this, opt_ptr);
  options_.emplace_back(opts);
}

//*************************************************************************
//
//       Methods for Initializing and Validating Configurable Objects
//
//*************************************************************************

Status Configurable::PrepareOptions(const ConfigOptions& opts) {
  // We ignore the invoke_prepare_options here intentionally,
  // as if you are here, you must have called PrepareOptions explicitly.
  Status status = Status::OK();
  for (const auto& opt_iter : options_) {
    if (opt_iter.type_map != nullptr) {
      for (const auto& map_iter : *(opt_iter.type_map)) {
        auto& opt_info = map_iter.second;
        if (opt_info.ShouldPrepare()) {
          status = opt_info.Prepare(opts, map_iter.first,
                                    ApplyOffset(this, opt_iter.opt_offset));
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
  return status;
}

Status Configurable::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  Status status;
  for (const auto& opt_iter : options_) {
    if (opt_iter.type_map != nullptr) {
      for (const auto& map_iter : *(opt_iter.type_map)) {
        auto& opt_info = map_iter.second;
        if (opt_info.ShouldValidate()) {
          status = opt_info.Validate(db_opts, cf_opts, map_iter.first,
                                     ApplyOffset(this, opt_iter.opt_offset));
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
  return status;
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Retrieving Options from Configurables */
/*                                                                               */
/*********************************************************************************/

const void* Configurable::GetOptionsPtr(const std::string& name) const {
  for (const auto& o : options_) {
    if (o.name == name) {
      return ApplyOffset(this, o.opt_offset);
    }
  }
  return nullptr;
}

std::string Configurable::GetOptionName(const std::string& opt_name) const {
  return opt_name;
}

const OptionTypeInfo* ConfigurableHelper::FindOption(
    const Configurable& configurable, const std::string& short_name,
    std::string* opt_name, void** opt_ptr) {
  for (const auto& iter : configurable.options_) {
    if (iter.type_map != nullptr) {
      const auto opt_info =
          OptionTypeInfo::Find(short_name, *(iter.type_map), opt_name);
      if (opt_info != nullptr) {
        *opt_ptr = ApplyOffset(&configurable, iter.opt_offset);
        return opt_info;
      }
    }
  }
  return nullptr;
}

//*************************************************************************
//
//       Methods for Configuring Options from Strings/Name-Value Pairs/Maps
//
//*************************************************************************

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map) {
  Status s = ConfigureFromMap(config_options, opts_map, nullptr);
  return s;
}

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  return ConfigureOptions(config_options, opts_map, unused);
}

Status Configurable::ConfigureOptions(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  std::string curr_opts;
  Status s;
  if (!opts_map.empty()) {
    // There are options in the map.
    // Save the current configuration in curr_opts and then configure the
    // options, but do not prepare them now.  We will do all the prepare when
    // the configuration is complete.
    ConfigOptions copy = config_options;
    copy.invoke_prepare_options = false;
    if (!config_options.ignore_unknown_options) {
      // If we are not ignoring unused, get the defaults in case we need to
      // reset
      copy.depth = ConfigOptions::kDepthDetailed;
      copy.delimiter = "; ";
      GetOptionString(copy, &curr_opts).PermitUncheckedError();
    }

    s = ConfigurableHelper::ConfigureOptions(copy, *this, opts_map, unused);
  }
  if (config_options.invoke_prepare_options && s.ok()) {
    s = PrepareOptions(config_options);
  }
  if (!s.ok() && !curr_opts.empty()) {
    ConfigOptions reset = config_options;
    reset.ignore_unknown_options = true;
    reset.invoke_prepare_options = true;
    reset.ignore_unsupported_options = true;
    // There are some options to reset from this current error
    ConfigureFromString(reset, curr_opts).PermitUncheckedError();
  }
  return s;
}

Status Configurable::ParseStringOptions(const ConfigOptions& /*config_options*/,
                                        const std::string& /*opts_str*/) {
  return Status::OK();
}

Status Configurable::ConfigureFromString(const ConfigOptions& config_options,
                                         const std::string& opts_str) {
  Status s;
  if (!opts_str.empty()) {
    if (opts_str.find(';') != std::string::npos ||
        opts_str.find('=') != std::string::npos) {
      std::unordered_map<std::string, std::string> opt_map;
      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = ConfigureFromMap(config_options, opt_map, nullptr);
      }
    } else {
      s = ParseStringOptions(config_options, opts_str);
      if (s.ok() && config_options.invoke_prepare_options) {
        s = PrepareOptions(config_options);
      }
    }
  } else if (config_options.invoke_prepare_options) {
    s = PrepareOptions(config_options);
  } else {
    s = Status::OK();
  }
  return s;
}

/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const ConfigOptions& config_options,
                                     const std::string& name,
                                     const std::string& value) {
  return ConfigurableHelper::ConfigureSingleOption(config_options, *this, name,
                                                   value);
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */

Status Configurable::ParseOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const std::string& opt_value, void* opt_ptr) {
  if (opt_info.IsMutable()) {
    if (config_options.mutable_options_only) {
      // This option is mutable. Treat all of its children as mutable as well
      ConfigOptions copy = config_options;
      copy.mutable_options_only = false;
      return opt_info.Parse(copy, opt_name, opt_value, opt_ptr);
    } else {
      return opt_info.Parse(config_options, opt_name, opt_value, opt_ptr);
    }
  } else if (config_options.mutable_options_only) {
    return Status::InvalidArgument("Option not changeable: " + opt_name);
  } else {
    return opt_info.Parse(config_options, opt_name, opt_value, opt_ptr);
  }
}

Status ConfigurableHelper::ConfigureOptions(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  std::unordered_map<std::string, std::string> remaining = opts_map;
  Status s = Status::OK();
  if (!opts_map.empty()) {
    for (const auto& iter : configurable.options_) {
      if (iter.type_map != nullptr) {
        s = ConfigureSomeOptions(config_options, configurable, *(iter.type_map),
                                 &remaining,
                                 ApplyOffset(&configurable, iter.opt_offset));
        if (remaining.empty()) {  // Are there more options left?
          break;
        } else if (!s.ok()) {
          break;
        }
      }
    }
  }
  if (unused != nullptr && !remaining.empty()) {
    unused->insert(remaining.begin(), remaining.end());
  }
  if (config_options.ignore_unknown_options) {
    s = Status::OK();
  } else if (s.ok() && unused == nullptr && !remaining.empty()) {
    s = Status::NotFound("Extra option not recognized",
                         remaining.begin()->first);
  }
  return s;
}

/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any properties that were found are removed from the options list;
 * upon return only options that were not found in this opt_map remain.

 * Returns:
 * -  OK if ignore_unknown_options is set
 * - InvalidArgument, if any option was invalid
 * - NotSupported, if any option is unsupported and ignore_unsupported_options
 is OFF
 * - OK, if no option was invalid or not supported (or ignored)
 */
Status ConfigurableHelper::ConfigureSomeOptions(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    std::unordered_map<std::string, std::string>* options, void* opt_ptr) {
  Status result = Status::OK();  // The last non-OK result (if any)
  Status notsup = Status::OK();  // The last NotSupported result (if any)
  std::string elem_name;
  int found = 1;
  std::unordered_set<std::string> unsupported;
  // While there are unused properties and we processed at least one,
  // go through the remaining unused properties and attempt to configure them.
  while (found > 0 && !options->empty()) {
    found = 0;
    notsup = Status::OK();
    for (auto it = options->begin(); it != options->end();) {
      const std::string& opt_name = configurable.GetOptionName(it->first);
      const std::string& opt_value = it->second;
      const auto opt_info =
          OptionTypeInfo::Find(opt_name, type_map, &elem_name);
      if (opt_info == nullptr) {  // Did not find the option.  Skip it
        ++it;
      } else {
        Status s = ConfigureOption(config_options, configurable, *opt_info,
                                   opt_name, elem_name, opt_value, opt_ptr);
        if (s.IsNotFound()) {
          ++it;
        } else if (s.IsNotSupported()) {
          notsup = s;
          unsupported.insert(it->first);
          ++it;  // Skip it for now
        } else {
          found++;
          it = options->erase(it);
          if (!s.ok()) {
            result = s;
          }
        }
      }
    }  // End for all remaining options
  }  // End while found one or options remain

  // Now that we have been through the list, remove any unsupported
  for (const auto& u : unsupported) {
    auto it = options->find(u);
    if (it != options->end()) {
      options->erase(it);
    }
  }
  if (config_options.ignore_unknown_options) {
    if (!result.ok()) {
      result.PermitUncheckedError();
    }
    if (!notsup.ok()) {
      notsup.PermitUncheckedError();
    }
    return Status::OK();
  } else if (!result.ok()) {
    if (!notsup.ok()) {
      notsup.PermitUncheckedError();
    }
    return result;
  } else if (config_options.ignore_unsupported_options) {
    if (!notsup.ok()) {
      notsup.PermitUncheckedError();
    }
    return Status::OK();
  } else {
    return notsup;
  }
}

Status ConfigurableHelper::ConfigureSingleOption(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::string& name, const std::string& value) {
  const std::string& opt_name = configurable.GetOptionName(name);
  std::string elem_name;
  void* opt_ptr = nullptr;
  const auto opt_info =
      FindOption(configurable, opt_name, &elem_name, &opt_ptr);
  if (opt_info == nullptr) {
    return Status::NotFound("Could not find option", name);
  } else {
    return ConfigureOption(config_options, configurable, *opt_info, opt_name,
                           elem_name, value, opt_ptr);
  }
}
Status ConfigurableHelper::ConfigureCustomizableOption(
    const ConfigOptions& config_options, Configurable& configurable,
    const OptionTypeInfo& opt_info, const std::string& opt_name,
    const std::string& name, const std::string& value, void* opt_ptr) {
  Customizable* custom = opt_info.AsRawPointer<Customizable>(opt_ptr);
  ConfigOptions copy = config_options;
  if (opt_info.IsMutable()) {
    // This option is mutable. Pass that property on to any subsequent calls
    copy.mutable_options_only = false;
  }

  if (opt_info.IsMutable() || !config_options.mutable_options_only) {
    // Either the option is mutable, or we are processing all of the options
    if (opt_name == name || name == OptionTypeInfo::kIdPropName() ||
        EndsWith(opt_name, OptionTypeInfo::kIdPropSuffix())) {
      return configurable.ParseOption(copy, opt_info, name, value, opt_ptr);
    } else if (value.empty()) {
      return Status::OK();
    } else if (custom == nullptr || !StartsWith(name, custom->GetId() + ".")) {
      return configurable.ParseOption(copy, opt_info, name, value, opt_ptr);
    } else if (value.find('=') != std::string::npos) {
      return custom->ConfigureFromString(copy, value);
    } else {
      return custom->ConfigureOption(copy, name, value);
    }
  } else {
    // We are processing immutable options, which means that we cannot change
    // the Customizable object itself, but could change its mutable properties.
    // Check to make sure that nothing is trying to change the Customizable
    if (custom == nullptr) {
      // We do not have a Customizable to configure.  This is OK if the
      // value is empty (nothing being configured) but an error otherwise
      if (value.empty()) {
        return Status::OK();
      } else {
        return Status::InvalidArgument("Option not changeable: " + opt_name);
      }
    } else if (EndsWith(opt_name, OptionTypeInfo::kIdPropSuffix()) ||
               name == OptionTypeInfo::kIdPropName()) {
      // We have a property of the form "id=value" or "table.id=value"
      // This is OK if we ID/value matches the current customizable object
      if (custom->GetId() == value) {
        return Status::OK();
      } else {
        return Status::InvalidArgument("Option not changeable: " + opt_name);
      }
    } else if (opt_name == name) {
      // The properties are of one of forms:
      //    name = { id = id; prop1 = value1; ... }
      //    name = { prop1=value1; prop2=value2; ... }
      //    name = ID
      // Convert the value to a map and extract the ID
      // If the ID does not match that of the current customizable, return an
      // error. Otherwise, update the current customizable via the properties
      // map
      std::unordered_map<std::string, std::string> props;
      std::string id;
      Status s =
          Configurable::GetOptionsMap(value, custom->GetId(), &id, &props);
      if (!s.ok()) {
        return s;
      } else if (custom->GetId() != id) {
        return Status::InvalidArgument("Option not changeable: " + opt_name);
      } else if (props.empty()) {
        return Status::OK();
      } else {
        return custom->ConfigureFromMap(copy, props);
      }
    } else {
      // Attempting to configure one of the properties of the customizable
      // Let it through
      return custom->ConfigureOption(copy, name, value);
    }
  }
}

Status ConfigurableHelper::ConfigureOption(
    const ConfigOptions& config_options, Configurable& configurable,
    const OptionTypeInfo& opt_info, const std::string& opt_name,
    const std::string& name, const std::string& value, void* opt_ptr) {
  if (opt_info.IsCustomizable()) {
    return ConfigureCustomizableOption(config_options, configurable, opt_info,
                                       opt_name, name, value, opt_ptr);
  } else if (opt_name == name) {
    return configurable.ParseOption(config_options, opt_info, opt_name, value,
                                    opt_ptr);
  } else if (opt_info.IsStruct() || opt_info.IsConfigurable()) {
    return configurable.ParseOption(config_options, opt_info, name, value,
                                    opt_ptr);
  } else {
    return Status::NotFound("Unknown how to configure option", name);
  }
}

//*******************************************************************************
//
//       Methods for Converting Options into strings
//
//*******************************************************************************

Status Configurable::GetOptionString(const ConfigOptions& config_options,
                                     std::string* result) const {
  assert(result);
  result->clear();
  return ConfigurableHelper::SerializeOptions(config_options, *this, "",
                                              result);
}

std::string Configurable::ToString(const ConfigOptions& config_options,
                                   const std::string& prefix) const {
  std::string result = SerializeOptions(config_options, prefix);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::SerializeOptions(const ConfigOptions& config_options,
                                           const std::string& header) const {
  std::string result;
  Status s = ConfigurableHelper::SerializeOptions(config_options, *this, header,
                                                  &result);
  assert(s.ok());
  return result;
}

Status Configurable::GetOption(const ConfigOptions& config_options,
                               const std::string& name,
                               std::string* value) const {
  return ConfigurableHelper::GetOption(config_options, *this,
                                       GetOptionName(name), value);
}

Status ConfigurableHelper::GetOption(const ConfigOptions& config_options,
                                     const Configurable& configurable,
                                     const std::string& short_name,
                                     std::string* value) {
  // Look for option directly
  assert(value);
  value->clear();

  std::string opt_name;
  void* opt_ptr = nullptr;
  const auto opt_info =
      FindOption(configurable, short_name, &opt_name, &opt_ptr);
  if (opt_info != nullptr) {
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";
    if (short_name == opt_name) {
      return opt_info->Serialize(embedded, opt_name, opt_ptr, value);
    } else if (opt_info->IsStruct()) {
      return opt_info->Serialize(embedded, opt_name, opt_ptr, value);
    } else if (opt_info->IsConfigurable()) {
      auto const* config = opt_info->AsRawPointer<Configurable>(opt_ptr);
      if (config != nullptr) {
        return config->GetOption(embedded, opt_name, value);
      }
    }
  }
  return Status::NotFound("Cannot find option: ", short_name);
}

Status ConfigurableHelper::SerializeOptions(const ConfigOptions& config_options,
                                            const Configurable& configurable,
                                            const std::string& prefix,
                                            std::string* result) {
  assert(result);
  for (auto const& opt_iter : configurable.options_) {
    if (opt_iter.type_map != nullptr) {
      for (const auto& map_iter : *(opt_iter.type_map)) {
        const auto& opt_name = map_iter.first;
        const auto& opt_info = map_iter.second;
        if (opt_info.ShouldSerialize()) {
          std::string value;
          Status s;
          void* opt_ptr = ApplyOffset(&configurable, opt_iter.opt_offset);
          if (!config_options.mutable_options_only) {
            s = opt_info.Serialize(config_options, prefix + opt_name, opt_ptr,
                                   &value);
          } else if (opt_info.IsMutable()) {
            ConfigOptions copy = config_options;
            copy.mutable_options_only = false;
            s = opt_info.Serialize(copy, prefix + opt_name, opt_ptr, &value);
          } else if (opt_info.IsConfigurable()) {
            // If it is a Configurable and we are either printing all of the
            // details or not printing only the name, this option should be
            // included in the list
            if (config_options.IsDetailed() ||
                !opt_info.IsEnabled(OptionTypeFlags::kStringNameOnly)) {
              s = opt_info.Serialize(config_options, prefix + opt_name, opt_ptr,
                                     &value);
            }
          }
          if (!s.ok()) {
            return s;
          } else if (!value.empty()) {
            // <prefix><opt_name>=<value><delimiter>
            result->append(prefix)
                .append(opt_name)
                .append("=")
                .append(value)
                .append(config_options.delimiter);
          }
        }
      }
    }
  }
  return Status::OK();
}

//********************************************************************************
//
// Methods for listing the options from Configurables
//
//********************************************************************************
Status Configurable::GetOptionNames(
    const ConfigOptions& config_options,
    std::unordered_set<std::string>* result) const {
  assert(result);
  return ConfigurableHelper::ListOptions(config_options, *this, "", result);
}

Status ConfigurableHelper::ListOptions(
    const ConfigOptions& config_options, const Configurable& configurable,
    const std::string& prefix, std::unordered_set<std::string>* result) {
  Status status;
  for (auto const& opt_iter : configurable.options_) {
    if (opt_iter.type_map != nullptr) {
      for (const auto& map_iter : *(opt_iter.type_map)) {
        const auto& opt_name = map_iter.first;
        const auto& opt_info = map_iter.second;
        // If the option is no longer used in rocksdb and marked as deprecated,
        // we skip it in the serialization.
        if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
          if (!config_options.mutable_options_only) {
            result->emplace(prefix + opt_name);
          } else if (opt_info.IsMutable()) {
            result->emplace(prefix + opt_name);
          }
        }
      }
    }
  }
  return status;
}

//*******************************************************************************
//
//       Methods for Comparing Configurables
//
//*******************************************************************************

bool Configurable::AreEquivalent(const ConfigOptions& config_options,
                                 const Configurable* other,
                                 std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || config_options.IsCheckDisabled()) {
    return true;
  } else if (other != nullptr) {
    return ConfigurableHelper::AreEquivalent(config_options, *this, *other,
                                             name);
  } else {
    return false;
  }
}

bool Configurable::OptionsAreEqual(const ConfigOptions& config_options,
                                   const OptionTypeInfo& opt_info,
                                   const std::string& opt_name,
                                   const void* const this_ptr,
                                   const void* const that_ptr,
                                   std::string* mismatch) const {
  if (opt_info.AreEqual(config_options, opt_name, this_ptr, that_ptr,
                        mismatch)) {
    return true;
  } else if (opt_info.AreEqualByName(config_options, opt_name, this_ptr,
                                     that_ptr)) {
    *mismatch = "";
    return true;
  } else {
    return false;
  }
}

bool ConfigurableHelper::AreEquivalent(const ConfigOptions& config_options,
                                       const Configurable& this_one,
                                       const Configurable& that_one,
                                       std::string* mismatch) {
  assert(mismatch != nullptr);
  for (auto const& o : this_one.options_) {
    const auto this_offset = this_one.GetOptionsPtr(o.name);
    const auto that_offset = that_one.GetOptionsPtr(o.name);
    if (this_offset != that_offset) {
      if (this_offset == nullptr || that_offset == nullptr) {
        return false;
      } else if (o.type_map != nullptr) {
        for (const auto& map_iter : *(o.type_map)) {
          const auto& opt_info = map_iter.second;
          if (config_options.IsCheckEnabled(opt_info.GetSanityLevel())) {
            if (!config_options.mutable_options_only) {
              if (!this_one.OptionsAreEqual(config_options, opt_info,
                                            map_iter.first, this_offset,
                                            that_offset, mismatch)) {
                return false;
              }
            } else if (opt_info.IsMutable()) {
              ConfigOptions copy = config_options;
              copy.mutable_options_only = false;
              if (!this_one.OptionsAreEqual(copy, opt_info, map_iter.first,
                                            this_offset, that_offset,
                                            mismatch)) {
                return false;
              }
            }
          }
        }
      }
    }
  }
  return true;
}

Status Configurable::GetOptionsMap(
    const std::string& value, const std::string& default_id, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  assert(id);
  assert(props);
  Status status;
  if (value.empty() || value == kNullptrString) {
    *id = default_id;
  } else if (value.find('=') == std::string::npos) {
    *id = value;
  } else {
    status = StringToMap(value, props);
    if (!status.ok()) {       // There was an error creating the map.
      *id = value;            // Treat the value as id
      props->clear();         // Clear the properties
      status = Status::OK();  // and ignore the error
    } else {
      auto iter = props->find(OptionTypeInfo::kIdPropName());
      if (iter != props->end()) {
        *id = iter->second;
        props->erase(iter);
        if (*id == kNullptrString) {
          id->clear();
        }
      } else if (!default_id.empty()) {
        *id = default_id;
      } else {           // No id property and no default
        *id = value;     // Treat the value as id
        props->clear();  // Clear the properties
      }
    }
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
