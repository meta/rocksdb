// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class ObjectRegistry;
class OptionTypeInfo;
struct ColumnFamilyOptions;
struct ConfigOptions;
struct DBOptions;

// Configurable is a base class used by the rocksdb that describes a
// standard way of configuring objects.  A Configurable object can:
//   -> Populate itself given:
//        - One or more "name/value" pair strings
//        - A string representing the set of name=value properties
//        - A map of name/value properties.
//   -> Convert itself into its string representation
//   -> Dump itself to a Logger
//   -> Compare itself to another Configurable object to see if the two objects
// have equivalent options settings
//
// If a derived class calls RegisterOptions to register (by name) how its
// options objects are to be processed, this functionality can typically be
// handled by this class without additional overrides. Otherwise, the derived
// class will need to implement the methods for handling the corresponding
// functionality.
class Configurable {
 protected:
  friend class ConfigurableHelper;
  struct RegisteredOptions {
    // The name of the options being registered
    std::string name;
    // Pointer to the object being registered, relative to `this` so that
    // RegisteredOptions are copyable from one Configurable to another of the
    // same type, assuming the option is a member of `this`.
    intptr_t opt_offset;
    // The map of options being registered
    const std::unordered_map<std::string, OptionTypeInfo>* type_map;
  };

 public:
  Configurable() = default;
  Configurable(const Configurable&) = default;
  Configurable& operator=(const Configurable&) = default;
  Configurable(Configurable&&) = default;
  Configurable& operator=(Configurable&&) = default;
  virtual ~Configurable() = default;

  // Returns the raw pointer of the named options that is used by this
  // object, or nullptr if this function is not supported.
  // Since the return value is a raw pointer, the object owns the
  // pointer and the caller should not delete the pointer.
  //
  // Note that changing the underlying options while the object
  // is currently used by any open DB is undefined behavior.
  // Developers should use DB::SetOptions() instead to dynamically change
  // options while the DB is open.
  template <typename T>
  const T* GetOptions() const {
    return GetOptions<T>(T::kName());
  }
  template <typename T>
  T* GetOptions() {
    return GetOptions<T>(T::kName());
  }
  template <typename T>
  const T* GetOptions(const std::string& name) const {
    return reinterpret_cast<const T*>(GetOptionsPtr(name));
  }
  template <typename T>
  T* GetOptions(const std::string& name) {
    // FIXME: Is this sometimes reading a raw pointer from a shared_ptr,
    // unsafely relying on the object layout?
    return reinterpret_cast<T*>(const_cast<void*>(GetOptionsPtr(name)));
  }

  // Configures the options for this class based on the input parameters.
  // On successful completion, the object is updated with the settings from
  // the opt_map.
  // If this method fails, an attempt is made to revert the object to original
  // state. Note that the revert may not be the original state but may be an
  // equivalent. For example, if the object contains an option that is a
  // shared_ptr, the shared_ptr may not be the original one but a copy (e.g. not
  // the Cache object that was passed in, but a Cache object of the same size).
  //
  // The acceptable values of the name/value pairs are documented with the
  // specific class/instance.
  //
  // @param config_options Controls how the arguments are processed.
  // @param opt_map Name/value pairs of the options to update
  // @param unused If specified, this value will return the name/value
  //      pairs from opt_map that were NotFound for this object.
  // @return OK If all values in the map were successfully updated
  //      If invoke_prepare_options is true, OK also implies
  //      PrepareOptions ran successfully.
  // @return NotFound If any of the names in the opt_map were not valid
  //      for this object.  If unused is specified, it will contain the
  //      collection of NotFound names.
  // @return NotSupported  If any of the names are valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If any of the values cannot be successfully
  //       parsed.  This can also be returned if PrepareOptions encounters an
  //       error.
  // @see ConfigOptions for a description of the controls.
  Status ConfigureFromMap(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opt_map);
  Status ConfigureFromMap(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opt_map,
      std::unordered_map<std::string, std::string>* unused);

  // Updates the named option to the input value, returning OK if successful.
  // Note that ConfigureOption does not cause PrepareOptions to be invoked.
  // @param config_options Controls how the name/value is processed.
  // @param name The name of the option to update
  // @param value The value to set for the named option
  // @return OK If the named field was successfully updated to value.
  // @return NotFound If the name is not valid for this object.
  // @return NotSupported  If the name is valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If the value cannot be successfully  parsed.
  Status ConfigureOption(const ConfigOptions& config_options,
                         const std::string& name, const std::string& value);

  // Configures the options for this class based on the input parameters.
  // On successful completion, the object is updated with the settings from
  // the opt_map.  If this method fails, an attempt is made to revert the
  // object to original state.  Note that the revert may not be the original
  // state but may be an equivalent.
  // @see ConfigureFromMap for more details
  // @param config_options Controls how the arguments are processed.
  // @param opt_str string containing the values to update.
  // @param unused If specified, this value will return the name/value
  //      pairs from opt_map that were NotFound for this object.
  // @return OK If all specified values were successfully updated
  //      If invoke_prepare_options is true, OK also implies
  //      PrepareOptions ran successfully.
  // @return NotFound If any of the names were not valid for this object.
  //      If unused is specified, it will contain the collection of NotFound
  //      names.
  // @return NotSupported  If any of the names are valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If any of the values cannot be successfully
  //       parsed.  This can also be returned if PrepareOptions encounters an
  //       error.
  Status ConfigureFromString(const ConfigOptions& config_options,
                             const std::string& opts);

  // Fills in result with the serialized options for this object.
  // This is the inverse of ConfigureFromString.
  // @param config_options Controls how serialization happens.
  // @param result The string representation of this object.
  // @return OK If the options for this object were successfully serialized.
  // @return InvalidArgument If one or more of the options could not be
  // serialized.
  Status GetOptionString(const ConfigOptions& config_options,
                         std::string* result) const;
  // Returns the serialized options for this object.
  // This method is similar to GetOptionString with no errors.
  // @param config_options Controls how serialization happens.
  // @param prefix A string to prepend to every option.
  // @return The serialized representation of the options for this object
  std::string ToString(const ConfigOptions& config_options) const {
    return ToString(config_options, "");
  }
  std::string ToString(const ConfigOptions& config_options,
                       const std::string& prefix) const;

  // Returns the list of option names associated with this configurable
  // @param config_options Controls how the names are returned
  // @param result The set of option names for this object. Note that
  //      options that are deprecated or aliases are not returned.
  // @return OK on success.
  Status GetOptionNames(const ConfigOptions& config_options,
                        std::unordered_set<std::string>* result) const;

  // Returns the value of the option associated with the input name
  // This method is the functional inverse of ConfigureOption
  // @param config_options Controls how the value is returned
  // @param name The name of the option to return a value for.
  // @param value The returned value associated with the named option.
  // @return OK If the named field was successfully updated to value.
  // @return NotFound If the name is not valid for this object.
  // @param InvalidArgument If the name is valid for this object but
  //      its value cannot be serialized.
  virtual Status GetOption(const ConfigOptions& config_options,
                           const std::string& name, std::string* value) const;

  // Checks to see if this Configurable is equivalent to other.
  // This method assumes that the two objects are of the same class.
  // @param config_options Controls how the options are compared.
  // @param other The other object to compare to.
  // @param mismatch If the objects do not match, this parameter contains
  //      the name of the option that triggered the match failure.
  // @param True if the objects match, false otherwise.
  virtual bool AreEquivalent(const ConfigOptions& config_options,
                             const Configurable* other,
                             std::string* name) const;

  // Returns a pretty-printed, human-readable version of the options.
  // This method is typically used to dump the options to a log file.
  // Classes should override this method
  virtual std::string GetPrintableOptions() const { return ""; }

  // Validates that the settings are valid/consistent and performs any object
  // initialization required by this object.  This method may be called as part
  // of Configure (if invoke_prepare_options is set), or may be invoked
  // separately.
  //
  // Once an object has been prepared, non-mutable options can no longer be
  // updated.
  //
  // Classes must override this method to provide any implementation-specific
  // initialization, such as opening log files or setting up cache parameters.
  // Implementations should be idempotent (e.g. don't re-open the log file or
  // reconfigure the cache), as there is the potential this method can be called
  // more than once.
  //
  // By default, this method will also prepare all nested (Inner and
  // OptionType::kConfigurable) objects.
  //
  // @param config_options Controls how the object is prepared.  Also contains
  //      a Logger and Env that can be used to initialize this object.
  // @return OK If the object was successfully initialized.
  // @return InvalidArgument If this object could not be successfully
  // initialized.
  virtual Status PrepareOptions(const ConfigOptions& config_options);

  // Checks to see if the settings are valid for this object.
  // This method checks to see if the input DBOptions and ColumnFamilyOptions
  // are valid for the settings of this object.  For example, an Env might not
  // support certain mmap modes or a TableFactory might require certain
  // settings.
  //
  // By default, this method will also validate all nested (Inner and
  // OptionType::kConfigurable) objects.
  //
  // @param db_opts The DBOptions to validate
  // @param cf_opts The ColumnFamilyOptions to validate
  // @return OK if the options are valid
  // @return InvalidArgument If the arguments are not valid for the options
  //       of the current object.
  virtual Status ValidateOptions(const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts) const;

  // Splits the input opt_value into the ID field and the remaining options.
  // The input opt_value can be in the form of "name" or "name=value
  // [;name=value]". The first form uses the "name" as an id with no options The
  // latter form converts the input into a map of name=value pairs and sets "id"
  // to the "id" value from the map.
  // @param opt_value The value to split into id and options
  // @param id The id field from the opt_value
  // @param options The remaining name/value pairs from the opt_value
  // @param default_id If specified and there is no id field in the map, this
  // value is returned as the ID
  // @return OK if the value was converted to a map successfully and an ID was
  // found.
  // @return InvalidArgument if the value could not be converted to a map or
  // there was or there is no id property in the map.
  static Status GetOptionsMap(
      const std::string& opt_value, const std::string& default_id,
      std::string* id, std::unordered_map<std::string, std::string>* options);

 protected:
  // Returns the raw pointer for the associated named option.
  // The name is typically the name of an option registered via the
  // Classes may override this method to provide further specialization (such as
  // returning a sub-option)
  //
  // The default implementation looks at the registered options.  If the
  // input name matches that of a registered option, the pointer registered
  // with that name is returned.
  // e.g,, RegisterOptions("X", &my_ptr, ...); GetOptionsPtr("X") returns
  // "my_ptr"
  virtual const void* GetOptionsPtr(const std::string& name) const;

  // Method for allowing options to be configured outside of the normal
  // registered options framework.  Classes may override this method if they
  // wish to support non-standard options implementations (such as configuring
  // themselves from constant or simple ":"-separated strings.
  //
  // The default implementation does nothing and returns OK
  virtual Status ParseStringOptions(const ConfigOptions& config_options,
                                    const std::string& opts_str);

  // Internal method to configure an object from a map of name-value options.
  // This method uses the input config_options to drive the configuration of
  // the options in opt_map.  Any option name that cannot be found from the
  // input set will be returned in "unused".
  //
  // Classes may override this method to extend the functionality if required.
  // @param config_options Controls how the options are configured and errors
  // handled.
  // @param opts_map The set of options to configure
  // @param unused Any options from opt_map that were not configured.
  // @returns a Status based on the rules outlined in ConfigureFromMap
  virtual Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused);

  // Method that configures a the specific opt_name from opt_value.
  // By default, this method calls opt_info.ParseOption with the
  // input parameters.
  // Classes may override this method to extend the functionality, or
  // change the returned Status.
  virtual Status ParseOption(const ConfigOptions& config_options,
                             const OptionTypeInfo& opt_info,
                             const std::string& opt_name,
                             const std::string& opt_value, void* opt_ptr);

  // Internal method to see if the single option name/info matches for this and
  // that Classes may override this value to change its behavior.
  // @param config_options Controls how the options are being matched
  // @param opt_info The OptionTypeInfo registered for this option name
  //      that controls what field is matched (offset) and how (type).
  // @param name The name associated with this opt_info.
  // @param this_ptr The base pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param that_ptr The other pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param bad_name  If the match fails, the name of the option that failed to
  // match.
  virtual bool OptionsAreEqual(const ConfigOptions& config_options,
                               const OptionTypeInfo& opt_info,
                               const std::string& name,
                               const void* const this_ptr,
                               const void* const that_ptr,
                               std::string* bad_name) const;
  // Internal method to serialize options (ToString)
  // Classes may override this value to change its behavior.
  virtual std::string SerializeOptions(const ConfigOptions& config_options,
                                       const std::string& header) const;

  //  Given a name (e.g. rocksdb.my.type.opt), returns the short name (opt)
  virtual std::string GetOptionName(const std::string& long_name) const;

  // Registers the input name with the options and associated map.
  // When classes register their options in this manner, most of the
  // functionality (excluding unknown options and validate/prepare) is
  // implemented by the base class.
  //
  // This method should be called in the class constructor to register the
  // option set for this object.  For example, to register the options
  // associated with the BlockBasedTableFactory, the constructor calls this
  // method passing in:
  // - the name of the options ("BlockBasedTableOptions");
  // - the options object (the BlockBasedTableOptions object for this object;
  // - the options type map for the BlockBasedTableOptions.
  // This registration allows the Configurable class to process the option
  // values associated with the BlockBasedTableOptions without further code in
  // the derived class.
  //
  // @param name    The name of this set of options (@see GetOptionsPtr)
  // @param opt_ptr Pointer to the options to associate with this name
  // @param opt_map Options map that controls how this option is configured.
  template <typename T>
  void RegisterOptions(
      T* opt_ptr,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map) {
    RegisterOptions(T::kName(), opt_ptr, opt_map);
  }
  void RegisterOptions(
      const std::string& name, void* opt_ptr,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map);

  // Returns true if there are registered options for this Configurable object
  inline bool HasRegisteredOptions() const { return !options_.empty(); }

 private:
  // Contains the collection of options (name, opt_offset, opt_map) associated
  // with this object. This collection is typically set in the constructor of
  // the specific Configurable via RegisterOptions().
  std::vector<RegisteredOptions> options_;
};
}  // namespace ROCKSDB_NAMESPACE
