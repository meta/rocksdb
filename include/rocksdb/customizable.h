// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * Customizable a base class used by the rocksdb that describes a
 * standard way of configuring and creating objects.  Customizable objects
 * are configurable objects that can be created from an ObjectRegistry.
 *
 * Customizable classes are used when there are multiple potential
 * implementations of a class for use by RocksDB (e.g. Table, Cache,
 * MergeOperator, etc).  The abstract base class is expected to define a method
 * declaring its type and a factory method for creating one of these, such as:
 * static const char *Type() { return "Table"; }
 * static Status CreateFromString(const ConfigOptions& options,
 *                                const std::string& id,
 *                                std::shared_ptr<TableFactory>* result);
 * The "Type" string is expected to be unique (no two base classes are the same
 * type). This factory is expected, based on the options and id, create and
 * return the appropriate derived type of the customizable class (e.g.
 * BlockBasedTableFactory, PlainTableFactory, etc). For extension developers,
 * helper classes and methods are provided for writing this factory.
 *
 * Instances of a Customizable class need to define:
 * - A "static const char *kClassName()" method.  This method defines the name
 * of the class instance (e.g. BlockBasedTable, LRUCache) and is used by the
 * CheckedCast method.
 * - The Name() of the object.  This name is used when creating and saving
 * instances of this class.  Typically this name will be the same as
 * kClassName().
 *
 * Additionally, Customizable classes should register any options used to
 * configure themselves with the Configurable subsystem.
 *
 * When a Customizable is being created, the "name" property specifies
 * the name of the instance being created.
 * For custom objects, their configuration and name can be specified by:
 * [prop]={name=X;option 1 = value1[; option2=value2...]}
 *
 * [prop].name=X
 * [prop].option1 = value1
 *
 * [prop].name=X
 * X.option1 =value1
 */
class Customizable : public Configurable {
 public:
  // Returns the name of this class of Customizable
  virtual const char* Name() const = 0;

  // Returns an identifier for this Customizable.
  // This could be its name or something more complex (like its URL/pattern).
  // Used for pretty printing.
  virtual std::string GetId() const {
    std::string id = Name();
    return id;
  }

  // This is typically determined by if the input name matches the
  // name of this object.
  // This method is typically used in conjunction with CheckedCast to find the
  // derived class instance from its base.  For example, if you have an Env
  // and want the "Default" env, you would IsInstanceOf("Default") to get
  // the default implementation.  This method should be used when you need a
  // specific derivative or implementation of a class.
  //
  // Intermediary caches (such as SharedCache) may wish to override this method
  // to check for the intermediary name (SharedCache).  Classes with multiple
  // potential names (e.g. "PosixEnv", "DefaultEnv") may also wish to override
  // this method.
  //
  // Note that IsInstanceOf only uses the "is-a" relationship and not "has-a".
  // Wrapped classes that have an Inner "has-a" should not be returned.
  //
  // @param name The name of the instance to find.
  // Returns true if the class is an instance of the input name.
  virtual bool IsInstanceOf(const std::string& name) const {
    if (name.empty()) {
      return false;
    } else if (name == Name()) {
      return true;
    } else {
      const char* nickname = NickName();
      if (nickname != nullptr && name == nickname) {
        return true;
      } else {
        return false;
      }
    }
  }

  const void* GetOptionsPtr(const std::string& name) const override {
    const void* ptr = Configurable::GetOptionsPtr(name);
    if (ptr != nullptr) {
      return ptr;
    } else {
      const auto inner = Inner();
      if (inner != nullptr) {
        return inner->GetOptionsPtr(name);
      } else {
        return nullptr;
      }
    }
  }

  // Returns the named instance of the Customizable as a T*, or nullptr if not
  // found. This method uses IsInstanceOf/Inner to find the appropriate class
  // instance and then casts it to the expected return type.
  template <typename T>
  const T* CheckedCast() const {
    if (IsInstanceOf(T::kClassName())) {
      return static_cast<const T*>(this);
    } else {
      const auto inner = Inner();
      if (inner != nullptr) {
        return inner->CheckedCast<T>();
      } else {
        return nullptr;
      }
    }
  }

  template <typename T>
  T* CheckedCast() {
    if (IsInstanceOf(T::kClassName())) {
      return static_cast<T*>(this);
    } else {
      auto inner = const_cast<Customizable*>(Inner());
      if (inner != nullptr) {
        return inner->CheckedCast<T>();
      } else {
        return nullptr;
      }
    }
  }

  // Checks to see if this Customizable is equivalent to other.
  // This method assumes that the two objects are of the same class.
  // @param config_options Controls how the options are compared.
  // @param other The other object to compare to.
  // @param mismatch If the objects do not match, this parameter contains
  //      the name of the option that triggered the match failure.
  // @param True if the objects match, false otherwise.
  // @see Configurable::AreEquivalent for more details
  bool AreEquivalent(const ConfigOptions& config_options,
                     const Configurable* other,
                     std::string* mismatch) const override;
  // Gets the value of the option associated with the input name
  // @see Configurable::GetOption for more details
  Status GetOption(const ConfigOptions& config_options, const std::string& name,
                   std::string* value) const override;
  // Helper method for getting for parsing the opt_value into the corresponding
  // options for use in potentially creating a new Customizable object (this
  // method is primarily a support method for LoadSharedObject et al for new
  // Customizable objects). The opt_value may be either name-value pairs
  // separated by ";" (a=b; c=d), or a simple name (a). In order to create a new
  // Customizable, the ID is determined by:
  // - If the value is a simple name (e.g. "BlockBasedTable"), the id is this
  // name;
  // - Otherwise, if there is a "id=value", the id is set to "value"
  // - Otherwise, if the input customizable is not null, custom->GetId is used
  // - Otherwise, an error is returned.
  //
  // If the opt_value is name-value pairs, these pairs will be returned in
  // options (without the id pair). If the ID being returned matches the ID of
  // the input custom object, then the options from the input object will also
  // be added to the returned options.
  //
  // This method returns non-OK if the ID could not be found, or if the
  // opt_value could not be parsed into name-value pairs.
  static Status GetOptionsMap(
      const ConfigOptions& config_options, const Customizable* custom,
      const std::string& opt_value, std::string* id,
      std::unordered_map<std::string, std::string>* options);

  // Helper method to configure a new object with the supplied options.
  // If the object is not null and invoke_prepare_options=true, the object
  // will be configured and prepared.
  // Returns success if the object is properly configured and (optionally)
  // prepared Returns InvalidArgument if the object is nullptr and there are
  // options in the map Returns the result of the ConfigureFromMap or
  // PrepareOptions
  static Status ConfigureNewObject(
      const ConfigOptions& config_options, Customizable* object,
      const std::unordered_map<std::string, std::string>& options);

  // Returns the inner class when a Customizable implements a has-a (wrapped)
  // relationship.  Derived classes that implement a has-a must override this
  // method in order to get CheckedCast to function properly.
  virtual const Customizable* Inner() const { return nullptr; }

 protected:
  // Generates a ID specific for this instance of the customizable.
  // The unique ID is of the form <name>:<addr>#pid, where:
  // - name is the Name() of this object;
  // - addr is the memory address of this object;
  // - pid is the process ID of this process ID for this process.
  // Note that if obj1 and obj2 have the same unique IDs, they must be the
  // same.  However, if an object is deleted and recreated, it may have the
  // same unique ID as a predecessor
  //
  // This method is useful for objects (especially ManagedObjects) that
  // wish to generate an ID that is specific for this instance and wish to
  // override the GetId() method.
  std::string GenerateIndividualId() const;

  // Some classes have both a class name (e.g. PutOperator) and a nickname
  // (e.g. put). Classes can override this method to return a
  // nickname.  Nicknames can be used by InstanceOf and object creation.
  virtual const char* NickName() const { return ""; }
  //  Given a name (e.g. rocksdb.my.type.opt), returns the short name (opt)
  std::string GetOptionName(const std::string& long_name) const override;
  std::string SerializeOptions(const ConfigOptions& options,
                               const std::string& prefix) const override;
};

}  // namespace ROCKSDB_NAMESPACE
