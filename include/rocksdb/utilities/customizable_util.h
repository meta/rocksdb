// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// The methods in this file are used to instantiate new Customizable
// instances of objects.  These methods are most typically used by
// the "CreateFromString" method of a customizable class.
// If not developing a new Type of customizable class, you probably
// do not need the methods in this file.
//
// See https://github.com/meta/rocksdb/wiki/RocksDB-Configurable-Objects
// for more information on how to develop and use customizable objects

#pragma once
#include <memory>
#include <unordered_map>

#include "rocksdb/convenience.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {
// Creates a new shared customizable instance object based on the
// input parameters using the object registry.
//
// The id parameter specifies the instance class of the object to create.
// The opt_map parameter specifies the configuration of the new instance.
//
// The config_options parameter controls the process and how errors are
// returned. If ignore_unknown_options=true, unknown values are ignored during
// the configuration. If ignore_unsupported_options=true, unknown instance types
// are ignored. If invoke_prepare_options=true, the resulting instance will be
// initialized (via PrepareOptions)
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param id The identifier of the new object being created.  This string
// will be used by the object registry to locate the appropriate object to
// create.
// @param opt_map Optional name-value pairs of properties to set for the newly
// created object
// @param result The newly created and configured instance.
template <typename T>
static Status NewSharedObject(
    const ConfigOptions& config_options, const std::string& id,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<T>* result) {
  if (!id.empty()) {
    Status status;
    status = config_options.registry->NewSharedObject(id, result);
    if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
      status = Status::OK();
    } else if (status.ok()) {
      status = Customizable::ConfigureNewObject(config_options, result->get(),
                                                opt_map);
    }
    return status;
  } else if (opt_map.empty()) {
    // There was no ID and no map (everything empty), so reset/clear the result
    result->reset();
    return Status::OK();
  } else {
    return Status::NotSupported("Cannot reset object ");
  }
}

// Creates a new managed customizable instance object based on the
// input parameters using the object registry.  Unlike "shared" objects,
// managed objects are limited to a single instance per ID.
//
// The id parameter specifies the instance class of the object to create.
// If an object with this id exists in the registry, the existing object
// will be returned.  If the object does not exist, a new one will be created.
//
// The opt_map parameter specifies the configuration of the new instance.
// If the object already exists, the existing object is returned "as is" and
// this parameter is ignored.
//
// The config_options parameter controls the process and how errors are
// returned. If ignore_unknown_options=true, unknown values are ignored during
// the configuration. If ignore_unsupported_options=true, unknown instance types
// are ignored. If invoke_prepare_options=true, the resulting instance will be
// initialized (via PrepareOptions)
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param id The identifier of the object.  This string
// will be used by the object registry to locate the appropriate object to
// create or return.
// @param opt_map Optional name-value pairs of properties to set for the newly
// created object
// @param result The managed instance.
template <typename T>
static Status NewManagedObject(
    const ConfigOptions& config_options, const std::string& id,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<T>* result) {
  Status status;
  if (!id.empty()) {
    status = config_options.registry->GetOrCreateManagedObject<T>(
        id, result, [config_options, opt_map](T* object) {
          return object->ConfigureFromMap(config_options, opt_map);
        });
    if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
      return Status::OK();
    }
  } else {
    status = Status::NotSupported("Cannot reset object ");
  }
  return status;
}

// Creates a new shared Customizable object based on the input parameters.
// This method parses the input value to determine the type of instance to
// create. If there is an existing instance (in result) and it is the same ID
// as the object being created, the existing configuration is stored and used as
// the default for the new object.
//
// The value parameter specified the instance class of the object to create.
// If it is a simple string (e.g. BlockBasedTable), then the instance will be
// created using the default settings.  If the value is a set of name-value
// pairs, then the "id" value is used to determine the instance to create and
// the remaining parameters are used to configure the object.  Id name-value
// pairs are specified, there should be an "id=value" pairing or an error may
// result.
//
// The config_options parameter controls the process and how errors are
// returned. If ignore_unknown_options=true, unknown values are ignored during
// the configuration. If ignore_unsupported_options=true, unknown instance types
// are ignored. If invoke_prepare_options=true, the resulting instance will be
// initialized (via PrepareOptions)
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to create and initailize the object
// @param result The newly created instance.
template <typename T>
static Status LoadSharedObject(const ConfigOptions& config_options,
                               const std::string& value,
                               std::shared_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;

  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else {
    return NewSharedObject(config_options, id, opt_map, result);
  }
}

// Creates a new shared Customizable object based on the input parameters.
//
// The value parameter specified the instance class of the object to create.
// If it is a simple string (e.g. BlockBasedTable), then the instance will be
// created using the default settings.  If the value is a set of name-value
// pairs, then the "id" value is used to determine the instance to create and
// the remaining parameters are used to configure the object.  Id name-value
// pairs are specified, there should be an "id=value" pairing or an error may
// result.
//
// The "id" field from the value (either the whole field or "id=XX") is used
// to determine the type/id of the object to return.  For a given id, there
// the same instance of the object will be returned from this method (as opposed
// to LoadSharedObject which would create different objects for the same id.
//
// The config_options parameter controls the process and how errors are
// returned. If ignore_unknown_options=true, unknown values are ignored during
// the configuration. If ignore_unsupported_options=true, unknown instance types
// are ignored. If invoke_prepare_options=true, the resulting instance will be
// initialized (via PrepareOptions)
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to create and initailize the object
// @param result The newly created instance.
template <typename T>
static Status LoadManagedObject(const ConfigOptions& config_options,
                                const std::string& value,
                                std::shared_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, nullptr, value,
                                              &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (value.empty()) {  // No Id and no options.  Clear the object
    *result = nullptr;
    return Status::OK();
  } else {
    return NewManagedObject(config_options, id, opt_map, result);
  }
}

// Creates a new unique pointer customizable instance object based on the
// input parameters using the object registry.
// @see NewSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param id The identifier of the new object being created.  This string
// will be used by the object registry to locate the appropriate object to
// create.
// @param opt_map Optional name-value pairs of properties to set for the newly
// created object
// @param result The newly created and configured instance.
template <typename T>
static Status NewUniqueObject(
    const ConfigOptions& config_options, const std::string& id,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::unique_ptr<T>* result) {
  if (!id.empty()) {
    Status status;
    status = config_options.registry->NewUniqueObject(id, result);
    if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
      status = Status::OK();
    } else if (status.ok()) {
      status = Customizable::ConfigureNewObject(config_options, result->get(),
                                                opt_map);
    }
    return status;
  } else if (opt_map.empty()) {
    // There was no ID and no map (everything empty), so reset/clear the result
    result->reset();
    return Status::OK();
  } else {
    return Status::NotSupported("Cannot reset object ");
  }
}

// Creates a new unique customizable instance object based on the input
// parameters.
// @see LoadSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to create and initailize the object
// @param result The newly created instance.
template <typename T>
static Status LoadUniqueObject(const ConfigOptions& config_options,
                               const std::string& value,
                               std::unique_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else {
    return NewUniqueObject(config_options, id, opt_map, result);
  }
}

// Creates a new static (raw pointer) customizable instance object based on the
// input parameters using the object registry.
// @see NewSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param id The identifier of the new object being created.  This string
// will be used by the object registry to locate the appropriate object to
// create.
// @param opt_map Optional name-value pairs of properties to set for the newly
// created object
// @param result The newly created and configured instance.
template <typename T>
static Status NewStaticObject(
    const ConfigOptions& config_options, const std::string& id,
    const std::unordered_map<std::string, std::string>& opt_map, T** result) {
  if (!id.empty()) {
    Status status;
    status = config_options.registry->NewStaticObject(id, result);
    if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
      status = Status::OK();
    } else if (status.ok()) {
      status =
          Customizable::ConfigureNewObject(config_options, *result, opt_map);
    }
    return status;
  } else if (opt_map.empty()) {
    // There was no ID and no map (everything empty), so reset/clear the result
    *result = nullptr;
    return Status::OK();
  } else {
    return Status::NotSupported("Cannot reset object ");
  }
}

// Creates a new static (raw pointer) customizable instance object based on the
// input parameters.
// @see LoadSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to create and initailize the object
// @param result The newly created instance.
template <typename T>
static Status LoadStaticObject(const ConfigOptions& config_options,
                               const std::string& value, T** result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, *result, value,
                                              &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else {
    return NewStaticObject(config_options, id, opt_map, result);
  }
}
}  // namespace ROCKSDB_NAMESPACE
