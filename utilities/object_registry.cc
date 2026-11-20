// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/object_registry.h"

#include <cctype>

#include "logging/logging.h"
#include "port/lang.h"
#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
bool MatchesInteger(const std::string& target, size_t start, size_t pos) {
  // If it is numeric, everything up to the match must be a number
  int digits = 0;
  if (target[start] == '-') {
    start++;  // Allow negative numbers
  }
  while (start < pos) {
    if (!isdigit(target[start++])) {
      return false;
    } else {
      digits++;
    }
  }
  return (digits > 0);
}

bool MatchesDecimal(const std::string& target, size_t start, size_t pos) {
  int digits = 0;
  if (target[start] == '-') {
    start++;  // Allow negative numbers
  }
  for (bool point = false; start < pos; start++) {
    if (target[start] == '.') {
      if (point) {
        return false;
      } else {
        point = true;
      }
    } else if (!isdigit(target[start])) {
      return false;
    } else {
      digits++;
    }
  }
  return (digits > 0);
}
}  // namespace

size_t ObjectLibrary::PatternEntry::MatchSeparatorAt(
    size_t start, Quantifier mode, const std::string& target, size_t tlen,
    const std::string& separator) const {
  size_t slen = separator.size();
  // See if there is enough space.  If so, find the separator
  if (tlen < start + slen) {
    return std::string::npos;  // not enough space left
  } else if (mode == kMatchExact) {
    // Exact mode means the next thing we are looking for is the separator
    if (target.compare(start, slen, separator) != 0) {
      return std::string::npos;
    } else {
      return start + slen;  // Found the separator, return where we found it
    }
  } else {
    auto pos = start + 1;
    if (!separator.empty()) {
      pos = target.find(separator, pos);
    }
    if (pos == std::string::npos) {
      return pos;
    } else if (mode == kMatchInteger) {
      if (!MatchesInteger(target, start, pos)) {
        return std::string::npos;
      }
    } else if (mode == kMatchDecimal) {
      if (!MatchesDecimal(target, start, pos)) {
        return std::string::npos;
      }
    }
    return pos + slen;
  }
}

bool ObjectLibrary::PatternEntry::MatchesTarget(const std::string& name,
                                                size_t nlen,
                                                const std::string& target,
                                                size_t tlen) const {
  if (separators_.empty()) {
    assert(optional_);  // If there are no separators, it must be only a name
    return nlen == tlen && name == target;
  } else if (nlen == tlen) {  // The lengths are the same
    return optional_ && name == target;
  } else if (tlen < nlen + slength_) {
    // The target is not long enough
    return false;
  } else if (target.compare(0, nlen, name) != 0) {
    return false;  // Target does not start with name
  } else {
    // Loop through all of the separators one at a time matching them.
    // Note that we first match the separator and then its quantifiers.
    // Since we expect the separator first, we start with an exact match
    // Subsequent matches will use the quantifier of the previous separator
    size_t start = nlen;
    auto mode = kMatchExact;
    for (size_t idx = 0; idx < separators_.size(); ++idx) {
      const auto& separator = separators_[idx];
      start = MatchSeparatorAt(start, mode, target, tlen, separator.first);
      if (start == std::string::npos) {
        return false;
      } else {
        mode = separator.second;
      }
    }
    // We have matched all of the separators.  Now check that what is left
    // unmatched in the target is acceptable.
    if (mode == kMatchExact) {
      return (start == tlen);
    } else if (start > tlen || (start == tlen && mode != kMatchZeroOrMore)) {
      return false;
    } else if (mode == kMatchInteger) {
      return MatchesInteger(target, start, tlen);
    } else if (mode == kMatchDecimal) {
      return MatchesDecimal(target, start, tlen);
    }
  }
  return true;
}

bool ObjectLibrary::PatternEntry::Matches(const std::string& target) const {
  auto tlen = target.size();
  if (MatchesTarget(name_, nlength_, target, tlen)) {
    return true;
  } else if (!names_.empty()) {
    for (const auto& alt : names_) {
      if (MatchesTarget(alt, alt.size(), target, tlen)) {
        return true;
      }
    }
  }
  return false;
}

size_t ObjectLibrary::GetFactoryCount(size_t* types) const {
  std::unique_lock<std::mutex> lock(mu_);
  *types = factories_.size();
  size_t factories = 0;
  for (const auto& e : factories_) {
    factories += e.second.size();
  }
  return factories;
}

size_t ObjectLibrary::GetFactoryCount(const std::string& type) const {
  std::unique_lock<std::mutex> lock(mu_);
  auto iter = factories_.find(type);
  if (iter != factories_.end()) {
    return iter->second.size();
  } else {
    return 0;
  }
}

void ObjectLibrary::GetFactoryNames(const std::string& type,
                                    std::vector<std::string>* names) const {
  assert(names);
  std::unique_lock<std::mutex> lock(mu_);
  auto iter = factories_.find(type);
  if (iter != factories_.end()) {
    for (const auto& f : iter->second) {
      names->emplace_back(f->Name());
    }
  }
}

void ObjectLibrary::GetFactoryTypes(
    std::unordered_set<std::string>* types) const {
  assert(types);
  std::unique_lock<std::mutex> lock(mu_);
  for (const auto& iter : factories_) {
    types->insert(iter.first);
  }
}

void ObjectLibrary::Dump(Logger* logger) const {
  std::unique_lock<std::mutex> lock(mu_);
  if (logger != nullptr && !factories_.empty()) {
    ROCKS_LOG_HEADER(logger, "    Registered Library: %s\n", id_.c_str());
    for (const auto& iter : factories_) {
      ROCKS_LOG_HEADER(logger, "    Registered factories for type[%s] ",
                       iter.first.c_str());
      bool printed_one = false;
      for (const auto& e : iter.second) {
        ROCKS_LOG_HEADER(logger, "%c %s", (printed_one) ? ',' : ':', e->Name());
        printed_one = true;
      }
    }
  }
}

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary>& ObjectLibrary::Default() {
  // Use avoid destruction here so the default ObjectLibrary will not be
  // statically destroyed and long-lived.
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<ObjectLibrary>, instance)
  (std::make_shared<ObjectLibrary>("default"));
  return instance;
}

ObjectRegistry::ObjectRegistry(const std::shared_ptr<ObjectLibrary>& library) {
  libraries_.push_back(library);
  for (const auto& b : builtins_) {
    RegisterPlugin(b.first, b.second);
  }
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::Default() {
  // Use avoid destruction here so the default ObjectRegistry will not be
  // statically destroyed and long-lived.
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<ObjectRegistry>, instance)
  (std::make_shared<ObjectRegistry>(ObjectLibrary::Default()));
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  return std::make_shared<ObjectRegistry>(Default());
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance(
    const std::shared_ptr<ObjectRegistry>& parent) {
  return std::make_shared<ObjectRegistry>(parent);
}

Status ObjectRegistry::SetManagedObject(
    const std::string& type, const std::string& id,
    const std::shared_ptr<Customizable>& object) {
  std::string object_key = ToManagedObjectKey(type, id);
  std::shared_ptr<Customizable> curr;
  if (parent_ != nullptr) {
    curr = parent_->GetManagedObject(type, id);
  }
  if (curr == nullptr) {
    // We did not find the object in any parent.  Update in the current
    std::unique_lock<std::mutex> lock(objects_mutex_);
    auto iter = managed_objects_.find(object_key);
    if (iter != managed_objects_.end()) {  // The object exists
      curr = iter->second.lock();
      if (curr != nullptr && curr != object) {
        return Status::InvalidArgument("Object already exists: ", object_key);
      } else {
        iter->second = object;
      }
    } else {
      // The object does not exist.  Add it
      managed_objects_[object_key] = object;
    }
  } else if (curr != object) {
    return Status::InvalidArgument("Object already exists: ", object_key);
  }
  return Status::OK();
}

std::shared_ptr<Customizable> ObjectRegistry::GetManagedObject(
    const std::string& type, const std::string& id) const {
  {
    std::unique_lock<std::mutex> lock(objects_mutex_);
    auto iter = managed_objects_.find(ToManagedObjectKey(type, id));
    if (iter != managed_objects_.end()) {
      return iter->second.lock();
    }
  }
  if (parent_ != nullptr) {
    return parent_->GetManagedObject(type, id);
  } else {
    return nullptr;
  }
}

Status ObjectRegistry::ListManagedObjects(
    const std::string& type, const std::string& name,
    std::vector<std::shared_ptr<Customizable>>* results) const {
  {
    std::string key = ToManagedObjectKey(type, name);
    std::unique_lock<std::mutex> lock(objects_mutex_);
    for (auto iter = managed_objects_.lower_bound(key);
         iter != managed_objects_.end() && StartsWith(iter->first, key);
         ++iter) {
      auto shared = iter->second.lock();
      if (shared != nullptr) {
        if (name.empty() || shared->IsInstanceOf(name)) {
          results->emplace_back(shared);
        }
      }
    }
  }
  if (parent_ != nullptr) {
    return parent_->ListManagedObjects(type, name, results);
  } else {
    return Status::OK();
  }
}

// Returns the number of registered types for this registry.
// If specified (not-null), types is updated to include the names of the
// registered types.
size_t ObjectRegistry::GetFactoryCount(const std::string& type) const {
  size_t count = 0;
  if (parent_ != nullptr) {
    count = parent_->GetFactoryCount(type);
  }
  std::unique_lock<std::mutex> lock(library_mutex_);
  for (const auto& library : libraries_) {
    count += library->GetFactoryCount(type);
  }
  return count;
}

void ObjectRegistry::GetFactoryNames(const std::string& type,
                                     std::vector<std::string>* names) const {
  assert(names);
  names->clear();
  if (parent_ != nullptr) {
    parent_->GetFactoryNames(type, names);
  }
  std::unique_lock<std::mutex> lock(library_mutex_);
  for (const auto& library : libraries_) {
    library->GetFactoryNames(type, names);
  }
}

void ObjectRegistry::GetFactoryTypes(
    std::unordered_set<std::string>* types) const {
  assert(types);
  if (parent_ != nullptr) {
    parent_->GetFactoryTypes(types);
  }
  std::unique_lock<std::mutex> lock(library_mutex_);
  for (const auto& library : libraries_) {
    library->GetFactoryTypes(types);
  }
}

void ObjectRegistry::Dump(Logger* logger) const {
  if (logger != nullptr) {
    std::unique_lock<std::mutex> lock(library_mutex_);
    if (!plugins_.empty()) {
      ROCKS_LOG_HEADER(logger, "    Registered Plugins:");
      bool printed_one = false;
      for (const auto& plugin : plugins_) {
        ROCKS_LOG_HEADER(logger, "%s%s", (printed_one) ? ", " : " ",
                         plugin.c_str());
        printed_one = true;
      }
      ROCKS_LOG_HEADER(logger, "\n");
    }
    for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
      iter->get()->Dump(logger);
    }
  }
  if (parent_ != nullptr) {
    parent_->Dump(logger);
  }
}

int ObjectRegistry::RegisterPlugin(const std::string& name,
                                   const RegistrarFunc& func) {
  if (!name.empty() && func != nullptr) {
    plugins_.push_back(name);
    return AddLibrary(name)->Register(func, name);
  } else {
    return -1;
  }
}

}  // namespace ROCKSDB_NAMESPACE
