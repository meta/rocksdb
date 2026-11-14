//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksjni/jni_multiget_helpers.h"

#include "jni_multiget_helpers.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

bool MultiGetJNIKeys::fromByteArrays(JNIEnv* env, jobjectArray jkeys) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);
    const jsize len_key = env->GetArrayLength(jkey_ba);
    std::unique_ptr<jbyte[]> key = std::make_unique<jbyte[]>(len_key);
    jbyte* raw_key = reinterpret_cast<jbyte*>(key.get());
    key_bufs.push_back(std::move(key));
    env->GetByteArrayRegion(jkey_ba, 0, len_key, raw_key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jkey);
      return false;
    }

    slices_.push_back(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(raw_key), len_key));
    env->DeleteLocalRef(jkey);
  }

  return true;
}

bool MultiGetJNIKeys::fromByteArrays(JNIEnv* env, jobjectArray jkeys,
                                     jintArray jkey_offs, jintArray jkey_lens) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  std::unique_ptr<jint[]> key_offs = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_offs, 0, num_keys, key_offs.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  std::unique_ptr<jint[]> key_lens = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_lens, 0, num_keys, key_lens.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);
    const jint len_key = key_lens[i];
    std::unique_ptr<jbyte[]> key = std::make_unique<jbyte[]>(len_key);
    jbyte* raw_key = reinterpret_cast<jbyte*>(key.get());
    key_bufs.push_back(std::move(key));
    env->GetByteArrayRegion(jkey_ba, key_offs[i], len_key, raw_key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jkey);
      return false;
    }

    slices_.push_back(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(raw_key), len_key));
    env->DeleteLocalRef(jkey);
  }
  return true;
}

bool MultiGetJNIKeys::fromByteBuffers(JNIEnv* env, jobjectArray jkeys,
                                      jintArray jkey_offs,
                                      jintArray jkey_lens) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  std::unique_ptr<jint[]> key_offs = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_offs, 0, num_keys, key_offs.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  std::unique_ptr<jint[]> key_lens = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_lens, 0, num_keys, key_lens.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    ROCKSDB_NAMESPACE::Slice key_slice(key + key_offs[i], key_lens[i]);
    slices_.push_back(key_slice);

    env->DeleteLocalRef(jkey);
  }
  return true;
}

ROCKSDB_NAMESPACE::Slice* MultiGetJNIKeys::data() { return slices_.data(); }

std::vector<ROCKSDB_NAMESPACE::Slice>::size_type MultiGetJNIKeys::size() {
  return slices_.size();
}

template <class TValue>
jobjectArray MultiGetJNIValues::byteArrays(
    JNIEnv* env, std::vector<TValue>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s) {
  jobjectArray jresults = ROCKSDB_NAMESPACE::ByteJni::new2dByteArray(
      env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    OutOfMemoryErrorJni::ThrowNew(env, "Insufficient Memory for results.");
    return nullptr;
  }

  // add to the jresults
  for (std::vector<ROCKSDB_NAMESPACE::Status>::size_type i = 0; i != s.size();
       i++) {
    if (s[i].ok()) {
      TValue* value = &values[i];
      jbyteArray jentry_value =
          ROCKSDB_NAMESPACE::JniUtil::createJavaByteArrayWithSizeCheck(
              env, value->data(), value->size());
      if (jentry_value == nullptr) {
        // exception set
        return nullptr;
      }

      env->SetObjectArrayElement(jresults, static_cast<jsize>(i), jentry_value);
      if (env->ExceptionCheck()) {
        // exception thrown:
        // ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->DeleteLocalRef(jentry_value);
    } else if (s[i].code() == ROCKSDB_NAMESPACE::Status::Code::kCorruption) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s[i]);
      return nullptr;
    } else if (s[i].code() != ROCKSDB_NAMESPACE::Status::Code::kNotFound) {
      // The only way to return an error for a single key is to exception the
      // entire multiGet() Previous behaviour was to return a nullptr value for
      // this case and potentially succesfully return values for other keys; we
      // retain this behaviour. To change it, we need to do the following:
      // ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s[i]);
      // return nullptr;
    }
  }
  return jresults;
}

template jobjectArray MultiGetJNIValues::byteArrays<std::string>(
    JNIEnv* env, std::vector<std::string>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s);

template jobjectArray
MultiGetJNIValues::byteArrays<ROCKSDB_NAMESPACE::PinnableSlice>(
    JNIEnv* env, std::vector<ROCKSDB_NAMESPACE::PinnableSlice>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s);

template <class TValue>
void MultiGetJNIValues::fillByteBuffersAndStatusObjects(
    JNIEnv* env, std::vector<TValue>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s, jobjectArray jvalues,
    jintArray jvalue_sizes, jobjectArray jstatuses) {
  std::vector<jint> value_size;
  for (int i = 0; i < static_cast<jint>(values.size()); i++) {
    auto jstatus = ROCKSDB_NAMESPACE::StatusJni::construct(env, s[i]);
    if (jstatus == nullptr) {
      // exception in context
      return;
    }
    env->SetObjectArrayElement(jstatuses, i, jstatus);

    if (s[i].ok()) {
      jobject jvalue_bytebuf = env->GetObjectArrayElement(jvalues, i);
      if (env->ExceptionCheck()) {
        // ArrayIndexOutOfBoundsException is thrown
        return;
      }
      jlong jvalue_capacity = env->GetDirectBufferCapacity(jvalue_bytebuf);
      if (jvalue_capacity == -1) {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
            env,
            "Invalid value(s) argument (argument is not a valid direct "
            "ByteBuffer)");
        return;
      }
      void* jvalue_address = env->GetDirectBufferAddress(jvalue_bytebuf);
      if (jvalue_address == nullptr) {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
            env,
            "Invalid value(s) argument (argument is not a valid direct "
            "ByteBuffer)");
        return;
      }

      // record num returned, push back that number, which may be bigger then
      // the ByteBuffer supplied. then copy as much as fits in the ByteBuffer.
      static const size_t INTEGER_MAX_VALUE =
          ((static_cast<size_t>(1)) << 31) - 1;
      if (values[i].size() > INTEGER_MAX_VALUE) {
        // Indicate that the result size is bigger than can be represented in a
        // java integer by setting the status to incomplete and the size to -1
        env->SetObjectArrayElement(
            jstatuses, i,
            ROCKSDB_NAMESPACE::StatusJni::construct(
                env, Status::Incomplete("result too large to represent")));
        value_size.push_back(-1);
      } else {
        value_size.push_back(static_cast<jint>(values[i].size()));
      }
      auto copy_bytes =
          std::min(static_cast<jlong>(values[i].size()), jvalue_capacity);
      memcpy(jvalue_address, values[i].data(), copy_bytes);
    } else {
      // bad status for this
      value_size.push_back(0);
    }
  }

  env->SetIntArrayRegion(jvalue_sizes, 0, static_cast<jint>(values.size()),
                         value_size.data());
}

template void MultiGetJNIValues::fillByteBuffersAndStatusObjects<
    ROCKSDB_NAMESPACE::PinnableSlice>(
    JNIEnv* env, std::vector<ROCKSDB_NAMESPACE::PinnableSlice>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s, jobjectArray jvalues,
    jintArray jvalue_sizes, jobjectArray jstatuses);

std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>>
ColumnFamilyJNIHelpers::handlesFromJLongArray(
    JNIEnv* env, jlongArray jcolumn_family_handles) {
  if (jcolumn_family_handles == nullptr) return nullptr;

  const jsize num_cols = env->GetArrayLength(jcolumn_family_handles);
  std::unique_ptr<jlong[]> jcf_handles = std::make_unique<jlong[]>(num_cols);
  env->GetLongArrayRegion(jcolumn_family_handles, 0, num_cols,
                          jcf_handles.get());
  if (env->ExceptionCheck())
    // ArrayIndexOutOfBoundsException
    return nullptr;
  auto cf_handles =
      std::make_unique<std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>>();

  for (jsize i = 0; i < num_cols; i++) {
    auto* cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
        jcf_handles.get()[i]);
    cf_handles->push_back(cf_handle);
  }

  return cf_handles;
}

ROCKSDB_NAMESPACE::ColumnFamilyHandle* ColumnFamilyJNIHelpers::handleFromJLong(
    JNIEnv* env, jlong jcolumn_family_handle) {
  auto cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
      jcolumn_family_handle);
  if (cf_handle == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
  return cf_handle;
};

};  // namespace ROCKSDB_NAMESPACE
