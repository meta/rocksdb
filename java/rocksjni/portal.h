// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <jni.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksjni/compaction_filter_factory_jnicallback.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/event_listener_jnicallback.h"
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/table_filter_jnicallback.h"
#include "rocksjni/trace_writer_jnicallback.h"
#include "rocksjni/transaction_notifier_jnicallback.h"
#include "rocksjni/wal_filter_jnicallback.h"
#include "rocksjni/writebatchhandlerjnicallback.h"

// Remove macro on windows
#ifdef DELETE
#undef DELETE
#endif

namespace ROCKSDB_NAMESPACE {

class JavaClass {
 public:
  /**
   * Gets and initializes a Java Class
   *
   * @param env A pointer to the Java environment
   * @param jclazz_name The fully qualified JNI name of the Java Class
   *     e.g. "java/lang/String"
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env, const char* jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }
};

// Native class template
template <class PTR, class DERIVED>
class RocksDBNativeClass : public JavaClass {};

// Native class template for sub-classes of RocksMutableObject
template <class PTR, class DERIVED>
class NativeRocksMutableObject : public RocksDBNativeClass<PTR, DERIVED> {
 public:
  /**
   * Gets the Java Method ID for the
   * RocksMutableObject#setNativeHandle(long, boolean) method
   *
   * @param env A pointer to the Java environment
   * @return The Java Method ID or nullptr the RocksMutableObject class cannot
   *     be accessed, or if one of the NoSuchMethodError,
   *     ExceptionInInitializerError or OutOfMemoryError exceptions is thrown
   */
  static jmethodID getSetNativeHandleMethod(JNIEnv* env) {
    static jclass jclazz = DERIVED::getJClass(env);
    if (jclazz == nullptr) {
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "setNativeHandle", "(JZ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Sets the C++ object pointer handle in the Java object
   *
   * @param env A pointer to the Java environment
   * @param jobj The Java object on which to set the pointer handle
   * @param ptr The C++ object pointer
   * @param java_owns_handle JNI_TRUE if ownership of the C++ object is
   *     managed by the Java object
   *
   * @return true if a Java exception is pending, false otherwise
   */
  static bool setHandle(JNIEnv* env, jobject jobj, PTR ptr,
                        jboolean java_owns_handle) {
    assert(jobj != nullptr);
    static jmethodID mid = getSetNativeHandleMethod(env);
    if (mid == nullptr) {
      return true;  // signal exception
    }

    env->CallVoidMethod(jobj, mid, GET_CPLUSPLUS_POINTER(ptr),
                        java_owns_handle);
    if (env->ExceptionCheck()) {
      return true;  // signal exception
    }

    return false;
  }
};

// Java Exception template
template <class DERIVED>
class JavaException : public JavaClass {
 public:
  /**
   * Create and throw a java exception with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    jclass jclazz = DERIVED::getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "JavaException::ThrowNew - Error: unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    const jint rs = env->ThrowNew(jclazz, msg.c_str());
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return true;
  }
};

// The portal class for java.lang.IllegalArgumentException
class IllegalArgumentExceptionJni
    : public JavaException<IllegalArgumentExceptionJni> {
 public:
  /**
   * Get the Java Class java.lang.IllegalArgumentException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "java/lang/IllegalArgumentException");
  }

  /**
   * Create and throw a Java IllegalArgumentException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the IllegalArgumentException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "IllegalArgumentExceptionJni::ThrowNew/class - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return JavaException::ThrowNew(env, s.ToString());
  }

  /**
   * Create and throw a Java IllegalArgumentException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }
};

// The portal class for java.lang.IllegalArgumentException
class OutOfMemoryErrorJni : public JavaException<OutOfMemoryErrorJni> {
 public:
  /**
   * Get the Java Class java.lang.OutOfMemoryError
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "java/lang/OutOfMemoryError");
  }

  /**
   * Create and throw a Java OutOfMemoryError with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /**
   * Create and throw a Java OutOfMemoryError with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the OutOfMemoryError class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "OutOfMemoryErrorJni::ThrowNew/class - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return JavaException::ThrowNew(env, s.ToString());
  }
};

// The portal class for org.rocksdb.Status.Code
class CodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.Code
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$Code");
  }

  /**
   * Get the Java Method: Status.Code#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.Status.SubCode
class SubCodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.SubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$SubCode");
  }

  /**
   * Get the Java Method: Status.SubCode#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }

  static ROCKSDB_NAMESPACE::Status::SubCode toCppSubCode(
      const jbyte jsub_code) {
    switch (jsub_code) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
      case 0x1:
        return ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout;
      case 0x2:
        return ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout;
      case 0x3:
        return ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit;
      case 0x4:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace;
      case 0x5:
        return ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock;
      case 0x6:
        return ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile;
      case 0x7:
        return ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit;

      case 0x7F:
      default:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
    }
  }
};

// The portal class for org.rocksdb.Status
class StatusJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::Status*, StatusJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Status
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Status");
  }

  /**
   * Get the Java Method: Status#getCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getCode", "()Lorg/rocksdb/Status$Code;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getSubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getSubCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getSubCode",
                                            "()Lorg/rocksdb/Status$SubCode;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getState
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getStateMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getState", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.Status object with the same properties as
   * the provided C++ ROCKSDB_NAMESPACE::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The ROCKSDB_NAMESPACE::Status object
   *
   * @return A reference to a Java org.rocksdb.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    // convert the Status state for Java
    jstring jstate = nullptr;
    if (status.getState() != nullptr) {
      const char* const state = status.getState();
      jstate = env->NewStringUTF(state);
      if (env->ExceptionCheck()) {
        if (jstate != nullptr) {
          env->DeleteLocalRef(jstate);
        }
        return nullptr;
      }
    }

    jobject jstatus =
        env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
                       toJavaStatusSubCode(status.subcode()), jstate);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jstate != nullptr) {
        env->DeleteLocalRef(jstate);
      }
      return nullptr;
    }

    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }

    return jstatus;
  }

  static jobject construct(JNIEnv* env, const Status* status) {
    return construct(env, *status);
  }

  // Returns the equivalent org.rocksdb.Status.Code for the provided
  // C++ ROCKSDB_NAMESPACE::Status::Code enum
  static jbyte toJavaStatusCode(const ROCKSDB_NAMESPACE::Status::Code& code) {
    switch (code) {
      case ROCKSDB_NAMESPACE::Status::Code::kOk:
        return 0x0;
      case ROCKSDB_NAMESPACE::Status::Code::kNotFound:
        return 0x1;
      case ROCKSDB_NAMESPACE::Status::Code::kCorruption:
        return 0x2;
      case ROCKSDB_NAMESPACE::Status::Code::kNotSupported:
        return 0x3;
      case ROCKSDB_NAMESPACE::Status::Code::kInvalidArgument:
        return 0x4;
      case ROCKSDB_NAMESPACE::Status::Code::kIOError:
        return 0x5;
      case ROCKSDB_NAMESPACE::Status::Code::kMergeInProgress:
        return 0x6;
      case ROCKSDB_NAMESPACE::Status::Code::kIncomplete:
        return 0x7;
      case ROCKSDB_NAMESPACE::Status::Code::kShutdownInProgress:
        return 0x8;
      case ROCKSDB_NAMESPACE::Status::Code::kTimedOut:
        return 0x9;
      case ROCKSDB_NAMESPACE::Status::Code::kAborted:
        return 0xA;
      case ROCKSDB_NAMESPACE::Status::Code::kBusy:
        return 0xB;
      case ROCKSDB_NAMESPACE::Status::Code::kExpired:
        return 0xC;
      case ROCKSDB_NAMESPACE::Status::Code::kTryAgain:
        return 0xD;
      case ROCKSDB_NAMESPACE::Status::Code::kColumnFamilyDropped:
        return 0xE;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent org.rocksdb.Status.SubCode for the provided
  // C++ ROCKSDB_NAMESPACE::Status::SubCode enum
  static jbyte toJavaStatusSubCode(
      const ROCKSDB_NAMESPACE::Status::SubCode& subCode) {
    switch (subCode) {
      case ROCKSDB_NAMESPACE::Status::SubCode::kNone:
        return 0x0;
      case ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout:
        return 0x1;
      case ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout:
        return 0x2;
      case ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit:
        return 0x3;
      case ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace:
        return 0x4;
      case ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock:
        return 0x5;
      case ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile:
        return 0x6;
      case ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit:
        return 0x7;
      default:
        return 0x7F;  // undefined
    }
  }

  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      const jbyte jcode_value, const jbyte jsub_code_value) {
    std::unique_ptr<ROCKSDB_NAMESPACE::Status> status;
    switch (jcode_value) {
      case 0x0:
        // Ok
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::OK()));
        break;
      case 0x1:
        // NotFound
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::NotFound(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x2:
        // Corruption
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Corruption(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x3:
        // NotSupported
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::NotSupported(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x4:
        // InvalidArgument
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::InvalidArgument(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x5:
        // IOError
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::IOError(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x6:
        // MergeInProgress
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::MergeInProgress(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x7:
        // Incomplete
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Incomplete(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x8:
        // ShutdownInProgress
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::ShutdownInProgress(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x9:
        // TimedOut
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::TimedOut(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xA:
        // Aborted
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Aborted(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xB:
        // Busy
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Busy(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xC:
        // Expired
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Expired(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xD:
        // TryAgain
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::TryAgain(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xE:
        // ColumnFamilyDropped
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::ColumnFamilyDropped(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x7F:
      default:
        return nullptr;
    }
    return status;
  }

  // Returns the equivalent ROCKSDB_NAMESPACE::Status for the Java
  // org.rocksdb.Status
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, const jobject jstatus) {
    jmethodID mid_code = getCodeMethod(env);
    if (mid_code == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jcode = env->CallObjectMethod(jstatus, mid_code);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    jmethodID mid_code_value = ROCKSDB_NAMESPACE::CodeJni::getValueMethod(env);
    if (mid_code_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    jbyte jcode_value = env->CallByteMethod(jcode, mid_code_value);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jmethodID mid_subCode = getSubCodeMethod(env);
    if (mid_subCode == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jsubCode = env->CallObjectMethod(jstatus, mid_subCode);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jbyte jsub_code_value = 0x0;  // None
    if (jsubCode != nullptr) {
      jmethodID mid_subCode_value =
          ROCKSDB_NAMESPACE::SubCodeJni::getValueMethod(env);
      if (mid_subCode_value == nullptr) {
        // exception occurred
        return nullptr;
      }
      jsub_code_value = env->CallByteMethod(jsubCode, mid_subCode_value);
      if (env->ExceptionCheck()) {
        // exception occurred
        if (jcode != nullptr) {
          env->DeleteLocalRef(jcode);
        }
        return nullptr;
      }
    }

    jmethodID mid_state = getStateMethod(env);
    if (mid_state == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jstate = env->CallObjectMethod(jstatus, mid_state);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jsubCode != nullptr) {
        env->DeleteLocalRef(jsubCode);
      }
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
        toCppStatus(jcode_value, jsub_code_value);

    // delete all local refs
    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }
    if (jsubCode != nullptr) {
      env->DeleteLocalRef(jsubCode);
    }
    if (jcode != nullptr) {
      env->DeleteLocalRef(jcode);
    }

    return status;
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni : public JavaException<RocksDBExceptionJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDBException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "org/rocksdb/RocksDBException");
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s->ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, std::unique_ptr<Status>& s) {
    return ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, *(s.get()));
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception =
        reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   * and status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    jstring jmsg = env->NewStringUTF(msg.c_str());
    if (jmsg == nullptr) {
      // exception thrown: OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/msg - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(
        env->NewObject(jclazz, mid, jmsg, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (jmsg != nullptr) {
      env->DeleteLocalRef(jmsg);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Get the Java Method: RocksDBException#getStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getStatusMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getStatus", "()Lorg/rocksdb/Status;");
    assert(mid != nullptr);
    return mid;
  }

  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, jthrowable jrocksdb_exception) {
    if (!env->IsInstanceOf(jrocksdb_exception, getJClass(env))) {
      // not an instance of RocksDBException
      return nullptr;
    }

    // get the java status object
    jmethodID mid = getStatusMethod(env);
    if (mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jobject jstatus = env->CallObjectMethod(jrocksdb_exception, mid);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    if (jstatus == nullptr) {
      return nullptr;  // no status available
    }

    return ROCKSDB_NAMESPACE::StatusJni::toCppStatus(env, jstatus);
  }
};

// The portal class for java.util.List
class ListJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.List
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/List");
  }

  /**
   * Get the Java Class java.util.ArrayList
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/ArrayList");
  }

  /**
   * Get the Java Class java.util.Iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getIteratorClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Iterator");
  }

  /**
   * Get the Java Method: List#iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getIteratorMethod(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#hasNext
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getHasNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if (jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jiterator_clazz, "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#next
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if (jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jiterator_clazz, "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ArrayList constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env) {
    jclass jarray_list_clazz = getArrayListClass(env);
    if (jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID mid =
        env->GetMethodID(jarray_list_clazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: List#add
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for java.lang.Byte
class ByteJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Byte
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Byte");
  }

  /**
   * Get the Java Class byte[]
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "[B");
  }

  /**
   * Creates a new 2-dimensional Java Byte Array byte[][]
   *
   * @param env A pointer to the Java environment
   * @param len The size of the first dimension
   *
   * @return A reference to the Java byte[][] or nullptr if an exception occurs
   */
  static jobjectArray new2dByteArray(JNIEnv* env, const jsize len) {
    jclass clazz = getArrayJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    return env->NewObjectArray(len, clazz, nullptr);
  }

  /**
   * Get the Java Method: Byte#byteValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getByteValueMethod(JNIEnv* env) {
    jclass clazz = getJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(clazz, "byteValue", "()B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Calls the Java Method: Byte#valueOf, returning a constructed Byte jobject
   *
   * @param env A pointer to the Java environment
   *
   * @return A constructing Byte object or nullptr if the class or method id
   * could not be retrieved, or an exception occurred
   */
  static jobject valueOf(JNIEnv* env, jbyte jprimitive_byte) {
    jclass clazz = getJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetStaticMethodID(clazz, "valueOf", "(B)Ljava/lang/Byte;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jbyte_obj =
        env->CallStaticObjectMethod(clazz, mid, jprimitive_byte);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jbyte_obj;
  }
};

// The portal class for java.nio.ByteBuffer
class ByteBufferJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.nio.ByteBuffer
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/nio/ByteBuffer");
  }

  /**
   * Get the Java Method: ByteBuffer#allocate
   *
   * @param env A pointer to the Java environment
   * @param jbytebuffer_clazz if you have a reference to a ByteBuffer class, or
   * nullptr
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getAllocateMethodId(JNIEnv* env,
                                       jclass jbytebuffer_clazz = nullptr) {
    const jclass jclazz =
        jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "allocate", "(I)Ljava/nio/ByteBuffer;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ByteBuffer#array
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getArrayMethodId(JNIEnv* env,
                                    jclass jbytebuffer_clazz = nullptr) {
    const jclass jclazz =
        jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "array", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  static jobject construct(JNIEnv* env, const bool direct,
                           const size_t capacity,
                           jclass jbytebuffer_clazz = nullptr) {
    return constructWith(env, direct, nullptr, capacity, jbytebuffer_clazz);
  }

  static jobject constructWith(JNIEnv* env, const bool direct, const char* buf,
                               const size_t capacity,
                               jclass jbytebuffer_clazz = nullptr) {
    if (direct) {
      bool allocated = false;
      if (buf == nullptr) {
        buf = new char[capacity];
        allocated = true;
      }
      jobject jbuf = env->NewDirectByteBuffer(const_cast<char*>(buf),
                                              static_cast<jlong>(capacity));
      if (jbuf == nullptr) {
        // exception occurred
        if (allocated) {
          delete[] static_cast<const char*>(buf);
        }
        return nullptr;
      }
      return jbuf;
    } else {
      const jclass jclazz =
          jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
      if (jclazz == nullptr) {
        // exception occurred accessing class
        return nullptr;
      }
      const jmethodID jmid_allocate =
          getAllocateMethodId(env, jbytebuffer_clazz);
      if (jmid_allocate == nullptr) {
        // exception occurred accessing class, or NoSuchMethodException or
        // OutOfMemoryError
        return nullptr;
      }
      const jobject jbuf = env->CallStaticObjectMethod(
          jclazz, jmid_allocate, static_cast<jint>(capacity));
      if (env->ExceptionCheck()) {
        // exception occurred
        return nullptr;
      }

      // set buffer data?
      if (buf != nullptr) {
        jbyteArray jarray = array(env, jbuf, jbytebuffer_clazz);
        if (jarray == nullptr) {
          // exception occurred
          env->DeleteLocalRef(jbuf);
          return nullptr;
        }

        jboolean is_copy = JNI_FALSE;
        jbyte* ja = reinterpret_cast<jbyte*>(
            env->GetPrimitiveArrayCritical(jarray, &is_copy));
        if (ja == nullptr) {
          // exception occurred
          env->DeleteLocalRef(jarray);
          env->DeleteLocalRef(jbuf);
          return nullptr;
        }

        memcpy(ja, const_cast<char*>(buf), capacity);

        env->ReleasePrimitiveArrayCritical(jarray, ja, 0);

        env->DeleteLocalRef(jarray);
      }

      return jbuf;
    }
  }

  static jbyteArray array(JNIEnv* env, const jobject& jbyte_buffer,
                          jclass jbytebuffer_clazz = nullptr) {
    const jmethodID mid = getArrayMethodId(env, jbytebuffer_clazz);
    if (mid == nullptr) {
      // exception occurred accessing class, or NoSuchMethodException or
      // OutOfMemoryError
      return nullptr;
    }
    const jobject jarray = env->CallObjectMethod(jbyte_buffer, mid);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }
    return static_cast<jbyteArray>(jarray);
  }
};

// The portal class for java.lang.Integer
class IntegerJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Integer
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Integer");
  }

  static jobject valueOf(JNIEnv* env, jint jprimitive_int) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetStaticMethodID(jclazz, "valueOf", "(I)Ljava/lang/Integer;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jinteger_obj =
        env->CallStaticObjectMethod(jclazz, mid, jprimitive_int);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jinteger_obj;
  }
};

// The portal class for java.lang.Long
class LongJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Long
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Long");
  }

  static jobject valueOf(JNIEnv* env, jlong jprimitive_long) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetStaticMethodID(jclazz, "valueOf", "(J)Ljava/lang/Long;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jlong_obj =
        env->CallStaticObjectMethod(jclazz, mid, jprimitive_long);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jlong_obj;
  }
};

// The portal class for java.lang.StringBuilder
class StringBuilderJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.StringBuilder
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/StringBuilder");
  }

  /**
   * Get the Java Method: StringBuilder#append
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Appends a C-style string to a StringBuilder
   *
   * @param env A pointer to the Java environment
   * @param jstring_builder Reference to a java.lang.StringBuilder
   * @param c_str A C-style string to append to the StringBuilder
   *
   * @return A reference to the updated StringBuilder, or a nullptr if
   *     an exception occurs
   */
  static jobject append(JNIEnv* env, jobject jstring_builder,
                        const char* c_str) {
    jmethodID mid = getListAddMethodId(env);
    if (mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jstring new_value_str = env->NewStringUTF(c_str);
    if (new_value_str == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jobject jresult_string_builder =
        env->CallObjectMethod(jstring_builder, mid, new_value_str);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(new_value_str);
      return nullptr;
    }

    return jresult_string_builder;
  }
};

// various utility functions for working with RocksDB and JNI
class JniUtil {
 public:
  /**
   * Detect if jlong overflows size_t
   *
   * @param jvalue the jlong value
   *
   * @return
   */
  inline static Status check_if_jlong_fits_size_t(const jlong& jvalue) {
    Status s = Status::OK();
    if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
      s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
    }
    return s;
  }

  /**
   * Obtains a reference to the JNIEnv from
   * the JVM
   *
   * If the current thread is not attached to the JavaVM
   * then it will be attached so as to retrieve the JNIEnv
   *
   * If a thread is attached, it must later be manually
   * released by calling JavaVM::DetachCurrentThread.
   * This can be handled by always matching calls to this
   * function with calls to {@link JniUtil::releaseJniEnv(JavaVM*, jboolean)}
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (OUT) A pointer to a boolean which
   *     will be set to JNI_TRUE if we had to attach the thread
   *
   * @return A pointer to the JNIEnv or nullptr if a fatal error
   *     occurs and the JNIEnv cannot be retrieved
   */
  static JNIEnv* getJniEnv(JavaVM* jvm, jboolean* attached) {
    assert(jvm != nullptr);

    JNIEnv* env;
    const jint env_rs =
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);

    if (env_rs == JNI_OK) {
      // current thread is already attached, return the JNIEnv
      *attached = JNI_FALSE;
      return env;
    } else if (env_rs == JNI_EDETACHED) {
      // current thread is not attached, attempt to attach
      const jint rs_attach =
          jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
      if (rs_attach == JNI_OK) {
        *attached = JNI_TRUE;
        return env;
      } else {
        // error, could not attach the thread
        std::cerr << "JniUtil::getJniEnv - Fatal: could not attach current "
                     "thread to JVM!"
                  << std::endl;
        return nullptr;
      }
    } else if (env_rs == JNI_EVERSION) {
      // error, JDK does not support JNI_VERSION_1_6+
      std::cerr
          << "JniUtil::getJniEnv - Fatal: JDK does not support JNI_VERSION_1_6"
          << std::endl;
      return nullptr;
    } else {
      std::cerr << "JniUtil::getJniEnv - Fatal: Unknown error: env_rs="
                << env_rs << std::endl;
      return nullptr;
    }
  }

  /**
   * Counterpart to {@link JniUtil::getJniEnv(JavaVM*, jboolean*)}
   *
   * Detachess the current thread from the JVM if it was previously
   * attached
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (IN) JNI_TRUE if we previously had to attach the thread
   *     to the JavaVM to get the JNIEnv
   */
  static void releaseJniEnv(JavaVM* jvm, jboolean& attached) {
    assert(jvm != nullptr);
    if (attached == JNI_TRUE) {
      const jint rs_detach = jvm->DetachCurrentThread();
      assert(rs_detach == JNI_OK);
      if (rs_detach != JNI_OK) {
        std::cerr << "JniUtil::getJniEnv - Warn: Unable to detach current "
                     "thread from JVM!"
                  << std::endl;
      }
    }
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std:string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              jboolean* has_exception) {
    return ROCKSDB_NAMESPACE::JniUtil::copyStrings(
        env, jss, env->GetArrayLength(jss), has_exception);
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param jss_len (IN) The length of the Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std:string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              const jsize jss_len,
                                              jboolean* has_exception) {
    std::vector<std::string> strs;
    strs.reserve(jss_len);
    for (jsize i = 0; i < jss_len; i++) {
      jobject js = env->GetObjectArrayElement(jss, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;
        return strs;
      }

      jstring jstr = static_cast<jstring>(js);
      const char* str = env->GetStringUTFChars(jstr, nullptr);
      if (str == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(js);
        *has_exception = JNI_TRUE;
        return strs;
      }

      strs.push_back(std::string(str));

      env->ReleaseStringUTFChars(jstr, str);
      env->DeleteLocalRef(js);
    }

    *has_exception = JNI_FALSE;
    return strs;
  }

  /**
   * Copies a jstring to a C-style null-terminated byte string
   * and releases the original jstring
   *
   * The jstring is copied as UTF-8
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A pointer to the copied string, or a
   *     nullptr if has_exception == JNI_TRUE
   */
  static std::unique_ptr<char[]> copyString(JNIEnv* env, jstring js,
                                            jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return nullptr;
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return nullptr;
    }

    const jsize utf_len = env->GetStringUTFLength(js);
    std::unique_ptr<char[]> str(
        new char[utf_len +
                 1]);  // Note: + 1 is needed for the c_str null terminator
    std::strcpy(str.get(), utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return str;
  }

  /**
   * Copies a jstring to a std::string
   * and releases the original jstring
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A std:string copy of the jstring, or an
   *     empty std::string if has_exception == JNI_TRUE
   */
  static std::string copyStdString(JNIEnv* env, jstring js,
                                   jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return std::string();
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return std::string();
    }

    std::string name(utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return name;
  }

  /**
   * Copies bytes from a std::string to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[], or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.c_str(), bytes.size());
  }

  /**
   * Given a Java byte[][] which is an array of java.lang.Strings
   * where each String is a byte[], the passed function `string_fn`
   * will be called on each String, the result is the collected by
   * calling the passed function `collector_fn`
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_strings (IN) A Java array of Strings expressed as bytes
   * @param string_fn (IN) A transform function to call for each String
   * @param collector_fn (IN) A collector which is called for the result
   *     of each `string_fn`
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs
   */
  template <typename T>
  static void byteStrings(JNIEnv* env, jobjectArray jbyte_strings,
                          std::function<T(const char*, const size_t)> string_fn,
                          std::function<void(size_t, T)> collector_fn,
                          jboolean* has_exception) {
    const jsize jlen = env->GetArrayLength(jbyte_strings);

    for (jsize i = 0; i < jlen; i++) {
      jobject jbyte_string_obj = env->GetObjectArrayElement(jbyte_strings, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;  // signal error
        return;
      }

      jbyteArray jbyte_string_ary =
          reinterpret_cast<jbyteArray>(jbyte_string_obj);
      T result = byteString(env, jbyte_string_ary, string_fn, has_exception);

      env->DeleteLocalRef(jbyte_string_obj);

      if (*has_exception == JNI_TRUE) {
        // exception thrown: OutOfMemoryError
        return;
      }

      collector_fn(i, result);
    }

    *has_exception = JNI_FALSE;
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    const jsize jbyte_string_len = env->GetArrayLength(jbyte_string_ary);
    return byteString<T>(env, jbyte_string_ary, jbyte_string_len, string_fn,
                         has_exception);
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param jbyte_string_len (IN) The length of the Java String
   *     expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      const jsize jbyte_string_len,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    jbyte* jbyte_string = env->GetByteArrayElements(jbyte_string_ary, nullptr);
    if (jbyte_string == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return T();  // caller must check has_exception
    }

    T result =
        string_fn(reinterpret_cast<char*>(jbyte_string), jbyte_string_len);

    env->ReleaseByteArrayElements(jbyte_string_ary, jbyte_string, JNI_ABORT);

    *has_exception = JNI_FALSE;
    return result;
  }

  /**
   * Converts a std::vector<string> to a Java byte[][] where each Java String
   * is expressed as a Java Byte Array byte[].
   *
   * @param env A pointer to the java environment
   * @param strings A vector of Strings
   *
   * @return A Java array of Strings expressed as bytes,
   *     or nullptr if an exception is thrown
   */
  static jobjectArray stringsBytes(JNIEnv* env,
                                   std::vector<std::string> strings) {
    jclass jcls_ba = ByteJni::getArrayJClass(env);
    if (jcls_ba == nullptr) {
      // exception occurred
      return nullptr;
    }

    const jsize len = static_cast<jsize>(strings.size());

    jobjectArray jbyte_strings = env->NewObjectArray(len, jcls_ba, nullptr);
    if (jbyte_strings == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len; i++) {
      std::string* str = &strings[i];
      const jsize str_len = static_cast<jsize>(str->size());

      jbyteArray jbyte_string_ary = env->NewByteArray(str_len);
      if (jbyte_string_ary == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetByteArrayRegion(
          jbyte_string_ary, 0, str_len,
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(str->c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetObjectArrayElement(jbyte_strings, i, jbyte_string_ary);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        // or ArrayStoreException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->DeleteLocalRef(jbyte_string_ary);
    }

    return jbyte_strings;
  }

  /**
   * Converts a std::vector<std::string> to a Java String[].
   *
   * @param env A pointer to the java environment
   * @param strings A vector of Strings
   *
   * @return A Java array of Strings,
   *     or nullptr if an exception is thrown
   */
  static jobjectArray toJavaStrings(JNIEnv* env,
                                    const std::vector<std::string>* strings) {
    jclass jcls_str = env->FindClass("java/lang/String");
    if (jcls_str == nullptr) {
      // exception occurred
      return nullptr;
    }

    const jsize len = static_cast<jsize>(strings->size());

    jobjectArray jstrings = env->NewObjectArray(len, jcls_str, nullptr);
    if (jstrings == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len; i++) {
      const std::string* str = &((*strings)[i]);
      jstring js = ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, str);
      if (js == nullptr) {
        env->DeleteLocalRef(jstrings);
        return nullptr;
      }

      env->SetObjectArrayElement(jstrings, i, js);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        // or ArrayStoreException
        env->DeleteLocalRef(js);
        env->DeleteLocalRef(jstrings);
        return nullptr;
      }
    }

    return jstrings;
  }

  /**
   * Creates a Java UTF String from a C++ std::string
   *
   * @param env A pointer to the java environment
   * @param string the C++ std::string
   * @param treat_empty_as_null true if empty strings should be treated as null
   *
   * @return the Java UTF string, or nullptr if the provided string
   *     is null (or empty and treat_empty_as_null is set), or if an
   *     exception occurs allocating the Java String.
   */
  static jstring toJavaString(JNIEnv* env, const std::string* string,
                              const bool treat_empty_as_null = false) {
    if (string == nullptr) {
      return nullptr;
    }

    if (treat_empty_as_null && string->empty()) {
      return nullptr;
    }

    return env->NewStringUTF(string->c_str());
  }

  /**
   * Copies bytes to a new jByteArray with the check of java array size
   * limitation.
   *
   * @param bytes pointer to memory to copy to a new jByteArray
   * @param size number of bytes to copy
   *
   * @return the Java byte[], or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java array size limitation to
   * avoid overflow.
   */
  static jbyteArray createJavaByteArrayWithSizeCheck(JNIEnv* env,
                                                     const char* bytes,
                                                     const size_t size) {
    // Limitation for java array size is vm specific
    // In general it cannot exceed Integer.MAX_VALUE (2^31 - 1)
    // Current HotSpot VM limitation for array size is Integer.MAX_VALUE - 5
    // (2^31 - 1 - 5) It means that the next call to env->NewByteArray can still
    // end with OutOfMemoryError("Requested array size exceeds VM limit") coming
    // from VM
    static const size_t MAX_JARRAY_SIZE = (static_cast<size_t>(1)) << 31;
    if (size > MAX_JARRAY_SIZE) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Requested array size exceeds VM limit");
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(size);
    jbyteArray jbytes = env->NewByteArray(jlen);
    if (jbytes == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    env->SetByteArrayRegion(
        jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes)));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jbytes);
      return nullptr;
    }

    return jbytes;
  }

  /**
   * Copies bytes from a ROCKSDB_NAMESPACE::Slice to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[] or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, const Slice& bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.data(), bytes.size());
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be used for RocksDB->Put etc.
   */
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> kv_op(
      std::function<ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Slice,
                                              ROCKSDB_NAMESPACE::Slice)>
          op,
      JNIEnv* env, jbyteArray jkey, jint jkey_len, jbyteArray jvalue,
      jint jvalue_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      if (key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
    ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                         jvalue_len);

    auto status = op(key_slice, value_slice);

    if (value != nullptr) {
      env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
    }
    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
        new ROCKSDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a key
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be used for RocksDB->Delete etc.
   */
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> k_op(
      std::function<ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Slice)> op,
      JNIEnv* env, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    auto status = op(key_slice);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
        new ROCKSDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a key which is a region of an array
   * Used to extract the common code from seek/seekForPrev.
   * Possible that it can be generalised from that.
   *
   * We use GetByteArrayRegion to copy the key region of the whole array into
   * a char[] We suspect this is not much slower than GetByteArrayElements,
   * which probably copies anyway.
   */
  static void k_op_region(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                          JNIEnv* env, jbyteArray jkey, jint jkey_off,
                          jint jkey_len) {
    const std::unique_ptr<char[]> key(new char[jkey_len]);
    if (key == nullptr) {
      jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
      env->ThrowNew(oom_class,
                    "Memory allocation failed in RocksDB JNI function");
      return;
    }
    env->GetByteArrayRegion(jkey, jkey_off, jkey_len,
                            reinterpret_cast<jbyte*>(key.get()));
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key.get()),
                                       jkey_len);
    op(key_slice);
  }

  /*
   * Helper for operations on a value
   * for example WriteBatchWithIndex->GetFromBatch
   */
  static jbyteArray v_op(std::function<ROCKSDB_NAMESPACE::Status(
                             ROCKSDB_NAMESPACE::Slice, std::string*)>
                             op,
                         JNIEnv* env, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    std::string value;
    ROCKSDB_NAMESPACE::Status s = op(key_slice, &value);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    if (s.IsNotFound()) {
      return nullptr;
    }

    if (s.ok()) {
      jbyteArray jret_value =
          env->NewByteArray(static_cast<jsize>(value.size()));
      if (jret_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jret_value, 0, static_cast<jsize>(value.size()),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        if (jret_value != nullptr) {
          env->DeleteLocalRef(jret_value);
        }
        return nullptr;
      }

      return jret_value;
    }

    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  /**
   * Creates a vector<T*> of C++ pointers from
   *     a Java array of C++ pointer addresses.
   *
   * @param env (IN) A pointer to the java environment
   * @param pointers (IN) A Java array of C++ pointer addresses
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs.
   *
   * @return A vector of C++ pointers.
   */
  template <typename T>
  static std::vector<T*> fromJPointers(JNIEnv* env, jlongArray jptrs,
                                       jboolean* has_exception) {
    const jsize jptrs_len = env->GetArrayLength(jptrs);
    std::vector<T*> ptrs;
    jlong* jptr = env->GetLongArrayElements(jptrs, nullptr);
    if (jptr == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return ptrs;
    }
    ptrs.reserve(jptrs_len);
    for (jsize i = 0; i < jptrs_len; i++) {
      ptrs.push_back(reinterpret_cast<T*>(jptr[i]));
    }
    env->ReleaseLongArrayElements(jptrs, jptr, JNI_ABORT);
    return ptrs;
  }

  /**
   * Creates a Java array of C++ pointer addresses
   *     from a vector of C++ pointers.
   *
   * @param env (IN) A pointer to the java environment
   * @param pointers (IN) A vector of C++ pointers
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs
   *
   * @return Java array of C++ pointer addresses.
   */
  template <typename T>
  static jlongArray toJPointers(JNIEnv* env, const std::vector<T*>& pointers,
                                jboolean* has_exception) {
    const jsize len = static_cast<jsize>(pointers.size());
    std::unique_ptr<jlong[]> results(new jlong[len]);
    std::transform(
        pointers.begin(), pointers.end(), results.get(),
        [](T* pointer) -> jlong { return GET_CPLUSPLUS_POINTER(pointer); });

    jlongArray jpointers = env->NewLongArray(len);
    if (jpointers == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return nullptr;
    }

    env->SetLongArrayRegion(jpointers, 0, len, results.get());
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      *has_exception = JNI_TRUE;
      env->DeleteLocalRef(jpointers);
      return nullptr;
    }

    *has_exception = JNI_FALSE;

    return jpointers;
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
   * from `op` and used for RocksDB->Put etc.
   */
  static void kv_op_direct(
      std::function<void(ROCKSDB_NAMESPACE::Slice&, ROCKSDB_NAMESPACE::Slice&)>
          op,
      JNIEnv* env, jobject jkey, jint jkey_off, jint jkey_len, jobject jval,
      jint jval_off, jint jval_len) {
    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    if (key == nullptr ||
        env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env,
                                                       "Invalid key argument");
      return;
    }

    char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
    if (value == nullptr ||
        env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Invalid value argument");
      return;
    }

    key += jkey_off;
    value += jval_off;

    ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);
    ROCKSDB_NAMESPACE::Slice value_slice(value, jval_len);

    op(key_slice, value_slice);
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
   * from `op` and used for RocksDB->Delete etc.
   */
  static void k_op_direct(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                          JNIEnv* env, jobject jkey, jint jkey_off,
                          jint jkey_len) {
    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    if (key == nullptr ||
        env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env,
                                                       "Invalid key argument");
      return;
    }

    key += jkey_off;

    ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);

    return op(key_slice);
  }

  template <class T>
  static jint copyToDirect(JNIEnv* env, T& source, jobject jtarget,
                           jint jtarget_off, jint jtarget_len) {
    char* target =
        reinterpret_cast<char*>(env->GetDirectBufferAddress(jtarget));
    if (target == nullptr ||
        env->GetDirectBufferCapacity(jtarget) < (jtarget_off + jtarget_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Invalid target argument");
      return 0;
    }

    target += jtarget_off;

    const jint cvalue_len = static_cast<jint>(source.size());
    const jint length = std::min(jtarget_len, cvalue_len);

    memcpy(target, source.data(), length);

    return cvalue_len;
  }
};

class MapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.Map
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Map");
  }

  /**
   * Get the Java Method: Map#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMapPutMethodId(JNIEnv* env) {
    jclass jlist_clazz = getJClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jlist_clazz, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }
};

class HashMapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.HashMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/HashMap");
  }

  /**
   * Create a new Java java.util.HashMap object.
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java java.util.HashMap object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const uint32_t initial_capacity = 16) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(I)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jhash_map =
        env->NewObject(jclazz, mid, static_cast<jint>(initial_capacity));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * A function which maps a std::pair<K,V> to a std::pair<JK, JV>
   *
   * @return Either a pointer to a std::pair<jobject, jobject>, or nullptr
   *     if an error occurs during the mapping
   */
  template <typename K, typename V, typename JK, typename JV>
  using FnMapKV =
      std::function<std::unique_ptr<std::pair<JK, JV>>(const std::pair<K, V>&)>;

  // template <class I, typename K, typename V, typename K1, typename V1,
  // typename std::enable_if<std::is_same<typename
  // std::iterator_traits<I>::value_type, std::pair<const K,V>>::value,
  // int32_t>::type = 0> static void putAll(JNIEnv* env, const jobject
  // jhash_map, I iterator, const FnMapKV<const K,V,K1,V1> &fn_map_kv) {
  /**
   * Returns true if it succeeds, false if an error occurs
   */
  template <class iterator_type, typename K, typename V>
  static bool putAll(JNIEnv* env, const jobject jhash_map,
                     iterator_type iterator, iterator_type end,
                     const FnMapKV<K, V, jobject, jobject>& fn_map_kv) {
    const jmethodID jmid_put =
        ROCKSDB_NAMESPACE::MapJni::getMapPutMethodId(env);
    if (jmid_put == nullptr) {
      return false;
    }

    for (auto it = iterator; it != end; ++it) {
      const std::unique_ptr<std::pair<jobject, jobject>> result =
          fn_map_kv(*it);
      if (result == nullptr) {
        // an error occurred during fn_map_kv
        return false;
      }
      env->CallObjectMethod(jhash_map, jmid_put, result->first, result->second);
      if (env->ExceptionCheck()) {
        // exception occurred
        env->DeleteLocalRef(result->second);
        env->DeleteLocalRef(result->first);
        return false;
      }

      // release local references
      env->DeleteLocalRef(result->second);
      env->DeleteLocalRef(result->first);
    }

    return true;
  }

  /**
   * Creates a java.util.Map<String, String> from a std::map<std::string,
   * std::string>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, std::string>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const std::string, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const std::string>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jstring jvalue = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.second), true);
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(
                      static_cast<jobject>(jkey),
                      static_cast<jobject>(jvalue)));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string,
   * uint32_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, uint32_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const uint32_t, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const uint32_t>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jobject jvalue = ROCKSDB_NAMESPACE::IntegerJni::valueOf(
                  env, static_cast<jint>(kv.second));
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                                  jvalue));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string,
   * uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const uint64_t, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const uint64_t>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jobject jvalue = ROCKSDB_NAMESPACE::LongJni::valueOf(
                  env, static_cast<jlong>(kv.second));
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                                  jvalue));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<uint32_t, uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<uint32_t, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<const uint32_t, const uint64_t,
                                                 jobject, jobject>
        fn_map_kv = [env](const std::pair<const uint32_t, const uint64_t>& kv) {
          jobject jkey = ROCKSDB_NAMESPACE::IntegerJni::valueOf(
              env, static_cast<jint>(kv.first));
          if (env->ExceptionCheck()) {
            // an error occurred
            return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
          }

          jobject jvalue = ROCKSDB_NAMESPACE::LongJni::valueOf(
              env, static_cast<jlong>(kv.second));
          if (env->ExceptionCheck()) {
            // an error occurred
            env->DeleteLocalRef(jkey);
            return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
          }

          return std::unique_ptr<std::pair<jobject, jobject>>(
              new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                              jvalue));
        };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }
};

// The portal class for org.rocksdb.RocksDB
class RocksDBJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::DB*, RocksDBJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksDB");
  }
};

// The portal class for org.rocksdb.Options
class OptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::Options*, OptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Options
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Options");
  }
};

// The portal class for org.rocksdb.DBOptions
class DBOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::DBOptions*, DBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DBOptions");
  }
};

// The portal class for org.rocksdb.ColumnFamilyOptions
class ColumnFamilyOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::ColumnFamilyOptions*,
                                ColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/ColumnFamilyOptions");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyOptions object with the same
   * properties as the provided C++ ROCKSDB_NAMESPACE::ColumnFamilyOptions
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to ROCKSDB_NAMESPACE::ColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const ColumnFamilyOptions* cfoptions) {
    auto* cfo = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(*cfoptions);
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, GET_CPLUSPLUS_POINTER(cfo));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};

// The portal class for org.rocksdb.WriteOptions
class WriteOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::WriteOptions*,
                                WriteOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteOptions");
  }
};

// The portal class for org.rocksdb.ReadOptions
class ReadOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::ReadOptions*,
                                ReadOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ReadOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ReadOptions");
  }
};

// The portal class for org.rocksdb.WriteBatch
class WriteBatchJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::WriteBatch*, WriteBatchJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch");
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch object
   *
   * @param env A pointer to the Java environment
   * @param wb A pointer to ROCKSDB_NAMESPACE::WriteBatch object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const WriteBatch* wb) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jwb = env->NewObject(jclazz, mid, GET_CPLUSPLUS_POINTER(wb));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jwb;
  }
};

// The portal class for org.rocksdb.WriteBatch.Handler
class WriteBatchHandlerJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::WriteBatchHandlerJniCallback*,
          WriteBatchHandlerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.Handler
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch$Handler");
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getPutCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getPutMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMergeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMergeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getSingleDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getSingleDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getDeleteRangeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getDeleteRangeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#logData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#putBlobIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getPutBlobIndexCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "putBlobIndex", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markBeginPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkBeginPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markBeginPrepare", "()V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markEndPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkEndPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markEndPrepare", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markNoop
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkNoopMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markNoop", "(Z)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markRollback
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkRollbackMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markRollback", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markCommit
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkCommitMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markCommit", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markCommitWithTimestamp
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMarkCommitWithTimestampMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "markCommitWithTimestamp", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#shouldContinue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getContinueMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "shouldContinue", "()Z");
    assert(mid != nullptr);
    return mid;
  }
};

class WriteBatchSavePointJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.SavePoint
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WriteBatch$SavePoint");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJJ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch.SavePoint object
   *
   * @param env A pointer to the Java environment
   * @param savePoint A pointer to ROCKSDB_NAMESPACE::WriteBatch::SavePoint
   * object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch.SavePoint object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const SavePoint& save_point) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = getConstructorMethodId(env);
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jsave_point =
        env->NewObject(jclazz, mid, static_cast<jlong>(save_point.size),
                       static_cast<jlong>(save_point.count),
                       static_cast<jlong>(save_point.content_flags));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jsave_point;
  }
};

// The portal class for org.rocksdb.WriteBatchWithIndex
class WriteBatchWithIndexJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::WriteBatchWithIndex*,
                                WriteBatchWithIndexJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatchWithIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/WriteBatchWithIndex");
  }
};

// The portal class for org.rocksdb.HistogramData
class HistogramDataJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.HistogramData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/HistogramData");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDDDJJD)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.BackupEngineOptions
class BackupEngineOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::BackupEngineOptions*,
                                BackupEngineOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupEngineOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/BackupEngineOptions");
  }
};

// The portal class for org.rocksdb.BackupEngine
class BackupEngineJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::BackupEngine*,
                                BackupEngineJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupEngine
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/BackupEngine");
  }
};

// The portal class for org.rocksdb.RocksIterator
class IteratorJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::Iterator*, IteratorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksIterator");
  }
};

// The portal class for org.rocksdb.FilterPolicy

enum FilterPolicyTypeJni {
  kUnknownFilterPolicy = 0x00,
  kBloomFilterPolicy = 0x01,
  kRibbonFilterPolicy = 0x02,
};
class FilterPolicyJni
    : public RocksDBNativeClass<
          std::shared_ptr<ROCKSDB_NAMESPACE::FilterPolicy>*, FilterPolicyJni> {
 private:
 public:
  /**
   * Get the Java Class org.rocksdb.FilterPolicy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FilterPolicy");
  }

  static jbyte toJavaIndexType(const FilterPolicyTypeJni& filter_policy_type) {
    return static_cast<jbyte>(filter_policy_type);
  }

  static FilterPolicyTypeJni getFilterPolicyType(
      const std::string& policy_class_name) {
    if (policy_class_name == "rocksdb.BuiltinBloomFilter") {
      return kBloomFilterPolicy;
    }
    return kUnknownFilterPolicy;
  }
};

// The portal class for org.rocksdb.ColumnFamilyHandle
class ColumnFamilyHandleJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
                                ColumnFamilyHandleJni> {
 public:
  static jobject fromCppColumnFamilyHandle(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ColumnFamilyHandle* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    return env->NewObject(jclazz, ctor, GET_CPLUSPLUS_POINTER(info));
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(J)V");
  }

  /**
   * Get the Java Class org.rocksdb.ColumnFamilyHandle
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ColumnFamilyHandle");
  }
};

// The portal class for org.rocksdb.FlushOptions
class FlushOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::FlushOptions*,
                                FlushOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.FlushOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FlushOptions");
  }
};

// The portal class for org.rocksdb.ComparatorOptions
class ComparatorOptionsJni
    : public RocksDBNativeClass<
          ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*,
          ComparatorOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ComparatorOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ComparatorOptions");
  }
};

// The portal class for org.rocksdb.AbstractCompactionFilterFactory
class AbstractCompactionFilterFactoryJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::CompactionFilterFactoryJniCallback*,
          AbstractCompactionFilterFactoryJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractCompactionFilterFactory
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(
        env, "org/rocksdb/AbstractCompactionFilterFactory");
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#createCompactionFilter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCreateCompactionFilterMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "createCompactionFilter", "(ZZ)J");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractTransactionNotifier
class AbstractTransactionNotifierJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::TransactionNotifierJniCallback*,
          AbstractTransactionNotifierJni> {
 public:
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(
        env, "org/rocksdb/AbstractTransactionNotifier");
  }

  // Get the java method `snapshotCreated`
  // of org.rocksdb.AbstractTransactionNotifier.
  static jmethodID getSnapshotCreatedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "snapshotCreated", "(J)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractComparatorJniBridge
class AbstractComparatorJniBridge : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparatorJniBridge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/AbstractComparatorJniBridge");
  }

  /**
   * Get the Java Method: Comparator#compareInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCompareInternalMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "compareInternal",
                               "(Lorg/rocksdb/AbstractComparator;Ljava/nio/"
                               "ByteBuffer;ILjava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortestSeparatorInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getFindShortestSeparatorInternalMethodId(JNIEnv* env,
                                                            jclass jclazz) {
    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "findShortestSeparatorInternal",
                               "(Lorg/rocksdb/AbstractComparator;Ljava/nio/"
                               "ByteBuffer;ILjava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortSuccessorInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getFindShortSuccessorInternalMethodId(JNIEnv* env,
                                                         jclass jclazz) {
    static jmethodID mid = env->GetStaticMethodID(
        jclazz, "findShortSuccessorInternal",
        "(Lorg/rocksdb/AbstractComparator;Ljava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractComparator
class AbstractComparatorJni
    : public RocksDBNativeClass<const ROCKSDB_NAMESPACE::ComparatorJniCallback*,
                                AbstractComparatorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractComparator");
  }

  /**
   * Get the Java Method: Comparator#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractSlice
class AbstractSliceJni
    : public NativeRocksMutableObject<const ROCKSDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractSlice");
  }
};

// The portal class for org.rocksdb.Slice
class SliceJni
    : public NativeRocksMutableObject<const ROCKSDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Slice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Slice");
  }

  /**
   * Constructs a Slice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java Slice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jslice = env->NewObject(jclazz, mid);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jslice;
  }
};

// The portal class for org.rocksdb.DirectSlice
class DirectSliceJni
    : public NativeRocksMutableObject<const ROCKSDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DirectSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DirectSlice");
  }

  /**
   * Constructs a DirectSlice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java DirectSlice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jdirect_slice = env->NewObject(jclazz, mid);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jdirect_slice;
  }
};

// The portal class for org.rocksdb.BackupInfo
class BackupInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/BackupInfo");
  }

  /**
   * Constructs a BackupInfo object
   *
   * @param env A pointer to the Java environment
   * @param backup_id id of the backup
   * @param timestamp timestamp of the backup
   * @param size size of the backup
   * @param number_files number of files related to the backup
   * @param app_metadata application specific metadata
   *
   * @return A reference to a Java BackupInfo object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
                            uint64_t size, uint32_t number_files,
                            const std::string& app_metadata) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(IJJILjava/lang/String;)V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jstring japp_metadata = nullptr;
    if (app_metadata != nullptr) {
      japp_metadata = env->NewStringUTF(app_metadata.c_str());
      if (japp_metadata == nullptr) {
        // exception occurred creating java string
        return nullptr;
      }
    }

    jobject jbackup_info = env->NewObject(jclazz, mid, backup_id, timestamp,
                                          size, number_files, japp_metadata);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(japp_metadata);
      return nullptr;
    }

    return jbackup_info;
  }
};

class BackupInfoListJni {
 public:
  /**
   * Converts a C++ std::vector<BackupInfo> object to
   * a Java ArrayList<org.rocksdb.BackupInfo> object
   *
   * @param env A pointer to the Java environment
   * @param backup_infos A vector of BackupInfo
   *
   * @return Either a reference to a Java ArrayList object, or a nullptr
   *     if an exception occurs
   */
  static jobject getBackupInfo(JNIEnv* env,
                               std::vector<BackupInfo> backup_infos) {
    jclass jarray_list_clazz =
        ROCKSDB_NAMESPACE::ListJni::getArrayListClass(env);
    if (jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID cstr_mid =
        ROCKSDB_NAMESPACE::ListJni::getArrayListConstructorMethodId(env);
    if (cstr_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jmethodID add_mid = ROCKSDB_NAMESPACE::ListJni::getListAddMethodId(env);
    if (add_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    // create java list
    jobject jbackup_info_handle_list =
        env->NewObject(jarray_list_clazz, cstr_mid, backup_infos.size());
    if (env->ExceptionCheck()) {
      // exception occurred constructing object
      return nullptr;
    }

    // insert in java list
    auto end = backup_infos.end();
    for (auto it = backup_infos.begin(); it != end; ++it) {
      auto backup_info = *it;

      jobject obj = ROCKSDB_NAMESPACE::BackupInfoJni::construct0(
          env, backup_info.backup_id, backup_info.timestamp, backup_info.size,
          backup_info.number_files, backup_info.app_metadata);
      if (env->ExceptionCheck()) {
        // exception occurred constructing object
        if (obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if (jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }

      jboolean rs =
          env->CallBooleanMethod(jbackup_info_handle_list, add_mid, obj);
      if (env->ExceptionCheck() || rs == JNI_FALSE) {
        // exception occurred calling method, or could not add
        if (obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if (jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }
    }

    return jbackup_info_handle_list;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator
class WBWIRocksIteratorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator");
  }

  /**
   * Get the Java Field: WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Field ID or nullptr if the class or field id could not
   *     be retrieved
   */
  static jfieldID getWriteEntryField(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jfieldID fid = env->GetFieldID(
        jclazz, "entry", "Lorg/rocksdb/WBWIRocksIterator$WriteEntry;");
    assert(fid != nullptr);
    return fid;
  }

  /**
   * Gets the value of the WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   * @param jwbwi_rocks_iterator A reference to a WBWIIterator
   *
   * @return A reference to a Java WBWIRocksIterator.WriteEntry object, or
   *     a nullptr if an exception occurs
   */
  static jobject getWriteEntry(JNIEnv* env, jobject jwbwi_rocks_iterator) {
    assert(jwbwi_rocks_iterator != nullptr);

    jfieldID jwrite_entry_field = getWriteEntryField(env);
    if (jwrite_entry_field == nullptr) {
      // exception occurred accessing the field
      return nullptr;
    }

    jobject jwe = env->GetObjectField(jwbwi_rocks_iterator, jwrite_entry_field);
    assert(jwe != nullptr);
    return jwe;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteType
class WriteTypeJni : public JavaClass {
 public:
  /**
   * Get the PUT enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject PUT(JNIEnv* env) { return getEnum(env, "PUT"); }

  /**
   * Get the MERGE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject MERGE(JNIEnv* env) { return getEnum(env, "MERGE"); }

  /**
   * Get the DELETE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DELETE(JNIEnv* env) { return getEnum(env, "DELETE"); }

  /**
   * Get the LOG enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject LOG(JNIEnv* env) { return getEnum(env, "LOG"); }

  // Returns the equivalent org.rocksdb.WBWIRocksIterator.WriteType for the
  // provided C++ ROCKSDB_NAMESPACE::WriteType enum
  static jbyte toJavaWriteType(const ROCKSDB_NAMESPACE::WriteType& writeType) {
    switch (writeType) {
      case ROCKSDB_NAMESPACE::WriteType::kPutRecord:
        return 0x0;
      case ROCKSDB_NAMESPACE::WriteType::kMergeRecord:
        return 0x1;
      case ROCKSDB_NAMESPACE::WriteType::kDeleteRecord:
        return 0x2;
      case ROCKSDB_NAMESPACE::WriteType::kSingleDeleteRecord:
        return 0x3;
      case ROCKSDB_NAMESPACE::WriteType::kDeleteRangeRecord:
        return 0x4;
      case ROCKSDB_NAMESPACE::WriteType::kLogDataRecord:
        return 0x5;
      case ROCKSDB_NAMESPACE::WriteType::kXIDRecord:
        return 0x6;
      default:
        return 0x7F;  // undefined
    }
  }

 private:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteType");
  }

  /**
   * Get an enum field of org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid = env->GetStaticFieldID(
        jclazz, name, "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    if (env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if (jfid == nullptr) {
      return nullptr;
    }

    jobject jwrite_type = env->GetStaticObjectField(jclazz, jfid);
    assert(jwrite_type != nullptr);
    return jwrite_type;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteEntry
class WriteEntryJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteEntry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
                                "org/rocksdb/WBWIRocksIterator$WriteEntry");
  }
};

// The portal class for org.rocksdb.InfoLogLevel
class InfoLogLevelJni : public JavaClass {
 public:
  /**
   * Get the DEBUG_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DEBUG_LEVEL(JNIEnv* env) {
    return getEnum(env, "DEBUG_LEVEL");
  }

  /**
   * Get the INFO_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject INFO_LEVEL(JNIEnv* env) { return getEnum(env, "INFO_LEVEL"); }

  /**
   * Get the WARN_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject WARN_LEVEL(JNIEnv* env) { return getEnum(env, "WARN_LEVEL"); }

  /**
   * Get the ERROR_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject ERROR_LEVEL(JNIEnv* env) {
    return getEnum(env, "ERROR_LEVEL");
  }

  /**
   * Get the FATAL_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject FATAL_LEVEL(JNIEnv* env) {
    return getEnum(env, "FATAL_LEVEL");
  }

  /**
   * Get the HEADER_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject HEADER_LEVEL(JNIEnv* env) {
    return getEnum(env, "HEADER_LEVEL");
  }

 private:
  /**
   * Get the Java Class org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/InfoLogLevel");
  }

  /**
   * Get an enum field of org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name, "Lorg/rocksdb/InfoLogLevel;");
    if (env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if (jfid == nullptr) {
      return nullptr;
    }

    jobject jinfo_log_level = env->GetStaticObjectField(jclazz, jfid);
    assert(jinfo_log_level != nullptr);
    return jinfo_log_level;
  }
};

// The portal class for org.rocksdb.Logger
class LoggerJni
    : public RocksDBNativeClass<
          std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*, LoggerJni> {
 public:
  /**
   * Get the Java Class org/rocksdb/Logger
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Logger");
  }

  /**
   * Get the Java Method: Logger#log
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getLogMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "log", "(Lorg/rocksdb/InfoLogLevel;Ljava/lang/String;)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.WalIterator.BatchResult
class BatchResultJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WalIterator.BatchResult
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WalIterator$BatchResult");
  }

  /**
   * Create a new Java org.rocksdb.WalIterator.BatchResult object
   * with the same properties as the provided C++ ROCKSDB_NAMESPACE::BatchResult
   * object
   *
   * @param env A pointer to the Java environment
   * @param batch_result The ROCKSDB_NAMESPACE::BatchResult object
   *
   * @return A reference to a Java
   *     org.rocksdb.WalIterator.BatchResult object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
                           ROCKSDB_NAMESPACE::BatchResult& batch_result) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jbatch_result = env->NewObject(jclazz, mid, batch_result.sequence,
                                           batch_result.writeBatchPtr.get());
    if (jbatch_result == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    batch_result.writeBatchPtr.release();
    return jbatch_result;
  }
};

// The portal class for org.rocksdb.BottommostLevelCompaction
class BottommostLevelCompactionJni {
 public:
  // Returns the equivalent org.rocksdb.BottommostLevelCompaction for the
  // provided C++ ROCKSDB_NAMESPACE::BottommostLevelCompaction enum
  static jint toJavaBottommostLevelCompaction(
      const ROCKSDB_NAMESPACE::BottommostLevelCompaction&
          bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kSkip:
        return 0x0;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::
          kIfHaveCompactionFilter:
        return 0x1;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForce:
        return 0x2;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForceOptimized:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::BottommostLevelCompaction
  // enum for the provided Java org.rocksdb.BottommostLevelCompaction
  static ROCKSDB_NAMESPACE::BottommostLevelCompaction
  toCppBottommostLevelCompaction(jint bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kSkip;
      case 0x1:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
      case 0x2:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForce;
      case 0x3:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForceOptimized;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
    }
  }
};

// The portal class for org.rocksdb.CompactionStopStyle
class CompactionStopStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStopStyle for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionStopStyle enum
  static jbyte toJavaCompactionStopStyle(
      const ROCKSDB_NAMESPACE::CompactionStopStyle& compaction_stop_style) {
    switch (compaction_stop_style) {
      case ROCKSDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleSimilarSize:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleTotalSize:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionStopStyle enum for
  // the provided Java org.rocksdb.CompactionStopStyle
  static ROCKSDB_NAMESPACE::CompactionStopStyle toCppCompactionStopStyle(
      jbyte jcompaction_stop_style) {
    switch (jcompaction_stop_style) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleTotalSize;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
    }
  }
};

// The portal class for org.rocksdb.CompressionType
class CompressionTypeJni {
 public:
  // Returns the equivalent org.rocksdb.CompressionType for the provided
  // C++ ROCKSDB_NAMESPACE::CompressionType enum
  static jbyte toJavaCompressionType(
      const ROCKSDB_NAMESPACE::CompressionType& compression_type) {
    switch (compression_type) {
      case ROCKSDB_NAMESPACE::CompressionType::kNoCompression:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompressionType::kSnappyCompression:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompressionType::kZlibCompression:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompressionType::kBZip2Compression:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompressionType::kLZ4Compression:
        return 0x4;
      case ROCKSDB_NAMESPACE::CompressionType::kLZ4HCCompression:
        return 0x5;
      case ROCKSDB_NAMESPACE::CompressionType::kXpressCompression:
        return 0x6;
      case ROCKSDB_NAMESPACE::CompressionType::kZSTD:
        return 0x7;
      case ROCKSDB_NAMESPACE::CompressionType::kDisableCompressionOption:
      default:
        return 0x7F;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompressionType enum for the
  // provided Java org.rocksdb.CompressionType
  static ROCKSDB_NAMESPACE::CompressionType toCppCompressionType(
      jbyte jcompression_type) {
    switch (jcompression_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompressionType::kSnappyCompression;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompressionType::kZlibCompression;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompressionType::kBZip2Compression;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompressionType::kLZ4Compression;
      case 0x5:
        return ROCKSDB_NAMESPACE::CompressionType::kLZ4HCCompression;
      case 0x6:
        return ROCKSDB_NAMESPACE::CompressionType::kXpressCompression;
      case 0x7:
        return ROCKSDB_NAMESPACE::CompressionType::kZSTD;
      case 0x7F:
      default:
        return ROCKSDB_NAMESPACE::CompressionType::kDisableCompressionOption;
    }
  }
};

// The portal class for org.rocksdb.CompactionPriority
class CompactionPriorityJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionPriority for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionPri enum
  static jbyte toJavaCompactionPriority(
      const ROCKSDB_NAMESPACE::CompactionPri& compaction_priority) {
    switch (compaction_priority) {
      case ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionPri::kMinOverlappingRatio:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompactionPri::kRoundRobin:
        return 0x4;
      default:
        return 0x0;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionPri enum for the
  // provided Java org.rocksdb.CompactionPriority
  static ROCKSDB_NAMESPACE::CompactionPri toCppCompactionPriority(
      jbyte jcompaction_priority) {
    switch (jcompaction_priority) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionPri::kMinOverlappingRatio;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompactionPri::kRoundRobin;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize;
    }
  }
};

// The portal class for org.rocksdb.WALRecoveryMode
class WALRecoveryModeJni {
 public:
  // Returns the equivalent org.rocksdb.WALRecoveryMode for the provided
  // C++ ROCKSDB_NAMESPACE::WALRecoveryMode enum
  static jbyte toJavaWALRecoveryMode(
      const ROCKSDB_NAMESPACE::WALRecoveryMode& wal_recovery_mode) {
    switch (wal_recovery_mode) {
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kTolerateCorruptedTailRecords:
        return 0x0;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency:
        return 0x1;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery:
        return 0x2;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords:
        return 0x3;
      default:
        // undefined/default
        return 0x2;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::WALRecoveryMode enum for the
  // provided Java org.rocksdb.WALRecoveryMode
  static ROCKSDB_NAMESPACE::WALRecoveryMode toCppWALRecoveryMode(
      jbyte jwal_recovery_mode) {
    switch (jwal_recovery_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::
            kTolerateCorruptedTailRecords;
      case 0x1:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency;
      case 0x2:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
      case 0x3:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
    }
  }
};

// The portal class for org.rocksdb.TickerType
class TickerTypeJni {
 public:
  // Returns the equivalent org.rocksdb.TickerType for the provided
  // C++ ROCKSDB_NAMESPACE::Tickers enum
  static jbyte toJavaTickerType(const ROCKSDB_NAMESPACE::Tickers& tickers) {
    switch (tickers) {
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS:
        return 0x0;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_HIT:
        return 0x1;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD:
        return 0x2;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_FAILURES:
        return 0x3;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_MISS:
        return 0x4;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_HIT:
        return 0x5;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD:
        return 0x6;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT:
        return 0x7;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_MISS:
        return 0x8;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_HIT:
        return 0x9;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD:
        return 0xA;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT:
        return 0xB;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_MISS:
        return 0xC;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_HIT:
        return 0xD;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD:
        return 0xE;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT:
        return 0xF;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_READ:
        return 0x10;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_WRITE:
        return 0x11;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_MISS:
        return 0x12;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_HIT:
        return 0x13;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_ADD:
        return 0x14;
      case ROCKSDB_NAMESPACE::Tickers::
          BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT:
        return 0x15;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_REDUNDANT:
        return 0x16;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD_REDUNDANT:
        return 0x17;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD_REDUNDANT:
        return 0x18;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD_REDUNDANT:
        return 0x19;
      case ROCKSDB_NAMESPACE::Tickers::
          BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT:
        return 0x1A;
      case ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_HITS:
        return 0x1B;
      case ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_FILTER_HITS:
        return 0x1C;
      case ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_INDEX_HITS:
        return 0x1D;
      case ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_DATA_HITS:
        return 0x1E;
      case ROCKSDB_NAMESPACE::Tickers::COMPRESSED_SECONDARY_CACHE_DUMMY_HITS:
        return 0x1F;
      case ROCKSDB_NAMESPACE::Tickers::COMPRESSED_SECONDARY_CACHE_HITS:
        return 0x20;
      case ROCKSDB_NAMESPACE::Tickers::COMPRESSED_SECONDARY_CACHE_PROMOTIONS:
        return 0x21;
      case ROCKSDB_NAMESPACE::Tickers::
          COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS:
        return 0x22;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_USEFUL:
        return 0x23;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_FULL_POSITIVE:
        return 0x24;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE:
        return 0x25;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_CHECKED:
        return 0x26;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_USEFUL:
        return 0x27;
      case ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_TRUE_POSITIVE:
        return 0x28;
      case ROCKSDB_NAMESPACE::Tickers::PERSISTENT_CACHE_HIT:
        return 0x29;
      case ROCKSDB_NAMESPACE::Tickers::PERSISTENT_CACHE_MISS:
        return 0x2A;
      case ROCKSDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_HIT:
        return 0x2B;
      case ROCKSDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_MISS:
        return 0x2C;
      case ROCKSDB_NAMESPACE::Tickers::MEMTABLE_HIT:
        return 0x2D;
      case ROCKSDB_NAMESPACE::Tickers::MEMTABLE_MISS:
        return 0x2E;
      case ROCKSDB_NAMESPACE::Tickers::GET_HIT_L0:
        return 0x2F;
      case ROCKSDB_NAMESPACE::Tickers::GET_HIT_L1:
        return 0x30;
      case ROCKSDB_NAMESPACE::Tickers::GET_HIT_L2_AND_UP:
        return 0x31;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY:
        return 0x32;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_OBSOLETE:
        return 0x33;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_RANGE_DEL:
        return 0x34;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_USER:
        return 0x35;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE:
        return 0x36;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE:
        return 0x37;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_CANCELLED:
        return 0x38;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_WRITTEN:
        return 0x39;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_READ:
        return 0x3A;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_UPDATED:
        return 0x3B;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_WRITTEN:
        return 0x3C;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_READ:
        return 0x3D;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_SEEK:
        return 0x3E;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_NEXT:
        return 0x3F;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_PREV:
        return 0x40;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_SEEK_FOUND:
        return 0x41;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_NEXT_FOUND:
        return 0x42;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_PREV_FOUND:
        return 0x43;
      case ROCKSDB_NAMESPACE::Tickers::ITER_BYTES_READ:
        return 0x44;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_ITER_SKIP:
        return 0x45;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION:
        return 0x46;
      case ROCKSDB_NAMESPACE::Tickers::NO_ITERATOR_CREATED:
        return 0x47;
      case ROCKSDB_NAMESPACE::Tickers::NO_ITERATOR_DELETED:
        return 0x48;
      case ROCKSDB_NAMESPACE::Tickers::NO_FILE_OPENS:
        return 0x49;
      case ROCKSDB_NAMESPACE::Tickers::NO_FILE_ERRORS:
        return 0x4A;
      case ROCKSDB_NAMESPACE::Tickers::STALL_MICROS:
        return 0x4B;
      case ROCKSDB_NAMESPACE::Tickers::DB_MUTEX_WAIT_MICROS:
        return 0x4C;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_CALLS:
        return 0x4D;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_READ:
        return 0x4E;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_BYTES_READ:
        return 0x4F;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_FOUND:
        return 0x50;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_MERGE_FAILURES:
        return 0x51;
      case ROCKSDB_NAMESPACE::Tickers::GET_UPDATES_SINCE_CALLS:
        return 0x52;
      case ROCKSDB_NAMESPACE::Tickers::WAL_FILE_SYNCED:
        return 0x53;
      case ROCKSDB_NAMESPACE::Tickers::WAL_FILE_BYTES:
        return 0x54;
      case ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_HIT:
        return -0x6C;
      case ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_MISS:
        return -0x6D;
      case ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_WAITED:
        return -0x6E;
      case ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_WAIT_MICROS:
        return -0x6F;
      case ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_FAILED:
        return -0x70;
      case ROCKSDB_NAMESPACE::Tickers::WRITE_DONE_BY_SELF:
        return 0x55;
      case ROCKSDB_NAMESPACE::Tickers::WRITE_DONE_BY_OTHER:
        return 0x56;
      case ROCKSDB_NAMESPACE::Tickers::WRITE_WITH_WAL:
        return 0x57;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES:
        return 0x58;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES:
        return 0x59;
      case ROCKSDB_NAMESPACE::Tickers::FLUSH_WRITE_BYTES:
        return 0x5A;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_MARKED:
        return 0x5B;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_PERIODIC:
        return 0x5C;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_TTL:
        return 0x5D;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_MARKED:
        return 0x5E;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_PERIODIC:
        return 0x5F;
      case ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_TTL:
        return 0x60;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES:
        return 0x61;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_ACQUIRES:
        return 0x62;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_RELEASES:
        return 0x63;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_CLEANUPS:
        return 0x64;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSED:
        return 0x65;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_DECOMPRESSED:
        return 0x66;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSED_FROM:
        return 0x67;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSED_TO:
        return 0x68;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSION_BYPASSED:
        return 0x69;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSION_REJECTED:
        return 0x6A;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSION_BYPASSED:
        return 0x6B;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSION_REJECTED:
        return 0x6C;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_DECOMPRESSED_FROM:
        return 0x6D;
      case ROCKSDB_NAMESPACE::Tickers::BYTES_DECOMPRESSED_TO:
        return 0x6E;
      case ROCKSDB_NAMESPACE::Tickers::MERGE_OPERATION_TOTAL_TIME:
        return 0x6F;
      case ROCKSDB_NAMESPACE::Tickers::FILTER_OPERATION_TOTAL_TIME:
        return 0x70;
      case ROCKSDB_NAMESPACE::Tickers::COMPACTION_CPU_TOTAL_TIME:
        return 0x71;
      case ROCKSDB_NAMESPACE::Tickers::ROW_CACHE_HIT:
        return 0x72;
      case ROCKSDB_NAMESPACE::Tickers::ROW_CACHE_MISS:
        return 0x73;
      case ROCKSDB_NAMESPACE::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES:
        return 0x74;
      case ROCKSDB_NAMESPACE::Tickers::READ_AMP_TOTAL_READ_BYTES:
        return 0x75;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_RATE_LIMITER_DRAINS:
        return 0x76;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_BYTES_READ:
        return -0x71;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_BYTES_WRITE:
        return -0x72;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_REQUESTS_READ:
        return -0x73;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_REQUESTS_WRITE:
        return -0x74;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_DELAYED_REQUESTS_READ:
        return -0x75;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_DELAYED_REQUESTS_WRITE:
        return -0x76;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_TOTAL_WAIT_MICROS_READ:
        return -0x77;
      case ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE:
        return -0x78;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_PUT:
        return 0x77;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_WRITE:
        return 0x78;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_GET:
        return 0x79;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_MULTIGET:
        return 0x7A;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_SEEK:
        return 0x7B;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_NEXT:
        return 0x7C;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_PREV:
        return 0x7D;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_KEYS_WRITTEN:
        return 0x7E;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_KEYS_READ:
        return 0x7F;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BYTES_WRITTEN:
        return -0x1;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BYTES_READ:
        return -0x2;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_INLINED_DEPRECATED:
        return -0x3;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_INLINED_TTL_DEPRECATED:
        return -0x4;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_BLOB:
        return -0x5;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_BLOB_TTL:
        return -0x6;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_BYTES_WRITTEN:
        return -0x7;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_BYTES_READ:
        return -0x8;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_SYNCED:
        return -0x9;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT:
        return -0xA;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE:
        return -0xB;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_COUNT:
        return -0xC;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_SIZE:
        return -0xD;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_FILES:
        return -0xE;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_NEW_FILES:
        return -0xF;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_FAILURES:
        return -0x10;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_KEYS_RELOCATED:
        return -0x11;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_BYTES_RELOCATED:
        return -0x12;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_NUM_FILES_EVICTED:
        return -0x13;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_NUM_KEYS_EVICTED:
        return -0x14;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_BYTES_EVICTED:
        return -0x15;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_MISS:
        return -0x16;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_HIT:
        return -0x17;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_ADD:
        return -0x18;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_ADD_FAILURES:
        return -0x19;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_BYTES_READ:
        return -0x1A;
      case ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_BYTES_WRITE:
        return -0x1B;
      case ROCKSDB_NAMESPACE::Tickers::TXN_PREPARE_MUTEX_OVERHEAD:
        return -0x1C;
      case ROCKSDB_NAMESPACE::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD:
        return -0x1D;
      case ROCKSDB_NAMESPACE::Tickers::TXN_DUPLICATE_KEY_OVERHEAD:
        return -0x1E;
      case ROCKSDB_NAMESPACE::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD:
        return -0x1F;
      case ROCKSDB_NAMESPACE::Tickers::TXN_GET_TRY_AGAIN:
        return -0x20;
      case ROCKSDB_NAMESPACE::Tickers::FILES_MARKED_TRASH:
        return -0x21;
      case ROCKSDB_NAMESPACE::Tickers::FILES_DELETED_FROM_TRASH_QUEUE:
        return -0x22;
      case ROCKSDB_NAMESPACE::Tickers::FILES_DELETED_IMMEDIATELY:
        return -0x23;
      case ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_BG_ERROR_COUNT:
        return -0x24;
      case ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_BG_IO_ERROR_COUNT:
        return -0x25;
      case ROCKSDB_NAMESPACE::Tickers::
          ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT:
        return -0x26;
      case ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_AUTORESUME_COUNT:
        return -0x27;
      case ROCKSDB_NAMESPACE::Tickers::
          ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT:
        return -0x28;
      case ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT:
        return -0x29;
      case ROCKSDB_NAMESPACE::Tickers::MEMTABLE_PAYLOAD_BYTES_AT_FLUSH:
        return -0x2A;
      case ROCKSDB_NAMESPACE::Tickers::MEMTABLE_GARBAGE_BYTES_AT_FLUSH:
        return -0x2B;
      case ROCKSDB_NAMESPACE::Tickers::VERIFY_CHECKSUM_READ_BYTES:
        return -0x2C;
      case ROCKSDB_NAMESPACE::Tickers::BACKUP_READ_BYTES:
        return -0x2D;
      case ROCKSDB_NAMESPACE::Tickers::BACKUP_WRITE_BYTES:
        return -0x2E;
      case ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_READ_BYTES:
        return -0x2F;
      case ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_WRITE_BYTES:
        return -0x30;
      case ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_RESUMED_BYTES:
        return -0x5F;
      case ROCKSDB_NAMESPACE::Tickers::HOT_FILE_READ_BYTES:
        return -0x31;
      case ROCKSDB_NAMESPACE::Tickers::WARM_FILE_READ_BYTES:
        return -0x32;
      case ROCKSDB_NAMESPACE::Tickers::COLD_FILE_READ_BYTES:
        return -0x33;
      case ROCKSDB_NAMESPACE::Tickers::HOT_FILE_READ_COUNT:
        return -0x34;
      case ROCKSDB_NAMESPACE::Tickers::WARM_FILE_READ_COUNT:
        return -0x35;
      case ROCKSDB_NAMESPACE::Tickers::COLD_FILE_READ_COUNT:
        return -0x36;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_READ_BYTES:
        return -0x37;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_READ_COUNT:
        return -0x38;
      case ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_READ_BYTES:
        return -0x39;
      case ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_READ_COUNT:
        return -0x3A;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_FILTERED:
        return -0x3B;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_FILTER_MATCH:
        return -0x3C;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_DATA:
        return -0x3D;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER:
        return -0x3E;
      case ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH:
        return -0x3F;
      case ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_FILTERED:
        return -0x40;
      case ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_FILTER_MATCH:
        return -0x41;
      case ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_DATA:
        return -0x42;
      case ROCKSDB_NAMESPACE::Tickers::
          NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER:
        return -0x43;
      case ROCKSDB_NAMESPACE::Tickers::
          NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH:
        return -0x44;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CHECKSUM_COMPUTE_COUNT:
        return -0x45;
      case ROCKSDB_NAMESPACE::Tickers::BLOCK_CHECKSUM_MISMATCH_COUNT:
        return -0x46;
      case ROCKSDB_NAMESPACE::Tickers::MULTIGET_COROUTINE_COUNT:
        return -0x47;
      case ROCKSDB_NAMESPACE::Tickers::READ_ASYNC_MICROS:
        return -0x48;
      case ROCKSDB_NAMESPACE::Tickers::ASYNC_READ_ERROR_COUNT:
        return -0x49;
      case ROCKSDB_NAMESPACE::Tickers::TABLE_OPEN_PREFETCH_TAIL_MISS:
        return -0x4A;
      case ROCKSDB_NAMESPACE::Tickers::TABLE_OPEN_PREFETCH_TAIL_HIT:
        return -0x4B;
      case ROCKSDB_NAMESPACE::Tickers::TIMESTAMP_FILTER_TABLE_CHECKED:
        return -0x4C;
      case ROCKSDB_NAMESPACE::Tickers::TIMESTAMP_FILTER_TABLE_FILTERED:
        return -0x4D;
      case ROCKSDB_NAMESPACE::Tickers::READAHEAD_TRIMMED:
        return -0x4E;
      case ROCKSDB_NAMESPACE::Tickers::FIFO_MAX_SIZE_COMPACTIONS:
        return -0x4F;
      case ROCKSDB_NAMESPACE::Tickers::FIFO_TTL_COMPACTIONS:
        return -0x50;
      case ROCKSDB_NAMESPACE::Tickers::PREFETCH_BYTES:
        return -0x51;
      case ROCKSDB_NAMESPACE::Tickers::PREFETCH_BYTES_USEFUL:
        return -0x52;
      case ROCKSDB_NAMESPACE::Tickers::PREFETCH_HITS:
        return -0x53;
      case ROCKSDB_NAMESPACE::Tickers::SST_FOOTER_CORRUPTION_COUNT:
        return -0x55;
      case ROCKSDB_NAMESPACE::Tickers::FILE_READ_CORRUPTION_RETRY_COUNT:
        return -0x56;
      case ROCKSDB_NAMESPACE::Tickers::FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT:
        return -0x57;
      case ROCKSDB_NAMESPACE::Tickers::FIFO_CHANGE_TEMPERATURE_COMPACTIONS:
        return -0x58;
      case ROCKSDB_NAMESPACE::Tickers::ICE_FILE_READ_BYTES:
        return -0x59;
      case ROCKSDB_NAMESPACE::Tickers::ICE_FILE_READ_COUNT:
        return -0x5A;
      case ROCKSDB_NAMESPACE::Tickers::COOL_FILE_READ_BYTES:
        return -0x5B;
      case ROCKSDB_NAMESPACE::Tickers::COOL_FILE_READ_COUNT:
        return -0x5C;
      case ROCKSDB_NAMESPACE::Tickers::NUMBER_WBWI_INGEST:
        return -0x5D;
      case ROCKSDB_NAMESPACE::Tickers::SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT:
        return -0x5E;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREPARE_CALLS:
        return -0x60;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREPARE_ERRORS:
        return -0x61;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_BLOCKS_PREFETCHED:
        return -0x62;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_BLOCKS_FROM_CACHE:
        return -0x63;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREFETCH_BYTES:
        return -0x64;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREFETCH_BLOCKS_WASTED:
        return -0x65;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_IO_REQUESTS:
        return -0x66;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_IO_COALESCED_NONADJACENT:
        return -0x67;
      case ROCKSDB_NAMESPACE::Tickers::MULTISCAN_SEEK_ERRORS:
        return -0x68;
      case ROCKSDB_NAMESPACE::Tickers::READ_PATH_RANGE_TOMBSTONES_INSERTED:
        return -0x69;
      case ROCKSDB_NAMESPACE::Tickers::READ_PATH_RANGE_TOMBSTONES_DISCARDED:
        return -0x6A;
      case ROCKSDB_NAMESPACE::Tickers::MANIFEST_VALIDATION_FAILURE_COUNT:
        return -0x6B;
      case ROCKSDB_NAMESPACE::Tickers::FLUSH_REASON_WRITE_BUFFER_FULL:
        return -0x79;
      case ROCKSDB_NAMESPACE::Tickers::FLUSH_REASON_WRITE_BUFFER_MANAGER:
        return -0x7A;
      case ROCKSDB_NAMESPACE::Tickers::
          FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS:
        return -0x7B;
      case ROCKSDB_NAMESPACE::Tickers::
          ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL:
        return -0x7C;
      case ROCKSDB_NAMESPACE::Tickers::
          ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER:
        return -0x7D;
      case ROCKSDB_NAMESPACE::Tickers::
          ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS:
        return -0x7E;
      case ROCKSDB_NAMESPACE::Tickers::ATOMIC_FLUSH_REQUEST_REASON_OTHER:
        return -0x7F;
      case ROCKSDB_NAMESPACE::Tickers::FILE_SUBMIT_ASYNC_READ_FALLBACK:
        return -0x80;
      case ROCKSDB_NAMESPACE::Tickers::TICKER_ENUM_MAX:
        // -0x54 is the max value at this time. Since these values are exposed
        // directly to Java clients, we'll keep the value the same till the next
        // major release.
        //
        // TODO: This particular case seems confusing and unnecessary to pin the
        // value since it's meant to be the number of tickers, not an actual
        // ticker value. But we aren't yet in a position to fix it since the
        // number of tickers doesn't fit in the Java representation (jbyte).
        return -0x54;
      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::Tickers enum for the
  // provided Java org.rocksdb.TickerType
  static ROCKSDB_NAMESPACE::Tickers toCppTickers(jbyte jticker_type) {
    switch (jticker_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS;
      case 0x1:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_HIT;
      case 0x2:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD;
      case 0x3:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_FAILURES;
      case 0x4:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_MISS;
      case 0x5:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_HIT;
      case 0x6:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD;
      case 0x7:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT;
      case 0x8:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_MISS;
      case 0x9:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_HIT;
      case 0xA:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD;
      case 0xB:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT;
      case 0xC:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_MISS;
      case 0xD:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_HIT;
      case 0xE:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD;
      case 0xF:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT;
      case 0x10:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_READ;
      case 0x11:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_WRITE;
      case 0x12:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_MISS;
      case 0x13:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_HIT;
      case 0x14:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSION_DICT_ADD;
      case 0x15:
        return ROCKSDB_NAMESPACE::Tickers::
            BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT;
      case 0x16:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_REDUNDANT;
      case 0x17:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD_REDUNDANT;
      case 0x18:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD_REDUNDANT;
      case 0x19:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD_REDUNDANT;
      case 0x1A:
        return ROCKSDB_NAMESPACE::Tickers::
            BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT;
      case 0x1B:
        return ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_HITS;
      case 0x1C:
        return ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_FILTER_HITS;
      case 0x1D:
        return ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_INDEX_HITS;
      case 0x1E:
        return ROCKSDB_NAMESPACE::Tickers::SECONDARY_CACHE_DATA_HITS;
      case 0x1F:
        return ROCKSDB_NAMESPACE::Tickers::
            COMPRESSED_SECONDARY_CACHE_DUMMY_HITS;
      case 0x20:
        return ROCKSDB_NAMESPACE::Tickers::COMPRESSED_SECONDARY_CACHE_HITS;
      case 0x21:
        return ROCKSDB_NAMESPACE::Tickers::
            COMPRESSED_SECONDARY_CACHE_PROMOTIONS;
      case 0x22:
        return ROCKSDB_NAMESPACE::Tickers::
            COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS;
      case 0x23:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_USEFUL;
      case 0x24:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_FULL_POSITIVE;
      case 0x25:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE;
      case 0x26:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_CHECKED;
      case 0x27:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_USEFUL;
      case 0x28:
        return ROCKSDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_TRUE_POSITIVE;
      case 0x29:
        return ROCKSDB_NAMESPACE::Tickers::PERSISTENT_CACHE_HIT;
      case 0x2A:
        return ROCKSDB_NAMESPACE::Tickers::PERSISTENT_CACHE_MISS;
      case 0x2B:
        return ROCKSDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_HIT;
      case 0x2C:
        return ROCKSDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_MISS;
      case 0x2D:
        return ROCKSDB_NAMESPACE::Tickers::MEMTABLE_HIT;
      case 0x2E:
        return ROCKSDB_NAMESPACE::Tickers::MEMTABLE_MISS;
      case 0x2F:
        return ROCKSDB_NAMESPACE::Tickers::GET_HIT_L0;
      case 0x30:
        return ROCKSDB_NAMESPACE::Tickers::GET_HIT_L1;
      case 0x31:
        return ROCKSDB_NAMESPACE::Tickers::GET_HIT_L2_AND_UP;
      case 0x32:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY;
      case 0x33:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_OBSOLETE;
      case 0x34:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_RANGE_DEL;
      case 0x35:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_USER;
      case 0x36:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE;
      case 0x37:
        return ROCKSDB_NAMESPACE::Tickers::
            COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE;
      case 0x38:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_CANCELLED;
      case 0x39:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_WRITTEN;
      case 0x3A:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_READ;
      case 0x3B:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_KEYS_UPDATED;
      case 0x3C:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_WRITTEN;
      case 0x3D:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_READ;
      case 0x3E:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_SEEK;
      case 0x3F:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_NEXT;
      case 0x40:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_PREV;
      case 0x41:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_SEEK_FOUND;
      case 0x42:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_NEXT_FOUND;
      case 0x43:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DB_PREV_FOUND;
      case 0x44:
        return ROCKSDB_NAMESPACE::Tickers::ITER_BYTES_READ;
      case 0x45:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_ITER_SKIP;
      case 0x46:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION;
      case 0x47:
        return ROCKSDB_NAMESPACE::Tickers::NO_ITERATOR_CREATED;
      case 0x48:
        return ROCKSDB_NAMESPACE::Tickers::NO_ITERATOR_DELETED;
      case 0x49:
        return ROCKSDB_NAMESPACE::Tickers::NO_FILE_OPENS;
      case 0x4A:
        return ROCKSDB_NAMESPACE::Tickers::NO_FILE_ERRORS;
      case 0x4B:
        return ROCKSDB_NAMESPACE::Tickers::STALL_MICROS;
      case 0x4C:
        return ROCKSDB_NAMESPACE::Tickers::DB_MUTEX_WAIT_MICROS;
      case 0x4D:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_CALLS;
      case 0x4E:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_READ;
      case 0x4F:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_BYTES_READ;
      case 0x50:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_FOUND;
      case 0x51:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_MERGE_FAILURES;
      case 0x52:
        return ROCKSDB_NAMESPACE::Tickers::GET_UPDATES_SINCE_CALLS;
      case 0x53:
        return ROCKSDB_NAMESPACE::Tickers::WAL_FILE_SYNCED;
      case 0x54:
        return ROCKSDB_NAMESPACE::Tickers::WAL_FILE_BYTES;
      case -0x6C:
        return ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_HIT;
      case -0x6D:
        return ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_MISS;
      case -0x6E:
        return ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_WAITED;
      case -0x6F:
        return ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_WAIT_MICROS;
      case -0x70:
        return ROCKSDB_NAMESPACE::Tickers::WAL_PRECREATE_FAILED;
      case 0x55:
        return ROCKSDB_NAMESPACE::Tickers::WRITE_DONE_BY_SELF;
      case 0x56:
        return ROCKSDB_NAMESPACE::Tickers::WRITE_DONE_BY_OTHER;
      case 0x57:
        return ROCKSDB_NAMESPACE::Tickers::WRITE_WITH_WAL;
      case 0x58:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES;
      case 0x59:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES;
      case 0x5A:
        return ROCKSDB_NAMESPACE::Tickers::FLUSH_WRITE_BYTES;
      case 0x5B:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_MARKED;
      case 0x5C:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_PERIODIC;
      case 0x5D:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_READ_BYTES_TTL;
      case 0x5E:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_MARKED;
      case 0x5F:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_PERIODIC;
      case 0x60:
        return ROCKSDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES_TTL;
      case 0x61:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES;
      case 0x62:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_ACQUIRES;
      case 0x63:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_RELEASES;
      case 0x64:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_CLEANUPS;
      case 0x65:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSED;
      case 0x66:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_DECOMPRESSED;
      case 0x67:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSED_FROM;
      case 0x68:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSED_TO;
      case 0x69:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSION_BYPASSED;
      case 0x6A:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_COMPRESSION_REJECTED;
      case 0x6B:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSION_BYPASSED;
      case 0x6C:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSION_REJECTED;
      case 0x6D:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_DECOMPRESSED_FROM;
      case 0x6E:
        return ROCKSDB_NAMESPACE::Tickers::BYTES_DECOMPRESSED_TO;
      case 0x6F:
        return ROCKSDB_NAMESPACE::Tickers::MERGE_OPERATION_TOTAL_TIME;
      case 0x70:
        return ROCKSDB_NAMESPACE::Tickers::FILTER_OPERATION_TOTAL_TIME;
      case 0x71:
        return ROCKSDB_NAMESPACE::Tickers::COMPACTION_CPU_TOTAL_TIME;
      case 0x72:
        return ROCKSDB_NAMESPACE::Tickers::ROW_CACHE_HIT;
      case 0x73:
        return ROCKSDB_NAMESPACE::Tickers::ROW_CACHE_MISS;
      case 0x74:
        return ROCKSDB_NAMESPACE::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES;
      case 0x75:
        return ROCKSDB_NAMESPACE::Tickers::READ_AMP_TOTAL_READ_BYTES;
      case 0x76:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_RATE_LIMITER_DRAINS;
      case -0x71:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_BYTES_READ;
      case -0x72:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_BYTES_WRITE;
      case -0x73:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_REQUESTS_READ;
      case -0x74:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_REQUESTS_WRITE;
      case -0x75:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_DELAYED_REQUESTS_READ;
      case -0x76:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_DELAYED_REQUESTS_WRITE;
      case -0x77:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_TOTAL_WAIT_MICROS_READ;
      case -0x78:
        return ROCKSDB_NAMESPACE::Tickers::RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE;
      case 0x77:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_PUT;
      case 0x78:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_WRITE;
      case 0x79:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_GET;
      case 0x7A:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_MULTIGET;
      case 0x7B:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_SEEK;
      case 0x7C:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_NEXT;
      case 0x7D:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_PREV;
      case 0x7E:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_KEYS_WRITTEN;
      case 0x7F:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_NUM_KEYS_READ;
      case -0x1:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BYTES_WRITTEN;
      case -0x2:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BYTES_READ;
      case -0x3:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_INLINED_DEPRECATED;
      case -0x4:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_INLINED_TTL_DEPRECATED;
      case -0x5:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_BLOB;
      case -0x6:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_WRITE_BLOB_TTL;
      case -0x7:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_BYTES_WRITTEN;
      case -0x8:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_BYTES_READ;
      case -0x9:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_FILE_SYNCED;
      case -0xA:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT;
      case -0xB:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE;
      case -0xC:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_COUNT;
      case -0xD:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_SIZE;
      case -0xE:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_FILES;
      case -0xF:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_NEW_FILES;
      case -0x10:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_FAILURES;
      case -0x11:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_NUM_KEYS_RELOCATED;
      case -0x12:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_GC_BYTES_RELOCATED;
      case -0x13:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_NUM_FILES_EVICTED;
      case -0x14:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_NUM_KEYS_EVICTED;
      case -0x15:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_FIFO_BYTES_EVICTED;
      case -0x16:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_MISS;
      case -0x17:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_HIT;
      case -0x18:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_ADD;
      case -0x19:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_ADD_FAILURES;
      case -0x1A:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_BYTES_READ;
      case -0x1B:
        return ROCKSDB_NAMESPACE::Tickers::BLOB_DB_CACHE_BYTES_WRITE;
      case -0x1C:
        return ROCKSDB_NAMESPACE::Tickers::TXN_PREPARE_MUTEX_OVERHEAD;
      case -0x1D:
        return ROCKSDB_NAMESPACE::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD;
      case -0x1E:
        return ROCKSDB_NAMESPACE::Tickers::TXN_DUPLICATE_KEY_OVERHEAD;
      case -0x1F:
        return ROCKSDB_NAMESPACE::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD;
      case -0x20:
        return ROCKSDB_NAMESPACE::Tickers::TXN_GET_TRY_AGAIN;
      case -0x21:
        return ROCKSDB_NAMESPACE::Tickers::FILES_MARKED_TRASH;
      case -0x22:
        return ROCKSDB_NAMESPACE::Tickers::FILES_DELETED_FROM_TRASH_QUEUE;
      case -0x23:
        return ROCKSDB_NAMESPACE::Tickers::FILES_DELETED_IMMEDIATELY;
      case -0x24:
        return ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_BG_ERROR_COUNT;
      case -0x25:
        return ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_BG_IO_ERROR_COUNT;
      case -0x26:
        return ROCKSDB_NAMESPACE::Tickers::
            ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT;
      case -0x27:
        return ROCKSDB_NAMESPACE::Tickers::ERROR_HANDLER_AUTORESUME_COUNT;
      case -0x28:
        return ROCKSDB_NAMESPACE::Tickers::
            ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT;
      case -0x29:
        return ROCKSDB_NAMESPACE::Tickers::
            ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT;
      case -0x2A:
        return ROCKSDB_NAMESPACE::Tickers::MEMTABLE_PAYLOAD_BYTES_AT_FLUSH;
      case -0x2B:
        return ROCKSDB_NAMESPACE::Tickers::MEMTABLE_GARBAGE_BYTES_AT_FLUSH;
      case -0x2C:
        return ROCKSDB_NAMESPACE::Tickers::VERIFY_CHECKSUM_READ_BYTES;
      case -0x2D:
        return ROCKSDB_NAMESPACE::Tickers::BACKUP_READ_BYTES;
      case -0x2E:
        return ROCKSDB_NAMESPACE::Tickers::BACKUP_WRITE_BYTES;
      case -0x2F:
        return ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_READ_BYTES;
      case -0x30:
        return ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_WRITE_BYTES;
      case -0x5F:
        return ROCKSDB_NAMESPACE::Tickers::REMOTE_COMPACT_RESUMED_BYTES;
      case -0x31:
        return ROCKSDB_NAMESPACE::Tickers::HOT_FILE_READ_BYTES;
      case -0x32:
        return ROCKSDB_NAMESPACE::Tickers::WARM_FILE_READ_BYTES;
      case -0x33:
        return ROCKSDB_NAMESPACE::Tickers::COLD_FILE_READ_BYTES;
      case -0x34:
        return ROCKSDB_NAMESPACE::Tickers::HOT_FILE_READ_COUNT;
      case -0x35:
        return ROCKSDB_NAMESPACE::Tickers::WARM_FILE_READ_COUNT;
      case -0x36:
        return ROCKSDB_NAMESPACE::Tickers::COLD_FILE_READ_COUNT;
      case -0x37:
        return ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_READ_BYTES;
      case -0x38:
        return ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_READ_COUNT;
      case -0x39:
        return ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_READ_BYTES;
      case -0x3A:
        return ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_READ_COUNT;
      case -0x3B:
        return ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_FILTERED;
      case -0x3C:
        return ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_FILTER_MATCH;
      case -0x3D:
        return ROCKSDB_NAMESPACE::Tickers::LAST_LEVEL_SEEK_DATA;
      case -0x3E:
        return ROCKSDB_NAMESPACE::Tickers::
            LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER;
      case -0x3F:
        return ROCKSDB_NAMESPACE::Tickers::
            LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH;
      case -0x40:
        return ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_FILTERED;
      case -0x41:
        return ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_FILTER_MATCH;
      case -0x42:
        return ROCKSDB_NAMESPACE::Tickers::NON_LAST_LEVEL_SEEK_DATA;
      case -0x43:
        return ROCKSDB_NAMESPACE::Tickers::
            NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER;
      case -0x44:
        return ROCKSDB_NAMESPACE::Tickers::
            NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH;
      case -0x45:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CHECKSUM_COMPUTE_COUNT;
      case -0x46:
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CHECKSUM_MISMATCH_COUNT;
      case -0x47:
        return ROCKSDB_NAMESPACE::Tickers::MULTIGET_COROUTINE_COUNT;
      case -0x48:
        return ROCKSDB_NAMESPACE::Tickers::READ_ASYNC_MICROS;
      case -0x49:
        return ROCKSDB_NAMESPACE::Tickers::ASYNC_READ_ERROR_COUNT;
      case -0x4A:
        return ROCKSDB_NAMESPACE::Tickers::TABLE_OPEN_PREFETCH_TAIL_MISS;
      case -0x4B:
        return ROCKSDB_NAMESPACE::Tickers::TABLE_OPEN_PREFETCH_TAIL_HIT;
      case -0x4C:
        return ROCKSDB_NAMESPACE::Tickers::TIMESTAMP_FILTER_TABLE_CHECKED;
      case -0x4D:
        return ROCKSDB_NAMESPACE::Tickers::TIMESTAMP_FILTER_TABLE_FILTERED;
      case -0x4E:
        return ROCKSDB_NAMESPACE::Tickers::READAHEAD_TRIMMED;
      case -0x4F:
        return ROCKSDB_NAMESPACE::Tickers::FIFO_MAX_SIZE_COMPACTIONS;
      case -0x50:
        return ROCKSDB_NAMESPACE::Tickers::FIFO_TTL_COMPACTIONS;
      case -0x51:
        return ROCKSDB_NAMESPACE::Tickers::PREFETCH_BYTES;
      case -0x52:
        return ROCKSDB_NAMESPACE::Tickers::PREFETCH_BYTES_USEFUL;
      case -0x53:
        return ROCKSDB_NAMESPACE::Tickers::PREFETCH_HITS;
      case -0x55:
        return ROCKSDB_NAMESPACE::Tickers::SST_FOOTER_CORRUPTION_COUNT;
      case -0x56:
        return ROCKSDB_NAMESPACE::Tickers::FILE_READ_CORRUPTION_RETRY_COUNT;
      case -0x57:
        return ROCKSDB_NAMESPACE::Tickers::
            FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT;
      case -0x58:
        return ROCKSDB_NAMESPACE::Tickers::FIFO_CHANGE_TEMPERATURE_COMPACTIONS;
      case -0x59:
        return ROCKSDB_NAMESPACE::Tickers::ICE_FILE_READ_BYTES;
      case -0x5A:
        return ROCKSDB_NAMESPACE::Tickers::ICE_FILE_READ_COUNT;
      case -0x5B:
        return ROCKSDB_NAMESPACE::Tickers::COOL_FILE_READ_BYTES;
      case -0x5C:
        return ROCKSDB_NAMESPACE::Tickers::COOL_FILE_READ_COUNT;
      case -0x5D:
        return ROCKSDB_NAMESPACE::Tickers::NUMBER_WBWI_INGEST;
      case -0x5E:
        return ROCKSDB_NAMESPACE::Tickers::
            SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT;
      case -0x60:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREPARE_CALLS;
      case -0x61:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREPARE_ERRORS;
      case -0x62:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_BLOCKS_PREFETCHED;
      case -0x63:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_BLOCKS_FROM_CACHE;
      case -0x64:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREFETCH_BYTES;
      case -0x65:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_PREFETCH_BLOCKS_WASTED;
      case -0x66:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_IO_REQUESTS;
      case -0x67:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_IO_COALESCED_NONADJACENT;
      case -0x68:
        return ROCKSDB_NAMESPACE::Tickers::MULTISCAN_SEEK_ERRORS;
      case -0x69:
        return ROCKSDB_NAMESPACE::Tickers::READ_PATH_RANGE_TOMBSTONES_INSERTED;
      case -0x6A:
        return ROCKSDB_NAMESPACE::Tickers::READ_PATH_RANGE_TOMBSTONES_DISCARDED;
      case -0x6B:
        return ROCKSDB_NAMESPACE::Tickers::MANIFEST_VALIDATION_FAILURE_COUNT;
      case -0x79:
        return ROCKSDB_NAMESPACE::Tickers::FLUSH_REASON_WRITE_BUFFER_FULL;
      case -0x7A:
        return ROCKSDB_NAMESPACE::Tickers::FLUSH_REASON_WRITE_BUFFER_MANAGER;
      case -0x7B:
        return ROCKSDB_NAMESPACE::Tickers::
            FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS;
      case -0x7C:
        return ROCKSDB_NAMESPACE::Tickers::
            ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL;
      case -0x7D:
        return ROCKSDB_NAMESPACE::Tickers::
            ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER;
      case -0x7E:
        return ROCKSDB_NAMESPACE::Tickers::
            ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS;
      case -0x7F:
        return ROCKSDB_NAMESPACE::Tickers::ATOMIC_FLUSH_REQUEST_REASON_OTHER;
      case -0x80:
        return ROCKSDB_NAMESPACE::Tickers::FILE_SUBMIT_ASYNC_READ_FALLBACK;
      case -0x54:
        // -0x54 is the max value at this time. Since these values are exposed
        // directly to Java clients, we'll keep the value the same till the next
        // major release.
        //
        // TODO: This particular case seems confusing and unnecessary to pin the
        // value since it's meant to be the number of tickers, not an actual
        // ticker value. But we aren't yet in a position to fix it since the
        // number of tickers doesn't fit in the Java representation (jbyte).
        return ROCKSDB_NAMESPACE::Tickers::TICKER_ENUM_MAX;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS;
    }
  }
};

// The portal class for org.rocksdb.HistogramType
class HistogramTypeJni {
 public:
  // Returns the equivalent org.rocksdb.HistogramType for the provided
  // C++ ROCKSDB_NAMESPACE::Histograms enum
  static jbyte toJavaHistogramsType(
      const ROCKSDB_NAMESPACE::Histograms& histograms) {
    switch (histograms) {
      case ROCKSDB_NAMESPACE::Histograms::DB_GET:
        return 0x0;
      case ROCKSDB_NAMESPACE::Histograms::DB_WRITE:
        return 0x1;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_TIME:
        return 0x2;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_CPU_TIME:
        return 0x3;
      case ROCKSDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME:
        return 0x4;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS:
        return 0x5;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS:
        return 0x6;
      case ROCKSDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS:
        return 0x7;
      case ROCKSDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS:
        return 0x8;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS:
        return 0x9;
      case ROCKSDB_NAMESPACE::Histograms::DB_MULTIGET:
        return 0xA;
      case ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS:
        return 0xB;
      case ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS:
        return 0xC;
      case ROCKSDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS:
        return 0xD;
      case ROCKSDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION:
        return 0xE;
      case ROCKSDB_NAMESPACE::Histograms::DB_SEEK:
        return 0xF;
      case ROCKSDB_NAMESPACE::Histograms::WRITE_STALL:
        return 0x10;
      case ROCKSDB_NAMESPACE::Histograms::SST_READ_MICROS:
        return 0x11;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_FLUSH_MICROS:
        return 0x12;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_COMPACTION_MICROS:
        return 0x13;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_OPEN_MICROS:
        return 0x14;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_GET_MICROS:
        return 0x15;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_MULTIGET_MICROS:
        return 0x16;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_ITERATOR_MICROS:
        return 0x17;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_VERIFY_DB_CHECKSUM_MICROS:
        return 0x18;
      case ROCKSDB_NAMESPACE::Histograms::
          FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS:
        return 0x19;
      case ROCKSDB_NAMESPACE::Histograms::SST_WRITE_MICROS:
        return 0x1A;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_FLUSH_MICROS:
        return 0x1B;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_COMPACTION_MICROS:
        return 0x1C;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_DB_OPEN_MICROS:
        return 0x1D;
      case ROCKSDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED:
        return 0x1E;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_READ:
        return 0x1F;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_WRITE:
        return 0x20;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET:
        return 0x21;
      case ROCKSDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS:
        return 0x22;
      case ROCKSDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS:
        return 0x23;
      case ROCKSDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS:
        return 0x24;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_KEY_SIZE:
        return 0x25;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_VALUE_SIZE:
        return 0x26;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_WRITE_MICROS:
        return 0x27;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_GET_MICROS:
        return 0x28;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_MULTIGET_MICROS:
        return 0x29;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_SEEK_MICROS:
        return 0x2A;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_NEXT_MICROS:
        return 0x2B;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_PREV_MICROS:
        return 0x2C;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS:
        return 0x2D;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS:
        return 0x2E;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS:
        return 0x2F;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_COMPRESSION_MICROS:
        return 0x30;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_DECOMPRESSION_MICROS:
        return 0x31;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor
      // version compatibility.
      case ROCKSDB_NAMESPACE::Histograms::FLUSH_TIME:
        return 0x32;
      case ROCKSDB_NAMESPACE::Histograms::SST_BATCH_SIZE:
        return 0x33;
      case ROCKSDB_NAMESPACE::Histograms::MULTIGET_IO_BATCH_SIZE:
        return 0x34;
      case ROCKSDB_NAMESPACE::Histograms::
          NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL:
        return 0x35;
      case ROCKSDB_NAMESPACE::Histograms::NUM_SST_READ_PER_LEVEL:
        return 0x36;
      case ROCKSDB_NAMESPACE::Histograms::NUM_LEVEL_READ_PER_MULTIGET:
        return 0x37;
      case ROCKSDB_NAMESPACE::Histograms::ERROR_HANDLER_AUTORESUME_RETRY_COUNT:
        return 0x38;
      case ROCKSDB_NAMESPACE::Histograms::ASYNC_READ_BYTES:
        return 0x39;
      case ROCKSDB_NAMESPACE::Histograms::POLL_WAIT_MICROS:
        return 0x3A;
      case ROCKSDB_NAMESPACE::Histograms::PREFETCHED_BYTES_DISCARDED:
        return 0x3B;
      case ASYNC_PREFETCH_ABORT_MICROS:
        return 0x3C;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_PREFETCH_TAIL_READ_BYTES:
        return 0x3D;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_PREFETCH_BYTES:
        return 0x3F;
      case ROCKSDB_NAMESPACE::Histograms::MULTISCAN_PREPARE_MICROS:
        return 0x40;
      case ROCKSDB_NAMESPACE::Histograms::MULTISCAN_BLOCKS_PER_PREPARE:
        return 0x41;
      case ROCKSDB_NAMESPACE::Histograms::BLOCK_KEY_DISTRIBUTION_CV:
        return 0x42;
      case ROCKSDB_NAMESPACE::Histograms::INGEST_EXTERNAL_FILE_PREPARE_TIME:
        return 0x43;
      case ROCKSDB_NAMESPACE::Histograms::INGEST_EXTERNAL_FILE_RUN_TIME:
        return 0x44;
      case ROCKSDB_NAMESPACE::Histograms::RATE_LIMITER_WAIT_MICROS_READ:
        return 0x45;
      case ROCKSDB_NAMESPACE::Histograms::RATE_LIMITER_WAIT_MICROS_WRITE:
        return 0x46;
      case ROCKSDB_NAMESPACE::Histograms::FLUSH_MEMTABLE_MEMORY_BYTES:
        return 0x47;
      case ROCKSDB_NAMESPACE::Histograms::FLUSH_MEMTABLE_TOTAL_DATA_SIZE:
        return 0x48;
      case ROCKSDB_NAMESPACE::Histograms::
          FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES:
        return 0x49;
      case ROCKSDB_NAMESPACE::Histograms::
          FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES:
        return 0x4A;
      case ROCKSDB_NAMESPACE::Histograms::OPEN_AND_COMPACT_DB_OPEN_MICROS:
        return 0x4B;
      case ROCKSDB_NAMESPACE::Histograms::
          IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS:
        return 0x4C;
      case ROCKSDB_NAMESPACE::Histograms::
          IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS:
        return 0x4D;
      case ROCKSDB_NAMESPACE::Histograms::
          IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS:
        return 0x4E;
      case ROCKSDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX:
        // 0x3E is reserved for backwards compatibility on current minor
        // version.
        return 0x3E;
      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::Histograms enum for the
  // provided Java org.rocksdb.HistogramsType
  static ROCKSDB_NAMESPACE::Histograms toCppHistograms(jbyte jhistograms_type) {
    switch (jhistograms_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Histograms::DB_GET;
      case 0x1:
        return ROCKSDB_NAMESPACE::Histograms::DB_WRITE;
      case 0x2:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_TIME;
      case 0x3:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_CPU_TIME;
      case 0x4:
        return ROCKSDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME;
      case 0x5:
        return ROCKSDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS;
      case 0x6:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS;
      case 0x7:
        return ROCKSDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS;
      case 0x8:
        return ROCKSDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS;
      case 0x9:
        return ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS;
      case 0xA:
        return ROCKSDB_NAMESPACE::Histograms::DB_MULTIGET;
      case 0xB:
        return ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS;
      case 0xC:
        return ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS;
      case 0xD:
        return ROCKSDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS;
      case 0xE:
        return ROCKSDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION;
      case 0xF:
        return ROCKSDB_NAMESPACE::Histograms::DB_SEEK;
      case 0x10:
        return ROCKSDB_NAMESPACE::Histograms::WRITE_STALL;
      case 0x11:
        return ROCKSDB_NAMESPACE::Histograms::SST_READ_MICROS;
      case 0x12:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_FLUSH_MICROS;
      case 0x13:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_COMPACTION_MICROS;
      case 0x14:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_OPEN_MICROS;
      case 0x15:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_GET_MICROS;
      case 0x16:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_MULTIGET_MICROS;
      case 0x17:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_ITERATOR_MICROS;
      case 0x18:
        return ROCKSDB_NAMESPACE::Histograms::
            FILE_READ_VERIFY_DB_CHECKSUM_MICROS;
      case 0x19:
        return ROCKSDB_NAMESPACE::Histograms::
            FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS;
      case 0x1A:
        return ROCKSDB_NAMESPACE::Histograms::SST_WRITE_MICROS;
      case 0x1B:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_FLUSH_MICROS;
      case 0x1C:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_COMPACTION_MICROS;
      case 0x1D:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_DB_OPEN_MICROS;
      case 0x1E:
        return ROCKSDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED;
      case 0x1F:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_READ;
      case 0x20:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_WRITE;
      case 0x21:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET;
      case 0x22:
        return ROCKSDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS;
      case 0x23:
        return ROCKSDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS;
      case 0x24:
        return ROCKSDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS;
      case 0x25:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_KEY_SIZE;
      case 0x26:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_VALUE_SIZE;
      case 0x27:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_WRITE_MICROS;
      case 0x28:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_GET_MICROS;
      case 0x29:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_MULTIGET_MICROS;
      case 0x2A:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_SEEK_MICROS;
      case 0x2B:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_NEXT_MICROS;
      case 0x2C:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_PREV_MICROS;
      case 0x2D:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS;
      case 0x2E:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS;
      case 0x2F:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS;
      case 0x30:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_COMPRESSION_MICROS;
      case 0x31:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_DECOMPRESSION_MICROS;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor
      // version compatibility.
      case 0x32:
        return ROCKSDB_NAMESPACE::Histograms::FLUSH_TIME;
      case 0x33:
        return ROCKSDB_NAMESPACE::Histograms::SST_BATCH_SIZE;
      case 0x34:
        return ROCKSDB_NAMESPACE::Histograms::MULTIGET_IO_BATCH_SIZE;
      case 0x35:
        return ROCKSDB_NAMESPACE::Histograms::
            NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL;
      case 0x36:
        return ROCKSDB_NAMESPACE::Histograms::NUM_SST_READ_PER_LEVEL;
      case 0x37:
        return ROCKSDB_NAMESPACE::Histograms::NUM_LEVEL_READ_PER_MULTIGET;
      case 0x38:
        return ROCKSDB_NAMESPACE::Histograms::
            ERROR_HANDLER_AUTORESUME_RETRY_COUNT;
      case 0x39:
        return ROCKSDB_NAMESPACE::Histograms::ASYNC_READ_BYTES;
      case 0x3A:
        return ROCKSDB_NAMESPACE::Histograms::POLL_WAIT_MICROS;
      case 0x3B:
        return ROCKSDB_NAMESPACE::Histograms::PREFETCHED_BYTES_DISCARDED;
      case 0x3C:
        return ROCKSDB_NAMESPACE::Histograms::ASYNC_PREFETCH_ABORT_MICROS;
      case 0x3D:
        return ROCKSDB_NAMESPACE::Histograms::
            TABLE_OPEN_PREFETCH_TAIL_READ_BYTES;
      case 0x3F:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_PREFETCH_BYTES;
      case 0x40:
        return ROCKSDB_NAMESPACE::Histograms::MULTISCAN_PREPARE_MICROS;
      case 0x41:
        return ROCKSDB_NAMESPACE::Histograms::MULTISCAN_BLOCKS_PER_PREPARE;
      case 0x42:
        return ROCKSDB_NAMESPACE::Histograms::BLOCK_KEY_DISTRIBUTION_CV;
      case 0x43:
        return ROCKSDB_NAMESPACE::Histograms::INGEST_EXTERNAL_FILE_PREPARE_TIME;
      case 0x44:
        return ROCKSDB_NAMESPACE::Histograms::INGEST_EXTERNAL_FILE_RUN_TIME;
      case 0x45:
        return ROCKSDB_NAMESPACE::Histograms::RATE_LIMITER_WAIT_MICROS_READ;
      case 0x46:
        return ROCKSDB_NAMESPACE::Histograms::RATE_LIMITER_WAIT_MICROS_WRITE;
      case 0x47:
        return ROCKSDB_NAMESPACE::Histograms::FLUSH_MEMTABLE_MEMORY_BYTES;
      case 0x48:
        return ROCKSDB_NAMESPACE::Histograms::FLUSH_MEMTABLE_TOTAL_DATA_SIZE;
      case 0x49:
        return ROCKSDB_NAMESPACE::Histograms::
            FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES;
      case 0x4A:
        return ROCKSDB_NAMESPACE::Histograms::
            FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES;
      case 0x4B:
        return ROCKSDB_NAMESPACE::Histograms::OPEN_AND_COMPACT_DB_OPEN_MICROS;
      case 0x4C:
        return ROCKSDB_NAMESPACE::Histograms::
            IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS;
      case 0x4D:
        return ROCKSDB_NAMESPACE::Histograms::
            IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS;
      case 0x4E:
        return ROCKSDB_NAMESPACE::Histograms::
            IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS;
      case 0x3E:
        // 0x3E is reserved for backwards compatibility on current minor
        // version.
        return ROCKSDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::Histograms::DB_GET;
    }
  }
};

// The portal class for org.rocksdb.StatsLevel
class StatsLevelJni {
 public:
  // Returns the equivalent org.rocksdb.StatsLevel for the provided
  // C++ ROCKSDB_NAMESPACE::StatsLevel enum
  static jbyte toJavaStatsLevel(
      const ROCKSDB_NAMESPACE::StatsLevel& stats_level) {
    switch (stats_level) {
      case ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers:
        return 0x0;
      case ROCKSDB_NAMESPACE::StatsLevel::kExceptTimeForMutex:
        return 0x1;
      case ROCKSDB_NAMESPACE::StatsLevel::kAll:
        return 0x2;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::StatsLevel enum for the
  // provided Java org.rocksdb.StatsLevel
  static ROCKSDB_NAMESPACE::StatsLevel toCppStatsLevel(jbyte jstats_level) {
    switch (jstats_level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
      case 0x1:
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptTimeForMutex;
      case 0x2:
        return ROCKSDB_NAMESPACE::StatsLevel::kAll;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
    }
  }
};

// The portal class for org.rocksdb.RateLimiterMode
class RateLimiterModeJni {
 public:
  // Returns the equivalent org.rocksdb.RateLimiterMode for the provided
  // C++ ROCKSDB_NAMESPACE::RateLimiter::Mode enum
  static jbyte toJavaRateLimiterMode(
      const ROCKSDB_NAMESPACE::RateLimiter::Mode& rate_limiter_mode) {
    switch (rate_limiter_mode) {
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kReadsOnly:
        return 0x0;
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly:
        return 0x1;
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kAllIo:
        return 0x2;

      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::RateLimiter::Mode enum for
  // the provided Java org.rocksdb.RateLimiterMode
  static ROCKSDB_NAMESPACE::RateLimiter::Mode toCppRateLimiterMode(
      jbyte jrate_limiter_mode) {
    switch (jrate_limiter_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kReadsOnly;
      case 0x1:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
      case 0x2:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kAllIo;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
    }
  }
};

// The portal class for org.rocksdb.MemoryUsageType
class MemoryUsageTypeJni {
 public:
  // Returns the equivalent org.rocksdb.MemoryUsageType for the provided
  // C++ ROCKSDB_NAMESPACE::MemoryUtil::UsageType enum
  static jbyte toJavaMemoryUsageType(
      const ROCKSDB_NAMESPACE::MemoryUtil::UsageType& usage_type) {
    switch (usage_type) {
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal:
        return 0x0;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed:
        return 0x1;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal:
        return 0x2;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal:
        return 0x3;
      default:
        // undefined: use kNumUsageTypes
        return 0x4;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::MemoryUtil::UsageType enum
  // for the provided Java org.rocksdb.MemoryUsageType
  static ROCKSDB_NAMESPACE::MemoryUtil::UsageType toCppMemoryUsageType(
      jbyte usage_type) {
    switch (usage_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal;
      case 0x1:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed;
      case 0x2:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal;
      case 0x3:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal;
      default:
        // undefined/default: use kNumUsageTypes
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kNumUsageTypes;
    }
  }
};

class PerfLevelTypeJni {
 public:
  static jbyte toJavaPerfLevelType(const ROCKSDB_NAMESPACE::PerfLevel level) {
    switch (level) {
      case ROCKSDB_NAMESPACE::PerfLevel::kUninitialized:
        return 0x0;
      case ROCKSDB_NAMESPACE::PerfLevel::kDisable:
        return 0x1;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableCount:
        return 0x2;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeExceptForMutex:
        return 0x3;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex:
        return 0x4;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTime:
        return 0x5;
      case ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds:
        return 0x6;
      default:
        return 0x6;
    }
  }

  static ROCKSDB_NAMESPACE::PerfLevel toCppPerfLevelType(const jbyte level) {
    switch (level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::PerfLevel::kUninitialized;
      case 0x1:
        return ROCKSDB_NAMESPACE::PerfLevel::kDisable;
      case 0x2:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableCount;
      case 0x3:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeExceptForMutex;
      case 0x4:
        return ROCKSDB_NAMESPACE::PerfLevel::
            kEnableTimeAndCPUTimeExceptForMutex;
      case 0x5:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableTime;
      case 0x6:
        return ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds;
      default:
        return ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds;
    }
  }
};

// The portal class for org.rocksdb.Transaction
class TransactionJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Transaction
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Transaction");
  }

  /**
   * Create a new Java org.rocksdb.Transaction.WaitingTransactions object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newWaitingTransactions(
      JNIEnv* env, jobject jtransaction, const uint32_t column_family_id,
      const std::string& key,
      const std::vector<TransactionID>& transaction_ids) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newWaitingTransactions",
        "(JLjava/lang/String;[J)Lorg/rocksdb/Transaction$WaitingTransactions;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const size_t len = transaction_ids.size();
    jlongArray jtransaction_ids = env->NewLongArray(static_cast<jsize>(len));
    if (jtransaction_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    jboolean is_copy;
    jlong* body = env->GetLongArrayElements(jtransaction_ids, &is_copy);
    if (body == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(transaction_ids[i]);
    }
    env->ReleaseLongArrayElements(jtransaction_ids, body,
                                  is_copy == JNI_TRUE ? 0 : JNI_ABORT);

    jobject jwaiting_transactions = env->CallObjectMethod(
        jtransaction, mid, static_cast<jlong>(column_family_id), jkey,
        jtransaction_ids);
    if (env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }

    return jwaiting_transactions;
  }
};

// The portal class for org.rocksdb.TransactionDB
class TransactionDBJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockInfo object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newDeadlockInfo(
      JNIEnv* env, jobject jtransaction_db,
      const ROCKSDB_NAMESPACE::TransactionID transaction_id,
      const uint32_t column_family_id, const std::string& waiting_key,
      const bool exclusive) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newDeadlockInfo",
        "(JJLjava/lang/String;Z)Lorg/rocksdb/TransactionDB$DeadlockInfo;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jwaiting_key = env->NewStringUTF(waiting_key.c_str());
    if (jwaiting_key == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    // resolve the column family id to a ColumnFamilyHandle
    jobject jdeadlock_info = env->CallObjectMethod(
        jtransaction_db, mid, transaction_id,
        static_cast<jlong>(column_family_id), jwaiting_key, exclusive);
    if (env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jwaiting_key);
      return nullptr;
    }

    return jdeadlock_info;
  }
};

// The portal class for org.rocksdb.TxnDBWritePolicy
class TxnDBWritePolicyJni {
 public:
  // Returns the equivalent org.rocksdb.TxnDBWritePolicy for the provided
  // C++ ROCKSDB_NAMESPACE::TxnDBWritePolicy enum
  static jbyte toJavaTxnDBWritePolicy(
      const ROCKSDB_NAMESPACE::TxnDBWritePolicy& txndb_write_policy) {
    switch (txndb_write_policy) {
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED:
        return 0x0;
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED:
        return 0x1;
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::TxnDBWritePolicy enum for the
  // provided Java org.rocksdb.TxnDBWritePolicy
  static ROCKSDB_NAMESPACE::TxnDBWritePolicy toCppTxnDBWritePolicy(
      jbyte jtxndb_write_policy) {
    switch (jtxndb_write_policy) {
      case 0x0:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
      case 0x1:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED;
      case 0x2:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
    }
  }
};

// The portal class for org.rocksdb.TransactionDB.KeyLockInfo
class KeyLockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.KeyLockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$KeyLockInfo");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.KeyLockInfo object
   * with the same properties as the provided C++ ROCKSDB_NAMESPACE::KeyLockInfo
   * object
   *
   * @param env A pointer to the Java environment
   * @param key_lock_info The ROCKSDB_NAMESPACE::KeyLockInfo object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.KeyLockInfo object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const ROCKSDB_NAMESPACE::KeyLockInfo& key_lock_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;[JZ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key_lock_info.key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const jsize jtransaction_ids_len =
        static_cast<jsize>(key_lock_info.ids.size());
    jlongArray jtransactions_ids = env->NewLongArray(jtransaction_ids_len);
    if (jtransactions_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    const jobject jkey_lock_info = env->NewObject(
        jclazz, mid, jkey, jtransactions_ids, key_lock_info.exclusive);
    if (jkey_lock_info == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jtransactions_ids);
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    return jkey_lock_info;
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockInfo
class DeadlockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$DeadlockInfo");
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockPath
class DeadlockPathJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockPath
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$DeadlockPath");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockPath object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.DeadlockPath object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const jobjectArray jdeadlock_infos,
                           const bool limit_exceeded) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "([LDeadlockInfo;Z)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jdeadlock_path =
        env->NewObject(jclazz, mid, jdeadlock_infos, limit_exceeded);
    if (jdeadlock_path == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    return jdeadlock_path;
  }
};

class AbstractTableFilterJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::TableFilterJniCallback*,
          AbstractTableFilterJni> {
 public:
  /**
   * Get the Java Method: TableFilter#filter(TableProperties)
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getFilterMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "filter", "(Lorg/rocksdb/TableProperties;)Z");
    assert(mid != nullptr);
    return mid;
  }

 private:
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFilter");
  }
};

class TablePropertiesJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.TableProperties object.
   *
   * @param env A pointer to the Java environment
   * @param table_properties A Cpp table properties object
   *
   * @return A reference to a Java org.rocksdb.TableProperties object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppTableProperties(
      JNIEnv* env, const ROCKSDB_NAMESPACE::TableProperties& table_properties) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>",
        "(JJJJJJJJJJJJJJJJJJJJJ[BLjava/lang/String;Ljava/lang/String;Ljava/"
        "lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/"
        "String;Ljava/util/Map;Ljava/util/Map;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jcolumn_family_name = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, table_properties.column_family_name);
    if (jcolumn_family_name == nullptr) {
      // exception occurred creating java string
      return nullptr;
    }

    jstring jfilter_policy_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.filter_policy_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      return nullptr;
    }

    jstring jcomparator_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.comparator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      return nullptr;
    }

    jstring jmerge_operator_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.merge_operator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      return nullptr;
    }

    jstring jprefix_extractor_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.prefix_extractor_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      return nullptr;
    }

    jstring jproperty_collectors_names =
        ROCKSDB_NAMESPACE::JniUtil::toJavaString(
            env, &table_properties.property_collectors_names, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      return nullptr;
    }

    jstring jcompression_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.compression_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      return nullptr;
    }

    // Map<String, String>
    jobject juser_collected_properties =
        ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(
            env, &table_properties.user_collected_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      return nullptr;
    }

    // Map<String, String>
    jobject jreadable_properties = ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(
        env, &table_properties.readable_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      env->DeleteLocalRef(juser_collected_properties);
      return nullptr;
    }

    jobject jtable_properties = env->NewObject(
        jclazz, mid, static_cast<jlong>(table_properties.data_size),
        static_cast<jlong>(table_properties.index_size),
        static_cast<jlong>(table_properties.index_partitions),
        static_cast<jlong>(table_properties.top_level_index_size),
        static_cast<jlong>(table_properties.index_key_is_user_key),
        static_cast<jlong>(table_properties.index_value_is_delta_encoded),
        static_cast<jlong>(table_properties.filter_size),
        static_cast<jlong>(table_properties.raw_key_size),
        static_cast<jlong>(table_properties.raw_value_size),
        static_cast<jlong>(table_properties.num_data_blocks),
        static_cast<jlong>(table_properties.num_entries),
        static_cast<jlong>(table_properties.num_deletions),
        static_cast<jlong>(table_properties.num_merge_operands),
        static_cast<jlong>(table_properties.num_range_deletions),
        static_cast<jlong>(table_properties.format_version),
        static_cast<jlong>(table_properties.fixed_key_len),
        static_cast<jlong>(table_properties.column_family_id),
        static_cast<jlong>(table_properties.oldest_ancestor_time),
        static_cast<jlong>(table_properties.oldest_key_time),
        static_cast<jlong>(
            table_properties.slow_compression_estimated_data_size),
        static_cast<jlong>(
            table_properties.fast_compression_estimated_data_size),
        jcolumn_family_name, jfilter_policy_name, jcomparator_name,
        jmerge_operator_name, jprefix_extractor_name,
        jproperty_collectors_names, jcompression_name,
        juser_collected_properties, jreadable_properties);

    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jtable_properties;
  }

 private:
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableProperties");
  }
};

class ColumnFamilyDescriptorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyDescriptor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyDescriptor");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyDescriptor object with the same
   * properties as the provided C++ ROCKSDB_NAMESPACE::ColumnFamilyDescriptor
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfd A pointer to ROCKSDB_NAMESPACE::ColumnFamilyDescriptor object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyDescriptor object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, ColumnFamilyDescriptor* cfd) {
    jbyteArray jcf_name = JniUtil::copyBytes(env, cfd->name);
    jobject cfopts = ColumnFamilyOptionsJni::construct(env, &(cfd->options));

    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "([BLorg/rocksdb/ColumnFamilyOptions;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, jcf_name, cfopts);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    return jcfd;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyName
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "columnFamilyName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "columnFamilyOptions", "()Lorg/rocksdb/ColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.IndexType
class IndexTypeJni {
 public:
  // Returns the equivalent org.rocksdb.IndexType for the provided
  // C++ ROCKSDB_NAMESPACE::IndexType enum
  static jbyte toJavaIndexType(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType& index_type) {
    switch (index_type) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::kBinarySearch:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::kHashSearch:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
          kTwoLevelIndexSearch:
        return 0x2;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
          kBinarySearchWithFirstKey:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::IndexType enum for the
  // provided Java org.rocksdb.IndexType
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType toCppIndexType(
      jbyte jindex_type) {
    switch (jindex_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearch;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kHashSearch;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kTwoLevelIndexSearch;
      case 0x3:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearchWithFirstKey;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearch;
    }
  }
};

// The portal class for org.rocksdb.DataBlockIndexType
class DataBlockIndexTypeJni {
 public:
  // Returns the equivalent org.rocksdb.DataBlockIndexType for the provided
  // C++ ROCKSDB_NAMESPACE::DataBlockIndexType enum
  static jbyte toJavaDataBlockIndexType(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType&
          index_type) {
    switch (index_type) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
          kDataBlockBinarySearch:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
          kDataBlockBinaryAndHash:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::DataBlockIndexType enum for
  // the provided Java org.rocksdb.DataBlockIndexType
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType
  toCppDataBlockIndexType(jbyte jindex_type) {
    switch (jindex_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinarySearch;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinaryAndHash;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinarySearch;
    }
  }
};

// The portal class for org.rocksdb.IndexSearchType
class IndexSearchTypeJni {
 public:
  // Returns the equivalent org.rocksdb.IndexSearchType for the provided
  // C++ ROCKSDB_NAMESPACE::BlockSearchType enum
  static jbyte toJavaIndexSearchType(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType&
          index_block_search_type) {
    switch (index_block_search_type) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::kBinary:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::
          kInterpolation:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::kAuto:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::BlockSearchType enum for
  // the provided Java org.rocksdb.IndexSearchType
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType
  toCppIndexSearchType(jbyte jindex_search_type) {
    switch (jindex_search_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::
            kBinary;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::
            kInterpolation;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::
            kAuto;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::BlockSearchType::
            kBinary;
    }
  }
};

// The portal class for org.rocksdb.OptimizeKeyCommonPrefix
class OptimizeKeyCommonPrefixJni {
 public:
  // Returns the equivalent org.rocksdb.OptimizeKeyCommonPrefix for the provided
  // C++ ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix enum
  static jbyte toJavaOptimizeKeyCommonPrefix(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix&
          optimize_key_common_prefix) {
    switch (optimize_key_common_prefix) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix::
          kDisabled:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix::
          kIfFastSeek:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix::
          kEnabled:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++
  // ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix enum for
  // the provided Java org.rocksdb.OptimizeKeyCommonPrefix
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::OptimizeKeyCommonPrefix
  toCppOptimizeKeyCommonPrefix(jbyte joptimize_key_common_prefix) {
    switch (joptimize_key_common_prefix) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::
            OptimizeKeyCommonPrefix::kDisabled;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::
            OptimizeKeyCommonPrefix::kIfFastSeek;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::
            OptimizeKeyCommonPrefix::kEnabled;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::
            OptimizeKeyCommonPrefix::kIfFastSeek;
    }
  }
};

// The portal class for org.rocksdb.ChecksumType
class ChecksumTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ChecksumType for the provided
  // C++ ROCKSDB_NAMESPACE::ChecksumType enum
  static jbyte toJavaChecksumType(
      const ROCKSDB_NAMESPACE::ChecksumType& checksum_type) {
    switch (checksum_type) {
      case ROCKSDB_NAMESPACE::ChecksumType::kNoChecksum:
        return 0x0;
      case ROCKSDB_NAMESPACE::ChecksumType::kCRC32c:
        return 0x1;
      case ROCKSDB_NAMESPACE::ChecksumType::kxxHash:
        return 0x2;
      case ROCKSDB_NAMESPACE::ChecksumType::kxxHash64:
        return 0x3;
      case ROCKSDB_NAMESPACE::ChecksumType::kXXH3:
        return 0x4;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ChecksumType enum for the
  // provided Java org.rocksdb.ChecksumType
  static ROCKSDB_NAMESPACE::ChecksumType toCppChecksumType(
      jbyte jchecksum_type) {
    switch (jchecksum_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ChecksumType::kNoChecksum;
      case 0x1:
        return ROCKSDB_NAMESPACE::ChecksumType::kCRC32c;
      case 0x2:
        return ROCKSDB_NAMESPACE::ChecksumType::kxxHash;
      case 0x3:
        return ROCKSDB_NAMESPACE::ChecksumType::kxxHash64;
      case 0x4:
        return ROCKSDB_NAMESPACE::ChecksumType::kXXH3;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ChecksumType::kXXH3;
    }
  }
};

// The portal class for org.rocksdb.IndexShorteningMode
class IndexShorteningModeJni {
 public:
  // Returns the equivalent org.rocksdb.IndexShorteningMode for the provided
  // C++ ROCKSDB_NAMESPACE::IndexShorteningMode enum
  static jbyte toJavaIndexShorteningMode(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode&
          index_shortening_mode) {
    switch (index_shortening_mode) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kNoShortening:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparators:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparatorsAndSuccessor:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::IndexShorteningMode enum for
  // the provided Java org.rocksdb.IndexShorteningMode
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode
  toCppIndexShorteningMode(jbyte jindex_shortening_mode) {
    switch (jindex_shortening_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kNoShortening;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparators;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparatorsAndSuccessor;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparators;
    }
  }
};

// The portal class for org.rocksdb.Priority
class PriorityJni {
 public:
  // Returns the equivalent org.rocksdb.Priority for the provided
  // C++ ROCKSDB_NAMESPACE::Env::Priority enum
  static jbyte toJavaPriority(
      const ROCKSDB_NAMESPACE::Env::Priority& priority) {
    switch (priority) {
      case ROCKSDB_NAMESPACE::Env::Priority::BOTTOM:
        return 0x0;
      case ROCKSDB_NAMESPACE::Env::Priority::LOW:
        return 0x1;
      case ROCKSDB_NAMESPACE::Env::Priority::HIGH:
        return 0x2;
      case ROCKSDB_NAMESPACE::Env::Priority::READ:
        return 0x3;
      case ROCKSDB_NAMESPACE::Env::Priority::TOTAL:
        return 0x4;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::env::Priority enum for the
  // provided Java org.rocksdb.Priority
  static ROCKSDB_NAMESPACE::Env::Priority toCppPriority(jbyte jpriority) {
    switch (jpriority) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Env::Priority::BOTTOM;
      case 0x1:
        return ROCKSDB_NAMESPACE::Env::Priority::LOW;
      case 0x2:
        return ROCKSDB_NAMESPACE::Env::Priority::HIGH;
      case 0x3:
        return ROCKSDB_NAMESPACE::Env::Priority::READ;
      case 0x4:
        return ROCKSDB_NAMESPACE::Env::Priority::TOTAL;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::Env::Priority::LOW;
    }
  }
};

// The portal class for org.rocksdb.ThreadType
class ThreadTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ThreadType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::ThreadType enum
  static jbyte toJavaThreadType(
      const ROCKSDB_NAMESPACE::ThreadStatus::ThreadType& thread_type) {
    switch (thread_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::USER:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::BOTTOM_PRIORITY:
        return 0x3;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::READ_PRIORITY:
        return 0x4;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::ThreadType enum
  // for the provided Java org.rocksdb.ThreadType
  static ROCKSDB_NAMESPACE::ThreadStatus::ThreadType toCppThreadType(
      jbyte jthread_type) {
    switch (jthread_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY;
      case 0x2:
        return ThreadStatus::ThreadType::USER;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::BOTTOM_PRIORITY;
      case 0x4:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::READ_PRIORITY;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY;
    }
  }
};

// The portal class for org.rocksdb.OperationType
class OperationTypeJni {
 public:
  // Returns the equivalent org.rocksdb.OperationType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationType enum
  static jbyte toJavaOperationType(
      const ROCKSDB_NAMESPACE::ThreadStatus::OperationType& operation_type) {
    switch (operation_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_COMPACTION:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_FLUSH:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_DBOPEN:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationType
  // enum for the provided Java org.rocksdb.OperationType
  static ROCKSDB_NAMESPACE::ThreadStatus::OperationType toCppOperationType(
      jbyte joperation_type) {
    switch (joperation_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_COMPACTION;
      case 0x2:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_FLUSH;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_DBOPEN;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN;
    }
  }
};

// The portal class for org.rocksdb.OperationStage
class OperationStageJni {
 public:
  // Returns the equivalent org.rocksdb.OperationStage for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationStage enum
  static jbyte toJavaOperationStage(
      const ROCKSDB_NAMESPACE::ThreadStatus::OperationStage& operation_stage) {
    switch (operation_stage) {
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_FLUSH_RUN:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_FLUSH_WRITE_L0:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_PREPARE:
        return 0x3;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_RUN:
        return 0x4;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_PROCESS_KV:
        return 0x5;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_INSTALL:
        return 0x6;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_SYNC_FILE:
        return 0x7;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_PICK_MEMTABLES_TO_FLUSH:
        return 0x8;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_MEMTABLE_ROLLBACK:
        return 0x9;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS:
        return 0xA;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationStage
  // enum for the provided Java org.rocksdb.OperationStage
  static ROCKSDB_NAMESPACE::ThreadStatus::OperationStage toCppOperationStage(
      jbyte joperation_stage) {
    switch (joperation_stage) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_FLUSH_RUN;
      case 0x2:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_FLUSH_WRITE_L0;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_PREPARE;
      case 0x4:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_RUN;
      case 0x5:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_PROCESS_KV;
      case 0x6:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_INSTALL;
      case 0x7:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_SYNC_FILE;
      case 0x8:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_PICK_MEMTABLES_TO_FLUSH;
      case 0x9:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_MEMTABLE_ROLLBACK;
      case 0xA:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN;
    }
  }
};

// The portal class for org.rocksdb.StateType
class StateTypeJni {
 public:
  // Returns the equivalent org.rocksdb.StateType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::StateType enum
  static jbyte toJavaStateType(
      const ROCKSDB_NAMESPACE::ThreadStatus::StateType& state_type) {
    switch (state_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_MUTEX_WAIT:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::StateType enum
  // for the provided Java org.rocksdb.StateType
  static ROCKSDB_NAMESPACE::ThreadStatus::StateType toCppStateType(
      jbyte jstate_type) {
    switch (jstate_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_MUTEX_WAIT;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN;
    }
  }
};

// The portal class for org.rocksdb.ThreadStatus
class ThreadStatusJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ThreadStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ThreadStatus");
  }

  /**
   * Create a new Java org.rocksdb.ThreadStatus object with the same
   * properties as the provided C++ ROCKSDB_NAMESPACE::ThreadStatus object
   *
   * @param env A pointer to the Java environment
   * @param thread_status A pointer to ROCKSDB_NAMESPACE::ThreadStatus object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ThreadStatus* thread_status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(JBLjava/lang/String;Ljava/lang/String;BJB[JB)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jdb_name =
        JniUtil::toJavaString(env, &(thread_status->db_name), true);
    if (env->ExceptionCheck()) {
      // an error occurred
      return nullptr;
    }

    jstring jcf_name =
        JniUtil::toJavaString(env, &(thread_status->cf_name), true);
    if (env->ExceptionCheck()) {
      // an error occurred
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }

    // long[]
    const jsize len = static_cast<jsize>(
        ROCKSDB_NAMESPACE::ThreadStatus::kNumOperationProperties);
    jlongArray joperation_properties = env->NewLongArray(len);
    if (joperation_properties == nullptr) {
      // an exception occurred
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jboolean is_copy;
    jlong* body = env->GetLongArrayElements(joperation_properties, &is_copy);
    if (body == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(joperation_properties);
      return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(thread_status->op_properties[i]);
    }
    env->ReleaseLongArrayElements(joperation_properties, body,
                                  is_copy == JNI_TRUE ? 0 : JNI_ABORT);

    jobject jcfd = env->NewObject(
        jclazz, mid, static_cast<jlong>(thread_status->thread_id),
        ThreadTypeJni::toJavaThreadType(thread_status->thread_type), jdb_name,
        jcf_name,
        OperationTypeJni::toJavaOperationType(thread_status->operation_type),
        static_cast<jlong>(thread_status->op_elapsed_micros),
        OperationStageJni::toJavaOperationStage(thread_status->operation_stage),
        joperation_properties,
        StateTypeJni::toJavaStateType(thread_status->state_type));
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(joperation_properties);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jdb_name);
    env->DeleteLocalRef(jcf_name);
    env->DeleteLocalRef(joperation_properties);

    return jcfd;
  }
};

// The portal class for org.rocksdb.CompactionStyle
class CompactionStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStyle for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionStyle enum
  static jbyte toJavaCompactionStyle(
      const ROCKSDB_NAMESPACE::CompactionStyle& compaction_style) {
    switch (compaction_style) {
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleUniversal:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleNone:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionStyle enum for the
  // provided Java org.rocksdb.CompactionStyle
  static ROCKSDB_NAMESPACE::CompactionStyle toCppCompactionStyle(
      jbyte jcompaction_style) {
    switch (jcompaction_style) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleUniversal;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleNone;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel;
    }
  }
};

// The portal class for org.rocksdb.CompactionReason
class CompactionReasonJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionReason for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionReason enum
  static jbyte toJavaCompactionReason(
      const ROCKSDB_NAMESPACE::CompactionReason& compaction_reason) {
    switch (compaction_reason) {
      case ROCKSDB_NAMESPACE::CompactionReason::kUnknown:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionReason::kLevelL0FilesNum:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionReason::kLevelMaxLevelSize:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeAmplification:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeRatio:
        return 0x4;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSortedRunNum:
        return 0x5;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOMaxSize:
        return 0x6;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOReduceNumFiles:
        return 0x7;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOTtl:
        return 0x8;
      case ROCKSDB_NAMESPACE::CompactionReason::kManualCompaction:
        return 0x9;
      case ROCKSDB_NAMESPACE::CompactionReason::kFilesMarkedForCompaction:
        return 0x10;
      case ROCKSDB_NAMESPACE::CompactionReason::kBottommostFiles:
        return 0x0A;
      case ROCKSDB_NAMESPACE::CompactionReason::kTtl:
        return 0x0B;
      case ROCKSDB_NAMESPACE::CompactionReason::kFlush:
        return 0x0C;
      case ROCKSDB_NAMESPACE::CompactionReason::kExternalSstIngestion:
        return 0x0D;
      case ROCKSDB_NAMESPACE::CompactionReason::kPeriodicCompaction:
        return 0x0E;
      case ROCKSDB_NAMESPACE::CompactionReason::kChangeTemperature:
        return 0x0F;
      case ROCKSDB_NAMESPACE::CompactionReason::kForcedBlobGC:
        return 0x11;
      case ROCKSDB_NAMESPACE::CompactionReason::kRoundRobinTtl:
        return 0x12;
      case ROCKSDB_NAMESPACE::CompactionReason::kRefitLevel:
        return 0x13;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionReason enum for the
  // provided Java org.rocksdb.CompactionReason
  static ROCKSDB_NAMESPACE::CompactionReason toCppCompactionReason(
      jbyte jcompaction_reason) {
    switch (jcompaction_reason) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionReason::kUnknown;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionReason::kLevelL0FilesNum;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionReason::kLevelMaxLevelSize;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeAmplification;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeRatio;
      case 0x5:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSortedRunNum;
      case 0x6:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOMaxSize;
      case 0x7:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOReduceNumFiles;
      case 0x8:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOTtl;
      case 0x9:
        return ROCKSDB_NAMESPACE::CompactionReason::kManualCompaction;
      case 0x10:
        return ROCKSDB_NAMESPACE::CompactionReason::kFilesMarkedForCompaction;
      case 0x0A:
        return ROCKSDB_NAMESPACE::CompactionReason::kBottommostFiles;
      case 0x0B:
        return ROCKSDB_NAMESPACE::CompactionReason::kTtl;
      case 0x0C:
        return ROCKSDB_NAMESPACE::CompactionReason::kFlush;
      case 0x0D:
        return ROCKSDB_NAMESPACE::CompactionReason::kExternalSstIngestion;
      case 0x0E:
        return ROCKSDB_NAMESPACE::CompactionReason::kPeriodicCompaction;
      case 0x0F:
        return ROCKSDB_NAMESPACE::CompactionReason::kChangeTemperature;
      case 0x11:
        return ROCKSDB_NAMESPACE::CompactionReason::kForcedBlobGC;
      case 0x12:
        return ROCKSDB_NAMESPACE::CompactionReason::kRoundRobinTtl;
      case 0x13:
        return ROCKSDB_NAMESPACE::CompactionReason::kRefitLevel;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionReason::kUnknown;
    }
  }
};

// The portal class for org.rocksdb.WalFileType
class WalFileTypeJni {
 public:
  // Returns the equivalent org.rocksdb.WalFileType for the provided
  // C++ ROCKSDB_NAMESPACE::WalFileType enum
  static jbyte toJavaWalFileType(
      const ROCKSDB_NAMESPACE::WalFileType& wal_file_type) {
    switch (wal_file_type) {
      case ROCKSDB_NAMESPACE::WalFileType::kArchivedLogFile:
        return 0x0;
      case ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::WalFileType enum for the
  // provided Java org.rocksdb.WalFileType
  static ROCKSDB_NAMESPACE::WalFileType toCppWalFileType(jbyte jwal_file_type) {
    switch (jwal_file_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WalFileType::kArchivedLogFile;
      case 0x1:
        return ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile;
    }
  }
};

class LogFileJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LogFile object.
   *
   * @param env A pointer to the Java environment
   * @param log_file A Cpp log file object
   *
   * @return A reference to a Java org.rocksdb.LogFile object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLogFile(JNIEnv* env,
                                ROCKSDB_NAMESPACE::LogFile* log_file) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;JBJJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    std::string path_name = log_file->PathName();
    jstring jpath_name =
        ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &path_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      return nullptr;
    }

    jobject jlog_file = env->NewObject(
        jclazz, mid, jpath_name, static_cast<jlong>(log_file->LogNumber()),
        ROCKSDB_NAMESPACE::WalFileTypeJni::toJavaWalFileType(log_file->Type()),
        static_cast<jlong>(log_file->StartSequence()),
        static_cast<jlong>(log_file->SizeFileBytes()));

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jpath_name);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jpath_name);

    return jlog_file;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LogFile");
  }
};

class LiveFileMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LiveFileMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param live_file_meta_data A Cpp live file meta data object
   *
   * @return A reference to a Java org.rocksdb.LiveFileMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLiveFileMetaData(
      JNIEnv* env, ROCKSDB_NAMESPACE::LiveFileMetaData* live_file_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>",
        "([BILjava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ[B)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jcolumn_family_name = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, live_file_meta_data->column_family_name);
    if (jcolumn_family_name == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    jstring jfile_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &live_file_meta_data->name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      return nullptr;
    }

    jstring jpath = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &live_file_meta_data->db_path, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      return nullptr;
    }

    jbyteArray jsmallest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, live_file_meta_data->smallestkey);
    if (jsmallest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      return nullptr;
    }

    jbyteArray jlargest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, live_file_meta_data->largestkey);
    if (jlargest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      return nullptr;
    }

    jbyteArray jfile_checksum = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, live_file_meta_data->file_checksum);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      return nullptr;
    }

    jobject jlive_file_meta_data = env->NewObject(
        jclazz, mid, jcolumn_family_name,
        static_cast<jint>(live_file_meta_data->level), jfile_name, jpath,
        static_cast<jlong>(live_file_meta_data->size),
        static_cast<jlong>(live_file_meta_data->smallest_seqno),
        static_cast<jlong>(live_file_meta_data->largest_seqno), jsmallest_key,
        jlargest_key,
        static_cast<jlong>(live_file_meta_data->num_reads_sampled),
        static_cast<jboolean>(live_file_meta_data->being_compacted),
        static_cast<jlong>(live_file_meta_data->num_entries),
        static_cast<jlong>(live_file_meta_data->num_deletions), jfile_checksum);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      env->DeleteLocalRef(jfile_checksum);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jcolumn_family_name);
    env->DeleteLocalRef(jfile_name);
    env->DeleteLocalRef(jpath);
    env->DeleteLocalRef(jsmallest_key);
    env->DeleteLocalRef(jlargest_key);
    env->DeleteLocalRef(jfile_checksum);

    return jlive_file_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LiveFileMetaData");
  }
};

class SstFileMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.SstFileMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param sst_file_meta_data A Cpp sst file meta data object
   *
   * @return A reference to a Java org.rocksdb.SstFileMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppSstFileMetaData(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::SstFileMetaData* sst_file_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ[B)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jfile_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &sst_file_meta_data->name, true);
    if (jfile_name == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    jstring jpath = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &sst_file_meta_data->db_path, true);
    if (jpath == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      return nullptr;
    }

    jbyteArray jsmallest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->smallestkey);
    if (jsmallest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      return nullptr;
    }

    jbyteArray jlargest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->largestkey);
    if (jlargest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      return nullptr;
    }

    jbyteArray jfile_checksum = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->file_checksum);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      return nullptr;
    }

    jobject jsst_file_meta_data = env->NewObject(
        jclazz, mid, jfile_name, jpath,
        static_cast<jlong>(sst_file_meta_data->size),
        static_cast<jint>(sst_file_meta_data->smallest_seqno),
        static_cast<jlong>(sst_file_meta_data->largest_seqno), jsmallest_key,
        jlargest_key, static_cast<jlong>(sst_file_meta_data->num_reads_sampled),
        static_cast<jboolean>(sst_file_meta_data->being_compacted),
        static_cast<jlong>(sst_file_meta_data->num_entries),
        static_cast<jlong>(sst_file_meta_data->num_deletions), jfile_checksum);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      env->DeleteLocalRef(jfile_checksum);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jfile_name);
    env->DeleteLocalRef(jpath);
    env->DeleteLocalRef(jsmallest_key);
    env->DeleteLocalRef(jlargest_key);
    env->DeleteLocalRef(jfile_checksum);

    return jsst_file_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/SstFileMetaData");
  }
};

class LevelMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LevelMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param level_meta_data A Cpp level meta data object
   *
   * @return A reference to a Java org.rocksdb.LevelMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLevelMetaData(
      JNIEnv* env, const ROCKSDB_NAMESPACE::LevelMetaData* level_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "(IJ[Lorg/rocksdb/SstFileMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(level_meta_data->files.size());
    jobjectArray jfiles =
        env->NewObjectArray(jlen, SstFileMetaDataJni::getJClass(env), nullptr);
    if (jfiles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jsize i = 0;
    for (auto it = level_meta_data->files.begin();
         it != level_meta_data->files.end(); ++it) {
      jobject jfile = SstFileMetaDataJni::fromCppSstFileMetaData(env, &(*it));
      if (jfile == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jfiles);
        return nullptr;
      }
      env->SetObjectArrayElement(jfiles, i++, jfile);
    }

    jobject jlevel_meta_data =
        env->NewObject(jclazz, mid, static_cast<jint>(level_meta_data->level),
                       static_cast<jlong>(level_meta_data->size), jfiles);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfiles);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jfiles);

    return jlevel_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LevelMetaData");
  }
};

class ColumnFamilyMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.ColumnFamilyMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param column_famly_meta_data A Cpp live file meta data object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppColumnFamilyMetaData(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::ColumnFamilyMetaData* column_famly_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "(JJ[B[Lorg/rocksdb/LevelMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jname = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, column_famly_meta_data->name);
    if (jname == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    const jsize jlen =
        static_cast<jsize>(column_famly_meta_data->levels.size());
    jobjectArray jlevels =
        env->NewObjectArray(jlen, LevelMetaDataJni::getJClass(env), nullptr);
    if (jlevels == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jname);
      return nullptr;
    }

    jsize i = 0;
    for (auto it = column_famly_meta_data->levels.begin();
         it != column_famly_meta_data->levels.end(); ++it) {
      jobject jlevel = LevelMetaDataJni::fromCppLevelMetaData(env, &(*it));
      if (jlevel == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jname);
        env->DeleteLocalRef(jlevels);
        return nullptr;
      }
      env->SetObjectArrayElement(jlevels, i++, jlevel);
    }

    jobject jcolumn_family_meta_data = env->NewObject(
        jclazz, mid, static_cast<jlong>(column_famly_meta_data->size),
        static_cast<jlong>(column_famly_meta_data->file_count), jname, jlevels);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jname);
      env->DeleteLocalRef(jlevels);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jname);
    env->DeleteLocalRef(jlevels);

    return jcolumn_family_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyMetaData");
  }
};

// The portal class for org.rocksdb.AbstractTraceWriter
class AbstractTraceWriterJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::TraceWriterJniCallback*,
          AbstractTraceWriterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractTraceWriter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/AbstractTraceWriter");
  }

  /**
   * Get the Java Method: AbstractTraceWriter#write
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getWriteProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "writeProxy", "(J)S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#closeWriter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCloseWriterProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "closeWriterProxy", "()S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#getFileSize
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getGetFileSizeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getFileSize", "()J");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractWalFilter
class AbstractWalFilterJni
    : public RocksDBNativeClass<const ROCKSDB_NAMESPACE::WalFilterJniCallback*,
                                AbstractWalFilterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractWalFilter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractWalFilter");
  }

  /**
   * Get the Java Method: AbstractWalFilter#columnFamilyLogNumberMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getColumnFamilyLogNumberMapMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "columnFamilyLogNumberMap",
                         "(Ljava/util/Map;Ljava/util/Map;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#logRecordFoundProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getLogRecordFoundProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "logRecordFoundProxy",
                                            "(JLjava/lang/String;JJ)S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.WalProcessingOption
class WalProcessingOptionJni {
 public:
  // Returns the equivalent org.rocksdb.WalProcessingOption for the provided
  // C++ ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption enum
  static jbyte toJavaWalProcessingOption(
      const ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption&
          wal_processing_option) {
    switch (wal_processing_option) {
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
          kContinueProcessing:
        return 0x0;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
          kIgnoreCurrentRecord:
        return 0x1;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kStopReplay:
        return 0x2;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kCorruptedRecord:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++
  // ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption enum for the provided
  // Java org.rocksdb.WalProcessingOption
  static ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption
  toCppWalProcessingOption(jbyte jwal_processing_option) {
    switch (jwal_processing_option) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kContinueProcessing;
      case 0x1:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kIgnoreCurrentRecord;
      case 0x2:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kStopReplay;
      case 0x3:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kCorruptedRecord;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kCorruptedRecord;
    }
  }
};

// The portal class for org.rocksdb.ReusedSynchronisationType
class ReusedSynchronisationTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ReusedSynchronisationType for the
  // provided C++ ROCKSDB_NAMESPACE::ReusedSynchronisationType enum
  static jbyte toJavaReusedSynchronisationType(
      const ROCKSDB_NAMESPACE::ReusedSynchronisationType&
          reused_synchronisation_type) {
    switch (reused_synchronisation_type) {
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::MUTEX:
        return 0x0;
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX:
        return 0x1;
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::THREAD_LOCAL:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ReusedSynchronisationType
  // enum for the provided Java org.rocksdb.ReusedSynchronisationType
  static ROCKSDB_NAMESPACE::ReusedSynchronisationType
  toCppReusedSynchronisationType(jbyte reused_synchronisation_type) {
    switch (reused_synchronisation_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::MUTEX;
      case 0x1:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX;
      case 0x2:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::THREAD_LOCAL;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX;
    }
  }
};
// The portal class for org.rocksdb.SanityLevel
class SanityLevelJni {
 public:
  // Returns the equivalent org.rocksdb.SanityLevel for the provided
  // C++ ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel enum
  static jbyte toJavaSanityLevel(
      const ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel& sanity_level) {
    switch (sanity_level) {
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::kSanityLevelNone:
        return 0x0;
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::
          kSanityLevelLooselyCompatible:
        return 0x1;
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::
          kSanityLevelExactMatch:
        return -0x01;
      default:
        return -0x01;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel
  // enum for the provided Java org.rocksdb.SanityLevel
  static ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel toCppSanityLevel(
      jbyte sanity_level) {
    switch (sanity_level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelNone;
      case 0x1:
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelLooselyCompatible;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelExactMatch;
    }
  }
};

// The portal class for org.rocksdb.PrepopulateBlobCache
class PrepopulateBlobCacheJni {
 public:
  // Returns the equivalent org.rocksdb.PrepopulateBlobCache for the provided
  // C++ ROCKSDB_NAMESPACE::PrepopulateBlobCache enum
  static jbyte toJavaPrepopulateBlobCache(
      ROCKSDB_NAMESPACE::PrepopulateBlobCache prepopulate_blob_cache) {
    switch (prepopulate_blob_cache) {
      case ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable:
        return 0x0;
      case ROCKSDB_NAMESPACE::PrepopulateBlobCache::kFlushOnly:
        return 0x1;
      default:
        return 0x7f;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::PrepopulateBlobCache enum for
  // the provided Java org.rocksdb.PrepopulateBlobCache
  static ROCKSDB_NAMESPACE::PrepopulateBlobCache toCppPrepopulateBlobCache(
      jbyte jprepopulate_blob_cache) {
    switch (jprepopulate_blob_cache) {
      case 0x0:
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable;
      case 0x1:
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kFlushOnly;
      case 0x7F:
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable;
    }
  }
};

// The portal class for org.rocksdb.AbstractListener.EnabledEventCallback
class EnabledEventCallbackJni {
 public:
  // Returns the set of equivalent C++
  // ROCKSDB_NAMESPACE::EnabledEventCallbackJni::EnabledEventCallback enums for
  // the provided Java jenabled_event_callback_values
  static std::set<EnabledEventCallback> toCppEnabledEventCallbacks(
      jlong jenabled_event_callback_values) {
    std::set<EnabledEventCallback> enabled_event_callbacks;
    for (size_t i = 0; i < EnabledEventCallback::NUM_ENABLED_EVENT_CALLBACK;
         ++i) {
      if (((1ULL << i) & jenabled_event_callback_values) > 0) {
        enabled_event_callbacks.emplace(static_cast<EnabledEventCallback>(i));
      }
    }
    return enabled_event_callbacks;
  }
};

// The portal class for org.rocksdb.AbstractEventListener
class AbstractEventListenerJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::EventListenerJniCallback*,
          AbstractEventListenerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractEventListener
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/AbstractEventListener");
  }

  /**
   * Get the Java Method: AbstractEventListener#onFlushCompletedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFlushCompletedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onFlushCompletedProxy",
                                            "(JLorg/rocksdb/FlushJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFlushBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFlushBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onFlushBeginProxy",
                                            "(JLorg/rocksdb/FlushJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileDeleted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileDeletedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onTableFileDeleted", "(Lorg/rocksdb/TableFileDeletionInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onCompactionBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnCompactionBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onCompactionBeginProxy",
                         "(JLorg/rocksdb/CompactionJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onCompactionCompletedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnCompactionCompletedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onCompactionCompletedProxy",
                         "(JLorg/rocksdb/CompactionJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileCreated
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileCreatedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onTableFileCreated", "(Lorg/rocksdb/TableFileCreationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileCreationStarted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileCreationStartedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onTableFileCreationStarted",
                         "(Lorg/rocksdb/TableFileCreationBriefInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onMemTableSealed
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnMemTableSealedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onMemTableSealed",
                                            "(Lorg/rocksdb/MemTableInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method:
   * AbstractEventListener#onColumnFamilyHandleDeletionStarted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnColumnFamilyHandleDeletionStartedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onColumnFamilyHandleDeletionStarted",
                         "(Lorg/rocksdb/ColumnFamilyHandle;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onExternalFileIngestedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnExternalFileIngestedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onExternalFileIngestedProxy",
                         "(JLorg/rocksdb/ExternalFileIngestionInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onBackgroundError
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnBackgroundErrorProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onBackgroundErrorProxy",
                                            "(BLorg/rocksdb/Status;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onStallConditionsChanged
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnStallConditionsChangedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onStallConditionsChanged",
                                            "(Lorg/rocksdb/WriteStallInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileReadFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileReadFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileReadFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileWriteFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileWriteFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileWriteFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileFlushFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileFlushFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileFlushFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileSyncFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileSyncFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileSyncFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileRangeSyncFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileRangeSyncFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileRangeSyncFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileTruncateFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileTruncateFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileTruncateFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileCloseFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileCloseFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileCloseFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#shouldBeNotifiedOnFileIO
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getShouldBeNotifiedOnFileIOMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "shouldBeNotifiedOnFileIO", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onErrorRecoveryBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnErrorRecoveryBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onErrorRecoveryBeginProxy",
                                            "(BLorg/rocksdb/Status;)Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onErrorRecoveryCompleted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnErrorRecoveryCompletedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onErrorRecoveryCompleted",
                                            "(Lorg/rocksdb/Status;)V");
    assert(mid != nullptr);
    return mid;
  }
};

class FlushJobInfoJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.FlushJobInfo object.
   *
   * @param env A pointer to the Java environment
   * @param flush_job_info A Cpp flush job info object
   *
   * @return A reference to a Java org.rocksdb.FlushJobInfo object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppFlushJobInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::FlushJobInfo* flush_job_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &flush_job_info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jfile_path = JniUtil::toJavaString(env, &flush_job_info->file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfile_path);
      return nullptr;
    }
    jobject jtable_properties = TablePropertiesJni::fromCppTableProperties(
        env, flush_job_info->table_properties);
    if (jtable_properties == nullptr) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jfile_path);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, static_cast<jlong>(flush_job_info->cf_id), jcf_name,
        jfile_path, static_cast<jlong>(flush_job_info->thread_id),
        static_cast<jint>(flush_job_info->job_id),
        static_cast<jboolean>(flush_job_info->triggered_writes_slowdown),
        static_cast<jboolean>(flush_job_info->triggered_writes_stop),
        static_cast<jlong>(flush_job_info->smallest_seqno),
        static_cast<jlong>(flush_job_info->largest_seqno), jtable_properties,
        static_cast<jbyte>(flush_job_info->flush_reason));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/FlushJobInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(JLjava/lang/String;Ljava/lang/String;JIZZJJLorg/"
                            "rocksdb/TableProperties;B)V");
  }
};

class TableFileDeletionInfoJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.TableFileDeletionInfo object.
   *
   * @param env A pointer to the Java environment
   * @param file_del_info A Cpp table file deletion info object
   *
   * @return A reference to a Java org.rocksdb.TableFileDeletionInfo object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppTableFileDeletionInfo(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::TableFileDeletionInfo* file_del_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jdb_name = JniUtil::toJavaString(env, &file_del_info->db_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jobject jstatus = StatusJni::construct(env, file_del_info->status);
    if (jstatus == nullptr) {
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jdb_name,
                          JniUtil::toJavaString(env, &file_del_info->file_path),
                          static_cast<jint>(file_del_info->job_id), jstatus);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFileDeletionInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(
        clazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;ILorg/rocksdb/Status;)V");
  }
};

class CompactionJobInfoJni : public JavaClass {
 public:
  static jobject fromCppCompactionJobInfo(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::CompactionJobInfo* compaction_job_info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    return env->NewObject(jclazz, ctor,
                          GET_CPLUSPLUS_POINTER(compaction_job_info));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/CompactionJobInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(J)V");
  }
};

class TableFileCreationInfoJni : public JavaClass {
 public:
  static jobject fromCppTableFileCreationInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::TableFileCreationInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jdb_name = JniUtil::toJavaString(env, &info->db_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }
    jstring jfile_path = JniUtil::toJavaString(env, &info->file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jobject jtable_properties =
        TablePropertiesJni::fromCppTableProperties(env, info->table_properties);
    if (jtable_properties == nullptr) {
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jobject jstatus = StatusJni::construct(env, info->status);
    if (jstatus == nullptr) {
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jtable_properties);
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, static_cast<jlong>(info->file_size),
                          jtable_properties, jstatus, jdb_name, jcf_name,
                          jfile_path, static_cast<jint>(info->job_id),
                          static_cast<jbyte>(info->reason));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFileCreationInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(
        clazz, "<init>",
        "(JLorg/rocksdb/TableProperties;Lorg/rocksdb/Status;Ljava/lang/"
        "String;Ljava/lang/String;Ljava/lang/String;IB)V");
  }
};

class TableFileCreationBriefInfoJni : public JavaClass {
 public:
  static jobject fromCppTableFileCreationBriefInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::TableFileCreationBriefInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jdb_name = JniUtil::toJavaString(env, &info->db_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }
    jstring jfile_path = JniUtil::toJavaString(env, &info->file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jdb_name, jcf_name, jfile_path,
                          static_cast<jint>(info->job_id),
                          static_cast<jbyte>(info->reason));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFileCreationBriefInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(
        clazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IB)V");
  }
};

class MemTableInfoJni : public JavaClass {
 public:
  static jobject fromCppMemTableInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::MemTableInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jcf_name,
                          static_cast<jlong>(info->first_seqno),
                          static_cast<jlong>(info->earliest_seqno),
                          static_cast<jlong>(info->num_entries),
                          static_cast<jlong>(info->num_deletes));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/MemTableInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;JJJJ)V");
  }
};

class ExternalFileIngestionInfoJni : public JavaClass {
 public:
  static jobject fromCppExternalFileIngestionInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ExternalFileIngestionInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jexternal_file_path =
        JniUtil::toJavaString(env, &info->external_file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jstring jinternal_file_path =
        JniUtil::toJavaString(env, &info->internal_file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jexternal_file_path);
      return nullptr;
    }
    jobject jtable_properties =
        TablePropertiesJni::fromCppTableProperties(env, info->table_properties);
    if (jtable_properties == nullptr) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jexternal_file_path);
      env->DeleteLocalRef(jinternal_file_path);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, jcf_name, jexternal_file_path, jinternal_file_path,
        static_cast<jlong>(info->global_seqno), jtable_properties);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ExternalFileIngestionInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/"
                            "String;JLorg/rocksdb/TableProperties;)V");
  }
};

class WriteStallInfoJni : public JavaClass {
 public:
  static jobject fromCppWriteStallInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::WriteStallInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jcf_name,
                          static_cast<jbyte>(info->condition.cur),
                          static_cast<jbyte>(info->condition.prev));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WriteStallInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;BB)V");
  }
};

class FileOperationInfoJni : public JavaClass {
 public:
  static jobject fromCppFileOperationInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::FileOperationInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jpath = JniUtil::toJavaString(env, &info->path);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jobject jstatus = StatusJni::construct(env, info->status);
    if (jstatus == nullptr) {
      env->DeleteLocalRef(jpath);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, jpath, static_cast<jlong>(info->offset),
        static_cast<jlong>(info->length),
        static_cast<jlong>(info->start_ts.time_since_epoch().count()),
        static_cast<jlong>(info->duration.count()), jstatus);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/FileOperationInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(Ljava/lang/String;JJJJLorg/rocksdb/Status;)V");
  }
};

class CompactRangeOptionsTimestampJni : public JavaClass {
 public:
  static jobject fromCppTimestamp(JNIEnv* env, const uint64_t start,
                                  const uint64_t range) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    return env->NewObject(jclazz, ctor, static_cast<jlong>(start),
                          static_cast<jlong>(range));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
                                "org/rocksdb/CompactRangeOptions$Timestamp");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(JJ)V");
  }
};

// The portal class for org.rocksdb.BlockBasedTableOptions
class BlockBasedTableOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::BlockBasedTableOptions*,
                                BlockBasedTableOptions> {
 public:
  /**
   * Get the Java Class org.rocksdb.BlockBasedTableConfig
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/BlockBasedTableConfig");
  }

  /**
   * Create a new Java org.rocksdb.BlockBasedTableConfig object with the
   * properties as the provided C++ ROCKSDB_NAMESPACE::BlockBasedTableOptions
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to ROCKSDB_NAMESPACE::ColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const BlockBasedTableOptions* table_factory_options) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID method_id_init = env->GetMethodID(
        jclazz, "<init>", "(ZZZZBBDBZJIIIJZZZZZIIZDZZJJBBBBJD)V");
    if (method_id_init == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    FilterPolicyTypeJni filter_policy_type =
        FilterPolicyTypeJni::kUnknownFilterPolicy;
    jlong filter_policy_handle = 0L;
    jdouble filter_policy_config_value = 0.0;
    if (table_factory_options->filter_policy) {
      auto filter_policy = table_factory_options->filter_policy.get();
      filter_policy_type = FilterPolicyJni::getFilterPolicyType(
          filter_policy->CompatibilityName());
      if (FilterPolicyTypeJni::kUnknownFilterPolicy != filter_policy_type) {
        filter_policy_handle = GET_CPLUSPLUS_POINTER(filter_policy);
      }
    }

    jobject jcfd = env->NewObject(
        jclazz, method_id_init,
        table_factory_options->cache_index_and_filter_blocks,
        table_factory_options->cache_index_and_filter_blocks_with_high_priority,
        table_factory_options->pin_l0_filter_and_index_blocks_in_cache,
        table_factory_options->pin_top_level_index_and_filter,
        IndexTypeJni::toJavaIndexType(table_factory_options->index_type),
        DataBlockIndexTypeJni::toJavaDataBlockIndexType(
            table_factory_options->data_block_index_type),
        table_factory_options->data_block_hash_table_util_ratio,
        ChecksumTypeJni::toJavaChecksumType(table_factory_options->checksum),
        table_factory_options->no_block_cache,
        static_cast<jlong>(table_factory_options->block_size),
        table_factory_options->block_size_deviation,
        table_factory_options->block_restart_interval,
        table_factory_options->index_block_restart_interval,
        static_cast<jlong>(table_factory_options->metadata_block_size),
        table_factory_options->partition_filters,
        table_factory_options->optimize_filters_for_memory,
        table_factory_options->use_delta_encoding,
        table_factory_options->whole_key_filtering,
        table_factory_options->verify_compression,
        table_factory_options->read_amp_bytes_per_bit,
        table_factory_options->format_version,
        table_factory_options->separate_key_value_in_data_block,
        table_factory_options->uniform_cv_threshold,
        table_factory_options->enable_index_compression,
        table_factory_options->block_align,
        static_cast<jlong>(table_factory_options->super_block_alignment_size),
        static_cast<jlong>(
            table_factory_options->super_block_alignment_space_overhead_ratio),
        IndexShorteningModeJni::toJavaIndexShorteningMode(
            table_factory_options->index_shortening),
        IndexSearchTypeJni::toJavaIndexSearchType(
            table_factory_options->index_block_search_type),
        OptimizeKeyCommonPrefixJni::toJavaOptimizeKeyCommonPrefix(
            table_factory_options->optimize_key_common_prefix),
        FilterPolicyJni::toJavaIndexType(filter_policy_type),
        filter_policy_handle, filter_policy_config_value);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // JAVA_ROCKSJNI_PORTAL_H_
