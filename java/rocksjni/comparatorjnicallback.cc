// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Comparator.

#include "rocksjni/comparatorjnicallback.h"

#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {
ComparatorJniCallback::ComparatorJniCallback(
    JNIEnv* env, jobject jcomparator,
    const ComparatorJniCallbackOptions* options)
    : JniCallback(env, jcomparator),
      m_options(std::make_unique<ComparatorJniCallbackOptions>(*options)) {
  // cache the AbstractComparatorJniBridge class as we will reuse it many times
  // for each callback
  m_abstract_comparator_jni_bridge_clazz = static_cast<jclass>(
      env->NewGlobalRef(AbstractComparatorJniBridge::getJClass(env)));

  // Note: The name of a Comparator will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jname_mid = AbstractComparatorJni::getNameMethodId(env);
  if (jname_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
  jstring js_name = (jstring)env->CallObjectMethod(m_jcallback_obj, jname_mid);
  if (env->ExceptionCheck()) {
    // exception thrown
    return;
  }
  jboolean has_exception = JNI_FALSE;
  m_name = JniUtil::copyString(env, js_name,
                               &has_exception);  // also releases jsName
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  // cache the ByteBuffer class as we will reuse it many times for each callback
  m_jbytebuffer_clazz =
      static_cast<jclass>(env->NewGlobalRef(ByteBufferJni::getJClass(env)));

  m_jcompare_mid = AbstractComparatorJniBridge::getCompareInternalMethodId(
      env, m_abstract_comparator_jni_bridge_clazz);
  if (m_jcompare_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jshortest_mid =
      AbstractComparatorJniBridge::getFindShortestSeparatorInternalMethodId(
          env, m_abstract_comparator_jni_bridge_clazz);
  if (m_jshortest_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jshort_mid =
      AbstractComparatorJniBridge::getFindShortSuccessorInternalMethodId(
          env, m_abstract_comparator_jni_bridge_clazz);
  if (m_jshort_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  // do we need reusable buffers?
  if (m_options->max_reused_buffer_size > -1) {
    if (m_options->reused_synchronisation_type ==
        ReusedSynchronisationType::THREAD_LOCAL) {
      // buffers reused per thread
      UnrefHandler unref = [](void* ptr) {
        ThreadLocalBuf* tlb = reinterpret_cast<ThreadLocalBuf*>(ptr);
        jboolean attached_thread = JNI_FALSE;
        JNIEnv* _env = JniUtil::getJniEnv(tlb->jvm, &attached_thread);
        if (_env != nullptr) {
          if (tlb->direct_buffer) {
            void* buf = _env->GetDirectBufferAddress(tlb->jbuf);
            delete[] static_cast<char*>(buf);
          }
          _env->DeleteGlobalRef(tlb->jbuf);
          JniUtil::releaseJniEnv(tlb->jvm, attached_thread);
        }
        delete tlb;
      };

      m_tl_buf_a = new ThreadLocalPtr(unref);
      m_tl_buf_b = new ThreadLocalPtr(unref);

      m_jcompare_buf_a = nullptr;
      m_jcompare_buf_b = nullptr;
      m_jshortest_buf_start = nullptr;
      m_jshortest_buf_limit = nullptr;
      m_jshort_buf_key = nullptr;

    } else {
      // buffers reused and shared across threads
      const bool adaptive = m_options->reused_synchronisation_type ==
                            ReusedSynchronisationType::ADAPTIVE_MUTEX;
      mtx_compare = std::unique_ptr<port::Mutex>(new port::Mutex(adaptive));
      mtx_shortest = std::unique_ptr<port::Mutex>(new port::Mutex(adaptive));
      mtx_short = std::unique_ptr<port::Mutex>(new port::Mutex(adaptive));

      m_jcompare_buf_a = env->NewGlobalRef(ByteBufferJni::construct(
          env, m_options->direct_buffer, m_options->max_reused_buffer_size,
          m_jbytebuffer_clazz));
      if (m_jcompare_buf_a == nullptr) {
        // exception thrown: OutOfMemoryError
        return;
      }

      m_jcompare_buf_b = env->NewGlobalRef(ByteBufferJni::construct(
          env, m_options->direct_buffer, m_options->max_reused_buffer_size,
          m_jbytebuffer_clazz));
      if (m_jcompare_buf_b == nullptr) {
        // exception thrown: OutOfMemoryError
        return;
      }

      m_jshortest_buf_start = env->NewGlobalRef(ByteBufferJni::construct(
          env, m_options->direct_buffer, m_options->max_reused_buffer_size,
          m_jbytebuffer_clazz));
      if (m_jshortest_buf_start == nullptr) {
        // exception thrown: OutOfMemoryError
        return;
      }

      m_jshortest_buf_limit = env->NewGlobalRef(ByteBufferJni::construct(
          env, m_options->direct_buffer, m_options->max_reused_buffer_size,
          m_jbytebuffer_clazz));
      if (m_jshortest_buf_limit == nullptr) {
        // exception thrown: OutOfMemoryError
        return;
      }

      m_jshort_buf_key = env->NewGlobalRef(ByteBufferJni::construct(
          env, m_options->direct_buffer, m_options->max_reused_buffer_size,
          m_jbytebuffer_clazz));
      if (m_jshort_buf_key == nullptr) {
        // exception thrown: OutOfMemoryError
        return;
      }

      m_tl_buf_a = nullptr;
      m_tl_buf_b = nullptr;
    }

  } else {
    m_jcompare_buf_a = nullptr;
    m_jcompare_buf_b = nullptr;
    m_jshortest_buf_start = nullptr;
    m_jshortest_buf_limit = nullptr;
    m_jshort_buf_key = nullptr;

    m_tl_buf_a = nullptr;
    m_tl_buf_b = nullptr;
  }
}

ComparatorJniCallback::~ComparatorJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  env->DeleteGlobalRef(m_abstract_comparator_jni_bridge_clazz);

  env->DeleteGlobalRef(m_jbytebuffer_clazz);

  if (m_jcompare_buf_a != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jcompare_buf_a);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jcompare_buf_a);
  }

  if (m_jcompare_buf_b != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jcompare_buf_b);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jcompare_buf_b);
  }

  if (m_jshortest_buf_start != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshortest_buf_start);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshortest_buf_start);
  }

  if (m_jshortest_buf_limit != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshortest_buf_limit);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshortest_buf_limit);
  }

  if (m_jshort_buf_key != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshort_buf_key);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshort_buf_key);
  }

  if (m_tl_buf_a != nullptr) {
    delete m_tl_buf_a;
  }

  if (m_tl_buf_b != nullptr) {
    delete m_tl_buf_b;
  }

  releaseJniEnv(attached_thread);
}

const char* ComparatorJniCallback::Name() const { return m_name.get(); }

int ComparatorJniCallback::Compare(const Slice& a, const Slice& b) const {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_a =
      static_cast<int64_t>(a.size()) <= m_options->max_reused_buffer_size;
  const bool reuse_jbuf_b =
      static_cast<int64_t>(b.size()) <= m_options->max_reused_buffer_size;

  MaybeLockForReuse(mtx_compare, reuse_jbuf_a || reuse_jbuf_b);

  jobject jcompare_buf_a =
      GetBuffer(env, a, reuse_jbuf_a, m_tl_buf_a, m_jcompare_buf_a);
  if (jcompare_buf_a == nullptr) {
    // exception occurred
    MaybeUnlockForReuse(mtx_compare, reuse_jbuf_a || reuse_jbuf_b);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return 0;
  }

  jobject jcompare_buf_b =
      GetBuffer(env, b, reuse_jbuf_b, m_tl_buf_b, m_jcompare_buf_b);
  if (jcompare_buf_b == nullptr) {
    // exception occurred
    if (!reuse_jbuf_a) {
      DeleteBuffer(env, jcompare_buf_a);
    }
    MaybeUnlockForReuse(mtx_compare, reuse_jbuf_a || reuse_jbuf_b);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return 0;
  }

  jint result = env->CallStaticIntMethod(
      m_abstract_comparator_jni_bridge_clazz, m_jcompare_mid, m_jcallback_obj,
      jcompare_buf_a, reuse_jbuf_a ? a.size() : -1, jcompare_buf_b,
      reuse_jbuf_b ? b.size() : -1);

  if (env->ExceptionCheck()) {
    // exception thrown from CallIntMethod
    env->ExceptionDescribe();  // print out exception to stderr
    result = 0;  // we could not get a result from java callback so use 0
  }

  if (!reuse_jbuf_a) {
    DeleteBuffer(env, jcompare_buf_a);
  }
  if (!reuse_jbuf_b) {
    DeleteBuffer(env, jcompare_buf_b);
  }

  MaybeUnlockForReuse(mtx_compare, reuse_jbuf_a || reuse_jbuf_b);

  releaseJniEnv(attached_thread);

  return result;
}

void ComparatorJniCallback::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  if (start == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_start = static_cast<int64_t>(start->length()) <=
                                m_options->max_reused_buffer_size;
  const bool reuse_jbuf_limit =
      static_cast<int64_t>(limit.size()) <= m_options->max_reused_buffer_size;

  MaybeLockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);

  Slice sstart(start->data(), start->length());
  jobject j_start_buf = GetBuffer(env, sstart, reuse_jbuf_start, m_tl_buf_a,
                                  m_jshortest_buf_start);
  if (j_start_buf == nullptr) {
    // exception occurred
    MaybeUnlockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jobject j_limit_buf = GetBuffer(env, limit, reuse_jbuf_limit, m_tl_buf_b,
                                  m_jshortest_buf_limit);
  if (j_limit_buf == nullptr) {
    // exception occurred
    if (!reuse_jbuf_start) {
      DeleteBuffer(env, j_start_buf);
    }
    MaybeUnlockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jint jstart_len = env->CallStaticIntMethod(
      m_abstract_comparator_jni_bridge_clazz, m_jshortest_mid, m_jcallback_obj,
      j_start_buf, reuse_jbuf_start ? start->length() : -1, j_limit_buf,
      reuse_jbuf_limit ? limit.size() : -1);

  if (env->ExceptionCheck()) {
    // exception thrown from CallIntMethod
    env->ExceptionDescribe();  // print out exception to stderr

  } else if (static_cast<size_t>(jstart_len) != start->length()) {
    // start buffer has changed in Java, so update `start` with the result
    bool copy_from_non_direct = false;
    if (reuse_jbuf_start) {
      // reused a buffer
      if (m_options->direct_buffer) {
        // reused direct buffer
        void* start_buf = env->GetDirectBufferAddress(j_start_buf);
        if (start_buf == nullptr) {
          if (!reuse_jbuf_start) {
            DeleteBuffer(env, j_start_buf);
          }
          if (!reuse_jbuf_limit) {
            DeleteBuffer(env, j_limit_buf);
          }
          MaybeUnlockForReuse(mtx_shortest,
                              reuse_jbuf_start || reuse_jbuf_limit);
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
              env, "Unable to get Direct Buffer Address");
          env->ExceptionDescribe();  // print out exception to stderr
          releaseJniEnv(attached_thread);
          return;
        }
        start->assign(static_cast<const char*>(start_buf), jstart_len);

      } else {
        // reused non-direct buffer
        copy_from_non_direct = true;
      }
    } else {
      // there was a new buffer
      if (m_options->direct_buffer) {
        // it was direct... don't forget to potentially truncate the `start`
        // string
        start->resize(jstart_len);
      } else {
        // it was non-direct
        copy_from_non_direct = true;
      }
    }

    if (copy_from_non_direct) {
      jbyteArray jarray =
          ByteBufferJni::array(env, j_start_buf, m_jbytebuffer_clazz);
      if (jarray == nullptr) {
        if (!reuse_jbuf_start) {
          DeleteBuffer(env, j_start_buf);
        }
        if (!reuse_jbuf_limit) {
          DeleteBuffer(env, j_limit_buf);
        }
        MaybeUnlockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);
        env->ExceptionDescribe();  // print out exception to stderr
        releaseJniEnv(attached_thread);
        return;
      }
      jboolean has_exception = JNI_FALSE;
      JniUtil::byteString<std::string>(
          env, jarray,
          [start, jstart_len](const char* data, const size_t) {
            return start->assign(data, static_cast<size_t>(jstart_len));
          },
          &has_exception);
      env->DeleteLocalRef(jarray);
      if (has_exception == JNI_TRUE) {
        if (!reuse_jbuf_start) {
          DeleteBuffer(env, j_start_buf);
        }
        if (!reuse_jbuf_limit) {
          DeleteBuffer(env, j_limit_buf);
        }
        env->ExceptionDescribe();  // print out exception to stderr
        MaybeUnlockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);
        releaseJniEnv(attached_thread);
        return;
      }
    }
  }

  if (!reuse_jbuf_start) {
    DeleteBuffer(env, j_start_buf);
  }
  if (!reuse_jbuf_limit) {
    DeleteBuffer(env, j_limit_buf);
  }

  MaybeUnlockForReuse(mtx_shortest, reuse_jbuf_start || reuse_jbuf_limit);

  releaseJniEnv(attached_thread);
}

void ComparatorJniCallback::FindShortSuccessor(std::string* key) const {
  if (key == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_key =
      static_cast<int64_t>(key->length()) <= m_options->max_reused_buffer_size;

  MaybeLockForReuse(mtx_short, reuse_jbuf_key);

  Slice skey(key->data(), key->length());
  jobject j_key_buf =
      GetBuffer(env, skey, reuse_jbuf_key, m_tl_buf_a, m_jshort_buf_key);
  if (j_key_buf == nullptr) {
    // exception occurred
    MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jint jkey_len = env->CallStaticIntMethod(
      m_abstract_comparator_jni_bridge_clazz, m_jshort_mid, m_jcallback_obj,
      j_key_buf, reuse_jbuf_key ? key->length() : -1);

  if (env->ExceptionCheck()) {
    // exception thrown from CallObjectMethod
    if (!reuse_jbuf_key) {
      DeleteBuffer(env, j_key_buf);
    }
    MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  if (static_cast<size_t>(jkey_len) != key->length()) {
    // key buffer has changed in Java, so update `key` with the result
    bool copy_from_non_direct = false;
    if (reuse_jbuf_key) {
      // reused a buffer
      if (m_options->direct_buffer) {
        // reused direct buffer
        void* key_buf = env->GetDirectBufferAddress(j_key_buf);
        if (key_buf == nullptr) {
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
              env, "Unable to get Direct Buffer Address");
          if (!reuse_jbuf_key) {
            DeleteBuffer(env, j_key_buf);
          }
          MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);
          env->ExceptionDescribe();  // print out exception to stderr
          releaseJniEnv(attached_thread);
          return;
        }
        key->assign(static_cast<const char*>(key_buf), jkey_len);
      } else {
        // reused non-direct buffer
        copy_from_non_direct = true;
      }
    } else {
      // there was a new buffer
      if (m_options->direct_buffer) {
        // it was direct... don't forget to potentially truncate the `key`
        // string
        key->resize(jkey_len);
      } else {
        // it was non-direct
        copy_from_non_direct = true;
      }
    }

    if (copy_from_non_direct) {
      jbyteArray jarray =
          ByteBufferJni::array(env, j_key_buf, m_jbytebuffer_clazz);
      if (jarray == nullptr) {
        if (!reuse_jbuf_key) {
          DeleteBuffer(env, j_key_buf);
        }
        MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);
        env->ExceptionDescribe();  // print out exception to stderr
        releaseJniEnv(attached_thread);
        return;
      }
      jboolean has_exception = JNI_FALSE;
      JniUtil::byteString<std::string>(
          env, jarray,
          [key, jkey_len](const char* data, const size_t) {
            return key->assign(data, static_cast<size_t>(jkey_len));
          },
          &has_exception);
      env->DeleteLocalRef(jarray);
      if (has_exception == JNI_TRUE) {
        if (!reuse_jbuf_key) {
          DeleteBuffer(env, j_key_buf);
        }
        MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);
        env->ExceptionDescribe();  // print out exception to stderr
        releaseJniEnv(attached_thread);
        return;
      }
    }
  }

  if (!reuse_jbuf_key) {
    DeleteBuffer(env, j_key_buf);
  }

  MaybeUnlockForReuse(mtx_short, reuse_jbuf_key);

  releaseJniEnv(attached_thread);
}

inline void ComparatorJniCallback::MaybeLockForReuse(
    const std::unique_ptr<port::Mutex>& mutex, const bool cond) const {
  // no need to lock if using thread_local
  if (m_options->reused_synchronisation_type !=
          ReusedSynchronisationType::THREAD_LOCAL &&
      cond) {
    mutex.get()->Lock();
  }
}

inline void ComparatorJniCallback::MaybeUnlockForReuse(
    const std::unique_ptr<port::Mutex>& mutex, const bool cond) const {
  // no need to unlock if using thread_local
  if (m_options->reused_synchronisation_type !=
          ReusedSynchronisationType::THREAD_LOCAL &&
      cond) {
    mutex.get()->Unlock();
  }
}

jobject ComparatorJniCallback::GetBuffer(JNIEnv* env, const Slice& src,
                                         bool reuse_buffer,
                                         ThreadLocalPtr* tl_buf,
                                         jobject jreuse_buffer) const {
  if (reuse_buffer) {
    if (m_options->reused_synchronisation_type ==
        ReusedSynchronisationType::THREAD_LOCAL) {
      // reuse thread-local bufffer
      ThreadLocalBuf* tlb = reinterpret_cast<ThreadLocalBuf*>(tl_buf->Get());
      if (tlb == nullptr) {
        // thread-local buffer has not yet been created, so create it
        jobject jtl_buf = env->NewGlobalRef(ByteBufferJni::construct(
            env, m_options->direct_buffer, m_options->max_reused_buffer_size,
            m_jbytebuffer_clazz));
        if (jtl_buf == nullptr) {
          // exception thrown: OutOfMemoryError
          return nullptr;
        }
        tlb = new ThreadLocalBuf(m_jvm, m_options->direct_buffer, jtl_buf);
        tl_buf->Reset(tlb);
      }
      return ReuseBuffer(env, src, tlb->jbuf);
    } else {
      // reuse class member buffer
      return ReuseBuffer(env, src, jreuse_buffer);
    }
  } else {
    // new buffer
    return NewBuffer(env, src);
  }
}

jobject ComparatorJniCallback::ReuseBuffer(JNIEnv* env, const Slice& src,
                                           jobject jreuse_buffer) const {
  // we can reuse the buffer
  if (m_options->direct_buffer) {
    // copy into direct buffer
    void* buf = env->GetDirectBufferAddress(jreuse_buffer);
    if (buf == nullptr) {
      // either memory region is undefined, given object is not a direct
      // java.nio.Buffer, or JNI access to direct buffers is not supported by
      // this virtual machine.
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Unable to get Direct Buffer Address");
      return nullptr;
    }
    memcpy(buf, src.data(), src.size());
  } else {
    // copy into non-direct buffer
    const jbyteArray jarray =
        ByteBufferJni::array(env, jreuse_buffer, m_jbytebuffer_clazz);
    if (jarray == nullptr) {
      // exception occurred
      return nullptr;
    }
    env->SetByteArrayRegion(
        jarray, 0, static_cast<jsize>(src.size()),
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(src.data())));
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jarray);
      return nullptr;
    }
    env->DeleteLocalRef(jarray);
  }
  return jreuse_buffer;
}

jobject ComparatorJniCallback::NewBuffer(JNIEnv* env, const Slice& src) const {
  // we need a new buffer
  jobject jbuf =
      ByteBufferJni::constructWith(env, m_options->direct_buffer, src.data(),
                                   src.size(), m_jbytebuffer_clazz);
  if (jbuf == nullptr) {
    // exception occurred
    return nullptr;
  }
  return jbuf;
}

void ComparatorJniCallback::DeleteBuffer(JNIEnv* env, jobject jbuffer) const {
  env->DeleteLocalRef(jbuffer);
}

}  // namespace ROCKSDB_NAMESPACE
