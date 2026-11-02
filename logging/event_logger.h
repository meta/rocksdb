//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <chrono>
#include <cmath>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>

#include "logging/log_buffer.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter {
 public:
  JSONWriter() : state_(kExpectKey), first_element_(true), in_array_(false) {
    stream_ << "{";
  }

  void AddKey(const std::string& key) {
    assert(state_ == kExpectKey);
    if (!first_element_) {
      stream_ << ", ";
    }
    WriteString(key.c_str());
    stream_ << ": ";
    state_ = kExpectValue;
    first_element_ = false;
  }

  void AddValue(const char* value) {
    assert(state_ == kExpectValue || state_ == kInArray);
    if (state_ == kInArray && !first_element_) {
      stream_ << ", ";
    }
    WriteString(value);
    if (state_ != kInArray) {
      state_ = kExpectKey;
    }
    first_element_ = false;
  }

  template <typename T>
  void AddValue(const T& value) {
    assert(state_ == kExpectValue || state_ == kInArray);
    if (state_ == kInArray && !first_element_) {
      stream_ << ", ";
    }
    if constexpr (std::is_floating_point_v<T>) {
      if (!std::isfinite(value)) {
        stream_ << "null";
      } else {
        stream_ << value;
      }
    } else {
      stream_ << value;
    }
    if (state_ != kInArray) {
      state_ = kExpectKey;
    }
    first_element_ = false;
  }

  void StartArray() {
    assert(state_ == kExpectValue);
    state_ = kInArray;
    in_array_ = true;
    stream_ << "[";
    first_element_ = true;
  }

  void EndArray() {
    assert(state_ == kInArray);
    state_ = kExpectKey;
    in_array_ = false;
    stream_ << "]";
    first_element_ = false;
  }

  void StartObject() {
    assert(state_ == kExpectValue);
    state_ = kExpectKey;
    stream_ << "{";
    first_element_ = true;
  }

  void EndObject() {
    assert(state_ == kExpectKey);
    stream_ << "}";
    first_element_ = false;
  }

  void StartArrayedObject() {
    assert(state_ == kInArray && in_array_);
    state_ = kExpectValue;
    if (!first_element_) {
      stream_ << ", ";
    }
    StartObject();
  }

  void EndArrayedObject() {
    assert(in_array_);
    EndObject();
    state_ = kInArray;
  }

  std::string Get() const { return stream_.str(); }

  JSONWriter& operator<<(const char* val) {
    if (state_ == kExpectKey) {
      AddKey(val);
    } else {
      AddValue(val);
    }
    return *this;
  }

  JSONWriter& operator<<(const std::string& val) {
    return *this << val.c_str();
  }

  template <typename T>
  JSONWriter& operator<<(const T& val) {
    assert(state_ != kExpectKey);
    AddValue(val);
    return *this;
  }

 private:
  void WriteString(const char* str) {
    stream_ << '"';
    for (const char* p = str; *p != '\0'; ++p) {
      const char c = *p;
      switch (c) {
        case '"':
          stream_ << "\\\"";
          break;
        case '\\':
          stream_ << "\\\\";
          break;
        case '\b':
          stream_ << "\\b";
          break;
        case '\f':
          stream_ << "\\f";
          break;
        case '\n':
          stream_ << "\\n";
          break;
        case '\r':
          stream_ << "\\r";
          break;
        case '\t':
          stream_ << "\\t";
          break;
        default:
          if (static_cast<unsigned char>(c) < 0x20) {
            char buf[7];
            snprintf(buf, sizeof(buf), "\\u%04x",
                     static_cast<unsigned char>(c));
            stream_ << buf;
          } else {
            stream_ << c;
          }
      }
    }
    stream_ << '"';
  }

  enum JSONWriterState {
    kExpectKey,
    kExpectValue,
    kInArray,
    kInArrayedObject,
  };
  JSONWriterState state_;
  bool first_element_;
  bool in_array_;
  std::ostringstream stream_;
};

class EventLoggerStream {
 public:
  template <typename T>
  EventLoggerStream& operator<<(const T& val) {
    MakeStream();
    *json_writer_ << val;
    return *this;
  }

  void StartArray() { json_writer_->StartArray(); }
  void EndArray() { json_writer_->EndArray(); }
  void StartObject() { json_writer_->StartObject(); }
  void EndObject() { json_writer_->EndObject(); }

  ~EventLoggerStream();

 private:
  void MakeStream() {
    if (!json_writer_) {
      json_writer_ = new JSONWriter();
      *this << "time_micros"
            << std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    }
  }
  friend class EventLogger;
  explicit EventLoggerStream(Logger* logger);
  explicit EventLoggerStream(LogBuffer* log_buffer, const size_t max_log_size);
  // exactly one is non-nullptr
  Logger* const logger_;
  LogBuffer* const log_buffer_;
  const size_t max_log_size_;  // used only for log_buffer_
  // ownership
  JSONWriter* json_writer_;
};

// here is an example of the output that will show up in the LOG:
// 2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros":
// 1421360005788015, "event": "table_file_creation", "file_number": 12,
// "file_size": 1909699}
class EventLogger {
 public:
  static const char* Prefix() { return "EVENT_LOG_v1"; }

  explicit EventLogger(Logger* logger) : logger_(logger) {}
  EventLoggerStream Log() { return EventLoggerStream(logger_); }
  EventLoggerStream LogToBuffer(LogBuffer* log_buffer) {
    return EventLoggerStream(log_buffer, LogBuffer::kDefaultMaxLogSize);
  }
  EventLoggerStream LogToBuffer(LogBuffer* log_buffer,
                                const size_t max_log_size) {
    return EventLoggerStream(log_buffer, max_log_size);
  }
  void Log(const JSONWriter& jwriter);
  static void Log(Logger* logger, const JSONWriter& jwriter);
  static void LogToBuffer(
      LogBuffer* log_buffer, const JSONWriter& jwriter,
      const size_t max_log_size = LogBuffer::kDefaultMaxLogSize);

 private:
  Logger* logger_;
};

}  // namespace ROCKSDB_NAMESPACE
