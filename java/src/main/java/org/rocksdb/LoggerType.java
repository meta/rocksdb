// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * Simple enumeration used for differentiating
 * the types of loggers when passing via the JNI
 * boundary.
 */
public enum LoggerType {
  JAVA_IMPLEMENTATION((byte) 0x1),
  STDERR_IMPLEMENTATION((byte) 0x2),
  FROM_OPTIONS_IMPLEMENTATION((byte) 0x3);

  private final byte value;

  LoggerType(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  byte getValue() {
    return value;
  }

  /**
   * Get LoggerType by byte value.
   *
   * @param value byte representation of LoggerType.
   *
   * @return {@link org.rocksdb.LoggerType} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  static LoggerType getLoggerType(final byte value) {
    for (final LoggerType loggerType : LoggerType.values()) {
      if (loggerType.getValue() == value) {
        return loggerType;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for LoggerType.");
  }
}
