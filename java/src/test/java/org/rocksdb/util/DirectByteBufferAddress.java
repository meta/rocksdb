//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Native memory addresses of direct {@link ByteBuffer}s, for testing the raw address APIs.
 *
 * <p>Reads the private {@code Buffer.address} field, so on Java 9 and later the JVM must run with
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}.
 */
public final class DirectByteBufferAddress {
  private static final Field ADDRESS_FIELD;

  static {
    try {
      ADDRESS_FIELD = Buffer.class.getDeclaredField("address");
      ADDRESS_FIELD.setAccessible(true);
    } catch (final ReflectiveOperationException | RuntimeException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private DirectByteBufferAddress() {}

  /**
   * @param buffer a direct buffer
   * @return the native address of the buffer's current position
   */
  public static long address(final ByteBuffer buffer) {
    try {
      return ADDRESS_FIELD.getLong(buffer) + buffer.position();
    } catch (final IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @param string the contents of the buffer
   * @return a new direct buffer holding the UTF-8 encoding of the string, ready to be read
   */
  public static ByteBuffer directBuffer(final String string) {
    final byte[] bytes = string.getBytes(UTF_8);
    final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes).flip();
    return buffer;
  }
}
