package org.rocksdb.ffm;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Function;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * A fluent interface is designed to mimic a Domain-Specific-Language (DSL) by relying heavily on
 * method-chaining.
 *
 * @see <a href="https://belief-driven-design.com/fluent-interfaces-b11b901f1eb/">Belief-Driven
 *     Design</a>
 */
public final class RocksFFM {
  static {
    RocksDB.loadLibrary();
  }

  public static DB db(Arena arena) {
    return new DBImpl(arena);
  }

  interface DB {
    DB option();
    OpenDB open(String path);
  }

  interface OpenDB {
    Getter get(MemorySegment key);
    Putter put(MemorySegment key, MemorySegment value);
  }

  interface Putter {
    void invoke() throws RocksDBException;
  }

  interface Getter {
    MemorySegment invoke() throws RocksDBException;
  }

  static MemorySegment dereference(MemorySegment reference, MemorySegment length) {
    if (reference.address() == NULL.address()) {
      // NULL returned as a value, usually when a get() status is NOT_FOUND
      return null;
    }
    long lengthValue = length.get(ValueLayout.JAVA_LONG, 0);
    return reference.asSlice(0L, lengthValue);
  }

  static void throwOnError(MemorySegment errPtr, Function<String, String> format)
      throws RocksDBException {
    MemorySegment error = errPtr.get(ValueLayout.ADDRESS, 0L);
    if (error.address() != NULL.address()) {
      error = error.reinterpret(Long.MAX_VALUE);
      String message = error.getString(0, UTF_8);
      throw new RocksDBException(format.apply(message));
    }
  }

  public static MemorySegment segmentOf(Arena arena, String s) {
    MemorySegment segment = arena.allocateFrom(s, UTF_8);
    return segment.asSlice(0, segment.byteSize() - 1);
  }

  /**
   * Extract a string from a segment
   * `MemorySegment.getString(offset, charset)` doesn't work for us, as it expects a C-style
   * null-terminated string
   *
   * @param segment to interpret as a String
   * @return the Java string contained in `segment`
   */
  public static String stringOf(MemorySegment segment) {
    int byteSize = (int) segment.byteSize();
    byte[] bytes = new byte[byteSize];
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, byteSize);
    return new String(bytes, UTF_8);
  }
}
