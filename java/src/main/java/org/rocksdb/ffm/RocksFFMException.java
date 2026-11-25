package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Function;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RocksFFMException extends Exception {
  public RocksFFMException(final String msg) {
    super(msg);
  }

  static void throwOnError(MemorySegment errPtr, Function<String, String> format)
      throws RocksFFMException {
    MemorySegment error = errPtr.get(ValueLayout.ADDRESS, 0L);
    if (error.address() != NULL.address()) {
      error = error.reinterpret(Long.MAX_VALUE);
      String message = error.getString(0, UTF_8);
      throw new RocksFFMException(format.apply(message));
    }
  }
}
