package org.rocksdb.japigen;

import java.util.function.Supplier;

public final class Lazy<T> {
  private Supplier<T> supplier;
  private T value;
  private boolean initialized = false;

  private Lazy(Supplier<T> supplier) {
    if (supplier == null) {
      throw new IllegalArgumentException("Supplier cannot be null");
    }
    this.supplier = supplier;
  }

  public static <T> Lazy<T> of(Supplier<T> supplier) {
    return new Lazy<>(supplier);
  }

  public T get() {
    if (!initialized) {
      value = supplier.get();
      initialized = true;
      supplier = null; // Free memory by allowing GC of the supplier/lambda
    }
    return value;
  }

  public boolean isInitialized() {
    return initialized;
  }
}

