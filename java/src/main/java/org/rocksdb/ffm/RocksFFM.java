package org.rocksdb.ffm;

import org.rocksdb.RocksDB;

import java.lang.foreign.SegmentAllocator;

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

  public static DBSpec db(SegmentAllocator allocator) {
    return new DBSpecImpl(allocator);
  }

}
