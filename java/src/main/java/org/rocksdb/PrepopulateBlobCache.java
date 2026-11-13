//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Enum PrepopulateBlobCache
 *
 * <p>Prepopulate warm/hot blobs which are already in memory into blob
 * cache at the time of flush. On a flush, the blob that is in memory
 * (in memtables) get flushed to the device. If using Direct IO,
 * additional IO is incurred to read this blob back into memory again,
 * which is avoided by enabling this option. This further helps if the
 * workload exhibits high temporal locality, where most of the reads go
 * to recently written data. This also helps in case of the remote file
 * system since it involves network traffic and higher latencies.</p>
 */
public enum PrepopulateBlobCache {
  PREPOPULATE_BLOB_DISABLE((byte) 0x0, "prepopulate_blob_disable", "kDisable"),
  PREPOPULATE_BLOB_FLUSH_ONLY((byte) 0x1, "prepopulate_blob_flush_only", "kFlushOnly");

  /**
   * <p>Get the PrepopulateBlobCache enumeration value by
   * passing the library name to this method.</p>
   *
   * <p>If library cannot be found the enumeration
   * value {@code PREPOPULATE_BLOB_DISABLE} will be returned.</p>
   *
   * @param libraryName prepopulate blob cache library name.
   *
   * @return PrepopulateBlobCache instance.
   */
  public static PrepopulateBlobCache getPrepopulateBlobCache(String libraryName) {
    if (libraryName != null) {
      for (PrepopulateBlobCache prepopulateBlobCache : values()) {
        if (prepopulateBlobCache.getLibraryName() != null
            && prepopulateBlobCache.getLibraryName().equals(libraryName)) {
          return prepopulateBlobCache;
        }
      }
    }
    return PREPOPULATE_BLOB_DISABLE;
  }

  /**
   * <p>Get the PrepopulateBlobCache enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of PrepopulateBlobCache.
   *
   * @return PrepopulateBlobCache instance.
   *
   * @throws IllegalArgumentException If PrepopulateBlobCache cannot be found for the
   *   provided byteIdentifier
   */
  public static PrepopulateBlobCache getPrepopulateBlobCache(byte byteIdentifier) {
    for (final PrepopulateBlobCache prepopulateBlobCache : values()) {
      if (prepopulateBlobCache.getValue() == byteIdentifier) {
        return prepopulateBlobCache;
      }
    }

    throw new IllegalArgumentException("Illegal value provided for PrepopulateBlobCache.");
  }

  /**
   * <p>Get a PrepopulateBlobCache value based on the string key in the C++ options output.
   * This gets used in support of getting options into Java from an options string,
   * which is generated at the C++ level.
   * </p>
   *
   * @param internalName the internal (C++) name by which the option is known.
   *
   * @return PrepopulateBlobCache instance (optional)
   */
  static PrepopulateBlobCache getFromInternal(final String internalName) {
    for (final PrepopulateBlobCache prepopulateBlobCache : values()) {
      if (prepopulateBlobCache.internalName_.equals(internalName)) {
        return prepopulateBlobCache;
      }
    }

    throw new IllegalArgumentException(
        "Illegal internalName '" + internalName + " ' provided for PrepopulateBlobCache.");
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * <p>Returns the library name of the prepopulate blob cache mode
   * identified by the enumeration value.</p>
   *
   * @return library name
   */
  public String getLibraryName() {
    return libraryName_;
  }

  PrepopulateBlobCache(final byte value, final String libraryName, final String internalName) {
    value_ = value;
    libraryName_ = libraryName;
    internalName_ = internalName;
  }

  private final byte value_;
  private final String libraryName_;
  private final String internalName_;
}
