// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Enum CompressionType
 *
 * <p>DB contents are stored in a set of blocks, each of which holds a
 * sequence of key,value pairs. Each block may be compressed before
 * being stored in a file. The following enum describes which
 * compression method (if any) is used to compress a block.</p>
 */
public enum CompressionType {
  NO_COMPRESSION((byte) 0x0, null, "kNoCompression"),
  SNAPPY_COMPRESSION((byte) 0x1, "snappy", "kSnappyCompression"),
  ZLIB_COMPRESSION((byte) 0x2, "z", "kZlibCompression"),
  BZLIB2_COMPRESSION((byte) 0x3, "bzip2", "kBZip2Compression"),
  LZ4_COMPRESSION((byte) 0x4, "lz4", "kLZ4Compression"),
  LZ4HC_COMPRESSION((byte) 0x5, "lz4hc", "kLZ4HCCompression"),
  XPRESS_COMPRESSION((byte) 0x6, "xpress", "kXpressCompression"),
  ZSTD_COMPRESSION((byte) 0x7, "zstd", "kZSTD"),
  DISABLE_COMPRESSION_OPTION((byte) 0x7F, null, "kDisableCompressionOption");

  /**
   * <p>Get the CompressionType enumeration value by
   * passing the library name to this method.</p>
   *
   * <p>If library cannot be found the enumeration
   * value {@code NO_COMPRESSION} will be returned.</p>
   *
   * @param libraryName compression library name.
   *
   * @return CompressionType instance.
   */
  public static CompressionType getCompressionType(final String libraryName) {
    if (libraryName != null) {
      for (final CompressionType compressionType : values()) {
        if (compressionType.getLibraryName() != null &&
            compressionType.getLibraryName().equals(libraryName)) {
          return compressionType;
        }
      }
    }
    return NO_COMPRESSION;
  }

  /**
   * <p>Get the CompressionType enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of CompressionType.
   *
   * @return CompressionType instance.
   *
   * @throws IllegalArgumentException If CompressionType cannot be found for the
   *   provided byteIdentifier
   */
  public static CompressionType getCompressionType(final byte byteIdentifier) {
    for (final CompressionType compressionType : values()) {
      if (compressionType.getValue() == byteIdentifier) {
        return compressionType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for CompressionType.");
  }

  /**
   * <p>Get a CompressionType value based on the string key in the C++ options output.
   * This gets used in support of getting options into Java from an options string,
   * which is generated at the C++ level.
   * </p>
   *
   * @param internalName the internal (C++) name by which the option is known.
   *
   * @return CompressionType instance (optional)
   */
  static CompressionType getFromInternal(final String internalName) {
    for (final CompressionType compressionType : values()) {
      if (compressionType.internalName_.equals(internalName)) {
        return compressionType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal internalName '" + internalName + " ' provided for CompressionType.");
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
   * <p>Returns the library name of the compression type
   * identified by the enumeration value.</p>
   *
   * @return library name
   */
  public String getLibraryName() {
    return libraryName_;
  }

  CompressionType(final byte value, final String libraryName, final String internalName) {
    value_ = value;
    libraryName_ = libraryName;
    internalName_ = internalName;
  }

  private final byte value_;
  private final String libraryName_;
  private final String internalName_;
}
