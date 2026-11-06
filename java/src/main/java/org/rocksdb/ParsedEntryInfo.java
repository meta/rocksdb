// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * Class to parse internal key to extract user key, entry type, sequence number.
 */
public class ParsedEntryInfo extends RocksObject {
  protected ParsedEntryInfo() {
    super(newParseEntryInstance());
  }

  @Override
  protected void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  /**
   * Returns the entryType of record in the sstFile.
   */
  public EntryType getEntryType() {
    assert (isOwningHandle());
    return EntryType.getEntryType(getEntryTypeJni(nativeHandle_));
  }

  /**
   * Returns the sequence number of the record in the sstFile.
   */
  public long getSequenceNumber() {
    return getSequenceNumberJni(nativeHandle_);
  }

  /**
   * Returns the user key of the record in the sstFile.
   */
  public byte[] getUserKey() {
    assert (isOwningHandle());
    return userKeyJni(nativeHandle_);
  }

  /**
   *
   * @param key Byte buffer to write the user key into.
   * @return length of the key.
   */
  public int userKey(final ByteBuffer key) {
    if (key == null) {
      throw new IllegalArgumentException("ByteBuffer parameters must not be null");
    }
    assert (isOwningHandle());
    final int result;
    if (key.isDirect()) {
      result = userKeyDirect(nativeHandle_, key, key.position(), key.remaining());
    } else {
      result = userKeyByteArray(
          nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining());
    }
    key.limit(Math.min(key.position() + result, key.limit()));
    return result;
  }

  /**
   * Parses internal key to get initialize the values in this class.
   * @param options options used while writing the sst file.
   * @param internalKey byte array containing the internal key.
   */
  public final void parseEntry(final Options options, final byte[] internalKey) {
    if (options == null || internalKey == null) {
      throw new IllegalArgumentException("ByteBuffer and options parameters must not be null");
    }
    assert (isOwningHandle());
    parseEntry(nativeHandle_, options.getNativeHandle(), internalKey, internalKey.length);
  }

  /**
   * Parses internal key to get initialize the values in this class.
   * @param options options used while writing the sst file.
   * @param internalKey ByteBuffer containing the internal key.
   */
  public final void parseEntry(final Options options, final ByteBuffer internalKey) {
    if (options == null || internalKey == null) {
      throw new IllegalArgumentException("ByteBuffer and options parameters must not be null");
    }
    assert (isOwningHandle());
    if (internalKey.isDirect()) {
      parseEntryDirect(nativeHandle_, options.getNativeHandle(), internalKey,
          internalKey.position(), internalKey.remaining());
    } else {
      parseEntryByteArray(nativeHandle_, options.getNativeHandle(), internalKey.array(),
          internalKey.arrayOffset() + internalKey.position(), internalKey.remaining());
    }
    internalKey.position(internalKey.limit());
  }

  private static native long newParseEntryInstance();

  private static native void parseEntry(
      final long handle, final long optionsHandle, final byte[] buffer, final int bufferLen);

  private static native void parseEntryDirect(
      final long handle, final long optionsHandle, final ByteBuffer buffer, final int bufferOffset,
      final int bufferLen);
  private static native void parseEntryByteArray(
      final long handle, final long optionsHandle, final byte[] buffer, final int bufferOffset, final int bufferLen);
  private static native int userKeyDirect(
      final long handle, final ByteBuffer target, final int bufferOffset, final int bufferLen);
  private static native int userKeyByteArray(
      final long handle, final byte[] target, final int bufferOffset, final int bufferLen);
  private static native byte[] userKeyJni(final long handle);
  private static native long getSequenceNumberJni(final long handle);
  private static native byte getEntryTypeJni(final long handle);
  private static native void disposeInternalJni(final long handle);
}
