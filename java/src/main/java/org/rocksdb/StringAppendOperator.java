// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * StringAppendOperator is a merge operator that concatenates
 * two strings.
 */
public class StringAppendOperator extends InBuiltMergeOperator {
  public StringAppendOperator() {
    this(',');
  }

  public StringAppendOperator(final char delim) {
    super(newSharedStringAppendOperator(delim));
  }

  public StringAppendOperator(final String delim) {
    super(newSharedStringAppendOperator(delim));
  }

  private static native long newSharedStringAppendOperator(final char delim);
  private static native long newSharedStringAppendOperator(final String delim);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);
}
