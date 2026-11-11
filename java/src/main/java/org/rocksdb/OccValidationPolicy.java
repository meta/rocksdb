//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Defines the policy for optimistic concurrency control validation.
 * This enum specifies the manner in which the validation occurs
 * during the commit stage.
 */
public enum OccValidationPolicy {
  /**
   * Validate serially at commit stage, AFTER entering the write-group.
   * This method processes isolation validation in a single-threaded manner
   * within the write-group, potentially suffering from high mutex contention
   * as discussed in the following issue:
   * <a href="https://github.com/facebook/rocksdb/issues/4402">GitHub 4402</a>
   */
  VALIDATE_SERIAL((byte) 0),

  /**
   * Validate parallelly before the commit stage, BEFORE entering the write-group.
   * This approach aims to reduce mutex contention by having each
   * transaction acquire locks for its write-set records in a well-defined
   * order prior to entering the write-group.
   */
  VALIDATE_PARALLEL((byte) 1);

  private final byte _value;

  /**
   * Constructor for the OccValidationPolicy enum.
   * @param _value the byte representation that corresponds to
   *               one of the above enums.
   */
  OccValidationPolicy(final byte _value) {
    this._value = _value;
  }

  /**
   * Retrieves the byte representation associated with this validation policy.
   * @return the byte representation of the validation policy.
   */
  public byte getValue() {
    return _value;
  }

  /**
   * Given a byte representation of a value, convert it to {@link OccValidationPolicy}.
   *
   * @param policy the byte representation of the policy.
   * @return the matching OccValidationPolicy.
   * @throws IllegalArgumentException if no matching policy is found.
   */
  public static OccValidationPolicy getOccValidationPolicy(final byte policy) {
    for (OccValidationPolicy value : OccValidationPolicy.values()) {
      if (value.getValue() == policy) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown OccValidationPolicy constant : " + policy);
  }
}
