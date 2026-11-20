package org.rocksdb;

import java.nio.ByteBuffer;

/*
    std::string& new_value;
    Slice& existing_operand;
    OpFailureScope op_failure_scope = OpFailureScope::kDefault;
 */
public class MergeOperatorOutput {
  public enum OpFailureScope {
    Default(0),
    TryMerge(1),
    MustMerge(2),
    OpFailureScopeMax(3);
    private final int status;
    OpFailureScope(int status) {
      this.status = status;
    }
  }

  private ByteBuffer directValue;
  private OpFailureScope op_failure_scope = OpFailureScope.Default;

  public MergeOperatorOutput(final ByteBuffer directValue) {
    this.directValue = directValue;
  }

  public MergeOperatorOutput(final ByteBuffer directValue, final OpFailureScope op_failure_scope) {
    this.directValue = directValue;
    this.op_failure_scope = op_failure_scope;
  }

  public ByteBuffer getDirectValue() {
    return directValue;
  }

  public OpFailureScope getOp_failure_scope() {
    return op_failure_scope;
  }

  /**
   * For JNI. Called from JniMergeOperatorV2
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private int getOpStatus() {
    return this.op_failure_scope.status;
  }
}