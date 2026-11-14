package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MergeOperatorV2Test {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private static byte[] KEY = "thisIsKey".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testMergeOperator() throws RocksDBException {
    try (TestMergeOperator mergeOperator = new TestMergeOperator();
        Options options = new Options()) {
      options.setMergeOperator(mergeOperator);
      options.setCreateIfMissing(true);

      try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        db.put(KEY, "value".getBytes());
        db.merge(KEY, "value1".getBytes());
        db.merge(KEY, "value2".getBytes());
        db.merge(KEY, "value3".getBytes());
        byte[] valueFromDb = db.get(KEY);
        assertThat(valueFromDb).containsExactly("10".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void middleOfByteBuffer() throws RocksDBException {
    try (MergeOperatorV2 mergeOperator = new MergeOperatorV2("Second operator") {
      @Override
      public MergeOperatorOutput fullMergeV2(
          ByteBuffer key, ByteBuffer existingValue, List<ByteBuffer> operand) {
        ByteBuffer b = ByteBuffer.allocateDirect(10);
        b.put("xxx".getBytes(StandardCharsets.UTF_8));
        b.put(new byte[] {0, 0});
        b.flip();
        b.position(3);
        return new MergeOperatorOutput(b);
      }
    }) {
      try (Options options = new Options()) {
        options.setMergeOperator(mergeOperator);
        options.setCreateIfMissing(true);

        try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
          db.put(KEY, "value".getBytes());
          db.merge(KEY, "value1".getBytes());
          db.merge(KEY, "value2".getBytes());
          db.merge(KEY, "value3".getBytes());
          byte[] valueFromDb = db.get(KEY);
          assertThat(valueFromDb).containsExactly(new byte[] {0, 0});
        }
      }
    }
  }

  @Test
  public void returnExistingOperandByteBuffer() throws RocksDBException {
    try (MergeOperatorV2 mergeOperator = new MergeOperatorV2("Third operator") {
      @Override
      public MergeOperatorOutput fullMergeV2(
          ByteBuffer key, ByteBuffer existingValue, List<ByteBuffer> operand) {
        return new MergeOperatorOutput(operand.get(1));
      }
    }) {
      try (Options options = new Options()) {
        options.setMergeOperator(mergeOperator);
        options.setCreateIfMissing(true);

        try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
          db.put(KEY, "value".getBytes());
          db.merge(KEY, "value1".getBytes());
          db.merge(KEY, "value2".getBytes());
          db.merge(KEY, "value3".getBytes());
          byte[] valueFromDb = db.get(KEY);
          assertThat(valueFromDb).containsExactly("value2".getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void returnFailureStatus() throws RocksDBException {
    try (MergeOperatorV2 mergeOperator = new MergeOperatorV2("CrashOperator") {
      @Override
      public MergeOperatorOutput fullMergeV2(
          ByteBuffer key, ByteBuffer existingValue, List<ByteBuffer> operand) {
        return new MergeOperatorOutput(null, MergeOperatorOutput.OpFailureScope.OpFailureScopeMax);
      }
    }) {
      try (Options options = new Options()) {
        options.setMergeOperator(mergeOperator);
        options.setCreateIfMissing(true);

        try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
          db.put(KEY, "value".getBytes());
          db.merge(KEY, "value1".getBytes());
          db.merge(KEY, "value2".getBytes());
          db.merge(KEY, "value3".getBytes());
          byte[] valueFromDb = db.get(KEY);
          fail("We should never reach this.");
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void throwJavaException() throws RocksDBException {
    try (MergeOperatorV2 mergeOperator = new MergeOperatorV2("CrashOperator") {
      @Override
      public MergeOperatorOutput fullMergeV2(
          ByteBuffer key, ByteBuffer existingValue, List<ByteBuffer> operand) {
        throw new RuntimeException("Never do this");
      }
    }) {
      try (Options options = new Options()) {
        options.setMergeOperator(mergeOperator);
        options.setCreateIfMissing(true);

        try (RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
          db.put(KEY, "value".getBytes());
          db.merge(KEY, "value1".getBytes());
          db.merge(KEY, "value2".getBytes());
          db.merge(KEY, "value3".getBytes());
          byte[] valueFromDb = db.get(KEY);
          fail("We should never reach this.");
        }
      }
    }
  }

  public static class TestMergeOperator extends MergeOperatorV2 {
    public TestMergeOperator() {
      super("TestMergeOperator");
    }

    @Override
    public MergeOperatorOutput fullMergeV2(
        ByteBuffer key, ByteBuffer existingValue, List<ByteBuffer> operand) {
      ByteBuffer b = ByteBuffer.allocateDirect(10);
      b.put("10".getBytes(StandardCharsets.UTF_8));
      b.flip();
      return new MergeOperatorOutput(b);
    }
  }
}
