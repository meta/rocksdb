package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class WideColumnTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void simpleWriteArray() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columndName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);
    }
  }

  @Test
  public void simpleReadWrite() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      List<WideColumn<byte[]>> result = new ArrayList<>();

      Status status = db.getEntity("someKey".getBytes(), result);

      assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

      assertThat(result).isNotEmpty();
      assertThat(result.get(0).getValue())
          .isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
      assertThat(result.get(0).getName()).isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void readKVAsWideColumn() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("someKey".getBytes(StandardCharsets.UTF_8),
          "someValue".getBytes(StandardCharsets.UTF_8));

      List<WideColumn<byte[]>> result = new ArrayList<>();

      Status status = db.getEntity("someKey".getBytes(), result);

      assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

      assertThat(result).isNotEmpty();

      assertThat(result.get(0).getName()).isEqualTo(new byte[] {});

      assertThat(result.get(0).getValue()).isEqualTo("someValue".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void readWideColumnAsKV() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      byte[] status = db.get("someKey".getBytes());

      assertThat(status).isNotNull();
    }
  }

  @Test
  public void readWideDefaultColumnAsKV() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      wideColumns.add(
          new WideColumn<>(new byte[] {}, "defaultColumnValue".getBytes(StandardCharsets.UTF_8)));

      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      byte[] status = db.get("someKey".getBytes());

      assertThat(status).isNotNull();
      assertThat(status).isEqualTo("defaultColumnValue".getBytes(StandardCharsets.UTF_8));
    }
  }
  @Test
  public void readWriteWithKeyOffset() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (final ColumnFamilyHandle cfHandle = db.getDefaultColumnFamily()) {
        List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

        wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
            "columnValue".getBytes(StandardCharsets.UTF_8)));
        db.putEntity(cfHandle, "someKey".getBytes(StandardCharsets.UTF_8), 1, 4, wideColumns);

        List<WideColumn<byte[]>> result = new ArrayList<>();

        Status status = db.getEntity(cfHandle, "asomeKeys".getBytes(), 2, 4, result);

        assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(result).isNotEmpty();
        assertThat(result.get(0).getValue())
            .isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
        assertThat(result.get(0).getName())
            .isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void putEntityDirectByteBuffer() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<ByteBuffer>> wideColumns = new ArrayList<>();

      ByteBuffer key = ByteBuffer.allocateDirect(100);
      ByteBuffer name = ByteBuffer.allocateDirect(100);
      ByteBuffer value = ByteBuffer.allocateDirect(100);

      key.put("someKey".getBytes(StandardCharsets.UTF_8));
      key.flip();

      name.put("columnName".getBytes(StandardCharsets.UTF_8));
      name.flip();

      value.put("columnValue".getBytes(StandardCharsets.UTF_8));
      value.flip();

      wideColumns.add(new WideColumn<>(name, value));
      db.putEntity(key, wideColumns);

      List<WideColumn<byte[]>> result = new ArrayList<>();

      Status status = db.getEntity("someKey".getBytes(), result);

      assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

      assertThat(result).isNotEmpty();
      assertThat(result.get(0).getValue())
          .isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
      assertThat(result.get(0).getName()).isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void putEntityNonDirectByteBuffer() throws RocksDBException {
    exceptionRule.expect(RocksDBException.class);
    exceptionRule.expectMessage(
        "Invalid \"value\" argument (argument is not a valid direct ByteBuffer)");
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<ByteBuffer>> wideColumns = new ArrayList<>();

      ByteBuffer key = ByteBuffer.allocateDirect(100);
      ByteBuffer name = ByteBuffer.allocateDirect(100);
      ByteBuffer value = ByteBuffer.allocate(100);

      key.put("someKey".getBytes(StandardCharsets.UTF_8));
      key.flip();

      name.put("columnName".getBytes(StandardCharsets.UTF_8));
      name.flip();

      value.put("columnValue".getBytes(StandardCharsets.UTF_8));
      value.flip();

      wideColumns.add(new WideColumn<>(name, value));
      db.putEntity(key, wideColumns);
    }
  }

  @Test
  public void getEntityDirect() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      List<WideColumn.ByteBufferWideColumn> result = new ArrayList<>();
      result.add(new WideColumn.ByteBufferWideColumn(
          ByteBuffer.allocateDirect(10), ByteBuffer.allocateDirect(10)));
      ByteBuffer key = ByteBuffer.allocateDirect(20);
      key.put("someKey".getBytes(StandardCharsets.UTF_8));
      key.flip();

      Status s = db.getEntity(key, result);

      assertThat(s).isNotNull();
      assertThat(s.getCode()).isEqualTo(Status.Code.Ok);
      WideColumn.ByteBufferWideColumn column = result.get(0);
      assertThat(column.getNameRequiredSize()).isEqualTo(10);
      assertThat(column.getValueRequiredSize()).isEqualTo(11);

      ByteBuffer valueBuffer = column.getValue();
      assertThat(valueBuffer.position()).isEqualTo(valueBuffer.capacity());
      valueBuffer.flip();
      byte[] value = new byte[10];
      valueBuffer.get(value);
      assertThat(value).isEqualTo("columnValu".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void getEntityDirectZeroString() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      byte[] columnName =
          new byte[] {0, 0, 'c', 'o', 'l', 'u', 'm', 'n', 'N', 'a', 'm', 'e', 0, 0, 0};
      byte[] columnValue =
          new byte[] {0, 0, 'c', 'o', 'l', 'u', 'm', 'n', 'V', 'a', 'l', 'u', 'e', 0, 0, 0};

      wideColumns.add(new WideColumn<>(columnName, columnValue));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      List<WideColumn.ByteBufferWideColumn> result = new ArrayList<>();
      result.add(new WideColumn.ByteBufferWideColumn(
          ByteBuffer.allocateDirect(15), ByteBuffer.allocateDirect(15)));
      ByteBuffer key = ByteBuffer.allocateDirect(20);
      key.put("someKey".getBytes(StandardCharsets.UTF_8));
      key.flip();

      Status s = db.getEntity(key, result);

      assertThat(s).isNotNull();
      assertThat(s.getCode()).isEqualTo(Status.Code.Ok);
      WideColumn.ByteBufferWideColumn column = result.get(0);
      assertThat(column.getNameRequiredSize()).isEqualTo(15);
      assertThat(column.getValueRequiredSize()).isEqualTo(16);

      ByteBuffer nameBuffer = column.getName();
      assertThat(nameBuffer.position()).isEqualTo(nameBuffer.capacity());
      nameBuffer.flip();
      byte[] name = new byte[15];
      nameBuffer.get(name);
      assertThat(name).isEqualTo(columnName);

      ByteBuffer valueBuffer = column.getValue();
      assertThat(valueBuffer.position()).isEqualTo(valueBuffer.capacity());
      valueBuffer.flip();
      byte[] value = new byte[15];
      valueBuffer.get(value);
      assertThat(value).isEqualTo(
          new byte[] {0, 0, 'c', 'o', 'l', 'u', 'm', 'n', 'V', 'a', 'l', 'u', 'e', 0, 0});
    }
  }

  @Test
  public void getEntityNonDirectBuffer() throws RocksDBException {
    exceptionRule.expect(RocksDBException.class);
    exceptionRule.expectMessage(
        "Invalid \"name\" argument (argument is not a valid direct ByteBuffer)");

    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

      wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
          "columnValue".getBytes(StandardCharsets.UTF_8)));
      db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

      List<WideColumn.ByteBufferWideColumn> result = new ArrayList<>();
      result.add(new WideColumn.ByteBufferWideColumn(
          ByteBuffer.allocate(10), ByteBuffer.allocateDirect(10)));
      ByteBuffer key = ByteBuffer.allocateDirect(20);
      key.put("someKey".getBytes(StandardCharsets.UTF_8));
      key.flip();

      db.getEntity(key, result);
    }
  }

  @Test
  public void simpleReadWriteWithColumnFamily() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      ColumnFamilyDescriptor cfDescriptor =
          new ColumnFamilyDescriptor("testColumnFamily".getBytes(StandardCharsets.UTF_8));

      try (ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor)) {
        List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

        wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8),
            "columnValue".getBytes(StandardCharsets.UTF_8)));
        db.putEntity(cfHandle, "someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

        List<WideColumn<byte[]>> result = new ArrayList<>();

        Status status = db.getEntity(cfHandle, "someKey".getBytes(), result);

        assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(result).isNotEmpty();
        assertThat(result.get(0).getValue())
            .isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
        assertThat(result.get(0).getName())
            .isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void noResult() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      List<WideColumn<byte[]>> result = new ArrayList<>();

      Status status = db.getEntity("someKey".getBytes(), result);

      assertThat(status.getCode()).isEqualTo(Status.Code.NotFound);

      assertThat(result).isEmpty();
    }
  }
}
