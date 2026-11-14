package org.rocksdb;

import java.nio.ByteBuffer;

public class WideColumn<T> {
  private final T name;
  private final T value;

  public WideColumn(T name, T value) {
    this.name = name;
    this.value = value;
  }

  public T getName() {
    return name;
  }

  public T getValue() {
    return value;
  }

  public static class ByteBufferWideColumn extends WideColumn<ByteBuffer> {
    private int nameRequiredSize;
    private int valueRequiredSize;

    public ByteBufferWideColumn(ByteBuffer name, ByteBuffer value) {
      super(name, value);
    }
    public ByteBufferWideColumn(
        ByteBuffer name, ByteBuffer value, int nameRequiredSize, int valueRequiredSize) {
      super(name, value);
      this.nameRequiredSize = nameRequiredSize;
      this.valueRequiredSize = valueRequiredSize;
    }

    public int getNameRequiredSize() {
      return nameRequiredSize;
    }

    public void setNameRequiredSize(int nameRequiredSize) {
      this.nameRequiredSize = nameRequiredSize;
    }

    public int getValueRequiredSize() {
      return valueRequiredSize;
    }

    public void setValueRequiredSize(int valueRequiredSize) {
      this.valueRequiredSize = valueRequiredSize;
    }
  }
}
