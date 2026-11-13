package org.rocksdb.japigen.function;

import org.rocksdb.japigen.CFunction;

import java.util.List;
import java.util.Optional;

import static org.rocksdb.japigen.function.Functions.javaTypeOf;

public class Receiver extends AnalysedFunction {

  protected final String canonicalType;
  protected final String cAPIFunctionPrefix;
  protected final String methodName;

  public Receiver(String source, CFunction cFunction) {
    super(source, cFunction);
    this.canonicalType = Functions
      .canonicalType(cFunction.receiver_type())
      .orElseThrow(() -> new RuntimeException(String.format("Receiver has no type %s", cFunction)));
    this.cAPIFunctionPrefix = Functions.prefix(cFunction.receiver_type())
      .orElseThrow(() -> new RuntimeException(String.format("Receiver has no type %s", cFunction)));
    this.methodName = Functions
      .canonicalMethodName(cFunction)
      .orElseThrow(() -> new RuntimeException(String.format("Receiver has no method %s", cFunction)));
  }

  @Override
  public Optional<String> canonicalType() {
    return Optional.of(canonicalType);
  }

  @Override
  public Optional<String> cAPIFunctionPrefix() {
    return Optional.of(cAPIFunctionPrefix);
  }

  /**
   * List the static imports required
   * @return
   */
  public List<String> staticImports() {
    return List.of(cFunction.name());
  }

  /**
   * List the normal imports required
   *
   * @return
   */
  public List<String> imports() {
    return List.of();
  }

  @Override
  public String toString() {
    return String.format("Receiver (%s : %s) [%s] %s", cFunction.receiver_name(), cFunction.receiver_type(), canonicalType, cFunction);
  }

  public void formatReceiver(StringBuilder sb) {
    throw new UnsupportedOperationException(String.format("Expected a subclass of %s with formatReceiver() supported", getClass()));
  }

  static class Getter extends Receiver {

    Getter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// GETTER\n");
      sb.append(String.format("""
        public %s %s() {
        return %s(%s);
        }
        """,
        javaTypeOf(cFunction.return_type()), methodName,
        cFunction.name(), cFunction.receiver_name()));
    }
  }

  static class StringGetter extends Receiver {

    StringGetter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    public List<String> imports() {
      return List.of("java.lang.foreign.Arena", "java.lang.foreign.ValueLayout", "java.nio.charset.StandardCharsets");
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// STRING GETTER\n");
      sb.append(String.format("// TODO STRING_GET() %s\n", cFunction));
      sb.append(String.format("""
        public String %s() {
        MemorySegment sizeSegment = Arena.ofAuto().allocate(ValueLayout.JAVA_LONG, 0L);
        MemorySegment stringSegment = %s(%s, sizeSegment);
        if (stringSegment == null) {
          return null;
        }
        // THIS stuff is standard, and should be a helper
        long size = sizeSegment.get(ValueLayout.JAVA_LONG, 0);
        byte[] bytes = new byte[Math.toIntExact(size)];
        MemorySegment result = MemorySegment.ofArray(bytes);
        MemorySegment.copy(stringSegment, ValueLayout.JAVA_BYTE, 0, result, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return new String(bytes, 0, (int)size, StandardCharsets.UTF_8);
        }
        """,
        methodName,
        cFunction.name(), cFunction.receiver_name()));
    }
  }

  static class StatusGetter extends Receiver {

    StatusGetter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// STATUS GETTER\n");
      sb.append(String.format("// TODO STATUS_GET() %s\n", cFunction));
    }
  }

  static class Setter extends Receiver {

    Setter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// SETTER\n");
      sb.append(String.format("""
        public void %s(%s %s) {
        %s(%s,%s);
        }
        """,
        methodName, javaTypeOf(cFunction.value_type()), cFunction.value_name(),
        cFunction.name(), cFunction.receiver_name(), cFunction.value_name()));
    }
  }

  static class StringSetter extends Receiver {

    StringSetter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// STRING SETTER\n");
      sb.append(String.format("// TODO STRING_SET() %s\n", cFunction));
    }
  }

  static class StatusSetter extends Receiver {

    StatusSetter(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// STATUS SETTER\n");
      sb.append(String.format("// TODO STATUS_SET() %s\n", cFunction));
    }
  }
}
