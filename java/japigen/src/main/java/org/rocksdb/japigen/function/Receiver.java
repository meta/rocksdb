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
      return List.of(
        "java.lang.foreign.Arena",
        "java.lang.foreign.ValueLayout",
        "java.nio.charset.StandardCharsets",
        "org.rocksdb.ffm.util.FFMSegment",
        "org.rocksdb.ffm.util.FFMArena");
    }

    @Override
    public void formatReceiver(StringBuilder sb) {
      sb.append("// STRING GETTER\n");
      sb.append(String.format("// TODO STRING_GET() %s\n", cFunction));
      sb.append(String.format("""
        public String %s() {
        MemorySegment sizeSegment = FFMArena.smallAuto().allocate(ValueLayout.JAVA_LONG, 1L);
        MemorySegment stringSegment = %s(%s, sizeSegment);
        if (stringSegment == null) {
          return null;
        }
        return FFMSegment.ofStringOfSize(stringSegment, sizeSegment);
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
      // There are only 2? of these.
      // Emit a comment, but do not emit a method.
      sb.append("// STATUS GETTER is not currently implemented\n");
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
}
