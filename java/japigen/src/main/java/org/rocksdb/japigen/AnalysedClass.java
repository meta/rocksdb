package org.rocksdb.japigen;

import org.rocksdb.japigen.function.AnalysedFunction;
import org.rocksdb.japigen.function.Receiver;

import java.util.*;
import java.util.function.Function;

public class AnalysedClass {

  protected final String canonicalType;
  protected final String cAPIFunctionPrefix;
  private final List<Receiver> receivers = new ArrayList<>();
  private final Lazy<String> receiverName = lazyReceiver(AnalysedFunction::receiverName);

  private AnalysedClass(String canonicalType, String cAPIFunctionPrefix) {

    this.canonicalType = canonicalType;
    this.cAPIFunctionPrefix = cAPIFunctionPrefix;
  }

  public void addReceiver(Receiver receiver) {
    receivers.add(receiver);
  }

  @Override public String toString() {
    return String.format("AnalysedClass { %s, %d receivers }", canonicalType, receivers.size());
  }

  public void formatClass(StringBuilder sb, String jextractAPIClass, String targetPackage, boolean createConstructor) {
    sb.append(String.format("package %s;\n\n", targetPackage));

    // Java JDK imports that the receiver(s) use
    Set<String> imports = new TreeSet<>(Collections.singleton("java.lang.foreign.MemorySegment"));

    // jextract-generated method imports that are called out to by the wrappers we generate
    Set<String> staticImports = new TreeSet<>();

    for (Receiver receiver : receivers) {
      staticImports.addAll(receiver.staticImports());
      imports.addAll(receiver.imports());
    }

    if (!imports.isEmpty()) {
      for (String standardImport : imports) {
        sb.append(String.format("import %s;\n", standardImport));
      }
      sb.append("\n");
    }

    if (!staticImports.isEmpty()) {
      for (String staticImport : staticImports) {
        sb.append(String.format("import static %s.%s.%s;\n", targetPackage, jextractAPIClass, staticImport));
      }
      sb.append("\n");
    }

    if (createConstructor) {
      sb.append(String.format("public class %s implements AutoCloseable {\n", canonicalType));
      // memory segment
      sb.append(String.format("private final MemorySegment %s;\n\n", receiverName.get()));
    } else {
      sb.append(String.format("public class %s {\n", canonicalType));
      // memory segment
      sb.append(String.format("private MemorySegment %s;\n\n", receiverName.get()));
    }
    // memory segment accessor method
    sb.append(String.format("public MemorySegment nativeSegment() {\nreturn %s;\n}\n\n", receiverName.get()));
    if (createConstructor) {
      // constructor
      sb.append(String.format("public %s() {\n%s = %s.%s%s();\n}\n\n",
        canonicalType, receiverName.get(), jextractAPIClass, cAPIFunctionPrefix, "_create"));
      // corresponding close
      sb.append(String.format("@Override public void close() {\n%s.%s%s(%s);\n}\n\n",
        jextractAPIClass, cAPIFunctionPrefix, "_destroy", receiverName.get()));
    }
    for (Receiver receiver : receivers) {
      receiver.formatReceiver(sb);
    }
    sb.append("}\n");
  }

  public static AnalysedClass of(String canonicalType, String cAPIFunctionPrefix) {
    return new AnalysedClass(canonicalType, cAPIFunctionPrefix);
  }

  private <T> Lazy<T> lazyReceiver(Function<Receiver, T> getFromReceiver) {
    return Lazy.of(() -> {
      if (receivers.isEmpty()) {
        throw new RuntimeException("Lazy query of name from empty list of receivers");
      }
      return getFromReceiver.apply(receivers.getFirst());
    });
  }
}
