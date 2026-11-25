package org.rocksdb.japigen.function;

import org.rocksdb.japigen.CFunction;

import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Functions {


  private static String lowercaseFirst(String s) {
    if (s.isEmpty()) return s;
    return s.substring(0, 1).toLowerCase() + s.substring(1);
  }

  private static String capitalizeFirst(String s) {
    if (s.isEmpty()) return s;
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }

  private static List<String> by(String input, String separator) {
    return Arrays.stream(input.split(separator)).map(String::strip).filter(s -> !s.isBlank()).toList();
  }

  private static Optional<String> transformType(String cType, Function<String, String> fWord, Collector<CharSequence, ?, String> collector) {
    List<String> cleaned = by(cType, " ");
    if (cleaned.isEmpty()) {
      return Optional.empty();
    } else {
      String type = cleaned.getLast();
      var separated = by(type, "_");
      if (separated.isEmpty()) {
        return Optional.empty();
      }
      if (separated.getLast().equals("t*")) {
        separated = new ArrayList<>(separated);
        separated.removeLast();
      }
      if (separated.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of (
        separated.stream().map(fWord).collect(collector));
    }
  }

  protected static Optional<String> canonicalType(String cType) {
    List<String> cleaned = by(cType, " ");
    if (cleaned.isEmpty()) {
      return Optional.empty();
    } else {
      String type = cleaned.getLast();
      var separated = by(type, "_");
      if (separated.isEmpty()) {
        return Optional.empty();
      }
      if (separated.getLast().equals("t*")) {
        separated = new ArrayList<>(separated);
        separated.removeLast();
      }
      if (separated.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of (
        separated.stream().map(Functions::capitalizeFirst).collect(Collectors.joining()));
    }
  }

  protected static Optional<String> prefix(String cType) {
    return transformType(cType, Function.identity(), Collectors.joining("_"));
  }

  static Optional<String> canonicalMethodName(CFunction cFunction) {
    var type = canonicalType(cFunction.receiver_type());
    var method = canonicalType(cFunction.name());
    var result = cFunction.receiver_name();
    if (type.isPresent() && method.isPresent() && method.get().startsWith(type.get())) {
      result = method.get().substring(type.get().length());
    }
    else if (method.isPresent()) {
      result = method.get();
    }
    return Optional.of(lowercaseFirst(result));
  }

  private final static List<String> typePrefixes = List.of("unsigned", "const");

  static String javaTypeOf(String cType) {
    if (cType.endsWith("*")) {
      // All pointers are undistinguished MemorySegments
      return "MemorySegment";
    }
    Map<String, MemoryLayout> conversion = Linker.nativeLinker().canonicalLayouts();
    MemoryLayout memoryLayout = conversion.get(cType);
    while (memoryLayout == null) {
      var words = cType.split(" ");
      if (words.length < 2 || !typePrefixes.contains(words[0])) break;
      cType = cType.substring(words[0].length() + 1);
      memoryLayout = conversion.get(cType);
    }
    if (memoryLayout == null && (cType.startsWith("u"))) {
      memoryLayout = conversion.get(cType.substring("u".length()));
    }
    Class<?> carrier = ((ValueLayout)memoryLayout).carrier();
    return carrier.getName();
  }
}
