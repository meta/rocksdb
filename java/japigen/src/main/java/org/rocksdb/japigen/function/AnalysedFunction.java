package org.rocksdb.japigen.function;

import org.rocksdb.japigen.CFunction;

import java.util.Optional;

public class AnalysedFunction {

  protected final String source;
  protected final CFunction cFunction;
  protected AnalysedFunction(String source, CFunction cFunction) {

    this.source = source;
    this.cFunction = cFunction;
  }

  public static AnalysedFunction of(String source, CFunction cFunction) {
    return switch (cFunction.kind()) {
      case "field_getter" -> new Receiver.Getter(source, cFunction);
      case "string_field_getter" -> new Receiver.StringGetter(source, cFunction);
      case "status_getter" -> new Receiver.StatusGetter(source, cFunction);
      case "field_setter" -> new Receiver.Setter(source, cFunction);
      case null -> new Empty(source, cFunction);
      default -> new Receiver(source, cFunction);
    };
  }

  /**
   * If the function has a canonical type, we can attach it to an AnalyseClass
   * and then do generation from it.
   *
   * @return the canonical type
   */
  public Optional<String> canonicalType() {
    return Optional.empty();
  }

  public Optional<String> cAPIFunctionPrefix() {
    return Optional.empty();
  }

  public String receiverName() {
    return cFunction.receiver_name();
  }

  static class Empty extends AnalysedFunction {

    private Empty(String source, CFunction cFunction) {
      super(source, cFunction);
    }

    @Override public String toString() {
      return String.format("Empty %s", cFunction);
    }
  }
}
