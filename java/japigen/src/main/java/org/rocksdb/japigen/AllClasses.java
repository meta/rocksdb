package org.rocksdb.japigen;

import org.rocksdb.japigen.function.AnalysedFunction;
import org.rocksdb.japigen.function.Receiver;

import java.util.*;

public class AllClasses {

  private final Map<String, AnalysedClass> analysedClasses = new HashMap<>();
  private final List<AnalysedFunction> orphans = new ArrayList<>();

  public void addFunction(AnalysedFunction function) {
    var functionType =  function.canonicalType();
    var cAPIFunctionPrefix = function.cAPIFunctionPrefix();
    if (function instanceof Receiver receiver && functionType.isPresent()) {
      String functionKey = functionType.get();
      AnalysedClass analysedClass = analysedClasses.get(functionKey);
      if (analysedClass == null) {
        analysedClass = AnalysedClass.of(functionKey, cAPIFunctionPrefix.get());
        analysedClasses.put(functionKey, analysedClass);
      }
      analysedClass.addReceiver(receiver);
    } else {
      orphans.add(function);
    }
  }

  public List<AnalysedClass> all() {
    return new ArrayList<>(analysedClasses.values());
  }
}
