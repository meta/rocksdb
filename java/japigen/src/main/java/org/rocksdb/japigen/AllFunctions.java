package org.rocksdb.japigen;

import org.rocksdb.japigen.function.AnalysedFunction;

import java.util.*;

// We want to scan all the functions, split by _, [rocksdb,...,t*] get the class name to use
public class AllFunctions {

  private final List<AnalysedFunction> analysedFunctions = new ArrayList<>();

  private final Map<String, AllClasses> analysedClasses = new HashMap<>();

  public List<AnalysedFunction> all() {
    return new ArrayList<>(analysedFunctions);
  }

  public void analyse(String source, CFunction cFunction) {
    var f = AnalysedFunction.of(source, cFunction);
    analysedFunctions.add(f);
  }
}
