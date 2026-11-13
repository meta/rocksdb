package org.rocksdb.util;

import java.util.HashMap;
import java.util.Map;

public class ArgUtil {
  public static Map<String, String> parseArgs(String[] args) {
    final Map<String, String> map = new HashMap<>();
    for (String arg : args) {
      String[] split = arg.trim().split("=");
      if (split.length == 2 && split[0].startsWith("-")) {
        String key = split[0].replaceFirst("-[-]", "").toLowerCase();
        if (!key.isEmpty()) {
          map.put(key, split[1]);
        }
      }
    }
    return map;
  }
}
