package org.rocksdb.japigen;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Hello world!
 */
public class JAPI {

  private static void usage() {
    fatal("Usage: japi --config <config-file>");
  }

  private static void fatal(String message) {
    System.err.println(message);
    System.exit(1);
  }

  public static void main(String[] args) throws IOException {
    // Do the thing

    if (args.length != 2) {
      usage();
    }
    if (!"--config".equals(args[0])) {
      usage();
    }
    String configPath = args[1];
    if (configPath == null || configPath.isBlank()) {
      usage();
    }

    // Load the config path
    ObjectMapper objectMapper = new ObjectMapper();
    Config config = null;
    try {
      config = JSONFile.load(objectMapper, configPath, Config.class);
    } catch (IOException e) {
      fatal(String.format("Error: japi --config %s could not be parsed %s", configPath, e.getMessage()));
    }
    Generator.generate(config);
  }
}
