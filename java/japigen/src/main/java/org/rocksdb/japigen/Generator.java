package org.rocksdb.japigen;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.japigen.function.AnalysedFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Generator {

  private final Config config;

  public Generator(Config config) {
    this.config = config;
  }

  public static void generate(Config config) throws IOException {
    new Generator(config).generate();
  }

  private void generate() throws IOException {
    AllFunctions functions = analyseFunctions();
    AllClasses classes = new AllClasses();
    for (AnalysedFunction function : functions.all()) {
      classes.addFunction(function);
    }

    for (AnalysedClass analysedClass : classes.all()) {
      boolean createConstructor = !config.noConstructor().contains(analysedClass.canonicalType);
      StringBuilder sb = new StringBuilder();
      analysedClass.formatClass(sb, config.jextractClass(), config.targetPackage(), createConstructor);
      JAPIFiles.writeJAPIFile(Path.of(config.targetSrcDir()), config.targetPackage(), analysedClass.canonicalType, sb);
    }
  }

  private AllFunctions analyseFunctions() throws IOException {
    File jsonDir = JSONFile.checkIsDirectory(config.generatedJSONDir());
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    AllFunctions allFunctions = new AllFunctions();
    for (File json : Objects.requireNonNull(jsonDir.listFiles((_, name) ->
      name.endsWith(".json") && name.startsWith("c_generated")
    ))) {
      if (!json.isFile()) {
        System.err.printf("JSON file %s is not a regular file%n", json.getName());
        continue;
      }
      System.err.printf("%s processed JSON %s\n", getClass().getName(), json);
      List<CFunction> functions = objectMapper.readValue(json, new TypeReference<>() {
      });
      for (var function : functions) {
        allFunctions.analyse(json.getName(), function);
      }
    }
    return allFunctions;
  }
}
