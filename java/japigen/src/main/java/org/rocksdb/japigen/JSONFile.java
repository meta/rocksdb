package org.rocksdb.japigen;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

public class JSONFile {

  public static <T> T load(ObjectMapper objectMapper, String filePath, Class<T> clazz) throws IOException {
    java.io.File jsonFile = Path.of(filePath).toFile();
    if (!jsonFile.exists()) {
      throw new FileNotFoundException(String.format("Does not exist file %s", filePath));
    }
    if (!jsonFile.isFile()) {
      throw new FileNotFoundException(String.format("Not a regular file %s", filePath));
    }
    try {
      return objectMapper.readValue(jsonFile, clazz);
    } catch (JsonMappingException jme) {
      throw new IOException(String.format("File %s could not be parsed %s", filePath, jme.getMessage()));
    }
  }

  public static File checkIsDirectory(String filePath) throws FileNotFoundException {
    java.io.File directory = Path.of(filePath).normalize().toFile();
    if (!directory.exists()) {
      throw new FileNotFoundException(String.format("Does not exist file %s", filePath));
    }
    if (!directory.isDirectory()) {
      throw new FileNotFoundException(String.format("Not a directory file %s", filePath));
    }
    return directory;
  }
}
