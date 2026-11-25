package org.rocksdb.japigen;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class JAPIFiles {

  static String copyrightPrefix =
    """
      //  Copyright (c) Meta Platforms, Inc. and affiliates.
      //  This source code is licensed under both the GPLv2 (found in the
      //  COPYING file in the root directory) and Apache 2.0 License
      //  (found in the LICENSE.Apache file in the root directory).
      
      """;

  public static void writeJAPIFile(Path targetSrcPath, String targetPackage, String className, StringBuilder contents) throws IOException {
    Files.createDirectories(targetSrcPath);
    Path packagePath = targetSrcPath;
    for (String part : targetPackage.split("\\.")) {
      packagePath = packagePath.resolve(Path.of(part));
    }
    Files.createDirectories(packagePath);
    Path classFilePath = packagePath.resolve(className + ".java");
    try (FileWriter fileWriter = new FileWriter(classFilePath.toString(), StandardCharsets.UTF_8, false)) {
      fileWriter.write(copyrightPrefix);
      fileWriter.write(contents.toString());
    }
  }
}
