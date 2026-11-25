// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.Environment;

public class NativeLibraryLoaderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void tempFolder() throws IOException {
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    final Path path = Paths.get(temporaryFolder.getRoot().getAbsolutePath());
    final String libname = Environment.getJniLibraryFileName("rocksdb");
    AtomicBoolean found = new AtomicBoolean(false);
    try (Stream<Path> children = Files.walk(path, 2)) {
      children.forEach(child -> {
        Path fileName = child.getFileName();
        String name = fileName.toString();
        if (libname.equals(name)) {
          found.set(true);
        }
      });
    }
    assertThat(found).as("Expected to find " + libname + " within " + path).isTrue();
  }

  @Test
  public void overridesExistingLibrary() throws IOException {
    final File first = NativeLibraryLoader.getInstance()
                           .loadLibraryFromJarToTemp(temporaryFolder.getRoot().getAbsolutePath())
                           .toFile();
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    assertThat(first.exists()).isTrue();
  }
}
