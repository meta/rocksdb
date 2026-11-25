// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.rocksdb.util.Environment;

/**
 * This class is used to load the RocksDB shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  //singleton
  private static final NativeLibraryLoader instance = new NativeLibraryLoader();
  private static boolean initialized = false;

  private static final String ROCKSDB_LIBRARY_NAME = "rocksdb";

  private static final String sharedLibraryName =
      Environment.getSharedLibraryName(ROCKSDB_LIBRARY_NAME);
  private static final String jniLibraryName = Environment.getJniLibraryName(ROCKSDB_LIBRARY_NAME);
  private static final /* @Nullable */ String fallbackJniLibraryName =
      Environment.getFallbackJniLibraryName(ROCKSDB_LIBRARY_NAME);
  private static final String jniLibraryFileName =
      Environment.getJniLibraryFileName(ROCKSDB_LIBRARY_NAME);
  private static final /* @Nullable */ String fallbackJniLibraryFileName =
      Environment.getFallbackJniLibraryFileName(ROCKSDB_LIBRARY_NAME);
  private static final String tempFilePrefix = "librocksdbjni";
  private static final String tempFileSuffix = Environment.getJniLibraryExtension();

  /**
   * If you set the System Property ROCKS_JAVA_DEBUG_NLL can be to true
   * messages about attempts to load the native library will be printed
   * to std out.
   */
  private static boolean DEBUG_LOADING =
      "true".equals(System.getProperty("ROCKS_JAVA_DEBUG_NLL", "false"));

  /**
   * Get a reference to the NativeLibraryLoader
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return instance;
  }

  /**
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath
   * {@link org.rocksdb.NativeLibraryLoader#loadLibraryFromJar(java.lang.String)}
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to when loading from the classpath.
   *   If null, or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.SystemPrintln"})
  public synchronized void loadLibrary(final String tmpDir) throws IOException {
    try {
      // try dynamic library
      System.loadLibrary(sharedLibraryName);
      if (DEBUG_LOADING) {
        System.out.println("Loaded shared dynamic library: " + sharedLibraryName);
      }
      return;
    } catch (final UnsatisfiedLinkError ule) {
      // ignore - try from static library
      if (DEBUG_LOADING) {
        System.out.println("Unable to load shared dynamic library: " + sharedLibraryName);
      }
    }

    try {
      // try static library
      System.loadLibrary(jniLibraryName);
      if (DEBUG_LOADING) {
        System.out.println("Loaded shared static library: " + jniLibraryName);
      }
      return;
    } catch (final UnsatisfiedLinkError ule) {
      // ignore - then try static library fallback or from jar
      if (DEBUG_LOADING) {
        System.out.println("Unable to load shared static library: " + jniLibraryName);
      }
    }

    if (fallbackJniLibraryName != null) {
      try {
        // try static library fallback
        System.loadLibrary(fallbackJniLibraryName);
        if (DEBUG_LOADING) {
          System.out.println("Loaded static library fallback: " + fallbackJniLibraryName);
        }
        return;
      } catch (final UnsatisfiedLinkError ule) {
        // ignore - then try from jar
        if (DEBUG_LOADING) {
          System.out.println(
              "Unable to load shared static fallback library: " + fallbackJniLibraryName);
        }
      }
    }

    // try jar
    loadLibraryFromJar(tmpDir);
  }

  /**
   * Attempts to extract the native RocksDB library
   * from the classpath and load it
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to. If null,
   *   or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  void loadLibraryFromJar(final String tmpDir)
      throws IOException {
    if (!initialized) {
      System.load(loadLibraryFromJarToTemp(tmpDir).getAbsolutePath());
      initialized = true;
    }
  }

  private File createTemp(final String tmpDir, final String libraryFileName) throws IOException {
    // create a temporary file to copy the library to
    final File temp;
    if (tmpDir == null || tmpDir.isEmpty()) {
      temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
    } else {
      final File parentDir = new File(tmpDir);
      if (!parentDir.exists()) {
        throw new RuntimeException(
            "Directory: " + parentDir.getAbsolutePath() + " does not exist!");
      }
      temp = new File(parentDir, libraryFileName);
      if (temp.exists() && !temp.delete()) {
        throw new RuntimeException(
            "File: " + temp.getAbsolutePath() + " already exists and cannot be removed.");
      }
      if (!temp.createNewFile()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath() + " could not be created.");
      }
    }
    if (temp.exists()) {
      temp.deleteOnExit();
      return temp;
    } else {
      throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
    }
  }

  @SuppressWarnings({"PMD.UseProperClassLoader", "PMD.UseTryWithResources", "PMD.SystemPrintln"})
  File loadLibraryFromJarToTemp(final String tmpDir) throws IOException {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(jniLibraryFileName)) {
      if (is != null) {
        final File temp = createTemp(tmpDir, jniLibraryFileName);
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return temp;
      } else {
        if (DEBUG_LOADING) {
          System.out.println("Unable to find: " + jniLibraryFileName + " on the classpath");
        }
      }
    }

    if (fallbackJniLibraryFileName == null) {
      throw new RuntimeException(
          jniLibraryFileName + " was not found inside JAR, and there is no fallback.");
    }

    try (InputStream is =
             getClass().getClassLoader().getResourceAsStream(fallbackJniLibraryFileName)) {
      if (is != null) {
        final File temp = createTemp(tmpDir, fallbackJniLibraryFileName);
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return temp;
      } else {
        if (DEBUG_LOADING) {
          System.out.println(
              "Unable to find fallback: " + fallbackJniLibraryFileName + " on the classpath");
        }
      }
    }

    throw new RuntimeException("Neither " + jniLibraryFileName + " or " + fallbackJniLibraryFileName
        + " were found inside the JAR, and there is no fallback.");
  }

  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
