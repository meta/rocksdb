// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import org.rocksdb.util.Environment;
import org.rocksdb.util.SharedTempFile;

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
      System.load(loadLibraryFromJarToTemp(tmpDir).toString());
      initialized = true;
    }
  }

  @SuppressWarnings({"PMD.UseProperClassLoader", "PMD.CloseResource"})
  private InputStream libraryFromJar() {
    InputStream is = getClass().getClassLoader().getResourceAsStream(jniLibraryFileName);
    if (is == null) {
      is = getClass().getClassLoader().getResourceAsStream(fallbackJniLibraryFileName);
    }
    if (is == null) {
      throw new RuntimeException(jniLibraryFileName + " was not found inside JAR.");
    }
    return is;
  }

  @SuppressWarnings("PMD.UseProperClassLoader")
  private String libraryResourcePath() {
    URL resource = getClass().getClassLoader().getResource(jniLibraryFileName);
    if (resource == null) {
      resource = getClass().getClassLoader().getResource(fallbackJniLibraryFileName);
    }
    if (resource == null) {
      throw new RuntimeException(jniLibraryFileName + " was not found inside JAR.");
    }
    return resource.getFile();
  }

  @SuppressWarnings({"PMD.UseTryWithResources", "PMD.CloseResource"})
  Path loadLibraryFromJarToTemp(final String tmpDir) throws IOException {
    String prefix = tempFilePrefix;
    String suffix = tempFileSuffix;
    if (jniLibraryFileName != null) {
      String[] split = jniLibraryFileName.split("\\.");
      if (split.length == 2) {
        prefix = split[0];
        suffix = split[1];
      }
    }
    SharedTempFile.Instance instance =
        new SharedTempFile.Instance(tmpDir, prefix, libraryResourcePath(), suffix);
    SharedTempFile sharedTemp = instance.create();
    SharedTempFile.Lock lock = sharedTemp.lock(this::libraryFromJar);
    Runtime.getRuntime().addShutdownHook(new Thread(lock::close));
    return sharedTemp.getContent();
  }

  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
