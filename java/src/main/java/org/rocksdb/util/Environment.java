// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb.util;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

public class Environment {
  @SuppressWarnings("FieldMayBeFinal")
  private static String OS = System.getProperty("os.name").toLowerCase(Locale.getDefault());
  @SuppressWarnings("FieldMayBeFinal")
  private static String ARCH = System.getProperty("os.arch").toLowerCase(Locale.getDefault());
  @SuppressWarnings("FieldMayBeFinal")
  private static String MUSL_ENVIRONMENT = System.getenv("ROCKSDB_MUSL_LIBC");

  private static final String LIBC_MUSL_PREFIX = "libc.musl";

  private static final String SPARCV9 = "sparcv9";

  /**
   * Will be lazily initialised by {@link #isMuslLibc()} instead of the previous static
   * initialisation. The lazy initialisation prevents Windows from reporting suspicious behaviour of
   * the JVM attempting IO on Unix paths.
   */
  private static Boolean MUSL_LIBC = null;

  public static boolean isAarch64() {
    return ARCH.contains("aarch64");
  }

  public static boolean isPowerPC() {
    return ARCH.contains("ppc");
  }

  public static boolean isS390x() {
    return ARCH.contains("s390x");
  }

  public static boolean isRiscv64() {
    return ARCH.contains("riscv64");
  }

  public static boolean isWindows() {
    return (OS.contains("win"));
  }

  public static boolean isFreeBSD() {
    return (OS.contains("freebsd"));
  }

  public static boolean isMac() {
    return (OS.contains("mac"));
  }

  public static boolean isAix() {
    return OS.contains("aix");
  }
  
  public static boolean isUnix() {
    return OS.contains("nix") ||
        OS.contains("nux");
  }

  /**
   * Determine if the environment has a musl libc.
   *
   * @return true if the environment has a musl libc, false otherwise.
   */
  public static boolean isMuslLibc() {
    if (MUSL_LIBC == null) {
      MUSL_LIBC = initIsMuslLibc();
    }
    return MUSL_LIBC;
  }

  /**
   * Determine if the environment has a musl libc.
   *
   * The initialisation counterpart of {@link #isMuslLibc()}.
   *
   * Intentionally package-private for testing.
   *
   * @return true if the environment has a musl libc, false otherwise.
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  static boolean initIsMuslLibc() {
    // consider explicit user setting from environment first
    if ("true".equalsIgnoreCase(MUSL_ENVIRONMENT)) {
      return true;
    }
    if ("false".equalsIgnoreCase(MUSL_ENVIRONMENT)) {
      return false;
    }

    // check if ldd indicates a muslc lib
    try {
      final Process p =
          new ProcessBuilder("/usr/bin/env", "sh", "-c", "ldd /usr/bin/env | grep -q musl").start();
      if (p.waitFor() == 0) {
        return true;
      }
    } catch (final IOException | InterruptedException e) {
      // do nothing, and move on to the next check
    }

    final File lib = new File("/lib");
    if (lib.exists() && lib.isDirectory() && lib.canRead()) {
      // attempt the most likely musl libc name first
      final String possibleMuslcLibName;
      if (isPowerPC()) {
        possibleMuslcLibName = "libc.musl-ppc64le.so.1";
      } else if (isAarch64()) {
        possibleMuslcLibName = "libc.musl-aarch64.so.1";
      } else if (isS390x()) {
        possibleMuslcLibName = "libc.musl-s390x.so.1";
      } else {
        possibleMuslcLibName = "libc.musl-x86_64.so.1";
      }
      final File possibleMuslcLib = new File(lib, possibleMuslcLibName);
      if (possibleMuslcLib.exists() && possibleMuslcLib.canRead()) {
        return true;
      }

      // fallback to scanning for a musl libc
      final File[] libFiles = lib.listFiles();
      if (libFiles == null) {
        return false;
      }
      for (final File f : libFiles) {
        if (f.getName().startsWith(LIBC_MUSL_PREFIX)) {
          return true;
        }
      }
    }

    return false;
  }

  public static boolean isOpenBSD() {
    return (OS.contains("openbsd"));
  }

  public static boolean is64Bit() {
    if (ARCH.contains(SPARCV9)) {
      return true;
    }
    return (ARCH.indexOf("64") > 0);
  }

  public static String getSharedLibraryName(final String name) {
    return name + "jni";
  }

  public static String getSharedLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getSharedLibraryName(name), true);
  }

  /**
   * Get the name of the libc implementation
   *
   * @return the name of the implementation,
   *    or null if the default for that platform (e.g. glibc on Linux).
   */
  public static /* @Nullable */ String getLibcName() {
    if (isMuslLibc()) {
      return "musl";
    } else {
      return null;
    }
  }

  private static String getLibcPostfix() {
    final String libcName = getLibcName();
    if (libcName == null) {
      return "";
    }
    return "-" + libcName;
  }

  public static String getJniLibraryName(final String name) {
    if (isUnix()) {
      final String arch = is64Bit() ? "64" : "32";
      if (isPowerPC() || isAarch64() || isRiscv64()) {
        return String.format("%sjni-linux-%s%s", name, ARCH, getLibcPostfix());
      } else if (isS390x()) {
        return String.format("%sjni-linux-%s", name, ARCH);
      } else {
        return String.format("%sjni-linux%s%s", name, arch, getLibcPostfix());
      }
    } else if (isMac()) {
      if (is64Bit()) {
        final String arch;
        if (isAarch64()) {
          arch = "arm64";
        } else {
          arch = "x86_64";
        }
        return String.format("%sjni-osx-%s", name, arch);
      } else {
        return String.format("%sjni-osx", name);
      }
    } else if (isFreeBSD()) {
      return String.format("%sjni-freebsd%s", name, is64Bit() ? "64" : "32");
    } else if (isAix() && is64Bit()) {
      return String.format("%sjni-aix64", name);
    } else if (isWindows() && is64Bit()) {
      return String.format("%sjni-win64", name);
    } else if (isOpenBSD()) {
      return String.format("%sjni-openbsd%s", name, is64Bit() ? "64" : "32");
    }

    throw new UnsupportedOperationException(String.format("Cannot determine JNI library name for ARCH='%s' OS='%s' name='%s'", ARCH, OS, name));
  }

  public static /*@Nullable*/ String getFallbackJniLibraryName(final String name) {
    if (isMac() && is64Bit()) {
      return String.format("%sjni-osx", name);
    }
    return null;
  }

  public static String getJniLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getJniLibraryName(name), false);
  }

  public static /*@Nullable*/ String getFallbackJniLibraryFileName(final String name) {
    final String fallbackJniLibraryName = getFallbackJniLibraryName(name);
    if (fallbackJniLibraryName == null) {
      return null;
    }
    return appendLibOsSuffix("lib" + fallbackJniLibraryName, false);
  }

  private static String appendLibOsSuffix(final String libraryFileName, final boolean shared) {
    if (isUnix() || isAix() || isFreeBSD() || isOpenBSD()) {
      return libraryFileName + ".so";
    } else if (isMac()) {
      return libraryFileName + (shared ? ".dylib" : ".jnilib");
    } else if (isWindows()) {
      return libraryFileName + ".dll";
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryExtension() {
    if (isWindows()) {
      return ".dll";
    }
    return (isMac()) ? ".jnilib" : ".so";
  }
}
