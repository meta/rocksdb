// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnvironmentTest {
  private static final String ARCH_FIELD_NAME = "ARCH";
  private static final String OS_FIELD_NAME = "OS";

  private static final String MUSL_ENVIRONMENT_FIELD_NAME = "MUSL_ENVIRONMENT";
  private static final String MUSL_LIBC_FIELD_NAME = "MUSL_LIBC";

  private static String INITIAL_OS;
  private static String INITIAL_ARCH;
  private static String INITIAL_MUSL_ENVIRONMENT;
  private static Boolean INITIAL_MUSL_LIBC;

  @BeforeClass
  public static void saveState() {
    INITIAL_ARCH = getEnvironmentClassField(ARCH_FIELD_NAME);
    INITIAL_OS = getEnvironmentClassField(OS_FIELD_NAME);
    INITIAL_MUSL_LIBC = getEnvironmentClassField(MUSL_LIBC_FIELD_NAME);
    INITIAL_MUSL_ENVIRONMENT = getEnvironmentClassField(MUSL_ENVIRONMENT_FIELD_NAME);
  }

  @Test
  public void mac32() {
    setEnvironmentClassFields("mac", "32");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".jnilib");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-osx.jnilib");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.dylib");
  }

  @Test
  public void mac64_x86_64() {
    setEnvironmentClassFields("mac", "x86_64");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".jnilib");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-osx-x86_64.jnilib");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-osx.jnilib");
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.dylib");
  }

  @Test
  public void macAarch64() {
    setEnvironmentClassFields("mac", "aarch64");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).isEqualTo(".jnilib");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-osx-arm64.jnilib");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-osx.jnilib");
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).isEqualTo("librocksdbjni.dylib");
  }

  @Test
  public void nix32() {
    // Linux
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Linux", "32");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux32.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
    // Linux musl-libc (Alpine)
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, true);
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux32-musl.so");
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
    // UNIX
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Unix", "32");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux32.so");
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void aix32() {
    // AIX
    setEnvironmentClassFields("aix", "32");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).isEqualTo("blah");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
  }

  @Test
  public void nix64() {
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Linux", "x64");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux64.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
    // Linux musl-libc (Alpine)
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, true);
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux64-musl.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
    // UNIX
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Unix", "x64");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-linux64.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
    // AIX
    setEnvironmentClassFields("aix", "x64");
    assertThat(Environment.isWindows()).isFalse();
    assertThat(Environment.getJniLibraryExtension()).
        isEqualTo(".so");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni-aix64.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
        isEqualTo("librocksdbjni.so");
  }

  @Test
  public void detectWindows(){
    setEnvironmentClassFields("win", "x64");
    assertThat(Environment.isWindows()).isTrue();
  }

  @Test
  public void win64() {
    setEnvironmentClassFields("win", "x64");
    assertThat(Environment.isWindows()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).
      isEqualTo(".dll");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
      isEqualTo("librocksdbjni-win64.dll");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
      isEqualTo("librocksdbjni.dll");
  }

  @Test
  public void winArm64() {
    setEnvironmentClassFields("win", "arm64");
    assertThat(Environment.isWindows()).isTrue();
    assertThat(Environment.isAarch64()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).
      isEqualTo(".dll");
    assertThat(Environment.getJniLibraryFileName("rocksdb")).
      isEqualTo("librocksdbjni-win-arm64.dll");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).
      isEqualTo("librocksdbjni.dll");
  }

  @Test
  public void ppc64le() {
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Linux", "ppc64le");
    assertThat(Environment.isUnix()).isTrue();
    assertThat(Environment.isPowerPC()).isTrue();
    assertThat(Environment.is64Bit()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).isEqualTo(".so");
    assertThat(Environment.getSharedLibraryName("rocksdb")).isEqualTo("rocksdbjni");
    assertThat(Environment.getJniLibraryName("rocksdb")).isEqualTo("rocksdbjni-linux-ppc64le");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-linux-ppc64le.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).isEqualTo("librocksdbjni.so");
    // Linux musl-libc (Alpine)
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, true);
    setEnvironmentClassFields("Linux", "ppc64le");
    assertThat(Environment.isUnix()).isTrue();
    assertThat(Environment.isPowerPC()).isTrue();
    assertThat(Environment.is64Bit()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).isEqualTo(".so");
    assertThat(Environment.getSharedLibraryName("rocksdb")).isEqualTo("rocksdbjni");
    assertThat(Environment.getJniLibraryName("rocksdb")).isEqualTo("rocksdbjni-linux-ppc64le-musl");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-linux-ppc64le-musl.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).isEqualTo("librocksdbjni.so");
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
  }

  @Test
  public void linuxArch64() {
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
    setEnvironmentClassFields("Linux", "aarch64");
    assertThat(Environment.isUnix()).isTrue();
    assertThat(Environment.isAarch64()).isTrue();
    assertThat(Environment.is64Bit()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).isEqualTo(".so");
    assertThat(Environment.getSharedLibraryName("rocksdb")).isEqualTo("rocksdbjni");
    assertThat(Environment.getJniLibraryName("rocksdb")).isEqualTo("rocksdbjni-linux-aarch64");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-linux-aarch64.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).isEqualTo("librocksdbjni.so");
    // Linux musl-libc (Alpine)
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, true);
    setEnvironmentClassFields("Linux", "aarch64");
    assertThat(Environment.isUnix()).isTrue();
    assertThat(Environment.isAarch64()).isTrue();
    assertThat(Environment.is64Bit()).isTrue();
    assertThat(Environment.getJniLibraryExtension()).isEqualTo(".so");
    assertThat(Environment.getSharedLibraryName("rocksdb")).isEqualTo("rocksdbjni");
    assertThat(Environment.getJniLibraryName("rocksdb")).isEqualTo("rocksdbjni-linux-aarch64-musl");
    assertThat(Environment.getJniLibraryFileName("rocksdb"))
        .isEqualTo("librocksdbjni-linux-aarch64-musl.so");
    assertThat(Environment.getFallbackJniLibraryFileName("rocksdb")).isNull();
    assertThat(Environment.getSharedLibraryFileName("rocksdb")).isEqualTo("librocksdbjni.so");
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, false);
  }

  @Test
  public void resolveIsMuslLibc() {
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, null);
    setEnvironmentClassFields("win", "anyarch");
    assertThat(Environment.isUnix()).isFalse();

    // with user input, will resolve to true if set as true. Even on OSs that appear absurd for
    // musl. Users choice
    assertThat(Environment.initIsMuslLibc()).isFalse();
    setEnvironmentClassField(MUSL_ENVIRONMENT_FIELD_NAME, "true");
    assertThat(Environment.initIsMuslLibc()).isTrue();
    setEnvironmentClassField(MUSL_ENVIRONMENT_FIELD_NAME, "false");
    assertThat(Environment.initIsMuslLibc()).isFalse();
  }

  private void setEnvironmentClassFields(final String osName, final String osArch) {
    setEnvironmentClassField(OS_FIELD_NAME, osName);
    setEnvironmentClassField(ARCH_FIELD_NAME, osArch);
  }

  @AfterClass
  public static void restoreState() {
    setEnvironmentClassField(OS_FIELD_NAME, INITIAL_OS);
    setEnvironmentClassField(ARCH_FIELD_NAME, INITIAL_ARCH);
    setEnvironmentClassField(MUSL_ENVIRONMENT_FIELD_NAME, INITIAL_MUSL_ENVIRONMENT);
    setEnvironmentClassField(MUSL_LIBC_FIELD_NAME, INITIAL_MUSL_LIBC);
  }

  @SuppressWarnings("unchecked")
  private static <T> T getEnvironmentClassField(final String fieldName) {
    final Field field;
    try {
      field = Environment.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      /* Fails in JDK 13; and not needed unless fields are final
      final Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      */
      return (T)field.get(null);
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void setEnvironmentClassField(final String fieldName, final Object value) {
    final Field field;
    try {
      field = Environment.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      /* Fails in JDK 13; and not needed unless fields are final
      final Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      */
      field.set(null, value);
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
