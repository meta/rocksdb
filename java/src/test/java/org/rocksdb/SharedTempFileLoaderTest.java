package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.SharedTempFile;

public class SharedTempFileLoaderTest {
  private final static int SIGKILL_CODE = 128 + 9;
  private final static int SIGTERM_CODE = 128 + 15;

  final static Random random = new Random();

  static InputStream mockContent() {
    ClassLoader classloader = NativeLibraryLoader.class.getClassLoader();
    InputStream is = classloader.getResourceAsStream("shared-temp-file.txt");
    return new BufferedInputStream(is);
  }

  static BufferedReader mockContentReader() {
    ClassLoader classloader = NativeLibraryLoader.class.getClassLoader();
    InputStream is = classloader.getResourceAsStream("shared-temp-file.txt");
    return new BufferedReader(new InputStreamReader(is));
  }

  static private void compare(BufferedReader expected, BufferedReader actual) throws IOException {
    String expectedLine = expected.readLine();
    String actualLine = actual.readLine();
    while (expectedLine != null) {
      assertThat(actualLine).isEqualTo(expectedLine);
      expectedLine = expected.readLine();
      actualLine = actual.readLine();
    }
    assertThat(actualLine).isNull();
  }

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String getTmpDir() {
    return temporaryFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void sharedTempFresh() throws IOException {
    String prefix = "rocksdbjnitest__" + random.nextLong() + "__";
    SharedTempFile.Instance instance =
        new SharedTempFile.Instance(getTmpDir(), prefix, "123", "jnilib");
    SharedTempFile sharedTemp = instance.create();
    System.err.println("sharedTempFresh() created: " + sharedTemp);
    Path content;
    try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
      content = sharedTemp.getContent();
      assertThat(Files.exists(content)).isTrue();
      try (BufferedReader shared =
               new BufferedReader(new InputStreamReader(Files.newInputStream(content)));
          BufferedReader resource = mockContentReader()) {
        compare(resource, shared);
      }
    }
    assertThat(Files.exists(content)).isFalse();
  }

  @Test
  public void sharedTempExists() throws IOException {
    String prefix = "rocksdbjnitest__" + random.nextLong() + "__";
    SharedTempFile.Instance instance =
        new SharedTempFile.Instance(getTmpDir(), prefix, "456", "jnilib");
    SharedTempFile sharedTemp = instance.create();
    System.err.println("sharedTempExists() created: " + sharedTemp);
    Path content;

    try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
      content = sharedTemp.getContent();
      assertThat(Files.exists(content)).isTrue();
      try (BufferedReader shared =
               new BufferedReader(new InputStreamReader(Files.newInputStream(content)));
          BufferedReader resource = mockContentReader()) {
        compare(resource, shared);
      }

      Path content2;
      SharedTempFile sharedTemp2 = instance.create();
      try (SharedTempFile.Lock ignored2 = sharedTemp2.lock(SharedTempFileLoaderTest::mockContent)) {
        content2 = sharedTemp2.getContent();
        assertThat(content2).isEqualTo(content);
        assertThat(Files.exists(content2)).isTrue();
        try (BufferedReader shared =
                 new BufferedReader(new InputStreamReader(Files.newInputStream(content)));
            BufferedReader resource = mockContentReader()) {
          compare(resource, shared);
        }
      }

      assertThat(Files.exists(content))
          .as("Content remains after unlock of 1/2 resources")
          .isTrue();
    }
    assertThat(Files.exists(content)).as("Content deleted after unlock of 2/2 resources").isFalse();
  }

  @Test
  public void openManySharedTemp() throws IOException {
    openMany("org.rocksdb.SharedTempFileMockMain", testClassPathWithJar(), null, Kill.None,
        "--tmpdir=" + getTmpDir());
  }

  @Test
  public void openManyRocksDBKill() throws IOException {
    // Do this to make processes wait a long time, and we will kill them exp;icitly before they exit
    openMany("org.rocksdb.SharedTempFileRocksDBMain", testClassPathWithJar(), null, Kill.Term,
        "--waitkill=true", "--tmpdir=" + getTmpDir());
  }

  @Test
  public void openManyRocksDBWait() throws IOException {
    // Do this to (1) not kill the process and let them exit by themselves
    openMany("org.rocksdb.SharedTempFileRocksDBMain", testClassPathWithJar(), null, Kill.None,
        "--tmpdir=" + getTmpDir());
  }

  /**
   * Validate that we can set java.library.path, and then not require the JAR
   *
   * @throws IOException
   */
  @Test
  public void openManyRocksDBWaitNoJar() throws IOException {
    // Like openManyRocksDBWait but check it can load the JNI library from java.library.path

    // Add definition of java.library.path to where it can find the shared library
    // String ld_library_path = System.getenv().get("PWD") + "/target";
    String ld_library_path = "target";
    Map<String, String> definitions = new HashMap<>();
    definitions.put("java.library.path", ld_library_path);

    openMany("org.rocksdb.SharedTempFileRocksDBMain", testClassPath(), definitions, Kill.None,
        "--tmpdir=" + getTmpDir());
  }

  enum Kill { None, Term, Kill }
  ;

  /**
   * A base list for forming the classpath to run tests
   * This does NOT include a built JAR,
   * so the NativeLibraryLoader will be unable to load the library from it.
   *
   * @return a path list of class, test class and library directories
   */
  private List<String> testClassPath() {
    final List<String> cp = new ArrayList<>();
    cp.add("target/test-classes");
    cp.add("target/classes");
    cp.add("test-libs/*");

    return cp;
  }

  /**
   * A list for forming the classpath to run tests
   * Any built JAR should appear at <code>target/name-of-jar</code>
   * so the NativeLibraryLoader will be able to load the library from it.
   *
   * @return the path list
   */
  private List<String> testClassPathWithJar() {
    final List<String> cp = testClassPath();
    cp.add("target/*");

    return cp;
  }

  /**
   * Helper to create multiples subprocesses and have them run a java class main
   *
   * @param mainClass java class to run main of
   * @param kill should we kill the processes after a short delay ?
   * @param args to pass to the main() method
   * @throws IOException if there is a problem creating/communication with the processes
   */
  private void openMany(final String mainClass, final List<String> cp,
      final Map<String, String> defns, final Kill kill, final String... args) throws IOException {
    final int DB_COUNT = 50;
    List<Process> processes = new ArrayList<>();
    List<BufferedReader> readers = new ArrayList<>();
    List<List<String>> lines = new ArrayList<>();
    List<Integer> exitCodes = new ArrayList<>();

    for (int i = 0; i < DB_COUNT; i++) {
      StringBuilder arguments = new StringBuilder();
      for (String arg : args) {
        arguments.append(" ").append(arg);
      }
      StringBuilder classpath = new StringBuilder();
      if (cp != null) {
        for (String element : cp) {
          classpath.append(element).append(File.pathSeparator);
        }
      }
      int length = classpath.length();
      if (length > 0) {
        // remove trailing ":"
        classpath.deleteCharAt(length - 1);
      }
      if (classpath.length() > 0) {
        classpath.insert(0, "-cp ");
      }
      StringBuilder definitions = new StringBuilder();
      if (defns != null) {
        for (Map.Entry<String, String> entry : defns.entrySet()) {
          definitions.append("-D")
              .append(entry.getKey())
              .append("=")
              .append(entry.getValue())
              .append(" ");
        }
      }
      final String invocation = "java " + definitions + classpath + " " + mainClass + arguments;
      Process process = Runtime.getRuntime().exec(invocation);
      processes.add(process);
      BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      readers.add(err);
      List<String> processLine = new ArrayList<>();
      processLine.add("|--------|" + i + "|--------|");
      processLine.add(invocation);
      lines.add(processLine);
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    boolean finished = false;
    while (!finished) {
      finished = true;
      for (int i = 0; i < DB_COUNT; i++) {
        if (readers.get(i).ready()) {
          finished = false;
          lines.get(i).add(readers.get(i).readLine());
        }
      }
    }

    int expectedExitCode;
    switch (kill) {
      case Kill:
        expectedExitCode = SIGKILL_CODE;
        break;
      case Term:
        expectedExitCode = SIGTERM_CODE;
        break;
      case None:
      default:
        expectedExitCode = 0;
    }
    for (Process process : processes) {
      switch (kill) {
        case Kill:
          process.destroyForcibly();
          break;
        case Term:
          process.destroy();
          break;
        case None:
        default:
          break;
      }
    }

    for (int i = 0; i < DB_COUNT; i++) {
      try {
        processes.get(i).waitFor();
        int exitCode = processes.get(i).exitValue();
        exitCodes.add(exitCode);
      } catch (InterruptedException ie) {
        throw new RuntimeException("Process interrupted");
      }
    }

    for (int i = 0; i < DB_COUNT; i++) {
      lines.get(i).add("|------------------|");
    }

    boolean ok = true;
    for (int i = 0; i < DB_COUNT; i++) {
      if (exitCodes.get(i) != expectedExitCode) {
        ok = false;
        System.err.println("Process " + i + " failed: " + exitCodes.get(i));
        for (String line : lines.get(i)) {
          System.err.println(line);
        }
      }
    }
    assertThat(ok).as("all subprocesses return success").isTrue();
  }
}
