package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.rocksdb.util.ArgUtil;
import org.rocksdb.util.SharedTempFile;

public class SharedTempFileMockMain {
  public static void main(final String[] args) throws IOException, InterruptedException {
    // uncouple precise start time from other processes
    // otherwise they all create their own temp
    final Random random = new Random();
    Thread.sleep(random.nextInt(1000));

    String tmpDir = System.getProperty("java.io.tmpdir");
    Map<String, String> argMap = ArgUtil.parseArgs(args);
    if (argMap.containsKey("tmpdir")) {
      tmpDir = argMap.get("tmpdir");
    }

    SharedTempFile.Instance instance =
        new SharedTempFile.Instance(tmpDir, "rocksdbmock", "789", "jnilib");
    SharedTempFile sharedTemp = instance.create();
    System.err.println(sharedTemp + " created/found");
    Path content;
    try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
      content = sharedTemp.getContent();
      assertThat(Files.exists(content)).isTrue();
      try (BufferedReader shared =
               new BufferedReader(new InputStreamReader(Files.newInputStream(content)));
          BufferedReader resource = SharedTempFileLoaderTest.mockContentReader()) {
        compare(resource, shared);
      }
      Thread.sleep(random.nextInt(1000));
    }
    System.err.println(sharedTemp + " finished");

    Thread.sleep(2000);
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
}
