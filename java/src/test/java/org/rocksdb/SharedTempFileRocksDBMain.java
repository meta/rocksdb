package org.rocksdb;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.rocksdb.util.ArgUtil;

public class SharedTempFileRocksDBMain {
  public static void main(final String[] args) throws InterruptedException {
    boolean waitKill = false;
    Map<String, String> argMap = ArgUtil.parseArgs(args);
    if (argMap.containsKey("waitkill")) {
      waitKill = Boolean.parseBoolean(argMap.get("waitkill"));
    }

    String tmpDir = System.getProperty("java.io.tmpdir");
    if (argMap.containsKey("tmpdir")) {
      tmpDir = argMap.get("tmpdir");
    }

    try {
      RocksDB.loadLibrary();

      Path dbDir = Files.createTempDirectory(Paths.get(tmpDir), "rocksdbdir");
      try (final RocksDB db = RocksDB.open(dbDir.toFile().getAbsolutePath())) {
        db.put("key".getBytes(), "value".getBytes());
        if (!Arrays.equals("value".getBytes(), db.get("key".getBytes()))) {
          System.err.println("Get: key != value");
          System.exit(2);
        }
      } finally {
        purgeDirectory(dbDir.toFile());
        Files.delete(dbDir);
      }
    } catch (Exception e) {
      System.err.println("Exit with exception: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }

    if (waitKill) {
      Thread.sleep(5 * 60 * 1000);
    }

    System.exit(0);
  }

  static void purgeDirectory(File dir) {
    for (File file : Objects.requireNonNull(dir.listFiles())) {
      if (file.isDirectory())
        purgeDirectory(file);
      file.delete();
    }
  }
}
