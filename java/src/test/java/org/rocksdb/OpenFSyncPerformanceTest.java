package org.rocksdb;

import java.util.ArrayList;
import org.junit.Test;

/**
 * This test is intended to be run only on MacOS
 * <p>
 * It is used to compare/validate versions of RocksDB built using
 * `make clean jclean; make -j12 rocksdbjava`
 * versus versions built using
 * `make clean jclean; MACOS_IGNORE_FULLFSYNC=1 make -j12 rocksdbjava`
 * The latter run this test about 10x faster on MacOS.
 * @link <a href="https://github.com/facebook/rocksdb/issues/13147">...</a>
 * `MACOS_IGNORE_FULLFSYNC` can be used to build fast test-only versions of RocksDB for efficient CI
 * usage
 */
public class OpenFSyncPerformanceTest {
  @Test
  public void testOpen() throws RocksDBException {
    int count = 100;
    ArrayList<Long> deltas = new ArrayList<>(count);
    long sum = 0;
    for (int i = 0; i < count; i++) {
      long start = System.currentTimeMillis();
      try (RocksDB db = RocksDB.open("test-open-" + i)) {
        long delta = System.currentTimeMillis() - start;
        // System.out.println("RocksDB.open() cost:" + (delta));
        sum += delta;
        deltas.add(delta);
      } finally {
        RocksDB.destroyDB("test-open-" + i, new Options());
      }
    }
    long mean = sum / count;
    long variance = 0;
    for (long d : deltas) {
      variance += (d - mean) * (d - mean);
    }
    double sd = Math.sqrt((double) (variance / count));
    System.out.println(
        "RocksDB.open() mean cost:" + (sum / count) + ", SD: " + sd + ", count: " + count);
  }
}
