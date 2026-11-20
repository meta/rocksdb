package org.rocksdb.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * A mechanism for sharing a single instance of a temporary file
 * The file persists (and uses storage space) only as long as the longest lived of its users.
 * <p>
 *   The implementation uses the java `FileLock` mechanism to mediate access, and implements what is
 *   effectively shared locks underneath that, with an empty file created by each locking process.
 *   Concurrent processes can compete to create the containing directory (expected to be in the
 * temporary space) and to create the "lock" file which is used via `FileLock` as the exclusive lock
 * on the contents of the directory. When a process (briefly) holds the exclusive lock, it must
 * check to discover whether the shared contents have been instantiated. If they have not, it has a
 * mechanism to do this. Once the shared contents are instantiated, or if they already exist, a
 * unique-to-the-process lock file is added to the directory, to denote that the process has a
 * shared lock on the contents. At this point the exclusive `FileLock` is released. When a process
 * has reached the point of holding a shared lock (by means of the locking file) it can read the
 * content file safely, knowing that it will not be removed. A process will typically register a
 * shutdown hook to release a `SharedTempFile.Lock` when it exits; it could choose to do sooner if
 * it did not need the resource at this point. The `unlock()` of a shared lock will acquire the
 * exclusive file lock, remove the shared file, and check whether the last shared file has been
 * removed. If it has been, the content file will be removed also. The containing directory, and the
 * exclusive lock file, are never removed within the `SharedTempFile` code, and if a new process
 * requests a lock, it can re-use the directory successfully. If a process is hard killed, and
 * therefore its shutdown hook is not called, its shared lock file will persist, and there will
 * never be a final unlock to remove the content file. This is unfortunate, but at most 1 content
 * file will ever be left behind; the same one will always continue to be re-shared when further
 * processes request the same resource.
 * </p>
 * <p>
 *     Shared temp files are used so that only a single temporary instance of the RocksJNI shared
 * library is created when it is loaded from within the RocksJNI jar. This prevents the infinite
 * destruction of storage (it has been observed) when each VM which creates its own copy, and is
 * later killed by a signal instead of terminating cleanly. In the shared temp file model, the
 * jnilib will persist, but will be re-used by other VMs.
 * </p>
 */
public class SharedTempFile {
  private final static String INSTANCE_LOCK = ".instance-lock";
  private final static String DIR_LOCK = ".dir-lock";

  private final Instance instance;

  private final Path directory;
  private final Path directoryLock;
  private final Path content;
  private Path instanceLock;

  private SharedTempFile(final Instance instance, final Path directory) {
    this.instance = instance;
    this.directory = directory;
    this.directoryLock = directory.resolve(instance.prefix + DIR_LOCK);
    this.content = directory.resolve(instance.prefix + "." + instance.suffix);
  }

  /**
   * Handler for a shared temp file of a particular prefix and suffix
   */
  public static class Instance {
    private final String tmpDir;
    private final String prefix;
    private final String digest;
    private final String suffix;

    /**
     * Create the shared temp file's containing directory
     * It may already exist, which is fine.
     *
     * Either way, once it exists, ensure that the contained lock file is also created
     *
     * @return the new shared temp file object
     * @throws IOException if there is an unexpected problem creating
     */
    @SuppressWarnings("PMD.EmptyCatchBlock")
    public SharedTempFile create() throws IOException {
      final Path directory = Paths.get(tmpDir).resolve(prefix + digest);
      try {
        Files.createDirectory(directory);
      } catch (FileAlreadyExistsException e) {
        // Already created
      }
      return new SharedTempFile(this, directory).ensureCreated();
    }

    public Instance(
        final String tmpDir, final String prefix, final String uniquifier, final String suffix) {
      this.tmpDir = tmpDir != null ? tmpDir : System.getProperty("java.io.tmpdir");
      this.prefix = prefix;
      this.digest = digestUniquifier(uniquifier);
      this.suffix = suffix;
    }
  }

  public class Lock implements AutoCloseable {
    @Override
    public void close() {
      unlock();
    }
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  private SharedTempFile ensureCreated() throws IOException {
    Path dirLock = directory.resolve(instance.prefix + DIR_LOCK);
    try {
      Files.createFile(dirLock);
    } catch (FileAlreadyExistsException e) {
      // Fine. Just needs to be created once.
    }

    return this;
  }

  private static String digestUniquifier(final String uniquifier) {
    try {
      byte[] digest =
          MessageDigest.getInstance("MD5").digest(uniquifier.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format("%02X", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not digest library resource name", e);
    }
  }

  /**
   * Lock the content as in use by the current SharedTempFile instance,
   * and create the content if it does not already exist as a file in the expected directory.
   *
   * @param contentCreator return a stream from which the content can be read if it is to be created
   * @throws IOException if a file system error occurs
   *
   * @return an autocloseable lock
   */
  public Lock lock(Callable<InputStream> contentCreator) throws IOException {
    try (FileChannel fc = FileChannel.open(directoryLock, StandardOpenOption.WRITE)) {
      try (FileLock ignored = fc.lock()) {
        instanceLock = Files.createTempFile(directory, instance.prefix, INSTANCE_LOCK);
        if (!Files.exists(content)) {
          Files.createFile(content);
          try (InputStream is = contentCreator.call()) {
            Files.copy(is, content, StandardCopyOption.REPLACE_EXISTING);
          } catch (Exception e) {
            throw new RuntimeException(
                "Unable to create content for SharedTempFile " + instance.prefix, e);
          }
        }
      }
    }

    return new Lock();
  }

  /**
   * Unlock the content, as it is no longer in use by the current SharedTempFile instance.
   * If this is the last user of the content (no other instance lock) delete it all.
   */
  private void unlock() {
    if (Files.exists(directory)) {
      try (FileChannel fc = FileChannel.open(directoryLock, StandardOpenOption.WRITE)) {
        try (FileLock ignored = fc.lock()) {
          Files.delete(instanceLock);
          // prefixNNN.lock - one instance lock for every VM currently locking the content file
          List<Path> lockFiles = new ArrayList<>();
          try (Stream<Path> children = Files.walk(directory, 1)) {
            children.forEach(path -> {
              Path fileName = path.getFileName();
              String name = fileName.toString();
              if (name.startsWith(instance.prefix) && name.endsWith(INSTANCE_LOCK)) {
                lockFiles.add(fileName);
              }
            });
          }
          if (lockFiles.isEmpty()) {
            // No VMs are locking this SharedTempFile, so we can delete it
            if (!Files.exists(content)) {
              throw new RuntimeException(
                  "SharedTempFile " + instance.prefix + " contents not found for deletion");
            }
            Files.delete(content);

            // At this point we have removed the content, but it is difficult to remove the dir
            // What happens to the contained lock file when the dir is renamed ?
            // This is presumably implementation dependent (e.g. advisory locking or not)
            // SO ? We just leave it hanging around, because:
            // 1. It's in a temporary so in theory it should just get deleted eventually
            // 2. It doesn't take up much space,  which was the point of the exercise.
            // 3. It may/will get used again anyway when found by `search()`
          }
        } catch (IOException e) {
          throw new RuntimeException(
              "SharedTempFile " + instance.prefix + " could not be locked", e);
        }
      } catch (IOException e) {
        throw new RuntimeException("SharedTempFile " + instance.prefix + " could not be opened", e);
      }
    }
  }

  public Path getContent() {
    return content;
  }

  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + "]{" + directory + "}";
  }
}
