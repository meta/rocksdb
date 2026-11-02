package org.rocksdb.ffm;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Function;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public final class RocksFFM {
  static {
    RocksDB.loadLibrary();
  }

  private static class ArenaHolder {
    protected final Arena arena;

    protected ArenaHolder(Arena arena) {
      this.arena = arena;
    }
  }

  public static DB db(Arena arena) {
    return new DBImpl(arena);
  }

  interface DB {
    DB option();
    OpenDB open(String path);
  }

  private final static class DBImpl extends ArenaHolder implements DB {
    private final MemorySegment dbOptions = rocksdb_c_api_2.rocksdb_options_create();

    private DBImpl(Arena arena) {
      super(arena);
    }

    public DBImpl option() {
      return this;
    }

    @Override
    public OpenDB open(String path) {
      // See RocksDB rules on options structs
      MemorySegment pathName = arena.allocateFrom(path, UTF_8);
      MemorySegment errptr = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
      MemorySegment db = rocksdb_c_api_2.rocksdb_open(dbOptions, pathName, errptr);

      return new OpenDBImpl(arena, db);
    }
  }

  private abstract static class WithDB extends ArenaHolder {
    protected final MemorySegment db;

    protected WithDB(Arena arena, MemorySegment db) {
      super(arena);
      this.db = db;
    }
  }

  interface OpenDB {
    Getter get(MemorySegment key);
    Getter get(String key);
    Putter put(MemorySegment key, MemorySegment value);
    Putter put(String key, MemorySegment value);
  }

  private final static class OpenDBImpl extends WithDB implements OpenDB {
    private OpenDBImpl(Arena arena, MemorySegment db) {
      super(arena, db);
    }

    public Getter get(MemorySegment key) {
      return new GetterImpl(arena, db, key);
    }

    public Getter get(String key) {
      return new GetterImpl(arena, db, segmentOf(arena, key));
    }

    public Putter put(MemorySegment key, MemorySegment value) {
      return new PutterImpl(arena, db, key, value);
    }

    public Putter put(String key, MemorySegment value) {
      return new PutterImpl(arena, db, segmentOf(arena, key), value);
    }
  }

  interface Putter {
    void invoke() throws RocksDBException;
  }

  private final static class PutterImpl extends WithDB implements Putter {
    private final MemorySegment key;
    private final MemorySegment value;
    private final MemorySegment writeOptions;

    PutterImpl(Arena arena, MemorySegment db, MemorySegment key, MemorySegment value) {
      super(arena, db);
      this.key = key;
      this.value = value;
      writeOptions = rocksdb_c_api.rocksdb_writeoptions_create();
    }

    @Override
    public void invoke() throws RocksDBException {
      MemorySegment error = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
      rocksdb_c_api_2.rocksdb_put(
          db, writeOptions, key, key.byteSize(), value, value.byteSize(), error);
      throwOnError(error, message -> String.format("rocksdb_get() %s", message));
    }
  }

  interface Getter {
    MemorySegment invoke() throws RocksDBException;
  }

  private final static class GetterImpl extends WithDB implements Getter {
    private final MemorySegment key;
    private final MemorySegment readOptions;

    GetterImpl(Arena arena, MemorySegment db, MemorySegment key) {
      super(arena, db);
      this.key = key;
      readOptions = rocksdb_c_api.rocksdb_readoptions_create();
    }

    public MemorySegment invoke() throws RocksDBException {
      MemorySegment valueLength = arena.allocateFrom(ValueLayout.JAVA_LONG, 0L);
      MemorySegment error = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
      MemorySegment value =
          rocksdb_c_api_2.rocksdb_get(db, readOptions, key, key.byteSize(), valueLength, error);
      throwOnError(error, message -> String.format("rocksdb_get() %s", message));
      return dereference(value, valueLength);
    }
  }

  private static MemorySegment dereference(MemorySegment reference, MemorySegment length) {
    if (reference.address() == NULL.address()) {
      // NULL returned as a value, usually when a get() status is NOT_FOUND
      return null;
    }
    long lengthValue = length.get(ValueLayout.JAVA_LONG, 0);
    return reference.asSlice(0L, lengthValue);
  }

  private static void throwOnError(MemorySegment errPtr, Function<String, String> format)
      throws RocksDBException {
    MemorySegment error = errPtr.get(ValueLayout.ADDRESS, 0L);
    if (error.address() != NULL.address()) {
      error = error.reinterpret(Long.MAX_VALUE);
      String message = error.getString(0, UTF_8);
      throw new RocksDBException(format.apply(message));
    }
  }

  public static MemorySegment segmentOf(Arena arena, String s) {
    MemorySegment segment = arena.allocateFrom(s, UTF_8);
    return segment.asSlice(0, segment.byteSize() - 1);
  }

  /**
   * Extract a string from a segment
   * `MemorySegment.getString(offset, charset)` doesn't work for us, as it expects a C-style
   * null-terminated string
   *
   * @param segment to interpret as a String
   * @return the Java string contained in `segment`
   */
  public static String stringOf(MemorySegment segment) {
    int byteSize = (int) segment.byteSize();
    byte[] bytes = new byte[byteSize];
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, byteSize);
    return new String(bytes, UTF_8);
  }
}
