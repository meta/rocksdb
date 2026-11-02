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
    public DB option();
    public OpenDB open(String path);
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
    public Getter get(MemorySegment key);
    public Getter get(String key);
    public Putter put(MemorySegment key, MemorySegment value);
    public Putter put(String key, MemorySegment value);
  }

  private final static class OpenDBImpl extends WithDB implements OpenDB {
    private OpenDBImpl(Arena arena, MemorySegment db) {
      super(arena, db);
    }

    public Getter get(MemorySegment key) {
      return new GetterImpl(arena, db, key);
    }

    public Getter get(String key) {
      return new GetterImpl(arena, db, string(arena, key));
    }

    public Putter put(MemorySegment key, MemorySegment value) {
      return new PutterImpl(arena, db, key, value);
    }

    public Putter put(String key, MemorySegment value) {
      return new PutterImpl(arena, db, string(arena, key), value);
    }
  }

  interface Putter {
    MemorySegment invoke() throws RocksDBException;
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
    public MemorySegment invoke() throws RocksDBException {
      return null;
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

  private static MemorySegment string(Arena arena, String s) {
    MemorySegment segment = arena.allocateFrom(s, UTF_8);
    return segment.asSlice(0, segment.byteSize() - 1);
  }
}
