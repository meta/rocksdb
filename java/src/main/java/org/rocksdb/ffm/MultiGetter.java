package org.rocksdb.ffm;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.invoke.VarHandle;
import java.util.Collection;

import static org.rocksdb.ffm.rocksdb_c_api$shared.C_LONG;
import static org.rocksdb.ffm.util.FFMSegment.stringOf;

public class MultiGetter extends WithDB {

  private static final VarHandle ADDRESS_ARRAY = AddressLayout.ADDRESS.arrayElementVarHandle();

  /**
   * It makes sense for the supplied allocator to be a slicing allocator,
   * or a slicing allocator with a bailout for large requests to use the underlying allocator.
   *
   * @param allocator
   * @param db
   */
  protected MultiGetter(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  /**
   * Get values for multiple keys
   *
   * @param readoptions options for this read
   * @param columnFamilyHandle all values fetched from a single column family
   * @param keys
   * @return
   */
  public Result get(RocksdbReadoptions readoptions, ColumnFamilyHandle columnFamilyHandle, String[] keys, boolean sortedInputs) {
    return get(readoptions, columnFamilyHandle, keys.length, Slice.keysSegment(allocator, keys), sortedInputs);
  }

  /**
   * Get values for multiple keys
   *
   * @param readoptions options for this read
   * @param columnFamilyHandle all values fetched from a single column family
   * @param keys as an array of byte arrays
   * @return
   */
  public Result get(RocksdbReadoptions readoptions, ColumnFamilyHandle columnFamilyHandle, byte[][] keys, boolean sortedInputs) {
    return get(readoptions, columnFamilyHandle, keys.length, Slice.keysSegment(allocator, keys), sortedInputs);
  }

  /**
   * Get values for multiple keys
   *
   * @param readoptions
   * @param columnFamilyHandle
   * @param keys as a collection of strings
   * @param sortedInputs
   * @return
   */
  public Result get(RocksdbReadoptions readoptions, ColumnFamilyHandle columnFamilyHandle, Collection<String> keys, boolean sortedInputs) {
    return get(readoptions, columnFamilyHandle, keys.toArray(new String[0]), sortedInputs);
  }

  /**
   * Common get() helper where keys have been packaged into a slice array
   *
   * @param readoptions
   * @param columnFamilyHandle
   * @param count
   * @param keysSlice
   * @param sortedInputs
   * @return
   */
  private Result get(RocksdbReadoptions readoptions, ColumnFamilyHandle columnFamilyHandle, int count, MemorySegment keysSlice, boolean sortedInputs) {
    MemorySegment values = allocator.allocate(AddressLayout.ADDRESS, count);
    MemorySegment errors = allocator.allocate(AddressLayout.ADDRESS, count);
    rocksdb_c_api.rocksdb_batched_multi_get_cf_slice(
      db,
      readoptions.nativeSegment(),
      columnFamilyHandle.nativeSegment(),
      count,
      keysSlice,
      values,
      errors,
      sortedInputs);
    return new Result(count, values, errors);
  }

  public class Result {
    private final int count;
    private final MemorySegment values;
    private final MemorySegment errors;

    public Result(int count, MemorySegment values, MemorySegment errors) {
      this.count = count;
      this.values = values;
      this.errors = errors;
    }

    public int getCount() {
      return count;
    }

    public MemorySegment getValue(int i) {
      MemorySegment pinnedSlice = (MemorySegment) ADDRESS_ARRAY.get(values, 0L, (long)i);
      if (pinnedSlice != null) {
        MemorySegment sliceLength = allocator.allocate(C_LONG, 1);
        MemorySegment slice = rocksdb_c_api.rocksdb_pinnableslice_value(pinnedSlice, sliceLength);
        if (slice != null) {
          if (slice.equals(MemorySegment.NULL)) {
            return slice;
          }
          // TODO (AP) is the length of returned slice ever NOT the same as the returned length ?
          // OR is it always 0, and exactly what we do need to do is reinterpret it..
          long length = sliceLength.get(C_LONG, 0);
          return slice.reinterpret(length);
        }
      }
      return null;
    }

    public boolean isError(int i) {
      MemorySegment error = (MemorySegment) ADDRESS_ARRAY.get(errors, 0L, (long)i);
      return !error.equals(MemorySegment.NULL);
    }

    public String getError(int i) {
      MemorySegment error = (MemorySegment) ADDRESS_ARRAY.get(errors, 0L, (long)i);
      if (error.equals(MemorySegment.NULL)) {
        return null;
      } else {
        return stringOf(error);
      }
    }
  }
}
