# Common Pitfalls and Best Practices

## Pitfalls

1. **Using ZSTD without dictionary for structured data**: Loses 20-50% compression potential
   - **Solution**: Enable dictionary with `max_dict_bytes=64KB`, `zstd_max_train_bytes=100MB`

2. **Enabling parallel compression for Snappy/LZ4**: Unlikely throughput gain, wastes memory
   - **Solution**: Only enable `parallel_threads > 1` for ZSTD/Zlib (lightweight codecs are not rejected but not recommended)

3. **Setting `max_dict_buffer_bytes < max_dict_bytes`**: Dictionary too small or fails to build
   - **Solution**: Set `max_dict_buffer_bytes = 0` (unlimited) or much larger than `max_dict_bytes`

4. **Compressing L0 with ZSTD**: Adds write latency for short-lived data
   - **Solution**: Use `compression_per_level={kNoCompression, kLZ4, ..., kZSTD}`

5. **Forgetting to benchmark**: Assuming compression always improves performance
   - **Solution**: Benchmark with `db_bench` on production-like data before production deployment

6. **Mismatched `compression_opts` and `bottommost_compression_opts`**: Inconsistent configuration
   - **Solution**: Explicitly set `bottommost_compression_opts.enabled=true` and configure all fields

## Best Practices

1. **Start with defaults**: `compression=kSnappyCompression` (if linked; `kNoCompression` otherwise), then optimize if storage is bottleneck
2. **Use per-level compression**: `kNoCompression` or `kLZ4` for hot levels, `kZSTD` for bottommost
3. **Enable ZSTD dictionary for structured data**: JSON, protobufs, logs with repetitive schemas
4. **Monitor compression ratio**: If ratio < 1.5:1, compression may not be worth CPU cost
5. **Tune `max_compressed_bytes_per_kb`**: The builder clamps this to max 1023, so compression always saves at least 1 byte per KB. Adjust to 800-900 for stricter thresholds
6. **Profile decompression**: If >10% of read CPU, consider LZ4/Snappy for hot data
7. **Test with production data**: Synthetic benchmarks may not reflect real compression ratios

## Debugging Compression Issues

**Check effective compression type**:

```bash
# Dump SST metadata
ldb manifest_dump --db=/path/to/db | grep compression

# Dump block metadata
sst_dump --file=/path/to/sst --command=scan --output_hex | grep compression_type
```

**Verify dictionary usage**:

```bash
# Check for compression dictionary block
sst_dump --file=/path/to/sst --command=raw | grep kCompressionDictionary
```

**Measure actual compression ratio**:

```bash
# Compare SST size to raw data size
ldb dump --db=/path/to/db --count_only  # Get key count
du -sh /path/to/db/*.sst                # Get SST sizes
# Ratio = (key_count * (key_size + value_size)) / sum(sst_sizes)
```
