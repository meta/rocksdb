//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// APIs for customizing compression in RocksDB.
//
// ***********************************************************************
// EXPERIMENTAL - subject to change while under development
// ***********************************************************************

#pragma once

#include <variant>

#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/data_structure.h"

namespace ROCKSDB_NAMESPACE {

// TODO: alias/adapt for compression
struct FilterBuildingContext;
class Decompressor;

// A Compressor represents a very specific but potentially adapting strategy for
// compressing blocks, including the relevant algorithm(s), options, dictionary,
// etc. as applicable--every input except the sequence of bytes to compress.
// Compressor is generally thread-safe so can be shared by multiple threads. (It
// could make sense to convert unique_ptr<Compressor> to
// shared_ptr<Compressor>.) A Compressor for data files is expected to be used
// for just one file, so that compression strategy can be explicitly
// reconsidered for each new file. However, a Compressor for in-memory use could
// live indefinitely.
//
// If a single thread is doing many compressions under the same strategy, it
// should request a WorkingArea that will in some cases make repeated
// compression in a single thread more efficient. Unlike the rest of Compressor,
// each WorkingArea can only be used by one thread at a time. WorkingAreas can
// have pre-allocated space and/or data structures, and/or thread-local
// statistics that are later incorporated into shared statistics objects.
//
// The Compressor marks each block with a CompressionType to guide
// decompression. However, the compression dictionary (or whether there is one
// associated) is determined at Compressor creation time, though the process of
// getting a Compressor with a dictionary starts with a Compressor without
// dictionary (which will often be relevant alongside); see relevant functions.
// If the Compressor wants to decide block-by-block whether to apply the
// configured dictionary, that would need to be encoded in CompressionType or
// the compressed output. (NOTE: this was historically NOT encoded in
// CompressionType and instead implied by BlockType and the presence of a
// dictionary block in the file. Some of the resulting awkwardness includes
// a number of built-in CompressionTypes that ignore any dictionary block in
// the file; therefore they cannot accommodate dictionary compression in the
// future without a schema change / extension.)
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Compressor {
 public:  // Auxiliary types
  // No dictionary should be used (for a given block type).
  struct DictDisabled {};

  // A recommendation for dictionary compression by collecting samples from
  // blocks. The caller should collect up to `max_sample_bytes` of sample data
  // and pass it to MaybeCloneSpecialized() to create a specialized compressor.
  struct DictSampling {
    // Maximum total bytes of sample data to collect from blocks.
    // This controls how much data is buffered before dictionary training.
    size_t max_sample_bytes = 0;
  };

  // A pre-defined dictionary that is recommended or specified for direct use
  // with MaybeCloneSpecialized(), without any sampling.
  struct DictPreDefined {
    // The owned raw/serialized dictionary bytes. Recommend std::move to
    // MaybeCloneSpecialized()
    std::string dict_data;
  };

  // The result type for GetDictGuidance() - indicates how dictionary
  // compression should be configured for a given block type.
  using DictConfig = std::variant<DictDisabled, DictSampling, DictPreDefined>;

  // Sample data collected from blocks for dictionary training.
  struct DictSamples {
    // All the sample input blocks stored contiguously
    std::string sample_data;
    // The lengths of each of the sample blocks in `sample_data`
    std::vector<size_t> sample_lens;

    bool empty() const { return sample_data.empty(); }
    bool Verify() const {
      size_t total_len = 0;
      for (auto len : sample_lens) {
        total_len += len;
      }
      return total_len == sample_data.size();
    }
  };

  // Arguments for MaybeCloneSpecialized() - provides either samples, a
  // pre-defined dictionary, or indicates no dictionary should be used.
  // NOTE: DictPreDefined here is the same type as above, allowing the
  // pre-defined dictionary from GetDictGuidance() to be passed through.
  using DictConfigArgs =
      std::variant<DictDisabled, DictSamples, DictPreDefined>;

  // A WorkingArea is an optional structure (both for callers and
  // implementations) that can enable optimizing repeated compressions by
  // reusing working space or thread-local tracking of statistics or trends.
  // This enables use of ZSTD context, for example.
  //
  // EXTENSIBLE or reinterpret_cast-able by custom Compressor implementations
  struct WorkingArea {};

 public:  // Functions
  Compressor() = default;
  virtual ~Compressor() = default;

  // Class name for logging / debugging purposes
  virtual const char* Name() const = 0;

  // Potentially more elaborate identifier for logging / debugging purposes
  virtual std::string GetId() const {
    std::string id = Name();
    return id;
  }

  // Returns the recommended dictionary configuration for the given block type.
  // See the comments on DictConfig and variants for details.
  //
  // NOTE: This may be called on the "base" Compressor returned by
  // CompressionManager, which is not yet configured with a dictionary,
  // or it can be skipped by callers not intending to handle dictionary
  // compression.
  virtual DictConfig GetDictGuidance(CacheEntryRole block_type) const {
    // Default implementation: no dictionary
    (void)block_type;
    return DictDisabled{};
  }

  // Returns the serialized form of the data dictionary associated with this
  // Compressor. NOTE: empty dict is equivalent to no dict.
  virtual Slice GetSerializedDict() const { return Slice(); }

  // If there's a dominant compression type returned by this compressor as
  // configured, return it. Otherwise, return kDisableCompressionOption.
  virtual CompressionType GetPreferredCompressionType() const {
    return CompressionType::kDisableCompressionOption;
  }

  // DEPRECATED: override MaybeOverrideCompressionOptions() instead, which
  // subsumes this hook.
  //
  // Returns a recommended number of parallel compression threads for SST
  // file building, or 0 to defer to CompressionOptions::parallel_threads.
  // Built-in compressors return the parallel_threads from their
  // CompressionOptions, enabling CompressionManager to override this by
  // customizing the options passed to GetCompressor().
  virtual uint32_t GetRecommendedParallelThreads() const { return 0; }

  // Allows this Compressor to override the subset of CompressionOptions that
  // are applied by the *caller* of the Compressor (e.g. the SST file builder)
  // rather than by the Compressor itself. These options govern how compression
  // is applied across a stream of blocks, not how any individual block is
  // compressed, so they are the builder's responsibility to honor. On entry,
  // `*to_modify` holds the options the caller is about to apply; the Compressor
  // may modify in place any of the following (and possibly more such options in
  // the future), and MUST leave all other fields unchanged:
  //   * parallel_threads
  //   * max_compressed_bytes_per_kb
  //   * auto_skip
  //   * auto_skip_min_sample_every
  // This does not affect the Compressor's own per-block compression behavior;
  // it only informs caller-side handling.
  //
  // The default implementation defers to the deprecated
  // GetRecommendedParallelThreads().
  virtual void MaybeOverrideCompressionOptions(
      CompressionOptions* to_modify) const {
    uint32_t recommended_parallel_threads = GetRecommendedParallelThreads();
    if (recommended_parallel_threads > 0) {
      to_modify->parallel_threads = recommended_parallel_threads;
    }
  }

  // Return a distinct but functionally equivalent Compressor. This is often
  // needed to implement MaybeCloneSpecialized() in wrapper compressors.
  virtual std::unique_ptr<Compressor> Clone() const = 0;

  // Create potential variants of the same Compressor that might be
  // (a) optimized for a particular block type (does not affect correct
  //     decompression), and/or
  // (b) configured to use a compression dictionary based on the provided
  //     configuration (samples or pre-defined dictionary). See the comments on
  //     DictConfigArgs and its variants for detail.
  //
  // Return of nullptr indicates no specialization exists or was attempted
  // and the caller should use the current Compressor for the desired scenario.
  // Using CacheEntryRole::kMisc for block_type generally means "unspecified".
  //
  // The exact dictionary associated with a returned compressor must be read
  // from GetSerializedDict().
  virtual std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictConfigArgs&& dict_config) const {
    // Default implementation: no specialization
    (void)block_type;
    (void)dict_config;
    return nullptr;
  }

  // A convenience function when a clone is needed and may or may not be
  // specialized.
  std::unique_ptr<Compressor> CloneMaybeSpecialized(
      CacheEntryRole block_type, DictConfigArgs&& dict_config) const {
    auto clone = MaybeCloneSpecialized(block_type, std::move(dict_config));
    if (clone == nullptr) {
      clone = Clone();
      assert(clone != nullptr);
    }
    return clone;
  }

  // To allow for flexible re-use / reclaimation, we have explicit Get and
  // Release functions, and usually wrap in a special RAII smart pointer.
  // For example, a WorkingArea could be saved/recycled in thread-local or
  // core-local storage, or heap managed, etc., though an explicit WorkingArea
  // is only advised for repeated compression (by a single thread).
  // ReleaseWorkingArea() in not intended to be called directly, but used by
  // ManagedWorkingArea.
  virtual void ReleaseWorkingArea(WorkingArea*) {}

  using ManagedWorkingArea =
      ManagedPtr<WorkingArea, Compressor, &Compressor::ReleaseWorkingArea>;

  // See struct WorkingArea above
  virtual ManagedWorkingArea ObtainWorkingArea() {
    // Default implementation: no working area
    return {};
  }

  // Compress `uncompressed_data` to buffer `compressed_output` of size
  // `*compressed_output_size`, storing the final compressed size in
  // `*compressed_output_size` and compression type in `*out_compression_type`.
  // Note that the compressed output will be decompressed by the sequence
  // Decompressor::ExtractUncompressedSize() followed by
  // Decompressor::DecompressBlock(), which must also be provided the same
  // CompressionType saved in `out_compression_type`. (In many configurations,
  // `compressed_output` will have a prefix storing the uncompressed_data size
  // before the compressed bytes returned by the underlying compression
  // algorithm. And the compression type is usually stored adjacent to the
  // compressed data, or in some cases assumed/asserted based on the particular
  // Compressor.)
  //
  // If return status is not OK, then some fatal condition has arisen. On OK
  // status, setting `*out_compression_type = kNoCompression` means compression
  // is declined and the caller should use the original uncompressed_data and
  // ignore any result in `compressed_output`. In this case, setting
  // *compressed_output_size to 0 suggests that compression was quickly
  // "bypassed" and *compressed_output_size > 0 suggests that compression was
  // attempted but rejected (e.g. insufficient compression ratio).
  //
  // On OK status and `*out_compression_type != kNoCompression`, compression has
  // happened with results in `compressed_output`, `compressed_output_size`, and
  // `out_compression_type`. The output compression type is allowed to vary from
  // call to call but does not for compressors from BuiltinV2CompressionManager.
  //
  // The working area is optional and used to optimize repeated compression by
  // a single thread. ManagedWorkingArea is provided rather than just
  // WorkingArea so that it can be used only if the `owner` matches expectation.
  // This could be useful for a Compressor wrapping more than one alternative
  // underlying Compressor.
  virtual Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                               size_t* compressed_output_size,
                               CompressionType* out_compression_type,
                               ManagedWorkingArea* working_area) = 0;

  // OPTIONAL: Return a decompressor that is optimized for output from this
  // compressor.
  virtual std::shared_ptr<Decompressor> GetOptimizedDecompressor() const {
    // Default implementation: no optimization. Get a Decompressor from the
    // CompressionManager.
    return nullptr;
  }

  // TODO: something to populate table properties based on settings, after all
  // or as WorkingAreas released. Maybe also update stats, or that could be in
  // thread-specific WorkingArea.
};

// A Decompressor usually has a wide capability to decompress all kinds of
// compressed data in the scope of a CompressionManager (see that class below),
// except
// (a) it might be optimized for or limited to a particular compression type(s)
//     (see GetDecompressor* functions for in CompressionManager),
// (b) distinct Decompressors are required to decompress with compression
//     dictionaries. (Decompressors are generally associated with empty/no
//     dictionary unless created with MaybeCloneForDict().)
//
// Similar to Compressor, Decompressor is generally thread safe except that each
// WorkingArea can only be used by a single thread at a time.
//
// Decompressors known to be associated with no dictionary are typically
// returned as shared_ptr, because they are broadly usable across threads.
// Because compression dictionaries are externally managed (see
// MaybeCloneForDict()), Decompressors associated with compression dictionaries
// are typically returned as unique_ptr, so that they are more easily
// guaranteed not to outlive their dictionaries (e.g. in block cache).
// Decompressors associated with compression dictionaries might include a
// processed or "digested" form of the raw dictionary for efficient repeated
// compressions.
//
// NOTE: Splitting the interface between ExtractUncompressedSize and
// DecompressBlock leaves to the caller details of (and flexibility in)
// allocating buffers for decompressing into. For example, the data could be
// decompressed into part of a single buffer allocated to hold a block's
// uncompressed contents along with an in-memory object representation of the
// block (to reduce fragmentation and other overheads of separate objects).
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Decompressor {
 public:
  Decompressor() = default;
  virtual ~Decompressor() = default;

  // A name for logging / debugging purposes
  virtual const char* Name() const = 0;

  // A WorkingArea is an optional structure (both for callers and
  // implementations) that can enable optimizing repeated decompressions by
  // reusing working space or thread-local tracking of statistics. This enables
  // use of ZSTD context, for example.
  //
  // EXTENSIBLE or reinterpret_cast-able by custom Compressor implementations
  struct WorkingArea {};

  // To allow for flexible re-use / reclaimation, we have explicit Obtain and
  // Release functions, which are typically wrapped in a special RAII smart
  // pointer. For example, a WorkingArea could be saved/recycled in thread-local
  // or core-local storage, or heap managed, etc., though an explicit
  // WorkingArea is only advised for repeated decompression (by a single
  // thread). ReleaseWorkingArea() in not intended to be called directly, but
  // used by ManagedWorkingArea.
  virtual void ReleaseWorkingArea(WorkingArea* wa) {
    // Default implementation: no working area
    (void)wa;
    assert(wa == nullptr);
  }

  using ManagedWorkingArea =
      ManagedPtr<WorkingArea, Decompressor, &Decompressor::ReleaseWorkingArea>;

  virtual ManagedWorkingArea ObtainWorkingArea(CompressionType /*preferred*/) {
    // Default implementation: no working area
    return {};
  }

  // If this Decompressor is associated with a (de)compression dictionary
  // (created with MaybeCloneForDict()), this returns a pointer to those raw (or
  // "serialized") bytes, which are externally managed (see
  // MaybeCloneForDict()).
  // Default: empty slice => no dictionary
  virtual const Slice& GetSerializedDict() const;

  // Create a variant of this Decompressor in `out` using the specified raw
  // ("serialized") dictionary. This step is required for decompressing data
  // compressed with the same dictionary. The new Decompressor references the
  // given Slice through its lifetime so the data it points to must be managed
  // by the caller along with (or beyond) the new Decompressor. If the
  // dictionary is processed into a form reusable by repeated compressions in
  // many threads, that happens within this call.
  //
  // Must return OK if and only if storing a result in `out`. Otherwise, could
  // return values like NotSupported - dictionary compression is not (yet)
  // supported for this kind of Decompressor. Corruption - dictionary is
  // malformed (though many implementations will accept any data as a
  // dictionary)
  //
  // RocksDB promises not to call this function with an empty dictionary slice
  // (equivalent to no dictionary).
  virtual Status MaybeCloneForDict(const Slice& /*serialized_dict*/,
                                   std::unique_ptr<Decompressor>* /*out*/) {
    return Status::NotSupported(
        "Dictionary compression not (yet) supported by " + std::string(Name()));
  }

  // Memory size of this object and others it owns. Does not include the
  // serialized dictionary (when used) which is externally managed.
  virtual size_t ApproximateOwnedMemoryUsage() const {
    // Default: negligible
    return 0;
  }

  // Potentially extensible by callers of Decompressor (but not recommended)
  struct Args {
    CompressionType compression_type = kNoCompression;
    Slice compressed_data;
    uint64_t uncompressed_size = 0;
    ManagedWorkingArea* working_area = nullptr;
  };

  // For efficiency on the read path, RocksDB strongly prefers the uncompressed
  // data size to be encoded in the compressed data in an easily accessible way,
  // so that allocation of a potentially long-lived buffer can be ideally sized.
  // This function determines the uncompressed size and potentially modifies
  // `args.compressed_data` to strip off the size metadata, for providing both
  // to DecompressBlock along with an appropriate buffer based on that size.
  // Some implementations will leave `compressed_data` unmodified and let
  // DecompressBlock call a library function that processes a format that
  // includes size metadata (e.g. Snappy).
  //
  // Even for legacy cases without size metadata (e.g. some very old RocksDB
  // formats), an exact size is required and could require decompressing the
  // data (here and in DecompressBlock()).
  //
  // Return non-OK in case of corrupt data or some other unworkable limitation
  // or failure.
  //
  // The default implementation uses a standard format for prepending
  // uncompressed size to the compressed payload. (RocksDB
  // compress_format_version=2 except Snappy)
  virtual Status ExtractUncompressedSize(Args& args);

  // Called to decompress a block of data after running ExtractUncompressedSize
  // on it. `args.compressed_data` is what ExtractUncompressedSize left there
  // after potentially stripping off the uncompressed size metadata. Returns OK
  // iff uncompressed data of size `uncompressed_size` is written to
  // `uncompressed_output`.
  virtual Status DecompressBlock(const Args& args,
                                 char* uncompressed_output) = 0;
};

// A CompressionManager represents
// * When/where/how to use different compressions
// * A schema (or set of schemas) and implementation for mapping
//     <CompressionType, dictionary, compressed data>
//   to uncompressed data (or error), which can expand over time (error in fewer
//   cases) for a given CompatibilityName() but can never change that mapping
//   (because that would break backward compatibility, potential quiet
//   corruption)
// TODO: consider adding optional streaming compression support (low priority)
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompressionManager
    : public std::enable_shared_from_this<CompressionManager>,
      public Customizable {
 public:
  CompressionManager() = default;
  ~CompressionManager() override = default;
  static const char* Type() { return "CompressionManager"; }

  // *************** Creating various Compression Managers *************** //
  // A name for the schema family of this CompressionManager. In short, if
  // two CompressionManagers have functionally the same Decompressor(s), they
  // should have the same CompatibilityName(), so that a compatible
  // CompressionManager/Decompressor might be used if the original is
  // unavailable. (Name() can be useful in addition to CompatibilityName() for
  // understanding what compression strategy was used.) This name should be
  // limited to legal variable names in C++ (alphanumeric and underscores).
  virtual const char* CompatibilityName() const = 0;

  // Default implementation checks the current compatibility name and returns
  // this CompressionManager (via `out`) if appropriate, and otherwise defers
  // to CreateFromString(). Failure should simply be a matter of "not found" in
  // which case nullptr is returned.
  virtual std::shared_ptr<CompressionManager> FindCompatibleCompressionManager(
      Slice compatibility_name);

  // Create or find a CompressionManager from a string, including built-in
  // CompressionManager types.
  // TODO: ObjectLibrary stuff
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<CompressionManager>* result);

  // Returns false iff a configuration that would pass the given compression
  // type to GetCompressor/GetCompressorForSST should be rejected (not
  // supported)
  virtual bool SupportsCompressionType(CompressionType type) const = 0;

  // Return a human-readable name for the given compression type within this
  // CompressionManager's schema. The default implementation returns the
  // generic built-in name such as "Snappy", "ZSTD", "Reserved4F", or
  // "Custom8A". Override to provide more specific names for custom
  // compression types.
  virtual std::string CompressionTypeToString(CompressionType type) const;

  // TODO: function to check compatibility with or sanitize CompressionOptions

  // ************************* Compressor creation *********************** //
  // Returning nullptr means compression is entirely disabled for the file,
  // which is valid at the discretion of the CompressionManager. Returning
  // nullptr should normally be the result if preferred == kNoCompression.
  //
  // Compressors returned here are configured WITHOUT a dictionary, so that
  // it's always possible to get correct compression->decompression results
  // if not opting-in to dictionary handling. The compressors may recommend
  // dictionary usage via GetDictGuidance() and creating a modified Compressor
  // for that. See Compressor::GetDictGuidance() etc. for details.
  //
  // These functions must be thread-safe.

  // Get a compressor for an SST file.
  // SUBJECT TO CHANGE
  // TODO: is it practical to get ColumnFamilyOptions plumbed into here?
  virtual std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext&, const CompressionOptions& opts,
      CompressionType preferred) {
    return GetCompressor(opts, preferred);
  }

  // Get a compressor for a generic/unspecified purpose (e.g. in-memory
  // compression).
  virtual std::unique_ptr<Compressor> GetCompressor(
      const CompressionOptions& opts, CompressionType type) = 0;

  // **************************** Decompressors ************************** //
  // Get a decompressor that is compatible with any blocks compressed by
  // compressors returned by this CompressionManager (at least this code
  // revision and earlier). (NOTE: recommended to return a shared_ptr alias of
  // this shared_ptr to a field that is a Decompressor.)
  // Justification for not making CompressionManager inherit Decompressor: this
  // tends to run into the diamond inheritance problem in implementations and
  // potential overheads of virtual inheritance.
  virtual std::shared_ptr<Decompressor> GetDecompressor() = 0;

  // Compatible with same as above, but potentially optimized for a certain
  // expected CompressionType
  virtual std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(
      CompressionType /*optimize_for_type*/) {
    // Safe default implementation
    return GetDecompressor();
  }

  // Get a decompressor that is allowed to have support only for the
  // CompressionTypes in the given start-to-end array (unique, sorted by
  // unsigned char)
  virtual std::shared_ptr<Decompressor> GetDecompressorForTypes(
      const CompressionType* /*types_begin*/,
      const CompressionType* /*types_end*/) {
    // Safe default implementation
    return GetDecompressor();
  }
};

// ************************* Utility wrappers etc. *********************** //
class CompressorWrapper : public Compressor {
 public:
  explicit CompressorWrapper(std::unique_ptr<Compressor> compressor)
      : wrapped_(std::move(compressor)) {}
  ~CompressorWrapper() override = default;
  // No copies
  CompressorWrapper(const CompressorWrapper&) = delete;
  CompressorWrapper& operator=(const CompressorWrapper&) = delete;
  CompressorWrapper(CompressorWrapper&&) = delete;
  CompressorWrapper& operator=(CompressorWrapper&&) = delete;

  DictConfig GetDictGuidance(CacheEntryRole block_type) const override {
    return wrapped_->GetDictGuidance(block_type);
  }

  Slice GetSerializedDict() const override {
    return wrapped_->GetSerializedDict();
  }

  CompressionType GetPreferredCompressionType() const override {
    return wrapped_->GetPreferredCompressionType();
  }

  uint32_t GetRecommendedParallelThreads() const override {
    return wrapped_->GetRecommendedParallelThreads();
  }

  void MaybeOverrideCompressionOptions(
      CompressionOptions* to_modify) const override {
    wrapped_->MaybeOverrideCompressionOptions(to_modify);
  }

  // NOTE: Clone() not implemented here because it needs to be in the derived
  // class

  // NOTE: MaybeCloneSpecialized() is only implemented here for convenience
  // when the wrapped Compressor uses the default implementation of
  // MaybeCloneSpecialized(). This needs to be overridden if not.
  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictConfigArgs&& dict_config) const override {
    auto clone =
        wrapped_->MaybeCloneSpecialized(block_type, std::move(dict_config));
    // Assert default no-op MaybeCloneSpecialized()
    assert(clone == nullptr);
    return clone;
  }

  ManagedWorkingArea ObtainWorkingArea() override {
    return wrapped_->ObtainWorkingArea();
  }

  // NOTE: Don't need to override ReleaseWorkingArea() here because
  // ManagedWorkingArea takes care of calling it on the Compressor that created
  // the WorkingArea.

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* working_area) override {
    return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                   compressed_output_size, out_compression_type,
                                   working_area);
  }

  std::shared_ptr<Decompressor> GetOptimizedDecompressor() const override {
    return wrapped_->GetOptimizedDecompressor();
  }

 protected:
  std::unique_ptr<Compressor> wrapped_;
};

class DecompressorWrapper : public Decompressor {
 public:
  explicit DecompressorWrapper(std::shared_ptr<Decompressor> decompressor)
      : wrapped_(std::move(decompressor)) {}
  ~DecompressorWrapper() override = default;
  // No copies
  DecompressorWrapper(const DecompressorWrapper&) = delete;
  DecompressorWrapper& operator=(const DecompressorWrapper&) = delete;
  DecompressorWrapper(DecompressorWrapper&&) = delete;
  DecompressorWrapper& operator=(DecompressorWrapper&&) = delete;

  const char* Name() const override { return wrapped_->Name(); }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    wrapped_->ReleaseWorkingArea(wa);
  }

  // NOTE: Don't need to override ReleaseWorkingArea() here because
  // ManagedWorkingArea takes care of calling it on the Decompressor that
  // created the WorkingArea.

  ManagedWorkingArea ObtainWorkingArea(CompressionType preferred) override {
    return wrapped_->ObtainWorkingArea(preferred);
  }

  const Slice& GetSerializedDict() const override {
    return wrapped_->GetSerializedDict();
  }

  Status MaybeCloneForDict(const Slice& serialized_dict,
                           std::unique_ptr<Decompressor>* out) override {
    // NOTE: derived class probably needs to override this to ensure a
    // derived wrapper around the new Decompressor
    return wrapped_->MaybeCloneForDict(serialized_dict, out);
  }

  size_t ApproximateOwnedMemoryUsage() const override {
    return wrapped_->ApproximateOwnedMemoryUsage();
  }

  Status ExtractUncompressedSize(Args& args) override {
    return wrapped_->ExtractUncompressedSize(args);
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    return wrapped_->DecompressBlock(args, uncompressed_output);
  }

 protected:
  std::shared_ptr<Decompressor> wrapped_;
};

// TODO: CompressorBase, for custom compressions

class CompressionManagerWrapper : public CompressionManager {
 public:
  explicit CompressionManagerWrapper(
      std::shared_ptr<CompressionManager> wrapped)
      : wrapped_(std::move(wrapped)) {}

  const char* CompatibilityName() const override {
    return wrapped_->CompatibilityName();
  }

  std::shared_ptr<CompressionManager> FindCompatibleCompressionManager(
      Slice compatibility_name) override {
    // NOTE: We expect that the wrapped CompressionManager will generally
    // be preferred if compatible, so the default implementation here does
    // not purely defer to the wrapped instance
    if (compatibility_name == CompatibilityName()) {
      return shared_from_this();
    } else {
      return wrapped_->FindCompatibleCompressionManager(compatibility_name);
    }
  }

  bool SupportsCompressionType(CompressionType type) const override {
    return wrapped_->SupportsCompressionType(type);
  }

  std::string CompressionTypeToString(CompressionType type) const override {
    return wrapped_->CompressionTypeToString(type);
  }

  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override {
    return wrapped_->GetCompressorForSST(context, opts, preferred);
  }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    return wrapped_->GetCompressor(opts, type);
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return wrapped_->GetDecompressor();
  }

  std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(
      CompressionType optimize_for_type) override {
    return wrapped_->GetDecompressorOptimizeFor(optimize_for_type);
  }

  std::shared_ptr<Decompressor> GetDecompressorForTypes(
      const CompressionType* types_begin,
      const CompressionType* types_end) override {
    return wrapped_->GetDecompressorForTypes(types_begin, types_end);
  }

 protected:
  std::shared_ptr<CompressionManager> wrapped_;
};

// Compression manager that implements the second schema for RocksDB built-in
// compression support. (The first schema is intentionally not provided here.)
// *** CURRENT STATE ***
// This is currently the latest schema for built-in compression, and the
// compression manager used when compression_manager=nullptr.
const std::shared_ptr<CompressionManager>& GetBuiltinV2CompressionManager();

// NOTE: No GetLatestBuiltinCompressionManager() is provided because that could
// lead to unexpected schema changes for user CompressionManagers building on
// the built-in schema, in the unlikely/rare case of a new built-in schema.

// Creates CompressionManager designed for the CPU and IO cost aware compression
// strategy
// EXPERIMENTAL
std::shared_ptr<CompressionManagerWrapper> CreateCostAwareCompressionManager(
    std::shared_ptr<CompressionManager> wrapped = nullptr);
}  // namespace ROCKSDB_NAMESPACE
