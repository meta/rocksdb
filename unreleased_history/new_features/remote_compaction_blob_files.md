Added BlobDB support for remote compaction. Blob garbage collection uses the
cutoff computed by the primary; workers receiving legacy input without a cutoff
safely disable blob garbage collection for that job. When the primary rejects a
remote blob result because its metadata, files, or capabilities fail validation,
it reruns the compaction locally, so `CompactRange` and `CompactFiles` succeed.
`CompactionService` `kFailure` semantics are unchanged; ZippyDB production also
maps service failures to local fallback through `HandleFailure(FALLBACK_LOCAL)`.
