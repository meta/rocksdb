# Fix Summary: blob_db

## Review Sources

Both reviews were available:
- `blob_db_review_cc.md` (Claude Code review) -- rated "good"
- `blob_db_review_codex.md` (Codex review) -- rated "needs work"

## Issues Fixed

| Category | Count | Details |
|----------|-------|---------|
| Correctness | 10 | linked_ssts semantics, GetBlobStats fields, OnBlobFileCreated semantics, BlobFileAdditionInfo type name, crash recovery failure modes, dynamic reconfig scope, manual compaction override scope, option validation conditionality, blob_file_size soft cap, cache LRU assumption |
| Completeness | 3 | Cache insert failure semantics on read, BlobFileCreationReason::kRecovery, missing Files: lines |
| Depth | 1 | Active vs. legacy statistics separation |
| Style | 0 | No style fixes needed |

## Disagreements Found

6 disagreements documented in `blob_db_debates.md`. Codex was correct in all 6 cases:

1. **linked_ssts semantics** (high risk): Not a full reference set, only inverse of oldest_blob_file_number
2. **Crash recovery failure modes** (medium risk): Multiple failure modes, not just size check
3. **GetBlobStats fields** (low risk): No total_file_count field
4. **OnBlobFileCreated semantics** (medium risk): Called for both success and failure
5. **Dynamic reconfig scope** (medium risk): Affects compaction output, not just new writes
6. **Statistics coverage** (high risk): Many tickers are legacy-only, not recorded by integrated BlobDB

## Changes Made

| File | Changes |
|------|---------|
| `02_blob_file_format.md` | Rewritten crash recovery section to describe all failure modes (size check, footer decode, CRC validation) |
| `04_write_path.md` | Clarified blob_file_size is a soft cap (file can exceed by one record); added kRecovery creation reason |
| `05_read_path.md` | Removed LRU assumption from cache table; added cache insert failure semantics for reads |
| `08_metadata_lifecycle.md` | Rewrote linked_ssts as inverse mapping of oldest_blob_file_number; fixed GetBlobStats to not include total_file_count; added Files: entry for version_builder.cc |
| `09_configuration.md` | Added Files: entries; fixed blob_file_size description as target/soft cap; rewrote dynamic reconfig to cover compaction-time effects; made option validation conditional on enable_blob_garbage_collection; clarified shared cache uses generic Cache interface; narrowed manual compaction override to only policy + cutoff |
| `10_statistics.md` | Split tickers and histograms into "actively recorded by integrated BlobDB" vs "legacy stacked BlobDB only"; fixed GetBlobStats fields; fixed OnBlobFileCreated to note success+failure; fixed BlobFileInfo to BlobFileAdditionInfo; added Files: entries |

## Self-Review Checklist

- [x] No line number references
- [x] No inline code quotes (uses backtick formatting for code identifiers per existing style)
- [x] No box-drawing characters
- [x] INVARIANT used correctly (linked_ssts non-empty assertion in 06)
- [x] Every claim validated against current code
- [x] index.md is SHORT (43 lines, within 40-80 target)
- [x] Each chapter has **Files:** line
