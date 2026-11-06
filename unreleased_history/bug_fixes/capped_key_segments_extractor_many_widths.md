Fixed a crash in experimental `MakeSharedCappedKeySegmentsExtractor()` with more than 6 byte widths: the extractor wrote the segment ends into an empty vector.
