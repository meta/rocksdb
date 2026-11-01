Fixed `DB::OpenAndCompact()` with `OpenAndCompactOptions::allow_resumption` not calling `EventListener::OnSubcompactionCompleted()` when the compaction fails to resume from its saved progress.
