Fixed external file ingestion with the experimental `DBOptions::fast_sst_open` opening each ingested file with the DB mutex held, which stalled writes and other DB operations during each open.
