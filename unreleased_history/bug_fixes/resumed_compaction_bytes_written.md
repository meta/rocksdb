Fixed the statistics of a remote compaction that resumes from saved progress (`OpenAndCompactOptions::allow_resumption`): the bytes written now include the output files restored from that progress.
