WAL iterators now preserve live-file open errors when the file still exists, while checking the archive for removed files regardless of how the file system classifies the open error.
