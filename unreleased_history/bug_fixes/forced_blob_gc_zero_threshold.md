Fixed a bug where `blob_garbage_collection_force_threshold = 0.0` made forced blob garbage collection mark blob files that contain no garbage, causing an otherwise idle DB to compact continuously.
