`WriteOptions::disableWAL` is now supported with `DBOptions::recycle_log_file_num > 0`, except with WritePrepared and WriteUnprepared transactions, instead of returning `Status::InvalidArgument`.
