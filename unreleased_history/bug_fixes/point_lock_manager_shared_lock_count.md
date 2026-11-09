Fixed the default point lock manager under-enforcing `TransactionDBOptions::max_num_locks` after shared locks were released, which could also fail a debug-build assertion.
