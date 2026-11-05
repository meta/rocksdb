`DB::Close()` now returns an error when closing an open WAL file fails, as it already does for the MANIFEST. Previously such WAL close errors were ignored.
