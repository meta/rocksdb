// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import org.rocksdb.ffm.RocksFFM;

public class RocksFFMSample {

    public static void main(final String[] args) {
        RocksFFM.justDoIt();
        System.err.println("Didn't die!");
    }
}
