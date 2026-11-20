//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "trace_replay/trace_analyzer.h"

namespace ROCKSDB_NAMESPACE {

// The trace_analyzer command line tool: parses the flags into
// TraceAnalyzerOptions and runs a TraceAnalyzer.
int trace_analyzer_tool(int argc, char** argv);

}  // namespace ROCKSDB_NAMESPACE
