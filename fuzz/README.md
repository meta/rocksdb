# Fuzzing RocksDB

## Overview

This directory contains [fuzz tests](https://en.wikipedia.org/wiki/Fuzzing) for RocksDB.
RocksDB testing infrastructure currently includes unit tests and [stress tests](https://github.com/meta/rocksdb/wiki/Stress-test),
we hope fuzz testing can catch more bugs.

## Prerequisite

We use [LLVM libFuzzer](http://llvm.org/docs/LibFuzzer.html) as the fuzzying engine,
so make sure you have [clang](https://clang.llvm.org/get_started.html) as your compiler.

Some tests rely on [structure aware fuzzing](https://github.com/google/fuzzing/blob/master/docs/structure-aware-fuzzing.md).
We use [protobuf](https://developers.google.com/protocol-buffers) to define structured input to the fuzzer,
and use [libprotobuf-mutator](https://github.com/google/libprotobuf-mutator) as the custom libFuzzer mutator.
So make sure you have protobuf and libprotobuf-mutator installed, and make sure `pkg-config` can find them.
On some systems, there are both protobuf2 and protobuf3 in the package management system,
make sure protobuf3 is installed.

If you do not want to install protobuf library yourself, you can rely on libprotobuf-mutator to download protobuf
for you. For details about installation, please refer to [libprotobuf-mutator README](https://github.com/google/libprotobuf-mutator#readme)

## Example

This example shows you how to do structure aware fuzzing to `rocksdb::SstFileWriter`.

After walking through the steps to create the fuzzer, we'll introduce a bug into `rocksdb::SstFileWriter::Put`,
then show that the fuzzer can catch the bug.

### Design the test

We want the fuzzing engine to automatically generate a list of database operations,
then we apply these operations to `SstFileWriter` in sequence,
finally, after the SST file is generated, we use `SstFileReader` to check the file's checksum.

### Define input

We define the database operations in protobuf, each operation has a type of operation and a key value pair,
see [proto/db_operation.proto](proto/db_operation.proto) for details.

### Define tests with the input

In [sst_file_writer_fuzzer.cc](sst_file_writer_fuzzer.cc),
we define the tests to be run on the generated input:

```
DEFINE_PROTO_FUZZER(DBOperations& input) {
  // apply the operations to SstFileWriter and use SstFileReader to verify checksum.
  // ...
}
```

`SstFileWriter` requires the keys of the operations to be unique and be in ascending order,
but the fuzzing engine generates the input randomly, so we need to process the generated input before
passing it to `DEFINE_PROTO_FUZZER`, this is accomplished by registering a post processor:

```
protobuf_mutator::libfuzzer::PostProcessorRegistration<DBOperations>
```

### Compile and link the fuzzer

In the rocksdb root directory, compile rocksdb library by `make static_lib`.

Go to the `fuzz` directory,
run `make sst_file_writer_fuzzer` to generate the fuzzer,
it will compile rocksdb static library, generate protobuf, then compile and link `sst_file_writer_fuzzer`.

### Introduce a bug

Manually introduce a bug to `SstFileWriter::Put`:

```
diff --git a/table/sst_file_writer.cc b/table/sst_file_writer.cc
index ab1ee7c4e..c7da9ffa0 100644
--- a/table/sst_file_writer.cc
+++ b/table/sst_file_writer.cc
@@ -277,6 +277,11 @@ Status SstFileWriter::Add(const Slice& user_key, const Slice& value) {
 }

 Status SstFileWriter::Put(const Slice& user_key, const Slice& value) {
+  if (user_key.starts_with("!")) {
+    if (value.ends_with("!")) {
+      return Status::Corruption("bomb");
+    }
+  }
   return rep_->Add(user_key, value, ValueType::kTypeValue);
 }
```

The bug is that for `Put`, if `user_key` starts with `!` and `value` ends with `!`, then corrupt.

### Run fuzz testing to catch the bug

Run the fuzzer by `time ./sst_file_writer_fuzzer`.

Here is the output on my machine:

```
Corruption: bomb
==59680== ERROR: libFuzzer: deadly signal
    #0 0x109487315 in __sanitizer_print_stack_trace+0x35 (libclang_rt.asan_osx_dynamic.dylib:x86_64+0x4d315)
    #1 0x108d63f18 in fuzzer::PrintStackTrace() FuzzerUtil.cpp:205
    #2 0x108d47613 in fuzzer::Fuzzer::CrashCallback() FuzzerLoop.cpp:232
    #3 0x7fff6af535fc in _sigtramp+0x1c (libsystem_platform.dylib:x86_64+0x35fc)
    #4 0x7ffee720f3ef  (<unknown module>)
    #5 0x7fff6ae29807 in abort+0x77 (libsystem_c.dylib:x86_64+0x7f807)
    #6 0x108cf1c4c in TestOneProtoInput(DBOperations&)+0x113c (sst_file_writer_fuzzer:x86_64+0x100302c4c)
    #7 0x108cf09be in LLVMFuzzerTestOneInput+0x16e (sst_file_writer_fuzzer:x86_64+0x1003019be)
    #8 0x108d48ce0 in fuzzer::Fuzzer::ExecuteCallback(unsigned char const*, unsigned long) FuzzerLoop.cpp:556
    #9 0x108d48425 in fuzzer::Fuzzer::RunOne(unsigned char const*, unsigned long, bool, fuzzer::InputInfo*, bool*) FuzzerLoop.cpp:470
    #10 0x108d4a626 in fuzzer::Fuzzer::MutateAndTestOne() FuzzerLoop.cpp:698
    #11 0x108d4b325 in fuzzer::Fuzzer::Loop(std::__1::vector<fuzzer::SizedFile, fuzzer::fuzzer_allocator<fuzzer::SizedFile> >&) FuzzerLoop.cpp:830
    #12 0x108d37fcd in fuzzer::FuzzerDriver(int*, char***, int (*)(unsigned char const*, unsigned long)) FuzzerDriver.cpp:829
    #13 0x108d652b2 in main FuzzerMain.cpp:19
    #14 0x7fff6ad5acc8 in start+0x0 (libdyld.dylib:x86_64+0x1acc8)

NOTE: libFuzzer has rudimentary signal handlers.
      Combine libFuzzer with AddressSanitizer or similar for better crash reports.
SUMMARY: libFuzzer: deadly signal
MS: 7 Custom-CustomCrossOver-InsertByte-Custom-ChangeBit-Custom-CustomCrossOver-; base unit: 90863b4d83c3f994bba0a417d0c2ee3b68f9e795
0x6f,0x70,0x65,0x72,0x61,0x74,0x69,0x6f,0x6e,0x73,0x20,0x7b,0xa,0x20,0x20,0x6b,0x65,0x79,0x3a,0x20,0x22,0x21,0x22,0xa,0x20,0x20,0x76,0x61,0x6c,0x75,0x65,0x3a,0x20,0x22,0x21,0x22,0xa,0x20,0x20,0x74,0x79,0x70,0x65,0x3a,0x20,0x50,0x55,0x54,0xa,0x7d,0xa,0x6f,0x70,0x65,0x72,0x61,0x74,0x69,0x6f,0x6e,0x73,0x20,0x7b,0xa,0x20,0x20,0x6b,0x65,0x79,0x3a,0x20,0x22,0x2b,0x22,0xa,0x20,0x20,0x74,0x79,0x70,0x65,0x3a,0x20,0x50,0x55,0x54,0xa,0x7d,0xa,0x6f,0x70,0x65,0x72,0x61,0x74,0x69,0x6f,0x6e,0x73,0x20,0x7b,0xa,0x20,0x20,0x6b,0x65,0x79,0x3a,0x20,0x22,0x2e,0x22,0xa,0x20,0x20,0x74,0x79,0x70,0x65,0x3a,0x20,0x50,0x55,0x54,0xa,0x7d,0xa,0x6f,0x70,0x65,0x72,0x61,0x74,0x69,0x6f,0x6e,0x73,0x20,0x7b,0xa,0x20,0x20,0x6b,0x65,0x79,0x3a,0x20,0x22,0x5c,0x32,0x35,0x33,0x22,0xa,0x20,0x20,0x74,0x79,0x70,0x65,0x3a,0x20,0x50,0x55,0x54,0xa,0x7d,0xa,
operations {\x0a  key: \"!\"\x0a  value: \"!\"\x0a  type: PUT\x0a}\x0aoperations {\x0a  key: \"+\"\x0a  type: PUT\x0a}\x0aoperations {\x0a  key: \".\"\x0a  type: PUT\x0a}\x0aoperations {\x0a  key: \"\\253\"\x0a  type: PUT\x0a}\x0a
artifact_prefix='./'; Test unit written to ./crash-a1460be302d09b548e61787178d9edaa40aea467
Base64: b3BlcmF0aW9ucyB7CiAga2V5OiAiISIKICB2YWx1ZTogIiEiCiAgdHlwZTogUFVUCn0Kb3BlcmF0aW9ucyB7CiAga2V5OiAiKyIKICB0eXBlOiBQVVQKfQpvcGVyYXRpb25zIHsKICBrZXk6ICIuIgogIHR5cGU6IFBVVAp9Cm9wZXJhdGlvbnMgewogIGtleTogIlwyNTMiCiAgdHlwZTogUFVUCn0K
./sst_file_writer_fuzzer  5.97s user 4.40s system 64% cpu 16.195 total
```

Within 6 seconds, it catches the bug.

The input that triggers the bug is persisted in `./crash-a1460be302d09b548e61787178d9edaa40aea467`:

```
$ cat ./crash-a1460be302d09b548e61787178d9edaa40aea467
operations {
  key: "!"
  value: "!"
  type: PUT
}
operations {
  key: "+"
  type: PUT
}
operations {
  key: "."
  type: PUT
}
operations {
  key: "\253"
  type: PUT
}
```

### Reproduce the crash to debug

The above crash can be reproduced by `./sst_file_writer_fuzzer ./crash-a1460be302d09b548e61787178d9edaa40aea467`,
so you can debug the crash.

## Future Work

According to [OSS-Fuzz](https://github.com/google/oss-fuzz),
`as of June 2020, OSS-Fuzz has found over 20,000 bugs in 300 open source projects.`

RocksDB can join OSS-Fuzz together with other open source projects such as sqlite.
