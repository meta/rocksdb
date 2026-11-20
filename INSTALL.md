## Compilation

**Important**: If you plan to run RocksDB in production, don't compile using default
`make` or `make all`. That will compile RocksDB in debug mode, which is much slower
than release mode.

RocksDB's library should be able to compile without any dependency installed,
although we recommend installing some compression libraries (see below).
We do depend on newer gcc/clang with C++20 support (GCC >= 11, Clang >= 10).

There are few options when compiling RocksDB:

* [recommended] `make static_lib` will compile librocksdb.a, RocksDB static library. Compiles static library in release mode.

* `make shared_lib` will compile librocksdb.so, RocksDB shared library. Compiles shared library in release mode.

* `make check` will compile and run all the unit tests. `make check` will compile RocksDB in debug mode.

* `make all` will compile our static library, and all our tools and unit tests. Our tools
depend on gflags 2.2.0 or newer. You will need to have gflags installed to run `make all`. This will compile RocksDB in debug mode. Don't
use binaries compiled by `make all` in production.

* By default the binary we produce is optimized for the CPU you're compiling on
(`-march=native` or the equivalent). To build a binary compatible with the most
general architecture supported by your CPU and compiler, set `PORTABLE=1` for
the build, but performance will suffer as many operations benefit from newer
and wider instructions. In addition to `PORTABLE=0` (default) and `PORTABLE=1`,
it can be set to an architecture name recognized by your compiler. For example,
on 64-bit x86, a reasonable compromise is `PORTABLE=haswell` which supports
many or most of the available optimizations while still being compatible with
most processors made since roughly 2013.

## Dependencies

* You can link RocksDB with following compression libraries:
  - [zlib](http://www.zlib.net/) - a library for data compression.
  - [bzip2](http://www.bzip.org/) - a library for data compression.
  - [lz4](https://github.com/lz4/lz4) - a library for extremely fast data compression.
  - [snappy](http://google.github.io/snappy/) - a library for fast
      data compression.
  - [zstandard](http://www.zstd.net) - Fast real-time compression
      algorithm.

* All our tools depend on:
  - [gflags](https://gflags.github.io/gflags/) - a library that handles
      command line flags processing. You can compile rocksdb library even
      if you don't have gflags installed.

* `make check` will also check code formatting, which requires [clang-format](https://clang.llvm.org/docs/ClangFormat.html)

* If you wish to build the RocksJava static target, then cmake is required for building Snappy.

* If you wish to run microbench (e.g, `make microbench`, `make ribbon_bench` or `cmake -DWITH_BENCHMARK=1`), Google benchmark >= 1.6.0 is needed.
* You can do the following to install Google benchmark. These commands are copied from `./build_tools/ubuntu20_image/Dockerfile`:

`$ git clone --depth 1 --branch v1.7.0 https://github.com/google/benchmark.git ~/benchmark`

`$ cd ~/benchmark && mkdir build && cd build && cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=0 && ninja && ninja install`

## Supported platforms

* **Linux - Ubuntu**
    * Upgrade your gcc to version at least 11 to get C++20 support.
    * Install gflags. First, try: `sudo apt-get install libgflags-dev`
      If this doesn't work and you're using Ubuntu, here's a nice tutorial:
      (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    * Install bzip2: `sudo apt-get install libbz2-dev`.
    * Install lz4: `sudo apt-get install liblz4-dev`.
    * Install zstandard: `sudo apt-get install libzstd-dev`.

* **Linux - CentOS / RHEL**
    * Upgrade your gcc to version at least 11 to get C++20 support
    * Install gflags:

              git clone https://github.com/gflags/gflags.git
              cd gflags
              git checkout v2.2.0
              ./configure && make && sudo make install

      **Notice**: Once installed, please add the include path for gflags to your `CPATH` environment variable and the
      lib path to `LIBRARY_PATH`. If installed with default settings, the include path will be `/usr/local/include`
      and the lib path will be `/usr/local/lib`.

    * Install snappy:

              sudo yum install snappy snappy-devel

    * Install zlib:

              sudo yum install zlib zlib-devel

    * Install bzip2:

              sudo yum install bzip2 bzip2-devel

    * Install lz4:

              sudo yum install lz4-devel

    * Install ASAN (optional for debugging):

              sudo yum install libasan

    * Install zstandard:
        * With [EPEL](https://fedoraproject.org/wiki/EPEL):

              sudo yum install libzstd-devel

        * With CentOS 8:

              sudo dnf install libzstd-devel

        * From source:

              wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
              mv v1.1.3.tar.gz zstd-1.1.3.tar.gz
              tar zxvf zstd-1.1.3.tar.gz
              cd zstd-1.1.3
              make && sudo make install

* **OS X**:
    * Install latest C++ compiler that supports C++20:
        * Update XCode:  run `xcode-select --install` (or install it from XCode App's settting).
        * Install via [homebrew](http://brew.sh/).
            * If you're first time developer in MacOS, you still need to run: `xcode-select --install` in your command line.
    * run `brew install rocksdb`

* **FreeBSD** (11.01):

    * You can either install RocksDB from the Ports system using `cd /usr/ports/databases/rocksdb && make install`, or you can follow the details below to install dependencies and compile from source code:

    * Install the dependencies for RocksDB:

        export BATCH=YES
        cd /usr/ports/devel/gmake && make install
        cd /usr/ports/devel/gflags && make install

        cd /usr/ports/archivers/snappy && make install
        cd /usr/ports/archivers/bzip2 && make install
        cd /usr/ports/archivers/liblz4 && make install
        cd /usr/ports/archivesrs/zstd && make install

        cd /usr/ports/devel/git && make install


    * Install the dependencies for RocksJava (optional):

        export BATCH=yes
        cd /usr/ports/java/openjdk7 && make install

    * Build RocksDB from source:
        cd ~
        git clone https://github.com/meta/rocksdb.git
        cd rocksdb
        gmake static_lib

    * Build RocksJava from source (optional):
        cd rocksdb
        export JAVA_HOME=/usr/local/openjdk7
        gmake rocksdbjava

* **OpenBSD** (6.3/-current):

    * As RocksDB is not available in the ports yet you have to build it on your own:

    * Install the dependencies for RocksDB:

      `pkg_add gmake gflags snappy bzip2 lz4 zstd git bash findutils gnuwatch`

    * Build RocksDB from source:

        ```bash
        cd ~
        git clone https://github.com/meta/rocksdb.git
        cd rocksdb
        gmake static_lib
        ```

    * Build RocksJava from source (optional):
        * In OpenBSD, JDK depends on XWindows system, so please check that you installed OpenBSD with `xbase` package.
        * Install dependencies : `pkg_add -v jdk%1.8`
        ```bash
        cd rocksdb
        export JAVA_HOME=/usr/local/jdk-1.8.0
        export PATH=$PATH:/usr/local/jdk-1.8.0/bin
        gmake rocksdbjava SHA256_CMD='sha256 -q'
        ```

* **iOS**:
  * Run: `TARGET_OS=IOS make static_lib`. When building the project which uses rocksdb iOS library, make sure to define an important pre-processing macros: `IOS_CROSS_COMPILE`.

* **Windows** (Visual Studio 2017 to up):
  * Read and follow the instructions at CMakeLists.txt
  * Or install via [vcpkg](https://github.com/microsoft/vcpkg)
       * run `vcpkg install rocksdb:x64-windows`

* **AIX 6.1**
    * Install AIX Toolbox rpms with gcc
    * Use these environment variables:

             export PORTABLE=1
             export CC=gcc
             export AR="ar -X64"
             export EXTRA_ARFLAGS=-X64
             export EXTRA_CFLAGS=-maix64
             export EXTRA_CXXFLAGS=-maix64
             export PLATFORM_LDFLAGS="-static-libstdc++ -static-libgcc"
             export LIBPATH=/opt/freeware/lib
             export JAVA_HOME=/usr/java8_64
             export PATH=/opt/freeware/bin:$PATH
