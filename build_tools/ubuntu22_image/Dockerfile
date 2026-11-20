# INSTRUCTIONS:
# I was not able to build docker images on an isolated devserver because of
# issues with proxy internet access. Use a public cloud or other Linux system.
# (I used a Debian system after installing docker features, adding my user to
# the docker and docker-registry groups, and logging out and back in to pick
# those up.)
#
# Meta employees: see https://fburl.com/rocksdb-docker to build this on a devvm.
#
# Follow https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic
# to login with your GitHub credentials, as in
#
# $ docker login ghcr.io -u pdillinger
#
# and paste the limited-purpose GitHub token into the terminal.
#
# Then in the build_tools/ubuntu22_image directory, (bump minor version for
# random docker file updates, major version tracks Ubuntu release)
#
# $ docker build -t ghcr.io/meta/rocksdb_ubuntu:22.2
# $ docker push ghcr.io/meta/rocksdb_ubuntu:22.2
#
# Might need to change visibility to public through
# https://github.com/orgs/facebook/packages/container/rocksdb_ubuntu/settings
# or similar.

# from official ubuntu 22.04
FROM ubuntu:22.04
# update system
RUN apt-get update
RUN apt-get upgrade -y
# install basic tools
RUN apt-get install -y vim wget curl ccache
# install tzdata noninteractive
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
# install git and default compilers
RUN apt-get install -y git gcc g++ clang clang-tools
# install basic package
RUN apt-get install -y lsb-release software-properties-common gnupg
# install gflags, tbb
RUN apt-get install -y libgflags-dev libtbb-dev
# install compression libs
RUN apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
# install cmake
RUN apt-get install -y cmake
RUN apt-get install -y libssl-dev
# install clang-13
WORKDIR /root
RUN wget https://apt.llvm.org/llvm.sh
RUN chmod +x llvm.sh
RUN ./llvm.sh 13 all
# There are incompatibilities between clang with -std=c++20 and libstdc++
# provided by gcc, so we have to compile with clang-13 using -stdlib=libc++
# and only one version of libc++ can be installed on the system at one time.
# So to avoid confusion we remove unusable clang-14 also.
RUN apt-get install libc++-13-dev libc++abi-13-dev
RUN apt-get purge -y clang-14 && apt-get autoremove -y

# install gcc-10 and more, default is 11
RUN apt-get install -y gcc-10 g++-10
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get install -y gcc-13 g++-13
# install apt-get install -y valgrind
RUN apt-get install -y valgrind
# install folly depencencies
# Missing compatible libunwind: RUN apt-get install -y libgoogle-glog-dev
# So instead install from source. This currently requires compiling with
# -DGLOG_USE_GLOG_EXPORT
RUN wget https://github.com/google/glog/archive/refs/tags/v0.7.1.tar.gz && tar xzf v0.7.1.tar.gz && cd glog-0.7.1/ && cmake -S . -B build -G "Unix Makefiles" && cmake --build build && cmake --build build --target install && cd .. && rm -rf v0.7.1.tar.gz glog-0.7.1
# install openjdk 8
RUN apt-get install -y openjdk-8-jdk
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
# install mingw
RUN apt-get install -y mingw-w64

# install gtest-parallel package
RUN git clone --single-branch --branch master --depth 1 https://github.com/google/gtest-parallel.git ~/gtest-parallel
ENV PATH $PATH:/root/gtest-parallel

# install libprotobuf for fuzzers test
RUN apt-get install -y ninja-build binutils liblzma-dev libz-dev pkg-config autoconf libtool
RUN git clone --branch v1.0 https://github.com/google/libprotobuf-mutator.git ~/libprotobuf-mutator && cd ~/libprotobuf-mutator && git checkout ffd86a32874e5c08a143019aad1aaf0907294c9f && mkdir build && cd build && cmake .. -GNinja -DCMAKE_C_COMPILER=clang-13 -DCMAKE_CXX_COMPILER=clang++-13 -DCMAKE_BUILD_TYPE=Release -DLIB_PROTO_MUTATOR_DOWNLOAD_PROTOBUF=ON && ninja && ninja install
ENV PKG_CONFIG_PATH /usr/local/OFF/:/root/libprotobuf-mutator/build/external.protobuf/lib/pkgconfig/
ENV PROTOC_BIN /root/libprotobuf-mutator/build/external.protobuf/bin/protoc

# install the latest google benchmark
RUN git clone --depth 1 --branch v1.7.0 https://github.com/google/benchmark.git ~/benchmark && cd ~/benchmark && mkdir build && cd build && cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=0 && ninja && ninja install && cd ~ && rm -rf /root/benchmark

# clean up
RUN rm -rf /var/lib/apt/lists/*
