# from official ubuntu 20.04
FROM ubuntu:20.04
# update system
RUN apt-get update && apt-get upgrade -y
# install basic tools
RUN apt-get install -y vim wget curl
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
# install gcc-7, 8, 10, 11, default is 9
RUN apt-get install -y gcc-7 g++-7
RUN apt-get install -y gcc-8 g++-8
RUN apt-get install -y gcc-10 g++-10
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get install -y gcc-11 g++-11
# install apt-get install -y valgrind
RUN apt-get install -y valgrind
# install folly dependencies
RUN apt-get install -y libgoogle-glog-dev
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
RUN git clone --depth 1 --branch v1.7.0 https://github.com/google/benchmark.git ~/benchmark
RUN cd ~/benchmark && mkdir build && cd build && cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=0 && ninja && ninja install

# clean up
RUN rm -rf /var/lib/apt/lists/*
RUN rm -rf /root/benchmark
