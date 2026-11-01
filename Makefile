# Copyright (c) 2011 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

# Inherit some settings from environment variables, if available

#-----------------------------------------------

BASH_EXISTS := $(shell which bash)
SHELL := $(shell which bash)
include common.mk

CLEAN_FILES = # deliberately empty, so we can append below.
CFLAGS += ${EXTRA_CFLAGS}
CXXFLAGS += ${EXTRA_CXXFLAGS}
LDFLAGS += $(EXTRA_LDFLAGS)
MACHINE ?= $(shell uname -m)
ARFLAGS = ${EXTRA_ARFLAGS} rs
STRIPFLAGS = -S -x

# Transform parallel LOG output into something more readable.
perl_command = perl -n \
  -e '@a=split("\t",$$_,-1); $$t=$$a[8];'				\
  -e '$$t =~ /.*if\s\[\[\s"(.*?\.[\w\/]+)/ and $$t=$$1;'		\
  -e '$$t =~ s,^\./,,;'							\
  -e '$$t =~ s, >.*,,; chomp $$t;'					\
  -e '$$t =~ /.*--gtest_filter=(.*?\.[\w\/]+)/ and $$t=$$1;'		\
  -e 'printf "%7.3f %s %s\n", $$a[3], $$a[6] == 0 ? "PASS" : "FAIL", $$t'
quoted_perl_command = $(subst ','\'',$(perl_command))

# DEBUG_LEVEL can have three values:
# * DEBUG_LEVEL=2; this is the ultimate debug mode. It will compile rocksdb
# without any optimizations. To compile with level 2, issue `make dbg`
# * DEBUG_LEVEL=1; debug level 1 enables all assertions and debug code, but
# compiles rocksdb with -O2 optimizations. this is the default debug level.
# `make all` or `make <binary_target>` compile RocksDB with debug level 1.
# We use this debug level when developing RocksDB.
# * DEBUG_LEVEL=0; this is the debug level we use for release. If you're
# running rocksdb in production you most definitely want to compile RocksDB
# with debug level 0. To compile with level 0, run `make shared_lib`,
# `make install-shared`, `make static_lib`, `make install-static` or
# `make install`

# Set the default DEBUG_LEVEL to 1
DEBUG_LEVEL?=1

# OBJ_DIR is where the object files reside.  Default to the current directory
OBJ_DIR?=.

# Check the MAKECMDGOALS to set the DEBUG_LEVEL and LIB_MODE appropriately

ifneq ($(filter clean release install, $(MAKECMDGOALS)),)
	DEBUG_LEVEL=0
endif
ifneq ($(filter dbg, $(MAKECMDGOALS)),)
	DEBUG_LEVEL=2
else ifneq ($(filter shared_lib install-shared, $(MAKECMDGOALS)),)
	DEBUG_LEVEL=0
	LIB_MODE=shared
else ifneq ($(filter static_lib install-static, $(MAKECMDGOALS)),)
	DEBUG_LEVEL=0
	LIB_MODE=static
else ifneq ($(filter jtest rocksdbjava%, $(MAKECMDGOALS)),)
	OBJ_DIR=jl
	LIB_MODE=shared
	ifneq ($(findstring rocksdbjavastatic, $(MAKECMDGOALS)),)
		OBJ_DIR=jls
		ifneq ($(DEBUG_LEVEL),2)
			DEBUG_LEVEL=0
		endif
		ifeq ($(MAKECMDGOALS),rocksdbjavastaticpublish)
			DEBUG_LEVEL=0
		endif
	endif
endif

# LIB_MODE says whether or not to use/build "shared" or "static" libraries.
# Mode "static" means to link against static libraries (.a)
# Mode "shared" means to link against shared libraries (.so, .sl, .dylib, etc)
#
ifeq ($(DEBUG_LEVEL), 0)
# For optimized, set the default LIB_MODE to static for code size/efficiency
	LIB_MODE?=static
else
# For debug, set the default LIB_MODE to shared for efficient `make check` etc.
	LIB_MODE?=shared
endif

# Set by the gen_check_scripts wrappers to mark a recursive make reusing the
# parent's make_config.mk: silence the parse-time banners and skip
# build_detect_platform (below).
$(if $(ROCKSDB_RECURSIVE_MAKE_SAME_CONFIG),,$(info $$DEBUG_LEVEL is $(DEBUG_LEVEL), $$LIB_MODE is $(LIB_MODE)))

# Detect what platform we're building on.
# Export some common variables that might have been passed as Make variables
# instead of environment variables.
#
# Skipped by the gen_check_scripts wrappers (which run concurrently under
# gnu_parallel): each would otherwise rewrite the shared make_config.mk
# non-atomically and a concurrent reader could catch it mid-write ("missing
# separator"). Requires an existing make_config.mk, so a normal build still detects.
ifeq ($(and $(ROCKSDB_RECURSIVE_MAKE_SAME_CONFIG),$(wildcard make_config.mk)),)
dummy := $(shell (export ROCKSDB_ROOT="$(CURDIR)"; \
                  export CXXFLAGS="$(EXTRA_CXXFLAGS)"; \
                  export LDFLAGS="$(EXTRA_LDFLAGS)"; \
                  export COMPILE_WITH_ASAN="$(COMPILE_WITH_ASAN)"; \
                  export COMPILE_WITH_TSAN="$(COMPILE_WITH_TSAN)"; \
                  export COMPILE_WITH_UBSAN="$(COMPILE_WITH_UBSAN)"; \
                  export PORTABLE="$(PORTABLE)"; \
                  export ROCKSDB_NO_FBCODE="$(ROCKSDB_NO_FBCODE)"; \
                  export USE_CLANG="$(USE_CLANG)"; \
                  export LIB_MODE="$(LIB_MODE)"; \
		  export ROCKSDB_CXX_STANDARD="$(ROCKSDB_CXX_STANDARD)"; \
		  export USE_FOLLY="$(USE_FOLLY)"; \
		  export USE_FOLLY_LITE="$(USE_FOLLY_LITE)"; \
                  "$(CURDIR)/build_tools/build_detect_platform" "$(CURDIR)/make_config.mk" $(if $(ROCKSDB_RECURSIVE_MAKE_SAME_CONFIG),2>/dev/null)))
endif
# this file (generated above, or reused when skipped) sets build flags and sources
include make_config.mk

# Figure out optimize level.
ifneq ($(DEBUG_LEVEL), 2)
	OPTIMIZE_LEVEL ?= -O2
endif
# `OPTIMIZE_LEVEL` is empty when the user does not set it and `DEBUG_LEVEL=2`.
# In that case, the compiler default (`-O0` for gcc and clang) will be used.
OPT += $(OPTIMIZE_LEVEL)

# compile with -O2 if debug level is not 2
ifneq ($(DEBUG_LEVEL), 2)
OPT += -fno-omit-frame-pointer
# Skip for archs that don't support -momit-leaf-frame-pointer
ifeq (,$(shell $(CXX) -fsyntax-only -momit-leaf-frame-pointer -xc /dev/null 2>&1))
OPT += -momit-leaf-frame-pointer
endif
endif

ifeq (,$(shell $(CXX) -fsyntax-only -maltivec -xc /dev/null 2>&1))
CXXFLAGS += -DHAS_ALTIVEC
CFLAGS += -DHAS_ALTIVEC
HAS_ALTIVEC=1
endif

ifeq (,$(shell $(CXX) -fsyntax-only -mcpu=power8 -xc /dev/null 2>&1))
CXXFLAGS += -DHAVE_POWER8
CFLAGS +=  -DHAVE_POWER8
HAVE_POWER8=1
endif

# if we're compiling for shared libraries, add the shared flags
ifeq ($(LIB_MODE),shared)
CXXFLAGS += $(PLATFORM_SHARED_CFLAGS) -DROCKSDB_DLL
CFLAGS +=  $(PLATFORM_SHARED_CFLAGS) -DROCKSDB_DLL
endif

GIT_COMMAND ?= git
ifeq ($(USE_COROUTINES), 1)
	USE_FOLLY = 1
	# glog/logging.h requires HAVE_CXX11_ATOMIC
	OPT += -DUSE_COROUTINES -DHAVE_CXX11_ATOMIC
	USE_RTTI = 1
ifneq ($(USE_CLANG), 1)
	PLATFORM_CXXFLAGS += -fcoroutines
endif
endif

# if we're compiling for release, compile without debug code (-DNDEBUG)
ifeq ($(DEBUG_LEVEL),0)
OPT += -DNDEBUG

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
else
	CXXFLAGS += -DROCKSDB_USE_RTTI
endif
else
ifneq ($(USE_RTTI), 0)
	CXXFLAGS += -DROCKSDB_USE_RTTI
else
	CXXFLAGS += -fno-rtti
endif

ifdef ASSERT_STATUS_CHECKED
# For ASC, turn off constructor elision, preventing the case where a constructor returned
# by a method may pass the ASC check if the status is checked in the inner method.  Forcing
# the copy constructor to be invoked disables the optimization and will cause the calling method
# to check the status in order to prevent an error from being raised.
PLATFORM_CXXFLAGS += -fno-elide-constructors
ifeq ($(filter -DROCKSDB_ASSERT_STATUS_CHECKED,$(OPT)),)
	OPT += -DROCKSDB_ASSERT_STATUS_CHECKED
endif
endif

$(if $(ROCKSDB_RECURSIVE_MAKE_SAME_CONFIG),,$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production))
endif

# `USE_LTO=1` enables link-time optimizations. Among other things, this enables
# more devirtualization opportunities and inlining across translation units.
# This can save significant overhead introduced by RocksDB's pluggable
# interfaces/internal abstractions, like in the iterator hierarchy. It works
# better when combined with profile-guided optimizations (not currently
# supported natively in Makefile).
ifeq ($(USE_LTO), 1)
	CXXFLAGS += -flto
	LDFLAGS += -flto -fuse-linker-plugin
endif

# `COERCE_CONTEXT_SWITCH=1` will inject spurious wakeup and
# random length of sleep or context switch at critical
# points (e.g, before acquring db mutex) in RocksDB.
# In this way, it coerces as many excution orders as possible in the hope of
# exposing the problematic excution order
COERCE_CONTEXT_SWITCH ?= 0
ifeq ($(COERCE_CONTEXT_SWITCH), 1)
OPT += -DCOERCE_CONTEXT_SWITCH
endif

#-----------------------------------------------
include src.mk

AM_DEFAULT_VERBOSITY ?= 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $@;
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $@;
am__v_CC_1 =

AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
ifneq ($(SKIP_LINK), 1)
am__v_CCLD_0 = @echo "  CCLD    " $@;
am__v_CCLD_1 =
else
am__v_CCLD_0 = @echo "  !CCLD   " $@; true skip
am__v_CCLD_1 = true skip
endif
AM_V_AR = $(am__v_AR_$(V))
am__v_AR_ = $(am__v_AR_$(AM_DEFAULT_VERBOSITY))
am__v_AR_0 = @echo "  AR      " $@;
am__v_AR_1 =

AM_LINK = $(AM_V_CCLD)$(CXX) -L. $(patsubst lib%.a, -l%, $(patsubst lib%.$(PLATFORM_SHARED_EXT), -l%, $^)) $(EXEC_LDFLAGS) -o $@ $(LDFLAGS) $(COVERAGEFLAGS)
AM_SHARE = $(AM_V_CCLD) $(CXX) $(PLATFORM_SHARED_LDFLAGS)$@ -L. $(patsubst lib%.$(PLATFORM_SHARED_EXT), -l%, $^) $(EXEC_LDFLAGS) $(LDFLAGS) -o $@

ROCKSDB_PLUGIN_MKS = $(foreach plugin, $(ROCKSDB_PLUGINS), plugin/$(plugin)/*.mk)
include $(ROCKSDB_PLUGIN_MKS)
ROCKSDB_PLUGIN_PROTO =ROCKSDB_NAMESPACE::ObjectLibrary\&, const std::string\&
ROCKSDB_PLUGIN_SOURCES = $(foreach p, $(ROCKSDB_PLUGINS), $(foreach source, $($(p)_SOURCES), plugin/$(p)/$(source)))
ROCKSDB_PLUGIN_HEADERS = $(foreach p, $(ROCKSDB_PLUGINS), $(foreach header, $($(p)_HEADERS), plugin/$(p)/$(header)))
ROCKSDB_PLUGIN_LIBS = $(foreach p, $(ROCKSDB_PLUGINS), $(foreach lib, $($(p)_LIBS), -l$(lib)))
ROCKSDB_PLUGIN_W_FUNCS = $(foreach p, $(ROCKSDB_PLUGINS), $(if $($(p)_FUNC), $(p)))
ROCKSDB_PLUGIN_EXTERNS = $(foreach p, $(ROCKSDB_PLUGIN_W_FUNCS), int $($(p)_FUNC)($(ROCKSDB_PLUGIN_PROTO));)
ROCKSDB_PLUGIN_BUILTINS = $(foreach p, $(ROCKSDB_PLUGIN_W_FUNCS), {\"$(p)\"\, $($(p)_FUNC)}\,)
ROCKSDB_PLUGIN_LDFLAGS = $(foreach plugin, $(ROCKSDB_PLUGINS), $($(plugin)_LDFLAGS))
ROCKSDB_PLUGIN_PKGCONFIG_REQUIRES = $(foreach plugin, $(ROCKSDB_PLUGINS), $($(plugin)_PKGCONFIG_REQUIRES))
ROCKSDB_PLUGIN_TESTS = $(foreach p, $(ROCKSDB_PLUGINS), $(foreach test, $($(p)_TESTS), plugin/$(p)/$(test)))

CXXFLAGS += $(foreach plugin, $(ROCKSDB_PLUGINS), $($(plugin)_CXXFLAGS))
PLATFORM_LDFLAGS += $(ROCKSDB_PLUGIN_LDFLAGS)

# Patch up the link flags for JNI from the plugins
JAVA_LDFLAGS += $(ROCKSDB_PLUGIN_LDFLAGS)
JAVA_STATIC_LDFLAGS += $(ROCKSDB_PLUGIN_LDFLAGS)

# Patch up the list of java native sources with files from the plugins
ROCKSDB_PLUGIN_JNI_NATIVE_SOURCES = $(foreach plugin, $(ROCKSDB_PLUGINS), $(foreach source, $($(plugin)_JNI_NATIVE_SOURCES), plugin/$(plugin)/$(source)))
ALL_JNI_NATIVE_SOURCES = $(JNI_NATIVE_SOURCES) $(ROCKSDB_PLUGIN_JNI_NATIVE_SOURCES)
ROCKSDB_PLUGIN_JNI_CXX_INCLUDEFLAGS = $(foreach plugin, $(ROCKSDB_PLUGINS), -I./plugin/$(plugin))

ifneq ($(strip $(ROCKSDB_PLUGIN_PKGCONFIG_REQUIRES)),)
LDFLAGS := $(LDFLAGS) $(shell pkg-config --libs $(ROCKSDB_PLUGIN_PKGCONFIG_REQUIRES))
ifneq ($(.SHELLSTATUS),0)
$(error pkg-config failed)
endif
CXXFLAGS := $(CXXFLAGS) $(shell pkg-config --cflags $(ROCKSDB_PLUGIN_PKGCONFIG_REQUIRES))
ifneq ($(.SHELLSTATUS),0)
$(error pkg-config failed)
endif
endif

CXXFLAGS += $(ARCHFLAG)

ifeq (,$(shell $(CXX) -fsyntax-only -march=armv8-a+crc+crypto -xc /dev/null 2>&1))
ifneq ($(PLATFORM),OS_MACOSX)
CXXFLAGS += -march=armv8-a+crc+crypto
CFLAGS += -march=armv8-a+crc+crypto
ARMCRC_SOURCE=1
endif
endif

export JAVAC_ARGS
CLEAN_FILES += rocksdb.pc

ifeq ($(V), 1)
$(info $(shell uname -a))
$(info $(shell $(CC) --version))
$(info $(shell $(CXX) --version))
endif

# ccache support
# Set USE_CCACHE=1 to enable ccache, or let it auto-detect
ifndef USE_CCACHE
  CCACHE := $(shell which ccache 2>/dev/null)
  ifneq ($(CCACHE),)
    USE_CCACHE := 1
  else
    USE_CCACHE := 0
  endif
endif

ifeq ($(USE_CCACHE), 1)
  CCACHE := $(shell which ccache 2>/dev/null)
  ifneq ($(CCACHE),)
    $(info Using ccache: $(CCACHE))
    CC := $(CCACHE) $(CC)
    CXX := $(CCACHE) $(CXX)
  else
    $(warning ccache requested but not found in PATH)
  endif
endif

missing_make_config_paths := $(shell				\
	grep "\./\S*\|/\S*" -o $(CURDIR)/make_config.mk | 	\
	while read path;					\
		do [ -e $$path ] || echo $$path; 		\
	done | sort | uniq | grep -v "/DOES/NOT/EXIST")

$(foreach path, $(missing_make_config_paths), \
	$(warning Warning: $(path) does not exist))

# This (the first rule) must depend on "all".
default: all

ifeq ($(PLATFORM), OS_AIX)
# no debug info
else ifneq ($(PLATFORM), IOS)
CFLAGS += -g
CXXFLAGS += -g
else
# no debug info for IOS, that will make our library big
OPT += -DNDEBUG
endif

ifeq ($(PLATFORM), OS_AIX)
ARFLAGS = -X64 rs
STRIPFLAGS = -X64 -x
endif

ifeq ($(LIB_MODE),shared)
# So that binaries are executable from build location, in addition to install location
EXEC_LDFLAGS += -Wl,-rpath -Wl,'$$ORIGIN'
endif

ifeq ($(PLATFORM), OS_MACOSX)
ifeq ($(ARCHFLAG), -arch arm64)
ifneq ($(MACHINE), arm64)
# If we're building on a non-arm64 machine but targeting arm64 Mac, we need to disable
# linking with jemalloc (as it won't be arm64-compatible) and remove some other options
# set during platform detection
DISABLE_JEMALLOC=1
PLATFORM_CCFLAGS := $(filter-out -march=native, $(PLATFORM_CCFLAGS))
PLATFORM_CXXFLAGS := $(filter-out -march=native, $(PLATFORM_CXXFLAGS))
endif
endif
endif

# ASAN doesn't work well with jemalloc. If we're compiling with ASAN, we should use regular malloc.
ifdef COMPILE_WITH_ASAN
	DISABLE_JEMALLOC=1
	ASAN_OPTIONS?=detect_stack_use_after_return=1
	export ASAN_OPTIONS
	EXEC_LDFLAGS += -fsanitize=address
	PLATFORM_CCFLAGS += -fsanitize=address
	PLATFORM_CXXFLAGS += -fsanitize=address
ifeq ($(LIB_MODE),shared)
ifdef USE_CLANG
# Fix false ODR violation; see https://github.com/google/sanitizers/issues/1017
	EXEC_LDFLAGS += -mllvm -asan-use-private-alias=1
	PLATFORM_CXXFLAGS += -mllvm -asan-use-private-alias=1
endif
endif
endif

# TSAN doesn't work well with jemalloc. If we're compiling with TSAN, we should use regular malloc.
ifdef COMPILE_WITH_TSAN
	DISABLE_JEMALLOC=1
	# Use a suppressions file instead of the process-wide TSAN default
	# suppressions hook, which belongs to the final application.
	TSAN_OPTIONS?=suppressions=$(CURDIR)/tools/tsan_suppressions.txt
	export TSAN_OPTIONS
	EXEC_LDFLAGS += -fsanitize=thread
	PLATFORM_CCFLAGS += -fsanitize=thread -fPIC -DFOLLY_SANITIZE_THREAD
	PLATFORM_CXXFLAGS += -fsanitize=thread -fPIC -DFOLLY_SANITIZE_THREAD
        # Turn off -pg when enabling TSAN testing, because that induces
        # a link failure.  TODO: find the root cause
	PROFILING_FLAGS =
	# Limit keys for crash test under TSAN to avoid error:
	# "ThreadSanitizer: DenseSlabAllocator overflow. Dying."
	CRASH_TEST_EXT_ARGS += --max_key=1000000
endif

# AIX doesn't work with -pg
ifeq ($(PLATFORM), OS_AIX)
	PROFILING_FLAGS =
endif

# USAN doesn't work well with jemalloc. If we're compiling with USAN, we should use regular malloc.
ifdef COMPILE_WITH_UBSAN
	DISABLE_JEMALLOC=1
	# Suppress alignment warning because murmurhash relies on casting unaligned
	# memory to integer. Fixing it may cause performance regression. 3-way crc32
	# relies on it too, although it can be rewritten to eliminate with minimal
	# performance regression.
	EXEC_LDFLAGS += -fsanitize=undefined -fno-sanitize-recover=all
	PLATFORM_CCFLAGS += -fsanitize=undefined -fno-sanitize-recover=all -DROCKSDB_UBSAN_RUN
	PLATFORM_CXXFLAGS += -fsanitize=undefined -fno-sanitize-recover=all -DROCKSDB_UBSAN_RUN
endif

ifdef ROCKSDB_VALGRIND_RUN
	PLATFORM_CCFLAGS += -DROCKSDB_VALGRIND_RUN
	PLATFORM_CXXFLAGS += -DROCKSDB_VALGRIND_RUN
endif
ifdef ROCKSDB_FULL_VALGRIND_RUN
	# Some tests are slow when run under valgrind and are only run when
	# explicitly requested via the ROCKSDB_FULL_VALGRIND_RUN compiler flag.
	PLATFORM_CCFLAGS += -DROCKSDB_VALGRIND_RUN -DROCKSDB_FULL_VALGRIND_RUN
	PLATFORM_CXXFLAGS += -DROCKSDB_VALGRIND_RUN -DROCKSDB_FULL_VALGRIND_RUN
endif

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
		PLATFORM_CCFLAGS  += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
		ifeq ($(USE_FOLLY),1)
			PLATFORM_CXXFLAGS += -DUSE_JEMALLOC
			PLATFORM_CCFLAGS  += -DUSE_JEMALLOC
		endif
		ifeq ($(USE_FOLLY_LITE),1)
			PLATFORM_CXXFLAGS += -DUSE_JEMALLOC
			PLATFORM_CCFLAGS  += -DUSE_JEMALLOC
		endif
	endif
	ifdef WITH_JEMALLOC_FLAG
		PLATFORM_LDFLAGS += -ljemalloc
		JAVA_LDFLAGS += -ljemalloc
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS)
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
	PLATFORM_CCFLAGS += $(JEMALLOC_INCLUDE)
endif

ifndef USE_FOLLY
	USE_FOLLY=0
endif

ifndef GTEST_THROW_ON_FAILURE
	export GTEST_THROW_ON_FAILURE=0
endif
ifndef GTEST_HAS_EXCEPTIONS
	export GTEST_HAS_EXCEPTIONS=1
endif

GTEST_DIR = third-party/gtest-1.8.1/fused-src
# AIX: pre-defined system headers are surrounded by an extern "C" block
ifeq ($(PLATFORM), OS_AIX)
	PLATFORM_CCFLAGS += -I$(GTEST_DIR)
	PLATFORM_CXXFLAGS += -I$(GTEST_DIR)
else
	PLATFORM_CCFLAGS += -isystem $(GTEST_DIR)
	PLATFORM_CXXFLAGS += -isystem $(GTEST_DIR)
endif

include folly.mk

ifdef TEST_CACHE_LINE_SIZE
  PLATFORM_CCFLAGS += -DTEST_CACHE_LINE_SIZE=$(TEST_CACHE_LINE_SIZE)
  PLATFORM_CXXFLAGS += -DTEST_CACHE_LINE_SIZE=$(TEST_CACHE_LINE_SIZE)
endif
ifdef TEST_UINT128_COMPAT
  PLATFORM_CCFLAGS += -DTEST_UINT128_COMPAT=1
  PLATFORM_CXXFLAGS += -DTEST_UINT128_COMPAT=1
endif
ifdef ROCKSDB_MODIFY_NPHASH
  PLATFORM_CCFLAGS += -DROCKSDB_MODIFY_NPHASH=1
  PLATFORM_CXXFLAGS += -DROCKSDB_MODIFY_NPHASH=1
endif

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare -Wshadow \
  -Wunused-parameter

ifeq (,$(filter amd64, $(MACHINE)))
	C_WARNING_FLAGS = -Wstrict-prototypes
endif

ifdef USE_CLANG
	# Used by some teams in Facebook
	WARNING_FLAGS += -Wshift-sign-overflow -Wambiguous-reversed-operator \
	  -Wimplicit-fallthrough -Wreinterpret-base-class -Wundefined-reinterpret-cast
endif

ifeq ($(PLATFORM), OS_OPENBSD)
	WARNING_FLAGS += -Wno-unused-lambda-capture
endif

ifndef DISABLE_WARNING_AS_ERROR
	WARNING_FLAGS += -Werror
endif


ifeq ($(NO_THREEWAY_CRC32C), 1)
	CXXFLAGS += -DNO_THREEWAY_CRC32C
endif

CFLAGS += $(C_WARNING_FLAGS) $(WARNING_FLAGS) -I. -I./include $(PLATFORM_CCFLAGS) $(OPT)
CXXFLAGS += $(WARNING_FLAGS) -I. -I./include $(PLATFORM_CXXFLAGS) $(OPT) -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

# Allow offsetof to work on non-standard layout types. Some compiler could
# completely reject our usage of offsetof, but we will solve that when it
# happens.
CXXFLAGS += -Wno-invalid-offsetof

LDFLAGS += $(PLATFORM_LDFLAGS)

LIB_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(LIB_SOURCES))
LIB_OBJECTS += $(patsubst %.cc, $(OBJ_DIR)/%.o, $(ROCKSDB_PLUGIN_SOURCES))
ifeq ($(HAVE_POWER8),1)
LIB_OBJECTS += $(patsubst %.c, $(OBJ_DIR)/%.o, $(LIB_SOURCES_C))
LIB_OBJECTS += $(patsubst %.S, $(OBJ_DIR)/%.o, $(LIB_SOURCES_ASM))
endif

ifeq ($(USE_FOLLY_LITE),1)
  LIB_OBJECTS += $(patsubst %.cpp, $(OBJ_DIR)/%.o, $(FOLLY_SOURCES))
endif

# range_tree is not compatible with non GNU libc on ppc64
# see https://jira.percona.com/browse/PS-7559
ifneq ($(PPC_LIBC_IS_GNU),0)
  LIB_OBJECTS += $(patsubst %.cc, $(OBJ_DIR)/%.o, $(RANGE_TREE_SOURCES))
endif

GTEST = $(OBJ_DIR)/$(GTEST_DIR)/gtest/gtest-all.o
TESTUTIL = $(OBJ_DIR)/test_util/testutil.o
TESTHARNESS = $(OBJ_DIR)/test_util/testharness.o $(TESTUTIL) $(GTEST)
VALGRIND_ERROR = 2
VALGRIND_VER := $(join $(VALGRIND_VER),valgrind)

VALGRIND_OPTS = --error-exitcode=$(VALGRIND_ERROR) --leak-check=full
# Not yet supported: --show-leak-kinds=definite,possible,reachable --errors-for-leak-kinds=definite,possible,reachable

# Work around valgrind hanging on systems with limited internet access
ifneq ($(shell which git 2>/dev/null && git config --get https.proxy),)
  export DEBUGINFOD_URLS=
endif

TEST_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(TEST_LIB_SOURCES) $(MOCK_LIB_SOURCES)) $(GTEST)
BENCH_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(BENCH_LIB_SOURCES))
CACHE_BENCH_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(CACHE_BENCH_LIB_SOURCES))
POINT_LOCK_BENCH_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(POINT_LOCK_BENCH_LIB_SOURCES))
TOOL_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(TOOL_LIB_SOURCES))
ANALYZE_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(ANALYZER_LIB_SOURCES))
STRESS_OBJECTS =  $(patsubst %.cc, $(OBJ_DIR)/%.o, $(STRESS_LIB_SOURCES))

# Exclude build_version.cc -- a generated source file -- from all sources.  Not needed for dependencies
ALL_SOURCES  = $(filter-out util/build_version.cc, $(LIB_SOURCES)) $(TEST_LIB_SOURCES) $(MOCK_LIB_SOURCES) $(GTEST_DIR)/gtest/gtest-all.cc
ALL_SOURCES += $(TOOL_LIB_SOURCES) $(BENCH_LIB_SOURCES) $(CACHE_BENCH_LIB_SOURCES) $(POINT_LOCK_BENCH_LIB_SOURCES) $(ANALYZER_LIB_SOURCES) $(STRESS_LIB_SOURCES)
ALL_SOURCES += $(TEST_MAIN_SOURCES) $(TOOL_MAIN_SOURCES) $(BENCH_MAIN_SOURCES)
ALL_SOURCES += $(ROCKSDB_PLUGIN_SOURCES) $(ROCKSDB_PLUGIN_TESTS)

PLUGIN_TESTS = $(patsubst %.cc, %, $(notdir $(ROCKSDB_PLUGIN_TESTS)))
TESTS = $(patsubst %.cc, %, $(notdir $(TEST_MAIN_SOURCES)))
TESTS += $(patsubst %.c, %, $(notdir $(TEST_MAIN_SOURCES_C)))
TESTS += $(PLUGIN_TESTS)

# `make check-headers` to verify that each header file includes its own deps
# and that public headers do not depend on internal headers
ifneq ($(filter check-headers, $(MAKECMDGOALS)),)
# TODO: add/support JNI headers
	DEV_HEADER_DIRS := $(sort include/ $(dir $(ALL_SOURCES)))
	FOLLY_DEPENDENT_HEADERS := \
		include/rocksdb/coro_db.h \
		include/rocksdb/coro_external_table.h \
		include/rocksdb/utilities/coro_stackable_db.h
# Some headers like in port/ are platform-specific
	DEV_HEADERS_TO_CHECK := $(filter-out $(FOLLY_DEPENDENT_HEADERS), $(shell $(FIND) $(DEV_HEADER_DIRS) -type f -name '*.h' | grep -E -v 'port/|plugin/|range_tree/|secondary_index/'))
	PUBLIC_HEADERS_TO_CHECK := $(filter-out $(FOLLY_DEPENDENT_HEADERS), $(shell $(FIND) include/ -type f -name '*.h'))
else
	DEV_HEADERS_TO_CHECK :=
	PUBLIC_HEADERS_TO_CHECK :=
endif
HEADER_OK_FILES = $(patsubst %.h, %.h.ok, $(DEV_HEADERS_TO_CHECK)) \
	$(patsubst %.h, %.h.pub, $(PUBLIC_HEADERS_TO_CHECK))

AM_V_CCH = $(am__v_CCH_$(V))
am__v_CCH_ = $(am__v_CCH_$(AM_DEFAULT_VERBOSITY))
am__v_CCH_0 = @echo "  CC.h    " $<;
am__v_CCH_1 =

# verify headers include their own dependencies, under dev build settings
%.h.ok: %.h # .h.ok not actually created, so re-checked on each invocation
# -DROCKSDB_NAMESPACE=42 ensures the namespace header is included
	$(AM_V_CCH) echo '#include "$<"' | $(CXX) $(CXXFLAGS) \
	  -DROCKSDB_NAMESPACE=42 -x c++ -c - -o /dev/null

# verify public headers do not depend on internal headers, under typical
# user build settings
%.h.pub: %.h # .h.pub not actually created, so re-checked on each invocation
	$(AM_V_CCH) cd include/ && echo '#include "$(patsubst include/%,%,$<)"' | \
	  $(CXX) -std=$(or $(ROCKSDB_CXX_STANDARD),c++20) -I. -DROCKSDB_NAMESPACE=42 -x c++ -c - -o /dev/null
	build_tools/check-public-header.sh $<

check-headers: $(HEADER_OK_FILES)

# options_settable_test doesn't pass with UBSAN as we use hack in the test
ifdef ASSERT_STATUS_CHECKED
# TODO: finish fixing all tests to pass this check
TESTS_FAILING_ASC = \
	c_test \
	env_test \
	range_locking_test \
	testutil_test \

# Since we have very few ASC exclusions left, excluding them from
# the build is the most convenient way to exclude them from testing
TESTS := $(filter-out $(TESTS_FAILING_ASC),$(TESTS))
endif

ROCKSDBTESTS_SUBSET ?= $(TESTS)

# c_test - doesn't use gtest, can't be sharded
# Other tests previously listed here (backup_engine_test, db_bloom_filter_test,
# perf_context_test, etc.) were NON_PARALLEL due to /dev/shm memory concerns
# when the old per-test-case sharding spawned thousands of processes. With the
# new gtest-based sharding (GTEST_SHARD_SIZE=10, max NCORES*8 shards), at most
# ~4 shards run concurrently on CI (4 cores), so peak memory is manageable
# (4 * 1GB = 4GB << 16GB shm).
NON_PARALLEL_TEST = \
	c_test \
	$(PLUGIN_TESTS) \

PARALLEL_TEST = $(filter-out $(NON_PARALLEL_TEST), $(ROCKSDBTESTS_SUBSET))

# Not necessarily well thought out or up-to-date, but matches old list
TESTS_PLATFORM_DEPENDENT := \
	db_basic_test \
	db_blob_basic_test \
	db_blob_direct_write_test \
	db_encryption_test \
	external_sst_file_basic_test \
	auto_roll_logger_test \
	bloom_test \
	dynamic_bloom_test \
	c_test \
	checkpoint_test \
	sorted_run_builder_test \
	crc32c_test \
	coding_test \
	inlineskiplist_test \
	env_basic_test \
	env_test \
	env_logger_test \
	io_posix_test \
	hash_test \
	random_test \
	ribbon_test \
	thread_local_test \
	work_queue_test \
	rate_limiter_test \
	perf_context_test \
	iostats_context_test \

# Sort ROCKSDBTESTS_SUBSET for filtering, except db_test is special (expensive)
# so is placed first (out-of-order)
ROCKSDBTESTS_SUBSET := $(filter db_test, $(ROCKSDBTESTS_SUBSET)) $(sort $(filter-out db_test, $(ROCKSDBTESTS_SUBSET)))

ifdef ROCKSDBTESTS_START
        ROCKSDBTESTS_SUBSET := $(shell echo $(ROCKSDBTESTS_SUBSET) | sed 's/^.*$(ROCKSDBTESTS_START)/$(ROCKSDBTESTS_START)/')
endif

ifdef ROCKSDBTESTS_END
        ROCKSDBTESTS_SUBSET := $(shell echo $(ROCKSDBTESTS_SUBSET) | sed 's/$(ROCKSDBTESTS_END).*//')
endif

ifeq ($(ROCKSDBTESTS_PLATFORM_DEPENDENT), only)
        ROCKSDBTESTS_SUBSET := $(filter $(TESTS_PLATFORM_DEPENDENT), $(ROCKSDBTESTS_SUBSET))
else ifeq ($(ROCKSDBTESTS_PLATFORM_DEPENDENT), exclude)
        ROCKSDBTESTS_SUBSET := $(filter-out $(TESTS_PLATFORM_DEPENDENT), $(ROCKSDBTESTS_SUBSET))
endif

# bench_tool_analyer main is in bench_tool_analyzer_tool, or this would be simpler...
TOOLS = $(patsubst %.cc, %, $(notdir $(patsubst %_tool.cc, %.cc, $(TOOLS_MAIN_SOURCES))))

TEST_LIBS = \
	librocksdb_env_basic_test.a

# TODO: add back forward_iterator_bench, after making it build in all environemnts.
BENCHMARKS = $(patsubst %.cc, %, $(notdir $(BENCH_MAIN_SOURCES)))

MICROBENCHS = $(patsubst %.cc, %, $(notdir $(MICROBENCH_SOURCES)))

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
  LIBNAME=librocksdb
# we should only run rocksdb in production with DEBUG_LEVEL 0
ifneq ($(DEBUG_LEVEL),0)
  LIBDEBUG=_debug
endif
endif
STATIC_LIBRARY = ${LIBNAME}$(LIBDEBUG).a
STATIC_TEST_LIBRARY =  ${LIBNAME}_test$(LIBDEBUG).a
STATIC_TOOLS_LIBRARY = ${LIBNAME}_tools$(LIBDEBUG).a
STATIC_STRESS_LIBRARY = ${LIBNAME}_stress$(LIBDEBUG).a

ALL_STATIC_LIBS = $(STATIC_LIBRARY) $(STATIC_TEST_LIBRARY) $(STATIC_TOOLS_LIBRARY) $(STATIC_STRESS_LIBRARY)

SHARED_TEST_LIBRARY =  ${LIBNAME}_test$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
SHARED_TOOLS_LIBRARY = ${LIBNAME}_tools$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
SHARED_STRESS_LIBRARY = ${LIBNAME}_stress$(LIBDEBUG).$(PLATFORM_SHARED_EXT)

ALL_SHARED_LIBS = $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4) $(SHARED_TEST_LIBRARY) $(SHARED_TOOLS_LIBRARY) $(SHARED_STRESS_LIBRARY)

ifeq ($(LIB_MODE),shared)
LIBRARY=$(SHARED1)
TEST_LIBRARY=$(SHARED_TEST_LIBRARY)
TOOLS_LIBRARY=$(SHARED_TOOLS_LIBRARY)
STRESS_LIBRARY=$(SHARED_STRESS_LIBRARY)
CLOUD_LIBRARY=$(SHARED_CLOUD_LIBRARY)
else
LIBRARY=$(STATIC_LIBRARY)
TEST_LIBRARY=$(STATIC_TEST_LIBRARY)
TOOLS_LIBRARY=$(STATIC_TOOLS_LIBRARY)
endif
STRESS_LIBRARY=$(STATIC_STRESS_LIBRARY)

ROCKSDB_MAJOR = $(shell grep -E "ROCKSDB_MAJOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell grep -E "ROCKSDB_MINOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell grep -E "ROCKSDB_PATCH.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)

# If NO_UPDATE_BUILD_VERSION is set we don't update util/build_version.cc, but
# the file needs to already exist or else the build will fail
ifndef NO_UPDATE_BUILD_VERSION

# By default, use the current date-time as the date.  If there are no changes,
# we will use the last commit date instead.
build_date := $(shell date "+%Y-%m-%d %T")

ifdef FORCE_GIT_SHA
	git_sha := $(FORCE_GIT_SHA)
	git_mod := 1
	git_date := $(build_date)
else
	git_sha := $(shell git rev-parse HEAD 2>/dev/null)
	git_tag  := $(shell git symbolic-ref -q --short HEAD 2> /dev/null || git describe --tags --exact-match 2>/dev/null)
	git_mod  := $(shell git diff-index HEAD --quiet 2>/dev/null; echo $$?)
	git_date := $(shell git log -1 --date=format:"%Y-%m-%d %T" --format="%ad" 2>/dev/null)
endif
gen_build_version = sed -e s/@GIT_SHA@/$(git_sha)/ -e s:@GIT_TAG@:"$(git_tag)": -e s/@GIT_MOD@/"$(git_mod)"/ -e s/@BUILD_DATE@/"$(build_date)"/ -e s/@GIT_DATE@/"$(git_date)"/ -e s/@ROCKSDB_PLUGIN_BUILTINS@/'$(ROCKSDB_PLUGIN_BUILTINS)'/ -e s/@ROCKSDB_PLUGIN_EXTERNS@/"$(ROCKSDB_PLUGIN_EXTERNS)"/ util/build_version.cc.in

# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
util/build_version.cc: $(filter-out $(OBJ_DIR)/util/build_version.o, $(LIB_OBJECTS)) util/build_version.cc.in
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@
endif
CLEAN_FILES += util/build_version.cc
default: all

#-----------------------------------------------
# Create platform independent shared libraries.
#-----------------------------------------------
ifneq ($(PLATFORM_SHARED_EXT),)

ifneq ($(PLATFORM_SHARED_VERSIONED),true)
SHARED1 = ${LIBNAME}$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
SHARED2 = $(SHARED1)
SHARED3 = $(SHARED1)
SHARED4 = $(SHARED1)
SHARED = $(SHARED1)
else
SHARED_MAJOR = $(ROCKSDB_MAJOR)
SHARED_MINOR = $(ROCKSDB_MINOR)
SHARED_PATCH = $(ROCKSDB_PATCH)
SHARED1 = ${LIBNAME}.$(PLATFORM_SHARED_EXT)
ifeq ($(PLATFORM), OS_MACOSX)
SHARED_OSX = $(LIBNAME)$(LIBDEBUG).$(SHARED_MAJOR)
SHARED2 = $(SHARED_OSX).$(PLATFORM_SHARED_EXT)
SHARED3 = $(SHARED_OSX).$(SHARED_MINOR).$(PLATFORM_SHARED_EXT)
SHARED4 = $(SHARED_OSX).$(SHARED_MINOR).$(SHARED_PATCH).$(PLATFORM_SHARED_EXT)
else
SHARED2 = $(SHARED1).$(SHARED_MAJOR)
SHARED3 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)
endif # MACOSX
SHARED = $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4)
$(SHARED1): $(SHARED4) $(SHARED2)
	ln -fs $(SHARED4) $(SHARED1)
$(SHARED2): $(SHARED4) $(SHARED3)
	ln -fs $(SHARED4) $(SHARED2)
$(SHARED3): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED3)

endif   # PLATFORM_SHARED_VERSIONED
$(SHARED4): $(LIB_OBJECTS)
	$(AM_V_CCLD) $(CXX) $(PLATFORM_SHARED_LDFLAGS)$(SHARED3) $(LIB_OBJECTS) $(LDFLAGS) -o $@
endif  # PLATFORM_SHARED_EXT

.PHONY: check clean coverage ldb_tests package dbg gen-pc build_size \
	release tags tags0 valgrind_check format static_lib shared_lib all \
	rocksdbjavastatic rocksdbjava install install-static install-shared \
	uninstall analyze tools tools_lib check-headers checkout_folly clang-tidy \
	check-c-api-c_test

# Auto-configure git hooks on first build so developers do not need to run
# "make install-hooks" manually. This is a no-op if already set.
setup-hooks:
	@if [ -d .git ] && [ -d githooks ]; then \
		cur=$$(git config core.hooksPath 2>/dev/null); \
		if [ "$$cur" != "githooks" ]; then \
			git config core.hooksPath githooks; \
			echo "git hooks: configured core.hooksPath = githooks"; \
		fi; \
	fi

all: setup-hooks $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(TESTS)

all_but_some_tests: $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(ROCKSDBTESTS_SUBSET)

static_lib: $(STATIC_LIBRARY)

shared_lib: $(SHARED)

stress_lib: $(STRESS_LIBRARY)

tools: $(TOOLS)

tools_lib: $(TOOLS_LIBRARY)

test_libs: $(TEST_LIBS)

benchmarks: $(BENCHMARKS)

microbench: $(MICROBENCHS)

run_microbench: $(MICROBENCHS)
	for t in $(MICROBENCHS); do echo "===== Running benchmark $$t (`date`)"; ./$$t || exit 1; done;

dbg: $(LIBRARY) $(BENCHMARKS) tools $(TESTS)

# creates library and programs
release: clean
	LIB_MODE=$(LIB_MODE) DEBUG_LEVEL=0 $(MAKE) $(LIBRARY) tools db_bench

coverage: clean
	COVERAGEFLAGS="-fprofile-arcs -ftest-coverage" LDFLAGS+="-lgcov" $(MAKE) J=1 all check
	cd coverage && ./coverage_test.sh
	# Delete intermediate files
	$(FIND) . -type f \( -name "*.gcda" -o -name "*.gcno" \) -exec rm -f {} \;

# Run all tests in parallel, accumulating per-test logs in t/log-*.
#
# Each t/run-* file is a tiny generated bourne shell script that invokes one of
# sub-tests. Why use a file for this?  Because that makes the invocation of
# parallel below simpler, which in turn makes the parsing of parallel's
# LOG simpler (the latter is for live monitoring as parallel
# tests run).
#
# Test names are extracted by running tests with --gtest_list_tests.
# This filter removes the "#"-introduced comments, and expands to
# fully-qualified names by changing input like this:
#
#   DBTest.
#     Empty
#     WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.
#     MultiThreaded/0  # GetParam() = 0
#     MultiThreaded/1  # GetParam() = 1
#
# into this:
#
#   DBTest.Empty
#   DBTest.WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/0
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/1
#

parallel_tests = $(patsubst %,parallel_%,$(PARALLEL_TEST))
.PHONY: gen_parallel_tests $(parallel_tests)
# Shard size controls how many test cases run per process. The actual number
# of shards per binary is: min(ceil(test_count / shard_size), NCORES * 8)
# This adapts to machine size: many small shards on beefy machines, fewer
# larger shards on CI (typically 2-4 cores).
GTEST_SHARD_SIZE ?= 10
NCORES ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Per-binary overrides for GTEST_SHARD_SIZE, used to chop up binaries whose
# individual test cases are slow enough that the default of 10 leaves one
# shard on the critical path of "make check". Format: whitespace-separated
# tokens "BINARY:SIZE". Sizes were tuned from `make suggest-slow-tests`
# output to keep max shard time under ~20s on a beefy box. Reducing too far
# adds per-shard fixed overhead, so don't use 1 unless individual tests are
# >5s.
SHARD_SIZE_OVERRIDES ?= \
  point_lock_manager_stress_test:1 \
  external_sst_file_test:1 \
  external_sst_file_basic_test:2 \
  db_test:3 \
  compaction_service_test:1

$(parallel_tests):
	$(AM_V_at)TEST_BINARY=$(patsubst parallel_%,%,$@); \
  TEST_COUNT=` \
    (./$$TEST_BINARY --gtest_list_tests 2>/dev/null || echo "  list_failure") \
    | grep -c '^ '`; \
	if [ "$$TEST_COUNT" -le 0 ]; then TEST_COUNT=1; fi; \
	SHARD_SIZE=$(GTEST_SHARD_SIZE); \
	for o in $(SHARD_SIZE_OVERRIDES); do \
		case "$$o" in \
			"$$TEST_BINARY":*) SHARD_SIZE=$${o#*:} ;; \
		esac; \
	done; \
	MAX_SHARDS=$$(( $(NCORES) * 8 )); \
	NUM_SHARDS=$$(( (TEST_COUNT + SHARD_SIZE - 1) / SHARD_SIZE )); \
	if [ "$$NUM_SHARDS" -gt "$$MAX_SHARDS" ]; then NUM_SHARDS=$$MAX_SHARDS; fi; \
	if [ "$$NUM_SHARDS" -le 0 ]; then NUM_SHARDS=1; fi; \
	echo "  Generating $$NUM_SHARDS shards for $$TEST_BINARY ($$TEST_COUNT tests, shard_size=$$SHARD_SIZE)"; \
	SHARD_IDX=0; \
	while [ "$$SHARD_IDX" -lt "$$NUM_SHARDS" ]; do \
		if [ -n "$(CI_TOTAL_SHARDS)" ] && [ $$(( $$SHARD_IDX % $(CI_TOTAL_SHARDS) )) -ne $(CI_SHARD_INDEX) ]; then \
			SHARD_IDX=$$((SHARD_IDX + 1)); \
			continue; \
		fi; \
		TEST_SCRIPT=t/run-$$TEST_BINARY-shard-$$SHARD_IDX; \
    printf '%s\n' \
      '#!/bin/sh' \
      "d=\$(TEST_TMPDIR)$$TEST_SCRIPT" \
      'mkdir -p $$d' \
      "TEST_TMPDIR=\$$d GTEST_TOTAL_SHARDS=$$NUM_SHARDS GTEST_SHARD_INDEX=$$SHARD_IDX $(DRIVER) ./$$TEST_BINARY" \
      'test_retcode=$$?' \
      '[ $$test_retcode -eq 0 ] && rm -rf $$d' \
      'exit $$test_retcode' \
		> $$TEST_SCRIPT; \
		chmod a=rx $$TEST_SCRIPT; \
		SHARD_IDX=$$((SHARD_IDX + 1)); \
	done

gen_parallel_tests:
	$(AM_V_at)mkdir -p t
	$(AM_V_at)$(FIND) t -type f -name 'run-*' -exec rm -f {} \;
	$(MAKE) $(parallel_tests)

# Auxiliary (non-unit-test) checks that are part of `make check`. Kept as a
# single list so the same set can run as parallel jobs alongside the C++ unit
# tests (see gen_check_scripts + check_0) and serially in the J=1 fallback.
CHECK_EXTRA_TARGETS =
ifneq ($(PLATFORM), OS_AIX)
CHECK_EXTRA_TARGETS += check-all-python
ifndef ASSERT_STATUS_CHECKED # not yet working with these tests
CHECK_EXTRA_TARGETS += ldb_tests db_crashtest_tests rocksdb_dump_test
endif
endif
ifndef SKIP_FORMAT_BUCK_CHECKS
CHECK_EXTRA_TARGETS += check-format check-buck-targets check-sources check-workflow-yaml check-c-api-gen
endif

# Generate a t/run-<target> wrapper per enabled auxiliary check; check_0's
# `$(FIND) t -name 'run-*'` then runs them as gnu_parallel jobs alongside the
# unit tests, each framed with a gtest-style pass/fail line. Under CI sharding,
# round-robin across shards so each check runs on exactly one shard. Kept
# separate from gen_parallel_tests (which cleans t/run-* first) so the valgrind_*
# targets, which also glob t/run-*, don't pick these up.
.PHONY: gen_check_scripts
gen_check_scripts:
	$(AM_V_at)mkdir -p t; \
	idx=0; \
	for tgt in $(CHECK_EXTRA_TARGETS); do \
		if [ -z "$(CI_TOTAL_SHARDS)" ] || [ $$(( idx % $(CI_TOTAL_SHARDS) )) -eq $(CI_SHARD_INDEX) ]; then \
			s=t/run-$$tgt; \
			printf '%s\n' \
				'#!/bin/sh' \
				'# Generated by `make gen_check_scripts`.' \
				"t=$$tgt" \
				'# Drop the parent jobserver from MAKEFLAGS (its fds are lost through' \
				'# gnu_parallel, causing a spurious warning); keep overrides after "--".' \
				'MAKEFLAGS=$$(printf "%s" "$$MAKEFLAGS" | sed -e "s/ --jobserver-auth=[^ ]*//g" -e "s/ --jobserver-fds=[^ ]*//g" -e "s/ -j[0-9]*//g")' \
				'export MAKEFLAGS' \
				'echo; echo "[----------]"; echo "[ RUN      ] >>> make $$t <<<"' \
				'$(MAKE) --no-print-directory ROCKSDB_RECURSIVE_MAKE_SAME_CONFIG=1 "$$t"' \
				'rc=$$?' \
				'if [ "$$rc" = 0 ]; then' \
				'  echo "[       OK ] >>> make $$t <<<"' \
				'else' \
				'  echo "[  FAILED  ] >>> make $$t <<<"' \
				'fi' \
				'echo "[----------]"' \
				'exit $$rc' \
				> $$s; \
			chmod a=rx $$s; \
		fi; \
		idx=$$((idx + 1)); \
	done

# Reorder input lines (one per test or per shard) so the longest-running
# tests/shards start first under "make check" parallel scheduling. The trick
# is to prefix matched names with a fake duration (100), sort numerically
# descending, then strip the prefix. Slow-matched tests therefore come ahead
# of all unmatched tests; ordering within each group is unspecified.
#
# Why this matters: gnu_parallel hands out jobs in input order. If a slow
# test starts only after most others have finished, the run is bottlenecked
# on its tail. Front-loading slow tests overlaps them with the bulk of
# faster ones for much better wall-clock time.
#
# Maintenance:
#   1) Run `make check` so LOG is up-to-date.
#   2) Run `make suggest-slow-tests` to see candidate slow binaries.
#   3) Add binaries with high per-shard time (slow critical path) or high
#      total time across many shards (need early queueing for fan-out).
# Each alternative is wrapped in `^.*NAME.*$$` so the *whole input line* is
# captured into $$1; then `s,(...),100 $$1,` prepends "100 " to the line.
#
# Tiers below are based on observed timings (see suggest-slow-tests).
# Tier 1: max single-shard time >= 30s (critical-path bottlenecks).
# Tier 2: max single-shard time 15-30s.
# Tier 3: huge total time across many tiny shards; front-loading them keeps
#         the tail of the run busy while big shards finish.
slow_test_regexp = \
	^.*point_lock_manager_stress_test.*$$|^.*db_test.*$$|^.*external_sst_file_test.*$$|^.*compaction_service_test.*$$|^.*corruption_test.*$$|^.*comparator_db_test.*$$|^.*external_sst_file_basic_test.*$$|^.*rate_limiter_test.*$$|^.*db_compaction_test.*$$|^.*write_prepared_transaction_test.*$$|^.*db_merge_operator_test.*$$|\
	^.*db_dynamic_level_test.*$$|^.*db_bloom_filter_test.*$$|^.*error_handler_fs_test.*$$|^.*merge_helper_test.*$$|^.*transaction_test.*$$|^.*db_kv_checksum_test.*$$|^.*inlineskiplist_test.*$$|\
	^.*db_with_timestamp_basic_test.*$$|^.*table_test.*$$|^.*db_wal_test.*$$|^.*block_based_table_reader_test.*$$|^.*block_test.*$$|\
	^.*run-check-c-api-gen.*$$|^.*run-check-format.*$$
prioritize_long_running_tests =						\
  perl -pe 's,($(slow_test_regexp)),100 $$1,'				\
    | sort -k1,1gr							\
    | sed 's/^[.0-9]* //'

# Helper: print binaries observed to be slow in the last `make check` run.
# Use this to decide whether `slow_test_regexp` above needs updating.
# Aggregates per-binary across shards; flags any binary whose max single-shard
# time is >= 20s OR whose total time is >= 200s.
.PHONY: suggest-slow-tests
suggest-slow-tests:
	@if [ ! -s LOG ]; then \
	  echo "No (or empty) LOG file. Run 'make check' first." >&2; exit 1; \
	fi
	@bash -c '$(quoted_perl_command)' < LOG | awk '\
	  { \
	    t=$$1; name=$$3; \
	    sub(/^t\/run-/, "", name); \
	    sub(/-shard-[0-9]+$$/, "", name); \
	    total[name] += t; \
	    if (t > max[name]) max[name] = t; \
	    count[name]++; \
	  } \
	  END { \
	    printf "%8s %8s %5s  %s\n", "TOTAL_s", "MAX_s", "SHRDS", "BINARY"; \
	    for (n in total) \
	      if (max[n] >= 20 || total[n] >= 200) \
	        printf "%8.1f %8.1f %5d  %s\n", total[n], max[n], count[n], n; \
	  }' | (read header; echo "$$header"; sort -k2,2gr)

# "make check" uses
# Run with "make J=1 check" to disable parallelism in "make check".
# Run with "make J=200% check" to run two parallel jobs per core.
# The default is to run one job per core (J=100%).
# See "man parallel" for its "-j ..." option.
J ?= 100%

# Use this regexp to select the subset of tests whose names match.
tests-regexp = .
EXCLUDE_TESTS_REGEX ?= "^$$"

ifeq ($(PRINT_PARALLEL_OUTPUTS), 1)
	parallel_redir =
else ifeq ($(QUIET_PARALLEL_TESTS), 1)
	parallel_redir = >& t/$(test_log_prefix)log-{/}
else
# Default: print failure output only, as it happens
# Note: gnu_parallel --eta is now always used, but has been modified to provide
# only infrequent updates when not connected to a terminal. (CircleCI will
# kill a job if no output for 10min.)
	parallel_redir = >& t/$(test_log_prefix)log-{/} || bash -c "cat t/$(test_log_prefix)log-{/}; exit $$?"
endif

.PHONY: check_0
check_0:
	@printf '%s\n' ''						\
	  'To monitor subtest <duration,pass/fail,name>,'		\
	  '  run "make watch-log" in a separate window' '';		\
	{ \
		NON_PARALLEL_LIST="$(filter-out $(PARALLEL_TEST),$(ROCKSDBTESTS_SUBSET))"; \
		if [ -n "$$NON_PARALLEL_LIST" ]; then \
		  printf './%s\n' $$NON_PARALLEL_LIST \
		    | if [ -n "$(CI_TOTAL_SHARDS)" ]; then \
		        awk -v s=$(CI_SHARD_INDEX) -v n=$(CI_TOTAL_SHARDS) '(NR-1)%n==s'; \
		      else cat; fi; \
		fi; \
		$(FIND) t -name 'run-*' -print; \
	} \
	  | $(prioritize_long_running_tests)				\
	  | grep -E '$(tests-regexp)'					\
	  | grep -E -v '$(EXCLUDE_TESTS_REGEX)'					\
	  | build_tools/gnu_parallel -j$(J) --plain --joblog=LOG --eta --gnu \
	    --tmpdir=$(TEST_TMPDIR) '{} $(parallel_redir)' ; \
	parallel_retcode=$$? ; \
	awk '{ if ($$7 != 0 || $$8 != 0) { if ($$7 == "Exitval") { h = $$0; } else { if (!f) print h; print; f = 1 } } } END { if(f) exit 1; }' < LOG ; \
	awk_retcode=$$?; \
	if [ $$parallel_retcode -ne 0 ] || [ $$awk_retcode -ne 0 ] ; then exit 1 ; fi

valgrind-exclude-regexp = InlineSkipTest.ConcurrentInsert|TransactionStressTest.DeadlockStress|DBCompactionTest.SuggestCompactRangeNoTwoLevel0Compactions|BackupableDBTest.RateLimiting|DBTest.CloseSpeedup|DBTest.ThreadStatusFlush|DBTest.RateLimitingTest|DBTest.EncodeDecompressedBlockSizeTest|FaultInjectionTest.UninstalledCompaction|HarnessTest.Randomized|ExternalSSTFileTest.CompactDuringAddFileRandom|ExternalSSTFileTest.IngestFileWithGlobalSeqnoRandomized|MySQLStyleTransactionTest.TransactionStressTest

.PHONY: valgrind_check_0
valgrind_check_0: test_log_prefix := valgrind_
valgrind_check_0:
	@printf '%s\n' ''						\
	  'To monitor subtest <duration,pass/fail,name>,'		\
	  '  run "make watch-log" in a separate window' '';		\
	{								\
	  printf './%s\n' $(filter-out $(PARALLEL_TEST) %skiplist_test options_settable_test, $(TESTS));		\
	  $(FIND) t -name 'run-*' -print; \
	}								\
	  | $(prioritize_long_running_tests)				\
	  | grep -E '$(tests-regexp)'					\
	  | grep -E -v '$(valgrind-exclude-regexp)'					\
	  | build_tools/gnu_parallel -j$(J) --plain --joblog=LOG --eta --gnu \
	   --tmpdir=$(TEST_TMPDIR) \
	   '(if [[ "{}" == "./"* ]] ; then $(DRIVER) {}; else {}; fi) \
	  $(parallel_redir)' \

CLEAN_FILES += t LOG $(TEST_TMPDIR)

# When running parallel "make check", you can monitor its progress
# from another window.
# Run "make watch_LOG" to show the duration,PASS/FAIL,name of parallel
# tests as they are being run.  We sort them so that longer-running ones
# appear at the top of the list and any failing tests remain at the top
# regardless of their duration. As with any use of "watch", hit ^C to
# interrupt.
watch-log:
	$(WATCH) --interval=0 'sort -k7,7nr -k4,4gr LOG|$(quoted_perl_command)'

dump-log:
	bash -c '$(quoted_perl_command)' < LOG

# Machine-parseable progress output for automated monitoring (e.g., Claude Code)
# Outputs JSON: {"status":"running","completed":45,"total":100,"failed":0,"percent":45,"eta_seconds":120}
check-progress:
	@build_tools/check_progress.sh

# If J != 1 and GNU parallel is installed, run the tests in parallel,
# via the check_0 rule above.  Otherwise, run them sequentially.
check: all
	$(AM_V_at)echo "Cleaning up stale test directories older than 3 hours..."; \
	  test_tmpdir_parent=$$(dirname $(TEST_TMPDIR)); \
	  find $$test_tmpdir_parent -maxdepth 1 -name 'rocksdb.*' -type d \
	    -mmin +180 -exec rm -rf {} + 2>/dev/null; \
	  true
	$(MAKE) gen_parallel_tests
	$(AM_V_GEN)if test "$(J)" != 1                                  \
	    && (build_tools/gnu_parallel --gnu --help 2>/dev/null) |                    \
	        grep -q 'GNU Parallel';                                 \
	then                                                            \
	    $(MAKE) gen_check_scripts;                                  \
	    $(MAKE) T="$$t" check_0;                       \
	else                                                            \
	    for t in $(TESTS); do                                       \
	      echo "===== Running $$t (`date`)"; ./$$t || exit 1; done;          \
	    for t in $(CHECK_EXTRA_TARGETS); do                                 \
	      echo "===== Running $$t (`date`)"; $(MAKE) $$t || exit 1; done;    \
	fi
	rm -rf $(TEST_TMPDIR)
# NOTE: ^^^^^^^^ don't add new auxiliary checks here, but instead to
# CHECK_EXTRA_TARGETS

# Check that the auto-generated C API files are up to date. It regenerates the
# fragments and the inlined c.h/c.cc and compares them against a snapshot of the
# checked-in copies (no net change when everything is up to date). It requires
# clang++ (libclang, used to parse the C++ headers) and clang-format. When those
# are unavailable the staleness/compat sub-checks are SKIPPED with a message so
# `make check` still works without the codegen toolchain; the link-completeness
# sub-check always runs (it needs no toolchain).
#
# Pin the formatter to match CI by setting CLANG_FORMAT_BINARY, e.g.:
#   make check-c-api-gen CLANG_FORMAT_BINARY=clang-format-21
# This target is part of `make check` and is skipped by SKIP_FORMAT_BUCK_CHECKS.
#
# Set CHECK_C_API_GEN_STRICT=1 to turn every "skip" below into a hard error, so a
# core CI job that runs this target cannot silently regress to a no-op if a
# prerequisite (clang++, or the compat baseline ref) goes missing. The dedicated
# build-linux-clang-21-no_test_run CI job runs this target with the flag set.
# Any non-empty value other than 0/no/false enables strict mode.
CLANG_FORMAT_BINARY ?=
# Backward-compatibility baseline for the C API (signature-level) check. CI
# overrides this with the PR's merge target or the branch's pre-push tip;
# locally it falls back to main or origin/main and is skipped if neither resolves.
API_COMPAT_REF ?= main
CHECK_C_API_GEN_STRICT ?=
check-c-api-gen:
	# Link-completeness is a property of the checked-in c.h/c.cc and needs no
	# clang toolchain, so it always runs: every declared public C API function
	# must have exactly one definition (guards against dropped wrappers that
	# would break downstream language bindings at link time).
	$(PYTHON) tools/c_api_gen/check_api_completeness.py
	# Backward-compatibility: no public C function may be removed or have its
	# signature changed vs the baseline. Skipped if the baseline ref is not
	# resolvable locally (CI passes an explicit ref), unless CHECK_C_API_GEN_STRICT.
	@strict=""; case "$(CHECK_C_API_GEN_STRICT)" in ""|0|no|NO|false|FALSE) ;; *) strict=1 ;; esac; \
	ref=""; \
	if git rev-parse --verify --quiet "$(API_COMPAT_REF)^{commit}" >/dev/null; then ref="$(API_COMPAT_REF)"; \
	elif git rev-parse --verify --quiet "origin/$(API_COMPAT_REF)^{commit}" >/dev/null; then ref="origin/$(API_COMPAT_REF)"; fi; \
	if [ -n "$$ref" ]; then \
	  $(PYTHON) tools/c_api_gen/check_api_compatibility.py --ref "$$ref"; \
	elif [ -n "$$strict" ]; then \
	  echo "ERROR: C API compat baseline '$(API_COMPAT_REF)' not resolvable and CHECK_C_API_GEN_STRICT is set" >&2; exit 1; \
	else \
	  echo "Skipping C API backward-compatibility check ($(API_COMPAT_REF) not found; set API_COMPAT_REF)"; \
	fi
	# Staleness: regenerate and confirm the checked-in output is current. Needs a
	# clang++ (the generator parses C++ ASTs). Use CLANG_CXX, which
	# build_tools/build_detect_platform resolves once (the fbcode clang++ in
	# fbcode builds, else a versioned clang++ on PATH) and passes explicitly, so
	# the check and the generator agree on the same binary without heuristics.
	@strict=""; case "$(CHECK_C_API_GEN_STRICT)" in ""|0|no|NO|false|FALSE) ;; *) strict=1 ;; esac; \
	cf_arg=""; \
	if [ -n "$(CLANG_FORMAT_BINARY)" ]; then cf_arg="--clang-format $(CLANG_FORMAT_BINARY)"; fi; \
	if [ -n "$(CLANG_CXX)" ] && command -v "$(CLANG_CXX)" >/dev/null 2>&1; then \
	  $(PYTHON) tools/c_api_gen/verify_generated_up_to_date.py --clang "$(CLANG_CXX)" $$cf_arg; \
	elif [ -n "$$strict" ]; then \
	  echo "ERROR: no clang++ found (CLANG_CXX unset) and CHECK_C_API_GEN_STRICT is set; cannot run the C API staleness check" >&2; exit 1; \
	else \
	  echo "Skipping C API codegen staleness check (no clang++ found; CLANG_CXX unset, install clang++ to enable)"; \
	fi

# Regenerate the checked-in C API generated files (fragments under c_api_gen/,
# the inlined include/rocksdb/c.h and db/c.cc, and the round-trip tests). Uses
# CLANG_CXX (resolved by build_tools/build_detect_platform) so it works in a gcc
# build too. Aborts up front if no usable clang++ is available, so a bad
# toolchain never leaves the tree in a partially regenerated state. Pin the
# formatter to match CI with CLANG_FORMAT_BINARY, e.g.:
#   make regen-c-api CLANG_FORMAT_BINARY=clang-format-21
.PHONY: regen-c-api
regen-c-api:
	@if [ -z "$(CLANG_CXX)" ] || ! command -v "$(CLANG_CXX)" >/dev/null 2>&1; then \
	  echo "ERROR: no clang++ found (CLANG_CXX unset); build on a host with clang++ or the fbcode toolchain to regenerate the C API" >&2; exit 1; \
	fi
	@cf_arg=""; \
	if [ -n "$(CLANG_FORMAT_BINARY)" ]; then cf_arg="--clang-format $(CLANG_FORMAT_BINARY)"; fi; \
	$(PYTHON) tools/c_api_gen/regen_all.py --clang "$(CLANG_CXX)" $$cf_arg

# Quick local validation for C API generation plus the focused C API test.
# This verifies the checked-in generated fragments as well as the inlined
# include/rocksdb/c.h and db/c.cc outputs, then runs c_test in an isolated
# TEST_TMPDIR to avoid stale-state failures.
check-c-api-c_test:
	$(PYTHON) tools/c_api_gen/verify_generated_up_to_date.py --clang "$(CLANG_CXX)"
	$(MAKE) c_test
	@tmpdir=$$(mktemp -d); \
	  trap 'rm -rf "$$tmpdir"' EXIT; \
	  echo "===== Running c_test with TEST_TMPDIR=$$tmpdir"; \
	  if command -v timeout >/dev/null 2>&1; then \
	    TEST_TMPDIR="$$tmpdir" timeout 60 ./c_test; \
	  elif command -v gtimeout >/dev/null 2>&1; then \
	    TEST_TMPDIR="$$tmpdir" gtimeout 60 ./c_test; \
	  else \
	    TEST_TMPDIR="$$tmpdir" ./c_test; \
	  fi

# TODO add ldb_tests
check_some: $(ROCKSDBTESTS_SUBSET)
	for t in $(ROCKSDBTESTS_SUBSET); do echo "===== Running $$t (`date`)"; ./$$t || exit 1; done

.PHONY: ldb_tests
ldb_tests: ldb
	$(PYTHON) tools/ldb_test.py

.PHONY: db_crashtest_tests
db_crashtest_tests:
	$(PYTHON) tools/db_crashtest_test.py

.PHONY: check-all-python
check-all-python:
	$(PYTHON) tools/check_all_python.py

.PHONY: rocksdb_dump_test
rocksdb_dump_test: rocksdb_dump rocksdb_undump
	sh tools/rocksdb_dump_test.sh

include crash_test.mk

asan_check: clean
	COMPILE_WITH_ASAN=1 $(MAKE) check -j32
	$(MAKE) clean

asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test
	$(MAKE) clean

whitebox_asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) whitebox_crash_test
	$(MAKE) clean

blackbox_asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) blackbox_crash_test
	$(MAKE) clean

asan_crash_test_with_atomic_flush: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

asan_crash_test_with_txn: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

asan_crash_test_with_best_efforts_recovery: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_best_efforts_recovery
	$(MAKE) clean

ubsan_check: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) check -j32
	$(MAKE) clean

ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test
	$(MAKE) clean

whitebox_ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) whitebox_crash_test
	$(MAKE) clean

blackbox_ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) blackbox_crash_test
	$(MAKE) clean

ubsan_crash_test_with_atomic_flush: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

ubsan_crash_test_with_txn: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

ubsan_crash_test_with_best_efforts_recovery: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_best_efforts_recovery
	$(MAKE) clean

full_valgrind_test:
	ROCKSDB_FULL_VALGRIND_RUN=1 DISABLE_JEMALLOC=1 PORTABLE=1 $(MAKE) valgrind_check

full_valgrind_test_some:
	ROCKSDB_FULL_VALGRIND_RUN=1 DISABLE_JEMALLOC=1 PORTABLE=1 $(MAKE) valgrind_check_some

valgrind_test:
	ROCKSDB_VALGRIND_RUN=1 DISABLE_JEMALLOC=1 PORTABLE=1 $(MAKE) valgrind_check

valgrind_test_some:
	ROCKSDB_VALGRIND_RUN=1 DISABLE_JEMALLOC=1 PORTABLE=1 $(MAKE) valgrind_check_some

valgrind_check: $(TESTS)
	$(MAKE) DRIVER="$(VALGRIND_VER) $(VALGRIND_OPTS)" gen_parallel_tests
	$(AM_V_GEN)if test "$(J)" != 1                                  \
	    && (build_tools/gnu_parallel --gnu --help 2>/dev/null) |    \
	        grep -q 'GNU Parallel';                                 \
	then                                                            \
	  $(MAKE)                                                       \
	  DRIVER="$(VALGRIND_VER) $(VALGRIND_OPTS)" valgrind_check_0;   \
	else                                                            \
		for t in $(filter-out %skiplist_test options_settable_test,$(TESTS)); do \
			$(VALGRIND_VER) $(VALGRIND_OPTS) ./$$t; \
			ret_code=$$?; \
			if [ $$ret_code -ne 0 ]; then \
				exit $$ret_code; \
			fi; \
		done; \
	fi

valgrind_check_some: $(ROCKSDBTESTS_SUBSET)
	for t in $(ROCKSDBTESTS_SUBSET); do \
		$(VALGRIND_VER) $(VALGRIND_OPTS) ./$$t; \
		ret_code=$$?; \
		if [ $$ret_code -ne 0 ]; then \
			exit $$ret_code; \
		fi; \
	done

test_names = \
  ./db_test --gtest_list_tests						\
    | perl -n								\
      -e 's/ *\#.*//;'							\
      -e '/^(\s*)(\S+)/; !$$1 and do {$$p=$$2; break};'			\
      -e 'print qq! $$p$$2!'

analyze: clean
	USE_CLANG=1 $(MAKE) analyze_incremental

analyze_incremental:
	$(CLANG_SCAN_BUILD) --use-analyzer=$(CLANG_ANALYZER) \
		--use-c++=$(CXX) --use-cc=$(CC) --status-bugs \
		-o $(CURDIR)/scan_build_report \
		$(MAKE) SKIP_LINK=1 dbg

CLEAN_FILES += unity.cc
unity.cc: Makefile util/build_version.cc.in
	rm -f $@ $@-t
	$(AM_V_at)$(gen_build_version) > util/build_version.cc
	for source_file in $(LIB_SOURCES); do \
		echo "#include \"$$source_file\"" >> $@-t; \
	done
	chmod a=r $@-t
	mv $@-t $@

unity.a: $(OBJ_DIR)/unity.o
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(OBJ_DIR)/unity.o


# try compiling db_test with unity
unity_test: $(OBJ_DIR)/db/db_basic_test.o $(OBJ_DIR)/db/db_test_util.o $(TEST_OBJECTS) $(TOOL_OBJECTS) unity.a
	$(AM_LINK)
	./unity_test

rocksdb.h rocksdb.cc: build_tools/amalgamate.py Makefile $(LIB_SOURCES) unity.cc
	build_tools/amalgamate.py -I. -i./include unity.cc -x include/rocksdb/c.h -H rocksdb.h -o rocksdb.cc

clean: clean-ext-libraries-all clean-rocks clean-rocksjava
# Removed here rather than in clean-rocks (via CLEAN_FILES) so the build-parameter
# auto-clean, which runs clean-rocks, doesn't delete the make_config.mk already
# included by the in-progress make.
	@rm -f make_config.mk

clean-not-downloaded: clean-ext-libraries-bin clean-rocks clean-not-downloaded-rocksjava
	@rm -f make_config.mk

clean-rocks:
# Not practical to exactly match all versions/variants in naming (e.g. debug or not)
	@echo Removing link targets
	@rm -f ${LIBNAME}*.so* ${LIBNAME}*.a $(BENCHMARKS) $(TOOLS) $(TESTS) $(PARALLEL_TEST) $(MICROBENCHS)
	@echo Finding and cleaning other files
	@rm -rf $(CLEAN_FILES) ios-x86 ios-arm scan_build_report
	@if $(FIND) Makefile -regextype awk &> /dev/null; then \
		$(FIND) . -regextype awk \
			-regex '.*/([.].*|third-party)' -prune -o \
			-type f -regex '.*[.]([oda]|gcda|gcno)' -exec rm -f {} \; ; \
	else \
		$(FIND) . -name "*.[oda]" -exec rm -f {} \; ; \
	fi
# Remove the build signature(s) LAST. Done after the object files are gone so
# that a Ctrl+C partway through clean leaves the old signature in place: it
# still matches the leftover objects, so a later build with different
# parameters is still detected (signature usefulness is preserved).
	@rm -f $(BUILD_SIG_FILE) jl/.build_signature jls/.build_signature

clean-rocksjava: clean-rocks
	rm -rf jl jls
	cd java && $(MAKE) clean

clean-not-downloaded-rocksjava:
	cd java && $(MAKE) clean-not-downloaded

clean-ext-libraries-all:
	rm -rf bzip2* snappy* zlib* lz4* zstd*

clean-ext-libraries-bin:
	$(FIND) . -maxdepth 1 -type d \( -name bzip2\* -or -name snappy\* -or -name zlib\* -or -name lz4\* -or -name zstd\* \) -prune -exec rm -rf {} \;

tags:
	ctags -R .
	cscope -b `$(FIND) . -name '*.cc'` `$(FIND) . -name '*.h'` `$(FIND) . -name '*.c'`
	ctags -e -R -o etags *

tags0:
	ctags -R .
	cscope -b `$(FIND) . -name '*.cc' -and ! -name '*_test.cc'` \
		  `$(FIND) . -name '*.c' -and ! -name '*_test.c'` \
		  `$(FIND) . -name '*.h' -and ! -name '*_test.h'`
	ctags -e -R -o etags *

format:
	build_tools/format-diff.sh

# Non-interactive format (auto-apply without prompts, for CI/automation/Claude Code)
format-auto:
	build_tools/format-diff.sh -y

check-format:
	build_tools/format-diff.sh -c


# Crude alternative to setup-hooks: copies hooks into .git/hooks/ instead of
# using core.hooksPath. The copies won't track changes to githooks/.
install-hooks:
	@echo "Installing git hooks from githooks/..."
	@if [ -d githooks ]; then \
		for hook in githooks/*; do \
			hook_name=$$(basename "$$hook"); \
			cp "$$hook" .git/hooks/"$$hook_name"; \
			chmod +x .git/hooks/"$$hook_name"; \
			echo "  Installed $$hook_name"; \
		done; \
		echo "Done. Hooks installed to .git/hooks/"; \
	else \
		echo "Error: githooks/ directory not found"; \
		exit 1; \
	fi

# Reverse of install-hooks (not needed if using setup-hooks / core.hooksPath).
uninstall-hooks:
	@echo "Removing installed git hooks..."
	@for hook in githooks/*; do \
		hook_name=$$(basename "$$hook"); \
		rm -f .git/hooks/"$$hook_name"; \
		echo "  Removed $$hook_name"; \
	done
	@echo "Done."

check-buck-targets:
	buckifier/check_buck_targets.sh

check-sources:
	build_tools/check-sources.sh

check-workflow-yaml:
	build_tools/check-workflow-yaml.sh

# Run clang-tidy on locally changed files, filtered to changed lines only.
# Requires compile_commands.json (generate with cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON).
# Override CLANG_TIDY_BINARY and CLANG_TIDY_JOBS as needed:
#   make clang-tidy CLANG_TIDY_BINARY=/usr/bin/clang-tidy CLANG_TIDY_JOBS=8
CLANG_TIDY_BINARY ?= /opt/homebrew/opt/llvm/bin/clang-tidy
CLANG_TIDY_JOBS ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
clang-tidy:
	python3 tools/run_clang_tidy.py --clang-tidy-binary $(CLANG_TIDY_BINARY) -j $(CLANG_TIDY_JOBS)

package:
	bash build_tools/make_package.sh $(SHARED_MAJOR).$(SHARED_MINOR)

# ---------------------------------------------------------------------------
# 	Unit tests and tools
# ---------------------------------------------------------------------------
$(STATIC_LIBRARY): $(LIB_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIB_OBJECTS)

$(STATIC_TEST_LIBRARY): $(TEST_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED_TEST_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(STATIC_TOOLS_LIBRARY): $(TOOL_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED_TOOLS_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(STATIC_STRESS_LIBRARY): $(ANALYZE_OBJECTS) $(STRESS_OBJECTS) $(TESTUTIL)
	$(AM_V_AR)rm -f $@ $(SHARED_STRESS_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(SHARED_TEST_LIBRARY): $(TEST_OBJECTS) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_TEST_LIBRARY)
	$(AM_SHARE)

$(SHARED_TOOLS_LIBRARY): $(TOOL_OBJECTS) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_TOOLS_LIBRARY)
	$(AM_SHARE)

$(SHARED_STRESS_LIBRARY): $(ANALYZE_OBJECTS) $(STRESS_OBJECTS) $(TESTUTIL) $(SHARED_TOOLS_LIBRARY) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_STRESS_LIBRARY)
	$(AM_SHARE)

librocksdb_env_basic_test.a: $(OBJ_DIR)/env/env_basic_test.o $(LIB_OBJECTS) $(TESTHARNESS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

db_bench: $(OBJ_DIR)/tools/db_bench.o $(BENCH_OBJECTS) $(TESTUTIL) $(LIBRARY)
	$(AM_LINK)

trace_analyzer: $(OBJ_DIR)/tools/trace_analyzer.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_trace_analyzer: $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer_tool.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cache_bench: $(OBJ_DIR)/cache/cache_bench.o $(CACHE_BENCH_OBJECTS) $(LIBRARY)
	$(AM_LINK)

point_lock_bench: $(OBJ_DIR)/utilities/transactions/lock/point/point_lock_bench.o $(POINT_LOCK_BENCH_OBJECTS) $(LIBRARY)
	$(AM_LINK)

persistent_cache_bench: $(OBJ_DIR)/utilities/persistent_cache/persistent_cache_bench.o $(LIBRARY)
	$(AM_LINK)

memtablerep_bench: $(OBJ_DIR)/memtable/memtablerep_bench.o $(LIBRARY)
	$(AM_LINK)

filter_bench: $(OBJ_DIR)/util/filter_bench.o $(LIBRARY)
	$(AM_LINK)

db_stress: $(OBJ_DIR)/db_stress_tool/db_stress.o $(STRESS_LIBRARY) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_stress_compression_manager: $(OBJ_DIR)/db_stress_tool/db_stress_compression_manager.o $(LIBRARY)
	$(AM_LINK)

write_stress: $(OBJ_DIR)/tools/write_stress.o $(LIBRARY)
	$(AM_LINK)

db_sanity_test: $(OBJ_DIR)/tools/db_sanity_test.o $(LIBRARY)
	$(AM_LINK)

db_repl_stress: $(OBJ_DIR)/tools/db_repl_stress.o $(LIBRARY)
	$(AM_LINK)

define MakeTestRule
$(notdir $(1:%.cc=%)): $(1:%.cc=$$(OBJ_DIR)/%.o) $$(TEST_LIBRARY) $$(LIBRARY)
	$$(AM_LINK)
endef

# For each PLUGIN test, create a rule to generate the test executable
$(foreach test, $(ROCKSDB_PLUGIN_TESTS), $(eval $(call MakeTestRule, $(test))))

arena_test: $(OBJ_DIR)/memory/arena_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memory_allocator_test: memory/memory_allocator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

autovector_test: $(OBJ_DIR)/util/autovector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

column_family_test: $(OBJ_DIR)/db/column_family_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_properties_collector_test: $(OBJ_DIR)/db/table_properties_collector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

bloom_test: $(OBJ_DIR)/util/bloom_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

dynamic_bloom_test: $(OBJ_DIR)/util/dynamic_bloom_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

c_test: $(OBJ_DIR)/db/c_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cache_test: $(OBJ_DIR)/cache/cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

coding_test: $(OBJ_DIR)/util/coding_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

hash_test: $(OBJ_DIR)/util/hash_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

random_test: $(OBJ_DIR)/util/random_test.o  $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ribbon_test: $(OBJ_DIR)/util/ribbon_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

option_change_migration_test: $(OBJ_DIR)/utilities/option_change_migration/option_change_migration_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

agg_merge_test: $(OBJ_DIR)/utilities/agg_merge/agg_merge_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

stringappend_test: $(OBJ_DIR)/utilities/merge_operators/string_append/stringappend_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_format_test: $(OBJ_DIR)/utilities/cassandra/cassandra_format_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_functional_test: $(OBJ_DIR)/utilities/cassandra/cassandra_functional_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_row_merge_test: $(OBJ_DIR)/utilities/cassandra/cassandra_row_merge_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_serialize_test: $(OBJ_DIR)/utilities/cassandra/cassandra_serialize_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

hash_table_test: $(OBJ_DIR)/utilities/persistent_cache/hash_table_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

histogram_test: $(OBJ_DIR)/monitoring/histogram_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

thread_local_test: $(OBJ_DIR)/util/thread_local_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

work_queue_test: $(OBJ_DIR)/util/work_queue_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

udt_util_test: $(OBJ_DIR)/util/udt_util_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

corruption_test: $(OBJ_DIR)/db/corruption_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

crc32c_test: $(OBJ_DIR)/util/crc32c_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

slice_test: $(OBJ_DIR)/util/slice_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

slice_transform_test: $(OBJ_DIR)/util/slice_transform_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_basic_test: $(OBJ_DIR)/db/db_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_basic_test: $(OBJ_DIR)/db/blob/db_blob_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_direct_write_test: $(OBJ_DIR)/db/blob/db_blob_direct_write_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_compaction_test: $(OBJ_DIR)/db/blob/db_blob_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_readonly_with_timestamp_test: $(OBJ_DIR)/db/db_readonly_with_timestamp_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_wide_basic_test: $(OBJ_DIR)/db/wide/db_wide_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_lazy_entity_test: $(OBJ_DIR)/db/wide/db_lazy_entity_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_wide_blob_direct_write_test: $(OBJ_DIR)/db/wide/db_wide_blob_direct_write_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_with_timestamp_basic_test: $(OBJ_DIR)/db/db_with_timestamp_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_with_timestamp_compaction_test: db/db_with_timestamp_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_encryption_test: $(OBJ_DIR)/db/db_encryption_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_open_with_config_test: $(OBJ_DIR)/db/db_open_with_config_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_test: $(OBJ_DIR)/db/db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_etc2_test: $(OBJ_DIR)/db/db_etc2_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_etc3_test: $(OBJ_DIR)/db/db_etc3_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compression_test: $(OBJ_DIR)/util/compression_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_logical_block_size_cache_test: $(OBJ_DIR)/db/db_logical_block_size_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_index_test: $(OBJ_DIR)/db/blob/db_blob_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_block_cache_test: $(OBJ_DIR)/db/db_block_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_bloom_filter_test: $(OBJ_DIR)/db/db_bloom_filter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_log_iter_test: $(OBJ_DIR)/db/db_log_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_compaction_filter_test: $(OBJ_DIR)/db/db_compaction_filter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_compaction_test: $(OBJ_DIR)/db/db_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_compaction_abort_test: $(OBJ_DIR)/db/db_compaction_abort_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_clip_test: $(OBJ_DIR)/db/db_clip_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_dynamic_level_test: $(OBJ_DIR)/db/db_dynamic_level_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_flush_test: $(OBJ_DIR)/db/db_flush_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_inplace_update_test: $(OBJ_DIR)/db/db_inplace_update_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iterator_test: $(OBJ_DIR)/db/db_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_kv_checksum_test: $(OBJ_DIR)/db/db_kv_checksum_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_memtable_test: $(OBJ_DIR)/db/db_memtable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_merge_operator_test: $(OBJ_DIR)/db/db_merge_operator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_merge_operand_test: $(OBJ_DIR)/db/db_merge_operand_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_options_test: $(OBJ_DIR)/db/db_options_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_range_del_test: $(OBJ_DIR)/db/db_range_del_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_rate_limiter_test: $(OBJ_DIR)/db/db_rate_limiter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_sst_test: $(OBJ_DIR)/db/db_sst_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_statistics_test: $(OBJ_DIR)/db/db_statistics_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_write_test: $(OBJ_DIR)/db/db_write_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

error_handler_fs_test: $(OBJ_DIR)/db/error_handler_fs_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

external_sst_file_basic_test: $(OBJ_DIR)/db/external_sst_file_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

external_sst_file_test: $(OBJ_DIR)/db/external_sst_file_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

import_column_family_test: $(OBJ_DIR)/db/import_column_family_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_tailing_iter_test: $(OBJ_DIR)/db/db_tailing_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iter_test: $(OBJ_DIR)/db/db_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iter_stress_test: $(OBJ_DIR)/db/db_iter_stress_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_universal_compaction_test: $(OBJ_DIR)/db/db_universal_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_wal_test: $(OBJ_DIR)/db/db_wal_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_io_failure_test: $(OBJ_DIR)/db/db_io_failure_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_properties_test: $(OBJ_DIR)/db/db_properties_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_table_properties_test: $(OBJ_DIR)/db/db_table_properties_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

log_write_bench: $(OBJ_DIR)/util/log_write_bench.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK) $(PROFILING_FLAGS)

seqno_time_test: $(OBJ_DIR)/db/seqno_time_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

plain_table_db_test: $(OBJ_DIR)/db/plain_table_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

comparator_db_test: $(OBJ_DIR)/db/comparator_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_reader_bench: $(OBJ_DIR)/table/table_reader_bench.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK) $(PROFILING_FLAGS)

perf_context_test: $(OBJ_DIR)/db/perf_context_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

prefix_test: $(OBJ_DIR)/db/prefix_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

backup_engine_test: $(OBJ_DIR)/utilities/backup/backup_engine_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

checkpoint_test: $(OBJ_DIR)/utilities/checkpoint/checkpoint_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_split_merge_test: $(OBJ_DIR)/utilities/db_split_merge_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

copy_engine_test: $(OBJ_DIR)/utilities/copy_engine/copy_engine_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sorted_run_builder_test: $(OBJ_DIR)/utilities/sorted_run_builder/sorted_run_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cache_simulator_test: $(OBJ_DIR)/utilities/simulator_cache/cache_simulator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sim_cache_test: $(OBJ_DIR)/utilities/simulator_cache/sim_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_mirror_test: $(OBJ_DIR)/utilities/env_mirror_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_timed_test: $(OBJ_DIR)/utilities/env_timed_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

object_registry_test: $(OBJ_DIR)/utilities/object_registry_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ttl_test: $(OBJ_DIR)/utilities/ttl/ttl_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

trie_index_db_test: $(OBJ_DIR)/utilities/trie_index/trie_index_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

trie_index_test: $(OBJ_DIR)/utilities/trie_index/trie_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

types_util_test: $(OBJ_DIR)/utilities/types_util_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_batch_with_index_test: $(OBJ_DIR)/utilities/write_batch_with_index/write_batch_with_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

flush_job_test: $(OBJ_DIR)/db/flush_job_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_iterator_test: $(OBJ_DIR)/db/compaction/compaction_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_job_test: $(OBJ_DIR)/db/compaction/compaction_job_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_job_stats_test: $(OBJ_DIR)/db/compaction/compaction_job_stats_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_service_test: $(OBJ_DIR)/db/compaction/compaction_service_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compact_for_tiering_collector_test: $(OBJ_DIR)/utilities/table_properties_collectors/compact_for_tiering_collector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compact_on_deletion_collector_test: $(OBJ_DIR)/utilities/table_properties_collectors/compact_on_deletion_collector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wal_manager_test: $(OBJ_DIR)/db/wal_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wal_edit_test: $(OBJ_DIR)/db/wal_edit_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

dbformat_test: $(OBJ_DIR)/db/dbformat_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

multi_cf_iterator_test: $(OBJ_DIR)/db/multi_cf_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_basic_test: $(OBJ_DIR)/env/env_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_test: $(OBJ_DIR)/env/env_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_posix_test: $(OBJ_DIR)/env/io_posix_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

fault_injection_test: $(OBJ_DIR)/db/fault_injection_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

fault_injection_fs_test: $(OBJ_DIR)/utilities/fault_injection_fs_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

rate_limiter_test: $(OBJ_DIR)/util/rate_limiter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

delete_scheduler_test: $(OBJ_DIR)/file/delete_scheduler_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

filename_test: $(OBJ_DIR)/db/filename_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

random_access_file_reader_test: $(OBJ_DIR)/file/random_access_file_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

file_reader_writer_test: $(OBJ_DIR)/util/file_reader_writer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_based_table_reader_test: table/block_based/block_based_table_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

full_filter_block_test: $(OBJ_DIR)/table/block_based/full_filter_block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

partitioned_filter_block_test: $(OBJ_DIR)/table/block_based/partitioned_filter_block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

log_test: $(OBJ_DIR)/db/log_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cleanable_test: $(OBJ_DIR)/table/cleanable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_test: $(OBJ_DIR)/table/table_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_fetcher_test: table/block_fetcher_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_test: $(OBJ_DIR)/table/block_based/block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

builtin_index_factory_test: $(OBJ_DIR)/table/block_based/builtin_index_factory_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

data_block_hash_index_test: $(OBJ_DIR)/table/block_based/data_block_hash_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

inlineskiplist_test: $(OBJ_DIR)/memtable/inlineskiplist_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

skiplist_test: $(OBJ_DIR)/memtable/skiplist_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_buffer_manager_test: $(OBJ_DIR)/memtable/write_buffer_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_edit_test: $(OBJ_DIR)/db/version_edit_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_set_test: $(OBJ_DIR)/db/version_set_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_picker_test: $(OBJ_DIR)/db/compaction/compaction_picker_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_builder_test: $(OBJ_DIR)/db/version_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

file_indexer_test: $(OBJ_DIR)/db/file_indexer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

reduce_levels_test: $(OBJ_DIR)/tools/reduce_levels_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_batch_test: $(OBJ_DIR)/db/write_batch_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_controller_test: $(OBJ_DIR)/db/write_controller_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merge_helper_test: $(OBJ_DIR)/db/merge_helper_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memory_test: $(OBJ_DIR)/utilities/memory/memory_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merge_test: $(OBJ_DIR)/db/merge_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merger_test: $(OBJ_DIR)/table/merger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

util_merge_operators_test: $(OBJ_DIR)/utilities/util_merge_operators_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_file_test: $(OBJ_DIR)/db/options_file_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

deletefile_test: $(OBJ_DIR)/db/deletefile_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

obsolete_files_test: $(OBJ_DIR)/db/obsolete_files_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

rocksdb_dump: $(OBJ_DIR)/tools/dump/rocksdb_dump.o $(LIBRARY)
	$(AM_LINK)

rocksdb_undump: $(OBJ_DIR)/tools/dump/rocksdb_undump.o $(LIBRARY)
	$(AM_LINK)

cuckoo_table_builder_test: $(OBJ_DIR)/table/cuckoo/cuckoo_table_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cuckoo_table_reader_test: $(OBJ_DIR)/table/cuckoo/cuckoo_table_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cuckoo_table_db_test: $(OBJ_DIR)/db/cuckoo_table_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

listener_test: $(OBJ_DIR)/db/listener_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

string_util_test: $(OBJ_DIR)/util/string_util_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

thread_list_test: $(OBJ_DIR)/util/thread_list_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compact_files_test: $(OBJ_DIR)/db/compact_files_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

configurable_test: options/configurable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

customizable_test: options/customizable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_test: $(OBJ_DIR)/options/options_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_settable_test: $(OBJ_DIR)/options/options_settable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_util_test: $(OBJ_DIR)/utilities/options/options_util_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_bench_tool_test: $(OBJ_DIR)/tools/db_bench_tool_test.o $(BENCH_OBJECTS) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

trace_analyzer_test: $(OBJ_DIR)/tools/trace_analyzer_test.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

event_logger_test: $(OBJ_DIR)/logging/event_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

timer_queue_test: $(OBJ_DIR)/util/timer_queue_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_dump_test: $(OBJ_DIR)/tools/sst_dump_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

optimistic_transaction_test: $(OBJ_DIR)/utilities/transactions/optimistic_transaction_test.o  $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

mock_env_test : $(OBJ_DIR)/env/mock_env_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

manual_compaction_test: $(OBJ_DIR)/db/manual_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

filelock_test: $(OBJ_DIR)/util/filelock_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

auto_roll_logger_test: $(OBJ_DIR)/logging/auto_roll_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_logger_test: $(OBJ_DIR)/logging/env_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memtable_list_test: $(OBJ_DIR)/db/memtable_list_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_callback_test: $(OBJ_DIR)/db/write_callback_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

heap_test: $(OBJ_DIR)/util/heap_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

point_lock_manager_test: utilities/transactions/lock/point/point_lock_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

point_lock_manager_stress_test: utilities/transactions/lock/point/point_lock_manager_stress_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

transaction_test: $(OBJ_DIR)/utilities/transactions/transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_committed_transaction_ts_test: $(OBJ_DIR)/utilities/transactions/write_committed_transaction_ts_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_prepared_transaction_test: $(OBJ_DIR)/utilities/transactions/write_prepared_transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_prepared_transaction_seqno_test: $(OBJ_DIR)/utilities/transactions/write_prepared_transaction_seqno_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_unprepared_transaction_test: $(OBJ_DIR)/utilities/transactions/write_unprepared_transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

timestamped_snapshot_test: $(OBJ_DIR)/utilities/transactions/timestamped_snapshot_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

tiered_compaction_test: $(OBJ_DIR)/db/compaction/tiered_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_dump: $(OBJ_DIR)/tools/sst_dump.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_dump: $(OBJ_DIR)/tools/blob_dump.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

repair_test: $(OBJ_DIR)/db/repair_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ldb_cmd_test: $(OBJ_DIR)/tools/ldb_cmd_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ldb: $(OBJ_DIR)/tools/ldb.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_split_merge: $(OBJ_DIR)/tools/db_split_merge.o $(LIBRARY)
	$(AM_LINK)

iostats_context_test: $(OBJ_DIR)/monitoring/iostats_context_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

persistent_cache_test: $(OBJ_DIR)/utilities/persistent_cache/persistent_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

statistics_test: $(OBJ_DIR)/monitoring/statistics_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

stats_history_test: $(OBJ_DIR)/monitoring/stats_history_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compressed_secondary_cache_test: $(OBJ_DIR)/cache/compressed_secondary_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

lru_cache_test: $(OBJ_DIR)/cache/lru_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

tiered_secondary_cache_test: $(OBJ_DIR)/cache/tiered_secondary_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_del_aggregator_test: $(OBJ_DIR)/db/range_del_aggregator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_del_aggregator_bench: $(OBJ_DIR)/db/range_del_aggregator_bench.o $(LIBRARY)
	$(AM_LINK)

blob_db_test: $(OBJ_DIR)/utilities/blob_db/blob_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

repeatable_thread_test: $(OBJ_DIR)/util/repeatable_thread_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_locking_test: utilities/transactions/lock/range/range_locking_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_tombstone_fragmenter_test: $(OBJ_DIR)/db/range_tombstone_fragmenter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_file_reader_test: $(OBJ_DIR)/table/sst_file_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_secondary_test: $(OBJ_DIR)/db/db_secondary_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_follower_test: $(OBJ_DIR)/db/db_follower_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_tracer_test: $(OBJ_DIR)/trace_replay/block_cache_tracer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_trace_analyzer_test: $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer_test.o $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

defer_test: $(OBJ_DIR)/util/defer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_counting_iterator_test: $(OBJ_DIR)/db/blob/blob_counting_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_addition_test: $(OBJ_DIR)/db/blob/blob_file_addition_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_builder_test: $(OBJ_DIR)/db/blob/blob_file_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_cache_test: $(OBJ_DIR)/db/blob/blob_file_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_garbage_test: $(OBJ_DIR)/db/blob/blob_file_garbage_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_reader_test: $(OBJ_DIR)/db/blob/blob_file_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_source_test: $(OBJ_DIR)/db/blob/blob_source_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_garbage_meter_test: $(OBJ_DIR)/db/blob/blob_garbage_meter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_dispatcher_test: $(OBJ_DIR)/util/io_dispatcher_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

timer_test: $(OBJ_DIR)/util/timer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

periodic_task_scheduler_test: $(OBJ_DIR)/db/periodic_task_scheduler_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

testutil_test: $(OBJ_DIR)/test_util/testutil_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_tracer_test: $(OBJ_DIR)/trace_replay/io_tracer_test.o $(OBJ_DIR)/trace_replay/io_tracer.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

prefetch_test: $(OBJ_DIR)/file/prefetch_test.o  $(OBJ_DIR)/tools/io_tracer_parser_tool.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_tracer_parser_test: $(OBJ_DIR)/tools/io_tracer_parser_test.o $(OBJ_DIR)/tools/io_tracer_parser_tool.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_tracer_parser: $(OBJ_DIR)/tools/io_tracer_parser.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_corruption_test: $(OBJ_DIR)/db/blob/db_blob_corruption_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_write_buffer_manager_test: $(OBJ_DIR)/db/db_write_buffer_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

clipping_iterator_test: $(OBJ_DIR)/db/compaction/clipping_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ribbon_bench: $(OBJ_DIR)/microbench/ribbon_bench.o $(LIBRARY)
	$(AM_LINK)

db_basic_bench: $(OBJ_DIR)/microbench/db_basic_bench.o $(LIBRARY)
	$(AM_LINK)

cache_reservation_manager_test: $(OBJ_DIR)/cache/cache_reservation_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wide_column_serialization_test: $(OBJ_DIR)/db/wide/wide_column_serialization_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wide_columns_helper_test: $(OBJ_DIR)/db/wide/wide_columns_helper_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

interval_test: $(OBJ_DIR)/util/interval_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

#-------------------------------------------------
# make install related stuff
PREFIX ?= /usr/local
LIBDIR ?= $(PREFIX)/lib
INSTALL_LIBDIR = $(DESTDIR)$(LIBDIR)

uninstall:
	rm -rf $(DESTDIR)$(PREFIX)/include/rocksdb \
	  $(INSTALL_LIBDIR)/$(LIBRARY) \
	  $(INSTALL_LIBDIR)/$(SHARED4) \
	  $(INSTALL_LIBDIR)/$(SHARED3) \
	  $(INSTALL_LIBDIR)/$(SHARED2) \
	  $(INSTALL_LIBDIR)/$(SHARED1) \
	  $(INSTALL_LIBDIR)/pkgconfig/rocksdb.pc

install-headers: gen-pc
	install -d $(INSTALL_LIBDIR)
	install -d $(INSTALL_LIBDIR)/pkgconfig
	for header_dir in `$(FIND) "include/rocksdb" -type d`; do \
		install -d $(DESTDIR)/$(PREFIX)/$$header_dir; \
	done
	for header in `$(FIND) "include/rocksdb" -type f -name *.h`; do \
		install -C -m 644 $$header $(DESTDIR)/$(PREFIX)/$$header; \
	done
	for header in $(ROCKSDB_PLUGIN_HEADERS); do \
		install -d $(DESTDIR)/$(PREFIX)/include/rocksdb/`dirname $$header`; \
		install -C -m 644 $$header $(DESTDIR)/$(PREFIX)/include/rocksdb/$$header; \
	done
	install -C -m 644 rocksdb.pc $(INSTALL_LIBDIR)/pkgconfig/rocksdb.pc

install-static: install-headers $(LIBRARY)
	install -d $(INSTALL_LIBDIR)
	install -C -m 755 $(LIBRARY) $(INSTALL_LIBDIR)

install-shared: install-headers $(SHARED4)
	install -d $(INSTALL_LIBDIR)
	install -C -m 755 $(SHARED4) $(INSTALL_LIBDIR)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED3)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED2)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED1)

# install static by default + install shared if it exists
install: install-static
	[ -e $(SHARED4) ] && $(MAKE) install-shared || :

# Generate the pkg-config file
gen-pc:
	-echo 'prefix=$(PREFIX)' > rocksdb.pc
	-echo 'exec_prefix=$${prefix}' >> rocksdb.pc
	-echo 'includedir=$${prefix}/include' >> rocksdb.pc
	-echo 'libdir=$(LIBDIR)' >> rocksdb.pc
	-echo '' >> rocksdb.pc
	-echo 'Name: rocksdb' >> rocksdb.pc
	-echo 'Description: An embeddable persistent key-value store for fast storage' >> rocksdb.pc
	-echo Version: $(shell ./build_tools/version.sh full) >> rocksdb.pc
	-echo 'Libs: -L$${libdir} $(EXEC_LDFLAGS) -lrocksdb' >> rocksdb.pc
	-echo 'Libs.private: $(PLATFORM_LDFLAGS)' >> rocksdb.pc
	-echo 'Cflags: -I$${includedir} $(PLATFORM_CXXFLAGS)' >> rocksdb.pc
	-echo 'Requires: $(subst ",,$(ROCKSDB_PLUGIN_PKGCONFIG_REQUIRES))' >> rocksdb.pc

#-------------------------------------------------


# ---------------------------------------------------------------------------
# Jni stuff
# ---------------------------------------------------------------------------
JAVA_INCLUDE = -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux
ifeq ($(PLATFORM), OS_OPENBSD)
	ifneq (,$(filter amd64 ppc64 ppc64le s390x arm64 aarch64 riscv64 sparc64 loongarch64, $(MACHINE)))
		ARCH := 64
	else
		ARCH := 32
	endif
else
	ARCH := $(shell getconf LONG_BIT)
endif

ifeq ($(shell ldd /usr/bin/env 2>/dev/null | grep -q musl; echo $$?),0)
        JNI_LIBC = musl
# GNU LibC (or glibc) is so pervasive we can assume it is the default
# else
#        JNI_LIBC = glibc
endif

ifneq ($(origin JNI_LIBC), undefined)
  JNI_LIBC_POSTFIX = -$(JNI_LIBC)
endif

ifeq (,$(ROCKSDBJNILIB))
ifneq (,$(filter ppc% s390x arm64 aarch64 riscv64 sparc64 loongarch64, $(MACHINE)))
	ROCKSDBJNILIB = librocksdbjni-linux-$(MACHINE)$(JNI_LIBC_POSTFIX).so
else
	ROCKSDBJNILIB = librocksdbjni-linux$(ARCH)$(JNI_LIBC_POSTFIX).so
endif
endif
ROCKSDB_JAVA_VERSION ?= $(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux$(ARCH)$(JNI_LIBC_POSTFIX).jar
ROCKSDB_JAR_ALL = rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar
ROCKSDB_JAVADOCS_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-javadoc.jar
ROCKSDB_SOURCES_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-sources.jar
SHA256_CMD = sha256sum

# The *_DOWNLOAD_PREFIX defaults below point at the upstream projects; CI
# overrides them to a mirror on the "third-party-deps" GitHub release (see
# .github/actions/pre-steps/action.yml). When bumping a version, see
# instructions in CI config comments for how to update that release.
ZLIB_VER ?= 1.3.1
ZLIB_SHA256 ?= 9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23
ZLIB_DOWNLOAD_PREFIX ?= http://zlib.net/zlib-
BZIP2_VER ?= 1.0.8
BZIP2_SHA256 ?= ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269
BZIP2_DOWNLOAD_PREFIX ?= http://sourceware.org/pub/bzip2/bzip2-
SNAPPY_VER ?= 1.2.2
SNAPPY_SHA256 ?= 90f74bc1fbf78a6c56b3c4a082a05103b3a56bb17bca1a27e052ea11723292dc
SNAPPY_DOWNLOAD_PREFIX ?= https://github.com/google/snappy/archive/
LZ4_VER ?= 1.10.0
LZ4_SHA256 ?= 537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b
LZ4_DOWNLOAD_PREFIX ?= https://github.com/lz4/lz4/archive/v
ZSTD_VER ?= 1.5.7
ZSTD_SHA256 ?= 37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3
ZSTD_DOWNLOAD_PREFIX ?= https://github.com/facebook/zstd/archive/v
# curl with the common options used to fetch the pinned third-party sources above
CURL_DOWNLOAD = curl --fail --location --retry 3 --retry-all-errors

ifeq ($(PLATFORM), OS_MACOSX)
ifeq (,$(findstring librocksdbjni-osx,$(ROCKSDBJNILIB)))
ifeq ($(MACHINE),arm64)
	ROCKSDBJNILIB = librocksdbjni-osx-arm64.jnilib
else ifeq ($(MACHINE),x86_64)
	ROCKSDBJNILIB = librocksdbjni-osx-x86_64.jnilib
else
	ROCKSDBJNILIB = librocksdbjni-osx.jnilib
endif
endif
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-osx.jar
	SHA256_CMD = openssl sha256 -r
ifneq ("$(wildcard $(JAVA_HOME)/include/darwin)","")
	JAVA_INCLUDE = -I$(JAVA_HOME)/include -I $(JAVA_HOME)/include/darwin
else
	JAVA_INCLUDE = -I/System/Library/Frameworks/JavaVM.framework/Headers/
endif
endif

ifeq ($(PLATFORM), OS_FREEBSD)
	JAVA_INCLUDE = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/freebsd
	ROCKSDBJNILIB = librocksdbjni-freebsd$(ARCH).so
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-freebsd$(ARCH).jar
endif
ifeq ($(PLATFORM), OS_AIX)
	JAVA_INCLUDE = -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/aix
	ROCKSDBJNILIB = librocksdbjni-aix.so
	EXTRACT_SOURCES = gunzip < TAR_GZ | tar xvf -
	SNAPPY_MAKE_TARGET = libsnappy.la
endif
ifeq ($(PLATFORM), OS_OPENBSD)
	JAVA_INCLUDE = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/openbsd
	ROCKSDBJNILIB = librocksdbjni-openbsd$(ARCH).so
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-openbsd$(ARCH).jar
endif
export SHA256_CMD

# ----------------------------------------------------------------------------
# Build parameter change detection
# ----------------------------------------------------------------------------
# RocksDB writes object files to the same $(OBJ_DIR) paths regardless of
# DEBUG_LEVEL, sanitizers (ASAN/TSAN/UBSAN), ASSERT_STATUS_CHECKED, RTTI, LTO,
# COERCE_CONTEXT_SWITCH, etc.  Most of those are applied in this Makefile after
# `include make_config.mk` and are therefore NOT reflected in make_config.mk,
# so switching them and rebuilding silently mixes incompatible object files.
# To guard against that, we hash the fully-resolved compile/link parameters
# (which already embed make_config.mk via PLATFORM_*) and compare against the
# value recorded from the previous build.  On mismatch the build stops so the
# user can `make clean`.  Knobs:
#   AUTO_CLEAN=1
#     run `make clean-rocks` automatically on mismatch
#   ALLOW_BUILD_PARAMETER_CHANGE=1
#     skip the check entirely (e.g. intentionally mixing DEBUG_LEVEL=1 and
#     DEBUG_LEVEL=2)
# The signature is stored per-$(OBJ_DIR), so Java (jl/jls) builds are tracked
# independently from the default build.
BUILD_SIG_FILE := $(OBJ_DIR)/.build_signature
# NOTE: deliberately NOT added to CLEAN_FILES. The signature is removed as the
# very last step of clean-rocks so that an interrupted clean keeps it (see
# clean-rocks), preserving change detection across a Ctrl+C'd clean.

# Goals that do not compile object files into the tree (pure utilities,
# informational, source/config checks): the change check is irrelevant for them.
BUILD_SIG_NONBUILD_GOALS := \
	clean clean-rocks clean-not-downloaded clean-rocksjava \
	clean-not-downloaded-rocksjava clean-ext-libraries-all \
	clean-ext-libraries-bin \
	format format-auto check-format check-buck-targets check-headers \
	check-sources check-workflow-yaml check-progress clang-tidy \
	tags tags0 package jclean checkout_folly build_folly \
	watch-log dump-log suggest-slow-tests list_all_tests gen-pc \
	gen_parallel_tests gen_check_scripts check-c-api-gen check-all-python \
	setup-hooks install-hooks uninstall-hooks uninstall db_crashtest_tests
# Goals that clean before building (depend on or invoke `clean`): they manage
# their own freshness, so the check must not block them (it would error before
# their built-in clean runs).
BUILD_SIG_NONBUILD_GOALS += \
	release coverage build_size analyze analyze_incremental \
	asan_check asan_crash_test asan_crash_test_with_atomic_flush \
	asan_crash_test_with_txn asan_crash_test_with_best_efforts_recovery \
	whitebox_asan_crash_test blackbox_asan_crash_test \
	ubsan_check ubsan_crash_test ubsan_crash_test_with_atomic_flush \
	ubsan_crash_test_with_txn ubsan_crash_test_with_best_efforts_recovery \
	whitebox_ubsan_crash_test blackbox_ubsan_crash_test

# Goals that explicitly clean; never trip the check when one is requested.
BUILD_SIG_CLEAN_GOALS := \
	clean clean-rocks clean-not-downloaded clean-rocksjava \
	clean-not-downloaded-rocksjava clean-ext-libraries-all \
	clean-ext-libraries-bin

BUILD_SIG_GOALS := $(if $(MAKECMDGOALS),$(MAKECMDGOALS),all)
BUILD_SIG_DO_BUILD := $(filter-out $(BUILD_SIG_NONBUILD_GOALS),$(BUILD_SIG_GOALS))
BUILD_SIG_CLEANING := $(filter $(BUILD_SIG_CLEAN_GOALS),$(MAKECMDGOALS))
# Dry runs (-n/--just-print) must not write the stamp, clean, or error; the
# parse-time $(shell) below would otherwise run even under -n. Note that
# actual options are canonicalized and shorted as possible such that all
# short options are in the first word of MAKEFLAGS.
BUILD_SIG_DRYRUN := $(findstring n,$(firstword -$(MAKEFLAGS)))

# Only enforce at the top level. Sub-makes (e.g. `check` invokes
# $(MAKE) gen_parallel_tests / check-c-api-gen / check_0) inherit the parent's
# build flags, so the top-level check already covers them; re-checking inside a
# sub-make only causes spurious failures.
ifneq ($(ALLOW_BUILD_PARAMETER_CHANGE),1)
ifeq ($(MAKELEVEL),0)
ifeq ($(BUILD_SIG_DRYRUN),)
ifeq ($(BUILD_SIG_CLEANING),)
ifneq ($(BUILD_SIG_DO_BUILD),)
  BUILD_SIG := $(shell printf '%s' '$(CC)|$(CXX)|$(CFLAGS)|$(CXXFLAGS)|$(LDFLAGS)|$(EXEC_LDFLAGS)' | $(SHA256_CMD) | cut -d ' ' -f 1)
  BUILD_SIG_OLD := $(shell cat $(BUILD_SIG_FILE) 2>/dev/null)
  ifneq ($(BUILD_SIG_OLD),)
  ifneq ($(BUILD_SIG_OLD),$(BUILD_SIG))
  ifeq ($(AUTO_CLEAN),1)
    $(info *** Build parameters changed since last build (OBJ_DIR=$(OBJ_DIR)); running 'make clean-rocks' because AUTO_CLEAN=1)
    BUILD_SIG_CLEAN_OUTPUT := $(shell $(MAKE) clean-rocks 1>&2)
    ifneq ($(.SHELLSTATUS),0)
      $(error AUTO_CLEAN: 'make clean-rocks' failed (exit $(.SHELLSTATUS)); not building against a partially-cleaned tree)
    endif
  else
    $(error Build parameters changed since the last build (OBJ_DIR=$(OBJ_DIR)). Existing object files are stale and must be removed. Run 'make clean', or set AUTO_CLEAN=1 to clean automatically, or ALLOW_BUILD_PARAMETER_CHANGE=1 to build anyway)
  endif
  endif
  endif
  # Record the current signature for the next build.
  BUILD_SIG_WRITE := $(shell mkdir -p $(OBJ_DIR) 2>/dev/null; printf '%s\n' '$(BUILD_SIG)' > $(BUILD_SIG_FILE))
endif
endif
endif
endif
endif

zlib-$(ZLIB_VER).tar.gz:
	$(CURL_DOWNLOAD) --output zlib-$(ZLIB_VER).tar.gz ${ZLIB_DOWNLOAD_PREFIX}$(ZLIB_VER).tar.gz
	ZLIB_SHA256_ACTUAL=`$(SHA256_CMD) zlib-$(ZLIB_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(ZLIB_SHA256)" != "$$ZLIB_SHA256_ACTUAL" ]; then \
		echo zlib-$(ZLIB_VER).tar.gz checksum mismatch, expected=\"$(ZLIB_SHA256)\" actual=\"$$ZLIB_SHA256_ACTUAL\"; \
		exit 1; \
	fi

libz.a: zlib-$(ZLIB_VER).tar.gz
	-rm -rf zlib-$(ZLIB_VER)
	tar xvzf zlib-$(ZLIB_VER).tar.gz
	if [ -n"$(ARCHFLAG)" ]; then \
		cd zlib-$(ZLIB_VER) && CFLAGS='-fPIC ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' ./configure --static --archs="$(ARCHFLAG)" && $(MAKE);  \
	else \
		cd zlib-$(ZLIB_VER) && CFLAGS='-fPIC ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' ./configure --static && $(MAKE);  \
	fi
	cp zlib-$(ZLIB_VER)/libz.a .

bzip2-$(BZIP2_VER).tar.gz:
	$(CURL_DOWNLOAD) --output bzip2-$(BZIP2_VER).tar.gz ${BZIP2_DOWNLOAD_PREFIX}$(BZIP2_VER).tar.gz
	BZIP2_SHA256_ACTUAL=`$(SHA256_CMD) bzip2-$(BZIP2_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(BZIP2_SHA256)" != "$$BZIP2_SHA256_ACTUAL" ]; then \
		echo bzip2-$(BZIP2_VER).tar.gz checksum mismatch, expected=\"$(BZIP2_SHA256)\" actual=\"$$BZIP2_SHA256_ACTUAL\"; \
		exit 1; \
	fi

libbz2.a: bzip2-$(BZIP2_VER).tar.gz
	-rm -rf bzip2-$(BZIP2_VER)
	tar xvzf bzip2-$(BZIP2_VER).tar.gz
	cd bzip2-$(BZIP2_VER) && $(MAKE) CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64 $(ARCHFLAG) ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' AR='ar ${EXTRA_ARFLAGS}' libbz2.a
	cp bzip2-$(BZIP2_VER)/libbz2.a .

snappy-$(SNAPPY_VER).tar.gz:
	$(CURL_DOWNLOAD) --output snappy-$(SNAPPY_VER).tar.gz ${SNAPPY_DOWNLOAD_PREFIX}$(SNAPPY_VER).tar.gz
	SNAPPY_SHA256_ACTUAL=`$(SHA256_CMD) snappy-$(SNAPPY_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(SNAPPY_SHA256)" != "$$SNAPPY_SHA256_ACTUAL" ]; then \
		echo snappy-$(SNAPPY_VER).tar.gz checksum mismatch, expected=\"$(SNAPPY_SHA256)\" actual=\"$$SNAPPY_SHA256_ACTUAL\"; \
		exit 1; \
	fi

libsnappy.a: snappy-$(SNAPPY_VER).tar.gz
	-rm -rf snappy-$(SNAPPY_VER)
	tar xvzf snappy-$(SNAPPY_VER).tar.gz
	mkdir snappy-$(SNAPPY_VER)/build
	cd snappy-$(SNAPPY_VER)/build && CFLAGS='$(ARCHFLAG) ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' CXXFLAGS='$(ARCHFLAG) ${JAVA_STATIC_DEPS_CXXFLAGS} ${EXTRA_CXXFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DSNAPPY_BUILD_BENCHMARKS=OFF -DSNAPPY_BUILD_TESTS=OFF ${PLATFORM_CMAKE_FLAGS} .. && $(MAKE) ${SNAPPY_MAKE_TARGET}
	cp snappy-$(SNAPPY_VER)/build/libsnappy.a .

lz4-$(LZ4_VER).tar.gz:
	$(CURL_DOWNLOAD) --output lz4-$(LZ4_VER).tar.gz ${LZ4_DOWNLOAD_PREFIX}$(LZ4_VER).tar.gz
	LZ4_SHA256_ACTUAL=`$(SHA256_CMD) lz4-$(LZ4_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(LZ4_SHA256)" != "$$LZ4_SHA256_ACTUAL" ]; then \
		echo lz4-$(LZ4_VER).tar.gz checksum mismatch, expected=\"$(LZ4_SHA256)\" actual=\"$$LZ4_SHA256_ACTUAL\"; \
		exit 1; \
	fi

liblz4.a: lz4-$(LZ4_VER).tar.gz
	-rm -rf lz4-$(LZ4_VER)
	tar xvzf lz4-$(LZ4_VER).tar.gz
	cd lz4-$(LZ4_VER)/lib && $(MAKE) CFLAGS='-fPIC -O2 $(ARCHFLAG) ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' all
	cp lz4-$(LZ4_VER)/lib/liblz4.a .

zstd-$(ZSTD_VER).tar.gz:
	$(CURL_DOWNLOAD) --output zstd-$(ZSTD_VER).tar.gz ${ZSTD_DOWNLOAD_PREFIX}$(ZSTD_VER).tar.gz
	ZSTD_SHA256_ACTUAL=`$(SHA256_CMD) zstd-$(ZSTD_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(ZSTD_SHA256)" != "$$ZSTD_SHA256_ACTUAL" ]; then \
		echo zstd-$(ZSTD_VER).tar.gz checksum mismatch, expected=\"$(ZSTD_SHA256)\" actual=\"$$ZSTD_SHA256_ACTUAL\"; \
		exit 1; \
	fi

libzstd.a: zstd-$(ZSTD_VER).tar.gz
	-rm -rf zstd-$(ZSTD_VER)
	tar xvzf zstd-$(ZSTD_VER).tar.gz
	cd zstd-$(ZSTD_VER)/lib && DESTDIR=. PREFIX= $(MAKE) CFLAGS='-fPIC -O2 $(ARCHFLAG) ${JAVA_STATIC_DEPS_CCFLAGS} ${EXTRA_CFLAGS}' LDFLAGS='${JAVA_STATIC_DEPS_LDFLAGS} ${EXTRA_LDFLAGS}' libzstd.a
	cp zstd-$(ZSTD_VER)/lib/libzstd.a .

# A version of each $(LIB_OBJECTS) compiled with -fPIC and a fixed set of static compression libraries
ifneq ($(ROCKSDB_JAVA_NO_COMPRESSION), 1)
JAVA_COMPRESSIONS = libz.a libbz2.a libsnappy.a liblz4.a libzstd.a
endif

JAVA_STATIC_FLAGS = -DZLIB -DBZIP2 -DSNAPPY -DLZ4 -DZSTD
JAVA_STATIC_INCLUDES = -I./zlib-$(ZLIB_VER) -I./bzip2-$(BZIP2_VER) -I./snappy-$(SNAPPY_VER) -I./snappy-$(SNAPPY_VER)/build -I./lz4-$(LZ4_VER)/lib -I./zstd-$(ZSTD_VER)/lib -I./zstd-$(ZSTD_VER)/lib/dictBuilder

ifneq ($(findstring rocksdbjavastatic, $(filter-out rocksdbjavastatic_deps, $(MAKECMDGOALS))),)
CXXFLAGS += $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES)
CFLAGS += $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES)
endif
rocksdbjavastatic:
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
endif
	$(MAKE) rocksdbjavastatic_deps
	$(MAKE) rocksdbjavastatic_libobjects
	$(MAKE) rocksdbjavastatic_javalib
	$(MAKE) rocksdbjava_jar

rocksdbjavastaticosx: rocksdbjavastaticosx_archs
	cd java; $(JAR_CMD)  -cf target/$(ROCKSDB_JAR) HISTORY*.md
	cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR) librocksdbjni-osx-x86_64.jnilib librocksdbjni-osx-arm64.jnilib
	cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1

rocksdbjavastaticosx_ub: rocksdbjavastaticosx_archs
	cd java/target; lipo -create -output librocksdbjni-osx.jnilib librocksdbjni-osx-x86_64.jnilib librocksdbjni-osx-arm64.jnilib
	cd java; $(JAR_CMD)  -cf target/$(ROCKSDB_JAR) HISTORY*.md
	cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR) librocksdbjni-osx.jnilib
	cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1

rocksdbjavastaticosx_archs:
	$(MAKE) rocksdbjavastaticosx_arch_x86_64
	$(MAKE) rocksdbjavastaticosx_arch_arm64

rocksdbjavastaticosx_arch_%:
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
endif
	$(MAKE) clean-ext-libraries-bin
	$(MAKE) clean-rocks
	ARCHFLAG="-arch $*" $(MAKE) rocksdbjavastatic_deps
	ARCHFLAG="-arch $*" $(MAKE) rocksdbjavastatic_libobjects
	ARCHFLAG="-arch $*" ROCKSDBJNILIB="librocksdbjni-osx-$*.jnilib" $(MAKE) rocksdbjavastatic_javalib

ifeq ($(JAR_CMD),)
ifneq ($(JAVA_HOME),)
JAR_CMD := $(JAVA_HOME)/bin/jar
else
JAR_CMD := jar
endif
endif
rocksdbjavastatic_javalib:
	cd java; $(MAKE) javalib
	rm -f java/target/$(ROCKSDBJNILIB)
	$(CXX) $(CXXFLAGS) -I./java/. $(JAVA_INCLUDE) -shared -fPIC \
	  -o ./java/target/$(ROCKSDBJNILIB) $(ALL_JNI_NATIVE_SOURCES) \
	  $(LIB_OBJECTS) $(COVERAGEFLAGS) \
	  $(JAVA_COMPRESSIONS) $(JAVA_STATIC_LDFLAGS)
	cd java/target;if [ "$(DEBUG_LEVEL)" == "0" ]; then \
		strip $(STRIPFLAGS) $(ROCKSDBJNILIB); \
	fi

rocksdbjava_jar:
	cd java; $(JAR_CMD)  -cf target/$(ROCKSDB_JAR) HISTORY*.md
	cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1

rocksdbjava_javadocs_jar:
	cd java/target/apidocs; $(JAR_CMD) -cf ../$(ROCKSDB_JAVADOCS_JAR) *
	openssl sha1 java/target/$(ROCKSDB_JAVADOCS_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAVADOCS_JAR).sha1

rocksdbjava_sources_jar:
	cd java/src/main/java; $(JAR_CMD) -cf ../../../target/$(ROCKSDB_SOURCES_JAR) org
	openssl sha1 java/target/$(ROCKSDB_SOURCES_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_SOURCES_JAR).sha1

rocksdbjavastatic_deps: $(JAVA_COMPRESSIONS)

rocksdbjavastatic_libobjects: $(LIB_OBJECTS)

rocksdbjavastaticrelease: rocksdbjavastaticosx rocksdbjava_javadocs_jar rocksdbjava_sources_jar
	cd java/crossbuild && (vagrant destroy -f || true) && vagrant up linux32 && vagrant halt linux32 && vagrant up linux64 && vagrant halt linux64 && vagrant up linux64-musl && vagrant halt linux64-musl
	cd java; $(JAR_CMD) -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR_ALL) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR_ALL).sha1

rocksdbjavastaticreleasedocker: rocksdbjavastaticosx rocksdbjavastaticdockerx86 rocksdbjavastaticdockerx86_64 rocksdbjavastaticdockerx86musl rocksdbjavastaticdockerx86_64musl rocksdbjava_javadocs_jar rocksdbjava_sources_jar
	cd java; $(JAR_CMD) -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR_ALL) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR_ALL).sha1

rocksdbjavastaticdockerx86:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x86-be --platform linux/386 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:centos7_x86-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerx86_64:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x64-be --platform linux/amd64 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:centos7_x64-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerppc64le:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_ppc64le-be --platform linux/ppc64le --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:centos7_ppc64le-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerarm64v8:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_arm64v8-be --platform linux/aarch64 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:centos7_arm64v8-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockers390x:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_s390x-be --platform linux/s390x --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:ubuntu18_s390x-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerriscv64:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_riscv64-be --platform linux/riscv64 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:ubuntu20_riscv64-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerx86musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x86-musl-be --platform linux/386 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:alpine3_x86-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerx86_64musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x64-musl-be --platform linux/amd64 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:alpine3_x64-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerppc64lemusl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_ppc64le-musl-be --platform linux/ppc64le --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:alpine3_ppc64le-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockerarm64v8musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_arm64v8-musl-be --platform linux/aarch64 --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:alpine3_arm64v8-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticdockers390xmusl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_s390x-musl-be --platform linux/s390x --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) --env J=$(J) evolvedbinary/rocksjava:alpine3_s390x-be /rocksdb-host/java/crossbuild/docker-build-linux.sh

rocksdbjavastaticpublish: rocksdbjavastaticrelease rocksdbjavastaticpublishcentral

rocksdbjavastaticpublishdocker: rocksdbjavastaticreleasedocker rocksdbjavastaticpublishcentral

ROCKSDB_JAVA_RELEASE_CLASSIFIERS = javadoc sources linux64 linux32 linux64-musl linux32-musl osx win64

rocksdbjavastaticpublishcentral: rocksdbjavageneratepom
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar
	$(foreach classifier, $(ROCKSDB_JAVA_RELEASE_CLASSIFIERS), mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar -Dclassifier=$(classifier);)

rocksdbjavageneratepom:
	cd java;cat pom.xml.template | sed 's/\$${ROCKSDB_JAVA_VERSION}/$(ROCKSDB_JAVA_VERSION)/' > pom.xml

rocksdbjavastaticnexusbundlejar: rocksdbjavageneratepom
	openssl sha1 -r java/pom.xml | awk '{  print $$1 }' > java/target/pom.xml.sha1
	openssl sha1 -r java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar | awk '{  print $$1 }' > java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar.sha1
	$(foreach classifier, $(ROCKSDB_JAVA_RELEASE_CLASSIFIERS), openssl sha1 -r java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar | awk '{  print $$1 }' > java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar.sha1;)
	gpg --yes --output java/target/pom.xml.asc -ab java/pom.xml
	gpg --yes -ab java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar
	$(foreach classifier, $(ROCKSDB_JAVA_RELEASE_CLASSIFIERS), gpg --yes -ab java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar;)
	$(JAR_CMD) cvf java/target/nexus-bundle-rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar -C java pom.xml -C java/target pom.xml.sha1 -C java/target pom.xml.asc -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar.sha1 -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar.asc
	$(foreach classifier, $(ROCKSDB_JAVA_RELEASE_CLASSIFIERS), $(JAR_CMD) uf java/target/nexus-bundle-rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar.sha1 -C java/target rocksdbjni-$(ROCKSDB_JAVA_VERSION)-$(classifier).jar.asc;)


# A version of each $(LIBOBJECTS) compiled with -fPIC

jl/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -fPIC -c $< -o $@ $(COVERAGEFLAGS)

rocksdbjava: $(LIB_OBJECTS)
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
endif
	$(AM_V_GEN)cd java; $(MAKE) javalib;
	$(AM_V_at)rm -f ./java/target/$(ROCKSDBJNILIB)
	$(AM_V_at)$(CXX) $(CXXFLAGS) -I./java/. -I./java/rocksjni $(JAVA_INCLUDE) $(ROCKSDB_PLUGIN_JNI_CXX_INCLUDEFLAGS) -shared -fPIC -o ./java/target/$(ROCKSDBJNILIB) $(ALL_JNI_NATIVE_SOURCES) $(LIB_OBJECTS) $(JAVA_LDFLAGS) $(COVERAGEFLAGS)
	$(AM_V_at)cd java; $(JAR_CMD) -cf target/$(ROCKSDB_JAR) HISTORY*.md
	$(AM_V_at)cd java/target; $(JAR_CMD) -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	$(AM_V_at)cd java/target/classes; $(JAR_CMD) -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	$(AM_V_at)openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1

jclean:
	cd java;$(MAKE) clean;

jtest_compile: rocksdbjava
	cd java;$(MAKE) java_test

jtest_run:
	cd java;$(MAKE) run_test

jtest: rocksdbjava
	cd java;$(MAKE) sample test

jpmd: rocksdbjavageneratepom
	cd java;$(MAKE) java java_test pmd

jdb_bench:
	cd java;$(MAKE) db_bench;

commit_prereq:
	echo "TODO: bring this back using parts of old precommit_checker.py and rocksdb-lego-determinator"
	false # J=$(J) build_tools/precommit_checker.py unit clang_unit release clang_release tsan asan ubsan lite unit_non_shm
	# $(MAKE) clean && $(MAKE) jclean && $(MAKE) rocksdbjava;

# ---------------------------------------------------------------------------
#   Build size testing
# ---------------------------------------------------------------------------

REPORT_BUILD_STATISTIC?=echo STATISTIC:

build_size:
	# === normal build, static ===
	$(MAKE) clean
	$(MAKE) static_lib
	$(REPORT_BUILD_STATISTIC) rocksdb.build_size.static_lib $$(stat --printf="%s" librocksdb.a)
	strip librocksdb.a
	$(REPORT_BUILD_STATISTIC) rocksdb.build_size.static_lib_stripped $$(stat --printf="%s" librocksdb.a)
	# === normal build, shared ===
	$(MAKE) clean
	$(MAKE) shared_lib
	$(REPORT_BUILD_STATISTIC) rocksdb.build_size.shared_lib $$(stat --printf="%s" `readlink -f librocksdb.so`)
	strip `readlink -f librocksdb.so`
	$(REPORT_BUILD_STATISTIC) rocksdb.build_size.shared_lib_stripped $$(stat --printf="%s" `readlink -f librocksdb.so`)

# ---------------------------------------------------------------------------
#  	Platform-specific compilation
# ---------------------------------------------------------------------------

ifeq ($(PLATFORM), IOS)
# For iOS, create universal object files to be used on both the simulator and
# a device.
XCODEROOT=$(shell xcode-select -print-path)
PLATFORMSROOT=$(XCODEROOT)/Platforms
SIMULATORROOT=$(PLATFORMSROOT)/iPhoneSimulator.platform/Developer
DEVICEROOT=$(PLATFORMSROOT)/iPhoneOS.platform/Developer
IOSVERSION=$(shell defaults read $(PLATFORMSROOT)/iPhoneOS.platform/version CFBundleShortVersionString)

.cc.o:
	mkdir -p ios-x86/$(dir $@)
	$(CXX) $(CXXFLAGS) -isysroot $(SIMULATORROOT)/SDKs/iPhoneSimulator$(IOSVERSION).sdk -arch i686 -arch x86_64 -c $< -o ios-x86/$@
	mkdir -p ios-arm/$(dir $@)
	xcrun -sdk iphoneos $(CXX) $(CXXFLAGS) -isysroot $(DEVICEROOT)/SDKs/iPhoneOS$(IOSVERSION).sdk -arch armv6 -arch armv7 -arch armv7s -arch arm64 -c $< -o ios-arm/$@
	lipo ios-x86/$@ ios-arm/$@ -create -output $@

.c.o:
	mkdir -p ios-x86/$(dir $@)
	$(CC) $(CFLAGS) -isysroot $(SIMULATORROOT)/SDKs/iPhoneSimulator$(IOSVERSION).sdk -arch i686 -arch x86_64 -c $< -o ios-x86/$@
	mkdir -p ios-arm/$(dir $@)
	xcrun -sdk iphoneos $(CC) $(CFLAGS) -isysroot $(DEVICEROOT)/SDKs/iPhoneOS$(IOSVERSION).sdk -arch armv6 -arch armv7 -arch armv7s -arch arm64 -c $< -o ios-arm/$@
	lipo ios-x86/$@ ios-arm/$@ -create -output $@

else
ifeq ($(HAVE_POWER8),1)
$(OBJ_DIR)/util/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR)/util/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif
$(OBJ_DIR)/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

$(OBJ_DIR)/%.o: %.cpp
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

$(OBJ_DIR)/%.o: %.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif

# ---------------------------------------------------------------------------
#  	Source files dependencies detection
# ---------------------------------------------------------------------------
# If skip dependencies is ON, skip including the dep files
ifneq ($(SKIP_DEPENDS), 1)
DEPFILES = $(patsubst %.cc, $(OBJ_DIR)/%.cc.d, $(ALL_SOURCES))
DEPFILES+ = $(patsubst %.c, $(OBJ_DIR)/%.c.d, $(LIB_SOURCES_C) $(TEST_MAIN_SOURCES_C))
ifeq ($(USE_FOLLY_LITE),1)
  DEPFILES +=$(patsubst %.cpp, $(OBJ_DIR)/%.cpp.d, $(FOLLY_SOURCES))
endif
endif

# Add proper dependency support so changing a .h file forces a .cc file to
# rebuild.

# The .d file indicates .cc file's dependencies on .h files. We generate such
# dependency by g++'s -MM option, whose output is a make dependency rule.
$(OBJ_DIR)/%.cc.d: %.cc
	@mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cc=.o)' -MT'$(<:%.cc=$(OBJ_DIR)/%.o)' \
          "$<" -o '$@'

$(OBJ_DIR)/%.cpp.d: %.cpp
	@mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cpp=.o)' -MT'$(<:%.cpp=$(OBJ_DIR)/%.o)' \
          "$<" -o '$@'

ifeq ($(HAVE_POWER8),1)
DEPFILES_C = $(patsubst %.c, $(OBJ_DIR)/%.c.d, $(LIB_SOURCES_C))
DEPFILES_ASM = $(patsubst %.S, $(OBJ_DIR)/%.S.d, $(LIB_SOURCES_ASM))

$(OBJ_DIR)/%.c.d: %.c
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.c=.o)' "$<" -o '$@'

$(OBJ_DIR)/%.S.d: %.S
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.S=.o)' "$<" -o '$@'

$(DEPFILES_C): %.c.d

$(DEPFILES_ASM): %.S.d
depend: $(DEPFILES) $(DEPFILES_C) $(DEPFILES_ASM)
else
depend: $(DEPFILES)
endif

build_subset_tests: $(ROCKSDBTESTS_SUBSET)
	$(AM_V_GEN)if [ -n "$${ROCKSDBTESTS_SUBSET_TESTS_TO_FILE}" ]; then echo "$(ROCKSDBTESTS_SUBSET)" > "$${ROCKSDBTESTS_SUBSET_TESTS_TO_FILE}"; else echo "$(ROCKSDBTESTS_SUBSET)"; fi

list_all_tests:
	echo "$(ROCKSDBTESTS_SUBSET)"

# Remove the rules for which dependencies should not be generated and see if any are left.
#If so, include the dependencies; if not, do not include the dependency files
ROCKS_DEP_RULES=$(filter-out clean format check-format check-buck-targets check-headers check-sources check-workflow-yaml clang-tidy jclean jtest package analyze tags rocksdbjavastatic% unity.% unity_test checkout_folly, $(MAKECMDGOALS))
ifneq ("$(ROCKS_DEP_RULES)", "")
-include $(DEPFILES)
endif
