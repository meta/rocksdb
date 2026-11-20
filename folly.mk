# This file contains the vast majority of folly-related build configuration
# for the checkout_folly and build_folly targets, so that this file can be
# hashed for purposes of caching folly builds and not hitting that cache when
# something here changes.

# This provides a Makefile simulation of a Meta-internal folly integration.
# It is not validated for general use.
#
# USE_FOLLY links the build targets with libfolly.a. The latter could be
# built using 'make build_folly', or built externally and specified in
# the CXXFLAGS and EXTRA_LDFLAGS env variables. The build_detect_platform
# script tries to detect if an external folly dependency has been specified.
# If not, it exports FOLLY_PATH to the path of the installed Folly and
# dependency libraries.
#
# USE_FOLLY_LITE cherry picks source files from Folly to include in the
# RocksDB library. Its faster and has fewer dependencies on 3rd party
# libraries, but with limited functionality. For example, coroutine
# functionality is not available.
ifeq ($(USE_FOLLY),1)
ifeq ($(USE_FOLLY_LITE),1)
$(error Please specify only one of USE_FOLLY and USE_FOLLY_LITE)
endif
ifneq ($(strip $(FOLLY_PATH)),)
	BOOST_PATH = $(shell (ls -d $(FOLLY_PATH)/../boost*))
	GFLAGS_PATH = $(shell (ls -d $(FOLLY_PATH)/../gflags*))
	GLOG_PATH = $(shell (ls -d $(FOLLY_PATH)/../glog*))
	LIBEVENT_PATH = $(shell (ls -d $(FOLLY_PATH)/../libevent*))
	XZ_PATH = $(shell (ls -d $(FOLLY_PATH)/../xz*))
	LIBSODIUM_PATH = $(shell (ls -d $(FOLLY_PATH)/../libsodium*))
	FMT_PATH = $(shell (ls -d $(FOLLY_PATH)/../fmt*))
	LIBUNWIND_PATH = $(shell (ls -d $(FOLLY_PATH)/../libunwind*))

	# For some reason, some libraries are under either lib or lib64
	BOOST_LIB_PATH = $(shell (ls -d $(BOOST_PATH)/lib*))
	GLOG_LIB_PATH = $(shell (ls -d $(GLOG_PATH)/lib*))
	FMT_LIB_PATH = $(shell (ls -d $(FMT_PATH)/lib*))
	LIBEVENT_LIB_PATH = $(shell (ls -d $(LIBEVENT_PATH)/lib*))

	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(BOOST_PATH)/include -I$(GLOG_PATH)/include -I$(LIBEVENT_PATH)/include -I$(XZ_PATH)/include -I$(LIBSODIUM_PATH)/include -I$(FOLLY_PATH)/include -I$(FMT_PATH)/include
		PLATFORM_CXXFLAGS += -I$(BOOST_PATH)/include -I$(GLOG_PATH)/include -I$(LIBEVENT_PATH)/include -I$(XZ_PATH)/include -I$(LIBSODIUM_PATH)/include -I$(FOLLY_PATH)/include -I$(FMT_PATH)/include
	else
		PLATFORM_CCFLAGS += -isystem $(BOOST_PATH)/include -isystem $(GLOG_PATH)/include -isystem $(LIBEVENT_PATH)/include -isystem $(XZ_PATH)/include -isystem $(LIBSODIUM_PATH)/include -isystem $(FOLLY_PATH)/include -isystem $(FMT_PATH)/include
		PLATFORM_CXXFLAGS += -isystem $(BOOST_PATH)/include -isystem $(GLOG_PATH)/include -isystem $(LIBEVENT_PATH)/include -isystem $(XZ_PATH)/include -isystem $(LIBSODIUM_PATH)/include -isystem $(FOLLY_PATH)/include -isystem $(FMT_PATH)/include
	endif

	# Add -ldl at the end as gcc resolves a symbol in a library by searching only in libraries specified later
	# in the command line

	PLATFORM_LDFLAGS += $(FOLLY_PATH)/lib/libfolly.a $(BOOST_LIB_PATH)/libboost_context.a $(BOOST_LIB_PATH)/libboost_filesystem.a $(BOOST_LIB_PATH)/libboost_atomic.a $(BOOST_LIB_PATH)/libboost_program_options.a $(BOOST_LIB_PATH)/libboost_thread.a $(LIBEVENT_LIB_PATH)/libevent.a $(LIBSODIUM_PATH)/lib/libsodium.a $(LIBUNWIND_PATH)/lib/libunwind.a -ldl
ifneq ($(DEBUG_LEVEL),0)
	PLATFORM_LDFLAGS += $(FMT_LIB_PATH)/libfmtd.a $(GLOG_LIB_PATH)/libglogd.so $(GFLAGS_PATH)/lib/libgflags_debug.so.2.3
else
	PLATFORM_LDFLAGS += $(FMT_LIB_PATH)/libfmt.a $(GLOG_LIB_PATH)/libglog.so $(GFLAGS_PATH)/lib/libgflags.so.2.3
endif
	PLATFORM_LDFLAGS += -Wl,-rpath=$(GLOG_LIB_PATH) -Wl,-rpath=$(GFLAGS_PATH)/lib
endif
	PLATFORM_CCFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
	PLATFORM_CXXFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
endif

ifeq ($(USE_FOLLY_LITE),1)
	# Path to the Folly source code and include files
	FOLLY_DIR = ./third-party/folly
ifneq ($(strip $(BOOST_SOURCE_PATH)),)
	BOOST_INCLUDE = $(shell (ls -d $(BOOST_SOURCE_PATH)/boost*/))
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(BOOST_INCLUDE)
		PLATFORM_CXXFLAGS += -I$(BOOST_INCLUDE)
	else
		PLATFORM_CCFLAGS += -isystem $(BOOST_INCLUDE)
		PLATFORM_CXXFLAGS += -isystem $(BOOST_INCLUDE)
	endif
endif  # BOOST_SOURCE_PATH
ifneq ($(strip $(FMT_SOURCE_PATH)),)
	FMT_INCLUDE = $(shell (ls -d $(FMT_SOURCE_PATH)/fmt*/include/))
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(FMT_INCLUDE)
		PLATFORM_CXXFLAGS += -I$(FMT_INCLUDE)
	else
		PLATFORM_CCFLAGS += -isystem $(FMT_INCLUDE)
		PLATFORM_CXXFLAGS += -isystem $(FMT_INCLUDE)
	endif
endif  # FMT_SOURCE_PATH
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -I$(FOLLY_DIR)
	else
		PLATFORM_CCFLAGS += -isystem $(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -isystem $(FOLLY_DIR)
	endif
	PLATFORM_CCFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
	PLATFORM_CXXFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
# TODO: fix linking with fbcode compiler config
	PLATFORM_LDFLAGS += -lglog
endif

FOLLY_COMMIT_HASH = 7ba92f0e29d24b453cee8ab27d117b1c7cc19589
FOLLY_GETDEPS_CACHE_DIR = /tmp/rocksdb-getdeps-cache

define restore_folly_getdeps_downloads
	@cd third-party/folly && \
		DOWNLOAD_DIR=`$(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir | sed 's|/installed/.*|/downloads|'` && \
		mkdir -p "$$DOWNLOAD_DIR" && \
		CACHE_DIR="$(FOLLY_GETDEPS_CACHE_DIR)" && \
		mkdir -p "$$CACHE_DIR" && \
		echo "Restoring cached downloads..." && \
		if ls "$$CACHE_DIR"/*.tar.gz "$$CACHE_DIR"/*.tar.xz "$$CACHE_DIR"/*.zip >/dev/null 2>&1; then \
			cp -n "$$CACHE_DIR"/*.tar.gz "$$CACHE_DIR"/*.tar.xz "$$CACHE_DIR"/*.zip "$$DOWNLOAD_DIR/" 2>/dev/null || true; \
		fi && \
		echo "Handling known unreliable downloads with fallback mirrors..." && \
		$(PYTHON) ../../build_tools/getdeps_fallback_mirror.py "$$DOWNLOAD_DIR" "$$CACHE_DIR" build/fbcode_builder/manifests
endef

define cache_folly_getdeps_downloads
	@cd third-party/folly && \
		DOWNLOAD_DIR=`$(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir | sed 's|/installed/.*|/downloads|'` && \
		CACHE_DIR="$(FOLLY_GETDEPS_CACHE_DIR)" && \
		if ls "$$DOWNLOAD_DIR"/*.tar.gz "$$DOWNLOAD_DIR"/*.tar.xz "$$DOWNLOAD_DIR"/*.zip >/dev/null 2>&1; then \
			cp -n "$$DOWNLOAD_DIR"/*.tar.gz "$$DOWNLOAD_DIR"/*.tar.xz "$$DOWNLOAD_DIR"/*.zip "$$CACHE_DIR/" 2>/dev/null || true; \
		fi
endef

# For public CI runs, checkout folly in a way that can build with RocksDB.
# This is mostly intended as a test-only simulation of Meta-internal folly
# integration.
checkout_folly:
	if [ -e third-party/folly ]; then \
		cd third-party/folly && ${GIT_COMMAND} fetch origin; \
	else \
		cd third-party && ${GIT_COMMAND} clone https://github.com/facebook/folly.git; \
	fi
	@# Pin to a particular version for public CI, so that PR authors don't
	@# need to worry about folly breaking our integration. Update periodically
	cd third-party/folly && git reset --hard $(FOLLY_COMMIT_HASH)
	@# Apparently missing include
	perl -pi -e 's/(#include <atomic>)/$$1\n#include <cstring>/' third-party/folly/folly/lang/Exception.h
	@# const mismatch
	perl -pi -e 's/: environ/: (const char**)(environ)/' third-party/folly/folly/Subprocess.cpp
	@# Pagure was decommissioned; use Debian's mirror of the release archive.
	perl -pi -e 's|https://releases.pagure.org/libaio/libaio-0.3.113.tar.gz|https://deb.debian.org/debian/pool/main/liba/libaio/libaio_0.3.113.orig.tar.gz|' third-party/folly/build/fbcode_builder/manifests/libaio
	@# Restore cached downloads and handle unreliable mirrors with fallback
	$(restore_folly_getdeps_downloads)
	@# NOTE: boost and fmt source will be needed for any build including `USE_FOLLY_LITE` builds as those depend on those headers
	@# Prefetch libaio so its source archive is also saved in the getdeps cache.
	cd third-party/folly && \
		for dep in boost fmt libaio; do \
			GETDEPS_USE_WGET=1 $(PYTHON) build/fbcode_builder/getdeps.py fetch $$dep || exit 1; \
		done
	@# Update cache with any new downloads
	$(cache_folly_getdeps_downloads)
	@# The boost source archive has a modular layout (headers under
	@# libs/*/include), so assemble the merged boost/ header tree that
	@# USE_FOLLY_LITE builds put on the include path. This avoids ~150
	@# -isystem flags (one per boost library) on every folly-lite compile.
	cd third-party/folly && \
		BOOST_SRC=`ls -d $$($(PYTHON) build/fbcode_builder/getdeps.py show-source-dir boost)/boost*/` && \
		mkdir -p "$${BOOST_SRC}boost" && \
		for inc in "$${BOOST_SRC}"libs/*/include "$${BOOST_SRC}"libs/numeric/*/include; do \
			cp -R "$$inc/boost/." "$${BOOST_SRC}boost/" || exit 1; \
		done

CXX_M_FLAGS = $(filter -m%, $(CXXFLAGS))

FOLLY_BUILD_FLAGS = --no-tests
# NOTE: To avoid ODR violations, we must build folly in debug mode iff
# building RocksDB in debug mode.
ifneq ($(DEBUG_LEVEL),0)
FOLLY_BUILD_FLAGS += --build-type Debug
endif

build_folly:
	FOLLY_INST_PATH=`cd third-party/folly && $(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir`; \
	if [ "$$FOLLY_INST_PATH" ]; then \
		rm -rf $${FOLLY_INST_PATH}/../../*; \
	else \
		echo "Please run checkout_folly first"; \
		false; \
	fi
	@# Restore fallback archives after the cleanup above removes downloads.
	$(restore_folly_getdeps_downloads)
	cd third-party/folly && \
		CXXFLAGS=" $(CXX_M_FLAGS) -DHAVE_CXX11_ATOMIC " GETDEPS_USE_WGET=1 $(PYTHON) build/fbcode_builder/getdeps.py build $(FOLLY_BUILD_FLAGS)
	@# Update cache with archives downloaded during the build
	$(cache_folly_getdeps_downloads)
	@# In the folly build, glog and gflags are only built as dynamic libraries,
	@# not static. This patchelf command is needed to reliably have the glog
	@# library find its dependency gflags, because apparently the rpath of the
	@# final binary is not used in resolving that transitive dependency.
	FOLLY_INST_PATH=`cd third-party/folly && $(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir`; \
	cd "$$FOLLY_INST_PATH" && patchelf --add-rpath $$PWD/../gflags-*/lib ../glog-*/lib*/libglog*.so.*.*.*
