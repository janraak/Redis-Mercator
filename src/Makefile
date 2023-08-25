
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
DEPENDENCY_TARGETS=hiredis linenoise lua
CPPSOURCES := $(wildcard*.cpp)

INCLUDE_PATHS = -isystem  -DUSE_JEMALLOC -I.. -I../../src -I../deps -I../deps/hiredis  -I../../deps/hdr_histogram -I../deps/linenoise -I../../deps/lua/src -I/usr/include 


# Compile flags for linux / osxext
CFLAGS ?= -std=c99
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -fpermissive  -O0  -fPIC
	SHOBJ_LDFLAGS ?= -shared 
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -fpermissive  -O0  -fPIC
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif


# To get ARM stack traces if Redis crashes we need a special C flag.
ifneq (,$(filter aarch64 armv,$(uname_M)))
        CFLAGS+=-funwind-tables
else
ifneq (,$(findstring armv,$(uname_M)))
        CFLAGS+=-funwind-tables
endif
endif


# Use -Wno-c11-extensions on clang, either where explicitly used or on
# platforms we can assume it's being used.
ifneq (,$(findstring clang,$(CC)))
  CFLAGS+=-Wno-c11-extensions
else
ifneq (,$(findstring FreeBSD,$(uname_S)))
  CFLAGS+=-Wno-c11-extensions
endif
endif

# if debug build is disabled, enable lto

CXXFLAGS ?=  -std=c++20  -fpermissive
CXXFLAGS += -flto  
ifneq ($(DEBUG_BUILD),1)
	# CXXFLAGS += -flto  -Wl,-flto -cpp
	CXXFLAGS +=   -flto 
endif

# WARNNING=-Wold-style-cast \
WARNNING=-Wno-variadic-macros \
-Werror \
-Wpedantic \
-Wextra \
-Wall \
-Wcast-align \
-Wcast-qual \
-Wframe-larger-than=32768 \
-Wno-strict-overflow \
-Wsync-nand \
-Wtrampolines \
-Wsign-compare \
-Werror=float-equal \
-Werror=missing-braces \
-Werror=init-self \
-Werror=logical-op \
-Werror=write-strings \
-Werror=address \
-Werror=array-bounds \
-Werror=char-subscripts \
-Werror=enum-compare \
-Werror=empty-body \
-Werror=main \
-Werror=aggressive-loop-optimizations \
-Werror=nonnull \
-Werror=parentheses \
-Werror=return-type \
-Werror=sequence-point \
-Werror=uninitialized \
-Werror=volatile-register-var \
-Werror=ignored-qualifiers \
-Wno-unused-function \
-Wno-missing-field-initializers \
-Wdouble-promotion \
-Wformat=2


# Include paths to dependencies
# Include REDIS Src
SHOBJ_CFLAGS+= -Wno-pointer-arith 

.SUFFIXES: .c .so .xo .o .cpp

DEBUG += -g -ggdb -fpermissive 

# OPT?=-O0

all: sdsWrapper.xo indexIntercepts.xo rxSuite.xo  rxSuiteHelpers.xo rxIndexer.so rxQuery.so rxIndexStore.so rxMercator.so rxRule.xo  rxRule.so sjiboleth.so rxGraphdb.so 

.c.xo: 
	$(CC) -o $@ -I.. -I. $(CFLAGS) $(SHOBJ_CFLAGS)  -isystem -DUSE_JEMALLOC -D_POSIX_C_SOURCE=199309L  $(INCLUDE_PATHS) -fPIC -Werror=main -c $<

.c.o: 
	$(CC) -o $@ -I.. -I. $(CFLAGS) $(SHOBJ_CFLAGS)  -isystem -DUSE_JEMALLOC -D_POSIX_C_SOURCE=199309L  $(INCLUDE_PATHS) -fPIC -Werror=main -c $<

OBSOLETEFLAGS=-std=c99 
FILT = $(filter-out $(OBSOLETEFLAGS), $(CFLAGS))

.cpp.xo:
	c++  -o $@ -I.. -I. $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)  $(INCLUDE_PATHS)  -fpermissive -isystem -DUSE_JEMALLOC  -Werror=main -std=c++20 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -isystem -DUSE_JEMALLOC    -fPIC  $<

.cpp.o:
	c++  -o $@ -I.. -I. $(CXXFLAGS) $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)  $(INCLUDE_PATHS)  -fpermissive -isystem -DUSE_JEMALLOC  -Werror=main -std=c++20 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -isystem -DUSE_JEMALLOC  -fPIC  $<

rxGraphdb.so: ../../src/redismodule.h rxGraphdb.cpp graphstack.hpp   rxSuite.xo  rxSuiteHelpers.xo   sjiboleth.so  client-pool.hpp  client-pool.cpp tls.xo sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxGraphdb.so  $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)  $(INCLUDE_PATHS) -g -ggdb -fpermissive  -fPIC  rxGraphdb.cpp  rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.so  client-pool.cpp tls.xo  sdsWrapper.xo   rxSessionMemory.xo

sjiboleth.so: ../../src/redismodule.h sjiboleth.cpp sjiboleth.hpp   rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.cpp sjiboleth-silnikparowy.cpp  sjiboleth-silnikparowy-ctx.cpp sjiboleth-generic.cpp sjiboleth-gremlin.cpp sjiboleth-json.cpp sjiboleth-query.cpp sjiboleth-text.cpp  sjiboleth-fablok.cpp rxSuite.xo  rxSuiteHelpers.xo sjiboleth.cpp         sjiboleth-generic.cpp  sjiboleth-parser.cpp  sjiboleth-text.cpp  sjiboleth-gremlin.cpp  sjiboleth-query.cpp   sjibolethWrapper.cpp sjiboleth-fablok.cpp  sjiboleth.xo  sjiboleth-json.cpp sjiboleth.hpp graphstack.hpp makefile client-pool.hpp client-pool.cpp tls.xo sdsWrapper.xo tls.xo   rxSessionMemory.xo
	g++ -o sjiboleth.so  $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)  $(INCLUDE_PATHS)  -g -ggdb -fpermissive   -isystem -DUSE_JEMALLOC -Werror=main -std=c++20 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -isystem -DUSE_JEMALLOC   -fPIC  sjiboleth.cpp  sjiboleth-fablok.cpp sjiboleth-generic.cpp  sjiboleth-gremlin.cpp  sjiboleth-json.cpp  sjiboleth-parser.cpp  sjiboleth-query.cpp sjiboleth-text.cpp  sjibolethWrapper.cpp   rxSuiteHelpers.xo client-pool.cpp tls.xo  sdsWrapper.xo tls.xo  rxSessionMemory.xo

makefile:
	echo "build complete"

rxRule.so: ../../src/redismodule.h rxRule.cpp rule.hpp    rxSuite.xo  rxSuiteHelpers.xo   sjiboleth.so   client-pool.hpp client-pool.cpp tls.xo simpleQueue.hpp   simpleQueue.cpp   sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxRule.so  -shared -g -ggdb -fpermissive  $(SHOBJ_CFLAGS)   $(INCLUDE_PATHS)  -fPIC rxRule.cpp rxSuiteHelpers.xo rxSuite.xo  sjiboleth.so  client-pool.cpp tls.xo  simpleQueue.cpp   sdsWrapper.xo  rxSessionMemory.xo

rxIndexer.so: ../../src/redismodule.h rxIndexer.cpp rxIndexer.hpp rule.hpp  rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.so indexIntercepts.xo simpleQueue.hpp client-pool.hpp client-pool.cpp tls.xo sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxIndexer.so  -shared -g -ggdb -fpermissive  $(SHOBJ_CFLAGS)   $(INCLUDE_PATHS)  -fPIC rxIndexer.cpp rxSuiteHelpers.xo rxSuite.xo sjiboleth.so  indexIntercepts.xo client-pool.cpp tls.xo sdsWrapper.xo  rxSessionMemory.xo

rxQuery.so: ../../src/redismodule.h rxQuery.cpp rxQuery.hpp rule.hpp  rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.so  sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxQuery.so -shared -g -ggdb -fpermissive   $(SHOBJ_CFLAGS)   $(INCLUDE_PATHS)  -fPIC rxQuery.cpp rxSuiteHelpers.xo rxSuite.xo sjiboleth.so  rxSessionMemory.xo   sdsWrapper.xo

rxIndexStore.so: ../../src/redismodule.h rxIndexStore.cpp rxIndexStore.hpp command-multiplexer.hpp  rxFetch-multiplexer.hpp rxDescribe-multiplexer.hpp rxIndex.hpp rxSuiteHelpers.xo   sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxIndexStore.so -shared -g -ggdb -fpermissive   $(SHOBJ_CFLAGS)   $(INCLUDE_PATHS)  -fPIC rxIndexStore.cpp rxSuiteHelpers.xo   rxSuite.xo   sdsWrapper.xo  rxSessionMemory.xo

rxMercator.so: ../../src/redismodule.h rxMercator.cpp rxMercatorMonitor.cpp rxMercatorMonitor.hpp rxMercator.hpp  rxSuiteHelpers.xo   sjiboleth.so client-pool.hpp client-pool.cpp tls.xo sdsWrapper.xo  rxSessionMemory.xo
	g++ -o rxMercator.so  -shared -g -ggdb -fpermissive  $(SHOBJ_CFLAGS)   $(INCLUDE_PATHS) -Wvarargs -fPIC rxMercator.cpp rxMercatorMonitor.cpp rxSuiteHelpers.xo   sjiboleth.xo rxSuite.xo sjiboleth.so   client-pool.cpp tls.xo  sdsWrapper.xo  rxSessionMemory.xo
rxSuite.xo:       ../../src/server.h ../../src/dict.h rxSuite.h rxSuite.c sdsWrapper.xo

rxSuiteHelpers.xo:       ../../src/server.h rxSuiteHelpers.h rxSuiteHelpers.c sdsWrapper.xo  rxSessionMemory.xo

indexIntercepts.xo: indexIntercepts.c indexIntercepts.h  simpleQueue.hpp sdsWrapper.xo  rxSessionMemory.xo

%.o: %.cpp $(CPPSOURCES)
	$(CXX) -o $@ $(CXXFLAGS)   $(WARNNING) $(CFLAGS) $(SHOBJ_CFLAGS) -std=c++20 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -s  -DUSE_JEMALLOC $(INCLUDE_PATHS) -fPIC  -c $< 

%.xo: %.cpp $(CPPSOURCES)
	$(CXX) -o $@ $(CXXFLAGS)   $(WARNNING) $(CFLAGS) $(SHOBJ_CFLAGS) -std=c++20 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -s  -DUSE_JEMALLOC $(INCLUDE_PATHS) -fPIC  -c $< 


clean:
	rm -rf *.xo *.so
