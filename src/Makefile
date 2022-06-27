
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
DEPENDENCY_TARGETS=hiredis linenoise lua
CPPSOURCES := $(wildcard*.cpp)



# Compile flags for linux / osx
CFLAGS ?= -std=c99
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -O2 
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -O2 
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

# if debug build is disabled, enable lto

CXXFLAGS ?=  -std=c++17  
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
SHOBJ_CFLAGS+= -I../../deps/hiredis -I../../deps/linenoise -I../../deps/lua/src -I/usr/include
# Include REDIS Src
SHOBJ_CFLAGS+= -I ../../src

.SUFFIXES: .c .so .xo .o .cpp

DEBUG += -g -ggdb

all:  sentenceProcessor.xo indexIntercepts.xo rxSuite.xo  rxSuiteHelpers.xo parser.xo token.xo queryEngine.xo rxIndexer.so rxQuery.so rxFetch.so rxRule.xo  rxRule.so sjiboleth.so rxGraphdb.so 

.c.xo: 
	$(CC) -I.. -I. $(CFLAGS) $(SHOBJ_CFLAGS)  -isystem -DUSE_JEMALLOC  -I../deps/hiredis -I../deps/linenoise -I../../deps/lua/src -I/usr/include -fPIC -c $< -o $@

OBSOLETEFLAGS=-std=c99 
FILT = $(filter-out $(OBSOLETEFLAGS), $(CFLAGS))

.cpp.xo:
	c++ -I.. -I. $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)   -isystem -DUSE_JEMALLOC  -Werror=main -std=c++17 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -isystem -DUSE_JEMALLOC  -I../../deps/hiredis -I../../deps/linenoise -I../../deps/lua/src -I/usr/include -fPIC  $< -o $@

rxGraphdb.so: ../../src/redismodule.h graphdb.cpp graphstack.hpp   rxSuite.xo  rxSuiteHelpers.xo  queryEngine.xo parser.xo token.xo
	g++  $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS) -g -ggdb -o rxGraphdb.so -fPIC  graphdb.cpp  rxSuite.xo  rxSuiteHelpers.xo  queryEngine.xo  parser.xo token.xo 

graphdb.xo: ../../src/redismodule.h graphdb.cpp graphdb.h queryEngine.c queryEngine.h  parser.c parser.h token.c token.h queryEngine.c queryEngine.h  sentenceProcessor.c sentenceProcessor.h  graphstack.hpp

sjiboleth.so: ../../src/redismodule.h sjiboleth.cpp sjiboleth.hpp   rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.cpp sjiboleth-silnikparowy.cpp  sjiboleth-silnikparowy-ctx.cpp sjiboleth-generic.cpp sjiboleth-gremlin.cpp sjiboleth-json.cpp sjiboleth-query.cpp sjiboleth-text.cpp  sjiboleth-fablok.cpp rxSuite.xo  rxSuiteHelpers.xo sjiboleth.cpp         sjiboleth-generic.cpp  sjiboleth-parser.cpp  sjiboleth-text.cpp  sjiboleth-gremlin.cpp  sjiboleth-query.cpp   sjibolethWrapper.cpp sjiboleth-fablok.cpp  sjiboleth.xo  sjiboleth-json.cpp sjiboleth.hpp graphstack.hpp makefile client-pool.cpp
	g++  $(CXXFLAGS) -shared  $(WARNNING) $(FILT) $(SHOBJ_CFLAGS)  -g -ggdb  -isystem -DUSE_JEMALLOC -Werror=main -std=c++17 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -isystem -DUSE_JEMALLOC  -I../deps/hiredis -I../deps/linenoise -I../../deps/lua/src -I/usr/include -fPIC  -o sjiboleth.so -fPIC  sjiboleth.cpp  sjiboleth-fablok.cpp sjiboleth-generic.cpp  sjiboleth-gremlin.cpp  sjiboleth-json.cpp  sjiboleth-parser.cpp  sjiboleth-query.cpp sjiboleth-text.cpp  sjibolethWrapper.cpp   rxSuiteHelpers.xo client-pool.cpp 
	cp sjiboleth.so ../..


makefile:
	echo "build complete"

rxRule.so: ../../src/redismodule.h rxRule.cpp rule.hpp    rxSuite.xo  rxSuiteHelpers.xo   sjiboleth.so  
	g++ -shared -g -ggdb -o rxRule.so  $(SHOBJ_CFLAGS)  -fPIC rxRule.cpp rxSuiteHelpers.xo rxSuite.xo  sjiboleth.so  

rxIndexer.so: ../../src/redismodule.h rxIndexer.cpp rxIndexer.hpp rule.hpp  rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.so indexIntercepts.xo simpleQueue.hpp
	g++ -shared -g -ggdb -o rxIndexer.so  $(SHOBJ_CFLAGS) -fPIC rxIndexer.cpp rxSuiteHelpers.xo rxSuite.xo sjiboleth.so  indexIntercepts.xo

rxQuery.so: ../../src/redismodule.h rxQuery.cpp rxQuery.hpp rule.hpp  rxSuite.xo  rxSuiteHelpers.xo  sjiboleth.so 
	g++ -shared -g -ggdb -o rxQuery.so  $(SHOBJ_CFLAGS)  -fPIC rxQuery.cpp rxSuiteHelpers.xo rxSuite.xo sjiboleth.so  

rxFetch.so: ../../src/redismodule.h rxFetch.cpp rxFetch.hpp command-duplexer.hpp  rxFetch-duplexer.hpp  rxIndex.hpp rxSuiteHelpers.xo  
	g++ -shared -g -ggdb -o rxFetch.so  $(SHOBJ_CFLAGS)  -fPIC rxFetch.cpp rxSuiteHelpers.xo   rxSuite.xo  

queryEngine.xo:       ../../src/redismodule.h  queryEngine.c       queryEngine.h       parser.c  token.c parser.h token.h 

rxSuite.xo:       ../../src/server.h ../../src/dict.h rxSuite.h rxSuite.c

rxSuiteHelpers.xo:       ../../src/server.h rxSuiteHelpers.h rxSuiteHelpers.c

indexIntercepts.xo: indexIntercepts.c indexIntercepts.h  simpleQueue.hpp

%.o: %.cpp $(CPPSOURCES)
	# $(CXX)                             -std=c++17 -faligned-new -Wno-write-strings   $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -s -isystem -DUSE_JEMALLOC -I..  -I../deps/hiredis -I../deps/linenoise -I../../deps/lua/src -I/usr/include -fPIC -c
	$(CXX) -I. -I.. $(CXXFLAGS)   $(WARNNING) $(CFLAGS) $(SHOBJ_CFLAGS) -std=c++17 -faligned-new -Wno-write-strings  $(OPT) $(DEBUG) -Wall -Wno-deprecated-declarations -s -isystem -DUSE_JEMALLOC -I.. -I../deps/hiredis -I../deps/linenoise -I../../deps/lua/src -I/usr/include -fPIC  -c $< -o $@

# queryEngine.so:
# 	 $(LD) -o $@ $< rxSuite.xo  rxSuiteHelpers.xo parser.xo  token.xo sentenceProcessor.xo queryEngine.xo indexIntercepts.xo simpleQueue.xo $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so
