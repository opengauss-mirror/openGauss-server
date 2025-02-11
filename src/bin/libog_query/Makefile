root_dir := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
word-dot = $(word $2,$(subst ., ,$1))
top_builddir = ../../../..

TARGET = og_query
ARLIB = lib$(TARGET).a

VERSION = 7.0.0
VERSION_MAJOR = $(call word-dot,$(VERSION),1)
VERSION_MINOR = $(call word-dot,$(VERSION),2)
VERSION_PATCH = $(call word-dot,$(VERSION),3)

SO_VERSION = $(shell printf '%02d%02d' $(VERSION_MAJOR) $(VERSION_MAJOR)).$(VERSION_MINOR)

ifeq ($(shell uname -s), Darwin)
	SOLIB = lib$(TARGET).dylib
	SONAME = lib$(TARGET).$(SO_VERSION).dylib
	SOLIBVER = lib$(TARGET).$(SO_VERSION).$(VERSION_PATCH).dylib
	SOFLAG = -install_name
else
	SOLIB = lib$(TARGET).so
	SONAME = $(SOLIB).$(SO_VERSION)
	SOLIBVER = $(SONAME).$(VERSION_PATCH)
	SOFLAG = -soname
endif

SRC_FILES := $(wildcard src*.cpp) $(wildcard contrib*.cpp)
OBJ_FILES := $(SRC_FILES:.cpp=.o)

override CFLAGS += -g3 -gdwarf-2 -I. -Iinclude -Iinclude/plugin_parser -I$(top_builddir)/src/include/knl -Wall -Wno-unused-function -Wno-unused-value -Wno-unused-variable -fno-strict-aliasing -fwrapv -fPIC -DPGXC -DENABLE_MOT -DENABLE_HTAP -fno-exceptions -Wwrite-strings -L$(GAUSSHOME)/lib -lstdc++ -lcjson -lsecurec -DUSE_ASSERT_CHECKING -DMEMORY_CONTEXT_CHECKING
override CXXFLAGS += -g3 -gdwarf-2 -I. -Iinclude -Iinclude/plugin_parser -I$(top_builddir)/src/include/knl -Wall -Wno-unused-function -Wno-unused-value -Wno-unused-variable -fno-strict-aliasing -fwrapv -fPIC -DPGXC -DENABLE_MOT -DENABLE_HTAP -fno-exceptions -Wwrite-strings -L$(GAUSSHOME)/lib -lstdc++ -lcjson -lsecurec -DUSE_ASSERT_CHECKING -DMEMORY_CONTEXT_CHECKING

override PG_CONFIGURE_FLAGS += -q --without-readline --without-zlib --without-icu

override TEST_CFLAGS += -g -I. -I./vendor -Wall
override TEST_LDFLAGS += -pthread

CFLAGS_OPT_LEVEL = -O3
ifeq ($(DEBUG),1)
	CFLAGS_OPT_LEVEL = -O0
endif
override CFLAGS += $(CFLAGS_OPT_LEVEL)

ifeq ($(DEBUG),1)
	# We always add -g, so this only has to enable assertion checking
	override CFLAGS += -D USE_ASSERT_CHECKING
endif

CLEANLIBS = $(ARLIB) $(SOLIB)
CLEANOBJS = $(OBJ_FILES)

AR ?= ar
AR := $(AR) rs
INSTALL = install
LN_S = ln -s
RM = rm -f
ECHO = echo

CC ?= cc

all: test build_shared

build: $(ARLIB)

build_shared: $(SOLIB)

clean:
	-@ $(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES) $(TESTS)
	-@ $(RM) -rf {test}/*.dSYM

.PHONY: all clean build build_shared

$(ARLIB): $(OBJ_FILES) Makefile
	@$(AR) $@ $(OBJ_FILES)

$(SOLIB): $(OBJ_FILES) Makefile
	@$(CC) $(CFLAGS) -shared -Wl,$(SOFLAG),$(SONAME) $(LDFLAGS) -o $@ $(OBJ_FILES) $(LIBS)

