#-------------------------------------------------------------------------
#
# Makefile for src/common/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/client_logic_hooks
top_builddir = ../../../../../

# shared library parameters
NAME=client_logic_hooks
encryption_hooks_dir = "$(top_builddir)/src/common/interfaces/libpq/client_logic_hooks/encryption_hooks"

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DPGXC -fstack-protector-all -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include  
override CPPFLAGS += -I$(top_builddir)/src/common/interfaces/libpq/ -I$(top_builddir)/src/include/libpq/ -I$(top_builddir)/src/include/libpq/client_logic_cache
override CPPFLAGS += -I$(top_builddir)/src/common/interfaces/libpq/client_logic_hooks -I$(encryption_hooks_dir)
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    override CPPFLAGS += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -l$(SECURE_C_CHECK) -L$(LIBKMC_LIB_PATH) -lkmc
  endif
  override CPPFLAGS += -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH) -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH) -lcjson -lcurl
endif
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -shared

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=column_hook_executor.o hooks_manager.o global_hook_executor.o hooks_factory.o abstract_hook_executor.o

OBJS += $(hooks_files:.cpp=.o)
encryption_hooks_files := $(shell find $(encryption_hooks_dir) -name '*.cpp' | sort)
OBJS += $(encryption_hooks_files:.cpp=.o)

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

override CPPFLAGS := -I$(ZLIB_INCLUDE_PATH) $(CPPFLAGS)

all:  all-static-lib

installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f ${OBJS}
	rm -f *.a

maintainer-clean: distclean maintainer-clean-lib
