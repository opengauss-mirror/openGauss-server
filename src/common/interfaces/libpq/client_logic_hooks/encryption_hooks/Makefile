#-------------------------------------------------------------------------
#
# Makefile for src/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/client_logic_hooks/encryption_hooks/
top_builddir = ../../../../../../

# shared library parameters
NAME=client_logic_encryption_hooks

override CPPFLAGS := -DFRONTEND -DPGXC -Wno-write-strings -fstack-protector-all -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/include/libpq -I$(top_builddir)/src/common/interfaces/libpq -I$(top_builddir)/src/common/interfaces/libpq/client_logic_hooks/ -I.
override CPPFLAGS += -l$(SECURE_C_CHECK) -lssl -lcrypto
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    override CPPFLAGS += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -l$(SECURE_C_CHECK) -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -shared

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
files := $(shell find . -name '*.cpp' | sort)
OBJS = $(files:.cpp=.o)

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

override CPPFLAGS := -I$(ZLIB_INCLUDE_PATH) $(CPPFLAGS)

all: all-static-lib

installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f *.o
	rm -f *.a

maintainer-clean: distclean maintainer-clean-lib
