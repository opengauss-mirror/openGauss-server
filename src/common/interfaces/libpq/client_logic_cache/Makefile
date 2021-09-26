#-------------------------------------------------------------------------
#
# Makefile for src/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/client_logic_cache
top_builddir = ../../../../../

# shared library parameters
NAME= client_logic_cache

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=cache_loader.o cached_column.o cached_column_manager.o cached_columns.o types_to_oid.o proc_list.o cached_proc.o
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DPGXC -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq 
override CPPFLAGS += -I$(top_builddir)/src/common/interfaces/libpq/client_logic_common -I$(top_builddir)/src/include/libpq/
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all -DHAVE_CE
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all


all: all-static-lib

installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f *.o
	rm -f client_logic_cache.a

../client_logic_common/cstring_oid_map.o:
	$(MAKE) -C ../client_logic_common cstring_oid_map.o
types_to_oid.o: ../client_logic_common/cstring_oid_map.o

maintainer-clean: distclean maintainer-clean-lib
