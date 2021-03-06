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

subdir = src/common/interfaces/libpq/client_logic_common
top_builddir = ../../../../../

# shared library parameters
NAME= client_logic_common

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE -DPGXC -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq -std=c++14 -I $(top_builddir)/src/include/libpq
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=statement_data.o client_logic_utils.o table_full_name.o cstrings_map.o cstring_oid_map.o

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib
test:
	g++ -std=c++14 -g main.cpp
all: all-static-lib

clean distclean: clean-lib
	rm -f *.o
maintainer-clean: distclean maintainer-clean-lib
