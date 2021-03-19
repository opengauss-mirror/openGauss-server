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

subdir = src/common/interfaces/libpq/parser_frontend
top_builddir = ../../../../../

# shared library parameters
NAME= client_logic_processor

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DPGXC -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq -I$(top_builddir)/src/include/libpq 
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all -DHAVE_CE
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=expr_processor.o pg_functions_support.o

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

all: all-static-lib

clean distclean: clean-lib
	rm -f *.o

maintainer-clean: distclean maintainer-clean-lib
