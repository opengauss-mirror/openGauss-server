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

subdir = src/common/interfaces/libpq/client_logic_fmt
top_builddir = ../../../../../

# shared library parameters
NAME=client_logic_fmt

override CPPFLAGS := -DFRONTEND -DPGXC -fstack-protector-all -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq  
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -shared

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib



all:  all-static-lib

installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f *.o
	rm -f *.a

maintainer-clean: distclean maintainer-clean-lib
