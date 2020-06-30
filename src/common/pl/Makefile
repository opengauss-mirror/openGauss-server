#-------------------------------------------------------------------------
#
# Makefile for src/common/pl (procedural languages)
#
# Copyright (c) 1994, Regents of the University of California
#
# src/common/pl/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/pl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = plpgsql

ifeq ($(with_python), yes)
SUBDIRS += plpython
else
ALWAYS_SUBDIRS += plpython
endif

$(recurse)
$(recurse_always)
