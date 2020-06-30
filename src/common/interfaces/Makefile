#-------------------------------------------------------------------------
#
# Makefile for src/common/interfaces
#
# Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = libpq ecpg

$(recurse)

all-ecpg-recurse: all-libpq-recurse
install-ecpg-recurse: install-libpq-recurse
