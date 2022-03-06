#
# Makefile for the pgxc remote query
#
# src/common/backend/pgxc/Makefile
#

subdir = src/common/backend/pgxc_single
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS     = barrier
else
SUBDIRS     = barrier locator copy nodemgr pool
endif

include $(top_srcdir)/src/gausskernel/common.mk
