#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for locator and data distribution
#
# Copyright(C) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/common/backend/pgxc_single/locator
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = locator.o redistrib.o

include $(top_srcdir)/src/gausskernel/common.mk
