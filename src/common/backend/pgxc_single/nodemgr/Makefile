#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for node management routines
#
#  Portions Copyright (c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/common/backend/pgxc_single/nodemgr
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
override CPPFLAGS := -I$(srcdir) $(CPPFLAGS) -I$(libpq_srcdir)

OBJS = nodemgr.o groupmgr.o

include $(top_srcdir)/src/gausskernel/common.mk
