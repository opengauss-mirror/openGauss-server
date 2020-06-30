#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pool
#
#  Portions Copyright (c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/common/backend/pgxc_single/pool
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = pgxcnode.o execRemote.o poolmgr.o poolcomm.o poolutils.o

include $(top_srcdir)/src/gausskernel/common.mk
