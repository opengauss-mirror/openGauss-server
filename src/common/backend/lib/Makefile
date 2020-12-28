#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for lib (miscellaneous stuff)
#
# IDENTIFICATION
#    src/common/backend/lib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/lib
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = binaryheap.o ilist.o dllist.o stringinfo.o bipartite_match.o hyperloglog.o circularqueue.o lrucache.o string.o

include $(top_srcdir)/src/gausskernel/common.mk
