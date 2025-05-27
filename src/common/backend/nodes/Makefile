#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for common/backend/nodes
#
# IDENTIFICATION
#    src/common/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/nodes
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = nodeFuncs.o nodes.o list.o bitmapset.o tidbitmap.o \
       copyfuncs.o equalfuncs.o makefuncs.o \
       outfuncs.o readfuncs.o print.o read.o params.o value.o

include $(top_srcdir)/src/gausskernel/common.mk
