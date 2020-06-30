#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for the genetic query optimizer module
#
# Copyright (c) 1994, Regents of the University of California
#
# src/gausskernel/optimizer/geqo/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/geqo
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS =	geqo_copy.o geqo_eval.o geqo_main.o geqo_misc.o \
	geqo_mutation.o geqo_pool.o geqo_random.o geqo_recombination.o \
	geqo_selection.o \
	geqo_erx.o geqo_pmx.o geqo_cx.o geqo_px.o geqo_ox1.o geqo_ox2.o

include $(top_srcdir)/src/gausskernel/common.mk
