#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for instruments/capture perf view
#
# IDENTIFICATION
#    src/gausskernel/cbb/instruments/capture_view/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/cbb/instruments/capture_view
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = capture_view.o

include $(top_srcdir)/src/gausskernel/common.mk
