#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for port/darwin
#
# IDENTIFICATION
#    src/common/port/darwin/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/port/darwin
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = system.o

include $(top_srcdir)/src/gausskernel/common.mk
