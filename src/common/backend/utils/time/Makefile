#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/time
#
# IDENTIFICATION
#    src/common/backend/utils/time/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/time
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = combocid.o snapmgr.o

include $(top_srcdir)/src/gausskernel/common.mk
