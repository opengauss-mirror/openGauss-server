#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/resowner
#
# IDENTIFICATION
#    src/common/backend/utils/resowner/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/resowner
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = resowner.o

include $(top_srcdir)/src/gausskernel/common.mk
