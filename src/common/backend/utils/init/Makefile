#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/init
#
# IDENTIFICATION
#    src/common/backend/utils/init/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/init
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = globals.o miscinit.o postinit.o

include $(top_srcdir)/src/gausskernel/common.mk
