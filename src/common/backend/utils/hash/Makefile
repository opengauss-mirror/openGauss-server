#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/hash
#
# IDENTIFICATION
#    src/common/backend/utils/hash/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/hash
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = dynahash.o hashfn.o

include $(top_srcdir)/src/gausskernel/common.mk
