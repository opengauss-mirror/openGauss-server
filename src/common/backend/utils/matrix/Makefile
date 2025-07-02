#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/matrix
#
# IDENTIFICATION
#    src/common/backend/utils/matrix/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/matrix
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = matrix_adaptor.o

include $(top_srcdir)/src/gausskernel/common.mk
