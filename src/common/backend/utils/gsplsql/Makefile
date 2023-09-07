#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/hash
#
# IDENTIFICATION
#    src/common/backend/utils/gsplsql/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/gsplsql
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = gsdependencies.o gsobject_dependencies.o

include $(top_srcdir)/src/gausskernel/common.mk
