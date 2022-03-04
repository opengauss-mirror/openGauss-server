#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/executor/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/db4ai/catalog
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = aifuncs.o model_warehouse.o

include $(top_srcdir)/src/gausskernel/common.mk
