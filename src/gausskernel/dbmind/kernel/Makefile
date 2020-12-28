#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/kernel/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/kernel
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = index_advisor.o hypopg_index.o

include $(top_srcdir)/src/gausskernel/common.mk
