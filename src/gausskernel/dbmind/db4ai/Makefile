#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/db4ai/Makefile
#
# ---------------------------------------------------------------------------------------


subdir = src/gausskernel/dbmind/db4ai
top_builddir = ../../../..

include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

SUBDIRS = catalog commands executor

include $(top_srcdir)/src/gausskernel/common.mk
