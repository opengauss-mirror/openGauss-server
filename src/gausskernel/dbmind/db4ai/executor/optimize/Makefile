#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/db4ai/executor/Makefile
#
# ---------------------------------------------------------------------------------------


subdir = src/gausskernel/dbmind/db4ai/executor/optimize
top_builddir = ../../../../../..

include $(top_builddir)/src/Makefile.global

PLATFORM_ARCH = $(shell uname -p)
ifeq ($(PLATFORM_ARCH),x86_64)
        override CPPFLAGS += -mavx
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

SUBDIRS = gd

include $(top_srcdir)/src/gausskernel/common.mk
