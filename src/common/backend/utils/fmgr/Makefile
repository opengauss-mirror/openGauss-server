#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/fmgr
#
# IDENTIFICATION
#    src/common/backend/utils/fmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/fmgr
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = dfmgr.o fmgr.o funcapi.o

override CPPFLAGS += -DDLSUFFIX=\"$(DLSUFFIX)\" -I${top_builddir}/contrib/log_fdw/
override CPPFLAGS += -DDLSUFFIX=\"$(DLSUFFIX)\" -I${top_builddir}/contrib/gc_fdw/

include $(top_srcdir)/src/gausskernel/common.mk
