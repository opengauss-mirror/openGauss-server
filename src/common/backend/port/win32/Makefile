#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for common/port/win32
#
# IDENTIFICATION
#    src/common/port/win32/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/port/win32
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = timer.o socket.o signal.o security.o mingwcompat.o
ifeq ($(have_win32_dbghelp), yes)
OBJS += crashdump.o
endif

include $(top_srcdir)/src/gausskernel/common.mk
