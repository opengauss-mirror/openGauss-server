#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/ddes/adapter/Makefile
#
# ---------------------------------------------------------------------------------------


subdir = src/gausskernel/ddes/adapter
top_builddir = ../../../..

include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_srcdir)/src/include/ddes/dms -DOPENGAUSS $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = ss_dms_bufmgr.o ss_dms_callback.o ss_dms_recovery.o ss_dms.o ss_init.o \
	ss_reform_common.o ss_switchover.o ss_transaction.o ss_aio.o ss_txnstatus.o \
  ss_xmin.o ss_dms_auxiliary.o

include $(top_srcdir)/src/gausskernel/common.mk

