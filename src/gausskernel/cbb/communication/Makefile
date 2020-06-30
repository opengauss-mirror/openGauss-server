
# ----------------------------------------------------------------------------
#
# Postgres-XC CM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
# src/gausskernel/cbb/communication/Makefile
#
# -----------------------------------------------------------------------------
subdir = src/gausskernel/cbb/communication
top_builddir=../../../..

include $(top_builddir)/src/Makefile.global

ifeq ($(enable_thread_check), yes)
  override CPPFLAGS :=  -I$(top_srcdir)/src/include/libcomm -I../ -I./ $(CPPFLAGS) -fno-non-call-exceptions 
else
  override CPPFLAGS :=  -I$(top_srcdir)/src/include/libcomm -I../ -I./ $(CPPFLAGS)
endif

SUBDIRS = sctp_core sctp_utils

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = libcomm.o

include $(top_srcdir)/src/gausskernel/common.mk

clean:
	rm -f $(OBJS)

distclean: clean

maintainer-clean: distclean
