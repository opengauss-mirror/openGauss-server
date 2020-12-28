#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for executor
#
# IDENTIFICATION
#    src/distribute/kernel/stream/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/process/stream
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = streamCore.o streamConsumer.o streamMain.o streamProducer.o streamTransportComm.o execStream.o stream_cost.o

override CPPFLAGS += -D__STDC_FORMAT_MACROS

include $(top_srcdir)/src/gausskernel/common.mk
