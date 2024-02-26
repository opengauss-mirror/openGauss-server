#
# Makefile for the runtime subsystem
#
# src/gausskernel/runtime/Makefile
#

subdir = src/gausskernel/runtime
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     =  executor  opfusion  vecexecutor

ifneq ($(enable_lite_mode), yes)
ifeq ($(enable_llvm), yes)
   SUBDIRS += codegen 
endif
endif

include $(top_srcdir)/src/gausskernel/common.mk
