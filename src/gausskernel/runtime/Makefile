#
# Makefile for the runtime subsystem
#
# src/gausskernel/runtime/Makefile
#

subdir = src/gausskernel/runtime
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     =  executor  vecexecutor

ifeq ($(enable_llvm), yes)
SUBDIRS     += codegen
endif

include $(top_srcdir)/src/gausskernel/common.mk
