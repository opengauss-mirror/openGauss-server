#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/optimizer/util/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/util
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

SUBDIRS	= learn

OBJS = clauses.o joininfo.o pathnode.o placeholder.o plancat.o predtest.o \
       relnode.o restrictinfo.o tlist.o var.o pruningboundary.o pgxcship.o pruning.o randomplan.o optimizerdebug.o planmem_walker.o \
       nodegroups.o plananalyzer.o optcommon.o dataskew.o joinskewinfo.o autoanalyzer.o bucketinfo.o bucketpruning.o subpartitionpruning.o

include $(top_srcdir)/src/gausskernel/common.mk
