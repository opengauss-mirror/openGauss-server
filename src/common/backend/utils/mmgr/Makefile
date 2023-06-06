#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/mmgr
#
# IDENTIFICATION
#    src/common/backend/utils/mmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/mmgr
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = aset.o mcxt.o opt_aset.o opt_mcxt.o portalmem.o memprot.o asetstk.o asetalg.o memtrack.o AsanMemoryAllocator.o memgroup.o memtrace.o mem_snapshot.o

include $(top_srcdir)/src/gausskernel/common.mk
