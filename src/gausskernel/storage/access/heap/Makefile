subdir = src/gausskernel/storage/access/heap
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = heapam.o heapam_visibility.o hio.o pruneheap.o rewriteheap.o syncscan.o tuptoaster.o visibilitymap.o

include $(top_srcdir)/src/gausskernel/common.mk
