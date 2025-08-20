subdir = src/gausskernel/storage/access/bloom
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

OBJS = blcost.o blinsert.o blscan.o blutils.o blvacuum.o

include $(top_srcdir)/src/gausskernel/common.mk
