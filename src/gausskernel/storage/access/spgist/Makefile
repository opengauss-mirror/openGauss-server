subdir = src/gausskernel/storage/access/spgist
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = spgutils.o spginsert.o spgscan.o spgvacuum.o \
	spgdoinsert.o spgxlog.o \
	spgtextproc.o spgquadtreeproc.o spgkdtreeproc.o

include $(top_srcdir)/src/gausskernel/common.mk
