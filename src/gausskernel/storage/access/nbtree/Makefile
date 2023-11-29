subdir = src/gausskernel/storage/access/nbtree
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = nbtcompare.o nbtdedup.o nbtinsert.o nbtpage.o nbtree.o nbtsearch.o \
       nbtutils.o nbtsort.o nbtxlog.o spq_btbuild.o

include $(top_srcdir)/src/gausskernel/common.mk
