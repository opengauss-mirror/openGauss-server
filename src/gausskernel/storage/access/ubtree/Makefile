subdir = src/gausskernel/storage/access/ubtree
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = ubtree.o ubtinsert.o ubtpage.o ubtsort.o ubtutils.o ubtsearch.o \
       ubtsplitloc.o ubtsplitloc_insertpt.o ubtxlog.o ubtdump.o ubtrecycle.o

include $(top_srcdir)/src/gausskernel/common.mk
