subdir = src/gausskernel/storage/access/ubtreepcr
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = ubtpcrinsert.o ubtpcrpage.o ubtpcrrecycle.o ubtpcrsearch.o ubtpcrsort.o ubtpcrsplitloc.o ubtpcrundo.o ubtpcrtd.o

include $(top_srcdir)/src/gausskernel/common.mk