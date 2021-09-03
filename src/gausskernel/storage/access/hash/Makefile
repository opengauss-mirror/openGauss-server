subdir = src/gausskernel/storage/access/hash
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = hash.o hashfunc.o hashinsert.o  hashovfl.o hashpage.o hashsearch.o\
       hashsort.o hashutil.o hash_xlog.o

include $(top_srcdir)/src/gausskernel/common.mk
