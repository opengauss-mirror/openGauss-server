subdir = src/gausskernel/storage/access/common
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = heaptuple.o indextuple.o printtup.o reloptions.o scankey.o \
	tupconvert.o tupdesc.o cstorescankey.o

include $(top_srcdir)/src/gausskernel/common.mk
