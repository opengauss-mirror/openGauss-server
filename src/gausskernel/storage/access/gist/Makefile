subdir = src/gausskernel/storage/access/gist
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = gist.o gistutil.o gistxlog.o gistvacuum.o gistget.o gistscan.o \
       gistproc.o gistsplit.o gistbuild.o gistbuildbuffers.o

include $(top_srcdir)/src/gausskernel/common.mk
