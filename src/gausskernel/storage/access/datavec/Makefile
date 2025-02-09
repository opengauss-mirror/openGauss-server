subdir = src/gausskernel/storage/access/datavec
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

OBJS = bitutils.o hnsw.o hnswbuild.o hnswdelete.o hnswinsert.o hnswscan.o hnswutils.o hnswvacuum.o \
       ivfbuild.o ivfflat.o ivfinsert.o ivfkmeans.o ivfscan.o ivfutils.o ivfvacuum.o vecindex.o \
	   utils.o hnswadaptor.o ivfadaptor.o

include $(top_srcdir)/src/gausskernel/common.mk
