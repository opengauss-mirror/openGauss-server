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
	   utils.o hnswadaptor.o ivfadaptor.o bm25.o bm25build.o bm25scan.o bm25utils.o bm25vacuum.o ivfnpuadaptor.o \
	   pg_prng.o diskann.o diskannbuild.o diskannscan.o diskannutils.o diskanndelete.o

include $(top_srcdir)/src/gausskernel/common.mk
