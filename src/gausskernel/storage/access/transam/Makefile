subdir = src/gausskernel/storage/access/transam
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
ifeq ($(enable_multiple_nodes), yes)
OBJS = clog.o multixact.o rmgr.o slru.o csnlog.o transam.o twophase.o \
	twophase_rmgr.o varsup.o double_write.o single_double_write.o seg_double_write.o redo_statistic.o multi_redo_api.o multi_redo_settings.o\
	xact.o xlog.o xlogfuncs.o \
	xloginsert.o xlogreader.o xlogutils.o cbmparsexlog.o cbmfuncs.o
else
OBJS = clog.o gtm_single.o multixact.o rmgr.o slru.o csnlog.o transam.o twophase.o \
	twophase_rmgr.o varsup.o double_write.o single_double_write.o seg_double_write.o redo_statistic.o multi_redo_api.o multi_redo_settings.o\
	xact.o xlog.o xlogfuncs.o \
	xloginsert.o xlogreader.o xlogutils.o cbmparsexlog.o cbmfuncs.o
endif

SUBDIRS = parallel_recovery extreme_rto  
    
include $(top_srcdir)/src/gausskernel/common.mk

# ensure that version checks in xlog.c get recompiled when catversion.h changes
xlog.o: xlog.cpp $(top_srcdir)/src/include/catalog/catversion.h

libpg-fe.h:
	$(LN_S) $(top_builddir)/contrib/gtm/client/libpg-fe.h $(top_srcdir)/src/include/
