#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/cache
#
# IDENTIFICATION
#    src/common/backend/utils/cache/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/cache
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = attoptcache.o catcache.o inval.o plancache.o relcache.o relmapper.o \
	spccache.o syscache.o lsyscache.o typcache.o ts_cache.o partcache.o		\
	relfilenodemap.o \
	knl_globalsysdbcache.o \
	knl_localsysdbcache.o \
	knl_globalsystabcache.o knl_globalsystupcache.o knl_globalbasedefcache.o knl_globaltabdefcache.o knl_globalpartdefcache.o \
	knl_localsystabcache.o knl_localsystupcache.o knl_localbasedefcache.o knl_localtabdefcache.o knl_localpartdefcache.o \
	knl_globalrelmapcache.o knl_globalbucketlist.o knl_globaldbstatmanager.o

include $(top_srcdir)/src/gausskernel/common.mk
