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
	relfilenodemap.o

include $(top_srcdir)/src/gausskernel/common.mk
