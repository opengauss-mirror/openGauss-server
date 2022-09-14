#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/db4ai/executor/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/db4ai/executor/optimize/gd
top_builddir = ../../../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = gd.o shuffle_cache.o predict.o \
      optimizer_gd.o optimizer_ngd.o optimizer_pca.o optimizer_ova.o

include $(top_srcdir)/src/gausskernel/common.mk
