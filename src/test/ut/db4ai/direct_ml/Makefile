#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/test/db4ai
#
# ---------------------------------------------------------------------------------------


subdir = src/test/db4ai
top_builddir = ../../..

include $(top_builddir)/src/Makefile.global

PLATFORM_ARCH = $(shell uname -p)
ifeq ($(PLATFORM_ARCH),x86_64)
        override CPPFLAGS += -mavx
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = main.o kernels.o pg_mock.o readers.o db4ai_gd_direct.o db4ai_kmeans_direct.o db4ai_xgboost_direct.o

include $(top_srcdir)/src/gausskernel/common.mk



