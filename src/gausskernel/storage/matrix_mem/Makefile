subdir = src/gausskernel/storage/matrix_mem
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = matrix_mem.o

include $(top_srcdir)/src/gausskernel/common.mk