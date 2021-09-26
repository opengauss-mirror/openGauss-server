subdir = src/gausskernel/storage/smgr/segment
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = data_file.o extent_group.o segbuffer.o space.o segxlog.o inverse_ptr.o xlog_atomic_op.o

include $(top_srcdir)/src/gausskernel/common.mk
