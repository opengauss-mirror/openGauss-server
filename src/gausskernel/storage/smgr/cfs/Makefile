subdir = src/gausskernel/storage/smgr/cfs
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = cfs_buffers.o cfs_md.o cfs_mddesc.o cfs_tools.o

include $(top_srcdir)/src/gausskernel/common.mk
