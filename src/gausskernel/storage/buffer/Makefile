subdir = src/gausskernel/storage/buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = buf_table.o buf_init.o bufmgr.o freelist.o localbuf.o

include $(top_srcdir)/src/gausskernel/common.mk
