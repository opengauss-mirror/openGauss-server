subdir = src/gausskernel/storage/smgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = md.o smgr.o smgrtype.o knl_uundofile.o segstore.o page_compression.o mmap_shared.o

SUBDIRS = segment

include $(top_srcdir)/src/gausskernel/common.mk
