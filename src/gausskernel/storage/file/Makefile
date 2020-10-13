subdir = src/gausskernel/storage/file
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = fd.o buffile.o copydir.o reinit.o lz4_file.o sharedfileset.o

include $(top_srcdir)/src/gausskernel/common.mk
