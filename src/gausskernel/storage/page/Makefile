subdir = src/gausskernel/storage/page
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS =  bufpage.o checksum.o itemptr.o pagecompress.o checksum_impl.o pageparse.o gs_xlogdump.o 

include $(top_srcdir)/src/gausskernel/common.mk
