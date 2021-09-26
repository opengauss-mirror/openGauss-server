subdir = src/gausskernel/storage/access/ustore/undo
top_builddir = ../../../../../../
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = knl_uundoapi.o knl_uundozone.o knl_uundospace.o knl_uundotxn.o knl_uundorecycle.o knl_uundoxlog.o

include $(top_srcdir)/src/gausskernel/common.mk

