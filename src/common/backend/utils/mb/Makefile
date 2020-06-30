#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/mb
#
# IDENTIFICATION
#    src/common/backend/utils/mb/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/mb
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = encnames.o conv.o mbutils.o wchar.o wstrcmp.o wstrncmp.o

include $(top_srcdir)/src/gausskernel/common.mk

clean distclean maintainer-clean:
	$(MAKE) -C conversion_procs $@
