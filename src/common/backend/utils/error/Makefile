#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/error
#
# IDENTIFICATION
#    src/common/backend/utils/error/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/error
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = assert.o elog.o be_module.o plog.o fatal_err.o

elog.o: $(top_builddir)/src/bin/gsqlerr/errmsg.h

$(top_builddir)/src/bin/gsqlerr/errmsg.h:
	$(MAKE) -C $(top_builddir)/src/bin/gsqlerr 
	
include $(top_srcdir)/src/gausskernel/common.mk
