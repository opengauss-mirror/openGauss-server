#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/commands/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/db4ai/commands
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = create_model.o predict_by.o explain_model.o

include $(top_srcdir)/src/gausskernel/common.mk
