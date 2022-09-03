#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/db4ai/executor/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/db4ai/executor/algorithms/bayes
top_builddir = ../../../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = bayes.o bayes_network_internal.o

include $(top_srcdir)/src/gausskernel/common.mk
