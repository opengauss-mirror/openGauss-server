#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/prep/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/prep
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = prepjointree.o prepnonjointree.o prepqual.o preprownum.o preptlist.o prepunion.o

include $(top_srcdir)/src/gausskernel/common.mk
