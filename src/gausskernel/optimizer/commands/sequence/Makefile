#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/commands/sequence/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/commands/sequence
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = sequence.o sequence_util.o

include $(top_srcdir)/src/gausskernel/common.mk
