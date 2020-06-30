#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/rewrite/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/rewrite
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = rewriteRemove.o rewriteDefine.o rewriteHandler.o \
       rewriteManip.o rewriteSupport.o rewriteRlsPolicy.o

include $(top_srcdir)/src/gausskernel/common.mk
