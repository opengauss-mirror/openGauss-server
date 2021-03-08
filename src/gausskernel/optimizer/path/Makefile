#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/path/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/path
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
$(info $(CPPFLAGS))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

ifeq ($(enable_multiple_nodes), yes)
OBJS = allpaths.o clausesel.o costsize.o equivclass.o indxpath.o \
       joinpath.o joinrels.o orindxpath.o pathkeys.o tidpath.o streampath_base.o \
       es_selectivity.o
else
OBJS = allpaths.o clausesel.o costsize.o equivclass.o indxpath.o \
       joinpath.o joinrels.o orindxpath.o pathkeys.o tidpath.o	\
       pgxcpath_single.o streampath_single.o streampath_base.o es_selectivity.o
endif

include $(top_srcdir)/src/gausskernel/common.mk
