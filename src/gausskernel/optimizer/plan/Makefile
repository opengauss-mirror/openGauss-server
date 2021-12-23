#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/plan/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/plan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

ifeq ($(enable_multiple_nodes), yes)
OBJS = analyzejoins.o createplan.o initsplan.o planagg.o planmain.o planner.o \
	setrefs.o subselect.o planrewrite.o stream_remove.o streamwalker.o streamplan.o \
	streamplan_utils.o planstartwith.o
else
OBJS = analyzejoins.o createplan.o initsplan.o planagg.o planmain.o planner.o \
	setrefs.o subselect.o pgxcplan_single.o streamplan_single.o dynsmp_single.o \
	planrewrite.o planrecursive_single.o stream_remove.o streamwalker.o streamplan.o \
	streamplan_utils.o planstartwith.o
endif

include $(top_srcdir)/src/gausskernel/common.mk
