#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for common/backend/regex
#
# IDENTIFICATION
#    src/common/backend/regex/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/regex
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = regcomp.o regerror.o regexec.o regfree.o regprefix.o

include $(top_srcdir)/src/gausskernel/common.mk

# mark inclusion dependencies between .cpp files explicitly
regcomp.o: regcomp.cpp regc_lex.cpp regc_color.cpp regc_nfa.cpp regc_cvec.cpp \
        regc_locale.cpp regc_pg_locale.cpp

regexec.o: regexec.cpp rege_dfa.cpp
