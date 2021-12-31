#-------------------------------------------------------------------------
#
# Makefile for parser
#
# src/common/backend/parser/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/parser
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= analyze.o gram.o hint_gram.o keywords.o kwlookup.o parser.o \
      parse_agg.o parse_clause.o parse_startwith.o parse_coerce.o parse_collate.o parse_cte.o \
      parse_expr.o parse_func.o parse_hint.o parse_merge.o parse_node.o parse_oper.o parse_param.o \
      parse_relation.o parse_target.o parse_type.o parse_utilcmd.o scansup.o parse_compatibility.o

FLEXFLAGS = -CF -b -p -p

include $(top_srcdir)/src/gausskernel/common.mk

# scan is compiled as part of gram
gram.o: scan.inc

# Latest flex causes warnings in this file.
ifeq ($(GCC),yes)
gram.o: CXXFLAGS += -Wno-error
endif

# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  To be safe for parallel
# make, we must chain the dependencies like this.  The semicolon is
# important, otherwise make will choose the built-in rule for
# gram.y=>gram.cpp.

gram.hpp: gram.cpp ;

gram.cpp: gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' gram.cpp
else
	@$(missing) bison $< $@
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' gram.cpp
endif

scan.inc: scan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
#	@if [ `wc -l <lex.backup` -eq 1 ]; then rm lex.backup; else echo "Scanner requires backup, see lex.backup."; exit 1; fi
else
	@$(missing) flex $< $@
endif

# Force these dependencies to be known even without dependency info built:
gram.o keywords.o parser.o: gram.hpp

hint_gram.o: hint_scan.inc
ifeq ($(GCC),yes)
hint_gram.o: CXXFLAGS += -Wno-error
endif

hint_gram.hpp: hint_gram.cpp ;

hint_gram.cpp: hint_gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' hint_gram.cpp
else
	@$(missing) bison $< $@
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' hint_gram.cpp
endif

hint_scan.inc: hint_scan.l | scan.inc
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	@if [ `wc -l <lex.backup` -eq 1 ]; then rm lex.backup; else echo "Hint Scanner requires backup, see lex.backup."; exit 1; fi
else
	@$(missing) flex $< $@
endif

hint_gram.o: hint_gram.hpp

# gram.cpp, gram.hpp, and scan.inc are in the distribution tarball, so they
# are not cleaned here.
clean distclean maintainer-clean:
	rm -f lex.backup


maintainer-check:
	$(PERL) $(top_srcdir)/src/tools/check_keywords.pl $(top_srcdir)
