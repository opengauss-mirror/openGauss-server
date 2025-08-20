EXTENSION = shark
MODULE_big = shark
BASECODEDIR=src
BEPARSERDIR=src/backend_parser
PLDIR=src/pltsql

shark--2.0.sql: shark--1.0.sql upgrade_script/shark--1.0--2.0.sql
	cat $^ > $@

DATA_built = shark--2.0.sql
DATA = shark--2.0.sql upgrade_script/shark--1.0--2.0.sql rollback_script/shark--2.0--1.0.sql
REGRESS = dummy

OBJS = shark.o dbcc.o varlena.o sqlvariant.o datefuncs.o
OBJS += $(BEPARSERDIR)/parser.o
OBJS += $(BEPARSERDIR)/gram-backend.o
OBJS += $(BEPARSERDIR)/keywords.o
OBJS += $(BEPARSERDIR)/varbinary.o
OBJS += $(PLDIR)/pl_gram.o $(PLDIR)/pl_handler.o $(PLDIR)/pl_comp.o
OBJS += $(PLDIR)/pl_scanner.o
OBJS += $(PLDIR)/pl_funcs.o
OBJS += $(BASECODEDIR)/tablecmds.o $(BASECODEDIR)/identitycmds.o

subdir = contrib/shark
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
regress_home = $(top_builddir)/src/test/regress
p = 25632
REGRESS_OPTS = -c 0 -d 1 -r 1 -p $(p) --single_node -w --keep_last_data=false  \
               --schedule=./parallel_schedule                                  \
               --regconf=$(regress_home)/regress.conf                          \
               --temp-config=$(regress_home)/make_fastcheck_postgresql.conf    \
               --dbcmpt=D
include $(top_srcdir)/contrib/contrib-global.mk

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -I$(top_srcdir)/contrib/shark -fPIC

# scan is compiled as part of gram
$(BEPARSERDIR)/gram-backend.o: $(BEPARSERDIR)/scan-backend.inc

# Latest flex causes warnings in this file.
ifeq ($(GCC),yes)
$(BEPARSERDIR)/gram-backend.o: CXXFLAGS += -Wno-error
endif

$(BEPARSERDIR)/gram-backend.hpp: $(BEPARSERDIR)/gram-backend.cpp ;

$(BEPARSERDIR)/gram-backend.y: $(top_srcdir)/src/common/backend/parser/gram.y $(BEPARSERDIR)/gram-tsql-prologue.y.h $(BEPARSERDIR)/gram-tsql-decl.y $(BEPARSERDIR)/gram-tsql-rule.y $(BEPARSERDIR)/gram-tsql-epilogue.y.cpp $(BEPARSERDIR)/include.pl
	$(PERL) $(BEPARSERDIR)/include.pl $(BEPARSERDIR) gram.y < $< > $@

$(BEPARSERDIR)/gram-backend.cpp: $(BEPARSERDIR)/gram-backend.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' $(BEPARSERDIR)/gram-backend.cpp
	sed -i 's/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g' $(BEPARSERDIR)/gram-backend.cpp
else
	@$(missing) bison $< $@
	sed -i 's/YY_NULL nullptr/YY_NULL 0/g' $(BEPARSERDIR)/gram-backend.cpp
	sed -i 's/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g' $(BEPARSERDIR)/gram-backend.cpp
endif

$(BEPARSERDIR)/scan-backend.inc: $(BEPARSERDIR)/scan-backend.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
#	@if [ `wc -l <lex.backup` -eq 1 ]; then rm lex.backup; else echo "Scanner requires backup, see lex.backup."; exit 1; fi
else
	@$(missing) flex $< $@
endif

$(BEPARSERDIR)/scan-backend.l: $(top_srcdir)/src/common/backend/parser/scan.l $(BEPARSERDIR)/scan-tsql-prologue-top.l.h $(BEPARSERDIR)/scan-tsql-prologue.l.h $(BEPARSERDIR)/scan-tsql-decl.l $(BEPARSERDIR)/scan-tsql-rule.l $(BEPARSERDIR)/scan-tsql-epilogue.l.cpp
	$(PERL) $(BEPARSERDIR)/include.pl $(BEPARSERDIR) scan.l < $< > $@

# Force these dependencies to be known even without dependency info built:
$(BEPARSERDIR)/gram-backend.o $(BEPARSERDIR)/keywords.o $(BEPARSERDIR)/varbinary.o $(BEPARSERDIR)/parser.o: $(BEPARSERDIR)/gram-backend.hpp $(BEPARSERDIR)/kwlist_d.h

# where to find gen_keywordlist.pl and subsidiary files
TOOLSDIR = $(top_srcdir)/src/tools
GEN_KEYWORDLIST = $(PERL) -I $(TOOLSDIR) $(TOOLSDIR)/gen_keywordlist.pl
GEN_KEYWORDLIST_DEPS = $(TOOLSDIR)/gen_keywordlist.pl $(TOOLSDIR)/PerfectHash.pm

# generate SQL keyword lookup table to be included into keywords*.o.
$(BEPARSERDIR)/kwlist_d.h: $(BEPARSERDIR)/kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --extern -o $(BEPARSERDIR)/ $(BEPARSERDIR)/kwlist.h --varname pgtsql_ScanKeywords

$(PLDIR)/pl_gram.o $(PLDIR)/pl_handler.o $(PLDIR)/pl_comp.o $(PLDIR)/pl_funcs.o $(PLDIR)/pl_scanner.o: $(PLDIR)/pl_gram.hpp $(PLDIR)/plerrcodes.h
$(PLDIR)/pl_scanner.o: $(PLDIR)/pl_reserved_kwlist_d.h $(PLDIR)/pl_unreserved_kwlist_d.h

$(PLDIR)/pl_gram.hpp: $(PLDIR)/pl_gram.cpp ;

$(PLDIR)/pl_gram.cpp: $(PLDIR)/gram.y $(top_builddir)/src/mtlocal.pl
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
	$(PERL) $(top_builddir)/src/mtlocal.pl $(PLDIR)/pl_gram.cpp
	$(PERL) $(top_builddir)/src/mtlocal.pl $(PLDIR)/pl_gram.hpp
	sed -i "s/plpgsql_yyparse/pltsql_yyparse/g" $(PLDIR)/pl_gram.cpp
	sed -i "s/plpgsql_yyparse/pltsql_yyparse/g" $(PLDIR)/pl_gram.hpp
	sed -i "s/plpgsql_yylex/pltsql_yylex/g" $(PLDIR)/pl_gram.cpp
	sed -i "s/plpgsql_yylex/pltsql_yylex/g" $(PLDIR)/pl_gram.hpp
	sed -i "s/plpgsql_yylex_single/pltsql_yylex_single/g" $(PLDIR)/pl_gram.cpp
	sed -i 's/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g' $(PLDIR)/pl_gram.cpp
else
	@$(missing) bison $< $@
	sed -i 's/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g' $(PLDIR)/pl_gram.cpp
endif

$(PLDIR)/plerrcodes.h: $(top_srcdir)/src/common/backend/utils/errcodes.txt $(top_srcdir)/src/common/pl/plpgsql/src/generate-plerrcodes.pl
	$(PERL) $(top_srcdir)/src/common/pl/plpgsql/src/generate-plerrcodes.pl $< > $@

# generate keyword headers for the scanner
$(PLDIR)/pl_reserved_kwlist_d.h: $(PLDIR)/pl_reserved_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --varname ReservedPLKeywords $<

$(PLDIR)/pl_unreserved_kwlist_d.h: $(PLDIR)/pl_unreserved_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --varname UnreservedPLKeywords $<

all:
	chmod +x ./smartmatch.pl
