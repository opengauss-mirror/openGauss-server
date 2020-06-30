subdir = src/common/interfaces/ecpg/test/sql
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/$(subdir)/../Makefile.regress

TESTS = array array.cpp \
	binary binary.cpp \
	code100 code100.cpp \
        copystdout copystdout.cpp \
	define define.cpp \
        desc desc.cpp \
        sqlda sqlda.cpp \
        describe describe.cpp \
        dyntest dyntest.cpp \
        dynalloc dynalloc.cpp \
        dynalloc2 dynalloc2.cpp \
        execute execute.cpp \
        fetch fetch.cpp \
        func func.cpp \
        indicators indicators.cpp \
	oldexec oldexec.cpp \
        parser parser.cpp \
        quote quote.cpp \
        show show.cpp \
        insupd insupd.cpp

all: $(TESTS)

oldexec.cpp:	oldexec.pgc ../regression.h
	$(ECPG) -r questionmarks -o $@ -I$(srcdir) $<
