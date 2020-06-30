subdir = src/common/interfaces/ecpg/test/compat_informix
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/$(subdir)/../Makefile.regress

# special informix compatibility switches
ECPG += -C INFORMIX
ECPG_NOIND  = $(ECPG) -r no_indicator

override LDFLAGS := -L../../compatlib $(LDFLAGS)
override LIBS := -lecpg_compat $(LIBS)

TESTS = test_informix test_informix.cpp \
        test_informix2 test_informix2.cpp \
        dec_test dec_test.cpp \
        rfmtdate rfmtdate.cpp \
        rfmtlong rfmtlong.cpp \
        rnull rnull.cpp \
        sqlda sqlda.cpp \
        describe describe.cpp \
        charfuncs charfuncs.cpp

all: $(TESTS)

test_informix.cpp: test_informix.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

test_informix2.cpp: test_informix2.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

sqlda.cpp: sqlda.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

dec_test.cpp: dec_test.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

rfmtdate.cpp: rfmtdate.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

rfmtlong.cpp: rfmtlong.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<

rnull.cpp: rnull.pgc ../regression.h
	$(ECPG_NOIND) -o $@ -I$(srcdir) $<

charfuncs.cpp: charfuncs.pgc ../regression.h
	$(ECPG) -o $@ -I$(srcdir) $<
