#-------------------------------------------------------------------------
#
# Makefile for src/bin/scripts
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/scripts/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "PostgreSQL utility"
PGAPPICON=win32

subdir = src/bin/scripts
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PROGRAMS = createdb createlang createuser dropdb droplang dropuser clusterdb vacuumdb reindexdb

override CPPFLAGS := -I$(top_srcdir)/src/bin/pg_dump -I$(top_srcdir)/src/bin/psql -I$(libpq_srcdir) $(CPPFLAGS)

CFLAGS += -Wl,-z,relro,-z,now
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
all: $(PROGRAMS)

%: %.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

createdb: createdb.o common.o dumputils.o dumpmem.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
createlang: createlang.o common.o dumputils.o dumpmem.o kwlookup.o keywords.o print.o mbprint.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
createuser: createuser.o common.o dumputils.o dumpmem.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
dropdb: dropdb.o common.o dumpmem.o dumputils.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
droplang: droplang.o common.o dumpmem.o dumputils.o print.o kwlookup.o keywords.o mbprint.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
dropuser: dropuser.o common.o dumpmem.o dumputils.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
clusterdb: clusterdb.o common.o dumpmem.o dumputils.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq
vacuumdb: vacuumdb.o common.o | submake-libpq
reindexdb: reindexdb.o common.o dumpmem.o dumputils.o kwlookup.o keywords.o $(top_builddir)/src/lib/elog/elog.a | submake-libpq

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a

dumpmem.cpp dumputils.cpp keywords.cpp: % : $(top_srcdir)/src/bin/pg_dump/%
	rm -f $@ && $(LN_S) $< .

print.cpp mbprint.cpp : % : $(top_srcdir)/src/bin/psql/%
	rm -f $@ && $(LN_S) $< .

kwlookup.cpp: % : $(top_srcdir)/src/common/backend/parser/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) createdb$(X)   '$(DESTDIR)$(bindir)'/createdb$(X)
	$(INSTALL_PROGRAM) dropdb$(X)     '$(DESTDIR)$(bindir)'/dropdb$(X)
	$(INSTALL_PROGRAM) createlang$(X) '$(DESTDIR)$(bindir)'/createlang$(X)
	$(INSTALL_PROGRAM) droplang$(X)   '$(DESTDIR)$(bindir)'/droplang$(X)
	$(INSTALL_PROGRAM) createuser$(X) '$(DESTDIR)$(bindir)'/createuser$(X)
	$(INSTALL_PROGRAM) dropuser$(X)   '$(DESTDIR)$(bindir)'/dropuser$(X)
	$(INSTALL_PROGRAM) clusterdb$(X)  '$(DESTDIR)$(bindir)'/clusterdb$(X)
	$(INSTALL_PROGRAM) vacuumdb$(X)   '$(DESTDIR)$(bindir)'/vacuumdb$(X)
	$(INSTALL_PROGRAM) reindexdb$(X)  '$(DESTDIR)$(bindir)'/reindexdb$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, $(addsuffix $(X), $(PROGRAMS)))


clean distclean maintainer-clean:
	rm -f $(addsuffix $(X), $(PROGRAMS)) $(addsuffix .o, $(PROGRAMS)) *.depend
	rm -f common.o dumpme.o dumputils.o kwlookup.o keywords.o print.o mbprint.o $(WIN32RES)
	rm -f dumpmem.o dumputils.cpp print.cpp mbprint.cpp kwlookup.cpp keywords.cpp
