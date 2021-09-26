#-------------------------------------------------------------------------
#
# Makefile for src/bin/initdb
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/initdb/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "gs_initdb - initialize a new database cluster"
PGAPPICON=win32

subdir = src/bin/initdb
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) -I$(top_srcdir)/src/common/timezone -fPIE $(CPPFLAGS)

# use system timezone data?
ifneq (,$(with_system_tzdata))
override CPPFLAGS += '-DSYSTEMTZDIR="$(with_system_tzdata)"'
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS=	initdb.o findtimezone.o localtime.o encnames.o pqsignal.o $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a
all: gs_initdb

gs_initdb: $(OBJS) | submake-libpgport
	$(CC) -fPIE -pie $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
utinitdb: $(OBJS) | submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o libutinitdb.so
	mv libutinitdb.so $(top_builddir)/../distribute/test/ut/lib

# We used to pull in all of libpq to get encnames and pqsignal, but that
# exposes us to risks of version skew if we link to a shared library.
# Do it the hard way, instead, so that we're statically linked.

encnames.cpp: % : $(top_srcdir)/src/common/backend/utils/mb/%
	rm -f $@ && $(LN_S) $< .

pqsignal.cpp: % : $(top_srcdir)/src/common/interfaces/libpq/%
	rm -f $@ && $(LN_S) $< .

# Likewise, pull in localtime.cpp from src/timezones

localtime.cpp: % : $(top_srcdir)/src/common/timezone/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) gs_initdb$(X) '$(DESTDIR)$(bindir)/gs_initdb$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_initdb$(X)'

clean distclean maintainer-clean:
	rm -f gs_initdb$(X) $(OBJS) encnames.cpp pqsignal.cpp localtime.cpp *.depend


# ensure that changes in datadir propagate into object file
initdb.o: initdb.cpp $(top_builddir)/src/Makefile.global

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
