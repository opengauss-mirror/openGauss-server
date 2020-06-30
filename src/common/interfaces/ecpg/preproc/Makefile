#-------------------------------------------------------------------------
#
# Makefile for src/common/interfaces/ecpg/preproc
#
# Copyright (c) 1998-2012, PostgreSQL Global Development Group
#
# src/common/interfaces/ecpg/preproc/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "ecpg - embedded SQL precompiler for C"
PGAPPICON=win32

subdir = src/common/interfaces/ecpg/preproc
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

MAJOR_VERSION= 4
MINOR_VERSION= 8
PATCHLEVEL=0

override CPPFLAGS := -I../include -I$(top_srcdir)/src/common/interfaces/ecpg/include \
	-I. -I$(srcdir) -DMAJOR_VERSION=$(MAJOR_VERSION) \
	-DMINOR_VERSION=$(MINOR_VERSION) -DPATCHLEVEL=$(PATCHLEVEL) \
	 $(CPPFLAGS)

override CFLAGS += $(PTHREAD_CFLAGS) -DECPG_COMPILE


ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS=	preproc.o type.o ecpg.o output.o parser.o \
	keywords.o c_keywords.o ecpg_keywords.o kwlookup.o ../ecpglib/typename.o descriptor.o variable.o \
	$(WIN32RES)

all: ecpg

ecpg: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $^ $(LIBS) $(PTHREAD_LIBS) -o $@$(X)

../ecpglib/typename.o: ../ecpglib/typename.cpp
	$(MAKE) -C $(dir $@) $(notdir $@)

# pgc is compiled as part of preproc
preproc.o: pgc.cpp

preproc.hpp: preproc.cpp ;

preproc.cpp: preproc.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

pgc.cpp: pgc.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

preproc.y: ../../../../common/backend/parser/gram.y parse.pl ecpg.addons ecpg.header ecpg.tokens ecpg.trailer ecpg.type
	$(PERL) $(srcdir)/parse.pl $(srcdir) < $< > $@
	$(PERL) $(srcdir)/check_rules.pl $(srcdir) $<

ecpg_keywords.o c_keywords.o keywords.o preproc.o parser.o: preproc.hpp

kwlookup.cpp: % : $(top_srcdir)/src/common/backend/parser/%
	rm -f $@ && $(LN_S) $< .

distprep: preproc.y preproc.cpp preproc.hpp pgc.cpp

install: all installdirs
	$(INSTALL_PROGRAM) ecpg$(X) '$(DESTDIR)$(bindir)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/ecpg$(X)'

clean distclean:
	rm -f kwlookup.cpp *.o ecpg$(X)
# garbage from development
	@rm -f core a.out

# `make distclean' must not remove preproc.y, preproc.cpp, preproc.h, or pgc.cpp
# since we want to ship those files in the distribution for people with
# inadequate tools.  Instead, `make maintainer-clean' will remove them.
maintainer-clean: distclean
	rm -f preproc.y preproc.cpp preproc.hpp pgc.cpp
