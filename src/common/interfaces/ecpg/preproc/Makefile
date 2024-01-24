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

# where to find gen_keywordlist.pl and subsidiary files
TOOLSDIR = $(top_srcdir)/src/tools
GEN_KEYWORDLIST = $(PERL) -I $(TOOLSDIR) $(TOOLSDIR)/gen_keywordlist.pl
GEN_KEYWORDLIST_DEPS = $(TOOLSDIR)/gen_keywordlist.pl $(TOOLSDIR)/PerfectHash.pm


ifeq ($(GCC),yes)
preproc.o: CXXFLAGS += -Wno-error
endif

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
	$(BISON) -d $(BISONFLAGS) -t -v -o $@ $<
else
	@$(missing) bison $< $@
endif

pgc.cpp: pgc.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $< 
	@grep -rn "size_t yy_size_t" $@;if [ $$? == 0 ]; then sed -i 's/int yyl;/size_t yyl;/g' $@;\
	sed -i "s/i < _yybytes_len/(size_t)i < (size_t)_yybytes_len/g" $@;\
	sed -i "s/n < max_size/(size_t)n < (size_t)max_size/g" $@;fi;
else
	@$(missing) flex $< $@
	@grep -rn "size_t yy_size_t" $@;if [ $$? == 0 ]; then sed -i 's/int yyl;/size_t yyl;/g' $@;\
	sed -i "s/i < _yybytes_len/(size_t)i < (size_t)_yybytes_len/g" $@;\
	sed -i "s/n < max_size/(size_t)n < (size_t)max_size/g" $@;fi;
endif

preproc.y: ../../../../common/backend/parser/gram.y parse.pl ecpg.addons ecpg.header ecpg.tokens ecpg.trailer ecpg.type
	$(PERL) $(srcdir)/parse.pl $(srcdir) < $< > $@
	$(PERL) $(srcdir)/check_rules.pl $(srcdir) $<

# generate keyword headers
c_kwlist_d.h: c_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --no-case-fold --varname ScanCKeywords $<

ecpg_kwlist_d.h: ecpg_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --varname ScanECPGKeywords $<

# Force these dependencies to be known even without dependency info built:
ecpg_keywords.o c_keywords.o keywords.o preproc.o parser.o: preproc.hpp $(top_srcdir)/src/include/parser/kwlist_d.h
ecpg_keywords.o: ecpg_kwlist_d.h
c_keywords.o: c_kwlist_d.h
keywords.o: $(top_srcdir)/src/include/parser/kwlist.h

kwlookup.cpp: % : $(top_srcdir)/src/common/backend/parser/%
	rm -f $@ && $(LN_S) $< .

distprep: preproc.y preproc.cpp preproc.hpp pgc.cpp

install: all installdirs
	$(INSTALL_PROGRAM) ecpg$(X) '$(DESTDIR)$(bindir)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/ecpg$(X)'

# preproc.y, preproc.c, preproc.h, pgc.c, c_kwlist_d.h, and ecpg_kwlist_d.h
# are in the distribution tarball, so they are not cleaned here.
clean distclean:
	rm -f kwlookup.cpp *.o ecpg$(X)
# garbage from development
	@rm -f core a.out

# `make distclean' must not remove preproc.y, preproc.cpp, preproc.h, or pgc.cpp
# since we want to ship those files in the distribution for people with
# inadequate tools.  Instead, `make maintainer-clean' will remove them.
maintainer-clean: distclean
	rm -f preproc.y preproc.cpp preproc.hpp pgc.cpp c_kwlist_d.h ecpg_kwlist_d.h
