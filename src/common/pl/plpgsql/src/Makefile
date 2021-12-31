#-------------------------------------------------------------------------
#
# Makefile for the pl/pgsql procedural language
#
# src/common/pl/plpgsql/src/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/pl/plpgsql/src
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

# Shared library parameters
#NAME= plpgsql

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)
SHLIB_LINK = $(filter -lintl, $(LIBS))
rpath =

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = pl_gram.o pl_handler.o pl_comp.o pl_package.o pl_exec.o pl_funcs.o pl_scanner.o plsql_packages.o pl_debugger.o pl_debugger_api.o pl_sql_validator.o pl_global_package_runtime_cache.o 

DATA = plpgsql.control plpgsql--1.0.sql plpgsql--unpackaged--1.0.sql

TARGET= libplpgsql.a

all: libplpgsql.a

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

libplpgsql.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

install: all install-data

installdirs: installdirs-lib
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall: uninstall-lib uninstall-data

install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

.PHONY: install-data uninstall-data

# Force these dependencies to be known even without dependency info built:
pl_gram.o pl_handler.o pl_comp.o pl_package.o pl_exec.o pl_funcs.o pl_scanner.o: pl_gram.hpp plerrcodes.h

# See notes in src/backend/parser/Makefile about the following two rules

pl_gram.hpp: pl_gram.cpp ;

pl_gram.cpp: gram.y $(top_builddir)/src/mtlocal.pl
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
	$(PERL) $(top_builddir)/src/mtlocal.pl $(srcdir)/pl_gram.cpp
	$(PERL) $(top_builddir)/src/mtlocal.pl $(srcdir)/pl_gram.hpp
else
	@$(missing) bison $< $@
endif

# generate plerrcodes.h from src/backend/utils/errcodes.txt
plerrcodes.h: $(top_srcdir)/src/common/backend/utils/errcodes.txt generate-plerrcodes.pl
	$(PERL) $(srcdir)/generate-plerrcodes.pl $< > $@

distprep: pl_gram.hpp pl_gram.cpp plerrcodes.h

# pl_gram.c, pl_gram.h and plerrcodes.h are in the distribution tarball,
# so they are not cleaned here.
clean distclean: clean-lib
	rm -f $(OBJS) libplpgsql.a

maintainer-clean: clean
	rm -f pl_gram.cpp pl_gram.hpp plerrcodes.h
