MODULE_big = dist_fdw

EXTENSION = dist_fdw

REGRESS = dist_fdw

EXTRA_CLEAN = sql/dist_fdw.sql expected/dist_fdw.out

subdir = src/gausskernel/storage/bulkload
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

parser.o:
	$(CC) $(CPPFLAGS) -DOBS_SERVER $(CXXFLAGS) -c parser.cpp -o parser.o

OBJS = dist_fdw.o foreignroutine.o vecforeignroutine.o importerror.o roach_adpter.o parser.o

DATA = dist_fdw.control dist_fdw--1.0.sql

# Shared library stuff
include $(top_srcdir)/src/gausskernel/common.mk

install:install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall:uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

clean:
