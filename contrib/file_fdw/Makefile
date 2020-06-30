#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for file_fdw
#
# IDENTIFICATION
#    contrib/file_fdw/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = file_fdw

EXTENSION = file_fdw

REGRESS = file_fdw

EXTRA_CLEAN = sql/file_fdw.sql expected/file_fdw.out

subdir = contrib/file_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global


OBJS = file_fdw.o

DATA = file_fdw.control file_fdw--1.0.sql

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
