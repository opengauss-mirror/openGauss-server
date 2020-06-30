# contrib/gc_fdw/Makefile

MODULE_big = postgres_fdw
OBJS = gc_fdw_single.o option_single.o deparse_single.o $(WIN32RES)

PGFILEDESC = "gc_fdw - foreign data wrapper for PostgreSQL"
SHLIB_LINK = $(libpq)
top_builddir = ../..

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS)

EXTENSION = gc_fdw
DATA = gc_fdw--1.0.sql gc_fdw.control


REGRESS = postgres_fdw

# the db name is hard-coded in the tests
override USE_MODULE_DB =

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/gc_fdw
include $(top_builddir)/src/Makefile.global

# Shared library stuff
include $(top_builddir)/src/gausskernel/common.mk
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
endif

