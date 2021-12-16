subdir = src/gausskernel/storage/replication/pgoutput
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	pgoutput.o
PGFILEDESC = "pgoutput - standard logical replication output plugin"
NAME = pgoutput

all: all-shared-lib

include $(top_builddir)/src/Makefile.shlib

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean maintainer-clean: clean-lib
	rm -f $(OBJS)
