subdir = src/gausskernel/storage/replication/pgoutput
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS += $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC
OBJS = \
	$(WIN32RES) \
	pgoutput.o
PGFILEDESC = "pgoutput - standard logical replication output plugin"
NAME = pgoutput

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all

all: all-shared-lib

include $(top_builddir)/src/Makefile.shlib

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean maintainer-clean: clean-lib
	rm -f $(OBJS)
