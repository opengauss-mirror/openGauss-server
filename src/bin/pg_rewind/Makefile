#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_rewind
#
# Portions Copyright (c) 2013-2015, PostgreSQL Global Development Group
#
# src/bin/pg_rewind/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "gs_rewind - repurpose an old master server as standby"
PGAPPICON = win32

subdir = src/bin/pg_rewind
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -I$(top_builddir)/src/bin/pg_ctl -I${top_builddir}/src/lib/page_compression -I${top_builddir}/src/include
override LDFLAGS := -L../../src/lib/page_compression
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = file_ops.o datapagemap.o fetch.o filemap.o logging.o parsexlog.o pg_rewind.o
#all:gs_rewind.a

pg_rewind.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) pg_rewind.a *.depend

distclean: clean

maintainer-clean: distclean

