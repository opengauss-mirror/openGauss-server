#-------------------------------------------------------------------------
#
# Makefile for src/tools/ifaddrs
#
# Copyright (c) 2003-2012, PostgreSQL Global Development Group
#
# src/tools/ifaddrs/Makefile
#
#-------------------------------------------------------------------------

subdir = src/tools/ifaddrs
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

libpq_backend_dir = $(top_builddir)/src/backend/libpq

override CPPFLAGS := -I$(libpq_backend_dir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = test_ifaddrs.o

all: test_ifaddrs

test_ifaddrs: test_ifaddrs.o $(libpq_backend_dir)/ip.o
	$(CC) $(CFLAGS) test_ifaddrs.o $(libpq_backend_dir)/ip.o $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

clean distclean maintainer-clean:
	rm -f test_ifaddrs$(X) $(OBJS) *.depend
