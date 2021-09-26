#----------------------------------------------------------------------------
#
# openGauss documentation tool makefile
#
# Copyright (c) 2010-2012 Postgres-XC Development Group
#
# pgxc/tools/Makefile
#
#----------------------------------------------------------------------------

subdir = src/common/pgxc/tools
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = makesgml

all distprep html man install installdirs uninstall clean distclean maintainer-clean maintainer-check:
	$(MAKE) -C $(SUBDIRS) $@
