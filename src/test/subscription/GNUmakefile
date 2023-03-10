#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/subscription (the publication/subscription tests)
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/subscription/GNUmakefile
#
#-------------------------------------------------------------------------

subdir = src/test/subscription
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)
p=25800
dbcompatibility=A
##
## Run tests
##

REGRESS_OPTS = --dlpath=. $(EXTRA_REGRESS_OPTS)
REG_CONF = --regconf=regress.conf

check: all
	sh $(CURDIR)/run_check.sh $(CURDIR) $p $(dbcompatibility)

##
## Clean up
##

# things created by various check targets
clean distclean maintainer-clean:
	rm -rf $(pg_regress_clean_files)
