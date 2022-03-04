#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/ipv6 (the regression tests)
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/ipv6/GNUmakefile
#
#-------------------------------------------------------------------------

subdir = src/test/ipv6
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

##
## Run tests
##

REGRESS_OPTS = --dlpath=. $(EXTRA_REGRESS_OPTS)
REG_CONF = --regconf=regress.conf

ipv6check_all: all
	export prefix=$(prefix) && sh $(CURDIR)/check_ipv6.sh ipv6_all

ipv6check_normal: all
	export prefix=$(prefix) && sh $(CURDIR)/check_ipv6.sh normal

ipv6check_primary_standby: all
	export prefix=$(prefix) && sh $(CURDIR)/check_ipv6.sh primary_standby

ipv6check_muti_standby: all
	export prefix=$(prefix) && sh $(CURDIR)/check_ipv6.sh muti_standby

ipv6check_casecade_standby: all
	export prefix=$(prefix) && sh $(CURDIR)/check_ipv6.sh casecade_standby

##
## Clean up
##

# things created by various check targets
clean distclean maintainer-clean:
	rm -rf $(pg_regress_clean_files)
