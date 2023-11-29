#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/regress (the regression tests)
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/regress/GNUmakefile
#
#-------------------------------------------------------------------------

subdir = src/test/ha
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

##
## Run tests
##

REGRESS_OPTS = --dlpath=. $(EXTRA_REGRESS_OPTS)
REG_CONF = --regconf=regress.conf

hacheck_all: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_single.sh
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_single.sh
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_cascade.sh

hacheck_single: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_single.sh 1 $(PART)

hacheck_multi_single: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_single.sh 1 $(PART)
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_cascade.sh

hacheck_multi_single_mot: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_single_mot.sh 1 $(PART)
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_cascade_mot.sh 1 $(PART)

hacheck_multi_single_shared_storage: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_single_shared_storage.sh 2 $(PART)

hacheck_single_standby_read: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_single_standby_read.sh

hacheck_multi_uwal: all
	export prefix=$(prefix) && sh $(CURDIR)/run_ha_multi_uwal.sh 2

##
## Clean up
##

# things created by various check targets
clean distclean maintainer-clean:
	rm -rf $(pg_regress_clean_files)
