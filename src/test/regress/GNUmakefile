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

subdir = src/test/regress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# get  string from script get_PlatForm_str.sh
PLAT_FORM_STR = $(shell sh $(top_builddir)/src/get_PlatForm_str.sh)
ifeq ($(PLAT_FORM_STR), Failed)
        exit 1;
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

# file with extra config for temp build
TEMP_CONF = --temp-config=$(srcdir)/make_check_postgresql.conf
ifdef TEMP_CONFIG
TEMP_CONF += --temp-config=$(TEMP_CONFIG)
endif

ud=4
uc=1
d=1
c=0
p=25632
s=1
runtest=1
n=0
dir=./tmp_check
abs_gausshome='$(GAUSSHOME)'
keep_last_data=false
old_bin_dir='./tmp_check/bin'
cpu_bit=$(shell uname -p)
aie_host='None'
aie_port='None'

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

# maximum simultaneous connections for parallel tests
MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

ifeq ($(cpu_bit), aarch64)
define exception_arm_cases
	sed -i '/xc_copy /s/xc_copy /xc_copy_arm /g' $(srcdir)/parallel_schedule
	sed -i '/hw_cstore_alter1 /s/hw_cstore_alter1 /hw_cstore_alter1_arm /g' $(srcdir)/parallel_schedule
	sed -i '/bulkload_end /s/bulkload_end /bulkload_end_arm /g' $(srcdir)/parallel_schedule
	sed -i '/bulkload_error_tbl /s/bulkload_error_tbl /bulkload_error_tbl_arm /g' $(srcdir)/parallel_schedule
endef
else
define exception_arm_cases
	export CODE_BASE=$(realpath $(top_builddir)/../)
endef
endif

# stuff to pass into build of pg_regress
EXTRADEFS = '-DHOST_TUPLE="$(host_tuple)"' \
	'-DMAKEPROG="$(MAKE)"' \
	'-DSHELLPROG="$(SHELL)"' \
	'-DDLSUFFIX="$(DLSUFFIX)"'

##
## Prepare for tests
##

# Build regression test driver

all: pg_regress$(X)

pg_regress$(X): pg_regress.o pg_regress_main.o | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

# dependencies ensure that path changes propagate
pg_regress.o: pg_regress.cpp $(top_builddir)/src/common/port/pg_config_paths.h
pg_regress.o: override CPPFLAGS += -I$(top_builddir)/src/common/port $(EXTRADEFS)

$(top_builddir)/src/common/port/pg_config_paths.h: $(top_builddir)/src/Makefile.global
	$(MAKE) -C $(top_builddir)/src/common/port pg_config_paths.h

install: all installdirs
	$(INSTALL_PROGRAM) pg_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'


# Build dynamically-loaded object file for CREATE FUNCTION ... LANGUAGE C.

NAME = regress
OBJS = regress.o

include $(top_srcdir)/src/Makefile.shlib

all: all-lib

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files = $(patsubst $(srcdir)/input/%.source,sql/%.sql, $(wildcard $(srcdir)/input/*.source))
output_files := $(patsubst $(srcdir)/output/%.source,expected/%.out, $(wildcard $(srcdir)/output/*.source))

# not installed by default

regress_data_files = \
	$(filter-out $(addprefix $(srcdir)/,$(output_files)),$(wildcard $(srcdir)/expected/*.out)) \
	$(wildcard $(srcdir)/input/*.source) \
	$(wildcard $(srcdir)/output/*.source) \
	$(filter-out $(addprefix $(srcdir)/,$(input_files)),$(wildcard $(srcdir)/sql/*.sql)) \
	$(wildcard $(srcdir)/data/*.data) \
	$(srcdir)/parallel_schedule $(srcdir)/serial_schedule $(srcdir)/resultmap

install-tests: all install install-lib installdirs-tests
	$(MAKE) -C $(top_builddir)/contrib/spi install
	for file in $(regress_data_files); do \
	  $(INSTALL_DATA) $$file '$(DESTDIR)$(pkglibdir)/regress/'$$file || exit; \
	done

installdirs-tests: installdirs
	$(MKDIR_P)  $(patsubst $(srcdir)/%/,'$(DESTDIR)$(pkglibdir)/regress/%',$(sort $(dir $(regress_data_files))))


# Get some extra C modules from contrib/spi and contrib/dummy_seclabel...

all: refint$(DLSUFFIX) autoinc$(DLSUFFIX) dummy_seclabel$(DLSUFFIX)
	chmod +x ./smartmatch.pl

refint$(DLSUFFIX): $(top_builddir)/contrib/spi/refint$(DLSUFFIX)
	cp $< $@

autoinc$(DLSUFFIX): $(top_builddir)/contrib/spi/autoinc$(DLSUFFIX)
	cp $< $@

dummy_seclabel$(DLSUFFIX): $(top_builddir)/contrib/dummy_seclabel/dummy_seclabel$(DLSUFFIX)
	cp $< $@

$(top_builddir)/contrib/spi/refint$(DLSUFFIX): $(top_srcdir)/contrib/spi/refint.cpp
	$(MAKE) -C $(top_builddir)/contrib/spi refint$(DLSUFFIX)

$(top_builddir)/contrib/spi/autoinc$(DLSUFFIX): $(top_srcdir)/contrib/spi/autoinc.cpp
	$(MAKE) -C $(top_builddir)/contrib/spi autoinc$(DLSUFFIX)

$(top_builddir)/contrib/dummy_seclabel/dummy_seclabel$(DLSUFFIX): $(top_builddir)/contrib/dummy_seclabel/dummy_seclabel.cpp
	$(MAKE) -C $(top_builddir)/contrib/dummy_seclabel dummy_seclabel$(DLSUFFIX)

# Tablespace setup

.PHONY: tablespace-setup
tablespace-setup:
	rm -rf ./testtablespace
	mkdir ./testtablespace
	test -d $(HOME)/memchk/asan || mkdir -p $(HOME)/memchk/asan

##
## Run tests
##

REGRESS_OPTS = --dlpath=. $(EXTRA_REGRESS_OPTS)
REG_CONF = --regconf=regress.conf

check: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) $(MAXCONNOPT) $(TEMP_CONF) $(EXTRA_TESTS) $(REG_CONF)
aiecheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule.aie --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_aiecheck_postgresql.conf --aiehost=$(aie_host) --aieport=$(aie_port) $(EXTRA_TESTS) $(REG_CONF)

fastcheck_initdb: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call exception_arm_cases) && \
        $(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r 0 -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf --init_database $(EXTRA_TESTS) $(REG_CONF)

fastcheck_single: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule0 -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_single_mot: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule16 -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_single_mot_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_parallel_initdb: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call exception_arm_cases) && \
	$(pg_regress_check) $(REGRESS_OPTS) --parallel_initdb -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call exception_arm_cases) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

securitycheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) -n $(n) --securitymode --schedule=$(srcdir)/security_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

installcheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule $(EXTRA_TESTS) $(REG_CONF)

installcheck-parallel: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call exception_arm_cases) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) -p $(p) --schedule=$(srcdir)/parallel_schedule$(PART) $(MAXCONNOPT) $(EXTRA_TESTS) $(REG_CONF) --abs_gausshome=$(GAUSSHOME)

##
## Clean up
##

clean distclean maintainer-clean: clean-lib
# things built by `all' target
	rm -f $(OBJS) refint$(DLSUFFIX) autoinc$(DLSUFFIX) dummy_seclabel$(DLSUFFIX)
	rm -f pg_regress_main.o pg_regress.o pg_regress$(X)
# things created by various check targets
	rm -f $(output_files) $(input_files)
	rm -rf testtablespace
	rm -rf $(pg_regress_clean_files)
	sed -i '/xc_copy_arm /s/xc_copy_arm /xc_copy /g' $(srcdir)/parallel_schedule
	sed -i '/hw_cstore_alter1_arm /s/hw_cstore_alter1_arm /hw_cstore_alter1 /g' $(srcdir)/parallel_schedule
	sed -i '/bulkload_end_arm /s/bulkload_end_arm /bulkload_end /g' $(srcdir)/parallel_schedule
	sed -i '/bulkload_error_tbl_arm /s/bulkload_error_tbl_arm /bulkload_error_tbl /g' $(srcdir)/parallel_schedule

