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
server_topdir = ../../../..
# get  string from script get_PlatForm_str.sh
PLAT_FORM_STR = $(shell sh $(top_builddir)/src/get_PlatForm_str.sh)
ifeq ($(PLAT_FORM_STR), Failed)
        exit 1;
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

# file with extra config for temp build
TEMP_CONF = --temp-config=$(srcdir)/make_check_postgresql.conf
TEMP_CONF_2PC = --temp-config=$(srcdir)/make_check_postgresql_2pc.conf
ifdef TEMP_CONFIG
TEMP_CONF += --temp-config=$(TEMP_CONFIG)
endif

d=1
c=0
p=25632
s=1
runtest=1

dir=./tmp_check
hdfscfgpath='/opt/config'
hdfsstoreplus=''
hdfshostname='10.185.178.239:25000,10.185.178.241'
obshostname='10.175.38.76'
obsbucket='gaussdb'
ak='PXOCFVMGOMNOJSAWI2I0'
sk='FiqwtDDeUCtsieCbOCWzrQ3fwJHFJetzzXbdLHEh'
abs_gausshome='$(GAUSSHOME)'
keep_last_data=false
inplace_data_base_dir=../upgrade
grayscale_data_base_dir=../../../../privategauss/test/grayscale_upgrade
old_bin_dir='./tmp_check/bin'
cpu_bit=$(shell uname -m)
aie_host='None'
aie_port='None'
upgrade_from=92497
inplace_upgrade_script_dir=../upgrade
grayscale_upgrade_script_dir=../grayscale_upgrade

# Qunit parameter
# Empty string means all
qunit_module=''
qunit_level=''

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

# maximum simultaneous connections for parallel tests
MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

ifeq ($(SUPPORT_HOTPATCH), yes)
ifeq ($(PART)_, _)
hotpatch_dir=$(realpath $(server_topdir)/distribute/lib/hotpatch/)
define hotpatch_error_out
    $(info "Can not find hotpatch fastcheck tool in $(hotpatch_dir)/tool/fastcheck/:")
    $(info "Please clone hotpatch repo and create soft link:")
    $(info "git clone ssh://git@lfg-y.codehub.huawei.com:2222/Gauss/GaussDBKernel/GaussDBKernel-hotpatch.git")
    $(info "ln -s GaussDBKernel-hotpatch/samples/ $(hotpatch_dir)/sample ")
    $(info "ln -s GaussDBKernel-hotpatch/tool/ $(hotpatch_dir)/tool ")
    $(error "Can not find hotpatch tools")
endef
define hotpatch_check_func
	$(if $(wildcard $(hotpatch_dir)/tool/fastcheck/patch_tool_llt.sh),,$(call hotpatch_error_out))
endef
else
define hotpatch_check_func
endef
endif

define hotpatch_setup_func
    export SERVER_CODE_BASE=$(realpath $(server_topdir)) && \
    export PATCHLIB_HOME=$(realpath $(LIBHOTPATCH_HOME))
endef
else
define hotpatch_setup_func
    sed -i '/hotpatch/s/test: hotpatch/#test: Hotpatch/g' $(srcdir)/parallel_schedule
endef
endif

ifeq ($(cpu_bit), aarch64)
define exception_arm_cases
	sed -i '/distribute_setop_1/s/distribute_setop_1/distribute_setop_ARM_1/g' $(srcdir)/parallel_schedule
	sed -i '/nodegroup_setop_smp_test/s/nodegroup_setop_smp_test/nodegroup_setop_smp_ARM_test/g' $(srcdir)/parallel_schedule
	sed -i '/nodegroup_setop_test/s/nodegroup_setop_test/nodegroup_setop_ARM_test/g' $(srcdir)/parallel_schedule
	sed -i '/random_plan/s/random_plan/random_ARM_plan/g' $(srcdir)/parallel_schedule
	sed -i '/shipping_no_from_list/s/shipping_no_from_list/shipping_no_from_ARM_list/g' $(srcdir)/parallel_schedule
	sed -i '/test_regex/s/test_regex/test_ARM_regex/g' $(srcdir)/parallel_schedule
	sed -i '/vec_expression1/s/vec_expression1/vec_expression_ARM1/g' $(srcdir)/parallel_schedule
	sed -i '/vec_expression2/s/vec_expression2/vec_expression_ARM2/g' $(srcdir)/parallel_schedule
	sed -i '/xc_copy/s/xc_copy/xc_ARM_copy/g' $(srcdir)/parallel_schedule
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
	$(MAKE) -C $(srcdir)/stub/roach_api_stub install

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
	$(call hotpatch_setup_func) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) $(MAXCONNOPT) $(TEMP_CONF) $(EXTRA_TESTS) $(REG_CONF)
2pccheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 4 -c 2 -p $(p) -r $(runtest) -b $(dir) --schedule=$(srcdir)/parallel_schedule_2pc $(MAXCONNOPT) $(TEMP_CONF_2PC) $(EXTRA_TESTS) $(REG_CONF)

redischeck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 8 -c 1 -p $(p) -r $(runtest) -b $(dir) --schedule=$(srcdir)/parallel_schedule$(PART).redistribute --keep_last_data=${keep_last_data} $(MAXCONNOPT) $(TEMP_CONF) $(EXTRA_TESTS) $(REG_CONF)

decodecheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule$(PART).decode -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)
wlmcheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	export TMP_USR=`whoami`; \
	cp /home/$$(whoami)/etc/gscgroup_$$(whoami).cfg $$GAUSSHOME/etc/; \
	if [ `whoami`x == 'CodeTestCov'x ];then \
		export WLM_TEST=${GAUSSHOME}; \
	else \
		export WLM_TEST=${CODE_BASE}/Code/src/test/regress/tmp_check/install/${GAUSSHOME}; \
	fi;\
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) --schedule=$(srcdir)/parallel_schedule.wlm --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_wlmcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

aiecheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule.aie --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_aiecheck_postgresql.conf --g_aiehost=$(aie_host) --g_aieport=$(aie_port) $(EXTRA_TESTS) $(REG_CONF)

redocheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -s $(s) -b $(dir) --schedule=$(srcdir)/parallel_schedule.redo $(MAXCONNOPT)  --temp-config=$(srcdir)/make_redocheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

redochecksmall: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -s $(s) -b $(dir) --schedule=$(srcdir)/parallel_schedule.redosmall $(MAXCONNOPT)  --temp-config=$(srcdir)/make_redocheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_initdb: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
        $(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r 0 -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome)  --single_node --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf --init_database $(EXTRA_TESTS) $(REG_CONF)

fastcheck_inplace: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
        $(call hotpatch_setup_func) && \
        $(call exception_arm_cases) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --schedule=$(srcdir)/parallel_schedule$(PART) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf --inplace_upgrade --data_base_dir=$(data_base_dir) --platform=$(PLAT_FORM_STR) --upgrade_script_dir=$(upgrade_script_dir) $(EXTRA_TESTS) $(REG_CONF) --upgrade_from=$(upgrade_from)

fastcheck_single: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule0$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_lite: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule.lite$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

upgradecheck_single: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule0$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)
    --data_base_dir=$(grayscale_data_base_dir) --platform=$(PLAT_FORM_STR) \
    --upgrade_script_dir=$(grayscale_upgrade_script_dir) \
    --old_bin_dir=$(old_bin_dir) --grayscale_full_mode \
    --upgrade_from=$(upgrade_from)

fastcheck_ce_single: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
        $(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --schedule=$(srcdir)/c

execute_fastcheck_ce_jdbc_tool:
	$(MAKE) -j 10 -C jdbc_client

execute_fastcheck_ce_single_jdbc: execute_fastcheck_ce_jdbc_tool
ifdef NO_BUILD
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	 $(pg_regress_installcheck) --schedule=$(srcdir)/ce_sched_jdbc $(REG_CONF) --jdbc || true
else
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	 $(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --schedule=$(srcdir)/ce_sched_jdbc --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf --data_base_dir=$(data_base_dir) $(REG_CONF) --jdbc || true
endif

fastcheck_single_segment: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule0$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf --enable-segment $(EXTRA_TESTS) $(REG_CONF)

fastcheck_single_db4ai: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule.db4ai -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_single_mot: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule20 -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_single_mot_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck_parallel_initdb: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(call exception_arm_cases) && \
	$(pg_regress_check) $(REGRESS_OPTS) --parallel_initdb -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

fastcheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 1 -c 0 -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --single_node --schedule=$(srcdir)/parallel_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

#Support QUnit check
qunitcheck: all tablespace-setup
	$(MAKE) -C ../QUnit/src  && export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
    $(pg_regress_check) $(REGRESS_OPTS) --qunit --qunit_module $(qunit_module) --qunit_level $(qunit_level)  --parallel_initdb -d $(d) -c $(c) -p $(p) -r 0 -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

securitycheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) -n $(n) --securitymode --schedule=$(srcdir)/security_schedule$(PART) -w --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

obscheck:all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) --obshostname=$(obshostname) --obsbucket=$(obsbucket) --ak=$(ak) --sk=$(sk) --schedule=$(srcdir)/obs_schedule$(PART) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

orccheckxian: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 2 -p $(p) -r $(runtest) -b $(dir) --hdfshostname=$(hdfshostname) --hdfscfgpath=$(hdfscfgpath) --hdfsport=25000 --schedule=$(srcdir)/orc_schedule$(PART) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

obsorccheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 2 -p $(p) -r $(runtest) -b $(dir) --obshostname=$(obshostname) --obsbucket=$(obsbucket)  --ak=$(ak) --sk=$(sk) --port=$(p) --schedule=$(srcdir)/obs_orc_schedule$(PART) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_obsorccheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

dfscheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) --hdfshostname=$(hdfshostname) --hdfsstoreplus=$(hdfsstoreplus) --hdfscfgpath=$(hdfscfgpath) --hdfsport=25000 --schedule=$(srcdir)/dfs_schedule$(PART) --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

dfsredischeck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 1 -p $(p) -r $(runtest) -b $(dir) --hdfshostname=$(hdfshostname) --hdfsstoreplus=$(hdfsstoreplus) --hdfscfgpath=$(hdfscfgpath) --hdfsport=25000 --schedule=$(srcdir)/parallel_schedule$(PART).dfsredistribute $(MAXCONNOPT) $(TEMP_CONF) $(EXTRA_TESTS) $(REG_CONF)

orcchecksmall: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 2 -p $(p) -r $(runtest) -b $(dir) --hdfshostname=$(hdfshostname) --hdfscfgpath=$(hdfscfgpath) --hdfsport=25000 --schedule=$(srcdir)/orc_schedule_small --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

orccheckusa: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 2 -p $(p) -r $(runtest) -b $(dir) --hdfshostname=10.122.195.197 --hdfsport=8020 --schedule=$(srcdir)/orc_schedule $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

parquetchecksmall: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_check) $(REGRESS_OPTS) -d 2 -c 2 -p $(p) -r $(runtest) -b $(dir) --hdfshostname=$(hdfshostname) --hdfscfgpath=$(hdfscfgpath) --hdfsport=25000 --schedule=$(srcdir)/parquet_schedule_small --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)

installcheck: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule $(EXTRA_TESTS) $(REG_CONF)

installcheck-parallel: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(call exception_arm_cases) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) -p $(p) --schedule=$(srcdir)/parallel_schedule$(PART) $(MAXCONNOPT) $(EXTRA_TESTS) $(REG_CONF) --abs_gausshome=$(GAUSSHOME)

# $(pg_regress_check) $(REGRESS_OPTS) -d $(d) -c $(c) -p $(p) -r $(runtest) -b $(dir) -n $(n) --abs_gausshome=$(abs_gausshome) --schedule=$(srcdir)/parallel_schedule$(PART)  --keep_last_data=${keep_last_data} $(MAXCONNOPT) --temp-config=$(srcdir)/make_fastcheck_postgresql.conf $(EXTRA_TESTS) $(REG_CONF)


standbycheck: all
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/standby_schedule --use-existing $(REG_CONF)

# old interfaces follow...

runcheck: check
runtest: installcheck
runtest-parallel: installcheck-parallel

bigtest: all tablespace-setup
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule numeric_big $(REG_CONF)

bigcheck: all tablespace-setup
	$(call hotpatch_check_func)
	export LD_LIBRARY_PATH=$(SSL_LIB_PATH):$(LD_LIBRARY_PATH) && \
	$(call hotpatch_setup_func) && \
	$(call exception_arm_cases) && \
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(MAXCONNOPT) numeric_big $(REG_CONF)


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
	$(MAKE) -C $(srcdir)/stub/roach_api_stub clean
