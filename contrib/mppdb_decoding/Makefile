# contrib/mppdb_decoding/Makefile

MODULES = mppdb_decoding
OBJS = mppdb_decoding.o


# Note: because we don't tell the Makefile there are any regression tests,
# we have to clean those result files explicitly
EXTRA_CLEAN = $(pg_regress_clean_files) ./regression_output ./isolation_output

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mppdb_decoding
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(enable_debug), no)
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now
else
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC
endif

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

# But it can nonetheless be very helpful to run tests on preexisting
# installation, allow to do so, but only if requested explicitly.
installcheck-force: regresscheck-install-force isolationcheck-install-force

#check: regresscheck isolationcheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-mppdb_decoding:
	$(MAKE) -C $(top_builddir)/contrib/mppdb_decoding

REGRESSCHECKS=ddl rewrite toast permissions decoding_in_xact decoding_into_rel binary prepared

regresscheck: all | submake-regress submake-mppdb_decoding
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/contrib/mppdb_decoding/logical.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/mppdb_decoding \
	    --outputdir=./regression_output \
	    $(REGRESSCHECKS)

regresscheck-install-force: | submake-regress submake-mppdb_decoding
	$(pg_regress_installcheck) \
	    --extra-install=contrib/mppdb_decoding \
	    $(REGRESSCHECKS)

ISOLATIONCHECKS=mxact delayed_startup ondisk_startup concurrent_ddl_dml

isolationcheck: all | submake-isolation submake-mppdb_decoding
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
	    --temp-config $(top_srcdir)/contrib/mppdb_decoding/logical.conf \
	    --extra-install=contrib/mppdb_decoding \
	    --outputdir=./isolation_output \
	    $(ISOLATIONCHECKS)

isolationcheck-install-force: all | submake-isolation submake-mppdb_decoding
	$(pg_isolation_regress_installcheck) \
	    --extra-install=contrib/mppdb_decoding \
	    $(ISOLATIONCHECKS)


all: objfiles.txt
     
 # generate mppdb_decoding objfiles.txt to suspend duplicate make of gaussdb
objfiles.txt: $(OBJS)
	@echo "" > objfiles.txt

objfiles_clean:
	rm -f objfiles.txt

clean: objfiles_clean

PHONY: submake-mppdb_decoding submake-regress check \
	regresscheck regresscheck-install-force \
	isolationcheck isolationcheck-install-force
