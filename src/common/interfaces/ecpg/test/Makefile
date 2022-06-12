# src/common/interfaces/ecpg/test/Makefile

subdir = src/common/interfaces/ecpg/test
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := \
	'-I$(top_builddir)/src/common/port' \
	'-I$(top_srcdir)/src/test/regress' \
	'-DHOST_TUPLE="$(host_tuple)"' \
	'-DMAKEPROG="$(MAKE)"' \
	'-DSHELLPROG="$(SHELL)"' \
	'-DDLSUFFIX="$(DLSUFFIX)"' \
	$(CPPFLAGS)

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

# default encoding for regression tests
ENCODING = SQL_ASCII

ifneq ($(build_os),mingw32)
abs_builddir := $(shell pwd)
else
abs_builddir := $(shell sh -c "pwd -W")
endif

pg_mem_check = export ASAN_OPTIONS='unmap_shadow_on_exit=1:abort_on_error=1:detect_leaks=1:force_check_leak=1:halt_on_error=0:alloc_dealloc_mismatch=0:fast_unwind_on_fatal=1:log_path=$(HOME)/memchk/asan/runlog' && export LSAN_OPTIONS='exitcode=0:suppressions=$(HOME)/memchk/memleak_ignore'

all install installdirs uninstall distprep:
	$(MAKE) -C connect $@
	$(MAKE) -C sql $@
	$(MAKE) -C pgtypeslib $@
	$(MAKE) -C preproc $@
	$(MAKE) -C compat_informix $@
	$(MAKE) -C thread $@

clean distclean maintainer-clean:
	$(MAKE) -C connect $@
	$(MAKE) -C sql $@
	$(MAKE) -C pgtypeslib $@
	$(MAKE) -C preproc $@
	$(MAKE) -C compat_informix $@
	$(MAKE) -C thread $@
	rm -rf tmp_check results log
	rm -f pg_regress regression.diffs regression.out pg_regress_ecpg.o

# Build regression test driver
all: pg_regress$(X)

ifeq ($(enable_memory_check_core), yes)
pg_regress$(X): pg_regress_ecpg.o $(top_builddir)/../distribute/test/regress/pg_regress.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $^ $(LIBS) -o $@
else
pg_regress$(X): pg_regress_ecpg.o $(top_builddir)/src/test/regress/pg_regress.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $^ $(LIBS) -o $@
endif

$(top_builddir)/src/test/regress/pg_regress.o: $(top_builddir)/src/test/regress/pg_regress.cpp
	$(MAKE) -C $(dir $@) $(notdir $@)

# dependencies ensure that path changes propagate
pg_regress_ecpg.o: pg_regress_ecpg.cpp $(top_builddir)/src/common/port/pg_config_paths.h

$(top_builddir)/src/common/port/pg_config_paths.h: $(top_builddir)/src/Makefile.global
	$(MAKE) -C $(top_builddir)/src/common/port pg_config_paths.h

# When doing a VPATH build, copy over the .pgc, .stdout and .stderr
# files so that the driver script can find them.  We have to use an
# absolute path for the targets, because otherwise make will try to
# locate the missing files using VPATH, and will find them in
# $(srcdir), but the point here is that we want to copy them from
# $(srcdir) to the build directory.

ifdef VPATH
remaining_files_src := $(wildcard $(srcdir)/*/*.pgc) $(wildcard $(srcdir)/expected/*.c) $(wildcard $(srcdir)/expected/*.stdout) $(wildcard $(srcdir)/expected/*.stderr) $(srcdir)/resultmap
remaining_files_build := $(patsubst $(srcdir)/%, $(abs_builddir)/%, $(remaining_files_src))

all: $(remaining_files_build)
$(remaining_files_build): $(abs_builddir)/%: $(srcdir)/%
	ln -s $< $@
endif

# Common options for tests. Also pick up anything passed in EXTRA_REGRESS_OPTS
REGRESS_OPTS = --dbname=regress1,connectdb --create-role=connectuser,connectdb $(EXTRA_REGRESS_OPTS) $(filter-out $(EXTRA_REGRESS_OPTS), $(VB_REGRESS_OPTS))

check: all
	rm -rf ./log
	$(MKDIR_P) ./log
	test -d $(HOME)/memchk/asan || mkdir -p $(HOME)/memchk/asan
	test -e $(HOME)/memchk/memleak_ignore || touch $(HOME)/memchk/memleak_ignore
	${pg_mem_check} && ./pg_regress $(REGRESS_OPTS) --top-builddir=$(top_builddir) --single_node -d 1 -c 0 -r 1 -p 57436 -b ./tmp_check -n  $(pg_regress_locale_flags) $(THREAD)  -w --keep_last_data=false  --temp-config=$(srcdir)/make_connect.conf --schedule=$(srcdir)/ecpg_schedule --regconf=regress.conf --ecpg

