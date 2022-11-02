#-------------------------------------------------------------------------
#
# Makefile for src
#
# Copyright (c) 1994, Regents of the University of California
#
# src/Makefile
#
#-------------------------------------------------------------------------

subdir = src
top_builddir = ..
include Makefile.global

ifeq ($(enable_multiple_nodes)_$(enable_privategauss)_$(enable_lite_mode), no_yes_no)
dirs_in_central_mode = $(top_builddir)/../distribute/cm $(top_builddir)/../distribute/bin 
endif

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS = \
	lib \
	$(top_builddir)/../distribute/cm \
	$(top_builddir)/../distribute/gtm \
	common \
	$(top_builddir)/../distribute/lib/hotpatch \
	$(top_builddir)/../distribute/bin \
	lib/elog \
	bin \
	gausskernel \
	common/backend/utils/mb/conversion_procs \
	common/backend/snowball \
	include \
	$(top_builddir)/../distribute/include \
	common/pl \
	makefiles \
	$(top_builddir)/../distribute/test/regress \
	$(top_builddir)/../distribute/test/ha
else
SUBDIRS = \
	lib \
	common \
	lib/elog \
	gausskernel $(dirs_in_central_mode) \
	gausskernel/storage/replication/pgoutput/ \
	bin \
	common/backend/utils/mb/conversion_procs \
	common/backend/snowball \
	include \
	common/pl \
	makefiles \
	test/regress
endif

# There are too many interdependencies between the subdirectories, so
# don't attempt parallel make here.
.NOTPARALLEL:

# cm and gtm all depends on etcdapi, build it first
ifeq ($(enable_multiple_nodes), yes)
all: $(top_builddir)/../distribute/cm/cm_etcdapi/libgoetcdapi.h

install: $(top_builddir)/../distribute/cm/cm_etcdapi/libgoetcdapi.h

$(top_builddir)/../distribute/cm/cm_etcdapi/libgoetcdapi.h:
	$(MAKE) -C $(top_builddir)/../distribute/cm/cm_etcdapi
endif

$(recurse)

install: install-local

install-local: installdirs-local
	$(INSTALL_DATA) Makefile.global '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.global'
	$(INSTALL_DATA) Makefile.port '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.port'
	$(INSTALL_DATA) $(srcdir)/Makefile.shlib '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.shlib'
	$(INSTALL_DATA) $(srcdir)/nls-global.mk '$(DESTDIR)$(pgxsdir)/$(subdir)/nls-global.mk'

installdirs: installdirs-local

installdirs-local:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall: uninstall-local

uninstall-local:
	rm -f $(addprefix '$(DESTDIR)$(pgxsdir)/$(subdir)'/, Makefile.global Makefile.port Makefile.shlib nls-global.mk)

distprep:
	$(MAKE) -C test/isolation $@

ifeq ($(enable_multiple_nodes), yes)
clean:
	$(MAKE) -C common $@
	$(MAKE) -C $(top_builddir)/../distribute/lib $@
	$(MAKE) -C lib $@
	$(MAKE) -C $(top_builddir)/../distribute/test $@
	$(MAKE) -C common/tutorial NO_PGXS=1 $@

distclean maintainer-clean:
	$(MAKE) -C $(top_builddir)/../distribute/test $@
	$(MAKE) -C common/tutorial NO_PGXS=1 $@
	rm -f Makefile.port Makefile.global

coverage:
	$(MAKE) -C lib $@
	$(MAKE) -C common $@
	$(MAKE) -C $(top_builddir)/../distribute/lib $@
	$(MAKE) -C cm $@	
	$(MAKE) -C gausskernel $@
	$(MAKE) -C common/utils/mb/conversion_procs $@
	$(MAKE) -C common/snowball $@
	$(MAKE) -C test/whitebox $@
	$(MAKE) -C bin $@
else
clean:
	$(MAKE) -C common $@
	$(MAKE) -C lib $@
	$(MAKE) -C test $@
	$(MAKE) -C common/tutorial NO_PGXS=1 $@
	$(MAKE) -C test/isolation $@
	$(MAKE) -C test/thread $@
	$(MAKE) -C test/whitebox $@

distclean maintainer-clean:
	$(MAKE) -C test $@
	$(MAKE) -C common/tutorial NO_PGXS=1 $@
	$(MAKE) -C test/isolation $@
	$(MAKE) -C test/thread $@
	$(MAKE) -C test/whitebox $@
	rm -f Makefile.port Makefile.global

coverage:
	$(MAKE) -C lib $@
	$(MAKE) -C common $@
	$(MAKE) -C cm $@	
	$(MAKE) -C gausskernel $@
	$(MAKE) -C common/utils/mb/conversion_procs $@
	$(MAKE) -C common/snowball $@
	$(MAKE) -C test/whitebox $@
	$(MAKE) -C bin $@
endif

.PHONY: install-local installdirs-local uninstall-local
