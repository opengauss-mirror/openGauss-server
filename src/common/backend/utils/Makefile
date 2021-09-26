#
# Makefile for utils
#
# src/common/backend/utils/Makefile
#

subdir = src/common/backend/utils
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS        = fmgrtab.o
SUBDIRS     = adt cache error fmgr hash init mb misc mmgr resowner sort time

# location of Catalog.pm
catalogdir  = $(top_srcdir)/src/common/backend/catalog

include $(top_srcdir)/src/gausskernel/common.mk

all: errcodes.h fmgroids.h probes.h

$(SUBDIRS:%=%-recursive): fmgroids.h

# see explanation in ../parser/Makefile
fmgroids.h: fmgrtab.cpp
ifeq ($(enable_multiple_nodes), yes)
  fmgrtab.cpp: Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/../distribute/kernel/catalog/distribute_builtin_funcs.ini
	  $(PERL)  $< $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/../distribute/kernel/catalog/distribute_builtin_funcs.ini
  pg_builtin_proc.h: Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/../distribute/kernel/catalog/distribute_builtin_funcs.ini
	  $(PERL)  $< $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/../distribute/kernel/catalog/distribute_builtin_funcs.ini
else
  fmgrtab.cpp: Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
	  $(PERL)  $< $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
  pg_builtin_proc.h: Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
	  $(PERL)  $< $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
endif


errcodes.h: $(top_srcdir)/src/common/backend/utils/errcodes.txt generate-errcodes.pl
	$(PERL) $(srcdir)/generate-errcodes.pl $< > $@

ifneq ($(enable_dtrace), yes)
probes.h: Gen_dummy_probes.sed
endif

probes.h: probes.d
ifeq ($(enable_dtrace), yes)
	$(DTRACE) -C -h -s $< -o $@.tmp
	sed -e 's/POSTGRESQL_/TRACE_POSTGRESQL_/g' $@.tmp >$@
	rm $@.tmp
else
	sed -f $(srcdir)/Gen_dummy_probes.sed $< >$@
endif


# fmgroids.h, fmgrtab.cpp  errcodes.h and pg_builtin_proc.h are in the distribution tarball, so they
# are not cleaned here.
clean:
	rm -f probes.h

maintainer-clean: clean
	rm -f fmgroids.h fmgrtab.cpp errcodes.h pg_builtin_proc.h
