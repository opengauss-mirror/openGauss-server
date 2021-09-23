# ---------------------------------------------------------------------------------------
# 
# Makefile
#        Makefile for gs_guc
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
# 
# IDENTIFICATION
#        src/bin/gs_guc/Makefile
# 
# ---------------------------------------------------------------------------------------

top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/gs_guc

override CPPFLAGS := -I$(LIBOPENSSL_INCLUDE_PATH) -I$(libpq_srcdir) $(CPPFLAGS)
CFLAGS += -Wl,-z,relro,-z,now

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

COMMON_OBJS = $(top_builddir)/src/lib/elog/elog.a

OBJS = pg_guc.o cluster_config.o cluster_guc.o pg_hba.o $(top_builddir)/src/lib/elog/elog.a \
	$(top_builddir)/src/lib/config/libconfig.a

all:gs_guc encrypt

gs_guc: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LIBS) $(pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)
encrypt: encrypt.o crypt.o $(COMMON_OBJS)| submake-libpgport
	$(CC) $(CFLAGS) $^ $(LIBS) $(pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

gs_guc_ut: $(OBJS) | submake-libpgport
	$(CC)  -fPIC -shared $(CFLAGS) $(OBJS) $(LIBS) $(pgport) $(LDFLAGS) $(LDFLAGS_EX) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib
	
install: all installdirs
	$(INSTALL_PROGRAM) gs_guc$(X) '$(DESTDIR)$(bindir)/gs_guc$(X)'
	$(INSTALL_PROGRAM) encrypt$(X) '$(DESTDIR)$(bindir)/encrypt$(X)'
ifeq ($(enable_multiple_nodes), yes)
	$(INSTALL_DATA) $(top_builddir)/../distribute/cluster_guc.conf$(X) '$(DESTDIR)$(bindir)/cluster_guc.conf$(X)'
else
	$(INSTALL_DATA) cluster_guc.conf$(X) '$(DESTDIR)$(bindir)/cluster_guc.conf$(X)'
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_guc$(X)'
	rm -f '$(DESTDIR)$(bindir)/encrypt$(X)'

clean distclean maintainer-clean:
	rm -f gs_guc$(X) $(OBJS) *.depend crypt.o encrypt.o  encrypt

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
	
$(top_builddir)/src/lib/config/libconfig.a:
	$(MAKE) -C $(top_builddir)/src/lib/config libconfig.a
# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
