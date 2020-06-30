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
LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

EXTRA_OBJS =  $(top_builddir)/src/gausskernel/cbb/utils/aes/aes.o

OBJS = pg_guc.o cluster_config.o cluster_guc.o pg_hba.o $(top_builddir)/src/lib/elog/elog.a \
	$(top_builddir)/src/lib/config/libconfig.a $(EXTRA_OBJS)

all:gs_guc encrypt

gs_guc: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LIBS) $(pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

encrypt: encrypt.o crypt.o | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LIBS) $(pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) gs_guc$(X) '$(DESTDIR)$(bindir)/gs_guc$(X)'
	$(INSTALL_PROGRAM) encrypt$(X) '$(DESTDIR)$(bindir)/encrypt$(X)'
	$(INSTALL_DATA) cluster_guc.conf$(X) '$(DESTDIR)$(bindir)/cluster_guc.conf$(X)'

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
