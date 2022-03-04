#-------------------------------------------------------------------------
#
# Makefile for contrib/pgxc_clean
#
# Portions Copyright (c) 2011-2012 Postgres-XC Development Group
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "gs_clean - Abort prepared transaction for a Postgres-XC Coordinator"
PGAPPICON = win32

top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/pgxc_clean

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -fpic -D_FORTIFY_SOURCE=2 -O2
CFLAGS += -Wl,-z,relro,-z,now -s -ftrapv -fPIE
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif
LDFLAGS += -pie
PROGRAM= pgxc_clean

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= pgxc_clean.o txninfo.o $(top_builddir)/src/lib/elog/elog.a

all:gs_clean

gs_clean: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) gs_clean$(X) '$(DESTDIR)$(bindir)/gs_clean$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_clean$(X)'

clean distclean maintainer-clean:
	rm -f gs_clean$(X) $(OBJS) *.depend

#Include GTM objects
gtm_builddir = $(top_builddir)/src/gtm
EX_OBJS = $(gtm_builddir)/common/assert.o \
	  $(gtm_builddir)/client/libgtmclient.a

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
