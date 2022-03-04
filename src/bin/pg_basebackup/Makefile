# ---------------------------------------------------------------------------------------
# 
# Makefile
#     Makefile for src/bin/pg_basebackup
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# 
# IDENTIFICATION
#        src/bin/pg_basebackup/Makefile
# 
# ---------------------------------------------------------------------------------------

PGFILEDESC = "gs_basebackup - takes a streaming base backup of a PostgreSQL instance"
PGAPPICON=win32

subdir = src/bin/pg_basebackup
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -I$(ZLIB_INCLUDE_PATH) $(CPPFLAGS) -DHAVE_LIBZ -fPIC -fPIE -DFRONTEND -I$(top_builddir)/src/bin/pg_rewind -I$(top_builddir)/src/bin/pg_ctl -I$(LZ4_INCLUDE_PATH)

LDFLAGS += -Wl,-z,relro,-z,now -L$(LZ4_LIB_PATH)
LIBS += -llz4
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

ifeq ($(enable_mot), yes)
OBJS=receivelog.o streamutil.o $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a $(top_builddir)/src/bin/pg_ctl/fetchmot.o \
     xlogreader.o  $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a $(top_builddir)/src/lib/build_query/libbuildquery.a \
     $(top_builddir)/src/lib/pgcommon/libpgcommon.a \
     $(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a \
	 $(top_builddir)/src/common/backend/lib/string.o
else
OBJS=receivelog.o streamutil.o $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a \
     xlogreader.o  $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a $(top_builddir)/src/lib/build_query/libbuildquery.a \
     $(top_builddir)/src/lib/pgcommon/libpgcommon.a \
     $(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a \
	 $(top_builddir)/src/common/backend/lib/string.o
endif


all: gs_basebackup pg_receivexlog pg_recvlogical

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a

gs_basebackup: pg_basebackup.o $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) pg_basebackup.o $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

pg_receivexlog: pg_receivexlog.o $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) pg_receivexlog.o $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

pg_recvlogical: pg_recvlogical.o $(OBJS) $(top_builddir)/src/lib/pgcommon/libpgcommon.a | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) pg_recvlogical.o $(OBJS) $(top_builddir)/src/lib/pgcommon/libpgcommon.a $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/transam/%
	rm -f $@ && $(LN_S) $< .


install: all installdirs
	$(INSTALL_PROGRAM) gs_basebackup$(X) '$(DESTDIR)$(bindir)/gs_basebackup$(X)'
	$(INSTALL_PROGRAM) pg_receivexlog$(X) '$(DESTDIR)$(bindir)/pg_receivexlog$(X)'
	$(INSTALL_PROGRAM) pg_recvlogical$(X) '$(DESTDIR)$(bindir)/pg_recvlogical$(X)'
	rm -f '$(DESTDIR)$(bindir)/gs_tar$(X)'
	ln -s gs_basebackup$(X) '$(DESTDIR)$(bindir)/gs_tar$(X)'
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_tar$(X)'
	rm -f '$(DESTDIR)$(bindir)/gs_basebackup$(X)'
	rm -f '$(DESTDIR)$(bindir)/pg_receivexlog$(X)'
	rm -f '$(DESTDIR)$(bindir)/pg_recvlogical$(X)'

clean distclean maintainer-clean:
	rm -f gs_basebackup$(X) pg_receivexlog$(X) pg_recvlogical$(X) gs_tar$(X) $(OBJS) \
		pg_basebackup.o pg_receivexlog.o pg_recvlogical.o *.depend

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/build_query/libbuildquery.a:
		$(MAKE) -C $(top_builddir)/src/lib/build_query libbuildquery.a

$(top_builddir)/src/lib/pgcommon/libpgcommon.a:
		$(MAKE) -C $(top_builddir)/src/lib/pgcommon libpgcommon.a

ifeq ($(enable_mot), yes)
$(top_builddir)/src/bin/pg_ctl/fetchmot.o:
		$(MAKE) -C $(top_builddir)/src/bin/pg_ctl fetchmot.o
endif

$(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a:
		$(MAKE) -C $(top_builddir)/src/lib/hotpatch/client libhotpatchclient.a

