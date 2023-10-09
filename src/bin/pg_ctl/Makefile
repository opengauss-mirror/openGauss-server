#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_ctl
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_ctl/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "gs_ctl - starts/stops/restarts the PostgreSQL server"
PGAPPICON=win32

subdir = src/bin/pg_ctl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -I$(ZLIB_INCLUDE_PATH) $(CPPFLAGS) -DHAVE_LIBZ -DFRONTEND -I$(top_builddir)/src/bin/pg_rewind -I${top_builddir}/src/lib/page_compression -I${top_builddir}/src/include
override LDFLAGS += -L$(LZ4_LIB_PATH) -L$(ZSTD_LIB_PATH) -L${top_builddir}/src/lib/page_compression
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss -lpagecompression -lzstd -llz4
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

ifeq ($(enable_mot), yes)
OBJS=	pg_ctl.o  pg_build.o fetchmot.o backup.o receivelog.o streamutil.o xlogreader.o xlogreader_common.o $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a $(top_builddir)/src/lib/build_query/libbuildquery.a \
             $(top_builddir)/src/bin/pg_rewind/pg_rewind.a $(top_builddir)/src/lib/pgcommon/libpgcommon.a \
             $(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a
else
OBJS=	pg_ctl.o  pg_build.o backup.o receivelog.o streamutil.o xlogreader.o xlogreader_common.o $(WIN32RES) $(top_builddir)/src/lib/elog/elog.a $(top_builddir)/src/lib/build_query/libbuildquery.a \
             $(top_builddir)/src/bin/pg_rewind/pg_rewind.a $(top_builddir)/src/lib/pgcommon/libpgcommon.a \
             $(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a
endif

OBJS += $(top_builddir)/src/gausskernel/storage/dss/dss_adaptor.o
OBJS += $(top_builddir)/src/gausskernel/storage/gs_uwal/gs_uwal_adaptor.o
override CXXFLAGS += -I${top_builddir}/src/include

all: submake-pagecompression gs_ctl

gs_ctl: $(OBJS) | submake-libpq submake-libpgport
	$(CC) -fPIC $(CXXFLAGS) $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

ctla: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib

xlogreader.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/transam/%
	rm -f $@ && $(LN_S) $< .
xlogreader_common.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/redo/%
	rm -f $@ && $(LN_S) $< .
install: all installdirs
	$(INSTALL_PROGRAM) gs_ctl$(X) '$(DESTDIR)$(bindir)/gs_ctl$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_ctl$(X)'

clean distclean maintainer-clean:
	rm -f gs_ctl$(X) $(OBJS) *.depend

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/build_query/libbuildquery.a:
	$(MAKE) -C $(top_builddir)/src/lib/build_query libbuildquery.a

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
$(top_builddir)/src/lib/pgcommon/libpgcommon.a:
	$(MAKE) -C $(top_builddir)/src/lib/pgcommon libpgcommon.a
$(top_builddir)/src/bin/pg_rewind/pg_rewind.a:
	$(MAKE) -C $(top_builddir)/src/bin/pg_rewind pg_rewind.a

$(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a:
	$(MAKE) -C $(top_builddir)/src/lib/hotpatch/client libhotpatchclient.a
