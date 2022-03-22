#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_probackup
#
# Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
#
#-------------------------------------------------------------------------

PROGRAM = gs_probackup

# utils
OBJS = configuration.o json.o logger.o \
	parray.o pgut.o thread.o remote.o file.o file_gz.o psprintf.o \
        atomics.o s_lock.o

OBJS += backup.o catalog.o configure.o data.o \
	delete.o dir.o fetch.o help.o init.o merge.o \
	ptrack.o pg_probackup.o restore.o show.o util.o \
	validate.o parsexlog.o

OBJS += datapagemap.o receivelog.o streamutil.o \
	xlogreader.o pg_lzcompress.o
OBJS += $(top_builddir)/src/lib/pgcommon/libpgcommon.a

EXTRA_CLEAN = datapagemap.cpp datapagemap.h \
	receivelog.cpp receivelog.h streamutil.cpp streamutil.h \
	xlogreader.cpp instr_time.h

subdir = src/bin/pg_probackup
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
EXTRA_CLEAN += logging.h

LDFLAGS += -L$(LZ4_LIB_PATH)
LIBS += -llz4
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif
PG_CPPFLAGS = -I$(libpq_srcdir) ${PTHREAD_CFLAGS} -Isrc -I$(top_builddir)/$(subdir) -I$(LZ4_INCLUDE_PATH) -I$(ZLIB_INCLUDE_PATH)
# add page_compression so .h
LDFLAGS += -L../../lib/page_compression
PG_CPPFLAGS = -I../../lib/page_compression
LIBS += -lpagecompression -lzstd
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS) -DHAVE_LIBZ
PG_LIBS_INTERNAL = $(libpq_pgport) ${PTHREAD_CFLAGS}

all: submake-pagecompression $(PROGRAM)

gs_probackup: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

configuration.o: datapagemap.h
backup.o: receivelog.h streamutil.h
ifneq (,$(wildcard $(top_builddir)/src/bin/pg_rewind/logging.h))
datapagemap.o: logging.h
endif
$(top_builddir)/src/lib/pgcommon/libpgcommon.a:
	$(MAKE) -C $(top_builddir)/src/lib/pgcommon libpgcommon.a
atomics.h: $(top_builddir)/src/include/utils/atomics.h
	rm -f $@ && $(LN_S) $(top_builddir)/src/include/utils/atomics.h $@
instr_time.h: $(top_builddir)/src/include/portability/instr_time.h
	rm -f $@ && $(LN_S) $(top_builddir)/src/include/portability/instr_time.h $@
datapagemap.cpp: $(top_builddir)/src/bin/pg_rewind/datapagemap.cpp
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_rewind/datapagemap.cpp $@
datapagemap.h: $(top_builddir)/src/bin/pg_rewind/datapagemap.h
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_rewind/datapagemap.h $@
receivelog.cpp: $(top_builddir)/src/bin/pg_basebackup/receivelog.cpp
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_basebackup/receivelog.cpp $@
ifneq (,$(wildcard $(top_builddir)/src/bin/pg_basebackup/walmethods.c))
receivelog.h: src/walmethods.h $(top_builddir)/src/bin/pg_basebackup/receivelog.h
else
receivelog.h: $(top_builddir)/src/bin/pg_basebackup/receivelog.h
endif
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_basebackup/receivelog.h $@
streamutil.cpp: $(top_builddir)/src/bin/pg_basebackup/streamutil.cpp
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_basebackup/streamutil.cpp $@
streamutil.h: $(top_builddir)/src/bin/pg_basebackup/streamutil.h
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_basebackup/streamutil.h $@
xlogreader.cpp: $(top_builddir)/src/gausskernel/storage/access/transam/xlogreader.cpp
	rm -f $@ && $(LN_S) $(top_builddir)/src/gausskernel/storage/access/transam/xlogreader.cpp $@
logging.h: $(top_builddir)/src/bin/pg_rewind/logging.h
	rm -f $@ && $(LN_S) $(top_builddir)/src/bin/pg_rewind/logging.h $@

ifeq ($(PORTNAME), aix)
	CC=xlc_r
endif

install: all installdirs
	$(INSTALL_PROGRAM) gs_probackup$(X) '$(DESTDIR)$(bindir)/gs_probackup$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_probackup$(X)'
.PHONY : clean
clean distclean maintainer-clean:
	rm -f gs_probackup $(OBJS) $(EXTRA_CLEAN)
