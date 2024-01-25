#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_dump
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_dump/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "gs_dump/gs_restore/gs_dumpall - backup and restore PostgreSQL databases"
PGAPPICON=win32

subdir = src/bin/pg_dump
top_builddir = ../../..
ifdef LLT
	LLT_LIBS_PATH = /home/vignesh/LLT/hutaf_llt/linux_avatar_64_dumprestore/
	LLT_LIB = NCSCore_suse10_x86-64
endif

include $(top_builddir)/src/Makefile.global

ifeq ($(enable_lite_mode), no)
ifdef LLT
	CC = /home/vignesh/LLT/hutaf_llt/g++
	LIBS += -lNCSCore_suse10_x86-64
	override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -DGSDUMP_LLT -DHAVE_LIBZ -DHAVE_CE
else
	override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -DHAVE_LIBZ -DHAVE_CE
	CC = g++
endif
else
ifdef LLT
	CC = /home/vignesh/LLT/hutaf_llt/g++
	LIBS += -lNCSCore_suse10_x86-64
	override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -DGSDUMP_LLT -DHAVE_LIBZ
else
	override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -DHAVE_LIBZ
	CC = g++
endif
endif

override CPPFLAGS := -I$(ZLIB_INCLUDE_PATH) -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH) $(CPPFLAGS)
override LDFLAGS := -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH) $(LDFLAGS)
ifdef LLT
    override LIBS := -L${LLT_LIBS_PATH} $(LIBS)
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS=	pg_backup_archiver.o pg_backup_db.o pg_backup_custom.o \
	pg_backup_null.o pg_backup_tar.o \
	pg_backup_directory.o dumpmem.o dumputils.o compress_io.o $(WIN32RES)
	
LIBS += -lcjson -lcurl -lz
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif
COMMON_OBJS = $(top_builddir)/src/lib/elog/elog.a

EXTRA_OBJS =  $(top_builddir)/src/gausskernel/cbb/utils/aes/aes.o
KEYWRDOBJS = keywords.o kwlookup.o

keywords.o: $(top_srcdir)/src/include/parser/kwlist_d.h

kwlookup.cpp: % : $(top_srcdir)/src/common/backend/parser/%
	rm -f $@ && $(LN_S) $< .

all: submake-aes gs_dump gs_restore gs_dumpall
ifeq ($(enable_lite_mode), no)
libpq_pgport:=$(subst -lpq,-lpq_ce,$(libpq_pgport))
endif
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    libpq_pgport += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -l$(SECURE_C_CHECK) -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif

ifeq ($(enable_lite_mode), no)
gs_dump: pg_dump.o common.o pg_dump_sort.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq_ce submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_dump.o common.o pg_dump_sort.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

gs_dump_ut: pg_dump.o common.o pg_dump_sort.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq_ce submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(MY_CFLAGS) pg_dump.o common.o pg_dump_sort.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib

gs_restore: pg_restore.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq_ce submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_restore.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

gs_dumpall: pg_dumpall.o dumputils.o dumpmem.o $(COMMON_OBJS) $(KEYWRDOBJS) | submake-libpq_ce submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_dumpall.o dumputils.o dumpmem.o $(COMMON_OBJS) $(KEYWRDOBJS)  $(EXTRA_OBJS) $(WIN32RES) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX)  -o $@$(X)

gs_dumpall_ut: pg_dumpall.o $(OBJS) $(COMMON_OBJS) $(KEYWRDOBJS) | submake-libpq_ce submake-libpgport
	$(CC)  -fPIC -shared $(CXXFLAGS) $(MY_CFLAGS) pg_dumpall.o $(OBJS) $(COMMON_OBJS) $(KEYWRDOBJS)  $(EXTRA_OBJS) $(WIN32RES) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib
else
gs_dump: pg_dump.o common.o pg_dump_sort.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_dump.o common.o pg_dump_sort.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

gs_dump_ut: pg_dump.o common.o pg_dump_sort.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(MY_CFLAGS) pg_dump.o common.o pg_dump_sort.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib

gs_restore: pg_restore.o $(OBJS) $(KEYWRDOBJS) $(COMMON_OBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_restore.o $(KEYWRDOBJS) $(OBJS) $(EXTRA_OBJS) $(COMMON_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

gs_dumpall: pg_dumpall.o dumputils.o dumpmem.o $(COMMON_OBJS) $(KEYWRDOBJS) | submake-libpq submake-libpgport
	$(CC) $(CXXFLAGS) $(MY_CFLAGS) pg_dumpall.o dumputils.o dumpmem.o $(COMMON_OBJS) $(KEYWRDOBJS)  $(EXTRA_OBJS) $(WIN32RES) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX)  -o $@$(X)

gs_dumpall_ut: pg_dumpall.o $(OBJS) $(COMMON_OBJS) $(KEYWRDOBJS) | submake-libpq submake-libpgport
	$(CC)  -fPIC -shared $(CXXFLAGS) $(MY_CFLAGS) pg_dumpall.o $(OBJS) $(COMMON_OBJS) $(KEYWRDOBJS)  $(EXTRA_OBJS) $(WIN32RES) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib
endif

.PHONY: submake-aes
submake-aes:
	$(MAKE) -C $(top_builddir)/src/gausskernel/cbb/utils/aes aes.o
	
install: all installdirs
	$(INSTALL_PROGRAM) gs_dump$(X) '$(DESTDIR)$(bindir)'/gs_dump$(X)
	$(INSTALL_PROGRAM) gs_restore$(X) '$(DESTDIR)$(bindir)'/gs_restore$(X)
	$(INSTALL_PROGRAM) gs_dumpall$(X) '$(DESTDIR)$(bindir)'/gs_dumpall$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, gs_dump$(X) gs_restore$(X) gs_dumpall$(X))

clean distclean maintainer-clean:
	rm -f gs_dump$(X) gs_restore$(X) gs_dumpall$(X) $(OBJS) pg_dump.o common.o pg_dump_sort.o pg_restore.o pg_dumpall.o kwlookup.cpp $(KEYWRDOBJS) *.depend

clean_gs_dump:
	rm -f gs_dump$(X) $(OBJS) pg_dump.o common.o pg_dump_sort.o pg_dumpall.o kwlookup.cpp $(KEYWRDOBJS)

clean_gs_dumpall:
	rm -f gs_dumpall$(X) $(OBJS) pg_dump.o common.o pg_dump_sort.o pg_dumpall.o kwlookup.cpp $(KEYWRDOBJS)

clean_gs_restore:
	rm -f gs_restore$(X) $(OBJS) common.o pg_restore.o kwlookup.cpp $(KEYWRDOBJS)

# Be sure that the necessary archives are compiled
$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
