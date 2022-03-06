#-------------------------------------------------------------------------
#
# Makefile for src/bin/psql
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/psql/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "gsql - the PostgreSQL interactive terminal"
PGAPPICON=win32

subdir = src/bin/psql
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

REFDOCDIR= $(top_srcdir)/doc/src/sgml/ref

MAKESGMLDIR = $(top_builddir)/src/common/pgxc/tools/makesgml
SGMLDIR= $(top_builddir)/doc/src/sgml

###################################################################
# libedit component
###################################################################
LIBEDIT_HOME = $(with_3rd)/$(BINARYPATH)/libedit/$(LIB_SUPPORT_LLT)
LIBEDIT_INCLUDE_PATH = $(LIBEDIT_HOME)/include
LIBEDIT_LIB_PATH = $(LIBEDIT_HOME)/lib

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) -I$(top_srcdir)/src/bin/pg_dump -DHAVE_CE -I$(LIBEDIT_INCLUDE_PATH) $(CPPFLAGS)

ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    CPPFLAGS += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif

$(top_builddir)/src/common/interfaces/libpq/client_logic_processor/stmt_processor.o:
	$(MAKE) -C $(top_builddir)/src/common/interfaces/libpq/client_logic_processor/ stmt_processor.o ENABLE_CE=1

describe.o:  $(top_builddir)/src/common/interfaces/libpq/client_logic_processor/stmt_processor.o

CFLAGS += -Wl,-z,relro,-z,now -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH)
LDFLAGS += -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH) -L$(LIBEDIT_LIB_PATH)
LIBS += -ledit -lcjson -lcurl
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif

##############################################################################
# memory checking component
###############################################################################
ifeq ($(enable_memory_check), yes)
  LIBS += -l$(MEMCHECK_LIB_NAME_ASAN)
  LDFLAGS += -L$(MEMCHECK_LIB_PATH) -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
  CXXFLAGS += -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
else
  ifeq ($(enable_thread_check), yes)
    LIBS += -l$(MEMCHECK_LIB_NAME_TSAN)
    LDFLAGS += -L$(MEMCHECK_LIB_PATH) -fsanitize=thread -fno-omit-frame-pointer
    CXXFLAGS += -fsanitize=thread -fno-omit-frame-pointer
  endif
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS=	command.o common.o help.o input.o stringutils.o mainloop.o copy.o \
	startup.o prompt.o variables.o large_obj.o print.o describe.o \
	mbprint.o dumputils.o keywords.o kwlookup.o tab-complete.o\
	sql_help.o \
	$(top_builddir)/src/lib/elog/elog.a \
	$(WIN32RES)
EXTRA_OBJS = $(top_builddir)/src/gausskernel/cbb/utils/aes/aes.o 

FLEXFLAGS = -Cfe -b -p -p

all: submake-aes gsql

libpq_pgport:=$(subst -lpq,-lpq_ce,$(libpq_pgport))

ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    libpq_pgport += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a

gsql: submake-libpq_ce submake-libpgport $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) $(EXTRA_OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -lncurses -o $@$(X)

help.o: sql_help.h

sqla: $(OBJS) 
	$(CC) -fPIC -shared $(CFLAGS) $(OBJS) $(EXTRA_OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -lncurses -o lib$@.so
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib
	
dumputils.cpp keywords.cpp: % : $(top_srcdir)/src/bin/pg_dump/%
	rm -f $@ && $(LN_S) $< .

kwlookup.cpp: % : $(top_srcdir)/src/common/backend/parser/%
	rm -f $@ && $(LN_S) $< .

sql_help.cpp: sql_help.h ;
sql_help.h: create_help.pl $(wildcard $(REFDOCDIR)/*.sgml)
	$(MAKE) -C $(MAKESGMLDIR)
	$(MAKE) -C $(SGMLDIR) sgml-files
	$(PERL) $< $(REFDOCDIR) $*

# psqlscan is compiled as part of mainloop
mainloop.o: psqlscan.inc

psqlscan.inc: psqlscan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	@if [ `wc -l <lex.backup` -eq 1 ]; then rm lex.backup; else echo "Scanner requires backup, see lex.backup."; exit 1; fi
else
	@$(missing) flex $< $@
endif

.PHONY: submake-aes
submake-aes:
	$(MAKE) -C $(top_builddir)/src/gausskernel/cbb/utils/aes aes.o
distprep: sql_help.h psqlscan.inc

install: all installdirs
	$(INSTALL_PROGRAM) gsql$(X) '$(DESTDIR)$(bindir)/gsql$(X)'
	$(INSTALL_DATA) $(srcdir)/psqlrc.sample '$(DESTDIR)$(datadir)/psqlrc.sample'
	$(INSTALL_DATA) retry_errcodes.conf '$(DESTDIR)$(bindir)/retry_errcodes.conf'
	 $(MAKE) -C $(libpq_builddir) install ENABLE_CE=1
	$(INSTALL_DATA) $(top_builddir)/src/common/interfaces/libpq/jdbc/client_logic_jni/libgauss_cl_jni.so$(X) '$(DESTDIR)$(bindir)/../lib/libgauss_cl_jni.so$(X)'
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gsql$(X)' '$(DESTDIR)$(datadir)/psqlrc.sample' '$(DESTDIR)$(bindir)/retry_errcodes.conf'

# psqlscan.inc is in the distribution tarball, so is not cleaned here
clean distclean:
	rm -f gsql$(X) $(OBJS) dumputils.cpp keywords.cpp kwlookup.cpp lex.backup *.depend
	$(MAKE) -C $(libpq_builddir) clean ENABLE_CE=1
maintainer-clean: distclean
	rm -f sql_help.h sql_help.cpp psqlscan.inc

