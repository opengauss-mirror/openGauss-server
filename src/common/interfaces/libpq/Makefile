#-------------------------------------------------------------------------
#
# Makefile for src/common/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq
top_builddir = ../../../../
backenddir = $(top_builddir)/src/common/backend
include $(top_builddir)/src/Makefile.global


# shared library parameters
NAME= pq
SO_MAJOR_VERSION= 5
SO_MINOR_VERSION= 5

DOXYGEN="$(top_builddir)/../$(BUILD_TOOLS_PATH)/doxygen/bin/doxygen"
override CPPFLAGS :=  -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/common/port -I$(top_srcdir)/src/common/port
override CPPFLAGS += -DPGXC -I$(srcdir) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/include/libpq/
override CPPFLAGS += -I$(top_builddir)/$(subdir)/client_logic_hooks
override CPPFLAGS += -I$(top_builddir)/$(subdir)/client_logic_hooks/cmk_entity_manager_hooks
override CPPFLAGS += -I$(top_builddir)/$(subdir)/client_logic_hooks/encryption_hooks
ifeq ($(enable_lite_mode), no)
override CPPFLAGS += -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH) -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH) -lcjson -lcurl
endif

ifeq "$(ENABLE_CE)" "1"
override CPPFLAGS += -DHAVE_CE -DWORDS_BIGENDIAN
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    override CPPFLAGS += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif
endif

ifneq ($(PORTNAME), win32)
override CXXFLAGS += $(PTHREAD_CFLAGS)
endif

# Need to recompile any external C files because we need
# all object files to use the same compile flags as libpq; some
# platforms require special flags.
LIBS := $(LIBS:-lpgport=)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all 
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=	fe-auth.o fe-connect.o fe-exec.o fe-misc.o fe-print.o fe-lobj.o \
	fe-protocol2.o fe-protocol3.o pqexpbuffer.o pqsignal.o fe-secure.o \
	libpq-events.o
# libpgport C files we always use
OBJS += chklocale.o inet_net_ntop.o noblock.o pgstrcasecmp.o thread.o cipher.o path.o pgsleep.o
# libpgport C files that are needed if identified by configure
OBJS += $(filter crypt.o getaddrinfo.o getpeereid.o inet_aton.o open.o snprintf.o strerror.o strlcpy.o win32error.o win32setlocale.o, $(LIBOBJS))
# backend/libpq
OBJS += ip.o md5.o sha2.o
# utils/mb
ifeq ($(PORTNAME), win32)
OBJS += encnames.o wchar.o
else
OBJS += encnames.o
endif

# thread fix 
OBJS += gs_strerror.o gs_env_r.o gs_syscall_lock.o gs_readdir.o


ifeq ($(PORTNAME), cygwin)
override shlib = cyg$(NAME)$(DLSUFFIX)
endif

ifeq ($(PORTNAME), win32)
# pgsleep.o is from libpgport
OBJS += pgsleep.o win32.o libpqrc.o

libpqrc.o: libpq.rc
	$(WINDRES) -i $< -o $@

ifeq ($(enable_thread_safety), yes)
OBJS += pthread-win32.o
endif
endif


# Add libraries that libpq depends (or might depend) on into the
# shared library link.  (The order in which you list them here doesn't
# matter.)
ifneq ($(PORTNAME), win32)
SHLIB_LINK += $(filter  -lcrypt -ldes -lcom_err -lcrypto -lssl -lk5crypto -lkrb5 -lgssapi_krb5 -lgss -lgssapi -lsocket -lnsl -lresolv -lintl -l$(SECURE_C_CHECK), $(LIBS)) $(LDAP_LIBS_FE) $(PTHREAD_LIBS)
else
SHLIB_LINK += $(filter  -lcrypt -ldes -lcom_err -lcrypto -lssl -lk5crypto -lkrb5 -lgssapi32 -lsocket -lnsl -lresolv -lintl -l$(SECURE_C_CHECK) $(PTHREAD_LIBS), $(LIBS)) $(LDAP_LIBS_FE)
endif
ifeq ($(enable_lite_mode), no)
    SHLIB_LINK += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif
ifeq ($(PORTNAME), win32)
SHLIB_LINK += -lshfolder -lwsock32 -lws2_32 -lsecur32 $(filter -leay32 -lssleay32 -lcomerr32 -lkrb5_32, $(LIBS))
endif

SHLIB_LINK +=  -lssl -lcrypto

.PHONY: subsystem app-lib-depends ce_docs no_parser

SHLIB_EXPORTS = exports.txt

ifeq "$(ENABLE_CE)" "1"
SUBDIRS = frontend_parser client_logic_common client_logic_expressions client_logic_cache client_logic_processor client_logic_fmt client_logic_hooks client_logic_data_fetcher
encryption_hooks_files := $(shell find client_logic_hooks/encryption_hooks -name '*.cpp' | sort)
cmk_entity_manager_hooks_files := $(shell find client_logic_hooks/cmk_entity_manager_hooks -maxdepth 1 -name '*.cpp' | sort)

OBJS += $(encryption_hooks_files:.cpp=.o)
OBJS += $(cmk_entity_manager_hooks_files:.cpp=.o)
endif

ifneq ($(enable_ut), yes)
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
ifneq ($(enable_lite_mode), yes)
  OBJS := $(filter-out client_logic_hooks/cmk_entity_manager_hooks/register_local_kms.o, $(OBJS))
else
  OBJS := $(filter-out client_logic_hooks/cmk_entity_manager_hooks/register_huawei_kms.o, $(OBJS))
  OBJS := $(filter-out client_logic_hooks/cmk_entity_manager_hooks/register_gs_ktool.o, $(OBJS))
endif
else
OBJS := $(filter-out client_logic_hooks/cmk_entity_manager_hooks/register_huawei_kms.o, $(OBJS))
OBJS := $(filter-out client_logic_hooks/cmk_entity_manager_hooks/register_gs_ktool.o, $(OBJS))
endif
endif


files:= $(foreach DIR,$(SUBDIRS),  $(wildcard $(DIR)/*.cpp) )
ifneq "$(ENABLE_CE)" "1"
files += $(srcdir)/frontend_parser/fe-wchar.o $(srcdir)/frontend_parser/frontend_mbutils.o $(srcdir)/frontend_parser/wstrncmp.cpp
OBJS += $(files:.cpp=.o)
else
CE_OBJS+=$(patsubst fe-%.o,libpq_ce/fe-%.o,$(OBJS))
mains:= $(foreach DIR,$(SUBDIRS),  $(wildcard $(DIR)/main.cpp))
files:= $(filter-out $(mains), $(files))
CE_OBJS += $(files:.cpp=.o)
CE_OBJS += cl_state.o

OBJS:=$(CE_OBJS)
NAME:=pq_ce

gs_ktool:
ifneq ($(enable_lite_mode), yes)
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
	$(MAKE) -C $(top_builddir)/../distribute/bin/gs_ktool
endif
endif

libpq_ce/fe-protocol3.o libpq_ce/fe-exec.o: client_logic_cache/types_to_oid.h
libpq_ce/fe-%.o: fe-%.cpp
	$(CC) -c -o $@ $(CFLAGS) $(CPPFLAGS)  $(CXXFLAG) $<
	
ce_docs:
	@$(DOXYGEN) cl_doxy
	@firefox $(top_builddir)/src/common/interfaces/libpq/gs_cl_docs/html/index.html&
libpq_ce: 
	@- if ! test -e libpq_ce; then mkdir libpq_ce; fi

endif


frontend_parser/gram.hpp:
	$(MAKE) -C frontend_parser gram.hpp
frontend_parser/parser.o frontend_parser/kwlookup.o:frontend_parser/gram.hpp


frontend_parser_basic:
	make fe-wchar.cpp -C frontend_parser

subsystem: 
	$(foreach DIR,$(SUBDIRS),make ENABLE_CE=1 -C $(DIR); )
#libpq.a: subsystem  all-lib

ifneq "$(ENABLE_CE)" "1"
all-lib-depends: frontend_parser_basic
	$(MAKE) all-lib
else
all-lib-depends: gs_ktool libpq_ce subsystem
	$(MAKE) all-lib
endif

all: all-lib-depends

utlibpq: all-lib
	cp libpq.so $(top_builddir)/../distribute/test/ut/lib/libutlibpq.so

utlibpq_ce: all-lib
	cp libpq_ce.so $(top_builddir)/../distribute/test/ut/lib/libutlibpq_ce.so

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib
backend_src = $(top_srcdir)/src/common/backend


# We use several backend modules verbatim, but since we need to
# compile with appropriate options to build a shared lib, we can't
# necessarily use the same object files as the backend uses. Instead,
# symlink the source files in here and build our own object file.
# For some libpgport modules, this only happens if configure decides
# the module is needed (see filter hack in OBJS, above).

chklocale.cpp crypt.cpp getaddrinfo.cpp getpeereid.cpp inet_aton.cpp inet_net_ntop.cpp noblock.cpp open.cpp pgsleep.cpp pgstrcasecmp.cpp cipher.cpp path.cpp snprintf.cpp strerror.cpp strlcpy.cpp thread.cpp win32error.cpp win32setlocale.cpp: % : $(top_srcdir)/src/common/port/%
	rm -f $@ && $(LN_S) $< .

ip.cpp md5.cpp sha2.cpp: % : $(backend_src)/libpq/%
	rm -f $@ && $(LN_S) $< .

encnames.cpp wchar.cpp: % : $(backend_src)/utils/mb/%
	rm -f $@ && $(LN_S) $< .

gs_strerror.cpp gs_env_r.cpp gs_syscall_lock.cpp gs_readdir.cpp: % : $(backend_src)/../port/%
	rm -f $@ && $(LN_S) $< .

distprep: libpq-dist.rc

libpq.rc libpq-dist.rc: libpq.rc.in
	sed -e 's/\(VERSION.*\),0 *$$/\1,'`date '+%y%j' | sed 's/^0*//'`'/' $< >$@

# Depend on Makefile.global to force rebuild on re-run of configure.
# (But libpq-dist.rc is shipped in the distribution for shell-less
# installations and is only updated by distprep.)
libpq.rc: $(top_builddir)/src/Makefile.global

fe-connect.o: fe-connect.cpp $(top_builddir)/src/common/port/pg_config_paths.h
fe-misc.o: fe-misc.cpp $(top_builddir)/src/common/port/pg_config_paths.h

$(top_builddir)/src/common/port/pg_config_paths.h:
	$(MAKE) -C $(top_builddir)/src/common/port pg_config_paths.h

install: all installdirs install-lib
	$(INSTALL_DATA) $(top_builddir)/src/include/libpq/libpq-fe.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) $(top_builddir)/src/include/libpq/libpq-events.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) $(top_builddir)/src/include/libpq/libpq-int.h '$(DESTDIR)$(includedir_internal)'
	$(INSTALL_DATA) $(top_builddir)/src/include/libpq/pqexpbuffer.h '$(DESTDIR)$(includedir_internal)'
	$(INSTALL_DATA) $(srcdir)/pg_service.conf.sample '$(DESTDIR)$(datadir)/pg_service.conf.sample'
	$(MKDIR_P) '$(DESTDIR)$(includedir)/../etc/localkms'

installcheck:
	$(MAKE) -C test $@

installdirs: installdirs-lib
	$(MKDIR_P) '$(DESTDIR)$(includedir)' '$(DESTDIR)$(includedir_internal)'

uninstall: uninstall-lib
	rm -f '$(DESTDIR)$(includedir)/libpq-fe.h'
	rm -f '$(DESTDIR)$(includedir)/libpq-events.h'
	rm -f '$(DESTDIR)$(includedir_internal)/libpq-int.h'
	rm -f '$(DESTDIR)$(includedir_internal)/pqexpbuffer.h'
	rm -f '$(DESTDIR)$(datadir)/pg_service.conf.sample'

clean_ce_docs:
	rm -rf gs_cl_docs

clean distclean: clean-lib
	$(MAKE) -C test $@
	rm -f $(OBJS) pthread.h libpq.rc
# Might be left over from a Win32 client-only build
	rm -f pg_config_paths.h
	rm -f inet_net_ntop.cpp noblock.cpp pgstrcasecmp.cpp thread.cpp cipher.cpp path.cpp
	rm -f chklocale.cpp crypt.cpp getaddrinfo.cpp getpeereid.cpp inet_aton.cpp open.cpp snprintf.cpp strerror.cpp strlcpy.cpp win32error.cpp win32setlocale.cpp
	rm -f pgsleep.cpp
	rm -f md5.cpp sha2.cpp ip.cpp
	rm -f encnames.cpp wchar.cpp
	rm -f gs_strerror gs_env_r.cpp gs_syscall_lock.cpp gs_readdir.cpp
	$(foreach DIR,$(SUBDIRS),make -C $(DIR) clean; )
maintainer-clean: distclean maintainer-clean-lib
	$(MAKE) -C test $@
	rm -f libpq-dist.rc
