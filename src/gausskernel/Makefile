#-------------------------------------------------------------------------
#
# Makefile for the openGauss
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/gausskernel/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "PostgreSQL Server"
# This is a daemon process, which is why it is not labeled as an executable


subdir = src/gausskernel
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

override CXXFLAGS += $(PTHREAD_CFLAGS)

# these directories: catalog  lib  libpq nodes  parser  po  port  regex  snowball  tsearch  utils pgxc
# under ../common 
#
SUBDIRS = ../common/backend bootstrap cbb optimizer process dbmind runtime  security  storage \
	$(top_builddir)/src/common/timezone $(top_builddir)/src/common/interfaces/libpq \
	$(top_builddir)/contrib/file_fdw $(top_builddir)/contrib/hdfs_fdw \
	$(top_builddir)/contrib/log_fdw $(top_builddir)/contrib/test_decoding $(top_builddir)/contrib/mppdb_decoding \
	$(top_builddir)/contrib/postgres_fdw

ifeq ($(enable_mysql_fdw), yes)
	SUBDIRS += $(top_builddir)/contrib/mysql_fdw
endif

ifeq ($(enable_oracle_fdw), yes)
	SUBDIRS += $(top_builddir)/contrib/oracle_fdw
endif

ifeq ($(enable_pldebugger), yes)
	SUBDIRS += $(top_builddir)/contrib/pldebugger
endif

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS += $(top_builddir)/contrib/carbondata \
	../distribute/kernel ../distribute/kernel/extension/roach_api ../distribute/kernel/extension/dimsearch/main \
	../distribute/kernel/extension/tsdb ../distribute/kernel/extension/streaming

ifeq ($(enable_qunit), yes)
SUBDIRS += $(top_builddir)/src/distribute/test/QUnit/test/wrapper
endif
else
SUBDIRS += $(top_builddir)/contrib/gc_fdw
ifeq ($(enable_qunit), yes)
SUBDIRS += $(top_builddir)/src/test/QUnit/test/wrapper
endif
endif

include $(srcdir)/common.mk

# As of 1/2010:
# The probes.o file is necessary for dtrace support on Solaris, and on recent
# versions of systemtap.  (Older systemtap releases just produce an empty
# file, but that's okay.)  However, OS X's dtrace doesn't use it and doesn't
# even recognize the -G option.  So, build probes.o except on Darwin.
# This might need adjustment as other platforms add dtrace support.
ifneq ($(PORTNAME), darwin)
ifeq ($(enable_dtrace), yes)
LOCALOBJS += ../common/backend/utils/probes.o
endif
endif

OBJS = $(SUBDIROBJS) $(LOCALOBJS) \
	$(top_builddir)/src/common/interfaces/libpq/fe-connect.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-secure.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-misc.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-protocol3.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-protocol2.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-exec.o \
	$(top_builddir)/src/common/interfaces/libpq/fe-auth.o \
	$(top_builddir)/src/common/interfaces/libpq/pqexpbuffer.o \
	$(top_builddir)/src/common/port/libpgport_srv.a \
	$(top_builddir)/src/lib/alarm/alarmclient.a \
	$(top_builddir)/src/common/interfaces/libpq/libpq.a \
	$(top_builddir)/src/common/pl/plpgsql/src/libplpgsql.a

ifeq ($(enable_gstrace), yes)
   OBJS += $(top_builddir)/src/lib/gstrace/common/gstrace_infra.o
endif

ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
   OBJS += $(top_builddir)/src/distribute/lib/hotpatch/hotpatch.o \
        $(top_builddir)/src/distribute/lib/hotpatch/common/libhotpatchcommon.a 
endif

ifeq ($(enable_multiple_nodes), yes)
   OBJS += $(top_builddir)/src/distribute/gtm/client/libgtmclient.a \
        $(top_builddir)/src/distribute/gtm/common/libgtmcommon.a
endif

OBJS += $(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a

# We put libpgport into OBJS, so remove it from LIBS; also add libldap
LIBS := $(filter-out -lpgport, $(LIBS)) $(LDAP_LIBS_BE)

# the link options for llvm
LLVM_ZLIB_NAME=z
LLVMLIBS := -Wl,--start-group -l$(LLVM_ZLIB_NAME) -pthread -lncurses -lrt -ldl -lm $(LLVM_LIBS) -Wl,--end-group -ltinfo
# The kernel doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

##############################################################################
# libobs component
###############################################################################
LIBS += -L$(LIBCURL_LIB_PATH) -L$(LIBOPENSSL_LIB_PATH) 
LIBS += -leSDKOBS -leSDKLogAPI -lssl -lcrypto -lpcre -liconv -lnghttp2 -llog4cpp -lsecurec  -lcurl

##############################################################################
# libobs component
###############################################################################
ifeq ($(SUPPORT_HOTPATCH), yes)
    LIBS += -ldoprapatch
    LDFLAGS += -L$(LIBHOTPATCH_LIB_PATH)
    CXXFLAGS += -I$(LIBHOTPATCH_INCLUDE_PATH)
    ifeq ($(PLAT_FORM_STR), euleros2.0_sp8_aarch64)
        ifeq ($(enable_debug), no)
            HOTPATCH_ATOMIC_LDS = -Wl,-T$(LIBHOTPATCH_TOOL_PATH)/atomic.lds
        else
            HOTPATCH_ATOMIC_LDS = -Wl,-T$(LIBHOTPATCH_TOOL_PATH)/atomic_debug.lds
        endif
    endif
    CRC_LOG=crc.log
    CRC_CHECK=$(LIBHOTPATCH_TOOL_PATH)/makepatch -CHECK -CRC gaussdb > $(CRC_LOG)
endif

##########################################################################
# append lz4 for compression : lz4.a or lz4.a
############################################################################
LIBS += -llz4

##########################################################################
# append cjson for json parser : cjson.a or cjson.a
############################################################################
LIBS += -lcjson

##########################################################################
# append hll for compression: libhll.so
############################################################################
LIBS += -lhll

#############################################################################
# PLJava component
###############################################################################
PLJAVA_LIB_PATH = $(PLJAVA_HOME)/lib
PLJAVA_JAR_PATH = $(PLJAVA_HOME)/java
LIBPLJAVA = libpljava.so
JARPLJAVA = pljava.jar

##############################################################################
# MOT component
##############################################################################
ifeq ($(enable_mot), yes)
  LIBS += -latomic -lmot_engine -lmasstree
  LDFLAGS += -L$(top_builddir)/src/gausskernel/storage/mot/core/bin -L$(MASSTREE_LIB_PATH)
  CXXFLAGS += -I$(JEMALLOC_INCLUDE_PATH)
endif

##############################################################################
# jemalloc component
###############################################################################
ifneq ($(enable_thread_check), yes)
  ifeq ($(enable_jemalloc), yes)
    LIBS += -l$(JEMALLOC_LIB_NAME)
  endif
  ifeq ($(enable_jemalloc_debug), yes)
    LIBS += -l$(JEMALLOC_LIB_NAME)
  endif
endif

#############################################################################
# cgroup library
##############################################################################
LIBS += -lcgroup
LIBCGROUP=libcgroup.so

############################################################################
# link orc for gaussdb
############################################################################
LIBS += -lorc

############################################################################
# link parquet for gaussdb
############################################################################
LDFLAGS += -L$(LIBPARQUET_LIB_PATH)
LIBS += -lparquet

############################################################################
# link carbondata for gaussdb
############################################################################
ifeq ($(enable_multiple_nodes), yes)
LIBS += -lcarbondata
LDFLAGS += -L$(LIBCARBONDATA_LIB_PATH)
endif

############################################################################
# link thrift for gaussdb
############################################################################
LDFLAGS += -L$(LIBTHRIFT_LIB_PATH)
LIBS += -lthrift

############################################################################
# link snappy for gaussdb
############################################################################
SNAPPY_LIB_NAME=
ifeq ($(enable_llt), yes)
 SNAPPY_LIB_NAME=snappy_pic
else
 ifeq ($(enable_ut), yes)
  SNAPPY_LIB_NAME=snappy_pic
 else
  SNAPPY_LIB_NAME=snappy	
 endif
endif
LIBS += -l${SNAPPY_LIB_NAME}

############################################################################
# link zstd for gaussdb
############################################################################
LDFLAGS += -L$(ZSTD_LIB_PATH)
LIBS += -lzstd

################################################################################
# protobuf
################################################################################
PROTOBUF_LIB_NAME=
ifeq ($(enable_llt), yes)
 PROTOBUF_LIB_NAME=protobuf_pic
else
 ifeq ($(enable_ut), yes)
  PROTOBUF_LIB_NAME=protobuf_pic
 else
  PROTOBUF_LIB_NAME=protobuf	
 endif
endif

LIBS += -l${PROTOBUF_LIB_NAME}

##################################################################################
# kerberos
##################################################################################
LIBS += -lcom_err_gauss -lgssapi_krb5_gauss -lkrb5_gauss -lgssrpc_gauss -lk5crypto_gauss -lkadm5clnt_mit -lkadm5srv_mit -lkdb5 -lkrb5support_gauss

##################################################################################
# libstd
#################################################################################
LIBSTD_LIB_NAME=stdc++
LIBS += -l${LIBSTD_LIB_NAME}

##################################################################################
# boost
###################################################################################
ifeq ($(enable_llt), yes)
 LIBS += -lboost_thread_pic -lboost_chrono_pic  -lboost_system_pic -lboost_atomic
else
 ifeq ($(enable_ut), yes)
  LIBS += -lboost_thread_pic -lboost_chrono_pic  -lboost_system_pic -lboost_atomic
 else
  LIBS += -lboost_thread -lboost_chrono  -lboost_system -lboost_atomic
 endif
endif

################################################################################
# libxml2
################################################################################
ifeq ($(enable_llt), yes)
 LIBS += -lxml2_pic
else
 ifeq ($(enable_ut), yes)
  LIBS += -lxml2_pic
 else
  LIBS += -lxml2
 endif
endif

################################################################################
#openssl
###############################################################################
LIBS += -lssl -lcrypto

#for ADIO
LIBS += -laio

################################################################################
# grpc
################################################################################
LIBS += -lgrpc++ -lgpr -lgrpc -lupb -labsl_str_format_internal -labsl_strings -labsl_spinlock_wait 
LIBS += -labsl_throw_delegate -labsl_strings_internal -labsl_base -labsl_dynamic_annotations -labsl_int128 \
    -labsl_bad_optional_access -labsl_raw_logging_internal -labsl_log_severity -laddress_sorting
################################################################################
# for 3rd support
################################################################################
LDFLAGS += -L$(LIBLLVM_LIB_PATH) -L$(LIBTINFO_LIB_PATH) \
    -L$(LIBOBS_LIB_PATH)  \
    -L$(LIBCGROUP_LIB_PATH) -L$(LIBORC_LIB_PATH) \
    -L$(SNAPPY_LIB_PATH) -L$(PROTOBUF_LIB_PATH) \
    -L$(KERBEROS_LIB_PATH) -L$(LIBSTD_LIB_PATH) -L$(BOOST_LIB_PATH) \
    -L$(LIBXML_LIB_PATH) -L$(LIBOPENSSL_LIB_PATH) -L$(GRPC_LIB_PATH) \
    -L$(LIBCARBONDATA_LIB_PATH)

  # append lz4 for compression: lz4
  LDFLAGS += -L$(LZ4_LIB_PATH)
  CXXFLAGS+= -I$(LZ4_INCLUDE_PATH)

  # append cjson for json parser in c: cjson
  LDFLAGS += -L$(CJSON_LIB_PATH)
  CXXFLAGS+= -I$(CJSON_INCLUDE_PATH)

  # append hll for compression: hll
  LDFLAGS += -L$(HLL_LIB_PATH)
  CXXFLAGS+= -I$(HLL_INCLUDE_PATH)

  CXXFLAGS+= -I$(LIBOBS_INCLUDE_PATH) 

ifeq ($(enable_multiple_nodes), yes)
  LDFLAGS += -L$(top_builddir)/src/distribute/gtm/libpg
endif

ifneq (, $(findstring __USE_NUMA, $(CFLAGS)))
	LIBS += -lnuma
endif

##########################################################################

all: submake-libpgport submake-schemapg libgstrace submake-libalarmclient gaussdb $(POSTGRES_IMP)
ifeq ($(enable_multiple_nodes), yes)
all: submake-libcarbondata
endif

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

gaussdb: $(OBJS) |
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) $(LLVMLIBS) $(HOTPATCH_ATOMIC_LDS) -o $@
	$(CRC_CHECK)

db: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) $(LLVMLIBS) -o libdb.so
	mv libdb.so $(top_builddir)/src/distribute/test/ut/lib

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

postgres: $(OBJS) postgres.def libpostgres.a
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def postgres.def
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def postgres.def
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack,$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	rm -f $@.exp $@.base

postgres.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libpostgres.a: postgres.def
	$(DLLTOOL) --dllname postgres.exe --def postgres.def --output-lib $@

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

postgres: $(OBJS) postgres.def libpostgres.a $(WIN32RES)
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def postgres.def
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def postgres.def
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack=$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	rm -f $@.exp $@.base

postgres.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libpostgres.a: postgres.def
	$(DLLTOOL) --dllname postgres.exe --def postgres.def --output-lib $@

endif # win32

ifeq ($(PORTNAME), aix)

postgres: $(POSTGRES_IMP)
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$(OBJS)) -Wl,-bE:$(top_builddir)/src/gausskernel/$(POSTGRES_IMP) $(LIBS) -o $@

$(POSTGRES_IMP): $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(call expand_subsys,$^)
ifeq ($(host_os), aix3.2.5)
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
ifneq (,$(findstring aix4.1, $(host_os)))
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
	$(MKLDEXPORT) SUBSYS.o . > $@
endif
endif
	@rm -f SUBSYS.o

endif # aix

libgstrace:
	$(MAKE) -C $(top_builddir)/src/lib/gstrace

ifeq ($(enable_multiple_nodes), yes)
# Update the commonly used headers before building the subdirectories
$(SUBDIRS:%=%-recursive): $(top_builddir)/src/include/parser/gram.hpp $(top_builddir)/src/include/catalog/schemapg.h $(top_builddir)/src/common/backend/utils/fmgroids.h $(top_builddir)/src/common/backend/utils/pg_builtin_proc.h $(top_builddir)/src/include/utils/errcodes.h $(top_builddir)/src/include/utils/probes.h $(top_builddir)/src/include/parser/hint_gram.hpp $(top_builddir)/src/include/replication/repl_gram.hpp $(top_builddir)/src/include/replication/syncrep_gram.hpp $(top_builddir)/src/include/storage/lwlocknames.h $(top_builddir)/contrib/carbondata/libcarbondata.a

else
$(SUBDIRS:%=%-recursive): $(top_builddir)/src/include/parser/gram.hpp $(top_builddir)/src/include/catalog/schemapg.h $(top_builddir)/src/common/backend/utils/fmgroids.h $(top_builddir)/src/common/backend/utils/pg_builtin_proc.h $(top_builddir)/src/include/utils/errcodes.h $(top_builddir)/src/include/utils/probes.h $(top_builddir)/src/include/parser/hint_gram.hpp $(top_builddir)/src/include/replication/repl_gram.hpp $(top_builddir)/src/include/replication/syncrep_gram.hpp $(top_builddir)/src/include/storage/lwlocknames.h

endif

# run this unconditionally to avoid needing to know its dependencies here:
submake-schemapg: | submake-fmgroids
	$(MAKE) -C ../common/backend/catalog schemapg.h

# src/port needs a convenient way to force errcodes.h to get built
submake-errcodes: $(top_builddir)/src/include/utils/errcodes.h

# src/port needs lwlocknames.h
submake-lwlocknames: $(top_builddir)/src/include/storage/lwlocknames.h

# libplgsql.a needs a convenient way to force fmgroids.h to get built
submake-fmgroids: $(top_builddir)/src/common/backend/utils/fmgroids.h $(top_builddir)/src/include/utils/fmgroids.h

# run this to build pljava
submake-libpljava: | submake-fmgroids
	$(top_builddir)/third_party/dependency/pljava/build.sh -m only_so

# run this to build carbondata-sdk
$(top_builddir)/contrib/carbondata/libcarbondata.a: 
	$(MAKE) -C $(top_builddir)/contrib/carbondata all

.PHONY: submake-schemapg submake-errcodes

../common/backend/catalog/schemapg.h: | submake-schemapg

$(top_builddir)/src/common/port/libpgport_srv.a: | submake-libpgport

$(top_builddir)/src/lib/alarm/alarmclient.a: | submake-libalarmclient

$(top_builddir)/src/common/pl/plpgsql/src/libplpgsql.a: | submake-libplpgsql submake-fmgroids

submake-libplpgsql: | submake-fmgroids

# Those are rules to create dependent GTM libraries automatically
$(top_builddir)/src/common/interfaces/libpq/libpq.a:
	$(MAKE) -C $(top_builddir)/src/common/interfaces/libpq

ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
$(top_builddir)/src/distribute/gtm/common/libgtmcommon.a:
	$(MAKE) -C $(top_builddir)/src/distribute/gtm/common libgtmcommon.a

$(top_builddir)/src/distribute/gtm/client/libgtmclient.a:
	$(MAKE) -C $(top_builddir)/src/distribute/gtm/client libgtmclient.a

$(top_builddir)/src/distribute/lib/hotpatch/common/libhotpatchcommon.a:
	$(MAKE) -C $(top_builddir)/src/distribute/lib/hotpatch/common libhotpatchcommon.a
endif
$(top_builddir)/src/lib/hotpatch/client/libhotpatchclient.a:
	$(MAKE) -C $(top_builddir)/src/lib/hotpatch/client libhotpatchclient.a

# The postgres.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
gaussdb.o: $(OBJS)
	$(CC) $(LDREL) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$^) $(LIBS) -o $@

# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

../common/backend/parser/gram.hpp: ../common/backend/parser/gram.y
	$(MAKE) -C ../common/backend/parser gram.hpp


../common/backend/parser/hint_gram.hpp: ../common/backend/parser/hint_gram.y
	$(MAKE) -C ../common/backend/parser hint_gram.hpp

storage/replication/repl_gram.hpp: storage/replication/repl_gram.y
	$(MAKE) -C storage/replication repl_gram.hpp

storage/replication/syncrep_gram.hpp: storage/replication/syncrep_gram.y
	$(MAKE) -C storage/replication syncrep_gram.hpp

ifeq ($(enable_multiple_nodes), yes)
$(top_builddir)/src/common/backend/utils/fmgroids.h: ../common/backend/utils/Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/src/distribute/kernel/catalog/distribute_builtin_funcs.ini
	$(MAKE) -C ../common/backend/utils fmgroids.h
else
$(top_builddir)/src/common/backend/utils/fmgroids.h: ../common/backend/utils/Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
	$(MAKE) -C ../common/backend/utils fmgroids.h
endif
../common/backend/utils/errcodes.h: ../common/backend/utils/generate-errcodes.pl ../common/backend/utils/errcodes.txt
	$(MAKE) -C ../common/backend/utils errcodes.h

../common/backend/utils/probes.h: ../common/backend/utils/probes.d
	$(MAKE) -C ../common/backend/utils probes.h

ifeq ($(enable_multiple_nodes), yes)
$(top_builddir)/src/common/backend/utils/pg_builtin_proc.h: ../common/backend/utils/Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini $(top_srcdir)/src/distribute/kernel/catalog/distribute_builtin_funcs.ini
	$(MAKE) -C ../common/backend/utils pg_builtin_proc.h
else
$(top_builddir)/src/common/backend/utils/pg_builtin_proc.h: ../common/backend/utils/Gen_fmgrtab.pl  $(top_srcdir)/src/common/backend/catalog/builtin_funcs.ini
	$(MAKE) -C ../common/backend/utils pg_builtin_proc.h
endif

storage/lmgr/lwlocknames.h: storage/lmgr/generate-lwlocknames.pl storage/lmgr/lwlocknames.txt
	$(MAKE) -C storage/lmgr lwlocknames.h lwlocknames.cpp

# Make symlinks for these headers in the include directory. That way
# we can cut down on the -I options. Also, a symlink is automatically
# up to date when we update the base file.
#
# The point of the prereqdir incantation in some of the rules below is to
# force the symlink to use an absolute path rather than a relative path.
# For headers which are generated by make distprep, the actual header within
# src/gausskernel will be in the source tree, while the symlink in src/include
# will be in the build tree, so a simple ../.. reference won't work.
# For headers generated during regular builds, we prefer a relative symlink.

$(top_builddir)/src/include/parser/gram.hpp: ../common/backend/parser/gram.hpp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/parser/hint_gram.hpp: ../common/backend/parser/hint_gram.hpp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/replication/repl_gram.hpp: storage/replication/repl_gram.hpp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/replication/syncrep_gram.hpp: storage/replication/syncrep_gram.hpp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/catalog/schemapg.h: ../common/backend/catalog/schemapg.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/errcodes.h: ../common/backend/utils/errcodes.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/fmgroids.h: $(top_builddir)/src/common/backend/utils/fmgroids.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/pg_builtin_proc.h: $(top_builddir)/src/common/backend/utils/pg_builtin_proc.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/probes.h: ../common/backend/utils/probes.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

../common/backend/utils/probes.o: ../common/backend/utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@

$(top_builddir)/src/include/storage/lwlocknames.h: storage/lmgr/lwlocknames.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .


##########################################################################

# Be sure that these files get removed by the maintainer-clean target
distprep:
	$(MAKE) -C ../common/backend/parser	gram.cpp gram.hpp scan.cpp hint_gram.cpp hint_gram.hpp hint_scan.cpp
	$(MAKE) -C bootstrap	bootparse.cpp bootscanner.cpp
	$(MAKE) -C ../common/backend/catalog	schemapg.h postgres.bki postgres.description postgres.shdescription
	$(MAKE) -C storage/replication	repl_gram.cpp repl_gram.hpp repl_scanner.cpp syncrep_gram.cpp syncrep_gram.hpp syncrep_scanner.cpp
	$(MAKE) -C storage/lmgr	lwlocknames.h lwlocknames.cpp
	$(MAKE) -C ../common/backend/utils	fmgrtab.cpp fmgroids.h errcodes.h
	$(MAKE) -C ../common/backend/utils/misc	guc-file.cpp


##########################################################################

install: all installdirs install-bin
	$(MAKE) -C ../common/backend/catalog install-data
	$(MAKE) -C runtime/codegen install-data
	$(MAKE) -C ../common/backend/tsearch install-data
	$(MAKE) -C storage/bulkload install-data
	$(MAKE) -C $(top_builddir)/contrib/file_fdw install
	$(MAKE) -C $(top_builddir)/contrib/hdfs_fdw install
	$(MAKE) -C $(top_builddir)/contrib/log_fdw install
ifeq ($(enable_multiple_nodes), yes)
	$(MAKE) -C ../distribute/kernel/extension/gauss_connector install
	$(MAKE) -C ../distribute/kernel/extension/roach_api install
	$(MAKE) -C ../distribute/kernel/extension/dimsearch install
	$(MAKE) -C ../distribute/kernel/extension/tsdb install
	$(MAKE) -C ../distribute/kernel/extension/streaming install
	$(MAKE) -C $(top_builddir)/contrib/pg_upgrade_support install
	$(MAKE) -C $(top_builddir)/contrib/gc_fdw install

	# This script is needed by postgis
	$(INSTALL_DATA) $(srcdir)/../../contrib/postgis/PostGIS_install.sh '$(DESTDIR)$(includedir_server)/../../../share/postgis/PostGIS_install.sh'
	$(INSTALL_DATA) $(srcdir)/../../contrib/postgis/extension_dependency.h '$(DESTDIR)$(includedir_server)'
endif
ifeq ($(enable_mot), yes)
	$(MAKE) -C $(top_builddir)/src/gausskernel/storage/mot/fdw_adapter/src install
endif
	$(MAKE) -C $(top_builddir)/contrib/test_decoding install
	$(MAKE) -C $(top_builddir)/contrib/mppdb_decoding install
	$(MAKE) -C $(top_builddir)/contrib/security_plugin install
	$(INSTALL_DATA) $(srcdir)/../common/backend/libpq/pg_hba.conf.sample '$(DESTDIR)$(datadir)/pg_hba.conf.sample'
	$(INSTALL_DATA) $(srcdir)/../common/backend/libpq/pg_ident.conf.sample '$(DESTDIR)$(datadir)/pg_ident.conf.sample'
	$(INSTALL_DATA) $(srcdir)/../common/backend/utils/misc/postgresql.conf.sample '$(DESTDIR)$(datadir)/postgresql.conf.sample'
	$(INSTALL_DATA) $(srcdir)/storage/access/transam/recovery.conf.sample '$(DESTDIR)$(datadir)/recovery.conf.sample'

	cd $(KERBEROS_LIB_PATH) && tar -cpf - ./* | ( cd $(DESTDIR)$(libdir); tar -xpf -  )
	mkdir -p '$(DESTDIR)$(libdir)/../temp/'
	rm -f $(DESTDIR)$(libdir)/libpcre*
	cp $(LIBOBS_LIB_PATH)/libpcre* '$(DESTDIR)$(libdir)/../temp/'
	mv $(DESTDIR)$(libdir)/../temp/* '$(DESTDIR)$(libdir)/'
	cp $(SECUREDYNAMICLIB_HOME)/libsecurec* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/liblog4* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/libeSDK* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/libxml2* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/libcharset* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/libiconv* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/OBS* '$(DESTDIR)$(libdir)/'
	cp $(LIBOBS_LIB_PATH)/libnghttp* '$(DESTDIR)$(libdir)/'
	cp $(LIBPARQUET_LIB_PATH)/libparquet* '$(DESTDIR)$(libdir)/'
	cp -d $(ZLIB_LIB_PATH)/libz* '$(DESTDIR)$(libdir)/'
	cp -d $(LZ4_LIB_PATH)/liblz4* '$(DESTDIR)$(libdir)/'
	cp -d $(HLL_LIB_PATH)/libhll* '$(DESTDIR)$(libdir)/'
	cp -d $(CJSON_LIB_PATH)/libcjson* '$(DESTDIR)$(libdir)/'
	cp $(GRPC_LIB_PATH)/* '$(DESTDIR)$(libdir)/'
ifneq (, $(findstring __USE_NUMA, $(CFLAGS)))
	cp $(NUMA_LIB_PATH)/* '$(DESTDIR)$(libdir)/'
endif
ifeq ($(enable_mot), yes)
	cp -d $(MASSTREE_LIB_PATH)/libmasstree* '$(DESTDIR)$(libdir)/'
endif
ifeq ($(with_3rd), NONE)
	cp $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libstdc++.so.6 '$(DESTDIR)$(libdir)/'
	cp $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libgcc_s.so.1 '$(DESTDIR)$(libdir)/'
ifeq ($(enable_mot), yes)
	cp -d $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libatomic.so* '$(DESTDIR)$(libdir)/'
endif
ifeq ($(enable_thread_check), yes)
	cp $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so '$(DESTDIR)$(libdir)/'
	cp $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so.0 '$(DESTDIR)$(libdir)/'
	cp $(top_builddir)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so.0.0.0 '$(DESTDIR)$(libdir)/'
endif
else
	cp $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libstdc++.so.6 '$(DESTDIR)$(libdir)/'
	cp $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libgcc_s.so.1 '$(DESTDIR)$(libdir)/'
ifeq ($(enable_mot), yes)
	cp -d $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libatomic.so* '$(DESTDIR)$(libdir)/'
endif
ifeq ($(enable_thread_check), yes)
	cp $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so '$(DESTDIR)$(libdir)/'
	cp $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so.0 '$(DESTDIR)$(libdir)/'
	cp $(with_3rd)/$(BUILD_TOOLS_PATH)/gcc$(subst .0,,$(CC_VERSION))/gcc/lib64/libtsan.so.0.0.0 '$(DESTDIR)$(libdir)/'
endif
endif
	cp -r $(with_jdk)/jre/* '$(DESTDIR)$(bindir)/../jre/'

	cp $(PLJAVA_LIB_PATH)/* '$(DESTDIR)$(libdir)/'
	cp $(PLJAVA_JAR_PATH)/$(JARPLJAVA) '$(DESTDIR)$(pkglibdir)/java/'
	cp $(PLJAVA_HOME)/udstools.py '$(DESTDIR)$(datadir)/tmp/'

	cp '$(top_builddir)/src/include/ssl/openssl_gsql.cnf' '$(DESTDIR)$(datadir)/../sslcert/gsql/openssl.cnf'
	cp '$(top_builddir)/src/include/ssl/openssl_gsql.cnf' '$(DESTDIR)$(datadir)/../sslcert/gds/openssl.cnf'
	cp '$(top_builddir)/src/include/ssl/openssl_grpc.cnf' '$(DESTDIR)$(datadir)/../sslcert/grpc/openssl.cnf'
	cp '$(top_builddir)/src/include/ssl/openssl_om.cnf' '$(DESTDIR)$(datadir)/../sslcert/om/openssl.cnf'
	cp '$(LIBOPENSSL_BIN_PATH)/openssl' '$(DESTDIR)$(bindir)/openssl'
	cp '$(LIBCURL_LIB_PATH)/libcurl.so' '$(DESTDIR)$(libdir)/libcurl.so'
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4' '$(DESTDIR)$(libdir)/libcurl.so.4'

ifeq ($(PLAT_FORM_STR), euleros2.0_sp2_x86_64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), euleros2.0_sp5_x86_64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), euleros2.0_sp8_aarch64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), openeuler_aarch64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), openeuler_x86_64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), centos7.6_x86_64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else ifeq ($(PLAT_FORM_STR), kylin_aarch64)
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.6.0' '$(DESTDIR)$(libdir)/libcurl.so.4.6.0'
else
	cp '$(LIBCURL_LIB_PATH)/libcurl.so.4.5.0' '$(DESTDIR)$(libdir)/libcurl.so.4.5.0'
endif

	rm -rf '$(DESTDIR)$(libdir)/../temp'

ifeq ($(SUPPORT_HOTPATCH), yes)
	cp $(LIBHOTPATCH_LIB_PATH)/libdoprapatch* '$(DESTDIR)$(libdir)/'
endif

install-bin: gaussdb $(POSTGRES_IMP) installdirs libcgroup
	$(INSTALL_PROGRAM) gaussdb$(X) '$(DESTDIR)$(bindir)/gaussdb$(X)'
	@if test -d $(KERBEROS_BIN_PATH); then cp -rf $(KERBEROS_BIN_PATH)/klist $(DESTDIR)$(bindir)/; fi  
	@if test -d $(KERBEROS_BIN_PATH); then cp -rf $(KERBEROS_BIN_PATH)/kinit $(DESTDIR)$(bindir)/; fi
	@if test -d $(KERBEROS_BIN_PATH); then cp -rf $(KERBEROS_BIN_PATH)/kdestroy $(DESTDIR)$(bindir)/; fi
	@if test -d $(KERBEROS_SBIN_PATH); then cp -rf $(KERBEROS_SBIN_PATH)/kdb5_util $(DESTDIR)$(bindir)/; fi
	@if test -d $(KERBEROS_SBIN_PATH); then cp -rf $(KERBEROS_SBIN_PATH)/krb5kdc $(DESTDIR)$(bindir)/; fi
	@if test -d $(KERBEROS_SBIN_PATH); then cp -rf $(KERBEROS_SBIN_PATH)/kadmin.local $(DESTDIR)$(bindir)/; fi
	@if test -d $(KERBEROS_SBIN_PATH); then cp -rf $(KERBEROS_SBIN_PATH)/kadmind $(DESTDIR)$(bindir)/; fi
ifeq (${host_cpu}, aarch64)
	$(INSTALL_PROGRAM) ../bin/scripts/setArmOptimization$(X).sh '$(DESTDIR)$(bindir)/setArmOptimization$(X).sh'
	$(INSTALL_PROGRAM) ../bin/scripts/bind_net_irq$(X).sh '$(DESTDIR)$(bindir)/bind_net_irq$(X).sh'
endif
	$(INSTALL_PROGRAM) '$(LZ4_BIN_PATH)/lz4' '$(DESTDIR)$(bindir)/lz4'

	@rm -f '$(DESTDIR)$(bindir)/gs_encrypt$(X)'
	ln -s gaussdb$(X) '$(DESTDIR)$(bindir)/gs_encrypt$(X)'

ifeq ($(enable_multiple_nodes), yes)
	@rm -f '$(DESTDIR)$(bindir)/gaussmaster$(X)'
	ln -s gaussdb$(X) '$(DESTDIR)$(bindir)/gaussmaster$(X)'
	$(INSTALL_PROGRAM) ../bin/scripts/drop_caches$(X).sh '$(DESTDIR)$(bindir)/drop_caches$(X).sh'
	$(INSTALL_PROGRAM) ../bin/scripts/run_drop_cache$(X).sh '$(DESTDIR)$(bindir)/run_drop_cache$(X).sh'
else
	cp -rf $(srcdir)/dbmind/tools '$(DESTDIR)$(bindir)/dbmind'
endif
ifeq ($(MAKE_EXPORTS), true)
	$(INSTALL_DATA) $(POSTGRES_IMP) '$(DESTDIR)$(pkglibdir)/$(POSTGRES_IMP)'
	$(INSTALL_PROGRAM) $(MKLDEXPORT) '$(DESTDIR)$(pgxsdir)/$(MKLDEXPORT_DIR)/mkldexport.sh'
endif

.PHONY: install-bin

#todo: need to use pg_config to create etc directory
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)' '$(DESTDIR)$(bindir)/../etc'
	cp '$(top_builddir)/src/gausskernel/storage/access/dfs/region_map'  '$(DESTDIR)$(bindir)/../etc/'
	@if test -d '$(top_builddir)/src/common/backend/libpq/kerberos'; then cp -rf '$(top_builddir)/src/common/backend/libpq/kerberos'  '$(DESTDIR)$(bindir)/../etc/'; fi
	$(MKDIR_P) '$(DESTDIR)$(llvmirdir)'
	$(MKDIR_P) '$(DESTDIR)$(libdir)/postgresql/pg_plugin'
	$(MKDIR_P) '$(DESTDIR)$(libdir)/postgresql/proc_srclib'
	$(MKDIR_P) '$(DESTDIR)$(libdir)/libsimsearch'
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)/java'
	$(MKDIR_P) '$(DESTDIR)$(bindir)/../jre'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/tmp'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert/gsql'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert/etcd'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert/grpc'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert/gds'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/../sslcert/om'

ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(MAKE_EXPORTS), true)
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(MKLDEXPORT_DIR)'
endif

libcgroup:
	cd $(DESTDIR)$(libdir) && rm -f libcgroup.*
	cp $(LIBCGROUP_LIB_PATH)/$(LIBCGROUP) '$(DESTDIR)$(libdir)/$(LIBCGROUP)'
	cd $(DESTDIR)$(libdir) && $(LN_S) $(LIBCGROUP) libcgroup.so.1

##########################################################################

uninstall:
	rm -rf '$(DESTDIR)$(bindir)/../etc/region_map'
	rm -rf '$(DESTDIR)$(bindir)/../etc/kerberos'
ifeq ($(enable_multiple_nodes), yes)
	rm -f '$(DESTDIR)$(bindir)/gaussdb$(X)' '$(DESTDIR)$(bindir)/gaussmaster'
	rm -f '$(DESTDIR)$(bindir)/gaussdb$(X)' '$(DESTDIR)$(bindir)/gs_encrypt'
else
	rm -f '$(DESTDIR)$(bindir)/gaussdb$(X)' 
endif
	rm -f $(DESTDIR)$(libdir)/libcgroup*
	rm -f $(DESTDIR)$(libdir)/libsimsearch/*
	rm -f '$(DESTDIR)$(libdir)/$(LIBPLJAVA)' '$(DESTDIR)$(pkglibdir)/java/$(JARPLJAVA)'

ifeq ($(MAKE_EXPORTS), true)
	rm -f '$(DESTDIR)$(pkglibdir)/$(POSTGRES_IMP)'
	rm -f '$(DESTDIR)$(pgxsdir)/$(MKLDEXPORT_DIR)/mkldexport.sh'
endif
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
	$(MAKE) -C ../common/backend/catalog uninstall-data
	$(MAKE) -C runtime/codegen uninstall-data
	$(MAKE) -C ../common/backend/tsearch uninstall-data
	$(MAKE) -C storage/bulkload uninstall-data
	$(MAKE) -C $(top_builddir)/contrib/file_fdw uninstall
	$(MAKE) -C $(top_builddir)/contrib/hdfs_fdw uninstall
ifeq ($(enable_multiple_nodes), yes)
	$(MAKE) -C ../distribute/kernel/extension/gauss_connector uninstall
	$(MAKE) -C ../distribute/kernel/extension/roach_api uninstall
	$(MAKE) -C ../distribute/kernel/extension/dimsearch uninstall
else
	$(MAKE) -C $(top_builddir)/contrib/gc_fdw uninstall
endif
ifeq ($(enable_mot), yes)
	$(MAKE) -C $(top_builddir)/src/gausskernel/storage/mot/fdw_adapter/src uninstall
endif
	$(MAKE) -C $(top_builddir)/contrib/log_fdw uninstall
	$(MAKE) -C $(top_builddir)/contrib/pg_upgrade_support clean
	$(MAKE) -C $(top_builddir)/contrib/test_decoding uninstall
	$(MAKE) -C $(top_builddir)/contrib/mppdb_decoding uninstall
	$(MAKE) -C $(top_builddir)/contrib/security_plugin uninstall
	rm -f '$(DESTDIR)$(datadir)/pg_hba.conf.sample' \
	      '$(DESTDIR)$(datadir)/pg_ident.conf.sample' \
	      '$(DESTDIR)$(datadir)/postgresql.conf.sample' \
	      '$(DESTDIR)$(datadir)/recovery.conf.sample'

##########################################################################

clean:
	rm -f $(LOCALOBJS) gaussdb$(X) $(POSTGRES_IMP) $(CRC_LOG) \
		$(top_builddir)/src/include/parser/gram.hpp \
		$(top_builddir)/src/include/parser/hint_gram.hpp \
		$(top_builddir)/src/include/catalog/schemapg.h \
		$(top_builddir)/src/include/utils/fmgroids.h \
		$(top_builddir)/src/common/backend/utils/fmgroids.h \
		$(top_builddir)/src/common/backend/utils/fmgrtab.cpp \
		$(top_builddir)/src/common/backend/utils/pg_builtin_proc.h \
		$(top_builddir)/src/include/utils/probes.h	\
		$(top_builddir)/src/include/replication/repl_gram.hpp \
		$(top_builddir)/src/include/replication/syncrep_gram.hpp  \
		$(top_builddir)/src/include/storage/lwlocknames.h

ifeq ($(PORTNAME), cygwin)
	rm -f postgres.dll postgres.def libpostgres.a
endif
ifeq ($(PORTNAME), win32)
	rm -f postgres.dll postgres.def libpostgres.a $(WIN32RES)
endif

distclean: clean
	rm -f ../common/backend/port/tas.s ../common/backend/port/dynloader.cpp ../common/backend/port/pg_sema.cpp ../common/backend/port/pg_shmem.cpp \
	      ../common/backend/port/pg_latch.cpp

maintainer-clean: distclean
	rm -f bootstrap/bootparse.cpp \
	      bootstrap/bootscanner.cpp \
	      ../common/backend/parser/gram.cpp \
	      ../common/backend/parser/gram.hpp \
	      ../common/backend/parser/hint_gram.hpp \
	      ../common/backend/parser/scan.cpp \
	      ../common/backend/catalog/schemapg.h \
	      ../common/backend/catalog/postgres.bki \
	      ../common/backend/catalog/postgres.description \
	      ../common/backend/catalog/postgres.shdescription \
	      storage/replication/repl_gram.cpp \
	      storage/replication/repl_gram.hpp \
	      storage/replication/repl_scanner.cpp \
	      storage/replication/syncrep_gram.cpp \
	      storage/replication/syncrep_gram.hpp \
	      storage/replication/repl_scanner.cpp \
	      storage/lmgr/lwlocknames.cpp \
	      storage/lmgr/lwlocknames.h \
	      ../common/backend/utils/errcodes.h \
	      ../common/backend/utils/misc/guc-file.cpp \
	      ../common/backend/utils/sort/qsort_tuple.cpp


##########################################################################
#
# Support for code development.
#
# Use target "quick" to build "postgres" when you know all the subsystems
# are up to date.  It saves the time of doing all the submakes.
.PHONY: quick
quick: $(OBJS)
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o gaussdb
