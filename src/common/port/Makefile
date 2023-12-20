#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for the port-specific subsystem of the backend
#
# These files are used in other directories for portability on systems
# with broken/missing library files, and for common code sharing.
#
# This makefile generates two outputs:
#
#	libpgport.a - contains object files with FRONTEND defined,
#		for use by client application and libraries
#
#	libpgport_srv.a - contains object files without FRONTEND defined,
#		for use only by the backend binaries
#
# LIBOBJS is set by configure (via Makefile.global) to be the list of object
# files that are conditionally needed as determined by configure's probing.
# OBJS adds additional object files that are always compiled.
#
# IDENTIFICATION
#    src/common/port/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/port
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

VERSION = 1

override CPPFLAGS := -I$(top_builddir)/src/common/port -DFRONTEND $(CPPFLAGS) $(CFLAGS_SSE42)
LIBS += $(PTHREAD_LIBS)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all

override CPPSources=$(shell find -name "*.cpp" ! -name "path.cpp" | sort)
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = $(LIBOBJS) chklocale.o dirmod.o erand48.o exec.o fls.o inet_net_ntop.o \
	noblock.o path.o pg_bitutils.o pgcheckdir.o pgmkdirp.o pgsleep.o \
	pgstrcasecmp.o qsort.o qsort_arg.o sprompt.o thread.o flock.o pgstrcasestr.o\
	gs_thread.o gs_env_r.o gs_getopt_r.o \
	gs_readdir.o gs_strerror.o gs_syscall_lock.o \
	gs_system.o \
	cipher.o tool_common.o \
	$(top_builddir)/src/gausskernel/storage/file/fio_device.o $(top_builddir)/src/gausskernel/storage/dss/fio_dss.o

ifeq "${host_cpu}" "aarch64"
OBJS +=  pg_crc32c_choose.o
else ifeq "${host_cpu}" "x86_64"
OBJS +=  pg_crc32c_sse42.o pg_crc32c_sb8.o pg_crc32c_choose.o
else
OBJS +=  pg_crc32c_sb8.o 
endif
# foo_srv.o and foo.o are both built from foo.c, but only foo.o has -DFRONTEND
OBJS_SRV = $(OBJS:%.o=%_srv.o)

AARCH64_CRC_OBJ =
ifeq "${host_cpu}" "aarch64"
  AARCH64_CRC_OBJ = crc32_arm_parallel.o
  OBJS += $(AARCH64_CRC_OBJ)
endif

OBJS_TOOL = fls.o strlcat.o strlcpy.o getpeereid.o chklocale.o dirmod.o erand48.o exec.o inet_net_ntop.o \
	noblock.o path.o pg_bitutils.o pgcheckdir.o pgmkdirp.o pgsleep.o pgstrcasecmp.o qsort.o qsort_arg.o sprompt.o thread.o flock.o pgstrcasestr.o \
	gs_thread.o gs_env_r.o gs_getopt_r.o gs_readdir.o gs_strerror.o gs_syscall_lock.o gs_system.o cipher.o \
	$(top_builddir)/src/gausskernel/storage/file/fio_device.o $(top_builddir)/src/gausskernel/storage/dss/fio_dss.o tool_common.o

all: libpgport.a libpgport_srv.a libpgport_tool.so

libpgport_tool.so: $(OBJS_TOOL)
	$(CC) -Wl,-z,relro,-z,now -fPIC -shared $(CFLAGS) $(CPPFLAGS)  -o libpgport_tool.so.$(VERSION) $^
	rm -f libpgport_tool.so
	ln -s libpgport_tool.so.$(VERSION) libpgport_tool.so

# libpgport is needed by some contrib
install: all installdirs
	$(INSTALL_STLIB) libpgport.a '$(DESTDIR)$(libdir)/libpgport.a'
	$(INSTALL_STLIB) libpgport_tool.so.$(VERSION) '$(DESTDIR)$(libdir)/libpgport_tool.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libpgport_tool.so && \
	ln -s libpgport_tool.so.$(VERSION) libpgport_tool.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libpgport.a'

libpgport.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

# thread.o needs PTHREAD_CFLAGS (but thread_srv.o does not)
thread.o: CFLAGS+=$(PTHREAD_CFLAGS)

crc32_arm_parallel.o: crc32_arm_parallel.S
	$(CC) $(CPPFLAGS) $(CFLAGS) -march=armv8-a+crc+crypto -o $@ -c $^


crc32_arm_parallel_srv.o: crc32_arm_parallel.S
	$(CC) $(CPPFLAGS) $(CFLAGS) -march=armv8-a+crc+crypto -o $@ -c $^

# pg_crc32c_sse42.o and its _srv.o version need CFLAGS_SSE42
CFLAGS_SSE42=-msse4.2
pg_crc32c_sse42.o: CFLAGS+=$(CFLAGS_SSE42)
pg_crc32c_sse42_srv.o: CFLAGS+=$(CFLAGS_SSE42)

#
# Server versions of object files
#

libpgport_srv.a: $(OBJS_SRV)
	$(AR) $(AROPT) $@ $^

libport.so: $(OBJS)
	$(CC) -fPIC -shared $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@ $^

libport_srv.so: $(OBJS_SRV)
	$(CC) -fPIC -shared $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@ $^

port: libport.so libport_srv.so
	mv *.so $(top_builddir)/../distribute/test/ut/lib
	
# Because this uses its own compilation rule, it doesn't use the
# dependency tracking logic from Makefile.global.  To make sure that
# dependency tracking works anyway for the *_srv.o files, depend on
# their *.o siblings as well, which do have proper dependencies.  It's
# a hack that might fail someday if there is a *_srv.o without a
# corresponding *.o, but it works for now (and those would probably go
# into src/backend/port/ anyway).
%_srv.o: %.cpp %.o
	$(CC) $(CFLAGS) $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@

$(OBJS_SRV): | submake-errcodes submake-lwlocknames

.PHONY: submake-errcodes submake-lwlocknames

submake-errcodes:
	$(MAKE) -C ../../gausskernel submake-errcodes

submake-lwlocknames:
	$(MAKE) -C ../../gausskernel submake-lwlocknames

# Dependency is to ensure that path changes propagate

path.o: path.cpp pg_config_paths.h

path_srv.o: path.cpp pg_config_paths.h

# We create a separate file rather than put these in pg_config.h
# because many of these values come from makefiles and are not
# available to configure.
pg_config_paths.h: $(top_builddir)/src/Makefile.global
	echo "#define PGBINDIR \"$(bindir)\"" >$@
	echo "#define PGSHAREDIR \"$(datadir)\"" >>$@
	echo "#define SYSCONFDIR \"$(sysconfdir)\"" >>$@
	echo "#define INCLUDEDIR \"$(includedir)\"" >>$@
	echo "#define PKGINCLUDEDIR \"$(pkgincludedir)\"" >>$@
	echo "#define INCLUDEDIRSERVER \"$(includedir_server)\"" >>$@
	echo "#define LIBDIR \"$(libdir)\"" >>$@
	echo "#define PKGLIBDIR \"$(pkglibdir)\"" >>$@
	echo "#define LOCALEDIR \"$(localedir)\"" >>$@
	echo "#define DOCDIR \"$(docdir)\"" >>$@
	echo "#define HTMLDIR \"$(htmldir)\"" >>$@
	echo "#define MANDIR \"$(mandir)\"" >>$@

clean distclean maintainer-clean:
	rm -f libpgport.a libpgport_srv.a $(OBJS) $(OBJS_SRV) pg_config_paths.h libpgport_tool.so libpgport_tool.so.$(VERSION) *.depend
