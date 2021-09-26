#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for the iprange module
#
# These files are used in other directories for portability on systems
# with broken/missing library files, and for common code sharing.
#
# This makefile generates one outputs:
#
#	libiprange.a - contains the iprange module
#
# LIBOBJS is set by configure (via Makefile.global) to be the list of object
# files that are conditionally needed as determined by configure's probing.
# OBJS adds additional object files that are always compiled.
#
# IDENTIFICATION
#    src/iprange/Makefile
#
#-------------------------------------------------------------------------

top_builddir = ../../../..
subdir=src/gausskernel/security/iprange
include $(top_builddir)/src/Makefile.global

VERSION = 1

target = libiprange.so

override CPPFLAGS := -I$(top_builddir)/src/include/iprange $(CPPFLAGS) $(CFLAGS_SSE42)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC 

override CPPSources=$(shell find -name "*.cpp" ! -name "path.cpp" | sort)
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = iprange.o

include $(top_srcdir)/src/gausskernel/common.mk

all: iprange.o

iprange.o: iprange.cpp $(top_builddir)/src/include/iprange/iprange.h

# libpgport is needed by some contrib
install: all installdirs
	$(INSTALL_STLIB) libiprange.a '$(DESTDIR)$(libdir)/libiprange.a'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libiprange.a'

libiprange.so: $(OBJS)
	$(CXX) -std=c++11  $(CPPFLAGS) -fPIC -shared -c iprange.cpp -o $(target)

iprange: libiprange.so
	mv *.so $(top_builddir)/../distribute/test/ut/lib

# Dependency is to ensure that path changes propagate

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
	rm -f libiprange.a iprange.so $(OBJS) $(OBJS_SRV)
