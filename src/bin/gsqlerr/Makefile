# ---------------------------------------------------------------------------------------
# 
# Makefile
#     Makefile for src/bin/gsqlerr
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# IDENTIFICATION
#        src/bin/gsqlerr/Makefile
# 
# ---------------------------------------------------------------------------------------
PGFILEDESC = "gsqlerr the PostgreSQL server"
PGAPPICON=win32

subdir = src/bin/gsqlerr
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS1=   scanEreport.o $(WIN32RES)

ifeq ($(enable_memory_check), yes)
LIBS = -lsecurec -l$(MEMCHECK_LIB_NAME_ASAN) -pthread -D_REENTRANT -ldl -lm -lrt
else
LIBS = -lsecurec
endif

ifeq ($(enable_thread_check), yes)
LIBS = -lsecurec -l$(MEMCHECK_LIB_NAME_TSAN) -pthread -D_REENTRANT -ldl -lm -lrt
endif

all:    
	make clean
	make scanEreport 
	make gsqlerr

scanEreport: $(OBJS1) | submake-libpgport
	$(CC) $(CXXFLAGS) $(OBJS1) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X) $(LIBS)
	$(top_builddir)/$(subdir)/scanEreport $(top_srcdir) ereport.txt $(top_srcdir)/src/common/backend/utils/errcodes.txt escan.txt

utscanEreport: $(OBJS1) | submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS1) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o libutscanEreport.so -lsecurec
	mv libutscanEreport.so $(top_builddir)/src/distribute/test/ut/lib

OBJS=   gsqlerr.o $(WIN32RES)

gsqlerr: $(OBJS) | submake-libpgport
	$(CC) $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

utgsqlerr: $(OBJS) | submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o libutgsqlerr.so -lsecurec
	mv libutgsqlerr.so $(top_builddir)/src/distribute/test/ut/lib

ifeq ($(enable_multiple_nodes), yes)
install: all installdirs
	$(INSTALL_PROGRAM) gsqlerr$(X) '$(DESTDIR)$(bindir)/gsqlerr$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gsqlerr$(X)'
endif

clean distclean maintainer-clean:
	rm -f gsqlerr$(X) $(OBJS) *.depend
	rm -f scanEreport$(X) $(OBJS1)

