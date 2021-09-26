#----------------------------------------------------------------------------
#
# openGauss makesgml tool makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/pgxc/tools/makesgml/Makefile
#
#-----------------------------------------------------------------------------

PGFILEDESC = "makesgml - convert .sgmlin to .sgml"
PGAPPICON=win32

subdir = src/common/pgxc/tools/makesgml
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS) -Wl,-z,relro,-z,now -fPIE

ifeq ($(enable_profiling), no)
  override CFLAGS += -pie
else
  override CFLAGS += -fPIC
endif

ifeq ($(enable_memory_check), yes)
  LDFLAGS += -l$(MEMCHECK_LIB_NAME_ASAN) -lpthread -ldl -lm -lrt
else
  ifeq ($(enable_thread_check), yes)
    LDFLAGS += -l$(MEMCHECK_LIB_NAME_TSAN) -lpthread -ldl -lm -lrt
  endif
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = makesgml.o

makesgml: $(OBJS)
	$(CC) -Wl,-z,relro,-z,now $(CFLAGS) $^ -o $@$(X) $(LDFLAGS)

all distprep html man: makesgml

install: all installdirs
	$(INSTALL_PROGRAM) makesgml$(X) '$(DESTDIR)$(bindir)/makesgml$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -rf '$(DESTDIR)$(bindir)/makesgml$(X)'

clean distclean maintenance-clean maintainer-clean:
	rm -rf makesgml$(X) $(OBJS) *.depend
