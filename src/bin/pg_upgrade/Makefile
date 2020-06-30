#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_format_cu
#
# src/bin/pg_format_cu/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_format_cu - do column store file algin before upgrade"

subdir = src/bin/pg_format_cu
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= pg_format_cu.o

CFLAGS += -Wl,-z,relro,-z,now

override CPPFLAGS += -fPIE
LDFLAGS += -pie

all: pg_format_cu

pg_format_cu: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_format_cu$(X) '$(DESTDIR)$(bindir)/pg_format_cu$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_format_cu$(X)'

clean distclean maintainer-clean:
	rm -f pg_format_cu$(X) $(OBJS) *.depend
