#-------------------------------------------------------------------------
#
# Makefile for ecpg pgtypes library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/ecpg/pgtypeslib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/ecpg/pgtypeslib
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

NAME= pgtypes
SO_MAJOR_VERSION= 3
SO_MINOR_VERSION= 3

override CPPFLAGS := -I../include -I$(top_srcdir)/src/common/interfaces/ecpg/include \
	-I$(top_srcdir)/src/include/utils -I$(libpq_srcdir) $(CPPFLAGS)
override CFLAGS += $(PTHREAD_CFLAGS)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

# Need to recompile any libpgport object files
LIBS := $(filter-out -lpgport, $(LIBS))

SHLIB_LINK += -lm

SHLIB_EXPORTS = exports.txt

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= numeric.o datetime.o common.o dt_common.o timestamp.o interval.o \
	pgstrcasecmp.o \
	$(filter rint.o snprintf.o, $(LIBOBJS))

all: all-lib

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

# We use some port modules verbatim, but since we need to
# compile with appropriate options to build a shared lib, we can't
# necessarily use the same object files as the backend uses. Instead,
# symlink the source files in here and build our own object file.

pgstrcasecmp.cpp rint.cpp snprintf.cpp: % : $(top_srcdir)/src/common/port/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean: clean-lib
	rm -f $(OBJS) pgstrcasecmp.cpp rint.cpp snprintf.cpp *.depend

maintainer-clean: distclean maintainer-clean-lib
