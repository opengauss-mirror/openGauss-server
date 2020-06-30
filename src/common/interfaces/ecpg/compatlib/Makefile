#-------------------------------------------------------------------------
#
# Makefile for ecpg compatibility library
#
# Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/ecpg/compatlib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/ecpg/compatlib
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

NAME= ecpg_compat
SO_MAJOR_VERSION= 3
SO_MINOR_VERSION= 4

override CPPFLAGS := -I../include -I$(top_srcdir)/src/interfaces/ecpg/include \
	-I$(libpq_srcdir) -I$(top_srcdir)/src/include/utils $(CPPFLAGS)
override CFLAGS += $(PTHREAD_CFLAGS)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

SHLIB_LINK = -L../ecpglib -lecpg -L../pgtypeslib -lpgtypes $(libpq) \
	$(filter -lintl -lm, $(LIBS)) $(PTHREAD_LIBS)
SHLIB_PREREQS = submake-ecpglib submake-pgtypeslib

SHLIB_EXPORTS = exports.txt

# Need to recompile any libpgport object files
LIBS := $(filter-out -lpgport, $(LIBS))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= informix.o $(filter snprintf.o, $(LIBOBJS))

all: all-lib

.PHONY: submake-ecpglib submake-pgtypeslib

submake-ecpglib:
	$(MAKE) -C $(top_builddir)/src/common/interfaces/ecpg/ecpglib all

submake-pgtypeslib:
	$(MAKE) -C $(top_builddir)/src/common/interfaces/ecpg/pgtypeslib all

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

snprintf.cpp: % : $(top_srcdir)/src/common/port/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean: clean-lib
	rm -f $(OBJS) snprintf.cpp *.depend

maintainer-clean: distclean maintainer-clean-lib
