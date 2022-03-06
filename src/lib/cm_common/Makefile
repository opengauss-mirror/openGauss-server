#----------------------------------------------------------------------------
#
# openGauss CM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# distribute/cm/cm_common/Makefile
#
#-----------------------------------------------------------------------------
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
subdir = src/lib/cm_common

VERSION = 2

override CFLAGS += -fstack-protector-strong -Wl,-z,relro,-z,now 
override CPPFLAGS := -I. -I$(libpq_srcdir) -I$(LIBCGROUP_INCLUDE_PATH) $(CPPFLAGS) -L$(LIBCGROUP_LIB_PATH) -lcgroup
LIBS += $(PTHREAD_LIBS)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC
override CPPFLAGS += -I$(top_builddir)/src/include/cm -I$(top_builddir)/../distribute/include/ -I$(top_builddir)/../distribute/include/cm -L$(top_builddir)/../distribute/cm/cm_etcdapi

include $(top_srcdir)/src/gausskernel/common.mk

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = cm_elog.o be_module.o cm_stringinfo.o cm_misc.o cm_cgroup.o cm_path.o

ETCDAPI=$(top_builddir)/../distribute/cm/cm_etcdapi/libetcdapi.so

.NOTPARALLEL:
all: cm_errcodes.h libcmcommon.a libcmcommon.so

cm_errcodes.h: cm_errcodes.txt
	$(PERL) generate-cm-errcodes.pl $< > $@
	rm -rf $(top_builddir)/src/include/cm/cm_errcodes.h
	$(LN_S) $(top_builddir)/src/lib/cm_common/cm_errcodes.h $(top_builddir)/src/include/cm/cm_errcodes.h

libcmcommon.so:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) cm_elog.cpp cm_stringinfo.cpp cm_misc.cpp cm_cgroup.cpp cm_path.cpp be_module.cpp -o libcmcommon.so.$(VERSION)
	rm -f libcmcommon.so && \
	ln -sf libcmcommon.so.$(VERSION) libcmcommon.so

libcmcommon.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libcmcommon.a libcmcommon.so libcmcommon.so.$(VERSION) *.depend
	rm -rf cm_errcodes.h
	rm -rf $(top_builddir)/src/include/cm/cm_errcodes.h

install: all installdirs
	$(INSTALL_STLIB) libcmcommon.a '$(DESTDIR)$(libdir)/libcmcommon.a'
	$(INSTALL_STLIB) libcmcommon.so.$(VERSION) '$(DESTDIR)$(libdir)/libcmcommon.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libcmcommon.so && \
	ln -sf libcmcommon.so.$(VERSION) libcmcommon.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmcommon.a'

$(top_builddir)/../distribute/cm/cm_etcdapi/libetcdapi.so:
	$(MAKE) -C $(top_builddir)/../distribute/cm/cm_etcdapi libetcdapi.so

distclean: clean

maintainer-clean: distclean
