#----------------------------------------------------------------------------
#
# MPPDB CM feconnect makefile
#
#
# distribute/cm/cm_communication/cm_feconnect/Makefile
#
#-----------------------------------------------------------------------------
top_builddir = ../../../../
include $(top_builddir)/src/Makefile.global
subdir = src/lib/cm_communication/cm_feconnect

VERSION = 1

override CFLAGS += -fstack-protector-strong -Wl,-z,relro,-z,now 
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

include $(top_srcdir)/src/gausskernel/common.mk

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = fe-misc.o fe-connect.o fe-protocol.o ip.o pqexpbuffer.o

all: libcmclient.a libcmclient.so

libcmclient.so:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) fe-misc.cpp fe-connect.cpp fe-protocol.cpp ip.cpp pqexpbuffer.cpp -o libcmclient.so.$(VERSION)
	rm -f libcmclient.so && \
	ln -sf libcmclient.so.$(VERSION) libcmclient.so

libcmclient.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

install: all installdirs
	$(INSTALL_STLIB) libcmclient.so.$(VERSION) '$(DESTDIR)$(libdir)/libcmclient.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libcmclient.so && \
	ln -sf libcmclient.so.$(VERSION) libcmclient.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

utcmclient:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) fe-misc.o pqexpbuffer.o -L$(SECURE_LIB_PATH) -l$(SECURE_C_CHECK) -o libutcmclient.so
	mv libutcmclient.so $(top_builddir)/../distribute/test/ut/lib/

clean:
	rm -f $(OBJS) libcmclient.a libcmclient.so libcmclient.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
