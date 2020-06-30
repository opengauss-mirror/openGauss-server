top_builddir=../../../../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gausskernel/storage/replication/heartbeat/libpq

VERSION = 1

include $(top_srcdir)/src/gausskernel/common.mk

override CFLAGS += -fstack-protector-strong -Wl,-z,relro,-z,now 

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

OBJS = pqcomm.o fe-connect.o

all: libhbpq.a libhbpq.so

libhbpq.so:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) pqcomm.cpp fe-connect.cpp -o libhbpq.so.$(VERSION)
	rm -f libhbpq.so && \
	ln -s libhbpq.so.$(VERSION) libhbpq.so

libhbpq.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

install: all installdirs
	$(INSTALL_STLIB) libhbpq.a '$(DESTDIR)$(libdir)/libhbpq.a'
	$(INSTALL_STLIB) libhbpq.so.$(VERSION) '$(DESTDIR)$(libdir)/libhbpq.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libhbpq.so && \
	ln -s libhbpq.so.$(VERSION) libhbpq.so
	$(INSTALL_DATA) $(top_builddir)/src/include/hb/libpq-fe.h '$(DESTDIR)$(includedir)/hb-libpq-fe.h'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
	$(MKDIR_P) '$(DESTDIR)$(includedir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libhbpq.a'
	rm -f '$(DESTDIR)$(includedir)/hb-libpq-fe.h'

clean:
	rm -f $(OBJS) libhbpq.a libhbpq.so libhbpq.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
