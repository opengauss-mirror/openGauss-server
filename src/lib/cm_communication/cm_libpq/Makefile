#----------------------------------------------------------------------------
#
# MPPDB CM libpq makefile
#
#
# distribute/cm/cm_communication/cm_libpq/Makefile
#
#-----------------------------------------------------------------------------
top_builddir = ../../../../
include $(top_builddir)/src/Makefile.global
subdir = src/lib/cm_communication/cm_libpq

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

OBJS = ip.o pqcomm.o pqformat.o strlcpy.o pqsignal.o

.NOTPARALLEL:
all: libcmpq.a libcmpq.so

libcmpq.so:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) ip.cpp pqcomm.cpp pqformat.cpp strlcpy.cpp pqsignal.cpp -o libcmpq.so.$(VERSION)
	rm -f libcmpq.so && \
	ln -s libcmpq.so.$(VERSION) libcmpq.so

libcmpq.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

install: all installdirs
	$(INSTALL_STLIB) libcmpq.a '$(DESTDIR)$(libdir)/libcmpq.a'
	$(INSTALL_STLIB) libcmpq.so.$(VERSION) '$(DESTDIR)$(libdir)/libcmpq.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libcmpq.so && \
	ln -s libcmpq.so.$(VERSION) libcmpq.so
	$(INSTALL_DATA) $(top_builddir)/src/include/cm/libpq-fe.h '$(DESTDIR)$(includedir)/cm-libpq-fe.h'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
	$(MKDIR_P) '$(DESTDIR)$(includedir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmpq.a'
	rm -f '$(DESTDIR)$(includedir)/cm-libpq-fe.h'

clean:
	rm -f $(OBJS) libcmpq.a libcmpq.so libcmpq.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
