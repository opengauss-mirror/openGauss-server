# contrib/gtmtester/Makefile

PGFILEDESC = "gtmtester - a program for measuring gtm performance"

PROGRAM = gtmtester
OBJS	= gtmtester.o \
          $(top_builddir)/src/gtm/client/libgtmclient.a \
          $(top_builddir)/src/gtm/common/libgtmcommon.a \
          $(top_builddir)/src/gtm/libpq/libgtmpq.a \
          $(top_builddir)/src/bin/common/elog.a \
          $(top_builddir)/src/test/gtmtest/libgtmtest.a \
          
	
PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gtmtester
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif
