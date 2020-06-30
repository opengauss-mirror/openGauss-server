# ---------------------------------------------------------------------------------------
# 
# Makefile
#        Makefile for pg_check_clog
# 
# IDENTIFICATION
#        contrib/pg_check_clog/Makefile
# 
# ---------------------------------------------------------------------------------------
PGFILEDESC = "pg_check_clog - a simple program for checking clog files"
PGAPPICON = win32

PROGRAM = pg_check_clog
OBJS	= pg_check_clog.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_check_clog
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif
