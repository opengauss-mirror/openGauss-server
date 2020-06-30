# ---------------------------------------------------------------------------------------
# 
# Makefile
#        Makefile pg_check_replslot
# 
# IDENTIFICATION
#        contrib/pg_check_replslot/Makefile
# 
# ---------------------------------------------------------------------------------------

PGFILEDESC = "pg_check_replslot - a simple program for checking slot files"
PGAPPICON = win32

PROGRAM = pg_check_replslot
OBJS	= pg_check_replslot.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_check_replslot
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif
