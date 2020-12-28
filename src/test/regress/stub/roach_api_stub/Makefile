# contrib/roach_api_stub/Makefile

MODULE_big = roach_api_stub
OBJS = roach_api_stub.o

EXTENSION = roach_api_stub
DATA = roach_api_stub--1.0.sql

REGRESS = roach_api_stub

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/roach
top_builddir = ../../../../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC
endif
