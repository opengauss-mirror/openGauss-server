# contrib/rack_dev/Makefile

MODULE_big = rack_dev
OBJS = rack_dev.o rack_mem_tuple_desc.o
PG_CPPFLAGS = -I$(libpq_srcdir) -I${top_builddir}/contrib/rack_dev/ -I${top_builddir}/contrib/rack_dev/include

EXTENSION = rack_dev
DATA = rack_dev--1.0.sql

exclude_option=-fPIE
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS))

REGRESS = rack_dev

# the db name is hard-coded in the tests
override USE_MODULE_DB =

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/rack_dev
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
