# contrib/ndpplugin/Makefile
top_builddir = ../../

RPC_INCLUDE = ./

RPC_CPPFLAGS = $(addprefix -I, $(RPC_INCLUDE)) -DNDP_CLIENT -DGlobalCache

OBJS := ndpplugin.o dynloader.o rpc.o ndp_check.o ndpoutfuncs.o ndpam.o
CPPFLAGS += $(RPC_CPPFLAGS)

MODULE_big = ndpplugin
EXTENSION = ndpplugin
REGRESS = ndpplugin
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress -c 0 -d 1 --single_node

DATA = ndpplugin--1.0.sql

override CPPFLAGS :=$(filter-out -fPIE, $(CPPFLAGS)) -fPIC

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/ndpplugin
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
