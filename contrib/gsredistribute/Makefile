# contrib/gsredistribute/Makefile

MODULE_big  = gsredistribute
OBJS        = gsredistribute.o

EXTENSION = gsredistribute
DATA = gsredistribute--1.0.sql gsredistribute--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gsredistribute
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
ifeq ($(enable_debug), no)
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC -fPIE
else
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC -fPIE
endif 
endif
