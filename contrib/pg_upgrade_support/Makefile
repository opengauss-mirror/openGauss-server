# contrib/pg_upgrade_support/Makefile

PGFILEDESC = "pg_upgrade_support - server-side functions for pg_upgrade"

MODULES = pg_upgrade_support

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_upgrade_support
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
ifeq ($(enable_debug), no)
  override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now
else
  override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC
endif
endif
