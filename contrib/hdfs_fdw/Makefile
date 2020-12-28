# contrib/hdfs_fdw/Makefile

MODULE_big = hdfs_fdw
OBJS = hdfs_fdw.o scheduler.o
SHLIB_LINK = -lz $(shell pkg-config --libs libprotobuf)
top_builddir = ../..
EXTENSION = hdfs_fdw
DATA = hdfs_fdw--1.0.sql hdfs_fdw.control

REGRESS = hdfs_fdw

EXTRA_CLEAN = sql/hdfs_fdw.sql expected/hdfs_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hdfs_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -I$(top_builddir)/contrib/hdfs_fdw/include -I$(LIBCARBONDATA_INCLUDE_PATH) -I$(LIBTHRIFT_INCLUDE_PATH)
install:install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall:uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))
endif
