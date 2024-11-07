# contrib/gms_i18n/Makefile
MODULE_big = gms_i18n
OBJS	= gms_i18n.o

EXTENSION = gms_i18n
DATA = gms_i18n--1.0.sql

exclude_option = -fPIE
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS))

REGRESS = gms_i18n

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gms_i18n
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
regress_home = $(top_builddir)/src/test/regress
REGRESS_OPTS = -c 0 -d 1 -r 1 -p 25632 --single_node -w --keep_last_data=false  \
               --regconf=$(regress_home)/regress.conf                           \
               --temp-config=$(regress_home)/make_fastcheck_postgresql.conf
include $(top_srcdir)/contrib/contrib-global.mk
endif