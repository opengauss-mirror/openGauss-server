# contrib/gms_lob/Makefile
MODULE_big = gms_lob
OBJS	= gms_lob.o

EXTENSION = gms_lob
DATA = gms_lob--1.0.sql

exclude_option = -fPIE
override CPPFLAGS := -fstack-protector-strong $(filter-out $(exclude_option),$(CPPFLAGS))

REGRESS = gms_lob

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gms_lob
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
regress_home = $(top_builddir)/src/test/regress
REGRESS_OPTS = -c 0 -d 1 -r 1 -p 25632 --single_node -w --keep_last_data=false  \
               --regconf=$(regress_home)/regress.conf                           \
               --temp-config=$(regress_home)/make_fastcheck_postgresql.conf
include $(top_srcdir)/contrib/contrib-global.mk
endif

gms_lob.o: gms_lob.cpp
