# contrib/gms_xmlgen/Makefile
MODULE_big = gms_xmlgen
OBJS = gms_xmlgen.o

EXTENSION = gms_xmlgen
DATA = gms_xmlgen--1.0.sql

exclude_option = -fPIE
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -fstack-protector-strong $(filter-out $(exclude_option),$(CPPFLAGS)) -I$(LIBXML_INCLUDE_PATH)/libxml2
REGRESS = gms_xmlgen

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gms_xmlgen
regress_home = $(top_builddir)/src/test/regress
REGRESS_OPTS = -c 0 -d 1 -r 1 -p 25633 --single_node -w --keep_last_data=false  \
               --regconf=$(regress_home)/regress.conf                           \
               --temp-config=$(regress_home)/make_fastcheck_postgresql.conf
include $(top_srcdir)/contrib/contrib-global.mk
endif
