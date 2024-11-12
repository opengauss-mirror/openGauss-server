# contrib/gms_utility/Makefile
MODULE_big = gms_utility
OBJS	= gms_utility.o

EXTENSION = gms_utility
DATA = gms_utility--1.0.sql

exclude_option = -fPIE
override CPPFLAGS := -fstack-protector-strong $(filter-out $(exclude_option),$(CPPFLAGS))

REGRESS = gms_utility

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gms_utility
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
regress_home = $(top_builddir)/src/test/regress
REGRESS_OPTS = -c 0 -d 1 -r 1 -p 36789 --single_node -w --keep_last_data=false  \
               --regconf=$(regress_home)/regress.conf                           \
               --temp-config=$(regress_home)/make_fastcheck_postgresql.conf
include $(top_srcdir)/contrib/contrib-global.mk
endif

gms_utility.o: gms_utility.cpp

check_utility:
	chmod +x ./smartmatch.pl
	make check