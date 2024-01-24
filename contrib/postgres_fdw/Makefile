# contrib/postgres_fdw/Makefile

MODULE_big = postgres_fdw
OBJS = postgres_fdw.o option.o deparse.o connection.o internal_interface.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql

REGRESS = postgres_fdw

# the db name is hard-coded in the tests
override USE_MODULE_DB =

all: objfiles.txt
     
 # generate postgres_fdw objfiles.txt to suspend duplicate make of gaussdb
objfiles.txt: $(OBJS)
	@echo "" > objfiles.txt

objfiles_clean:
	rm -f objfiles.txt

clean: objfiles_clean

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

exclude_option=-fPIE
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS))
