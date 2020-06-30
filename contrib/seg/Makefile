# contrib/seg/Makefile

MODULE_big = seg
OBJS = seg.o segparse.o

EXTENSION = seg
DATA = seg--1.0.sql seg--unpackaged--1.0.sql

REGRESS = seg

EXTRA_CLEAN = y.tab.cpp y.tab.h

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/seg
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


# segscan is compiled as part of segparse
segparse.o: segscan.cpp

segparse.cpp: segparse.y
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

segscan.cpp: segscan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

distprep: segparse.cpp segscan.cpp

maintainer-clean:
	rm -f segparse.cpp segscan.cpp
