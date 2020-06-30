# contrib/cube/Makefile

MODULE_big = cube
OBJS= cube.o cubeparse.o

EXTENSION = cube
DATA = cube--1.0.sql cube--unpackaged--1.0.sql

REGRESS = cube

EXTRA_CLEAN = y.tab.cpp y.tab.h

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cube
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


# cubescan is compiled as part of cubeparse
cubeparse.o: cubescan.cpp cubeparse.cpp

cubeparse.cpp: cubeparse.y
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

cubescan.cpp: cubescan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

distprep: cubeparse.cpp cubescan.cpp

maintainer-clean:
	rm -f cubeparse.cpp cubescan.cpp
