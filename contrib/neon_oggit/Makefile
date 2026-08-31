# contrib/neon_oggit/Makefile

MODULES = neon_oggit
OBJS = neon_oggit.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/neon_oggit
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

all: objfiles.txt

objfiles.txt: $(OBJS)
	@echo "" > objfiles.txt

objfiles_clean:
	rm -f objfiles.txt

clean: objfiles_clean
