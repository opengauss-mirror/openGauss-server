# contrib/pagehack/Makefile
MODULE_big	= pagehack
OBJS		= pagehack.o

# executable program, even there is no database server/client
PROGRAM = pagehack

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pagehack
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
enable_shared = false

ifeq ($(enable_debug), yes)
	PG_CPPFLAGS += -DDEBUG
endif

include $(top_srcdir)/contrib/contrib-global.mk
endif
