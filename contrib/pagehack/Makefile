# contrib/pagehack/Makefile
MODULE_big	= pagehack
OBJS		= pagehack.o \
	$(top_builddir)/src/gausskernel/storage/dss/dss_adaptor.o

# executable program, even there is no database server/client
PROGRAM = pagehack
all: submake-pagecompression

exclude_option = -fPIC
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pagehack
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS += -I${top_builddir}/src/lib/page_compression
override LDFLAGS += -L${top_builddir}/src/lib/page_compression -Wl,-z,relro,-z,now
override CFLAGS += -lpagecompression -lzstd -llz4 -fstack-protector-all
ifeq ($(enable_debug), yes)
	PG_CPPFLAGS += -DDEBUG
endif

include $(top_srcdir)/contrib/contrib-global.mk
endif