# contrib/pg_upgrade/Makefile

PGFILEDESC = "pg_upgrade - an in-place binary upgrade utility"
PGAPPICON = win32

PROGRAM  = gs_upgrade
OBJS = check.o controldata.o dump.o exec.o file.o function.o info.o \
       option.o page.o pg_upgrade.o relfilenode.o server.o \
       tablespace.o util.o version.o version_old_8_3.o cluster_upgrade.o cluster_config.o $(WIN32RES)

PG_CPPFLAGS  = -DFRONTEND -DDLSUFFIX=\"$(DLSUFFIX)\" -I$(srcdir) -I$(libpq_srcdir) -DGS_COLLECTOR_BUILD -Wl,-z,relro,-z,now -fPIE
PG_LIBS = $(libpq_pgport) $(top_builddir)/src/lib/config/libconfig.a -pie -Wl,-z,relro,-z,now -lgssapi_krb5_gauss -lkrb5_gauss -lk5crypto_gauss -lcom_err_gauss -lkrb5support_gauss

EXTRA_CLEAN = analyze_new_cluster.sh delete_old_cluster.sh log/ tmp_check/

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_upgrade
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: test.sh all
	MAKE=$(MAKE) bindir=$(bindir) libdir=$(libdir) $(SHELL) $< --install
