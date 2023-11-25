# contrib/pg_xlogdump/Makefile

PGFILEDESC = "pg_xlogdump - decode and display WAL"
PGAPPICON=win32

PROGRAM = pg_xlogdump
OBJS = pg_xlogdump.o compat.o xlogreader.o xlogreader_common.o rmgrdesc.o \
	$(top_builddir)/src/gausskernel/storage/smgr/cfs/cfs_mddesc.o \
	$(top_builddir)/src/lib/pgcommon/libpgcommon.a \
	$(RMGRDESCOBJS) $(WIN32RES) \
	$(top_builddir)/src/gausskernel/storage/dss/dss_adaptor.o \
	$(top_builddir)/src/gausskernel/storage/gs_uwal/gs_uwal_adaptor.o

RMGRDESCSOURCES = $(notdir $(wildcard $(top_srcdir)/src/gausskernel/storage/access/rmgrdesc/*desc.cpp))
RMGRDESCOBJS = $(patsubst %.cpp,%.o,$(RMGRDESCSOURCES))

EXTRA_CLEAN = $(RMGRDESCSOURCES) xlogreader.cpp

subdir = contrib/pg_xlogdump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk


override CPPFLAGS := -DFRONTEND $(CPPFLAGS)  -fstack-protector-all -Wl,-z,relro,-z,now
override LDFLAGS += -Wl,-z,relro,-z,now
override CFLAGS += -fstack-protector-all

xlogreader.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/transam/%
	rm -f $@ && $(LN_S) $< .
xlogreader_common.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/redo/%
	rm -f $@ && $(LN_S) $< .
$(RMGRDESCSOURCES): % : $(top_srcdir)/src/gausskernel/storage/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

$(top_builddir)/src/lib/pgcommon/libpgcommon.a:
	$(MAKE) -C $(top_builddir)/src/lib/pgcommon libpgcommon.a
