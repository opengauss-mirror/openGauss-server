# View README.gs_filedump.md first

# note this must match version macros in gs_filedump.h
PGFILEDESC = "gs_filedump - decode and display datafile"
PGAPPICON = win32

# OBJS = gs_filedump.o decode.o decode_ustore.o stringinfo.o checksum.o \

PROGRAM = gs_filedump
OBJS = gs_filedump.o decode.o stringinfo.o checksum.o segment.o \
			$(top_builddir)/src/bin/pg_probackup/pg_lzcompress.o \

REGRESS = datatypes float numeric xml toast
TAP_TESTS = 1
EXTRA_CLEAN = *.heap $(wildcard [1-9]???[0-9]) # testsuite leftovers

subdir = contrib/gs_filedump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

