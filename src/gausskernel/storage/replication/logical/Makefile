

subdir = src/gausskernel/storage/replication/logical
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS)

OBJS = decode.o logical.o logicalfuncs.o reorderbuffer.o snapbuild.o

include $(top_srcdir)/src/gausskernel/common.mk
