

subdir = src/gausskernel/storage/replication/logical
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS)

OBJS = decode.o launcher.o logical.o logicalfuncs.o origin.o proto.o relation.o reorderbuffer.o snapbuild.o worker.o parallel_decode_worker.o parallel_decode.o parallel_reorderbuffer.o logical_queue.o logical_parse.o

include $(top_srcdir)/src/gausskernel/common.mk
