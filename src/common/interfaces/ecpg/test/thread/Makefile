subdir = src/common/interfaces/ecpg/test/thread
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/$(subdir)/../Makefile.regress


TESTS = thread_implicit thread_implicit.cpp \
        thread thread.cpp \
        prep prep.cpp \
        descriptor descriptor.cpp \
        alloc alloc.cpp

all: $(TESTS)
