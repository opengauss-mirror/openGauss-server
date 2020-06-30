subdir = src/common/interfaces/ecpg/test/pgtypeslib
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/$(subdir)/../Makefile.regress

TESTS = dt_test dt_test.cpp \
        dt_test2 dt_test2.cpp \
        num_test num_test.cpp \
        num_test2 num_test2.cpp \
        nan_test nan_test.cpp

all: $(TESTS)
