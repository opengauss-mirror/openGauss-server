subdir = src/common/interfaces/ecpg/test/preproc
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/$(subdir)/../Makefile.regress

TESTS = comment comment.cpp \
	define define.cpp \
	init init.cpp \
	strings strings.cpp \
	type type.cpp \
	whenever whenever.cpp \
	array_of_struct array_of_struct.cpp \
	cursor cursor.cpp \
	variable variable.cpp \
	outofscope outofscope.cpp \
	autoprep autoprep.cpp

all: $(TESTS)

array_of_struct.cpp:	array_of_struct.pgc ../regression.h
	$(ECPG) -c -o $@ -I$(srcdir) $<

autoprep.cpp:     autoprep.pgc ../regression.h
	$(ECPG) -r prepare -o $@ -I$(srcdir) $<

strings.cpp:     strings.pgc strings.h ../regression.h
	$(ECPG) -i -o $@ -I$(srcdir) $<
