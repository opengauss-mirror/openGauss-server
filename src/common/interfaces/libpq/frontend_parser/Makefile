#-------------------------------------------------------------------------
#
# Makefile for src/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/frontend_parser
top_builddir = ../../../../../
backenddir = $(top_builddir)/src/common/backend

# shared library parameters
NAME= parser_frontend

override CPPFLAGS := -DFRONTEND_PARSER -DFRONTEND -DPGXC -fstack-protector-all -I$(srcdir) -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq -I$(top_builddir)/src/common/interfaces/libpq/frontend_parser $(CPPFLAGS)
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
#override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS:=parser.o parser_compatibility.o scansup.o frontend_mbutils.o list.o wstrncmp.o fe-wchar.o \
	fe-value.o fe-makefuncs.o fe-copyfuncs.o fe-define.o fe-parse_merge.o gram.o kwlookup.o 

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib
parser.o gram.o: scan.cpp
gram.hpp: gram.cpp
gram.o  kwlookup.o parser.o: gram.hpp $(top_srcdir)/src/include/parser/kwlist_d.h
scan.o: gram.hpp scan.cpp
#gram.cpp: build-parser

gram.cpp: gram.y
	/usr/bin/bison -d  -o gram.cpp gram.y -t
#	$(MAKE) -f Makefile.parser

scan.cpp: scan.l
	/usr/bin/flex -CF -b -p -o'scan.cpp' scan.l

fe-define.cpp: $(top_builddir)/src/gausskernel/optimizer/commands/define.cpp
	cp -v $(top_builddir)/src/gausskernel/optimizer/commands/define.cpp fe-define.cpp
fe-copyfuncs.cpp: $(backenddir)/nodes/copyfuncs.cpp
	cp -v $(backenddir)/nodes/copyfuncs.cpp fe-copyfuncs.cpp
fe-makefuncs.cpp: $(backenddir)/nodes/makefuncs.cpp
	cp -v $(backenddir)/nodes/makefuncs.cpp fe-makefuncs.cpp
fe-parse_merge.cpp: $(backenddir)/parser/parse_merge.cpp
	cp -v $(backenddir)/parser/parse_merge.cpp fe-parse_merge.cpp
fe-value.cpp: $(backenddir)/nodes/value.cpp
	cp -v $(backenddir)/nodes/value.cpp fe-value.cpp
fe-wchar.cpp: $(backenddir)/utils/mb/wchar.cpp
	cp -v $(backenddir)/utils/mb/wchar.cpp fe-wchar.cpp
softlinks: fe-define.cpp fe-copyfuncs.cpp fe-makefuncs.cpp fe-parse_merge.cpp fe-value.cpp fe-wchar.cpp
removefiles:
	rm -f fe-*.cpp
all:softlinks scan.o gram.o ${OBJS} all-static-lib

clean: clean-lib submake-kwlist-clean
	rm -f gram.cpp gram.hpp scan.cpp
	rm -f *.o *.a
	rm -f test


