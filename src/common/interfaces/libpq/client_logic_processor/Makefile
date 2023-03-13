#-------------------------------------------------------------------------
#
# Makefile for src/interfaces/libpq library
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/common/interfaces/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/parser_frontend
top_builddir = ../../../../../

# shared library parameters
NAME= client_logic_processor

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DPGXC -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/include/libpq -I../
override CPPFLAGS += -I$(top_builddir)/src/common/interfaces/libpq/client_logic_cache/include
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all -DHAVE_CE
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC  -fstack-protector-all

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=raw_value.o raw_values_cont.o processor_utils.o pre_stmt_processor.o post_stmt_processor.o stmt_processor.o func_processor.o ../frontend_parser/parser.o ../client_logic_common/client_logic_utils.o where_clause_processor.o values_processor.o create_stmt_processor.o
OBJS+=prepared_statements_list.o prepared_statement.o func_hardcoded_values.o

include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

../frontend_parser/parser.o:  ../frontend_parser/gram.hpp
	$(MAKE) -C ../frontend_parser parser.o
../frontend_parser/gram.hpp:
	$(MAKE) -C ../frontend_parser gram.hpp
../client_logic_cache/types_to_oid.h:
	$(MAKE) -C ../client_logic_cache types_to_oid.h
stmt_processor.o: ../client_logic_cache/types_to_oid.h
../client_logic_common/client_logic_utils.o: 
	$(MAKE) -C ../client_logic_common client_logic_utils.o
all: ../frontend_parser/gram.hpp  all-static-lib
installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f *.o
	rm -f ../frontend_parser/parser.o ../../../common/client_logic_utils/client_logic_utils.o

maintainer-clean: distclean maintainer-clean-lib
test: 
	g++ -g  -fPIE -O0 --std=c++14-o test main.cpp  -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/interfaces/libpq \
		-L$(top_builddir)/src/interfaces/libpq -lpq -L../frontend_parser -lparser_frontend
