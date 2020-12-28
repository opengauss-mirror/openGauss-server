#
# Makefile for the common subsystem
#
# src/common/backend/Makefile
#

subdir = src/common/backend
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS     = catalog  client_logic  lib  libpq  nodes  parser  port  regex  snowball  tsearch  utils
else
SUBDIRS     = catalog  client_logic  lib  libpq  nodes  parser  port  regex  snowball  tsearch  utils pgxc_single
endif

include $(top_srcdir)/src/gausskernel/common.mk
