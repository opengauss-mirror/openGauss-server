#---------------------------------------------------------------------------------------
#
#  IDENTIFICATION
#        src/gausskernel/optimizer/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS     = geqo path plan prep util rewrite commands sqladvisor sqlpatch gplanmgr aioptimizer
else
SUBDIRS     = geqo path plan prep util rewrite commands sqladvisor sqlpatch statistics gplanmgr aioptimizer
endif

include $(top_srcdir)/src/gausskernel/common.mk
