subdir = src/gausskernel/storage/access
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS	    = cbtree common dfs heap index nbtree ubtree psort rmgrdesc transam obs hash spgist gist gin hbstore redo table ustore 
ifeq ($(enable_lite_mode), no)
SUBDIRS += archive
endif

include $(top_srcdir)/src/gausskernel/common.mk
