subdir = src/gausskernel/storage/access
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS	    = cbtree common heap index nbtree ubtree psort rmgrdesc transam obs hash spgist gist gin hbstore redo table ustore datavec ubtreepcr
ifeq ($(enable_lite_mode), no)
ifeq ($(with_obs), yes)
    SUBDIRS += archive
endif
endif

include $(top_srcdir)/src/gausskernel/common.mk
