subdir = src/gausskernel/storage/access/ustore
top_builddir = ../../../../../

include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

OBJS = knl_uheap.o knl_upage.o knl_utuple.o knl_uhio.o knl_uscan.o knl_uvisibility.o knl_uvacuumlazy.o knl_uundorecord.o knl_uundovec.o knl_undolauncher.o knl_undoworker.o knl_undorequest.o knl_undoaction.o knl_pruneuheap.o knl_uredo.o knl_uextremeredo.o knl_uam.o knl_utuptoaster.o $(top_builddir)/src/test/whitebox/knl_whitebox_test.o knl_uverify.o
SUBDIRS = undo
include $(top_srcdir)/src/gausskernel/common.mk
