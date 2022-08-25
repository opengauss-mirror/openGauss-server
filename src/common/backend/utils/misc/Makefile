#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/misc
#
# IDENTIFICATION
#    src/common/backend/utils/misc/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/utils/misc
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = guc.o help_config.o pg_rusage.o pgfincore.o ps_status.o superuser.o tzparser.o \
       rbtree.o anls_opt.o sec_rls_utils.o elf_parser.o pg_controldata.o oidrbtree.o

# This location might depend on the installation directories. Therefore
# we can't subsitute it into pg_config.h.
ifdef krb_srvtab
override CPPFLAGS += -DPG_KRB_SRVTAB='"$(krb_srvtab)"'
endif

SUBDIRS = guc

include $(top_srcdir)/src/gausskernel/common.mk

# guc-file is compiled as part of guc
guc.o: guc-file.inc

guc-file.inc: guc-file.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

# Note: guc-file.inc is not deleted by 'make clean',
# since we want to ship it in distribution tarballs.
clean:
	@rm -f lex.yy.cpp
