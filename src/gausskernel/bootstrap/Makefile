#-------------------------------------------------------------------------
#
# Makefile for the bootstrap module
#
# src/backend/bootstrap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/bootstrap
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS= bootparse.o bootstrap.o

include $(top_srcdir)/src/gausskernel/common.mk


# bootscanner is compiled as part of bootparse
bootparse.o: bootscanner.inc

bootparse.cpp: bootparse.y $(top_builddir)/src/mtlocal.pl
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ $<
	$(PERL) $(top_builddir)/src/mtlocal.pl $(srcdir)/bootparse.cpp
else
	@$(missing) bison $< $@
endif

bootscanner.inc: bootscanner.l $(top_builddir)/src/mtlocal.pl
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	$(PERL) $(top_builddir)/src/mtlocal.pl $(srcdir)/bootscanner.inc
	sed -i 's/YY_NULL/YY_ZERO/g' bootscanner.inc
else
	@$(missing) flex $< $@
endif


# bootparse.cpp and bootscanner.inc are in the distribution tarball, so
# they are not cleaned here.
