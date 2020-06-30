#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for client hotpatch 
#
# IDENTIFICATION
#    src/lib/hotpatch/client/Makefile
#
#-------------------------------------------------------------------------

subdir = src/lib/hotpatch/client
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = hotpatch_client.o 

include $(top_srcdir)/src/gausskernel/common.mk

all: libhotpatchclient.a


libhotpatchclient.a: $(OBJS) 
	$(AR) $(AROPT) $@ $^
clean:
	rm -f $(OBJS) libhotpatchclient.a *.depend

distclean: clean

maintainer-clean: distclean
