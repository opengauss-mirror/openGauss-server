#
# Makefile for mutil ip test
#

subdir = src/test/mutil_ip
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(PORTNAME), win32)
LDLIBS += -lws2_32
endif

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
override LDLIBS := $(libpq_pgport) $(LDLIBS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
PROGS = testlibpq

check_mutilip: $(PROGS)
	export prefix=$(prefix) && sh check_mutilip.sh

clean:
	rm -f $(PROGS) *.depend
	rm -f ./results/*