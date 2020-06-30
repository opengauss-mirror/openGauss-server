subdir = src/gausskernel/storage/lmgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
ifneq ($(enable_thread_check), yes)
OBJS = lmgr.o lock.o proc.o deadlock.o lwlock.o spin.o s_lock.o predicate.o lwlock_be.o lwlocknames.o
else
OBJS = lmgr.o lock.o proc.o deadlock.o lwlock.o spin.o s_lock.o predicate.o lwlock_be.o lwlocknames.o
endif

include $(top_srcdir)/src/gausskernel/common.mk

ifdef TAS
TASPATH = $(top_builddir)/src/common/backend/port/tas.o
endif

s_lock_test: s_lock.cpp $(top_builddir)/src/common/port/libpgport.a
	$(CC) $(CPPFLAGS) $(CFLAGS) -DS_LOCK_TEST=1 $(srcdir)/s_lock.cpp \
		$(TASPATH) -L $(top_builddir)/src/port -lpgport -o s_lock_test

lwlocknames.cpp: lwlocknames.h
	touch $@

lwlocknames.h: $(top_srcdir)/src/gausskernel/storage/lmgr/lwlocknames.txt generate-lwlocknames.pl
	$(PERL) $(srcdir)/generate-lwlocknames.pl $<

check: s_lock_test
	./s_lock_test

clean distclean maintainer-clean:
	rm -f s_lock_test
	rm -f lwlocknames.h lwlocknames.cpp
