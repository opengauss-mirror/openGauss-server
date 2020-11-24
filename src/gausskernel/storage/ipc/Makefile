subdir = src/gausskernel/storage/ipc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# seems to be required 1999/07/22 bjm
ifeq "$(findstring alpha,$(host_cpu))" "alpha"
ifeq "$(GCC)" "yes"
override CFLAGS+= -fno-inline
endif
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = ipc.o ipci.o pmsignal.o pg_barrier.o procarray.o procsignal.o shmem.o shmqueue.o \
	sinval.o sinvaladt.o standby.o shm_mq.o shm_toc.o dsm.o

include $(top_srcdir)/src/gausskernel/common.mk