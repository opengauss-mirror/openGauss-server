#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for client_logic
#
# IDENTIFICATION
#    src/backend/client_logic/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/client_logic
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
.PHONY: copy_cstrings
copy_cstrings: 
	$(MAKE) -C $(top_builddir)/src/common/interfaces/libpq/client_logic_common cstrings_map.o
	cp $(top_builddir)/src/common/interfaces/libpq/client_logic_common/cstrings_map.o . 
cstrings_map.o: copy_cstrings
OBJS= client_logic.o cache.o cstrings_map.o client_logic_proc.o

include $(top_srcdir)/src/gausskernel/common.mk
