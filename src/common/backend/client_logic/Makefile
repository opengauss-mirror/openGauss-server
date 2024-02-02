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
DEPENDENCY_PATH = $(top_builddir)/src/common/interfaces/libpq/client_logic_common/
DEPENDENCY_FILE = cstrings_map.cpp
DEPENDENCY_FILE_O = $(patsubst %.cpp,%.o, $(DEPENDENCY_FILE))
cstrings_map.o: $(DEPENDENCY_PATH)/$(DEPENDENCY_FILE)
	@echo $(DEPENDENCY_PATH)$(DEPENDENCY_FILE)
	$(MAKE) -C $(DEPENDENCY_PATH) $(DEPENDENCY_FILE_O)
	@cp $(DEPENDENCY_PATH)$(DEPENDENCY_FILE_O) . 
OBJS= client_logic.o cache.o cstrings_map.o client_logic_proc.o

include $(top_srcdir)/src/gausskernel/common.mk
