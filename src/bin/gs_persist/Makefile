#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# 
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
# 
#          http://license.coscl.org.cn/MulanPSL2
# 
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
# 
# Makefile
# 
# 
# IDENTIFICATION
#        src/bin/gs_persist/Makefile
# 
# ---------------------------------------------------------------------------------------
PGFILEDESC = "gs_persist - persist key for shared storage"
PGAPPICON=win32

top_builddir = ../../..
inc_dir = ./
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= gs_persist.o scsi_persist.o scsi_persistent_reserve.o

override CPPFLAGS := -I$(inc_dir) $(CPPFLAGS) -fPIE 

all: gs_persist

gs_persist: $(OBJS)
	$(CC) $(CXXFLAGS) $(CPPFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) gs_persist$(X) '$(DESTDIR)$(bindir)/gs_persist$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_persist$(X)'

clean distclean maintainer-clean:
	rm -f gs_persist$(X) $(OBJS) *.depend
