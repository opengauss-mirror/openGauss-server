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
# -------------------------------------------------------------------------
# 
# Makefile
# 		 Makefile for src/bin/smb_mgr
# 
# IDENTIFICATION
# 		 src/bin/smb_mgr/Makefile
# 
# -------------------------------------------------------------------------

PGFILEDESC = "smb_mgr - smb shared memory manager"
PGAPPICON = win32

subdir = src/bin/smb_mgr
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -DFRONTEND -fPIE $(CPPFLAGS) -I${top_builddir}/src/include
override LDFLAGS := $(LDFLAGS) -L/usr/local/lib
RPATH_FLAG = -Wl,-rpath=/usr/local/softbus/ctrlbus/lib

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = smb_mgr.o matrix_mem.o $(WIN32RES)

link_matrix_mem:
	ln -fs $(top_builddir)/src/gausskernel/storage/matrix_mem/matrix_mem.cpp matrix_mem.cpp

matrix_mem.o: link_matrix_mem
	$(CC) $(CPPFLAGS) -c matrix_mem.cpp -o matrix_mem.o

all: $(OBJS)
	$(CC) -fPIE -pie $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) $(RPATH_FLAG) -o smb_mgr

install: all installdirs
	$(INSTALL_PROGRAM) smb_mgr$(X) '$(DESTDIR)$(bindir)/smb_mgr$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/smb_mgr$(X)'

clean distclean maintainer-clean:
	rm -f smb_mgr$(X) $(OBJS) *.depend
	rm -f matrix_mem.cpp