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
#        Makefile for gs_cgroup
#
# IDENTIFICATION
#        src/bin/gs_cgroup/Makefile
#
# ---------------------------------------------------------------------------------------

top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/gs_cgroup

override CPPFLAGS := -I$(libpq_srcdir) -fPIE $(CPPFLAGS)
LDFLAGS += -pie
CFLAGS += -Wl,-z,relro,-z,now

LIBS := $(filter-out -lz -ledit -lssl -lcrypto , $(LIBS))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = cgconf.o cgexec.o cgptree.o gsutil.o gsconfig.o main.o cgexcp.o $(top_builddir)/src/lib/elog/elog.a

all:gs_cgroup

gs_cgroup: $(OBJS) 
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X) -lcgroup

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a

gscgroup: $(OBJS)
	$(CC) -fPIC -shared $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -lcgroup -o lib$@.so 
	mv lib$@.so $(top_builddir)/../distribute/test/ut/lib

install: all installdirs
	$(INSTALL_PROGRAM) gs_cgroup$(X) '$(DESTDIR)$(bindir)/gs_cgroup$(X)'
	chmod 755 ../scripts/runsessionstat$(X).sh && $(INSTALL_PROGRAM) ../scripts/runsessionstat$(X).sh '$(DESTDIR)$(bindir)/runsessionstat$(X).sh'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_cgroup$(X)'

clean distclean maintainer-clean:
	rm -f gs_cgroup$(X) $(OBJS) *.depend

