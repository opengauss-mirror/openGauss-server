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
#        Alarm management client makefile
# 
# IDENTIFICATION
#        src/lib/alarm/Makefile
# 
# ---------------------------------------------------------------------------------------

top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir = src/alarm/client
override CPPFLAGS := -I. -I../common/ -I$(top_srcdir)/../Platform/N9000_RPC_Lib/ $(CPPFLAGS) -g3 -ggdb3  -O3 -l$(SECURE_C_CHECK)
LDFLAGS += $(top_srcdir)/../Platform/N9000_RPC_Lib/

include $(top_srcdir)/src/gausskernel/common.mk

ifneq "$(MAKECMDGOALS)" "clean"
   ifneq "$(MAKECMDGOALS)" "distclean"
      ifneq "$(shell which g++ |grep hutaf_llt |wc -l)"  "1"
         -include $(DEPEND)
      endif
   endif
endif
OBJS = alarm.o alarm_log.o


all: alarmclient.a

alarmclient.a: $(OBJS)
	$(AR) $(AROPT) $@ $^
	
utalarm: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS) -o libutalarm.so
	mv libutalarm.so $(top_builddir)/../distribute/test/ut/lib
	
install: all installdirs
	$(INSTALL_DATA) alarmItem.conf '$(DESTDIR)$(bindir)/alarmItem.conf'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

clean:
	rm -f $(OBJS) alarmclient.a *.depend

distclean: clean

maintainer-clean: distclean



