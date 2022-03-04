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
#        Makefile for src/lib/config/
# 
# IDENTIFICATION
#       src/lib/config/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/common/lib/config
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

VERSION = 4

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC

OBJS = cm_config.o

TARGET = libconfig.a

all: libconfig.a libconfig.so

libconfig.so:
	$(CC) -fstack-protector-strong -Wl,-z,relro,-z,now -fPIC -shared $(CFLAGS) $(CPPFLAGS) cm_config.cpp -o libconfig.so.$(VERSION)
	rm -f libconfig.so && \
	ln -s libconfig.so.$(VERSION) libconfig.so

libconfig.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

utconfig:
	$(CC) -fPIC -shared $(CFLAGS) $(CPPFLAGS) cm_config.cpp -L$(SECURE_LIB_PATH) -l$(SECURE_C_CHECK) -o libutconfig.so
	mv libutconfig.so $(top_builddir)/../distribute/test/ut/lib/

install: all installdirs
	$(INSTALL_STLIB) libconfig.so.$(VERSION) '$(DESTDIR)$(libdir)/libconfig.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libconfig.so && \
	ln -s libconfig.so.$(VERSION) libconfig.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

clean distclean maintainer-clean:
	rm -f $(OBJS) libconfig.a libconfig.so libconfig.so.$(VERSION) *.depend
