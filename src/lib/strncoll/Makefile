#
# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
#        Makefile for src/lib/strncoll/
#
# IDENTIFICATION
#       src/lib/strncoll/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/lib/strncoll
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

VERSION = 1

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -I. -I$(top_builddir)/src/include -std=c++14 -DHAVE_LOCALE_T=1
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -I. -I$(top_builddir)/src/include -std=c++14 -DHAVE_LOCALE_T=1

OBJS = strncoll.o

TARGET = libstrncoll.a

all: libstrncoll.so

libstrncoll.so: strncoll.cpp $(top_srcdir)/src/include/utils/strncoll_glibc.h
	$(CC) -fstack-protector-strong -Wl,-z,relro,-z,now -fPIC -shared $(CFLAGS) $(CPPFLAGS) strncoll.cpp -o libstrncoll.so.$(VERSION)
	rm -f libstrncoll.so && \
	ln -s libstrncoll.so.$(VERSION) libstrncoll.so

install: all installdirs
	$(INSTALL_STLIB) libstrncoll.so.$(VERSION) '$(DESTDIR)$(libdir)/libstrncoll.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libstrncoll.so && \
	ln -s libstrncoll.so.$(VERSION) libstrncoll.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

clean distclean maintainer-clean:
	rm -f $(OBJS) libstrncoll.a libstrncoll.so libstrncoll.so.$(VERSION) *.depend