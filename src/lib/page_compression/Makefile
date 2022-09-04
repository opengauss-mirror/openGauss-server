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

subdir = src/lib/page_compression
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

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -DFRONTEND -I. -I$(LZ4_INCLUDE_PATH) -std=c++14 # backend/utils/errcodes.h
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -DFRONTEND -I. -I$(LZ4_INCLUDE_PATH) -std=c++14 # backend/utils/errcodes.h

OBJS = PageCompression.o

TARGET = libpagecompression.a

all: libpagecompression.so

libpagecompression.so:
	sh page_compression.sh $(top_builddir)/src .
	$(CC) -fstack-protector-strong -Wl,-z,relro,-z,now -fPIC -shared $(CFLAGS) $(CPPFLAGS) PageCompression.cpp checksum_impl.cpp pg_lzcompress.cpp cfs_tools.cpp -o libpagecompression.so.$(VERSION)
	rm -f libpagecompression.so && \
	ln -s libpagecompression.so.$(VERSION) libpagecompression.so

install: all installdirs
	$(INSTALL_STLIB) libpagecompression.so.$(VERSION) '$(DESTDIR)$(libdir)/libpagecompression.so.$(VERSION)'
	cd '$(DESTDIR)$(libdir)' && \
	rm -f libpagecompression.so && \
	ln -s libpagecompression.so.$(VERSION) libpagecompression.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

clean distclean maintainer-clean:
	rm -f $(OBJS) libpagecompression.a pg_lzcompress.o checksum_impl.o cfs_tools.o libpagecompression.so libpagecompression.so.$(VERSION) *.depend
	rm -f checksum_impl.cpp pg_lzcompress.cpp cfs_tools.cpp
	rm -rf utils storage
