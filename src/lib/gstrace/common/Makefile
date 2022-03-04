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
#        gstrace common makefile
# 
# IDENTIFICATION
#        src/lib/gstrace/common/Makefile
# 
# ---------------------------------------------------------------------------------------

top_builddir=../../../..
include $(top_builddir)/src/Makefile.global
subdir=src/lib/gstrace/common

include $(top_srcdir)/src/gausskernel/common.mk

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = $(top_builddir)/src/common/port/libpgport.a gstrace_infra.o
LIBS += -l$(SECURE_C_CHECK)
override CXXFLAGS := -I$(top_builddir)/src/include $(CXXFLAGS) -DHAVE_UNSETENV

all: libgstrace.a

libgstrace.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

gstrace_infra.o: gstrace_infra.cpp
	$(CC) -c $^  $(CXXFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -fPIC -ftls-model='initial-exec' -DENABLE_GSTRACE -o $@

install: all

clean:
	rm -f $(OBJS) *.so *.depend

distclean: clean

maintainer-clean: distclean

$(top_builddir)/src/common/port/libpgport.a:
	$(MAKE) -C $(top_builddir)/src/common/port/
