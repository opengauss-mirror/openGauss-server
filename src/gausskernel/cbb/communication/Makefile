#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#         http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#
# ----------------------------------------------------------------------------
#
# openGauss CM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
# src/gausskernel/cbb/communication/Makefile
#
# -----------------------------------------------------------------------------
subdir = src/gausskernel/cbb/communication
top_builddir=../../../..

include $(top_builddir)/src/Makefile.global

ifeq ($(enable_thread_check), yes)
  override CPPFLAGS :=  -I$(top_srcdir)/src/include/libcomm -I../ -I./ $(CPPFLAGS) -fno-non-call-exceptions 
else
  override CPPFLAGS :=  -I$(top_srcdir)/src/include/libcomm -I../ -I./ $(CPPFLAGS)
endif

SUBDIRS = libcomm_core libcomm_utils comm_proxy

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = libcomm.o libcomm_common.o

include $(top_srcdir)/src/gausskernel/common.mk

clean:
	rm -f $(OBJS)

distclean: clean

maintainer-clean: distclean
