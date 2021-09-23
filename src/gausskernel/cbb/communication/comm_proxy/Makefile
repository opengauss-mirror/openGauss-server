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
# Postgres-XC CM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
# src/gausskernel/cbb/communication/comm_proxy/Makefile
#
# -----------------------------------------------------------------------------

subdir = src/gausskernel/cbb/communication/comm_proxy
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

#override CPPFLAGS := -I$(top_srcdir)/src/gausskernel/cbb/communication/ -I$(top_srcdir)/src/include/libcomm/ $(CPPFLAGS) -fpermissive -w -DUSE_LIBNET
#override CPPFLAGS := $(CPPFLAGS) -fpermissive -w -DUSE_LIBNET

override CPPFLAGS := -I$(top_srcdir)/src/gausskernel/cbb/communication/ -I$(top_srcdir)/src/include/libcomm/ $(CPPFLAGS)
override CPPFLAGS := $(CPPFLAGS) -DUSE_INLINE

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = comm_common.o comm_core.o comm_interface.o comm_thread.o comm_proxy.o comm_connection.o comm_dfx.o comm_buffer.o comm_controller.o comm_sock.o

include $(top_srcdir)/src/gausskernel/common.mk
