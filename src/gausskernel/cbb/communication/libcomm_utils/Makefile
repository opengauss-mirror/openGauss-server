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
# -------------------------------------------------------------------------
#
# IDENTIFICATION
#    src/gausskernel/cbb/communication/libcomm_utils/Makefile
#
# -------------------------------------------------------------------------

subdir = src/gausskernel/cbb/communication/libcomm_utils
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global


override CPPFLAGS := -I$(top_srcdir)/src/gausskernel/cbb/communication/ -I$(top_srcdir)/src/include/libcomm/ $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = libcomm_util.o libcomm_err.o libcomm_list.o libcomm_thread.o libcomm_lqueue.o libcomm_queue.o libcomm_perf.o \
      libcomm_adapter.o libcomm_interface.o libcomm_shakehands.o libcomm_memory.o libcomm_client_ssl.o libcomm_server_ssl.o

include $(top_srcdir)/src/gausskernel/common.mk
