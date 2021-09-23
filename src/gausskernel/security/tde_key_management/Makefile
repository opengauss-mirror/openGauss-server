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
#     Makefile for TDE Key Management
#
# IDENTIFICATION
#        src/gausskernel/security/tde_key_management/Makefile
#
# ---------------------------------------------------------------------------------------

subdir=src/gausskernel/security/tde_key_management
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := $(CPPFLAGS) -I $(top_builddir)/src/include/tde_key_management

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= ckms_message.o kms_interface.o data_common.o tde_key_manager.o tde_key_storage.o http_common.o

include $(top_srcdir)/src/gausskernel/common.mk

