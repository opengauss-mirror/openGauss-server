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
#     Makefile for the storage subsystem
# 
# IDENTIFICATION
#        src/gausskernel/storage/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     = access  bulkload  replication  buffer  cmgr  cstore  dfs  file  freespace  ipc  large_object  lmgr page  remote  smgr tcap sync dorado_operation  xlog_share_storage

ifeq ($(enable_mot), yes)
SUBDIRS     += mot
endif

include $(top_srcdir)/src/gausskernel/common.mk
