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
#     Makefile for access/transam/ondemand_extreme_rto
# 
# IDENTIFICATION
#        src/gausskernel/storage/access/transam/ondemand_extreme_rto/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage/access/transam/ondemand_extreme_rto
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

OBJS = dispatcher.o page_redo.o posix_semaphore.o redo_item.o \
	spsc_blocking_queue.o txn_redo.o batch_redo.o xlog_read.o redo_utils.o

include $(top_srcdir)/src/gausskernel/common.mk
