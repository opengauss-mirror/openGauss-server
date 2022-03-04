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
#     Makefile for the access methods module
# 
# IDENTIFICATION
#     src/gausskernel/storage/access/dfs/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage/access/dfs
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_lite_mode), no)
SUBDIRS = orc text csv parquet
else
SUBDIRS = text csv
endif

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS += carbondata
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = dfs_uri.o dfs_am.o dfs_delete.o dfs_insert.o dfs_parinsert.o dfs_query.o dfs_query_check.o dfs_query_reader.o dfs_update.o dfsdesc.o  dfs_stream_factory.o common_parser.o common_reader.o

include $(top_srcdir)/src/gausskernel/common.mk
