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
#     src/gausskernel/storage/access/dfs/parquet/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage/access/dfs/parquet
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -std=c++11 $(filter-out -std=gnu++98, $(CPPFLAGS))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = parquet_reader.o parquet_file_reader.o parquet_column_reader.o parquet_input_stream_adapter.o

include $(top_srcdir)/src/gausskernel/common.mk

