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
#     Makefile for storage/gs_cstore
# 
# IDENTIFICATION
#        src/gausskernel/storage/cstore/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage/cstore
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = compression

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = cu.o custorage.o cucache_mgr.o cstore_allocspace.o cstore_mem_alloc.o cstore_am.o cstore_delete.o cstore_insert.o cstore_psort.o cstore_update.o cstore_minmax_func.o cstore_roughcheck_func.o cstore_rewrite.o cstore_vector.o cstore_delta.o

include $(top_srcdir)/src/gausskernel/common.mk
