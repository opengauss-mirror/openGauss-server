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
#     Makefile for storage/cmgr
# 
# IDENTIFICATION
#        src/gausskernel/storage/cmgr/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/storage/htap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CPPFLAGS += -I${top_builddir}/contrib/ndpplugin

OBJS = imcs_hash_table.o imcucache_mgr.o ss_imcucache_mgr.o imcstore_insert.o imcs_ctlg.o imcustorage.o imcstore_am.o imcstore_vacuum.o imcstore_delta.o borrow_mem_pool.o share_mem_pool.o

include $(top_srcdir)/src/gausskernel/common.mk
