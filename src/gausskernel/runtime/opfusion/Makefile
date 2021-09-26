#
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for opfusion
#
# IDENTIFICATION
#    src/gausskernel/runtime/opfusion/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/runtime/opfusion
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS:=-std=c++14 $(CPPFLAGS)
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = opfusion_agg.o opfusion_delete.o opfusion_index.o opfusion_indexonlyscan.o opfusion_indexscan.o \
       opfusion_insert.o opfusion_mot.o opfusion_scan.o opfusion_select.o opfusion_selectforupdate.o \
       opfusion_sort.o opfusion_uheaptablescan.o opfusion_update.o opfusion_util.o opfusion.o

override CPPFLAGS += -D__STDC_FORMAT_MACROS

include $(top_srcdir)/src/gausskernel/common.mk
