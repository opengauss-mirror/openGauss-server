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
#     Makefile for the MOT storage engine core components
#
# IDENTIFICATION
#        src/gausskernel/storage/mot/core/Makefile
#
# ---------------------------------------------------------------------------------------

all: mot_target


subdir=src/gausskernel/storage/mot/core
top_builddir ?= ../../../../../

include $(top_builddir)/src/Makefile.global

mot_target:
	@make -f Makefile.local

clean:
	@make -f Makefile.local clean

