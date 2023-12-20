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
# See the Mulan PSL v2 for more details.#
#
#----------------------------------------------------------------------------
#
# IDENTIFICATION
#   src/gausskernel/cbb/Makefile
#
#----------------------------------------------------------------------------
#

subdir = src/gausskernel/cbb
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS     = extension  grpc  instruments  communication  workload utils

ifeq ($(enable_bbox), yes)
SUBDIRS		+= bbox
endif
include $(top_srcdir)/src/gausskernel/common.mk
