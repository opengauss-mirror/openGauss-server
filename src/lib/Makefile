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
#        
# 
# IDENTIFICATION
#        src/lib/Makefile
# 
# ---------------------------------------------------------------------------------------

PGFILEDESC = "common library"
subdir = src/lib
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = build_query config pgcommon alarm gstrace page_compression hotpatch cm_common cm_communication

$(recurse)

