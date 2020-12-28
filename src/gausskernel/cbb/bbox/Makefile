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
# See the Mulan PSL v2 for more details.
#
# -------------------------------------------------------------------------
#
# IDENTIFICATION
# src/gausskernel/cbb/bbox
#
# -------------------------------------------------------------------------

subdir = src/gausskernel/cbb/bbox
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

LIBS := $(filter-out -lssl -lcrypto, $(LIBS))

# be-fsstubs is here for historical reasons, probably belongs elsewhere

ifneq "$(MAKECMDGOALS)" "clean"
   ifneq "$(MAKECMDGOALS)" "distclean"
      ifneq "$(shell which g++ |grep hutaf_llt |wc -l)"  "1"
         -include $(DEPEND)
      endif
   endif
endif
OBJS =  bbox_create.o bbox_elf_dump.o bbox_lib.o bbox_print.o bbox_syscall_support.o \
		bbox_threads.o bbox_elf_dump_base.o gs_bbox.o

include $(top_srcdir)/src/gausskernel/common.mk

