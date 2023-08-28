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
# Makefile--
#    Makefile for access/psort
#
# IDENTIFICATION
#    src/backend/access/psort/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/storage/access/redo
SUBDIRS = standby_read
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = redo_barrier.o redo_bufpage.o redo_clog.o redo_csnlog.o redo_dbcommands.o redo_ginxlog.o redo_gistxlog.o redo_hash.o redo_heapam.o redo_nbtpage.o redo_nbtxlog.o redo_pruneheap.o \
	redo_relmapper.o redo_sequence.o redo_slotfuncs.o redo_spgxlog.o redo_storage.o redo_tablespace.o redo_transam.o redo_visibilitymap.o redo_xact.o redo_xlog.o \
	xlogreader_common.o redo_xlogutils.o redo_ubtxlog.o redo_segpage.o

include $(top_srcdir)/src/gausskernel/common.mk
