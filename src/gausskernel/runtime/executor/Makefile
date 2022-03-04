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
#    Makefile for executor
#
# IDENTIFICATION
#    src/gausskernel/runtime/executor/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/runtime/executor
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

OBJS = execAmi.o execCurrent.o execGrouping.o execJunk.o execMain.o \
       execProcnode.o execQual.o execReplication.o execScan.o execTuples.o \
       execUtils.o functions.o instrument.o nodeAppend.o nodeAgg.o \
       nodeBitmapAnd.o nodeBitmapOr.o \
       nodeBitmapHeapscan.o nodeBitmapIndexscan.o nodeHash.o \
       nodeHashjoin.o nodeIndexscan.o nodeIndexonlyscan.o \
       nodeLimit.o nodeLockRows.o \
       nodeMaterial.o nodeMergeAppend.o nodeMergejoin.o nodeModifyTable.o \
       nodeNestloop.o nodeFunctionscan.o nodeRecursiveunion.o nodeResult.o \
       nodeSamplescan.o nodeSeqscan.o nodeSetOp.o nodeSort.o nodeUnique.o \
       nodeValuesscan.o nodeCtescan.o nodeStartWithOp.o nodeWorktablescan.o \
       nodeGroup.o nodeSubplan.o nodeSubqueryscan.o nodeTidscan.o \
       nodeForeignscan.o nodeWindowAgg.o tstoreReceiver.o spi.o \
       nodePartIterator.o nodeStub.o execClusterResize.o lightProxy.o execMerge.o \
       nodeExtensible.o route.o nodeTrainModel.o db4ai_common.o spiDbesql.o

override CPPFLAGS += -D__STDC_FORMAT_MACROS

include $(top_srcdir)/src/gausskernel/common.mk
