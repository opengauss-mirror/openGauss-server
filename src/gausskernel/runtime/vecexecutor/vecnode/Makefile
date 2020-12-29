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
#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#    src/gausskernel/runtime/vecexecutor/vecnode/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/runtime/vecexecutor/vecnode
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS:=-std=c++14 $(CPPFLAGS)
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = vechashtable.o vecagg.o vecplainagg.o vecsortagg.o vechashagg.o  vecsort.o vechashjoin.o vecstream.o  vectortorow.o \
        veccstore.o veccstoreindexscan.o vecrowtovector.o \
	  vecforeignscan.o vecmodifytable.o vecremotequery.o vecresult.o  vecscan.o vecsubqueryscan.o vecpartiterator.o \
	   vecrescan.o vecappend.o veclimit.o vecconstraints.o vecsetop.o vecgroup.o vecunique.o vecgrpuniq.o vecmaterial.o vecnestloop.o \
       vecstore.o vecmergejoin.o vecwindowagg.o veccstoreindexheapscan.o veccstoreindexctidscan.o veccstoreindexand.o veccstoreindexor.o \
	   dfsscan.o vecsubplan.o vecdfsindexscan.o vecmergeinto.o vectsstorescan.o
override CPPFLAGS += -D__STDC_FORMAT_MACROS	  
 
include $(top_srcdir)/src/gausskernel/common.mk
