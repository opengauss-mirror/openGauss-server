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
#        Makefile for gstrace
# 
# openGauss GTM makefile
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#        src/lib/gstrace/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/lib/gstrace
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global


# Supress parallel build to avoid depencies in the subdirectories. 
.NOTPARALLEL: 

SUBDIRS = common tool 
CONFIGFILE = $(wildcard ./config/*.in)
MODULECONFIGFILE = $(filter-out ./config/comps.in,$(CONFIGFILE))
HEADERFILENAME = $(subst .in,_gstrace.h,$(MODULECONFIGFILE))
HEADERFILENAME_WITHPATH = $(patsubst ./config/%,./../../include/gstrace/%,$(HEADERFILENAME))
HEADERFILE += $(HEADERFILENAME_WITHPATH)
HEADERFILE += ./../../include/gstrace/comps.h ./../../include/gstrace/funcs.comps.h

all: $(HEADERFILE) 

install: all 

$(HEADERFILE): $(CONFIGFILE)
	python ./script/trc_gen.py -s ./config/ -o ./../../include/gstrace

clean:
	rm -f ./../../include/gstrace/*_gstrace.h
	rm -f ./../../include/gstrace/comps.h
	rm -f ./../../include/gstrace/funcs.comps.h

.PHONY: install clean

$(recurse)
