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
#     Makefile for the MOT FDW adapter
# 
# IDENTIFICATION
#        src/gausskernel/storage/mot/fdw_adapter/Makefile
#
# ---------------------------------------------------------------------------------------

MODULE_big = mot_fdw

EXTENSION = mot_fdw

REGRESS = mot_fdw

EXTRA_CLEAN = sql/mot_fdw.sql expected/mot_fdw.out
DATA = mot_fdw--1.0.sql mot_fdw.control

subdir=src/gausskernel/storage/mot/fdw_adapter
top_builddir ?= ../../../../../

ENGINE_INC = $(top_builddir)/src/gausskernel/storage/mot/core

include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/mot_fdw.o $(OBJ_DIR)/mot_internal.o $(OBJ_DIR)/mot_fdw_xlog.o $(OBJ_DIR)/mot_match_index.o $(OBJ_DIR)/mot_fdw_error.o

DATA = mot_fdw.control mot_fdw--1.0.sql

DEPS := $(OBJ_DIR)/mot_fdw.d $(OBJ_DIR)/mot_internal.d $(OBJ_DIR)/mot_fdx_xlog.d $(OBJ_DIR)/mot_match_index.d $(OBJ_DIR)/mot_fdw_error.d

# Shared library stuff
include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -DMOT_SECURE -I$(top_builddir)/src/gausskernel/storage/mot/jit_exec -I$(top_builddir)/src/gausskernel/storage/mot/fdw_adapter -I$(ENGINE_INC) -I$(ENGINE_INC)/storage -I$(ENGINE_INC)/system -I$(ENGINE_INC)/memory -I$(ENGINE_INC)/memory/garbage_collector
override CXXFLAGS +=  -I$(ENGINE_INC)/infra -I$(ENGINE_INC)/infra/config -I$(ENGINE_INC)/infra/containers  -I$(ENGINE_INC)/infra/stats -I$(ENGINE_INC)/infra/synchronization -I$(ENGINE_INC)/concurrency_control -I$(ENGINE_INC)/storage/index -I$(ENGINE_INC)/system/transaction -I$(ENGINE_INC)/system/common -I$(ENGINE_INC)/system/statistics -I$(ENGINE_INC)/system/transaction_logger -I$(ENGINE_INC)/system/transaction_logger/asynchronous_redo_log -I$(ENGINE_INC)/system/transaction_logger/synchronous_redo_log -I$(ENGINE_INC)/system/transaction_logger/group_synchronous_redo_log -I$(ENGINE_INC)/system/checkpoint -I$(ENGINE_INC)/system/recovery -I$(ENGINE_INC)/utils

override CXXFLAGS += -faligned-new

$(OBJS): | buildrepo

install: install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'
	$(INSTALL_DATA) $(ENGINE_INC)/mot.conf '$(DESTDIR)$(datadir)/mot.conf.sample'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall: uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))
	rm -f $(DESTDIR)$(datadir)/mot.conf.sample

clean: 
	rm -rf $(OBJ_DIR)

buildrepo:
	@$(call make-repo)

$(OBJ_DIR)/%.o: %.cpp
	$(COMPILE.cpp) -std=c++11 -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

show:
	@echo "CC_VERSION=${CC_VERSION}"
	@echo "CC=${CC}"
	@echo
	@echo "DEBUG=${DEBUG}"
	@echo
	@echo "BUILD=${BUILD}"
	@echo
	@echo "SRC_DIRS=${SRC_DIRS}"
	@echo
	@echo "CPPFLAGS=${CPPFLAGS}"
	@echo
	@echo "CFLAGS=${CFLAGS}"
	@echo
	@echo "LDFLAGS=${LDFLAGS}"
	@echo
	@echo "CXXFLAGS=${CXXFLAGS}"
	@echo
	@echo "CPPS=${CPPS}"
	@echo
	@echo "OBJS=${OBJS}"

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR); 
endef

