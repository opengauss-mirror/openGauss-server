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
#     Makefile for the MOT lite executor
#
# IDENTIFICATION
#        src/gausskernel/storage/mot/jit_exec/Makefile
#
# ---------------------------------------------------------------------------------------

MODULE_big = jit_exec

EXTENSION = jit_exec

REGRESS = jit_exec


subdir = src/gausskernel/storage/mot/jit_exec
top_builddir = ../../../../../
ENGINE_INC = ../core

include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj
OBJS = $(OBJ_DIR)/jit_exec.o $(OBJ_DIR)/jit_common.o $(OBJ_DIR)/jit_llvm_query_codegen.o $(OBJ_DIR)/jit_llvm_blocks.o $(OBJ_DIR)/jit_tvm_query_codegen.o $(OBJ_DIR)/jit_tvm_blocks.o $(OBJ_DIR)/jit_helpers.o $(OBJ_DIR)/jit_context.o $(OBJ_DIR)/jit_source.o $(OBJ_DIR)/jit_source_pool.o $(OBJ_DIR)/jit_source_map.o $(OBJ_DIR)/jit_context_pool.o $(OBJ_DIR)/jit_plan.o $(OBJ_DIR)/jit_plan_expr.o $(OBJ_DIR)/jit_explain.o $(OBJ_DIR)/jit_llvm_util.o $(OBJ_DIR)/jit_tvm_util.o $(OBJ_DIR)/jit_llvm.o $(OBJ_DIR)/jit_tvm.o $(OBJ_DIR)/jit_statistics.o

DEPS := $(OBJ_DIR)/jit_exec.d $(OBJ_DIR)/jit_common.d $(OBJ_DIR)/jit_llvm_query_codegen.d $(OBJ_DIR)/jit_llvm_blocks.d $(OBJ_DIR)/jit_tvm_query_codegen.d $(OBJ_DIR)/jit_tvm_blocks.d $(OBJ_DIR)/jit_helpers.d $(OBJ_DIR)/jit_context.d $(OBJ_DIR)/jit_source.d $(OBJ_DIR)/jit_source_pool.d $(OBJ_DIR)/jit_source_map.d $(OBJ_DIR)/jit_context_pool.d $(OBJ_DIR)/jit_plan.d $(OBJ_DIR)/jit_plan_expr.d $(OBJ_DIR)/jit_explain.d $(OBJ_DIR)/jit_llvm_util.d $(OBJ_DIR)/jit_tvm_util.d $(OBJ_DIR)/jit_llvm.d $(OBJ_DIR)/jit_tvm.d $(OBJ_DIR)/jit_statistics.d

# Shared library stuff
include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -DMOT_SECURE -I$(top_builddir)/src/gausskernel/storage/mot/jit_exec -I$(top_builddir)/src/gausskernel/storage/mot/fdw_adapter -I$(ENGINE_INC) -I$(ENGINE_INC)/storage -I$(ENGINE_INC)/system -I$(ENGINE_INC)/memory -I$(ENGINE_INC)/memory/garbage_collector
override CXXFLAGS +=  -I$(ENGINE_INC)/infra -I$(ENGINE_INC)/infra/config -I$(ENGINE_INC)/infra/containers  -I$(ENGINE_INC)/infra/stats -I$(ENGINE_INC)/infra/synchronization -I$(ENGINE_INC)/concurrency_control -I$(ENGINE_INC)/storage/index -I$(ENGINE_INC)/system/transaction -I$(ENGINE_INC)/system/common -I$(ENGINE_INC)/system/statistics -I$(ENGINE_INC)/system/transaction_logger -I$(ENGINE_INC)/system/transaction_logger/asynchronous_redo_log -I$(ENGINE_INC)/system/transaction_logger/synchronous_redo_log -I$(ENGINE_INC)/system/transaction_logger/group_synchronous_redo_log -I$(ENGINE_INC)/system/checkpoint -I$(ENGINE_INC)/system/recovery -I$(ENGINE_INC)/utils

override CXXFLAGS += -faligned-new -fno-rtti

override CXXFLAGS += -D__STDC_FORMAT_MACROS

$(OBJS): | buildrepo

install: install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall: uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

clean: 
	rm -rf $(OBJ_DIR)

buildrepo:
	@$(call make-repo)

$(OBJ_DIR)/%.o: %.cpp
	$(COMPILE.cpp) -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR);
endef

