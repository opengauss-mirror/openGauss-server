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
#     Makefile for the pldebugger
#
# IDENTIFICATION
#        contrib/pldebugger/Makefile
#
# ---------------------------------------------------------------------------------------

all:pldebuger_target
install:install-data

top_builddir = ../../
include $(top_builddir)/src/Makefile.global
PLDEBUGGER_DIR=$(PLDBG_HOME)
#source code
PLDEBUGGER_PACKAGE=pldebugger_3_0
PLDEBUGGER_PATCH=opengauss_pldebugger

.PHONY: pldebugger_target
pldebuger_target:
	@$(call create_pldebugger_sources)
	@make -C output/$(PLDEBUGGER_PACKAGE)

.PHONY: install-data
install-data: pldebugger_target
	@make -C output/$(PLDEBUGGER_PACKAGE) install

uninstall distclean clean:
	@rm -rf $(PLDEBUGGER_DIR)/$(PLDEBUGGER_PACKAGE) ./output
define create_pldebugger_sources
        mkdir output; \
        cp -f $(PLDEBUGGER_DIR)/* output/; \
        cd ./output/; \
        sh patch.sh;  \
        if [ $$? -eq 1 ]; then \
            exit 1; \
        fi;
endef
